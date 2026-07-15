use super::*;

#[derive(Clone)]
enum MetadataValue {
    Text(String),
    Bool(bool),
}

fn metadata_rows(table: &str, sources: &[FederatedSource]) -> Vec<Vec<MetadataValue>> {
    match table {
        "aql_tables" => QUERY_SCHEMAS
            .iter()
            .map(|schema| {
                vec![
                    MetadataValue::Text(schema.name.to_string()),
                    MetadataValue::Text(
                        if schema.name.starts_with("aql_") {
                            "metadata"
                        } else {
                            "canonical"
                        }
                        .to_string(),
                    ),
                ]
            })
            .collect(),
        "aql_columns" => QUERY_SCHEMAS
            .iter()
            .flat_map(|schema| {
                schema.columns.iter().map(move |column| {
                    vec![
                        MetadataValue::Text(schema.name.to_string()),
                        MetadataValue::Text(column.name.to_string()),
                        MetadataValue::Text(query_data_type_name(column.data_type).to_string()),
                        MetadataValue::Bool(column.nullable),
                        MetadataValue::Text(access_class_name(column.access).to_string()),
                    ]
                })
            })
            .collect(),
        "aql_sources" => sources
            .iter()
            .map(|source| {
                vec![
                    MetadataValue::Text(source.manifest.source_id.to_string()),
                    MetadataValue::Text(source.manifest.agent_id.clone()),
                    MetadataValue::Text(source.manifest.display_name.clone()),
                    MetadataValue::Text(source.manifest.format_fingerprint.clone()),
                    MetadataValue::Text(
                        if source.manifest.snapshot.is_some() {
                            "weak"
                        } else {
                            "unavailable"
                        }
                        .to_string(),
                    ),
                ]
            })
            .collect(),
        "aql_capabilities" => sources
            .iter()
            .flat_map(|source| {
                QUERY_SCHEMAS
                    .iter()
                    .filter(|schema| !schema.name.starts_with("aql_"))
                    .map(move |schema| {
                        vec![
                            MetadataValue::Text(source.manifest.source_id.to_string()),
                            MetadataValue::Text(schema.name.to_string()),
                            MetadataValue::Bool(
                                schema.name == "agents"
                                    || source
                                        .manifest
                                        .capabilities
                                        .iter()
                                        .any(|capability| capability == schema.name),
                            ),
                        ]
                    })
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn metadata_array(rows: &[Vec<MetadataValue>], index: usize) -> Result<ArrayRef> {
    let Some(first) = rows.first().and_then(|row| row.get(index)) else {
        return Err(DataFusionError::Plan(
            "metadata column is unavailable".to_string(),
        ));
    };
    match first {
        MetadataValue::Text(_) => Ok(Arc::new(StringArray::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataValue::Text(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
        MetadataValue::Bool(_) => Ok(Arc::new(BooleanArray::from(
            rows.iter()
                .map(|row| match row.get(index) {
                    Some(MetadataValue::Bool(value)) => Some(*value),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        ))),
    }
}

fn query_data_type_name(data_type: QueryDataType) -> &'static str {
    match data_type {
        QueryDataType::Text => "VARCHAR",
        QueryDataType::Int64 => "BIGINT",
        QueryDataType::Bool => "BOOLEAN",
        QueryDataType::Timestamp => "TIMESTAMP",
        QueryDataType::Json => "JSON",
    }
}

fn access_class_name(access: AccessClass) -> &'static str {
    match access {
        AccessClass::Safe => "safe",
        AccessClass::Path => "path",
        AccessClass::Content => "content",
        AccessClass::ToolInput => "tool-input",
        AccessClass::ToolOutput => "tool-output",
        AccessClass::Secret => "secret",
    }
}

#[derive(Clone)]
pub(super) struct Binding {
    pub(super) sources: Vec<FederatedSource>,
    pub(super) options: QueryOptions,
    pub(super) metadata: Arc<Mutex<QueryMetadata>>,
}

pub(super) struct DeferredTable {
    table: &'static QueryTableSchema,
    binding: Mutex<Option<Binding>>,
    schema: SchemaRef,
}

impl std::fmt::Debug for DeferredTable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeferredTable")
            .field("table", &self.table.name)
            .finish_non_exhaustive()
    }
}

impl DeferredTable {
    pub(super) fn new(table: &'static QueryTableSchema) -> Self {
        let schema = Arc::new(Schema::new(
            table
                .columns
                .iter()
                .map(|column| {
                    Field::new(
                        column.name,
                        match column.data_type {
                            QueryDataType::Text | QueryDataType::Json => DataType::Utf8,
                            QueryDataType::Int64 => DataType::Int64,
                            QueryDataType::Bool => DataType::Boolean,
                            QueryDataType::Timestamp => {
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                            }
                        },
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        Self {
            table,
            binding: Mutex::new(None),
            schema,
        }
    }

    pub(super) fn bind(&self, binding: Binding) -> std::result::Result<(), QueryError> {
        let mut slot = self.binding.lock().map_err(|_| QueryError::SqlRejected {
            stage: "bind",
            reason: "query provider state is unavailable",
        })?;
        *slot = Some(binding);
        Ok(())
    }
}

#[async_trait]
impl TableProvider for DeferredTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if expr_to_predicate(filter).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let binding = self
            .binding
            .lock()
            .map_err(|_| DataFusionError::Execution("query provider state is unavailable".into()))?
            .clone()
            .ok_or_else(|| DataFusionError::Execution("query provider is not bound".into()))?;
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.table.columns.len()).collect());
        let projected_schema = Arc::new(self.schema.project(&projection)?);
        let partition = Arc::new(AdapterPartition {
            table: self.table,
            binding,
            projection,
            schema: projected_schema.clone(),
            limit,
            predicates: filters.iter().filter_map(expr_to_predicate).collect(),
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            projected_schema,
            vec![partition],
            None,
            [],
            false,
            limit,
        )?))
    }
}

struct AdapterPartition {
    table: &'static QueryTableSchema,
    binding: Binding,
    projection: Vec<usize>,
    schema: SchemaRef,
    limit: Option<usize>,
    predicates: Vec<aql_adapter_api::Predicate>,
}

impl std::fmt::Debug for AdapterPartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdapterPartition")
            .field("table", &self.table.name)
            .field("projection", &self.projection)
            .field("predicates", &self.predicates.len())
            .finish_non_exhaustive()
    }
}

impl PartitionStream for AdapterPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> SendableRecordBatchStream {
        let reservation =
            MemoryConsumer::new("AQL session reconciliation").register(context.memory_pool());
        let state = AdapterBatchState::new(self, reservation);
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream::unfold(state, |mut state| async move {
                state.next_batch().map(|batch| (batch, state))
            }),
        ))
    }
}

struct AdapterBatchState {
    table: &'static QueryTableSchema,
    table_name: Option<TableName>,
    binding: Binding,
    projection: Vec<usize>,
    requested: Vec<ColumnName>,
    schema: SchemaRef,
    limit: Option<usize>,
    predicates: Vec<aql_adapter_api::Predicate>,
    source_index: usize,
    current: Option<RecordStream>,
    current_diagnostics: Option<ScanDiagnostics>,
    reconciliation_memory: MemoryReservation,
    session_records: Vec<CanonicalRecord>,
    pending_sessions: VecDeque<CanonicalRecord>,
    sessions_reconciled: bool,
    seen: BTreeSet<String>,
    emitted: usize,
    agents_emitted: bool,
    metadata_emitted: bool,
    finished: bool,
}

impl AdapterBatchState {
    fn new(partition: &AdapterPartition, reconciliation_memory: MemoryReservation) -> Self {
        let table_name = match partition.table.name {
            "agents" => None,
            "sessions" => Some(TableName::Sessions),
            "messages" => Some(TableName::Messages),
            "tool_calls" => Some(TableName::ToolCalls),
            "usage" => Some(TableName::Usage),
            "session_edges" => Some(TableName::SessionEdges),
            "artifacts" => Some(TableName::Artifacts),
            _ => None,
        };
        Self {
            table: partition.table,
            table_name,
            binding: partition.binding.clone(),
            projection: partition.projection.clone(),
            requested: partition
                .projection
                .iter()
                .map(|index| ColumnName::new(partition.table.columns[*index].name))
                .collect(),
            schema: partition.schema.clone(),
            limit: partition.limit,
            predicates: partition.predicates.clone(),
            source_index: 0,
            current: None,
            current_diagnostics: None,
            reconciliation_memory,
            session_records: Vec::new(),
            pending_sessions: VecDeque::new(),
            sessions_reconciled: false,
            seen: BTreeSet::new(),
            emitted: 0,
            agents_emitted: false,
            metadata_emitted: false,
            finished: false,
        }
    }

    fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        if self.finished {
            return None;
        }
        if self.table.name.starts_with("aql_") {
            return self.next_metadata_batch();
        }
        if self.table.name == "agents" {
            return self.next_agents_batch();
        }

        let Some(table_name) = self.table_name else {
            self.finished = true;
            return Some(Err(DataFusionError::Plan(
                "logical plan contains an unknown table".to_string(),
            )));
        };
        if table_name == TableName::Sessions {
            return self.next_session_batch();
        }
        self.next_record_batch(table_name)
    }

    fn next_agents_batch(&mut self) -> Option<Result<RecordBatch>> {
        self.finished = true;
        if self.agents_emitted {
            return None;
        }
        self.agents_emitted = true;
        if let Err(error) = self
            .binding
            .options
            .budget
            .charge_records(self.binding.sources.len() as u64)
        {
            return Some(Err(DataFusionError::External(Box::new(error))));
        }
        let manifests = self
            .binding
            .sources
            .iter()
            .map(|source| source.manifest.clone())
            .collect::<Vec<_>>();
        let arrays = self
            .projection
            .iter()
            .map(|index| agent_array(self.table.columns[*index].name, &manifests))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(self.binding.sources.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn next_metadata_batch(&mut self) -> Option<Result<RecordBatch>> {
        self.finished = true;
        if self.metadata_emitted {
            return None;
        }
        self.metadata_emitted = true;
        let rows = metadata_rows(self.table.name, &self.binding.sources);
        if let Err(error) = self
            .binding
            .options
            .budget
            .charge_records(rows.len() as u64)
        {
            return Some(Err(DataFusionError::External(Box::new(error))));
        }
        let arrays = self
            .projection
            .iter()
            .map(|index| metadata_array(&rows, *index))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(rows.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn next_record_batch(&mut self, table_name: TableName) -> Option<Result<RecordBatch>> {
        let mut records = Vec::with_capacity(1024);
        while records.len() < 1024 && self.limit.is_none_or(|limit| self.emitted < limit) {
            if self.current.is_none() {
                let remaining = self
                    .limit
                    .map(|limit| limit.saturating_sub(self.emitted) as u64);
                match self.open_next_source(table_name, remaining) {
                    Ok(true) => {}
                    Ok(false) => {
                        self.finished = true;
                        break;
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
            }

            match self.current.as_mut().and_then(Iterator::next) {
                Some(Ok(record)) => {
                    if let Err(error) = validate_record_metrics(&record) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    if !self.seen.insert(record_identity(&record)) {
                        self.finished = true;
                        return Some(Err(DataFusionError::Execution(
                            "adapter produced a duplicate canonical entity".to_string(),
                        )));
                    }
                    records.push(record);
                    self.emitted += 1;
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Some(Err(DataFusionError::External(Box::new(error))));
                }
                None => {
                    if let Err(error) = self.flush_current_diagnostics() {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.current = None;
                    self.source_index += 1;
                }
            }
        }
        if records.is_empty() {
            self.finished = true;
            return None;
        }
        if self.limit.is_some_and(|limit| self.emitted >= limit) {
            if let Err(error) = self.flush_current_diagnostics() {
                self.finished = true;
                return Some(Err(error));
            }
            self.current = None;
            self.finished = true;
        }
        let arrays = self
            .projection
            .iter()
            .map(|index| record_array(self.table, self.table.columns[*index].name, &records))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(records.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn open_next_source(&mut self, table_name: TableName, limit: Option<u64>) -> Result<bool> {
        loop {
            let Some(source) = self.binding.sources.get(self.source_index) else {
                return Ok(false);
            };
            let capability = table_capability(table_name);
            if !source
                .manifest
                .capabilities
                .iter()
                .any(|candidate| candidate == capability)
            {
                self.source_index += 1;
                continue;
            }
            let predicates = if table_name == TableName::Sessions {
                self.predicates
                    .iter()
                    .filter(|predicate| session_identity_predicate(predicate))
                    .cloned()
                    .collect()
            } else {
                self.predicates.clone()
            };
            let result = source
                .adapter
                .scan(ScanRequest {
                    source: source.manifest.clone(),
                    table: table_name,
                    projection: self.requested.clone(),
                    predicates,
                    limit,
                    order_hint: Vec::new(),
                    access: self.binding.options.access,
                    budget: self.binding.options.budget.clone(),
                    cancellation: self.binding.options.cancellation.clone(),
                    snapshot: source.manifest.snapshot.clone(),
                })
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let mut metadata = self.binding.metadata.lock().map_err(|_| {
                DataFusionError::External(Box::new(aql_adapter_api::AdapterError::Internal {
                    stage: "query_metadata".to_string(),
                }))
            })?;
            metadata.scans.push(ScanMetadata {
                table: self.table.name.to_string(),
                source_id: source.manifest.source_id.to_string(),
                predicate_pushdown: result
                    .pushdown
                    .predicates
                    .iter()
                    .map(|state| format!("{state:?}").to_ascii_lowercase())
                    .collect(),
                limit_pushdown: result
                    .pushdown
                    .limit
                    .map(|state| format!("{state:?}").to_ascii_lowercase()),
                ordering_pushdown: result
                    .pushdown
                    .ordering
                    .iter()
                    .map(|state| format!("{state:?}").to_ascii_lowercase())
                    .collect(),
                snapshot_strength: format!("{:?}", result.snapshot.strength).to_ascii_lowercase(),
                stale: result.snapshot.stale,
            });
            drop(metadata);
            self.current_diagnostics = Some(result.diagnostics);
            self.current = Some(result.records);
            return Ok(true);
        }
    }

    fn next_session_batch(&mut self) -> Option<Result<RecordBatch>> {
        if !self.sessions_reconciled
            && let Err(error) = self.reconcile_sessions()
        {
            self.finished = true;
            return Some(Err(error));
        }

        let records = self
            .pending_sessions
            .drain(..self.pending_sessions.len().min(1024))
            .collect::<Vec<_>>();
        if records.is_empty() {
            self.finished = true;
            return None;
        }
        self.emitted += records.len();
        if self.pending_sessions.is_empty() {
            self.finished = true;
        }
        let arrays = self
            .projection
            .iter()
            .map(|index| record_array(self.table, self.table.columns[*index].name, &records))
            .collect::<Result<Vec<_>>>();
        Some(arrays.and_then(|arrays| {
            RecordBatch::try_new_with_options(
                self.schema.clone(),
                arrays,
                &RecordBatchOptions::new().with_row_count(Some(records.len())),
            )
            .map_err(Into::into)
        }))
    }

    fn reconcile_sessions(&mut self) -> Result<()> {
        self.collect_session_records()?;
        let reconciled = Catalog.reconcile_sessions(std::mem::take(&mut self.session_records));
        let mut metadata = self.binding.metadata.lock().map_err(|_| {
            DataFusionError::External(Box::new(aql_adapter_api::AdapterError::Internal {
                stage: "query_metadata".to_string(),
            }))
        })?;
        metadata
            .warnings
            .extend(reconciled.warnings.iter().map(|warning| {
                format!(
                    "catalog:{:?}:entity_id={}:field={}",
                    warning.kind,
                    warning.entity_id,
                    warning.field.as_deref().unwrap_or("none")
                )
            }));
        drop(metadata);

        let mut records = reconciled
            .records
            .into_iter()
            .map(CanonicalRecord::Session)
            .collect::<Vec<_>>();
        if let Some(limit) = self.limit {
            records.truncate(limit);
        }
        self.pending_sessions = records.into();
        self.sessions_reconciled = true;
        Ok(())
    }

    fn collect_session_records(&mut self) -> Result<()> {
        loop {
            if self.current.is_none() && !self.open_next_source(TableName::Sessions, None)? {
                return Ok(());
            }
            match self.current.as_mut().and_then(Iterator::next) {
                Some(Ok(record)) => {
                    validate_record_metrics(&record)?;
                    self.reconciliation_memory
                        .try_grow(retained_session_bytes(&record)?)?;
                    self.session_records.push(record);
                }
                Some(Err(error)) => return Err(DataFusionError::External(Box::new(error))),
                None => {
                    self.flush_current_diagnostics()?;
                    self.current = None;
                    self.source_index += 1;
                }
            }
        }
    }

    fn flush_current_diagnostics(&mut self) -> Result<()> {
        let Some(diagnostics) = self.current_diagnostics.take() else {
            return Ok(());
        };
        let warnings = diagnostics
            .snapshot()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut metadata = self.binding.metadata.lock().map_err(|_| {
            DataFusionError::External(Box::new(aql_adapter_api::AdapterError::Internal {
                stage: "query_metadata".to_string(),
            }))
        })?;
        metadata.warnings.extend(warnings.iter().map(|warning| {
            format!(
                "adapter:{:?}:source_kind={}:stage={}",
                warning.kind, warning.source_kind, warning.stage
            )
        }));
        Ok(())
    }
}

fn session_identity_predicate(predicate: &aql_adapter_api::Predicate) -> bool {
    use aql_adapter_api::Predicate;

    match predicate {
        Predicate::Eq(column, _)
        | Predicate::In(column, _)
        | Predicate::Range { column, .. }
        | Predicate::IsNull(column) => column.as_str() == "session_id",
        Predicate::And(predicates) => predicates.iter().all(session_identity_predicate),
        Predicate::Unsupported(_) => false,
    }
}

fn retained_session_bytes(record: &CanonicalRecord) -> Result<usize> {
    let CanonicalRecord::Session(session) = record else {
        return Err(DataFusionError::Execution(
            "sessions adapter produced a non-session record".to_string(),
        ));
    };
    let mut bytes = std::mem::size_of_val(session);
    for value in [
        session.session_id.as_str(),
        session.native_id.as_str(),
        session.source_id.as_str(),
        &session.agent_id,
    ] {
        bytes = bytes.saturating_add(value.len());
    }
    for value in [
        &session.title,
        &session.preview,
        &session.cwd,
        &session.project,
        &session.model,
        &session.provider,
        &session.status,
    ]
    .into_iter()
    .flatten()
    {
        bytes = bytes.saturating_add(value.capacity());
    }
    for (field, provenance) in &session.provenance {
        bytes = bytes
            .saturating_add(std::mem::size_of::<(String, Vec<aql_model::Provenance>)>())
            .saturating_add(3 * std::mem::size_of::<usize>())
            .saturating_add(field.capacity())
            .saturating_add(
                provenance
                    .capacity()
                    .saturating_mul(std::mem::size_of::<aql_model::Provenance>()),
            );
        for item in provenance {
            bytes = bytes
                .saturating_add(item.source_id.as_str().len())
                .saturating_add(item.source_kind.capacity())
                .saturating_add(item.source_locator.capacity())
                .saturating_add(item.source_version.as_ref().map_or(0, String::capacity))
                .saturating_add(item.watermark.as_ref().map_or(0, String::capacity));
        }
    }
    for (key, value) in &session.extensions {
        bytes = bytes
            .saturating_add(std::mem::size_of::<(String, serde_json::Value)>())
            .saturating_add(3 * std::mem::size_of::<usize>())
            .saturating_add(key.capacity())
            .saturating_add(json_retained_bytes(value));
    }
    Ok(bytes)
}

fn json_retained_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<serde_json::Value>())
            .saturating_add(
                values
                    .iter()
                    .map(json_retained_bytes)
                    .fold(0, usize::saturating_add),
            ),
        serde_json::Value::Object(values) => values.iter().fold(0, |bytes, (key, value)| {
            bytes
                .saturating_add(std::mem::size_of::<(String, serde_json::Value)>())
                .saturating_add(3 * std::mem::size_of::<usize>())
                .saturating_add(key.capacity())
                .saturating_add(json_retained_bytes(value))
        }),
    }
}

fn table_capability(table: TableName) -> &'static str {
    match table {
        TableName::Sessions => "sessions",
        TableName::Messages => "messages",
        TableName::ToolCalls => "tool_calls",
        TableName::Usage => "usage",
        TableName::SessionEdges => "session_edges",
        TableName::Artifacts => "artifacts",
    }
}

fn record_identity(record: &CanonicalRecord) -> String {
    match record {
        CanonicalRecord::Session(value) => value.session_id.to_string(),
        CanonicalRecord::Message(value) => value.message_id.to_string(),
        CanonicalRecord::ToolCall(value) => value.tool_call_id.to_string(),
        CanonicalRecord::Usage(value) => value.usage_id.to_string(),
        CanonicalRecord::SessionEdge(value) => value.edge_id.to_string(),
        CanonicalRecord::Artifact(value) => value.artifact_id.to_string(),
    }
}

fn validate_record_metrics(record: &CanonicalRecord) -> Result<()> {
    let invalid = match record {
        CanonicalRecord::Session(value) => value.tokens_used.is_some_and(|tokens| tokens < 0),
        CanonicalRecord::Message(value) => {
            let tokens = [value.input_tokens, value.output_tokens, value.cached_tokens];
            tokens.into_iter().flatten().any(|tokens| tokens < 0)
                || tokens
                    .into_iter()
                    .flatten()
                    .try_fold(0_i64, i64::checked_add)
                    .is_none()
        }
        CanonicalRecord::Usage(value) => [
            value.input_tokens,
            value.output_tokens,
            value.cached_tokens,
            value.total_tokens,
        ]
        .into_iter()
        .flatten()
        .chain([
            value.message_count,
            value.tool_call_count,
            value.error_count,
        ])
        .any(|tokens| tokens < 0),
        CanonicalRecord::ToolCall(_)
        | CanonicalRecord::SessionEdge(_)
        | CanonicalRecord::Artifact(_) => false,
    };
    if invalid {
        Err(DataFusionError::Execution(
            "canonical metrics are invalid".to_string(),
        ))
    } else {
        Ok(())
    }
}

pub(super) fn expr_to_predicate(expr: &Expr) -> Option<aql_adapter_api::Predicate> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            Some(aql_adapter_api::Predicate::And(vec![
                expr_to_predicate(&binary.left)?,
                expr_to_predicate(&binary.right)?,
            ]))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            column_and_literal(&binary.left, &binary.right)
                .or_else(|| column_and_literal(&binary.right, &binary.left))
                .map(|(column, literal)| aql_adapter_api::Predicate::Eq(column, literal))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::GtEq || binary.op == Operator::LtEq => {
            let (column, literal) = column_and_literal(&binary.left, &binary.right)?;
            Some(aql_adapter_api::Predicate::Range {
                column,
                lower: (binary.op == Operator::GtEq).then_some(literal.clone()),
                upper: (binary.op == Operator::LtEq).then_some(literal),
            })
        }
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(column) => Some(aql_adapter_api::Predicate::IsNull(ColumnName::new(
                column.name.clone(),
            ))),
            _ => None,
        },
        Expr::InList(list) if !list.negated => {
            let Expr::Column(column) = list.expr.as_ref() else {
                return None;
            };
            let literals = list
                .list
                .iter()
                .map(expr_literal)
                .collect::<Option<Vec<_>>>()?;
            Some(aql_adapter_api::Predicate::In(
                ColumnName::new(column.name.clone()),
                literals,
            ))
        }
        _ => None,
    }
}

fn column_and_literal(
    column: &Expr,
    literal: &Expr,
) -> Option<(ColumnName, aql_adapter_api::Literal)> {
    let Expr::Column(column) = column else {
        return None;
    };
    Some((ColumnName::new(column.name.clone()), expr_literal(literal)?))
}

fn expr_literal(expr: &Expr) -> Option<aql_adapter_api::Literal> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    match value {
        ScalarValue::Null => Some(aql_adapter_api::Literal::Null),
        ScalarValue::Boolean(value) => value.map(aql_adapter_api::Literal::Bool),
        ScalarValue::Int64(value) => value.map(aql_adapter_api::Literal::Integer),
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => {
            value.clone().map(aql_adapter_api::Literal::Text)
        }
        ScalarValue::TimestampMillisecond(value, _) => value.map(aql_adapter_api::Literal::Integer),
        _ => None,
    }
}
