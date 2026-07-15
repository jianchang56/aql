use super::*;
use aql_adapter_api::{
    AdapterError, AdapterSchema, Capabilities, ColumnCapability, ProbeRequest, ProbeResult,
    PushdownReport, PushdownState, ScanResult, SnapshotReport, SnapshotStrength,
};
use aql_adapter_codex::CodexAdapter;
use aql_model::{
    EntityId, IdentityConfidence, MessageRecord, NativeId, SessionRecord, SnapshotState,
    SnapshotToken, SourceId, ToolCallRecord,
};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::prelude::{col, lit};

#[test]
fn scalar_parameters_bind_only_value_placeholders() {
    let parameters = BTreeMap::from([
        (
            "name".to_string(),
            SqlParameter::Text("x' OR true --".to_string()),
        ),
        ("limit".to_string(), SqlParameter::Int64(5)),
    ]);
    let bound = bind_sql_parameters(
        "SELECT session_id FROM sessions WHERE title = :name LIMIT :limit",
        &parameters,
    )
    .expect("scalar parameters bind");
    assert!(bound.contains("title = 'x'' OR true --'"));
    validate_read_only_sql(&bound).expect("bound SQL remains read only");

    assert!(bind_sql_parameters("SELECT :missing", &BTreeMap::new()).is_err());
    assert!(
        bind_sql_parameters(
            "SELECT session_id FROM sessions",
            &BTreeMap::from([("unused".to_string(), SqlParameter::Bool(true))]),
        )
        .is_err()
    );
    assert!(
        bind_sql_parameters(
            "SELECT session_id FROM :table",
            &BTreeMap::from([(
                "table".to_string(),
                SqlParameter::Text("sessions".to_string())
            )]),
        )
        .is_err()
    );
    assert!(bind_sql_parameters(&"x".repeat(MAX_SQL_BYTES + 1), &BTreeMap::new()).is_err());
}

#[test]
fn limit_offset_and_float_parameters_remain_explicit() {
    let sql = validate_read_only_sql(
        "SELECT session_id FROM sessions ORDER BY session_id LIMIT 2 OFFSET 1",
    )
    .expect("explicit LIMIT/OFFSET is accepted");
    let mut parameters = BTreeMap::new();
    parameters.insert("ratio".to_string(), SqlParameter::Float64(1.5));
    let bound = bind_sql_parameters(
        "SELECT session_id FROM sessions WHERE tokens_used > :ratio",
        &parameters,
    )
    .expect("float parameter binds as a scalar");
    assert!(bound.contains("1.5"));
    let _ = sql;
}

#[tokio::test]
async fn ordering_warning_is_limited_to_unordered_pagination() {
    let source = FederatedSource {
        adapter: Arc::new(CodexAdapter::new(b"synthetic-adapter".to_vec())),
        manifest: SourceManifest {
            source_id: SourceId::new("synthetic-source"),
            agent_id: "codex".to_string(),
            display_name: "Synthetic Codex".to_string(),
            data_root_token: "root:synthetic".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        },
    };
    let count = validate_read_only_sql("SELECT COUNT(*) FROM aql_tables")
        .expect("aggregate query is valid");
    let count_result = prepare_query(&count, QueryOptions::default())
        .await
        .expect("aggregate query prepares")
        .execute(vec![source.clone()])
        .await
        .expect("aggregate query executes");
    assert!(count_result.metadata.warnings.is_empty());

    let page = validate_read_only_sql("SELECT table_name FROM aql_tables LIMIT 1")
        .expect("page query is valid");
    let page_result = prepare_query(&page, QueryOptions::default())
        .await
        .expect("page query prepares")
        .execute(vec![source])
        .await
        .expect("page query executes");
    assert_eq!(
        page_result.metadata.warnings,
        vec!["result ordering is unspecified; add ORDER BY for stable pagination"]
    );
}

#[tokio::test]
async fn metadata_rows_do_not_consume_source_record_budget() {
    let sql =
        validate_read_only_sql("SELECT table_name FROM aql_tables ORDER BY table_name LIMIT 1")
            .expect("metadata query is valid");
    let options = QueryOptions {
        budget: ResourceBudget {
            max_records: 1,
            ..ResourceBudget::default()
        },
        ..QueryOptions::default()
    };
    let source = FederatedSource {
        adapter: Arc::new(CodexAdapter::new(b"synthetic-adapter".to_vec())),
        manifest: SourceManifest {
            source_id: SourceId::new("synthetic-source"),
            agent_id: "codex".to_string(),
            display_name: "Synthetic Codex".to_string(),
            data_root_token: "root:synthetic".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        },
    };
    let result = prepare_query(&sql, options)
        .await
        .expect("metadata query prepares")
        .execute(vec![source])
        .await
        .expect("internal metadata does not consume the source record budget");
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
}

struct SyntheticSessionAdapter {
    session: SessionRecord,
}

struct SyntheticUsageAdapter {
    sessions: Vec<SessionRecord>,
    messages: Vec<MessageRecord>,
    tool_calls: Vec<ToolCallRecord>,
}

impl AgentAdapter for SyntheticUsageAdapter {
    fn id(&self) -> &'static str {
        "synthetic-usage"
    }

    fn probe(&self, _request: &ProbeRequest) -> std::result::Result<ProbeResult, AdapterError> {
        unreachable!("engine test binds its manifest directly")
    }

    fn capabilities(&self, _manifest: &SourceManifest) -> Capabilities {
        Capabilities {
            tables: vec![
                TableName::Sessions,
                TableName::Messages,
                TableName::ToolCalls,
            ],
            columns: Vec::new(),
            snapshot_strength: SnapshotStrength::Strong,
        }
    }

    fn schema(&self, _manifest: &SourceManifest) -> AdapterSchema {
        AdapterSchema {
            columns: Vec::new(),
        }
    }

    fn scan(&self, request: ScanRequest) -> std::result::Result<ScanResult, AdapterError> {
        let records: Vec<CanonicalRecord> = match request.table {
            TableName::Sessions => self
                .sessions
                .iter()
                .cloned()
                .map(CanonicalRecord::Session)
                .collect(),
            TableName::Messages => self
                .messages
                .iter()
                .cloned()
                .map(CanonicalRecord::Message)
                .collect(),
            TableName::ToolCalls => self
                .tool_calls
                .iter()
                .cloned()
                .map(CanonicalRecord::ToolCall)
                .collect(),
            TableName::Usage | TableName::SessionEdges | TableName::Artifacts => {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "synthetic_usage_table".to_string(),
                });
            }
        };
        Ok(ScanResult {
            records: Box::new(records.into_iter().map(Ok)),
            pushdown: PushdownReport {
                predicates: request
                    .predicates
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
                limit: request.limit.map(|_| PushdownState::Unsupported),
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics: ScanDiagnostics::default(),
            snapshot: SnapshotReport {
                token: request.snapshot,
                strength: SnapshotStrength::Strong,
                stale: false,
            },
        })
    }
}

impl AgentAdapter for SyntheticSessionAdapter {
    fn id(&self) -> &'static str {
        "synthetic"
    }

    fn probe(&self, _request: &ProbeRequest) -> std::result::Result<ProbeResult, AdapterError> {
        unreachable!("federated engine tests bind manifests directly")
    }

    fn capabilities(&self, _manifest: &SourceManifest) -> Capabilities {
        Capabilities {
            tables: vec![TableName::Sessions],
            columns: self.schema(_manifest).columns,
            snapshot_strength: SnapshotStrength::Strong,
        }
    }

    fn schema(&self, _manifest: &SourceManifest) -> AdapterSchema {
        AdapterSchema {
            columns: vec![
                ColumnCapability {
                    name: ColumnName::new("session_id"),
                    access: AccessClass::Safe,
                },
                ColumnCapability {
                    name: ColumnName::new("title"),
                    access: AccessClass::Content,
                },
            ],
        }
    }

    fn scan(&self, request: ScanRequest) -> std::result::Result<ScanResult, AdapterError> {
        assert_eq!(request.table, TableName::Sessions);
        let diagnostics = ScanDiagnostics::default();
        let records = request
            .predicates
            .iter()
            .all(|predicate| synthetic_session_matches(&self.session, predicate))
            .then(|| CanonicalRecord::Session(self.session.clone()))
            .into_iter()
            .map(Ok);
        Ok(ScanResult {
            records: Box::new(records),
            pushdown: PushdownReport {
                predicates: request
                    .predicates
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
                limit: request.limit.map(|_| PushdownState::Exact),
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics,
            snapshot: SnapshotReport {
                token: request.snapshot,
                strength: SnapshotStrength::Strong,
                stale: false,
            },
        })
    }
}

fn synthetic_session_matches(
    session: &SessionRecord,
    predicate: &aql_adapter_api::Predicate,
) -> bool {
    use aql_adapter_api::{Literal, Predicate};

    match predicate {
        Predicate::Eq(column, Literal::Text(value)) if column.as_str() == "session_id" => {
            session.session_id.as_str() == value
        }
        Predicate::Eq(column, Literal::Text(value)) if column.as_str() == "title" => {
            session.title.as_deref() == Some(value)
        }
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| synthetic_session_matches(session, predicate)),
        _ => true,
    }
}

fn synthetic_session(source_id: &str, native_id: &str) -> SessionRecord {
    let source_id = SourceId::new(source_id);
    let native_id = NativeId::new(native_id);
    SessionRecord {
        session_id: EntityId::from_parts("synthetic", &source_id, &native_id),
        native_id,
        source_id,
        agent_id: "synthetic".to_string(),
        title: None,
        preview: None,
        cwd: None,
        project: None,
        model: None,
        provider: None,
        created_at: None,
        updated_at: None,
        status: None,
        archived: None,
        message_count: None,
        tool_call_count: None,
        tokens_used: None,
        identity_confidence: IdentityConfidence::Exact,
        snapshot_state: SnapshotState::Consistent,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

fn synthetic_message(
    source_id: &str,
    session_id: &EntityId,
    suffix: &str,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_tokens: Option<i64>,
    is_error: Option<bool>,
) -> MessageRecord {
    MessageRecord {
        message_id: EntityId::new(format!("{session_id}:message-{suffix}")),
        session_id: session_id.clone(),
        source_id: SourceId::new(source_id),
        sequence: 1,
        role: "assistant".to_string(),
        kind: Some("message".to_string()),
        content: None,
        content_json: None,
        model: Some("synthetic-model".to_string()),
        created_at: None,
        input_tokens,
        output_tokens,
        cached_tokens,
        is_error,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

fn synthetic_tool_call(source_id: &str, session_id: &EntityId) -> ToolCallRecord {
    ToolCallRecord {
        tool_call_id: EntityId::new(format!("{session_id}:tool-1")),
        session_id: session_id.clone(),
        message_id: None,
        source_id: SourceId::new(source_id),
        sequence: 1,
        tool_name: "synthetic_tool".to_string(),
        namespace: None,
        arguments: None,
        output: None,
        status: Some("error".to_string()),
        started_at: None,
        ended_at: None,
        duration_ms: None,
        exit_code: Some(1),
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

#[tokio::test]
async fn sensitive_identifiers_are_rejected_before_engine_execution() {
    let sql = validate_read_only_sql("SELECT title FROM sessions").expect("valid SQL");
    let error = prepare_query(&sql, QueryOptions::default())
        .await
        .expect_err("title must require content access");
    assert!(matches!(error, QueryError::AccessDenied("content")));
}

#[tokio::test]
async fn agents_query_combines_manifests_bound_to_different_adapters() {
    let source = |source_id: &str, agent_id: &str| SourceManifest {
        source_id: SourceId::new(source_id),
        agent_id: agent_id.to_string(),
        display_name: agent_id.to_string(),
        data_root_token: format!("root:{source_id}"),
        format_fingerprint: format!("{agent_id}-fixture-v1"),
        capabilities: vec!["sessions".to_string()],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let sql = validate_read_only_sql("SELECT agent_id FROM agents ORDER BY agent_id")
        .expect("valid agents query");
    let result = prepare_query(&sql, QueryOptions::default())
        .await
        .expect("query prepares")
        .execute(vec![
            FederatedSource {
                adapter: Arc::new(CodexAdapter::new(b"adapter-a".to_vec())),
                manifest: source("source-a", "codex"),
            },
            FederatedSource {
                adapter: Arc::new(CodexAdapter::new(b"adapter-b".to_vec())),
                manifest: source("source-b", "claude-code"),
            },
        ])
        .await
        .expect("federated agents query succeeds");

    let values = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("agent_id is a string")
                .iter()
                .flatten()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec!["claude-code", "codex"]);
}

#[tokio::test]
async fn sessions_query_consumes_each_sources_bound_adapter() {
    let manifest = |source_id: &str| SourceManifest {
        source_id: SourceId::new(source_id),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: format!("root:{source_id}"),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec!["sessions".to_string()],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let sql = validate_read_only_sql("SELECT session_id FROM sessions ORDER BY session_id")
        .expect("valid sessions query");
    let result = prepare_query(&sql, QueryOptions::default())
        .await
        .expect("query prepares")
        .execute(vec![
            FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter {
                    session: synthetic_session("source-a", "session-a"),
                }),
                manifest: manifest("source-a"),
            },
            FederatedSource {
                adapter: Arc::new(SyntheticSessionAdapter {
                    session: synthetic_session("source-b", "session-b"),
                }),
                manifest: manifest("source-b"),
            },
        ])
        .await
        .expect("federated sessions query succeeds");

    assert_eq!(result.metadata.scans.len(), 2);
    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn federated_sessions_are_reconciled_before_publication() {
    let manifest = |source_id: &str| SourceManifest {
        source_id: SourceId::new(source_id),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: format!("root:{source_id}"),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec!["sessions".to_string()],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let mut first = synthetic_session("shared-source", "shared-session");
    first.title = Some("Synthetic first title".to_string());
    let mut second = first.clone();
    second.title = Some("Synthetic conflicting title".to_string());
    let sql = validate_read_only_sql("SELECT title FROM sessions").expect("valid query");
    let result = prepare_query(
        &sql,
        QueryOptions {
            access: AccessGrant {
                content: true,
                ..AccessGrant::default()
            },
            ..QueryOptions::default()
        },
    )
    .await
    .expect("query prepares")
    .execute(vec![
        FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter { session: first }),
            manifest: manifest("source-a"),
        },
        FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter { session: second }),
            manifest: manifest("source-b"),
        },
    ])
    .await
    .expect("matching sessions reconcile");

    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("title is a string")
            .value(0),
        "Synthetic first title"
    );
    assert!(
        result
            .metadata
            .warnings
            .iter()
            .any(|warning| warning.contains("catalog:FieldConflict"))
    );
}

#[tokio::test]
async fn federated_session_predicates_run_after_reconciliation() {
    let manifest = |source_id: &str| SourceManifest {
        source_id: SourceId::new(source_id),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: format!("root:{source_id}"),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec!["sessions".to_string()],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let mut authoritative = synthetic_session("shared-source", "shared-session");
    authoritative.title = Some("Synthetic authoritative title".to_string());
    let mut conflicting = authoritative.clone();
    conflicting.title = Some("Synthetic matching title".to_string());
    let sql = validate_read_only_sql(
        "SELECT session_id FROM sessions WHERE title = 'Synthetic matching title'",
    )
    .expect("valid query");
    let result = prepare_query(
        &sql,
        QueryOptions {
            access: AccessGrant {
                content: true,
                ..AccessGrant::default()
            },
            ..QueryOptions::default()
        },
    )
    .await
    .expect("query prepares")
    .execute(vec![
        FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter {
                session: authoritative,
            }),
            manifest: manifest("source-a"),
        },
        FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter {
                session: conflicting,
            }),
            manifest: manifest("source-b"),
        },
    ])
    .await
    .expect("query executes");

    assert_eq!(
        result
            .batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        0
    );
    assert!(
        result
            .metadata
            .scans
            .iter()
            .all(|scan| scan.predicate_pushdown.is_empty())
    );
}

#[tokio::test]
async fn session_reconciliation_obeys_query_memory_budget() {
    let mut session = synthetic_session("memory-source", "memory-session");
    session.title = Some("x".repeat(64 * 1024));
    let manifest = SourceManifest {
        source_id: SourceId::new("memory-source"),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: "root:memory-source".to_string(),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec!["sessions".to_string()],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let sql = validate_read_only_sql("SELECT title FROM sessions").expect("valid query");
    let result = prepare_query(
        &sql,
        QueryOptions {
            access: AccessGrant {
                content: true,
                ..AccessGrant::default()
            },
            max_memory_bytes: 32 * 1024,
            ..QueryOptions::default()
        },
    )
    .await
    .expect("query prepares")
    .execute(vec![FederatedSource {
        adapter: Arc::new(SyntheticSessionAdapter { session }),
        manifest,
    }])
    .await;
    let Err(error) = result else {
        panic!("reconciliation must respect the query memory pool");
    };

    assert!(error.to_string().contains("Resources exhausted"));
}

#[tokio::test]
async fn count_star_preserves_rows_for_zero_column_adapter_projection() {
    let manifest = SourceManifest {
        source_id: SourceId::new("source-count"),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: "root:count".to_string(),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec!["sessions".to_string()],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let sql = validate_read_only_sql("SELECT COUNT(*) AS sessions FROM sessions")
        .expect("valid count query");
    let result = prepare_query(&sql, QueryOptions::default())
        .await
        .expect("query prepares")
        .execute(vec![FederatedSource {
            adapter: Arc::new(SyntheticSessionAdapter {
                session: synthetic_session("source-count", "session-count"),
            }),
            manifest,
        }])
        .await
        .expect("count star succeeds");
    let counts = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is int64");
    assert_eq!(counts.value(0), 1);
}

#[tokio::test]
async fn streaming_metadata_requires_consuming_the_stream_to_eof() {
    let source = FederatedSource {
        adapter: Arc::new(SyntheticSessionAdapter {
            session: synthetic_session("source-stream", "session-stream"),
        }),
        manifest: SourceManifest {
            source_id: SourceId::new("source-stream"),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: "root:stream".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string()],
            snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
            warnings: Vec::new(),
        },
    };
    let sql =
        validate_read_only_sql("SELECT session_id FROM sessions").expect("valid streaming query");

    let unfinished = prepare_query(&sql, QueryOptions::default())
        .await
        .expect("query prepares")
        .execute_stream(vec![source.clone()])
        .await
        .expect("stream starts");
    assert!(matches!(
        unfinished.metadata.finish(),
        Err(QueryError::SqlRejected {
            stage: "metadata",
            ..
        })
    ));

    let StreamingQueryResult {
        mut stream,
        metadata,
    } = prepare_query(&sql, QueryOptions::default())
        .await
        .expect("query prepares")
        .execute_stream(vec![source])
        .await
        .expect("stream starts");
    let first = stream
        .next()
        .await
        .expect("stream yields a batch")
        .expect("batch succeeds");
    assert_eq!(first.num_rows(), 1);
    assert!(stream.next().await.is_none());
    let metadata = metadata.finish().expect("EOF finalizes metadata");
    assert_eq!(metadata.source_ids, vec!["source-stream"]);
}

#[tokio::test]
async fn privacy_functions_are_deterministic_and_do_not_return_original_values() {
    let mut session = synthetic_session("source-private", "session-private");
    session.title = Some("Synthetic private title".to_string());
    session.cwd = Some("/workspace/example/project".to_string());
    let adapter = Arc::new(SyntheticSessionAdapter { session });
    let manifest = SourceManifest {
        source_id: SourceId::new("source-private"),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: "root:private".to_string(),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec!["sessions".to_string()],
        snapshot: None,
        warnings: Vec::new(),
    };
    let sql = validate_read_only_sql(
            "SELECT REDACT(title, 'hash') AS hash_one, REDACT(title, 'hash') AS hash_two, REDACT(title, 'last4') AS tail, MASK_PATH(cwd, 2) AS masked_cwd, REDACT(CAST(NULL AS VARCHAR)) AS null_value FROM sessions",
        )
        .expect("valid privacy function query");
    let options = QueryOptions {
        access: AccessGrant {
            path: true,
            content: true,
            ..AccessGrant::default()
        },
        redaction_salt: b"synthetic-redaction-salt".to_vec(),
        ..QueryOptions::default()
    };
    let result = prepare_query(&sql, options)
        .await
        .expect("query prepares")
        .execute(vec![FederatedSource { adapter, manifest }])
        .await
        .expect("privacy functions execute");
    let batch = &result.batches[0];
    let value = |column: usize| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("privacy output is text")
            .value(0)
    };
    assert!(value(0).starts_with("hmac:"));
    assert!(!value(0).contains("Synthetic"));
    assert_eq!(value(0), value(1));
    assert_eq!(value(2), "[REDACTED]…itle");
    assert_eq!(value(3), "…/example/project");
    assert!(batch.column(4).is_null(0));
}

#[tokio::test]
async fn usage_view_preserves_null_tokens_and_aggregates_explicit_metrics() {
    let session = synthetic_session("source-usage", "session-usage");
    let session_id = session.session_id.clone();
    let adapter = Arc::new(SyntheticUsageAdapter {
        sessions: vec![session],
        messages: vec![
            synthetic_message(
                "source-usage",
                &session_id,
                "known",
                Some(10),
                Some(20),
                Some(5),
                Some(false),
            ),
            synthetic_message(
                "source-usage",
                &session_id,
                "unknown",
                None,
                None,
                None,
                Some(true),
            ),
        ],
        tool_calls: vec![synthetic_tool_call("source-usage", &session_id)],
    });
    let manifest = SourceManifest {
        source_id: SourceId::new("source-usage"),
        agent_id: "synthetic".to_string(),
        display_name: "Synthetic".to_string(),
        data_root_token: "root:usage".to_string(),
        format_fingerprint: "synthetic-v1".to_string(),
        capabilities: vec![
            "sessions".to_string(),
            "messages".to_string(),
            "tool_calls".to_string(),
        ],
        snapshot: Some(SnapshotToken::new("synthetic-snapshot")),
        warnings: Vec::new(),
    };
    let sql = validate_read_only_sql(
            "SELECT SUM(input_tokens), SUM(total_tokens), SUM(message_count), SUM(tool_call_count), SUM(error_count) FROM usage",
        )
        .expect("valid usage aggregate");
    let result = prepare_query(&sql, QueryOptions::default())
        .await
        .expect("usage query prepares")
        .execute(vec![FederatedSource { adapter, manifest }])
        .await
        .expect("usage aggregate succeeds");
    let batch = &result.batches[0];
    let value = |column: usize| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("aggregate is int64")
            .value(0)
    };
    assert_eq!(value(0), 10);
    assert_eq!(value(1), 35);
    assert_eq!(value(2), 2);
    assert_eq!(value(3), 1);
    assert_eq!(value(4), 2);
}

#[tokio::test]
async fn negative_or_overflowing_message_tokens_fail_closed() {
    for tokens in [(Some(-1), None, None), (Some(i64::MAX), Some(1), None)] {
        let session = synthetic_session("source-invalid", "session-invalid");
        let session_id = session.session_id.clone();
        let adapter = Arc::new(SyntheticUsageAdapter {
            sessions: vec![session],
            messages: vec![synthetic_message(
                "source-invalid",
                &session_id,
                "invalid",
                tokens.0,
                tokens.1,
                tokens.2,
                None,
            )],
            tool_calls: Vec::new(),
        });
        let manifest = SourceManifest {
            source_id: SourceId::new("source-invalid"),
            agent_id: "synthetic".to_string(),
            display_name: "Synthetic".to_string(),
            data_root_token: "root:invalid".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            capabilities: vec!["sessions".to_string(), "messages".to_string()],
            snapshot: None,
            warnings: Vec::new(),
        };
        let sql =
            validate_read_only_sql("SELECT total_tokens FROM usage").expect("valid usage query");
        let result = prepare_query(&sql, QueryOptions::default())
            .await
            .expect("query prepares")
            .execute(vec![FederatedSource { adapter, manifest }])
            .await;
        let Err(error) = result else {
            panic!("invalid token metrics must fail closed");
        };
        assert!(matches!(error, QueryError::Engine(_)));
    }
}

#[tokio::test]
async fn plan_access_does_not_match_aliases_or_substrings() {
    let safe_alias =
        validate_read_only_sql("SELECT session_id AS content, model AS output FROM sessions")
            .expect("valid SQL");
    prepare_query(&safe_alias, QueryOptions::default())
        .await
        .expect("safe aliases are not sensitive columns");

    let output_tokens =
        validate_read_only_sql("SELECT output_tokens FROM messages").expect("valid SQL");
    prepare_query(&output_tokens, QueryOptions::default())
        .await
        .expect("output_tokens is not tool output");
}

#[tokio::test]
async fn plan_access_covers_hidden_and_derived_references() {
    for (sql, grant) in [
        (
            "SELECT session_id FROM sessions WHERE title LIKE '%synthetic%'",
            "content",
        ),
        ("SELECT session_id FROM sessions ORDER BY cwd", "path"),
        (
            "WITH private AS (SELECT content AS x FROM messages) SELECT x FROM private",
            "content",
        ),
        ("SELECT COUNT(arguments) FROM tool_calls", "tool-input"),
        (
            "SELECT session_id FROM tool_calls WHERE output IS NOT NULL",
            "tool-output",
        ),
        (
            "SELECT sessions.session_id FROM sessions JOIN messages ON sessions.session_id = messages.session_id AND messages.content IS NOT NULL",
            "content",
        ),
    ] {
        let validated = validate_read_only_sql(sql).expect("valid SQL");
        let error = prepare_query(&validated, QueryOptions::default())
            .await
            .expect_err("sensitive source lineage must require a grant");
        assert!(matches!(error, QueryError::AccessDenied(actual) if actual == grant));
    }

    for (sql, grant) in [
        ("SELECT REDACT(content) FROM messages", "content"),
        ("SELECT MASK_PATH(cwd, 2) FROM sessions", "path"),
        (
            "SELECT session_id FROM messages WHERE REDACT(content) = '[REDACTED]'",
            "content",
        ),
    ] {
        let validated = validate_read_only_sql(sql).expect("valid privacy function query");
        let error = prepare_query(&validated, QueryOptions::default())
            .await
            .expect_err("privacy functions must preserve input lineage access");
        assert!(matches!(error, QueryError::AccessDenied(actual) if actual == grant));
    }
}

#[tokio::test]
async fn safe_wildcards_exclude_sensitive_columns() {
    let sessions = validate_read_only_sql("SELECT * FROM sessions").expect("valid wildcard");
    let normalized = sessions.normalized_sql();
    assert!(normalized.contains("sessions.session_id"));
    assert!(!normalized.contains("sessions.title"));
    assert!(!normalized.contains("sessions.cwd"));
    prepare_query(&sessions, QueryOptions::default())
        .await
        .expect("safe wildcard must not require grants");

    let qualified =
        validate_read_only_sql("SELECT s.* FROM sessions AS s").expect("valid wildcard");
    assert!(qualified.normalized_sql().contains("s.session_id"));
    assert!(!qualified.normalized_sql().contains("s.title"));

    let cte = validate_read_only_sql(
            "WITH mixed AS (SELECT session_id, title AS private_title FROM sessions) SELECT * FROM mixed",
        )
        .expect("valid CTE wildcard");
    let normalized = cte.normalized_sql();
    assert!(normalized.contains("mixed.session_id"));
    assert!(!normalized.contains("mixed.private_title"));
    prepare_query(&cte, QueryOptions::default())
        .await
        .expect("CTE wildcard must omit sensitive lineage");

    let count = validate_read_only_sql("SELECT COUNT(*) FROM sessions").expect("valid count");
    assert!(count.normalized_sql().contains("COUNT(*)"));
    prepare_query(&count, QueryOptions::default())
        .await
        .expect("COUNT(*) must not read sensitive fields");

    let artifacts =
        validate_read_only_sql("SELECT * FROM artifacts").expect("valid artifacts wildcard");
    let normalized = artifacts.normalized_sql();
    assert!(normalized.contains("artifacts.artifact_id"));
    assert!(!normalized.contains("artifacts.name"));
    assert!(!normalized.contains("artifacts.path"));
    assert!(!normalized.contains("artifacts.content"));
    let error = prepare_query(&artifacts, QueryOptions::default())
        .await
        .expect_err("artifact enumeration requires path access");
    assert!(matches!(error, QueryError::AccessDenied("path")));
    let prepared = prepare_query(
        &artifacts,
        QueryOptions {
            access: AccessGrant {
                path: true,
                ..AccessGrant::default()
            },
            ..QueryOptions::default()
        },
    )
    .await
    .expect("path grant allows artifact metadata wildcard");
    assert!(
        prepared
            .plan_summary()
            .required_access
            .contains(&"path".to_string())
    );

    for (sql, grant, path_granted) in [
        ("SELECT path FROM artifacts", "path", false),
        ("SELECT name FROM artifacts", "content", true),
        ("SELECT content_json FROM artifacts", "content", true),
    ] {
        let sql = validate_read_only_sql(sql).expect("valid artifact query");
        let error = prepare_query(
            &sql,
            QueryOptions {
                access: AccessGrant {
                    path: path_granted,
                    ..AccessGrant::default()
                },
                ..QueryOptions::default()
            },
        )
        .await
        .expect_err("sensitive artifact field requires a grant");
        assert!(matches!(error, QueryError::AccessDenied(actual) if actual == grant));
    }
}

#[test]
fn wildcard_modifiers_are_rejected() {
    assert!(matches!(
        validate_read_only_sql("SELECT * EXCLUDE (title) FROM sessions"),
        Err(QueryError::SqlRejected {
            stage: "wildcard",
            ..
        })
    ));
}

#[test]
fn filter_translation_is_conservative() {
    assert!(matches!(
        expr_to_predicate(&col("native_id").eq(lit("session-minimal"))),
        Some(aql_adapter_api::Predicate::Eq(column, aql_adapter_api::Literal::Text(value)))
            if column.as_str() == "native_id" && value == "session-minimal"
    ));
    assert!(matches!(
        expr_to_predicate(&col("updated_at").gt_eq(lit(100_i64))),
        Some(aql_adapter_api::Predicate::Range {
            lower: Some(aql_adapter_api::Literal::Integer(100)),
            ..
        })
    ));
    assert!(expr_to_predicate(&col("model").like(lit("example%"))).is_none());

    let provider = DeferredTable::new(&QUERY_SCHEMAS[1]);
    let supported = col("native_id").eq(lit("session-minimal"));
    let unsupported = col("model").like(lit("example%"));
    assert_eq!(
        provider
            .supports_filters_pushdown(&[&supported, &unsupported])
            .expect("pushdown declaration must succeed"),
        vec![
            TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Unsupported,
        ]
    );
}

#[test]
fn read_only_firewall_accepts_select_and_cte() {
    validate_read_only_sql("SELECT session_id FROM sessions")
        .expect("canonical SELECT must be accepted");
    validate_read_only_sql(
        "WITH recent AS (SELECT session_id FROM sessions) SELECT session_id FROM recent",
    )
    .expect("CTE over a canonical table must be accepted");
}

#[test]
fn read_only_firewall_rejects_writes_multiple_statements_and_external_tables() {
    for sql in [
        "DELETE FROM sessions",
        "SELECT session_id FROM sessions; DELETE FROM sessions",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM read_csv('fixture.csv')",
        "SELECT * FROM unknown_table",
    ] {
        assert!(
            matches!(
                validate_read_only_sql(sql),
                Err(QueryError::SqlRejected { .. })
            ),
            "query should be rejected"
        );
    }
}

#[test]
fn read_only_firewall_rejects_disallowed_functions_and_complexity() {
    assert!(matches!(
        validate_read_only_sql("SELECT dangerous(session_id) FROM sessions"),
        Err(QueryError::SqlRejected { .. })
    ));
    let oversized = "x".repeat(MAX_SQL_BYTES + 1);
    assert!(matches!(
        validate_read_only_sql(&oversized),
        Err(QueryError::SqlRejected { .. })
    ));
    for sql in [
        "SELECT REDACT(title, 'unknown') FROM sessions",
        "SELECT REDACT(title, model) FROM sessions",
        "SELECT MASK_PATH(cwd, 0) FROM sessions",
        "SELECT MASK_PATH(cwd, 17) FROM sessions",
        "SELECT MASK_PATH(cwd, tokens_used) FROM sessions",
    ] {
        assert!(matches!(
            validate_read_only_sql(sql),
            Err(QueryError::SqlRejected { .. })
        ));
    }
}

#[test]
fn common_string_and_time_functions_are_allowlisted() {
    for sql in [
        "SELECT replace(title, 'old', 'new') FROM sessions",
        "SELECT date_part('year', created_at) FROM sessions",
        "SELECT lower(agent_id), coalesce(model, 'unknown') FROM sessions",
    ] {
        validate_read_only_sql(sql).expect("common analysis function should be accepted");
    }
}

#[test]
fn query_schemas_match_the_public_table_order() {
    assert_eq!(
        QUERY_SCHEMAS
            .iter()
            .map(|schema| schema.name)
            .collect::<Vec<_>>(),
        vec![
            "aql_tables",
            "aql_columns",
            "aql_sources",
            "aql_capabilities",
            "agents",
            "sessions",
            "messages",
            "tool_calls",
            "usage",
            "session_edges",
            "artifacts",
        ]
    );
    let secret_columns = QUERY_SCHEMAS
        .iter()
        .flat_map(|schema| schema.columns)
        .filter(|column| column.access == AccessClass::Secret)
        .count();
    assert_eq!(secret_columns, 0);
    let artifacts = QUERY_SCHEMAS
        .iter()
        .find(|schema| schema.name == "artifacts")
        .expect("artifacts schema exists");
    assert_eq!(
        artifacts
            .columns
            .iter()
            .find(|column| column.name == "path")
            .expect("path column exists")
            .access,
        AccessClass::Path
    );
    for column in ["name", "content", "content_json"] {
        assert_eq!(
            artifacts
                .columns
                .iter()
                .find(|candidate| candidate.name == column)
                .expect("content column exists")
                .access,
            AccessClass::Content
        );
    }
}
