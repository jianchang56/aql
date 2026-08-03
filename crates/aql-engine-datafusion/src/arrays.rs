use super::*;

/// Resolves projection indices to unique columns in first-use order, plus the
/// unique-column slot for each projection entry. Repeated projections of one
/// column share a single built array.
fn projected_columns<'a>(
    table: &'a QueryTableSchema,
    projection: &[usize],
) -> (Vec<&'a QueryColumn>, Vec<usize>) {
    let mut columns = Vec::<&QueryColumn>::new();
    let mut slots = Vec::with_capacity(projection.len());
    for index in projection {
        let column = &table.columns[*index];
        let slot = match columns
            .iter()
            .position(|candidate| candidate.name == column.name)
        {
            Some(slot) => slot,
            None => {
                columns.push(column);
                columns.len() - 1
            }
        };
        slots.push(slot);
    }
    (columns, slots)
}

/// Builds one Arrow array per projected agents column, consuming the bound
/// sources so manifest payload strings move into the arrays instead of being
/// cloned.
pub(super) fn agents_arrays(
    table: &QueryTableSchema,
    projection: &[usize],
    sources: Vec<FederatedSource>,
) -> Result<Vec<ArrayRef>> {
    let (columns, slots) = projected_columns(table, projection);
    for column in &columns {
        match column.name {
            "source_id" | "agent_id" | "display_name" | "format_fingerprint" | "snapshot_state"
            | "capabilities" => {}
            _ => return Err(DataFusionError::Plan("unknown agents column".to_string())),
        }
    }
    let mut values = columns
        .iter()
        .map(|_| Vec::with_capacity(sources.len()))
        .collect::<Vec<Vec<Option<String>>>>();
    for source in sources {
        let FederatedSource { adapter, manifest } = source;
        // Values that borrow the whole manifest are produced before any field
        // is moved out of it.
        for (slot, column) in columns.iter().enumerate() {
            match column.name {
                "source_id" => values[slot].push(Some(manifest.source_id.to_string())),
                "snapshot_state" => values[slot].push(Some(
                    declared_snapshot_state(adapter.capabilities(&manifest).snapshot_strength)
                        .to_string(),
                )),
                "capabilities" => values[slot].push(Some(
                    serde_json::to_string(&manifest.capabilities)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?,
                )),
                _ => {}
            }
        }
        let SourceManifest {
            mut agent_id,
            mut display_name,
            mut format_fingerprint,
            ..
        } = manifest;
        for (slot, column) in columns.iter().enumerate() {
            match column.name {
                "agent_id" => values[slot].push(Some(std::mem::take(&mut agent_id))),
                "display_name" => values[slot].push(Some(std::mem::take(&mut display_name))),
                "format_fingerprint" => {
                    values[slot].push(Some(std::mem::take(&mut format_fingerprint)));
                }
                _ => {}
            }
        }
    }
    let arrays = values
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
        .collect::<Vec<_>>();
    Ok(slots.into_iter().map(|slot| arrays[slot].clone()).collect())
}

/// Builds one Arrow array per projected column, consuming the records so
/// payload strings move into the arrays instead of being cloned. Records must
/// already be validated against the scanned table; nothing is checked here.
pub(super) fn record_arrays(
    table: &QueryTableSchema,
    projection: &[usize],
    records: Vec<CanonicalRecord>,
) -> Vec<ArrayRef> {
    let (columns, slots) = projected_columns(table, projection);
    let mut records = records;
    let arrays = columns
        .iter()
        .map(|column| -> ArrayRef {
            match column.data_type {
                QueryDataType::Text | QueryDataType::Json => Arc::new(StringArray::from(
                    records
                        .iter_mut()
                        .map(|record| take_record_text(record, column.name))
                        .collect::<Vec<_>>(),
                )),
                QueryDataType::Int64 => Arc::new(Int64Array::from(
                    records
                        .iter()
                        .map(|record| record_int(record, column.name))
                        .collect::<Vec<_>>(),
                )),
                QueryDataType::Bool => Arc::new(BooleanArray::from(
                    records
                        .iter()
                        .map(|record| record_bool(record, column.name))
                        .collect::<Vec<_>>(),
                )),
                QueryDataType::Timestamp => Arc::new(
                    TimestampMillisecondArray::from(
                        records
                            .iter()
                            .map(|record| record_timestamp(record, column.name))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
            }
        })
        .collect::<Vec<_>>();
    slots.into_iter().map(|slot| arrays[slot].clone()).collect()
}

fn take_record_text(record: &mut CanonicalRecord, column: &str) -> Option<String> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "session_id" => Some(value.session_id.to_string()),
            "native_id" => Some(value.native_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "agent_id" => Some(std::mem::take(&mut value.agent_id)),
            "title" => value.title.take(),
            "preview" => value.preview.take(),
            "cwd" => value.cwd.take(),
            "project" => value.project.take(),
            "model" => value.model.take(),
            "provider" => value.provider.take(),
            "status" => value.status.take(),
            "identity_confidence" => {
                Some(format!("{:?}", value.identity_confidence).to_ascii_lowercase())
            }
            "snapshot_state" => Some(format!("{:?}", value.snapshot_state).to_ascii_lowercase()),
            _ => None,
        },
        CanonicalRecord::Message(value) => match column {
            "message_id" => Some(value.message_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "role" => Some(std::mem::take(&mut value.role)),
            "kind" => value.kind.take(),
            "content" => value.content.take(),
            "content_json" => value.content_json.take().map(|json| json.to_string()),
            "model" => value.model.take(),
            _ => None,
        },
        CanonicalRecord::ToolCall(value) => match column {
            "tool_call_id" => Some(value.tool_call_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "message_id" => value.message_id.as_ref().map(ToString::to_string),
            "source_id" => Some(value.source_id.to_string()),
            "tool_name" => Some(std::mem::take(&mut value.tool_name)),
            "namespace" => value.namespace.take(),
            "arguments" => value.arguments.take().map(|json| json.to_string()),
            "output" => value.output.take(),
            "status" => value.status.take(),
            _ => None,
        },
        CanonicalRecord::Usage(value) => match column {
            "usage_id" => Some(value.usage_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "agent_id" => Some(std::mem::take(&mut value.agent_id)),
            "session_id" => value.session_id.as_ref().map(ToString::to_string),
            "model" => value.model.take(),
            "provider" => value.provider.take(),
            _ => None,
        },
        CanonicalRecord::SessionEdge(value) => match column {
            "edge_id" => Some(value.edge_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "parent_session_id" => Some(value.parent_session_id.to_string()),
            "child_session_id" => Some(value.child_session_id.to_string()),
            "edge_kind" => Some(std::mem::take(&mut value.edge_kind)),
            "native_edge_id" => value.native_edge_id.as_ref().map(ToString::to_string),
            _ => None,
        },
        CanonicalRecord::Artifact(value) => match column {
            "artifact_id" => Some(value.artifact_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "tool_call_id" => value.tool_call_id.as_ref().map(ToString::to_string),
            "kind" => Some(std::mem::take(&mut value.kind)),
            "name" => value.name.take(),
            "path" => value.path.take(),
            "media_type" => value.media_type.take(),
            "content" => value.content.take(),
            "content_json" => value.content_json.take().map(|json| json.to_string()),
            _ => None,
        },
    }
}

fn record_int(record: &CanonicalRecord, column: &str) -> Option<i64> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "message_count" => value.message_count,
            "tool_call_count" => value.tool_call_count,
            "tokens_used" => value.tokens_used,
            _ => None,
        },
        CanonicalRecord::Message(value) => match column {
            "sequence" => Some(value.sequence),
            "input_tokens" => value.input_tokens,
            "output_tokens" => value.output_tokens,
            "cached_tokens" => value.cached_tokens,
            _ => None,
        },
        CanonicalRecord::ToolCall(value) => match column {
            "sequence" => Some(value.sequence),
            "duration_ms" => value.duration_ms,
            "exit_code" => value.exit_code,
            _ => None,
        },
        CanonicalRecord::Usage(value) => match column {
            "input_tokens" => value.input_tokens,
            "output_tokens" => value.output_tokens,
            "cached_tokens" => value.cached_tokens,
            "total_tokens" => value.total_tokens,
            "message_count" => Some(value.message_count),
            "tool_call_count" => Some(value.tool_call_count),
            "error_count" => Some(value.error_count),
            _ => None,
        },
        CanonicalRecord::Artifact(value) if column == "size_bytes" => value.size_bytes,
        CanonicalRecord::SessionEdge(_) | CanonicalRecord::Artifact(_) => None,
    }
}

fn record_bool(record: &CanonicalRecord, column: &str) -> Option<bool> {
    match record {
        CanonicalRecord::Session(value) if column == "archived" => value.archived,
        CanonicalRecord::Message(value) if column == "is_error" => value.is_error,
        _ => None,
    }
}

fn record_timestamp(record: &CanonicalRecord, column: &str) -> Option<i64> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "created_at" => value.created_at.map(|time| time.timestamp_millis()),
            "updated_at" => value.updated_at.map(|time| time.timestamp_millis()),
            _ => None,
        },
        CanonicalRecord::Message(value) if column == "created_at" => {
            value.created_at.map(|time| time.timestamp_millis())
        }
        CanonicalRecord::ToolCall(value) => match column {
            "started_at" => value.started_at.map(|time| time.timestamp_millis()),
            "ended_at" => value.ended_at.map(|time| time.timestamp_millis()),
            _ => None,
        },
        CanonicalRecord::Usage(value) if column == "bucket_start" => {
            value.bucket_start.map(|time| time.timestamp_millis())
        }
        CanonicalRecord::SessionEdge(value) if column == "created_at" => {
            value.created_at.map(|time| time.timestamp_millis())
        }
        CanonicalRecord::Artifact(value) if column == "created_at" => {
            value.created_at.map(|time| time.timestamp_millis())
        }
        _ => None,
    }
}
