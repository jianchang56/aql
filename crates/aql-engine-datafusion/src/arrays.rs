use super::*;

pub(super) fn agents_array(column: &str, sources: &[SourceManifest]) -> Result<ArrayRef> {
    let values = sources
        .iter()
        .map(|source| match column {
            "source_id" => Ok(source.source_id.to_string()),
            "agent_id" => Ok(source.agent_id.clone()),
            "display_name" => Ok(source.display_name.clone()),
            "format_fingerprint" => Ok(source.format_fingerprint.clone()),
            "snapshot_state" => Ok(if source.snapshot.is_some() {
                "weak"
            } else {
                "unavailable"
            }
            .to_string()),
            "capabilities" => serde_json::to_string(&source.capabilities)
                .map_err(|error| DataFusionError::External(Box::new(error))),
            _ => Err(DataFusionError::Plan("unknown agents column".to_string())),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from(
        values.into_iter().map(Some).collect::<Vec<_>>(),
    )))
}

pub(super) fn record_array(
    table: &QueryTableSchema,
    column: &str,
    records: &[CanonicalRecord],
) -> Result<ArrayRef> {
    let data_type = table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .map(|candidate| candidate.data_type)
        .ok_or_else(|| DataFusionError::Plan("unknown canonical column".to_string()))?;
    match data_type {
        QueryDataType::Text | QueryDataType::Json => Ok(Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| record_text(record, column))
                .collect::<Vec<_>>(),
        ))),
        QueryDataType::Int64 => Ok(Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| record_int(record, column))
                .collect::<Vec<_>>(),
        ))),
        QueryDataType::Bool => Ok(Arc::new(BooleanArray::from(
            records
                .iter()
                .map(|record| record_bool(record, column))
                .collect::<Vec<_>>(),
        ))),
        QueryDataType::Timestamp => Ok(Arc::new(
            TimestampMillisecondArray::from(
                records
                    .iter()
                    .map(|record| record_timestamp(record, column))
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        )),
    }
}

fn record_text(record: &CanonicalRecord, column: &str) -> Option<String> {
    match record {
        CanonicalRecord::Session(value) => match column {
            "session_id" => Some(value.session_id.to_string()),
            "native_id" => Some(value.native_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "agent_id" => Some(value.agent_id.clone()),
            "title" => value.title.clone(),
            "preview" => value.preview.clone(),
            "cwd" => value.cwd.clone(),
            "project" => value.project.clone(),
            "model" => value.model.clone(),
            "provider" => value.provider.clone(),
            "status" => value.status.clone(),
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
            "role" => Some(value.role.clone()),
            "kind" => value.kind.clone(),
            "content" => value.content.clone(),
            "content_json" => value.content_json.as_ref().map(ToString::to_string),
            "model" => value.model.clone(),
            _ => None,
        },
        CanonicalRecord::ToolCall(value) => match column {
            "tool_call_id" => Some(value.tool_call_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "message_id" => value.message_id.as_ref().map(ToString::to_string),
            "source_id" => Some(value.source_id.to_string()),
            "tool_name" => Some(value.tool_name.clone()),
            "namespace" => value.namespace.clone(),
            "arguments" => value.arguments.as_ref().map(ToString::to_string),
            "output" => value.output.clone(),
            "status" => value.status.clone(),
            _ => None,
        },
        CanonicalRecord::Usage(value) => match column {
            "usage_id" => Some(value.usage_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "agent_id" => Some(value.agent_id.clone()),
            "session_id" => value.session_id.as_ref().map(ToString::to_string),
            "model" => value.model.clone(),
            "provider" => value.provider.clone(),
            _ => None,
        },
        CanonicalRecord::SessionEdge(value) => match column {
            "edge_id" => Some(value.edge_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "parent_session_id" => Some(value.parent_session_id.to_string()),
            "child_session_id" => Some(value.child_session_id.to_string()),
            "edge_kind" => Some(value.edge_kind.clone()),
            "native_edge_id" => value.native_edge_id.as_ref().map(ToString::to_string),
            _ => None,
        },
        CanonicalRecord::Artifact(value) => match column {
            "artifact_id" => Some(value.artifact_id.to_string()),
            "source_id" => Some(value.source_id.to_string()),
            "session_id" => Some(value.session_id.to_string()),
            "tool_call_id" => value.tool_call_id.as_ref().map(ToString::to_string),
            "kind" => Some(value.kind.clone()),
            "name" => value.name.clone(),
            "path" => value.path.clone(),
            "media_type" => value.media_type.clone(),
            "content" => value.content.clone(),
            "content_json" => value.content_json.as_ref().map(ToString::to_string),
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
