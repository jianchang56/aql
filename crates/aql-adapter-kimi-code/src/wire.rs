use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aql_adapter_api::{
    AdapterError, AdapterWarning, AdapterWarningKind, ColumnName, PushdownReport, PushdownState,
    ScanDiagnostics, ScanRequest, ScanResult, SnapshotReport, SnapshotStrength, TableName,
    check_scan_state,
    util::{normalize_limit, projected, read_limited_line},
};
use aql_model::{
    CanonicalRecord, EntityId, MessageRecord, NativeId, SessionEdgeRecord, ToolCallRecord,
    UsageRecord,
};
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;

use super::cache::{
    CacheLookup, FileCacheKey, ParseCacheHandle, ParsedWireFile, SensitiveClasses, required_classes,
};
use super::{
    FileIdentity, KimiCodeAdapter, LocatedSession, RawLegacyCustom, RootBinding, SafeAgent,
    open_regular_no_follow, validate_agents, validate_directory, validate_session_chain,
    validate_workdir_bucket,
};

const MAX_WIRE_RECORD_BYTES: usize = 1024 * 1024;

/// Cancellation/deadline polling cadence while replaying a cached parse.
const REPLAY_CHECK_RECORDS: u64 = 1024;

pub(super) fn scan(
    root: RootBinding,
    mut request: ScanRequest,
    cache: ParseCacheHandle,
) -> Result<ScanResult, AdapterError> {
    let diagnostics = ScanDiagnostics::default();
    let predicate_count = request.predicates.len();
    let ordering_count = request.order_hint.len();
    let limit_state = normalize_limit(&mut request);
    Ok(ScanResult {
        records: Box::new(WireStream {
            root,
            request,
            diagnostics: diagnostics.clone(),
            cache,
            after_session: None,
            listing: None,
            agents: VecDeque::new(),
            current: None,
            emitted: 0,
            finished: false,
        }),
        pushdown: PushdownReport {
            predicates: vec![PushdownState::Unsupported; predicate_count],
            limit: limit_state,
            ordering: vec![PushdownState::Unsupported; ordering_count],
        },
        diagnostics,
        snapshot: SnapshotReport {
            token: None,
            strength: SnapshotStrength::Weak,
            stale: false,
        },
    })
}

pub(super) fn scan_edges(
    root: RootBinding,
    mut request: ScanRequest,
) -> Result<ScanResult, AdapterError> {
    let diagnostics = ScanDiagnostics::default();
    let predicate_count = request.predicates.len();
    let ordering_count = request.order_hint.len();
    let limit_state = normalize_limit(&mut request);
    Ok(ScanResult {
        records: Box::new(EdgeStream {
            root,
            request,
            after_session: None,
            listing: None,
            ready: VecDeque::new(),
            emitted: 0,
            finished: false,
        }),
        pushdown: PushdownReport {
            predicates: vec![PushdownState::Unsupported; predicate_count],
            limit: limit_state,
            ordering: vec![PushdownState::Unsupported; ordering_count],
        },
        diagnostics,
        snapshot: SnapshotReport {
            token: None,
            strength: SnapshotStrength::Weak,
            stale: false,
        },
    })
}

struct EdgeStream {
    root: RootBinding,
    request: ScanRequest,
    after_session: Option<(String, String)>,
    listing: Option<Vec<LocatedSession>>,
    ready: VecDeque<SessionEdgeRecord>,
    emitted: u64,
    finished: bool,
}

/// Streams one canonical wire table, parsing each wire file once per adapter
/// (query) lifetime and replaying cached parses afterwards.
struct WireStream {
    root: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    cache: ParseCacheHandle,
    after_session: Option<(String, String)>,
    listing: Option<Vec<LocatedSession>>,
    agents: VecDeque<(PathBuf, String, EntityId)>,
    current: Option<ReplayFile>,
    emitted: u64,
    finished: bool,
}

/// Projection-masked records of one wire file being replayed, plus the
/// end-of-file identity re-check owed when the parse came from the cache.
struct ReplayFile {
    path: PathBuf,
    identity: FileIdentity,
    len: u64,
    records: VecDeque<CanonicalRecord>,
    recheck: bool,
    since_check: u64,
}

struct PendingTool {
    native_id: String,
    session_id: EntityId,
    sequence: i64,
    name: String,
    arguments: Option<serde_json::Value>,
    started_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AgentState {
    created_at: DateTime<Utc>,
    agents: BTreeMap<String, SafeAgent>,
}

/// Borrowed workDir view for the bucket check in [`read_agent_state`]; the
/// legacy `custom.cwd` fallback keeps its raw form until the bucket hash needs
/// the exact encoded bytes.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawWorkdir<'a> {
    #[serde(borrow)]
    work_dir: Option<&'a RawValue>,
    #[serde(borrow)]
    custom: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    time: Option<i64>,
    protocol_version: Option<&'a str>,
    #[serde(borrow)]
    message: Option<&'a RawValue>,
    #[serde(borrow)]
    event: Option<&'a RawValue>,
    model: Option<&'a str>,
    #[serde(borrow)]
    usage: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageMeta {
    role: String,
    is_error: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageContent {
    content: Vec<ContentPart>,
}

#[derive(Deserialize)]
struct MessageContentJson {
    content: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ContentPart {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "think")]
    Think { think: String },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Usage {
    input_other: i64,
    output: i64,
    input_cache_read: i64,
    input_cache_creation: i64,
}

#[derive(Deserialize)]
struct EventEnvelope<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(borrow)]
    args: Option<&'a RawValue>,
    #[serde(rename = "toolCallId")]
    tool_call_id: Option<&'a str>,
    name: Option<&'a str>,
    #[serde(borrow)]
    result: Option<&'a RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolResult<'a> {
    #[serde(borrow)]
    output: &'a RawValue,
    is_error: Option<bool>,
}

impl WireStream {
    /// Advances to the next agent wire file, serving its parse from the
    /// per-query cache when identity, pinned length, grant, and needed
    /// classes all match, and parsing it once otherwise.
    fn load_next_file(&mut self) -> Result<Option<ReplayFile>, AdapterError> {
        loop {
            if let Some((path, agent_id, logical_session)) = self.agents.pop_front() {
                validate_wire_chain(&self.root, &path)?;
                let (file, metadata, identity) = match open_regular_no_follow(&path, "kimi_wire") {
                    Ok(value) => value,
                    Err(AdapterError::NotFound { .. }) => {
                        self.diagnostics.push(AdapterWarning {
                            kind: AdapterWarningKind::IncompleteCapability,
                            source_kind: "kimi_wire".to_string(),
                            stage: "missing_agent_wire".to_string(),
                        })?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let len = metadata.len();
                if len > self.request.budget.max_bytes_read {
                    return Err(AdapterError::BudgetExceeded {
                        resource: "bytes_read".to_string(),
                        actual: len,
                    });
                }
                let needed = required_classes(self.request.table, &self.request.projection);
                let key = FileCacheKey::new(
                    &self.request.source.source_id,
                    identity,
                    len,
                    self.request.access,
                );
                let lookup = self
                    .cache
                    .lock()
                    .map_err(|_| AdapterError::Internal {
                        stage: "kimi_parse_cache".to_string(),
                    })?
                    .lookup(&key, needed);
                let (parsed, from_cache) = match lookup {
                    CacheLookup::Hit(parsed) => (parsed, true),
                    CacheLookup::Miss(widened) => {
                        let extract = needed.union(widened).granted(self.request.access);
                        let (parsed, complete) = parse_file(
                            &self.root,
                            file,
                            &path,
                            identity,
                            len,
                            &agent_id,
                            &logical_session,
                            &self.request,
                            extract,
                            &self.diagnostics,
                        )?;
                        let parsed = Arc::new(parsed);
                        if complete {
                            self.cache
                                .lock()
                                .map_err(|_| AdapterError::Internal {
                                    stage: "kimi_parse_cache".to_string(),
                                })?
                                .insert(key, Arc::clone(&parsed));
                        }
                        (parsed, false)
                    }
                };
                return Ok(Some(ReplayFile {
                    path,
                    identity,
                    len,
                    records: replay_records(&parsed, self.request.table, &self.request.projection),
                    recheck: from_cache,
                    since_check: 0,
                }));
            }
            let listing = match self.listing.as_ref() {
                Some(listing) => listing,
                None => self.listing.insert(KimiCodeAdapter::list_session_dirs(
                    &self.root,
                    &self.request,
                )?),
            };
            let Some((session_dir, key)) =
                KimiCodeAdapter::next_session_dir(listing, self.after_session.as_ref())
            else {
                return Ok(None);
            };
            self.after_session = Some(key);
            let native = session_dir
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "kimi_session_id".to_string(),
                })?;
            let main_session = EntityId::from_parts(
                "kimi-code",
                &self.request.source.source_id,
                &NativeId::new(native),
            );
            let state = read_agent_state(&self.root, &session_dir, &self.request)?;
            let mut agent_ids = state.agents.into_keys().collect::<Vec<_>>();
            agent_ids.sort_by(|left, right| {
                (left != "main", left.as_str()).cmp(&(right != "main", right.as_str()))
            });
            self.agents = agent_ids
                .into_iter()
                .map(|agent| {
                    let logical_session = if agent == "main" {
                        main_session.clone()
                    } else {
                        EntityId::from_parts(
                            "kimi-code",
                            &self.request.source.source_id,
                            &NativeId::new(format!("{native}/agent/{agent}")),
                        )
                    };
                    (
                        session_dir.join("agents").join(&agent).join("wire.jsonl"),
                        agent,
                        logical_session,
                    )
                })
                .collect();
        }
    }
}

/// Parses one wire file once, fanning every envelope out to the messages,
/// tool_calls, and usage builders.
///
/// Sensitive values are extracted only for classes in `extract` (already
/// narrowed to the request grant), preserving the pre-cache per-column
/// gating, including its distinct failure stages. A scan whose effective
/// limit is reached mid-file stops early and reports `complete = false` so
/// the partial parse is never cached.
#[allow(clippy::too_many_arguments)]
fn parse_file(
    root: &RootBinding,
    file: File,
    path: &Path,
    identity: FileIdentity,
    len: u64,
    agent_id: &str,
    session_id: &EntityId,
    request: &ScanRequest,
    extract: SensitiveClasses,
    diagnostics: &ScanDiagnostics,
) -> Result<(ParsedWireFile, bool), AdapterError> {
    let mut reader = BufReader::new(file.take(len));
    let mut line = Vec::new();
    let mut builders = WireBuilders::default();
    let mut metadata_seen = false;
    let mut complete = true;
    while let Some((terminated, bytes)) = read_limited_line(
        &mut reader,
        &mut line,
        MAX_WIRE_RECORD_BYTES,
        "kimi_wire_record_bytes",
        "kimi_wire_read",
    )? {
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
        request.budget.charge_bytes_read(bytes as u64)?;
        let Some(envelope) = parse_envelope(&line, terminated, diagnostics)? else {
            break;
        };
        if !metadata_seen {
            if envelope.kind != "metadata"
                || !matches!(envelope.protocol_version, Some("1.4" | "1.0"))
            {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "kimi_wire_protocol".to_string(),
                });
            }
            metadata_seen = true;
            continue;
        }
        builders.feed(
            &envelope,
            agent_id,
            session_id,
            request,
            extract,
            diagnostics,
        )?;
        let produced = match request.table {
            TableName::Messages => Some(builders.messages.len()),
            TableName::ToolCalls => Some(builders.tool_calls.len()),
            TableName::Usage => Some(builders.usage.len()),
            _ => None,
        };
        if let (Some(produced), Some(limit)) = (produced, request.limit)
            && produced as u64 >= limit
        {
            complete = false;
            break;
        }
    }
    if complete {
        // The parse holds no per-table lazy reader anymore, so the pinned
        // watermark is revalidated here exactly once: handle length plus the
        // path identity/chain check the pre-cache stream ran at EOF.
        let file_metadata = reader
            .get_ref()
            .get_ref()
            .metadata()
            .map_err(|_| AdapterError::SnapshotUnavailable)?;
        if file_metadata.len() < len {
            return Err(AdapterError::SnapshotUnavailable);
        }
        revalidate_wire_path(root, path, identity, len)?;
        if !metadata_seen {
            return Err(AdapterError::UnsupportedFormat {
                stage: "kimi_wire_metadata".to_string(),
            });
        }
        for (_, pending) in std::mem::take(&mut builders.pending_tools) {
            builders.tool_calls.push(tool_record(
                request,
                pending,
                None,
                Some("interrupted"),
                None,
            ));
        }
    }
    Ok((builders.finish(extract), complete))
}

/// Per-file parse state shared by the three table builders.
#[derive(Default)]
struct WireBuilders {
    message_sequence: i64,
    usage_sequence: i64,
    tool_sequence: i64,
    pending_tools: BTreeMap<String, PendingTool>,
    messages: Vec<MessageRecord>,
    tool_calls: Vec<ToolCallRecord>,
    usage: Vec<UsageRecord>,
}

impl WireBuilders {
    /// Feeds one envelope to every table builder; the per-kind sequence
    /// counters keep native IDs identical to the pre-cache per-table scans.
    fn feed(
        &mut self,
        envelope: &Envelope<'_>,
        agent_id: &str,
        session_id: &EntityId,
        request: &ScanRequest,
        extract: SensitiveClasses,
        diagnostics: &ScanDiagnostics,
    ) -> Result<(), AdapterError> {
        match envelope.kind {
            "context.append_message" => {
                self.feed_message(envelope, agent_id, session_id, request, extract)
            }
            "usage.record" => self.feed_usage(envelope, session_id, request),
            "context.append_loop_event" => {
                self.feed_tool_event(envelope, session_id, request, extract, diagnostics)
            }
            "turn.prompt" | "turn.steer" => Ok(()),
            _ => diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::UnknownEvent,
                source_kind: "kimi_wire".to_string(),
                stage: "unknown_record".to_string(),
            }),
        }
    }

    fn feed_message(
        &mut self,
        envelope: &Envelope<'_>,
        agent_id: &str,
        session_id: &EntityId,
        request: &ScanRequest,
        extract: SensitiveClasses,
    ) -> Result<(), AdapterError> {
        let raw = envelope
            .message
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "kimi_message".to_string(),
            })?;
        let meta: MessageMeta =
            serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
                stage: "kimi_message".to_string(),
            })?;
        if !matches!(meta.role.as_str(), "system" | "user" | "assistant" | "tool") {
            return Err(AdapterError::CorruptSource {
                stage: "kimi_message_role".to_string(),
            });
        }
        self.message_sequence += 1;
        let wants_content = extract.includes(SensitiveClasses::CONTENT);
        let wants_content_json = extract.includes(SensitiveClasses::CONTENT_JSON);
        if (wants_content || wants_content_json)
            && raw.get().len() as u64 > request.budget.max_single_value_bytes
        {
            return Err(AdapterError::BudgetExceeded {
                resource: "single_value_bytes".to_string(),
                actual: raw.get().len() as u64,
            });
        }
        let content = if wants_content {
            let content: MessageContent =
                serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
                    stage: "kimi_message_content".to_string(),
                })?;
            Some(
                content
                    .content
                    .into_iter()
                    .filter_map(|part| match part {
                        ContentPart::Text { text } => Some(text),
                        ContentPart::Think { think } => Some(think),
                        ContentPart::Other => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
            )
        } else {
            None
        };
        let content_json = if wants_content_json {
            Some(
                serde_json::from_str::<MessageContentJson>(raw.get())
                    .map_err(|_| AdapterError::CorruptSource {
                        stage: "kimi_message_content".to_string(),
                    })?
                    .content,
            )
        } else {
            None
        };
        let native = NativeId::new(format!(
            "{}/{agent_id}/message/{}",
            session_id, self.message_sequence
        ));
        self.messages.push(MessageRecord {
            message_id: EntityId::from_parts("kimi-code", &request.source.source_id, &native),
            session_id: session_id.clone(),
            source_id: request.source.source_id.clone(),
            sequence: self.message_sequence,
            role: meta.role,
            kind: Some("message".to_string()),
            content,
            content_json,
            model: None,
            created_at: timestamp(envelope.time)?,
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            is_error: meta.is_error,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        });
        Ok(())
    }

    fn feed_usage(
        &mut self,
        envelope: &Envelope<'_>,
        session_id: &EntityId,
        request: &ScanRequest,
    ) -> Result<(), AdapterError> {
        let usage: Usage = serde_json::from_str(
            envelope
                .usage
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "kimi_usage".to_string(),
                })?
                .get(),
        )
        .map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_usage".to_string(),
        })?;
        if [
            usage.input_other,
            usage.output,
            usage.input_cache_read,
            usage.input_cache_creation,
        ]
        .iter()
        .any(|value| *value < 0)
        {
            return Err(AdapterError::CorruptSource {
                stage: "kimi_usage_negative".to_string(),
            });
        }
        self.usage_sequence += 1;
        let input = usage
            .input_other
            .checked_add(usage.input_cache_read)
            .and_then(|value| value.checked_add(usage.input_cache_creation))
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "kimi_usage_overflow".to_string(),
            })?;
        let total = input
            .checked_add(usage.output)
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "kimi_usage_overflow".to_string(),
            })?;
        let native = NativeId::new(format!("{session_id}/usage/{}", self.usage_sequence));
        self.usage.push(UsageRecord {
            usage_id: EntityId::from_parts("kimi-code", &request.source.source_id, &native),
            source_id: request.source.source_id.clone(),
            agent_id: "kimi-code".to_string(),
            session_id: Some(session_id.clone()),
            model: envelope.model.map(str::to_string),
            provider: None,
            bucket_start: timestamp(envelope.time)?,
            input_tokens: Some(input),
            output_tokens: Some(usage.output),
            cached_tokens: Some(usage.input_cache_read),
            total_tokens: Some(total),
            message_count: 0,
            tool_call_count: 0,
            error_count: 0,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        });
        Ok(())
    }

    fn feed_tool_event(
        &mut self,
        envelope: &Envelope<'_>,
        session_id: &EntityId,
        request: &ScanRequest,
        extract: SensitiveClasses,
        diagnostics: &ScanDiagnostics,
    ) -> Result<(), AdapterError> {
        let event: EventEnvelope<'_> = serde_json::from_str(
            envelope
                .event
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "kimi_tool_event".to_string(),
                })?
                .get(),
        )
        .map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_tool_event".to_string(),
        })?;
        match event.kind {
            "tool.call" => {
                self.tool_sequence += 1;
                let id = event
                    .tool_call_id
                    .ok_or_else(|| AdapterError::CorruptSource {
                        stage: "kimi_tool_call_id".to_string(),
                    })?;
                let arguments = if extract.includes(SensitiveClasses::TOOL_INPUT) {
                    let raw = event.args.ok_or_else(|| AdapterError::CorruptSource {
                        stage: "kimi_tool_args".to_string(),
                    })?;
                    if raw.get().len() as u64 > request.budget.max_single_value_bytes {
                        return Err(AdapterError::BudgetExceeded {
                            resource: "single_value_bytes".to_string(),
                            actual: raw.get().len() as u64,
                        });
                    }
                    Some(serde_json::from_str(raw.get()).map_err(|_| {
                        AdapterError::CorruptSource {
                            stage: "kimi_tool_args".to_string(),
                        }
                    })?)
                } else {
                    None
                };
                self.pending_tools.insert(
                    format!("{session_id}:{id}"),
                    PendingTool {
                        native_id: id.to_string(),
                        session_id: session_id.clone(),
                        sequence: self.tool_sequence,
                        name: event
                            .name
                            .ok_or_else(|| AdapterError::CorruptSource {
                                stage: "kimi_tool_name".to_string(),
                            })?
                            .to_string(),
                        arguments,
                        started_at: timestamp(envelope.time)?,
                    },
                );
            }
            "tool.result" => {
                let id = event
                    .tool_call_id
                    .ok_or_else(|| AdapterError::CorruptSource {
                        stage: "kimi_tool_result_id".to_string(),
                    })?;
                let Some(call) = self.pending_tools.remove(&format!("{session_id}:{id}")) else {
                    diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::IncompleteCapability,
                        source_kind: "kimi_wire".to_string(),
                        stage: "unpaired_tool_result".to_string(),
                    })?;
                    return Ok(());
                };
                let result: ToolResult<'_> = serde_json::from_str(
                    event
                        .result
                        .ok_or_else(|| AdapterError::CorruptSource {
                            stage: "kimi_tool_result".to_string(),
                        })?
                        .get(),
                )
                .map_err(|_| AdapterError::CorruptSource {
                    stage: "kimi_tool_result".to_string(),
                })?;
                let output = if extract.includes(SensitiveClasses::TOOL_OUTPUT) {
                    if result.output.get().len() as u64 > request.budget.max_single_value_bytes {
                        return Err(AdapterError::BudgetExceeded {
                            resource: "single_value_bytes".to_string(),
                            actual: result.output.get().len() as u64,
                        });
                    }
                    Some(
                        match serde_json::from_str::<serde_json::Value>(result.output.get())
                            .map_err(|_| AdapterError::CorruptSource {
                                stage: "kimi_tool_output".to_string(),
                            })? {
                            serde_json::Value::String(value) => value,
                            other => other.to_string(),
                        },
                    )
                } else {
                    None
                };
                self.tool_calls.push(tool_record(
                    request,
                    call,
                    output,
                    Some(if result.is_error == Some(true) {
                        "error"
                    } else {
                        "completed"
                    }),
                    timestamp(envelope.time)?,
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(self, extract: SensitiveClasses) -> ParsedWireFile {
        ParsedWireFile {
            messages: self.messages,
            tool_calls: self.tool_calls,
            usage: self.usage,
            extracted: extract,
        }
    }
}

/// Clones the cached records of `table`, masking sensitive fields down to the
/// requesting projection (the cache stores class-wide extractions).
fn replay_records(
    parsed: &ParsedWireFile,
    table: TableName,
    projection: &[ColumnName],
) -> VecDeque<CanonicalRecord> {
    match table {
        TableName::Messages => {
            let wants_content = projected(projection, "content");
            let wants_json = projected(projection, "content_json");
            parsed
                .messages
                .iter()
                .map(|record| {
                    let mut record = record.clone();
                    if !wants_content {
                        record.content = None;
                    }
                    if !wants_json {
                        record.content_json = None;
                    }
                    CanonicalRecord::Message(record)
                })
                .collect()
        }
        TableName::ToolCalls => {
            let wants_arguments = projected(projection, "arguments");
            let wants_output = projected(projection, "output");
            parsed
                .tool_calls
                .iter()
                .map(|record| {
                    let mut record = record.clone();
                    if !wants_arguments {
                        record.arguments = None;
                    }
                    if !wants_output {
                        record.output = None;
                    }
                    CanonicalRecord::ToolCall(record)
                })
                .collect()
        }
        TableName::Usage => parsed
            .usage
            .iter()
            .cloned()
            .map(CanonicalRecord::Usage)
            .collect(),
        _ => VecDeque::new(),
    }
}

/// Re-checks one wire file by path at the end of a cache replay: the same
/// identity/length watermark the parse path enforces at EOF, aligned with the
/// §9 scan-end check for parses that were not re-read this scan.
fn revalidate_wire_path(
    root: &RootBinding,
    path: &Path,
    identity: FileIdentity,
    len: u64,
) -> Result<(), AdapterError> {
    validate_wire_chain(root, path)?;
    let metadata = fs::symlink_metadata(path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() < len {
        return Err(AdapterError::SnapshotUnavailable);
    }
    if aql_fs::file_identity(path).map_err(|_| AdapterError::SnapshotUnavailable)? != identity {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

fn parse_envelope<'a>(
    line: &'a [u8],
    complete: bool,
    diagnostics: &ScanDiagnostics,
) -> Result<Option<Envelope<'a>>, AdapterError> {
    match serde_json::from_slice(line) {
        Ok(value) => Ok(Some(value)),
        Err(_) if !complete => {
            diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::TruncatedRecord,
                source_kind: "kimi_wire".to_string(),
                stage: "truncated_tail".to_string(),
            })?;
            Ok(None)
        }
        Err(_) => Err(AdapterError::CorruptSource {
            stage: "kimi_wire_record".to_string(),
        }),
    }
}

impl Iterator for WireStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .request
                .limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            return None;
        }
        loop {
            if let Some(current) = self.current.as_mut() {
                if current.since_check >= REPLAY_CHECK_RECORDS {
                    if let Err(error) = check_scan_state(
                        &self.request.cancellation,
                        &self.request.budget,
                        self.emitted,
                        self.request.budget.bytes_read_used(),
                    ) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    current.since_check = 0;
                }
                if let Some(record) = current.records.pop_front() {
                    if let Err(error) = self.request.budget.charge_records(1) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    current.since_check += 1;
                    self.emitted += 1;
                    return Some(Ok(record));
                }
            } else {
                if let Err(error) = check_scan_state(
                    &self.request.cancellation,
                    &self.request.budget,
                    self.emitted,
                    self.request.budget.bytes_read_used(),
                ) {
                    self.finished = true;
                    return Some(Err(error));
                }
                match self.load_next_file() {
                    Ok(Some(file)) => {
                        self.current = Some(file);
                        continue;
                    }
                    Ok(None) => {
                        self.finished = true;
                        return None;
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
            }
            // The current file's records are drained; a cache-sourced parse
            // still owes the §9 end-of-scan identity re-check.
            let current = self.current.take()?;
            if current.recheck
                && let Err(error) =
                    revalidate_wire_path(&self.root, &current.path, current.identity, current.len)
            {
                self.finished = true;
                return Some(Err(error));
            }
        }
    }
}

impl Iterator for EdgeStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .request
                .limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            return None;
        }
        if let Err(error) = check_scan_state(
            &self.request.cancellation,
            &self.request.budget,
            self.emitted,
            self.request.budget.bytes_read_used(),
        ) {
            self.finished = true;
            return Some(Err(error));
        }
        if let Some(edge) = self.ready.pop_front() {
            if let Err(error) = self.request.budget.charge_records(1) {
                self.finished = true;
                return Some(Err(error));
            }
            self.emitted += 1;
            return Some(Ok(CanonicalRecord::SessionEdge(edge)));
        }
        loop {
            let listing = match self.listing.as_ref() {
                Some(listing) => listing,
                None => match KimiCodeAdapter::list_session_dirs(&self.root, &self.request) {
                    Ok(listing) => self.listing.insert(listing),
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                },
            };
            let next = match KimiCodeAdapter::next_session_dir(listing, self.after_session.as_ref())
            {
                Some(next) => next,
                None => {
                    self.finished = true;
                    return None;
                }
            };
            self.after_session = Some(next.1);
            let native = match next.0.file_name().and_then(|name| name.to_str()) {
                Some(native) => native.to_string(),
                None => {
                    self.finished = true;
                    return Some(Err(AdapterError::CorruptSource {
                        stage: "kimi_session_id".to_string(),
                    }));
                }
            };
            let state = match read_agent_state(&self.root, &next.0, &self.request) {
                Ok(state) => state,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            for (child, agent) in state.agents {
                if child == "main" {
                    continue;
                }
                let Some(parent) = agent.parent_agent_id else {
                    self.finished = true;
                    return Some(Err(AdapterError::CorruptSource {
                        stage: "kimi_agent_parent".to_string(),
                    }));
                };
                let parent_id = logical_session_id(&self.request, &native, &parent);
                let child_id = logical_session_id(&self.request, &native, &child);
                let edge_native = NativeId::new(format!("{native}/edge/{parent}/{child}"));
                self.ready.push_back(SessionEdgeRecord {
                    edge_id: EntityId::from_parts(
                        "kimi-code",
                        &self.request.source.source_id,
                        &edge_native,
                    ),
                    source_id: self.request.source.source_id.clone(),
                    parent_session_id: parent_id,
                    child_session_id: child_id,
                    edge_kind: "subagent".to_string(),
                    created_at: Some(state.created_at),
                    native_edge_id: Some(edge_native),
                    provenance: BTreeMap::new(),
                    extensions: BTreeMap::new(),
                });
            }
            if let Some(edge) = self.ready.pop_front() {
                if let Err(error) = self.request.budget.charge_records(1) {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.emitted += 1;
                return Some(Ok(CanonicalRecord::SessionEdge(edge)));
            }
        }
    }
}

fn logical_session_id(request: &ScanRequest, native_session: &str, agent: &str) -> EntityId {
    let native = if agent == "main" {
        NativeId::new(native_session)
    } else {
        NativeId::new(format!("{native_session}/agent/{agent}"))
    };
    EntityId::from_parts("kimi-code", &request.source.source_id, &native)
}

fn read_agent_state(
    root: &RootBinding,
    session: &Path,
    request: &ScanRequest,
) -> Result<AgentState, AdapterError> {
    validate_session_chain(root, session)?;
    let path = session.join("state.json");
    let (mut file, metadata, _identity) = open_regular_no_follow(&path, "kimi_state")?;
    if metadata.len() > super::MAX_STATE_BYTES {
        return Err(AdapterError::UnsupportedFormat {
            stage: "kimi_state_type_or_size".to_string(),
        });
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize + 1);
    file.by_ref()
        .take(metadata.len() + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| AdapterError::PermissionDenied {
            stage: "kimi_state_read".to_string(),
        })?;
    request.budget.charge_bytes_read(bytes.len() as u64)?;
    let after = file
        .metadata()
        .map_err(|_| AdapterError::SnapshotUnavailable)?;
    if after.len() != metadata.len() || bytes.len() as u64 != metadata.len() {
        return Err(AdapterError::SnapshotUnavailable);
    }
    validate_session_chain(root, session)?;
    let state: AgentState =
        serde_json::from_slice(&bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_agents".to_string(),
        })?;
    let raw: RawWorkdir<'_> =
        serde_json::from_slice(&bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_path".to_string(),
        })?;
    let legacy_cwd = raw
        .custom
        .map(|value| serde_json::from_str::<RawLegacyCustom<'_>>(value.get()))
        .transpose()
        .map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_path".to_string(),
        })?
        .and_then(|custom| custom.cwd);
    let _bucket_verified = validate_workdir_bucket(raw.work_dir, legacy_cwd, session)?;
    validate_agents(&state.agents)?;
    Ok(state)
}

fn validate_wire_chain(root: &RootBinding, wire: &Path) -> Result<(), AdapterError> {
    let agent = wire.parent().ok_or(AdapterError::SnapshotUnavailable)?;
    let agents = agent.parent().ok_or(AdapterError::SnapshotUnavailable)?;
    let session = agents.parent().ok_or(AdapterError::SnapshotUnavailable)?;
    if agents.file_name().and_then(|name| name.to_str()) != Some("agents") {
        return Err(AdapterError::SnapshotUnavailable);
    }
    validate_session_chain(root, session)?;
    validate_directory(agents)?;
    validate_directory(agent)?;
    Ok(())
}

fn tool_record(
    request: &ScanRequest,
    call: PendingTool,
    output: Option<String>,
    status: Option<&str>,
    ended_at: Option<DateTime<Utc>>,
) -> ToolCallRecord {
    let native = NativeId::new(format!("{}/tool/{}", call.session_id, call.native_id));
    ToolCallRecord {
        tool_call_id: EntityId::from_parts("kimi-code", &request.source.source_id, &native),
        session_id: call.session_id,
        message_id: None,
        source_id: request.source.source_id.clone(),
        sequence: call.sequence,
        tool_name: call.name,
        namespace: None,
        arguments: call.arguments,
        output,
        status: status.map(str::to_string),
        started_at: call.started_at,
        ended_at,
        duration_ms: None,
        exit_code: None,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

fn timestamp(value: Option<i64>) -> Result<Option<DateTime<Utc>>, AdapterError> {
    value
        .map(|millis| {
            Utc.timestamp_millis_opt(millis)
                .single()
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "kimi_timestamp".to_string(),
                })
        })
        .transpose()
}
