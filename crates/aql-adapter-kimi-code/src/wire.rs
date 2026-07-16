use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Take};
use std::path::{Path, PathBuf};

use aql_adapter_api::{
    AdapterError, AdapterWarning, AdapterWarningKind, PushdownReport, PushdownState,
    ScanDiagnostics, ScanRequest, ScanResult, SnapshotReport, SnapshotStrength, TableName,
    check_scan_state,
};
use aql_model::{
    CanonicalRecord, EntityId, MessageRecord, NativeId, SessionEdgeRecord, ToolCallRecord,
    UsageRecord,
};
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{
    FileIdentity, KimiCodeAdapter, RootBinding, SafeAgent, normalize_limit, open_regular_no_follow,
    projected, validate_agents, validate_directory, validate_session_chain,
    validate_workdir_bucket,
};

const MAX_WIRE_RECORD_BYTES: usize = 1024 * 1024;

pub(super) fn scan(
    root: RootBinding,
    mut request: ScanRequest,
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
            after_session: None,
            agents: VecDeque::new(),
            current: None,
            current_session: None,
            sequence: 0,
            emitted: 0,
            finished: false,
            pending_tools: BTreeMap::new(),
            ready: VecDeque::new(),
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
    ready: VecDeque<SessionEdgeRecord>,
    emitted: u64,
    finished: bool,
}

struct WireStream {
    root: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    after_session: Option<(String, String)>,
    agents: VecDeque<(PathBuf, String, EntityId)>,
    current: Option<WireFile>,
    current_session: Option<EntityId>,
    sequence: i64,
    emitted: u64,
    finished: bool,
    pending_tools: BTreeMap<String, PendingTool>,
    ready: VecDeque<CanonicalRecord>,
}

struct WireFile {
    reader: BufReader<Take<File>>,
    path: PathBuf,
    identity: FileIdentity,
    expected_len: u64,
    metadata_seen: bool,
    agent_id: String,
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
    fn open_next_file(&mut self) -> Result<bool, AdapterError> {
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
                if metadata.len() > self.request.budget.max_bytes_read {
                    return Err(AdapterError::BudgetExceeded {
                        resource: "bytes_read".to_string(),
                        actual: metadata.len(),
                    });
                }
                self.current = Some(WireFile {
                    reader: BufReader::new(file.take(metadata.len())),
                    path,
                    identity,
                    expected_len: metadata.len(),
                    metadata_seen: false,
                    agent_id,
                });
                self.current_session = Some(logical_session);
                self.sequence = 0;
                return Ok(true);
            }
            let Some((session_dir, key)) =
                KimiCodeAdapter::next_session_dir(&self.root, self.after_session.as_ref())?
            else {
                return Ok(false);
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

    fn parse_line(&mut self, line: &[u8], complete: bool) -> Result<(), AdapterError> {
        let envelope: Envelope<'_> = match serde_json::from_slice(line) {
            Ok(value) => value,
            Err(_) if !complete => {
                self.diagnostics.push(AdapterWarning {
                    kind: AdapterWarningKind::TruncatedRecord,
                    source_kind: "kimi_wire".to_string(),
                    stage: "truncated_tail".to_string(),
                })?;
                return Ok(());
            }
            Err(_) => {
                return Err(AdapterError::CorruptSource {
                    stage: "kimi_wire_record".to_string(),
                });
            }
        };
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| AdapterError::Internal {
                stage: "kimi_wire_state".to_string(),
            })?;
        if !current.metadata_seen {
            if envelope.kind != "metadata"
                || !matches!(envelope.protocol_version, Some("1.4" | "1.0"))
            {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "kimi_wire_protocol".to_string(),
                });
            }
            current.metadata_seen = true;
            return Ok(());
        }
        match self.request.table {
            TableName::Messages if envelope.kind == "context.append_message" => {
                self.parse_message(&envelope)?;
            }
            TableName::Usage if envelope.kind == "usage.record" => self.parse_usage(&envelope)?,
            TableName::ToolCalls if envelope.kind == "context.append_loop_event" => {
                self.parse_tool_event(&envelope)?;
            }
            _ if matches!(
                envelope.kind,
                "turn.prompt"
                    | "turn.steer"
                    | "context.append_loop_event"
                    | "usage.record"
                    | "context.append_message"
            ) => {}
            _ => self.diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::UnknownEvent,
                source_kind: "kimi_wire".to_string(),
                stage: "unknown_record".to_string(),
            })?,
        }
        Ok(())
    }

    fn parse_message(&mut self, envelope: &Envelope<'_>) -> Result<(), AdapterError> {
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
        self.sequence += 1;
        let wants_content = projected(&self.request.projection, "content");
        let wants_content_json = projected(&self.request.projection, "content_json");
        if (wants_content || wants_content_json)
            && raw.get().len() as u64 > self.request.budget.max_single_value_bytes
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
        let session_id = self
            .current_session
            .clone()
            .ok_or_else(|| AdapterError::Internal {
                stage: "kimi_wire_session".to_string(),
            })?;
        let agent = self
            .current
            .as_ref()
            .map(|file| file.agent_id.as_str())
            .unwrap_or("main");
        let native = NativeId::new(format!("{}/{agent}/message/{}", session_id, self.sequence));
        self.ready
            .push_back(CanonicalRecord::Message(MessageRecord {
                message_id: EntityId::from_parts(
                    "kimi-code",
                    &self.request.source.source_id,
                    &native,
                ),
                session_id,
                source_id: self.request.source.source_id.clone(),
                sequence: self.sequence,
                role: meta.role,
                kind: Some("message".to_string()),
                content,
                content_json,
                model: None,
                created_at: timestamp(envelope.time),
                input_tokens: None,
                output_tokens: None,
                cached_tokens: None,
                is_error: meta.is_error,
                provenance: BTreeMap::new(),
                extensions: BTreeMap::new(),
            }));
        Ok(())
    }

    fn parse_usage(&mut self, envelope: &Envelope<'_>) -> Result<(), AdapterError> {
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
        self.sequence += 1;
        let session_id = self
            .current_session
            .clone()
            .ok_or_else(|| AdapterError::Internal {
                stage: "kimi_wire_session".to_string(),
            })?;
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
        let native = NativeId::new(format!("{session_id}/usage/{}", self.sequence));
        self.ready.push_back(CanonicalRecord::Usage(UsageRecord {
            usage_id: EntityId::from_parts("kimi-code", &self.request.source.source_id, &native),
            source_id: self.request.source.source_id.clone(),
            agent_id: "kimi-code".to_string(),
            session_id: Some(session_id),
            model: envelope.model.map(str::to_string),
            provider: None,
            bucket_start: timestamp(envelope.time),
            input_tokens: Some(input),
            output_tokens: Some(usage.output),
            cached_tokens: Some(usage.input_cache_read),
            total_tokens: Some(total),
            message_count: 0,
            tool_call_count: 0,
            error_count: 0,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }));
        Ok(())
    }

    fn parse_tool_event(&mut self, envelope: &Envelope<'_>) -> Result<(), AdapterError> {
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
        let session_id = self
            .current_session
            .clone()
            .ok_or_else(|| AdapterError::Internal {
                stage: "kimi_wire_session".to_string(),
            })?;
        match event.kind {
            "tool.call" => {
                self.sequence += 1;
                let id = event
                    .tool_call_id
                    .ok_or_else(|| AdapterError::CorruptSource {
                        stage: "kimi_tool_call_id".to_string(),
                    })?;
                let arguments = if projected(&self.request.projection, "arguments") {
                    let raw = event.args.ok_or_else(|| AdapterError::CorruptSource {
                        stage: "kimi_tool_args".to_string(),
                    })?;
                    if raw.get().len() as u64 > self.request.budget.max_single_value_bytes {
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
                        session_id,
                        sequence: self.sequence,
                        name: event
                            .name
                            .ok_or_else(|| AdapterError::CorruptSource {
                                stage: "kimi_tool_name".to_string(),
                            })?
                            .to_string(),
                        arguments,
                        started_at: timestamp(envelope.time),
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
                    self.diagnostics.push(AdapterWarning {
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
                let output = if projected(&self.request.projection, "output") {
                    if result.output.get().len() as u64 > self.request.budget.max_single_value_bytes
                    {
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
                self.ready.push_back(CanonicalRecord::ToolCall(tool_record(
                    &self.request,
                    call,
                    output,
                    Some(if result.is_error == Some(true) {
                        "error"
                    } else {
                        "completed"
                    }),
                    timestamp(envelope.time),
                )));
            }
            _ => {}
        }
        Ok(())
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
            if let Some(record) = self.ready.pop_front() {
                if let Err(error) = self.request.budget.charge_records(1) {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.emitted += 1;
                return Some(Ok(record));
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
            if self.current.is_none() {
                match self.open_next_file() {
                    Ok(true) => {}
                    Ok(false) => {
                        self.finished = true;
                        return None;
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
            }
            let read = {
                let current = self.current.as_mut()?;
                read_limited_line(&mut current.reader)
            };
            match read {
                Ok(Some((line, complete, bytes))) => {
                    if let Err(error) = self.request.budget.charge_bytes_read(bytes as u64) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    if let Err(error) = self.parse_line(&line, complete) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
                Ok(None) => {
                    let current = self.current.take()?;
                    let metadata = match current.reader.get_ref().get_ref().metadata() {
                        Ok(value) => value,
                        Err(_) => {
                            self.finished = true;
                            return Some(Err(AdapterError::SnapshotUnavailable));
                        }
                    };
                    let path_identity = match aql_fs::file_identity(&current.path) {
                        Ok(value) => value,
                        Err(_) => {
                            self.finished = true;
                            return Some(Err(AdapterError::SnapshotUnavailable));
                        }
                    };
                    if metadata.len() < current.expected_len
                        || path_identity != current.identity
                        || validate_wire_chain(&self.root, &current.path).is_err()
                    {
                        self.finished = true;
                        return Some(Err(AdapterError::SnapshotUnavailable));
                    }
                    if !current.metadata_seen {
                        self.finished = true;
                        return Some(Err(AdapterError::UnsupportedFormat {
                            stage: "kimi_wire_metadata".to_string(),
                        }));
                    }
                    if self.request.table == TableName::ToolCalls && !self.pending_tools.is_empty()
                    {
                        let pending = std::mem::take(&mut self.pending_tools);
                        for (_, call) in pending {
                            self.ready.push_back(CanonicalRecord::ToolCall(tool_record(
                                &self.request,
                                call,
                                None,
                                Some("interrupted"),
                                None,
                            )));
                        }
                    }
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
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
            let next =
                match KimiCodeAdapter::next_session_dir(&self.root, self.after_session.as_ref()) {
                    Ok(Some(next)) => next,
                    Ok(None) => {
                        self.finished = true;
                        return None;
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
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
    if after.len() != metadata.len()
        || bytes.len() as u64 != metadata.len()
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    validate_session_chain(root, session)?;
    let state: AgentState =
        serde_json::from_slice(&bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_agents".to_string(),
        })?;
    let _bucket_verified = validate_workdir_bucket(&bytes, session)?;
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

fn read_limited_line(
    reader: &mut BufReader<Take<File>>,
) -> Result<Option<(Vec<u8>, bool, usize)>, AdapterError> {
    let mut output = Vec::new();
    let mut consumed = 0;
    loop {
        let buffer = reader
            .fill_buf()
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "kimi_wire_read".to_string(),
            })?;
        if buffer.is_empty() {
            return if output.is_empty() {
                Ok(None)
            } else {
                Ok(Some((output, false, consumed)))
            };
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(buffer.len(), |index| index + 1);
        let payload = if newline.is_some() { take - 1 } else { take };
        if output.len() + payload > MAX_WIRE_RECORD_BYTES {
            return Err(AdapterError::BudgetExceeded {
                resource: "kimi_wire_record_bytes".to_string(),
                actual: (output.len() + payload) as u64,
            });
        }
        output.extend_from_slice(&buffer[..payload]);
        reader.consume(take);
        consumed += take;
        if newline.is_some() {
            return Ok(Some((output, true, consumed)));
        }
    }
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

fn timestamp(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(|millis| Utc.timestamp_millis_opt(millis).single())
}
