use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Take};

use aql_adapter_api::{
    AdapterError, AdapterWarning, AdapterWarningKind, RecordStream, ScanDiagnostics, ScanRequest,
    TableName, check_scan_state,
};
use aql_model::{CanonicalRecord, EntityId, MessageRecord, NativeId, ToolCallRecord, UsageRecord};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{
    LogicalTranscript, MAX_RECORD_BYTES, MainTranscripts, RootBinding, TranscriptDescriptor,
    TranscriptKind, open_transcript, projected, revalidate_transcript,
};

pub(super) struct SessionSummary {
    pub preview: Option<String>,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub message_count: Option<i64>,
    pub tool_call_count: Option<i64>,
    pub tokens_used: Option<i64>,
}

struct TranscriptStream {
    root: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    descriptors: VecDeque<TranscriptDescriptor>,
    mains: MainTranscripts,
    current: Option<CurrentTranscript>,
    ready: VecDeque<CanonicalRecord>,
    emitted: u64,
    finished: bool,
}

struct CurrentTranscript {
    reader: BufReader<Take<File>>,
    logical: LogicalTranscript,
    sequence: i64,
    tool_sequence: i64,
    pending_tools: BTreeMap<String, PendingTool>,
    seen_entries: BTreeSet<String>,
    seen_usage: BTreeMap<String, UsageValues>,
}

struct PendingTool {
    native_id: String,
    name: String,
    arguments: Option<serde_json::Value>,
    session_id: EntityId,
    message_id: EntityId,
    sequence: i64,
    started_at: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    uuid: Option<&'a str>,
    session_id: Option<&'a str>,
    agent_id: Option<&'a str>,
    version: Option<&'a str>,
    timestamp: Option<&'a str>,
    is_api_error_message: Option<bool>,
    #[serde(borrow)]
    cwd: Option<&'a RawValue>,
    #[serde(borrow)]
    message: Option<&'a RawValue>,
    #[serde(borrow)]
    last_prompt: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct MessageEnvelope<'a> {
    role: &'a str,
    id: Option<&'a str>,
    model: Option<&'a str>,
    #[serde(borrow)]
    content: &'a RawValue,
    #[serde(borrow)]
    usage: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct ContentBlock<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: Option<&'a str>,
    name: Option<&'a str>,
    tool_use_id: Option<&'a str>,
    is_error: Option<bool>,
    #[serde(borrow)]
    text: Option<&'a RawValue>,
    #[serde(borrow)]
    thinking: Option<&'a RawValue>,
    #[serde(borrow)]
    input: Option<&'a RawValue>,
    #[serde(borrow)]
    content: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Usage {
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cache_creation_input_tokens: Option<i64>,
    cache_read_input_tokens: Option<i64>,
}

pub(super) fn scan(
    root: RootBinding,
    descriptors: Vec<TranscriptDescriptor>,
    mains: MainTranscripts,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
) -> RecordStream {
    Box::new(TranscriptStream {
        root,
        request,
        diagnostics,
        descriptors: descriptors.into(),
        mains,
        current: None,
        ready: VecDeque::new(),
        emitted: 0,
        finished: false,
    })
}

pub(super) fn resolve_logical(
    root: &RootBinding,
    descriptor: TranscriptDescriptor,
    mains: &MainTranscripts,
    request: &ScanRequest,
    diagnostics: &ScanDiagnostics,
) -> Result<LogicalTranscript, AdapterError> {
    match descriptor.kind.clone() {
        TranscriptKind::Main { session } => Ok(LogicalTranscript {
            descriptor,
            main_native: session.clone(),
            logical_native: session,
            agent: None,
        }),
        TranscriptKind::Agent { agent } => {
            let main = first_identity(root, &descriptor, request, diagnostics, Some(&agent))?;
            if !mains.contains(&(descriptor.project_key.clone(), main.clone())) {
                return Err(AdapterError::CorruptSource {
                    stage: "claude_agent_parent".to_string(),
                });
            }
            Ok(LogicalTranscript {
                descriptor,
                logical_native: format!("{main}/agent/{agent}"),
                main_native: main,
                agent: Some(agent),
            })
        }
    }
}

pub(super) fn summarize(
    root: &RootBinding,
    logical: &LogicalTranscript,
    request: &ScanRequest,
    diagnostics: &ScanDiagnostics,
) -> Result<SessionSummary, AdapterError> {
    let wants_preview = projected(&request.projection, "preview");
    let wants_path =
        projected(&request.projection, "cwd") || projected(&request.projection, "project");
    let wants_model = projected(&request.projection, "model");
    let wants_created = projected(&request.projection, "created_at");
    let wants_messages = projected(&request.projection, "message_count");
    let wants_tools = projected(&request.projection, "tool_call_count");
    let wants_tokens = projected(&request.projection, "tokens_used");
    if !(wants_preview
        || wants_path
        || wants_model
        || wants_created
        || wants_messages
        || wants_tools
        || wants_tokens)
    {
        return Ok(SessionSummary {
            preview: None,
            cwd: None,
            model: None,
            created_at: None,
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
        });
    }

    let file = open_transcript(root, &logical.descriptor)?;
    let mut reader = BufReader::new(file.take(logical.descriptor.len));
    let mut preview = None;
    let mut cwd = None;
    let mut model = None;
    let mut created_at: Option<DateTime<Utc>> = None;
    let mut message_count = 0_i64;
    let mut tool_call_count = 0_i64;
    let mut tokens_used = 0_i64;
    let mut has_tokens = false;
    let mut seen_assistant_messages = BTreeSet::new();
    let mut seen_usage = BTreeMap::new();
    let mut seen_entries = BTreeSet::new();
    while let Some((line, complete, bytes)) = read_limited_line(&mut reader)? {
        request.budget.charge_bytes_read(bytes as u64)?;
        let envelope = match parse_envelope(&line, complete, diagnostics)? {
            Some(value) => value,
            None => break,
        };
        validate_envelope_identity(&envelope, logical)?;
        let timestamp = parse_timestamp(envelope.timestamp)?;
        if wants_created && timestamp.is_some() {
            created_at = match (created_at, timestamp) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (None, candidate) => candidate,
                (current, None) => current,
            };
        }
        if wants_path && let Some(raw) = envelope.cwd {
            cwd = Some(raw_string(raw, request.budget.max_single_value_bytes)?);
        }
        if envelope.kind == "last-prompt" && wants_preview {
            if let Some(raw) = envelope.last_prompt {
                preview = Some(raw_string(raw, request.budget.max_single_value_bytes)?);
            }
            continue;
        }
        if !matches!(envelope.kind, "user" | "assistant") {
            if !known_event(envelope.kind) {
                warn_unknown(diagnostics)?;
            }
            continue;
        }
        let uuid = envelope.uuid.ok_or_else(|| AdapterError::CorruptSource {
            stage: "claude_message_uuid".to_string(),
        })?;
        if !seen_entries.insert(uuid.to_string()) {
            return Err(AdapterError::CorruptSource {
                stage: "claude_duplicate_entry".to_string(),
            });
        }
        let message = parse_message_envelope(&envelope)?;
        if wants_messages {
            let unique = if envelope.kind == "assistant" {
                message.id.unwrap_or(uuid)
            } else {
                uuid
            };
            if envelope.kind != "assistant" || seen_assistant_messages.insert(unique.to_string()) {
                message_count =
                    message_count
                        .checked_add(1)
                        .ok_or_else(|| AdapterError::CorruptSource {
                            stage: "claude_message_count".to_string(),
                        })?;
            }
        }
        if wants_model && let Some(value) = message.model {
            model = Some(value.to_string());
        }
        if wants_tools {
            let blocks = content_blocks(message.content)?;
            let count = blocks
                .iter()
                .filter(|block| block.kind == "tool_use")
                .count();
            tool_call_count = tool_call_count
                .checked_add(
                    i64::try_from(count).map_err(|_| AdapterError::CorruptSource {
                        stage: "claude_tool_count".to_string(),
                    })?,
                )
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_tool_count".to_string(),
                })?;
        }
        if wants_tokens
            && envelope.kind == "assistant"
            && let Some(raw) = message.usage
        {
            let key = message.id.ok_or_else(|| AdapterError::CorruptSource {
                stage: "claude_usage_id".to_string(),
            })?;
            if let Some(value) = usage_values(raw)? {
                if let Some(previous) = seen_usage.get(key) {
                    if previous != &value {
                        return Err(AdapterError::CorruptSource {
                            stage: "claude_usage_conflict".to_string(),
                        });
                    }
                } else {
                    tokens_used = tokens_used.checked_add(value.total).ok_or_else(|| {
                        AdapterError::CorruptSource {
                            stage: "claude_usage_overflow".to_string(),
                        }
                    })?;
                    seen_usage.insert(key.to_string(), value);
                    has_tokens = true;
                }
            }
        }
    }
    revalidate_transcript(root, &logical.descriptor, reader.get_ref().get_ref())?;
    Ok(SessionSummary {
        preview,
        cwd,
        model,
        created_at,
        message_count: wants_messages.then_some(message_count),
        tool_call_count: wants_tools.then_some(tool_call_count),
        tokens_used: (wants_tokens && has_tokens).then_some(tokens_used),
    })
}

impl Iterator for TranscriptStream {
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
                let Some(descriptor) = self.descriptors.pop_front() else {
                    self.finished = true;
                    return None;
                };
                let logical = match resolve_logical(
                    &self.root,
                    descriptor,
                    &self.mains,
                    &self.request,
                    &self.diagnostics,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
                let file = match open_transcript(&self.root, &logical.descriptor) {
                    Ok(value) => value,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
                self.current = Some(CurrentTranscript {
                    reader: BufReader::new(file.take(logical.descriptor.len)),
                    logical,
                    sequence: 0,
                    tool_sequence: 0,
                    pending_tools: BTreeMap::new(),
                    seen_entries: BTreeSet::new(),
                    seen_usage: BTreeMap::new(),
                });
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
                    let mut current = self.current.take()?;
                    if let Err(error) = revalidate_transcript(
                        &self.root,
                        &current.logical.descriptor,
                        current.reader.get_ref().get_ref(),
                    ) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    if self.request.table == TableName::ToolCalls {
                        for (_, pending) in std::mem::take(&mut current.pending_tools) {
                            self.ready.push_back(CanonicalRecord::ToolCall(tool_record(
                                &self.request,
                                pending,
                                None,
                                "interrupted",
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

impl TranscriptStream {
    fn parse_line(&mut self, line: &[u8], complete: bool) -> Result<(), AdapterError> {
        let Some(envelope) = parse_envelope(line, complete, &self.diagnostics)? else {
            return Ok(());
        };
        let current = self
            .current
            .as_ref()
            .ok_or_else(|| AdapterError::Internal {
                stage: "claude_current_transcript".to_string(),
            })?;
        validate_envelope_identity(&envelope, &current.logical)?;
        if matches!(envelope.kind, "user" | "assistant") {
            let uuid = envelope.uuid.ok_or_else(|| AdapterError::CorruptSource {
                stage: "claude_message_uuid".to_string(),
            })?;
            let current = self
                .current
                .as_mut()
                .ok_or_else(|| AdapterError::Internal {
                    stage: "claude_current_transcript".to_string(),
                })?;
            if !current.seen_entries.insert(uuid.to_string()) {
                return Err(AdapterError::CorruptSource {
                    stage: "claude_duplicate_entry".to_string(),
                });
            }
        }
        match self.request.table {
            TableName::Messages if matches!(envelope.kind, "user" | "assistant") => {
                self.parse_message(&envelope)?;
            }
            TableName::ToolCalls if matches!(envelope.kind, "user" | "assistant") => {
                self.parse_tools(&envelope)?;
            }
            TableName::Usage if envelope.kind == "assistant" => {
                self.parse_usage(&envelope)?;
            }
            _ if known_event(envelope.kind) => {}
            _ => warn_unknown(&self.diagnostics)?,
        }
        Ok(())
    }

    fn parse_message(&mut self, envelope: &Envelope<'_>) -> Result<(), AdapterError> {
        let message = parse_message_envelope(envelope)?;
        if message.role != envelope.kind {
            return Err(AdapterError::CorruptSource {
                stage: "claude_message_role".to_string(),
            });
        }
        let uuid = envelope.uuid.ok_or_else(|| AdapterError::CorruptSource {
            stage: "claude_message_uuid".to_string(),
        })?;
        let wants_content = projected(&self.request.projection, "content");
        let wants_json = projected(&self.request.projection, "content_json");
        let parsed = sanitized_message_content(
            message.content,
            wants_content,
            wants_json,
            self.request.budget.max_single_value_bytes,
        )?;
        if parsed.unknown_blocks {
            self.diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::UnknownField,
                source_kind: "claude_transcript".to_string(),
                stage: "unknown_content_block".to_string(),
            })?;
        }
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| AdapterError::Internal {
                stage: "claude_current_transcript".to_string(),
            })?;
        current.sequence += 1;
        let native = NativeId::new(format!("{}/message/{uuid}", current.logical.logical_native));
        let role = if parsed.only_tool_results {
            "tool".to_string()
        } else {
            message.role.to_string()
        };
        self.ready
            .push_back(CanonicalRecord::Message(MessageRecord {
                message_id: EntityId::from_parts(
                    "claude-code",
                    &self.request.source.source_id,
                    &native,
                ),
                session_id: logical_session_id(&self.request, &current.logical),
                source_id: self.request.source.source_id.clone(),
                sequence: current.sequence,
                role,
                kind: Some(parsed.kind),
                content: parsed.content,
                content_json: parsed.content_json,
                model: message.model.map(str::to_string),
                created_at: parse_timestamp(envelope.timestamp)?,
                input_tokens: None,
                output_tokens: None,
                cached_tokens: None,
                is_error: Some(
                    envelope.is_api_error_message == Some(true) || parsed.tool_result_error,
                ),
                provenance: BTreeMap::new(),
                extensions: BTreeMap::new(),
            }));
        Ok(())
    }

    fn parse_tools(&mut self, envelope: &Envelope<'_>) -> Result<(), AdapterError> {
        let message = parse_message_envelope(envelope)?;
        let blocks = content_blocks(message.content)?;
        if blocks
            .iter()
            .any(|block| !matches!(block.kind, "text" | "thinking" | "tool_use" | "tool_result"))
        {
            self.diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::UnknownField,
                source_kind: "claude_transcript".to_string(),
                stage: "unknown_content_block".to_string(),
            })?;
        }
        let timestamp = parse_timestamp(envelope.timestamp)?;
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| AdapterError::Internal {
                stage: "claude_current_transcript".to_string(),
            })?;
        if envelope.kind == "assistant" {
            let uuid = envelope.uuid.ok_or_else(|| AdapterError::CorruptSource {
                stage: "claude_message_uuid".to_string(),
            })?;
            for block in blocks.into_iter().filter(|block| block.kind == "tool_use") {
                let id = block.id.ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_tool_id".to_string(),
                })?;
                let name = block.name.ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_tool_name".to_string(),
                })?;
                if id.is_empty() || id.len() > 512 || name.is_empty() || name.len() > 256 {
                    return Err(AdapterError::CorruptSource {
                        stage: "claude_tool_identity".to_string(),
                    });
                }
                let input = block.input.ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_tool_input".to_string(),
                })?;
                let arguments = if projected(&self.request.projection, "arguments") {
                    bounded_json(input, self.request.budget.max_single_value_bytes)?
                } else {
                    None
                };
                current.tool_sequence += 1;
                let message_native =
                    NativeId::new(format!("{}/message/{uuid}", current.logical.logical_native));
                let pending = PendingTool {
                    native_id: format!("{}/tool/{id}", current.logical.logical_native),
                    name: name.to_string(),
                    arguments,
                    session_id: logical_session_id(&self.request, &current.logical),
                    message_id: EntityId::from_parts(
                        "claude-code",
                        &self.request.source.source_id,
                        &message_native,
                    ),
                    sequence: current.tool_sequence,
                    started_at: timestamp,
                };
                if current
                    .pending_tools
                    .insert(id.to_string(), pending)
                    .is_some()
                {
                    return Err(AdapterError::CorruptSource {
                        stage: "claude_duplicate_tool".to_string(),
                    });
                }
            }
        } else {
            for block in blocks
                .into_iter()
                .filter(|block| block.kind == "tool_result")
            {
                let id = block
                    .tool_use_id
                    .ok_or_else(|| AdapterError::CorruptSource {
                        stage: "claude_tool_result_id".to_string(),
                    })?;
                let Some(pending) = current.pending_tools.remove(id) else {
                    self.diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::IncompleteCapability,
                        source_kind: "claude_transcript".to_string(),
                        stage: "unpaired_tool_result".to_string(),
                    })?;
                    continue;
                };
                let raw_output = block.content.ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_tool_output".to_string(),
                })?;
                if !raw_output.get().trim_start().starts_with('"') {
                    return Err(AdapterError::UnsupportedFormat {
                        stage: "claude_tool_output_type".to_string(),
                    });
                }
                let is_error = block.is_error.ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_tool_result_status".to_string(),
                })?;
                let output = if projected(&self.request.projection, "output") {
                    Some(tool_output(
                        raw_output,
                        self.request.budget.max_single_value_bytes,
                    )?)
                } else {
                    None
                };
                let status = if is_error { "error" } else { "completed" };
                self.ready.push_back(CanonicalRecord::ToolCall(tool_record(
                    &self.request,
                    pending,
                    output,
                    status,
                    timestamp,
                )));
            }
        }
        Ok(())
    }

    fn parse_usage(&mut self, envelope: &Envelope<'_>) -> Result<(), AdapterError> {
        let message = parse_message_envelope(envelope)?;
        let Some(raw) = message.usage else {
            return Ok(());
        };
        let key = message.id.ok_or_else(|| AdapterError::CorruptSource {
            stage: "claude_usage_id".to_string(),
        })?;
        let current = self
            .current
            .as_mut()
            .ok_or_else(|| AdapterError::Internal {
                stage: "claude_current_transcript".to_string(),
            })?;
        let Some(values) = usage_values(raw)? else {
            return Ok(());
        };
        if let Some(previous) = current.seen_usage.get(key) {
            if previous != &values {
                return Err(AdapterError::CorruptSource {
                    stage: "claude_usage_conflict".to_string(),
                });
            }
            return Ok(());
        }
        current.seen_usage.insert(key.to_string(), values.clone());
        let native = NativeId::new(format!("{}/usage/{key}", current.logical.logical_native));
        self.ready.push_back(CanonicalRecord::Usage(UsageRecord {
            usage_id: EntityId::from_parts("claude-code", &self.request.source.source_id, &native),
            source_id: self.request.source.source_id.clone(),
            agent_id: "claude-code".to_string(),
            session_id: Some(logical_session_id(&self.request, &current.logical)),
            model: message.model.map(str::to_string),
            provider: None,
            bucket_start: parse_timestamp(envelope.timestamp)?,
            input_tokens: values.input,
            output_tokens: values.output,
            cached_tokens: values.cached,
            total_tokens: Some(values.total),
            message_count: 0,
            tool_call_count: 0,
            error_count: i64::from(envelope.is_api_error_message == Some(true)),
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }));
        Ok(())
    }
}

fn first_identity(
    root: &RootBinding,
    descriptor: &TranscriptDescriptor,
    request: &ScanRequest,
    diagnostics: &ScanDiagnostics,
    expected_agent: Option<&str>,
) -> Result<String, AdapterError> {
    let file = open_transcript(root, descriptor)?;
    let mut reader = BufReader::new(file.take(descriptor.len));
    loop {
        let Some((line, complete, bytes)) = read_limited_line(&mut reader)? else {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_transcript_identity".to_string(),
            });
        };
        request.budget.charge_bytes_read(bytes as u64)?;
        let Some(envelope) = parse_envelope(&line, complete, diagnostics)? else {
            continue;
        };
        let Some(session) = envelope.session_id else {
            continue;
        };
        if let Some(expected) = expected_agent {
            if !matches!(envelope.kind, "user" | "assistant") {
                continue;
            }
            if envelope.agent_id != Some(expected) {
                return Err(AdapterError::CorruptSource {
                    stage: "claude_agent_identity".to_string(),
                });
            }
        }
        if matches!(envelope.kind, "user" | "assistant")
            && envelope
                .version
                .is_none_or(|version| !version.starts_with("2."))
        {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_version".to_string(),
            });
        }
        revalidate_transcript(root, descriptor, reader.get_ref().get_ref())?;
        return Ok(session.to_string());
    }
}

fn validate_envelope_identity(
    envelope: &Envelope<'_>,
    logical: &LogicalTranscript,
) -> Result<(), AdapterError> {
    if matches!(envelope.kind, "user" | "assistant") {
        if envelope.session_id != Some(logical.main_native.as_str()) {
            return Err(AdapterError::CorruptSource {
                stage: "claude_session_identity".to_string(),
            });
        }
        if envelope
            .version
            .is_none_or(|version| !version.starts_with("2."))
        {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_version".to_string(),
            });
        }
    }
    if let Some(expected) = logical.agent.as_deref()
        && matches!(envelope.kind, "user" | "assistant")
        && envelope.agent_id != Some(expected)
    {
        return Err(AdapterError::CorruptSource {
            stage: "claude_agent_identity".to_string(),
        });
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
                source_kind: "claude_transcript".to_string(),
                stage: "truncated_tail".to_string(),
            })?;
            Ok(None)
        }
        Err(_) => Err(AdapterError::CorruptSource {
            stage: "claude_transcript_json".to_string(),
        }),
    }
}

fn parse_message_envelope<'a>(
    envelope: &'a Envelope<'a>,
) -> Result<MessageEnvelope<'a>, AdapterError> {
    serde_json::from_str(
        envelope
            .message
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "claude_message".to_string(),
            })?
            .get(),
    )
    .map_err(|_| AdapterError::CorruptSource {
        stage: "claude_message".to_string(),
    })
}

fn content_blocks<'a>(raw: &'a RawValue) -> Result<Vec<ContentBlock<'a>>, AdapterError> {
    if !raw.get().trim_start().starts_with('[') {
        return Ok(Vec::new());
    }
    let blocks: Vec<ContentBlock<'a>> =
        serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
            stage: "claude_message_content".to_string(),
        })?;
    for block in &blocks {
        let valid = match block.kind {
            "text" => block.text.is_some_and(raw_json_string),
            "thinking" => block.thinking.is_some_and(raw_json_string),
            "tool_use" => {
                block
                    .id
                    .is_some_and(|value| !value.is_empty() && value.len() <= 512)
                    && block
                        .name
                        .is_some_and(|value| !value.is_empty() && value.len() <= 256)
                    && block
                        .input
                        .is_some_and(|value| value.get().trim_start().starts_with('{'))
            }
            "tool_result" => {
                block
                    .tool_use_id
                    .is_some_and(|value| !value.is_empty() && value.len() <= 512)
                    && block.content.is_some_and(raw_json_string)
                    && block.is_error.is_some()
            }
            _ => true,
        };
        if !valid {
            return Err(AdapterError::CorruptSource {
                stage: "claude_content_block".to_string(),
            });
        }
    }
    Ok(blocks)
}

fn raw_json_string(raw: &RawValue) -> bool {
    let value = raw.get().trim();
    value.len() >= 2 && value.starts_with('"') && value.ends_with('"')
}

struct ParsedMessageContent {
    content: Option<String>,
    content_json: Option<serde_json::Value>,
    kind: String,
    only_tool_results: bool,
    tool_result_error: bool,
    unknown_blocks: bool,
}

fn sanitized_message_content(
    raw: &RawValue,
    wants_content: bool,
    wants_json: bool,
    maximum: u64,
) -> Result<ParsedMessageContent, AdapterError> {
    let encoded = raw.get().trim_start();
    if encoded.starts_with('"') {
        let value = if wants_content || wants_json {
            Some(raw_string(raw, maximum)?)
        } else {
            None
        };
        return Ok(ParsedMessageContent {
            content: wants_content.then(|| value.clone()).flatten(),
            content_json: wants_json
                .then(|| value.map(serde_json::Value::String))
                .flatten(),
            kind: "message".to_string(),
            only_tool_results: false,
            tool_result_error: false,
            unknown_blocks: false,
        });
    }
    let blocks = content_blocks(raw)?;
    let only_tool_results =
        !blocks.is_empty() && blocks.iter().all(|block| block.kind == "tool_result");
    let tool_result_error = blocks
        .iter()
        .any(|block| block.kind == "tool_result" && block.is_error == Some(true));
    let has_text = blocks.iter().any(|block| block.kind == "text");
    let has_thinking = blocks.iter().any(|block| block.kind == "thinking");
    let has_tool_use = blocks.iter().any(|block| block.kind == "tool_use");
    let unknown_blocks = blocks
        .iter()
        .any(|block| !matches!(block.kind, "text" | "thinking" | "tool_use" | "tool_result"));
    let kind = match (has_text, has_thinking, has_tool_use, only_tool_results) {
        (_, _, _, true) => "tool_result",
        (false, false, true, false) => "tool_use",
        (false, true, false, false) => "reasoning",
        (true, false, false, false) => "message",
        _ => "mixed",
    }
    .to_string();
    let mut content = None;
    let mut json_blocks = Vec::new();
    for block in blocks {
        let (kind, value) = match block.kind {
            "text" => ("text", block.text),
            "thinking" => ("thinking", block.thinking),
            _ => continue,
        };
        if !(wants_content || wants_json) {
            continue;
        }
        let value = raw_string(
            value.ok_or_else(|| AdapterError::CorruptSource {
                stage: "claude_text_block".to_string(),
            })?,
            maximum,
        )?;
        if wants_content {
            append_content(&mut content, &value, maximum)?;
        }
        if wants_json {
            let mut object = serde_json::Map::new();
            object.insert(
                "type".to_string(),
                serde_json::Value::String(kind.to_string()),
            );
            object.insert(kind.to_string(), serde_json::Value::String(value));
            json_blocks.push(serde_json::Value::Object(object));
        }
    }
    Ok(ParsedMessageContent {
        content,
        content_json: (wants_json && !json_blocks.is_empty())
            .then_some(serde_json::Value::Array(json_blocks)),
        kind,
        only_tool_results,
        tool_result_error,
        unknown_blocks,
    })
}

fn raw_string(raw: &RawValue, maximum: u64) -> Result<String, AdapterError> {
    let bytes = raw.get().as_bytes();
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return Err(AdapterError::CorruptSource {
            stage: "claude_sensitive_type".to_string(),
        });
    }
    let upper = bytes.len().saturating_sub(2) as u64;
    if upper > maximum {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: upper,
        });
    }
    serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
        stage: "claude_sensitive_string".to_string(),
    })
}

fn append_content(
    current: &mut Option<String>,
    value: &str,
    maximum: u64,
) -> Result<(), AdapterError> {
    let separator = usize::from(current.is_some());
    let existing = current.as_ref().map_or(0, String::len);
    let total = existing
        .checked_add(separator)
        .and_then(|value_len| value_len.checked_add(value.len()))
        .ok_or_else(|| AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: u64::MAX,
        })?;
    if total as u64 > maximum {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: total as u64,
        });
    }
    let target = current.get_or_insert_with(String::new);
    if !target.is_empty() {
        target.push('\n');
    }
    target.push_str(value);
    Ok(())
}

fn bounded_json(raw: &RawValue, maximum: u64) -> Result<Option<serde_json::Value>, AdapterError> {
    if raw.get().len() as u64 > maximum {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: raw.get().len() as u64,
        });
    }
    let value: serde_json::Value =
        serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
            stage: "claude_tool_input".to_string(),
        })?;
    if !value.is_object() {
        return Err(AdapterError::UnsupportedFormat {
            stage: "claude_tool_input_type".to_string(),
        });
    }
    Ok(Some(value))
}

fn tool_output(raw: &RawValue, maximum: u64) -> Result<String, AdapterError> {
    if raw.get().len() as u64 > maximum {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: raw.get().len() as u64,
        });
    }
    let value: serde_json::Value =
        serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
            stage: "claude_tool_output".to_string(),
        })?;
    Ok(match value {
        serde_json::Value::String(value) => value,
        other => other.to_string(),
    })
}

#[derive(Clone, Eq, PartialEq)]
struct UsageValues {
    input: Option<i64>,
    output: Option<i64>,
    cached: Option<i64>,
    total: i64,
}

fn usage_values(raw: &RawValue) -> Result<Option<UsageValues>, AdapterError> {
    if raw.get().trim() == "null" {
        return Ok(None);
    }
    let usage: Usage =
        serde_json::from_str(raw.get()).map_err(|_| AdapterError::CorruptSource {
            stage: "claude_usage".to_string(),
        })?;
    let values = [
        usage.input_tokens,
        usage.output_tokens,
        usage.cache_creation_input_tokens,
        usage.cache_read_input_tokens,
    ];
    if values.into_iter().flatten().any(|value| value < 0) {
        return Err(AdapterError::CorruptSource {
            stage: "claude_usage_negative".to_string(),
        });
    }
    if values.into_iter().all(|value| value.is_none()) {
        return Ok(None);
    }
    let input = match (usage.input_tokens, usage.cache_creation_input_tokens) {
        (None, None) => None,
        (left, right) => Some(
            left.unwrap_or(0)
                .checked_add(right.unwrap_or(0))
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "claude_usage_overflow".to_string(),
                })?,
        ),
    };
    let total = input
        .unwrap_or(0)
        .checked_add(usage.output_tokens.unwrap_or(0))
        .and_then(|value| value.checked_add(usage.cache_read_input_tokens.unwrap_or(0)))
        .ok_or_else(|| AdapterError::CorruptSource {
            stage: "claude_usage_overflow".to_string(),
        })?;
    Ok(Some(UsageValues {
        input,
        output: usage.output_tokens,
        cached: usage.cache_read_input_tokens,
        total,
    }))
}

fn tool_record(
    request: &ScanRequest,
    pending: PendingTool,
    output: Option<String>,
    status: &str,
    ended_at: Option<DateTime<Utc>>,
) -> ToolCallRecord {
    let native = NativeId::new(pending.native_id);
    let duration_ms = pending.started_at.zip(ended_at).and_then(|(start, end)| {
        let duration = end.signed_duration_since(start).num_milliseconds();
        (duration >= 0).then_some(duration)
    });
    ToolCallRecord {
        tool_call_id: EntityId::from_parts("claude-code", &request.source.source_id, &native),
        session_id: pending.session_id,
        message_id: Some(pending.message_id),
        source_id: request.source.source_id.clone(),
        sequence: pending.sequence,
        tool_name: pending.name,
        namespace: None,
        arguments: pending.arguments,
        output,
        status: Some(status.to_string()),
        started_at: pending.started_at,
        ended_at,
        duration_ms,
        exit_code: None,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

fn logical_session_id(request: &ScanRequest, logical: &LogicalTranscript) -> EntityId {
    EntityId::from_parts(
        "claude-code",
        &request.source.source_id,
        &NativeId::new(logical.logical_native.clone()),
    )
}

fn parse_timestamp(value: Option<&str>) -> Result<Option<DateTime<Utc>>, AdapterError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(value)
                .map(|value| value.with_timezone(&Utc))
                .map_err(|_| AdapterError::CorruptSource {
                    stage: "claude_timestamp".to_string(),
                })
        })
        .transpose()
}

fn known_event(kind: &str) -> bool {
    matches!(
        kind,
        "assistant"
            | "attachment"
            | "file-history-snapshot"
            | "last-prompt"
            | "mode"
            | "permission-mode"
            | "queue-operation"
            | "system"
            | "user"
    )
}

fn warn_unknown(diagnostics: &ScanDiagnostics) -> Result<(), AdapterError> {
    diagnostics.push(AdapterWarning {
        kind: AdapterWarningKind::UnknownEvent,
        source_kind: "claude_transcript".to_string(),
        stage: "unknown_event".to_string(),
    })
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
                stage: "claude_transcript_read".to_string(),
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
        if output.len() + payload > MAX_RECORD_BYTES {
            return Err(AdapterError::BudgetExceeded {
                resource: "claude_record_bytes".to_string(),
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
