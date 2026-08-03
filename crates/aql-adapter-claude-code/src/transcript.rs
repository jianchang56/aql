use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{BufReader, Read};
use std::sync::Arc;

use aql_adapter_api::{
    AdapterError, AdapterWarning, AdapterWarningKind, ColumnName, RecordStream, ScanDiagnostics,
    ScanRequest, TableName, check_scan_state,
    util::{append_content, projected, read_limited_line},
};
use aql_model::{CanonicalRecord, EntityId, MessageRecord, NativeId, ToolCallRecord, UsageRecord};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::value::RawValue;

use super::cache::{
    CacheLookup, FileCacheKey, ParseCacheHandle, ParsedFile, SensitiveClasses, required_classes,
};
use super::{
    LogicalTranscript, MAX_RECORD_BYTES, MainTranscripts, RootBinding, TranscriptDescriptor,
    TranscriptKind, open_transcript, revalidate_transcript, revalidate_transcript_path,
};

#[derive(Clone)]
pub(crate) struct SessionSummary {
    pub preview: Option<String>,
    pub cwd: Option<String>,
    pub model: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub message_count: Option<i64>,
    pub tool_call_count: Option<i64>,
    pub tokens_used: Option<i64>,
}

impl SessionSummary {
    pub(crate) const fn empty() -> Self {
        Self {
            preview: None,
            cwd: None,
            model: None,
            created_at: None,
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
        }
    }
}

/// Streams one canonical transcript table, parsing each transcript once per
/// adapter (query) lifetime and replaying cached parses afterwards.
struct TranscriptStream {
    root: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    cache: ParseCacheHandle,
    descriptors: VecDeque<TranscriptDescriptor>,
    mains: MainTranscripts,
    current: Option<ReplayFile>,
    emitted: u64,
    finished: bool,
}

/// Projection-masked records of one transcript being replayed, plus the
/// end-of-file identity re-check owed when the parse came from the cache.
struct ReplayFile {
    descriptor: TranscriptDescriptor,
    records: VecDeque<CanonicalRecord>,
    recheck: bool,
    since_check: u64,
}

/// Cancellation/deadline polling cadence while replaying a cached parse.
const REPLAY_CHECK_RECORDS: u64 = 1024;

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
    cache: ParseCacheHandle,
) -> RecordStream {
    Box::new(TranscriptStream {
        root,
        request,
        diagnostics,
        cache,
        descriptors: descriptors.into(),
        mains,
        current: None,
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

pub(super) struct LoadedFile {
    pub(super) logical: LogicalTranscript,
    pub(super) parsed: Arc<ParsedFile>,
    pub(super) from_cache: bool,
}

/// Returns the single-pass parse of one transcript, served from the per-query
/// cache when identity, pinned length, grant, and needed classes all match.
pub(super) fn load_parsed(
    root: &RootBinding,
    descriptor: TranscriptDescriptor,
    mains: &MainTranscripts,
    request: &ScanRequest,
    diagnostics: &ScanDiagnostics,
    cache: &ParseCacheHandle,
) -> Result<LoadedFile, AdapterError> {
    let needed = required_classes(request.table, &request.projection);
    let key = FileCacheKey::new(
        &request.source.source_id,
        descriptor.identity,
        descriptor.len,
        request.access,
    );
    let lookup = cache
        .lock()
        .map_err(|_| AdapterError::Internal {
            stage: "claude_parse_cache".to_string(),
        })?
        .lookup(&key, needed);
    match lookup {
        CacheLookup::Hit(parsed) => {
            // The cached parse already validated agent parentage under the
            // same pinned watermark; repeat only the membership check against
            // this scan's inventory. The file itself is re-validated by the
            // caller's end-of-file identity re-check.
            if parsed.agent.is_some()
                && !mains.contains(&(descriptor.project_key.clone(), parsed.main_native.clone()))
            {
                return Err(AdapterError::CorruptSource {
                    stage: "claude_agent_parent".to_string(),
                });
            }
            let logical = LogicalTranscript {
                descriptor,
                main_native: parsed.main_native.clone(),
                logical_native: parsed.logical_native.clone(),
                agent: parsed.agent.clone(),
            };
            Ok(LoadedFile {
                logical,
                parsed,
                from_cache: true,
            })
        }
        CacheLookup::Miss(widened) => {
            let logical = resolve_logical(root, descriptor, mains, request, diagnostics)?;
            let extract = needed.union(widened).granted(request.access);
            let (parsed, complete) = parse_file(root, &logical, request, extract, diagnostics)?;
            let parsed = Arc::new(parsed);
            if complete {
                cache
                    .lock()
                    .map_err(|_| AdapterError::Internal {
                        stage: "claude_parse_cache".to_string(),
                    })?
                    .insert(key, Arc::clone(&parsed));
            }
            Ok(LoadedFile {
                logical,
                parsed,
                from_cache: false,
            })
        }
    }
}

/// Parses one transcript once, fanning every envelope out to the messages,
/// tool_calls, and usage builders plus the Safe session summary accumulator.
///
/// Sensitive values are extracted only for classes in `extract` (already
/// narrowed to the request grant); Safe aggregates are always computed. A
/// scan whose effective limit is reached mid-file stops early and reports
/// `complete = false` so the partial parse is never cached.
fn parse_file(
    root: &RootBinding,
    logical: &LogicalTranscript,
    request: &ScanRequest,
    extract: SensitiveClasses,
    diagnostics: &ScanDiagnostics,
) -> Result<(ParsedFile, bool), AdapterError> {
    let file = open_transcript(root, &logical.descriptor)?;
    let mut reader = BufReader::new(file.take(logical.descriptor.len));
    let mut line = Vec::new();
    let mut builders = ParseBuilders::default();
    let mut complete = true;
    while let Some((terminated, bytes)) = read_limited_line(
        &mut reader,
        &mut line,
        MAX_RECORD_BYTES,
        "claude_record_bytes",
        "claude_transcript_read",
    )? {
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
        request.budget.charge_bytes_read(bytes as u64)?;
        let Some(envelope) = parse_envelope(&line, terminated, diagnostics)? else {
            break;
        };
        validate_envelope_identity(&envelope, logical)?;
        builders.feed(&envelope, logical, request, extract, diagnostics)?;
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
        revalidate_transcript(root, &logical.descriptor, reader.get_ref().get_ref())?;
        for (_, pending) in std::mem::take(&mut builders.pending_tools) {
            builders
                .tool_calls
                .push(tool_record(request, pending, None, "interrupted", None));
        }
    }
    Ok((builders.finish(logical, extract), complete))
}

/// Per-transcript parse state shared by the three table builders and the
/// session summary accumulator.
#[derive(Default)]
struct ParseBuilders {
    sequence: i64,
    tool_sequence: i64,
    pending_tools: BTreeMap<String, PendingTool>,
    seen_entries: BTreeSet<String>,
    seen_usage: BTreeMap<String, UsageValues>,
    seen_assistant_messages: BTreeSet<String>,
    messages: Vec<MessageRecord>,
    tool_calls: Vec<ToolCallRecord>,
    usage: Vec<UsageRecord>,
    preview: Option<String>,
    cwd: Option<String>,
    model: Option<String>,
    created_at: Option<DateTime<Utc>>,
    message_count: i64,
    tool_call_count: i64,
    tokens_used: i64,
    has_tokens: bool,
}

/// Immutable context every builder stage of one envelope shares.
struct FeedContext<'a> {
    logical: &'a LogicalTranscript,
    request: &'a ScanRequest,
    extract: SensitiveClasses,
    diagnostics: &'a ScanDiagnostics,
}

impl ParseBuilders {
    fn feed(
        &mut self,
        envelope: &Envelope<'_>,
        logical: &LogicalTranscript,
        request: &ScanRequest,
        extract: SensitiveClasses,
        diagnostics: &ScanDiagnostics,
    ) -> Result<(), AdapterError> {
        let context = FeedContext {
            logical,
            request,
            extract,
            diagnostics,
        };
        let timestamp = parse_timestamp(envelope.timestamp)?;
        self.created_at = match (self.created_at, timestamp) {
            (Some(current), Some(candidate)) => Some(current.min(candidate)),
            (None, candidate) => candidate,
            (current, None) => current,
        };
        if extract.includes(SensitiveClasses::PATH)
            && let Some(raw) = envelope.cwd
        {
            self.cwd = Some(raw_string(raw, request.budget.max_single_value_bytes)?);
        }
        if envelope.kind == "last-prompt" {
            if extract.includes(SensitiveClasses::CONTENT)
                && let Some(raw) = envelope.last_prompt
            {
                self.preview = Some(raw_string(raw, request.budget.max_single_value_bytes)?);
            }
            return Ok(());
        }
        if !matches!(envelope.kind, "user" | "assistant") {
            if !known_event(envelope.kind) {
                warn_unknown(diagnostics)?;
            }
            return Ok(());
        }
        let uuid = envelope.uuid.ok_or_else(|| AdapterError::CorruptSource {
            stage: "claude_message_uuid".to_string(),
        })?;
        if !self.seen_entries.insert(uuid.to_string()) {
            return Err(AdapterError::CorruptSource {
                stage: "claude_duplicate_entry".to_string(),
            });
        }
        let message = parse_message_envelope(envelope)?;
        if message.role != envelope.kind {
            return Err(AdapterError::CorruptSource {
                stage: "claude_message_role".to_string(),
            });
        }
        let raw_content = message.content;
        let blocks = if raw_content.get().trim_start().starts_with('[') {
            content_blocks(raw_content)?
        } else {
            Vec::new()
        };
        if blocks
            .iter()
            .any(|block| !matches!(block.kind, "text" | "thinking" | "tool_use" | "tool_result"))
        {
            diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::UnknownField,
                source_kind: "claude_transcript".to_string(),
                stage: "unknown_content_block".to_string(),
            })?;
        }
        let unique = if envelope.kind == "assistant" {
            message.id.unwrap_or(uuid)
        } else {
            uuid
        };
        if envelope.kind != "assistant" || self.seen_assistant_messages.insert(unique.to_string()) {
            self.message_count =
                self.message_count
                    .checked_add(1)
                    .ok_or_else(|| AdapterError::CorruptSource {
                        stage: "claude_message_count".to_string(),
                    })?;
        }
        if let Some(model) = message.model {
            self.model = Some(model.to_string());
        }
        let tool_uses = blocks
            .iter()
            .filter(|block| block.kind == "tool_use")
            .count();
        self.tool_call_count = self
            .tool_call_count
            .checked_add(
                i64::try_from(tool_uses).map_err(|_| AdapterError::CorruptSource {
                    stage: "claude_tool_count".to_string(),
                })?,
            )
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "claude_tool_count".to_string(),
            })?;
        if envelope.kind == "assistant"
            && let Some(raw) = message.usage
        {
            self.feed_usage(envelope, &message, raw, timestamp, &context)?;
        }
        self.feed_message(envelope, &message, &blocks, uuid, timestamp, &context)?;
        self.feed_tools(envelope, &blocks, uuid, timestamp, &context)?;
        Ok(())
    }

    /// Feeds one assistant usage payload to the usage table builder and the
    /// summary token accumulator; both share one dedup/conflict check per
    /// API message ID.
    fn feed_usage(
        &mut self,
        envelope: &Envelope<'_>,
        message: &MessageEnvelope<'_>,
        raw: &RawValue,
        timestamp: Option<DateTime<Utc>>,
        context: &FeedContext<'_>,
    ) -> Result<(), AdapterError> {
        let Some(values) = usage_values(raw)? else {
            return Ok(());
        };
        let key = message.id.ok_or_else(|| AdapterError::CorruptSource {
            stage: "claude_usage_id".to_string(),
        })?;
        if let Some(previous) = self.seen_usage.get(key) {
            if previous != &values {
                return Err(AdapterError::CorruptSource {
                    stage: "claude_usage_conflict".to_string(),
                });
            }
            return Ok(());
        }
        self.seen_usage.insert(key.to_string(), values.clone());
        self.tokens_used = self.tokens_used.checked_add(values.total).ok_or_else(|| {
            AdapterError::CorruptSource {
                stage: "claude_usage_overflow".to_string(),
            }
        })?;
        self.has_tokens = true;
        let native = NativeId::new(format!("{}/usage/{key}", context.logical.logical_native));
        self.usage.push(UsageRecord {
            usage_id: EntityId::from_parts(
                "claude-code",
                &context.request.source.source_id,
                &native,
            ),
            source_id: context.request.source.source_id.clone(),
            agent_id: "claude-code".to_string(),
            session_id: Some(logical_session_id(context.request, context.logical)),
            model: message.model.map(str::to_string),
            provider: None,
            bucket_start: timestamp,
            input_tokens: values.input,
            output_tokens: values.output,
            cached_tokens: values.cached,
            total_tokens: Some(values.total),
            message_count: 0,
            tool_call_count: 0,
            error_count: i64::from(envelope.is_api_error_message == Some(true)),
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        });
        Ok(())
    }

    fn feed_message(
        &mut self,
        envelope: &Envelope<'_>,
        message: &MessageEnvelope<'_>,
        blocks: &[ContentBlock<'_>],
        uuid: &str,
        timestamp: Option<DateTime<Utc>>,
        context: &FeedContext<'_>,
    ) -> Result<(), AdapterError> {
        let wants_content = context.extract.includes(SensitiveClasses::CONTENT);
        let parsed = sanitized_message_content(
            message.content,
            blocks,
            wants_content,
            wants_content,
            context.request.budget.max_single_value_bytes,
        )?;
        self.sequence += 1;
        let native = NativeId::new(format!("{}/message/{uuid}", context.logical.logical_native));
        let role = if parsed.only_tool_results {
            "tool".to_string()
        } else {
            message.role.to_string()
        };
        self.messages.push(MessageRecord {
            message_id: EntityId::from_parts(
                "claude-code",
                &context.request.source.source_id,
                &native,
            ),
            session_id: logical_session_id(context.request, context.logical),
            source_id: context.request.source.source_id.clone(),
            sequence: self.sequence,
            role,
            kind: Some(parsed.kind),
            content: parsed.content,
            content_json: parsed.content_json,
            model: message.model.map(str::to_string),
            created_at: timestamp,
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            is_error: Some(envelope.is_api_error_message == Some(true) || parsed.tool_result_error),
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        });
        Ok(())
    }

    fn feed_tools(
        &mut self,
        envelope: &Envelope<'_>,
        blocks: &[ContentBlock<'_>],
        uuid: &str,
        timestamp: Option<DateTime<Utc>>,
        context: &FeedContext<'_>,
    ) -> Result<(), AdapterError> {
        if envelope.kind == "assistant" {
            for block in blocks.iter().filter(|block| block.kind == "tool_use") {
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
                let arguments = if context.extract.includes(SensitiveClasses::TOOL_INPUT) {
                    bounded_json(input, context.request.budget.max_single_value_bytes)?
                } else {
                    None
                };
                self.tool_sequence += 1;
                let message_native =
                    NativeId::new(format!("{}/message/{uuid}", context.logical.logical_native));
                let pending = PendingTool {
                    native_id: format!("{}/tool/{id}", context.logical.logical_native),
                    name: name.to_string(),
                    arguments,
                    session_id: logical_session_id(context.request, context.logical),
                    message_id: EntityId::from_parts(
                        "claude-code",
                        &context.request.source.source_id,
                        &message_native,
                    ),
                    sequence: self.tool_sequence,
                    started_at: timestamp,
                };
                if self.pending_tools.insert(id.to_string(), pending).is_some() {
                    return Err(AdapterError::CorruptSource {
                        stage: "claude_duplicate_tool".to_string(),
                    });
                }
            }
        } else {
            for block in blocks.iter().filter(|block| block.kind == "tool_result") {
                let id = block
                    .tool_use_id
                    .ok_or_else(|| AdapterError::CorruptSource {
                        stage: "claude_tool_result_id".to_string(),
                    })?;
                let Some(pending) = self.pending_tools.remove(id) else {
                    context.diagnostics.push(AdapterWarning {
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
                let output = if context.extract.includes(SensitiveClasses::TOOL_OUTPUT) {
                    Some(tool_output(
                        raw_output,
                        context.request.budget.max_single_value_bytes,
                    )?)
                } else {
                    None
                };
                let status = if is_error { "error" } else { "completed" };
                self.tool_calls.push(tool_record(
                    context.request,
                    pending,
                    output,
                    status,
                    timestamp,
                ));
            }
        }
        Ok(())
    }

    fn finish(self, logical: &LogicalTranscript, extract: SensitiveClasses) -> ParsedFile {
        ParsedFile {
            main_native: logical.main_native.clone(),
            logical_native: logical.logical_native.clone(),
            agent: logical.agent.clone(),
            messages: self.messages,
            tool_calls: self.tool_calls,
            usage: self.usage,
            summary: SessionSummary {
                preview: self.preview,
                cwd: self.cwd,
                model: self.model,
                created_at: self.created_at,
                message_count: Some(self.message_count),
                tool_call_count: Some(self.tool_call_count),
                tokens_used: self.has_tokens.then_some(self.tokens_used),
            },
            extracted: extract,
        }
    }
}

/// Clones the cached records of `table`, masking sensitive fields down to the
/// requesting projection (the cache stores class-wide extractions).
fn replay_records(
    parsed: &ParsedFile,
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
                let Some(descriptor) = self.descriptors.pop_front() else {
                    self.finished = true;
                    return None;
                };
                match load_parsed(
                    &self.root,
                    descriptor,
                    &self.mains,
                    &self.request,
                    &self.diagnostics,
                    &self.cache,
                ) {
                    Ok(loaded) => {
                        let records = replay_records(
                            &loaded.parsed,
                            self.request.table,
                            &self.request.projection,
                        );
                        self.current = Some(ReplayFile {
                            descriptor: loaded.logical.descriptor,
                            records,
                            recheck: loaded.from_cache,
                            since_check: 0,
                        });
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
                continue;
            }
            // The current file's records are drained; a cache-sourced parse
            // still owes the §9 end-of-scan identity re-check.
            let current = self.current.take()?;
            if current.recheck
                && let Err(error) = revalidate_transcript_path(&self.root, &current.descriptor)
            {
                self.finished = true;
                return Some(Err(error));
            }
        }
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
    let mut line = Vec::new();
    loop {
        let Some((complete, bytes)) = read_limited_line(
            &mut reader,
            &mut line,
            MAX_RECORD_BYTES,
            "claude_record_bytes",
            "claude_transcript_read",
        )?
        else {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_transcript_identity".to_string(),
            });
        };
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
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
}

/// Builds the sanitized message view from already validated `blocks` (empty
/// when `raw` is not a JSON array), so one envelope's content is parsed once
/// for every table builder.
fn sanitized_message_content(
    raw: &RawValue,
    blocks: &[ContentBlock<'_>],
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
        });
    }
    let only_tool_results =
        !blocks.is_empty() && blocks.iter().all(|block| block.kind == "tool_result");
    let tool_result_error = blocks
        .iter()
        .any(|block| block.kind == "tool_result" && block.is_error == Some(true));
    let has_text = blocks.iter().any(|block| block.kind == "text");
    let has_thinking = blocks.iter().any(|block| block.kind == "thinking");
    let has_tool_use = blocks.iter().any(|block| block.kind == "tool_use");
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
