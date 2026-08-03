//! Per-query bounded parse cache for the Codex adapter.
//!
//! One adapter instance serves exactly one query (the CLI rebinds sources per
//! query), so scans of the messages/tool_calls/artifacts tables used to
//! re-read and re-parse every rollout file once per table. This module caches
//! the single-pass parse outcome of each rollout in adapter memory for the
//! adapter lifetime only: nothing is persisted, nothing crosses queries, and
//! retained memory is capped by [`MAX_PARSE_CACHE_BYTES`] with a
//! fill-then-stop fallback to re-parsing.
//!
//! The cache key pins the scan-start watermark (file identity and pinned
//! length) plus the access grant, so appends, replacements, and grant changes
//! always miss and fall back to the fully validated read path. The locating
//! thread ID is part of the key because a rollout record's session identity
//! comes from the SQLite locator row, not from the file itself. Replay only
//! serves records whose sensitive classes were extracted under the same
//! grant, and masks sensitive fields down to the requesting projection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use aql_adapter_api::util::projected;
use aql_adapter_api::{AccessGrant, ColumnName, TableName};
use aql_model::{ArtifactRecord, MessageRecord, SourceId, ToolCallRecord};

use super::FileIdentity;

/// Maximum total estimated bytes retained by one adapter instance's cache.
pub(crate) const MAX_PARSE_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Shared handle through which every scan stream of one query reaches the
/// cache; the mutex is never held across I/O.
pub(crate) type ParseCacheHandle = Arc<Mutex<ParseCache>>;

/// Set of sensitive access classes whose values a parse pass extracted.
///
/// `ARTIFACTS` covers the patch-change structure (a path-class payload gated
/// on the path grant, mirroring the table-level grant check); replay masks
/// individual columns down to each projection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct SensitiveClasses(u8);

impl SensitiveClasses {
    pub(crate) const CONTENT: Self = Self(1);
    pub(crate) const TOOL_INPUT: Self = Self(2);
    pub(crate) const TOOL_OUTPUT: Self = Self(4);
    pub(crate) const ARTIFACTS: Self = Self(8);

    pub(crate) const fn empty() -> Self {
        Self(0)
    }

    const fn bits(self) -> u8 {
        self.0
    }

    /// Returns whether every class in `needed` is part of this set.
    pub(crate) const fn includes(self, needed: Self) -> bool {
        self.0 & needed.0 == needed.0
    }

    pub(crate) const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Narrows the set to classes the grant allows (defense in depth).
    pub(crate) fn granted(self, access: AccessGrant) -> Self {
        Self(self.0 & Self::from_grant(access).0)
    }

    fn from_grant(access: AccessGrant) -> Self {
        let mut classes = Self::empty();
        if access.content {
            classes = classes.union(Self::CONTENT);
        }
        if access.tool_input {
            classes = classes.union(Self::TOOL_INPUT);
        }
        if access.tool_output {
            classes = classes.union(Self::TOOL_OUTPUT);
        }
        if access.path {
            classes = classes.union(Self::ARTIFACTS);
        }
        classes
    }
}

/// Sensitive classes the scan of `table` under `projection` requires.
pub(crate) fn required_classes(table: TableName, projection: &[ColumnName]) -> SensitiveClasses {
    let mut needed = SensitiveClasses::empty();
    match table {
        TableName::Messages => {
            if projected(projection, "content") {
                needed = needed.union(SensitiveClasses::CONTENT);
            }
        }
        TableName::ToolCalls => {
            if projected(projection, "arguments") {
                needed = needed.union(SensitiveClasses::TOOL_INPUT);
            }
            if projected(projection, "output") {
                needed = needed.union(SensitiveClasses::TOOL_OUTPUT);
            }
        }
        TableName::Artifacts => {
            needed = needed.union(SensitiveClasses::ARTIFACTS);
            if projected(projection, "content") || projected(projection, "content_json") {
                needed = needed.union(SensitiveClasses::CONTENT);
            }
        }
        TableName::Sessions | TableName::Usage | TableName::SessionEdges => {}
    }
    needed
}

/// Cache key pinning one rollout file at its scan-start watermark.
///
/// `len` is part of the key so an appended rollout misses; `identity` catches
/// same-length replacement; `native_id` keeps two thread rows that locate the
/// same file apart (their records derive session identity from the row); the
/// grant bits keep differently authorized parses apart as defense in depth.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct FileCacheKey {
    source_id: SourceId,
    native_id: String,
    identity: FileIdentity,
    len: u64,
    access_bits: u8,
}

impl FileCacheKey {
    pub(crate) fn new(
        source_id: &SourceId,
        native_id: &str,
        identity: FileIdentity,
        len: u64,
        access: AccessGrant,
    ) -> Self {
        Self {
            source_id: source_id.clone(),
            native_id: native_id.to_string(),
            identity,
            len,
            access_bits: SensitiveClasses::from_grant(access).bits(),
        }
    }
}

/// One rollout parsed once, fanned out to every rollout-backed canonical
/// table, kept for the adapter (query) lifetime.
pub(crate) struct ParsedRolloutFile {
    pub(crate) messages: Vec<MessageRecord>,
    pub(crate) tool_calls: Vec<ToolCallRecord>,
    pub(crate) artifacts: Vec<ArtifactRecord>,
    pub(crate) extracted: SensitiveClasses,
}

/// Cache lookup outcome: a replayable parse, or a miss carrying the classes a
/// previous narrower parse already extracted (widening re-parse input).
pub(crate) enum CacheLookup {
    Hit(Arc<ParsedRolloutFile>),
    Miss(SensitiveClasses),
}

struct CacheEntry {
    parsed: Arc<ParsedRolloutFile>,
    retained_bytes: usize,
}

/// Insertion-ordered, bounded, query-lifetime parse cache.
///
/// Only parses that ran to the pinned length are inserted, so every stored
/// entry is complete by construction. Concurrent scans of one file may parse
/// it twice; the last insert wins and later lookups simply revalidate the
/// widened extraction set. Once the retained estimate reaches the limit, new
/// files stop being cached (fill-then-stop) and their scans fall back to the
/// previous re-parse behavior.
pub(crate) struct ParseCache {
    map: BTreeMap<FileCacheKey, CacheEntry>,
    retained_total: usize,
    limit: usize,
}

impl ParseCache {
    pub(crate) fn new() -> Self {
        Self::with_limit(MAX_PARSE_CACHE_BYTES)
    }

    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            retained_total: 0,
            limit,
        }
    }

    pub(crate) fn lookup(&self, key: &FileCacheKey, needed: SensitiveClasses) -> CacheLookup {
        match self.map.get(key) {
            Some(entry) if entry.parsed.extracted.includes(needed) => {
                CacheLookup::Hit(Arc::clone(&entry.parsed))
            }
            Some(entry) => CacheLookup::Miss(entry.parsed.extracted),
            None => CacheLookup::Miss(SensitiveClasses::empty()),
        }
    }

    pub(crate) fn insert(&mut self, key: FileCacheKey, parsed: Arc<ParsedRolloutFile>) {
        let retained_bytes = estimated_retained_bytes(&parsed);
        if let Some(previous) = self.map.remove(&key) {
            self.retained_total = self.retained_total.saturating_sub(previous.retained_bytes);
        }
        if retained_bytes > self.limit.saturating_sub(self.retained_total) {
            return;
        }
        self.retained_total = self.retained_total.saturating_add(retained_bytes);
        self.map.insert(
            key,
            CacheEntry {
                parsed,
                retained_bytes,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn cached_len(&self) -> usize {
        self.map.len()
    }
}

impl Default for ParseCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Conservative estimate of the heap memory one cached parse retains,
/// mirroring the engine's retained-session-bytes accounting idiom.
fn estimated_retained_bytes(parsed: &ParsedRolloutFile) -> usize {
    let mut bytes = std::mem::size_of::<ParsedRolloutFile>()
        .saturating_add(
            parsed
                .messages
                .capacity()
                .saturating_mul(std::mem::size_of::<MessageRecord>()),
        )
        .saturating_add(
            parsed
                .tool_calls
                .capacity()
                .saturating_mul(std::mem::size_of::<ToolCallRecord>()),
        )
        .saturating_add(
            parsed
                .artifacts
                .capacity()
                .saturating_mul(std::mem::size_of::<ArtifactRecord>()),
        );
    for record in &parsed.messages {
        bytes = bytes.saturating_add(message_retained_bytes(record));
    }
    for record in &parsed.tool_calls {
        bytes = bytes.saturating_add(tool_call_retained_bytes(record));
    }
    for record in &parsed.artifacts {
        bytes = bytes.saturating_add(artifact_retained_bytes(record));
    }
    bytes
}

fn message_retained_bytes(record: &MessageRecord) -> usize {
    let mut bytes = record
        .message_id
        .as_str()
        .len()
        .saturating_add(record.session_id.as_str().len())
        .saturating_add(record.source_id.as_str().len())
        .saturating_add(record.role.capacity());
    for value in [&record.kind, &record.content, &record.model]
        .into_iter()
        .flatten()
    {
        bytes = bytes.saturating_add(value.capacity());
    }
    if let Some(json) = &record.content_json {
        bytes = bytes.saturating_add(json_retained_bytes(json));
    }
    bytes
}

fn tool_call_retained_bytes(record: &ToolCallRecord) -> usize {
    let mut bytes = record
        .tool_call_id
        .as_str()
        .len()
        .saturating_add(record.session_id.as_str().len())
        .saturating_add(record.message_id.as_ref().map_or(0, |id| id.as_str().len()))
        .saturating_add(record.source_id.as_str().len())
        .saturating_add(record.tool_name.capacity());
    for value in [&record.namespace, &record.output].into_iter().flatten() {
        bytes = bytes.saturating_add(value.capacity());
    }
    if let Some(json) = &record.arguments {
        bytes = bytes.saturating_add(json_retained_bytes(json));
    }
    bytes
}

fn artifact_retained_bytes(record: &ArtifactRecord) -> usize {
    let mut bytes = record
        .artifact_id
        .as_str()
        .len()
        .saturating_add(record.source_id.as_str().len())
        .saturating_add(record.session_id.as_str().len())
        .saturating_add(
            record
                .tool_call_id
                .as_ref()
                .map_or(0, |id| id.as_str().len()),
        )
        .saturating_add(record.kind.capacity());
    for value in [
        &record.name,
        &record.path,
        &record.media_type,
        &record.content,
    ]
    .into_iter()
    .flatten()
    {
        bytes = bytes.saturating_add(value.capacity());
    }
    if let Some(json) = &record.content_json {
        bytes = bytes.saturating_add(json_retained_bytes(json));
    }
    bytes
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

#[cfg(test)]
mod tests {
    use super::*;

    struct TempFile(std::path::PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn empty_parsed(extracted: SensitiveClasses) -> Arc<ParsedRolloutFile> {
        Arc::new(ParsedRolloutFile {
            messages: Vec::new(),
            tool_calls: Vec::new(),
            artifacts: Vec::new(),
            extracted,
        })
    }

    fn identity() -> (TempFile, FileIdentity) {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time must be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "aql-codex-cache-{}-{nonce}.jsonl",
            std::process::id()
        ));
        std::fs::write(&path, b"{}\n").expect("fixture file written");
        let identity = aql_fs::file_identity(&path).expect("fixture identity");
        (TempFile(path), identity)
    }

    fn key(identity: FileIdentity, index: u64) -> FileCacheKey {
        FileCacheKey {
            source_id: SourceId::new("agent:source"),
            native_id: "thread".to_string(),
            identity,
            len: index,
            access_bits: 0,
        }
    }

    #[test]
    fn hit_requires_the_full_needed_class_set() {
        let (_file, identity) = identity();
        let mut cache = ParseCache::new();
        cache.insert(key(identity, 1), empty_parsed(SensitiveClasses::CONTENT));
        assert!(matches!(
            cache.lookup(&key(identity, 1), SensitiveClasses::empty()),
            CacheLookup::Hit(_)
        ));
        assert!(matches!(
            cache.lookup(&key(identity, 1), SensitiveClasses::CONTENT),
            CacheLookup::Hit(_)
        ));
        let lookup = cache.lookup(&key(identity, 1), SensitiveClasses::ARTIFACTS);
        assert!(
            matches!(lookup, CacheLookup::Miss(classes) if classes == SensitiveClasses::CONTENT)
        );
        let lookup = cache.lookup(&key(identity, 2), SensitiveClasses::CONTENT);
        assert!(
            matches!(lookup, CacheLookup::Miss(classes) if classes == SensitiveClasses::empty())
        );
    }

    #[test]
    fn insert_widens_and_fills_then_stops() {
        let (_file, identity) = identity();
        let mut cache = ParseCache::with_limit(8 * 1024);
        cache.insert(key(identity, 1), empty_parsed(SensitiveClasses::empty()));
        cache.insert(key(identity, 1), empty_parsed(SensitiveClasses::CONTENT));
        assert_eq!(cache.cached_len(), 1, "widening replaces in place");
        assert!(matches!(
            cache.lookup(&key(identity, 1), SensitiveClasses::CONTENT),
            CacheLookup::Hit(_)
        ));

        let mut inserted = 1_usize;
        for index in 2..10_000_u64 {
            cache.insert(
                key(identity, index),
                empty_parsed(SensitiveClasses::empty()),
            );
            if matches!(
                cache.lookup(&key(identity, index), SensitiveClasses::empty()),
                CacheLookup::Miss(_)
            ) {
                break;
            }
            inserted += 1;
        }
        assert!(
            inserted < 9_999,
            "the bounded cache stopped accepting new files"
        );
        assert!(
            matches!(
                cache.lookup(&key(identity, 1), SensitiveClasses::CONTENT),
                CacheLookup::Hit(_),
            ),
            "earlier entries stay cached after the cap closes"
        );
    }

    #[test]
    fn zero_limit_caches_nothing() {
        let (_file, identity) = identity();
        let mut cache = ParseCache::with_limit(0);
        cache.insert(key(identity, 1), empty_parsed(SensitiveClasses::empty()));
        assert!(matches!(
            cache.lookup(&key(identity, 1), SensitiveClasses::empty()),
            CacheLookup::Miss(_)
        ));
    }

    #[test]
    fn full_cache_falls_back_to_reparsing() {
        use aql_adapter_api::{
            AccessGrant, AgentAdapter, CancellationToken, ProbeRequest, ResourceBudget, ScanRequest,
        };

        use crate::CodexAdapter;

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time must be after epoch")
            .as_nanos();
        let fixtures = std::env::temp_dir().join(format!(
            "aql-codex-cache-fixtures-{}-{nonce}",
            std::process::id()
        ));
        aql_test_support::generate_codex(&fixtures, 0).expect("fixture generator succeeds");
        let root = fixtures.join("minimal");
        let rollout = root.join("sessions/2026/01/01/rollout-minimal.jsonl");
        let rollout_len = std::fs::metadata(&rollout).expect("rollout metadata").len();

        let run = |adapter: &CodexAdapter| -> u64 {
            let source = adapter
                .probe(&ProbeRequest {
                    data_root: root.to_string_lossy().into_owned(),
                })
                .expect("fixture probes")
                .manifests
                .into_iter()
                .next()
                .expect("manifest exists");
            let budget = ResourceBudget::default();
            for (table, projection) in [
                (TableName::Messages, &["message_id", "role"][..]),
                (
                    TableName::ToolCalls,
                    &["tool_call_id", "tool_name", "status"][..],
                ),
            ] {
                let request = ScanRequest {
                    source: source.clone(),
                    table,
                    projection: projection
                        .iter()
                        .map(|name| ColumnName::new(*name))
                        .collect(),
                    predicates: Vec::new(),
                    limit: None,
                    order_hint: Vec::new(),
                    access: AccessGrant::default(),
                    budget: budget.clone(),
                    cancellation: CancellationToken::default(),
                    snapshot: None,
                };
                let count = adapter
                    .scan(request)
                    .expect("scan starts")
                    .records
                    .collect::<Result<Vec<_>, _>>()
                    .expect("scan succeeds")
                    .len();
                assert!(count > 0);
            }
            budget.bytes_read_used()
        };

        let cached = CodexAdapter::new(b"fixture-salt".to_vec());
        assert_eq!(
            run(&cached),
            rollout_len,
            "one parse serves every rollout table of the query"
        );
        let uncached = CodexAdapter::new_with_parse_cache_limit(b"fixture-salt".to_vec(), 1);
        assert_eq!(
            run(&uncached),
            2 * rollout_len,
            "a full cache falls back to per-table re-parsing"
        );
        std::fs::remove_dir_all(fixtures).expect("fixtures removed");
    }
}
