//! Behavioral tests for the per-query single-pass parse cache: one parse per
//! rollout file per adapter (query) lifetime, byte-identical replays, one
//! warning per parse, and fail-closed cache invalidation.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aql_adapter_api::{
    AccessGrant, AdapterError, AdapterWarningKind, AgentAdapter, CancellationToken, ColumnName,
    ProbeRequest, ResourceBudget, ScanRequest, TableName,
};
use aql_adapter_codex::CodexAdapter;
use aql_model::{CanonicalRecord, SourceManifest};
use chrono::{DateTime, Utc};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixture_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after epoch")
        .as_nanos();
    let output = std::env::temp_dir().join(format!(
        "aql-codex-cache-fixtures-{}-{nonce}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    aql_test_support::generate_codex(&output, 0).expect("fixture generator must succeed");
    output
}

fn manifest(adapter: &CodexAdapter, root: &Path) -> SourceManifest {
    adapter
        .probe(&ProbeRequest {
            data_root: root.to_string_lossy().into_owned(),
        })
        .expect("fixture probe must succeed")
        .manifests
        .into_iter()
        .next()
        .expect("probe must return a manifest")
}

fn request(source: SourceManifest, table: TableName, projection: &[&str]) -> ScanRequest {
    ScanRequest {
        source,
        table,
        projection: projection
            .iter()
            .map(|name| ColumnName::new(*name))
            .collect(),
        predicates: Vec::new(),
        limit: None,
        order_hint: Vec::new(),
        access: AccessGrant::default(),
        budget: ResourceBudget::default(),
        cancellation: CancellationToken::default(),
        snapshot: None,
    }
}

fn collect(adapter: &CodexAdapter, request: ScanRequest) -> Vec<CanonicalRecord> {
    adapter
        .scan(request)
        .expect("scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("scan succeeds")
}

/// Locates the rollout file of the single-session fixtures.
fn rollout_path(root: &Path, file: &str) -> PathBuf {
    root.join("sessions/2026/01/01").join(file)
}

/// Provenance carries wall-clock `observed_at` timestamps, so cross-run
/// comparisons pin them to the epoch; every other field is compared exactly.
fn normalized(records: &[CanonicalRecord]) -> Vec<CanonicalRecord> {
    let epoch = DateTime::<Utc>::from_timestamp(0, 0).expect("epoch exists");
    records
        .iter()
        .cloned()
        .map(|mut record| {
            let provenance = match &mut record {
                CanonicalRecord::Session(value) => &mut value.provenance,
                CanonicalRecord::Message(value) => &mut value.provenance,
                CanonicalRecord::ToolCall(value) => &mut value.provenance,
                CanonicalRecord::Usage(value) => &mut value.provenance,
                CanonicalRecord::SessionEdge(value) => &mut value.provenance,
                CanonicalRecord::Artifact(value) => &mut value.provenance,
            };
            for entries in provenance.values_mut() {
                for entry in entries {
                    entry.observed_at = epoch;
                }
            }
            record
        })
        .collect()
}

fn scan_messages(adapter: &CodexAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::Messages,
        &["message_id", "role", "kind", "content"],
    );
    scan.access.content = true;
    collect(adapter, scan)
}

fn scan_tool_calls(adapter: &CodexAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "arguments", "output", "status"],
    );
    scan.access.tool_input = true;
    scan.access.tool_output = true;
    collect(adapter, scan)
}

fn scan_artifacts(adapter: &CodexAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::Artifacts,
        &["artifact_id", "kind", "path", "content", "content_json"],
    );
    scan.access.path = true;
    scan.access.content = true;
    collect(adapter, scan)
}

#[test]
fn one_parse_charges_source_bytes_once_per_query() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let rollout_len = fs::metadata(rollout_path(&root, "rollout-minimal.jsonl"))
        .expect("rollout metadata")
        .len();

    // One adapter (one query): the first table parses, the second replays.
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();
    let mut messages = request(source.clone(), TableName::Messages, &["message_id", "role"]);
    messages.budget = budget.clone();
    assert_eq!(collect(&adapter, messages).len(), 2);
    let mut tools = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "status"],
    );
    tools.budget = budget.clone();
    assert_eq!(collect(&adapter, tools).len(), 1);
    assert_eq!(
        budget.bytes_read_used(),
        rollout_len,
        "two table scans charge one parse worth of rollout bytes"
    );

    // The artifacts table needs the path-granted ARTIFACTS class, which a
    // safe projection never extracted: exactly one widening re-parse.
    let mut artifacts = request(source.clone(), TableName::Artifacts, &["artifact_id"]);
    artifacts.access.path = true;
    artifacts.budget = budget.clone();
    assert!(collect(&adapter, artifacts).is_empty());
    assert_eq!(
        budget.bytes_read_used(),
        2 * rollout_len,
        "the artifacts class widens with a single re-parse"
    );

    // Baseline: a fresh adapter per table re-parses every time.
    let baseline = ResourceBudget::default();
    for table in [TableName::Messages, TableName::ToolCalls] {
        let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        let mut scan = request(source, table, &["message_id"]);
        scan.budget = baseline.clone();
        collect(&adapter, scan);
    }
    assert_eq!(baseline.bytes_read_used(), 2 * rollout_len);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn cache_replay_is_byte_identical_to_uncached_scans() {
    let fixtures = fixture_root();
    let root = fixtures.join("artifacts");

    // Uncached baseline: every table scanned through a fresh adapter.
    let scan_fresh = |run: fn(&CodexAdapter, &SourceManifest) -> Vec<CanonicalRecord>| {
        let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        run(&adapter, &source)
    };
    let baseline = (
        scan_fresh(scan_messages),
        scan_fresh(scan_tool_calls),
        scan_fresh(scan_artifacts),
    );

    // Cached run: one adapter scans every table sequentially, then replays
    // messages once more from the fully widened entry.
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let cached = (
        scan_messages(&adapter, &source),
        scan_tool_calls(&adapter, &source),
        scan_artifacts(&adapter, &source),
        scan_messages(&adapter, &source),
    );

    assert_eq!(
        normalized(&baseline.0),
        normalized(&cached.0),
        "messages replay matches"
    );
    assert_eq!(
        normalized(&baseline.1),
        normalized(&cached.1),
        "tool_calls replay matches"
    );
    assert_eq!(
        normalized(&baseline.2),
        normalized(&cached.2),
        "artifacts replay matches"
    );
    assert_eq!(
        normalized(&baseline.0),
        normalized(&cached.3),
        "the widened entry replays messages matches"
    );
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn parse_warnings_are_emitted_once_per_query() {
    let fixtures = fixture_root();
    let root = fixtures.join("unknown-event");
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);

    let mut total = 0;
    for (table, projection, expected) in [
        (TableName::Messages, &["message_id"][..], 2_usize),
        (TableName::ToolCalls, &["tool_call_id"][..], 1_usize),
    ] {
        let result = adapter
            .scan(request(source.clone(), table, projection))
            .expect("scan starts");
        let diagnostics = result.diagnostics.clone();
        let records = result
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect("scan succeeds");
        assert_eq!(records.len(), expected, "{table:?} produced records");
        total += diagnostics
            .snapshot()
            .expect("diagnostics")
            .iter()
            .filter(|warning| warning.kind == AdapterWarningKind::UnknownEvent)
            .count();
    }
    assert_eq!(
        total, 1,
        "the unknown fixture event warns once per query, not once per table"
    );
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn limit_early_stop_is_never_cached() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let rollout_len = fs::metadata(rollout_path(&root, "rollout-minimal.jsonl"))
        .expect("rollout metadata")
        .len();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();

    let mut limited = request(source.clone(), TableName::Messages, &["message_id"]);
    limited.limit = Some(1);
    limited.budget = budget.clone();
    assert_eq!(collect(&adapter, limited).len(), 1, "the limit truncates");
    let partial = budget.bytes_read_used();
    assert!(
        partial < rollout_len,
        "a limited scan stops before the pinned end"
    );

    // The partial parse must not serve the next table: a full parse re-reads
    // the rollout and charges its length again.
    let mut tools = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "status"],
    );
    tools.budget = budget.clone();
    assert_eq!(collect(&adapter, tools).len(), 1);
    assert_eq!(budget.bytes_read_used(), partial + rollout_len);

    // That full parse is cached, so the messages table replays it for free.
    let mut messages = request(source, TableName::Messages, &["message_id"]);
    messages.budget = budget.clone();
    assert_eq!(collect(&adapter, messages).len(), 2);
    assert_eq!(budget.bytes_read_used(), partial + rollout_len);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn growth_or_replacement_invalidates_the_cached_parse() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let rollout = rollout_path(&root, "rollout-minimal.jsonl");

    // Append growth changes the pinned length: the next table re-parses
    // through the full validation chain and re-warns.
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();
    let mut messages = request(source.clone(), TableName::Messages, &["message_id"]);
    messages.budget = budget.clone();
    assert_eq!(collect(&adapter, messages).len(), 2);
    let rollout_len = fs::metadata(&rollout).expect("rollout metadata").len();
    assert_eq!(budget.bytes_read_used(), rollout_len);

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&rollout)
        .expect("append fixture rollout");
    file.write_all(b"{\"timestamp\":\"2026-01-01T00:00:05Z\",\"type\":\"future_fixture_event\",\"payload\":{}}\n")
        .expect("append succeeds");
    drop(file);
    let grown_len = fs::metadata(&rollout).expect("rollout metadata").len();

    let mut tools = request(source.clone(), TableName::ToolCalls, &["tool_call_id"]);
    tools.budget = budget.clone();
    let result = adapter.scan(tools).expect("re-parse scan starts");
    let diagnostics = result.diagnostics.clone();
    assert_eq!(
        result
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect("re-parse succeeds")
            .len(),
        1
    );
    assert_eq!(
        budget.bytes_read_used(),
        rollout_len + grown_len,
        "growth misses the cache and re-parses the appended rollout"
    );
    assert_eq!(
        diagnostics
            .snapshot()
            .expect("diagnostics")
            .iter()
            .filter(|warning| warning.kind == AdapterWarningKind::UnknownEvent)
            .count(),
        1,
        "the re-parse reproduces the warning"
    );

    // The grown parse is cached, so the messages table replays it for free.
    let mut replay = request(source, TableName::Messages, &["message_id"]);
    replay.budget = budget.clone();
    assert_eq!(collect(&adapter, replay).len(), 2);
    assert_eq!(budget.bytes_read_used(), rollout_len + grown_len);

    // Replacement after a cached parse is caught by the replay-side §9
    // end-of-scan identity re-check.
    let replacement = fixtures.join("rollout-replacement.jsonl");
    fs::copy(&rollout, &replacement).expect("replacement copy prepared");
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    assert_eq!(
        collect(
            &adapter,
            request(source.clone(), TableName::Messages, &["message_id"])
        )
        .len(),
        2
    );

    let mut replay = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("replay scan starts")
        .records;
    assert!(
        replay.next().is_some_and(|record| record.is_ok()),
        "cached records replay before the end-of-scan check"
    );
    fs::remove_file(&rollout).expect("rollout displaced");
    fs::rename(&replacement, &rollout).expect("replacement installed");
    assert!(
        replay.next().is_some_and(|record| record.is_ok()),
        "the second cached record still replays"
    );
    assert_eq!(
        replay.next(),
        Some(Err(AdapterError::SnapshotUnavailable)),
        "the replay identity re-check fails closed"
    );
    assert!(replay.next().is_none());
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn concurrent_scans_of_one_file_stay_correct() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");

    let baseline_adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let baseline_source = manifest(&baseline_adapter, &root);
    let baseline_messages = collect(
        &baseline_adapter,
        request(
            baseline_source.clone(),
            TableName::Messages,
            &["message_id", "role"],
        ),
    );
    let baseline_tools = collect(
        &baseline_adapter,
        request(
            baseline_source,
            TableName::ToolCalls,
            &["tool_call_id", "status"],
        ),
    );

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let shared = &adapter;
    std::thread::scope(|scope| {
        let first_source = source.clone();
        let first = scope.spawn(move || {
            collect(
                shared,
                request(first_source, TableName::Messages, &["message_id", "role"]),
            )
        });
        let second_source = source.clone();
        let second = scope.spawn(move || {
            collect(
                shared,
                request(
                    second_source,
                    TableName::ToolCalls,
                    &["tool_call_id", "status"],
                ),
            )
        });
        let messages = first.join().expect("messages thread joins");
        let tools = second.join().expect("tool_calls thread joins");
        assert_eq!(normalized(&messages), normalized(&baseline_messages));
        assert_eq!(normalized(&tools), normalized(&baseline_tools));
    });
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}
