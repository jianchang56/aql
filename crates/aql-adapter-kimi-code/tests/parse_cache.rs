//! Behavioral tests for the per-query single-pass parse cache: one parse per
//! wire file per adapter (query) lifetime, byte-identical replays, one
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
use aql_adapter_kimi_code::KimiCodeAdapter;
use aql_model::{CanonicalRecord, SourceManifest};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixtures() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("valid system time")
        .as_nanos();
    let output = std::env::temp_dir().join(format!(
        "aql-kimi-cache-fixtures-{}-{nonce}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    aql_test_support::generate_kimi(&output).expect("fixture generator succeeds");
    output
}

fn manifest(adapter: &KimiCodeAdapter, root: &Path) -> SourceManifest {
    adapter
        .probe(&ProbeRequest {
            data_root: root.to_string_lossy().into_owned(),
        })
        .expect("fixture probes")
        .manifests
        .into_iter()
        .next()
        .expect("manifest exists")
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

fn collect(adapter: &KimiCodeAdapter, request: ScanRequest) -> Vec<CanonicalRecord> {
    adapter
        .scan(request)
        .expect("scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("scan succeeds")
}

/// Locates the main agent wire file of the single-session fixtures.
fn wire_path(root: &Path, session: &str) -> PathBuf {
    root.join(format!(
        "sessions/wd_workspace_84b61923346a/{session}/agents/main/wire.jsonl"
    ))
}

fn unknown_event_warnings(
    adapter: &KimiCodeAdapter,
    request: ScanRequest,
) -> (Vec<CanonicalRecord>, usize) {
    let result = adapter.scan(request).expect("scan starts");
    let diagnostics = result.diagnostics.clone();
    let records = result
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("scan succeeds");
    let count = diagnostics
        .snapshot()
        .expect("diagnostics")
        .iter()
        .filter(|warning| warning.kind == AdapterWarningKind::UnknownEvent)
        .count();
    (records, count)
}

fn scan_messages(adapter: &KimiCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::Messages,
        &["message_id", "role", "kind", "content", "content_json"],
    );
    scan.access.content = true;
    collect(adapter, scan)
}

fn scan_tool_calls(adapter: &KimiCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "arguments", "output", "status"],
    );
    scan.access.tool_input = true;
    scan.access.tool_output = true;
    collect(adapter, scan)
}

fn scan_usage(adapter: &KimiCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    collect(
        adapter,
        request(
            source.clone(),
            TableName::Usage,
            &[
                "usage_id",
                "input_tokens",
                "output_tokens",
                "cached_tokens",
                "total_tokens",
            ],
        ),
    )
}

#[test]
fn one_parse_charges_source_bytes_once_per_query() {
    let fixtures = fixtures();
    let root = fixtures.join("full");
    let wire_len = fs::metadata(wire_path(&root, "session-full"))
        .expect("wire metadata")
        .len();
    let tables = [
        (TableName::Messages, &["message_id", "role"][..]),
        (
            TableName::ToolCalls,
            &["tool_call_id", "tool_name", "status"][..],
        ),
        (
            TableName::Usage,
            &["usage_id", "input_tokens", "output_tokens", "total_tokens"][..],
        ),
    ];

    // One adapter (one query): the first table parses, the rest replay. The
    // session listing and state.json reads are per-scan inventory and stay
    // charged on every scan; only the wire parse collapses to once.
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();
    for (table, projection) in &tables {
        let mut scan = request(source.clone(), *table, projection);
        scan.budget = budget.clone();
        assert!(
            !collect(&adapter, scan).is_empty(),
            "{table:?} produced records"
        );
    }
    let cached_total = budget.bytes_read_used();

    // Baseline: a fresh adapter per table re-parses the wire file every time.
    let baseline = ResourceBudget::default();
    for (table, projection) in &tables {
        let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        let mut scan = request(source, *table, projection);
        scan.budget = baseline.clone();
        collect(&adapter, scan);
    }
    assert_eq!(
        baseline.bytes_read_used(),
        cached_total + 2 * wire_len,
        "three table scans charge one parse worth of wire bytes"
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn cache_replay_is_byte_identical_to_uncached_scans() {
    let fixtures = fixtures();
    let root = fixtures.join("full");

    // Uncached baseline: every table scanned through a fresh adapter.
    let scan_fresh = |run: fn(&KimiCodeAdapter, &SourceManifest) -> Vec<CanonicalRecord>| {
        let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        run(&adapter, &source)
    };
    let baseline = (
        scan_fresh(scan_messages),
        scan_fresh(scan_tool_calls),
        scan_fresh(scan_usage),
    );

    // Cached run: one adapter scans every table sequentially.
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let cached = (
        scan_messages(&adapter, &source),
        scan_tool_calls(&adapter, &source),
        scan_usage(&adapter, &source),
    );

    assert_eq!(baseline.0, cached.0, "messages replay matches");
    assert_eq!(baseline.1, cached.1, "tool_calls replay matches");
    assert_eq!(baseline.2, cached.2, "usage replay matches");
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn parse_warnings_are_emitted_once_per_query() {
    let fixtures = fixtures();
    let root = fixtures.join("unknown-record");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);

    let mut total = 0;
    for (table, projection) in [
        (TableName::Messages, &["message_id"][..]),
        (TableName::ToolCalls, &["tool_call_id"][..]),
        (TableName::Usage, &["usage_id"][..]),
    ] {
        let (records, warnings) =
            unknown_event_warnings(&adapter, request(source.clone(), table, projection));
        assert!(
            records.is_empty(),
            "{table:?} has no records in this fixture"
        );
        total += warnings;
    }
    assert_eq!(
        total, 1,
        "the unknown fixture record warns once per query, not once per table"
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn limit_early_stop_is_never_cached() {
    let fixtures = fixtures();
    let root = fixtures.join("full");
    let wire_len = fs::metadata(wire_path(&root, "session-full"))
        .expect("wire metadata")
        .len();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();

    let mut limited = request(source.clone(), TableName::Messages, &["message_id"]);
    limited.limit = Some(1);
    limited.budget = budget.clone();
    assert_eq!(collect(&adapter, limited).len(), 1, "the limit truncates");
    let partial = budget.bytes_read_used();

    // The partial parse must not serve the next table: a full parse re-reads
    // the wire file and charges its length again.
    let mut tools = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "status"],
    );
    tools.budget = budget.clone();
    assert_eq!(collect(&adapter, tools).len(), 1);
    let reparsed = budget.bytes_read_used();

    // That full parse is cached, so the usage table replays it for free; its
    // delta is the per-scan inventory overhead (listing + state.json) only.
    let mut usage = request(source, TableName::Usage, &["usage_id", "total_tokens"]);
    usage.budget = budget.clone();
    assert_eq!(collect(&adapter, usage).len(), 1);
    let replayed = budget.bytes_read_used();

    let overhead = replayed - reparsed;
    assert_eq!(
        reparsed - partial - overhead,
        wire_len,
        "the partial parse is never cached: the next table re-parses the whole wire file"
    );
    let partial_wire = partial - overhead;
    assert!(
        partial_wire < wire_len,
        "a limited scan stops before the pinned end"
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn growth_or_replacement_invalidates_the_cached_parse() {
    let fixtures = fixtures();

    // Append growth changes the pinned length: the next table re-parses
    // through the full validation chain and re-warns.
    let root = fixtures.join("active-boundary");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();
    let mut messages = request(source.clone(), TableName::Messages, &["message_id"]);
    messages.budget = budget.clone();
    assert_eq!(collect(&adapter, messages).len(), 1);
    let first = budget.bytes_read_used();

    let wire = wire_path(&root, "session-active-boundary");
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&wire)
        .expect("append fixture wire");
    file.write_all(b"{\"type\":\"future-after-boundary\"}\n")
        .expect("append succeeds");
    drop(file);
    let grown_len = fs::metadata(&wire).expect("wire metadata").len();

    let mut usage = request(source.clone(), TableName::Usage, &["usage_id"]);
    usage.budget = budget.clone();
    let (records, warnings) = unknown_event_warnings(&adapter, usage);
    assert!(records.is_empty(), "the fixture has no usage records");
    assert_eq!(warnings, 1, "the re-parse reproduces the warning");
    let reparsed = budget.bytes_read_used();

    // The grown parse is cached; the tool_calls replay isolates the per-scan
    // inventory overhead so the re-parse delta matches the grown length.
    let mut tools = request(source, TableName::ToolCalls, &["tool_call_id"]);
    tools.budget = budget.clone();
    assert!(collect(&adapter, tools).is_empty());
    let replayed = budget.bytes_read_used();
    assert_eq!(
        reparsed - first - (replayed - reparsed),
        grown_len,
        "growth misses the cache and re-parses the appended wire file"
    );

    // Replacement after a cached parse is caught by the replay-side §9
    // end-of-scan identity re-check.
    let root = fixtures.join("full");
    let wire = wire_path(&root, "session-full");
    let replacement = fixtures.join("wire-replacement.jsonl");
    fs::copy(&wire, &replacement).expect("replacement copy prepared");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    assert_eq!(
        collect(
            &adapter,
            request(source.clone(), TableName::Messages, &["message_id"])
        )
        .len(),
        3
    );

    let mut replay = adapter
        .scan(request(source, TableName::Usage, &["usage_id"]))
        .expect("replay scan starts")
        .records;
    assert!(
        replay.next().is_some_and(|record| record.is_ok()),
        "cached records replay before the end-of-scan check"
    );
    fs::remove_file(&wire).expect("wire displaced");
    fs::rename(&replacement, &wire).expect("replacement installed");
    assert_eq!(
        replay.next(),
        Some(Err(AdapterError::SnapshotUnavailable)),
        "the replay identity re-check fails closed"
    );
    assert!(replay.next().is_none());
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn concurrent_scans_of_one_file_stay_correct() {
    let fixtures = fixtures();
    let root = fixtures.join("full");

    let baseline_adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let baseline_source = manifest(&baseline_adapter, &root);
    let baseline_messages = collect(
        &baseline_adapter,
        request(
            baseline_source.clone(),
            TableName::Messages,
            &["message_id", "role"],
        ),
    );
    let baseline_usage = collect(
        &baseline_adapter,
        request(
            baseline_source,
            TableName::Usage,
            &["usage_id", "total_tokens"],
        ),
    );

    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
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
                    TableName::Usage,
                    &["usage_id", "total_tokens"],
                ),
            )
        });
        let messages = first.join().expect("messages thread joins");
        let usage = second.join().expect("usage thread joins");
        assert_eq!(messages, baseline_messages);
        assert_eq!(usage, baseline_usage);
    });
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}
