//! Behavioral tests for the per-query single-pass parse cache: one parse per
//! transcript per adapter (query) lifetime, byte-identical replays, one
//! warning per parse, and fail-closed cache invalidation.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use aql_adapter_api::{
    AccessGrant, AdapterError, AdapterWarningKind, AgentAdapter, CancellationToken, ColumnName,
    ProbeRequest, ResourceBudget, ScanRequest, TableName,
};
use aql_adapter_claude_code::ClaudeCodeAdapter;
use aql_model::{CanonicalRecord, SourceManifest};

fn fixtures() -> tempfile::TempDir {
    let output = tempfile::tempdir().expect("temporary fixture directory");
    aql_test_support::generate_claude(output.path()).expect("fixture generator succeeds");
    output
}

fn manifest(adapter: &ClaudeCodeAdapter, root: &Path) -> SourceManifest {
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

fn collect(adapter: &ClaudeCodeAdapter, request: ScanRequest) -> Vec<CanonicalRecord> {
    adapter
        .scan(request)
        .expect("scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("scan succeeds")
}

fn find_main(root: &Path) -> PathBuf {
    fs::read_dir(root.join("projects/synthetic-project"))
        .expect("project exists")
        .map(|entry| entry.expect("entry").path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| !name.starts_with("agent-") && name.ends_with(".jsonl"))
        })
        .expect("main transcript exists")
}

fn unknown_event_warnings(
    adapter: &ClaudeCodeAdapter,
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

fn scan_messages(adapter: &ClaudeCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::Messages,
        &["message_id", "role", "kind", "content", "content_json"],
    );
    scan.access.content = true;
    collect(adapter, scan)
}

fn scan_tool_calls(adapter: &ClaudeCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "arguments", "output", "status"],
    );
    scan.access.tool_input = true;
    scan.access.tool_output = true;
    collect(adapter, scan)
}

fn scan_usage(adapter: &ClaudeCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    collect(
        adapter,
        request(
            source.clone(),
            TableName::Usage,
            &["usage_id", "input_tokens", "output_tokens", "total_tokens"],
        ),
    )
}

fn scan_sessions(adapter: &ClaudeCodeAdapter, source: &SourceManifest) -> Vec<CanonicalRecord> {
    let mut scan = request(
        source.clone(),
        TableName::Sessions,
        &[
            "session_id",
            "preview",
            "cwd",
            "model",
            "created_at",
            "message_count",
            "tool_call_count",
            "tokens_used",
        ],
    );
    scan.access.content = true;
    scan.access.path = true;
    collect(adapter, scan)
}

#[test]
fn one_parse_charges_source_bytes_once_per_query() {
    let fixtures = fixtures();
    let root = fixtures.path().join("full");
    let file_len = fs::metadata(find_main(&root))
        .expect("transcript metadata")
        .len();
    let tables = [
        (TableName::Messages, &["message_id", "role", "kind"][..]),
        (
            TableName::ToolCalls,
            &["tool_call_id", "tool_name", "status"][..],
        ),
        (
            TableName::Usage,
            &["usage_id", "input_tokens", "output_tokens", "total_tokens"][..],
        ),
        (
            TableName::Sessions,
            &[
                "session_id",
                "message_count",
                "tool_call_count",
                "tokens_used",
            ][..],
        ),
    ];

    // One adapter (one query): the first table parses, the rest replay.
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
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
    assert_eq!(
        budget.bytes_read_used(),
        file_len,
        "four table scans charge one parse worth of source bytes"
    );

    // Baseline: a fresh adapter per table re-parses every time.
    let baseline = ResourceBudget::default();
    for (table, projection) in &tables {
        let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        let mut scan = request(source, *table, projection);
        scan.budget = baseline.clone();
        collect(&adapter, scan);
    }
    assert_eq!(baseline.bytes_read_used(), 4 * file_len);
}

#[test]
fn cache_replay_is_byte_identical_to_uncached_scans() {
    let fixtures = fixtures();
    let root = fixtures.path().join("full");

    // Uncached baseline: every table scanned through a fresh adapter.
    let scan_fresh = |run: fn(&ClaudeCodeAdapter, &SourceManifest) -> Vec<CanonicalRecord>| {
        let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        run(&adapter, &source)
    };
    let baseline = (
        scan_fresh(scan_messages),
        scan_fresh(scan_tool_calls),
        scan_fresh(scan_usage),
        scan_fresh(scan_sessions),
    );

    // Cached run: one adapter scans every table sequentially.
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let cached = (
        scan_messages(&adapter, &source),
        scan_tool_calls(&adapter, &source),
        scan_usage(&adapter, &source),
        scan_sessions(&adapter, &source),
    );

    assert_eq!(baseline.0, cached.0, "messages replay matches");
    assert_eq!(baseline.1, cached.1, "tool_calls replay matches");
    assert_eq!(baseline.2, cached.2, "usage replay matches");
    assert_eq!(baseline.3, cached.3, "sessions replay matches");
}

#[test]
fn parse_warnings_are_emitted_once_per_query() {
    let fixtures = fixtures();
    let root = fixtures.path().join("full");
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);

    let mut total = 0;
    for table in [TableName::Messages, TableName::ToolCalls, TableName::Usage] {
        let (records, warnings) =
            unknown_event_warnings(&adapter, request(source.clone(), table, &["native_id"]));
        assert!(!records.is_empty(), "{table:?} produced records");
        total += warnings;
    }
    assert_eq!(
        total, 1,
        "the unknown fixture event warns once per query, not once per table"
    );
}

#[test]
fn limit_early_stop_is_never_cached() {
    let fixtures = fixtures();
    let root = fixtures.path().join("full");
    let file_len = fs::metadata(find_main(&root))
        .expect("transcript metadata")
        .len();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();

    let mut limited = request(source.clone(), TableName::Messages, &["message_id"]);
    limited.limit = Some(1);
    limited.budget = budget.clone();
    assert_eq!(collect(&adapter, limited).len(), 1, "the limit truncates");
    let partial = budget.bytes_read_used();
    assert!(
        partial < file_len,
        "a limited scan stops before the pinned end"
    );

    // The partial parse must not serve the next table: a full parse re-reads
    // the transcript and charges its length again.
    let mut tools = request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "status"],
    );
    tools.budget = budget.clone();
    assert_eq!(collect(&adapter, tools).len(), 1);
    assert_eq!(budget.bytes_read_used(), partial + file_len);

    // That full parse is cached, so the usage table replays it for free.
    let mut usage = request(source, TableName::Usage, &["usage_id", "total_tokens"]);
    usage.budget = budget.clone();
    assert_eq!(collect(&adapter, usage).len(), 2);
    assert_eq!(budget.bytes_read_used(), partial + file_len);
}

#[test]
fn growth_or_replacement_invalidates_the_cached_parse() {
    let fixtures = fixtures();

    // Append growth changes the pinned length: the next table re-parses
    // through the full validation chain and re-warns.
    let root = fixtures.path().join("active-boundary");
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let budget = ResourceBudget::default();
    let mut messages = request(source.clone(), TableName::Messages, &["message_id"]);
    messages.budget = budget.clone();
    assert_eq!(collect(&adapter, messages).len(), 2);
    let first_len = fs::metadata(find_main(&root))
        .expect("transcript metadata")
        .len();
    assert_eq!(budget.bytes_read_used(), first_len);

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(find_main(&root))
        .expect("append fixture transcript");
    file.write_all(b"{\"type\":\"future-after-boundary\"}\n")
        .expect("append succeeds");
    let grown_len = fs::metadata(find_main(&root))
        .expect("transcript metadata")
        .len();

    let mut usage = request(source, TableName::Usage, &["usage_id", "total_tokens"]);
    usage.budget = budget.clone();
    let (records, warnings) = unknown_event_warnings(&adapter, usage);
    assert_eq!(records.len(), 1);
    assert_eq!(
        budget.bytes_read_used(),
        first_len + grown_len,
        "growth misses the cache and re-parses the appended transcript"
    );
    assert_eq!(warnings, 1, "the re-parse reproduces the warning");

    // Replacement after a cached parse is caught by the replay-side §9
    // end-of-scan identity re-check.
    let root = fixtures.path().join("root-replacement");
    let replacement = fixtures.path().join("root-replacement-replacement");
    let displaced = fixtures.path().join("root-replacement-displaced");
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
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
        .scan(request(source, TableName::Usage, &["usage_id"]))
        .expect("replay scan starts")
        .records;
    fs::rename(&root, &displaced).expect("root displaced");
    fs::rename(&replacement, &root).expect("replacement installed");
    assert!(
        replay.next().is_some_and(|record| record.is_ok()),
        "cached records replay before the end-of-scan check"
    );
    assert_eq!(
        replay.next(),
        Some(Err(AdapterError::SnapshotUnavailable)),
        "the replay identity re-check fails closed"
    );
    assert!(replay.next().is_none());
}

#[test]
fn concurrent_scans_of_one_file_stay_correct() {
    let fixtures = fixtures();
    let root = fixtures.path().join("full");

    let baseline_adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
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

    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
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
}
