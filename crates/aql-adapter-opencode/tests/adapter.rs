use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aql_adapter_api::{
    AccessGrant, AgentAdapter, ColumnName, FileAccessObserver, Literal, Predicate, ProbeRequest,
    ResourceBudget, ScanRequest, SourceKind, TableName,
};
use aql_adapter_opencode::OpenCodeAdapter;
use aql_model::CanonicalRecord;
use rusqlite::Connection;

static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct OpenCounter(AtomicU64);

impl FileAccessObserver for OpenCounter {
    fn opened(&self, _source_kind: SourceKind) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn bytes_read(&self, _source_kind: SourceKind, _count: u64) {}
}

fn fixtures() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "aql-opencode-integration-{}-{}",
        std::process::id(),
        FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed),
    ));
    aql_test_support::generate_opencode(&root).expect("fixture generator succeeds");
    root
}

fn manifest(adapter: &OpenCodeAdapter, root: &Path) -> aql_model::SourceManifest {
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

fn scan(
    adapter: &OpenCodeAdapter,
    manifest: aql_model::SourceManifest,
    table: TableName,
    projection: &[&str],
    access: AccessGrant,
    predicates: Vec<Predicate>,
    limit: Option<u64>,
) -> Result<aql_adapter_api::ScanResult, aql_adapter_api::AdapterError> {
    adapter.scan(ScanRequest {
        source: manifest,
        table,
        projection: projection
            .iter()
            .map(|name| ColumnName::new(*name))
            .collect(),
        predicates,
        limit,
        order_hint: Vec::new(),
        access,
        budget: ResourceBudget::default(),
        cancellation: Default::default(),
        snapshot: None,
    })
}

#[test]
fn sessions_and_parent_edges_are_explicit_and_deterministic() {
    let roots = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let source = roots.join("parent-child");
    let manifest = manifest(&adapter, &source);
    let sessions = scan(
        &adapter,
        manifest.clone(),
        TableName::Sessions,
        &["native_id", "archived", "tokens_used"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("sessions scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("sessions scan succeeds");
    assert_eq!(sessions.len(), 2);
    assert!(matches!(
        &sessions[0],
        CanonicalRecord::Session(record) if record.native_id.as_str() == "ses_synthetic_child"
    ));
    let edges = scan(
        &adapter,
        manifest,
        TableName::SessionEdges,
        &["parent_session_id", "child_session_id", "edge_kind"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("edge scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("edge scan succeeds");
    assert_eq!(edges.len(), 1);
    assert!(matches!(
        &edges[0],
        CanonicalRecord::SessionEdge(record)
            if record.edge_kind == "parent"
                && record.parent_session_id.as_str().contains("ses_synthetic_parent")
                && record.child_session_id.as_str().contains("ses_synthetic_child")
    ));
    fs::remove_dir_all(roots).expect("fixtures remove");
}

#[test]
fn sensitive_session_columns_require_grants_before_open_and_are_size_bounded() {
    let roots = fixtures();
    let counter = Arc::new(OpenCounter::default());
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec()).with_observer(counter.clone());
    let source = roots.join("oversized-session-sensitive");
    let manifest = manifest(&adapter, &source);
    let opens_after_probe = counter.0.load(Ordering::Relaxed);
    let denied = scan(
        &adapter,
        manifest.clone(),
        TableName::Sessions,
        &["title"],
        AccessGrant::default(),
        Vec::new(),
        None,
    );
    assert!(denied.is_err());
    assert_eq!(counter.0.load(Ordering::Relaxed), opens_after_probe);

    let safe = scan(
        &adapter,
        manifest.clone(),
        TableName::Sessions,
        &["session_id"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("safe scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("safe scan skips malformed sensitive values");
    assert_eq!(safe.len(), 1);

    let mut request = ScanRequest {
        source: manifest,
        table: TableName::Sessions,
        projection: vec![ColumnName::new("title")],
        predicates: Vec::new(),
        limit: None,
        order_hint: Vec::new(),
        access: AccessGrant {
            content: true,
            ..AccessGrant::default()
        },
        budget: ResourceBudget {
            max_single_value_bytes: 64,
            ..ResourceBudget::default()
        },
        cancellation: Default::default(),
        snapshot: None,
    };
    let mut records = adapter
        .scan(request.clone())
        .expect("bounded content stream opens")
        .records;
    assert!(records.next().expect("content row is attempted").is_err());
    drop(records);
    request.projection = vec![ColumnName::new("cwd")];
    request.access = AccessGrant {
        path: true,
        ..AccessGrant::default()
    };
    let mut records = adapter
        .scan(request)
        .expect("bounded path stream opens")
        .records;
    assert!(records.next().expect("path row is attempted").is_err());
    drop(records);
    fs::remove_dir_all(roots).expect("fixtures remove");
}

#[test]
fn exact_filter_can_push_limit_and_unsupported_filter_cannot() {
    let roots = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let manifest = manifest(&adapter, &roots.join("multi-session"));
    let exact = scan(
        &adapter,
        manifest.clone(),
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        vec![Predicate::Eq(
            ColumnName::new("native_id"),
            Literal::Text("ses_synthetic_beta".to_string()),
        )],
        Some(1),
    )
    .expect("exact stream opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("exact stream succeeds");
    assert_eq!(exact.len(), 1);
    assert!(matches!(
        &exact[0],
        CanonicalRecord::Session(record) if record.native_id.as_str() == "ses_synthetic_beta"
    ));
    let unsupported = scan(
        &adapter,
        manifest,
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        vec![Predicate::Unsupported("synthetic".to_string())],
        Some(1),
    )
    .expect("unsupported stream opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("unsupported stream succeeds");
    assert_eq!(unsupported.len(), 2);
    fs::remove_dir_all(roots).expect("fixtures remove");
}

#[test]
fn active_wal_session_is_visible_without_changing_database_or_wal() {
    let roots = fixtures();
    let source = roots.join("minimal");
    let database = source.join("opencode.db");
    let writer = Connection::open(&database).expect("writer opens");
    writer
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .expect("WAL enables");
    writer
        .execute(
            "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?1, 'project-synthetic', 'wal-session', '/synthetic/wal', 'Synthetic WAL title', '1.17.18', 1767225606000, 1767225606000)",
            ["ses_synthetic_wal_visible"],
        )
        .expect("WAL row commits");
    let wal = source.join("opencode.db-wal");
    let db_before = fs::read(&database).expect("DB bytes read");
    let wal_before = fs::read(&wal).expect("WAL bytes read");
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let manifest = manifest(&adapter, &source);
    let rows = scan(
        &adapter,
        manifest,
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("WAL scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("WAL scan succeeds");
    assert!(rows.iter().any(|record| matches!(
        record,
        CanonicalRecord::Session(session) if session.native_id.as_str() == "ses_synthetic_wal_visible"
    )));
    assert_eq!(fs::read(&database).expect("DB bytes re-read"), db_before);
    assert_eq!(fs::read(&wal).expect("WAL bytes re-read"), wal_before);
    drop(writer);
    fs::remove_dir_all(roots).expect("fixtures remove");
}

#[test]
fn cleanly_closed_wal_session_is_visible_without_sidecars_or_changes() {
    let roots = fixtures();
    let source = roots.join("minimal");
    let database = source.join("opencode.db");
    {
        let writer = Connection::open(&database).expect("writer opens");
        writer
            .execute_batch("PRAGMA journal_mode=WAL;")
            .expect("WAL enables");
        writer
            .execute(
                "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?1, 'project-synthetic', 'wal-closed-session', '/synthetic/wal', 'Synthetic closed WAL title', '1.17.18', 1767225607000, 1767225607000)",
                ["ses_synthetic_wal_closed"],
            )
            .expect("WAL row commits");
    }
    let wal = source.join("opencode.db-wal");
    let shm = source.join("opencode.db-shm");
    assert!(!wal.exists() && !shm.exists());
    let database_before = fs::read(&database).expect("DB bytes read");
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let manifest = manifest(&adapter, &source);
    let rows = scan(
        &adapter,
        manifest.clone(),
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("cleanly-closed WAL scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("cleanly-closed WAL scan succeeds");
    assert!(rows.iter().any(|record| matches!(
        record,
        CanonicalRecord::Session(session) if session.native_id.as_str() == "ses_synthetic_wal_closed"
    )));
    assert_eq!(
        fs::read(&database).expect("DB bytes re-read"),
        database_before
    );
    assert!(!wal.exists() && !shm.exists());

    let writer = Connection::open(&database).expect("writer reopens");
    writer
        .execute_batch("PRAGMA wal_autocheckpoint=0;")
        .expect("autocheckpoint disables");
    writer
        .execute(
            "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?1, 'project-synthetic', 'wal-drift-session', '/synthetic/wal', 'Synthetic drift title', '1.17.18', 1767225608000, 1767225608000)",
            ["ses_synthetic_wal_drift"],
        )
        .expect("WAL row commits");
    assert!(wal.is_file());
    assert!(matches!(
        scan(
            &adapter,
            manifest,
            TableName::Sessions,
            &["native_id"],
            AccessGrant::default(),
            Vec::new(),
            None,
        ),
        Err(aql_adapter_api::AdapterError::SnapshotUnavailable)
    ));
    drop(writer);
    fs::remove_dir_all(roots).expect("fixtures remove");
}

#[test]
fn messages_tools_and_derived_usage_inputs_follow_the_pinned_projection() {
    let roots = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let manifest = manifest(&adapter, &roots.join("full"));
    let safe_messages = scan(
        &adapter,
        manifest.clone(),
        TableName::Messages,
        &[
            "message_id",
            "sequence",
            "role",
            "model",
            "input_tokens",
            "output_tokens",
            "cached_tokens",
        ],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("safe messages open")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("safe messages succeed");
    assert_eq!(safe_messages.len(), 2);
    assert!(matches!(
        &safe_messages[0],
        CanonicalRecord::Message(message)
            if message.role == "user" && message.sequence == 1 && message.content.is_none()
    ));
    assert!(matches!(
        &safe_messages[1],
        CanonicalRecord::Message(message)
            if message.role == "assistant"
                && message.sequence == 2
                && message.input_tokens.is_none()
                && message.output_tokens.is_none()
                && message.cached_tokens.is_none()
    ));

    let usage = scan(
        &adapter,
        manifest.clone(),
        TableName::Usage,
        &[
            "input_tokens",
            "output_tokens",
            "cached_tokens",
            "total_tokens",
        ],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("explicit usage stream opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("explicit usage stream succeeds");
    assert_eq!(usage.len(), 1);
    assert!(matches!(
        &usage[0],
        CanonicalRecord::Usage(record)
            if record.input_tokens == Some(11)
                && record.output_tokens == Some(7)
                && record.cached_tokens == Some(4)
                && record.total_tokens == Some(24)
                && record.message_count == 0
    ));

    let content_messages = scan(
        &adapter,
        manifest.clone(),
        TableName::Messages,
        &["role", "content"],
        AccessGrant {
            content: true,
            ..AccessGrant::default()
        },
        Vec::new(),
        None,
    )
    .expect("content messages open")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("content messages succeed");
    assert!(matches!(
        &content_messages[0],
        CanonicalRecord::Message(message)
            if message.content.as_deref() == Some("Synthetic OpenCode question")
    ));
    assert!(matches!(
        &content_messages[1],
        CanonicalRecord::Message(message)
            if message.content.as_deref() == Some("Synthetic OpenCode answer")
    ));

    let safe_tools = scan(
        &adapter,
        manifest.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "status", "duration_ms"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("safe tools open")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("safe tools succeed");
    assert_eq!(safe_tools.len(), 1);
    assert!(matches!(
        &safe_tools[0],
        CanonicalRecord::ToolCall(tool)
            if tool.tool_name == "synthetic_tool"
                && tool.status.as_deref() == Some("completed")
                && tool.duration_ms == Some(1000)
                && tool.arguments.is_none()
                && tool.output.is_none()
    ));

    let sensitive_tools = scan(
        &adapter,
        manifest,
        TableName::ToolCalls,
        &["arguments", "output"],
        AccessGrant {
            tool_input: true,
            tool_output: true,
            ..AccessGrant::default()
        },
        Vec::new(),
        None,
    )
    .expect("sensitive tools open")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("sensitive tools succeed");
    assert!(matches!(
        &sensitive_tools[0],
        CanonicalRecord::ToolCall(tool)
            if tool.arguments.as_ref().and_then(|value| value.get("value")).and_then(|value| value.as_str()) == Some("synthetic input")
                && tool.output.as_deref() == Some("Synthetic OpenCode tool output")
    ));
    fs::remove_dir_all(roots).expect("fixtures remove");
}

#[test]
fn malformed_unknown_duplicate_and_oversized_parts_have_closed_outcomes() {
    let roots = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());

    let malformed_manifest = manifest(&adapter, &roots.join("malformed-json"));
    let mut malformed = scan(
        &adapter,
        malformed_manifest,
        TableName::Messages,
        &["role"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("malformed stream opens")
    .records;
    assert!(
        malformed
            .next()
            .expect("malformed row is attempted")
            .is_err()
    );
    drop(malformed);

    let unknown_manifest = manifest(&adapter, &roots.join("unknown-part"));
    let unknown = scan(
        &adapter,
        unknown_manifest,
        TableName::Messages,
        &["role"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("unknown stream opens");
    let diagnostics = unknown.diagnostics.clone();
    assert_eq!(
        unknown
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect("unknown part is skipped")
            .len(),
        1
    );
    assert_eq!(diagnostics.snapshot().expect("warnings read").len(), 1);

    let duplicate_manifest = manifest(&adapter, &roots.join("duplicate-representations"));
    let duplicate = scan(
        &adapter,
        duplicate_manifest,
        TableName::Messages,
        &["role"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("duplicate fixture opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("only message projection is read");
    assert_eq!(duplicate.len(), 2);

    let oversized_manifest = manifest(&adapter, &roots.join("oversized-json"));
    let safe = scan(
        &adapter,
        oversized_manifest.clone(),
        TableName::Messages,
        &["role"],
        AccessGrant::default(),
        Vec::new(),
        None,
    )
    .expect("oversized safe stream opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("unprojected oversized text is skipped");
    assert_eq!(safe.len(), 1);
    let mut request = ScanRequest {
        source: oversized_manifest,
        table: TableName::Messages,
        projection: vec![ColumnName::new("content")],
        predicates: Vec::new(),
        limit: None,
        order_hint: Vec::new(),
        access: AccessGrant {
            content: true,
            ..AccessGrant::default()
        },
        budget: ResourceBudget {
            max_single_value_bytes: 64,
            ..ResourceBudget::default()
        },
        cancellation: Default::default(),
        snapshot: None,
    };
    let mut sensitive = adapter
        .scan(request.clone())
        .expect("bounded sensitive stream opens")
        .records;
    assert!(
        sensitive
            .next()
            .expect("oversized text is attempted")
            .is_err()
    );
    drop(sensitive);
    request.table = TableName::ToolCalls;
    request.projection = vec![ColumnName::new("arguments")];
    request.access = AccessGrant {
        tool_input: true,
        ..AccessGrant::default()
    };
    assert!(
        adapter
            .scan(request)
            .expect("tool stream opens")
            .records
            .collect::<Result<Vec<_>, _>>()
            .is_ok()
    );
    fs::remove_dir_all(roots).expect("fixtures remove");
}
