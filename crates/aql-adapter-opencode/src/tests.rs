use std::sync::atomic::{AtomicU64, Ordering};

use aql_adapter_api::{AccessGrant, ColumnName, Literal, OrderingHint, ResourceBudget};

use super::*;

static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixtures() -> PathBuf {
    let output = std::env::temp_dir().join(format!(
        "aql-opencode-adapter-{}-{:016x}-{}",
        std::process::id(),
        rand_value(),
        FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed),
    ));
    aql_test_support::generate_opencode(&output).expect("fixture generator succeeds");
    output
}

fn rand_value() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time is available")
        .as_nanos()
}

#[test]
fn probe_accepts_pinned_schema_and_rejects_drift_and_symlink() {
    let root = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let accepted = adapter.probe(&ProbeRequest {
        data_root: root.join("minimal").to_string_lossy().into_owned(),
    });
    assert!(accepted.is_ok());
    for fixture in [
        "future-schema",
        "missing-migration",
        "corrupt-db",
        "symlink-db",
    ] {
        assert!(
            adapter
                .probe(&ProbeRequest {
                    data_root: root.join(fixture).to_string_lossy().into_owned(),
                })
                .is_err(),
            "{fixture} must fail closed"
        );
    }
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn authorizer_denies_forbidden_tables_and_sql_features() {
    let root = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let binding = OpenCodeAdapter::validate_root(&root.join("forbidden-tables"))
        .expect("fixture root validates");
    let connection = adapter
        .open_connection(&binding, AuthorizerPolicy::schema())
        .expect("schema connection opens");
    assert!(connection.prepare("SELECT data FROM credential").is_err());
    assert!(connection.prepare("SELECT data FROM account").is_err());
    assert!(
        connection
            .prepare("ATTACH DATABASE ':memory:' AS extra")
            .is_err()
    );
    assert!(
        connection
            .prepare("CREATE TEMP TABLE forbidden(value TEXT)")
            .is_err()
    );
    drop(connection);
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn active_wal_probe_preserves_database_and_wal_bytes() {
    let root = fixtures();
    let source = root.join("minimal");
    let database = source.join("opencode.db");
    let writer = Connection::open(&database).expect("synthetic writer opens");
    writer
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .expect("synthetic WAL mode enables");
    writer
            .execute(
                "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?1, 'project-synthetic', 'wal-session', '/synthetic/wal', 'Synthetic WAL title', '1.17.18', 1767225600000, 1767225600000)",
                ["ses_synthetic_wal"],
            )
            .expect("synthetic WAL row commits");
    let wal = source.join("opencode.db-wal");
    let shm = source.join("opencode.db-shm");
    assert!(wal.is_file());
    assert!(shm.is_file());
    let database_before = fs::read(&database).expect("database bytes read");
    let wal_before = fs::read(&wal).expect("WAL bytes read");
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    adapter
        .probe(&ProbeRequest {
            data_root: source.to_string_lossy().into_owned(),
        })
        .expect("active WAL source probes");
    assert_eq!(
        fs::read(&database).expect("database bytes re-read"),
        database_before
    );
    assert_eq!(fs::read(&wal).expect("WAL bytes re-read"), wal_before);
    drop(writer);
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn cleanly_closed_wal_probe_leaves_no_sidecars_and_unchanged_bytes() {
    let root = fixtures();
    let source = root.join("minimal");
    let database = source.join("opencode.db");
    {
        let writer = Connection::open(&database).expect("synthetic writer opens");
        writer
            .execute_batch("PRAGMA journal_mode=WAL;")
            .expect("synthetic WAL mode enables");
        writer
            .execute(
                "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?1, 'project-synthetic', 'wal-closed-session', '/synthetic/wal', 'Synthetic closed WAL title', '1.17.18', 1767225607000, 1767225607000)",
                ["ses_synthetic_wal_closed"],
            )
            .expect("synthetic WAL row commits");
    }
    let wal = source.join("opencode.db-wal");
    let shm = source.join("opencode.db-shm");
    assert!(!wal.exists() && !shm.exists());
    let database_before = fs::read(&database).expect("database bytes read");
    assert_eq!(
        &database_before[18..20],
        &[2, 2],
        "fixture database is in WAL mode"
    );
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    adapter
        .probe(&ProbeRequest {
            data_root: source.to_string_lossy().into_owned(),
        })
        .expect("cleanly-closed WAL source probes");
    assert_eq!(
        fs::read(&database).expect("database bytes re-read"),
        database_before
    );
    assert!(!wal.exists() && !shm.exists());
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn cleanly_closed_wal_then_new_wal_fails_closed() {
    let root = fixtures();
    let source = root.join("minimal");
    let database = source.join("opencode.db");
    {
        let writer = Connection::open(&database).expect("synthetic writer opens");
        writer
            .execute_batch("PRAGMA journal_mode=WAL;")
            .expect("synthetic WAL mode enables");
    }
    assert!(!source.join("opencode.db-wal").exists());
    let binding = OpenCodeAdapter::validate_root(&source).expect("source validates");
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    adapter
        .probe(&ProbeRequest {
            data_root: source.to_string_lossy().into_owned(),
        })
        .expect("cleanly-closed WAL source probes");
    let writer = Connection::open(&database).expect("synthetic writer reopens");
    writer
        .execute_batch("PRAGMA wal_autocheckpoint=0;")
        .expect("autocheckpoint disables");
    writer
        .execute(
            "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?1, 'project-synthetic', 'wal-drift-session', '/synthetic/wal', 'Synthetic drift title', '1.17.18', 1767225608000, 1767225608000)",
            ["ses_synthetic_wal_drift"],
        )
        .expect("synthetic WAL row commits");
    assert!(source.join("opencode.db-wal").is_file());
    assert!(matches!(
        OpenCodeAdapter::validate_binding(&binding),
        Err(AdapterError::SnapshotUnavailable)
    ));
    drop(writer);
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[cfg(unix)]
#[test]
fn wal_symlink_and_root_replacement_fail_closed() {
    use std::os::unix::fs::symlink;

    let root = fixtures();
    let source = root.join("minimal");
    let outside = root.join("outside-wal");
    fs::write(&outside, b"synthetic outside WAL").expect("outside WAL writes");
    symlink(&outside, source.join("opencode.db-wal")).expect("WAL symlink creates");
    assert!(OpenCodeAdapter::validate_root(&source).is_err());
    fs::remove_file(source.join("opencode.db-wal")).expect("WAL symlink removes");

    let binding = OpenCodeAdapter::validate_root(&source).expect("source validates");
    let moved = root.join("moved");
    fs::rename(&source, &moved).expect("source moves");
    fs::create_dir(&source).expect("replacement root creates");
    fs::copy(moved.join("opencode.db"), source.join("opencode.db"))
        .expect("replacement database copies");
    assert!(matches!(
        OpenCodeAdapter::validate_binding(&binding),
        Err(AdapterError::SnapshotUnavailable)
    ));
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[derive(Default)]
struct ScanOptions {
    predicates: Vec<Predicate>,
    limit: Option<u64>,
    order_hint: Vec<OrderingHint>,
}

fn scan(
    adapter: &OpenCodeAdapter,
    source: &Path,
    table: TableName,
    projection: &[&str],
    access: AccessGrant,
    options: ScanOptions,
) -> Result<ScanResult, AdapterError> {
    let manifest = adapter
        .probe(&ProbeRequest {
            data_root: source.to_string_lossy().into_owned(),
        })
        .expect("fixture probes")
        .manifests
        .into_iter()
        .next()
        .expect("manifest exists");
    adapter.scan(ScanRequest {
        source: manifest,
        table,
        projection: projection
            .iter()
            .map(|name| ColumnName::new(*name))
            .collect(),
        predicates: options.predicates,
        limit: options.limit,
        order_hint: options.order_hint,
        access,
        budget: ResourceBudget::default(),
        cancellation: Default::default(),
        snapshot: None,
    })
}

#[test]
fn edge_stream_keeps_all_rows_when_limit_is_not_exact() {
    let root = fixtures();
    let source = root.join("parent-child");
    {
        let writer = Connection::open(source.join("opencode.db")).expect("writer opens");
        writer
            .execute(
                "INSERT INTO session (id, project_id, slug, directory, title, version, parent_id, time_created, time_updated) VALUES ('ses_synthetic_child_two', 'project-synthetic', 'synthetic-session', '/synthetic/workspace', 'Synthetic OpenCode title', '1.17.18', 'ses_synthetic_parent', 1767225600100, 1767225605100)",
                [],
            )
            .expect("second child inserts");
    }
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let filtered = scan(
        &adapter,
        &source,
        TableName::SessionEdges,
        &["edge_id"],
        AccessGrant::default(),
        ScanOptions {
            predicates: vec![Predicate::Unsupported("synthetic".to_string())],
            limit: Some(1),
            ..ScanOptions::default()
        },
    )
    .expect("filtered edge scan opens");
    assert_eq!(filtered.pushdown.limit, Some(PushdownState::Unsupported));
    assert_eq!(
        filtered.pushdown.predicates,
        vec![PushdownState::Unsupported]
    );
    let edges = filtered
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("filtered edge scan succeeds");
    assert_eq!(edges.len(), 2, "stream must not apply an un-pushed limit");

    let hinted = scan(
        &adapter,
        &source,
        TableName::SessionEdges,
        &["edge_id"],
        AccessGrant::default(),
        ScanOptions {
            limit: Some(1),
            order_hint: vec![OrderingHint {
                column: ColumnName::new("edge_id"),
                descending: false,
            }],
            ..ScanOptions::default()
        },
    )
    .expect("hinted edge scan opens");
    assert_eq!(hinted.pushdown.limit, Some(PushdownState::Unsupported));
    let edges = hinted
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("hinted edge scan succeeds");
    assert_eq!(
        edges.len(),
        2,
        "stream must not apply a limit blocked by an order hint"
    );

    let exact = scan(
        &adapter,
        &source,
        TableName::SessionEdges,
        &["edge_id"],
        AccessGrant::default(),
        ScanOptions {
            limit: Some(1),
            ..ScanOptions::default()
        },
    )
    .expect("exact edge scan opens");
    assert_eq!(exact.pushdown.limit, Some(PushdownState::Exact));
    let edges = exact
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("exact edge scan succeeds");
    assert_eq!(edges.len(), 1, "exact limit still truncates");
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn session_scan_reads_key_fields_with_grants() {
    let root = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let sessions = scan(
        &adapter,
        &root.join("full"),
        TableName::Sessions,
        &[
            "native_id",
            "title",
            "cwd",
            "project",
            "model",
            "provider",
            "tokens_used",
            "archived",
        ],
        AccessGrant {
            path: true,
            content: true,
            ..AccessGrant::default()
        },
        ScanOptions::default(),
    )
    .expect("sessions scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("sessions scan succeeds");
    assert_eq!(sessions.len(), 1);
    assert!(matches!(
        &sessions[0],
        CanonicalRecord::Session(record)
            if record.native_id.as_str() == "ses_synthetic_full"
                && record.title.as_deref() == Some("Synthetic OpenCode title")
                && record.cwd.as_deref() == Some("/synthetic/workspace")
                && record.project.as_deref() == Some("relative/synthetic")
                && record.model.as_deref() == Some("synthetic-model")
                && record.provider.as_deref() == Some("synthetic-provider")
                && record.tokens_used == Some(24)
                && record.archived == Some(false)
    ));
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn message_scan_paginates_sessions_and_batches_in_stable_order() {
    let root = fixtures();
    let source = root.join("multi-session");
    {
        let mut writer = Connection::open(source.join("opencode.db")).expect("writer opens");
        let data = r#"{"role":"user","time":{"created":1767225601000},"agent":"synthetic-agent","model":{"providerID":"synthetic-provider","modelID":"synthetic-model"}}"#;
        let transaction = writer.transaction().expect("transaction begins");
        for index in 0..150_u32 {
            for (session, base) in [
                ("ses_synthetic_alpha", 0_u32),
                ("ses_synthetic_beta", 500_u32),
            ] {
                let ordinal = base + index;
                transaction
                    .execute(
                        "INSERT INTO message(id, session_id, time_created, time_updated, data) VALUES (?1, ?2, ?3, ?3, ?4)",
                        rusqlite::params![
                            format!("msg_synthetic_{ordinal:04}"),
                            session,
                            1767225601000_i64 + i64::from(ordinal),
                            data
                        ],
                    )
                    .expect("message inserts");
            }
        }
        transaction.commit().expect("transaction commits");
    }
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let messages = scan(
        &adapter,
        &source,
        TableName::Messages,
        &["role", "sequence"],
        AccessGrant::default(),
        ScanOptions::default(),
    )
    .expect("messages scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("messages scan succeeds");
    assert_eq!(messages.len(), 300);
    for (index, record) in messages.iter().enumerate() {
        let CanonicalRecord::Message(message) = record else {
            panic!("messages table emits only message records");
        };
        let (session, sequence) = if index < 150 {
            ("ses_synthetic_alpha", index as i64 + 1)
        } else {
            ("ses_synthetic_beta", index as i64 - 149)
        };
        assert!(
            message.session_id.as_str().contains(session),
            "record {index} belongs to {session}"
        );
        assert_eq!(message.sequence, sequence, "record {index} sequence");
        assert_eq!(message.role, "user");
        assert!(message.content.is_none());
    }
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn corrupt_part_fails_closed_and_unknown_part_warns_without_panicking() {
    let root = fixtures();
    let source = root.join("full");
    {
        let writer = Connection::open(source.join("opencode.db")).expect("writer opens");
        writer
            .execute(
                "INSERT INTO part(id, message_id, session_id, time_created, time_updated, data) VALUES ('prt_002_corrupt', 'msg_synthetic_user', 'ses_synthetic_full', 1767225601500, 1767225601500, '{malformed fixture json]')",
                [],
            )
            .expect("corrupt part inserts");
    }
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let mut records = scan(
        &adapter,
        &source,
        TableName::Messages,
        &["role"],
        AccessGrant::default(),
        ScanOptions::default(),
    )
    .expect("corrupt part scan opens")
    .records;
    let error = records
        .next()
        .expect("corrupt part row is attempted")
        .expect_err("corrupt part fails closed");
    assert!(matches!(error, AdapterError::CorruptSource { .. }));
    drop(records);

    let unknown = scan(
        &adapter,
        &root.join("unknown-part"),
        TableName::Messages,
        &["role"],
        AccessGrant::default(),
        ScanOptions::default(),
    )
    .expect("unknown part scan opens");
    let diagnostics = unknown.diagnostics.clone();
    let records = unknown
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("unknown part is skipped");
    assert_eq!(records.len(), 1);
    let warnings = diagnostics.snapshot().expect("warnings read");
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].kind, AdapterWarningKind::UnknownEvent);
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn content_json_only_projection_leaves_content_empty() {
    let root = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let access = AccessGrant {
        content: true,
        ..AccessGrant::default()
    };
    let json_only = scan(
        &adapter,
        &root.join("full"),
        TableName::Messages,
        &["role", "content_json"],
        access,
        ScanOptions::default(),
    )
    .expect("content_json scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("content_json scan succeeds");
    assert_eq!(json_only.len(), 2);
    for record in &json_only {
        let CanonicalRecord::Message(message) = record else {
            panic!("messages table emits only message records");
        };
        assert!(
            message.content.is_none(),
            "content must stay empty for a content_json-only projection"
        );
        assert!(message.content_json.is_none());
    }
    let content = scan(
        &adapter,
        &root.join("full"),
        TableName::Messages,
        &["role", "content"],
        access,
        ScanOptions::default(),
    )
    .expect("content scan opens")
    .records
    .collect::<Result<Vec<_>, _>>()
    .expect("content scan succeeds");
    assert!(matches!(
        &content[0],
        CanonicalRecord::Message(message)
            if message.content.as_deref() == Some("Synthetic OpenCode question")
    ));
    assert!(matches!(
        &content[1],
        CanonicalRecord::Message(message)
            if message.content.as_deref() == Some("Synthetic OpenCode answer")
    ));
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn mixed_predicates_are_reported_unsupported_without_filtering() {
    let root = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let source = root.join("multi-session");
    let mixed = scan(
        &adapter,
        &source,
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        ScanOptions {
            predicates: vec![
                Predicate::Eq(
                    ColumnName::new("native_id"),
                    Literal::Text("ses_synthetic_alpha".to_string()),
                ),
                Predicate::Unsupported("synthetic".to_string()),
            ],
            ..ScanOptions::default()
        },
    )
    .expect("mixed predicate scan opens");
    assert_eq!(
        mixed.pushdown.predicates,
        vec![PushdownState::Unsupported, PushdownState::Unsupported],
        "mixed predicates are all reported as not applied"
    );
    let sessions = mixed
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("mixed predicate scan succeeds");
    assert_eq!(
        sessions.len(),
        2,
        "no predicate filters when not all are exact"
    );

    let exact = scan(
        &adapter,
        &source,
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        ScanOptions {
            predicates: vec![Predicate::Eq(
                ColumnName::new("native_id"),
                Literal::Text("ses_synthetic_alpha".to_string()),
            )],
            ..ScanOptions::default()
        },
    )
    .expect("exact predicate scan opens");
    assert_eq!(exact.pushdown.predicates, vec![PushdownState::Exact]);
    let sessions = exact
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("exact predicate scan succeeds");
    assert_eq!(sessions.len(), 1);
    fs::remove_dir_all(root).expect("fixtures are removed");
}

#[test]
fn clean_end_of_stream_passes_final_revalidation() {
    let root = fixtures();
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    for (fixture, table, projection, expected) in [
        ("full", TableName::Sessions, &["native_id"][..], 1),
        ("full", TableName::Messages, &["role"][..], 2),
        ("full", TableName::ToolCalls, &["tool_name"][..], 1),
        ("full", TableName::Usage, &["total_tokens"][..], 1),
        ("full", TableName::SessionEdges, &["edge_id"][..], 0),
        ("parent-child", TableName::SessionEdges, &["edge_id"][..], 1),
    ] {
        let records = scan(
            &adapter,
            &root.join(fixture),
            table,
            projection,
            AccessGrant::default(),
            ScanOptions::default(),
        )
        .expect("scan opens")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("unmodified source reaches EOF without error");
        assert_eq!(records.len(), expected, "{fixture} {table:?}");
    }
    fs::remove_dir_all(root).expect("fixtures are removed");
}

// Unix-only: replacing the root directory requires renaming a directory that
// still contains an open database file, which Windows file locking forbids
// (mirrors `wal_symlink_and_root_replacement_fail_closed`).
#[cfg(unix)]
#[test]
fn replaced_root_fails_closed_at_end_of_stream() {
    let root = fixtures();
    let source = root.join("multi-session");
    let adapter = OpenCodeAdapter::new(b"synthetic-salt".to_vec());
    let mut records = scan(
        &adapter,
        &source,
        TableName::Sessions,
        &["native_id"],
        AccessGrant::default(),
        ScanOptions::default(),
    )
    .expect("scan opens")
    .records;
    assert!(
        records.next().expect("first row exists").is_ok(),
        "rows before replacement stream normally"
    );
    let moved = root.join("moved");
    fs::rename(&source, &moved).expect("source moves");
    fs::create_dir(&source).expect("replacement root creates");
    fs::copy(moved.join("opencode.db"), source.join("opencode.db"))
        .expect("replacement database copies");
    let tail = records.collect::<Vec<_>>();
    assert_eq!(tail.len(), 2, "buffered row, then the tail check fails");
    assert!(
        tail[0].is_ok(),
        "buffered rows still stream from the snapshot"
    );
    assert!(matches!(tail[1], Err(AdapterError::SnapshotUnavailable)));
    fs::remove_dir_all(root).expect("fixtures are removed");
}
