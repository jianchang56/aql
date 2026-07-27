use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aql_adapter_api::{
    AccessGrant, AdapterError, AdapterWarningKind, AgentAdapter, CancellationToken, ColumnName,
    FileAccessObserver, Literal, Predicate, ProbeRequest, PushdownState, ResourceBudget,
    ScanRequest, SourceKind, TableName,
};
use aql_adapter_codex::CodexAdapter;
use aql_model::{CanonicalRecord, SourceManifest};
use rusqlite::Connection;
use sha2::{Digest, Sha256};

static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct Observer {
    opens: Mutex<BTreeMap<String, u64>>,
    bytes: Mutex<BTreeMap<String, u64>>,
}

impl Observer {
    fn count(&self, kind: SourceKind) -> u64 {
        *self
            .opens
            .lock()
            .expect("observer lock must be available")
            .get(kind_name(kind))
            .unwrap_or(&0)
    }

    fn bytes(&self, kind: SourceKind) -> u64 {
        *self
            .bytes
            .lock()
            .expect("observer lock must be available")
            .get(kind_name(kind))
            .unwrap_or(&0)
    }
}

impl FileAccessObserver for Observer {
    fn opened(&self, source_kind: SourceKind) {
        *self
            .opens
            .lock()
            .expect("observer lock must be available")
            .entry(kind_name(source_kind).to_string())
            .or_default() += 1;
    }

    fn bytes_read(&self, source_kind: SourceKind, count: u64) {
        *self
            .bytes
            .lock()
            .expect("observer lock must be available")
            .entry(kind_name(source_kind).to_string())
            .or_default() += count;
    }
}

#[cfg(unix)]
struct RootReplacingObserver {
    root: PathBuf,
    moved: PathBuf,
    replacement: PathBuf,
    replaced: Mutex<bool>,
}

#[cfg(unix)]
impl FileAccessObserver for RootReplacingObserver {
    fn opened(&self, source_kind: SourceKind) {
        if !matches!(source_kind, SourceKind::Rollout) {
            return;
        }
        let mut replaced = self.replaced.lock().expect("replacement lock");
        if *replaced {
            return;
        }
        fs::rename(&self.root, &self.moved).expect("bound root moves aside");
        fs::rename(&self.replacement, &self.root).expect("replacement root moves into place");
        *replaced = true;
    }

    fn bytes_read(&self, _source_kind: SourceKind, _count: u64) {}
}

fn kind_name(kind: SourceKind) -> &'static str {
    match kind {
        SourceKind::StateDatabase => "state",
        SourceKind::SessionIndex => "index",
        SourceKind::Rollout => "rollout",
    }
}

fn fixture_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after epoch")
        .as_nanos();
    let counter = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let output = std::env::temp_dir().join(format!(
        "aql-adapter-fixtures-{}-{nonce}-{counter}",
        std::process::id()
    ));
    aql_test_support::generate_codex(&output, 100).expect("fixture generator must succeed");
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

#[test]
fn format_fingerprint_does_not_change_with_ordinary_session_deletion() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let adapter = CodexAdapter::new(b"fixture-installation-salt".to_vec());
    let before = manifest(&adapter, &root).format_fingerprint;
    Connection::open(root.join("sqlite/state_5.sqlite"))
        .expect("fixture database opens")
        .execute("DELETE FROM threads", [])
        .expect("fixture sessions delete");
    let after = manifest(&adapter, &root).format_fingerprint;
    assert_eq!(before, after);
    fs::remove_dir_all(fixtures).expect("fixture root is removed");
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

#[test]
fn metadata_scan_does_not_open_rollout() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    assert!(source.data_root_token.starts_with("codex-root:codex:"));
    assert!(!source.data_root_token.contains("aql-adapter-fixtures"));
    let result = adapter
        .scan(request(
            source,
            TableName::Sessions,
            &["session_id", "model", "updated_at"],
        ))
        .expect("metadata scan must succeed");
    let records = result
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("metadata records must be valid");
    assert_eq!(records.len(), 1);
    let CanonicalRecord::Session(session) = &records[0] else {
        panic!("expected a session record");
    };
    assert!(session.provenance.contains_key("model"));
    assert!(session.provenance.contains_key("updated_at"));
    assert!(!session.provenance.contains_key("title"));
    assert!(!session.provenance.contains_key("cwd"));
    assert_eq!(observer.count(SourceKind::Rollout), 0);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn content_access_is_rejected_before_rollout_open() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let error = match adapter.scan(request(source, TableName::Messages, &["content"])) {
        Ok(_) => panic!("content scan must require a grant"),
        Err(error) => error,
    };
    assert!(matches!(error, AdapterError::AccessDenied { .. }));
    assert_eq!(observer.count(SourceKind::Rollout), 0);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn unsupported_usage_table_fails_before_source_open() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let state_opens = observer.count(SourceKind::StateDatabase);

    assert!(matches!(
        adapter.scan(request(source.clone(), TableName::Usage, &[])),
        Err(AdapterError::UnsupportedFormat { .. })
    ));
    assert_eq!(observer.count(SourceKind::StateDatabase), state_opens);
    assert_eq!(observer.count(SourceKind::Rollout), 0);
    assert_eq!(observer.count(SourceKind::SessionIndex), 0);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn single_sensitive_value_budget_is_enforced() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let mut scan = request(source, TableName::Messages, &["content"]);
    scan.access.content = true;
    scan.budget.max_single_value_bytes = 4;
    let error = match adapter.scan(scan) {
        Ok(result) => match result.records.collect::<Result<Vec<_>, _>>() {
            Ok(_) => panic!("oversized content must fail"),
            Err(error) => error,
        },
        Err(error) => error,
    };
    assert!(matches!(
        error,
        AdapterError::BudgetExceeded { resource, .. } if resource == "single_value_bytes"
    ));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn authorized_messages_and_tools_are_parsed_from_fixture_rollout() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("minimal"));

    let mut messages = request(source.clone(), TableName::Messages, &["content"]);
    messages.access.content = true;
    let message_records: Vec<_> = adapter
        .scan(messages)
        .expect("message scan must succeed")
        .records
        .collect::<Result<_, _>>()
        .expect("message records must be valid");
    assert_eq!(message_records.len(), 2);
    let CanonicalRecord::Message(message) = &message_records[0] else {
        panic!("expected a message record");
    };
    assert!(message.provenance.contains_key("content"));
    assert!(!message.provenance.contains_key("model"));

    let mut tools = request(source, TableName::ToolCalls, &["arguments", "output"]);
    tools.access.tool_input = true;
    tools.access.tool_output = true;
    let tool_records: Vec<_> = adapter
        .scan(tools)
        .expect("tool scan must succeed")
        .records
        .collect::<Result<_, _>>()
        .expect("tool records must be valid");
    assert_eq!(tool_records.len(), 1);
    let CanonicalRecord::ToolCall(tool) = &tool_records[0] else {
        panic!("expected a tool call");
    };
    assert_eq!(tool.status.as_deref(), Some("success"));
    assert_eq!(tool.output.as_deref(), Some("Synthetic tool output"));
    assert!(tool.provenance.contains_key("arguments"));
    assert!(tool.provenance.contains_key("output"));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn artifacts_require_path_for_the_table_and_content_for_payloads() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("artifacts"));
    assert!(source.capabilities.contains(&"artifacts".to_string()));
    let opens_after_probe = observer.count(SourceKind::Rollout);

    let denied = match adapter.scan(request(
        source.clone(),
        TableName::Artifacts,
        &["artifact_id", "kind"],
    )) {
        Ok(_) => panic!("artifact enumeration must require path access"),
        Err(error) => error,
    };
    assert!(matches!(denied, AdapterError::AccessDenied { column } if column == "artifacts"));
    assert_eq!(observer.count(SourceKind::Rollout), opens_after_probe);

    let mut metadata = request(
        source.clone(),
        TableName::Artifacts,
        &["artifact_id", "kind", "path", "content"],
    );
    metadata.access.path = true;
    let denied = match adapter.scan(metadata) {
        Ok(_) => panic!("artifact payload must separately require content access"),
        Err(error) => error,
    };
    assert!(matches!(denied, AdapterError::AccessDenied { column } if column == "content"));

    let mut metadata = request(
        source.clone(),
        TableName::Artifacts,
        &["artifact_id", "kind", "path"],
    );
    metadata.access.path = true;
    let records = adapter
        .scan(metadata)
        .expect("path-authorized artifact metadata scan must start")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("artifact metadata records must be valid");
    assert_eq!(records.len(), 3);
    let metadata_ids = records
        .iter()
        .filter_map(|record| match record {
            CanonicalRecord::Artifact(artifact) => Some(artifact.artifact_id.to_string()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(metadata_ids.len(), 3);
    assert!(records.iter().all(|record| {
        matches!(record, CanonicalRecord::Artifact(artifact)
            if artifact.path.is_some()
                && artifact.content.is_none()
                && artifact.content_json.is_none()
                && artifact.provenance.contains_key("path"))
    }));
    assert!(records.iter().any(|record| {
        matches!(record, CanonicalRecord::Artifact(artifact) if artifact.kind == "patch")
    }));

    let mut payload = request(
        source,
        TableName::Artifacts,
        &["artifact_id", "path", "content", "content_json"],
    );
    payload.access.path = true;
    payload.access.content = true;
    let records = adapter
        .scan(payload)
        .expect("authorized artifact payload scan must start")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("artifact payload records must be valid");
    assert_eq!(records.len(), 3);
    let payload_ids = records
        .iter()
        .filter_map(|record| match record {
            CanonicalRecord::Artifact(artifact) => Some(artifact.artifact_id.to_string()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(payload_ids, metadata_ids);
    assert_eq!(
        records
            .iter()
            .filter(|record| {
                matches!(record, CanonicalRecord::Artifact(artifact)
            if artifact.content.is_some() && artifact.content_json.is_some())
            })
            .count(),
        2
    );

    let mut oversized = request(
        manifest(&adapter, &fixtures.join("artifacts")),
        TableName::Artifacts,
        &["content"],
    );
    oversized.access.path = true;
    oversized.access.content = true;
    oversized.budget.max_single_value_bytes = 4;
    let error = match adapter.scan(oversized) {
        Ok(result) => result
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect_err("oversized artifact content must fail"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        AdapterError::BudgetExceeded { resource, .. } if resource == "single_value_bytes"
    ));
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn missing_rollout_degrades_with_a_warning_instead_of_failing_the_scan() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let database =
        Connection::open(root.join("sqlite/state_5.sqlite")).expect("fixture database must open");
    let rollout_path: String = database
        .query_row("SELECT rollout_path FROM threads LIMIT 1", [], |row| {
            row.get(0)
        })
        .expect("fixture rollout locator must exist");
    drop(database);
    fs::remove_file(root.join(rollout_path)).expect("fixture rollout must be removable");

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let result = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("missing rollout must produce a degraded stream");
    let diagnostics = result.diagnostics.clone();
    assert_eq!(result.records.count(), 0);
    assert!(
        diagnostics
            .snapshot()
            .expect("diagnostics must remain readable")
            .iter()
            .any(
                |warning| warning.kind == AdapterWarningKind::IncompleteCapability
                    && warning.stage == "missing_rollout"
            )
    );
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn unknown_and_truncated_events_become_warnings() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());

    for scenario in ["unknown-event", "truncated-jsonl"] {
        let source = manifest(&adapter, &fixtures.join(scenario));
        let result = adapter
            .scan(request(source, TableName::Messages, &["message_id"]))
            .expect("recoverable rollout must scan");
        assert_eq!(result.records.count(), 2);
        assert!(
            !result
                .diagnostics
                .snapshot()
                .expect("diagnostics must be readable")
                .is_empty()
        );
    }
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn probe_reports_compatible_schema_drift_and_rejects_missing_identity() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());

    let added = adapter
        .probe(&ProbeRequest {
            data_root: fixtures.join("added-column").to_string_lossy().into_owned(),
        })
        .expect("added optional columns must remain compatible");
    assert!(
        added.manifests[0]
            .warnings
            .contains(&"unknown_optional_columns".to_string())
    );

    let unknown_version = adapter
        .probe(&ProbeRequest {
            data_root: fixtures
                .join("unknown-version")
                .to_string_lossy()
                .into_owned(),
        })
        .expect("an unknown version with a compatible schema may degrade");
    assert!(
        unknown_version.manifests[0]
            .format_fingerprint
            .contains("user-version-99")
    );
    assert!(
        unknown_version.manifests[0]
            .warnings
            .contains(&"unrecognized_user_version".to_string())
    );

    let error = adapter
        .probe(&ProbeRequest {
            data_root: fixtures
                .join("missing-critical")
                .to_string_lossy()
                .into_owned(),
        })
        .expect_err("missing rollout identity must be rejected");
    assert!(matches!(error, AdapterError::UnsupportedFormat { .. }));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn missing_optional_columns_are_null_with_a_warning() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let probe = adapter
        .probe(&ProbeRequest {
            data_root: fixtures
                .join("missing-optional")
                .to_string_lossy()
                .into_owned(),
        })
        .expect("missing optional fields must degrade");
    assert!(
        probe.manifests[0]
            .warnings
            .contains(&"missing_optional_columns".to_string())
    );
    let mut scan = request(
        probe.manifests[0].clone(),
        TableName::Sessions,
        &["preview"],
    );
    scan.access.content = true;
    let records = adapter
        .scan(scan)
        .expect("optional projection must remain queryable")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("records must be valid");
    let CanonicalRecord::Session(session) = &records[0] else {
        panic!("expected a session record");
    };
    assert!(session.preview.is_none());
    assert!(!session.provenance.contains_key("preview"));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn active_wal_read_preserves_database_and_wal() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let database = root.join("sqlite/state_5.sqlite");
    let writer = Connection::open(&database).expect("fixture writer must open");
    writer
        .pragma_update(None, "journal_mode", "WAL")
        .expect("fixture must enter WAL mode");
    writer
        .pragma_update(None, "wal_autocheckpoint", 0)
        .expect("fixture autocheckpoint must be disabled");
    writer
        .execute(
            "UPDATE threads SET updated_at_ms = updated_at_ms + 1 WHERE id = 'session-minimal'",
            [],
        )
        .expect("fixture WAL write must succeed");

    let wal = database.with_file_name("state_5.sqlite-wal");
    let shm = database.with_file_name("state_5.sqlite-shm");
    assert!(wal.is_file());
    assert!(shm.is_file());
    let database_before = file_digest(&database);
    let wal_before = file_digest(&wal);
    let sidecars_before = directory_entries(database.parent().expect("database has a parent"));

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let records = adapter
        .scan(request(
            source,
            TableName::Sessions,
            &["session_id", "updated_at"],
        ))
        .expect("active WAL metadata must be readable")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("active WAL records must be valid");
    assert_eq!(records.len(), 1);
    assert_eq!(
        database_before,
        file_digest(&database),
        "AQL must not modify DB"
    );
    assert_eq!(wal_before, file_digest(&wal), "AQL must not modify WAL");
    assert_eq!(
        sidecars_before,
        directory_entries(database.parent().expect("database has a parent")),
        "AQL must not create a journal, WAL, SHM, or cache sidecar"
    );
    assert!(shm.is_file(), "the writer-owned SHM must remain present");

    drop(writer);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[cfg(unix)]
#[test]
fn source_tree_can_be_queried_when_it_is_not_writable() {
    use std::os::unix::fs::PermissionsExt;

    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    set_tree_mode(&root, 0o555, 0o444);
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let records = adapter
        .scan(request(
            source,
            TableName::Sessions,
            &["session_id", "model"],
        ))
        .expect("read-only source must be queryable")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("read-only records must be valid");
    assert_eq!(records.len(), 1);
    set_tree_mode(&root, 0o755, 0o644);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");

    fn set_tree_mode(root: &Path, directory_mode: u32, file_mode: u32) {
        for entry in walk(root) {
            let mode = if entry.is_dir() {
                directory_mode
            } else {
                file_mode
            };
            std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(mode))
                .expect("fixture permissions must be mutable");
        }
    }

    fn walk(root: &Path) -> Vec<PathBuf> {
        let mut paths = vec![root.to_path_buf()];
        let mut index = 0;
        while index < paths.len() {
            let path = paths[index].clone();
            index += 1;
            if path.is_dir() {
                paths.extend(
                    std::fs::read_dir(&path)
                        .expect("fixture directory must be readable")
                        .map(|entry| entry.expect("fixture entry must be readable").path()),
                );
            }
        }
        paths.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        paths
    }
}

fn file_digest(path: &Path) -> Vec<u8> {
    let bytes = std::fs::read(path).expect("fixture sidecar must be readable");
    Sha256::digest(bytes).to_vec()
}

fn directory_entries(path: &Path) -> BTreeSet<String> {
    std::fs::read_dir(path)
        .expect("fixture directory must be readable")
        .map(|entry| {
            entry
                .expect("fixture entry must be readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

#[test]
fn session_time_range_is_exact_and_unsafe_limit_is_not_applied() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("large-metadata"));

    let mut ranged = request(source.clone(), TableName::Sessions, &["session_id"]);
    ranged.predicates = vec![Predicate::Range {
        column: ColumnName::new("updated_at"),
        lower: Some(Literal::Integer(1_767_225_650_000)),
        upper: None,
    }];
    let ranged_result = adapter.scan(ranged).expect("range scan must succeed");
    assert_eq!(
        ranged_result.pushdown.predicates,
        vec![PushdownState::Exact]
    );
    assert_eq!(ranged_result.records.count(), 51);

    let mut unsupported = request(source, TableName::Sessions, &["session_id"]);
    unsupported.predicates = vec![Predicate::Unsupported("synthetic_filter".to_string())];
    unsupported.limit = Some(1);
    let unsupported_result = adapter
        .scan(unsupported)
        .expect("unsupported predicate must be left to the engine");
    assert_eq!(
        unsupported_result.pushdown.limit,
        Some(PushdownState::Unsupported)
    );
    assert_eq!(unsupported_result.records.count(), 100);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn unprojected_content_is_skipped_instead_of_deserialized() {
    let fixtures = fixture_root();
    let rollout = fixtures.join("minimal/sessions/2026/01/01/rollout-minimal.jsonl");
    let original = std::fs::read_to_string(&rollout).expect("fixture rollout must be readable");
    let modified = original.replacen(
        r#""content":[{"type":"input_text","text":"Synthetic question"}]"#,
        r#""content":{"synthetic_invalid_shape":true}"#,
        1,
    );
    assert_ne!(modified, original);
    std::fs::write(&rollout, modified).expect("synthetic fixture rollout must be writable");

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let safe = adapter
        .scan(request(
            source.clone(),
            TableName::Messages,
            &["message_id", "role"],
        ))
        .expect("unprojected content must be skipped");
    assert_eq!(safe.records.count(), 2);

    let mut sensitive = request(source, TableName::Messages, &["content"]);
    sensitive.access.content = true;
    let mut sensitive = adapter
        .scan(sensitive)
        .expect("creating a sensitive stream must not read the source");
    assert!(matches!(
        sensitive.records.next(),
        Some(Err(AdapterError::CorruptSource { .. }))
    ));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn authorized_title_reads_session_index_and_reports_conflict() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("conflict"));
    let mut scan = request(source, TableName::Sessions, &["session_id", "title"]);
    scan.access.content = true;
    let result = adapter
        .scan(scan)
        .expect("authorized title scan must succeed");
    assert_eq!(result.records.count(), 1);
    assert_eq!(
        result
            .diagnostics
            .snapshot()
            .expect("diagnostics must be readable")
            .len(),
        1
    );
    assert_eq!(observer.count(SourceKind::SessionIndex), 1);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn message_limit_stops_rollout_reading_early() {
    let fixtures = fixture_root();

    let full_observer = Arc::new(Observer::default());
    let full_adapter =
        CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(full_observer.clone());
    let full_source = manifest(&full_adapter, &fixtures.join("minimal"));
    let full = full_adapter
        .scan(request(
            full_source,
            TableName::Messages,
            &["message_id", "role"],
        ))
        .expect("full message scan must succeed");
    assert_eq!(full.records.count(), 2);

    let limited_observer = Arc::new(Observer::default());
    let limited_adapter =
        CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(limited_observer.clone());
    let limited_source = manifest(&limited_adapter, &fixtures.join("minimal"));
    let mut limited_request = request(limited_source, TableName::Messages, &["message_id", "role"]);
    limited_request.limit = Some(1);
    let limited = limited_adapter
        .scan(limited_request)
        .expect("limited message scan must succeed");
    assert_eq!(limited.records.count(), 1);
    assert!(limited_observer.bytes(SourceKind::Rollout) < full_observer.bytes(SourceKind::Rollout));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn rollout_stream_does_not_read_source_until_consumed() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let mut result = adapter
        .scan(request(
            source,
            TableName::Messages,
            &["message_id", "role"],
        ))
        .expect("creating a rollout stream must succeed");

    assert_eq!(
        observer.bytes(SourceKind::Rollout),
        0,
        "scan must not eagerly consume rollout bytes"
    );
    assert!(
        result
            .records
            .next()
            .transpose()
            .expect("first record is valid")
            .is_some()
    );
    let after_first = observer.bytes(SourceKind::Rollout);
    assert!(after_first > 0);
    drop(result.records);

    assert_eq!(
        observer.bytes(SourceKind::Rollout),
        after_first,
        "dropping the stream must stop all source consumption"
    );
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn session_stream_opens_database_one_page_at_a_time() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("multi-source"));
    let opens_after_probe = observer.count(SourceKind::StateDatabase);
    let mut result = adapter
        .scan(request(source, TableName::Sessions, &["session_id"]))
        .expect("creating a session stream must succeed");

    assert_eq!(observer.count(SourceKind::StateDatabase), opens_after_probe);
    assert!(
        result
            .records
            .next()
            .transpose()
            .expect("first page is valid")
            .is_some()
    );
    assert_eq!(
        observer.count(SourceKind::StateDatabase),
        opens_after_probe + 1
    );
    drop(result.records);
    assert_eq!(
        observer.count(SourceKind::StateDatabase),
        opens_after_probe + 1,
        "dropping the stream must prevent another page query"
    );
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn session_edges_stream_explicit_cycles_and_warns_for_dangling_children() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("edges"));
    assert!(source.capabilities.contains(&"session_edges".to_string()));
    let opens_after_probe = observer.count(SourceKind::StateDatabase);
    let result = adapter
        .scan(request(
            source,
            TableName::SessionEdges,
            &[
                "edge_id",
                "parent_session_id",
                "child_session_id",
                "edge_kind",
            ],
        ))
        .expect("edge stream must be created");
    assert_eq!(observer.count(SourceKind::StateDatabase), opens_after_probe);
    let diagnostics = result.diagnostics.clone();
    let records = result
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("synthetic edges must be valid");
    assert_eq!(records.len(), 3);
    assert_eq!(
        diagnostics
            .snapshot()
            .expect("diagnostics must remain readable")
            .len(),
        1
    );
    assert!(records.iter().any(|record| {
        matches!(record, CanonicalRecord::SessionEdge(edge) if edge.edge_kind == "running")
    }));
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn rollout_byte_budget_is_checked_before_unbounded_scan() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let mut scan = request(source, TableName::Messages, &["message_id"]);
    scan.budget.max_bytes_read = 1;
    let mut result = adapter
        .scan(scan)
        .expect("creating a budgeted stream must not read the source");
    assert_eq!(observer.bytes(SourceKind::Rollout), 0);
    assert!(matches!(
        result.records.next(),
        Some(Err(AdapterError::BudgetExceeded { .. }))
    ));
    assert_eq!(observer.bytes(SourceKind::Rollout), 0);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn checkpointed_wal_is_readable_without_creating_sidecars() {
    let fixtures = fixture_root();
    let root = fixtures.join("checkpointed-wal");
    let database = root.join("sqlite/state_5.sqlite");
    let wal = database.with_file_name("state_5.sqlite-wal");
    let shm = database.with_file_name("state_5.sqlite-shm");
    assert!(
        !wal.exists() && !shm.exists(),
        "fixture must be cleanly checkpointed"
    );
    let entries_before = directory_entries(database.parent().expect("database has a parent"));

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let records = adapter
        .scan(request(
            source.clone(),
            TableName::Sessions,
            &["session_id", "tokens_used"],
        ))
        .expect("checkpointed WAL metadata must be readable")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("checkpointed WAL records must be valid");
    assert_eq!(records.len(), 1);

    let messages = adapter
        .scan(request(
            source,
            TableName::Messages,
            &["message_id", "role"],
        ))
        .expect("checkpointed WAL rollout locators must resolve")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("checkpointed WAL messages must be valid");
    assert_eq!(messages.len(), 2);

    assert_eq!(
        entries_before,
        directory_entries(database.parent().expect("database has a parent")),
        "AQL must not create a journal, WAL, SHM, or cache sidecar"
    );
    assert!(!wal.exists() && !shm.exists(), "no sidecar may appear");
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn hot_wal_without_shm_fails_closed_without_creating_sidecars() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let database = root.join("sqlite/state_5.sqlite");
    let writer = Connection::open(&database).expect("fixture writer must open");
    writer
        .pragma_update(None, "journal_mode", "WAL")
        .expect("fixture must enter WAL mode");
    writer
        .pragma_update(None, "wal_autocheckpoint", 0)
        .expect("fixture autocheckpoint must be disabled");
    writer
        .execute(
            "UPDATE threads SET updated_at_ms = updated_at_ms + 1 WHERE id = 'session-minimal'",
            [],
        )
        .expect("fixture WAL write must succeed");
    let wal = database.with_file_name("state_5.sqlite-wal");
    assert!(wal.is_file());

    // A copied hot WAL has no shared-memory index; opening it would force
    // SQLite to run recovery and create sidecars next to the source.
    let copied = fixtures.join("hot-wal-copy");
    fs::create_dir_all(copied.join("sqlite")).expect("copied sqlite directory");
    fs::copy(&database, copied.join("sqlite/state_5.sqlite")).expect("copy database");
    fs::copy(&wal, copied.join("sqlite/state_5.sqlite-wal")).expect("copy hot WAL");
    let entries_before = directory_entries(&copied.join("sqlite"));

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let error = adapter
        .probe(&ProbeRequest {
            data_root: copied.to_string_lossy().into_owned(),
        })
        .expect_err("hot WAL must fail closed");
    assert!(
        matches!(error, AdapterError::SnapshotUnavailable),
        "hot WAL must not report a misleading format error: {error}"
    );
    assert_eq!(
        entries_before,
        directory_entries(&copied.join("sqlite")),
        "failing closed must not create, checkpoint, or delete sidecars"
    );
    assert!(
        !copied.join("sqlite/state_5.sqlite-shm").exists(),
        "no SHM sidecar may appear"
    );

    drop(writer);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn hostile_rollout_locators_fail_closed() {
    for locator in [
        "../escape.jsonl",
        "sessions/../escape.jsonl",
        "sessions/../../outside/escape.jsonl",
        "/absolute/rollout.jsonl",
        r"C:\absolute\rollout.jsonl",
        "other/rollout.jsonl",
        "archived_sessions/../escape.jsonl",
        "sessions",
        "",
    ] {
        let fixtures = fixture_root();
        let root = fixtures.join("minimal");
        Connection::open(root.join("sqlite/state_5.sqlite"))
            .expect("fixture database must open")
            .execute("UPDATE threads SET rollout_path = ?1", [locator])
            .expect("fixture locator must update");
        let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
        let source = manifest(&adapter, &root);
        let mut result = adapter
            .scan(request(source, TableName::Messages, &["message_id"]))
            .expect("creating a rollout stream must not read the source");
        assert!(
            matches!(
                result.records.next(),
                Some(Err(AdapterError::CorruptSource { .. }))
            ),
            "locator {locator:?} must fail closed"
        );
        drop(result);
        fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
    }
}

#[cfg(unix)]
#[test]
fn rollout_open_rejects_root_replacement_after_database_open() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let moved = fixtures.join("bound-root");
    let replacement = fixtures.join("replacement-root");
    let replacement_rollout = replacement.join("sessions/2026/01/01/rollout-minimal.jsonl");
    fs::create_dir_all(replacement_rollout.parent().expect("rollout parent"))
        .expect("replacement rollout parent");
    fs::write(
        &replacement_rollout,
        b"{\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"replacement\"}}\n",
    )
    .expect("replacement rollout");
    let observer = Arc::new(RootReplacingObserver {
        root: root.clone(),
        moved,
        replacement,
        replaced: Mutex::new(false),
    });
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer);
    let source = manifest(&adapter, &root);
    let mut result = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("creating a rollout stream remains lazy");
    assert!(matches!(
        result.records.next(),
        Some(Err(AdapterError::SnapshotUnavailable))
    ));
    drop(result);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[cfg(unix)]
#[test]
fn symlink_rollout_component_fails_closed() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let outside = fixtures.join("outside.jsonl");
    fs::write(&outside, b"{\"type\":\"session_meta\",\"payload\":{}}\n")
        .expect("outside rollout must be writable");
    fs::remove_file(root.join("sessions/2026/01/01/rollout-minimal.jsonl"))
        .expect("original rollout must be removable");
    std::os::unix::fs::symlink(
        &outside,
        root.join("sessions/2026/01/01/rollout-minimal.jsonl"),
    )
    .expect("rollout symlink must be creatable");
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut result = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("creating a rollout stream must not read the source");
    assert!(matches!(
        result.records.next(),
        Some(Err(AdapterError::PermissionDenied { .. }))
    ));
    drop(result);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[cfg(unix)]
#[test]
fn probe_rejects_symlink_and_group_writable_roots() {
    use std::os::unix::fs::PermissionsExt;

    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());

    let link = fixtures.join("root-link");
    std::os::unix::fs::symlink(&root, &link).expect("root symlink must be creatable");
    let error = adapter
        .probe(&ProbeRequest {
            data_root: link.to_string_lossy().into_owned(),
        })
        .expect_err("symlink root must be rejected");
    assert!(matches!(error, AdapterError::UnsupportedFormat { .. }));

    fs::set_permissions(&root, fs::Permissions::from_mode(0o775))
        .expect("fixture permissions must be mutable");
    let error = adapter
        .probe(&ProbeRequest {
            data_root: root.to_string_lossy().into_owned(),
        })
        .expect_err("group-writable root must be rejected");
    assert!(matches!(error, AdapterError::PermissionDenied { .. }));
    fs::set_permissions(&root, fs::Permissions::from_mode(0o755))
        .expect("fixture permissions must be restorable");
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn database_replacement_or_shrink_invalidates_the_bound_snapshot() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let database = root.join("sqlite/state_5.sqlite");
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let token = source
        .snapshot
        .clone()
        .expect("probe must bind a snapshot token");
    assert!(
        token.as_str().starts_with("codex-snapshot:"),
        "token must be identity-derived: {}",
        token.as_str()
    );

    // Replacement keeps the same schema but changes the database identity.
    // Create the replacement while the original still exists so Unix filesystems
    // cannot reuse the original inode for the replacement fixture.
    let replacement = database.with_file_name("state_5.sqlite.replacement");
    fs::copy(
        fixtures.join("separate-root-a/sqlite/state_5.sqlite"),
        &replacement,
    )
    .expect("replacement database must copy");
    fs::remove_file(&database).expect("original database must be removable");
    fs::rename(&replacement, &database).expect("replacement database must move into place");
    let mut result = adapter
        .scan(request(
            source.clone(),
            TableName::Sessions,
            &["session_id"],
        ))
        .expect("creating a session stream must not read the source");
    assert!(matches!(
        result.records.next(),
        Some(Err(AdapterError::SnapshotUnavailable))
    ));
    drop(result);

    let rebound = manifest(&adapter, &root);
    assert_ne!(
        rebound.snapshot, source.snapshot,
        "replacement must produce a fresh snapshot token"
    );

    // Shrinking the bound database also fails closed.
    fs::OpenOptions::new()
        .write(true)
        .open(&database)
        .expect("database must open for truncation")
        .set_len(4)
        .expect("database must truncate");
    let mut result = adapter
        .scan(request(rebound, TableName::Sessions, &["session_id"]))
        .expect("creating a session stream must not read the source");
    assert!(matches!(
        result.records.next(),
        Some(Err(AdapterError::SnapshotUnavailable))
    ));
    drop(result);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn newest_state_database_is_selected_deterministically() {
    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let database = root.join("sqlite/state_5.sqlite");
    fs::copy(&database, root.join("sqlite/state_4.sqlite")).expect("older sibling must copy");
    fs::copy(&database, root.join("sqlite/state_6.sqlite")).expect("newer sibling must copy");
    for (version, title) in [
        ("state_4.sqlite", "Synthetic older database"),
        ("state_6.sqlite", "Synthetic newer database"),
    ] {
        Connection::open(root.join("sqlite").join(version))
            .expect("sibling database must open")
            .execute("UPDATE threads SET title = ?1", [title])
            .expect("sibling title must update");
    }

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut scan = request(source, TableName::Sessions, &["session_id", "title"]);
    scan.access.content = true;
    let records = adapter
        .scan(scan)
        .expect("multi-version scan must succeed")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("multi-version records must be valid");
    assert_eq!(records.len(), 1);
    let CanonicalRecord::Session(session) = &records[0] else {
        panic!("expected a session record");
    };
    assert_eq!(session.title.as_deref(), Some("Synthetic newer database"));
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn session_scan_reuses_one_connection_and_reads_the_index_once() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("large-metadata"));
    let opens_after_probe = observer.count(SourceKind::StateDatabase);
    let mut scan = request(source, TableName::Sessions, &["session_id", "title"]);
    scan.access.content = true;
    let records = adapter
        .scan(scan)
        .expect("paged session scan must succeed")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("paged session records must be valid");
    assert_eq!(records.len(), 100);
    assert_eq!(
        observer.count(SourceKind::StateDatabase),
        opens_after_probe + 1,
        "one scan must open one connection"
    );
    assert_eq!(
        observer.count(SourceKind::SessionIndex),
        1,
        "the session index must be read once per scan"
    );
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn rollout_scan_reuses_one_connection_across_files() {
    let fixtures = fixture_root();
    let observer = Arc::new(Observer::default());
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec()).with_observer(observer.clone());
    let source = manifest(&adapter, &fixtures.join("edges"));
    let opens_after_probe = observer.count(SourceKind::StateDatabase);
    let records = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("multi-file rollout scan must succeed")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("multi-file rollout records must be valid");
    assert_eq!(records.len(), 4);
    assert_eq!(
        observer.count(SourceKind::StateDatabase),
        opens_after_probe + 1,
        "one rollout scan must open one connection"
    );
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn oversized_session_index_record_fails_the_bounded_read() {
    use std::io::Write;

    let fixtures = fixture_root();
    let root = fixtures.join("minimal");
    let mut oversized = br#"{"id":"session-minimal","thread_name":""#.to_vec();
    oversized.extend(std::iter::repeat_n(b'a', 1024 * 1024 + 1));
    oversized.extend_from_slice(b"\"}\n");
    let mut index = fs::OpenOptions::new()
        .append(true)
        .open(root.join("session_index.jsonl"))
        .expect("session index must open");
    index
        .write_all(&oversized)
        .expect("oversized index line must be written");
    drop(index);

    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut scan = request(source, TableName::Sessions, &["session_id", "title"]);
    scan.access.content = true;
    let mut result = adapter
        .scan(scan)
        .expect("creating a session stream must not read the source");
    assert!(matches!(
        result.records.next(),
        Some(Err(AdapterError::BudgetExceeded { resource, .. }))
            if resource == "codex_index_record_bytes"
    ));
    drop(result);
    fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}
