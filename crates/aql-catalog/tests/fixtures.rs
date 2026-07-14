use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aql_adapter_api::{
    AccessGrant, AgentAdapter, CancellationToken, ColumnName, ProbeRequest, ResourceBudget,
    ScanRequest, TableName,
};
use aql_adapter_codex::CodexAdapter;
use aql_catalog::Catalog;

static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixture_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after epoch")
        .as_nanos();
    let counter = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let output = std::env::temp_dir().join(format!(
        "aql-catalog-fixtures-{}-{nonce}-{counter}",
        std::process::id()
    ));
    aql_test_support::generate_codex(&output, 100).expect("fixture generator must succeed");
    output
}

fn scan_sessions(adapter: &CodexAdapter, root: &Path) -> Vec<aql_model::CanonicalRecord> {
    let source = adapter
        .probe(&ProbeRequest {
            data_root: root.to_string_lossy().into_owned(),
        })
        .expect("fixture probe must succeed")
        .manifests
        .remove(0);
    adapter
        .scan(ScanRequest {
            source,
            table: TableName::Sessions,
            projection: vec![ColumnName::new("session_id"), ColumnName::new("updated_at")],
            predicates: Vec::new(),
            limit: None,
            order_hint: Vec::new(),
            access: AccessGrant::default(),
            budget: ResourceBudget::default(),
            cancellation: CancellationToken::default(),
            snapshot: None,
        })
        .expect("fixture scan must succeed")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("fixture records must be valid")
}

#[test]
fn multi_source_fixture_produces_one_logical_session() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let records = scan_sessions(&adapter, &fixtures.join("multi-source"));
    let result = Catalog.reconcile_sessions(records);
    assert_eq!(result.records.len(), 1);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}

#[test]
fn separate_root_fixtures_produce_distinct_entities() {
    let fixtures = fixture_root();
    let adapter = CodexAdapter::new(b"fixture-salt".to_vec());
    let mut records = scan_sessions(&adapter, &fixtures.join("separate-root-a"));
    records.extend(scan_sessions(&adapter, &fixtures.join("separate-root-b")));
    let result = Catalog.reconcile_sessions(records);
    assert_eq!(result.records.len(), 2);
    assert_ne!(result.records[0].session_id, result.records[1].session_id);
    std::fs::remove_dir_all(fixtures).expect("fixture tree must be removable");
}
