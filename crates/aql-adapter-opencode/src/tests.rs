use std::sync::atomic::{AtomicU64, Ordering};

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
