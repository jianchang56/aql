use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const EXPECTED_FIXTURES: [&str; 26] = [
    "active-boundary",
    "full",
    "future-protocol",
    "huge-sensitive",
    "huge-wire",
    "invalid-state",
    "legacy-1.0",
    "malformed-record",
    "minimal",
    "mismatched-bucket",
    "missing-index",
    "missing-protocol",
    "missing-state",
    "missing-workdir",
    "multi-session",
    "root-replacement",
    "root-replacement-replacement",
    "stale-index",
    "subagent",
    "symlink-agent-dir",
    "symlink-sessions-dir",
    "symlink-state",
    "symlink-wire",
    "truncated-tail",
    "unpaired-tools",
    "unknown-record",
];

fn temporary_directory(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("aql-fixtures-{suffix}-{nonce}"))
}

fn generate(output: &Path) {
    aql_test_support::generate_kimi(output).expect("fixture generator must succeed");
}

fn logical_snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn visit(root: &Path, current: &Path, files: &mut Vec<(String, Vec<u8>)>) {
        let mut entries: Vec<_> = fs::read_dir(current)
            .expect("fixture directory must be readable")
            .map(|entry| entry.expect("fixture entry must be readable"))
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .expect("fixture path must be below root")
                .to_string_lossy()
                .into_owned();
            // Compare links by their stored target instead of following them:
            // the relative targets are themselves deterministic fixture bytes,
            // and Windows cannot resolve forward-slash link targets at all.
            let metadata = fs::symlink_metadata(&path)
                .unwrap_or_else(|error| panic!("fixture entry {path:?} must stat: {error}"));
            if metadata.file_type().is_symlink() {
                let target = fs::read_link(&path).unwrap_or_else(|error| {
                    panic!("fixture link {path:?} must be readable: {error}")
                });
                files.push((relative, target.as_os_str().as_encoded_bytes().to_vec()));
            } else if metadata.is_dir() {
                visit(root, &path, files);
            } else {
                let content = fs::read(&path).unwrap_or_else(|error| {
                    panic!("fixture file {path:?} must be readable: {error}")
                });
                files.push((relative, content));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, root, &mut files);
    files
}

#[test]
fn generates_all_scenarios_deterministically() {
    let first = temporary_directory("first");
    let second = temporary_directory("second");
    generate(&first);
    generate(&second);

    let names: BTreeSet<_> = fs::read_dir(&first)
        .expect("generated root must be readable")
        .map(|entry| {
            entry
                .expect("fixture entry must be readable")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert_eq!(names, EXPECTED_FIXTURES.map(str::to_string).into());
    assert_eq!(logical_snapshot(&first), logical_snapshot(&second));

    fs::remove_dir_all(first).expect("first fixture tree must be removable");
    fs::remove_dir_all(second).expect("second fixture tree must be removable");
}
