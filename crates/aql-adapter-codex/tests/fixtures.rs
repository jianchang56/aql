use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const EXPECTED_FIXTURES: [&str; 15] = [
    "added-column",
    "artifacts",
    "conflict",
    "empty",
    "edges",
    "large-metadata",
    "minimal",
    "missing-critical",
    "missing-optional",
    "multi-profile-a",
    "multi-profile-b",
    "multi-source",
    "truncated-jsonl",
    "unknown-event",
    "unknown-version",
];

fn temporary_directory(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("aql-fixtures-{suffix}-{nonce}"))
}

fn generate(output: &Path) {
    aql_test_support::generate_codex(output, 100).expect("fixture generator must succeed");
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
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .expect("fixture path must be below root")
                    .to_string_lossy()
                    .into_owned();
                let content = fs::read(path).expect("fixture file must be readable");
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

#[test]
fn committed_fixture_sources_contain_only_synthetic_literals() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../aql-test-support");
    let generator = [
        root.join("src/codex.rs"),
        root.join("assets/codex-schema.sql"),
    ]
    .into_iter()
    .map(|path| fs::read_to_string(path).expect("fixture source must be readable"))
    .collect::<String>();
    let forbidden_patterns = [
        ["/", "Users", "/"].concat(),
        ["@", "example.com"].concat(),
        ["BEGIN ", "PRIVATE KEY"].concat(),
        ["s", "k", "-"].concat(),
    ];
    for forbidden in forbidden_patterns {
        assert!(
            !generator.contains(&forbidden),
            "fixture generator contains forbidden literal {forbidden}"
        );
    }
    assert!(generator.contains("Synthetic"));
    assert!(generator.contains("/workspace/example"));
}
