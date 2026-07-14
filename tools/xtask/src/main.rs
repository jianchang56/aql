use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::{fs, os::unix::fs::MetadataExt, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("error={error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("fixture") => fixture(&mut args)?,
        Some("verify") => verify(args.next().as_deref())?,
        Some("real-smoke") => real_smoke()?,
        _ => return Err(usage().into()),
    }
    Ok(())
}

fn usage() -> &'static str {
    "usage: cargo xtask fixture KIND OUTPUT [COUNT]\n       KIND: claude|codex|kimi|opencode\n       cargo xtask verify [workspace|cli|adapters|release|performance|docs]\n       cargo xtask real-smoke"
}

fn fixture(args: &mut impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    let kind = args.next().ok_or("fixture kind is required")?;
    let output = args.next().ok_or("fixture output is required")?;
    match kind.as_str() {
        "codex" => {
            let count = args
                .next()
                .map(|value| value.parse::<usize>())
                .transpose()?
                .unwrap_or(1_000_000);
            aql_test_support::generate_codex(Path::new(&output), count)?;
        }
        "claude" | "claude-code" => aql_test_support::generate_claude(Path::new(&output))?,
        "kimi" | "kimi-code" => aql_test_support::generate_kimi(Path::new(&output))?,
        "opencode" => aql_test_support::generate_opencode(Path::new(&output))?,
        _ => return Err("unknown fixture kind".into()),
    }
    Ok(())
}

fn verify(scope: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    match scope {
        None => {
            verify_workspace()?;
            verify_docs()?;
            verify_release()?;
            verify_performance()?;
        }
        Some("workspace") => verify_workspace()?,
        Some("cli") => {
            run_cargo(&["test", "--locked", "-p", "aql-cli"])?;
        }
        Some("adapters") => {
            run_cargo(&[
                "test",
                "--locked",
                "-p",
                "aql-adapter-codex",
                "-p",
                "aql-adapter-claude-code",
                "-p",
                "aql-adapter-kimi-code",
                "-p",
                "aql-adapter-opencode",
                "-p",
                "aql-catalog",
            ])?;
        }
        Some("release") => verify_release()?,
        Some("performance") => verify_performance()?,
        Some("docs") => verify_docs()?,
        _ => return Err(usage().into()),
    }
    Ok(())
}

fn verify_workspace() -> Result<(), Box<dyn std::error::Error>> {
    run_cargo(&["fmt", "--all", "--", "--check"])?;
    run_cargo(&[
        "clippy",
        "--locked",
        "--workspace",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ])?;
    run_cargo(&["test", "--locked", "--workspace"])?;
    Ok(())
}

fn verify_release() -> Result<(), Box<dyn std::error::Error>> {
    run_cargo(&["test", "--locked", "-p", "aql-release"])?;
    Ok(())
}

fn verify_performance() -> Result<(), Box<dyn std::error::Error>> {
    run_cargo(&[
        "test",
        "--locked",
        "-p",
        "aql-adapter-codex",
        "rollout_stream_does_not_read_source_until_consumed",
    ])?;
    run_cargo(&[
        "test",
        "--locked",
        "-p",
        "aql-adapter-codex",
        "message_limit_stops_rollout_reading_early",
    ])?;
    run_cargo(&[
        "test",
        "--locked",
        "-p",
        "aql-adapter-api",
        "cancellation_precedes_budget_errors",
    ])?;
    run_cargo(&[
        "test",
        "--locked",
        "-p",
        "aql-adapter-api",
        "cloned_budgets_share_query_usage",
    ])?;
    run_cargo(&[
        "test",
        "--locked",
        "-p",
        "aql-adapter-codex",
        "rollout_byte_budget_is_checked_before_unbounded_scan",
    ])?;
    run_cargo(&[
        "test",
        "--locked",
        "-p",
        "aql-adapter-codex",
        "single_sensitive_value_budget_is_enforced",
    ])?;
    Ok(())
}

fn verify_docs() -> Result<(), Box<dyn std::error::Error>> {
    for path in [
        "README.md",
        "AGENTS.md",
        "docs/README.md",
        "docs/user-guide.md",
        "docs/installation.md",
        "docs/architecture.md",
        "docs/claude-code-format-notes.md",
        "docs/compatibility.md",
        "docs/privacy-threat-model.md",
        ".github/workflows/release.yml",
    ] {
        if !Path::new(path).is_file() {
            return Err(format!("required documentation is missing: {path}").into());
        }
    }
    let forbidden = [
        "fixtures/",
        "scripts/verify_phase",
        "scripts/build_release",
        ".py",
    ];
    for path in [
        "README.md",
        "AGENTS.md",
        "docs/README.md",
        "docs/user-guide.md",
        "docs/installation.md",
        "docs/architecture.md",
    ] {
        let text = std::fs::read_to_string(path)?;
        if let Some(value) = forbidden.iter().find(|value| text.contains(**value)) {
            return Err(format!(
                "current documentation {path} contains obsolete reference {value}"
            )
            .into());
        }
    }
    let release = std::fs::read_to_string(".github/workflows/release.yml")?;
    for required in [
        "contents: read",
        "contents: write",
        "--verify-tag",
        "cargo test --locked",
        "cargo build --locked --release",
        "aql-release -- build",
        "aql-release -- verify",
        "aql-release -- formula",
        "aarch64-linux",
        "aarch64-macos",
        "x86_64-linux",
        "x86_64-macos",
        "--draft=false",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
    ] {
        if !release.contains(required) {
            return Err(format!("release workflow is missing required gate: {required}").into());
        }
    }
    if release.contains("curl ") || release.contains("wget ") || release.contains("sudo ") {
        return Err("release workflow must not use ad-hoc downloads or sudo".into());
    }
    let build_section = release
        .split_once("  build:\n")
        .and_then(|(_, remaining)| remaining.split_once("  publish:\n"))
        .map(|(build, _)| build)
        .ok_or("release workflow build/publish job structure is missing")?;
    if build_section.contains("contents: write") || build_section.contains("GH_TOKEN") {
        return Err("release build job must not hold release write credentials".into());
    }
    for line in release.lines().map(str::trim) {
        if let Some(action) = line.strip_prefix("uses: ") {
            let Some((_, revision)) = action.rsplit_once('@') else {
                return Err("release workflow Action is not pinned".into());
            };
            if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err("release workflow Action must use a full commit SHA".into());
            }
        }
    }
    Ok(())
}

fn real_smoke() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("AQL_RUN_REAL_SMOKE").as_deref() != Ok("1") {
        return Err("real smoke requires explicit AQL_RUN_REAL_SMOKE=1 authorization".into());
    }
    run_cargo(&["build", "--locked", "-p", "aql-cli"])?;
    let home = std::env::var_os("AQL_REAL_HOME")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .ok_or("HOME is required")?;
    let temporary = tempfile::Builder::new()
        .prefix("aql-real-smoke-")
        .tempdir()?;
    let state = temporary.path().join("state");
    let config = temporary.path().join("config");
    run_aql(&home, &state, &config, &["database", "discover"])?;
    let data = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"));
    let candidates = [
        ("claude", "claude-code", home.join(".claude")),
        ("codex", "codex", home.join(".codex")),
        ("kimi", "kimi-code", home.join(".kimi-code")),
        ("opencode", "opencode", data.join("opencode")),
    ];
    let mut checked = 0usize;
    for (database, adapter, root) in candidates {
        if !root.is_dir() {
            continue;
        }
        let before = source_snapshot(adapter, &root)?;
        run_aql(&home, &state, &config, &["doctor", "-d", database])?;
        run_aql(
            &home,
            &state,
            &config,
            &[
                "query",
                "-d",
                database,
                "--output",
                "json",
                "SELECT COUNT(*) AS sessions FROM sessions",
            ],
        )?;
        if source_snapshot(adapter, &root)? != before {
            return Err(format!("real smoke changed {adapter} source metadata").into());
        }
        checked += 1;
        println!("adapter={adapter} status=passed");
    }
    if checked == 0 {
        return Err("no installed Agent data root was available for real smoke".into());
    }
    if config.exists() {
        return Err("real smoke created forbidden configured-database state".into());
    }
    println!("status=real-safe-smoke-passed");
    Ok(())
}

fn run_aql(
    home: &Path,
    state: &Path,
    config: &Path,
    arguments: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("target/debug/aql")
        .args(arguments)
        .env("HOME", home)
        .env("AQL_HOME", state)
        .env("AQL_CONFIG_HOME", config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(format!("aql failed with {status}").into());
    }
    Ok(())
}

#[derive(PartialEq, Eq)]
struct Snapshot(Vec<(String, u64, u64, u32, u64, i64)>);

fn source_snapshot(adapter: &str, root: &Path) -> Result<Snapshot, Box<dyn std::error::Error>> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err("real smoke root is not a no-follow directory".into());
    }
    let mut paths = match adapter {
        "claude-code" => claude_paths(root)?,
        "codex" => [
            "sqlite/state_5.sqlite",
            "sqlite/state_5.sqlite-wal",
            "session_index.jsonl",
        ]
        .into_iter()
        .map(|value| root.join(value))
        .collect(),
        "opencode" => ["opencode.db", "opencode.db-wal"]
            .into_iter()
            .map(|value| root.join(value))
            .collect(),
        "kimi-code" => kimi_paths(root)?,
        _ => return Err("unknown real smoke adapter".into()),
    };
    paths.retain(|path| path.exists());
    if paths.len() > 100_000 {
        return Err("real smoke file-count bound exceeded".into());
    }
    paths.sort();
    let mut snapshot = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("real smoke allowlist contains an unsafe file".into());
        }
        snapshot.push((
            path.strip_prefix(root)?.to_string_lossy().into_owned(),
            metadata.dev(),
            metadata.ino(),
            metadata.mode(),
            metadata.len(),
            metadata.mtime_nsec(),
        ));
    }
    Ok(Snapshot(snapshot))
}

fn kimi_paths(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut paths = vec![root.join("session_index.jsonl")];
    let sessions = root.join("sessions");
    if !sessions.is_dir() {
        return Ok(paths);
    }
    for first in fs::read_dir(sessions)? {
        let first = first?.path();
        if !first.is_dir() {
            continue;
        }
        for second in fs::read_dir(first)? {
            let second = second?.path();
            if second.is_dir() {
                paths.push(second.join("state.json"));
                paths.push(second.join("wire.jsonl"));
            }
        }
    }
    Ok(paths)
}

fn claude_paths(root: &Path) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let projects = root.join("projects");
    if !projects.is_dir() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for project in fs::read_dir(projects)? {
        let project = project?.path();
        let metadata = fs::symlink_metadata(&project)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        for entry in fs::read_dir(project)? {
            let path = entry?.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            paths.push(path);
            if paths.len() > 100_000 {
                return Err("Claude real smoke file-count bound exceeded".into());
            }
        }
    }
    Ok(paths)
}

fn run_cargo(arguments: &[&str]) -> Result<ExitStatus, Box<dyn std::error::Error>> {
    let status = Command::new("cargo").args(arguments).status()?;
    if !status.success() {
        return Err(format!("cargo failed with {status}").into());
    }
    Ok(status)
}
