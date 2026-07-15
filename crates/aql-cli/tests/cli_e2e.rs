#![cfg(unix)]

use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct TestEnvironment {
    temporary: tempfile::TempDir,
    home: PathBuf,
    state: PathBuf,
    config: PathBuf,
    data: PathBuf,
}

impl TestEnvironment {
    fn new() -> Self {
        let temporary = tempfile::tempdir().expect("create isolated CLI environment");
        let root = temporary
            .path()
            .canonicalize()
            .expect("canonicalize isolated CLI environment");
        let home = root.join("home");
        let state = root.join("state");
        let config = root.join("config");
        let data = root.join("data");
        fs::create_dir(&home).expect("create isolated HOME");
        fs::create_dir(&data).expect("create isolated data home");
        Self {
            temporary,
            home,
            state,
            config,
            data,
        }
    }

    fn install_codex(&self, fixture: &str, count: usize) -> PathBuf {
        let generated = self.temporary.path().join(format!("codex-{fixture}"));
        aql_test_support::generate_codex(&generated, count).expect("generate Codex fixtures");
        let target = self.home.join(".codex");
        fs::rename(generated.join(fixture), &target).expect("install synthetic Codex root");
        target
    }

    fn install_kimi(&self, fixture: &str) -> PathBuf {
        let generated = self.temporary.path().join(format!("kimi-{fixture}"));
        aql_test_support::generate_kimi(&generated).expect("generate Kimi fixtures");
        let target = self.home.join(".kimi-code");
        fs::rename(generated.join(fixture), &target).expect("install synthetic Kimi root");
        target
    }

    fn standalone_codex(&self, name: &str) -> PathBuf {
        let generated = self
            .temporary
            .path()
            .join(format!("standalone-codex-{name}"));
        aql_test_support::generate_codex(&generated, 0).expect("generate standalone Codex root");
        generated.join("minimal")
    }

    fn standalone_kimi(&self, name: &str) -> PathBuf {
        let generated = self
            .temporary
            .path()
            .join(format!("standalone-kimi-{name}"));
        aql_test_support::generate_kimi(&generated).expect("generate standalone Kimi root");
        generated.join("minimal")
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aql"));
        command
            .env("HOME", &self.home)
            .env("AQL_HOME", &self.state)
            .env("AQL_CONFIG_HOME", &self.config)
            .env("XDG_DATA_HOME", &self.data)
            .env_remove("AQL_ERROR_FORMAT")
            .env_remove("AQL_MAX_RECORDS")
            .env_remove("AQL_MAX_BYTES_READ")
            .env_remove("AQL_MAX_OUTPUT_BYTES")
            .env_remove("AQL_MAX_SINGLE_VALUE_BYTES")
            .env_remove("AQL_MAX_MEMORY_BYTES")
            .env_remove("AQL_TIMEOUT")
            .stdin(Stdio::null());
        command
    }

    fn run<I, S>(&self, arguments: I) -> Output
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        self.command()
            .args(arguments)
            .output()
            .expect("run aql binary")
    }
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is UTF-8")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout={}\nstderr={}",
        stdout(output),
        stderr(output)
    );
}

#[test]
fn query_formats_bare_output_file_and_budgets_are_end_to_end() {
    let environment = TestEnvironment::new();
    environment.install_codex("minimal", 0);
    let sql = "SELECT session_id FROM sessions ORDER BY session_id";

    for format in ["json", "jsonl", "csv"] {
        let output = environment.run(["query", "-d", "codex", "--output", format, sql]);
        assert_success(&output);
        assert!(stdout(&output).contains("session-minimal"));
        if format == "csv" {
            assert!(stdout(&output).starts_with("session_id\r\n"));
        } else if format == "json" {
            assert!(stdout(&output).starts_with("[{\"session_id\":"));
        } else {
            assert!(stdout(&output).starts_with("{\"session_id\":"));
        }
    }
    let table = environment.run(["query", "-d", "codex", sql]);
    assert_success(&table);
    assert!(stdout(&table).contains("| session_id"));
    assert!(stdout(&table).contains("session-minimal"));

    let metadata = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "SELECT table_name FROM aql_tables ORDER BY table_name",
    ]);
    assert_success(&metadata);
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata.stdout).expect("parse metadata output");
    let table_names = metadata
        .as_array()
        .expect("metadata output is an array")
        .iter()
        .filter_map(|row| row["table_name"].as_str())
        .collect::<Vec<_>>();
    assert!(table_names.contains(&"aql_columns"));
    assert!(table_names.contains(&"sessions"));

    let source_metadata = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "SELECT agent_id, format_fingerprint FROM aql_sources",
    ]);
    assert_success(&source_metadata);
    assert!(stdout(&source_metadata).contains("codex"));

    let show_tables = environment.run(["query", "-d", "codex", "SHOW TABLES;"]);
    assert_success(&show_tables);
    assert!(stdout(&show_tables).contains("aql_columns"));
    assert!(stdout(&show_tables).contains("sessions"));

    let describe = environment.run(["query", "-d", "codex", "DESCRIBE sessions;"]);
    assert_success(&describe);
    assert!(stdout(&describe).contains("session_id"));
    assert!(stdout(&describe).contains("VARCHAR"));

    let bad_describe = environment.run([
        "--error-format",
        "json",
        "query",
        "-d",
        "codex",
        "DESCRIBE missing_table",
    ]);
    assert_eq!(bad_describe.status.code(), Some(2));
    let error: serde_json::Value =
        serde_json::from_slice(&bad_describe.stderr).expect("parse structured SQL error");
    assert_eq!(error["category"], "invalid_request");
    assert_eq!(error["stage"], "control");
    assert_eq!(error["location"]["line"], 1);
    assert!(error["hint"].is_null());

    let functions = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "SELECT replace(agent_id, 'codex', 'agent') AS normalized, date_part('year', created_at) AS year FROM sessions",
    ]);
    assert_success(&functions);
    assert!(stdout(&functions).contains("agent"));

    let script = environment
        .home
        .parent()
        .expect("isolated root")
        .join("query.aql");
    fs::write(&script, "SELECT session_id FROM sessions LIMIT 1;")
        .expect("write synthetic AQL script");
    let scripted = environment.run([
        "query".into(),
        "-d".into(),
        "codex".into(),
        "--output".into(),
        "json".into(),
        "--file".into(),
        script.as_os_str().to_owned(),
    ]);
    assert_success(&scripted);
    assert!(stdout(&scripted).contains("session-minimal"));

    let legacy = environment
        .home
        .parent()
        .expect("isolated root")
        .join("query.sql");
    fs::write(&legacy, "SELECT session_id FROM sessions LIMIT 1;")
        .expect("write legacy extension script");
    let rejected_script = environment.run([
        "query".into(),
        "-d".into(),
        "codex".into(),
        "--file".into(),
        legacy.as_os_str().to_owned(),
    ]);
    assert_eq!(rejected_script.status.code(), Some(2));
    assert!(rejected_script.stdout.is_empty());

    let output_directory = environment.temporary.path().join("bare-output");
    fs::create_dir(&output_directory).expect("create bare output directory");
    let output = environment
        .command()
        .current_dir(&output_directory)
        .args([
            "query",
            "-d",
            "codex",
            "--output",
            "json",
            "--output-file",
            "result.json",
            sql,
        ])
        .output()
        .expect("run bare output query");
    assert_success(&output);
    assert!(output.stdout.is_empty());
    let bare_result =
        fs::read_to_string(output_directory.join("result.json")).expect("read bare output");
    assert!(bare_result.starts_with("[{\"session_id\":"));
    assert!(bare_result.contains("session-minimal"));

    let limited = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "--max-output-bytes",
        "8",
        sql,
    ]);
    assert_eq!(limited.status.code(), Some(5));
    assert!(limited.stdout.is_empty());
    assert!(stderr(&limited).contains("error_category=resource_limit"));

    let limited_target = environment.temporary.path().join("limited.json");
    let limited_file = environment.run([
        "query".into(),
        "-d".into(),
        "codex".into(),
        "--output".into(),
        "json".into(),
        "--max-output-bytes".into(),
        "8".into(),
        "--output-file".into(),
        limited_target.as_os_str().to_owned(),
        sql.into(),
    ]);
    assert_eq!(limited_file.status.code(), Some(5));
    assert!(limited_file.stdout.is_empty());
    assert!(!limited_target.exists());
}

#[test]
fn explain_stdout_file_and_format_rejection_are_end_to_end() {
    let environment = TestEnvironment::new();
    environment.install_codex("minimal", 0);
    let sql = "EXPLAIN SELECT session_id FROM sessions";

    let stdout_plan = environment.run(["query", "-d", "codex", sql]);
    assert_success(&stdout_plan);
    assert!(stdout(&stdout_plan).contains("plan.tables=sessions"));
    assert!(stdout(&stdout_plan).contains("plan.source_id="));

    let target = environment.temporary.path().join("plan.txt");
    let file_plan = environment.run([
        "query".into(),
        "-d".into(),
        "codex".into(),
        "--output-file".into(),
        target.as_os_str().to_owned(),
        sql.into(),
    ]);
    assert_success(&file_plan);
    assert!(file_plan.stdout.is_empty());
    assert!(
        fs::read_to_string(&target)
            .expect("read EXPLAIN file")
            .contains("plan.tables=sessions")
    );

    let rejected = environment.run(["query", "-d", "codex", "--output", "json", sql]);
    assert_eq!(rejected.status.code(), Some(2));
    assert!(rejected.stdout.is_empty());
    assert!(stderr(&rejected).contains("error_category=invalid_request"));
}

#[test]
fn explicit_all_and_failed_multi_member_doctor_are_transactional() {
    let environment = TestEnvironment::new();
    environment.install_codex("minimal", 0);
    environment.install_kimi("minimal");

    let all = environment.run([
        "query",
        "-d",
        "all",
        "--output",
        "json",
        "SELECT agent_id, COUNT(*) AS sessions FROM sessions GROUP BY agent_id ORDER BY agent_id",
    ]);
    assert_success(&all);
    let rows: serde_json::Value = serde_json::from_slice(&all.stdout).expect("parse all output");
    let rows = rows.as_array().expect("all output is an array");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row["agent_id"] == "codex"));
    assert!(rows.iter().any(|row| row["agent_id"] == "kimi-code"));

    let codex = environment.standalone_codex("doctor");
    let kimi = environment.standalone_kimi("doctor");
    let add_args = vec![
        OsString::from("database"),
        OsString::from("add"),
        OsString::from("pair"),
        OsString::from("--member"),
        OsString::from(format!("codex={}", codex.display())),
        OsString::from("--member"),
        OsString::from(format!("kimi={}", kimi.display())),
        OsString::from("--acknowledge-persistent-path"),
    ];
    let add = environment.run(add_args);
    assert_success(&add);
    fs::remove_dir_all(&kimi).expect("remove second configured member");
    let doctor = environment.run(["doctor", "-d", "pair"]);
    assert!(!doctor.status.success());
    assert!(doctor.stdout.is_empty());
}

#[test]
fn timeout_symlink_and_parent_replacement_publish_nothing() {
    let environment = TestEnvironment::new();
    environment.install_codex("large-metadata", 50_000);
    let sql = "SELECT session_id FROM sessions ORDER BY session_id";

    let timed_out = environment.run([
        "query",
        "-d",
        "codex",
        "--timeout",
        "1ns",
        "--output",
        "json",
        sql,
    ]);
    assert_eq!(timed_out.status.code(), Some(5));
    assert!(timed_out.stdout.is_empty());
    assert!(stderr(&timed_out).contains("error_category=deadline_exceeded"));

    let outside = environment.temporary.path().join("outside.json");
    fs::write(&outside, b"unchanged").expect("create outside target");
    let link = environment.temporary.path().join("result-link.json");
    std::os::unix::fs::symlink(&outside, &link).expect("create output symlink");
    let symlinked = environment.run([
        "query".into(),
        "-d".into(),
        "codex".into(),
        "--output-file".into(),
        link.as_os_str().to_owned(),
        "SELECT session_id FROM sessions LIMIT 1".into(),
    ]);
    assert!(!symlinked.status.success());
    assert!(symlinked.stdout.is_empty());
    assert_eq!(
        fs::read(&outside).expect("read outside target"),
        b"unchanged"
    );

    let output_directory = environment.temporary.path().join("replace-parent");
    fs::create_dir(&output_directory).expect("create output parent");
    let target = output_directory.join("result.json");
    let mut child = environment
        .command()
        .env("AQL_MAX_RECORDS", "100000")
        .args([
            "query".into(),
            "-d".into(),
            "codex".into(),
            "--output".into(),
            "json".into(),
            "--output-file".into(),
            target.as_os_str().to_owned(),
            sql.into(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn parent replacement query");
    wait_for_pending_output(&output_directory, &mut child);
    let displaced = environment
        .temporary
        .path()
        .join("replace-parent-displaced");
    fs::rename(&output_directory, &displaced).expect("move original output parent");
    fs::create_dir(&output_directory).expect("replace output parent");
    let replaced = child
        .wait_with_output()
        .expect("wait for parent replacement query");
    assert!(!replaced.status.success());
    assert!(replaced.stdout.is_empty());
    assert!(!target.exists());
    assert!(
        fs::read_dir(&displaced)
            .expect("list displaced output parent")
            .next()
            .is_none(),
        "abandoned private output must be removed"
    );
}

fn wait_for_pending_output(directory: &Path, child: &mut std::process::Child) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pending = fs::read_dir(directory)
            .expect("list output parent")
            .filter_map(Result::ok)
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".aql-output-")
            });
        if pending {
            return;
        }
        assert!(
            child.try_wait().expect("poll query").is_none(),
            "query exited before the output parent could be replaced"
        );
        assert!(
            Instant::now() < deadline,
            "timed out waiting for private output file"
        );
        thread::sleep(Duration::from_millis(1));
    }
}
