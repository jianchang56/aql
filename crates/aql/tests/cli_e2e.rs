use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::{Duration, Instant};

fn symlink_file(source: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source, target)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(source, target)
    }
}

fn symlink_dir(source: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(source, target)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_dir(source, target)
    }
}

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

fn tree_snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries: Vec<_> = fs::read_dir(&directory)
            .expect("list snapshot directory")
            .map(|entry| entry.expect("read snapshot entry").path())
            .collect();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .expect("snapshot path is below root")
                    .to_string_lossy()
                    .into_owned();
                files.push((relative, fs::read(&path).expect("read snapshot file")));
            }
        }
    }
    files.sort();
    files
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

    let page = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "SELECT table_name FROM aql_tables ORDER BY table_name LIMIT 2 OFFSET 1",
    ]);
    assert_success(&page);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&page.stdout)
            .expect("parse explicit page")
            .as_array()
            .expect("page is an array")
            .len(),
        2
    );

    let float_parameter = environment.run([
        "query",
        "-d",
        "codex",
        "--param",
        "minimum=float:0.5",
        "--output",
        "json",
        "SELECT session_id FROM sessions WHERE tokens_used > :minimum",
    ]);
    assert_success(&float_parameter);

    let unordered = environment.run([
        "query",
        "-d",
        "codex",
        "SELECT session_id FROM sessions LIMIT 1",
    ]);
    assert_success(&unordered);
    assert!(stderr(&unordered).contains("result ordering is unspecified"));

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
    symlink_file(&outside, &link).expect("create output symlink");
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

    #[cfg(unix)]
    let output_directory = environment.temporary.path().join("replace-parent");
    #[cfg(unix)]
    fs::create_dir(&output_directory).expect("create output parent");
    #[cfg(unix)]
    let target = output_directory.join("result.json");
    #[cfg(unix)]
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
    #[cfg(unix)]
    wait_for_pending_output(&output_directory, &mut child);
    #[cfg(unix)]
    let displaced = environment
        .temporary
        .path()
        .join("replace-parent-displaced");
    #[cfg(unix)]
    fs::rename(&output_directory, &displaced).expect("move original output parent");
    #[cfg(unix)]
    fs::create_dir(&output_directory).expect("replace output parent");
    #[cfg(unix)]
    let replaced = child
        .wait_with_output()
        .expect("wait for parent replacement query");
    #[cfg(unix)]
    assert!(!replaced.status.success());
    #[cfg(unix)]
    assert!(replaced.stdout.is_empty());
    #[cfg(unix)]
    assert!(!target.exists());
    #[cfg(unix)]
    assert!(
        fs::read_dir(&displaced)
            .expect("list displaced output parent")
            .next()
            .is_none(),
        "abandoned private output must be removed"
    );
}

#[test]
fn output_target_is_rejected_before_source_probe() {
    let environment = TestEnvironment::new();
    fs::create_dir(environment.home.join(".codex")).expect("create incompatible Codex root");
    let target = environment.temporary.path().join("already-exists.json");
    fs::write(&target, b"unchanged").expect("create existing output target");

    let result = environment.run([
        "query".into(),
        "-d".into(),
        "codex".into(),
        "--output-file".into(),
        target.as_os_str().to_owned(),
        "SELECT session_id FROM sessions LIMIT 1".into(),
    ]);
    assert_eq!(result.status.code(), Some(4));
    assert!(result.stdout.is_empty());
    assert!(stderr(&result).contains("error_category=already_exists"));
    assert_eq!(
        fs::read(&target).expect("read existing target"),
        b"unchanged"
    );

    let explain = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "EXPLAIN SELECT session_id FROM sessions LIMIT 1",
    ]);
    assert_eq!(explain.status.code(), Some(2));
    assert!(explain.stdout.is_empty());
    assert!(stderr(&explain).contains("error_category=invalid_request"));
}

#[test]
fn symlinked_builtin_candidate_root_fails_on_the_query_path() {
    let environment = TestEnvironment::new();
    let target = environment.standalone_codex("linked-candidate");
    symlink_dir(&target, &environment.home.join(".codex"))
        .expect("create built-in candidate symlink");

    let discover = environment.run(["database", "discover"]);
    assert_success(&discover);
    assert!(stdout(&discover).contains("database=codex status=incompatible"));

    let query = environment.run([
        "query",
        "-d",
        "codex",
        "SELECT session_id FROM sessions LIMIT 1",
    ]);
    assert_eq!(query.status.code(), Some(4));
    assert!(query.stdout.is_empty());
    assert!(stderr(&query).contains("error_category=source_unavailable"));

    let intermediate = TestEnvironment::new();
    intermediate.install_codex("minimal", 0);
    let linked_home = intermediate.temporary.path().join("linked-home");
    symlink_dir(&intermediate.home, &linked_home).expect("create intermediate HOME symlink");
    let all = intermediate
        .command()
        .env("HOME", &linked_home)
        .args([
            "query",
            "-d",
            "all",
            "SELECT session_id FROM sessions LIMIT 1",
        ])
        .output()
        .expect("run all through symlinked HOME");
    assert_eq!(all.status.code(), Some(4));
    assert!(all.stdout.is_empty());
    assert!(stderr(&all).contains("error_category=source_unavailable"));
}

#[test]
fn discover_and_show_mask_real_paths_and_leave_no_residue() {
    let environment = TestEnvironment::new();
    environment.install_codex("minimal", 0);
    // A planted entry outside the four fixed candidates must never be
    // reported: discovery is fixed-candidate and non-recursive.
    fs::write(environment.home.join("stray.txt"), b"synthetic stray")
        .expect("plant stray HOME entry");
    let home_before = tree_snapshot(&environment.home);
    let real_paths = [
        environment.home.to_string_lossy().into_owned(),
        environment.temporary.path().to_string_lossy().into_owned(),
    ];

    let discover = environment.run(["database", "discover"]);
    assert_success(&discover);
    let discovered = stdout(&discover);
    assert_eq!(
        discovered,
        "database=claude status=missing\n\
         database=codex status=compatible\n\
         database=kimi status=missing\n\
         database=opencode status=missing\n"
    );
    assert!(
        !discovered.contains("stray"),
        "discovery reported a non-candidate entry: {discovered}"
    );
    let discover_stderr = stderr(&discover);
    let show = environment.run(["database", "show", "codex"]);
    assert_success(&show);
    let shown = stdout(&show);
    assert_eq!(
        shown,
        "database=codex\nagent=codex\nstatus=compatible\npath=masked\n"
    );
    let show_stderr = stderr(&show);
    for output in [&discovered, &discover_stderr, &shown, &show_stderr] {
        for real_path in &real_paths {
            assert!(
                !output.contains(real_path),
                "discovery output leaked a real path {real_path}: {output}"
            );
        }
    }

    // Discovery and show are read-only probes: no state or config root is
    // created and nothing below HOME changes.
    assert_eq!(
        tree_snapshot(&environment.home),
        home_before,
        "discovery left residue below HOME"
    );
    assert!(
        !environment.state.exists(),
        "discovery must not create the AQL state root"
    );
    assert!(
        !environment.config.exists(),
        "discovery must not create the AQL config root"
    );
}

#[test]
fn configured_database_show_masks_member_roots_without_path_access() {
    let environment = TestEnvironment::new();
    let codex = environment.standalone_codex("masked-member");
    let add = environment.run(vec![
        OsString::from("database"),
        OsString::from("add"),
        OsString::from("masked"),
        OsString::from("--member"),
        OsString::from(format!("codex={}", codex.display())),
        OsString::from("--acknowledge-persistent-path"),
    ]);
    assert_success(&add);

    let show = environment.run(["database", "show", "masked"]);
    assert_success(&show);
    let shown = stdout(&show);
    assert_eq!(
        shown,
        "database=masked\nmembers=1\nmember.1.adapter=codex\nmember.1.root=masked\n"
    );
    let stored_root = codex
        .canonicalize()
        .expect("canonicalize member root")
        .to_string_lossy()
        .into_owned();
    assert!(
        !shown.contains(&stored_root),
        "configured show leaked a member root: {shown}"
    );
    assert!(stderr(&show).is_empty());
}

#[test]
fn query_persists_nothing_beyond_the_preexisting_installation_key() {
    let environment = TestEnvironment::new();
    environment.install_codex("minimal", 0);
    // Pre-create a valid key so this test can assert that queries leave an
    // existing installation identity unchanged.
    fs::create_dir(&environment.state).expect("create isolated state root");
    aql_fs::set_mode(&environment.state, 0o700).expect("state root is private");
    let salt = [7_u8; 32];
    let key = environment.state.join("installation.key");
    fs::write(&key, salt).expect("pre-create installation key");
    aql_fs::set_mode(&key, 0o600).expect("installation key is private");
    let home_before = tree_snapshot(&environment.home);

    let select = environment.run([
        "query",
        "-d",
        "codex",
        "--output",
        "json",
        "SELECT session_id FROM sessions ORDER BY session_id",
    ]);
    assert_success(&select);
    assert!(stdout(&select).contains("session-minimal"));
    let control = environment.run(["query", "-d", "codex", "SHOW TABLES;"]);
    assert_success(&control);

    // The state root holds only the untouched installation key: no SQL text,
    // shell history, query results, grants, or payload copies are persisted.
    let entries: Vec<String> = fs::read_dir(&environment.state)
        .expect("list state root")
        .map(|entry| {
            entry
                .expect("read state entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert_eq!(entries, ["installation.key"]);
    assert_eq!(fs::read(&key).expect("read installation key"), salt);
    assert!(
        !environment.config.exists(),
        "queries must not create the AQL config root"
    );
    assert_eq!(
        tree_snapshot(&environment.home),
        home_before,
        "query left residue below HOME"
    );
}

#[test]
fn relative_aql_home_is_rejected_instead_of_cwd_anchored() {
    let environment = TestEnvironment::new();
    let output = environment
        .command()
        .env("AQL_HOME", "relative-state")
        .args(["database", "list"])
        .output()
        .expect("run database list with relative AQL_HOME");
    assert_eq!(output.status.code(), Some(2));
    assert!(stderr(&output).contains("error_category=invalid_request"));
    assert!(stderr(&output).contains("AQL state root must be absolute"));
}

#[cfg(unix)]
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
