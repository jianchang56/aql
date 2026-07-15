use super::*;
use datafusion::arrow::array::{BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

struct BrokenWriter;

impl Write for BrokenWriter {
    fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "synthetic"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn named_query_parameters_are_scalar_and_unique() {
    let parameters = parse_sql_parameters(&[
        "name=example".to_string(),
        "count=42".to_string(),
        "enabled=true".to_string(),
        "ratio=float:1.5".to_string(),
        "missing=null".to_string(),
        "reserved=text:true".to_string(),
        "numeric_text=text:42".to_string(),
    ])
    .expect("parameters parse");
    assert_eq!(
        parameters["name"],
        SqlParameter::Text("example".to_string())
    );
    assert_eq!(parameters["count"], SqlParameter::Int64(42));
    assert_eq!(parameters["enabled"], SqlParameter::Bool(true));
    assert_eq!(parameters["ratio"], SqlParameter::Float64(1.5));
    assert_eq!(parameters["missing"], SqlParameter::Null);
    assert_eq!(
        parameters["reserved"],
        SqlParameter::Text("true".to_string())
    );
    assert_eq!(
        parameters["numeric_text"],
        SqlParameter::Text("42".to_string())
    );
    assert!(parse_sql_parameters(&["1bad=value".to_string()]).is_err());
    assert!(parse_sql_parameters(&["x=1".to_string(), "x=2".to_string()]).is_err());
    let error = parse_sql_parameters(&["x=bool:yes".to_string()])
        .expect_err("invalid explicit bool must fail");
    assert_eq!(error_exit_code(&error), 2);
    assert_eq!(error_category(&error), "invalid_request");
    assert!(parse_sql_parameters(&["x=float:NaN".to_string()]).is_err());

    Cli::try_parse_from([
        "aql",
        "query",
        "-d",
        "codex",
        "--param",
        "project=demo",
        "SELECT session_id FROM sessions WHERE project = :project",
    ])
    .expect("query parameter CLI parses");
}

#[test]
fn database_cli_is_short_clear_and_mutually_exclusive() {
    let parsed = Cli::try_parse_from([
        "aql",
        "query",
        "-d",
        "codex",
        "SELECT session_id FROM sessions LIMIT 1",
    ])
    .expect("database query syntax must parse");
    let Some(Command::Query { database, .. }) = parsed.command else {
        panic!("query command expected");
    };
    assert_eq!(database, "codex");
    assert!(
        Cli::try_parse_from([
            "aql",
            "database",
            "add",
            "work",
            "--member",
            "codex=/synthetic/codex",
            "--acknowledge-persistent-path",
        ])
        .is_ok()
    );
    assert!(
        Cli::try_parse_from([
            "aql",
            "database",
            "add",
            "work",
            "--agent",
            "codex",
            "--path",
            "/synthetic/codex",
            "--acknowledge-persistent-path",
        ])
        .is_err()
    );
    assert!(
        Cli::try_parse_from([
            "aql",
            "query",
            "--database",
            "codex",
            "--profile",
            "daily",
            "SELECT session_id FROM sessions",
        ])
        .is_err()
    );
}

#[test]
fn public_help_exposes_only_the_focused_command_surface() {
    let help = generated_command().render_long_help().to_string();
    assert!(help.contains("database"));
    assert!(help.contains("schema"));
    assert!(help.contains("examples"));
    assert!(!help.contains("profile"));
    assert!(!help.contains("sources"));
    assert!(!help.contains("action"));
    assert!(!help.contains("export"));
    assert!(!help.contains("report"));
    assert!(!help.contains("search"));
    assert!(!help.contains("index"));
    assert!(Cli::try_parse_from(["aql", "--quiet", "schema"]).is_ok());
}

#[test]
fn data_commands_accept_one_consistent_database_option() {
    for arguments in [
        vec!["aql", "doctor", "-d", "codex"],
        vec![
            "aql",
            "query",
            "-d",
            "codex",
            "--output-file",
            "synthetic.json",
            "SELECT 1",
        ],
    ] {
        Cli::try_parse_from(arguments).expect("database syntax must parse");
    }
    for arguments in [
        vec!["aql", "doctor"],
        vec!["aql", "query", "SELECT 1"],
        vec!["aql", "export", "SELECT 1"],
        vec!["aql", "report", "summary"],
        vec!["aql", "search", "synthetic"],
        vec!["aql", "index", "status"],
        vec!["aql", "profile", "list"],
        vec!["aql", "sources", "discover"],
        vec!["aql", "action", "capabilities"],
    ] {
        assert!(Cli::try_parse_from(arguments).is_err());
    }
}

#[test]
fn human_readable_limits_parse_without_breaking_raw_integers() {
    assert_eq!(parse_count("100k").expect("count parses"), 100_000);
    assert_eq!(parse_count("42").expect("raw count parses"), 42);
    assert_eq!(
        parse_byte_size("256MiB").expect("binary size parses"),
        256 * 1024 * 1024
    );
    assert_eq!(
        parse_byte_size("64MB").expect("decimal size parses"),
        64_000_000
    );
    assert_eq!(parse_byte_size("1024").expect("raw bytes parse"), 1024);
    assert!(parse_byte_size("1.5MiB").is_err());
    Cli::try_parse_from([
        "aql",
        "query",
        "-d",
        "codex",
        "--max-output-bytes",
        "32MiB",
        "SELECT 1",
    ])
    .expect("human-readable CLI limits parse");
    for removed in [
        "--max-records",
        "--max-bytes-read",
        "--max-single-value-bytes",
        "--max-memory-bytes",
    ] {
        assert!(
            Cli::try_parse_from(["aql", "query", "-d", "codex", removed, "1", "SELECT 1"]).is_err()
        );
    }
}

#[test]
fn automation_environment_defaults_exclude_database_and_access() {
    let command = Cli::command();
    let query = command
        .get_subcommands()
        .find(|command| command.get_name() == "query")
        .expect("query command exists");
    let environment = |name: &str| {
        query
            .get_arguments()
            .find(|argument| argument.get_id() == name)
            .and_then(clap::Arg::get_env)
            .map(|value| value.to_string_lossy().into_owned())
    };
    assert_eq!(environment("timeout").as_deref(), Some("AQL_TIMEOUT"));
    assert_eq!(environment("max_records"), None);
    assert_eq!(environment("database"), None);
    assert_eq!(environment("access"), None);
}

#[test]
fn query_sql_inputs_are_mutually_exclusive_and_explain_is_plan_only() {
    Cli::try_parse_from(["aql", "query", "-d", "codex", "--file", "query.aql"])
        .expect("AQL file syntax");
    Cli::try_parse_from(["aql", "query", "-d", "codex", "--stdin"]).expect("stdin SQL syntax");
    assert!(Cli::try_parse_from(["aql", "query", "-d", "codex", "--stdin", "SELECT 1"]).is_err());
    assert_eq!(explain_sql(" EXPLAIN SELECT 1"), Some("SELECT 1"));
    assert_eq!(
        explain_sql("EXPLAIN ANALYZE SELECT 1"),
        Some("ANALYZE SELECT 1")
    );
    assert_eq!(explain_sql("SELECT 1"), None);
    Cli::try_parse_from(["aql", "query", "-d", "codex", "--diagnostics", "SELECT 1"])
        .expect("focused diagnostics flag parses");
    for removed in ["--plan", "--metadata", "--diagnose"] {
        assert!(Cli::try_parse_from(["aql", "query", "-d", "codex", removed, "SELECT 1"]).is_err());
    }
}

#[test]
fn control_queries_rewrite_to_canonical_metadata_selects() {
    assert_eq!(
        rewrite_control_query("SHOW TABLES;").expect("SHOW TABLES rewrites"),
        Some("SELECT table_name, table_kind FROM aql_tables ORDER BY table_name".to_string())
    );
    assert_eq!(
        rewrite_control_query("DESC sessions").expect("DESC rewrites"),
        Some("SELECT column_name, data_type, nullable, access_class FROM aql_columns WHERE table_name = 'sessions' ORDER BY ordinal_position".to_string())
    );
    assert!(rewrite_control_query("DESCRIBE does_not_exist").is_err());
    let error = rewrite_control_query("DESCRIBE does_not_exist")
        .expect_err("unknown control table is rejected");
    assert_eq!(error_stage(&error), "control");
    assert_eq!(error_location(&error), Some((1, 10)));
    let multiline = rewrite_control_query("DESCRIBE\n  does_not_exist")
        .expect_err("multiline unknown control table is rejected");
    assert_eq!(error_location(&multiline), Some((2, 3)));
    assert_eq!(
        rewrite_control_query("SHOW TABLES; SELECT 1").unwrap(),
        None
    );
}

#[test]
fn sql_file_input_is_bounded_regular_and_no_follow() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let query = temporary
        .path()
        .canonicalize()
        .expect("canonical temporary directory")
        .join("query.aql");
    fs::write(&query, b"SELECT 1").expect("write query");
    assert_eq!(
        read_sql_input(None, Some(query.clone()), false).expect("read query"),
        "SELECT 1"
    );
    let link = temporary.path().join("query-link.aql");
    std::os::unix::fs::symlink(&query, &link).expect("query symlink");
    assert!(read_sql_input(None, Some(link), false).is_err());
    let sql_file = temporary.path().join("query.sql");
    fs::write(&sql_file, b"SELECT 1").expect("write legacy SQL extension");
    assert!(read_sql_input(None, Some(sql_file), false).is_err());
    let real_directory = temporary.path().join("real-directory");
    fs::create_dir(&real_directory).expect("create real directory");
    let nested_query = real_directory.join("nested.sql");
    fs::write(&nested_query, b"SELECT 2").expect("write nested query");
    let directory_link = temporary.path().join("directory-link");
    std::os::unix::fs::symlink(&real_directory, &directory_link)
        .expect("intermediate directory symlink");
    assert!(
        read_sql_input(None, Some(directory_link.join("nested.sql")), false).is_err(),
        "SQL file paths must reject intermediate directory symlinks"
    );
    fs::write(&query, vec![b'x'; MAX_SQL_INPUT_BYTES as usize + 1]).expect("write oversized query");
    assert!(read_sql_input(None, Some(query), false).is_err());
}

#[test]
fn errors_have_stable_hints_for_database_workflows() {
    let database = database_not_found("unknown database; run SHOW DATABASES");
    assert_eq!(error_category(&database), "not_found");
    assert_eq!(error_exit_code(&database), 4);
    assert!(error_hint(&database).is_some());

    let misleading = io::Error::other("unknown database; query timed out; invalid");
    assert_eq!(error_category(&misleading), "internal");
    assert_eq!(error_exit_code(&misleading), 1);
    assert!(error_hint(&misleading).is_none());
    assert_eq!(shell_quote("SELECT 1"), "'SELECT 1'");
    assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
}

#[test]
fn command_line_and_shell_errors_offer_contextual_recovery() {
    let missing_database = match Cli::try_parse_from(["aql", "query", "SELECT 1"]) {
        Ok(_) => panic!("database is required"),
        Err(error) => error,
    };
    assert!(cli_parse_error_hint(&missing_database).contains("aql database list"));

    let access = aql_engine_datafusion::QueryError::AccessDenied("content");
    let rendered = shell_query_error(&access);
    assert!(rendered.contains("GRANT CONTENT FOR SESSION;"));
    assert!(!rendered.contains("--access"));
}

#[test]
fn shell_statement_splitter_handles_multiline_literals_and_comments() {
    let mut buffer =
        "SELECT 'semi;colon'\nFROM sessions; -- ignored ;\nSHOW TABLES; tail".to_string();
    let statements = drain_shell_statements(&mut buffer).expect("valid shell statements");
    assert_eq!(
        statements,
        vec![
            "SELECT 'semi;colon'\nFROM sessions",
            "-- ignored ;\nSHOW TABLES"
        ]
    );
    assert_eq!(shell_words(&statements[1]), vec!["SHOW", "TABLES"]);
    assert_eq!(buffer, " tail");

    let mut block = "SELECT /* ; */ session_id FROM sessions;".to_string();
    assert_eq!(
        drain_shell_statements(&mut block).expect("block comment is handled"),
        vec!["SELECT /* ; */ session_id FROM sessions"]
    );
    assert!(block.is_empty());
}

#[test]
fn shell_session_access_is_explicit_temporary_and_bounded() {
    let mut access = Vec::new();
    grant_shell_access(&shell_words("grant content for session"), &mut access)
        .expect("content grant is accepted");
    grant_shell_access(&shell_words("grant tool input for session"), &mut access)
        .expect("tool input grant is accepted");
    grant_shell_access(&shell_words("GRANT CONTENT FOR SESSION"), &mut access)
        .expect("duplicate grant is harmless");
    assert_eq!(access, vec![Access::Content, Access::ToolInput]);
    assert!(grant_shell_access(&shell_words("grant secret for session"), &mut access).is_err());
    assert_eq!(
        shell_prompt(Some("codex"), &access),
        "aql[codex|content+tool-input]> "
    );
    assert_eq!(shell_prompt(None, &[]), "aql[none|safe]> ");
}

#[test]
fn shell_welcome_guides_selection_without_choosing_a_default() {
    let databases = vec!["claude".to_string(), "codex".to_string()];
    let welcome = shell_welcome(&databases, None);
    assert!(welcome.iter().any(|line| line == "1. SHOW DATABASES;"));
    assert!(welcome.iter().any(|line| line == "2. USE <database>;"));
    assert!(
        !welcome
            .iter()
            .any(|line| line.starts_with("Selected database:"))
    );

    let selected = shell_welcome(&databases, Some("codex"));
    assert!(
        selected
            .iter()
            .any(|line| line == "Selected database: codex")
    );
}

#[test]
fn shell_schema_commands_use_the_engine_canonical_schema() {
    assert!(QUERY_SCHEMAS.iter().any(|schema| schema.name == "sessions"));
    let sessions = QUERY_SCHEMAS
        .iter()
        .find(|schema| schema.name == "sessions")
        .expect("sessions schema exists");
    assert!(
        sessions
            .columns
            .iter()
            .any(|column| column.name == "session_id")
    );
}

#[test]
fn schema_and_examples_have_explicit_list_modes() {
    assert!(Cli::try_parse_from(["aql", "schema", "--list"]).is_ok());
    assert!(Cli::try_parse_from(["aql", "schema", "sessions", "--list"]).is_err());
    assert!(Cli::try_parse_from(["aql", "examples", "--list"]).is_ok());
    assert!(Cli::try_parse_from(["aql", "examples", "token-usage", "--list"]).is_err());
}

#[test]
fn database_members_reject_unknown_duplicate_and_overlap_before_probe() {
    let root =
        std::env::temp_dir().join(format!("aql-source-parse-{:016x}", rand::random::<u64>()));
    let nested = root.join("nested");
    fs::create_dir_all(&nested).expect("create synthetic source roots");
    let absolute = root.to_string_lossy();
    let nested_absolute = nested.to_string_lossy();
    assert!(parse_source_specs(vec![format!("unknown={absolute}")]).is_err());
    assert!(parse_source_specs(vec![format!("claude-code={absolute}")]).is_ok());
    assert!(parse_source_specs(vec![format!("opencode={absolute}")]).is_ok());
    assert!(parse_source_specs(vec!["codex=relative".to_string()]).is_err());
    assert!(
        parse_source_specs(vec![
            format!("codex={absolute}"),
            format!("kimi-code={absolute}")
        ])
        .is_err()
    );
    assert!(
        parse_source_specs(vec![
            format!("codex={absolute}"),
            format!("kimi-code={nested_absolute}")
        ])
        .is_err()
    );
    fs::remove_dir_all(root).expect("remove synthetic source roots");
}

#[test]
fn broken_pipe_cancels_without_panicking() {
    let cancellation = CancellationToken::default();
    write_rendered(&mut BrokenWriter, "synthetic", &cancellation)
        .expect("broken pipe is a normal pipeline termination");
    assert!(cancellation.is_cancelled());
}

#[test]
fn json_output_preserves_arrow_types_and_json_columns() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("ok", DataType::Boolean, false),
            Field::new("arguments", DataType::Utf8, true),
            Field::new("missing", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec![Some("{\"synthetic\":true}")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .expect("synthetic batch must be valid");
    let rows = batches_to_values(&[batch]).expect("typed JSON conversion must succeed");
    assert_eq!(rows[0]["count"], serde_json::json!(7));
    assert_eq!(rows[0]["ok"], serde_json::json!(true));
    assert_eq!(rows[0]["arguments"], serde_json::json!({"synthetic": true}));
    assert_eq!(rows[0]["missing"], serde_json::Value::Null);
}

#[test]
fn structured_renderers_stop_at_the_output_limit() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["synthetic-output"]))],
    )
    .expect("synthetic batch");
    assert!(batches_to_json_limited(std::slice::from_ref(&batch), 8).is_err());
    assert!(batches_to_jsonl_limited(std::slice::from_ref(&batch), 8).is_err());
    assert!(batches_to_csv_limited(std::slice::from_ref(&batch), 8).is_err());
    assert!(batches_to_table_limited(std::slice::from_ref(&batch), 8).is_err());
}

#[test]
fn streaming_renderers_preserve_multi_batch_format_boundaries() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let first = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["one", "two"]))],
    )
    .expect("first synthetic batch");
    let second = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["three"]))])
        .expect("second synthetic batch");
    let batches = vec![first, second];

    let (json, json_summary) =
        render_batches_for_test(Output::Json, &batches, u64::MAX).expect("streaming JSON");
    assert_eq!(
        json,
        "[{\"value\":\"one\"},{\"value\":\"two\"},{\"value\":\"three\"}]\n"
    );
    assert_eq!(json_summary.returned_rows, 3);

    let (jsonl, _) =
        render_batches_for_test(Output::Jsonl, &batches, u64::MAX).expect("streaming JSONL");
    assert_eq!(
        jsonl,
        "{\"value\":\"one\"}\n{\"value\":\"two\"}\n{\"value\":\"three\"}\n"
    );

    let (csv, _) = render_batches_for_test(Output::Csv, &batches, u64::MAX).expect("streaming CSV");
    assert_eq!(csv, "value\r\none\r\ntwo\r\nthree\r\n");

    let (table, _) =
        render_batches_for_test(Output::Table, &batches, u64::MAX).expect("streaming table");
    let expected = format!(
        "{}\n",
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).expect("reference table")
    );
    assert_eq!(table, expected);
}

#[cfg(unix)]
#[test]
fn streaming_render_failure_never_publishes_the_output_target() {
    let root =
        std::env::temp_dir().join(format!("aql-stream-output-{:016x}", rand::random::<u64>()));
    fs::create_dir(&root).expect("create synthetic output directory");

    let first = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "first",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["one"]))],
    )
    .expect("first synthetic batch");
    let second = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "second",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["two"]))],
    )
    .expect("second synthetic batch");

    let inconsistent_target = root.join("inconsistent.csv");
    {
        let mut output = TransactionalOutput::create(Some(&inconsistent_target))
            .expect("create transactional output");
        let mut renderer = StreamingRenderer::new(Output::Csv, ResourceBudget::default())
            .expect("create renderer");
        renderer.start(output.writer()).expect("start renderer");
        renderer
            .write_batch(output.writer(), &first)
            .expect("first batch renders");
        assert!(renderer.write_batch(output.writer(), &second).is_err());
    }
    assert!(!inconsistent_target.exists());

    let budget_target = root.join("budget.json");
    {
        let mut output =
            TransactionalOutput::create(Some(&budget_target)).expect("create budget output");
        let budget = ResourceBudget {
            max_output_bytes: 8,
            ..ResourceBudget::default()
        };
        let mut renderer =
            StreamingRenderer::new(Output::Json, budget).expect("create budget renderer");
        renderer
            .start(output.writer())
            .expect("start JSON renderer");
        assert!(renderer.write_batch(output.writer(), &first).is_err());
    }
    assert!(!budget_target.exists());
    assert_eq!(
        fs::read_dir(&root).expect("list output directory").count(),
        0
    );
    fs::remove_dir_all(root).expect("clean synthetic output directory");
}

#[test]
fn csv_output_preserves_null_empty_literal_and_rfc4180_text() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("\\N"),
            Some("comma,quote\"\nline"),
            Some("Unicode-中文"),
        ]))],
    )
    .expect("synthetic batch must be valid");

    let csv = batches_to_csv(&[batch]).expect("CSV conversion must succeed");
    assert_eq!(
        csv.rendered,
        "value\r\n\\N\r\n\"\"\r\n\"\\N\"\r\n\"comma,quote\"\"\nline\"\r\nUnicode-中文\r\n"
    );
    assert!(!csv.formula_escaped);
}

#[test]
fn csv_output_is_always_formula_safe() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "title",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![
            "=cmd", "+sum", "-1+2", "@name", "\tcell", "\rcell", "safe",
        ]))],
    )
    .expect("synthetic batch must be valid");

    let safe = batches_to_csv(&[batch]).expect("safe CSV conversion must succeed");
    assert_eq!(
        safe.rendered,
        "title\r\n'=cmd\r\n'+sum\r\n'-1+2\r\n'@name\r\n'\tcell\r\n\"'\rcell\"\r\nsafe\r\n"
    );
    assert!(safe.formula_escaped);

    assert!(
        Cli::try_parse_from([
            "aql",
            "query",
            "-d",
            "codex",
            "--csv-formulas",
            "raw",
            "SELECT 1",
        ])
        .is_err()
    );
}

#[test]
fn csv_output_preserves_typed_json_and_timestamp_values() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("ok", DataType::Boolean, false),
            Field::new("arguments", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(StringArray::from(vec!["{\"z\":1,\"a\":2}"])),
            Arc::new(TimestampMillisecondArray::from(vec![1767225600000])),
        ],
    )
    .expect("synthetic batch must be valid");

    let csv = batches_to_csv(&[batch]).expect("typed CSV conversion must succeed");
    assert_eq!(
        csv.rendered,
        "count,ok,arguments,created_at\r\n7,true,\"{\"\"a\"\":2,\"\"z\"\":1}\",2026-01-01T00:00:00+00:00\r\n"
    );
    assert!(!csv.formula_escaped);
}

#[test]
fn csv_output_rejects_controls_and_inconsistent_schemas() {
    let control = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["bad\u{0007}value"]))],
    )
    .expect("synthetic batch must be valid");
    assert!(batches_to_csv(&[control]).is_err());

    let first = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "first",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["one"]))],
    )
    .expect("first synthetic batch must be valid");
    let second = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "second",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec!["two"]))],
    )
    .expect("second synthetic batch must be valid");
    assert!(batches_to_csv(&[first, second]).is_err());
}

#[test]
fn generated_release_docs_are_deterministic_and_exclude_internal_arguments() {
    generated_command().debug_assert();
    let generated = generated_command();
    let generated_query = generated
        .get_subcommands()
        .find(|command| command.get_name() == "query")
        .expect("generated query command exists");
    assert!(
        generated_query
            .get_arguments()
            .all(|argument| argument.get_id() != "shell_summary")
    );
    let generated_database_add = generated
        .get_subcommands()
        .find(|command| command.get_name() == "database")
        .and_then(|command| {
            command
                .get_subcommands()
                .find(|command| command.get_name() == "add")
        })
        .expect("generated database add command exists");
    assert!(
        generated_database_add
            .get_arguments()
            .find(|argument| argument.get_id() == "member")
            .is_some_and(clap::Arg::is_required_set)
    );

    let contains_token = |text: &str, token: &str| {
        text.split(|character: char| {
            !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
        })
        .any(|word| word == token)
    };
    for shell in [
        CompletionShell::Bash,
        CompletionShell::Zsh,
        CompletionShell::Fish,
    ] {
        let first = render_completions(shell);
        let second = render_completions(shell);
        assert_eq!(first, second);
        let text = String::from_utf8(first).expect("completion output must be UTF-8");
        for internal in [
            "action",
            "export",
            "report",
            "search",
            "index",
            "profile",
            "sources",
            "--profile",
            "--source",
            "--data-root",
            "synthetic-channel-root",
            "synthetic-fault",
        ] {
            assert!(
                !contains_token(&text, internal),
                "completion output exposed hidden surface: {internal}"
            );
        }
    }

    let first = render_manpage().expect("man page generation must succeed");
    let second = render_manpage().expect("man page generation must succeed");
    assert_eq!(first, second);
    let text = String::from_utf8(first).expect("man page must be UTF-8");
    for internal in [
        "ACTION",
        "EXPORT",
        "REPORT",
        "SEARCH",
        "INDEX",
        "PROFILE",
        "SOURCES",
        "--profile",
        "--source",
        "--data-root",
        "synthetic-channel-root",
        "synthetic-fault",
    ] {
        assert!(
            !contains_token(&text, internal),
            "man page exposed hidden surface: {internal}"
        );
    }
}

#[test]
fn version_metadata_is_stable_and_host_clean() {
    let first = render_version(VersionOutput::Json).expect("JSON version must render");
    let second = render_version(VersionOutput::Json).expect("JSON version must render");
    assert_eq!(first, second);
    let metadata: serde_json::Value =
        serde_json::from_str(&first).expect("version output must be JSON");
    assert_eq!(metadata["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(metadata["canonical_schema"], "aql-canonical-v0");
    assert_eq!(metadata["config_schema"], CONFIG_SCHEMA_VERSION);
    for forbidden in ["HOME", "USER", "workspace", "timestamp", "dirty", "secret"] {
        assert!(!first.contains(forbidden));
    }
}

#[test]
fn agents_plan_capability_is_manifest_derived() {
    assert!(source_supports_table(&[], "agents"));
    assert!(!source_supports_table(&[], "sessions"));
    assert!(source_supports_table(&["sessions".to_string()], "sessions"));
}

#[test]
fn stable_error_categories_have_stable_exit_codes() {
    assert_eq!(
        error_exit_code(&aql_engine_datafusion::QueryError::SqlRejected {
            stage: "parse",
            reason: "synthetic",
        }),
        2
    );
    assert_eq!(
        error_exit_code(&aql_engine_datafusion::QueryError::AccessDenied("content")),
        3
    );
    assert_eq!(
        error_exit_code(&aql_adapter_api::AdapterError::NotFound {
            stage: "synthetic".to_string(),
        }),
        4
    );
    assert_eq!(
        error_exit_code(&aql_adapter_api::AdapterError::BudgetExceeded {
            resource: "records".to_string(),
            actual: 1,
        }),
        5
    );
    let memory = aql_engine_datafusion::QueryError::Engine(
        datafusion::error::DataFusionError::ResourcesExhausted("synthetic".to_string()),
    );
    assert_eq!(error_category(&memory), "resource_limit");
    assert_eq!(error_exit_code(&memory), 5);

    let nested_budget =
        aql_engine_datafusion::QueryError::Engine(datafusion::error::DataFusionError::Context(
            "synthetic context".to_string(),
            Box::new(datafusion::error::DataFusionError::External(Box::new(
                aql_adapter_api::AdapterError::BudgetExceeded {
                    resource: "bytes_read".to_string(),
                    actual: 2,
                },
            ))),
        ));
    assert_eq!(error_category(&nested_budget), "resource_limit");
    assert_eq!(error_exit_code(&nested_budget), 5);

    let nested_source =
        aql_engine_datafusion::QueryError::Engine(datafusion::error::DataFusionError::External(
            Box::new(aql_adapter_api::AdapterError::UnsupportedFormat {
                stage: "synthetic".to_string(),
            }),
        ));
    assert_eq!(error_category(&nested_source), "source_unavailable");
    assert_eq!(error_exit_code(&nested_source), 4);

    let timeout = deadline_exceeded("synthetic deadline");
    assert_eq!(error_category(&timeout), "deadline_exceeded");
    assert_eq!(error_exit_code(&timeout), 5);
    let cancelled = query_cancelled();
    assert_eq!(error_category(&cancelled), "cancelled");
    assert_eq!(error_exit_code(&cancelled), 130);
    let parameters = aql_engine_datafusion::QueryError::SqlRejected {
        stage: "parameters",
        reason: "synthetic",
    };
    assert_eq!(error_stage(&parameters), "parameters");
    assert!(error_hint(&parameters).is_some());
    assert_eq!(error_location(&parameters), None);
}

#[test]
fn installation_salt_uses_private_no_follow_state() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let root = std::env::temp_dir().join(format!(
        "aql-installation-salt-{:016x}",
        rand::random::<u64>()
    ));
    fs::create_dir(&root).expect("create synthetic parent");
    let root = root.canonicalize().expect("canonicalize synthetic parent");
    let state_root = root.join("state");
    let first = load_or_create_installation_salt(&state_root).expect("salt is created");
    let second = load_or_create_installation_salt(&state_root).expect("salt is reloaded");
    assert_eq!(first, second);
    assert_eq!(first.len(), 32);
    assert_eq!(
        fs::metadata(&state_root)
            .expect("state root metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let salt_path = state_root.join("installation.key");
    assert_eq!(
        fs::metadata(&salt_path)
            .expect("salt metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    fs::remove_file(&salt_path).expect("remove private salt");
    let outside = root.join("outside.key");
    fs::write(&outside, [7_u8; 32]).expect("create outside file");
    symlink(&outside, &salt_path).expect("create salt symlink");
    assert!(load_or_create_installation_salt(&state_root).is_err());
    assert_eq!(
        fs::read(&outside).expect("outside file remains"),
        [7_u8; 32]
    );

    let real_parent = root.join("real-parent");
    fs::create_dir(&real_parent).expect("create real parent");
    let linked_parent = root.join("linked-parent");
    symlink(&real_parent, &linked_parent).expect("create intermediate symlink");
    assert!(load_or_create_installation_salt(&linked_parent.join("state")).is_err());
    assert!(!real_parent.join("state").exists());
    fs::remove_dir_all(root).expect("clean synthetic state");
}

#[cfg(unix)]
#[test]
fn secure_output_is_private_atomic_and_never_overwrites() {
    use std::os::unix::fs::PermissionsExt;

    let root =
        std::env::temp_dir().join(format!("aql-secure-output-{:016x}", rand::random::<u64>()));
    fs::create_dir(&root).expect("create synthetic output directory");
    let target = root.join("report.json");

    let mut output = SecureOutputFile::create(&target).expect("create private temp");
    output.writer().write_all(b"first").expect("write temp");
    output.commit().expect("atomic initial commit");
    assert_eq!(fs::read(&target).expect("read committed file"), b"first");
    assert_eq!(
        fs::metadata(&target)
            .expect("stat committed file")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert!(SecureOutputFile::create(&target).is_err());
    assert_eq!(
        fs::read_dir(&root).expect("list output directory").count(),
        1
    );
    fs::remove_dir_all(root).expect("clean synthetic output directory");
}

#[cfg(unix)]
#[test]
fn secure_output_rejects_symlinks_and_cleans_abandoned_temps() {
    use std::os::unix::fs::symlink;

    let root =
        std::env::temp_dir().join(format!("aql-secure-output-{:016x}", rand::random::<u64>()));
    fs::create_dir(&root).expect("create synthetic output directory");
    let outside = root.join("outside.json");
    fs::write(&outside, b"outside").expect("create outside file");
    let link = root.join("link.json");
    symlink(&outside, &link).expect("create synthetic symlink");
    assert!(SecureOutputFile::create(&link).is_err());
    assert_eq!(
        fs::read(&outside).expect("outside remains readable"),
        b"outside"
    );

    let abandoned = root.join("abandoned.json");
    {
        let mut output = SecureOutputFile::create(&abandoned).expect("create abandoned temp");
        output
            .writer()
            .write_all(b"partial")
            .expect("write partial");
    }
    assert!(!abandoned.exists());
    assert_eq!(
        fs::read_dir(&root)
            .expect("list output directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("aql-output"))
            .count(),
        0
    );

    let real_parent = root.join("real-parent");
    fs::create_dir(&real_parent).expect("create real parent");
    let linked_parent = root.join("linked-parent");
    symlink(&real_parent, &linked_parent).expect("create intermediate directory symlink");
    assert!(SecureOutputFile::create(&linked_parent.join("nested.json")).is_err());

    fs::remove_dir_all(root).expect("clean synthetic output directory");
}

#[cfg(unix)]
#[test]
fn secure_output_detects_target_and_parent_replacement() {
    let root =
        std::env::temp_dir().join(format!("aql-secure-output-{:016x}", rand::random::<u64>()));
    fs::create_dir(&root).expect("create synthetic output directory");
    let target = root.join("report.json");
    let mut output = SecureOutputFile::create(&target).expect("open target");
    output
        .writer()
        .write_all(b"new")
        .expect("write pending output");
    fs::write(&target, b"attacker").expect("replace target identity");
    assert!(output.commit().is_err());
    assert_eq!(fs::read(&target).expect("read changed target"), b"attacker");

    let parent_target = root.join("parent.json");
    let mut parent_output =
        SecureOutputFile::create(&parent_target).expect("open parent test output");
    parent_output
        .writer()
        .write_all(b"new")
        .expect("write parent test output");
    let moved = root.with_extension("moved");
    fs::rename(&root, &moved).expect("move original directory");
    fs::create_dir(&root).expect("replace directory path");
    assert!(parent_output.commit().is_err());
    assert!(!parent_target.exists());
    fs::remove_dir_all(root).expect("clean replacement directory");
    fs::remove_dir_all(moved).expect("clean original directory");
}
