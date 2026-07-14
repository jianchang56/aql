use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, io, io::IsTerminal, io::Read, io::Write};

use aql_adapter_api::{
    AccessGrant, AgentAdapter, CancellationToken, ColumnName, ProbeRequest, ResourceBudget,
    ScanRequest, TableName,
};
use aql_adapter_claude_code::ClaudeCodeAdapter;
use aql_adapter_codex::CodexAdapter;
use aql_adapter_kimi_code::KimiCodeAdapter;
use aql_adapter_opencode::OpenCodeAdapter;
use aql_config::{
    CONFIG_SCHEMA_VERSION, ConfigError, ConfigStore, Database as ConfiguredDatabase, DatabaseMember,
};
use aql_engine_datafusion::{
    FederatedSource, QUERY_SCHEMAS, QueryDataType, QueryOptions, SqlParameter, bind_sql_parameters,
    prepare_query, validate_read_only_sql,
};
use aql_model::AccessClass;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};

mod cli_args;
mod database;
mod output;
mod render;
mod shell;

use cli_args::*;
use database::*;
use output::SecureOutputFile;
#[cfg(test)]
use render::batches_to_values;
use render::{batches_to_csv, batches_to_json, batches_to_jsonl};
use shell::*;

#[derive(Debug)]
enum CliError {
    InvalidArgument(String),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgument(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for CliError {}

fn invalid_argument(message: impl Into<String>) -> CliError {
    CliError::InvalidArgument(message.into())
}

const MAX_SQL_INPUT_BYTES: u64 = 64 * 1024;

fn read_sql_input(
    sql: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(sql) = sql {
        if sql.len() as u64 > MAX_SQL_INPUT_BYTES {
            return Err("SQL input exceeds the fixed 64 KiB limit".into());
        }
        return Ok(sql);
    }
    if let Some(path) = file {
        if path.to_string_lossy().contains("://") || path.as_os_str() == "-" {
            return Err("--file requires a local file path".into());
        }
        let components = path.components().collect::<Vec<_>>();
        let Some(std::path::Component::Normal(file_name)) = components.last() else {
            return Err("SQL file path must name one regular file".into());
        };
        let directory_flags = rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC;
        let mut directory = rustix::fs::openat(
            rustix::fs::CWD,
            if path.is_absolute() { "/" } else { "." },
            directory_flags,
            rustix::fs::Mode::empty(),
        )?;
        for component in &components[..components.len() - 1] {
            match component {
                std::path::Component::RootDir | std::path::Component::CurDir => {}
                std::path::Component::Normal(name) => {
                    directory = rustix::fs::openat(
                        &directory,
                        PathBuf::from(name),
                        directory_flags,
                        rustix::fs::Mode::empty(),
                    )?;
                }
                _ => return Err("SQL file path must not contain parent traversal".into()),
            }
        }
        let descriptor = rustix::fs::openat(
            &directory,
            PathBuf::from(file_name),
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let mut file = fs::File::from(descriptor);
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_SQL_INPUT_BYTES {
            return Err("SQL file must be a bounded regular file".into());
        }
        let mut input = String::new();
        Read::by_ref(&mut file)
            .take(MAX_SQL_INPUT_BYTES + 1)
            .read_to_string(&mut input)?;
        if input.len() as u64 != metadata.len() {
            return Err("SQL file changed while it was being read".into());
        }
        return Ok(input);
    }
    if stdin {
        let mut input = String::new();
        io::stdin()
            .lock()
            .take(MAX_SQL_INPUT_BYTES + 1)
            .read_to_string(&mut input)?;
        if input.len() as u64 > MAX_SQL_INPUT_BYTES {
            return Err("stdin SQL exceeds the fixed 64 KiB limit".into());
        }
        return Ok(input);
    }
    Err("one SQL input is required".into())
}

fn explain_sql(sql: &str) -> Option<&str> {
    let trimmed = sql.trim_start();
    let prefix = trimmed.get(..7)?;
    if prefix.eq_ignore_ascii_case("EXPLAIN")
        && trimmed
            .as_bytes()
            .get(7)
            .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        Some(trimmed[7..].trim_start())
    } else {
        None
    }
}

#[tokio::main]
async fn main() {
    let requested_format = requested_error_format();
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            return;
        }
        Err(error) => {
            let exit_code = error.exit_code();
            let hint = cli_parse_error_hint(&error);
            match requested_format {
                ErrorFormat::Text => {
                    let _ = error.print();
                    eprintln!("hint={hint}");
                }
                ErrorFormat::Json => eprintln!(
                    "{}",
                    serde_json::json!({
                        "category": "invalid_request",
                        "message": "invalid command-line arguments",
                        "hint": hint,
                        "exit_code": exit_code,
                    })
                ),
            }
            std::process::exit(exit_code);
        }
    };
    let error_format = cli.error_format;
    if let Err(error) = run(cli).await {
        render_error(error.as_ref(), error_format);
        std::process::exit(error_exit_code(error.as_ref()));
    }
}

fn cli_parse_error_hint(error: &clap::Error) -> &'static str {
    if error.kind() == clap::error::ErrorKind::MissingRequiredArgument
        && error.to_string().contains("--database <DATABASE>")
    {
        "run `aql database list`, then retry with `-d <database>`"
    } else {
        "run `aql --help` or `aql <command> --help`"
    }
}

fn requested_error_format() -> ErrorFormat {
    let mut arguments = std::env::args_os().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == "--error-format=json" {
            return ErrorFormat::Json;
        }
        if argument == "--error-format" && arguments.next().is_some_and(|value| value == "json") {
            return ErrorFormat::Json;
        }
    }
    if std::env::var_os("AQL_ERROR_FORMAT").is_some_and(|value| value == "json") {
        return ErrorFormat::Json;
    }
    ErrorFormat::Text
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let quiet = cli.quiet;
    let Some(command) = cli.command else {
        return run_shell(None).await;
    };
    match command {
        Command::Shell { database } => run_shell(database).await?,
        Command::Version { output } => {
            println!("{}", render_version(output)?);
        }
        Command::Completions { shell } => {
            io::stdout().lock().write_all(&render_completions(shell))?;
        }
        Command::Man => {
            io::stdout().lock().write_all(&render_manpage()?)?;
        }
        Command::Doctor { database } => {
            let inputs = resolve_database_inputs(&database)?;
            let installation_salt = installation_salt()?;
            let sources = bind_sources(inputs.source_specs, installation_salt)?;
            for bound in sources {
                let manifest = &bound.manifest;
                println!("agent={}", manifest.agent_id);
                println!("format={}", manifest.format_fingerprint);
                println!("capabilities={}", manifest.capabilities.join(","));
                for warning in &manifest.warnings {
                    println!("warning={warning}");
                }
                let diagnostic_budget = ResourceBudget {
                    max_records: 100,
                    max_bytes_read: 16 * 1024 * 1024,
                    max_output_bytes: 0,
                    max_single_value_bytes: 16 * 1024 * 1024,
                    ..ResourceBudget::default()
                };
                let diagnostics = bound.adapter.scan(ScanRequest {
                    source: manifest.clone(),
                    table: TableName::Sessions,
                    projection: vec![ColumnName::new("session_id")],
                    predicates: Vec::new(),
                    limit: Some(100),
                    order_hint: Vec::new(),
                    access: AccessGrant::default(),
                    budget: diagnostic_budget,
                    cancellation: CancellationToken::default(),
                    snapshot: manifest.snapshot.clone(),
                })?;
                let mut session_records = 0_u64;
                for record in diagnostics.records {
                    record?;
                    session_records += 1;
                }
                println!("session_records={session_records}");
                for warning in diagnostics.diagnostics.snapshot()? {
                    println!("warning={:?}", warning.kind);
                }
            }
        }
        Command::Query {
            database,
            output,
            output_file,
            access,
            param,
            limits,
            diagnostics,
            shell_summary,
            sql,
            file,
            stdin,
        } => {
            execute_query(
                QueryExecution {
                    database,
                    output,
                    output_file,
                    access,
                    param,
                    limits,
                    diagnostics,
                    shell_summary,
                    sql,
                    file,
                    stdin,
                },
                quiet,
            )
            .await?;
        }
        Command::Database { database } => execute_database_command(database)?,
        Command::Schema {
            table,
            list,
            output,
        } => {
            if list {
                render_schema_list(output)?;
            } else {
                render_schema(table, output)?;
            }
        }
        Command::Examples { name, list } => {
            render_examples(if list { None } else { name })?;
        }
    }
    Ok(())
}

struct QueryExecution {
    database: String,
    output: Output,
    output_file: Option<PathBuf>,
    access: Vec<Access>,
    param: Vec<String>,
    limits: ExecutionLimits,
    diagnostics: bool,
    shell_summary: bool,
    sql: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
}

async fn execute_query(
    request: QueryExecution,
    quiet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let QueryExecution {
        database,
        output,
        output_file,
        access,
        param,
        limits,
        diagnostics,
        shell_summary,
        sql,
        file,
        stdin,
    } = request;
    let ExecutionLimits {
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        max_memory_bytes,
        timeout,
    } = limits;
    let parse_started = Instant::now();
    let sql = read_sql_input(sql, file, stdin)?;
    let query_started = Instant::now();
    let (sql, explain) = match explain_sql(&sql) {
        Some(inner) if !inner.is_empty() => (inner, true),
        Some(_) => return Err("EXPLAIN requires one SELECT or WITH query".into()),
        None => (sql.as_str(), false),
    };
    let bound_sql = bind_sql_parameters(sql, &parse_sql_parameters(&param)?)?;
    let validated_sql = validate_read_only_sql(&bound_sql)?;
    diagnostic_timing(diagnostics, "parse", parse_started);
    let (query_budget, _) = execution_budget(
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        timeout,
    )?;
    let mut options = QueryOptions {
        access: access_grant(&access),
        budget: query_budget,
        max_memory_bytes,
        ..QueryOptions::default()
    };
    let authorize_started = Instant::now();
    prepare_query(&validated_sql, options.clone()).await?;
    diagnostic_timing(diagnostics, "authorize", authorize_started);
    let mut secure_output = output_file
        .as_deref()
        .map(SecureOutputFile::create)
        .transpose()?;
    let inputs = resolve_database_inputs(&database)?;
    let installation_salt = installation_salt()?;
    options.redaction_salt = installation_salt.clone();
    let cancellation = options.cancellation.clone();
    let budget = options.budget.clone();
    let prepared = prepare_query(&validated_sql, options).await?;
    let probe_started = Instant::now();
    let sources = bind_sources(inputs.source_specs, installation_salt)?;
    diagnostic_timing(diagnostics, "probe", probe_started);
    if sources.is_empty() {
        return Err("probe returned no compatible source".into());
    }
    if explain {
        let summary = prepared.plan_summary();
        eprintln!("plan.tables={}", summary.tables.join(","));
        eprintln!("plan.columns={}", summary.columns.join(","));
        eprintln!("plan.required_access={}", summary.required_access.join(","));
        for reason in &summary.access_reasons {
            eprintln!("plan.access_reason={reason}");
        }
        for pushdown in &summary.pushdown {
            eprintln!("plan.pushdown={pushdown}");
        }
        eprintln!("plan.max_records={}", summary.max_records);
        eprintln!("plan.max_bytes_read={}", summary.max_bytes_read);
        eprintln!("plan.max_output_bytes={}", summary.max_output_bytes);
        eprintln!("plan.max_memory_bytes={}", summary.max_memory_bytes);
        for source in &sources {
            eprintln!("plan.source_id={}", source.manifest.source_id);
            eprintln!("plan.format={}", source.manifest.format_fingerprint);
            for table in &summary.tables {
                let supported = source_supports_table(&source.manifest.capabilities, table);
                eprintln!(
                    "plan.source_capability=source:{},table:{table},supported:{}",
                    source.manifest.source_id, supported
                );
            }
        }
        return Ok(());
    }
    let execute_started = Instant::now();
    let mut query_task = tokio::spawn(async move { prepared.execute(sources).await });
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let result = tokio::select! {
        result = &mut query_task => result??,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            cancellation.cancel();
            if tokio::time::timeout(
                std::time::Duration::from_secs(3),
                &mut query_task,
            )
            .await
            .is_err()
            {
                return Err("query cancellation did not stop within three seconds".into());
            }
            return Err("query cancelled".into());
        }
        _ = &mut deadline => {
            cancellation.cancel();
            if tokio::time::timeout(
                Duration::from_secs(3),
                &mut query_task,
            )
            .await
            .is_err()
            {
                return Err("query timeout did not stop within three seconds".into());
            }
            return Err("query timed out".into());
        }
    };
    diagnostic_timing(diagnostics, "execute", execute_started);
    if !quiet {
        for warning in &result.metadata.warnings {
            eprintln!("warning={warning}");
        }
    }
    if diagnostics {
        eprintln!("metadata.sources={}", result.metadata.source_ids.join(","));
        eprintln!(
            "metadata.records_scanned={}",
            result.metadata.records_scanned
        );
        eprintln!("metadata.bytes_read={}", result.metadata.bytes_read);
        for scan in &result.metadata.scans {
            eprintln!(
                "metadata.scan=table:{},source:{},predicates:{},limit:{},ordering:{},snapshot:{},stale:{}",
                scan.table,
                scan.source_id,
                scan.predicate_pushdown.join("+"),
                scan.limit_pushdown.as_deref().unwrap_or("none"),
                scan.ordering_pushdown.join("+"),
                scan.snapshot_strength,
                scan.stale,
            );
        }
    }
    let render_started = Instant::now();
    let batches = result.batches;
    let returned_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    let (rendered, formula_escaped) = match output {
        Output::Table => (pretty_format_batches(&batches)?.to_string(), false),
        Output::Json => (batches_to_json(&batches)?, false),
        Output::Jsonl => (batches_to_jsonl(&batches)?, false),
        Output::Csv => {
            let csv = batches_to_csv(&batches)?;
            (csv.rendered, csv.formula_escaped)
        }
    };
    budget.charge_output_bytes(rendered_publication_len(&rendered) as u64)?;
    if formula_escaped && !quiet {
        eprintln!("warning=CSV formula-like text was escaped");
    }
    if !access.is_empty() && (secure_output.is_some() || !io::stdout().is_terminal()) && !quiet {
        eprintln!("warning=sensitive access was granted for non-terminal output");
    }
    if let Some(output) = secure_output.as_mut() {
        write_rendered(output.writer(), &rendered, &cancellation)?;
    } else {
        write_rendered(&mut io::stdout().lock(), &rendered, &cancellation)?;
    }
    if let Some(output) = secure_output {
        output.commit()?;
    }
    diagnostic_timing(diagnostics, "render", render_started);
    if shell_summary && !quiet {
        eprintln!(
            "({returned_rows} rows, {} ms)",
            query_started.elapsed().as_millis()
        );
    }
    Ok(())
}

fn access_grant(values: &[Access]) -> AccessGrant {
    let mut grant = AccessGrant::default();
    for value in values {
        match value {
            Access::Path => grant.path = true,
            Access::Content => grant.content = true,
            Access::ToolInput => grant.tool_input = true,
            Access::ToolOutput => grant.tool_output = true,
        }
    }
    grant
}

fn execution_budget(
    max_records: u64,
    max_bytes_read: u64,
    max_output_bytes: u64,
    max_single_value_bytes: u64,
    timeout: Duration,
) -> Result<(ResourceBudget, Instant), Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or("timeout exceeds the supported range")?;
    Ok((
        ResourceBudget {
            max_records,
            max_bytes_read,
            max_output_bytes,
            max_single_value_bytes,
            deadline: Some(deadline),
            ..ResourceBudget::default()
        },
        deadline,
    ))
}

fn parse_sql_parameters(
    values: &[String],
) -> Result<std::collections::BTreeMap<String, SqlParameter>, CliError> {
    let mut parameters = std::collections::BTreeMap::new();
    for value in values {
        let (name, raw) = value
            .split_once('=')
            .ok_or_else(|| invalid_argument("query parameters must use NAME=VALUE"))?;
        if name.is_empty()
            || name.len() > 64
            || !name.bytes().enumerate().all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphanumeric() && (index > 0 || byte.is_ascii_alphabetic())
            })
        {
            return Err(invalid_argument(
                "query parameter names must be ASCII identifiers",
            ));
        }
        if raw.len() > 16 * 1024 * 1024 {
            return Err(invalid_argument(
                "query parameter value exceeds the fixed size limit",
            ));
        }
        let parameter = match raw {
            value if value.starts_with("text:") => SqlParameter::Text(value[5..].to_string()),
            value if value.starts_with("int:") => SqlParameter::Int64(
                value[4..]
                    .parse()
                    .map_err(|_| invalid_argument("query int parameter must fit i64"))?,
            ),
            value if value.starts_with("bool:") => match &value[5..] {
                "true" => SqlParameter::Bool(true),
                "false" => SqlParameter::Bool(false),
                _ => {
                    return Err(invalid_argument(
                        "query bool parameter must be true or false",
                    ));
                }
            },
            "null" => SqlParameter::Null,
            "true" => SqlParameter::Bool(true),
            "false" => SqlParameter::Bool(false),
            _ if integer_text(raw) => SqlParameter::Int64(raw.parse().map_err(|_| {
                invalid_argument("query integer parameter is outside the i64 range")
            })?),
            _ => SqlParameter::Text(raw.to_string()),
        };
        if parameters.insert(name.to_string(), parameter).is_some() {
            return Err(invalid_argument("query parameter names must be unique"));
        }
    }
    Ok(parameters)
}

fn integer_text(value: &str) -> bool {
    let digits = value
        .strip_prefix('-')
        .or_else(|| value.strip_prefix('+'))
        .unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

fn diagnostic_timing(enabled: bool, stage: &str, started: Instant) {
    if enabled {
        eprintln!(
            "diagnostic.stage={stage},elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }
}

fn source_supports_table(capabilities: &[String], table: &str) -> bool {
    table == "agents" || capabilities.iter().any(|candidate| candidate == table)
}

fn error_hint(error: &(dyn std::error::Error + 'static)) -> Option<String> {
    if let Some(aql_engine_datafusion::QueryError::AccessDenied(access)) =
        error.downcast_ref::<aql_engine_datafusion::QueryError>()
    {
        return match *access {
            "content" => Some(access_retry_hint("content", "Content")),
            "path" => Some(access_retry_hint("path", "Path")),
            "tool-input" => Some(access_retry_hint("tool-input", "tool input")),
            "tool-output" => Some(access_retry_hint("tool-output", "tool output")),
            _ => Some(
                "run `aql schema <table>` and add only the required temporary access grant"
                    .to_string(),
            ),
        };
    }
    if matches!(
        error.downcast_ref::<aql_adapter_api::AdapterError>(),
        Some(aql_adapter_api::AdapterError::AccessDenied { .. })
    ) {
        return Some(
            "run `aql schema <table>` and add only the required temporary access grant".to_string(),
        );
    }
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("no database selected")
        || message.contains("at least one explicit source")
        || message.contains("requires -d <database>")
        || message.contains("unknown database")
        || message.contains("unknown or unavailable database")
    {
        Some("run `aql database list`, then select one with `-d <database>`".to_string())
    } else if message.contains("requires --access content") {
        Some(access_retry_hint("content", "Content"))
    } else if message.contains("requires --access path") {
        Some(access_retry_hint("path", "Path"))
    } else if message.contains("sql input") || message.contains("one sql input") {
        Some("pass SQL directly, with `--file query.sql`, or with `--stdin`".to_string())
    } else {
        None
    }
}

fn access_retry_hint(access: &str, label: &str) -> String {
    if io::stderr().is_terminal()
        && let Some(command) = retry_query_command(access)
    {
        return format!("retry only if {label} is genuinely needed: `{command}`");
    }
    format!("retry with `--access {access}` only when the query genuinely needs {label}")
}

fn retry_query_command(access: &str) -> Option<String> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let query_index = arguments.iter().position(|argument| argument == "query")?;
    if arguments
        .windows(2)
        .any(|pair| pair == ["--access", access])
    {
        return None;
    }
    arguments.splice(
        query_index + 1..query_index + 1,
        ["--access".to_string(), access.to_string()],
    );
    Some(
        std::iter::once("aql".to_string())
            .chain(arguments)
            .map(|argument| shell_quote(&argument))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b'=' | b':')
        })
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }
}

fn render_error(error: &(dyn std::error::Error + 'static), format: ErrorFormat) {
    let category = error_category(error);
    let exit_code = error_exit_code(error);
    let hint = error_hint(error);
    match format {
        ErrorFormat::Text => {
            eprintln!("error_category={category}");
            eprintln!("error={error}");
            if let Some(hint) = hint {
                eprintln!("hint={hint}");
            }
        }
        ErrorFormat::Json => {
            eprintln!(
                "{}",
                serde_json::json!({
                    "category": category,
                    "message": error.to_string(),
                    "hint": hint,
                    "exit_code": exit_code,
                })
            );
        }
    }
}

fn error_exit_code(error: &(dyn std::error::Error + 'static)) -> i32 {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return match cli {
            CliError::InvalidArgument(_) => 2,
        };
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { .. } => 2,
            aql_engine_datafusion::QueryError::AccessDenied(_) => 3,
            aql_engine_datafusion::QueryError::Engine(engine)
                if engine.to_string().contains("resource budget exceeded")
                    || engine.to_string().contains("Resources exhausted")
                    || engine.to_string().contains("Not enough memory") =>
            {
                5
            }
            aql_engine_datafusion::QueryError::Engine(_) => 1,
        };
    }
    if let Some(adapter) = error.downcast_ref::<aql_adapter_api::AdapterError>() {
        return match adapter {
            aql_adapter_api::AdapterError::AccessDenied { .. } => 3,
            aql_adapter_api::AdapterError::BudgetExceeded { .. }
            | aql_adapter_api::AdapterError::Cancelled => 5,
            aql_adapter_api::AdapterError::NotFound { .. }
            | aql_adapter_api::AdapterError::PermissionDenied { .. }
            | aql_adapter_api::AdapterError::UnsupportedFormat { .. }
            | aql_adapter_api::AdapterError::CorruptSource { .. }
            | aql_adapter_api::AdapterError::SnapshotUnavailable => 4,
            aql_adapter_api::AdapterError::Internal { .. } => 1,
        };
    }
    if let Some(config) = error.downcast_ref::<ConfigError>() {
        return match config {
            ConfigError::InvalidDatabaseName | ConfigError::InvalidMember => 2,
            ConfigError::Missing
            | ConfigError::UnsafeRoot
            | ConfigError::RootOverlap
            | ConfigError::InvalidOwnershipMarker
            | ConfigError::UnknownFile
            | ConfigError::InvalidConfig
            | ConfigError::UnsupportedSchema
            | ConfigError::DatabaseExists
            | ConfigError::DatabaseMissing
            | ConfigError::LockHeld
            | ConfigError::StateChanged
            | ConfigError::Io(_)
            | ConfigError::Platform(_) => 4,
        };
    }
    let message = error.to_string();
    if message == "query cancelled" {
        130
    } else if message.contains("timed out") || message.contains("resource budget exceeded") {
        5
    } else if message.contains("unknown database")
        || message.contains("unknown or unavailable database")
    {
        4
    } else if message.contains("requires --access") {
        3
    } else if message.contains("unsupported") {
        4
    } else if message.contains("invalid")
        || message.contains("No database selected")
        || message.contains("at least one explicit source")
        || message.contains("requires --acknowledge")
    {
        2
    } else {
        1
    }
}

fn error_category(error: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return match cli {
            CliError::InvalidArgument(_) => "invalid_request",
        };
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { .. } => "invalid_request",
            aql_engine_datafusion::QueryError::AccessDenied(_) => "access_denied",
            aql_engine_datafusion::QueryError::Engine(_) => "execution_failed",
        };
    }
    if let Some(adapter) = error.downcast_ref::<aql_adapter_api::AdapterError>() {
        return match adapter {
            aql_adapter_api::AdapterError::AccessDenied { .. } => "access_denied",
            aql_adapter_api::AdapterError::BudgetExceeded { .. } => "resource_limit",
            aql_adapter_api::AdapterError::Cancelled => "cancelled",
            aql_adapter_api::AdapterError::NotFound { .. }
            | aql_adapter_api::AdapterError::PermissionDenied { .. }
            | aql_adapter_api::AdapterError::UnsupportedFormat { .. }
            | aql_adapter_api::AdapterError::CorruptSource { .. }
            | aql_adapter_api::AdapterError::SnapshotUnavailable => "source_unavailable",
            aql_adapter_api::AdapterError::Internal { .. } => "internal",
        };
    }
    if let Some(config) = error.downcast_ref::<ConfigError>() {
        return match config {
            ConfigError::InvalidDatabaseName | ConfigError::InvalidMember => "invalid_request",
            ConfigError::DatabaseExists => "already_exists",
            ConfigError::DatabaseMissing | ConfigError::Missing => "not_found",
            ConfigError::LockHeld => "concurrent_writer",
            ConfigError::UnsafeRoot
            | ConfigError::RootOverlap
            | ConfigError::InvalidOwnershipMarker
            | ConfigError::UnknownFile
            | ConfigError::InvalidConfig
            | ConfigError::UnsupportedSchema
            | ConfigError::StateChanged => "state_integrity",
            ConfigError::Io(_) | ConfigError::Platform(_) => "state_unavailable",
        };
    }
    let message = error.to_string();
    if message == "query cancelled" {
        "cancelled"
    } else if message.contains("timed out") {
        "deadline_exceeded"
    } else if message.contains("resource budget exceeded") {
        "resource_limit"
    } else if message.contains("unknown database")
        || message.contains("unknown or unavailable database")
    {
        "not_found"
    } else if message.contains("requires --access") {
        "access_denied"
    } else if message.contains("unsupported") {
        "unsupported"
    } else if message.contains("invalid")
        || message.contains("No database selected")
        || message.contains("at least one explicit source")
        || message.contains("requires --acknowledge")
    {
        "invalid_request"
    } else {
        "internal"
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|_| "invalid duration".to_string())?;
    if duration.is_zero() {
        Err("timeout must be greater than zero".to_string())
    } else {
        Ok(duration)
    }
}

fn parse_count(value: &str) -> Result<u64, String> {
    parse_scaled_u64(value, &[("k", 1_000), ("m", 1_000_000)], "record count")
}

fn parse_byte_size(value: &str) -> Result<u64, String> {
    parse_scaled_u64(
        value,
        &[
            ("gib", 1024 * 1024 * 1024),
            ("mib", 1024 * 1024),
            ("kib", 1024),
            ("gb", 1_000_000_000),
            ("mb", 1_000_000),
            ("kb", 1_000),
            ("b", 1),
        ],
        "byte size",
    )
}

fn parse_usize_byte_size(value: &str) -> Result<usize, String> {
    usize::try_from(parse_byte_size(value)?).map_err(|_| "byte size is too large".to_string())
}

fn parse_scaled_u64(value: &str, suffixes: &[(&str, u64)], label: &str) -> Result<u64, String> {
    let normalized = value.trim().to_ascii_lowercase();
    let (digits, multiplier) = suffixes
        .iter()
        .find_map(|(suffix, multiplier)| {
            normalized
                .strip_suffix(suffix)
                .map(|digits| (digits, *multiplier))
        })
        .unwrap_or((normalized.as_str(), 1));
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid {label}"));
    }
    digits
        .parse::<u64>()
        .map_err(|_| format!("invalid {label}"))?
        .checked_mul(multiplier)
        .ok_or_else(|| format!("{label} is too large"))
}

fn write_rendered(
    writer: &mut impl Write,
    rendered: &str,
    cancellation: &CancellationToken,
) -> io::Result<()> {
    let result = if rendered.ends_with('\n') {
        writer.write_all(rendered.as_bytes())
    } else {
        writeln!(writer, "{rendered}")
    };
    if let Err(error) = result {
        if error.kind() == io::ErrorKind::BrokenPipe {
            cancellation.cancel();
            return Ok(());
        }
        return Err(error);
    }
    Ok(())
}

fn rendered_publication_len(rendered: &str) -> usize {
    rendered.len() + usize::from(!rendered.ends_with('\n'))
}

fn installation_salt() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let state_root = aql_state_root()?;
    load_or_create_installation_salt(&state_root)
}

#[cfg(unix)]
fn load_or_create_installation_salt(
    state_root: &std::path::Path,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use rustix::fs::{Mode, OFlags};

    let directory = open_or_create_private_state_root(state_root)?;
    let directory_stat = rustix::fs::fstat(&directory)?;
    if rustix::fs::FileType::from_raw_mode(directory_stat.st_mode)
        != rustix::fs::FileType::Directory
        || directory_stat.st_mode & 0o077 != 0
    {
        return Err("AQL state root has unsafe permissions or type".into());
    }

    match rustix::fs::openat(
        &directory,
        "installation.key",
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(descriptor) => read_private_installation_salt(descriptor),
        Err(error) if error == rustix::io::Errno::NOENT => {
            let salt: [u8; 32] = rand::random();
            let descriptor = match rustix::fs::openat(
                &directory,
                "installation.key",
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_raw_mode(0o600),
            ) {
                Ok(descriptor) => descriptor,
                Err(error) if error == rustix::io::Errno::EXIST => {
                    let descriptor = rustix::fs::openat(
                        &directory,
                        "installation.key",
                        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )?;
                    return read_private_installation_salt(descriptor);
                }
                Err(error) => return Err(error.into()),
            };
            let mut file: fs::File = descriptor.into();
            file.write_all(&salt)?;
            file.sync_all()?;
            rustix::fs::fsync(&directory)?;
            Ok(salt.to_vec())
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn open_or_create_private_state_root(
    state_root: &std::path::Path,
) -> Result<std::os::fd::OwnedFd, Box<dyn std::error::Error>> {
    use rustix::fs::{Mode, OFlags};

    if !state_root.is_absolute()
        || state_root.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err("AQL state root must be normalized and absolute".into());
    }
    let components = state_root
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.is_empty() {
        return Err("AQL state root cannot be the filesystem root".into());
    }
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut directory = rustix::fs::openat(rustix::fs::CWD, "/", flags, Mode::empty())?;
    for component in components {
        match rustix::fs::openat(&directory, component, flags, Mode::empty()) {
            Ok(next) => directory = next,
            Err(error) if error == rustix::io::Errno::NOENT => {
                match rustix::fs::mkdirat(&directory, component, Mode::from_raw_mode(0o700)) {
                    Ok(()) => {}
                    Err(error) if error == rustix::io::Errno::EXIST => {}
                    Err(error) => return Err(error.into()),
                }
                directory = rustix::fs::openat(&directory, component, flags, Mode::empty())?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(directory)
}

#[cfg(unix)]
fn read_private_installation_salt(
    descriptor: std::os::fd::OwnedFd,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let stat = rustix::fs::fstat(&descriptor)?;
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile
        || stat.st_mode & 0o077 != 0
    {
        return Err("installation salt has unsafe permissions or type".into());
    }
    let mut salt = Vec::with_capacity(32);
    let mut file: fs::File = descriptor.into();
    file.read_to_end(&mut salt)?;
    if salt.len() != 32 {
        return Err("installation salt has an invalid length".into());
    }
    Ok(salt)
}

#[cfg(not(unix))]
fn load_or_create_installation_salt(
    state_root: &std::path::Path,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    fs::create_dir_all(state_root)?;
    let salt_path = state_root.join("installation.key");
    match fs::read(&salt_path) {
        Ok(salt) if salt.len() == 32 => Ok(salt),
        Ok(_) => Err("installation salt has an invalid length".into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let salt: [u8; 32] = rand::random();
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(salt_path)?;
            file.write_all(&salt)?;
            file.sync_all()?;
            Ok(salt.to_vec())
        }
        Err(error) => Err(error.into()),
    }
}

fn aql_state_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(if let Some(path) = std::env::var_os("AQL_HOME") {
        PathBuf::from(path)
    } else if cfg!(target_os = "macos") {
        PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?)
            .join("Library/Application Support/aql")
    } else if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        PathBuf::from(path).join("aql")
    } else {
        PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?).join(".local/share/aql")
    })
}

fn generated_command() -> clap::Command {
    let command = Cli::command();
    public_command(&command).name("aql")
}

fn public_command(source: &clap::Command) -> clap::Command {
    let mut command = clap::Command::new(source.get_name().to_owned())
        .subcommand_required(source.is_subcommand_required_set())
        .arg_required_else_help(source.is_arg_required_else_help_set())
        .disable_help_flag(source.is_disable_help_flag_set())
        .disable_help_subcommand(source.is_disable_help_subcommand_set());
    if let Some(value) = source.get_version() {
        command = command.version(value.to_owned());
    }
    if let Some(value) = source.get_long_version() {
        command = command.long_version(value.to_owned());
    }
    if let Some(value) = source.get_author() {
        command = command.author(value.to_owned());
    }
    if let Some(value) = source.get_about() {
        command = command.about(value.clone());
    }
    if let Some(value) = source.get_long_about() {
        command = command.long_about(value.clone());
    }
    if let Some(value) = source.get_before_help() {
        command = command.before_help(value.clone());
    }
    if let Some(value) = source.get_after_help() {
        command = command.after_help(value.clone());
    }
    for argument in source
        .get_arguments()
        .filter(|argument| !argument.is_hide_set())
    {
        // This command tree is used only to render public completion and man
        // output. Public database selection is required because hidden
        // compatibility alternatives are intentionally omitted here.
        let argument = if argument.get_id() == "member" {
            let mut member = clap::Arg::new("member")
                .long("member")
                .value_name("AGENT=PATH")
                .action(clap::ArgAction::Append)
                .required(true);
            if let Some(help) = argument.get_help() {
                member = member.help(help.clone());
            }
            member
        } else if argument.get_id() == "database" && source.get_name() != "shell" {
            let mut database = clap::Arg::new("database")
                .short('d')
                .long("database")
                .value_name("DATABASE")
                .required(true);
            if let Some(help) = argument.get_help() {
                database = database.help(help.clone());
            }
            database
        } else {
            argument
                .clone()
                .conflicts_with(clap::builder::Resettable::Reset)
                .requires(clap::builder::Resettable::Reset)
                .overrides_with(clap::builder::Resettable::Reset)
        };
        command = command.arg(argument);
    }
    for subcommand in source
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set())
    {
        command = command.subcommand(public_command(subcommand));
    }
    command
}

fn build_metadata() -> serde_json::Value {
    serde_json::json!({
        "canonical_schema": "aql-canonical-v0",
        "config_schema": CONFIG_SCHEMA_VERSION,
        "package": env!("CARGO_PKG_NAME"),
        "target": format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS),
        "version": env!("CARGO_PKG_VERSION"),
    })
}

fn render_version(output: VersionOutput) -> Result<String, Box<dyn std::error::Error>> {
    let metadata = build_metadata();
    Ok(match output {
        VersionOutput::Json => serde_json::to_string(&metadata)?,
        VersionOutput::Text => format!(
            "aql {}\ntarget={}\ncanonical_schema={}\nconfig_schema={}",
            metadata["version"]
                .as_str()
                .ok_or("missing build version")?,
            metadata["target"].as_str().ok_or("missing build target")?,
            metadata["canonical_schema"]
                .as_str()
                .ok_or("missing canonical schema")?,
            metadata["config_schema"]
                .as_str()
                .ok_or("missing config schema")?,
        ),
    })
}

fn render_completions(shell: CompletionShell) -> Vec<u8> {
    let mut command = generated_command();
    let mut rendered = Vec::new();
    clap_complete::generate(
        clap_complete::Shell::from(shell),
        &mut command,
        "aql",
        &mut rendered,
    );
    rendered
}

fn render_manpage() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut rendered = Vec::new();
    clap_mangen::Man::new(generated_command()).render(&mut rendered)?;
    Ok(rendered)
}

#[cfg(test)]
mod tests;
