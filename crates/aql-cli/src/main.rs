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
use futures::StreamExt;
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
#[cfg(test)]
use output::SecureOutputFile;
use output::TransactionalOutput;
use render::StreamingRenderer;
#[cfg(test)]
use render::{
    batches_to_csv, batches_to_csv_limited, batches_to_json_limited, batches_to_jsonl_limited,
    batches_to_table_limited, batches_to_values, render_batches_for_test,
};
use shell::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CliErrorKind {
    InvalidRequest,
    NotFound,
    SourceUnavailable,
    DeadlineExceeded,
    Cancelled,
    Unsupported,
    AlreadyExists,
    StateIntegrity,
    StateUnavailable,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CliErrorHint {
    #[default]
    None,
    DatabaseSelection,
    SqlInput,
}

#[derive(Debug)]
struct CliError {
    kind: CliErrorKind,
    message: String,
    hint: CliErrorHint,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CliError {}

fn invalid_argument(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::InvalidRequest, message)
}

fn sql_input_error(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::InvalidRequest, message).with_hint(CliErrorHint::SqlInput)
}

fn database_not_found(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::NotFound, message).with_hint(CliErrorHint::DatabaseSelection)
}

fn source_unavailable(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::SourceUnavailable, message)
}

fn deadline_exceeded(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::DeadlineExceeded, message)
}

fn query_cancelled() -> CliError {
    cli_error(CliErrorKind::Cancelled, "query cancelled")
}

fn unsupported(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::Unsupported, message)
}

fn already_exists(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::AlreadyExists, message)
}

fn state_integrity(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::StateIntegrity, message)
}

fn state_unavailable(message: impl Into<String>) -> CliError {
    cli_error(CliErrorKind::StateUnavailable, message)
}

fn cli_error(kind: CliErrorKind, message: impl Into<String>) -> CliError {
    CliError {
        kind,
        message: message.into(),
        hint: CliErrorHint::None,
    }
}

impl CliError {
    fn with_hint(mut self, hint: CliErrorHint) -> Self {
        self.hint = hint;
        self
    }
}

const MAX_SQL_INPUT_BYTES: u64 = 64 * 1024;

fn read_sql_input(
    sql: Option<String>,
    file: Option<PathBuf>,
    stdin: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(sql) = sql {
        if sql.len() as u64 > MAX_SQL_INPUT_BYTES {
            return Err(sql_input_error("SQL input exceeds the fixed 64 KiB limit").into());
        }
        return Ok(sql);
    }
    if let Some(path) = file {
        if path.to_string_lossy().contains("://") || path.as_os_str() == "-" {
            return Err(sql_input_error("--file requires a local file path").into());
        }
        let components = path.components().collect::<Vec<_>>();
        let Some(std::path::Component::Normal(file_name)) = components.last() else {
            return Err(sql_input_error("SQL file path must name one regular file").into());
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
                _ => {
                    return Err(
                        sql_input_error("SQL file path must not contain parent traversal").into(),
                    );
                }
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
            return Err(sql_input_error("SQL file must be a bounded regular file").into());
        }
        let mut input = String::new();
        Read::by_ref(&mut file)
            .take(MAX_SQL_INPUT_BYTES + 1)
            .read_to_string(&mut input)?;
        if input.len() as u64 != metadata.len() {
            return Err(sql_input_error("SQL file changed while it was being read").into());
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
            return Err(sql_input_error("stdin SQL exceeds the fixed 64 KiB limit").into());
        }
        return Ok(input);
    }
    Err(sql_input_error("one SQL input is required").into())
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
    use clap::error::{ContextKind, ContextValue};

    let missing_database = matches!(
        error.get(ContextKind::InvalidArg),
        Some(ContextValue::Strings(arguments))
            if arguments.iter().any(|argument| argument == "--database <DATABASE>")
    );
    if error.kind() == clap::error::ErrorKind::MissingRequiredArgument && missing_database {
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
            let sources = bind_sources(inputs, installation_salt)?;
            let deadline = Instant::now()
                .checked_add(Duration::from_secs(30))
                .ok_or_else(|| invalid_argument("doctor deadline is invalid"))?;
            let diagnostic_budget = ResourceBudget {
                max_records: 100,
                max_bytes_read: 16 * 1024 * 1024,
                max_output_bytes: 64 * 1024,
                max_single_value_bytes: 16 * 1024 * 1024,
                deadline: Some(deadline),
                ..ResourceBudget::default()
            };
            let cancellation = CancellationToken::default();
            let source_count = u64::try_from(sources.len())?;
            if source_count == 0 {
                return Err(source_unavailable("probe returned no compatible source").into());
            }
            let per_source_limit = (100_u64 / source_count).max(1);
            let mut report = String::new();
            use std::fmt::Write as _;
            for bound in sources {
                let manifest = &bound.manifest;
                writeln!(report, "agent={}", manifest.agent_id)?;
                writeln!(report, "format={}", manifest.format_fingerprint)?;
                writeln!(report, "capabilities={}", manifest.capabilities.join(","))?;
                for warning in &manifest.warnings {
                    writeln!(report, "warning={warning}")?;
                }
                let diagnostics = bound.adapter.scan(ScanRequest {
                    source: manifest.clone(),
                    table: TableName::Sessions,
                    projection: vec![ColumnName::new("session_id")],
                    predicates: Vec::new(),
                    limit: Some(per_source_limit),
                    order_hint: Vec::new(),
                    access: AccessGrant::default(),
                    budget: diagnostic_budget.clone(),
                    cancellation: cancellation.clone(),
                    snapshot: manifest.snapshot.clone(),
                })?;
                let mut session_records = 0_u64;
                for record in diagnostics.records {
                    record?;
                    session_records += 1;
                }
                writeln!(report, "session_records={session_records}")?;
                for warning in diagnostics.diagnostics.snapshot()? {
                    writeln!(report, "warning={:?}", warning.kind)?;
                }
            }
            diagnostic_budget.charge_output_bytes(report.len() as u64)?;
            io::stdout().lock().write_all(report.as_bytes())?;
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
        max_output_bytes,
        timeout,
    } = limits;
    let max_records = environment_u64("AQL_MAX_RECORDS", 100_000, parse_count)?;
    let max_bytes_read = environment_u64("AQL_MAX_BYTES_READ", 256 * 1024 * 1024, parse_byte_size)?;
    let max_single_value_bytes = environment_u64(
        "AQL_MAX_SINGLE_VALUE_BYTES",
        16 * 1024 * 1024,
        parse_byte_size,
    )?;
    let max_memory_bytes = usize::try_from(environment_u64(
        "AQL_MAX_MEMORY_BYTES",
        256 * 1024 * 1024,
        parse_byte_size,
    )?)
    .map_err(|_| invalid_argument("AQL_MAX_MEMORY_BYTES is too large"))?;
    let (query_budget, deadline) = execution_budget(
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        timeout,
    )?;
    let parse_started = Instant::now();
    let sql = read_sql_input(sql, file, stdin)?;
    let query_started = Instant::now();
    let (sql, explain) = match explain_sql(&sql) {
        Some(inner) if !inner.is_empty() => (inner, true),
        Some(_) => {
            return Err(invalid_argument("EXPLAIN requires one SELECT or WITH query").into());
        }
        None => (sql.as_str(), false),
    };
    let bound_sql = bind_sql_parameters(sql, &parse_sql_parameters(&param)?)?;
    let validated_sql = validate_read_only_sql(&bound_sql)?;
    ensure_before_deadline(deadline)?;
    diagnostic_timing(diagnostics, "parse", parse_started);
    let mut options = QueryOptions {
        access: access_grant(&access),
        budget: query_budget,
        max_memory_bytes,
        ..QueryOptions::default()
    };
    let installation_salt = installation_salt()?;
    options.redaction_salt = installation_salt.clone();
    let cancellation = options.cancellation.clone();
    let budget = options.budget.clone();
    let authorize_started = Instant::now();
    let authorization_timeout = remaining_timeout(deadline)?;
    let prepared = tokio::time::timeout(
        authorization_timeout,
        prepare_query(&validated_sql, options),
    )
    .await
    .map_err(|_| deadline_exceeded("query timed out"))??;
    diagnostic_timing(diagnostics, "authorize", authorize_started);
    let mut transactional_output = TransactionalOutput::create(output_file.as_deref())?;
    ensure_before_deadline(deadline)?;
    let inputs = resolve_database_inputs(&database)?;
    ensure_before_deadline(deadline)?;
    let probe_started = Instant::now();
    let sources = bind_sources(inputs, installation_salt)?;
    ensure_before_deadline(deadline)?;
    diagnostic_timing(diagnostics, "probe", probe_started);
    if sources.is_empty() {
        return Err(source_unavailable("probe returned no compatible source").into());
    }
    if explain {
        if output != Output::Table {
            return Err(invalid_argument("EXPLAIN supports only the default table output").into());
        }
        let summary = prepared.plan_summary();
        let mut rendered = String::new();
        use std::fmt::Write as _;
        writeln!(rendered, "plan.tables={}", summary.tables.join(","))?;
        writeln!(rendered, "plan.columns={}", summary.columns.join(","))?;
        writeln!(
            rendered,
            "plan.required_access={}",
            summary.required_access.join(",")
        )?;
        for reason in &summary.access_reasons {
            writeln!(rendered, "plan.access_reason={reason}")?;
        }
        for pushdown in &summary.pushdown {
            writeln!(rendered, "plan.pushdown={pushdown}")?;
        }
        writeln!(rendered, "plan.max_records={}", summary.max_records)?;
        writeln!(rendered, "plan.max_bytes_read={}", summary.max_bytes_read)?;
        writeln!(
            rendered,
            "plan.max_output_bytes={}",
            summary.max_output_bytes
        )?;
        writeln!(
            rendered,
            "plan.max_memory_bytes={}",
            summary.max_memory_bytes
        )?;
        for source in &sources {
            writeln!(rendered, "plan.source_id={}", source.manifest.source_id)?;
            writeln!(
                rendered,
                "plan.format={}",
                source.manifest.format_fingerprint
            )?;
            for table in &summary.tables {
                let supported = source_supports_table(&source.manifest.capabilities, table);
                writeln!(
                    rendered,
                    "plan.source_capability=source:{},table:{table},supported:{}",
                    source.manifest.source_id, supported
                )?;
            }
        }
        budget.charge_output_bytes(rendered_publication_len(&rendered) as u64)?;
        write_rendered(transactional_output.writer(), &rendered, &cancellation)?;
        transactional_output.publish(&cancellation)?;
        return Ok(());
    }
    let execute_started = Instant::now();
    let execution_timeout = remaining_timeout(deadline)?;
    let deadline_sleep = tokio::time::sleep(execution_timeout);
    tokio::pin!(deadline_sleep);
    let streaming = tokio::select! {
        result = prepared.execute_stream(sources) => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            cancellation.cancel();
            return Err(query_cancelled().into());
        }
        _ = &mut deadline_sleep => {
            cancellation.cancel();
            return Err(deadline_exceeded("query timed out").into());
        }
    };
    let aql_engine_datafusion::StreamingQueryResult {
        mut stream,
        metadata,
    } = streaming;
    let mut renderer = StreamingRenderer::new(output, budget.clone())?;
    let mut render_duration = Duration::ZERO;
    let render_started = Instant::now();
    renderer.start(transactional_output.writer())?;
    render_duration += render_started.elapsed();
    loop {
        let next = tokio::select! {
            next = stream.next() => next,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                cancellation.cancel();
                return Err(query_cancelled().into());
            }
            _ = &mut deadline_sleep => {
                cancellation.cancel();
                return Err(deadline_exceeded("query timed out").into());
            }
        };
        let Some(batch) = next else {
            break;
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                cancellation.cancel();
                return Err(error.into());
            }
        };
        ensure_before_deadline(deadline)?;
        let render_started = Instant::now();
        if let Err(error) = renderer.write_batch(transactional_output.writer(), &batch) {
            cancellation.cancel();
            return Err(error);
        }
        render_duration += render_started.elapsed();
        if let Err(error) = ensure_before_deadline(deadline) {
            cancellation.cancel();
            return Err(error);
        }
    }
    diagnostic_timing(diagnostics, "execute", execute_started);
    if let Err(error) = ensure_before_deadline(deadline) {
        cancellation.cancel();
        return Err(error);
    }
    let render_started = Instant::now();
    let summary = renderer.finish(transactional_output.writer())?;
    render_duration += render_started.elapsed();
    let metadata = metadata.finish()?;
    ensure_before_deadline(deadline)?;
    transactional_output.publish(&cancellation)?;
    if !quiet {
        for warning in &metadata.warnings {
            eprintln!("warning={warning}");
        }
    }
    if diagnostics {
        eprintln!("metadata.sources={}", metadata.source_ids.join(","));
        eprintln!("metadata.records_scanned={}", metadata.records_scanned);
        eprintln!("metadata.bytes_read={}", metadata.bytes_read);
        for scan in &metadata.scans {
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
    if summary.formula_escaped && !quiet {
        eprintln!("warning=CSV formula-like text was escaped");
    }
    if !access.is_empty() && (output_file.is_some() || !io::stdout().is_terminal()) && !quiet {
        eprintln!("warning=sensitive access was granted for non-terminal output");
    }
    diagnostic_duration(diagnostics, "render", render_duration);
    if shell_summary && !quiet {
        eprintln!(
            "({} rows, {} ms)",
            summary.returned_rows,
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
        .ok_or_else(|| invalid_argument("timeout exceeds the supported range"))?;
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

fn remaining_timeout(deadline: Instant) -> Result<Duration, Box<dyn std::error::Error>> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| deadline_exceeded("query timed out").into())
}

fn ensure_before_deadline(deadline: Instant) -> Result<(), Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        Err(deadline_exceeded("query timed out").into())
    } else {
        Ok(())
    }
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
    diagnostic_duration(enabled, stage, started.elapsed());
}

fn diagnostic_duration(enabled: bool, stage: &str, duration: Duration) {
    if enabled {
        eprintln!(
            "diagnostic.stage={stage},elapsed_ms={}",
            duration.as_millis()
        );
    }
}

fn source_supports_table(capabilities: &[String], table: &str) -> bool {
    table == "agents" || capabilities.iter().any(|candidate| candidate == table)
}

fn error_hint(error: &(dyn std::error::Error + 'static)) -> Option<String> {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return match cli.hint {
            CliErrorHint::None => None,
            CliErrorHint::DatabaseSelection => {
                Some("run `aql database list`, then select one with `-d <database>`".to_string())
            }
            CliErrorHint::SqlInput => {
                Some("pass SQL directly, with `--file query.sql`, or with `--stdin`".to_string())
            }
        };
    }
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
    if let Some(aql_engine_datafusion::QueryError::Engine(engine)) =
        error.downcast_ref::<aql_engine_datafusion::QueryError>()
        && matches!(
            datafusion_adapter_error(engine),
            Some(aql_adapter_api::AdapterError::AccessDenied { .. })
        )
    {
        return Some(
            "run `aql schema <table>` and add only the required temporary access grant".to_string(),
        );
    }
    if matches!(
        error.downcast_ref::<aql_adapter_api::AdapterError>(),
        Some(aql_adapter_api::AdapterError::AccessDenied { .. })
    ) {
        return Some(
            "run `aql schema <table>` and add only the required temporary access grant".to_string(),
        );
    }
    None
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
        return match cli.kind {
            CliErrorKind::InvalidRequest => 2,
            CliErrorKind::NotFound
            | CliErrorKind::SourceUnavailable
            | CliErrorKind::Unsupported
            | CliErrorKind::AlreadyExists
            | CliErrorKind::StateIntegrity
            | CliErrorKind::StateUnavailable => 4,
            CliErrorKind::DeadlineExceeded => 5,
            CliErrorKind::Cancelled => 130,
        };
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { .. } => 2,
            aql_engine_datafusion::QueryError::AccessDenied(_) => 3,
            aql_engine_datafusion::QueryError::Engine(engine)
                if datafusion_resource_limited(engine) =>
            {
                5
            }
            aql_engine_datafusion::QueryError::Engine(engine) => {
                datafusion_adapter_error(engine).map_or(1, adapter_error_exit_code)
            }
        };
    }
    if let Some(adapter) = error.downcast_ref::<aql_adapter_api::AdapterError>() {
        return adapter_error_exit_code(adapter);
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
    1
}

fn error_category(error: &(dyn std::error::Error + 'static)) -> &'static str {
    if let Some(cli) = error.downcast_ref::<CliError>() {
        return match cli.kind {
            CliErrorKind::InvalidRequest => "invalid_request",
            CliErrorKind::NotFound => "not_found",
            CliErrorKind::SourceUnavailable => "source_unavailable",
            CliErrorKind::DeadlineExceeded => "deadline_exceeded",
            CliErrorKind::Cancelled => "cancelled",
            CliErrorKind::Unsupported => "unsupported",
            CliErrorKind::AlreadyExists => "already_exists",
            CliErrorKind::StateIntegrity => "state_integrity",
            CliErrorKind::StateUnavailable => "state_unavailable",
        };
    }
    if let Some(query) = error.downcast_ref::<aql_engine_datafusion::QueryError>() {
        return match query {
            aql_engine_datafusion::QueryError::SqlRejected { .. } => "invalid_request",
            aql_engine_datafusion::QueryError::AccessDenied(_) => "access_denied",
            aql_engine_datafusion::QueryError::Engine(engine)
                if datafusion_resource_limited(engine) =>
            {
                "resource_limit"
            }
            aql_engine_datafusion::QueryError::Engine(engine) => {
                datafusion_adapter_error(engine).map_or("execution_failed", adapter_error_category)
            }
        };
    }
    if let Some(adapter) = error.downcast_ref::<aql_adapter_api::AdapterError>() {
        return adapter_error_category(adapter);
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
    "internal"
}

fn datafusion_resource_limited(error: &datafusion::error::DataFusionError) -> bool {
    use datafusion::error::DataFusionError;

    match error {
        DataFusionError::ResourcesExhausted(_) => true,
        DataFusionError::External(error) => error
            .downcast_ref::<aql_adapter_api::AdapterError>()
            .is_some_and(|error| {
                matches!(error, aql_adapter_api::AdapterError::BudgetExceeded { .. })
            }),
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            datafusion_resource_limited(error)
        }
        DataFusionError::Collection(errors) => errors.iter().any(datafusion_resource_limited),
        DataFusionError::Shared(error) => datafusion_resource_limited(error),
        _ => false,
    }
}

fn datafusion_adapter_error(
    error: &datafusion::error::DataFusionError,
) -> Option<&aql_adapter_api::AdapterError> {
    use datafusion::error::DataFusionError;

    match error {
        DataFusionError::External(error) => error.downcast_ref::<aql_adapter_api::AdapterError>(),
        DataFusionError::Context(_, error) | DataFusionError::Diagnostic(_, error) => {
            datafusion_adapter_error(error)
        }
        DataFusionError::Collection(errors) => errors.iter().find_map(datafusion_adapter_error),
        DataFusionError::Shared(error) => datafusion_adapter_error(error),
        _ => None,
    }
}

fn adapter_error_exit_code(error: &aql_adapter_api::AdapterError) -> i32 {
    match error {
        aql_adapter_api::AdapterError::AccessDenied { .. } => 3,
        aql_adapter_api::AdapterError::BudgetExceeded { .. }
        | aql_adapter_api::AdapterError::Cancelled => 5,
        aql_adapter_api::AdapterError::NotFound { .. }
        | aql_adapter_api::AdapterError::PermissionDenied { .. }
        | aql_adapter_api::AdapterError::UnsupportedFormat { .. }
        | aql_adapter_api::AdapterError::CorruptSource { .. }
        | aql_adapter_api::AdapterError::SnapshotUnavailable => 4,
        aql_adapter_api::AdapterError::Internal { .. } => 1,
    }
}

fn adapter_error_category(error: &aql_adapter_api::AdapterError) -> &'static str {
    match error {
        aql_adapter_api::AdapterError::AccessDenied { .. } => "access_denied",
        aql_adapter_api::AdapterError::BudgetExceeded { .. } => "resource_limit",
        aql_adapter_api::AdapterError::Cancelled => "cancelled",
        aql_adapter_api::AdapterError::NotFound { .. }
        | aql_adapter_api::AdapterError::PermissionDenied { .. }
        | aql_adapter_api::AdapterError::UnsupportedFormat { .. }
        | aql_adapter_api::AdapterError::CorruptSource { .. }
        | aql_adapter_api::AdapterError::SnapshotUnavailable => "source_unavailable",
        aql_adapter_api::AdapterError::Internal { .. } => "internal",
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

fn environment_u64(
    name: &str,
    default: u64,
    parser: fn(&str) -> Result<u64, String>,
) -> Result<u64, CliError> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(default);
    };
    let value = value
        .to_str()
        .ok_or_else(|| invalid_argument(format!("{name} must be valid UTF-8")))?;
    parser(value).map_err(|reason| invalid_argument(format!("{name}: {reason}")))
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
        return Err(state_integrity("AQL state root has unsafe permissions or type").into());
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
        return Err(state_integrity("AQL state root must be normalized and absolute").into());
    }
    let components = state_root
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect::<Vec<_>>();
    if components.is_empty() {
        return Err(state_integrity("AQL state root cannot be the filesystem root").into());
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
        return Err(state_integrity("installation salt has unsafe permissions or type").into());
    }
    let mut salt = Vec::with_capacity(32);
    let mut file: fs::File = descriptor.into();
    file.read_to_end(&mut salt)?;
    if salt.len() != 32 {
        return Err(state_integrity("installation salt has an invalid length").into());
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
        Ok(_) => Err(state_integrity("installation salt has an invalid length").into()),
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
        PathBuf::from(std::env::var_os("HOME").ok_or_else(|| state_unavailable("HOME is not set"))?)
            .join("Library/Application Support/aql")
    } else if let Some(path) = std::env::var_os("XDG_DATA_HOME") {
        PathBuf::from(path).join("aql")
    } else {
        PathBuf::from(std::env::var_os("HOME").ok_or_else(|| state_unavailable("HOME is not set"))?)
            .join(".local/share/aql")
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
