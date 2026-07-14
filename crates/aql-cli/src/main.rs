use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, io, io::IsTerminal, io::Read, io::Write};

use aql_action_claude_code::ClaudeCodeActionAdapter;
use aql_action_codex::CodexActionAdapter;
use aql_action_kimi_code::KimiCodeActionAdapter;
use aql_action_opencode::OpenCodeActionAdapter;
use aql_action_synthetic::{SyntheticActionAdapter, SyntheticFault};
use aql_actions::{
    ACTION_AUDIT_SCHEMA_VERSION, ACTION_PLAN_SCHEMA_VERSION, ACTION_STORE_SCHEMA_VERSION,
    ActionArguments, ActionCapability, ActionExecutionResult, ActionOperation, ActionPlan,
    ActionReconciliation, ActionState, ActionStore, AgentActionAdapter, ApprovedAction,
    CapabilityStatus, MAX_PLAN_TTL_MS, SanitizedResultCode, UnsignedActionPlan,
};
use aql_adapter_api::{
    AccessGrant, AgentAdapter, CancellationToken, ColumnName, ProbeRequest, ResourceBudget,
    ScanRequest, TableName,
};
use aql_adapter_claude_code::ClaudeCodeAdapter;
use aql_adapter_codex::CodexAdapter;
use aql_adapter_kimi_code::KimiCodeAdapter;
use aql_adapter_opencode::OpenCodeAdapter;
use aql_config::{CONFIG_SCHEMA_VERSION, ConfigError, ConfigStore, Profile, ProfileSource};
use aql_engine_datafusion::{
    FederatedSource, QUERY_SCHEMAS, QueryDataType, QueryMetadata, QueryOptions, SqlParameter,
    StreamingQueryResult, bind_sql_parameters, prepare_query, validate_read_only_sql,
};
use aql_index::{
    INDEX_SCHEMA_VERSION, IndexFreshness, IndexGeneration, IndexPolicy, IndexStore, IndexWatermark,
    SearchOptions, TOKENIZER_VERSION, WatermarkComponent, require_fts5,
};
use aql_model::{AccessClass, CanonicalRecord, EntityId, SourceId, installation_scoped_hmac};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};

mod actions;
mod cli_args;
mod database;
mod output;
mod render;
mod shell;

use actions::*;
use cli_args::*;
use database::*;
use output::{
    SecureOutputFile, TransactionalOutput, portable_metadata, publish_bytes, write_export_chunk,
};
#[cfg(test)]
use render::batches_to_values;
use render::{
    arrow_json_value, batch_row_to_value, batches_to_csv, batches_to_json, batches_to_jsonl,
    validate_csv_options,
};
use shell::*;

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
            match requested_format {
                ErrorFormat::Text => {
                    let _ = error.print();
                }
                ErrorFormat::Json => eprintln!(
                    "{}",
                    serde_json::json!({
                        "category": "invalid_request",
                        "message": "invalid command-line arguments",
                        "hint": "run `aql --help` or `aql <command> --help`",
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
        Command::Doctor {
            data_root,
            source,
            profile,
            database,
        } => {
            let inputs = resolve_source_inputs(data_root, source, profile, database)?;
            let installation_salt = installation_salt()?;
            let sources = bind_sources(inputs.data_root, inputs.source_specs, installation_salt)?;
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
            data_root,
            source,
            profile,
            database,
            output,
            csv_formulas,
            acknowledge_raw_csv_formulas,
            access,
            param,
            limits,
            plan,
            metadata,
            diagnose,
            shell_summary,
            sql,
            file,
            stdin,
        } => {
            execute_query(
                QueryExecution {
                    data_root,
                    source,
                    profile,
                    database,
                    output,
                    csv_formulas,
                    acknowledge_raw_csv_formulas,
                    access,
                    param,
                    limits,
                    plan,
                    metadata,
                    diagnose,
                    shell_summary,
                    sql,
                    file,
                    stdin,
                },
                quiet,
            )
            .await?;
        }
        Command::Export {
            data_root,
            source,
            profile,
            database,
            access,
            param,
            limits,
            file,
            sql,
        } => {
            execute_export(
                ExportExecution {
                    data_root,
                    source,
                    profile,
                    database,
                    access,
                    param,
                    limits,
                    file,
                    sql,
                },
                quiet,
            )
            .await?;
        }
        Command::Report {
            data_root,
            source,
            profile,
            database,
            access,
            limits,
            report,
        } => {
            execute_report(ReportExecution {
                data_root,
                source,
                profile,
                database,
                access,
                limits,
                report,
            })
            .await?;
        }
        Command::Search {
            data_root,
            source,
            profile,
            database,
            access,
            limit,
            source_id,
            session_id,
            document_kind,
            context_tokens,
            max_output_bytes,
            timeout,
            query,
        } => {
            let grant = access_grant(&access);
            if !grant.content {
                return Err("search requires --access content".into());
            }
            let deadline = Instant::now()
                .checked_add(timeout)
                .ok_or("timeout exceeds the supported range")?;
            let inputs = resolve_source_inputs(data_root, source, profile, database)?;
            let state_root = aql_state_root()?;
            let selected = parse_sources(inputs.data_root, inputs.source_specs)?;
            let mut stores = Vec::with_capacity(selected.len());
            let mut generation_count = 0_usize;
            for source in &selected {
                let store = IndexStore::open_existing(&state_root, &source.canonical_root)?;
                let generations = store
                    .active_generations()?
                    .into_iter()
                    .filter(|generation| generation.policy == IndexPolicy::Content)
                    .collect::<Vec<_>>();
                generation_count = generation_count
                    .checked_add(generations.len())
                    .ok_or("search source count overflow")?;
                stores.push((store, generations));
            }
            if generation_count == 0 {
                return Err(aql_index::IndexError::RebuildRequired.into());
            }
            if generation_count > 64 {
                return Err("search source count exceeds the supported limit".into());
            }
            let mut hits = Vec::new();
            let search_options = SearchOptions {
                source_id,
                session_id,
                document_kind,
                context_tokens,
            };
            for (store, generations) in &stores {
                for generation in generations {
                    if Instant::now() >= deadline {
                        return Err("search timed out".into());
                    }
                    hits.extend(store.search_generation_with_options(
                        generation,
                        &query,
                        limit,
                        &search_options,
                    )?);
                }
            }
            hits.sort_by(|left, right| {
                left.rank
                    .total_cmp(&right.rank)
                    .then_with(|| left.document_id.cmp(&right.document_id))
            });
            hits.truncate(limit as usize);
            let mut written = 0_u64;
            let mut stdout = io::stdout().lock();
            for hit in hits {
                if Instant::now() >= deadline {
                    return Err("search timed out".into());
                }
                let rendered = serde_json::to_string(&serde_json::json!({
                    "document_id": hit.document_id,
                    "source_id": hit.source_id,
                    "session_id": hit.session_id,
                    "message_id": hit.message_id,
                    "document_kind": hit.document_kind,
                    "rank": hit.rank,
                    "access_class": "content_derived",
                    "context": hit.context,
                }))?;
                written = written
                    .checked_add(rendered.len() as u64 + 1)
                    .ok_or("search output size overflow")?;
                if written > max_output_bytes {
                    return Err("search output budget exceeded".into());
                }
                writeln!(stdout, "{rendered}")?;
            }
            stdout.flush()?;
            if !io::stdout().is_terminal() && !quiet {
                eprintln!("warning=search results are derived from persisted Content");
            }
        }
        Command::Action { action } => execute_action_command(action)?,
        Command::Index { index } => match index {
            IndexCommand::Status {
                data_root,
                source,
                profile,
                database,
                output,
            } => {
                let data_root = resolve_single_source_root(data_root, source, profile, database)?;
                let state_root = aql_state_root()?;
                match IndexStore::open_existing(&state_root, &data_root) {
                    Ok(store) => {
                        let generations = store.active_generations()?;
                        match output {
                            IndexStatusOutput::Text => {
                                if generations.is_empty() {
                                    println!("freshness=missing");
                                }
                                for generation in generations {
                                    println!("source_id={}", generation.source_id);
                                    println!("policy={}", generation.policy);
                                    println!("freshness={}", generation.freshness);
                                    println!("records={}", generation.record_count);
                                    println!("size_bytes={}", generation.size_bytes);
                                    println!("schema={}", generation.schema_version);
                                }
                            }
                            IndexStatusOutput::Json => println!(
                                "{}",
                                serde_json::to_string(&serde_json::json!({
                                    "freshness": if generations.is_empty() { "missing" } else { "available" },
                                    "generations": generations,
                                }))?
                            ),
                        }
                    }
                    Err(aql_index::IndexError::Missing) => match output {
                        IndexStatusOutput::Text => println!("freshness=missing"),
                        IndexStatusOutput::Json => {
                            println!("{{\"freshness\":\"missing\",\"generations\":[]}}")
                        }
                    },
                    Err(error) => return Err(error.into()),
                }
            }
            IndexCommand::Build {
                data_root,
                source,
                profile,
                database,
                policy,
                access,
                acknowledge_persistent_sensitive_copy,
                max_records,
                max_bytes_read,
                max_index_bytes,
                timeout,
            } => execute_index_write(
                IndexWriteRequest {
                    data_root,
                    source,
                    profile,
                    database,
                    policy,
                    access,
                    acknowledge_persistent_sensitive_copy,
                    max_records,
                    max_bytes_read,
                    max_index_bytes,
                    timeout,
                },
                MetadataWriteMode::Build,
            )?,
            IndexCommand::Update {
                data_root,
                source,
                profile,
                database,
                policy,
                access,
                acknowledge_persistent_sensitive_copy,
                max_records,
                max_bytes_read,
                max_index_bytes,
                timeout,
            } => execute_index_write(
                IndexWriteRequest {
                    data_root,
                    source,
                    profile,
                    database,
                    policy,
                    access,
                    acknowledge_persistent_sensitive_copy,
                    max_records,
                    max_bytes_read,
                    max_index_bytes,
                    timeout,
                },
                MetadataWriteMode::Update,
            )?,
            IndexCommand::Clear {
                data_root,
                source,
                profile,
                database,
                source_id,
                all,
                acknowledge_clear_all_indexes,
            } => {
                let data_root = resolve_single_source_root(data_root, source, profile, database)?;
                if !all && source_id.is_none() {
                    return Err("index clear requires --source-id or --all".into());
                }
                if all && !acknowledge_clear_all_indexes {
                    return Err("--all requires --acknowledge-clear-all-indexes".into());
                }
                let state_root = aql_state_root()?;
                let store = IndexStore::open_existing(&state_root, &data_root)?;
                let lock = store.acquire_write_lock()?;
                let removed = if all {
                    store.clear_all(lock)?
                } else {
                    store.clear_source(
                        source_id
                            .as_deref()
                            .ok_or("index clear requires --source-id")?,
                        lock,
                    )?
                };
                println!("removed_generations={removed}");
            }
            IndexCommand::Repair {
                data_root,
                source,
                profile,
                database,
            } => {
                let data_root = resolve_single_source_root(data_root, source, profile, database)?;
                let state_root = aql_state_root()?;
                let store = IndexStore::open_existing(&state_root, &data_root)?;
                let lock = store.acquire_write_lock()?;
                let removed = store.repair_abandoned(lock)?;
                println!("removed_abandoned_files={removed}");
            }
        },
        Command::Sources { sources } => execute_source_command(sources)?,
        Command::Profile { profile } => execute_profile_command(profile, "profile")?,
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

struct ReportExecution {
    data_root: Option<PathBuf>,
    source: Vec<String>,
    profile: Option<String>,
    database: Option<String>,
    access: Vec<Access>,
    limits: ExecutionLimits,
    report: ReportKind,
}

async fn execute_report(request: ReportExecution) -> Result<(), Box<dyn std::error::Error>> {
    let ReportExecution {
        data_root,
        source,
        profile,
        database,
        access,
        limits,
        report,
    } = request;
    let ExecutionLimits {
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        max_memory_bytes,
        timeout,
    } = limits;
    let (budget, deadline_at) = execution_budget(
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        timeout,
    )?;
    let mut options = QueryOptions {
        access: access_grant(&access),
        budget: budget.clone(),
        max_memory_bytes,
        ..QueryOptions::default()
    };
    let sections = report_sections(report);
    let mut validated = Vec::with_capacity(sections.len());
    for section in &sections {
        let sql = validate_read_only_sql(section.sql)?;
        prepare_query(&sql, options.clone()).await?;
        validated.push(sql);
    }
    let inputs = resolve_source_inputs(data_root, source, profile, database)?;
    let installation_salt = installation_salt()?;
    options.redaction_salt = installation_salt.clone();
    let cancellation = options.cancellation.clone();
    let sources = bind_sources(inputs.data_root, inputs.source_specs, installation_salt)?;
    let mut transaction = TransactionalOutput::new(max_memory_bytes);
    let report_title = match report {
        ReportKind::Summary => "# AQL Agent Usage Summary\n\n",
        ReportKind::Project => "# AQL Project Activity Report\n\n",
    };
    if !write_export_chunk(
        &mut transaction,
        report_title.as_bytes(),
        &budget,
        &cancellation,
    )? {
        return Ok(());
    }
    for (section, sql) in sections.iter().zip(validated.iter()) {
        let prepared = prepare_query(sql, options.clone()).await?;
        let result = prepared.execute_stream(sources.clone()).await?;
        let remaining = deadline_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            cancellation.cancel();
            return Err("query timed out".into());
        }
        if !stream_markdown_section(
            &mut transaction,
            section.title,
            result,
            &budget,
            &cancellation,
            remaining,
        )
        .await?
        {
            return Ok(());
        }
    }
    publish_bytes(
        &mut io::stdout().lock(),
        transaction.as_bytes(),
        &cancellation,
    )?;
    Ok(())
}

struct QueryExecution {
    data_root: Option<PathBuf>,
    source: Vec<String>,
    profile: Option<String>,
    database: Option<String>,
    output: Output,
    csv_formulas: CsvFormulaMode,
    acknowledge_raw_csv_formulas: bool,
    access: Vec<Access>,
    param: Vec<String>,
    limits: ExecutionLimits,
    plan: bool,
    metadata: bool,
    diagnose: bool,
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
        data_root,
        source,
        profile,
        database,
        output,
        csv_formulas,
        acknowledge_raw_csv_formulas,
        access,
        param,
        limits,
        plan,
        metadata,
        diagnose,
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
    validate_csv_options(output, csv_formulas, acknowledge_raw_csv_formulas)?;
    let sql = read_sql_input(sql, file, stdin)?;
    let query_started = Instant::now();
    let (sql, explain) = match explain_sql(&sql) {
        Some(inner) if !inner.is_empty() => (inner, true),
        Some(_) => return Err("EXPLAIN requires one SELECT or WITH query".into()),
        None => (sql.as_str(), false),
    };
    let bound_sql = bind_sql_parameters(sql, &parse_sql_parameters(&param)?)?;
    let validated_sql = validate_read_only_sql(&bound_sql)?;
    diagnostic_timing(diagnose, "parse", parse_started);
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
    diagnostic_timing(diagnose, "authorize", authorize_started);
    let inputs = resolve_source_inputs(data_root, source, profile, database)?;
    let installation_salt = installation_salt()?;
    options.redaction_salt = installation_salt.clone();
    let cancellation = options.cancellation.clone();
    let budget = options.budget.clone();
    let prepared = prepare_query(&validated_sql, options).await?;
    let probe_started = Instant::now();
    let sources = bind_sources(inputs.data_root, inputs.source_specs, installation_salt)?;
    diagnostic_timing(diagnose, "probe", probe_started);
    if sources.is_empty() {
        return Err("probe returned no compatible source".into());
    }
    if plan || explain {
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
                let capability = match table.as_str() {
                    "sessions" => "sessions",
                    "messages" => "messages",
                    "tool_calls" => "tool_calls",
                    "usage" => "usage",
                    "session_edges" => "session_edges",
                    "artifacts" => "artifacts",
                    "agents" => "sessions",
                    _ => table,
                };
                eprintln!(
                    "plan.source_capability=source:{},table:{table},supported:{}",
                    source.manifest.source_id,
                    source
                        .manifest
                        .capabilities
                        .iter()
                        .any(|candidate| candidate == capability)
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
    diagnostic_timing(diagnose, "execute", execute_started);
    if !quiet {
        for warning in &result.metadata.warnings {
            eprintln!("warning={warning}");
        }
    }
    if metadata {
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
            let csv = batches_to_csv(&batches, csv_formulas)?;
            (csv.rendered, csv.formula_escaped)
        }
    };
    budget.charge_output_bytes(rendered_publication_len(&rendered) as u64)?;
    if formula_escaped && !quiet {
        eprintln!("warning=CSV formula-like text was escaped");
    }
    if !access.is_empty() && !io::stdout().is_terminal() && !quiet {
        eprintln!("warning=sensitive access was granted for non-terminal output");
    }
    write_rendered(&mut io::stdout().lock(), &rendered, &cancellation)?;
    diagnostic_timing(diagnose, "render", render_started);
    if shell_summary && !quiet {
        eprintln!(
            "({returned_rows} rows, {} ms)",
            query_started.elapsed().as_millis()
        );
    }
    Ok(())
}

struct ExportExecution {
    data_root: Option<PathBuf>,
    source: Vec<String>,
    profile: Option<String>,
    database: Option<String>,
    access: Vec<Access>,
    param: Vec<String>,
    limits: ExecutionLimits,
    file: Option<PathBuf>,
    sql: String,
}

async fn execute_export(
    request: ExportExecution,
    quiet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let ExportExecution {
        data_root,
        source,
        profile,
        database,
        access,
        param,
        limits,
        file,
        sql,
    } = request;
    let ExecutionLimits {
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        max_memory_bytes,
        timeout,
    } = limits;
    let bound_sql = bind_sql_parameters(&sql, &parse_sql_parameters(&param)?)?;
    let validated_sql = validate_read_only_sql(&bound_sql)?;
    let (budget, deadline_at) = execution_budget(
        max_records,
        max_bytes_read,
        max_output_bytes,
        max_single_value_bytes,
        timeout,
    )?;
    let mut options = QueryOptions {
        access: access_grant(&access),
        budget: budget.clone(),
        max_memory_bytes,
        ..QueryOptions::default()
    };
    prepare_query(&validated_sql, options.clone()).await?;
    let inputs = resolve_source_inputs(data_root, source, profile, database)?;
    let mut secure_output = file.as_deref().map(SecureOutputFile::create).transpose()?;
    let installation_salt = installation_salt()?;
    options.redaction_salt = installation_salt.clone();
    let cancellation = options.cancellation.clone();
    let prepared = prepare_query(&validated_sql, options).await?;
    let sources = bind_sources(inputs.data_root, inputs.source_specs, installation_salt)?;
    let result = prepared.execute_stream(sources).await?;
    let remaining = deadline_at.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        cancellation.cancel();
        return Err("query timed out".into());
    }
    if let Some(output) = secure_output.as_mut() {
        stream_portable_json(output.writer(), result, &budget, &cancellation, remaining).await?;
    } else {
        if !access.is_empty() && !io::stdout().is_terminal() && !quiet {
            eprintln!("warning=sensitive access was granted for non-terminal output");
        }
        let mut transaction = TransactionalOutput::new(max_memory_bytes);
        stream_portable_json(&mut transaction, result, &budget, &cancellation, remaining).await?;
        publish_bytes(
            &mut io::stdout().lock(),
            transaction.as_bytes(),
            &cancellation,
        )?;
    }
    if let Some(output) = secure_output {
        output.commit()?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum MetadataWriteMode {
    Build,
    Update,
}

fn execute_index_write(
    request: IndexWriteRequest,
    mode: MetadataWriteMode,
) -> Result<(), Box<dyn std::error::Error>> {
    let policy = IndexPolicy::from(request.policy);
    let grant = access_grant(&request.access);
    if policy == IndexPolicy::Content
        && (!grant.content || !request.acknowledge_persistent_sensitive_copy)
    {
        return Err("content indexing requires --access content and --acknowledge-persistent-sensitive-copy".into());
    }
    if policy == IndexPolicy::Content {
        require_fts5()?;
    }
    let deadline = Instant::now()
        .checked_add(request.timeout)
        .ok_or("timeout exceeds the supported range")?;
    let budget = ResourceBudget {
        max_records: request.max_records,
        max_bytes_read: request.max_bytes_read,
        max_output_bytes: 0,
        max_single_value_bytes: 16 * 1024 * 1024,
        deadline: Some(deadline),
        ..ResourceBudget::default()
    };
    let inputs = resolve_source_inputs(
        request.data_root,
        request.source,
        request.profile,
        request.database,
    )?;
    write_index(inputs, budget, mode, policy, grant, request.max_index_bytes)
}

fn write_index(
    inputs: SourceInputs,
    budget: ResourceBudget,
    mode: MetadataWriteMode,
    policy: IndexPolicy,
    access: AccessGrant,
    max_index_bytes: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = parse_sources(inputs.data_root, inputs.source_specs)?;
    if parsed.len() != 1 {
        return Err("index build/update requires exactly one source".into());
    }
    let selected = &parsed[0];
    let data_root = &selected.canonical_root;
    let state_root = aql_state_root()?;
    let store = match mode {
        MetadataWriteMode::Build => IndexStore::create(&state_root, data_root)?,
        MetadataWriteMode::Update => {
            IndexStore::open_existing(&state_root, data_root).map_err(|error| match error {
                aql_index::IndexError::Missing => aql_index::IndexError::RebuildRequired,
                other => other,
            })?
        }
    };
    let lock = store.acquire_write_lock()?;
    let installation_salt = installation_salt()?;
    let adapter = read_adapter(&selected.adapter_id, &installation_salt)?;
    let manifests = adapter
        .probe(&ProbeRequest {
            data_root: selected.root.to_string_lossy().into_owned(),
        })?
        .manifests;
    if manifests.is_empty() {
        return Err("probe returned no compatible source".into());
    }
    let active_generations = if matches!(mode, MetadataWriteMode::Update) {
        store.active_generations()?
    } else {
        Vec::new()
    };
    for manifest in manifests {
        if matches!(mode, MetadataWriteMode::Update) {
            let Some(active) = active_generations.iter().find(|generation| {
                generation.source_id == manifest.source_id.as_str() && generation.policy == policy
            }) else {
                return Err(aql_index::IndexError::RebuildRequired.into());
            };
            if active.schema_version != INDEX_SCHEMA_VERSION
                || active.adapter_id != manifest.agent_id
                || active.format_fingerprint != manifest.format_fingerprint
                || active.tokenizer_version
                    != (policy == IndexPolicy::Content).then(|| TOKENIZER_VERSION.to_string())
                || active.freshness != IndexFreshness::Fresh
            {
                return Err(aql_index::IndexError::RebuildRequired.into());
            }
        }
        let mut builder = store.begin_generation(&lock)?;
        if policy == IndexPolicy::Content {
            builder.initialize_content_fts()?;
        }
        let empty_watermark = IndexWatermark::default();
        builder.put_metadata_record(
            "agent",
            manifest.source_id.as_str(),
            manifest.source_id.as_str(),
            &serde_json::json!({
                "source_id": manifest.source_id.as_str(),
                "agent_id": &manifest.agent_id,
                "display_name": &manifest.display_name,
                "format_fingerprint": &manifest.format_fingerprint,
                "capabilities": &manifest.capabilities,
                "snapshot_available": manifest.snapshot.is_some(),
            }),
            &empty_watermark,
        )?;
        let mut session_projection = vec![
            "session_id",
            "source_id",
            "agent_id",
            "model",
            "provider",
            "created_at",
            "updated_at",
            "archived",
            "tokens_used",
        ];
        if policy == IndexPolicy::Content {
            session_projection.extend(["title", "preview"]);
        }
        let scan = adapter.scan(ScanRequest {
            source: manifest.clone(),
            table: TableName::Sessions,
            projection: session_projection
                .into_iter()
                .map(ColumnName::new)
                .collect(),
            predicates: Vec::new(),
            limit: None,
            order_hint: Vec::new(),
            access,
            budget: budget.clone(),
            cancellation: CancellationToken::default(),
            snapshot: manifest.snapshot.clone(),
        })?;
        let mut record_count = 1_u64;
        let mut max_updated_at_ms: Option<i64> = None;
        for record in scan.records {
            let CanonicalRecord::Session(session) = record? else {
                return Err("session index scan produced a non-session record".into());
            };
            max_updated_at_ms = match (max_updated_at_ms, session.updated_at) {
                (Some(current), Some(value)) => Some(current.max(value.timestamp_millis())),
                (None, Some(value)) => Some(value.timestamp_millis()),
                (current, None) => current,
            };
            let persistent_session_id = installation_scoped_hmac(
                "aql-index-session-id-v1",
                session.session_id.as_str(),
                &installation_salt,
            );
            let mut record_watermark = IndexWatermark::default();
            record_watermark.components.insert(
                "state".to_string(),
                WatermarkComponent::Sqlite {
                    schema_fingerprint: manifest.format_fingerprint.clone(),
                    max_updated_at_ms: session.updated_at.map(|value| value.timestamp_millis()),
                    max_native_id_hmac: None,
                },
            );
            builder.put_metadata_record(
                "session",
                &persistent_session_id,
                session.source_id.as_str(),
                &serde_json::json!({
                    "session_id": &persistent_session_id,
                    "source_id": session.source_id.as_str(),
                    "agent_id": &session.agent_id,
                    "model": &session.model,
                    "provider": &session.provider,
                    "created_at": session.created_at,
                    "updated_at": session.updated_at,
                    "status": &session.status,
                    "archived": session.archived,
                    "message_count": session.message_count,
                    "tool_call_count": session.tool_call_count,
                    "tokens_used": session.tokens_used,
                    "identity_confidence": &session.identity_confidence,
                    "snapshot_state": &session.snapshot_state,
                }),
                &record_watermark,
            )?;
            record_count = record_count
                .checked_add(1)
                .ok_or("index record count overflow")?;
            if policy == IndexPolicy::Content {
                for (kind, value) in [
                    ("session_title", session.title.as_deref()),
                    ("session_preview", session.preview.as_deref()),
                ] {
                    if let Some(content) = value.filter(|content| !content.is_empty()) {
                        let document_id = installation_scoped_hmac(
                            kind,
                            session.session_id.as_str(),
                            &installation_salt,
                        );
                        builder.put_content_document(
                            &document_id,
                            session.source_id.as_str(),
                            &persistent_session_id,
                            None,
                            kind,
                            content,
                            &record_watermark,
                        )?;
                        record_count = record_count
                            .checked_add(1)
                            .ok_or("index record count overflow")?;
                    }
                }
            }
        }
        if policy == IndexPolicy::Content {
            let messages = adapter.scan(ScanRequest {
                source: manifest.clone(),
                table: TableName::Messages,
                projection: ["message_id", "session_id", "content"]
                    .into_iter()
                    .map(ColumnName::new)
                    .collect(),
                predicates: Vec::new(),
                limit: None,
                order_hint: Vec::new(),
                access,
                budget: budget.clone(),
                cancellation: CancellationToken::default(),
                snapshot: manifest.snapshot.clone(),
            })?;
            for record in messages.records {
                let CanonicalRecord::Message(message) = record? else {
                    return Err("message index scan produced a non-message record".into());
                };
                if message.role == "tool" {
                    continue;
                }
                let Some(content) = message
                    .content
                    .as_deref()
                    .filter(|content| !content.is_empty())
                else {
                    continue;
                };
                let persistent_session_id = installation_scoped_hmac(
                    "aql-index-session-id-v1",
                    message.session_id.as_str(),
                    &installation_salt,
                );
                let persistent_message_id = installation_scoped_hmac(
                    "aql-index-message-id-v1",
                    message.message_id.as_str(),
                    &installation_salt,
                );
                let document_id = installation_scoped_hmac(
                    "message_content",
                    message.message_id.as_str(),
                    &installation_salt,
                );
                builder.put_content_document(
                    &document_id,
                    message.source_id.as_str(),
                    &persistent_session_id,
                    Some(&persistent_message_id),
                    "message_content",
                    content,
                    &IndexWatermark::default(),
                )?;
                record_count = record_count
                    .checked_add(1)
                    .ok_or("index record count overflow")?;
            }
        }
        let mut watermark = IndexWatermark::default();
        watermark.components.insert(
            "state".to_string(),
            WatermarkComponent::Sqlite {
                schema_fingerprint: manifest.format_fingerprint.clone(),
                max_updated_at_ms,
                max_native_id_hmac: None,
            },
        );
        let completed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            .try_into()?;
        let generation = IndexGeneration {
            generation_id: builder.generation_id().to_string(),
            schema_version: INDEX_SCHEMA_VERSION.to_string(),
            policy,
            source_id: manifest.source_id.to_string(),
            adapter_id: manifest.agent_id,
            format_fingerprint: manifest.format_fingerprint,
            tokenizer_version: (policy == IndexPolicy::Content)
                .then(|| TOKENIZER_VERSION.to_string()),
            watermark,
            snapshot_strength: "weak".to_string(),
            freshness: IndexFreshness::Fresh,
            record_count,
            size_bytes: 0,
            completed_at_ms: Some(completed_at_ms),
            file_name: builder.file_name().to_string(),
            active: true,
        };
        let _ = builder.commit(&generation, max_index_bytes)?;
        println!("source_id={}", generation.source_id);
        println!("policy={}", generation.policy);
        println!("records={}", generation.record_count);
        println!("freshness={}", generation.freshness);
        if matches!(mode, MetadataWriteMode::Update) {
            println!("update_mode=full_reconcile");
        }
    }
    lock.release()?;
    Ok(())
}

struct ReportSection {
    title: &'static str,
    sql: &'static str,
}

fn report_sections(report: ReportKind) -> Vec<ReportSection> {
    match report {
        ReportKind::Summary => vec![
            ReportSection {
                title: "Overview by model",
                sql: "SELECT agent_id, model, provider, MIN(bucket_start) AS first_activity, MAX(bucket_start) AS last_activity, COUNT(DISTINCT session_id) AS sessions, SUM(message_count) AS messages, SUM(tool_call_count) AS tool_calls, SUM(error_count) AS errors, SUM(input_tokens) AS input_tokens, SUM(output_tokens) AS output_tokens, SUM(cached_tokens) AS cached_tokens, SUM(total_tokens) AS total_tokens FROM usage GROUP BY agent_id, model, provider ORDER BY agent_id, model, provider",
            },
            ReportSection {
                title: "Failed tools",
                sql: "SELECT tool_name, status, COUNT(*) AS failed_calls FROM tool_calls WHERE status IN ('error', 'failed') GROUP BY tool_name, status ORDER BY failed_calls DESC, tool_name",
            },
            ReportSection {
                title: "Sources",
                sql: "SELECT agent_id, provider, COUNT(*) AS sessions, MIN(created_at) AS first_session, MAX(updated_at) AS last_session FROM sessions GROUP BY agent_id, provider ORDER BY agent_id, provider",
            },
        ],
        ReportKind::Project => vec![
            ReportSection {
                title: "Project activity by model",
                sql: "SELECT MASK_PATH(s.cwd, 2) AS project, s.model, MIN(u.bucket_start) AS first_activity, MAX(u.bucket_start) AS last_activity, COUNT(DISTINCT s.session_id) AS sessions, SUM(u.message_count) AS messages, SUM(u.tool_call_count) AS tool_calls, SUM(u.error_count) AS errors, SUM(u.total_tokens) AS total_tokens FROM sessions s LEFT JOIN usage u ON s.session_id = u.session_id GROUP BY MASK_PATH(s.cwd, 2), s.model ORDER BY project, s.model",
            },
            ReportSection {
                title: "Failed tools by project",
                sql: "SELECT MASK_PATH(s.cwd, 2) AS project, t.tool_name, t.status, COUNT(*) AS failed_calls FROM tool_calls t JOIN sessions s ON t.session_id = s.session_id WHERE t.status IN ('error', 'failed') GROUP BY MASK_PATH(s.cwd, 2), t.tool_name, t.status ORDER BY failed_calls DESC, project, t.tool_name",
            },
        ],
    }
}

async fn stream_markdown_section(
    writer: &mut impl Write,
    title: &str,
    result: StreamingQueryResult,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<bool, Box<dyn std::error::Error>> {
    let StreamingQueryResult {
        mut stream,
        metadata,
    } = result;
    let schema = stream.schema();
    let mut header = format!("## {}\n\n|", markdown_escape(title));
    for field in schema.fields() {
        header.push(' ');
        header.push_str(&markdown_escape(field.name()));
        header.push_str(" |");
    }
    header.push_str("\n|");
    for _ in schema.fields() {
        header.push_str(" --- |");
    }
    header.push('\n');
    if !write_export_chunk(writer, header.as_bytes(), budget, cancellation)? {
        return Ok(false);
    }
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        let batch = tokio::select! {
            batch = stream.next() => batch,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                cancellation.cancel();
                return Err("query cancelled".into());
            }
            _ = &mut deadline => {
                cancellation.cancel();
                return Err("query timed out".into());
            }
        };
        let Some(batch) = batch else { break };
        let batch = batch?;
        for row_index in 0..batch.num_rows() {
            let mut row = String::from("|");
            for column_index in 0..batch.num_columns() {
                let value = arrow_json_value(&batch, column_index, row_index)?;
                row.push(' ');
                row.push_str(&markdown_value(&value));
                row.push_str(" |");
            }
            row.push('\n');
            if !write_export_chunk(writer, row.as_bytes(), budget, cancellation)? {
                return Ok(false);
            }
        }
    }
    let metadata = metadata.finish()?;
    if !metadata.warnings.is_empty() {
        let mut warnings = String::from("\nWarnings:\n\n");
        for warning in metadata.warnings {
            warnings.push_str("- ");
            warnings.push_str(&markdown_escape(&warning));
            warnings.push('\n');
        }
        if !write_export_chunk(writer, warnings.as_bytes(), budget, cancellation)? {
            return Ok(false);
        }
    }
    write_export_chunk(writer, b"\n", budget, cancellation)
}

fn markdown_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "unknown".to_string(),
        serde_json::Value::String(value) => markdown_escape(value),
        other => markdown_escape(&other.to_string()),
    }
}

fn markdown_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '|' => escaped.push_str("\\|"),
            '\n' => escaped.push_str("<br>"),
            '\r' => escaped.push_str("<br>"),
            '`' => escaped.push_str("\\`"),
            character if character.is_control() => escaped.push('\u{fffd}'),
            character => escaped.push(character),
        }
    }
    escaped
}

async fn stream_portable_json(
    writer: &mut impl Write,
    result: StreamingQueryResult,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let StreamingQueryResult {
        mut stream,
        metadata,
    } = result;
    if !write_export_chunk(
        writer,
        b"{\"format\":\"aql-portable-v1\",\"schema_version\":1,\"records\":[",
        budget,
        cancellation,
    )? {
        return Ok(());
    }
    let mut first = true;
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        let batch = tokio::select! {
            batch = stream.next() => batch,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                cancellation.cancel();
                return Err("query cancelled".into());
            }
            _ = &mut deadline => {
                cancellation.cancel();
                return Err("query timed out".into());
            }
        };
        let Some(batch) = batch else { break };
        let batch = batch?;
        for row_index in 0..batch.num_rows() {
            let row = batch_row_to_value(&batch, row_index)?;
            let encoded = serde_json::to_vec(&row)?;
            if !first && !write_export_chunk(writer, b",", budget, cancellation)? {
                return Ok(());
            }
            if !write_export_chunk(writer, &encoded, budget, cancellation)? {
                return Ok(());
            }
            first = false;
        }
    }
    let metadata = portable_metadata(metadata.finish()?);
    let suffix = serde_json::to_vec(&metadata)?;
    if !write_export_chunk(writer, b"],\"metadata\":", budget, cancellation)?
        || !write_export_chunk(writer, &suffix, budget, cancellation)?
        || !write_export_chunk(writer, b"}\n", budget, cancellation)?
    {
        return Ok(());
    }
    writer.flush()?;
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
) -> Result<std::collections::BTreeMap<String, SqlParameter>, Box<dyn std::error::Error>> {
    let mut parameters = std::collections::BTreeMap::new();
    for value in values {
        let (name, raw) = value
            .split_once('=')
            .ok_or("query parameters must use NAME=VALUE")?;
        if name.is_empty()
            || name.len() > 64
            || !name.bytes().enumerate().all(|(index, byte)| {
                byte == b'_'
                    || byte.is_ascii_alphanumeric() && (index > 0 || byte.is_ascii_alphabetic())
            })
        {
            return Err("query parameter names must be ASCII identifiers".into());
        }
        if raw.len() > 16 * 1024 * 1024 {
            return Err("query parameter value exceeds the fixed size limit".into());
        }
        let parameter = match raw {
            "null" => SqlParameter::Null,
            "true" => SqlParameter::Bool(true),
            "false" => SqlParameter::Bool(false),
            _ if integer_text(raw) => SqlParameter::Int64(
                raw.parse()
                    .map_err(|_| "query integer parameter is outside the i64 range")?,
            ),
            _ => SqlParameter::Text(raw.to_string()),
        };
        if parameters.insert(name.to_string(), parameter).is_some() {
            return Err("query parameter names must be unique".into());
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
        || message.contains("unknown database")
        || message.contains("unknown or unavailable database")
    {
        Some("run `aql database list`, then select one with `-d <database>`".to_string())
    } else if message.contains("requires --access content") {
        Some(access_retry_hint("content", "Content"))
    } else if message.contains("requires --access path") {
        Some(access_retry_hint("path", "Path"))
    } else if message.contains("rebuild") && message.contains("index") {
        Some("run `aql index build -d <database> --policy metadata`; Content search requires explicit Content indexing".to_string())
    } else if message.contains("sql input") || message.contains("one sql input") {
        Some("pass SQL directly, with `--file query.sql`, or with `--stdin`".to_string())
    } else if message.contains("profile missing") {
        Some("run `aql database list` or create it with `aql database add`".to_string())
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
    if let Some(action) = error.downcast_ref::<aql_actions::ActionError>() {
        return match action {
            aql_actions::ActionError::InvalidPlan
            | aql_actions::ActionError::UnsupportedPlanSchema
            | aql_actions::ActionError::PlanExpired
            | aql_actions::ActionError::PlanDigestMismatch
            | aql_actions::ActionError::ConfirmationMismatch
            | aql_actions::ActionError::InvalidArguments
            | aql_actions::ActionError::ArgumentCommitmentMismatch => 2,
            aql_actions::ActionError::AuditLimitExceeded => 5,
            aql_actions::ActionError::Unsupported(_)
            | aql_actions::ActionError::InvalidAudit
            | aql_actions::ActionError::UnsupportedAuditSchema
            | aql_actions::ActionError::AuditTampered
            | aql_actions::ActionError::Commitment
            | aql_actions::ActionError::StateRootOverlap
            | aql_actions::ActionError::UnsafeStateRoot
            | aql_actions::ActionError::MissingState
            | aql_actions::ActionError::InvalidOwnershipMarker
            | aql_actions::ActionError::LockHeld
            | aql_actions::ActionError::StateChanged
            | aql_actions::ActionError::InvalidStoredPlan
            | aql_actions::ActionError::Io(_)
            | aql_actions::ActionError::Platform(_) => 4,
        };
    }
    if let Some(config) = error.downcast_ref::<ConfigError>() {
        return match config {
            ConfigError::InvalidProfileName | ConfigError::InvalidSource => 2,
            ConfigError::Missing
            | ConfigError::UnsafeRoot
            | ConfigError::RootOverlap
            | ConfigError::InvalidOwnershipMarker
            | ConfigError::UnknownFile
            | ConfigError::InvalidConfig
            | ConfigError::UnsupportedSchema
            | ConfigError::ProfileExists
            | ConfigError::ProfileMissing
            | ConfigError::LockHeld
            | ConfigError::StateChanged
            | ConfigError::Io(_)
            | ConfigError::Platform(_) => 4,
        };
    }
    let message = error.to_string();
    if message.contains("outcome is unknown") {
        6
    } else if message == "query cancelled" {
        130
    } else if message.contains("timed out") || message.contains("resource budget exceeded") {
        5
    } else if message.contains("unknown database")
        || message.contains("unknown or unavailable database")
        || (message.contains("index rebuild") && message.contains("required"))
    {
        4
    } else if message.contains("requires --access") {
        3
    } else if message.contains("unsupported")
        || message.contains("revision changed")
        || message.contains("already been consumed")
        || message.contains("must be isolated")
    {
        4
    } else if message.contains("invalid")
        || message.contains("No database selected")
        || message.contains("at least one explicit source")
        || message.contains("requires --new-title")
        || message.contains("requires --confirm")
        || message.contains("requires --acknowledge")
    {
        2
    } else {
        1
    }
}

fn error_category(error: &(dyn std::error::Error + 'static)) -> &'static str {
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
    if let Some(action) = error.downcast_ref::<aql_actions::ActionError>() {
        return match action {
            aql_actions::ActionError::Unsupported(_) => "unsupported",
            aql_actions::ActionError::InvalidPlan
            | aql_actions::ActionError::UnsupportedPlanSchema
            | aql_actions::ActionError::PlanExpired
            | aql_actions::ActionError::PlanDigestMismatch
            | aql_actions::ActionError::ConfirmationMismatch
            | aql_actions::ActionError::InvalidArguments
            | aql_actions::ActionError::ArgumentCommitmentMismatch => "invalid_request",
            aql_actions::ActionError::AuditLimitExceeded => "resource_limit",
            aql_actions::ActionError::InvalidAudit
            | aql_actions::ActionError::UnsupportedAuditSchema
            | aql_actions::ActionError::AuditTampered
            | aql_actions::ActionError::Commitment
            | aql_actions::ActionError::StateRootOverlap
            | aql_actions::ActionError::UnsafeStateRoot
            | aql_actions::ActionError::MissingState
            | aql_actions::ActionError::InvalidOwnershipMarker
            | aql_actions::ActionError::StateChanged
            | aql_actions::ActionError::InvalidStoredPlan => "state_integrity",
            aql_actions::ActionError::LockHeld => "concurrent_writer",
            aql_actions::ActionError::Io(_) | aql_actions::ActionError::Platform(_) => {
                "state_unavailable"
            }
        };
    }
    if let Some(config) = error.downcast_ref::<ConfigError>() {
        return match config {
            ConfigError::InvalidProfileName | ConfigError::InvalidSource => "invalid_request",
            ConfigError::ProfileExists => "already_exists",
            ConfigError::ProfileMissing | ConfigError::Missing => "not_found",
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
    if message.contains("outcome is unknown") {
        "unknown_outcome"
    } else if message == "query cancelled" {
        "cancelled"
    } else if message.contains("timed out") {
        "deadline_exceeded"
    } else if message.contains("resource budget exceeded") {
        "resource_limit"
    } else if message.contains("unknown database")
        || message.contains("unknown or unavailable database")
    {
        "not_found"
    } else if message.contains("index rebuild") && message.contains("required") {
        "index_missing"
    } else if message.contains("requires --access") {
        "access_denied"
    } else if message.contains("unsupported") {
        "unsupported"
    } else if message.contains("revision changed") {
        "revision_conflict"
    } else if message.contains("already been consumed") {
        "already_consumed"
    } else if message.contains("must be isolated") {
        "isolation_violation"
    } else if message.contains("invalid")
        || message.contains("No database selected")
        || message.contains("at least one explicit source")
        || message.contains("requires --new-title")
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
        "action_audit_schema": ACTION_AUDIT_SCHEMA_VERSION,
        "action_plan_schema": ACTION_PLAN_SCHEMA_VERSION,
        "action_store_schema": ACTION_STORE_SCHEMA_VERSION,
        "canonical_schema": "aql-canonical-v0",
        "config_schema": CONFIG_SCHEMA_VERSION,
        "index_schema": INDEX_SCHEMA_VERSION,
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
            "aql {}\ntarget={}\ncanonical_schema={}\nconfig_schema={}\nindex_schema={}\naction_plan_schema={}\naction_audit_schema={}\naction_store_schema={}",
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
            metadata["index_schema"]
                .as_str()
                .ok_or("missing index schema")?,
            metadata["action_plan_schema"]
                .as_str()
                .ok_or("missing Action plan schema")?,
            metadata["action_audit_schema"]
                .as_str()
                .ok_or("missing Action audit schema")?,
            metadata["action_store_schema"]
                .as_str()
                .ok_or("missing Action store schema")?,
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
