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
    FederatedSource, QUERY_SCHEMAS, QueryDataType, QueryMetadata, QueryOptions,
    StreamingQueryResult, prepare_query, validate_read_only_sql,
};
use aql_index::{
    INDEX_SCHEMA_VERSION, IndexFreshness, IndexGeneration, IndexPolicy, IndexStore, IndexWatermark,
    TOKENIZER_VERSION, WatermarkComponent, require_fts5,
};
use aql_model::{AccessClass, CanonicalRecord, EntityId, SourceId, installation_scoped_hmac};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use futures::StreamExt;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};

#[derive(Parser)]
#[command(
    name = "aql",
    version,
    about = "Query local agent data",
    long_about = "Query explicitly selected local Claude Code, Codex, Kimi Code and OpenCode data. Run without a subcommand on a terminal for the SQL-first shell, or use `query --database <name>`. AQL never selects a default database. `all` must be explicit and checks only fixed local candidates. CSV output is formula-safe by default; sensitive Path/Content/tool grants and persistent database paths require explicit acknowledgement."
)]
struct Cli {
    /// Select stable text or single-line JSON error rendering.
    #[arg(
        long,
        global = true,
        env = "AQL_ERROR_FORMAT",
        value_enum,
        default_value_t = ErrorFormat::Text
    )]
    error_format: ErrorFormat,
    /// Suppress non-essential warnings and shell summaries; errors and requested metadata remain visible.
    #[arg(long, global = true)]
    quiet: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start an interactive SQL-first shell.
    Shell {
        /// Select an initial logical database.
        #[arg(short = 'd', long)]
        database: Option<String>,
    },
    /// Print deterministic package, target and schema version metadata.
    Version {
        #[arg(long, value_enum, default_value_t = VersionOutput::Text)]
        output: VersionOutput,
    },
    /// Generate a deterministic shell completion script on stdout.
    Completions {
        #[arg(value_enum)]
        shell: CompletionShell,
    },
    /// Generate the deterministic aql(1) man page on stdout.
    Man,
    /// Probe explicitly selected source roots without mutating them.
    Doctor {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        /// Select the database to diagnose; no database is selected implicitly.
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
    },
    /// Run one read-only SQL query against an explicit database.
    Query {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        /// Select a built-in or configured database.
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        /// Select table, JSON, JSONL or RFC 4180 CSV rendering.
        #[arg(long, value_enum, default_value_t = Output::Table)]
        output: Output,
        /// Escape spreadsheet formula-like text by default; raw requires acknowledgement.
        #[arg(long, value_enum, default_value_t = CsvFormulaMode::Safe)]
        csv_formulas: CsvFormulaMode,
        /// Acknowledge that raw CSV formulas may execute when opened in a spreadsheet.
        #[arg(long)]
        acknowledge_raw_csv_formulas: bool,
        /// Grant access to sensitive column classes for this query only; repeat as needed.
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        /// Stop after scanning this many canonical records across the complete query.
        #[arg(long, env = "AQL_MAX_RECORDS", default_value = "100k", value_parser = parse_count)]
        max_records: u64,
        /// Maximum source bytes that all adapters may read for this query.
        #[arg(long, env = "AQL_MAX_BYTES_READ", default_value = "256MiB", value_parser = parse_byte_size)]
        max_bytes_read: u64,
        /// Maximum bytes that may be published to stdout.
        #[arg(long, env = "AQL_MAX_OUTPUT_BYTES", default_value = "64MiB", value_parser = parse_byte_size)]
        max_output_bytes: u64,
        /// Reject any single sensitive value larger than this limit.
        #[arg(long, env = "AQL_MAX_SINGLE_VALUE_BYTES", default_value = "16MiB", value_parser = parse_byte_size)]
        max_single_value_bytes: u64,
        /// Maximum in-memory query execution working set.
        #[arg(long, env = "AQL_MAX_MEMORY_BYTES", default_value = "256MiB", value_parser = parse_usize_byte_size)]
        max_memory_bytes: usize,
        /// Cancel the complete query when this duration elapses.
        #[arg(long, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
        /// Print the authorized, redacted query plan without executing the query.
        #[arg(long)]
        plan: bool,
        /// Print source, scan, pushdown and budget metadata to stderr.
        #[arg(long)]
        metadata: bool,
        #[arg(skip)]
        shell_summary: bool,
        #[arg(conflicts_with_all = ["file", "stdin"], required_unless_present_any = ["file", "stdin"])]
        sql: Option<String>,
        /// Read one SQL statement from a bounded local regular file.
        #[arg(long, conflicts_with_all = ["sql", "stdin"])]
        file: Option<PathBuf>,
        /// Read one SQL statement from stdin.
        #[arg(long, conflicts_with_all = ["sql", "file"])]
        stdin: bool,
    },
    /// Export one read-only query as portable JSON.
    Export {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        /// Select the database to export; no database is selected implicitly.
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        /// Grant access to sensitive column classes for this export only; repeat as needed.
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        /// Stop after scanning this many canonical records across the complete export.
        #[arg(long, env = "AQL_MAX_RECORDS", default_value = "100k", value_parser = parse_count)]
        max_records: u64,
        /// Maximum source bytes that all adapters may read.
        #[arg(long, env = "AQL_MAX_BYTES_READ", default_value = "256MiB", value_parser = parse_byte_size)]
        max_bytes_read: u64,
        /// Maximum bytes in the complete portable JSON export.
        #[arg(long, env = "AQL_MAX_OUTPUT_BYTES", default_value = "64MiB", value_parser = parse_byte_size)]
        max_output_bytes: u64,
        /// Reject any single sensitive value larger than this limit.
        #[arg(long, env = "AQL_MAX_SINGLE_VALUE_BYTES", default_value = "16MiB", value_parser = parse_byte_size)]
        max_single_value_bytes: u64,
        /// Maximum in-memory query execution working set.
        #[arg(long, env = "AQL_MAX_MEMORY_BYTES", default_value = "256MiB", value_parser = parse_usize_byte_size)]
        max_memory_bytes: usize,
        /// Cancel the complete export when this duration elapses.
        #[arg(long, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
        /// Atomically write the export to this new local file instead of stdout.
        #[arg(long = "output-file", visible_alias = "file")]
        file: Option<PathBuf>,
        sql: String,
    },
    /// Render a predefined read-only Markdown report.
    Report {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        /// Select the database used by the report.
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        /// Grant access to sensitive column classes for this report only; repeat as needed.
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        /// Stop after scanning this many canonical records across all report queries.
        #[arg(long, env = "AQL_MAX_RECORDS", default_value = "100k", value_parser = parse_count)]
        max_records: u64,
        /// Maximum source bytes that all report queries may read.
        #[arg(long, env = "AQL_MAX_BYTES_READ", default_value = "256MiB", value_parser = parse_byte_size)]
        max_bytes_read: u64,
        /// Maximum rendered Markdown bytes.
        #[arg(long, env = "AQL_MAX_OUTPUT_BYTES", default_value = "64MiB", value_parser = parse_byte_size)]
        max_output_bytes: u64,
        /// Reject any single sensitive value larger than this limit.
        #[arg(long, env = "AQL_MAX_SINGLE_VALUE_BYTES", default_value = "16MiB", value_parser = parse_byte_size)]
        max_single_value_bytes: u64,
        /// Maximum in-memory execution working set.
        #[arg(long, env = "AQL_MAX_MEMORY_BYTES", default_value = "256MiB", value_parser = parse_usize_byte_size)]
        max_memory_bytes: usize,
        /// Cancel the complete report when this duration elapses.
        #[arg(long, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
        #[command(subcommand)]
        report: ReportKind,
    },
    /// Search an explicitly created local Content index.
    Search {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        /// Select the database whose explicitly built Content index is searched.
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        /// Grant Content access for this search invocation.
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        /// Maximum number of search matches to return.
        #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(u64).range(1..=1000))]
        limit: u64,
        /// Maximum rendered search-result bytes.
        #[arg(long, env = "AQL_MAX_OUTPUT_BYTES", default_value = "1MiB", value_parser = parse_byte_size)]
        max_output_bytes: u64,
        /// Cancel the search when this duration elapses.
        #[arg(long, env = "AQL_TIMEOUT", default_value = "5s", value_parser = parse_duration)]
        timeout: Duration,
        query: String,
    },
    #[command(hide = true)]
    Action {
        #[command(subcommand)]
        action: ActionCommand,
    },
    /// Manage explicit AQL-owned indexes.
    Index {
        #[command(subcommand)]
        index: IndexCommand,
    },
    /// Explicitly discover fixed, documented local Agent candidate roots.
    #[command(hide = true)]
    Sources {
        #[command(subcommand)]
        sources: SourcesCommand,
    },
    /// Manage private named source profiles; no profile is selected implicitly.
    #[command(hide = true)]
    Profile {
        #[command(subcommand)]
        profile: ProfileCommand,
    },
    /// Discover and manage logical databases.
    Database {
        #[command(subcommand)]
        database: DatabaseCommand,
    },
    /// Show the canonical SQL schema without opening Agent data.
    Schema {
        #[arg(conflicts_with = "list")]
        table: Option<String>,
        /// List canonical table names without rendering their columns.
        #[arg(long)]
        list: bool,
        #[arg(long, value_enum, default_value_t = SchemaOutput::Table)]
        output: SchemaOutput,
    },
    /// Print curated read-only SQL examples without executing them.
    Examples {
        #[arg(conflicts_with = "list")]
        name: Option<String>,
        /// List available example names.
        #[arg(long)]
        list: bool,
    },
}

#[derive(Subcommand)]
enum SourcesCommand {
    /// Run bounded, non-persistent discovery without revealing candidate paths.
    Discover,
}

#[derive(Subcommand)]
enum DatabaseCommand {
    /// List available built-in and configured databases.
    List,
    /// Probe fixed local Agent candidates without revealing their paths.
    Discover,
    /// Add a named database backed by one or more explicit Agent paths.
    Add {
        name: String,
        /// Add one atomic member as AGENT=/absolute/path; repeat for federation.
        #[arg(
            long = "member",
            value_name = "AGENT=PATH",
            conflicts_with_all = ["agent", "path"],
            required_unless_present = "agent"
        )]
        member: Vec<String>,
        #[arg(long = "agent", hide = true, requires = "path")]
        agent: Vec<String>,
        #[arg(long = "path", hide = true, requires = "agent")]
        path: Vec<PathBuf>,
        /// Confirm storage of adapter IDs and exact absolute paths only; SQL, grants and results are never stored.
        #[arg(long)]
        acknowledge_persistent_path: bool,
    },
    /// Show a named database; paths remain masked without Path access.
    Show {
        name: String,
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
    },
    /// Remove one configured database without touching Agent data or indexes.
    Remove { name: String },
}

#[derive(Subcommand)]
enum ProfileCommand {
    /// Persist a new profile without overwriting an existing name.
    Add {
        name: String,
        #[arg(long = "source", required = true)]
        source: Vec<String>,
        /// Acknowledge persistent storage of the exact source paths.
        #[arg(long)]
        acknowledge_persistent_path: bool,
    },
    /// List profiles with source paths masked.
    List,
    /// Show one profile; paths remain masked unless Path access is granted.
    Show {
        name: String,
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
    },
    /// Remove one named profile without touching Agent data or AQL indexes.
    Remove { name: String },
}

#[derive(Subcommand)]
enum ActionCommand {
    Capabilities {
        #[arg(long)]
        source_id: String,
        #[arg(long, hide = true)]
        synthetic_channel_root: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = ActionOutput::Text)]
        output: ActionOutput,
        #[arg(long, default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    Plan {
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long)]
        source_id: String,
        #[arg(long)]
        entity_id: String,
        #[arg(long, value_enum)]
        operation: ActionOperationArg,
        #[arg(long)]
        new_title: Option<String>,
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        #[arg(long, default_value = "5m", value_parser = parse_duration)]
        ttl: Duration,
        #[arg(long, hide = true)]
        synthetic_channel_root: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = ActionOutput::Text)]
        output: ActionOutput,
        #[arg(long, default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    Apply {
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long)]
        action_id: String,
        #[arg(long)]
        confirm: String,
        #[arg(long)]
        new_title: Option<String>,
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        #[arg(long, hide = true)]
        synthetic_channel_root: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = SyntheticFaultArg::None, hide = true)]
        synthetic_fault: SyntheticFaultArg,
        #[arg(long, value_enum, default_value_t = ActionOutput::Text)]
        output: ActionOutput,
        #[arg(long, default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    Inspect {
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long)]
        action_id: String,
        #[arg(long, value_enum, default_value_t = ActionOutput::Text)]
        output: ActionOutput,
        #[arg(long, default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    Reconcile {
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long)]
        action_id: String,
        #[arg(long, hide = true)]
        synthetic_channel_root: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = ActionOutput::Text)]
        output: ActionOutput,
        #[arg(long, default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    AuditVerify {
        #[arg(long)]
        data_root: PathBuf,
        #[arg(long, value_enum, default_value_t = ActionOutput::Text)]
        output: ActionOutput,
        #[arg(long, default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum ActionOutput {
    Text,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
enum VersionOutput {
    Text,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
enum SchemaOutput {
    Table,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
enum ErrorFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
enum CompletionShell {
    Bash,
    Zsh,
    Fish,
}

impl From<CompletionShell> for clap_complete::Shell {
    fn from(value: CompletionShell) -> Self {
        match value {
            CompletionShell::Bash => Self::Bash,
            CompletionShell::Zsh => Self::Zsh,
            CompletionShell::Fish => Self::Fish,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum ActionOperationArg {
    #[value(name = "session.archive")]
    Archive,
    #[value(name = "session.unarchive")]
    Unarchive,
    #[value(name = "session.rename")]
    Rename,
}

impl From<ActionOperationArg> for ActionOperation {
    fn from(value: ActionOperationArg) -> Self {
        match value {
            ActionOperationArg::Archive => Self::SessionArchive,
            ActionOperationArg::Unarchive => Self::SessionUnarchive,
            ActionOperationArg::Rename => Self::SessionRename,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum SyntheticFaultArg {
    None,
    RejectBeforeApply,
    DelayBeforeDispatch,
    DelayBeforeApply,
    DelayResponseAfterApply,
    LoseResponseAfterApply,
}

impl From<SyntheticFaultArg> for SyntheticFault {
    fn from(value: SyntheticFaultArg) -> Self {
        match value {
            SyntheticFaultArg::None => Self::None,
            SyntheticFaultArg::RejectBeforeApply => Self::RejectBeforeApply,
            SyntheticFaultArg::DelayBeforeDispatch => Self::DelayBeforeDispatch,
            SyntheticFaultArg::DelayBeforeApply => Self::DelayBeforeApply,
            SyntheticFaultArg::DelayResponseAfterApply => Self::DelayResponseAfterApply,
            SyntheticFaultArg::LoseResponseAfterApply => Self::LoseResponseAfterApply,
        }
    }
}

#[derive(Subcommand)]
enum IndexCommand {
    /// Show whether the selected database has a current AQL-owned index.
    Status {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        /// Select text or stable JSON status rendering.
        #[arg(long, value_enum, default_value_t = IndexStatusOutput::Text)]
        output: IndexStatusOutput,
    },
    /// Build a new transactional index generation for the selected database.
    Build {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        /// Store metadata only, or include authorized message Content.
        #[arg(long, value_enum, default_value_t = IndexPolicyArg::Metadata)]
        policy: IndexPolicyArg,
        /// Grant sensitive access needed by the selected index policy.
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        /// Confirm that Content will be copied into persistent AQL-owned storage.
        #[arg(long)]
        acknowledge_persistent_sensitive_copy: bool,
        #[arg(long, env = "AQL_MAX_RECORDS", default_value = "100k", value_parser = parse_count)]
        max_records: u64,
        #[arg(long, env = "AQL_MAX_BYTES_READ", default_value = "256MiB", value_parser = parse_byte_size)]
        max_bytes_read: u64,
        #[arg(long, env = "AQL_MAX_INDEX_BYTES", default_value = "512MiB", value_parser = parse_byte_size)]
        max_index_bytes: u64,
        #[arg(long, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    /// Incrementally publish a replacement generation using saved watermarks.
    Update {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        #[arg(long, value_enum, default_value_t = IndexPolicyArg::Metadata)]
        policy: IndexPolicyArg,
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        #[arg(long)]
        acknowledge_persistent_sensitive_copy: bool,
        #[arg(long, env = "AQL_MAX_RECORDS", default_value = "100k", value_parser = parse_count)]
        max_records: u64,
        #[arg(long, env = "AQL_MAX_BYTES_READ", default_value = "256MiB", value_parser = parse_byte_size)]
        max_bytes_read: u64,
        #[arg(long, env = "AQL_MAX_INDEX_BYTES", default_value = "512MiB", value_parser = parse_byte_size)]
        max_index_bytes: u64,
        #[arg(long, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
        timeout: Duration,
    },
    /// Remove one source index or all indexes owned by AQL.
    Clear {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
        #[arg(long, conflicts_with = "all")]
        source_id: Option<String>,
        #[arg(long, conflicts_with = "source_id")]
        all: bool,
        #[arg(long, requires = "all")]
        acknowledge_clear_all_indexes: bool,
    },
    /// Remove only validated abandoned AQL index generations.
    Repair {
        #[arg(long, hide = true, conflicts_with_all = ["source", "profile", "database"])]
        data_root: Option<PathBuf>,
        #[arg(long = "source", hide = true, conflicts_with_all = ["profile", "database"])]
        source: Vec<String>,
        #[arg(long, hide = true, conflicts_with_all = ["data_root", "source", "database"])]
        profile: Option<String>,
        #[arg(
            short = 'd',
            long,
            conflicts_with_all = ["data_root", "source", "profile"],
            required_unless_present_any = ["data_root", "source", "profile"]
        )]
        database: Option<String>,
    },
}

struct IndexWriteRequest {
    data_root: Option<PathBuf>,
    source: Vec<String>,
    profile: Option<String>,
    database: Option<String>,
    policy: IndexPolicyArg,
    access: Vec<Access>,
    acknowledge_persistent_sensitive_copy: bool,
    max_records: u64,
    max_bytes_read: u64,
    max_index_bytes: u64,
    timeout: Duration,
}

#[derive(Clone, Copy, ValueEnum)]
enum IndexPolicyArg {
    Metadata,
    Content,
}

#[derive(Clone, Copy, ValueEnum)]
enum IndexStatusOutput {
    Text,
    Json,
}

impl From<IndexPolicyArg> for IndexPolicy {
    fn from(value: IndexPolicyArg) -> Self {
        match value {
            IndexPolicyArg::Metadata => Self::Metadata,
            IndexPolicyArg::Content => Self::Content,
        }
    }
}

#[derive(Clone, Copy, Subcommand)]
enum ReportKind {
    /// Render database-wide session, message, tool and usage totals.
    Summary,
    /// Render activity grouped by canonical project.
    Project,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
enum Output {
    Table,
    Json,
    Jsonl,
    Csv,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
enum CsvFormulaMode {
    Safe,
    Raw,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Access {
    Path,
    Content,
    ToolInput,
    ToolOutput,
}

struct ParsedSource {
    adapter_id: String,
    root: PathBuf,
    canonical_root: PathBuf,
}

struct SourceInputs {
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
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

fn profile_to_source_inputs(profile: Profile) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    let source_specs = profile
        .sources
        .into_iter()
        .map(|source| {
            let root = source
                .source_root
                .to_str()
                .ok_or("profile source path is invalid")?;
            Ok(format!("{}={root}", source.adapter_id))
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    Ok(SourceInputs {
        data_root: None,
        source_specs,
    })
}

struct DatabaseCandidate {
    name: &'static str,
    adapter_id: &'static str,
    root: PathBuf,
}

fn database_candidates() -> Result<Vec<DatabaseCandidate>, Box<dyn std::error::Error>> {
    let home = PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?);
    if !home.is_absolute() {
        return Err("HOME must be absolute for database discovery".into());
    }
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"));
    if !data_home.is_absolute() {
        return Err("XDG_DATA_HOME must be absolute for database discovery".into());
    }
    Ok(vec![
        DatabaseCandidate {
            name: "claude",
            adapter_id: "claude-code",
            root: home.join(".claude"),
        },
        DatabaseCandidate {
            name: "codex",
            adapter_id: "codex",
            root: home.join(".codex"),
        },
        DatabaseCandidate {
            name: "kimi",
            adapter_id: "kimi-code",
            root: home.join(".kimi-code"),
        },
        DatabaseCandidate {
            name: "opencode",
            adapter_id: "opencode",
            root: data_home.join("opencode"),
        },
    ])
}

fn read_adapter(
    adapter_id: &str,
    installation_salt: &[u8],
) -> Result<Arc<dyn AgentAdapter>, Box<dyn std::error::Error>> {
    match adapter_id {
        "claude-code" => Ok(Arc::new(ClaudeCodeAdapter::new(installation_salt.to_vec()))),
        "codex" => Ok(Arc::new(CodexAdapter::new(installation_salt.to_vec()))),
        "kimi-code" => Ok(Arc::new(KimiCodeAdapter::new(installation_salt.to_vec()))),
        "opencode" => Ok(Arc::new(OpenCodeAdapter::new(installation_salt.to_vec()))),
        _ => Err("unknown source adapter".into()),
    }
}

fn bind_sources(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
    installation_salt: Vec<u8>,
) -> Result<Vec<FederatedSource>, Box<dyn std::error::Error>> {
    let parsed = parse_sources(data_root, source_specs)?;
    let mut bound = Vec::new();
    let mut source_ids = std::collections::BTreeSet::new();
    for source in parsed {
        let adapter = read_adapter(&source.adapter_id, &installation_salt)?;
        let probe = adapter.probe(&ProbeRequest {
            data_root: source.root.to_string_lossy().into_owned(),
        })?;
        if probe.manifests.is_empty() {
            return Err("probe returned no compatible source".into());
        }
        for manifest in probe.manifests {
            if !source_ids.insert(manifest.source_id.clone()) {
                return Err("duplicate source identity".into());
            }
            bound.push(FederatedSource {
                adapter: adapter.clone(),
                manifest,
            });
        }
    }
    Ok(bound)
}

fn parse_sources(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
) -> Result<Vec<ParsedSource>, Box<dyn std::error::Error>> {
    if data_root.is_some() && !source_specs.is_empty() {
        return Err("--data-root cannot be combined with --source".into());
    }
    let raw = if let Some(root) = data_root {
        vec![("codex".to_string(), root, false)]
    } else {
        if source_specs.is_empty() {
            return Err("at least one source is required".into());
        }
        if source_specs.len() > 16 {
            return Err("source count exceeds the supported limit".into());
        }
        let mut parsed = Vec::with_capacity(source_specs.len());
        for spec in source_specs {
            let (adapter_id, root) = spec
                .split_once('=')
                .ok_or("source must use adapter=/absolute/path syntax")?;
            if !matches!(
                adapter_id,
                "claude-code" | "codex" | "kimi-code" | "opencode"
            ) {
                return Err("unknown source adapter".into());
            }
            if root.is_empty() {
                return Err("source path cannot be empty".into());
            }
            parsed.push((adapter_id.to_string(), PathBuf::from(root), true));
        }
        parsed
    };

    let mut result = Vec::with_capacity(raw.len());
    for (adapter_id, root, require_absolute) in raw {
        if require_absolute && !root.is_absolute() {
            return Err("source path must be absolute".into());
        }
        let canonical = fs::canonicalize(&root).map_err(|_| "source path is unavailable")?;
        result.push(ParsedSource {
            adapter_id,
            root,
            canonical_root: canonical,
        });
    }
    for (index, left) in result.iter().enumerate() {
        for right in result.iter().skip(index + 1) {
            if left.canonical_root == right.canonical_root
                || left.canonical_root.starts_with(&right.canonical_root)
                || right.canonical_root.starts_with(&left.canonical_root)
            {
                return Err("duplicate or overlapping source roots".into());
            }
        }
    }
    Ok(result)
}

fn resolve_source_inputs(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
    profile_name: Option<String>,
    database_name: Option<String>,
) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    if let Some(database) = database_name {
        if data_root.is_some() || !source_specs.is_empty() || profile_name.is_some() {
            return Err(
                "--database cannot be combined with --data-root, --source or --profile".into(),
            );
        }
        return resolve_database_inputs(&database);
    }
    if profile_name.is_none() {
        if data_root.is_none() && source_specs.is_empty() {
            return Err("at least one explicit source or --profile is required".into());
        }
        return Ok(SourceInputs {
            data_root,
            source_specs,
        });
    }
    if data_root.is_some() || !source_specs.is_empty() {
        return Err("--profile cannot be combined with --data-root or --source".into());
    }
    let name = profile_name.ok_or("profile selection is invalid")?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let store = ConfigStore::open_existing(&aql_config_root()?, std::slice::from_ref(&state_root))?;
    let profile = store.get_validated(&name, std::slice::from_ref(&state_root))?;
    profile_to_source_inputs(profile)
}

fn resolve_single_source_root(
    data_root: Option<PathBuf>,
    source_specs: Vec<String>,
    profile_name: Option<String>,
    database_name: Option<String>,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let inputs = resolve_source_inputs(data_root, source_specs, profile_name, database_name)?;
    let mut parsed = parse_sources(inputs.data_root, inputs.source_specs)?;
    if parsed.len() != 1 {
        return Err("this index operation requires a database with exactly one source".into());
    }
    Ok(parsed.remove(0).canonical_root)
}

fn profile_source_inputs(name: &str) -> Result<Option<SourceInputs>, Box<dyn std::error::Error>> {
    aql_config::validate_profile_name(name)?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let config_root = aql_config_root()?;
    match fs::symlink_metadata(&config_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let store = match ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root)) {
        Ok(store) => store,
        Err(ConfigError::Missing) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !store.list()?.iter().any(|profile| profile.name == name) {
        return Ok(None);
    }
    let profile = store.get_validated(name, std::slice::from_ref(&state_root))?;
    Ok(Some(profile_to_source_inputs(profile)?))
}

fn candidate_is_compatible(
    candidate: &DatabaseCandidate,
    deadline: Instant,
    salt: &[u8],
) -> Result<bool, Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        return Err("database discovery timed out".into());
    }
    let metadata = match fs::symlink_metadata(&candidate.root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Ok(false),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(false);
    }
    let adapter = read_adapter(candidate.adapter_id, salt)?;
    Ok(adapter
        .probe(&ProbeRequest {
            data_root: candidate.root.to_string_lossy().into_owned(),
        })
        .is_ok_and(|probe| !probe.manifests.is_empty()))
}

fn resolve_database_inputs(name: &str) -> Result<SourceInputs, Box<dyn std::error::Error>> {
    let normalized = name.to_ascii_lowercase();
    if name != normalized
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err("database name must use lowercase ASCII letters, digits, '_' or '-'".into());
    }
    if let Some(inputs) = profile_source_inputs(name)? {
        return Ok(inputs);
    }
    let candidates = database_candidates()?;
    if name == "all" {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(5))
            .ok_or("database discovery timeout is invalid")?;
        let salt: [u8; 32] = rand::random();
        let mut source_specs = Vec::new();
        for candidate in &candidates {
            if candidate_is_compatible(candidate, deadline, &salt)? {
                source_specs.push(format!(
                    "{}={}",
                    candidate.adapter_id,
                    candidate.root.to_string_lossy()
                ));
            }
        }
        if source_specs.is_empty() {
            return Err("database 'all' has no compatible local Agent data".into());
        }
        return Ok(SourceInputs {
            data_root: None,
            source_specs,
        });
    }
    let candidate = candidates
        .into_iter()
        .find(|candidate| candidate.name == name)
        .ok_or("unknown database; run SHOW DATABASES")?;
    Ok(SourceInputs {
        data_root: None,
        source_specs: vec![format!(
            "{}={}",
            candidate.adapter_id,
            candidate.root.to_string_lossy()
        )],
    })
}

fn aql_config_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let root = if let Some(path) = std::env::var_os("AQL_CONFIG_HOME") {
        PathBuf::from(path)
    } else if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("aql")
    } else {
        PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?).join(".config/aql")
    };
    if !root.is_absolute() {
        return Err("AQL config root must be absolute".into());
    }
    Ok(root)
}

fn execute_profile_command(
    command: ProfileCommand,
    noun: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_root = aql_config_root()?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    match command {
        ProfileCommand::Add {
            name,
            source,
            acknowledge_persistent_path,
        } => {
            if !acknowledge_persistent_path {
                return Err("profile add requires --acknowledge-persistent-path".into());
            }
            aql_config::validate_profile_name(&name)?;
            let parsed = parse_sources(None, source)?;
            let mut protected = Vec::with_capacity(parsed.len() + 1);
            protected.push(state_root.clone());
            protected.extend(parsed.iter().map(|item| item.canonical_root.clone()));
            let profile = Profile {
                name: name.clone(),
                sources: parsed
                    .into_iter()
                    .map(|item| ProfileSource {
                        adapter_id: item.adapter_id,
                        source_root: item.canonical_root,
                    })
                    .collect(),
            };
            let store = ConfigStore::create(&config_root, &protected)?;
            let lock = store.acquire_write_lock()?;
            store.add(profile, std::slice::from_ref(&state_root), lock)?;
            println!("{noun}={name}");
            println!("status=added");
        }
        ProfileCommand::List => {
            let profiles =
                match ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root)) {
                    Ok(store) => store.list()?,
                    Err(ConfigError::Missing) => Vec::new(),
                    Err(error) => return Err(error.into()),
                };
            println!("{noun}s={}", profiles.len());
            for profile in profiles {
                let adapters = profile
                    .sources
                    .iter()
                    .map(|source| source.adapter_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "{noun}={} sources={} adapters={adapters}",
                    profile.name,
                    profile.sources.len()
                );
            }
        }
        ProfileCommand::Show { name, access } => {
            let store =
                ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root))?;
            let profile = store.get_validated(&name, std::slice::from_ref(&state_root))?;
            let path_access = access_grant(&access).path;
            println!("{noun}={}", profile.name);
            println!("sources={}", profile.sources.len());
            for (index, source) in profile.sources.iter().enumerate() {
                println!("source.{}.adapter={}", index + 1, source.adapter_id);
                if path_access {
                    println!(
                        "source.{}.root={}",
                        index + 1,
                        source.source_root.to_string_lossy()
                    );
                } else {
                    println!("source.{}.root=masked", index + 1);
                }
            }
            if path_access && !io::stdout().is_terminal() {
                eprintln!("warning=Path access was granted for non-terminal output");
            }
        }
        ProfileCommand::Remove { name } => {
            aql_config::validate_profile_name(&name)?;
            let store =
                ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root))?;
            let lock = store.acquire_write_lock()?;
            store.remove(&name, lock)?;
            println!("{noun}={name}");
            println!("status=removed");
        }
    }
    Ok(())
}

fn execute_database_command(command: DatabaseCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        DatabaseCommand::List => {
            let names = available_database_names()?;
            println!("databases={}", names.len());
            for name in names {
                println!("database={name}");
            }
            Ok(())
        }
        DatabaseCommand::Discover => discover_sources(),
        DatabaseCommand::Add {
            name,
            member,
            agent,
            path,
            acknowledge_persistent_path,
        } => {
            let members = if member.is_empty() {
                if agent.len() != path.len() {
                    return Err(
                        "database add requires the same number of --agent and --path values".into(),
                    );
                }
                agent.into_iter().zip(path).collect::<Vec<_>>()
            } else {
                member
                    .into_iter()
                    .map(|member| {
                        let (agent, path) = member
                            .split_once('=')
                            .ok_or("database member must use AGENT=/absolute/path syntax")?;
                        if agent.is_empty() || path.is_empty() {
                            return Err("database member agent and path cannot be empty".into());
                        }
                        Ok((agent.to_string(), PathBuf::from(path)))
                    })
                    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?
            };
            let source = members
                .into_iter()
                .map(
                    |(agent, path)| -> Result<String, Box<dyn std::error::Error>> {
                        let adapter = match agent.as_str() {
                            "claude" | "claude-code" => "claude-code",
                            "codex" => "codex",
                            "kimi" | "kimi-code" => "kimi-code",
                            "opencode" => "opencode",
                            _ => {
                                return Err(
                                    "unknown database agent; use claude, codex, kimi or opencode"
                                        .into(),
                                );
                            }
                        };
                        let path = path.to_str().ok_or("database path is not valid UTF-8")?;
                        Ok(format!("{adapter}={path}"))
                    },
                )
                .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
            execute_profile_command(
                ProfileCommand::Add {
                    name,
                    source,
                    acknowledge_persistent_path,
                },
                "database",
            )
        }
        DatabaseCommand::Show { name, access } => {
            if profile_source_inputs(&name)?.is_some() {
                return execute_profile_command(ProfileCommand::Show { name, access }, "database");
            }
            let path_access = access_grant(&access).path;
            if name == "all" {
                println!("database=all");
                println!("kind=explicit-federation");
                println!(
                    "members={}",
                    available_database_names()?
                        .into_iter()
                        .filter(|database| database != "all")
                        .collect::<Vec<_>>()
                        .join(",")
                );
                return Ok(());
            }
            let candidate = database_candidates()?
                .into_iter()
                .find(|candidate| candidate.name == name)
                .ok_or("unknown database")?;
            let deadline = Instant::now()
                .checked_add(Duration::from_secs(5))
                .ok_or("database discovery timeout is invalid")?;
            let salt: [u8; 32] = rand::random();
            println!("database={name}");
            println!("agent={}", candidate.adapter_id);
            println!(
                "status={}",
                if candidate_is_compatible(&candidate, deadline, &salt)? {
                    "compatible"
                } else {
                    "unavailable"
                }
            );
            if path_access {
                println!("path={}", candidate.root.to_string_lossy());
                if !io::stdout().is_terminal() {
                    eprintln!("warning=Path access was granted for non-terminal output");
                }
            } else {
                println!("path=masked");
            }
            Ok(())
        }
        DatabaseCommand::Remove { name } => {
            execute_profile_command(ProfileCommand::Remove { name }, "database")
        }
    }
}

fn execute_source_command(command: SourcesCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        SourcesCommand::Discover => discover_sources(),
    }
}

fn discover_sources() -> Result<(), Box<dyn std::error::Error>> {
    let candidates = database_candidates()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or("discovery timeout is invalid")?;
    let ephemeral_salt: [u8; 32] = rand::random();
    let mut results = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if Instant::now() >= deadline {
            return Err("source discovery timed out".into());
        }
        let status = match fs::symlink_metadata(&candidate.root) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => "missing",
            Err(_) => "incompatible",
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                "incompatible"
            }
            Ok(_) => {
                let adapter = read_adapter(candidate.adapter_id, &ephemeral_salt)?;
                match adapter.probe(&ProbeRequest {
                    data_root: candidate.root.to_string_lossy().into_owned(),
                }) {
                    Ok(probe) if !probe.manifests.is_empty() => "compatible",
                    _ => "incompatible",
                }
            }
        };
        results.push((candidate.name, status));
    }
    if Instant::now() >= deadline {
        return Err("source discovery timed out".into());
    }
    for (database, status) in results {
        println!("database={database} status={status}");
    }
    Ok(())
}

fn configured_database_names() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    let config_root = aql_config_root()?;
    match fs::symlink_metadata(&config_root) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    match ConfigStore::open_existing(&config_root, std::slice::from_ref(&state_root)) {
        Ok(store) => Ok(store
            .list()?
            .into_iter()
            .map(|profile| profile.name)
            .collect()),
        Err(ConfigError::Missing) => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

fn available_database_names() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or("database discovery timeout is invalid")?;
    let salt: [u8; 32] = rand::random();
    let mut names = configured_database_names()?
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut built_in_count = 0_usize;
    for candidate in database_candidates()? {
        if candidate_is_compatible(&candidate, deadline, &salt)? {
            names.insert(candidate.name.to_string());
            built_in_count += 1;
        }
    }
    if built_in_count > 0 {
        names.insert("all".to_string());
    }
    Ok(names.into_iter().collect())
}

fn database_is_available(name: &str) -> Result<bool, Box<dyn std::error::Error>> {
    if profile_source_inputs(name)?.is_some() {
        return Ok(true);
    }
    let candidates = database_candidates()?;
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .ok_or("database discovery timeout is invalid")?;
    let salt: [u8; 32] = rand::random();
    if name == "all" {
        for candidate in &candidates {
            if candidate_is_compatible(candidate, deadline, &salt)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    let Some(candidate) = candidates
        .into_iter()
        .find(|candidate| candidate.name == name)
    else {
        return Ok(false);
    };
    candidate_is_compatible(&candidate, deadline, &salt)
}

fn drain_shell_statements(buffer: &mut String) -> Result<Vec<String>, &'static str> {
    if buffer.len() > 64 * 1024 {
        return Err("statement exceeds the fixed 64 KiB limit");
    }
    let bytes = buffer.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0_usize;
    let mut index = 0_usize;
    let mut single_quote = false;
    let mut double_quote = false;
    let mut line_comment = false;
    let mut block_comment = false;
    while index < bytes.len() {
        if line_comment {
            if bytes[index] == b'\n' {
                line_comment = false;
            }
            index += 1;
            continue;
        }
        if block_comment {
            if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') {
                block_comment = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if single_quote {
            if bytes[index] == b'\'' {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                } else {
                    single_quote = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if double_quote {
            if bytes[index] == b'"' {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 2;
                } else {
                    double_quote = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        match bytes[index] {
            b'\'' => single_quote = true,
            b'"' => double_quote = true,
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                line_comment = true;
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                block_comment = true;
                index += 1;
            }
            b';' => {
                let statement = buffer[start..index].trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    if start > 0 {
        buffer.drain(..start);
    }
    Ok(statements)
}

fn shell_words(statement: &str) -> Vec<String> {
    let mut command = statement.trim_start();
    loop {
        if let Some(rest) = command.strip_prefix("--") {
            command = rest
                .split_once('\n')
                .map_or("", |(_, remainder)| remainder)
                .trim_start();
            continue;
        }
        if let Some(rest) = command.strip_prefix("/*") {
            command = rest
                .split_once("*/")
                .map_or("", |(_, remainder)| remainder)
                .trim_start();
            continue;
        }
        break;
    }
    command
        .split_ascii_whitespace()
        .map(|word| word.to_ascii_uppercase())
        .collect()
}

fn query_type_name(data_type: QueryDataType) -> &'static str {
    match data_type {
        QueryDataType::Text => "TEXT",
        QueryDataType::Int64 => "BIGINT",
        QueryDataType::Bool => "BOOLEAN",
        QueryDataType::Timestamp => "TIMESTAMP",
        QueryDataType::Json => "JSON",
    }
}

fn access_class_name(access: AccessClass) -> &'static str {
    match access {
        AccessClass::Safe => "SAFE",
        AccessClass::Path => "PATH",
        AccessClass::Content => "CONTENT",
        AccessClass::ToolInput => "TOOL_INPUT",
        AccessClass::ToolOutput => "TOOL_OUTPUT",
        AccessClass::Secret => "SECRET",
    }
}

const SQL_EXAMPLES: [(&str, &str); 4] = [
    (
        "sessions-by-model",
        "SELECT model, COUNT(*) AS sessions\nFROM sessions\nGROUP BY model\nORDER BY sessions DESC;",
    ),
    (
        "token-usage",
        "SELECT agent_id, SUM(total_tokens) AS total_tokens\nFROM usage\nGROUP BY agent_id\nORDER BY total_tokens DESC;",
    ),
    (
        "recent-sessions",
        "SELECT session_id, agent_id, model, updated_at\nFROM sessions\nORDER BY updated_at DESC\nLIMIT 20;",
    ),
    (
        "recent-tools",
        "SELECT agent_id, tool_name, started_at\nFROM tool_calls\nORDER BY started_at DESC\nLIMIT 20;",
    ),
];

fn render_schema(
    table: Option<String>,
    output: SchemaOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    let selected = if let Some(table) = table {
        let table = table.to_ascii_lowercase();
        vec![
            QUERY_SCHEMAS
                .iter()
                .find(|schema| schema.name == table)
                .ok_or("unknown canonical table")?,
        ]
    } else {
        QUERY_SCHEMAS.iter().collect::<Vec<_>>()
    };
    match output {
        SchemaOutput::Table => {
            println!("table\tcolumn\ttype\tnullable\taccess");
            for schema in selected {
                for column in schema.columns {
                    println!(
                        "{}\t{}\t{}\t{}\t{}",
                        schema.name,
                        column.name,
                        query_type_name(column.data_type),
                        if column.nullable { "YES" } else { "NO" },
                        access_class_name(column.access),
                    );
                }
            }
        }
        SchemaOutput::Json => {
            let tables = selected
                .into_iter()
                .map(|schema| {
                    serde_json::json!({
                        "name": schema.name,
                        "columns": schema.columns.iter().map(|column| serde_json::json!({
                            "name": column.name,
                            "type": query_type_name(column.data_type),
                            "nullable": column.nullable,
                            "access": access_class_name(column.access).to_ascii_lowercase(),
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            println!("{}", serde_json::to_string(&tables)?);
        }
    }
    Ok(())
}

fn render_schema_list(output: SchemaOutput) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        SchemaOutput::Table => {
            println!("table");
            for schema in QUERY_SCHEMAS {
                println!("{}", schema.name);
            }
        }
        SchemaOutput::Json => println!(
            "{}",
            serde_json::to_string(
                &QUERY_SCHEMAS
                    .iter()
                    .map(|schema| schema.name)
                    .collect::<Vec<_>>()
            )?
        ),
    }
    Ok(())
}

fn render_examples(name: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(name) = name {
        let sql = SQL_EXAMPLES
            .iter()
            .find_map(|(candidate, sql)| (*candidate == name).then_some(*sql))
            .ok_or("unknown example; run `aql examples`")?;
        println!("{sql}");
    } else {
        println!("example");
        for (name, _) in SQL_EXAMPLES {
            println!("{name}");
        }
    }
    Ok(())
}

fn grant_shell_access(words: &[String], access: &mut Vec<Access>) -> Result<(), &'static str> {
    let grant = match words {
        [grant, class, for_word, session]
            if grant == "GRANT" && for_word == "FOR" && session == "SESSION" =>
        {
            match class.as_str() {
                "CONTENT" => Access::Content,
                "PATH" => Access::Path,
                _ => return Err("expected CONTENT, PATH, TOOL INPUT or TOOL OUTPUT"),
            }
        }
        [grant, tool, direction, for_word, session]
            if grant == "GRANT" && tool == "TOOL" && for_word == "FOR" && session == "SESSION" =>
        {
            match direction.as_str() {
                "INPUT" => Access::ToolInput,
                "OUTPUT" => Access::ToolOutput,
                _ => return Err("expected TOOL INPUT or TOOL OUTPUT"),
            }
        }
        _ => return Err("use GRANT <class> FOR SESSION"),
    };
    if !access.contains(&grant) {
        access.push(grant);
    }
    Ok(())
}

struct ShellHelper {
    candidates: Vec<String>,
}

impl ShellHelper {
    fn new(databases: &[String]) -> Self {
        let mut candidates = vec![
            "SELECT",
            "WITH",
            "EXPLAIN",
            "SHOW",
            "DATABASES",
            "TABLES",
            "ACCESS",
            "STATUS",
            "USE",
            "DESCRIBE",
            "GRANT",
            "REVOKE",
            "CONTENT",
            "PATH",
            "TOOL",
            "INPUT",
            "OUTPUT",
            "FOR",
            "SESSION",
            "HELP",
            "EXIT",
            "QUIT",
            "FROM",
            "WHERE",
            "GROUP",
            "BY",
            "ORDER",
            "LIMIT",
            "JOIN",
            "ON",
            "AS",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        candidates.extend(QUERY_SCHEMAS.iter().map(|schema| schema.name.to_owned()));
        candidates.extend(
            QUERY_SCHEMAS
                .iter()
                .flat_map(|schema| schema.columns.iter().map(|column| column.name.to_owned())),
        );
        candidates.extend(databases.iter().cloned());
        candidates.sort();
        candidates.dedup();
        Self { candidates }
    }
}

impl Helper for ShellHelper {}
impl Validator for ShellHelper {}
impl Highlighter for ShellHelper {}
impl Hinter for ShellHelper {
    type Hint = String;
}
impl Completer for ShellHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        position: usize,
        _context: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let start = line[..position]
            .rfind(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .map_or(0, |index| index + 1);
        let prefix = line[start..position].to_ascii_lowercase();
        let matches = self
            .candidates
            .iter()
            .filter(|candidate| candidate.to_ascii_lowercase().starts_with(&prefix))
            .map(|candidate| Pair {
                display: candidate.clone(),
                replacement: candidate.clone(),
            })
            .collect();
        Ok((start, matches))
    }
}

async fn run_shell(initial_database: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err("interactive shell requires terminal stdin and stdout".into());
    }
    let mut selected_database = None;
    if let Some(database) = initial_database {
        let database = database.to_ascii_lowercase();
        if !database_is_available(&database)? {
            return Err("unknown or unavailable database; run SHOW DATABASES".into());
        }
        selected_database = Some(database);
    }
    let mut access = Vec::new();
    let mut buffer = String::new();
    let databases = available_database_names()?;
    let editor_config = rustyline::Config::builder()
        .max_history_size(100)?
        .history_ignore_space(true)
        .build();
    let mut editor =
        rustyline::Editor::<ShellHelper, rustyline::history::DefaultHistory>::with_config(
            editor_config,
        )?;
    editor.set_helper(Some(ShellHelper::new(&databases)));
    for line in shell_welcome(&databases, selected_database.as_deref()) {
        println!("{line}");
    }
    loop {
        let prompt = if buffer.is_empty() {
            shell_prompt(selected_database.as_deref(), &access)
        } else {
            "      -> ".to_owned()
        };
        let line = match editor.readline(&prompt) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                buffer.clear();
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!();
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        buffer.push_str(&line);
        buffer.push('\n');
        let statements = match drain_shell_statements(&mut buffer) {
            Ok(statements) => statements,
            Err(error) => {
                eprintln!("ERROR: {error}");
                buffer.clear();
                continue;
            }
        };
        for statement in statements {
            let _ = editor.add_history_entry(statement.as_str());
            let words = shell_words(&statement);
            match words.as_slice() {
                [show, databases] if show == "SHOW" && databases == "DATABASES" => {
                    println!("database");
                    for database in available_database_names()? {
                        println!("{database}");
                    }
                }
                [use_word, database] if use_word == "USE" => {
                    let database = database.to_ascii_lowercase();
                    if !database_is_available(&database)? {
                        eprintln!(
                            "ERROR: Unknown or unavailable database '{database}'. Run SHOW DATABASES;"
                        );
                    } else {
                        selected_database = Some(database.clone());
                        println!("Database changed to {database}");
                    }
                }
                [show, tables] if show == "SHOW" && tables == "TABLES" => {
                    println!("table");
                    for schema in QUERY_SCHEMAS {
                        println!("{}", schema.name);
                    }
                }
                [show, access_word] if show == "SHOW" && access_word == "ACCESS" => {
                    println!("access");
                    if access.is_empty() {
                        println!("none");
                    } else {
                        for grant in &access {
                            println!("{}", format!("{grant:?}").to_ascii_lowercase());
                        }
                    }
                }
                [show, status] if show == "SHOW" && status == "STATUS" => {
                    println!(
                        "database={} access_grants={} timeout=30s max_records=100000 history=persistent:false",
                        selected_database.as_deref().unwrap_or("none"),
                        access.len(),
                    );
                }
                [describe, table] if describe == "DESCRIBE" || describe == "DESC" => {
                    let table = table.to_ascii_lowercase();
                    if let Some(schema) = QUERY_SCHEMAS.iter().find(|schema| schema.name == table) {
                        println!("column\ttype\tnullable\taccess");
                        for column in schema.columns {
                            println!(
                                "{}\t{}\t{}\t{}",
                                column.name,
                                query_type_name(column.data_type),
                                if column.nullable { "YES" } else { "NO" },
                                access_class_name(column.access)
                            );
                        }
                    } else {
                        eprintln!("ERROR: Unknown table '{table}'. Run SHOW TABLES;");
                    }
                }
                [revoke, all, for_word, session]
                    if revoke == "REVOKE"
                        && all == "ALL"
                        && for_word == "FOR"
                        && session == "SESSION" =>
                {
                    access.clear();
                    println!("Session access revoked");
                }
                [first, ..] if first == "GRANT" => match grant_shell_access(&words, &mut access) {
                    Ok(()) => println!("Session access granted"),
                    Err(error) => eprintln!("ERROR: {error}"),
                },
                [exit] if exit == "EXIT" || exit == "QUIT" => return Ok(()),
                [help] if help == "HELP" => {
                    println!(
                        "SHOW DATABASES; | USE <database>; | SHOW TABLES; | DESCRIBE <table>;"
                    );
                    println!(
                        "SHOW ACCESS; | SHOW STATUS; | GRANT <class> FOR SESSION; | REVOKE ALL FOR SESSION;"
                    );
                    println!("SELECT ...; | WITH ... SELECT ...; | EXPLAIN SELECT ...; | EXIT;");
                }
                [first, ..] if first == "SELECT" || first == "WITH" || first == "EXPLAIN" => {
                    let Some(database) = selected_database.clone() else {
                        eprintln!(
                            "ERROR: No database selected. Run SHOW DATABASES; and USE <database>;"
                        );
                        continue;
                    };
                    let query = Cli {
                        error_format: ErrorFormat::Text,
                        quiet: false,
                        command: Some(Command::Query {
                            data_root: None,
                            source: Vec::new(),
                            profile: None,
                            database: Some(database),
                            output: Output::Table,
                            csv_formulas: CsvFormulaMode::Safe,
                            acknowledge_raw_csv_formulas: false,
                            access: access.clone(),
                            max_records: 100_000,
                            max_bytes_read: 256 * 1024 * 1024,
                            max_output_bytes: 64 * 1024 * 1024,
                            max_single_value_bytes: 16 * 1024 * 1024,
                            max_memory_bytes: 256 * 1024 * 1024,
                            timeout: Duration::from_secs(30),
                            plan: false,
                            metadata: false,
                            shell_summary: true,
                            sql: Some(statement),
                            file: None,
                            stdin: false,
                        }),
                    };
                    if let Err(error) = Box::pin(run(query)).await {
                        eprintln!("ERROR: {error}");
                    }
                }
                _ => {
                    eprintln!(
                        "ERROR: Expected HELP, SHOW, USE, DESCRIBE, GRANT, REVOKE, SELECT, EXPLAIN or EXIT"
                    )
                }
            }
        }
    }
}

fn shell_prompt(selected_database: Option<&str>, access: &[Access]) -> String {
    let grants = if access.is_empty() {
        "safe".to_string()
    } else {
        access
            .iter()
            .map(|grant| match grant {
                Access::Path => "path",
                Access::Content => "content",
                Access::ToolInput => "tool-input",
                Access::ToolOutput => "tool-output",
            })
            .collect::<Vec<_>>()
            .join("+")
    };
    format!("aql[{}|{grants}]> ", selected_database.unwrap_or("none"))
}

fn shell_welcome(databases: &[String], selected_database: Option<&str>) -> Vec<String> {
    let mut lines = vec![
        "AQL interactive shell. End statements with ';'.".to_string(),
        format!("Known databases: {}", databases.join(", ")),
    ];
    if let Some(database) = selected_database {
        lines.push(format!("Selected database: {database}"));
        lines.push("Next: SELECT * FROM sessions LIMIT 10;".to_string());
    } else {
        lines.extend([
            "1. SHOW DATABASES;".to_string(),
            "2. USE <database>;".to_string(),
            "3. SELECT * FROM sessions LIMIT 10;".to_string(),
        ]);
    }
    lines
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
            max_records,
            max_bytes_read,
            max_output_bytes,
            max_single_value_bytes,
            max_memory_bytes,
            timeout,
            plan,
            metadata,
            shell_summary,
            sql,
            file,
            stdin,
        } => {
            validate_csv_options(output, csv_formulas, acknowledge_raw_csv_formulas)?;
            let sql = read_sql_input(sql, file, stdin)?;
            let query_started = Instant::now();
            let (sql, explain) = match explain_sql(&sql) {
                Some(inner) if !inner.is_empty() => (inner, true),
                Some(_) => return Err("EXPLAIN requires one SELECT or WITH query".into()),
                None => (sql.as_str(), false),
            };
            let validated_sql = validate_read_only_sql(sql)?;
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
            prepare_query(&validated_sql, options.clone()).await?;
            let inputs = resolve_source_inputs(data_root, source, profile, database)?;
            let installation_salt = installation_salt()?;
            options.redaction_salt = installation_salt.clone();
            let cancellation = options.cancellation.clone();
            let budget = options.budget.clone();
            let prepared = prepare_query(&validated_sql, options).await?;
            let sources = bind_sources(inputs.data_root, inputs.source_specs, installation_salt)?;
            if sources.is_empty() {
                return Err("probe returned no compatible source".into());
            }
            if plan || explain {
                let summary = prepared.plan_summary();
                eprintln!("plan.tables={}", summary.tables.join(","));
                eprintln!("plan.columns={}", summary.columns.join(","));
                eprintln!("plan.required_access={}", summary.required_access.join(","));
                eprintln!("plan.max_records={}", summary.max_records);
                eprintln!("plan.max_bytes_read={}", summary.max_bytes_read);
                eprintln!("plan.max_output_bytes={}", summary.max_output_bytes);
                eprintln!("plan.max_memory_bytes={}", summary.max_memory_bytes);
                for source in &sources {
                    eprintln!("plan.source_id={}", source.manifest.source_id);
                    eprintln!("plan.format={}", source.manifest.format_fingerprint);
                }
                return Ok(());
            }
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
            if shell_summary && !quiet {
                eprintln!(
                    "({returned_rows} rows, {} ms)",
                    query_started.elapsed().as_millis()
                );
            }
        }
        Command::Export {
            data_root,
            source,
            profile,
            database,
            access,
            max_records,
            max_bytes_read,
            max_output_bytes,
            max_single_value_bytes,
            max_memory_bytes,
            timeout,
            file,
            sql,
        } => {
            let validated_sql = validate_read_only_sql(&sql)?;
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
                stream_portable_json(output.writer(), result, &budget, &cancellation, remaining)
                    .await?;
            } else {
                if !access.is_empty() && !io::stdout().is_terminal() && !quiet {
                    eprintln!("warning=sensitive access was granted for non-terminal output");
                }
                let mut transaction = TransactionalOutput::new(max_memory_bytes);
                stream_portable_json(&mut transaction, result, &budget, &cancellation, remaining)
                    .await?;
                publish_bytes(
                    &mut io::stdout().lock(),
                    transaction.as_bytes(),
                    &cancellation,
                )?;
            }
            if let Some(output) = secure_output {
                output.commit()?;
            }
        }
        Command::Report {
            data_root,
            source,
            profile,
            database,
            access,
            max_records,
            max_bytes_read,
            max_output_bytes,
            max_single_value_bytes,
            max_memory_bytes,
            timeout,
            report,
        } => {
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
        }
        Command::Search {
            data_root,
            source,
            profile,
            database,
            access,
            limit,
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
            for (store, generations) in &stores {
                for generation in generations {
                    if Instant::now() >= deadline {
                        return Err("search timed out".into());
                    }
                    hits.extend(store.search_generation(generation, &query, limit)?);
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

fn execute_action_command(command: ActionCommand) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ActionCommand::Capabilities {
            source_id,
            synthetic_channel_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), None)?;
            let source_id = SourceId::new(source_id);
            let adapter = action_adapter(
                &source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::None,
            )?;
            let capabilities = adapter.action_capabilities(&source_id)?;
            ensure_action_deadline(deadline)?;
            match output {
                ActionOutput::Text => {
                    for capability in capabilities {
                        println!("operation={}", capability.operation);
                        println!("capability_version={}", capability.capability_version);
                        println!(
                            "required_access={}",
                            match capability.required_access {
                                aql_actions::ActionAccess::Safe => "safe",
                                aql_actions::ActionAccess::Content => "content",
                            }
                        );
                        println!("reversible={}", capability.reversible);
                        match capability.status {
                            CapabilityStatus::Supported {
                                official_channel_id,
                                official_channel_version,
                                ..
                            } => {
                                println!("status=supported");
                                println!("official_channel_id={official_channel_id}");
                                println!("official_channel_version={official_channel_version}");
                            }
                            CapabilityStatus::Unsupported { reason } => {
                                println!("status=unsupported");
                                println!("reason={}", unsupported_reason_name(reason));
                            }
                        }
                    }
                }
                ActionOutput::Json => println!("{}", serde_json::to_string(&capabilities)?),
            }
        }
        ActionCommand::Plan {
            data_root,
            source_id,
            entity_id,
            operation,
            new_title,
            access,
            ttl,
            synthetic_channel_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            let operation = ActionOperation::from(operation);
            validate_action_arguments(operation, new_title.as_deref(), &access)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), Some(&data_root))?;
            let source_id = SourceId::new(source_id);
            let entity_id = EntityId::new(entity_id);
            let adapter = action_adapter(
                &source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::None,
            )?;
            let capability = supported_capability(&*adapter, &source_id, operation)?;
            let observed = adapter.observe_target(&source_id, &entity_id)?;
            ensure_action_deadline(deadline)?;
            if observed.source_id != source_id || observed.entity_id != entity_id {
                return Err("Action Adapter returned a mismatched target binding".into());
            }
            let ttl_ms = i64::try_from(ttl.as_millis())?;
            if ttl_ms <= 0 || ttl_ms > MAX_PLAN_TTL_MS {
                return Err("Action plan TTL must be between 1ms and 1h".into());
            }
            let signing_key = installation_salt()?;
            let now_ms = unix_time_ms()?;
            let arguments = match operation {
                ActionOperation::SessionRename => ActionArguments::rename(
                    new_title.as_deref().ok_or("rename requires --new-title")?,
                    &signing_key,
                )?,
                ActionOperation::SessionArchive | ActionOperation::SessionUnarchive => {
                    ActionArguments::None
                }
            };
            let action_id = format!("action-{:032x}", rand::random::<u128>());
            let plan = ActionPlan::sign(
                UnsignedActionPlan {
                    schema_version: ACTION_PLAN_SCHEMA_VERSION.to_string(),
                    action_id: action_id.clone(),
                    idempotency_key: format!("idempotency-{:032x}", rand::random::<u128>()),
                    adapter_id: capability_channel_id(&capability)?.to_string(),
                    capability_version: capability.capability_version,
                    source_id,
                    entity_id,
                    operation,
                    arguments,
                    expected_revision: observed.revision.clone(),
                    created_at_ms: now_ms,
                    expires_at_ms: now_ms
                        .checked_add(ttl_ms)
                        .ok_or("Action plan expiry overflow")?,
                },
                &signing_key,
            )?;
            let state_root = aql_state_root()?;
            ensure_action_deadline(deadline)?;
            let store = ActionStore::create(&state_root, &data_root)?;
            let lock = store.acquire_write_lock()?;
            let _ = store.publish_plan(&lock, plan.clone(), now_ms, &signing_key)?;
            lock.release()?;
            render_action_plan_summary(&plan, &observed.revision, &signing_key, output)?;
        }
        ActionCommand::Apply {
            data_root,
            action_id,
            confirm,
            new_title,
            access,
            synthetic_channel_root,
            synthetic_fault,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), Some(&data_root))?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let lock = store.acquire_write_lock()?;
            let stored = store.load_plan(&action_id)?;
            let now_ms = unix_time_ms()?;
            stored.plan.verify(&signing_key, now_ms)?;
            stored.plan.confirm(&confirm)?;
            validate_action_arguments(
                stored.plan.unsigned.operation,
                new_title.as_deref(),
                &access,
            )?;
            if stored.plan.unsigned.operation == ActionOperation::SessionRename {
                stored.plan.unsigned.arguments.verify_rename(
                    new_title.as_deref().ok_or("rename requires --new-title")?,
                    &signing_key,
                )?;
            }
            let _ = store.recover_incomplete_audit_tail(&lock, &signing_key)?;
            if store
                .latest_audit_for_action(&action_id, &signing_key)?
                .is_some()
            {
                return Err(
                    "Action plan has already been consumed; inspect or reconcile it".into(),
                );
            }
            let adapter = action_adapter(
                &stored.plan.unsigned.source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::from(synthetic_fault),
            )?;
            let capability = supported_capability(
                &*adapter,
                &stored.plan.unsigned.source_id,
                stored.plan.unsigned.operation,
            )?;
            if capability.capability_version != stored.plan.unsigned.capability_version
                || capability_channel_id(&capability)? != stored.plan.unsigned.adapter_id
            {
                return Err("Action capability changed after planning".into());
            }
            let observed = adapter.observe_target(
                &stored.plan.unsigned.source_id,
                &stored.plan.unsigned.entity_id,
            )?;
            if observed.revision != stored.plan.unsigned.expected_revision {
                return Err("Action target revision changed; create a new plan".into());
            }
            ensure_action_deadline(deadline)?;
            store.append_audit_transition(
                &lock,
                &stored.plan,
                ActionState::IntentDurable,
                SanitizedResultCode::IntentRecorded,
                now_ms,
                &signing_key,
            )?;
            if matches!(synthetic_fault, SyntheticFaultArg::DelayBeforeDispatch) {
                std::thread::sleep(Duration::from_millis(250));
            }
            if Instant::now() >= deadline {
                store.append_audit_transition(
                    &lock,
                    &stored.plan,
                    ActionState::ReconciledNotApplied,
                    SanitizedResultCode::ReconciledNotApplied,
                    unix_time_ms()?,
                    &signing_key,
                )?;
                lock.release()?;
                return Err("Action timed out before dispatch".into());
            }
            store.append_audit_transition(
                &lock,
                &stored.plan,
                ActionState::Executing,
                SanitizedResultCode::DispatchStarted,
                unix_time_ms()?,
                &signing_key,
            )?;
            let approved = ApprovedAction {
                plan: stored.plan.clone(),
                supplied_rename: new_title,
            };
            let result = match adapter.execute(&approved) {
                Ok(result) => result,
                Err(_) => ActionExecutionResult::UnknownOutcome,
            };
            let (state, code) = execution_audit(result);
            if let Err(error) = store.append_audit_transition(
                &lock,
                &stored.plan,
                state,
                code,
                unix_time_ms()?,
                &signing_key,
            ) {
                return Err(format!(
                    "Action outcome is unknown because durable outcome recording failed: {error}"
                )
                .into());
            }
            lock.release()?;
            render_action_result(&action_id, state, output)?;
        }
        ActionCommand::Inspect {
            data_root,
            action_id,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let stored = store.load_plan(&action_id)?;
            stored.plan.verify_digest(&signing_key)?;
            let latest = store.latest_audit_for_action(&action_id, &signing_key)?;
            ensure_action_deadline(deadline)?;
            let state = latest
                .as_ref()
                .map_or(ActionState::Planned, |record| record.unsigned.state);
            render_action_inspect(
                &stored.plan,
                externally_visible_action_state(state),
                &signing_key,
                output,
            )?;
        }
        ActionCommand::Reconcile {
            data_root,
            action_id,
            synthetic_channel_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), Some(&data_root))?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let lock = store.acquire_write_lock()?;
            let stored = store.load_plan(&action_id)?;
            stored.plan.verify_digest(&signing_key)?;
            let _ = store.recover_incomplete_audit_tail(&lock, &signing_key)?;
            let latest = store
                .latest_audit_for_action(&action_id, &signing_key)?
                .ok_or("Action has no durable intent to reconcile")?;
            let mut state = latest.unsigned.state;
            if matches!(
                state,
                ActionState::Succeeded
                    | ActionState::Conflicted
                    | ActionState::Rejected
                    | ActionState::ReconciledSucceeded
                    | ActionState::ReconciledNotApplied
                    | ActionState::ManualIntervention
            ) {
                lock.release()?;
                return render_action_result(&action_id, state, output);
            }
            if state == ActionState::Executing {
                store.append_audit_transition(
                    &lock,
                    &stored.plan,
                    ActionState::UnknownOutcome,
                    SanitizedResultCode::OutcomeUnknown,
                    unix_time_ms()?,
                    &signing_key,
                )?;
                state = ActionState::UnknownOutcome;
            }
            ensure_action_deadline(deadline)?;
            let adapter = action_adapter(
                &stored.plan.unsigned.source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::None,
            )?;
            let reconciliation = adapter.reconcile(
                &stored.plan.unsigned.action_id,
                &stored.plan.unsigned.idempotency_key,
            )?;
            let (final_state, code) = reconciliation_audit(state, reconciliation)?;
            store.append_audit_transition(
                &lock,
                &stored.plan,
                final_state,
                code,
                unix_time_ms()?,
                &signing_key,
            )?;
            lock.release()?;
            render_action_result(&action_id, final_state, output)?;
        }
        ActionCommand::AuditVerify {
            data_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let records = store.verify_audit(&signing_key)?;
            ensure_action_deadline(deadline)?;
            match output {
                ActionOutput::Text => println!("audit=valid\nrecords={records}"),
                ActionOutput::Json => println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "audit": "valid",
                        "records": records,
                    }))?
                ),
            }
        }
    }
    Ok(())
}

fn action_adapter(
    source_id: &SourceId,
    synthetic_channel_root: Option<&std::path::Path>,
    fault: SyntheticFault,
) -> Result<Box<dyn AgentActionAdapter>, Box<dyn std::error::Error>> {
    Ok(match synthetic_channel_root {
        Some(root) => Box::new(SyntheticActionAdapter::open(root)?.with_fault(fault)),
        None if source_id.as_str().starts_with("claude-code:") => Box::new(ClaudeCodeActionAdapter),
        None if source_id.as_str().starts_with("kimi-code:") => Box::new(KimiCodeActionAdapter),
        None if source_id.as_str().starts_with("opencode:") => Box::new(OpenCodeActionAdapter),
        None => Box::new(CodexActionAdapter),
    })
}

fn action_deadline(timeout: Duration) -> Result<Instant, Box<dyn std::error::Error>> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "Action timeout is too large".into())
}

fn ensure_action_deadline(deadline: Instant) -> Result<(), Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        Err("Action timed out before dispatch".into())
    } else {
        Ok(())
    }
}

fn validate_synthetic_root_isolation(
    synthetic_channel_root: Option<&std::path::Path>,
    data_root: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(synthetic_root) = synthetic_channel_root else {
        return Ok(());
    };
    let synthetic_root = synthetic_root.canonicalize()?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    if paths_overlap(&synthetic_root, &state_root) {
        return Err("synthetic Action channel must be isolated from AQL state".into());
    }
    if let Some(data_root) = data_root {
        let data_root = data_root.canonicalize()?;
        if paths_overlap(&synthetic_root, &data_root) {
            return Err("synthetic Action channel must be isolated from Agent data".into());
        }
    }
    Ok(())
}

fn canonical_or_prospective(path: &std::path::Path) -> io::Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "path has no final component")
            })?;
            Ok(parent.canonicalize()?.join(name))
        }
        Err(error) => Err(error),
    }
}

fn paths_overlap(left: &std::path::Path, right: &std::path::Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn supported_capability(
    adapter: &dyn AgentActionAdapter,
    source_id: &SourceId,
    operation: ActionOperation,
) -> Result<ActionCapability, Box<dyn std::error::Error>> {
    let capability = adapter
        .action_capabilities(source_id)?
        .into_iter()
        .find(|capability| capability.operation == operation)
        .ok_or("Action capability is not declared")?;
    match capability.status {
        CapabilityStatus::Supported { .. } => Ok(capability),
        CapabilityStatus::Unsupported { reason } => Err(format!(
            "Action capability is unsupported: {}",
            unsupported_reason_name(reason)
        )
        .into()),
    }
}

fn capability_channel_id(
    capability: &ActionCapability,
) -> Result<&str, Box<dyn std::error::Error>> {
    match &capability.status {
        CapabilityStatus::Supported {
            official_channel_id,
            ..
        } => Ok(official_channel_id),
        CapabilityStatus::Unsupported { .. } => Err("Action capability is unsupported".into()),
    }
}

fn validate_action_arguments(
    operation: ActionOperation,
    new_title: Option<&str>,
    access: &[Access],
) -> Result<(), Box<dyn std::error::Error>> {
    match operation {
        ActionOperation::SessionRename => {
            if !access_grant(access).content {
                return Err("session.rename requires --access content".into());
            }
            let title = new_title.ok_or("session.rename requires --new-title")?;
            if title.is_empty()
                || title.len() > 4_096
                || title.chars().any(char::is_control)
                || title.trim() != title
            {
                return Err("session.rename title is invalid".into());
            }
        }
        ActionOperation::SessionArchive | ActionOperation::SessionUnarchive => {
            if new_title.is_some() {
                return Err("--new-title is valid only for session.rename".into());
            }
        }
    }
    Ok(())
}

fn execution_audit(result: ActionExecutionResult) -> (ActionState, SanitizedResultCode) {
    match result {
        ActionExecutionResult::Succeeded => (ActionState::Succeeded, SanitizedResultCode::Applied),
        ActionExecutionResult::Conflicted => (
            ActionState::Conflicted,
            SanitizedResultCode::RevisionConflict,
        ),
        ActionExecutionResult::Rejected => (ActionState::Rejected, SanitizedResultCode::Rejected),
        ActionExecutionResult::UnknownOutcome => (
            ActionState::UnknownOutcome,
            SanitizedResultCode::OutcomeUnknown,
        ),
    }
}

fn reconciliation_audit(
    prior: ActionState,
    result: ActionReconciliation,
) -> Result<(ActionState, SanitizedResultCode), Box<dyn std::error::Error>> {
    Ok(match (prior, result) {
        (ActionState::UnknownOutcome, ActionReconciliation::Succeeded) => (
            ActionState::ReconciledSucceeded,
            SanitizedResultCode::ReconciledApplied,
        ),
        (
            ActionState::UnknownOutcome | ActionState::IntentDurable,
            ActionReconciliation::NotApplied,
        ) => (
            ActionState::ReconciledNotApplied,
            SanitizedResultCode::ReconciledNotApplied,
        ),
        (ActionState::UnknownOutcome, ActionReconciliation::ManualIntervention) => (
            ActionState::ManualIntervention,
            SanitizedResultCode::ManualInterventionRequired,
        ),
        _ => return Err("Action reconciliation result is inconsistent with audit state".into()),
    })
}

fn render_action_plan_summary(
    plan: &ActionPlan,
    revision: &str,
    signing_key: &[u8],
    output: ActionOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    let revision_commitment = installation_scoped_hmac("action-revision-v1", revision, signing_key);
    match output {
        ActionOutput::Text => {
            println!("action_id={}", plan.unsigned.action_id);
            println!("operation={}", plan.unsigned.operation);
            println!("source_id={}", plan.unsigned.source_id);
            println!("entity_id={}", plan.unsigned.entity_id);
            println!("revision_commitment={revision_commitment}");
            println!("expires_at_ms={}", plan.unsigned.expires_at_ms);
            println!("confirm={}", plan.plan_digest);
        }
        ActionOutput::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action_id": plan.unsigned.action_id,
                "operation": plan.unsigned.operation,
                "source_id": plan.unsigned.source_id,
                "entity_id": plan.unsigned.entity_id,
                "revision_commitment": revision_commitment,
                "expires_at_ms": plan.unsigned.expires_at_ms,
                "confirm": plan.plan_digest,
            }))?
        ),
    }
    Ok(())
}

fn render_action_inspect(
    plan: &ActionPlan,
    state: ActionState,
    signing_key: &[u8],
    output: ActionOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    let revision_commitment = installation_scoped_hmac(
        "action-revision-v1",
        &plan.unsigned.expected_revision,
        signing_key,
    );
    match output {
        ActionOutput::Text => {
            println!("action_id={}", plan.unsigned.action_id);
            println!("operation={}", plan.unsigned.operation);
            println!("state={}", action_state_name(state));
            println!("revision_commitment={revision_commitment}");
            println!("expires_at_ms={}", plan.unsigned.expires_at_ms);
        }
        ActionOutput::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action_id": plan.unsigned.action_id,
                "operation": plan.unsigned.operation,
                "state": action_state_name(state),
                "revision_commitment": revision_commitment,
                "expires_at_ms": plan.unsigned.expires_at_ms,
            }))?
        ),
    }
    Ok(())
}

fn render_action_result(
    action_id: &str,
    state: ActionState,
    output: ActionOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        ActionOutput::Text => {
            println!("action_id={action_id}");
            println!("state={}", action_state_name(state));
        }
        ActionOutput::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action_id": action_id,
                "state": action_state_name(state),
            }))?
        ),
    }
    Ok(())
}

fn action_state_name(state: ActionState) -> &'static str {
    match state {
        ActionState::Planned => "planned",
        ActionState::IntentDurable => "intent_durable",
        ActionState::Executing => "executing",
        ActionState::Succeeded => "succeeded",
        ActionState::Conflicted => "conflicted",
        ActionState::Rejected => "rejected",
        ActionState::UnknownOutcome => "unknown_outcome",
        ActionState::ReconciledSucceeded => "reconciled_succeeded",
        ActionState::ReconciledNotApplied => "reconciled_not_applied",
        ActionState::ManualIntervention => "manual_intervention",
    }
}

fn externally_visible_action_state(state: ActionState) -> ActionState {
    if state == ActionState::Executing {
        ActionState::UnknownOutcome
    } else {
        state
    }
}

fn unsupported_reason_name(reason: aql_actions::UnsupportedReason) -> &'static str {
    match reason {
        aql_actions::UnsupportedReason::OfficialChannelUndocumented => {
            "official_channel_undocumented"
        }
        aql_actions::UnsupportedReason::TargetBindingUnavailable => "target_binding_unavailable",
        aql_actions::UnsupportedReason::AtomicPreconditionUnavailable => {
            "atomic_precondition_unavailable"
        }
        aql_actions::UnsupportedReason::IdempotencyAndOutcomeUnavailable => {
            "idempotency_and_outcome_unavailable"
        }
        aql_actions::UnsupportedReason::StableResultUnavailable => "stable_result_unavailable",
        aql_actions::UnsupportedReason::DisposableProfileUnavailable => {
            "disposable_profile_unavailable"
        }
        aql_actions::UnsupportedReason::InverseOperationUnavailable => {
            "inverse_operation_unavailable"
        }
    }
}

fn unix_time_ms() -> Result<i64, Box<dyn std::error::Error>> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
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

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
struct SecureOutputFile {
    directory: std::os::fd::OwnedFd,
    directory_path: PathBuf,
    directory_identity: FileIdentity,
    target_name: std::ffi::OsString,
    temporary_name: std::ffi::OsString,
    file: fs::File,
    committed: bool,
}

#[cfg(unix)]
impl SecureOutputFile {
    fn create(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        use rustix::fs::{AtFlags, Mode, OFlags};

        let target_name = path
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or("export target must have a file name")?
            .to_os_string();
        let directory_path = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let directory = rustix::fs::openat(
            rustix::fs::CWD,
            directory_path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )?;
        let directory_stat = rustix::fs::fstat(&directory)?;
        let directory_identity = identity(&directory_stat);
        match rustix::fs::statat(&directory, &target_name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(_) => return Err("export target already exists".into()),
            Err(error) if error == rustix::io::Errno::NOENT => {}
            Err(error) => return Err(error.into()),
        }
        let temporary_name =
            std::ffi::OsString::from(format!(".aql-export-{:016x}.tmp", rand::random::<u64>()));
        let temporary = rustix::fs::openat(
            &directory,
            &temporary_name,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::from_raw_mode(0o600),
        )?;
        Ok(Self {
            directory,
            directory_path: directory_path.to_path_buf(),
            directory_identity,
            target_name,
            temporary_name,
            file: temporary.into(),
            committed: false,
        })
    }

    fn writer(&mut self) -> &mut fs::File {
        &mut self.file
    }

    fn commit(mut self) -> Result<(), Box<dyn std::error::Error>> {
        use rustix::fs::{AtFlags, RenameFlags};

        self.file.flush()?;
        self.file.sync_all()?;
        let current_directory =
            rustix::fs::statat(rustix::fs::CWD, &self.directory_path, AtFlags::empty())?;
        if identity(&current_directory) != self.directory_identity {
            return Err("export target directory changed during write".into());
        }
        match rustix::fs::statat(
            &self.directory,
            &self.target_name,
            AtFlags::SYMLINK_NOFOLLOW,
        ) {
            Ok(_) => return Err("export target appeared during write".into()),
            Err(error) if error == rustix::io::Errno::NOENT => {}
            Err(error) => return Err(error.into()),
        }
        rustix::fs::renameat_with(
            &self.directory,
            &self.temporary_name,
            &self.directory,
            &self.target_name,
            RenameFlags::NOREPLACE,
        )?;
        rustix::fs::fsync(&self.directory)?;
        self.committed = true;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for SecureOutputFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = rustix::fs::unlinkat(
                &self.directory,
                &self.temporary_name,
                rustix::fs::AtFlags::empty(),
            );
        }
    }
}

#[cfg(unix)]
fn identity(stat: &rustix::fs::Stat) -> FileIdentity {
    FileIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino,
    }
}

#[cfg(not(unix))]
struct SecureOutputFile;

#[cfg(not(unix))]
impl SecureOutputFile {
    fn create(_path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        Err("secure file export is not supported on this platform".into())
    }

    fn writer(&mut self) -> &mut fs::File {
        unreachable!()
    }

    fn commit(self) -> Result<(), Box<dyn std::error::Error>> {
        unreachable!()
    }
}

fn portable_metadata(metadata: QueryMetadata) -> serde_json::Value {
    serde_json::json!({
        "source_ids": metadata.source_ids,
        "warnings": metadata.warnings,
        "records_scanned": metadata.records_scanned,
        "bytes_read": metadata.bytes_read,
        "output_bytes_before_metadata": metadata.output_bytes,
        "scans": metadata.scans.into_iter().map(|scan| serde_json::json!({
            "table": scan.table,
            "source_id": scan.source_id,
            "snapshot_strength": scan.snapshot_strength,
            "stale": scan.stale,
        })).collect::<Vec<_>>(),
    })
}

fn write_export_chunk(
    writer: &mut impl Write,
    bytes: &[u8],
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<bool, Box<dyn std::error::Error>> {
    budget.charge_output_bytes(bytes.len() as u64)?;
    if let Err(error) = writer.write_all(bytes) {
        if error.kind() == io::ErrorKind::BrokenPipe {
            cancellation.cancel();
            return Ok(false);
        }
        return Err(error.into());
    }
    Ok(true)
}

struct TransactionalOutput {
    bytes: Vec<u8>,
    maximum: usize,
}

impl TransactionalOutput {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl Write for TransactionalOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("transactional output size overflow"))?;
        if next > self.maximum {
            return Err(io::Error::other(
                "transactional output exceeds the memory budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn publish_bytes(
    writer: &mut impl Write,
    bytes: &[u8],
    cancellation: &CancellationToken,
) -> io::Result<()> {
    if let Err(error) = writer.write_all(bytes) {
        if error.kind() == io::ErrorKind::BrokenPipe {
            cancellation.cancel();
            return Ok(());
        }
        return Err(error);
    }
    writer.flush()
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

fn batches_to_json(batches: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    Ok(serde_json::to_string(&batches_to_values(batches)?)?)
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

fn batches_to_jsonl(batches: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    let rows = batches_to_values(batches)?;
    Ok(rows
        .into_iter()
        .map(|row| serde_json::to_string(&row))
        .collect::<Result<Vec<_>, _>>()?
        .join("\n"))
}

struct CsvRendering {
    rendered: String,
    formula_escaped: bool,
}

fn validate_csv_options(
    output: Output,
    formulas: CsvFormulaMode,
    acknowledge_raw: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if output != Output::Csv && (formulas != CsvFormulaMode::Safe || acknowledge_raw) {
        return Err("CSV formula options require --output csv".into());
    }
    if output == Output::Csv && formulas == CsvFormulaMode::Raw && !acknowledge_raw {
        return Err("raw CSV formulas require --acknowledge-raw-csv-formulas".into());
    }
    if formulas == CsvFormulaMode::Safe && acknowledge_raw {
        return Err("--acknowledge-raw-csv-formulas requires --csv-formulas raw".into());
    }
    Ok(())
}

fn batches_to_csv(
    batches: &[RecordBatch],
    formulas: CsvFormulaMode,
) -> Result<CsvRendering, Box<dyn std::error::Error>> {
    let Some(first) = batches.first() else {
        return Ok(CsvRendering {
            rendered: String::new(),
            formula_escaped: false,
        });
    };
    let schema = first.schema();
    let mut rendered = String::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            rendered.push(',');
        }
        rendered.push_str(&csv_quote(field.name(), false)?);
    }
    rendered.push_str("\r\n");
    let mut formula_escaped = false;
    for batch in batches {
        if batch.schema() != schema {
            return Err("CSV batches have inconsistent schemas".into());
        }
        for row_index in 0..batch.num_rows() {
            for column_index in 0..batch.num_columns() {
                if column_index > 0 {
                    rendered.push(',');
                }
                let cell = csv_arrow_cell(
                    batch,
                    column_index,
                    row_index,
                    formulas,
                    &mut formula_escaped,
                )?;
                rendered.push_str(&cell);
            }
            rendered.push_str("\r\n");
        }
    }
    Ok(CsvRendering {
        rendered,
        formula_escaped,
    })
}

fn csv_arrow_cell(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
    formulas: CsvFormulaMode,
    formula_escaped: &mut bool,
) -> Result<String, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::DataType;

    if batch.column(column_index).is_null(row_index) {
        return Ok("\\N".to_string());
    }
    let field = batch.schema().field(column_index).clone();
    let value = arrow_json_value(batch, column_index, row_index)?;
    match value {
        serde_json::Value::Null => Ok("\\N".to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(mut value) => {
            let formula_sensitive = matches!(field.data_type(), DataType::Utf8)
                && !matches!(
                    field.name().as_str(),
                    "capabilities" | "content_json" | "arguments"
                );
            if formula_sensitive && formulas == CsvFormulaMode::Safe && starts_csv_formula(&value) {
                value.insert(0, '\'');
                *formula_escaped = true;
            }
            let force_quote = value.is_empty() || value == "\\N";
            csv_quote(&value, force_quote)
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            csv_quote(&serde_json::to_string(&value)?, false)
        }
    }
}

fn starts_csv_formula(value: &str) -> bool {
    value
        .as_bytes()
        .first()
        .is_some_and(|byte| matches!(byte, b'=' | b'+' | b'-' | b'@' | b'\t' | b'\r'))
}

fn csv_quote(value: &str, force: bool) -> Result<String, Box<dyn std::error::Error>> {
    if value.chars().any(|character| {
        character == '\0' || (character.is_control() && !matches!(character, '\t' | '\r' | '\n'))
    }) {
        return Err("CSV text contains an unsupported control character".into());
    }
    let quoted = force
        || value
            .as_bytes()
            .iter()
            .any(|byte| matches!(byte, b',' | b'"' | b'\r' | b'\n'));
    if !quoted {
        return Ok(value.to_string());
    }
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        if character == '"' {
            output.push('"');
        }
        output.push(character);
    }
    output.push('"');
    Ok(output)
}

fn batches_to_values(
    batches: &[RecordBatch],
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            rows.push(batch_row_to_value(batch, row_index)?);
        }
    }
    Ok(rows)
}

fn batch_row_to_value(
    batch: &RecordBatch,
    row_index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut row = serde_json::Map::new();
    for (column_index, field) in batch.schema().fields().iter().enumerate() {
        row.insert(
            field.name().clone(),
            arrow_json_value(batch, column_index, row_index)?,
        );
    }
    Ok(serde_json::Value::Object(row))
}

fn arrow_json_value(
    batch: &RecordBatch,
    column_index: usize,
    row_index: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use datafusion::arrow::array::{
        Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::datatypes::DataType;

    let array = batch.column(column_index);
    if array.is_null(row_index) {
        return Ok(serde_json::Value::Null);
    }
    let field = batch.schema().field(column_index).clone();
    match field.data_type() {
        DataType::Utf8 => {
            let value = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("invalid UTF-8 Arrow array")?
                .value(row_index);
            if matches!(
                field.name().as_str(),
                "capabilities" | "content_json" | "arguments"
            ) {
                Ok(serde_json::from_str(value)?)
            } else {
                Ok(serde_json::Value::String(value.to_string()))
            }
        }
        DataType::Int64 => Ok(serde_json::Value::Number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("invalid Int64 Arrow array")?
                .value(row_index)
                .into(),
        )),
        DataType::Boolean => Ok(serde_json::Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("invalid Boolean Arrow array")?
                .value(row_index),
        )),
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, _) => {
            let millis = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or("invalid timestamp Arrow array")?
                .value(row_index);
            let timestamp = chrono::DateTime::from_timestamp_millis(millis)
                .ok_or("timestamp is outside the supported range")?;
            Ok(serde_json::Value::String(timestamp.to_rfc3339()))
        }
        _ => Err("unsupported Arrow output type".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(database.as_deref(), Some("codex"));
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
            .is_ok()
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
    fn public_help_uses_database_model_and_hides_legacy_and_action_surfaces() {
        let help = generated_command().render_long_help().to_string();
        assert!(help.contains("database"));
        assert!(help.contains("schema"));
        assert!(help.contains("examples"));
        assert!(!help.contains("profile"));
        assert!(!help.contains("sources"));
        assert!(!help.contains("action"));
        assert!(Cli::try_parse_from(["aql", "--quiet", "schema"]).is_ok());
    }

    #[test]
    fn data_commands_accept_one_consistent_database_option() {
        for arguments in [
            vec!["aql", "doctor", "-d", "codex"],
            vec!["aql", "export", "-d", "codex", "SELECT 1"],
            vec![
                "aql",
                "export",
                "-d",
                "codex",
                "--output-file",
                "synthetic.json",
                "SELECT 1",
            ],
            vec![
                "aql",
                "export",
                "-d",
                "codex",
                "--file",
                "synthetic.json",
                "SELECT 1",
            ],
            vec!["aql", "report", "-d", "codex", "summary"],
            vec!["aql", "search", "-d", "codex", "synthetic"],
            vec!["aql", "index", "status", "-d", "codex"],
            vec!["aql", "index", "build", "-d", "codex"],
            vec!["aql", "index", "update", "-d", "codex"],
            vec!["aql", "index", "repair", "-d", "codex"],
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
            "--max-records",
            "10k",
            "--max-memory-bytes",
            "32MiB",
            "SELECT 1",
        ])
        .expect("human-readable CLI limits parse");
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
        assert_eq!(
            environment("max_records").as_deref(),
            Some("AQL_MAX_RECORDS")
        );
        assert_eq!(environment("database"), None);
        assert_eq!(environment("access"), None);
    }

    #[test]
    fn query_sql_inputs_are_mutually_exclusive_and_explain_is_plan_only() {
        Cli::try_parse_from(["aql", "query", "-d", "codex", "--file", "query.sql"])
            .expect("SQL file syntax");
        Cli::try_parse_from(["aql", "query", "-d", "codex", "--stdin"]).expect("stdin SQL syntax");
        assert!(
            Cli::try_parse_from(["aql", "query", "-d", "codex", "--stdin", "SELECT 1"]).is_err()
        );
        assert_eq!(explain_sql(" EXPLAIN SELECT 1"), Some("SELECT 1"));
        assert_eq!(
            explain_sql("EXPLAIN ANALYZE SELECT 1"),
            Some("ANALYZE SELECT 1")
        );
        assert_eq!(explain_sql("SELECT 1"), None);
    }

    #[test]
    fn sql_file_input_is_bounded_regular_and_no_follow() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let query = temporary
            .path()
            .canonicalize()
            .expect("canonical temporary directory")
            .join("query.sql");
        fs::write(&query, b"SELECT 1").expect("write query");
        assert_eq!(
            read_sql_input(None, Some(query.clone()), false).expect("read query"),
            "SELECT 1"
        );
        let link = temporary.path().join("query-link.sql");
        std::os::unix::fs::symlink(&query, &link).expect("query symlink");
        assert!(read_sql_input(None, Some(link), false).is_err());
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
        fs::write(&query, vec![b'x'; MAX_SQL_INPUT_BYTES as usize + 1])
            .expect("write oversized query");
        assert!(read_sql_input(None, Some(query), false).is_err());
    }

    #[test]
    fn errors_have_stable_hints_for_database_and_index_workflows() {
        let database = io::Error::other("unknown database; run SHOW DATABASES");
        assert_eq!(error_category(&database), "not_found");
        assert_eq!(error_exit_code(&database), 4);
        assert!(error_hint(&database).is_some());
        let index = io::Error::other("index rebuild required");
        assert_eq!(error_category(&index), "index_missing");
        assert!(error_hint(&index).is_some());
        assert_eq!(shell_quote("SELECT 1"), "'SELECT 1'");
        assert_eq!(shell_quote("a'b"), "'a'\"'\"'b'");
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
    fn source_specs_reject_unknown_ambiguous_duplicate_and_overlap_before_probe() {
        let root =
            std::env::temp_dir().join(format!("aql-source-parse-{:016x}", rand::random::<u64>()));
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create synthetic source roots");
        let absolute = root.to_string_lossy();
        let nested_absolute = nested.to_string_lossy();
        assert!(parse_sources(None, vec![format!("unknown={absolute}")]).is_err());
        assert!(parse_sources(None, vec![format!("claude-code={absolute}")]).is_ok());
        assert!(parse_sources(None, vec![format!("opencode={absolute}")]).is_ok());
        assert!(parse_sources(Some(root.clone()), vec![format!("codex={absolute}")]).is_err());
        assert!(
            parse_sources(
                None,
                vec![format!("codex={absolute}"), format!("kimi-code={absolute}")]
            )
            .is_err()
        );
        assert!(
            parse_sources(
                None,
                vec![
                    format!("codex={absolute}"),
                    format!("kimi-code={nested_absolute}")
                ]
            )
            .is_err()
        );
        fs::remove_dir_all(root).expect("remove synthetic source roots");
    }

    #[test]
    fn data_root_alias_accepts_relative_path_and_maps_to_codex() {
        let parsed = parse_sources(Some(PathBuf::from(".")), Vec::new())
            .expect("relative legacy data root remains accepted");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].adapter_id, "codex");
        assert!(parsed[0].canonical_root.is_absolute());
    }

    #[test]
    fn transactional_output_is_memory_bounded_and_publishes_only_complete_bytes() {
        let mut output = TransactionalOutput::new(4);
        output.write_all(b"safe").expect("bounded write succeeds");
        assert_eq!(output.as_bytes(), b"safe");
        assert!(output.write_all(b"x").is_err());

        let cancellation = CancellationToken::default();
        let mut published = Vec::new();
        publish_bytes(&mut published, output.as_bytes(), &cancellation)
            .expect("complete transaction publishes");
        assert_eq!(published, b"safe");
    }
    use datafusion::arrow::array::{
        BooleanArray, Int64Array, StringArray, TimestampMillisecondArray,
    };
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

        let csv =
            batches_to_csv(&[batch], CsvFormulaMode::Safe).expect("CSV conversion must succeed");
        assert_eq!(
            csv.rendered,
            "value\r\n\\N\r\n\"\"\r\n\"\\N\"\r\n\"comma,quote\"\"\nline\"\r\nUnicode-中文\r\n"
        );
        assert!(!csv.formula_escaped);
    }

    #[test]
    fn csv_output_is_formula_safe_by_default_and_raw_is_explicit() {
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

        let safe = batches_to_csv(std::slice::from_ref(&batch), CsvFormulaMode::Safe)
            .expect("safe CSV conversion must succeed");
        assert_eq!(
            safe.rendered,
            "title\r\n'=cmd\r\n'+sum\r\n'-1+2\r\n'@name\r\n'\tcell\r\n\"'\rcell\"\r\nsafe\r\n"
        );
        assert!(safe.formula_escaped);

        let raw = batches_to_csv(&[batch], CsvFormulaMode::Raw)
            .expect("raw CSV conversion must succeed after CLI validation");
        assert_eq!(
            raw.rendered,
            "title\r\n=cmd\r\n+sum\r\n-1+2\r\n@name\r\n\tcell\r\n\"\rcell\"\r\nsafe\r\n"
        );
        assert!(!raw.formula_escaped);

        assert!(validate_csv_options(Output::Csv, CsvFormulaMode::Raw, false).is_err());
        assert!(validate_csv_options(Output::Csv, CsvFormulaMode::Raw, true).is_ok());
        assert!(validate_csv_options(Output::Json, CsvFormulaMode::Raw, true).is_err());
        assert!(validate_csv_options(Output::Json, CsvFormulaMode::Safe, true).is_err());
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

        let csv = batches_to_csv(&[batch], CsvFormulaMode::Safe)
            .expect("typed CSV conversion must succeed");
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
        assert!(batches_to_csv(&[control], CsvFormulaMode::Safe).is_err());

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
        assert!(batches_to_csv(&[first, second], CsvFormulaMode::Safe).is_err());
    }

    #[test]
    fn generated_release_docs_are_deterministic_and_exclude_internal_arguments() {
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
        assert_eq!(metadata["index_schema"], INDEX_SCHEMA_VERSION);
        for forbidden in ["HOME", "USER", "workspace", "timestamp", "dirty", "secret"] {
            assert!(!first.contains(forbidden));
        }
    }

    #[test]
    fn markdown_cells_escape_tables_lines_controls_and_fences() {
        assert_eq!(
            markdown_escape("a|b\n```\r\u{0007}\\c"),
            "a\\|b<br>\\`\\`\\`<br>�\\\\c"
        );
        assert_eq!(markdown_value(&serde_json::Value::Null), "unknown");
        assert_eq!(
            markdown_value(&serde_json::json!({"safe": true})),
            "{\"safe\":true}"
        );
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
        assert_eq!(
            error_exit_code(&aql_actions::ActionError::ConfirmationMismatch),
            2
        );
        assert_eq!(
            error_category(&aql_actions::ActionError::ConfirmationMismatch),
            "invalid_request"
        );
        assert_eq!(
            error_category(&aql_actions::ActionError::AuditTampered),
            "state_integrity"
        );
        assert_eq!(
            error_category(&io::Error::other(
                "Action outcome is unknown because durable outcome recording failed"
            )),
            "unknown_outcome"
        );
    }

    #[test]
    fn executing_is_externally_reported_as_unknown_outcome() {
        assert_eq!(
            externally_visible_action_state(ActionState::Executing),
            ActionState::UnknownOutcome
        );
        assert_eq!(
            externally_visible_action_state(ActionState::Succeeded),
            ActionState::Succeeded
        );
    }

    #[cfg(unix)]
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
                .filter(|entry| entry.file_name().to_string_lossy().contains("aql-export"))
                .count(),
            0
        );
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
}
