use super::*;

#[derive(Parser)]
#[command(
    name = "aql",
    version,
    about = "Query local agent data",
    long_about = "Query explicitly selected local Claude Code, Codex, Kimi Code and OpenCode data. Run without a subcommand on a terminal for the SQL-first shell, or use `query --database <name>`. AQL never selects a default database. `all` must be explicit and checks only fixed local candidates. CSV output is formula-safe by default; sensitive Path/Content/tool grants and persistent database paths require explicit acknowledgement."
)]
pub(super) struct Cli {
    /// Select stable text or single-line JSON error rendering.
    #[arg(
        long,
        global = true,
        env = "AQL_ERROR_FORMAT",
        value_enum,
        default_value_t = ErrorFormat::Text
    )]
    pub(super) error_format: ErrorFormat,
    /// Suppress non-essential warnings and shell summaries; errors and requested metadata remain visible.
    #[arg(long, global = true)]
    pub(super) quiet: bool,
    #[command(subcommand)]
    pub(super) command: Option<Command>,
}

#[derive(Args, Clone)]
pub(super) struct ExecutionLimits {
    /// Stop after scanning this many canonical records across the complete operation.
    #[arg(long, env = "AQL_MAX_RECORDS", default_value = "100k", value_parser = parse_count)]
    pub(super) max_records: u64,
    /// Maximum source bytes that all adapters may read.
    #[arg(long, env = "AQL_MAX_BYTES_READ", default_value = "256MiB", value_parser = parse_byte_size)]
    pub(super) max_bytes_read: u64,
    /// Maximum bytes that may be published.
    #[arg(long, env = "AQL_MAX_OUTPUT_BYTES", default_value = "64MiB", value_parser = parse_byte_size)]
    pub(super) max_output_bytes: u64,
    /// Reject any single sensitive value larger than this limit.
    #[arg(long, env = "AQL_MAX_SINGLE_VALUE_BYTES", default_value = "16MiB", value_parser = parse_byte_size)]
    pub(super) max_single_value_bytes: u64,
    /// Maximum in-memory execution working set.
    #[arg(long, env = "AQL_MAX_MEMORY_BYTES", default_value = "256MiB", value_parser = parse_usize_byte_size)]
    pub(super) max_memory_bytes: usize,
    /// Cancel the complete operation when this duration elapses.
    #[arg(long, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
    pub(super) timeout: Duration,
}

#[derive(Subcommand)]
pub(super) enum Command {
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
        #[command(flatten)]
        limits: ExecutionLimits,
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
        #[command(flatten)]
        limits: ExecutionLimits,
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
        #[command(flatten)]
        limits: ExecutionLimits,
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
pub(super) enum SourcesCommand {
    /// Run bounded, non-persistent discovery without revealing candidate paths.
    Discover,
}

#[derive(Subcommand)]
pub(super) enum DatabaseCommand {
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
pub(super) enum ProfileCommand {
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
pub(super) enum ActionCommand {
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
pub(super) enum ActionOutput {
    Text,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum VersionOutput {
    Text,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum SchemaOutput {
    Table,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum ErrorFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum CompletionShell {
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
pub(super) enum ActionOperationArg {
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
pub(super) enum SyntheticFaultArg {
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
pub(super) enum IndexCommand {
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

pub(super) struct IndexWriteRequest {
    pub(super) data_root: Option<PathBuf>,
    pub(super) source: Vec<String>,
    pub(super) profile: Option<String>,
    pub(super) database: Option<String>,
    pub(super) policy: IndexPolicyArg,
    pub(super) access: Vec<Access>,
    pub(super) acknowledge_persistent_sensitive_copy: bool,
    pub(super) max_records: u64,
    pub(super) max_bytes_read: u64,
    pub(super) max_index_bytes: u64,
    pub(super) timeout: Duration,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum IndexPolicyArg {
    Metadata,
    Content,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum IndexStatusOutput {
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
pub(super) enum ReportKind {
    /// Render database-wide session, message, tool and usage totals.
    Summary,
    /// Render activity grouped by canonical project.
    Project,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
pub(super) enum Output {
    Table,
    Json,
    Jsonl,
    Csv,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
pub(super) enum CsvFormulaMode {
    Safe,
    Raw,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(super) enum Access {
    Path,
    Content,
    ToolInput,
    ToolOutput,
}

pub(super) struct ParsedSource {
    pub(super) adapter_id: String,
    pub(super) root: PathBuf,
    pub(super) canonical_root: PathBuf,
}

pub(super) struct SourceInputs {
    pub(super) data_root: Option<PathBuf>,
    pub(super) source_specs: Vec<String>,
}

pub(super) const MAX_SQL_INPUT_BYTES: u64 = 64 * 1024;
