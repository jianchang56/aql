use super::*;

#[derive(Parser)]
#[command(
    name = "aql",
    version,
    about = "Query local agent data",
    long_about = "Query explicitly selected local Claude Code, Codex, Kimi Code and OpenCode data. Run without a subcommand on a terminal for the SQL-first shell, or use `query --database <name>`. AQL never selects a default database. `all` must be explicit and checks only fixed local candidates. CSV output is always formula-safe; sensitive Path/Content/tool grants and persistent database paths require explicit acknowledgement."
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
    /// Suppress non-essential warnings and shell summaries; errors and diagnostics remain visible.
    #[arg(long, global = true)]
    pub(super) quiet: bool,
    #[command(subcommand)]
    pub(super) command: Option<Command>,
}

#[derive(Args, Clone)]
pub(super) struct ExecutionLimits {
    /// Maximum bytes that may be published.
    #[arg(long, global = true, env = "AQL_MAX_OUTPUT_BYTES", default_value = "64MiB", value_parser = parse_byte_size)]
    pub(super) max_output_bytes: u64,
    /// Cancel the complete operation when this duration elapses.
    #[arg(long, global = true, env = "AQL_TIMEOUT", default_value = "30s", value_parser = parse_duration)]
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
    /// Print deterministic package, target and canonical schema version metadata.
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
    /// Probe an explicitly selected database without mutating it.
    Doctor {
        /// Select the database to diagnose; no database is selected implicitly.
        #[arg(short = 'd', long)]
        database: String,
    },
    /// Run one read-only SQL query against an explicit database.
    Query {
        /// Select a built-in or configured database.
        #[arg(short = 'd', long)]
        database: String,
        /// Select table, JSON, JSONL or RFC 4180 CSV rendering.
        #[arg(long, value_enum, default_value_t = Output::Table)]
        output: Output,
        /// Atomically write the rendered result to this new file instead of stdout.
        #[arg(long)]
        output_file: Option<PathBuf>,
        /// Grant access to sensitive column classes for this query only; repeat as needed.
        #[arg(long = "access", value_enum)]
        access: Vec<Access>,
        /// Bind NAME=VALUE; use text:, int:, float: or bool: to disambiguate values.
        #[arg(long = "param", value_name = "NAME=VALUE")]
        param: Vec<String>,
        #[command(flatten)]
        limits: ExecutionLimits,
        /// Print privacy-safe source, scan, budget and timing diagnostics to stderr.
        #[arg(long)]
        diagnostics: bool,
        #[arg(skip)]
        shell_summary: bool,
        #[arg(conflicts_with_all = ["file", "stdin"], required_unless_present_any = ["file", "stdin"])]
        sql: Option<String>,
        /// Read one read-only query from a bounded local .aql file.
        #[arg(long, conflicts_with_all = ["sql", "stdin"])]
        file: Option<PathBuf>,
        /// Read one SQL statement from stdin.
        #[arg(long, conflicts_with_all = ["sql", "file"])]
        stdin: bool,
    },
    /// Discover and manage logical databases.
    Database {
        #[command(subcommand)]
        database: DatabaseCommand,
    },
    /// Show one table schema, or use --list for a short overview.
    Schema {
        #[arg(conflicts_with = "list")]
        table: Option<String>,
        /// List canonical table names without rendering their columns.
        #[arg(long)]
        list: bool,
        /// Select table or stable JSON rendering.
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
pub(super) enum DatabaseCommand {
    /// List available built-in and configured databases.
    List,
    /// Probe fixed local Agent candidates without revealing their paths.
    Discover,
    /// Add a named database backed by one or more explicit Agent paths.
    Add {
        name: String,
        /// Add one atomic member as AGENT=/absolute/path; repeat for federation.
        #[arg(long = "member", value_name = "AGENT=PATH", required = true)]
        member: Vec<String>,
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
    /// Remove one configured database without touching Agent data.
    Remove { name: String },
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

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
pub(super) enum Output {
    Table,
    Json,
    Jsonl,
    Csv,
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
    pub(super) source_specs: Vec<String>,
    pub(super) skip_unavailable: bool,
}
