//! Read-only adapter for the pinned OpenCode 1.17.18 local SQLite format.

#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use aql_adapter_api::{
    AdapterError, AdapterSchema, AdapterWarning, AdapterWarningKind, AgentAdapter, Capabilities,
    ColumnCapability, FileAccessObserver, Predicate, ProbeRequest, ProbeResult, PushdownReport,
    PushdownState, ScanDiagnostics, ScanRequest, ScanResult, SnapshotReport, SnapshotStrength,
    SourceKind, TableName, check_scan_state,
    util::{
        SessionPredicateCapabilities, append_content, column_capability, immutable_uri,
        limit_pushdown, projected, session_matches, session_predicate_state,
    },
    validate_projection_access,
};
use aql_model::{
    CanonicalRecord, EntityId, IdentityConfidence, MessageRecord, NativeId, SessionEdgeRecord,
    SessionRecord, SnapshotState, SourceId, SourceManifest, ToolCallRecord, UsageRecord,
};
use chrono::{DateTime, Utc};
use rusqlite::config::DbConfig;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;
use serde_json::value::RawValue;

mod authorizer;
mod storage;

use authorizer::AuthorizerPolicy;
use storage::{optional_file_identity, safe_directory, safe_file, table_columns};

const MAX_JSON_RECORD_BYTES: u64 = 16 * 1024 * 1024;
// Rows pulled per keyset batch. The batch queries below bake in `LIMIT 256`
// and must agree with this value: a fill never buffers more rows than one
// batch query returns, and the keyset cursor only advances when a buffered
// row is consumed, so rows left unfetched are re-read by the next batch.
const SCAN_BATCH_ROWS: usize = 256;
// Memory guard for one buffered batch. A fill stops early once buffered
// payload bytes exceed this bound, so worst-case buffering stays near this
// value plus one record even for records close to `MAX_JSON_RECORD_BYTES`.
const SCAN_BATCH_BYTES: u64 = 8 * 1024 * 1024;
type MessageCursor = (String, i64, String);
type LoadedMessage = (CurrentMessage, MessageCursor, u64);
type SessionCursor = (i64, String);
type LoadedSession = (SessionRecord, SessionCursor, u64);

const FORMAT: &str = "opencode-1.17.18-schema-38-message-v1";
const TERMINAL_MIGRATION: &str = "20260622202450_simplify_session_input";

const MIGRATIONS: [&str; 38] = [
    "20260127222353_familiar_lady_ursula",
    "20260211171708_add_project_commands",
    "20260213144116_wakeful_the_professor",
    "20260225215848_workspace",
    "20260227213759_add_session_workspace_id",
    "20260228203230_blue_harpoon",
    "20260303231226_add_workspace_fields",
    "20260309230000_move_org_to_state",
    "20260312043431_session_message_cursor",
    "20260323234822_events",
    "20260410174513_workspace-name",
    "20260413175956_chief_energizer",
    "20260423070820_add_icon_url_override",
    "20260427172553_slow_nightmare",
    "20260428004200_add_session_path",
    "20260501142318_next_venus",
    "20260504145000_add_sync_owner",
    "20260507164347_add_workspace_time",
    "20260510033149_session_usage",
    "20260511000411_data_migration_state",
    "20260511173437_session-metadata",
    "20260601010001_normalize_storage_paths",
    "20260601202201_amazing_prowler",
    "20260602002951_lowly_union_jack",
    "20260602182828_add_project_directories",
    "20260603001617_session_message_projection_indexes",
    "20260603040000_session_message_projection_order",
    "20260603141458_session_input_inbox",
    "20260603160727_jittery_ezekiel_stane",
    "20260604172448_event_sourced_session_input",
    "20260605003541_add_session_context_snapshot",
    "20260605042240_add_context_epoch_agent",
    "20260611035744_credential",
    "20260611192811_lush_chimera",
    "20260612174303_project_dir_strategy",
    "20260622142730_simplify_session_context_epoch",
    "20260622170816_reset_v2_session_state",
    "20260622202450_simplify_session_input",
];

const SESSION_COLUMNS: [&str; 30] = [
    "id",
    "project_id",
    "workspace_id",
    "parent_id",
    "slug",
    "directory",
    "path",
    "title",
    "version",
    "share_url",
    "summary_additions",
    "summary_deletions",
    "summary_files",
    "summary_diffs",
    "metadata",
    "cost",
    "tokens_input",
    "tokens_output",
    "tokens_reasoning",
    "tokens_cache_read",
    "tokens_cache_write",
    "revert",
    "permission",
    "agent",
    "model",
    "time_created",
    "time_updated",
    "time_compacting",
    "time_archived",
    "rowid",
];
const MESSAGE_COLUMNS: [&str; 5] = ["id", "session_id", "time_created", "time_updated", "data"];
const PART_COLUMNS: [&str; 6] = [
    "id",
    "message_id",
    "session_id",
    "time_created",
    "time_updated",
    "data",
];

type FileIdentity = aql_fs::FileIdentity;

#[derive(Clone)]
struct RootBinding {
    path: PathBuf,
    root_identity: FileIdentity,
    database_identity: FileIdentity,
    wal_identity: Option<FileIdentity>,
    shm_identity: Option<FileIdentity>,
}

struct SessionStream {
    connection: Connection,
    binding: RootBinding,
    request: ScanRequest,
    after: Option<SessionCursor>,
    buffer: VecDeque<SessionRow>,
    predicates: Vec<Predicate>,
    effective_limit: Option<u64>,
    emitted: u64,
    finished: bool,
}

struct EdgeStream {
    connection: Connection,
    binding: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    after_child: Option<String>,
    buffer: VecDeque<EdgeRow>,
    emitted: u64,
    effective_limit: Option<u64>,
    finished: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum MessageMode {
    Messages,
    Tools,
}

struct MessagePartStream {
    connection: Connection,
    binding: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    mode: MessageMode,
    after_message: Option<MessageCursor>,
    messages: VecDeque<MessageRow>,
    current: Option<CurrentMessage>,
    parts: VecDeque<PartRow>,
    last_session: Option<String>,
    sequence: i64,
    emitted: u64,
    effective_limit: Option<u64>,
    finished: bool,
}

struct CurrentMessage {
    native_id: String,
    session_native_id: String,
    created_at: DateTime<Utc>,
    metadata: MessageMetadata,
    after_part: Option<String>,
    content: Option<String>,
}

enum ParsedPart {
    Text(Option<String>),
    Tool(ParsedTool),
    Ignored,
    Unknown,
}

struct ParsedTool {
    call_id: String,
    tool_name: String,
    arguments: Option<serde_json::Value>,
    output: Option<String>,
    status: String,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    duration_ms: Option<i64>,
}

struct MessageMetadata {
    role: String,
    model: Option<String>,
    provider: Option<String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_tokens: Option<i64>,
    total_tokens: Option<i64>,
    is_error: Option<bool>,
}

struct UsageStream {
    connection: Connection,
    binding: RootBinding,
    request: ScanRequest,
    after_message: Option<MessageCursor>,
    messages: VecDeque<MessageRow>,
    emitted: u64,
    effective_limit: Option<u64>,
    finished: bool,
}

/// One raw `message` row buffered by a batch pull. JSON parsing and budget
/// accounting are deferred until the row is consumed so error and charge
/// positions match a row-at-a-time read exactly.
struct MessageRow {
    native_id: String,
    session_native_id: String,
    created_ms: i64,
    data_len: i64,
    data: Vec<u8>,
}

/// One raw `part` row buffered by a batch pull, consumed like [`MessageRow`].
struct PartRow {
    part_id: String,
    data_len: i64,
    data: Vec<u8>,
}

/// One raw `session` row buffered by a batch pull. Sensitive columns stay
/// `None` unless the projection selected them, mirroring the previous
/// row-at-a-time read.
struct SessionRow {
    native: String,
    parent: Option<String>,
    agent: Option<String>,
    created_ms: i64,
    updated_ms: i64,
    archived_ms: Option<i64>,
    tokens: [i64; 5],
    model_len: Option<i64>,
    model: Option<String>,
    title_len: Option<i64>,
    title: Option<String>,
    directory_len: Option<i64>,
    directory: Option<String>,
    path_len: Option<i64>,
    path: Option<String>,
}

/// One raw parent-edge row buffered by a batch pull.
struct EdgeRow {
    child: String,
    parent: String,
    created_ms: i64,
    parent_exists: bool,
}

#[derive(Deserialize)]
struct ModelValue {
    id: String,
    #[serde(rename = "providerID")]
    provider_id: String,
}

#[derive(Deserialize)]
struct MessageRole {
    role: String,
}

#[derive(Deserialize)]
struct UserMessageData {
    model: UserMessageModel,
}

#[derive(Deserialize)]
struct UserMessageModel {
    #[serde(rename = "providerID")]
    provider_id: String,
    #[serde(rename = "modelID")]
    model_id: String,
}

#[derive(Deserialize)]
struct AssistantMessageData {
    #[serde(rename = "modelID")]
    model_id: String,
    #[serde(rename = "providerID")]
    provider_id: String,
    tokens: MessageTokens,
    error: Option<serde::de::IgnoredAny>,
}

#[derive(Deserialize)]
struct MessageTokens {
    total: Option<i64>,
    input: i64,
    output: i64,
    reasoning: i64,
    cache: CacheTokens,
}

#[derive(Deserialize)]
struct CacheTokens {
    read: i64,
    write: i64,
}

#[derive(Deserialize)]
struct PartKind {
    #[serde(rename = "type")]
    part_type: String,
}

#[derive(Deserialize)]
struct TextPart<'a> {
    #[serde(borrow)]
    text: &'a RawValue,
}

#[derive(Deserialize)]
struct ToolPart<'a> {
    #[serde(rename = "callID")]
    call_id: String,
    tool: String,
    #[serde(borrow)]
    state: &'a RawValue,
}

#[derive(Deserialize)]
struct ToolStateKind {
    status: String,
}

#[derive(Deserialize)]
struct ToolStatePending<'a> {
    #[serde(borrow)]
    input: &'a RawValue,
}

#[derive(Deserialize)]
struct ToolStateRunning<'a> {
    #[serde(borrow)]
    input: &'a RawValue,
    time: ToolStart,
}

#[derive(Deserialize)]
struct ToolStateCompleted<'a> {
    #[serde(borrow)]
    input: &'a RawValue,
    #[serde(borrow)]
    output: &'a RawValue,
    time: ToolRange,
}

#[derive(Deserialize)]
struct ToolStateError<'a> {
    #[serde(borrow)]
    input: &'a RawValue,
    #[serde(borrow)]
    error: &'a RawValue,
    time: ToolRange,
}

#[derive(Deserialize)]
struct ToolStart {
    start: i64,
}

#[derive(Deserialize)]
struct ToolRange {
    start: i64,
    end: i64,
}

/// Read-only adapter for the pinned OpenCode SQLite schema and active WAL view.
pub struct OpenCodeAdapter {
    installation_salt: Vec<u8>,
    observer: Option<Arc<dyn FileAccessObserver>>,
    roots: Mutex<BTreeMap<SourceId, RootBinding>>,
}

impl OpenCodeAdapter {
    /// Creates an adapter using an installation-local salt for stable source IDs.
    #[must_use]
    pub fn new(installation_salt: impl Into<Vec<u8>>) -> Self {
        Self {
            installation_salt: installation_salt.into(),
            observer: None,
            roots: Mutex::new(BTreeMap::new()),
        }
    }

    /// Installs a source-access observer used by contract tests and audits.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn FileAccessObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    fn opened(&self) {
        if let Some(observer) = &self.observer {
            observer.opened(SourceKind::StateDatabase);
        }
    }

    fn validate_root(path: &Path) -> Result<RootBinding, AdapterError> {
        #[cfg(unix)]
        let root_metadata = safe_directory(path, "opencode_root")?;
        #[cfg(not(unix))]
        safe_directory(path, "opencode_root")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if root_metadata.permissions().mode() & 0o022 != 0 {
                return Err(AdapterError::PermissionDenied {
                    stage: "opencode_root_permissions".to_string(),
                });
            }
        }
        let path = path.canonicalize().map_err(|_| AdapterError::NotFound {
            stage: "opencode_root_canonicalize".to_string(),
        })?;
        let database = path.join("opencode.db");
        safe_file(&database, "opencode_database")?;
        let wal_identity = optional_file_identity(&path.join("opencode.db-wal"), "opencode_wal")?;
        let shm_identity = optional_file_identity(&path.join("opencode.db-shm"), "opencode_shm")?;
        let root_identity =
            aql_fs::directory_identity(&path).map_err(|_| AdapterError::SnapshotUnavailable)?;
        let database_identity =
            aql_fs::file_identity(&database).map_err(|_| AdapterError::SnapshotUnavailable)?;
        Ok(RootBinding {
            path,
            root_identity,
            database_identity,
            wal_identity,
            shm_identity,
        })
    }

    fn validate_binding(binding: &RootBinding) -> Result<(), AdapterError> {
        safe_directory(&binding.path, "opencode_root_revalidate")?;
        safe_file(
            &binding.path.join("opencode.db"),
            "opencode_database_revalidate",
        )?;
        if aql_fs::directory_identity(&binding.path)
            .map_err(|_| AdapterError::SnapshotUnavailable)?
            != binding.root_identity
            || aql_fs::file_identity(&binding.path.join("opencode.db"))
                .map_err(|_| AdapterError::SnapshotUnavailable)?
                != binding.database_identity
            || optional_file_identity(
                &binding.path.join("opencode.db-wal"),
                "opencode_wal_revalidate",
            )? != binding.wal_identity
            || optional_file_identity(
                &binding.path.join("opencode.db-shm"),
                "opencode_shm_revalidate",
            )? != binding.shm_identity
        {
            return Err(AdapterError::SnapshotUnavailable);
        }
        Ok(())
    }

    fn open_connection(
        &self,
        binding: &RootBinding,
        policy: AuthorizerPolicy,
    ) -> Result<Connection, AdapterError> {
        Self::validate_binding(binding)?;
        self.opened();
        let database = binding.path.join("opencode.db");
        let connection = if binding.wal_identity.is_none() {
            // No WAL companion exists, so the database was cleanly checkpointed
            // and closed. Open it with immutable semantics so SQLite never
            // creates `-shm`/`-wal` sidecars and never attempts WAL recovery.
            // The binding keeps `wal_identity == None`; the identity re-checks
            // fail closed if a WAL ever appears afterwards.
            Connection::open_with_flags(
                immutable_uri(&database, "opencode_database_open")?,
                OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
        } else {
            Connection::open_with_flags(
                database,
                OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX
                    | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )
        }
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "opencode_database_open".to_string(),
        })?;
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)
            .and_then(|_| {
                connection.set_db_config(DbConfig::SQLITE_DBCONFIG_WRITABLE_SCHEMA, false)
            })
            .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false))
            .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false))
            .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_VIEW, false))
            .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DML, false))
            .and_then(|_| connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DQS_DDL, false))
            .and_then(|_| {
                connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)
            })
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_database_config".to_string(),
            })?;
        connection
            .execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF;")
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_database_read_only".to_string(),
            })?;
        connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if policy.allows(context) {
                    Authorization::Allow
                } else {
                    Authorization::Deny
                }
            }))
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_database_authorizer".to_string(),
            })?;
        Self::validate_binding(binding)?;
        Ok(connection)
    }

    fn validate_schema(&self, binding: &RootBinding) -> Result<(), AdapterError> {
        let connection = self.open_connection(binding, AuthorizerPolicy::schema())?;
        for (table, required) in [
            ("session", SESSION_COLUMNS.as_slice()),
            ("message", MESSAGE_COLUMNS.as_slice()),
            ("part", PART_COLUMNS.as_slice()),
        ] {
            let columns = table_columns(&connection, table)?;
            if !required
                .iter()
                .filter(|column| **column != "rowid")
                .all(|column| columns.contains(*column))
            {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "opencode_required_columns".to_string(),
                });
            }
        }
        let mut statement = connection
            .prepare("SELECT id FROM migration ORDER BY id")
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_migration_query".to_string(),
            })?;
        let migrations = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_migration_query".to_string(),
            })?
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_migration_value".to_string(),
            })?;
        let expected = MIGRATIONS.into_iter().collect::<BTreeSet<_>>();
        if !migrations.contains(TERMINAL_MIGRATION)
            || migrations.len() != expected.len()
            || migrations
                .iter()
                .any(|migration| !expected.contains(migration.as_str()))
        {
            return Err(AdapterError::UnsupportedFormat {
                stage: "opencode_migration_set".to_string(),
            });
        }
        Self::validate_binding(binding)
    }

    fn root_for(&self, source: &SourceManifest) -> Result<RootBinding, AdapterError> {
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "opencode_roots".to_string(),
            })?
            .get(&source.source_id)
            .cloned()
            .ok_or_else(|| AdapterError::NotFound {
                stage: "opencode_manifest".to_string(),
            })
    }

    fn open_scan_connection(
        &self,
        binding: &RootBinding,
        policy: AuthorizerPolicy,
        request: &ScanRequest,
    ) -> Result<Connection, AdapterError> {
        let connection = self.open_connection(binding, policy)?;
        let cancellation = request.cancellation.clone();
        let deadline = request.budget.deadline;
        connection
            .progress_handler(
                1_000,
                Some(move || {
                    cancellation.is_cancelled()
                        || deadline.is_some_and(|limit| std::time::Instant::now() >= limit)
                }),
            )
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "opencode_progress_handler".to_string(),
            })?;
        connection
            .execute_batch("BEGIN")
            .map_err(|error| db_error(error, request, "opencode_snapshot_begin"))?;
        // Binding revalidation cadence (contract section 9): the binding is
        // verified when the stream opens (here and inside `open_connection`)
        // and once more when the stream reaches natural exhaustion, so a
        // root/database replaced mid-scan fails closed at the tail instead
        // of silently completing. In between, the connection holds the
        // database file handle inside one read transaction and per-record
        // revalidation is intentionally not repeated — the same cadence the
        // Claude adapter applies per transcript file. Early stops (limit,
        // budget, cancellation) skip the end check. A failed check at open
        // still fails closed and refuses the scan.
        Self::validate_binding(binding)?;
        Ok(connection)
    }

    fn session_policy(request: &ScanRequest) -> AuthorizerPolicy {
        let mut columns = BTreeSet::from([
            "id",
            "parent_id",
            "agent",
            "time_created",
            "time_updated",
            "time_archived",
            "tokens_input",
            "tokens_output",
            "tokens_reasoning",
            "tokens_cache_read",
            "tokens_cache_write",
        ]);
        if projected(&request.projection, "model") || projected(&request.projection, "provider") {
            columns.insert("model");
        }
        if projected(&request.projection, "title") {
            columns.insert("title");
        }
        if projected(&request.projection, "cwd") {
            columns.insert("directory");
        }
        if projected(&request.projection, "project") {
            columns.insert("path");
        }
        AuthorizerPolicy::table("session", columns)
    }

    fn open_session_stream(&self, request: ScanRequest) -> Result<ScanResult, AdapterError> {
        let binding = self.root_for(&request.source)?;
        let policy = Self::session_policy(&request);
        let connection = self.open_scan_connection(&binding, policy, &request)?;
        let predicate_states = request
            .predicates
            .iter()
            .map(|predicate| session_predicate_state(&SESSION_PREDICATE_CAPABILITIES, predicate))
            .collect::<Vec<_>>();
        let all_exact = predicate_states
            .iter()
            .all(|state| *state == PushdownState::Exact);
        // Predicates are applied all-or-nothing. When they are not all exact
        // the stream applies none of them, so every predicate must be
        // reported as `Unsupported` to keep the report honest about what
        // actually executed (contract section 5).
        let reported_states = if all_exact {
            predicate_states
        } else {
            vec![PushdownState::Unsupported; request.predicates.len()]
        };
        let (effective_limit, limit_state) = limit_pushdown(&request, all_exact);
        let diagnostics = ScanDiagnostics::default();
        Ok(ScanResult {
            records: Box::new(SessionStream {
                connection,
                binding,
                request: request.clone(),
                after: None,
                buffer: VecDeque::new(),
                predicates: if all_exact {
                    request.predicates.clone()
                } else {
                    Vec::new()
                },
                effective_limit,
                emitted: 0,
                finished: false,
            }),
            pushdown: PushdownReport {
                predicates: reported_states,
                limit: limit_state,
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics,
            snapshot: SnapshotReport {
                token: request.source.snapshot.clone(),
                strength: SnapshotStrength::Weak,
                stale: false,
            },
        })
    }

    fn open_edge_stream(&self, request: ScanRequest) -> Result<ScanResult, AdapterError> {
        let binding = self.root_for(&request.source)?;
        let policy = AuthorizerPolicy::table("session", ["id", "parent_id", "time_created"]);
        let connection = self.open_scan_connection(&binding, policy, &request)?;
        let diagnostics = ScanDiagnostics::default();
        // The stream only keeps the limit when nothing needs re-checking by
        // the engine; otherwise `effective_limit` stays `None` and the stream
        // must not truncate, matching the message/part and usage streams.
        let (effective_limit, limit_state) =
            limit_pushdown(&request, request.predicates.is_empty());
        Ok(ScanResult {
            records: Box::new(EdgeStream {
                connection,
                binding,
                request: request.clone(),
                diagnostics: diagnostics.clone(),
                after_child: None,
                buffer: VecDeque::new(),
                emitted: 0,
                effective_limit,
                finished: false,
            }),
            pushdown: PushdownReport {
                predicates: request
                    .predicates
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
                limit: limit_state,
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics,
            snapshot: SnapshotReport {
                token: request.source.snapshot.clone(),
                strength: SnapshotStrength::Weak,
                stale: false,
            },
        })
    }

    fn open_message_part_stream(
        &self,
        request: ScanRequest,
        mode: MessageMode,
    ) -> Result<ScanResult, AdapterError> {
        let binding = self.root_for(&request.source)?;
        let connection =
            self.open_scan_connection(&binding, AuthorizerPolicy::message_parts(), &request)?;
        let diagnostics = ScanDiagnostics::default();
        let (effective_limit, limit_state) =
            limit_pushdown(&request, request.predicates.is_empty());
        Ok(ScanResult {
            records: Box::new(MessagePartStream {
                connection,
                binding,
                request: request.clone(),
                diagnostics: diagnostics.clone(),
                mode,
                after_message: None,
                messages: VecDeque::new(),
                current: None,
                parts: VecDeque::new(),
                last_session: None,
                sequence: 0,
                emitted: 0,
                effective_limit,
                finished: false,
            }),
            pushdown: PushdownReport {
                predicates: request
                    .predicates
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
                limit: limit_state,
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics,
            snapshot: SnapshotReport {
                token: request.source.snapshot.clone(),
                strength: SnapshotStrength::Weak,
                stale: false,
            },
        })
    }

    fn open_usage_stream(&self, request: ScanRequest) -> Result<ScanResult, AdapterError> {
        let binding = self.root_for(&request.source)?;
        let connection =
            self.open_scan_connection(&binding, AuthorizerPolicy::messages_only(), &request)?;
        let (effective_limit, limit_state) =
            limit_pushdown(&request, request.predicates.is_empty());
        let diagnostics = ScanDiagnostics::default();
        Ok(ScanResult {
            records: Box::new(UsageStream {
                connection,
                binding,
                request: request.clone(),
                after_message: None,
                messages: VecDeque::new(),
                emitted: 0,
                effective_limit,
                finished: false,
            }),
            pushdown: PushdownReport {
                predicates: request
                    .predicates
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
                limit: limit_state,
                ordering: request
                    .order_hint
                    .iter()
                    .map(|_| PushdownState::Unsupported)
                    .collect(),
            },
            diagnostics,
            snapshot: SnapshotReport {
                token: request.source.snapshot.clone(),
                strength: SnapshotStrength::Weak,
                stale: false,
            },
        })
    }
}

impl AgentAdapter for OpenCodeAdapter {
    fn id(&self) -> &'static str {
        "opencode"
    }

    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError> {
        let binding = Self::validate_root(Path::new(&request.data_root))?;
        self.validate_schema(&binding)?;
        let root_text = binding.path.to_string_lossy();
        let source_id = SourceId::for_data_root(self.id(), &root_text, &self.installation_salt);
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "opencode_roots".to_string(),
            })?
            .insert(source_id.clone(), binding);
        Ok(ProbeResult {
            manifests: vec![SourceManifest {
                source_id,
                agent_id: self.id().to_string(),
                display_name: "OpenCode 1.17.18".to_string(),
                data_root_token: "selected-opencode-root".to_string(),
                format_fingerprint: FORMAT.to_string(),
                capabilities: vec![
                    "sessions".to_string(),
                    "messages".to_string(),
                    "tool_calls".to_string(),
                    "usage".to_string(),
                    "session_edges".to_string(),
                ],
                snapshot: None,
                warnings: Vec::new(),
            }],
        })
    }

    fn capabilities(&self, _manifest: &SourceManifest) -> Capabilities {
        Capabilities {
            tables: vec![
                TableName::Sessions,
                TableName::Messages,
                TableName::ToolCalls,
                TableName::Usage,
                TableName::SessionEdges,
            ],
            columns: columns(),
            snapshot_strength: SnapshotStrength::Weak,
        }
    }

    fn schema(&self, _manifest: &SourceManifest) -> AdapterSchema {
        AdapterSchema { columns: columns() }
    }

    fn scan(&self, request: ScanRequest) -> Result<ScanResult, AdapterError> {
        validate_projection_access(
            &request.projection,
            &self.schema(&request.source),
            request.access,
        )?;
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
        match request.table {
            TableName::Sessions => self.open_session_stream(request),
            TableName::SessionEdges => self.open_edge_stream(request),
            TableName::Messages => self.open_message_part_stream(request, MessageMode::Messages),
            TableName::ToolCalls => self.open_message_part_stream(request, MessageMode::Tools),
            TableName::Usage => self.open_usage_stream(request),
            _ => Err(AdapterError::UnsupportedFormat {
                stage: "opencode_table".to_string(),
            }),
        }
    }
}

// End-of-stream watermark re-check (contract section 9): the binding was
// verified when the stream opened; natural exhaustion verifies it once more
// before reporting EOF so a root or database replaced mid-scan fails closed
// instead of silently completing. Early stops (exact limit, budget,
// cancellation) do not take this path and skip the check.
fn end_of_stream(binding: &RootBinding) -> Option<Result<CanonicalRecord, AdapterError>> {
    match OpenCodeAdapter::validate_binding(binding) {
        Ok(()) => None,
        Err(error) => Some(Err(error)),
    }
}

impl Iterator for MessagePartStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .effective_limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            return None;
        }
        loop {
            if let Err(error) = check_scan_state(
                &self.request.cancellation,
                &self.request.budget,
                self.emitted,
                self.request.budget.bytes_read_used(),
            ) {
                self.finished = true;
                return Some(Err(error));
            }
            if self.current.is_none() {
                if self.messages.is_empty()
                    && let Err(error) = fill_message_buffer(
                        &self.connection,
                        &self.request,
                        self.after_message.as_ref(),
                        &mut self.messages,
                    )
                {
                    self.finished = true;
                    return Some(Err(error));
                }
                let Some(row) = self.messages.pop_front() else {
                    self.finished = true;
                    return end_of_stream(&self.binding);
                };
                match load_message(row, &self.request) {
                    Ok((message, cursor, bytes)) => {
                        if let Err(error) = self.request.budget.charge_bytes_read(bytes) {
                            self.finished = true;
                            return Some(Err(error));
                        }
                        if self.last_session.as_deref() != Some(&message.session_native_id) {
                            self.last_session = Some(message.session_native_id.clone());
                            self.sequence = 0;
                        }
                        self.after_message = Some(cursor);
                        self.current = Some(message);
                    }
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                }
            }
            let mut current = self.current.take()?;
            if self.parts.is_empty()
                && let Err(error) = fill_part_buffer(
                    &self.connection,
                    &self.request,
                    &current.session_native_id,
                    &current.native_id,
                    current.after_part.as_deref(),
                    &mut self.parts,
                )
            {
                self.finished = true;
                return Some(Err(error));
            }
            let Some(part_row) = self.parts.pop_front() else {
                if self.mode == MessageMode::Tools {
                    continue;
                }
                self.sequence += 1;
                let record = message_record(current, self.sequence, &self.request);
                if let Err(error) = self.request.budget.charge_records(1) {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.emitted += 1;
                return Some(Ok(CanonicalRecord::Message(record)));
            };
            match load_part(part_row, &self.request, self.mode) {
                Ok((part_id, part, bytes)) => {
                    current.after_part = Some(part_id);
                    if let Err(error) = self.request.budget.charge_bytes_read(bytes) {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    match part {
                        ParsedPart::Text(text) => {
                            if let Some(text) = text
                                && let Err(error) = append_content(
                                    &mut current.content,
                                    &text,
                                    self.request.budget.max_single_value_bytes,
                                )
                            {
                                self.finished = true;
                                return Some(Err(error));
                            }
                            self.current = Some(current);
                        }
                        ParsedPart::Tool(tool) => {
                            self.sequence += 1;
                            let record = tool_record(tool, &current, self.sequence, &self.request);
                            self.current = Some(current);
                            if let Err(error) = self.request.budget.charge_records(1) {
                                self.finished = true;
                                return Some(Err(error));
                            }
                            self.emitted += 1;
                            return Some(Ok(CanonicalRecord::ToolCall(record)));
                        }
                        ParsedPart::Ignored => {
                            self.current = Some(current);
                        }
                        ParsedPart::Unknown => {
                            if let Err(error) = self.diagnostics.push(AdapterWarning {
                                kind: AdapterWarningKind::UnknownEvent,
                                source_kind: "opencode_part".to_string(),
                                stage: "unknown_part".to_string(),
                            }) {
                                self.finished = true;
                                return Some(Err(error));
                            }
                            self.current = Some(current);
                        }
                    }
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

impl Iterator for UsageStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .effective_limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            return None;
        }
        loop {
            if let Err(error) = check_scan_state(
                &self.request.cancellation,
                &self.request.budget,
                self.emitted,
                self.request.budget.bytes_read_used(),
            ) {
                self.finished = true;
                return Some(Err(error));
            }
            if self.messages.is_empty()
                && let Err(error) = fill_message_buffer(
                    &self.connection,
                    &self.request,
                    self.after_message.as_ref(),
                    &mut self.messages,
                )
            {
                self.finished = true;
                return Some(Err(error));
            }
            let Some(row) = self.messages.pop_front() else {
                self.finished = true;
                return end_of_stream(&self.binding);
            };
            let (message, cursor, bytes) = match load_message(row, &self.request) {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            self.after_message = Some(cursor);
            if let Err(error) = self.request.budget.charge_bytes_read(bytes) {
                self.finished = true;
                return Some(Err(error));
            }
            let Some(total_tokens) = message.metadata.total_tokens else {
                continue;
            };
            let native = NativeId::new(format!("{}/usage", message.native_id));
            let session_native = NativeId::new(message.session_native_id);
            let record = UsageRecord {
                usage_id: EntityId::from_parts("opencode", &self.request.source.source_id, &native),
                source_id: self.request.source.source_id.clone(),
                agent_id: "opencode".to_string(),
                session_id: Some(EntityId::from_parts(
                    "opencode",
                    &self.request.source.source_id,
                    &session_native,
                )),
                model: message.metadata.model,
                provider: message.metadata.provider,
                bucket_start: Some(message.created_at),
                input_tokens: message.metadata.input_tokens,
                output_tokens: message.metadata.output_tokens,
                cached_tokens: message.metadata.cached_tokens,
                total_tokens: Some(total_tokens),
                message_count: 0,
                tool_call_count: 0,
                error_count: 0,
                provenance: BTreeMap::new(),
                extensions: BTreeMap::new(),
            };
            if let Err(error) = self.request.budget.charge_records(1) {
                self.finished = true;
                return Some(Err(error));
            }
            self.emitted += 1;
            return Some(Ok(CanonicalRecord::Usage(record)));
        }
    }
}

impl Iterator for SessionStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .effective_limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            return None;
        }
        loop {
            if let Err(error) = check_scan_state(
                &self.request.cancellation,
                &self.request.budget,
                self.emitted,
                self.request.budget.bytes_read_used(),
            ) {
                self.finished = true;
                return Some(Err(error));
            }
            if self.buffer.is_empty()
                && let Err(error) = fill_session_buffer(
                    &self.connection,
                    &self.request,
                    self.after.as_ref(),
                    &mut self.buffer,
                )
            {
                self.finished = true;
                return Some(Err(error));
            }
            let Some(row) = self.buffer.pop_front() else {
                self.finished = true;
                return end_of_stream(&self.binding);
            };
            let (record, cursor, bytes) = match load_session(row, &self.request) {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            self.after = Some(cursor);
            if let Err(error) = self.request.budget.charge_bytes_read(bytes) {
                self.finished = true;
                return Some(Err(error));
            }
            if !self.predicates.iter().all(|predicate| {
                session_matches(&SESSION_PREDICATE_CAPABILITIES, &record, predicate)
            }) {
                continue;
            }
            if let Err(error) = self.request.budget.charge_records(1) {
                self.finished = true;
                return Some(Err(error));
            }
            self.emitted += 1;
            return Some(Ok(CanonicalRecord::Session(record)));
        }
    }
}

impl Iterator for EdgeStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .effective_limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            return None;
        }
        if let Err(error) = check_scan_state(
            &self.request.cancellation,
            &self.request.budget,
            self.emitted,
            self.request.budget.bytes_read_used(),
        ) {
            self.finished = true;
            return Some(Err(error));
        }
        if self.buffer.is_empty()
            && let Err(error) = fill_edge_buffer(
                &self.connection,
                &self.request,
                self.after_child.as_deref(),
                &mut self.buffer,
            )
        {
            self.finished = true;
            return Some(Err(error));
        }
        let Some(row) = self.buffer.pop_front() else {
            self.finished = true;
            return end_of_stream(&self.binding);
        };
        match load_edge(row, &self.request) {
            Ok((record, child, bytes, dangling)) => {
                self.after_child = Some(child);
                if let Err(error) = self.request.budget.charge_bytes_read(bytes) {
                    self.finished = true;
                    return Some(Err(error));
                }
                if let Err(error) = self.request.budget.charge_records(1) {
                    self.finished = true;
                    return Some(Err(error));
                }
                if dangling
                    && let Err(error) = self.diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::IncompleteCapability,
                        source_kind: "opencode_database".to_string(),
                        stage: "opencode_edge_parent".to_string(),
                    })
                {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.emitted += 1;
                Some(Ok(CanonicalRecord::SessionEdge(record)))
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

// Batch pull of up to `SCAN_BATCH_ROWS` message rows (also bounded by
// `SCAN_BATCH_BYTES`) starting after the keyset cursor. One prepare per batch
// replaces the previous prepare-per-record, and the keyset `WHERE`/`ORDER BY`
// shape is unchanged so the emitted row order matches the old
// `LIMIT 1`-per-record walk exactly.
fn fill_message_buffer(
    connection: &Connection,
    request: &ScanRequest,
    after: Option<&MessageCursor>,
    buffer: &mut VecDeque<MessageRow>,
) -> Result<(), AdapterError> {
    let mut statement = connection
        .prepare(
            "SELECT id, session_id, time_created, length(CAST(data AS BLOB)) AS data_len, data \
             FROM message \
             WHERE (?1 IS NULL OR session_id > ?1 \
                    OR (session_id = ?1 AND time_created > ?2) \
                    OR (session_id = ?1 AND time_created = ?2 AND id > ?3)) \
             ORDER BY session_id, time_created, id LIMIT 256",
        )
        .map_err(|error| db_error(error, request, "opencode_message_prepare"))?;
    let session = after.map(|cursor| cursor.0.as_str());
    let created = after.map(|cursor| cursor.1);
    let id = after.map(|cursor| cursor.2.as_str());
    let mut rows = statement
        .query(rusqlite::params![session, created, id])
        .map_err(|error| db_error(error, request, "opencode_message_query"))?;
    let mut batch_bytes = 0_u64;
    while buffer.len() < SCAN_BATCH_ROWS && batch_bytes < SCAN_BATCH_BYTES {
        let Some(row) = rows
            .next()
            .map_err(|error| db_error(error, request, "opencode_message_row"))?
        else {
            break;
        };
        let native_id: String = row
            .get("id")
            .map_err(|error| db_error(error, request, "opencode_message_id"))?;
        let session_native_id: String = row
            .get("session_id")
            .map_err(|error| db_error(error, request, "opencode_message_session"))?;
        let created_ms: i64 = row
            .get("time_created")
            .map_err(|error| db_error(error, request, "opencode_message_created"))?;
        let data_len: i64 = row
            .get("data_len")
            .map_err(|error| db_error(error, request, "opencode_message_length"))?;
        let data = text_value(row, "data", request, "opencode_message_data")?.to_vec();
        batch_bytes = batch_bytes.saturating_add(
            data.len() as u64 + native_id.len() as u64 + session_native_id.len() as u64,
        );
        buffer.push_back(MessageRow {
            native_id,
            session_native_id,
            created_ms,
            data_len,
            data,
        });
    }
    Ok(())
}

// Converts one buffered message row into the current message, applying the
// same per-record size, parse, and byte-accounting steps (in the same order)
// as the previous row-at-a-time read.
fn load_message(row: MessageRow, request: &ScanRequest) -> Result<LoadedMessage, AdapterError> {
    let MessageRow {
        native_id,
        session_native_id,
        created_ms,
        data_len,
        data,
    } = row;
    let data_len = bounded_record_length(data_len, request, "opencode_message_size")?;
    let metadata = parse_message_metadata(&data)?;
    let created_at = timestamp(created_ms, "opencode_message_created")?;
    let bytes = data_len
        .checked_add(native_id.len() as u64)
        .and_then(|value| value.checked_add(session_native_id.len() as u64))
        .and_then(|value| value.checked_add(8))
        .ok_or_else(|| AdapterError::BudgetExceeded {
            resource: "bytes_read".to_string(),
            actual: u64::MAX,
        })?;
    Ok((
        CurrentMessage {
            native_id: native_id.clone(),
            session_native_id: session_native_id.clone(),
            created_at,
            metadata,
            after_part: None,
            content: None,
        },
        (session_native_id, created_ms, native_id),
        bytes,
    ))
}

// Batch pull of one message's parts after the keyset cursor, same ordering
// and batching rationale as `fill_message_buffer`.
fn fill_part_buffer(
    connection: &Connection,
    request: &ScanRequest,
    session_id: &str,
    message_id: &str,
    after_part: Option<&str>,
    buffer: &mut VecDeque<PartRow>,
) -> Result<(), AdapterError> {
    let mut statement = connection
        .prepare(
            "SELECT id, length(CAST(data AS BLOB)) AS data_len, data \
             FROM part \
             WHERE session_id = ?1 AND message_id = ?2 AND (?3 IS NULL OR id > ?3) \
             ORDER BY id LIMIT 256",
        )
        .map_err(|error| db_error(error, request, "opencode_part_prepare"))?;
    let mut rows = statement
        .query(rusqlite::params![session_id, message_id, after_part])
        .map_err(|error| db_error(error, request, "opencode_part_query"))?;
    let mut batch_bytes = 0_u64;
    while buffer.len() < SCAN_BATCH_ROWS && batch_bytes < SCAN_BATCH_BYTES {
        let Some(row) = rows
            .next()
            .map_err(|error| db_error(error, request, "opencode_part_row"))?
        else {
            break;
        };
        let part_id: String = row
            .get("id")
            .map_err(|error| db_error(error, request, "opencode_part_id"))?;
        let data_len: i64 = row
            .get("data_len")
            .map_err(|error| db_error(error, request, "opencode_part_length"))?;
        let data = text_value(row, "data", request, "opencode_part_data")?.to_vec();
        batch_bytes = batch_bytes.saturating_add(data.len() as u64 + part_id.len() as u64);
        buffer.push_back(PartRow {
            part_id,
            data_len,
            data,
        });
    }
    Ok(())
}

// Parses one buffered part row, mirroring the previous row-at-a-time parse
// order: size bound, kind probe, then mode-specific payload parsing.
fn load_part(
    row: PartRow,
    request: &ScanRequest,
    mode: MessageMode,
) -> Result<(String, ParsedPart, u64), AdapterError> {
    let PartRow {
        part_id,
        data_len,
        data,
    } = row;
    let data_len = bounded_record_length(data_len, request, "opencode_part_size")?;
    let kind =
        serde_json::from_slice::<PartKind>(&data).map_err(|_| AdapterError::CorruptSource {
            stage: "opencode_part_kind".to_string(),
        })?;
    let parsed = match (mode, kind.part_type.as_str()) {
        (MessageMode::Messages, "text") => {
            // `content` is filled only when the projection asks for it. A
            // `content_json`-only projection skips text parsing entirely:
            // the engine advertises the column and the adapter emits NULL,
            // matching the codex adapter (contract section 4).
            if projected(&request.projection, "content") {
                ParsedPart::Text(Some(parse_text_part(&data, request)?))
            } else {
                ParsedPart::Text(None)
            }
        }
        (MessageMode::Tools, "tool") => ParsedPart::Tool(parse_tool_part(&data, request)?),
        (
            _,
            "reasoning" | "file" | "patch" | "snapshot" | "subtask" | "compaction" | "retry"
            | "step-start" | "step-finish" | "agent" | "text" | "tool",
        ) => ParsedPart::Ignored,
        _ => ParsedPart::Unknown,
    };
    let bytes =
        data_len
            .checked_add(part_id.len() as u64)
            .ok_or_else(|| AdapterError::BudgetExceeded {
                resource: "bytes_read".to_string(),
                actual: u64::MAX,
            })?;
    Ok((part_id, parsed, bytes))
}

fn parse_message_metadata(raw: &[u8]) -> Result<MessageMetadata, AdapterError> {
    let role =
        serde_json::from_slice::<MessageRole>(raw).map_err(|_| AdapterError::CorruptSource {
            stage: "opencode_message_role".to_string(),
        })?;
    match role.role.as_str() {
        "user" => {
            let value = serde_json::from_slice::<UserMessageData>(raw).map_err(|_| {
                AdapterError::CorruptSource {
                    stage: "opencode_user_message".to_string(),
                }
            })?;
            Ok(MessageMetadata {
                role: role.role,
                model: Some(value.model.model_id),
                provider: Some(value.model.provider_id),
                input_tokens: None,
                output_tokens: None,
                cached_tokens: None,
                total_tokens: None,
                is_error: Some(false),
            })
        }
        "assistant" => {
            let value = serde_json::from_slice::<AssistantMessageData>(raw).map_err(|_| {
                AdapterError::CorruptSource {
                    stage: "opencode_assistant_message".to_string(),
                }
            })?;
            for token in [
                value.tokens.input,
                value.tokens.output,
                value.tokens.reasoning,
                value.tokens.cache.read,
                value.tokens.cache.write,
            ] {
                if token < 0 {
                    return Err(AdapterError::CorruptSource {
                        stage: "opencode_message_tokens".to_string(),
                    });
                }
            }
            let cached = value
                .tokens
                .cache
                .read
                .checked_add(value.tokens.cache.write)
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "opencode_message_tokens".to_string(),
                })?;
            let _total = value
                .tokens
                .input
                .checked_add(value.tokens.output)
                .and_then(|total| total.checked_add(value.tokens.reasoning))
                .and_then(|total| total.checked_add(cached))
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "opencode_message_tokens".to_string(),
                })?;
            let total = value.tokens.total.unwrap_or(_total);
            if total < 0 {
                return Err(AdapterError::CorruptSource {
                    stage: "opencode_message_tokens".to_string(),
                });
            }
            Ok(MessageMetadata {
                role: role.role,
                model: Some(value.model_id),
                provider: Some(value.provider_id),
                input_tokens: Some(value.tokens.input),
                output_tokens: Some(value.tokens.output),
                cached_tokens: Some(cached),
                total_tokens: Some(total),
                is_error: Some(value.error.is_some()),
            })
        }
        _ => Err(AdapterError::UnsupportedFormat {
            stage: "opencode_message_role".to_string(),
        }),
    }
}

fn parse_text_part(raw: &[u8], request: &ScanRequest) -> Result<String, AdapterError> {
    let value =
        serde_json::from_slice::<TextPart<'_>>(raw).map_err(|_| AdapterError::CorruptSource {
            stage: "opencode_text_part".to_string(),
        })?;
    check_raw_value(value.text, request)?;
    serde_json::from_str::<String>(value.text.get()).map_err(|_| AdapterError::CorruptSource {
        stage: "opencode_text_value".to_string(),
    })
}

fn parse_tool_part(raw: &[u8], request: &ScanRequest) -> Result<ParsedTool, AdapterError> {
    let value =
        serde_json::from_slice::<ToolPart<'_>>(raw).map_err(|_| AdapterError::CorruptSource {
            stage: "opencode_tool_part".to_string(),
        })?;
    let kind = serde_json::from_str::<ToolStateKind>(value.state.get()).map_err(|_| {
        AdapterError::CorruptSource {
            stage: "opencode_tool_state".to_string(),
        }
    })?;
    let wants_input = projected(&request.projection, "arguments");
    let wants_output = projected(&request.projection, "output");
    let (arguments, output, started_at, ended_at) =
        match kind.status.as_str() {
            "pending" => {
                let state = serde_json::from_str::<ToolStatePending<'_>>(value.state.get())
                    .map_err(|_| AdapterError::CorruptSource {
                        stage: "opencode_tool_pending".to_string(),
                    })?;
                (
                    parse_arguments(state.input, wants_input, request)?,
                    None,
                    None,
                    None,
                )
            }
            "running" => {
                let state = serde_json::from_str::<ToolStateRunning<'_>>(value.state.get())
                    .map_err(|_| AdapterError::CorruptSource {
                        stage: "opencode_tool_running".to_string(),
                    })?;
                (
                    parse_arguments(state.input, wants_input, request)?,
                    None,
                    Some(timestamp(state.time.start, "opencode_tool_start")?),
                    None,
                )
            }
            "completed" => {
                let state = serde_json::from_str::<ToolStateCompleted<'_>>(value.state.get())
                    .map_err(|_| AdapterError::CorruptSource {
                        stage: "opencode_tool_completed".to_string(),
                    })?;
                (
                    parse_arguments(state.input, wants_input, request)?,
                    parse_output(state.output, wants_output, request)?,
                    Some(timestamp(state.time.start, "opencode_tool_start")?),
                    Some(timestamp(state.time.end, "opencode_tool_end")?),
                )
            }
            "error" => {
                let state = serde_json::from_str::<ToolStateError<'_>>(value.state.get()).map_err(
                    |_| AdapterError::CorruptSource {
                        stage: "opencode_tool_error".to_string(),
                    },
                )?;
                (
                    parse_arguments(state.input, wants_input, request)?,
                    parse_output(state.error, wants_output, request)?,
                    Some(timestamp(state.time.start, "opencode_tool_start")?),
                    Some(timestamp(state.time.end, "opencode_tool_end")?),
                )
            }
            _ => {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "opencode_tool_state".to_string(),
                });
            }
        };
    let duration_ms = match (started_at, ended_at) {
        (Some(start), Some(end)) => Some(
            end.timestamp_millis()
                .checked_sub(start.timestamp_millis())
                .filter(|value| *value >= 0)
                .ok_or_else(|| AdapterError::CorruptSource {
                    stage: "opencode_tool_duration".to_string(),
                })?,
        ),
        _ => None,
    };
    Ok(ParsedTool {
        call_id: value.call_id,
        tool_name: value.tool,
        arguments,
        output,
        status: kind.status,
        started_at,
        ended_at,
        duration_ms,
    })
}

fn parse_arguments(
    value: &RawValue,
    selected: bool,
    request: &ScanRequest,
) -> Result<Option<serde_json::Value>, AdapterError> {
    if !selected {
        return Ok(None);
    }
    check_raw_value(value, request)?;
    serde_json::from_str(value.get())
        .map(Some)
        .map_err(|_| AdapterError::CorruptSource {
            stage: "opencode_tool_input".to_string(),
        })
}

fn parse_output(
    value: &RawValue,
    selected: bool,
    request: &ScanRequest,
) -> Result<Option<String>, AdapterError> {
    if !selected {
        return Ok(None);
    }
    check_raw_value(value, request)?;
    serde_json::from_str(value.get())
        .map(Some)
        .map_err(|_| AdapterError::CorruptSource {
            stage: "opencode_tool_output".to_string(),
        })
}

fn check_raw_value(value: &RawValue, request: &ScanRequest) -> Result<(), AdapterError> {
    let length = value.get().len() as u64;
    if length > request.budget.max_single_value_bytes {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: length,
        });
    }
    Ok(())
}

fn message_record(current: CurrentMessage, sequence: i64, request: &ScanRequest) -> MessageRecord {
    let native = NativeId::new(current.native_id);
    let session_native = NativeId::new(current.session_native_id);
    MessageRecord {
        message_id: EntityId::from_parts("opencode", &request.source.source_id, &native),
        session_id: EntityId::from_parts("opencode", &request.source.source_id, &session_native),
        source_id: request.source.source_id.clone(),
        sequence,
        role: current.metadata.role,
        kind: None,
        content: current.content,
        content_json: None,
        model: current.metadata.model,
        created_at: Some(current.created_at),
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        is_error: current.metadata.is_error,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

fn tool_record(
    tool: ParsedTool,
    current: &CurrentMessage,
    sequence: i64,
    request: &ScanRequest,
) -> ToolCallRecord {
    let native = NativeId::new(tool.call_id);
    let session_native = NativeId::new(current.session_native_id.clone());
    let message_native = NativeId::new(current.native_id.clone());
    ToolCallRecord {
        tool_call_id: EntityId::from_parts("opencode", &request.source.source_id, &native),
        session_id: EntityId::from_parts("opencode", &request.source.source_id, &session_native),
        message_id: Some(EntityId::from_parts(
            "opencode",
            &request.source.source_id,
            &message_native,
        )),
        source_id: request.source.source_id.clone(),
        sequence,
        tool_name: tool.tool_name,
        namespace: None,
        arguments: tool.arguments,
        output: tool.output,
        status: Some(tool.status),
        started_at: tool.started_at,
        ended_at: tool.ended_at,
        duration_ms: tool.duration_ms,
        exit_code: None,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    }
}

fn bounded_record_length(
    length: i64,
    request: &ScanRequest,
    stage: &str,
) -> Result<u64, AdapterError> {
    let length = u64::try_from(length).map_err(|_| AdapterError::CorruptSource {
        stage: stage.to_string(),
    })?;
    if length > MAX_JSON_RECORD_BYTES {
        return Err(AdapterError::UnsupportedFormat {
            stage: stage.to_string(),
        });
    }
    if length > request.budget.max_bytes_read {
        return Err(AdapterError::BudgetExceeded {
            resource: "bytes_read".to_string(),
            actual: length,
        });
    }
    Ok(length)
}

fn text_value<'a>(
    row: &'a rusqlite::Row<'a>,
    column: &str,
    request: &ScanRequest,
    stage: &str,
) -> Result<&'a [u8], AdapterError> {
    match row
        .get_ref(column)
        .map_err(|error| db_error(error, request, stage))?
    {
        rusqlite::types::ValueRef::Text(value) => Ok(value),
        _ => Err(AdapterError::CorruptSource {
            stage: stage.to_string(),
        }),
    }
}

// Batch pull of session rows after the keyset cursor. The selected-column
// list, keyset `WHERE` shape, and ordering are unchanged from the previous
// per-record `LIMIT 1` walk; see `fill_message_buffer`.
fn fill_session_buffer(
    connection: &Connection,
    request: &ScanRequest,
    after: Option<&SessionCursor>,
    buffer: &mut VecDeque<SessionRow>,
) -> Result<(), AdapterError> {
    let wants_model =
        projected(&request.projection, "model") || projected(&request.projection, "provider");
    let wants_title = projected(&request.projection, "title");
    let wants_cwd = projected(&request.projection, "cwd");
    let wants_project = projected(&request.projection, "project");
    let mut selected = vec![
        "id",
        "parent_id",
        "agent",
        "time_created",
        "time_updated",
        "time_archived",
        "tokens_input",
        "tokens_output",
        "tokens_reasoning",
        "tokens_cache_read",
        "tokens_cache_write",
    ];
    if wants_model {
        selected.extend(["length(CAST(model AS BLOB)) AS model_len", "model"]);
    }
    if wants_title {
        selected.extend(["length(CAST(title AS BLOB)) AS title_len", "title"]);
    }
    if wants_cwd {
        selected.extend([
            "length(CAST(directory AS BLOB)) AS directory_len",
            "directory",
        ]);
    }
    if wants_project {
        selected.extend(["length(CAST(path AS BLOB)) AS path_len", "path"]);
    }
    let sql = format!(
        "SELECT {} FROM session \
         WHERE (?1 IS NULL OR time_updated > ?1 OR (time_updated = ?1 AND id > ?2)) \
         ORDER BY time_updated, id LIMIT {SCAN_BATCH_ROWS}",
        selected.join(", ")
    );
    let after_time = after.map(|cursor| cursor.0);
    let after_id = after.map_or("", |cursor| cursor.1.as_str());
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| db_error(error, request, "opencode_session_prepare"))?;
    let mut rows = statement
        .query(rusqlite::params![after_time, after_id])
        .map_err(|error| db_error(error, request, "opencode_session_query"))?;
    let mut batch_bytes = 0_u64;
    while buffer.len() < SCAN_BATCH_ROWS && batch_bytes < SCAN_BATCH_BYTES {
        let Some(row) = rows
            .next()
            .map_err(|error| db_error(error, request, "opencode_session_row"))?
        else {
            break;
        };
        let native: String = row
            .get("id")
            .map_err(|error| db_error(error, request, "opencode_session_id"))?;
        let parent: Option<String> = row
            .get("parent_id")
            .map_err(|error| db_error(error, request, "opencode_session_parent"))?;
        let agent: Option<String> = row
            .get("agent")
            .map_err(|error| db_error(error, request, "opencode_session_agent"))?;
        let created_ms: i64 = row
            .get("time_created")
            .map_err(|error| db_error(error, request, "opencode_session_created"))?;
        let updated_ms: i64 = row
            .get("time_updated")
            .map_err(|error| db_error(error, request, "opencode_session_updated"))?;
        let archived_ms: Option<i64> = row
            .get("time_archived")
            .map_err(|error| db_error(error, request, "opencode_session_archived"))?;
        let read_token = |column: &str| {
            row.get::<_, i64>(column)
                .map_err(|error| db_error(error, request, "opencode_session_tokens"))
        };
        let tokens = [
            read_token("tokens_input")?,
            read_token("tokens_output")?,
            read_token("tokens_reasoning")?,
            read_token("tokens_cache_read")?,
            read_token("tokens_cache_write")?,
        ];
        let mut model_len = None;
        let mut model = None;
        if wants_model {
            model_len = row
                .get("model_len")
                .map_err(|error| db_error(error, request, "opencode_session_model_length"))?;
            model = row
                .get("model")
                .map_err(|error| db_error(error, request, "opencode_session_model"))?;
        }
        let mut title_len = None;
        let mut title = None;
        if wants_title {
            title_len = row
                .get("title_len")
                .map_err(|error| db_error(error, request, "opencode_session_title"))?;
            title = row
                .get("title")
                .map_err(|error| db_error(error, request, "opencode_session_title"))?;
        }
        let mut directory_len = None;
        let mut directory = None;
        if wants_cwd {
            directory_len = row
                .get("directory_len")
                .map_err(|error| db_error(error, request, "opencode_session_directory"))?;
            directory = row
                .get("directory")
                .map_err(|error| db_error(error, request, "opencode_session_directory"))?;
        }
        let mut path_len = None;
        let mut path = None;
        if wants_project {
            path_len = row
                .get("path_len")
                .map_err(|error| db_error(error, request, "opencode_session_path"))?;
            path = row
                .get("path")
                .map_err(|error| db_error(error, request, "opencode_session_path"))?;
        }
        batch_bytes = batch_bytes.saturating_add(
            native.len() as u64
                + parent.as_ref().map_or(0, String::len) as u64
                + agent.as_ref().map_or(0, String::len) as u64
                + model.as_ref().map_or(0, String::len) as u64
                + title.as_ref().map_or(0, String::len) as u64
                + directory.as_ref().map_or(0, String::len) as u64
                + path.as_ref().map_or(0, String::len) as u64,
        );
        buffer.push_back(SessionRow {
            native,
            parent,
            agent,
            created_ms,
            updated_ms,
            archived_ms,
            tokens,
            model_len,
            model,
            title_len,
            title,
            directory_len,
            directory,
            path_len,
            path,
        });
    }
    Ok(())
}

// Converts one buffered session row into a record, applying the same
// per-record checks (token sum, sensitive length bounds, model JSON parse,
// timestamps) in the same order as the previous row-at-a-time read.
fn load_session(row: SessionRow, request: &ScanRequest) -> Result<LoadedSession, AdapterError> {
    let SessionRow {
        native,
        parent,
        agent,
        created_ms,
        updated_ms,
        archived_ms,
        tokens,
        model_len,
        model,
        title_len,
        title,
        directory_len,
        directory,
        path_len,
        path,
    } = row;
    let wants_model =
        projected(&request.projection, "model") || projected(&request.projection, "provider");
    let wants_title = projected(&request.projection, "title");
    let wants_cwd = projected(&request.projection, "cwd");
    let wants_project = projected(&request.projection, "project");
    let tokens_used = checked_token_sum(&tokens)?;
    let (model, provider, model_bytes) = if wants_model {
        check_optional_length(model_len, request, "opencode_session_model_size")?;
        let parsed = model
            .as_deref()
            .map(serde_json::from_str::<ModelValue>)
            .transpose()
            .map_err(|_| AdapterError::CorruptSource {
                stage: "opencode_session_model".to_string(),
            })?;
        (
            parsed.as_ref().map(|value| value.id.clone()),
            parsed.as_ref().map(|value| value.provider_id.clone()),
            model.as_ref().map_or(0, String::len) as u64,
        )
    } else {
        (None, None, 0)
    };
    let (title, title_bytes) = buffered_optional_text(
        title_len,
        title,
        wants_title,
        request,
        "opencode_session_title",
    )?;
    let (cwd, cwd_bytes) = buffered_optional_text(
        directory_len,
        directory,
        wants_cwd,
        request,
        "opencode_session_directory",
    )?;
    let (project, project_bytes) = buffered_optional_text(
        path_len,
        path,
        wants_project,
        request,
        "opencode_session_path",
    )?;
    let created_at = timestamp(created_ms, "opencode_session_created")?;
    let updated_at = timestamp(updated_ms, "opencode_session_updated")?;
    if let Some(value) = archived_ms {
        timestamp(value, "opencode_session_archived")?;
    }
    let native_id = NativeId::new(native.clone());
    let session_id = EntityId::from_parts("opencode", &request.source.source_id, &native_id);
    let bytes = native.len() as u64
        + parent.as_ref().map_or(0, String::len) as u64
        + agent.as_ref().map_or(0, String::len) as u64
        + model_bytes
        + title_bytes
        + cwd_bytes
        + project_bytes
        + 8 * 8;
    Ok((
        SessionRecord {
            session_id,
            native_id,
            source_id: request.source.source_id.clone(),
            agent_id: "opencode".to_string(),
            title,
            preview: None,
            cwd,
            project,
            model,
            provider,
            created_at: Some(created_at),
            updated_at: Some(updated_at),
            status: None,
            archived: Some(archived_ms.is_some()),
            message_count: None,
            tool_call_count: None,
            tokens_used: Some(tokens_used),
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: SnapshotState::Weak,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        },
        (updated_ms, native),
        bytes,
    ))
}

// Batch pull of parent-edge rows after the keyset cursor, same ordering and
// batching rationale as `fill_message_buffer`.
fn fill_edge_buffer(
    connection: &Connection,
    request: &ScanRequest,
    after_child: Option<&str>,
    buffer: &mut VecDeque<EdgeRow>,
) -> Result<(), AdapterError> {
    let mut statement = connection
        .prepare(
            "SELECT child.id, child.parent_id, child.time_created, \
                    EXISTS(SELECT 1 FROM session parent WHERE parent.id = child.parent_id) \
             FROM session child \
             WHERE child.parent_id IS NOT NULL AND (?1 IS NULL OR child.id > ?1) \
             ORDER BY child.id LIMIT 256",
        )
        .map_err(|error| db_error(error, request, "opencode_edge_prepare"))?;
    let mut rows = statement
        .query([after_child])
        .map_err(|error| db_error(error, request, "opencode_edge_query"))?;
    let mut batch_bytes = 0_u64;
    while buffer.len() < SCAN_BATCH_ROWS && batch_bytes < SCAN_BATCH_BYTES {
        let Some(row) = rows
            .next()
            .map_err(|error| db_error(error, request, "opencode_edge_row"))?
        else {
            break;
        };
        let child: String = row
            .get(0)
            .map_err(|error| db_error(error, request, "opencode_edge_child"))?;
        let parent: String = row
            .get(1)
            .map_err(|error| db_error(error, request, "opencode_edge_parent"))?;
        let created_ms: i64 = row
            .get(2)
            .map_err(|error| db_error(error, request, "opencode_edge_created"))?;
        let parent_exists: bool = row
            .get(3)
            .map_err(|error| db_error(error, request, "opencode_edge_parent_exists"))?;
        batch_bytes = batch_bytes.saturating_add(child.len() as u64 + parent.len() as u64);
        buffer.push_back(EdgeRow {
            child,
            parent,
            created_ms,
            parent_exists,
        });
    }
    Ok(())
}

// Converts one buffered edge row into a record, mirroring the previous
// row-at-a-time construction and byte accounting.
fn load_edge(
    row: EdgeRow,
    request: &ScanRequest,
) -> Result<(SessionEdgeRecord, String, u64, bool), AdapterError> {
    let EdgeRow {
        child,
        parent,
        created_ms,
        parent_exists,
    } = row;
    let child_native = NativeId::new(child.clone());
    let parent_native = NativeId::new(parent.clone());
    let native_edge = NativeId::new(format!("{parent}->{child}"));
    let record = SessionEdgeRecord {
        edge_id: EntityId::from_parts("opencode", &request.source.source_id, &native_edge),
        source_id: request.source.source_id.clone(),
        parent_session_id: EntityId::from_parts(
            "opencode",
            &request.source.source_id,
            &parent_native,
        ),
        child_session_id: EntityId::from_parts(
            "opencode",
            &request.source.source_id,
            &child_native,
        ),
        edge_kind: "parent".to_string(),
        created_at: Some(timestamp(created_ms, "opencode_edge_created")?),
        native_edge_id: Some(native_edge),
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    };
    Ok((
        record,
        child.clone(),
        (child.len() + parent.len() + 8) as u64,
        !parent_exists,
    ))
}

fn buffered_optional_text(
    length: Option<i64>,
    value: Option<String>,
    selected: bool,
    request: &ScanRequest,
    stage: &str,
) -> Result<(Option<String>, u64), AdapterError> {
    if !selected {
        return Ok((None, 0));
    }
    check_optional_length(length, request, stage)?;
    let bytes = value.as_ref().map_or(0, String::len) as u64;
    Ok((value, bytes))
}

fn check_optional_length(
    length: Option<i64>,
    request: &ScanRequest,
    stage: &str,
) -> Result<(), AdapterError> {
    let Some(length) = length else {
        return Ok(());
    };
    let length = u64::try_from(length).map_err(|_| AdapterError::CorruptSource {
        stage: stage.to_string(),
    })?;
    if length > request.budget.max_single_value_bytes {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: length,
        });
    }
    Ok(())
}

fn checked_token_sum(values: &[i64]) -> Result<i64, AdapterError> {
    values.iter().try_fold(0_i64, |total, value| {
        if *value < 0 {
            return Err(AdapterError::CorruptSource {
                stage: "opencode_session_tokens".to_string(),
            });
        }
        total
            .checked_add(*value)
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "opencode_session_tokens".to_string(),
            })
    })
}

fn timestamp(value: i64, stage: &str) -> Result<DateTime<Utc>, AdapterError> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| AdapterError::CorruptSource {
        stage: stage.to_string(),
    })
}

const SESSION_PREDICATE_CAPABILITIES: SessionPredicateCapabilities = SessionPredicateCapabilities {
    eq_text: &["session_id", "native_id", "source_id", "agent_id"],
    eq_bool: &["archived"],
    is_null: &["status"],
};

fn columns() -> Vec<ColumnCapability> {
    [
        "session_id",
        "native_id",
        "source_id",
        "agent_id",
        "title",
        "preview",
        "cwd",
        "project",
        "model",
        "provider",
        "created_at",
        "updated_at",
        "status",
        "archived",
        "message_count",
        "tool_call_count",
        "tokens_used",
        "edge_id",
        "parent_session_id",
        "child_session_id",
        "edge_kind",
        "native_edge_id",
        "message_id",
        "sequence",
        "role",
        "kind",
        "content",
        "content_json",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "is_error",
        "tool_call_id",
        "tool_name",
        "namespace",
        "arguments",
        "output",
        "started_at",
        "ended_at",
        "duration_ms",
        "exit_code",
        "usage_id",
        "bucket_start",
        "total_tokens",
        "error_count",
    ]
    .into_iter()
    .map(column_capability)
    .collect()
}

fn db_error(error: rusqlite::Error, request: &ScanRequest, stage: &str) -> AdapterError {
    if request.cancellation.is_cancelled() {
        AdapterError::Cancelled
    } else if request
        .budget
        .deadline
        .is_some_and(|deadline| std::time::Instant::now() >= deadline)
    {
        AdapterError::BudgetExceeded {
            resource: "deadline".to_string(),
            actual: request.budget.records_used(),
        }
    } else {
        let _ = error;
        AdapterError::CorruptSource {
            stage: stage.to_string(),
        }
    }
}

#[cfg(test)]
mod tests;
