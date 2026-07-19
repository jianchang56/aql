//! Read-only Codex adapter.

#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use aql_adapter_api::{
    AdapterError, AdapterSchema, AdapterWarning, AdapterWarningKind, AgentAdapter, Capabilities,
    ColumnCapability, ColumnName, FileAccessObserver, Literal, ProbeRequest, ProbeResult,
    PushdownReport, PushdownState, ResourceBudget, ScanDiagnostics, ScanRequest, ScanResult,
    SnapshotReport, SnapshotStrength, SourceKind, TableName, check_scan_state,
    validate_projection_access,
};
use aql_model::{
    AccessClass, ArtifactRecord, CanonicalRecord, EntityId, IdentityConfidence, MessageRecord,
    NativeId, Provenance, SessionEdgeRecord, SessionRecord, SnapshotState, SnapshotToken, SourceId,
    SourceManifest, ToolCallRecord, installation_scoped_hmac,
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

mod rollout;

use rollout::{ParsedArtifactChange, ParsedPayload, ReadFields, parse_next, read_limited_line};

pub use aql_adapter_api as adapter_api;
pub use aql_model as model;

const MAX_INDEX_RECORD_BYTES: usize = 1024 * 1024;

type FileIdentity = aql_fs::FileIdentity;

/// Probe-time binding to one canonicalized Codex root and its state database.
///
/// Scan-time opens revalidate every bound identity and fail closed on
/// replacement or shrink instead of reading a changed source.
#[derive(Clone)]
struct RootBinding {
    path: PathBuf,
    identity: FileIdentity,
    database: PathBuf,
    database_identity: FileIdentity,
    database_len: u64,
    wal_identity: Option<FileIdentity>,
    shm_identity: Option<FileIdentity>,
}

/// Read-only adapter for Codex session metadata, rollout streams, and artifacts.
pub struct CodexAdapter {
    installation_salt: Vec<u8>,
    observer: Option<Arc<dyn FileAccessObserver>>,
    roots: Mutex<BTreeMap<SourceId, RootBinding>>,
}

struct RolloutFileState {
    reader: BufReader<std::io::Take<File>>,
    session_id: EntityId,
    sequence: i64,
    pending_tools: VecDeque<ToolCallRecord>,
    pending_artifacts: VecDeque<ArtifactRecord>,
}

struct RolloutRecordStream {
    root: RootBinding,
    observer: Option<Arc<dyn FileAccessObserver>>,
    connection: Option<Connection>,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    after_id: Option<String>,
    current: Option<RolloutFileState>,
    emitted: u64,
    finished: bool,
    installation_salt: Vec<u8>,
}

struct SessionRecordStream {
    root: RootBinding,
    observer: Option<Arc<dyn FileAccessObserver>>,
    connection: Option<Connection>,
    index_titles: Option<BTreeMap<String, String>>,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    after_id: Option<String>,
    emitted: u64,
    finished: bool,
}

impl RolloutRecordStream {
    fn opened(&self, kind: SourceKind) {
        if let Some(observer) = &self.observer {
            observer.opened(kind);
        }
    }

    fn bytes_read(&self, kind: SourceKind, count: u64) {
        if let Some(observer) = &self.observer {
            observer.bytes_read(kind, count);
        }
    }

    fn open_next_file(&mut self) -> Result<bool, AdapterError> {
        loop {
            if self.connection.is_none() {
                self.connection = Some(open_state_database(&self.root, &self.observer)?);
            }
            let Some(connection) = self.connection.as_ref() else {
                return Err(AdapterError::Internal {
                    stage: "rollout_connection".to_string(),
                });
            };
            let row = match &self.after_id {
                Some(after_id) => connection.query_row(
                    "SELECT id, rollout_path FROM threads WHERE id > ?1 ORDER BY id LIMIT 1",
                    [after_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                ),
                None => connection.query_row(
                    "SELECT id, rollout_path FROM threads ORDER BY id LIMIT 1",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                ),
            };
            let (native_id, relative_path) = match row {
                Ok(row) => row,
                Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(false),
                Err(_) => {
                    return Err(AdapterError::CorruptSource {
                        stage: "read_rollout_locator".to_string(),
                    });
                }
            };
            self.after_id = Some(native_id.clone());
            let locator = validate_rollout_locator(&relative_path)?;
            self.opened(SourceKind::Rollout);
            let (file, file_size) = match open_rollout_file(&self.root, &locator) {
                Ok(opened) => opened,
                Err(AdapterError::NotFound { .. }) => {
                    self.diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::IncompleteCapability,
                        source_kind: "rollout".to_string(),
                        stage: "missing_rollout".to_string(),
                    })?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let native = NativeId::new(native_id);
            self.current = Some(RolloutFileState {
                reader: BufReader::new(file.take(file_size)),
                session_id: EntityId::from_parts("codex", &self.request.source.source_id, &native),
                sequence: 0,
                pending_tools: VecDeque::new(),
                pending_artifacts: VecDeque::new(),
            });
            return Ok(true);
        }
    }

    fn finish_record(&mut self, record: CanonicalRecord) -> Result<CanonicalRecord, AdapterError> {
        check_record_value_size(&record, &self.request.budget)?;
        self.request.budget.charge_records(1)?;
        self.emitted += 1;
        Ok(record)
    }

    /// Releases the shared connection and any open rollout file so a terminal
    /// stream never pins source files beyond its own lifetime.
    fn release(&mut self) {
        self.current = None;
        self.connection = None;
    }
}

impl Iterator for SessionRecordStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .request
                .limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            self.connection = None;
            return None;
        }
        if let Err(error) = check_scan_state(
            &self.request.cancellation,
            &self.request.budget,
            self.emitted,
            self.request.budget.bytes_read_used(),
        ) {
            self.finished = true;
            self.connection = None;
            return Some(Err(error));
        }
        if self.connection.is_none() {
            self.connection = Some(match open_state_database(&self.root, &self.observer) {
                Ok(connection) => connection,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            });
        }
        let Some(connection) = self.connection.as_ref() else {
            return Some(Err(AdapterError::Internal {
                stage: "session_connection".to_string(),
            }));
        };
        let mut page_request = self.request.clone();
        page_request.limit = Some(1);
        let loaded = scan_sessions(&page_request, connection, self.after_id.as_deref());
        let loaded = match loaded {
            Ok(loaded) => loaded,
            Err(error) => {
                self.finished = true;
                self.connection = None;
                return Some(Err(error));
            }
        };
        let Some(record) = loaded.into_iter().next() else {
            self.finished = true;
            self.connection = None;
            return None;
        };
        let CanonicalRecord::Session(session) = &record else {
            self.finished = true;
            self.connection = None;
            return Some(Err(AdapterError::Internal {
                stage: "session_record".to_string(),
            }));
        };
        self.after_id = Some(session.native_id.as_str().to_string());
        if projected(&self.request.projection, "title") {
            if self.index_titles.is_none() {
                self.index_titles = Some(
                    match load_session_index_titles(&self.root, &self.request, &self.observer) {
                        Ok(titles) => titles,
                        Err(error) => {
                            self.finished = true;
                            self.connection = None;
                            return Some(Err(error));
                        }
                    },
                );
            }
            if let (Some(index_titles), Some(database_title)) =
                (&self.index_titles, session.title.as_ref())
                && index_titles
                    .get(session.native_id.as_str())
                    .is_some_and(|index_title| index_title != database_title)
                && let Err(error) = self
                    .diagnostics
                    .push(warning(AdapterWarningKind::FieldConflict))
            {
                self.finished = true;
                self.connection = None;
                return Some(Err(error));
            }
        }
        self.emitted += 1;
        Some(Ok(record))
    }
}

impl Iterator for RolloutRecordStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished
            || self
                .request
                .limit
                .is_some_and(|limit| self.emitted >= limit)
        {
            self.finished = true;
            self.release();
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
                self.release();
                return Some(Err(error));
            }
            if self.current.is_none() {
                match self.open_next_file() {
                    Ok(true) => {}
                    Ok(false) => {
                        self.finished = true;
                        self.release();
                        return None;
                    }
                    Err(error) => {
                        self.finished = true;
                        self.release();
                        return Some(Err(error));
                    }
                }
            }
            let mut current = self.current.take()?;
            if self.request.table == TableName::Artifacts
                && let Some(artifact) = current.pending_artifacts.pop_front()
            {
                self.current = Some(current);
                return Some(self.finish_record(CanonicalRecord::Artifact(artifact)));
            }
            let fields = ReadFields {
                content: projected(&self.request.projection, "content"),
                arguments: projected(&self.request.projection, "arguments"),
                output: projected(&self.request.projection, "output"),
                artifacts: self.request.table == TableName::Artifacts,
                artifact_content: projected(&self.request.projection, "content")
                    || projected(&self.request.projection, "content_json"),
            };
            let parsed = match parse_next(&mut current.reader, &fields) {
                Ok(parsed) => parsed,
                Err(error) => {
                    self.finished = true;
                    self.release();
                    return Some(Err(error));
                }
            };
            if let Err(error) = self.request.budget.charge_bytes_read(parsed.bytes_read) {
                self.finished = true;
                self.release();
                return Some(Err(error));
            }
            self.bytes_read(SourceKind::Rollout, parsed.bytes_read);
            for kind in parsed.warnings {
                if let Err(error) = self.diagnostics.push(warning(kind)) {
                    self.finished = true;
                    self.release();
                    return Some(Err(error));
                }
            }
            let Some(event) = parsed.event else {
                if self.request.table == TableName::ToolCalls
                    && let Some(call) = current.pending_tools.pop_front()
                {
                    self.current = Some(current);
                    return Some(self.finish_record(CanonicalRecord::ToolCall(call)));
                }
                self.current = None;
                continue;
            };
            let is_response_item = event.event_type.as_deref() == Some("response_item");
            let is_patch_event = self.request.table == TableName::Artifacts
                && event.event_type.as_deref() == Some("event_msg")
                && event.payload.item_type.as_deref() == Some("patch_apply_end");
            if !is_response_item && !is_patch_event {
                self.current = Some(current);
                continue;
            }
            match (self.request.table, event.payload.item_type.as_deref()) {
                (TableName::Messages, Some("message")) => {
                    current.sequence += 1;
                    let record = CanonicalRecord::Message(message_from_event(
                        &self.request.source.source_id,
                        current.session_id.clone(),
                        current.sequence,
                        event.payload,
                        event.timestamp,
                    ));
                    self.current = Some(current);
                    return Some(self.finish_record(record));
                }
                (TableName::ToolCalls, Some("function_call")) => {
                    current.sequence += 1;
                    current.pending_tools.push_back(tool_from_event(
                        &self.request.source.source_id,
                        current.session_id.clone(),
                        current.sequence,
                        event.payload,
                    ));
                    self.current = Some(current);
                }
                (TableName::ToolCalls, Some("function_call_output")) => {
                    let call_id = event.payload.call_id.as_deref().unwrap_or("<missing>");
                    if let Some(index) = current
                        .pending_tools
                        .iter()
                        .position(|call| call.tool_call_id.as_str().ends_with(call_id))
                    {
                        let mut call = current.pending_tools.remove(index)?;
                        call.output = event.payload.output;
                        call.status = Some("success".to_string());
                        if call.output.is_some() {
                            call.provenance.extend(provenance_map_for_source(
                                &self.request.source.source_id,
                                None,
                                "rollout",
                                &["output", "status"],
                            ));
                        }
                        self.current = Some(current);
                        return Some(self.finish_record(CanonicalRecord::ToolCall(call)));
                    }
                    self.current = Some(current);
                }
                (TableName::Artifacts, Some("patch_apply_end")) => {
                    let call_id = event.payload.call_id.clone();
                    for change in event.payload.changes {
                        current.sequence += 1;
                        current.pending_artifacts.push_back(artifact_from_change(
                            &self.request.source.source_id,
                            current.session_id.clone(),
                            current.sequence,
                            call_id.as_deref(),
                            change,
                            event.timestamp,
                            &self.request.projection,
                            &self.installation_salt,
                        ));
                    }
                    if let Some(artifact) = current.pending_artifacts.pop_front() {
                        self.current = Some(current);
                        return Some(self.finish_record(CanonicalRecord::Artifact(artifact)));
                    }
                    self.current = Some(current);
                }
                _ => self.current = Some(current),
            }
        }
    }
}

impl CodexAdapter {
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

    fn opened(&self, kind: SourceKind) {
        if let Some(observer) = &self.observer {
            observer.opened(kind);
        }
    }

    fn root(&self, manifest: &SourceManifest) -> Result<RootBinding, AdapterError> {
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "source_registry".to_string(),
            })?
            .get(&manifest.source_id)
            .cloned()
            .ok_or_else(|| AdapterError::NotFound {
                stage: "source_registry".to_string(),
            })
    }

    /// Validates one candidate root and binds the identities that scans later
    /// revalidate, failing closed on symlink roots, world/group-writable roots,
    /// hostile sidecars, and hot WAL files that would require recovery writes.
    fn validate_root(path: &Path) -> Result<RootBinding, AdapterError> {
        let metadata = fs::symlink_metadata(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => AdapterError::NotFound {
                stage: "codex_root".to_string(),
            },
            _ => AdapterError::PermissionDenied {
                stage: "codex_root".to_string(),
            },
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "codex_root_type".to_string(),
            });
        }
        let opened =
            aql_fs::open_absolute_dir(path).map_err(|_| AdapterError::PermissionDenied {
                stage: "codex_root_open".to_string(),
            })?;
        let opened_metadata =
            opened
                .dir_metadata()
                .map_err(|_| AdapterError::PermissionDenied {
                    stage: "codex_root_metadata".to_string(),
                })?;
        #[cfg(unix)]
        {
            use cap_std::fs::PermissionsExt;
            if opened_metadata.permissions().mode() & 0o022 != 0 {
                return Err(AdapterError::PermissionDenied {
                    stage: "codex_root_permissions".to_string(),
                });
            }
        }
        let path = path.canonicalize().map_err(|_| AdapterError::NotFound {
            stage: "codex_root_canonicalize".to_string(),
        })?;
        let directory =
            aql_fs::open_absolute_dir(&path).map_err(|_| AdapterError::SnapshotUnavailable)?;
        let identity = aql_fs::identity(
            &directory
                .dir_metadata()
                .map_err(|_| AdapterError::SnapshotUnavailable)?,
        );
        if identity != aql_fs::identity(&opened_metadata) {
            return Err(AdapterError::SnapshotUnavailable);
        }
        let database = database_path(&path)?;
        let database_metadata =
            fs::symlink_metadata(&database).map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => AdapterError::NotFound {
                    stage: "codex_state_database".to_string(),
                },
                _ => AdapterError::PermissionDenied {
                    stage: "codex_state_database".to_string(),
                },
            })?;
        if database_metadata.file_type().is_symlink() || !database_metadata.is_file() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "codex_state_database_type".to_string(),
            });
        }
        let wal_identity =
            optional_file_identity(&sidecar_path(&database, "-wal"), "codex_state_database_wal")?;
        let shm_identity =
            optional_file_identity(&sidecar_path(&database, "-shm"), "codex_state_database_shm")?;
        if wal_identity.is_some() && shm_identity.is_none() {
            // A WAL without its shared-memory index would force SQLite to run
            // recovery and create sidecars next to the source; fail closed.
            return Err(AdapterError::SnapshotUnavailable);
        }
        let database_identity =
            aql_fs::file_identity(&database).map_err(|_| AdapterError::SnapshotUnavailable)?;
        Ok(RootBinding {
            path,
            identity,
            database,
            database_identity,
            database_len: database_metadata.len(),
            wal_identity,
            shm_identity,
        })
    }

    fn session_schema() -> Vec<ColumnCapability> {
        vec![
            column("session_id", AccessClass::Safe),
            column("native_id", AccessClass::Safe),
            column("source_id", AccessClass::Safe),
            column("agent_id", AccessClass::Safe),
            column("title", AccessClass::Content),
            column("preview", AccessClass::Content),
            column("cwd", AccessClass::Path),
            column("model", AccessClass::Safe),
            column("provider", AccessClass::Safe),
            column("created_at", AccessClass::Safe),
            column("updated_at", AccessClass::Safe),
            column("archived", AccessClass::Safe),
            column("tokens_used", AccessClass::Safe),
        ]
    }

    fn message_schema() -> Vec<ColumnCapability> {
        vec![
            column("message_id", AccessClass::Safe),
            column("session_id", AccessClass::Safe),
            column("sequence", AccessClass::Safe),
            column("role", AccessClass::Safe),
            column("kind", AccessClass::Safe),
            column("content", AccessClass::Content),
            column("created_at", AccessClass::Safe),
        ]
    }

    fn tool_schema() -> Vec<ColumnCapability> {
        vec![
            column("tool_call_id", AccessClass::Safe),
            column("session_id", AccessClass::Safe),
            column("sequence", AccessClass::Safe),
            column("tool_name", AccessClass::Safe),
            column("arguments", AccessClass::ToolInput),
            column("output", AccessClass::ToolOutput),
            column("status", AccessClass::Safe),
        ]
    }

    fn session_edge_schema() -> Vec<ColumnCapability> {
        vec![
            column("edge_id", AccessClass::Safe),
            column("source_id", AccessClass::Safe),
            column("parent_session_id", AccessClass::Safe),
            column("child_session_id", AccessClass::Safe),
            column("edge_kind", AccessClass::Safe),
            column("created_at", AccessClass::Safe),
            column("native_edge_id", AccessClass::Safe),
        ]
    }

    fn artifact_schema() -> Vec<ColumnCapability> {
        vec![
            column("artifact_id", AccessClass::Safe),
            column("source_id", AccessClass::Safe),
            column("session_id", AccessClass::Safe),
            column("tool_call_id", AccessClass::Safe),
            column("kind", AccessClass::Safe),
            column("name", AccessClass::Content),
            column("path", AccessClass::Path),
            column("media_type", AccessClass::Safe),
            column("size_bytes", AccessClass::Safe),
            column("created_at", AccessClass::Safe),
            column("content", AccessClass::Content),
            column("content_json", AccessClass::Content),
        ]
    }
}

fn scan_session_edge(
    request: &ScanRequest,
    connection: &Connection,
    after_child_id: Option<&str>,
) -> Result<Option<(CanonicalRecord, bool)>, AdapterError> {
    let row = match after_child_id {
            Some(after) => connection.query_row(
                "SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges WHERE child_thread_id > ?1 ORDER BY child_thread_id LIMIT 1",
                [after],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            ),
            None => connection.query_row(
                "SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges ORDER BY child_thread_id LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            ),
        };
    let (parent, child, status) = match row {
        Ok(row) => row,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
        Err(_) => {
            return Err(AdapterError::CorruptSource {
                stage: "read_session_edge".to_string(),
            });
        }
    };
    check_scan_state(&request.cancellation, &request.budget, 1, 0)?;
    request.budget.charge_records(1)?;
    let parent_native = NativeId::new(parent.clone());
    let child_native = NativeId::new(child.clone());
    let edge_native = NativeId::new(format!("{parent}->{child}"));
    let record = SessionEdgeRecord {
        edge_id: EntityId::from_parts("codex", &request.source.source_id, &edge_native),
        source_id: request.source.source_id.clone(),
        parent_session_id: EntityId::from_parts("codex", &request.source.source_id, &parent_native),
        child_session_id: EntityId::from_parts("codex", &request.source.source_id, &child_native),
        edge_kind: status,
        created_at: None,
        native_edge_id: Some(edge_native),
        provenance: provenance_map_for_source(
            &request.source.source_id,
            Some(&request.source.format_fingerprint),
            "state_database",
            &[
                "edge_id",
                "source_id",
                "parent_session_id",
                "child_session_id",
                "edge_kind",
                "native_edge_id",
            ],
        ),
        extensions: BTreeMap::new(),
    };
    let child_exists: i64 = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM threads WHERE id = ?1)",
            [&child],
            |row| row.get(0),
        )
        .map_err(|_| AdapterError::CorruptSource {
            stage: "check_session_edge_child".to_string(),
        })?;
    Ok(Some((
        CanonicalRecord::SessionEdge(record),
        child_exists == 0,
    )))
}

fn scan_sessions(
    request: &ScanRequest,
    connection: &Connection,
    after_id: Option<&str>,
) -> Result<Vec<CanonicalRecord>, AdapterError> {
    let available_columns = thread_columns(connection)?;
    let mut selected_columns = vec!["id"];
    for (logical, physical) in [
        ("created_at", "created_at_ms"),
        ("updated_at", "updated_at_ms"),
        ("provider", "model_provider"),
        ("cwd", "cwd"),
        ("title", "title"),
        ("tokens_used", "tokens_used"),
        ("archived", "archived"),
        ("model", "model"),
        ("preview", "preview"),
    ] {
        if projected(&request.projection, logical) && available_columns.contains(physical) {
            selected_columns.push(physical);
        }
    }
    if request.predicates.iter().any(is_updated_at_range)
        && available_columns.contains("updated_at_ms")
    {
        selected_columns.push("updated_at_ms");
    }
    selected_columns.sort_unstable();
    selected_columns.dedup();
    let query = format!(
        "SELECT {} FROM threads{} ORDER BY id",
        selected_columns.join(", "),
        if after_id.is_some() {
            " WHERE id > ?1"
        } else {
            ""
        }
    );
    let mut statement =
        connection
            .prepare(&query)
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "prepare_threads".to_string(),
            })?;
    let mut rows = match after_id {
        Some(after_id) => statement.query([after_id]),
        None => statement.query([]),
    }
    .map_err(|_| AdapterError::CorruptSource {
        stage: "query_threads".to_string(),
    })?;
    let mut records = Vec::new();
    while let Some(row) = rows.next().map_err(|_| AdapterError::CorruptSource {
        stage: "read_threads".to_string(),
    })? {
        check_scan_state(
            &request.cancellation,
            &request.budget,
            records.len() as u64 + 1,
            0,
        )?;
        let native = NativeId::new(row.get::<_, String>("id").map_err(db_read_error)?);
        let updated_at_ms = row.get::<_, i64>("updated_at_ms").ok();
        if !matches_session_predicates(&native, updated_at_ms, &request.predicates) {
            continue;
        }
        request.budget.charge_records(1)?;
        let entity = EntityId::from_parts("codex", &request.source.source_id, &native);
        let selected = |name| projected(&request.projection, name);
        let mut record = SessionRecord {
            session_id: entity,
            native_id: native,
            source_id: request.source.source_id.clone(),
            agent_id: "codex".to_string(),
            title: selected("title").then(|| row.get("title").ok()).flatten(),
            preview: selected("preview")
                .then(|| row.get("preview").ok())
                .flatten(),
            cwd: selected("cwd").then(|| row.get("cwd").ok()).flatten(),
            project: None,
            model: selected("model").then(|| row.get("model").ok()).flatten(),
            provider: selected("provider")
                .then(|| row.get("model_provider").ok())
                .flatten(),
            created_at: selected("created_at")
                .then(|| timestamp_millis(row.get("created_at_ms").ok()))
                .flatten(),
            updated_at: selected("updated_at")
                .then(|| timestamp_millis(row.get("updated_at_ms").ok()))
                .flatten(),
            status: None,
            archived: selected("archived")
                .then(|| row.get::<_, i64>("archived").ok().map(|value| value != 0))
                .flatten(),
            message_count: None,
            tool_call_count: None,
            tokens_used: selected("tokens_used")
                .then(|| row.get("tokens_used").ok())
                .flatten(),
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: SnapshotState::Weak,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        };
        let provenance_fields = session_provenance_fields(&record, &request.projection);
        record.provenance = provenance_map_for_source(
            &request.source.source_id,
            Some(&request.source.format_fingerprint),
            "state_database",
            &provenance_fields,
        );
        let record = CanonicalRecord::Session(record);
        check_record_value_size(&record, &request.budget)?;
        records.push(record);
        if request
            .limit
            .is_some_and(|limit| records.len() as u64 >= limit)
        {
            break;
        }
    }
    Ok(records)
}

/// Reads `session_index.jsonl` once per scan under the shared budget and
/// returns the native-id to title evidence map used for conflict warnings.
/// The index is title evidence only; missing files degrade to an empty map.
fn load_session_index_titles(
    root: &RootBinding,
    request: &ScanRequest,
    observer: &Option<Arc<dyn FileAccessObserver>>,
) -> Result<BTreeMap<String, String>, AdapterError> {
    let mut titles = BTreeMap::new();
    validate_binding(root)?;
    let directory = open_bound_root(root)?;
    let path = Path::new("session_index.jsonl");
    let metadata = match directory.symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(titles),
        Err(_) => {
            return Err(AdapterError::PermissionDenied {
                stage: "session_index_metadata".to_string(),
            });
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AdapterError::UnsupportedFormat {
            stage: "session_index_type".to_string(),
        });
    }
    if let Some(observer) = observer {
        observer.opened(SourceKind::SessionIndex);
    }
    let mut options = cap_std::fs::OpenOptions::new();
    options.read(true);
    let file =
        aql_fs::open_file(&directory, path, options).map_err(|_| AdapterError::NotFound {
            stage: "open_session_index".to_string(),
        })?;
    let mut reader = BufReader::new(file.into_std());
    while let Some((line, _complete, consumed)) = read_limited_line(
        &mut reader,
        MAX_INDEX_RECORD_BYTES,
        "codex_index_record_bytes",
    )? {
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
        request.budget.charge_bytes_read(consumed as u64)?;
        if let Some(observer) = observer {
            observer.bytes_read(SourceKind::SessionIndex, consumed as u64);
        }
        let value: JsonValue =
            serde_json::from_slice(&line).map_err(|_| AdapterError::CorruptSource {
                stage: "parse_session_index".to_string(),
            })?;
        let Some(id) = value.get("id").and_then(JsonValue::as_str) else {
            continue;
        };
        let Some(index_title) = value.get("thread_name").and_then(JsonValue::as_str) else {
            continue;
        };
        titles.insert(id.to_string(), index_title.to_string());
    }
    Ok(titles)
}

impl AgentAdapter for CodexAdapter {
    fn id(&self) -> &'static str {
        "codex"
    }

    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError> {
        let binding = Self::validate_root(Path::new(&request.data_root))?;
        self.opened(SourceKind::StateDatabase);
        let connection =
            open_state_database_file(&binding.database, binding.wal_identity.is_some())?;
        validate_binding(&binding)?;
        let has_threads: i64 = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='threads')",
                [],
                |row| row.get(0),
            )
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "probe_threads".to_string(),
            })?;
        if has_threads == 0 {
            return Err(AdapterError::UnsupportedFormat {
                stage: "probe_threads".to_string(),
            });
        }
        let columns = thread_columns(&connection)?;
        for required in ["id", "rollout_path"] {
            if !columns.contains(required) {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "probe_required_columns".to_string(),
                });
            }
        }
        let user_version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "probe_user_version".to_string(),
            })?;
        let base_fingerprint = format_fingerprint(&connection, user_version)?;
        let has_session_edges: i64 = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='thread_spawn_edges')",
                [],
                |row| row.get(0),
            )
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "probe_session_edges".to_string(),
            })?;
        if has_session_edges != 0 {
            let edge_columns = table_columns(&connection, "thread_spawn_edges")?;
            if !["parent_thread_id", "child_thread_id", "status"]
                .into_iter()
                .all(|column| edge_columns.contains(column))
            {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "probe_session_edge_columns".to_string(),
                });
            }
        }
        let format_fingerprint = format!(
            "{base_fingerprint}:index-{}:rollout-{}:edges-{}",
            if binding.path.join("session_index.jsonl").is_file() {
                "jsonl-v0"
            } else {
                "none"
            },
            "jsonl-v0",
            if has_session_edges != 0 {
                "sqlite-v0"
            } else {
                "none"
            }
        );
        let known_columns: BTreeSet<_> = [
            "id",
            "rollout_path",
            "created_at",
            "updated_at",
            "source",
            "model_provider",
            "cwd",
            "title",
            "tokens_used",
            "archived",
            "cli_version",
            "model",
            "created_at_ms",
            "updated_at_ms",
            "preview",
        ]
        .into_iter()
        .collect();
        let optional_columns: BTreeSet<_> = [
            "model_provider",
            "cwd",
            "title",
            "tokens_used",
            "archived",
            "model",
            "created_at_ms",
            "updated_at_ms",
            "preview",
        ]
        .into_iter()
        .collect();
        let mut warnings = Vec::new();
        if columns
            .iter()
            .any(|column| !known_columns.contains(column.as_str()))
        {
            warnings.push("unknown_optional_columns".to_string());
        }
        if optional_columns
            .iter()
            .any(|column| !columns.contains(*column))
        {
            warnings.push("missing_optional_columns".to_string());
        }
        if user_version != 5 {
            warnings.push("unrecognized_user_version".to_string());
        }
        let root_text = binding.path.to_string_lossy();
        let source_id =
            SourceId::for_data_root("codex", &root_text, self.installation_salt.as_slice());
        let snapshot = snapshot_token(&binding);
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "source_registry".to_string(),
            })?
            .insert(source_id.clone(), binding);
        let data_root_token = format!("codex-root:{}", source_id.as_str());
        Ok(ProbeResult {
            manifests: vec![SourceManifest {
                source_id,
                agent_id: "codex".to_string(),
                display_name: "Codex".to_string(),
                data_root_token,
                format_fingerprint,
                capabilities: [
                    "sessions".to_string(),
                    "messages".to_string(),
                    "tool_calls".to_string(),
                    "artifacts".to_string(),
                ]
                .into_iter()
                .chain((has_session_edges != 0).then(|| "session_edges".to_string()))
                .collect(),
                snapshot: Some(snapshot),
                warnings,
            }],
        })
    }

    fn capabilities(&self, manifest: &SourceManifest) -> Capabilities {
        let mut columns = Self::session_schema();
        columns.extend(Self::message_schema());
        columns.extend(Self::tool_schema());
        columns.extend(Self::artifact_schema());
        if manifest
            .capabilities
            .iter()
            .any(|value| value == "session_edges")
        {
            columns.extend(Self::session_edge_schema());
        }
        let mut tables = vec![
            TableName::Sessions,
            TableName::Messages,
            TableName::ToolCalls,
            TableName::Artifacts,
        ];
        if manifest
            .capabilities
            .iter()
            .any(|value| value == "session_edges")
        {
            tables.push(TableName::SessionEdges);
        }
        Capabilities {
            tables,
            columns,
            snapshot_strength: SnapshotStrength::Weak,
        }
    }

    fn schema(&self, manifest: &SourceManifest) -> AdapterSchema {
        let mut columns = Self::session_schema();
        columns.extend(Self::message_schema());
        columns.extend(Self::tool_schema());
        columns.extend(Self::artifact_schema());
        if manifest
            .capabilities
            .iter()
            .any(|value| value == "session_edges")
        {
            columns.extend(Self::session_edge_schema());
        }
        AdapterSchema { columns }
    }

    fn scan(&self, request: ScanRequest) -> Result<ScanResult, AdapterError> {
        let table_schema = AdapterSchema {
            columns: match request.table {
                TableName::Sessions => Self::session_schema(),
                TableName::Messages => Self::message_schema(),
                TableName::ToolCalls => Self::tool_schema(),
                TableName::Artifacts => Self::artifact_schema(),
                TableName::SessionEdges
                    if request
                        .source
                        .capabilities
                        .iter()
                        .any(|value| value == "session_edges") =>
                {
                    Self::session_edge_schema()
                }
                TableName::Usage | TableName::SessionEdges => {
                    return Err(AdapterError::UnsupportedFormat {
                        stage: "unsupported_table".to_string(),
                    });
                }
            },
        };
        validate_projection_access(&request.projection, &table_schema, request.access)?;
        if request.table == TableName::Artifacts && !request.access.path {
            return Err(AdapterError::AccessDenied {
                column: "artifacts".to_string(),
            });
        }
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;

        let predicates: Vec<_> = request
            .predicates
            .iter()
            .map(|predicate| predicate_pushdown(request.table, predicate))
            .collect();
        let limit_is_exact = predicates
            .iter()
            .all(|state| *state == PushdownState::Exact)
            && request.order_hint.is_empty();
        let mut effective_request = request.clone();
        if !limit_is_exact {
            effective_request.limit = None;
        }
        if effective_request.table == TableName::SessionEdges {
            let root = self.root(&effective_request.source)?;
            let diagnostics = ScanDiagnostics::default();
            let stream_diagnostics = diagnostics.clone();
            let observer = self.observer.clone();
            let mut connection: Option<Connection> = None;
            let mut after_child_id: Option<String> = None;
            let mut emitted = 0_u64;
            let records = Box::new(std::iter::from_fn(move || {
                if effective_request
                    .limit
                    .is_some_and(|limit| emitted >= limit)
                {
                    return None;
                }
                if connection.is_none() {
                    connection = Some(match open_state_database(&root, &observer) {
                        Ok(connection) => connection,
                        Err(error) => return Some(Err(error)),
                    });
                }
                let Some(connection) = connection.as_ref() else {
                    return Some(Err(AdapterError::Internal {
                        stage: "session_edge_connection".to_string(),
                    }));
                };
                let result = match scan_session_edge(
                    &effective_request,
                    connection,
                    after_child_id.as_deref(),
                ) {
                    Ok(result) => result,
                    Err(error) => return Some(Err(error)),
                };
                let (record, dangling) = result?;
                let CanonicalRecord::SessionEdge(edge) = &record else {
                    return Some(Err(AdapterError::Internal {
                        stage: "session_edge_record".to_string(),
                    }));
                };
                after_child_id = edge
                    .native_edge_id
                    .as_ref()
                    .and_then(|native| native.as_str().split_once("->"))
                    .map(|(_, child)| child.to_string());
                if dangling
                    && let Err(error) = stream_diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::IncompleteCapability,
                        source_kind: "state_database".to_string(),
                        stage: "session_edge_identity".to_string(),
                    })
                {
                    return Some(Err(error));
                }
                emitted += 1;
                Some(Ok(record))
            }));
            return Ok(ScanResult {
                records,
                pushdown: PushdownReport {
                    predicates,
                    limit: request.limit.map(|_| {
                        if limit_is_exact {
                            PushdownState::Exact
                        } else {
                            PushdownState::Unsupported
                        }
                    }),
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
            });
        }
        if matches!(
            effective_request.table,
            TableName::Messages | TableName::ToolCalls | TableName::Artifacts
        ) {
            let root = self.root(&effective_request.source)?;
            let diagnostics = ScanDiagnostics::default();
            let records = Box::new(RolloutRecordStream {
                root,
                observer: self.observer.clone(),
                connection: None,
                request: effective_request,
                diagnostics: diagnostics.clone(),
                after_id: None,
                current: None,
                emitted: 0,
                finished: false,
                installation_salt: self.installation_salt.clone(),
            });
            return Ok(ScanResult {
                records,
                pushdown: PushdownReport {
                    predicates,
                    limit: request.limit.map(|_| {
                        if limit_is_exact {
                            PushdownState::Exact
                        } else {
                            PushdownState::Unsupported
                        }
                    }),
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
            });
        }

        let root = self.root(&effective_request.source)?;
        let diagnostics = ScanDiagnostics::default();
        let records = Box::new(SessionRecordStream {
            root,
            observer: self.observer.clone(),
            connection: None,
            index_titles: None,
            request: effective_request,
            diagnostics: diagnostics.clone(),
            after_id: None,
            emitted: 0,
            finished: false,
        });
        Ok(ScanResult {
            records,
            pushdown: PushdownReport {
                predicates,
                limit: request.limit.map(|_| {
                    if limit_is_exact {
                        PushdownState::Exact
                    } else {
                        PushdownState::Unsupported
                    }
                }),
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

fn column(name: &str, access: AccessClass) -> ColumnCapability {
    ColumnCapability {
        name: ColumnName::new(name),
        access,
    }
}

fn projected(projection: &[ColumnName], name: &str) -> bool {
    projection.iter().any(|column| column.as_str() == name)
}

fn db_read_error(_error: rusqlite::Error) -> AdapterError {
    AdapterError::CorruptSource {
        stage: "read_thread_value".to_string(),
    }
}

/// Selects the newest `state_<version>.sqlite` candidate deterministically
/// instead of relying on directory iteration order.
fn database_path(root: &Path) -> Result<PathBuf, AdapterError> {
    let sqlite_root = root.join("sqlite");
    let entries = fs::read_dir(&sqlite_root).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => AdapterError::NotFound {
            stage: "codex_state_database".to_string(),
        },
        _ => AdapterError::PermissionDenied {
            stage: "codex_state_database".to_string(),
        },
    })?;
    let mut newest: Option<(u64, PathBuf)> = None;
    for entry in entries {
        let entry = entry.map_err(|_| AdapterError::PermissionDenied {
            stage: "codex_state_database_entry".to_string(),
        })?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(version) = name
            .strip_prefix("state_")
            .and_then(|rest| rest.strip_suffix(".sqlite"))
            .and_then(|version| version.parse::<u64>().ok())
        else {
            continue;
        };
        if newest
            .as_ref()
            .is_none_or(|(current, _)| version > *current)
        {
            newest = Some((version, entry.path()));
        }
    }
    newest
        .map(|(_, path)| path)
        .ok_or_else(|| AdapterError::NotFound {
            stage: "codex_state_database".to_string(),
        })
}

fn sidecar_path(database: &Path, suffix: &str) -> PathBuf {
    let mut name = database
        .file_name()
        .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
    name.push(suffix);
    database.with_file_name(name)
}

fn optional_file_identity(path: &Path, stage: &str) -> Result<Option<FileIdentity>, AdapterError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(AdapterError::UnsupportedFormat {
                stage: stage.to_string(),
            })
        }
        Ok(_) => aql_fs::file_identity(path)
            .map(Some)
            .map_err(|_| AdapterError::SnapshotUnavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(AdapterError::PermissionDenied {
            stage: stage.to_string(),
        }),
    }
}

/// Revalidates every identity bound at probe time, failing closed on
/// replacement, shrink, or sidecar drift instead of reading a changed source.
fn validate_binding(binding: &RootBinding) -> Result<(), AdapterError> {
    let root_metadata =
        fs::symlink_metadata(&binding.path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if root_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
        || aql_fs::directory_identity(&binding.path)
            .map_err(|_| AdapterError::SnapshotUnavailable)?
            != binding.identity
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    let database_metadata =
        fs::symlink_metadata(&binding.database).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if database_metadata.file_type().is_symlink()
        || !database_metadata.is_file()
        || database_metadata.len() < binding.database_len
        || aql_fs::file_identity(&binding.database)
            .map_err(|_| AdapterError::SnapshotUnavailable)?
            != binding.database_identity
        || optional_file_identity(
            &sidecar_path(&binding.database, "-wal"),
            "codex_state_database_wal",
        )? != binding.wal_identity
        || optional_file_identity(
            &sidecar_path(&binding.database, "-shm"),
            "codex_state_database_shm",
        )? != binding.shm_identity
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

/// Opens the currently named root without following symlinks and proves that
/// the opened capability is the directory bound during probing. The capability
/// lives only through the following relative open, avoiding long-lived Windows
/// directory locks while closing the path-replacement race.
fn open_bound_root(binding: &RootBinding) -> Result<cap_std::fs::Dir, AdapterError> {
    let directory =
        aql_fs::open_absolute_dir(&binding.path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    let metadata = directory
        .dir_metadata()
        .map_err(|_| AdapterError::SnapshotUnavailable)?;
    if aql_fs::identity(&metadata) != binding.identity {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(directory)
}

/// Opens the bound state database after identity revalidation. A database
/// without a WAL sidecar is opened with immutable semantics so the read never
/// creates `-wal`/`-shm` files or attempts WAL recovery next to the source; a
/// bound active WAL (both sidecars present) keeps the plain read-only path.
fn open_state_database(
    root: &RootBinding,
    observer: &Option<Arc<dyn FileAccessObserver>>,
) -> Result<Connection, AdapterError> {
    validate_binding(root)?;
    if let Some(observer) = observer {
        observer.opened(SourceKind::StateDatabase);
    }
    let connection = open_state_database_file(&root.database, root.wal_identity.is_some())?;
    validate_binding(root)?;
    Ok(connection)
}

fn open_state_database_file(database: &Path, active_wal: bool) -> Result<Connection, AdapterError> {
    if active_wal {
        Connection::open_with_flags(
            database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "open_state_database".to_string(),
        })
    } else {
        Connection::open_with_flags(
            immutable_uri(database)?,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "open_state_database".to_string(),
        })
    }
}

/// Builds a `file:` URI that opens the database with immutable semantics,
/// percent-encoding the characters SQLite treats as URI delimiters.
fn immutable_uri(database: &Path) -> Result<String, AdapterError> {
    let text = database
        .to_str()
        .ok_or_else(|| AdapterError::UnsupportedFormat {
            stage: "state_database_uri".to_string(),
        })?;
    let text = normalize_windows_verbatim(text);
    let mut uri = String::with_capacity(text.len() + 24);
    uri.push_str("file:");
    let bytes = text.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        uri.push_str("///");
    }
    for character in text.chars() {
        match character {
            '\\' => uri.push('/'),
            '%' => uri.push_str("%25"),
            '?' => uri.push_str("%3F"),
            '#' => uri.push_str("%23"),
            _ => uri.push(character),
        }
    }
    uri.push_str("?immutable=1");
    Ok(uri)
}

#[cfg(windows)]
fn normalize_windows_verbatim(text: &str) -> std::borrow::Cow<'_, str> {
    if let Some(rest) = text.strip_prefix(r"\\?\UNC\") {
        return std::borrow::Cow::Owned(format!(r"\\{rest}"));
    }
    std::borrow::Cow::Borrowed(text.strip_prefix(r"\\?\").unwrap_or(text))
}

#[cfg(not(windows))]
fn normalize_windows_verbatim(text: &str) -> &str {
    text
}

/// Derives the opaque snapshot token from the identities bound during probing.
fn snapshot_token(binding: &RootBinding) -> SnapshotToken {
    let mut digest = Sha256::new();
    digest.update(b"codex-snapshot-v0\0");
    for identity in [binding.identity, binding.database_identity] {
        digest.update(identity.device().to_le_bytes());
        digest.update(identity.inode().to_le_bytes());
    }
    digest.update(binding.database_len.to_le_bytes());
    for sidecar in [binding.wal_identity, binding.shm_identity] {
        match sidecar {
            Some(identity) => {
                digest.update([1_u8]);
                digest.update(identity.device().to_le_bytes());
                digest.update(identity.inode().to_le_bytes());
            }
            None => digest.update([0_u8]),
        }
    }
    let hash = digest.finalize();
    SnapshotToken::new(format!(
        "codex-snapshot:{}",
        hash[..12]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

/// Validates a store-supplied rollout locator before any filesystem access.
/// Only normalized relative paths below the known rollout roots are accepted;
/// absolute paths, parent components, and every other component shape fail
/// closed.
fn validate_rollout_locator(locator: &str) -> Result<PathBuf, AdapterError> {
    let invalid = || AdapterError::CorruptSource {
        stage: "rollout_locator".to_string(),
    };
    let path = Path::new(locator);
    let mut components = path.components();
    let Some(Component::Normal(root)) = components.next() else {
        return Err(invalid());
    };
    if root != "sessions" && root != "archived_sessions" {
        return Err(invalid());
    }
    let mut depth = 1_usize;
    for component in components {
        if !matches!(component, Component::Normal(_)) {
            return Err(invalid());
        }
        depth += 1;
    }
    if depth < 2 {
        return Err(invalid());
    }
    Ok(path.to_path_buf())
}

/// Opens one validated rollout locator below the bound root without following
/// any symlink component, returning the file and a fixed byte boundary taken
/// from the opened file itself.
fn open_rollout_file(root: &RootBinding, locator: &Path) -> Result<(File, u64), AdapterError> {
    validate_binding(root)?;
    let directory = open_bound_root(root)?;
    let mut options = cap_std::fs::OpenOptions::new();
    options.read(true);
    let file =
        aql_fs::open_file(&directory, locator, options).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => AdapterError::NotFound {
                stage: "open_rollout".to_string(),
            },
            _ => AdapterError::PermissionDenied {
                stage: "open_rollout".to_string(),
            },
        })?;
    let file = file.into_std();
    let metadata = file
        .metadata()
        .map_err(|_| AdapterError::PermissionDenied {
            stage: "stat_rollout".to_string(),
        })?;
    if !metadata.is_file() {
        return Err(AdapterError::CorruptSource {
            stage: "rollout_locator_type".to_string(),
        });
    }
    Ok((file, metadata.len()))
}

fn check_record_value_size(
    record: &CanonicalRecord,
    budget: &ResourceBudget,
) -> Result<(), AdapterError> {
    let sizes: Vec<usize> = match record {
        CanonicalRecord::Session(value) => [
            value.title.as_ref(),
            value.preview.as_ref(),
            value.cwd.as_ref(),
            value.project.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(String::len)
        .collect(),
        CanonicalRecord::Message(value) => value
            .content
            .iter()
            .map(String::len)
            .chain(value.content_json.iter().map(|json| json.to_string().len()))
            .collect(),
        CanonicalRecord::ToolCall(value) => value
            .arguments
            .iter()
            .map(|json| json.to_string().len())
            .chain(value.output.iter().map(String::len))
            .collect(),
        CanonicalRecord::Usage(value) => [value.model.as_ref(), value.provider.as_ref()]
            .into_iter()
            .flatten()
            .map(String::len)
            .collect(),
        CanonicalRecord::SessionEdge(value) => vec![value.edge_kind.len()],
        CanonicalRecord::Artifact(value) => [
            value.name.as_ref(),
            value.path.as_ref(),
            value.media_type.as_ref(),
            value.content.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(String::len)
        .chain(value.content_json.iter().map(|json| json.to_string().len()))
        .collect(),
    };
    if let Some(actual) = sizes
        .into_iter()
        .map(|size| size as u64)
        .find(|size| *size > budget.max_single_value_bytes)
    {
        Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual,
        })
    } else {
        Ok(())
    }
}

fn thread_columns(connection: &Connection) -> Result<BTreeSet<String>, AdapterError> {
    table_columns(connection, "threads")
}

fn table_columns(connection: &Connection, table: &str) -> Result<BTreeSet<String>, AdapterError> {
    if !matches!(table, "threads" | "thread_spawn_edges") {
        return Err(AdapterError::Internal {
            stage: "table_columns_allowlist".to_string(),
        });
    }
    let query = format!("SELECT name FROM pragma_table_info('{table}') ORDER BY cid");
    let mut statement =
        connection
            .prepare(&query)
            .map_err(|_| AdapterError::UnsupportedFormat {
                stage: "read_thread_columns".to_string(),
            })?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "read_thread_columns".to_string(),
        })?;
    rows.collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "read_thread_columns".to_string(),
        })
}

fn format_fingerprint(connection: &Connection, user_version: i64) -> Result<String, AdapterError> {
    let mut statement = connection
        .prepare("SELECT name, type FROM pragma_table_info('threads') ORDER BY cid")
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "fingerprint_schema".to_string(),
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|_| AdapterError::UnsupportedFormat {
            stage: "fingerprint_schema".to_string(),
        })?;
    let mut digest = Sha256::new();
    for row in rows {
        let (name, data_type) = row.map_err(|_| AdapterError::UnsupportedFormat {
            stage: "fingerprint_schema".to_string(),
        })?;
        digest.update(name.as_bytes());
        digest.update(b":");
        digest.update(data_type.as_bytes());
        digest.update(b"\n");
    }
    let hash = digest.finalize();
    Ok(format!(
        "codex-state:user-version-{user_version}:schema-{}",
        hash[..12]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ))
}

fn provenance_map_for_source(
    source_id: &SourceId,
    source_version: Option<&str>,
    source_kind: &str,
    fields: &[&str],
) -> BTreeMap<String, Vec<Provenance>> {
    fields
        .iter()
        .map(|field| {
            (
                (*field).to_string(),
                vec![Provenance {
                    source_id: source_id.clone(),
                    source_kind: source_kind.to_string(),
                    source_locator: source_kind.to_string(),
                    source_version: source_version.map(str::to_string),
                    observed_at: Utc::now(),
                    watermark: None,
                    derived: false,
                }],
            )
        })
        .collect()
}

fn session_provenance_fields<'a>(
    record: &SessionRecord,
    projection: &'a [ColumnName],
) -> Vec<&'a str> {
    let mut fields = vec!["session_id", "native_id", "source_id", "agent_id"];
    fields.extend(projection.iter().filter_map(|column| {
        let present = match column.as_str() {
            "title" => record.title.is_some(),
            "preview" => record.preview.is_some(),
            "cwd" => record.cwd.is_some(),
            "project" => record.project.is_some(),
            "model" => record.model.is_some(),
            "provider" => record.provider.is_some(),
            "created_at" => record.created_at.is_some(),
            "updated_at" => record.updated_at.is_some(),
            "status" => record.status.is_some(),
            "archived" => record.archived.is_some(),
            "message_count" => record.message_count.is_some(),
            "tool_call_count" => record.tool_call_count.is_some(),
            "tokens_used" => record.tokens_used.is_some(),
            "identity_confidence" | "snapshot_state" => true,
            _ => false,
        };
        present.then(|| column.as_str())
    }));
    fields.sort_unstable();
    fields.dedup();
    fields
}

fn timestamp_millis(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(DateTime::from_timestamp_millis)
}

fn matches_session_predicates(
    native_id: &NativeId,
    updated_at_ms: Option<i64>,
    predicates: &[aql_adapter_api::Predicate],
) -> bool {
    predicates.iter().all(|predicate| match predicate {
        aql_adapter_api::Predicate::Eq(column, Literal::Text(value))
            if column.as_str() == "native_id" =>
        {
            native_id.as_str() == value
        }
        aql_adapter_api::Predicate::Eq(column, Literal::Text(value))
            if column.as_str() == "session_id" =>
        {
            value
                .rsplit(':')
                .next()
                .is_some_and(|value| value == native_id.as_str())
        }
        aql_adapter_api::Predicate::Range {
            column,
            lower,
            upper,
        } if column.as_str() == "updated_at" => updated_at_ms.is_some_and(|value| {
            lower
                .as_ref()
                .is_none_or(|bound| literal_i64(bound).is_some_and(|bound| value >= bound))
                && upper
                    .as_ref()
                    .is_none_or(|bound| literal_i64(bound).is_some_and(|bound| value <= bound))
        }),
        _ => true,
    })
}

fn literal_i64(literal: &Literal) -> Option<i64> {
    match literal {
        Literal::Integer(value) => Some(*value),
        _ => None,
    }
}

fn is_updated_at_range(predicate: &aql_adapter_api::Predicate) -> bool {
    matches!(
        predicate,
        aql_adapter_api::Predicate::Range { column, .. } if column.as_str() == "updated_at"
    )
}

fn predicate_pushdown(table: TableName, predicate: &aql_adapter_api::Predicate) -> PushdownState {
    if table != TableName::Sessions {
        return PushdownState::Unsupported;
    }
    match predicate {
        aql_adapter_api::Predicate::Eq(column, Literal::Text(_))
            if column.as_str() == "session_id" || column.as_str() == "native_id" =>
        {
            PushdownState::Exact
        }
        aql_adapter_api::Predicate::Range {
            column,
            lower,
            upper,
        } if column.as_str() == "updated_at"
            && lower
                .as_ref()
                .is_none_or(|value| literal_i64(value).is_some())
            && upper
                .as_ref()
                .is_none_or(|value| literal_i64(value).is_some()) =>
        {
            PushdownState::Exact
        }
        _ => PushdownState::Unsupported,
    }
}

fn message_from_event(
    source_id: &SourceId,
    session_id: EntityId,
    sequence: i64,
    payload: ParsedPayload,
    created_at: Option<DateTime<Utc>>,
) -> MessageRecord {
    let mut record = MessageRecord {
        message_id: EntityId::new(format!("{}:message-{sequence}", session_id.as_str())),
        session_id,
        source_id: source_id.clone(),
        sequence,
        role: payload.role.unwrap_or_else(|| "unknown".to_string()),
        kind: Some("message".to_string()),
        content: payload.content,
        content_json: None,
        model: None,
        created_at,
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        is_error: None,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    };
    let mut fields = vec!["message_id", "session_id", "sequence", "role", "kind"];
    if record.content.is_some() {
        fields.push("content");
    }
    record.provenance = provenance_map_for_source(source_id, None, "rollout", &fields);
    record
}

fn tool_from_event(
    source_id: &SourceId,
    session_id: EntityId,
    sequence: i64,
    payload: ParsedPayload,
) -> ToolCallRecord {
    let call_id = payload.call_id.as_deref().unwrap_or("missing-call-id");
    let mut record = ToolCallRecord {
        tool_call_id: EntityId::new(format!("{}:{call_id}", session_id.as_str())),
        session_id,
        message_id: None,
        source_id: source_id.clone(),
        sequence,
        tool_name: payload.name.unwrap_or_else(|| "unknown".to_string()),
        namespace: None,
        arguments: payload.arguments,
        output: None,
        status: Some("pending".to_string()),
        started_at: None,
        ended_at: None,
        duration_ms: None,
        exit_code: None,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    };
    let mut fields = vec![
        "tool_call_id",
        "session_id",
        "sequence",
        "tool_name",
        "status",
    ];
    if record.arguments.is_some() {
        fields.push("arguments");
    }
    record.provenance = provenance_map_for_source(source_id, None, "rollout", &fields);
    record
}

#[allow(clippy::too_many_arguments)]
fn artifact_from_change(
    source_id: &SourceId,
    session_id: EntityId,
    sequence: i64,
    call_id: Option<&str>,
    change: ParsedArtifactChange,
    created_at: Option<DateTime<Utc>>,
    projection: &[ColumnName],
    installation_salt: &[u8],
) -> ArtifactRecord {
    let identity_input = format!("{}\0{sequence}\0{}", session_id.as_str(), change.path);
    let native_id = NativeId::new(installation_scoped_hmac(
        "codex-patch-artifact",
        &identity_input,
        installation_salt,
    ));
    let include_content = projected(projection, "content");
    let include_content_json = projected(projection, "content_json");
    let content = include_content
        .then(|| {
            change
                .content
                .clone()
                .or_else(|| change.unified_diff.clone())
        })
        .flatten();
    let content_json = include_content_json.then(|| {
        let mut object = serde_json::Map::new();
        if let Some(change_type) = &change.change_type {
            object.insert("type".to_string(), JsonValue::String(change_type.clone()));
        }
        if let Some(move_path) = &change.move_path {
            object.insert(
                "move_path".to_string(),
                JsonValue::String(move_path.clone()),
            );
        }
        if let Some(value) = &change.content {
            object.insert("content".to_string(), JsonValue::String(value.clone()));
        }
        if let Some(value) = &change.unified_diff {
            object.insert("unified_diff".to_string(), JsonValue::String(value.clone()));
        }
        JsonValue::Object(object)
    });
    let tool_call_id =
        call_id.map(|call_id| EntityId::new(format!("{}:{call_id}", session_id.as_str())));
    let mut record = ArtifactRecord {
        artifact_id: EntityId::from_parts("codex", source_id, &native_id),
        source_id: source_id.clone(),
        session_id,
        tool_call_id,
        kind: change.change_type.unwrap_or_else(|| "patch".to_string()),
        name: None,
        path: Some(change.path),
        media_type: None,
        size_bytes: None,
        created_at,
        content,
        content_json,
        provenance: BTreeMap::new(),
        extensions: BTreeMap::new(),
    };
    let mut fields = vec!["artifact_id", "source_id", "session_id", "kind", "path"];
    if record.tool_call_id.is_some() {
        fields.push("tool_call_id");
    }
    if record.created_at.is_some() {
        fields.push("created_at");
    }
    if record.content.is_some() {
        fields.push("content");
    }
    if record.content_json.is_some() {
        fields.push("content_json");
    }
    record.provenance = provenance_map_for_source(source_id, None, "rollout", &fields);
    record
}

fn warning(kind: AdapterWarningKind) -> AdapterWarning {
    AdapterWarning {
        kind,
        source_kind: "rollout".to_string(),
        stage: "scan".to_string(),
    }
}
