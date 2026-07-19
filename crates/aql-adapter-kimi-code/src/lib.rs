//! Read-only adapter for the pinned Kimi Code 0.23.3 local format.

#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use aql_adapter_api::{
    AdapterError, AdapterSchema, AdapterWarning, AdapterWarningKind, AgentAdapter, Capabilities,
    ColumnCapability, ColumnName, Literal, Predicate, ProbeRequest, ProbeResult, PushdownReport,
    PushdownState, ScanDiagnostics, ScanRequest, ScanResult, SnapshotReport, SnapshotStrength,
    TableName, check_scan_state, validate_projection_access,
};
use aql_model::{
    AccessClass, CanonicalRecord, EntityId, IdentityConfidence, NativeId, SessionRecord,
    SnapshotState, SourceId, SourceManifest,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const MAX_STATE_BYTES: u64 = 1024 * 1024;
const MAX_INDEX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_INDEX_LINE_BYTES: usize = 64 * 1024;
const MAX_SESSION_INVENTORY: usize = 100_000;
const FORMAT: &str = "kimi-code-0.23.3-wire-1.4";
type SessionKey = (String, String);
type LocatedSession = (PathBuf, SessionKey);

mod wire;

/// Read-only adapter for Kimi Code indexes, session state, and declared wire streams.
pub struct KimiCodeAdapter {
    installation_salt: Vec<u8>,
    roots: Mutex<BTreeMap<SourceId, RootBinding>>,
}

#[derive(Clone)]
struct RootBinding {
    path: PathBuf,
    identity: FileIdentity,
}

type FileIdentity = aql_fs::FileIdentity;

struct SessionRecordStream {
    root: RootBinding,
    request: ScanRequest,
    after: Option<SessionKey>,
    emitted: u64,
    finished: bool,
    index: Option<BTreeMap<String, PathBuf>>,
    listing: Option<Vec<LocatedSession>>,
    diagnostics: ScanDiagnostics,
    ready: std::collections::VecDeque<SessionRecord>,
}

struct SessionFilterStream {
    inner: SessionRecordStream,
    predicates: Vec<Predicate>,
    limit: Option<u64>,
    emitted: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SafeState {
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    archived: Option<bool>,
    agents: BTreeMap<String, SafeAgent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SafeAgent {
    #[serde(rename = "type")]
    agent_type: String,
    parent_agent_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TitleState {
    title: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreviewState {
    last_prompt: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PathState {
    work_dir: Option<String>,
    custom: Option<PathLegacyCustom>,
}

#[derive(Deserialize)]
struct PathLegacyCustom {
    cwd: Option<String>,
}

#[derive(Deserialize)]
struct RawTitle<'a> {
    #[serde(borrow)]
    title: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPreview<'a> {
    #[serde(borrow)]
    last_prompt: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPath<'a> {
    #[serde(borrow)]
    work_dir: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    custom: Option<RawLegacyCustom<'a>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawWorkdirAuthority<'a> {
    #[serde(borrow)]
    work_dir: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    custom: Option<RawLegacyCustom<'a>>,
}

#[derive(Deserialize)]
struct RawLegacyCustom<'a> {
    #[serde(borrow)]
    cwd: Option<&'a serde_json::value::RawValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexEntry {
    session_id: String,
    session_dir: String,
    #[serde(rename = "workDir")]
    _work_dir: serde::de::IgnoredAny,
}

impl KimiCodeAdapter {
    /// Creates an adapter using an installation-local salt for stable source IDs.
    #[must_use]
    pub fn new(installation_salt: impl Into<Vec<u8>>) -> Self {
        Self {
            installation_salt: installation_salt.into(),
            roots: Mutex::new(BTreeMap::new()),
        }
    }

    fn validate_root(path: &Path) -> Result<RootBinding, AdapterError> {
        let metadata = fs::symlink_metadata(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => AdapterError::NotFound {
                stage: "kimi_root".to_string(),
            },
            _ => AdapterError::PermissionDenied {
                stage: "kimi_root".to_string(),
            },
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "kimi_root_type".to_string(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o022 != 0 {
                return Err(AdapterError::PermissionDenied {
                    stage: "kimi_root_permissions".to_string(),
                });
            }
        }
        let path = path.canonicalize().map_err(|_| AdapterError::NotFound {
            stage: "kimi_root_canonicalize".to_string(),
        })?;
        let identity = aql_fs::directory_identity(&path).map_err(|_| AdapterError::NotFound {
            stage: "kimi_root_identity".to_string(),
        })?;
        Ok(RootBinding { path, identity })
    }

    fn root_for(&self, source: &SourceManifest) -> Result<RootBinding, AdapterError> {
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "kimi_roots".to_string(),
            })?
            .get(&source.source_id)
            .cloned()
            .ok_or_else(|| AdapterError::NotFound {
                stage: "kimi_manifest".to_string(),
            })
    }

    /// Enumerates the bucket/session layout once per scan and returns every
    /// accepted session sorted by `(bucket, session)` key, replacing the
    /// previous per-session re-enumeration.
    fn list_session_dirs(
        root: &RootBinding,
        request: &ScanRequest,
    ) -> Result<Vec<LocatedSession>, AdapterError> {
        validate_root_identity(root)?;
        let sessions = root.path.join("sessions");
        validate_directory(&sessions)?;
        let mut found: Vec<LocatedSession> = Vec::new();
        let buckets = match fs::read_dir(&sessions) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(_) => {
                return Err(AdapterError::PermissionDenied {
                    stage: "kimi_sessions".to_string(),
                });
            }
        };
        for bucket in buckets {
            check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
            let bucket = bucket.map_err(|_| AdapterError::PermissionDenied {
                stage: "kimi_bucket".to_string(),
            })?;
            request
                .budget
                .charge_bytes_read(bucket.file_name().as_encoded_bytes().len() as u64)?;
            let kind = bucket
                .file_type()
                .map_err(|_| AdapterError::PermissionDenied {
                    stage: "kimi_bucket_type".to_string(),
                })?;
            if !kind.is_dir() || kind.is_symlink() {
                continue;
            }
            let bucket_name = bucket.file_name().to_string_lossy().into_owned();
            if !safe_id(&bucket_name) {
                continue;
            }
            let sessions =
                fs::read_dir(bucket.path()).map_err(|_| AdapterError::PermissionDenied {
                    stage: "kimi_bucket_read".to_string(),
                })?;
            for session in sessions {
                check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
                let session = session.map_err(|_| AdapterError::PermissionDenied {
                    stage: "kimi_session_entry".to_string(),
                })?;
                request
                    .budget
                    .charge_bytes_read(session.file_name().as_encoded_bytes().len() as u64)?;
                let kind = session
                    .file_type()
                    .map_err(|_| AdapterError::PermissionDenied {
                        stage: "kimi_session_type".to_string(),
                    })?;
                if kind.is_dir()
                    && !kind.is_symlink()
                    && safe_id(&session.file_name().to_string_lossy())
                {
                    let key = (
                        bucket_name.clone(),
                        session.file_name().to_string_lossy().into_owned(),
                    );
                    found.push((session.path(), key));
                    if found.len() > MAX_SESSION_INVENTORY {
                        return Err(AdapterError::BudgetExceeded {
                            resource: "kimi_session_inventory".to_string(),
                            actual: found.len() as u64,
                        });
                    }
                }
            }
        }
        found.sort_by(|left, right| left.1.cmp(&right.1));
        validate_root_identity(root)?;
        Ok(found)
    }

    /// Returns the first listed session with a key strictly after `after`.
    fn next_session_dir(
        listing: &[LocatedSession],
        after: Option<&SessionKey>,
    ) -> Option<LocatedSession> {
        let index = listing.partition_point(|(_, key)| Some(key) <= after);
        listing.get(index).cloned()
    }

    fn read_state(
        root: &RootBinding,
        path: &Path,
        request: &ScanRequest,
    ) -> Result<(Vec<SessionRecord>, bool), AdapterError> {
        validate_session_chain(root, path)?;
        let state_path = path.join("state.json");
        let (mut file, metadata, _identity) = open_regular_no_follow(&state_path, "kimi_state")?;
        if metadata.len() > MAX_STATE_BYTES {
            return Err(AdapterError::UnsupportedFormat {
                stage: "kimi_state_type_or_size".to_string(),
            });
        }
        let mut bytes = Vec::with_capacity(metadata.len().min(MAX_STATE_BYTES) as usize + 1);
        file.by_ref()
            .take(metadata.len() + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "kimi_state_read".to_string(),
            })?;
        request.budget.charge_bytes_read(bytes.len() as u64)?;
        let after = file
            .metadata()
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "kimi_state_revalidate".to_string(),
            })?;
        if after.len() != metadata.len() || bytes.len() as u64 != metadata.len() {
            return Err(AdapterError::SnapshotUnavailable);
        }
        validate_session_chain(root, path)?;
        let safe: SafeState =
            serde_json::from_slice(&bytes).map_err(|_| AdapterError::CorruptSource {
                stage: "kimi_state_parse".to_string(),
            })?;
        let bucket_verified = validate_workdir_bucket(&bytes, path)?;
        validate_agents(&safe.agents)?;
        let wants_title = projected(&request.projection, "title");
        let wants_preview = projected(&request.projection, "preview");
        let wants_path =
            projected(&request.projection, "cwd") || projected(&request.projection, "project");
        let title = if wants_title {
            validate_title_size(&bytes, request.budget.max_single_value_bytes)?;
            serde_json::from_slice::<TitleState>(&bytes)
                .map(|state| state.title)
                .map_err(|_| AdapterError::CorruptSource {
                    stage: "kimi_state_content".to_string(),
                })?
        } else {
            None
        };
        let preview = if wants_preview {
            validate_preview_size(&bytes, request.budget.max_single_value_bytes)?;
            serde_json::from_slice::<PreviewState>(&bytes)
                .map(|state| state.last_prompt)
                .map_err(|_| AdapterError::CorruptSource {
                    stage: "kimi_state_content".to_string(),
                })?
        } else {
            None
        };
        let path_state = if wants_path {
            validate_path_size(&bytes, request.budget.max_single_value_bytes)?;
            Some(serde_json::from_slice::<PathState>(&bytes).map_err(|_| {
                AdapterError::CorruptSource {
                    stage: "kimi_state_path".to_string(),
                }
            })?)
        } else {
            None
        };
        let native = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "kimi_session_id".to_string(),
            })?;
        let native_id = NativeId::new(native);
        let main_session_id =
            EntityId::from_parts("kimi-code", &request.source.source_id, &native_id);
        let mut records = vec![SessionRecord {
            session_id: main_session_id,
            native_id,
            source_id: request.source.source_id.clone(),
            agent_id: "kimi-code".to_string(),
            title,
            preview,
            cwd: path_state.and_then(|state| {
                state
                    .work_dir
                    .or_else(|| state.custom.and_then(|custom| custom.cwd))
            }),
            project: None,
            model: None,
            provider: None,
            created_at: Some(safe.created_at),
            updated_at: Some(safe.updated_at),
            status: None,
            archived: safe.archived,
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: if bucket_verified {
                SnapshotState::Weak
            } else {
                SnapshotState::Stale
            },
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }];
        for (agent_id, agent) in &safe.agents {
            if agent_id == "main" {
                continue;
            }
            let child_native = NativeId::new(format!("{native}/agent/{agent_id}"));
            records.push(SessionRecord {
                session_id: EntityId::from_parts(
                    "kimi-code",
                    &request.source.source_id,
                    &child_native,
                ),
                native_id: child_native,
                source_id: request.source.source_id.clone(),
                agent_id: "kimi-code".to_string(),
                title: None,
                preview: None,
                cwd: None,
                project: None,
                model: None,
                provider: None,
                created_at: Some(safe.created_at),
                updated_at: Some(safe.updated_at),
                status: Some(agent.agent_type.clone()),
                archived: safe.archived,
                message_count: None,
                tool_call_count: None,
                tokens_used: None,
                identity_confidence: IdentityConfidence::Exact,
                snapshot_state: if bucket_verified {
                    SnapshotState::Weak
                } else {
                    SnapshotState::Stale
                },
                provenance: BTreeMap::new(),
                extensions: BTreeMap::new(),
            });
        }
        Ok((records, bucket_verified))
    }

    fn read_index(
        root: &RootBinding,
        request: &ScanRequest,
        diagnostics: &ScanDiagnostics,
    ) -> Result<BTreeMap<String, PathBuf>, AdapterError> {
        validate_root_identity(root)?;
        let path = root.path.join("session_index.jsonl");
        if !path.exists() {
            diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::StaleSnapshot,
                source_kind: "kimi_index".to_string(),
                stage: "missing_index".to_string(),
            })?;
            return Ok(BTreeMap::new());
        }
        let (mut file, metadata, _identity) = open_regular_no_follow(&path, "kimi_index")?;
        if metadata.len() > MAX_INDEX_BYTES {
            return Err(AdapterError::BudgetExceeded {
                resource: "kimi_index_bytes".to_string(),
                actual: metadata.len(),
            });
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize + 1);
        file.by_ref()
            .take(metadata.len() + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "kimi_index_read".to_string(),
            })?;
        request.budget.charge_bytes_read(bytes.len() as u64)?;
        let after = file
            .metadata()
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "kimi_index_revalidate".to_string(),
            })?;
        if after.len() != metadata.len() || bytes.len() as u64 != metadata.len() {
            return Err(AdapterError::SnapshotUnavailable);
        }
        let sessions_root = root.path.join("sessions");
        let mut result = BTreeMap::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let rest = &bytes[offset..];
            let newline = rest.iter().position(|byte| *byte == b'\n');
            let (line, consumed, complete) = match newline {
                Some(index) => (&rest[..index], index + 1, true),
                None => (rest, rest.len(), false),
            };
            offset += consumed;
            if line.is_empty() {
                continue;
            }
            if line.len() > MAX_INDEX_LINE_BYTES {
                return Err(AdapterError::BudgetExceeded {
                    resource: "kimi_index_line_bytes".to_string(),
                    actual: line.len() as u64,
                });
            }
            let entry = match serde_json::from_slice::<IndexEntry>(line) {
                Ok(entry) => entry,
                Err(_) => {
                    diagnostics.push(AdapterWarning {
                        kind: if complete {
                            AdapterWarningKind::UnknownField
                        } else {
                            AdapterWarningKind::TruncatedRecord
                        },
                        source_kind: "kimi_index".to_string(),
                        stage: "invalid_index_entry".to_string(),
                    })?;
                    continue;
                }
            };
            let session_dir = if safe_id(&entry.session_id) {
                resolve_index_session_dir(&sessions_root, &entry.session_dir).filter(
                    |session_dir| {
                        session_dir.starts_with(&sessions_root)
                            && session_dir.file_name().and_then(|name| name.to_str())
                                == Some(entry.session_id.as_str())
                    },
                )
            } else {
                None
            };
            let Some(session_dir) = session_dir else {
                diagnostics.push(AdapterWarning {
                    kind: AdapterWarningKind::StaleSnapshot,
                    source_kind: "kimi_index".to_string(),
                    stage: "invalid_index_locator".to_string(),
                })?;
                continue;
            };
            result.insert(entry.session_id, session_dir);
        }
        validate_root_identity(root)?;
        Ok(result)
    }
}

impl AgentAdapter for KimiCodeAdapter {
    fn id(&self) -> &'static str {
        "kimi-code"
    }

    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError> {
        let root = Self::validate_root(Path::new(&request.data_root))?;
        let sessions = root.path.join("sessions");
        if validate_directory(&sessions).is_err() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "kimi_sessions_missing".to_string(),
            });
        }
        let root_text = root.path.to_string_lossy();
        let source_id = SourceId::for_data_root(self.id(), &root_text, &self.installation_salt);
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "kimi_roots".to_string(),
            })?
            .insert(source_id.clone(), root);
        Ok(ProbeResult {
            manifests: vec![SourceManifest {
                source_id,
                agent_id: self.id().to_string(),
                display_name: "Kimi Code 0.23.3".to_string(),
                data_root_token: "selected-kimi-code-root".to_string(),
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

    fn scan(&self, mut request: ScanRequest) -> Result<ScanResult, AdapterError> {
        validate_projection_access(
            &request.projection,
            &self.schema(&request.source),
            request.access,
        )?;
        check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
        if matches!(
            request.table,
            TableName::Messages | TableName::ToolCalls | TableName::Usage
        ) {
            let root = self.root_for(&request.source)?;
            return wire::scan(root, request);
        }
        if request.table == TableName::SessionEdges {
            let root = self.root_for(&request.source)?;
            return wire::scan_edges(root, request);
        }
        if request.table != TableName::Sessions {
            return Err(AdapterError::UnsupportedFormat {
                stage: "kimi_table".to_string(),
            });
        }
        let root = self.root_for(&request.source)?;
        let diagnostics = ScanDiagnostics::default();
        let predicate_states = request
            .predicates
            .iter()
            .map(session_predicate_state)
            .collect::<Vec<_>>();
        let predicates_exact = predicate_states
            .iter()
            .all(|state| *state == PushdownState::Exact);
        let filter_predicates = if predicates_exact {
            request.predicates.clone()
        } else {
            Vec::new()
        };
        let filter_limit = request
            .limit
            .filter(|_| predicates_exact && request.order_hint.is_empty());
        let limit_state = request.limit.map(|_| {
            if filter_limit.is_some() {
                PushdownState::Exact
            } else {
                PushdownState::Unsupported
            }
        });
        let ordering_count = request.order_hint.len();
        request.limit = None;
        Ok(ScanResult {
            records: Box::new(SessionFilterStream {
                inner: SessionRecordStream {
                    root,
                    request,
                    after: None,
                    emitted: 0,
                    finished: false,
                    index: None,
                    listing: None,
                    diagnostics: diagnostics.clone(),
                    ready: std::collections::VecDeque::new(),
                },
                predicates: filter_predicates,
                limit: filter_limit,
                emitted: 0,
            }),
            pushdown: PushdownReport {
                predicates: predicate_states,
                limit: limit_state,
                ordering: vec![PushdownState::Unsupported; ordering_count],
            },
            diagnostics,
            snapshot: SnapshotReport {
                token: None,
                strength: SnapshotStrength::Weak,
                stale: false,
            },
        })
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
        if let Some(record) = self.ready.pop_front() {
            if let Err(error) = self.request.budget.charge_records(1) {
                self.finished = true;
                return Some(Err(error));
            }
            self.emitted += 1;
            return Some(Ok(CanonicalRecord::Session(record)));
        }
        if self.index.is_none() {
            match KimiCodeAdapter::read_index(&self.root, &self.request, &self.diagnostics) {
                Ok(index) => self.index = Some(index),
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
        let listing = match self.listing.as_ref() {
            Some(listing) => listing,
            None => match KimiCodeAdapter::list_session_dirs(&self.root, &self.request) {
                Ok(listing) => self.listing.insert(listing),
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            },
        };
        let next = match KimiCodeAdapter::next_session_dir(listing, self.after.as_ref()) {
            Some(next) => next,
            None => {
                if self.index.as_ref().is_some_and(|index| !index.is_empty())
                    && let Err(error) = self.diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::StaleSnapshot,
                        source_kind: "kimi_index".to_string(),
                        stage: "stale_index_entry".to_string(),
                    })
                {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.finished = true;
                return None;
            }
        };
        self.after = Some(next.1);
        let native = next.0.file_name().and_then(|name| name.to_str());
        let indexed = native.and_then(|id| self.index.as_mut().and_then(|index| index.remove(id)));
        if indexed.as_ref() != Some(&next.0)
            && let Err(error) = self.diagnostics.push(AdapterWarning {
                kind: AdapterWarningKind::StaleSnapshot,
                source_kind: "kimi_index".to_string(),
                stage: "unindexed_or_moved_session".to_string(),
            })
        {
            self.finished = true;
            return Some(Err(error));
        }
        match KimiCodeAdapter::read_state(&self.root, &next.0, &self.request) {
            Ok((mut records, bucket_verified)) => {
                if !bucket_verified
                    && let Err(error) = self.diagnostics.push(AdapterWarning {
                        kind: AdapterWarningKind::IncompleteCapability,
                        source_kind: "kimi_state".to_string(),
                        stage: "missing_workdir_authority".to_string(),
                    })
                {
                    self.finished = true;
                    return Some(Err(error));
                }
                let Some(record) = records.first().cloned() else {
                    self.finished = true;
                    return Some(Err(AdapterError::CorruptSource {
                        stage: "kimi_session_records".to_string(),
                    }));
                };
                records.remove(0);
                self.ready.extend(records);
                if let Err(error) = self.request.budget.charge_records(1) {
                    self.finished = true;
                    return Some(Err(error));
                }
                self.emitted += 1;
                Some(Ok(CanonicalRecord::Session(record)))
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

impl Iterator for SessionFilterStream {
    type Item = Result<CanonicalRecord, AdapterError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit.is_some_and(|limit| self.emitted >= limit) {
            return None;
        }
        loop {
            let record = self.inner.next()?;
            match record {
                Ok(CanonicalRecord::Session(session)) => {
                    if self
                        .predicates
                        .iter()
                        .all(|predicate| session_matches(&session, predicate))
                    {
                        self.emitted += 1;
                        return Some(Ok(CanonicalRecord::Session(session)));
                    }
                }
                Ok(_) => {
                    return Some(Err(AdapterError::Internal {
                        stage: "kimi_session_filter".to_string(),
                    }));
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

fn session_predicate_state(predicate: &Predicate) -> PushdownState {
    let exact = match predicate {
        Predicate::Eq(column, literal) => session_literal_supported(column, literal),
        Predicate::In(column, literals) => {
            !literals.is_empty()
                && literals
                    .iter()
                    .all(|literal| session_literal_supported(column, literal))
        }
        Predicate::IsNull(column) => matches!(column.as_str(), "status" | "archived"),
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| session_predicate_state(predicate) == PushdownState::Exact),
        Predicate::Range { .. } | Predicate::Unsupported(_) => false,
    };
    if exact {
        PushdownState::Exact
    } else {
        PushdownState::Unsupported
    }
}

fn session_literal_supported(column: &ColumnName, literal: &Literal) -> bool {
    matches!(
        (column.as_str(), literal),
        (
            "session_id" | "native_id" | "source_id" | "agent_id" | "status",
            Literal::Text(_)
        ) | ("archived", Literal::Bool(_))
    )
}

fn session_matches(session: &SessionRecord, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Eq(column, literal) => session_value_matches(session, column, literal),
        Predicate::In(column, literals) => literals
            .iter()
            .any(|literal| session_value_matches(session, column, literal)),
        Predicate::IsNull(column) => match column.as_str() {
            "status" => session.status.is_none(),
            "archived" => session.archived.is_none(),
            _ => false,
        },
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| session_matches(session, predicate)),
        Predicate::Range { .. } | Predicate::Unsupported(_) => true,
    }
}

fn session_value_matches(session: &SessionRecord, column: &ColumnName, literal: &Literal) -> bool {
    match (column.as_str(), literal) {
        ("session_id", Literal::Text(value)) => session.session_id.as_str() == value,
        ("native_id", Literal::Text(value)) => session.native_id.as_str() == value,
        ("source_id", Literal::Text(value)) => session.source_id.as_str() == value,
        ("agent_id", Literal::Text(value)) => &session.agent_id == value,
        ("status", Literal::Text(value)) => session.status.as_ref() == Some(value),
        ("archived", Literal::Bool(value)) => session.archived == Some(*value),
        _ => false,
    }
}

fn safe_id(value: &str) -> bool {
    !matches!(value, "." | "..")
        && !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Resolves an index locator to a session directory below `sessions_root`.
///
/// Absolute locators (the pinned on-disk form) pass through unchanged for the
/// caller's allowlist checks. Relative locators resolve against the sessions
/// root only when they consist of exactly two safe normal components
/// (`<bucket>/<session>`), so `.`/`..`, prefix, and root components can never
/// escape the allowlisted tree.
fn resolve_index_session_dir(sessions_root: &Path, locator: &str) -> Option<PathBuf> {
    let path = Path::new(locator);
    if path.is_absolute() {
        return Some(path.to_path_buf());
    }
    let components = path.components().collect::<Vec<_>>();
    let [
        std::path::Component::Normal(bucket),
        std::path::Component::Normal(session),
    ] = components.as_slice()
    else {
        return None;
    };
    let bucket = bucket.to_str()?;
    let session = session.to_str()?;
    if !safe_id(bucket) || !safe_id(session) {
        return None;
    }
    Some(sessions_root.join(bucket).join(session))
}

fn validate_root_identity(root: &RootBinding) -> Result<(), AdapterError> {
    let metadata =
        fs::symlink_metadata(&root.path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || aql_fs::directory_identity(&root.path).map_err(|_| AdapterError::SnapshotUnavailable)?
            != root.identity
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

fn validate_session_chain(root: &RootBinding, session: &Path) -> Result<(), AdapterError> {
    validate_root_identity(root)?;
    let sessions = root.path.join("sessions");
    let bucket = session.parent().ok_or(AdapterError::SnapshotUnavailable)?;
    if bucket.parent() != Some(sessions.as_path()) {
        return Err(AdapterError::SnapshotUnavailable);
    }
    validate_directory(&sessions)?;
    validate_directory(bucket)?;
    validate_directory(session)?;
    Ok(())
}

fn validate_directory(path: &Path) -> Result<(), AdapterError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

fn open_regular_no_follow(
    path: &Path,
    stage: &str,
) -> Result<(File, fs::Metadata, FileIdentity), AdapterError> {
    let before = fs::symlink_metadata(path).map_err(|_| AdapterError::NotFound {
        stage: stage.to_string(),
    })?;
    if before.file_type().is_symlink() || !before.is_file() {
        return Err(AdapterError::UnsupportedFormat {
            stage: format!("{stage}_type_or_size"),
        });
    }
    let mut options = cap_std::fs::OpenOptions::new();
    options.read(true);
    let file =
        aql_fs::open_ambient_file(path, options).map_err(|_| AdapterError::PermissionDenied {
            stage: format!("{stage}_open"),
        })?;
    let identity =
        aql_fs::identity(
            &file
                .metadata()
                .map_err(|_| AdapterError::PermissionDenied {
                    stage: format!("{stage}_metadata"),
                })?,
        );
    let file = file.into_std();
    let opened = file
        .metadata()
        .map_err(|_| AdapterError::PermissionDenied {
            stage: format!("{stage}_metadata"),
        })?;
    if !opened.is_file() {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok((file, opened, identity))
}

fn raw_string_size(
    raw: Option<&serde_json::value::RawValue>,
    maximum: u64,
) -> Result<(), AdapterError> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let encoded = raw.get().as_bytes();
    if encoded.len() < 2 || encoded.first() != Some(&b'"') || encoded.last() != Some(&b'"') {
        return Err(AdapterError::CorruptSource {
            stage: "kimi_sensitive_type".to_string(),
        });
    }
    let upper_bound = encoded.len().saturating_sub(2) as u64;
    if upper_bound > maximum {
        return Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: upper_bound,
        });
    }
    Ok(())
}

fn validate_title_size(bytes: &[u8], maximum: u64) -> Result<(), AdapterError> {
    let raw: RawTitle<'_> =
        serde_json::from_slice(bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_content".to_string(),
        })?;
    raw_string_size(raw.title, maximum)
}

fn validate_preview_size(bytes: &[u8], maximum: u64) -> Result<(), AdapterError> {
    let raw: RawPreview<'_> =
        serde_json::from_slice(bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_content".to_string(),
        })?;
    raw_string_size(raw.last_prompt, maximum)
}

fn validate_path_size(bytes: &[u8], maximum: u64) -> Result<(), AdapterError> {
    let raw: RawPath<'_> =
        serde_json::from_slice(bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_path".to_string(),
        })?;
    raw_string_size(
        raw.work_dir
            .or_else(|| raw.custom.and_then(|custom| custom.cwd)),
        maximum,
    )
}

fn validate_workdir_bucket(bytes: &[u8], session: &Path) -> Result<bool, AdapterError> {
    let raw: RawWorkdirAuthority<'_> =
        serde_json::from_slice(bytes).map_err(|_| AdapterError::CorruptSource {
            stage: "kimi_state_path".to_string(),
        })?;
    let Some(work_dir) = raw
        .work_dir
        .or_else(|| raw.custom.and_then(|custom| custom.cwd))
    else {
        return Ok(false);
    };
    let digest = hash_normalized_json_path(work_dir.get().as_bytes())?;
    let bucket = session
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .ok_or_else(|| AdapterError::CorruptSource {
            stage: "kimi_workdir_bucket".to_string(),
        })?;
    let suffix = bucket
        .rsplit_once('_')
        .map(|(_, suffix)| suffix)
        .filter(|suffix| suffix.len() == 12)
        .ok_or_else(|| AdapterError::CorruptSource {
            stage: "kimi_workdir_bucket".to_string(),
        })?;
    let expected = digest[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if !bucket.starts_with("wd_") || suffix != expected {
        return Err(AdapterError::CorruptSource {
            stage: "kimi_workdir_bucket".to_string(),
        });
    }
    Ok(true)
}

fn hash_normalized_json_path(encoded: &[u8]) -> Result<[u8; 32], AdapterError> {
    if encoded.len() < 2 || encoded.first() != Some(&b'"') || encoded.last() != Some(&b'"') {
        return Err(AdapterError::CorruptSource {
            stage: "kimi_workdir_type".to_string(),
        });
    }
    let mut state = PathHashState::default();
    let mut index = 1;
    while index + 1 < encoded.len() {
        let byte = encoded[index];
        index += 1;
        if byte != b'\\' {
            state.push(byte)?;
            continue;
        }
        let escape = *encoded
            .get(index)
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "kimi_workdir_escape".to_string(),
            })?;
        index += 1;
        match escape {
            b'"' | b'\\' | b'/' => state.push(escape)?,
            b'b' => state.push(8)?,
            b'f' => state.push(12)?,
            b'n' => state.push(b'\n')?,
            b'r' => state.push(b'\r')?,
            b't' => state.push(b'\t')?,
            b'u' => {
                let (first, next) = parse_hex_quad(encoded, index)?;
                index = next;
                let scalar = if (0xD800..=0xDBFF).contains(&first) {
                    if encoded.get(index..index + 2) != Some(b"\\u") {
                        return Err(AdapterError::CorruptSource {
                            stage: "kimi_workdir_unicode".to_string(),
                        });
                    }
                    let (second, next) = parse_hex_quad(encoded, index + 2)?;
                    index = next;
                    if !(0xDC00..=0xDFFF).contains(&second) {
                        return Err(AdapterError::CorruptSource {
                            stage: "kimi_workdir_unicode".to_string(),
                        });
                    }
                    0x10000 + (((first - 0xD800) as u32) << 10) + (second - 0xDC00) as u32
                } else {
                    first as u32
                };
                let character =
                    char::from_u32(scalar).ok_or_else(|| AdapterError::CorruptSource {
                        stage: "kimi_workdir_unicode".to_string(),
                    })?;
                let mut buffer = [0_u8; 4];
                for byte in character.encode_utf8(&mut buffer).bytes() {
                    state.push(byte)?;
                }
            }
            _ => {
                return Err(AdapterError::CorruptSource {
                    stage: "kimi_workdir_escape".to_string(),
                });
            }
        }
    }
    state.finish()
}

fn parse_hex_quad(encoded: &[u8], start: usize) -> Result<(u16, usize), AdapterError> {
    let end = start
        .checked_add(4)
        .ok_or_else(|| AdapterError::CorruptSource {
            stage: "kimi_workdir_unicode".to_string(),
        })?;
    let digits = encoded
        .get(start..end)
        .ok_or_else(|| AdapterError::CorruptSource {
            stage: "kimi_workdir_unicode".to_string(),
        })?;
    let mut value = 0_u16;
    for digit in digits {
        value = value
            .checked_mul(16)
            .and_then(|value| {
                value.checked_add(match digit {
                    b'0'..=b'9' => (digit - b'0') as u16,
                    b'a'..=b'f' => (digit - b'a' + 10) as u16,
                    b'A'..=b'F' => (digit - b'A' + 10) as u16,
                    _ => return None,
                })
            })
            .ok_or_else(|| AdapterError::CorruptSource {
                stage: "kimi_workdir_unicode".to_string(),
            })?;
    }
    Ok((value, end))
}

struct PathHashState {
    hasher: Sha256,
    bytes: usize,
    component_len: usize,
    component_all_dots: bool,
}

impl Default for PathHashState {
    fn default() -> Self {
        Self {
            hasher: Sha256::new(),
            bytes: 0,
            component_len: 0,
            component_all_dots: true,
        }
    }
}

impl PathHashState {
    fn push(&mut self, byte: u8) -> Result<(), AdapterError> {
        if self.bytes == 0 && byte != b'/' {
            return Err(AdapterError::CorruptSource {
                stage: "kimi_workdir_absolute".to_string(),
            });
        }
        if byte == 0 || byte < 0x20 {
            return Err(AdapterError::CorruptSource {
                stage: "kimi_workdir_control".to_string(),
            });
        }
        self.hasher.update([byte]);
        self.bytes += 1;
        if byte == b'/' {
            if self.bytes > 1
                && (self.component_len == 0 || (self.component_all_dots && self.component_len <= 2))
            {
                return Err(AdapterError::CorruptSource {
                    stage: "kimi_workdir_normalized".to_string(),
                });
            }
            self.component_len = 0;
            self.component_all_dots = true;
        } else {
            self.component_len += 1;
            self.component_all_dots &= byte == b'.';
        }
        Ok(())
    }

    fn finish(self) -> Result<[u8; 32], AdapterError> {
        if self.bytes == 0
            || (self.bytes > 1
                && (self.component_len == 0
                    || (self.component_all_dots && self.component_len <= 2)))
        {
            return Err(AdapterError::CorruptSource {
                stage: "kimi_workdir_normalized".to_string(),
            });
        }
        Ok(self.hasher.finalize().into())
    }
}

fn validate_agents(agents: &BTreeMap<String, SafeAgent>) -> Result<(), AdapterError> {
    let main = agents
        .get("main")
        .ok_or_else(|| AdapterError::CorruptSource {
            stage: "kimi_main_agent".to_string(),
        })?;
    if main.agent_type != "main" || main.parent_agent_id.is_some() {
        return Err(AdapterError::CorruptSource {
            stage: "kimi_main_agent".to_string(),
        });
    }
    let ids: BTreeSet<&str> = agents.keys().map(String::as_str).collect();
    for (id, agent) in agents {
        if !safe_id(id)
            || (id != "main"
                && (agent.agent_type != "sub"
                    || agent
                        .parent_agent_id
                        .as_deref()
                        .is_none_or(|parent| !ids.contains(parent))))
        {
            return Err(AdapterError::CorruptSource {
                stage: "kimi_agent_authority".to_string(),
            });
        }
    }
    Ok(())
}

fn projected(projection: &[ColumnName], name: &str) -> bool {
    projection.iter().any(|column| column.as_str() == name)
}

fn normalize_limit(request: &mut ScanRequest) -> Option<PushdownState> {
    request.limit.map(|_| {
        if request.predicates.is_empty() && request.order_hint.is_empty() {
            PushdownState::Exact
        } else {
            request.limit = None;
            PushdownState::Unsupported
        }
    })
}

fn columns() -> Vec<ColumnCapability> {
    [
        ("session_id", AccessClass::Safe),
        ("native_id", AccessClass::Safe),
        ("source_id", AccessClass::Safe),
        ("agent_id", AccessClass::Safe),
        ("title", AccessClass::Content),
        ("preview", AccessClass::Content),
        ("cwd", AccessClass::Path),
        ("project", AccessClass::Path),
        ("created_at", AccessClass::Safe),
        ("updated_at", AccessClass::Safe),
        ("archived", AccessClass::Safe),
        ("model", AccessClass::Safe),
        ("message_id", AccessClass::Safe),
        ("sequence", AccessClass::Safe),
        ("role", AccessClass::Safe),
        ("kind", AccessClass::Safe),
        ("content", AccessClass::Content),
        ("content_json", AccessClass::Content),
        ("is_error", AccessClass::Safe),
        ("tool_call_id", AccessClass::Safe),
        ("tool_name", AccessClass::Safe),
        ("namespace", AccessClass::Safe),
        ("arguments", AccessClass::ToolInput),
        ("output", AccessClass::ToolOutput),
        ("status", AccessClass::Safe),
        ("started_at", AccessClass::Safe),
        ("ended_at", AccessClass::Safe),
        ("duration_ms", AccessClass::Safe),
        ("exit_code", AccessClass::Safe),
        ("usage_id", AccessClass::Safe),
        ("provider", AccessClass::Safe),
        ("bucket_start", AccessClass::Safe),
        ("input_tokens", AccessClass::Safe),
        ("output_tokens", AccessClass::Safe),
        ("cached_tokens", AccessClass::Safe),
        ("total_tokens", AccessClass::Safe),
        ("message_count", AccessClass::Safe),
        ("tool_call_count", AccessClass::Safe),
        ("error_count", AccessClass::Safe),
        ("edge_id", AccessClass::Safe),
        ("parent_session_id", AccessClass::Safe),
        ("child_session_id", AccessClass::Safe),
        ("edge_kind", AccessClass::Safe),
        ("native_edge_id", AccessClass::Safe),
    ]
    .into_iter()
    .map(|(name, access)| ColumnCapability {
        name: ColumnName::new(name),
        access,
    })
    .collect()
}
