//! Read-only adapter for the observed Claude Code 2.x local transcript format.

#![deny(missing_docs)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use aql_adapter_api::{
    AdapterError, AdapterSchema, AgentAdapter, Capabilities, ColumnCapability, ColumnName,
    ProbeRequest, ProbeResult, PushdownReport, PushdownState, ScanDiagnostics, ScanRequest,
    ScanResult, SnapshotReport, SnapshotStrength, TableName, check_scan_state,
    util::{column_capability, normalize_limit, projected},
    validate_projection_access,
};
use aql_model::{
    CanonicalRecord, EntityId, IdentityConfidence, NativeId, SessionEdgeRecord, SessionRecord,
    SnapshotState, SourceId, SourceManifest,
};
use chrono::{DateTime, Utc};

mod cache;
mod transcript;

use cache::{ParseCache, ParseCacheHandle};
use transcript::SessionSummary;

const FORMAT: &str = "claude-code-2.x-jsonl-observed-v1";
const MAX_TRANSCRIPTS: usize = 100_000;
const MAX_TRANSCRIPT_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_RECORD_BYTES: usize = 1024 * 1024;
pub(crate) type MainTranscripts = BTreeSet<(String, String)>;
type TranscriptInventory = (Vec<TranscriptDescriptor>, MainTranscripts);

/// Read-only adapter for bounded Claude Code project transcripts.
///
/// One adapter instance serves one query (callers rebind sources per query);
/// `parse_cache` holds the single-pass parse of each transcript for that
/// lifetime only, bounded by an in-memory byte cap.
pub struct ClaudeCodeAdapter {
    installation_salt: Vec<u8>,
    roots: Mutex<BTreeMap<SourceId, RootBinding>>,
    parse_cache: ParseCacheHandle,
}

#[derive(Clone)]
pub(crate) struct RootBinding {
    path: PathBuf,
    identity: FileIdentity,
    projects: PathBuf,
    projects_identity: FileIdentity,
}

pub(crate) type FileIdentity = aql_fs::FileIdentity;

#[derive(Clone)]
pub(crate) struct TranscriptDescriptor {
    path: PathBuf,
    project: PathBuf,
    project_identity: FileIdentity,
    project_key: String,
    kind: TranscriptKind,
    identity: FileIdentity,
    len: u64,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub(crate) enum TranscriptKind {
    Main { session: String },
    Agent { agent: String },
}

#[derive(Clone)]
pub(crate) struct LogicalTranscript {
    descriptor: TranscriptDescriptor,
    main_native: String,
    logical_native: String,
    agent: Option<String>,
}

struct SessionStream {
    root: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    cache: ParseCacheHandle,
    descriptors: VecDeque<TranscriptDescriptor>,
    mains: MainTranscripts,
    emitted: u64,
    finished: bool,
}

struct EdgeStream {
    root: RootBinding,
    request: ScanRequest,
    diagnostics: ScanDiagnostics,
    descriptors: VecDeque<TranscriptDescriptor>,
    mains: MainTranscripts,
    emitted: u64,
    finished: bool,
}

impl ClaudeCodeAdapter {
    /// Creates an adapter using an installation-local salt for stable source IDs.
    #[must_use]
    pub fn new(installation_salt: impl Into<Vec<u8>>) -> Self {
        Self {
            installation_salt: installation_salt.into(),
            roots: Mutex::new(BTreeMap::new()),
            parse_cache: Arc::new(Mutex::new(ParseCache::new())),
        }
    }

    /// Creates an adapter with a test-sized parse cache limit.
    #[cfg(test)]
    pub(crate) fn new_with_parse_cache_limit(
        installation_salt: impl Into<Vec<u8>>,
        parse_cache_bytes: usize,
    ) -> Self {
        Self {
            installation_salt: installation_salt.into(),
            roots: Mutex::new(BTreeMap::new()),
            parse_cache: Arc::new(Mutex::new(ParseCache::with_limit(parse_cache_bytes))),
        }
    }

    fn validate_root(path: &Path) -> Result<RootBinding, AdapterError> {
        let metadata = fs::symlink_metadata(path).map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => AdapterError::NotFound {
                stage: "claude_root".to_string(),
            },
            _ => AdapterError::PermissionDenied {
                stage: "claude_root".to_string(),
            },
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_root_type".to_string(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o022 != 0 {
                return Err(AdapterError::PermissionDenied {
                    stage: "claude_root_permissions".to_string(),
                });
            }
        }
        let path = path.canonicalize().map_err(|_| AdapterError::NotFound {
            stage: "claude_root_canonicalize".to_string(),
        })?;
        let root_identity =
            aql_fs::directory_identity(&path).map_err(|_| AdapterError::NotFound {
                stage: "claude_root_identity".to_string(),
            })?;
        let projects = path.join("projects");
        let projects_metadata =
            fs::symlink_metadata(&projects).map_err(|_| AdapterError::UnsupportedFormat {
                stage: "claude_projects_missing".to_string(),
            })?;
        if projects_metadata.file_type().is_symlink() || !projects_metadata.is_dir() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_projects_type".to_string(),
            });
        }
        let projects_identity =
            aql_fs::directory_identity(&projects).map_err(|_| AdapterError::UnsupportedFormat {
                stage: "claude_projects_identity".to_string(),
            })?;
        Ok(RootBinding {
            path,
            identity: root_identity,
            projects,
            projects_identity,
        })
    }

    fn root_for(&self, source: &SourceManifest) -> Result<RootBinding, AdapterError> {
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "claude_roots".to_string(),
            })?
            .get(&source.source_id)
            .cloned()
            .ok_or_else(|| AdapterError::NotFound {
                stage: "claude_manifest".to_string(),
            })
    }
}

impl AgentAdapter for ClaudeCodeAdapter {
    fn id(&self) -> &'static str {
        "claude-code"
    }

    fn probe(&self, request: &ProbeRequest) -> Result<ProbeResult, AdapterError> {
        let root = Self::validate_root(Path::new(&request.data_root))?;
        let (descriptors, _) = enumerate_transcripts(&root, None)?;
        if !descriptors
            .iter()
            .any(|item| matches!(item.kind, TranscriptKind::Main { .. }))
        {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_main_transcript_missing".to_string(),
            });
        }
        let root_text = root.path.to_string_lossy();
        let source_id = SourceId::for_data_root(self.id(), &root_text, &self.installation_salt);
        self.roots
            .lock()
            .map_err(|_| AdapterError::Internal {
                stage: "claude_roots".to_string(),
            })?
            .insert(source_id.clone(), root);
        Ok(ProbeResult {
            manifests: vec![SourceManifest {
                source_id,
                agent_id: self.id().to_string(),
                display_name: "Claude Code 2.x".to_string(),
                data_root_token: "selected-claude-code-root".to_string(),
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
        let root = self.root_for(&request.source)?;
        let (descriptors, mains) = enumerate_transcripts(&root, Some(&request))?;
        let diagnostics = ScanDiagnostics::default();
        let predicate_count = request.predicates.len();
        let ordering_count = request.order_hint.len();
        let limit = normalize_limit(&mut request);
        let records = match request.table {
            TableName::Sessions => Box::new(SessionStream {
                root,
                request,
                diagnostics: diagnostics.clone(),
                cache: Arc::clone(&self.parse_cache),
                descriptors: descriptors.into(),
                mains,
                emitted: 0,
                finished: false,
            }) as aql_adapter_api::RecordStream,
            TableName::Messages | TableName::ToolCalls | TableName::Usage => transcript::scan(
                root,
                descriptors,
                mains,
                request,
                diagnostics.clone(),
                Arc::clone(&self.parse_cache),
            ),
            TableName::SessionEdges => Box::new(EdgeStream {
                root,
                request,
                diagnostics: diagnostics.clone(),
                descriptors: descriptors.into(),
                mains,
                emitted: 0,
                finished: false,
            }),
            TableName::Artifacts => {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "claude_table".to_string(),
                });
            }
        };
        Ok(ScanResult {
            records,
            pushdown: PushdownReport {
                predicates: vec![PushdownState::Unsupported; predicate_count],
                limit,
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

impl Iterator for SessionStream {
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
        let descriptor = self.descriptors.pop_front()?;
        let (logical, summary) = if wants_session_summary(&self.request.projection) {
            let loaded = match transcript::load_parsed(
                &self.root,
                descriptor,
                &self.mains,
                &self.request,
                &self.diagnostics,
                &self.cache,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            if loaded.from_cache
                && let Err(error) =
                    revalidate_transcript_path(&self.root, &loaded.logical.descriptor)
            {
                self.finished = true;
                return Some(Err(error));
            }
            let summary = mask_session_summary(&loaded.parsed.summary, &self.request.projection);
            (loaded.logical, summary)
        } else {
            let logical = match transcript::resolve_logical(
                &self.root,
                descriptor,
                &self.mains,
                &self.request,
                &self.diagnostics,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            (logical, SessionSummary::empty())
        };
        let native_id = NativeId::new(logical.logical_native.clone());
        let record = SessionRecord {
            session_id: EntityId::from_parts(
                "claude-code",
                &self.request.source.source_id,
                &native_id,
            ),
            native_id,
            source_id: self.request.source.source_id.clone(),
            agent_id: "claude-code".to_string(),
            title: None,
            preview: summary.preview,
            cwd: summary.cwd.clone(),
            project: summary.cwd,
            model: summary.model,
            provider: None,
            created_at: summary.created_at,
            updated_at: logical.descriptor.updated_at,
            status: logical.agent.as_ref().map(|_| "subagent".to_string()),
            archived: Some(false),
            message_count: summary.message_count,
            tool_call_count: summary.tool_call_count,
            tokens_used: summary.tokens_used,
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: SnapshotState::Weak,
            provenance: BTreeMap::new(),
            extensions: BTreeMap::new(),
        };
        if let Err(error) = self.request.budget.charge_records(1) {
            self.finished = true;
            return Some(Err(error));
        }
        self.emitted += 1;
        Some(Ok(CanonicalRecord::Session(record)))
    }
}

impl Iterator for EdgeStream {
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
            let descriptor = self.descriptors.pop_front()?;
            if !matches!(descriptor.kind, TranscriptKind::Agent { .. }) {
                continue;
            }
            let logical = match transcript::resolve_logical(
                &self.root,
                descriptor,
                &self.mains,
                &self.request,
                &self.diagnostics,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            let agent = logical.agent.clone().ok_or_else(|| AdapterError::Internal {
                stage: "claude_edge_agent".to_string(),
            });
            let agent = match agent {
                Ok(value) => value,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            let parent_native = NativeId::new(logical.main_native.clone());
            let child_native = NativeId::new(logical.logical_native.clone());
            let edge_native = NativeId::new(format!("{}/edge/{agent}", logical.main_native));
            let record = SessionEdgeRecord {
                edge_id: EntityId::from_parts(
                    "claude-code",
                    &self.request.source.source_id,
                    &edge_native,
                ),
                source_id: self.request.source.source_id.clone(),
                parent_session_id: EntityId::from_parts(
                    "claude-code",
                    &self.request.source.source_id,
                    &parent_native,
                ),
                child_session_id: EntityId::from_parts(
                    "claude-code",
                    &self.request.source.source_id,
                    &child_native,
                ),
                edge_kind: "subagent".to_string(),
                created_at: logical.descriptor.updated_at,
                native_edge_id: Some(edge_native),
                provenance: BTreeMap::new(),
                extensions: BTreeMap::new(),
            };
            if let Err(error) = self.request.budget.charge_records(1) {
                self.finished = true;
                return Some(Err(error));
            }
            self.emitted += 1;
            return Some(Ok(CanonicalRecord::SessionEdge(record)));
        }
    }
}

fn enumerate_transcripts(
    root: &RootBinding,
    scan_state: Option<&ScanRequest>,
) -> Result<TranscriptInventory, AdapterError> {
    validate_root_identity(root)?;
    let mut descriptors = Vec::new();
    let mut mains = BTreeSet::new();
    let mut main_ids = BTreeSet::new();
    let projects = fs::read_dir(&root.projects).map_err(|_| AdapterError::PermissionDenied {
        stage: "claude_projects_read".to_string(),
    })?;
    for project in projects {
        if let Some(request) = scan_state {
            check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
        }
        let project = project.map_err(|_| AdapterError::PermissionDenied {
            stage: "claude_project_entry".to_string(),
        })?;
        let file_type = project
            .file_type()
            .map_err(|_| AdapterError::PermissionDenied {
                stage: "claude_project_type".to_string(),
            })?;
        if file_type.is_symlink() {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_project_symlink".to_string(),
            });
        }
        if !file_type.is_dir() {
            continue;
        }
        let project_key =
            project
                .file_name()
                .into_string()
                .map_err(|_| AdapterError::UnsupportedFormat {
                    stage: "claude_project_name".to_string(),
                })?;
        if !safe_project_key(&project_key) {
            return Err(AdapterError::UnsupportedFormat {
                stage: "claude_project_name".to_string(),
            });
        }
        let project_path = project.path();
        validate_directory(&project_path)?;
        let project_identity = aql_fs::directory_identity(&project_path)
            .map_err(|_| AdapterError::SnapshotUnavailable)?;
        let files = fs::read_dir(&project_path).map_err(|_| AdapterError::PermissionDenied {
            stage: "claude_project_read".to_string(),
        })?;
        for entry in files {
            if let Some(request) = scan_state {
                check_scan_state(&request.cancellation, &request.budget, 0, 0)?;
            }
            let entry = entry.map_err(|_| AdapterError::PermissionDenied {
                stage: "claude_transcript_entry".to_string(),
            })?;
            let file_type = entry
                .file_type()
                .map_err(|_| AdapterError::PermissionDenied {
                    stage: "claude_transcript_type".to_string(),
                })?;
            let name =
                entry
                    .file_name()
                    .into_string()
                    .map_err(|_| AdapterError::UnsupportedFormat {
                        stage: "claude_transcript_name".to_string(),
                    })?;
            if !name.ends_with(".jsonl") {
                continue;
            }
            if file_type.is_symlink() || !file_type.is_file() {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "claude_transcript_type".to_string(),
                });
            }
            let stem = name.trim_end_matches(".jsonl");
            let kind = if uuid(stem) {
                if !main_ids.insert(stem.to_string()) {
                    return Err(AdapterError::CorruptSource {
                        stage: "claude_duplicate_session".to_string(),
                    });
                }
                mains.insert((project_key.clone(), stem.to_string()));
                TranscriptKind::Main {
                    session: stem.to_string(),
                }
            } else if let Some(agent) = stem.strip_prefix("agent-") {
                if !safe_component(agent, 128) {
                    return Err(AdapterError::UnsupportedFormat {
                        stage: "claude_agent_name".to_string(),
                    });
                }
                TranscriptKind::Agent {
                    agent: agent.to_string(),
                }
            } else {
                return Err(AdapterError::UnsupportedFormat {
                    stage: "claude_transcript_name".to_string(),
                });
            };
            let metadata =
                fs::symlink_metadata(entry.path()).map_err(|_| AdapterError::PermissionDenied {
                    stage: "claude_transcript_metadata".to_string(),
                })?;
            if metadata.len() > MAX_TRANSCRIPT_BYTES {
                return Err(AdapterError::BudgetExceeded {
                    resource: "claude_transcript_bytes".to_string(),
                    actual: metadata.len(),
                });
            }
            descriptors.push(TranscriptDescriptor {
                path: entry.path(),
                project: project_path.clone(),
                project_identity,
                project_key: project_key.clone(),
                kind,
                identity: aql_fs::file_identity(&entry.path())
                    .map_err(|_| AdapterError::SnapshotUnavailable)?,
                len: metadata.len(),
                updated_at: metadata.modified().ok().map(DateTime::<Utc>::from),
            });
            if descriptors.len() > MAX_TRANSCRIPTS {
                return Err(AdapterError::BudgetExceeded {
                    resource: "claude_transcript_count".to_string(),
                    actual: descriptors.len() as u64,
                });
            }
        }
    }
    descriptors.sort_by(|left, right| {
        (&left.project_key, &left.path).cmp(&(&right.project_key, &right.path))
    });
    validate_root_identity(root)?;
    Ok((descriptors, mains))
}

pub(crate) fn validate_root_identity(root: &RootBinding) -> Result<(), AdapterError> {
    let root_metadata =
        fs::symlink_metadata(&root.path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    let projects_metadata =
        fs::symlink_metadata(&root.projects).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if root_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
        || aql_fs::directory_identity(&root.path).map_err(|_| AdapterError::SnapshotUnavailable)?
            != root.identity
        || projects_metadata.file_type().is_symlink()
        || !projects_metadata.is_dir()
        || aql_fs::directory_identity(&root.projects)
            .map_err(|_| AdapterError::SnapshotUnavailable)?
            != root.projects_identity
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

pub(crate) fn validate_descriptor_chain(
    root: &RootBinding,
    descriptor: &TranscriptDescriptor,
) -> Result<(), AdapterError> {
    validate_root_identity(root)?;
    if descriptor.project.parent() != Some(root.projects.as_path())
        || descriptor.path.parent() != Some(descriptor.project.as_path())
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    validate_directory(&descriptor.project)?;
    if aql_fs::directory_identity(&descriptor.project)
        .map_err(|_| AdapterError::SnapshotUnavailable)?
        != descriptor.project_identity
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

fn validate_directory(path: &Path) -> Result<(), AdapterError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AdapterError::SnapshotUnavailable)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

pub(crate) fn open_transcript(
    root: &RootBinding,
    descriptor: &TranscriptDescriptor,
) -> Result<File, AdapterError> {
    validate_descriptor_chain(root, descriptor)?;
    let before = fs::symlink_metadata(&descriptor.path).map_err(|_| AdapterError::NotFound {
        stage: "claude_transcript".to_string(),
    })?;
    if before.file_type().is_symlink()
        || !before.is_file()
        || aql_fs::file_identity(&descriptor.path).map_err(|_| AdapterError::SnapshotUnavailable)?
            != descriptor.identity
        || before.len() < descriptor.len
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    let mut options = cap_std::fs::OpenOptions::new();
    options.read(true);
    let file = aql_fs::open_ambient_file(&descriptor.path, options).map_err(|_| {
        AdapterError::PermissionDenied {
            stage: "claude_transcript_open".to_string(),
        }
    })?;
    let opened = file
        .metadata()
        .map_err(|_| AdapterError::PermissionDenied {
            stage: "claude_transcript_metadata".to_string(),
        })?;
    if !opened.is_file()
        || aql_fs::identity(&opened) != descriptor.identity
        || opened.len() < descriptor.len
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(file.into_std())
}

pub(crate) fn revalidate_transcript(
    root: &RootBinding,
    descriptor: &TranscriptDescriptor,
    file: &File,
) -> Result<(), AdapterError> {
    let metadata = file
        .metadata()
        .map_err(|_| AdapterError::SnapshotUnavailable)?;
    if metadata.len() < descriptor.len {
        return Err(AdapterError::SnapshotUnavailable);
    }
    validate_descriptor_chain(root, descriptor)
}

/// Re-checks one transcript by path at the end of a cache replay: the same
/// identity/length watermark `open_transcript` enforces, aligned with the §9
/// scan-end check for parses that were not re-read this scan.
pub(crate) fn revalidate_transcript_path(
    root: &RootBinding,
    descriptor: &TranscriptDescriptor,
) -> Result<(), AdapterError> {
    validate_descriptor_chain(root, descriptor)?;
    let metadata = fs::symlink_metadata(&descriptor.path).map_err(|_| AdapterError::NotFound {
        stage: "claude_transcript".to_string(),
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || aql_fs::file_identity(&descriptor.path).map_err(|_| AdapterError::SnapshotUnavailable)?
            != descriptor.identity
        || metadata.len() < descriptor.len
    {
        return Err(AdapterError::SnapshotUnavailable);
    }
    Ok(())
}

/// Returns whether the projection needs any transcript-derived session
/// summary column; Safe-only identity projections skip transcript reads.
fn wants_session_summary(projection: &[ColumnName]) -> bool {
    [
        "preview",
        "cwd",
        "project",
        "model",
        "created_at",
        "message_count",
        "tool_call_count",
        "tokens_used",
    ]
    .iter()
    .any(|name| projected(projection, name))
}

/// Masks a cached summary down to the requesting projection, reproducing the
/// per-column gating the pre-cache summarize pass applied.
fn mask_session_summary(summary: &SessionSummary, projection: &[ColumnName]) -> SessionSummary {
    let wants_path = projected(projection, "cwd") || projected(projection, "project");
    SessionSummary {
        preview: projected(projection, "preview")
            .then(|| summary.preview.clone())
            .flatten(),
        cwd: wants_path.then(|| summary.cwd.clone()).flatten(),
        model: projected(projection, "model")
            .then(|| summary.model.clone())
            .flatten(),
        created_at: projected(projection, "created_at")
            .then_some(summary.created_at)
            .flatten(),
        message_count: projected(projection, "message_count")
            .then_some(summary.message_count)
            .flatten(),
        tool_call_count: projected(projection, "tool_call_count")
            .then_some(summary.tool_call_count)
            .flatten(),
        tokens_used: projected(projection, "tokens_used")
            .then_some(summary.tokens_used)
            .flatten(),
    }
}

fn safe_component(value: &str, maximum: usize) -> bool {
    !matches!(value, "." | "..")
        && !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn safe_project_key(value: &str) -> bool {
    !matches!(value, "." | "..")
        && !value.is_empty()
        && value.len() <= 1024
        && !value.chars().any(char::is_control)
}

fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

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
        "created_at",
        "updated_at",
        "archived",
        "model",
        "provider",
        "message_count",
        "tool_call_count",
        "tokens_used",
        "message_id",
        "sequence",
        "role",
        "kind",
        "content",
        "content_json",
        "is_error",
        "tool_call_id",
        "tool_name",
        "namespace",
        "arguments",
        "output",
        "status",
        "started_at",
        "ended_at",
        "duration_ms",
        "exit_code",
        "usage_id",
        "bucket_start",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "total_tokens",
        "error_count",
        "edge_id",
        "parent_session_id",
        "child_session_id",
        "edge_kind",
        "native_edge_id",
    ]
    .into_iter()
    .map(column_capability)
    .collect()
}
