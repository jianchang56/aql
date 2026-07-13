//! AQL-owned optional index contracts and storage schema.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

pub const INDEX_SCHEMA_VERSION: &str = "aql-index-v1";
pub const TOKENIZER_VERSION: &str = "unicode61-v1";

pub const CATALOG_SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS index_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS generations (
    generation_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    policy TEXT NOT NULL CHECK (policy IN ('metadata', 'content')),
    source_id TEXT NOT NULL,
    adapter_id TEXT NOT NULL,
    format_fingerprint TEXT NOT NULL,
    tokenizer_version TEXT,
    watermark_json TEXT NOT NULL,
    snapshot_strength TEXT NOT NULL,
    freshness TEXT NOT NULL CHECK (freshness IN ('fresh', 'stale', 'incompatible', 'building', 'missing')),
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    completed_at_ms INTEGER,
    file_name TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1))
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS one_active_generation
ON generations(source_id, policy) WHERE active = 1;
"#;

pub const GENERATION_SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS generation_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
CREATE TABLE IF NOT EXISTS metadata_records (
    record_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    safe_json TEXT NOT NULL,
    watermark_json TEXT NOT NULL,
    PRIMARY KEY(record_type, entity_id)
) STRICT;
CREATE TABLE IF NOT EXISTS content_documents (
    document_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id TEXT,
    document_kind TEXT NOT NULL CHECK (document_kind IN ('session_title', 'session_preview', 'message_content')),
    content TEXT NOT NULL,
    watermark_json TEXT NOT NULL
) STRICT;
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexPolicy {
    Metadata,
    Content,
}

impl fmt::Display for IndexPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Metadata => "metadata",
            Self::Content => "content",
        })
    }
}

impl FromStr for IndexPolicy {
    type Err = IndexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "metadata" => Ok(Self::Metadata),
            "content" => Ok(Self::Content),
            _ => Err(IndexError::InvalidPolicy),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexFreshness {
    Fresh,
    Stale,
    Incompatible,
    Building,
    Missing,
}

impl fmt::Display for IndexFreshness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Incompatible => "incompatible",
            Self::Building => "building",
            Self::Missing => "missing",
        })
    }
}

impl FromStr for IndexFreshness {
    type Err = IndexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fresh" => Ok(Self::Fresh),
            "stale" => Ok(Self::Stale),
            "incompatible" => Ok(Self::Incompatible),
            "building" => Ok(Self::Building),
            "missing" => Ok(Self::Missing),
            _ => Err(IndexError::InvalidMetadata),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexWatermark {
    pub schema_version: String,
    pub components: BTreeMap<String, WatermarkComponent>,
}

impl Default for IndexWatermark {
    fn default() -> Self {
        Self {
            schema_version: INDEX_SCHEMA_VERSION.to_string(),
            components: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WatermarkComponent {
    Sqlite {
        schema_fingerprint: String,
        max_updated_at_ms: Option<i64>,
        max_native_id_hmac: Option<String>,
    },
    AppendFile {
        source_identity_hmac: String,
        byte_offset: u64,
        byte_length: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexGeneration {
    pub generation_id: String,
    pub schema_version: String,
    pub policy: IndexPolicy,
    pub source_id: String,
    pub adapter_id: String,
    pub format_fingerprint: String,
    pub tokenizer_version: Option<String>,
    pub watermark: IndexWatermark,
    pub snapshot_strength: String,
    pub freshness: IndexFreshness,
    pub record_count: u64,
    pub size_bytes: u64,
    pub completed_at_ms: Option<i64>,
    pub file_name: String,
    pub active: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SearchHit {
    pub document_id: String,
    pub source_id: String,
    pub session_id: String,
    pub message_id: Option<String>,
    pub document_kind: String,
    pub rank: f64,
}

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("invalid index policy")]
    InvalidPolicy,
    #[error("unsupported index schema")]
    UnsupportedSchema,
    #[error("invalid index metadata")]
    InvalidMetadata,
    #[error("index state root overlaps the agent data root")]
    StateRootOverlap,
    #[error("index state root has unsafe permissions or type")]
    UnsafeStateRoot,
    #[error("index ownership marker is missing or invalid")]
    InvalidOwnershipMarker,
    #[error("another index writer holds the lock")]
    LockHeld,
    #[error("index state is missing")]
    Missing,
    #[error("index rebuild is required")]
    RebuildRequired,
    #[error("SQLite FTS5 is not available")]
    FtsUnavailable,
    #[error("invalid full-text search request")]
    InvalidSearch,
    #[error("index disk budget exceeded")]
    DiskBudgetExceeded,
    #[error("index state changed during the operation")]
    StateChanged,
    #[error("index generation is invalid")]
    InvalidGeneration,
    #[error("index I/O failed")]
    Io(#[from] std::io::Error),
    #[error("index platform operation failed")]
    Platform(#[from] rustix::io::Errno),
    #[error("index storage error")]
    Storage(#[from] rusqlite::Error),
}

const OWNERSHIP_MARKER: &[u8] = b"aql-index-owned-v1\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

pub struct IndexStore {
    state_root: PathBuf,
    index_root: PathBuf,
    state_directory: Arc<OwnedFd>,
    state_identity: FileIdentity,
    index_directory: Arc<OwnedFd>,
    version_directory: Arc<OwnedFd>,
    generations_directory: Arc<OwnedFd>,
}

pub struct IndexWriteLock {
    version_directory: Arc<OwnedFd>,
    lock_directory: Arc<OwnedFd>,
    file: File,
    released: bool,
}

pub struct GenerationBuilder<'lock> {
    _lock: &'lock IndexWriteLock,
    store_state_root: PathBuf,
    store_state_directory: Arc<OwnedFd>,
    store_state_identity: FileIdentity,
    version_directory: Arc<OwnedFd>,
    generations_directory: Arc<OwnedFd>,
    generation_id: String,
    temporary_name: String,
    final_name: String,
    connection: Option<Connection>,
    committed: bool,
}

impl IndexStore {
    pub fn create(state_root: &Path, data_root: &Path) -> Result<Self, IndexError> {
        let data_root = data_root.canonicalize()?;
        let prospective_state_root = prospective_private_root(state_root)?;
        if paths_overlap(&prospective_state_root, &data_root) {
            return Err(IndexError::StateRootOverlap);
        }
        let (state_root, state_directory) = open_or_create_private_root(state_root)?;
        let state_stat = rustix::fs::fstat(&state_directory)?;
        validate_private_directory(&state_stat)?;
        let state_identity = identity(&state_stat);
        let state_directory = Arc::new(state_directory);

        let index_directory = open_or_create_child_directory(&state_directory, "index")?;
        let version_directory = open_or_create_child_directory(&index_directory, "v1")?;
        ensure_ownership_marker(&version_directory)?;
        let generations_directory =
            open_or_create_child_directory(&version_directory, "generations")?;
        let index_root = state_root.join("index/v1");
        let store = Self {
            state_root,
            index_root,
            state_directory,
            state_identity,
            index_directory: Arc::new(index_directory),
            version_directory: Arc::new(version_directory),
            generations_directory: Arc::new(generations_directory),
        };
        Ok(store)
    }

    pub fn open_existing(state_root: &Path, data_root: &Path) -> Result<Self, IndexError> {
        let data_root = data_root.canonicalize()?;
        let state_root = state_root.canonicalize().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                IndexError::Missing
            } else {
                IndexError::Io(error)
            }
        })?;
        if paths_overlap(&state_root, &data_root) {
            return Err(IndexError::StateRootOverlap);
        }
        let state_directory = rustix::fs::openat(
            rustix::fs::CWD,
            &state_root,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::DIRECTORY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let state_stat = rustix::fs::fstat(&state_directory)?;
        validate_private_directory(&state_stat)?;
        let state_identity = identity(&state_stat);
        let state_directory = Arc::new(state_directory);
        let index_directory = open_child_directory(&state_directory, "index")?;
        let version_directory = open_child_directory(&index_directory, "v1")?;
        ensure_existing_ownership_marker(&version_directory)?;
        let generations_directory = open_child_directory(&version_directory, "generations")?;
        let index_root = state_root.join("index/v1");
        let catalog_path = index_root.join("catalog.sqlite");
        drop(open_catalog_read_only(&version_directory, &catalog_path)?);
        Ok(Self {
            state_root,
            index_root,
            state_directory,
            state_identity,
            index_directory: Arc::new(index_directory),
            version_directory: Arc::new(version_directory),
            generations_directory: Arc::new(generations_directory),
        })
    }

    #[must_use]
    pub fn index_root(&self) -> &Path {
        &self.index_root
    }

    pub fn acquire_write_lock(&self) -> Result<IndexWriteLock, IndexError> {
        let descriptor = rustix::fs::openat(
            &self.state_directory,
            ".aql-index-v1.lock",
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        let stat = rustix::fs::fstat(&descriptor)?;
        validate_private_file(&stat)?;
        if let Err(error) = rustix::fs::flock(
            &descriptor,
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        ) {
            if error == rustix::io::Errno::WOULDBLOCK {
                return Err(IndexError::LockHeld);
            }
            return Err(error.into());
        }
        let mut file: File = descriptor.into();
        file.set_len(0)?;
        writeln!(file, "pid={}", std::process::id())?;
        file.sync_all()?;
        let lock = IndexWriteLock {
            version_directory: self.version_directory.clone(),
            lock_directory: self.state_directory.clone(),
            file,
            released: false,
        };
        self.ensure_catalog()?;
        Ok(lock)
    }

    pub fn begin_generation<'lock>(
        &self,
        lock: &'lock IndexWriteLock,
    ) -> Result<GenerationBuilder<'lock>, IndexError> {
        if lock.released || !Arc::ptr_eq(&self.version_directory, &lock.version_directory) {
            return Err(IndexError::InvalidGeneration);
        }
        self.validate_state_identity()?;
        let generation_id = format!("{:032x}", rand::random::<u128>());
        let temporary_name = format!(".building-{generation_id}.sqlite");
        let final_name = format!("{generation_id}.sqlite");
        let descriptor = rustix::fs::openat(
            &self.generations_directory,
            temporary_name.as_str(),
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        drop(descriptor);
        let temporary_path = self.index_root.join("generations").join(&temporary_name);
        let connection = Connection::open(&temporary_path)?;
        connection.execute_batch("PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL;")?;
        initialize_generation(&connection)?;
        Ok(GenerationBuilder {
            _lock: lock,
            store_state_root: self.state_root.clone(),
            store_state_directory: self.state_directory.clone(),
            store_state_identity: self.state_identity,
            version_directory: self.version_directory.clone(),
            generations_directory: self.generations_directory.clone(),
            generation_id,
            temporary_name,
            final_name,
            connection: Some(connection),
            committed: false,
        })
    }

    pub fn active_generations(&self) -> Result<Vec<IndexGeneration>, IndexError> {
        self.validate_state_identity()?;
        let connection = open_catalog_read_only(
            &self.version_directory,
            &self.index_root.join("catalog.sqlite"),
        )?;
        let mut statement = connection.prepare(
            "SELECT generation_id, schema_version, policy, source_id, adapter_id, format_fingerprint, tokenizer_version, watermark_json, snapshot_strength, freshness, record_count, size_bytes, completed_at_ms, file_name, active FROM generations WHERE active = 1 ORDER BY source_id, policy",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, Option<i64>>(12)?,
                row.get::<_, String>(13)?,
                row.get::<_, i64>(14)?,
            ))
        })?;
        rows.map(|row| {
            let (
                generation_id,
                schema_version,
                policy,
                source_id,
                adapter_id,
                format_fingerprint,
                tokenizer_version,
                watermark_json,
                snapshot_strength,
                freshness,
                record_count,
                size_bytes,
                completed_at_ms,
                file_name,
                active,
            ) = row?;
            if schema_version != INDEX_SCHEMA_VERSION
                || record_count < 0
                || size_bytes < 0
                || !matches!(active, 0 | 1)
            {
                return Err(IndexError::InvalidMetadata);
            }
            Ok(IndexGeneration {
                generation_id,
                schema_version,
                policy: policy.parse()?,
                source_id,
                adapter_id,
                format_fingerprint,
                tokenizer_version,
                watermark: serde_json::from_str(&watermark_json)
                    .map_err(|_| IndexError::InvalidMetadata)?,
                snapshot_strength,
                freshness: freshness.parse()?,
                record_count: record_count as u64,
                size_bytes: size_bytes as u64,
                completed_at_ms,
                file_name,
                active: active == 1,
            })
        })
        .collect()
    }

    pub fn search_generation(
        &self,
        generation: &IndexGeneration,
        query: &str,
        limit: u64,
    ) -> Result<Vec<SearchHit>, IndexError> {
        if generation.policy != IndexPolicy::Content
            || generation.freshness != IndexFreshness::Fresh
            || generation.schema_version != INDEX_SCHEMA_VERSION
            || generation.tokenizer_version.as_deref() != Some(TOKENIZER_VERSION)
            || !generation.active
            || limit == 0
            || limit > 1_000
        {
            return Err(IndexError::InvalidSearch);
        }
        let fts_query = parse_search_query(query)?;
        self.validate_state_identity()?;
        validate_generation_file_name(&generation.file_name)?;
        let before = rustix::fs::statat(
            &self.generations_directory,
            generation.file_name.as_str(),
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        validate_private_file(&before)?;
        let connection = Connection::open_with_flags(
            self.index_root
                .join("generations")
                .join(&generation.file_name),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let after = rustix::fs::statat(
            &self.generations_directory,
            generation.file_name.as_str(),
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        validate_private_file(&after)?;
        if identity(&before) != identity(&after) {
            return Err(IndexError::StateChanged);
        }
        let actual_schema: String = connection.query_row(
            "SELECT value FROM generation_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )?;
        if actual_schema != INDEX_SCHEMA_VERSION {
            return Err(IndexError::UnsupportedSchema);
        }
        let mut statement = connection.prepare(
            "SELECT d.document_id, d.source_id, d.session_id, d.message_id, d.document_kind, bm25(content_fts) AS rank FROM content_fts JOIN content_documents d ON d.rowid = content_fts.rowid WHERE content_fts MATCH ?1 ORDER BY rank, d.document_id LIMIT ?2",
        )?;
        let limit = i64::try_from(limit).map_err(|_| IndexError::InvalidSearch)?;
        let rows = statement.query_map(rusqlite::params![fts_query, limit], |row| {
            Ok(SearchHit {
                document_id: row.get(0)?,
                source_id: row.get(1)?,
                session_id: row.get(2)?,
                message_id: row.get(3)?,
                document_kind: row.get(4)?,
                rank: row.get(5)?,
            })
        })?;
        let hits = rows.collect::<Result<Vec<_>, _>>()?;
        if hits.iter().any(|hit| !hit.rank.is_finite()) {
            return Err(IndexError::InvalidSearch);
        }
        Ok(hits)
    }

    pub fn clear_source(&self, source_id: &str, lock: IndexWriteLock) -> Result<u64, IndexError> {
        self.validate_state_identity()?;
        let mut connection = Connection::open(self.index_root.join("catalog.sqlite"))?;
        initialize_catalog(&connection)?;
        let file_names = {
            let mut statement = connection.prepare(
                "SELECT file_name FROM generations WHERE source_id = ?1 ORDER BY generation_id",
            )?;
            statement
                .query_map([source_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for file_name in &file_names {
            validate_generation_file_name(file_name)?;
            let stat = rustix::fs::statat(
                &self.generations_directory,
                file_name.as_str(),
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            )?;
            validate_private_file(&stat)?;
        }
        for file_name in &file_names {
            rustix::fs::unlinkat(
                &self.generations_directory,
                file_name.as_str(),
                rustix::fs::AtFlags::empty(),
            )?;
        }
        rustix::fs::fsync(&self.generations_directory)?;
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM generations WHERE source_id = ?1", [source_id])?;
        transaction.commit()?;
        rustix::fs::fsync(&self.version_directory)?;
        lock.release()?;
        Ok(file_names.len() as u64)
    }

    pub fn clear_all(self, lock: IndexWriteLock) -> Result<u64, IndexError> {
        self.validate_state_identity()?;
        let connection = open_catalog_read_only(
            &self.version_directory,
            &self.index_root.join("catalog.sqlite"),
        )?;
        let file_names = {
            let mut statement =
                connection.prepare("SELECT file_name FROM generations ORDER BY generation_id")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        drop(connection);
        let expected = file_names
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let actual = std::fs::read_dir(self.index_root.join("generations"))?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        if actual != expected {
            return Err(IndexError::InvalidGeneration);
        }
        for file_name in &file_names {
            validate_generation_file_name(file_name)?;
            let stat = rustix::fs::statat(
                &self.generations_directory,
                file_name.as_str(),
                rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
            )?;
            validate_private_file(&stat)?;
        }
        let allowed_root_entries = [
            "OWNED_BY_AQL".to_string(),
            "catalog.sqlite".to_string(),
            "generations".to_string(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
        let actual_root_entries = std::fs::read_dir(&self.index_root)?
            .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        if actual_root_entries != allowed_root_entries {
            return Err(IndexError::InvalidOwnershipMarker);
        }
        for file_name in &file_names {
            rustix::fs::unlinkat(
                &self.generations_directory,
                file_name.as_str(),
                rustix::fs::AtFlags::empty(),
            )?;
        }
        rustix::fs::fsync(&self.generations_directory)?;
        for name in ["catalog.sqlite", "OWNED_BY_AQL"] {
            rustix::fs::unlinkat(&self.version_directory, name, rustix::fs::AtFlags::empty())?;
        }
        rustix::fs::unlinkat(
            &self.version_directory,
            "generations",
            rustix::fs::AtFlags::REMOVEDIR,
        )?;
        rustix::fs::unlinkat(&self.index_directory, "v1", rustix::fs::AtFlags::REMOVEDIR)?;
        rustix::fs::unlinkat(
            &self.state_directory,
            "index",
            rustix::fs::AtFlags::REMOVEDIR,
        )?;
        rustix::fs::fsync(&self.state_directory)?;
        lock.release()?;
        Ok(file_names.len() as u64)
    }

    pub fn repair_abandoned(&self, lock: IndexWriteLock) -> Result<u64, IndexError> {
        self.validate_state_identity()?;
        let connection = open_catalog_read_only(
            &self.version_directory,
            &self.index_root.join("catalog.sqlite"),
        )?;
        let known = {
            let mut statement = connection.prepare("SELECT file_name FROM generations")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<std::collections::BTreeSet<_>, _>>()?
        };
        drop(connection);
        let mut abandoned = Vec::new();
        for entry in std::fs::read_dir(self.index_root.join("generations"))? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if known.contains(&name) {
                validate_generation_file_name(&name)?;
                let stat = rustix::fs::statat(
                    &self.generations_directory,
                    name.as_str(),
                    rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                )?;
                validate_private_file(&stat)?;
            } else if validate_abandoned_generation_name(&name) {
                let stat = rustix::fs::statat(
                    &self.generations_directory,
                    name.as_str(),
                    rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                )?;
                validate_private_file(&stat)?;
                abandoned.push(name);
            } else {
                return Err(IndexError::InvalidGeneration);
            }
        }
        for file_name in &abandoned {
            rustix::fs::unlinkat(
                &self.generations_directory,
                file_name.as_str(),
                rustix::fs::AtFlags::empty(),
            )?;
        }
        rustix::fs::fsync(&self.generations_directory)?;
        lock.release()?;
        Ok(abandoned.len() as u64)
    }

    fn ensure_catalog(&self) -> Result<(), IndexError> {
        match rustix::fs::openat(
            &self.version_directory,
            "catalog.sqlite",
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        ) {
            Ok(descriptor) => drop(descriptor),
            Err(error) if error == rustix::io::Errno::EXIST => {
                let stat = rustix::fs::statat(
                    &self.version_directory,
                    "catalog.sqlite",
                    rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
                )?;
                validate_private_file(&stat)?;
            }
            Err(error) => return Err(error.into()),
        }
        let connection = Connection::open(self.index_root.join("catalog.sqlite"))?;
        initialize_catalog(&connection)
    }

    fn validate_state_identity(&self) -> Result<(), IndexError> {
        let current = rustix::fs::statat(
            rustix::fs::CWD,
            &self.state_root,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        if identity(&current) != self.state_identity
            || identity(&rustix::fs::fstat(&self.state_directory)?) != self.state_identity
        {
            return Err(IndexError::StateChanged);
        }
        Ok(())
    }
}

impl IndexWriteLock {
    pub fn release(mut self) -> Result<(), IndexError> {
        rustix::fs::unlinkat(
            &self.lock_directory,
            ".aql-index-v1.lock",
            rustix::fs::AtFlags::empty(),
        )?;
        rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for IndexWriteLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = rustix::fs::unlinkat(
                &self.lock_directory,
                ".aql-index-v1.lock",
                rustix::fs::AtFlags::empty(),
            );
            let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
        }
    }
}

impl GenerationBuilder<'_> {
    #[must_use]
    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }

    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.final_name
    }

    pub fn connection(&mut self) -> Result<&mut Connection, IndexError> {
        self.connection
            .as_mut()
            .ok_or(IndexError::InvalidGeneration)
    }

    pub fn put_metadata_record(
        &mut self,
        record_type: &str,
        entity_id: &str,
        source_id: &str,
        safe_json: &serde_json::Value,
        watermark: &IndexWatermark,
    ) -> Result<(), IndexError> {
        if !matches!(record_type, "agent" | "session") {
            return Err(IndexError::InvalidMetadata);
        }
        self.connection()?.execute(
            "INSERT INTO metadata_records(record_type, entity_id, source_id, safe_json, watermark_json) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(record_type, entity_id) DO UPDATE SET source_id = excluded.source_id, safe_json = excluded.safe_json, watermark_json = excluded.watermark_json",
            rusqlite::params![
                record_type,
                entity_id,
                source_id,
                serde_json::to_string(safe_json).map_err(|_| IndexError::InvalidMetadata)?,
                serde_json::to_string(watermark).map_err(|_| IndexError::InvalidMetadata)?,
            ],
        )?;
        Ok(())
    }

    pub fn initialize_content_fts(&mut self) -> Result<(), IndexError> {
        self.connection()?.execute_batch(
            r#"
            CREATE VIRTUAL TABLE content_fts USING fts5(
                content,
                content = 'content_documents',
                content_rowid = 'rowid',
                tokenize = 'unicode61 remove_diacritics 2',
                prefix = '2 3 4'
            );
            CREATE TRIGGER content_documents_insert AFTER INSERT ON content_documents BEGIN
                INSERT INTO content_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
            CREATE TRIGGER content_documents_delete AFTER DELETE ON content_documents BEGIN
                INSERT INTO content_fts(content_fts, rowid, content)
                VALUES ('delete', old.rowid, old.content);
            END;
            CREATE TRIGGER content_documents_update AFTER UPDATE ON content_documents BEGIN
                INSERT INTO content_fts(content_fts, rowid, content)
                VALUES ('delete', old.rowid, old.content);
                INSERT INTO content_fts(rowid, content) VALUES (new.rowid, new.content);
            END;
            "#,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_content_document(
        &mut self,
        document_id: &str,
        source_id: &str,
        session_id: &str,
        message_id: Option<&str>,
        document_kind: &str,
        content: &str,
        watermark: &IndexWatermark,
    ) -> Result<(), IndexError> {
        if !matches!(
            document_kind,
            "session_title" | "session_preview" | "message_content"
        ) {
            return Err(IndexError::InvalidMetadata);
        }
        self.connection()?.execute(
            "INSERT INTO content_documents(document_id, source_id, session_id, message_id, document_kind, content, watermark_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(document_id) DO UPDATE SET source_id = excluded.source_id, session_id = excluded.session_id, message_id = excluded.message_id, document_kind = excluded.document_kind, content = excluded.content, watermark_json = excluded.watermark_json",
            rusqlite::params![
                document_id,
                source_id,
                session_id,
                message_id,
                document_kind,
                content,
                serde_json::to_string(watermark).map_err(|_| IndexError::InvalidMetadata)?,
            ],
        )?;
        Ok(())
    }

    pub fn commit(
        mut self,
        generation: &IndexGeneration,
        max_size_bytes: u64,
    ) -> Result<PathBuf, IndexError> {
        if generation.generation_id != self.generation_id
            || generation.schema_version != INDEX_SCHEMA_VERSION
            || generation.file_name != self.final_name
            || generation.freshness != IndexFreshness::Fresh
        {
            return Err(IndexError::InvalidGeneration);
        }
        let connection = self
            .connection
            .take()
            .ok_or(IndexError::InvalidGeneration)?;
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(IndexError::InvalidGeneration);
        }
        connection
            .close()
            .map_err(|(_, error)| IndexError::Storage(error))?;
        let descriptor = rustix::fs::openat(
            &self.generations_directory,
            self.temporary_name.as_str(),
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )?;
        let stat = rustix::fs::fstat(&descriptor)?;
        let actual_size = u64::try_from(stat.st_size).map_err(|_| IndexError::InvalidMetadata)?;
        if actual_size > max_size_bytes {
            return Err(IndexError::DiskBudgetExceeded);
        }
        let file: File = descriptor.into();
        file.sync_all()?;
        validate_root_identity(
            &self.store_state_root,
            &self.store_state_directory,
            self.store_state_identity,
        )?;
        rustix::fs::renameat_with(
            &self.generations_directory,
            self.temporary_name.as_str(),
            &self.generations_directory,
            self.final_name.as_str(),
            rustix::fs::RenameFlags::NOREPLACE,
        )?;
        rustix::fs::fsync(&self.generations_directory)?;

        let catalog_path = self.store_state_root.join("index/v1/catalog.sqlite");
        let mut catalog = Connection::open(catalog_path)?;
        initialize_catalog(&catalog)?;
        let transaction = catalog.transaction()?;
        transaction.execute(
            "UPDATE generations SET active = 0 WHERE source_id = ?1 AND policy = ?2 AND active = 1",
            rusqlite::params![generation.source_id, generation.policy.to_string()],
        )?;
        transaction.execute(
            "INSERT INTO generations(generation_id, schema_version, policy, source_id, adapter_id, format_fingerprint, tokenizer_version, watermark_json, snapshot_strength, freshness, record_count, size_bytes, completed_at_ms, file_name, active) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, 1)",
            rusqlite::params![
                generation.generation_id,
                generation.schema_version,
                generation.policy.to_string(),
                generation.source_id,
                generation.adapter_id,
                generation.format_fingerprint,
                generation.tokenizer_version,
                serde_json::to_string(&generation.watermark)
                    .map_err(|_| IndexError::InvalidMetadata)?,
                generation.snapshot_strength,
                generation.freshness.to_string(),
                i64::try_from(generation.record_count)
                    .map_err(|_| IndexError::InvalidMetadata)?,
                i64::try_from(actual_size)
                    .map_err(|_| IndexError::InvalidMetadata)?,
                generation.completed_at_ms,
                generation.file_name,
            ],
        )?;
        transaction.commit()?;
        rustix::fs::fsync(&self.version_directory)?;
        self.committed = true;
        Ok(self
            .store_state_root
            .join("index/v1/generations")
            .join(&self.final_name))
    }
}

impl Drop for GenerationBuilder<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.connection.take();
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let name = format!("{}{suffix}", self.temporary_name);
            let _ = rustix::fs::unlinkat(
                &self.generations_directory,
                name.as_str(),
                rustix::fs::AtFlags::empty(),
            );
        }
    }
}

fn open_or_create_private_root(path: &Path) -> Result<(PathBuf, OwnedFd), IndexError> {
    let normalized = prospective_private_root(path)?;
    let parent = normalized
        .parent()
        .ok_or(IndexError::UnsafeStateRoot)?
        .to_path_buf();
    let name = normalized
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(IndexError::UnsafeStateRoot)?;
    let parent_directory = rustix::fs::openat(
        rustix::fs::CWD,
        &parent,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    match rustix::fs::mkdirat(
        &parent_directory,
        name,
        rustix::fs::Mode::from_raw_mode(0o700),
    ) {
        Ok(()) => {}
        Err(error) if error == rustix::io::Errno::EXIST => {}
        Err(error) => return Err(error.into()),
    }
    let directory = rustix::fs::openat(
        &parent_directory,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    validate_private_directory(&rustix::fs::fstat(&directory)?)?;
    Ok((parent.join(name), directory))
}

fn prospective_private_root(path: &Path) -> Result<PathBuf, IndexError> {
    let parent = path
        .parent()
        .ok_or(IndexError::UnsafeStateRoot)?
        .canonicalize()?;
    let name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(IndexError::UnsafeStateRoot)?;
    Ok(parent.join(name))
}

fn open_or_create_child_directory(parent: &OwnedFd, name: &str) -> Result<OwnedFd, IndexError> {
    match rustix::fs::mkdirat(parent, name, rustix::fs::Mode::from_raw_mode(0o700)) {
        Ok(()) => {}
        Err(error) if error == rustix::io::Errno::EXIST => {}
        Err(error) => return Err(error.into()),
    }
    let directory = rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    validate_private_directory(&rustix::fs::fstat(&directory)?)?;
    Ok(directory)
}

fn open_child_directory(parent: &OwnedFd, name: &str) -> Result<OwnedFd, IndexError> {
    let directory = rustix::fs::openat(
        parent,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::NOENT {
            IndexError::Missing
        } else {
            IndexError::Platform(error)
        }
    })?;
    validate_private_directory(&rustix::fs::fstat(&directory)?)?;
    Ok(directory)
}

fn ensure_ownership_marker(directory: &OwnedFd) -> Result<(), IndexError> {
    match rustix::fs::openat(
        directory,
        "OWNED_BY_AQL",
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(0o600),
    ) {
        Ok(descriptor) => {
            let mut file: File = descriptor.into();
            file.write_all(OWNERSHIP_MARKER)?;
            file.sync_all()?;
        }
        Err(error) if error == rustix::io::Errno::EXIST => {
            let descriptor = rustix::fs::openat(
                directory,
                "OWNED_BY_AQL",
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )?;
            validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
            let mut file: File = descriptor.into();
            let mut marker = Vec::new();
            file.read_to_end(&mut marker)?;
            if marker != OWNERSHIP_MARKER {
                return Err(IndexError::InvalidOwnershipMarker);
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn ensure_existing_ownership_marker(directory: &OwnedFd) -> Result<(), IndexError> {
    let descriptor = rustix::fs::openat(
        directory,
        "OWNED_BY_AQL",
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::NOENT {
            IndexError::InvalidOwnershipMarker
        } else {
            IndexError::Platform(error)
        }
    })?;
    validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
    let mut file: File = descriptor.into();
    let mut marker = Vec::new();
    file.read_to_end(&mut marker)?;
    if marker != OWNERSHIP_MARKER {
        return Err(IndexError::InvalidOwnershipMarker);
    }
    Ok(())
}

fn open_catalog_read_only(
    version_directory: &OwnedFd,
    path: &Path,
) -> Result<Connection, IndexError> {
    let before = rustix::fs::statat(
        version_directory,
        "catalog.sqlite",
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
    )
    .map_err(|error| {
        if error == rustix::io::Errno::NOENT {
            IndexError::Missing
        } else {
            IndexError::Platform(error)
        }
    })?;
    validate_private_file(&before)?;
    let connection = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let after = rustix::fs::statat(
        version_directory,
        "catalog.sqlite",
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
    )?;
    validate_private_file(&after)?;
    if identity(&before) != identity(&after) {
        return Err(IndexError::StateChanged);
    }
    let actual: String = connection.query_row(
        "SELECT value FROM index_meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )?;
    if actual != INDEX_SCHEMA_VERSION {
        return Err(IndexError::UnsupportedSchema);
    }
    Ok(connection)
}

fn validate_private_directory(stat: &rustix::fs::Stat) -> Result<(), IndexError> {
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::Directory
        || stat.st_mode & 0o077 != 0
    {
        return Err(IndexError::UnsafeStateRoot);
    }
    Ok(())
}

fn validate_private_file(stat: &rustix::fs::Stat) -> Result<(), IndexError> {
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile
        || stat.st_mode & 0o077 != 0
    {
        return Err(IndexError::UnsafeStateRoot);
    }
    Ok(())
}

fn validate_root_identity(
    path: &Path,
    descriptor: &OwnedFd,
    expected: FileIdentity,
) -> Result<(), IndexError> {
    let current = rustix::fs::statat(rustix::fs::CWD, path, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
    if identity(&current) != expected || identity(&rustix::fs::fstat(descriptor)?) != expected {
        return Err(IndexError::StateChanged);
    }
    Ok(())
}

fn identity(stat: &rustix::fs::Stat) -> FileIdentity {
    FileIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino,
    }
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn parse_search_query(query: &str) -> Result<String, IndexError> {
    if query.is_empty() || query.len() > 4_096 || query.chars().any(char::is_control) {
        return Err(IndexError::InvalidSearch);
    }
    let characters = query.chars().collect::<Vec<_>>();
    let mut position = 0;
    let mut atoms = Vec::new();
    while position < characters.len() {
        while position < characters.len() && characters[position].is_whitespace() {
            position += 1;
        }
        if position == characters.len() {
            break;
        }
        if characters[position] == '"' {
            position += 1;
            let start = position;
            while position < characters.len() && characters[position] != '"' {
                if !(characters[position].is_alphanumeric()
                    || characters[position] == '_'
                    || characters[position].is_whitespace())
                {
                    return Err(IndexError::InvalidSearch);
                }
                position += 1;
            }
            if position == characters.len() || position == start {
                return Err(IndexError::InvalidSearch);
            }
            let phrase = characters[start..position].iter().collect::<String>();
            let words = phrase.split_whitespace().collect::<Vec<_>>();
            if words.is_empty() {
                return Err(IndexError::InvalidSearch);
            }
            atoms.push(format!("\"{}\"", words.join(" ")));
            position += 1;
        } else {
            let start = position;
            while position < characters.len()
                && (characters[position].is_alphanumeric() || characters[position] == '_')
            {
                position += 1;
            }
            if position == start {
                return Err(IndexError::InvalidSearch);
            }
            let word = characters[start..position].iter().collect::<String>();
            if matches!(
                word.to_ascii_uppercase().as_str(),
                "AND" | "OR" | "NOT" | "NEAR"
            ) {
                return Err(IndexError::InvalidSearch);
            }
            let prefix = position < characters.len() && characters[position] == '*';
            if prefix {
                position += 1;
            }
            atoms.push(if prefix {
                format!("\"{word}\"*")
            } else {
                format!("\"{word}\"")
            });
        }
        if position < characters.len() && !characters[position].is_whitespace() {
            return Err(IndexError::InvalidSearch);
        }
    }
    if atoms.is_empty() || atoms.len() > 32 {
        return Err(IndexError::InvalidSearch);
    }
    Ok(atoms.join(" "))
}

fn validate_generation_file_name(file_name: &str) -> Result<(), IndexError> {
    let Some(generation_id) = file_name.strip_suffix(".sqlite") else {
        return Err(IndexError::InvalidGeneration);
    };
    if generation_id.len() != 32
        || !generation_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(IndexError::InvalidGeneration);
    }
    Ok(())
}

fn validate_abandoned_generation_name(file_name: &str) -> bool {
    let base = ["-journal", "-wal", "-shm", ""]
        .into_iter()
        .find_map(|suffix| file_name.strip_suffix(suffix))
        .unwrap_or(file_name);
    let Some(generation_id) = base
        .strip_prefix(".building-")
        .and_then(|name| name.strip_suffix(".sqlite"))
    else {
        return false;
    };
    generation_id.len() == 32
        && generation_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub fn initialize_catalog(connection: &Connection) -> Result<(), IndexError> {
    connection.execute_batch(CATALOG_SCHEMA_SQL)?;
    connection.execute(
        "INSERT INTO index_meta(key, value) VALUES ('schema_version', ?1) ON CONFLICT(key) DO NOTHING",
        [INDEX_SCHEMA_VERSION],
    )?;
    let actual: String = connection.query_row(
        "SELECT value FROM index_meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )?;
    if actual != INDEX_SCHEMA_VERSION {
        return Err(IndexError::UnsupportedSchema);
    }
    Ok(())
}

pub fn initialize_generation(connection: &Connection) -> Result<(), IndexError> {
    connection.execute_batch(GENERATION_SCHEMA_SQL)?;
    connection.execute(
        "INSERT INTO generation_meta(key, value) VALUES ('schema_version', ?1) ON CONFLICT(key) DO NOTHING",
        [INDEX_SCHEMA_VERSION],
    )?;
    let actual: String = connection.query_row(
        "SELECT value FROM generation_meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )?;
    if actual != INDEX_SCHEMA_VERSION {
        return Err(IndexError::UnsupportedSchema);
    }
    Ok(())
}

pub fn require_fts5() -> Result<(), IndexError> {
    let connection = Connection::open_in_memory()?;
    connection
        .execute(
            "CREATE VIRTUAL TABLE fts5_capability_probe USING fts5(content)",
            [],
        )
        .map_err(|_| IndexError::FtsUnavailable)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn test_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is available")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("aql-index-{label}-{}-{nonce}", std::process::id()));
        fs::create_dir(&root).expect("test root is created");
        root
    }

    fn publish_generation(
        store: &IndexStore,
        lock: &IndexWriteLock,
        source_id: &str,
        entity_id: &str,
    ) -> IndexGeneration {
        let mut builder = store.begin_generation(lock).expect("generation starts");
        builder
            .put_metadata_record(
                "session",
                entity_id,
                source_id,
                &serde_json::json!({"session_id": entity_id}),
                &IndexWatermark::default(),
            )
            .expect("metadata record inserts");
        let generation = IndexGeneration {
            generation_id: builder.generation_id().to_string(),
            schema_version: INDEX_SCHEMA_VERSION.to_string(),
            policy: IndexPolicy::Metadata,
            source_id: source_id.to_string(),
            adapter_id: "synthetic".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            tokenizer_version: None,
            watermark: IndexWatermark::default(),
            snapshot_strength: "weak".to_string(),
            freshness: IndexFreshness::Fresh,
            record_count: 1,
            size_bytes: 0,
            completed_at_ms: Some(7),
            file_name: builder.file_name().to_string(),
            active: true,
        };
        builder
            .commit(&generation, u64::MAX)
            .expect("generation publishes");
        generation
    }

    #[test]
    fn policies_and_watermarks_have_stable_serialization() {
        assert_eq!(IndexPolicy::Metadata.to_string(), "metadata");
        assert_eq!(
            "content".parse::<IndexPolicy>().unwrap(),
            IndexPolicy::Content
        );
        assert!("secret".parse::<IndexPolicy>().is_err());

        let mut watermark = IndexWatermark::default();
        watermark.components.insert(
            "state".to_string(),
            WatermarkComponent::Sqlite {
                schema_fingerprint: "synthetic-schema".to_string(),
                max_updated_at_ms: Some(7),
                max_native_id_hmac: Some("synthetic-hmac".to_string()),
            },
        );
        let encoded = serde_json::to_string(&watermark).unwrap();
        let decoded: IndexWatermark = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, watermark);
    }

    #[test]
    fn schemas_initialize_and_reject_future_versions() {
        let catalog = Connection::open_in_memory().unwrap();
        initialize_catalog(&catalog).unwrap();
        assert_eq!(
            catalog
                .query_row("SELECT COUNT(*) FROM generations", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        catalog
            .execute(
                "UPDATE index_meta SET value = 'future-index-v2' WHERE key = 'schema_version'",
                [],
            )
            .unwrap();
        assert!(matches!(
            initialize_catalog(&catalog),
            Err(IndexError::UnsupportedSchema)
        ));

        let generation = Connection::open_in_memory().unwrap();
        initialize_generation(&generation).unwrap();
        assert_eq!(
            generation
                .query_row("SELECT COUNT(*) FROM metadata_records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn content_fts_uses_fixed_unicode_phrase_and_prefix_behavior() {
        require_fts5().expect("bundled SQLite provides FTS5");
        let root = test_root("content-fts");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let lock = store.acquire_write_lock().expect("writer locks");
        let mut builder = store.begin_generation(&lock).expect("generation starts");
        builder
            .initialize_content_fts()
            .expect("content FTS initializes");
        builder
            .put_content_document(
                "document-one",
                "source-one",
                "session-one",
                Some("message-one"),
                "message_content",
                "你好世界 cafe coding agent",
                &IndexWatermark::default(),
            )
            .expect("content document inserts");
        assert!(matches!(
            builder.put_content_document(
                "document-two",
                "source-one",
                "session-one",
                None,
                "tool_output",
                "forbidden",
                &IndexWatermark::default(),
            ),
            Err(IndexError::InvalidMetadata)
        ));
        for query in ["你好世界", "cafe", "cod*", "\"coding agent\""] {
            let count = builder
                .connection()
                .expect("generation connection exists")
                .query_row(
                    "SELECT COUNT(*) FROM content_fts WHERE content_fts MATCH ?1",
                    [query],
                    |row| row.get::<_, i64>(0),
                )
                .expect("bound FTS query succeeds");
            assert_eq!(count, 1, "query must match: {query}");
        }
        let generation = IndexGeneration {
            generation_id: builder.generation_id().to_string(),
            schema_version: INDEX_SCHEMA_VERSION.to_string(),
            policy: IndexPolicy::Content,
            source_id: "source-one".to_string(),
            adapter_id: "synthetic".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            tokenizer_version: Some(TOKENIZER_VERSION.to_string()),
            watermark: IndexWatermark::default(),
            snapshot_strength: "weak".to_string(),
            freshness: IndexFreshness::Fresh,
            record_count: 1,
            size_bytes: 0,
            completed_at_ms: Some(7),
            file_name: builder.file_name().to_string(),
            active: true,
        };
        builder
            .commit(&generation, u64::MAX)
            .expect("content generation publishes");
        for query in ["你好世界", "cafe", "cod*", "\"coding agent\""] {
            let hits = store
                .search_generation(&generation, query, 10)
                .expect("validated search succeeds");
            assert_eq!(hits.len(), 1, "query must return one hit: {query}");
            assert_eq!(hits[0].document_kind, "message_content");
        }
        for invalid in ["", "*", "hello OR world", "field:value", "\"unterminated"] {
            assert!(matches!(
                store.search_generation(&generation, invalid, 10),
                Err(IndexError::InvalidSearch)
            ));
        }
        lock.release().expect("writer lock releases");
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn private_store_rejects_overlap_symlinks_and_lock_contention() {
        let root = test_root("private");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let overlapping = data_root.join("state");
        assert!(matches!(
            IndexStore::create(&overlapping, &data_root),
            Err(IndexError::StateRootOverlap)
        ));
        assert!(!overlapping.exists());

        let real_state = root.join("real-state");
        fs::create_dir(&real_state).expect("real state root is created");
        fs::set_permissions(&real_state, fs::Permissions::from_mode(0o700))
            .expect("private permissions are set");
        let linked_state = root.join("linked-state");
        symlink(&real_state, &linked_state).expect("state symlink is created");
        assert!(IndexStore::create(&linked_state, &data_root).is_err());

        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("private store is created");
        assert_eq!(
            fs::metadata(&state_root)
                .expect("state metadata exists")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(store.index_root().join("OWNED_BY_AQL"))
                .expect("ownership marker exists")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let lock = store.acquire_write_lock().expect("first writer locks");
        assert!(matches!(
            store.acquire_write_lock(),
            Err(IndexError::LockHeld)
        ));
        lock.release().expect("lock releases");
        assert!(!state_root.join(".aql-index-v1.lock").exists());
        fs::write(state_root.join(".aql-index-v1.lock"), b"stale-pid-metadata")
            .expect("stale lock file is created");
        fs::set_permissions(
            state_root.join(".aql-index-v1.lock"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("stale lock remains private");
        store
            .acquire_write_lock()
            .expect("unheld stale lock is recovered without PID guessing")
            .release()
            .expect("recovered lock releases");
        assert!(!state_root.join(".aql-index-v1.lock").exists());
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn generations_publish_atomically_and_abandoned_builds_are_removed() {
        let root = test_root("generation");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let lock = store.acquire_write_lock().expect("writer locks");

        let abandoned = store
            .begin_generation(&lock)
            .expect("building generation starts");
        let abandoned_name = abandoned.temporary_name.clone();
        drop(abandoned);
        assert!(
            !store
                .index_root()
                .join("generations")
                .join(abandoned_name)
                .exists()
        );
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _builder = store
                .begin_generation(&lock)
                .expect("panic generation starts");
            panic!("synthetic unwind");
        }));
        assert!(unwind.is_err());
        assert!(
            fs::read_dir(store.index_root().join("generations"))
                .expect("generation directory lists")
                .all(|entry| !entry
                    .expect("generation entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .contains("building"))
        );

        let mut builder = store.begin_generation(&lock).expect("generation starts");
        builder
            .connection()
            .expect("generation connection exists")
            .execute(
                "INSERT INTO metadata_records(record_type, entity_id, source_id, safe_json, watermark_json) VALUES ('session', 'synthetic-session', 'synthetic-source', '{}', '{}')",
                [],
            )
            .expect("synthetic metadata inserts");
        let generation = IndexGeneration {
            generation_id: builder.generation_id.clone(),
            schema_version: INDEX_SCHEMA_VERSION.to_string(),
            policy: IndexPolicy::Metadata,
            source_id: "synthetic-source".to_string(),
            adapter_id: "synthetic".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            tokenizer_version: None,
            watermark: IndexWatermark::default(),
            snapshot_strength: "weak".to_string(),
            freshness: IndexFreshness::Fresh,
            record_count: 1,
            size_bytes: 0,
            completed_at_ms: Some(7),
            file_name: builder.final_name.clone(),
            active: true,
        };
        let published = builder
            .commit(&generation, u64::MAX)
            .expect("generation publishes");
        assert!(published.is_file());
        assert_eq!(
            Connection::open(store.index_root().join("catalog.sqlite"))
                .expect("catalog opens")
                .query_row(
                    "SELECT COUNT(*) FROM generations WHERE active = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("active count is readable"),
            1
        );
        assert_eq!(
            fs::read_dir(store.index_root().join("generations"))
                .expect("generation directory lists")
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains("building"))
                .count(),
            0
        );
        let mut over_budget = store
            .begin_generation(&lock)
            .expect("over-budget generation starts");
        over_budget
            .put_metadata_record(
                "session",
                "over-budget-session",
                "synthetic-source",
                &serde_json::json!({"safe": true}),
                &IndexWatermark::default(),
            )
            .expect("over-budget record writes");
        let over_budget_generation = IndexGeneration {
            generation_id: over_budget.generation_id().to_string(),
            file_name: over_budget.file_name().to_string(),
            ..generation.clone()
        };
        assert!(matches!(
            over_budget.commit(&over_budget_generation, 1),
            Err(IndexError::DiskBudgetExceeded)
        ));
        let active = store.active_generations().expect("active generation reads");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].generation_id, generation.generation_id);
        assert!(
            fs::read_dir(store.index_root().join("generations"))
                .expect("generation directory lists")
                .all(|entry| !entry
                    .expect("generation entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .contains("building"))
        );
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn parent_replacement_prevents_generation_publish() {
        let root = test_root("replacement");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let lock = store.acquire_write_lock().expect("writer locks");
        let builder = store.begin_generation(&lock).expect("generation starts");
        let generation = IndexGeneration {
            generation_id: builder.generation_id.clone(),
            schema_version: INDEX_SCHEMA_VERSION.to_string(),
            policy: IndexPolicy::Metadata,
            source_id: "synthetic-source".to_string(),
            adapter_id: "synthetic".to_string(),
            format_fingerprint: "synthetic-v1".to_string(),
            tokenizer_version: None,
            watermark: IndexWatermark::default(),
            snapshot_strength: "weak".to_string(),
            freshness: IndexFreshness::Fresh,
            record_count: 0,
            size_bytes: 0,
            completed_at_ms: Some(7),
            file_name: builder.final_name.clone(),
            active: true,
        };
        let moved = root.join("moved-state");
        fs::rename(&state_root, &moved).expect("state root moves");
        fs::create_dir(&state_root).expect("replacement state root is created");
        assert!(matches!(
            builder.commit(&generation, u64::MAX),
            Err(IndexError::StateChanged)
        ));
        assert!(
            fs::read_dir(moved.join("index/v1/generations"))
                .expect("original generations list")
                .all(|entry| !entry
                    .expect("generation entry is readable")
                    .file_name()
                    .to_string_lossy()
                    .contains("building"))
        );
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn clear_source_removes_only_the_requested_source() {
        let root = test_root("clear-source");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let build_lock = store.acquire_write_lock().expect("writer locks");
        let first = publish_generation(&store, &build_lock, "source-one", "session-one");
        let second = publish_generation(&store, &build_lock, "source-two", "session-two");
        build_lock.release().expect("build lock releases");

        let lock = store.acquire_write_lock().expect("writer locks");
        assert_eq!(
            store
                .clear_source("source-one", lock)
                .expect("source clears"),
            1
        );
        assert!(!state_root.join(".aql-index-v1.lock").exists());
        assert!(
            !store
                .index_root()
                .join("generations")
                .join(first.file_name)
                .exists()
        );
        assert!(
            store
                .index_root()
                .join("generations")
                .join(second.file_name)
                .is_file()
        );
        let active = store.active_generations().expect("active generations read");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].source_id, "source-two");
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn clear_all_removes_only_the_owned_index_tree() {
        let root = test_root("clear-all");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        fs::create_dir(&state_root).expect("state root is created");
        fs::set_permissions(&state_root, fs::Permissions::from_mode(0o700))
            .expect("state root is private");
        fs::write(state_root.join("installation.key"), b"synthetic-key")
            .expect("installation key is created");
        fs::set_permissions(
            state_root.join("installation.key"),
            fs::Permissions::from_mode(0o600),
        )
        .expect("installation key is private");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let build_lock = store.acquire_write_lock().expect("writer locks");
        publish_generation(&store, &build_lock, "source-one", "session-one");
        publish_generation(&store, &build_lock, "source-two", "session-two");
        build_lock.release().expect("build lock releases");

        let lock = store.acquire_write_lock().expect("writer locks");
        assert_eq!(store.clear_all(lock).expect("all indexes clear"), 2);
        assert!(!state_root.join("index").exists());
        assert!(state_root.join("installation.key").is_file());
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn clear_all_fails_closed_on_unknown_files_and_releases_the_lock() {
        let root = test_root("clear-unknown");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let build_lock = store.acquire_write_lock().expect("writer locks");
        let generation = publish_generation(&store, &build_lock, "source-one", "session-one");
        build_lock.release().expect("build lock releases");
        fs::write(
            store.index_root().join("generations/unknown-file"),
            b"do not delete",
        )
        .expect("unknown file is created");

        let lock = store.acquire_write_lock().expect("writer locks");
        assert!(matches!(
            store.clear_all(lock),
            Err(IndexError::InvalidGeneration)
        ));
        assert!(!state_root.join(".aql-index-v1.lock").exists());
        assert!(
            state_root
                .join("index/v1/generations")
                .join(generation.file_name)
                .is_file()
        );
        assert!(
            state_root
                .join("index/v1/generations/unknown-file")
                .is_file()
        );
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn repair_removes_only_valid_abandoned_generation_files() {
        let root = test_root("repair");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let build_lock = store.acquire_write_lock().expect("writer locks");
        let generation = publish_generation(&store, &build_lock, "source-one", "session-one");
        build_lock.release().expect("build lock releases");
        let abandoned = store
            .index_root()
            .join("generations/.building-0123456789abcdef0123456789abcdef.sqlite");
        fs::write(&abandoned, b"synthetic abandoned generation")
            .expect("abandoned generation is created");
        fs::set_permissions(&abandoned, fs::Permissions::from_mode(0o600))
            .expect("abandoned generation is private");

        let lock = store.acquire_write_lock().expect("writer locks");
        assert_eq!(
            store
                .repair_abandoned(lock)
                .expect("abandoned generation repairs"),
            1
        );
        assert!(!abandoned.exists());
        assert!(
            store
                .index_root()
                .join("generations")
                .join(generation.file_name)
                .is_file()
        );
        assert!(!state_root.join(".aql-index-v1.lock").exists());
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn clear_rejects_symlink_generations_and_invalid_ownership_markers() {
        let root = test_root("clear-symlink");
        let data_root = root.join("data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = IndexStore::create(&state_root, &data_root).expect("store is created");
        let build_lock = store.acquire_write_lock().expect("writer locks");
        let generation = publish_generation(&store, &build_lock, "source-one", "session-one");
        build_lock.release().expect("build lock releases");
        let generation_path = store
            .index_root()
            .join("generations")
            .join(&generation.file_name);
        fs::remove_file(&generation_path).expect("generation file is removed");
        symlink(&data_root, &generation_path).expect("generation symlink is created");

        let lock = store.acquire_write_lock().expect("writer locks");
        assert!(store.clear_source("source-one", lock).is_err());
        assert!(!state_root.join(".aql-index-v1.lock").exists());
        assert!(
            fs::symlink_metadata(&generation_path)
                .expect("generation symlink remains")
                .file_type()
                .is_symlink()
        );
        drop(store);

        fs::write(state_root.join("index/v1/OWNED_BY_AQL"), b"not-owned")
            .expect("ownership marker is replaced");
        assert!(matches!(
            IndexStore::open_existing(&state_root, &data_root),
            Err(IndexError::InvalidOwnershipMarker)
        ));
        fs::remove_file(state_root.join("index/v1/OWNED_BY_AQL"))
            .expect("invalid marker is removed");
        assert!(IndexStore::open_existing(&state_root, &data_root).is_err());
        fs::remove_dir_all(root).expect("test root is removed");
    }
}
