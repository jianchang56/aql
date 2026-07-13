use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    ACTION_AUDIT_SCHEMA_VERSION, ACTION_STORE_SCHEMA_VERSION, ActionError, ActionPlan, ActionState,
    AuditRecord, SanitizedResultCode, UnsignedAuditRecord,
};

const OWNERSHIP_MARKER: &[u8] = b"aql-actions-owned-v1\n";
const MAX_PLAN_BYTES: u64 = 64 * 1024;
const MAX_AUDIT_RECORD_BYTES: usize = 64 * 1024;
const MAX_AUDIT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_AUDIT_RECORDS: usize = 100_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredActionPlan {
    pub storage_schema_version: String,
    pub plan: ActionPlan,
}

pub struct ActionStore {
    state_root: PathBuf,
    actions_root: PathBuf,
    state_directory: Arc<OwnedFd>,
    state_identity: FileIdentity,
    version_directory: Arc<OwnedFd>,
    plans_directory: Arc<OwnedFd>,
}

pub struct ActionWriteLock {
    version_directory: Arc<OwnedFd>,
    lock_directory: Arc<OwnedFd>,
    file: File,
    released: bool,
}

struct AtomicPlanFile {
    directory: Arc<OwnedFd>,
    temporary_name: String,
    file: File,
    committed: bool,
}

impl ActionStore {
    pub fn create(state_root: &Path, data_root: &Path) -> Result<Self, ActionError> {
        let data_root = data_root.canonicalize()?;
        let prospective = prospective_private_root(state_root)?;
        if paths_overlap(&prospective, &data_root) {
            return Err(ActionError::StateRootOverlap);
        }
        let (state_root, state_directory) = open_or_create_private_root(state_root)?;
        let state_stat = rustix::fs::fstat(&state_directory)?;
        validate_private_directory(&state_stat)?;
        let state_identity = identity(&state_stat);
        let state_directory = Arc::new(state_directory);
        let actions_directory = open_or_create_child_directory(&state_directory, "actions")?;
        let version_directory = open_or_create_child_directory(&actions_directory, "v1")?;
        ensure_ownership_marker(&version_directory)?;
        let plans_directory = open_or_create_child_directory(&version_directory, "plans")?;
        let store = Self {
            actions_root: state_root.join("actions/v1"),
            state_root,
            state_directory,
            state_identity,
            version_directory: Arc::new(version_directory),
            plans_directory: Arc::new(plans_directory),
        };
        store.validate_layout()?;
        Ok(store)
    }

    pub fn open_existing(state_root: &Path, data_root: &Path) -> Result<Self, ActionError> {
        let data_root = data_root.canonicalize()?;
        let state_root = state_root.canonicalize().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ActionError::MissingState
            } else {
                ActionError::Io(error)
            }
        })?;
        if paths_overlap(&state_root, &data_root) {
            return Err(ActionError::StateRootOverlap);
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
        let actions_directory = open_child_directory(&state_directory, "actions")?;
        let version_directory = open_child_directory(&actions_directory, "v1")?;
        ensure_existing_ownership_marker(&version_directory)?;
        let plans_directory = open_child_directory(&version_directory, "plans")?;
        let store = Self {
            actions_root: state_root.join("actions/v1"),
            state_root,
            state_directory,
            state_identity,
            version_directory: Arc::new(version_directory),
            plans_directory: Arc::new(plans_directory),
        };
        store.validate_layout()?;
        Ok(store)
    }

    #[must_use]
    pub fn actions_root(&self) -> &Path {
        &self.actions_root
    }

    pub fn acquire_write_lock(&self) -> Result<ActionWriteLock, ActionError> {
        self.validate_state_identity()?;
        let descriptor = rustix::fs::openat(
            &self.state_directory,
            ".aql-actions-v1.lock",
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
        if let Err(error) = rustix::fs::flock(
            &descriptor,
            rustix::fs::FlockOperation::NonBlockingLockExclusive,
        ) {
            if error == rustix::io::Errno::WOULDBLOCK {
                return Err(ActionError::LockHeld);
            }
            return Err(error.into());
        }
        let mut file: File = descriptor.into();
        file.set_len(0)?;
        writeln!(file, "pid={}", std::process::id())?;
        file.sync_all()?;
        Ok(ActionWriteLock {
            version_directory: self.version_directory.clone(),
            lock_directory: self.state_directory.clone(),
            file,
            released: false,
        })
    }

    pub fn publish_plan(
        &self,
        lock: &ActionWriteLock,
        plan: ActionPlan,
        now_ms: i64,
        signing_key: &[u8],
    ) -> Result<PathBuf, ActionError> {
        self.validate_lock(lock)?;
        plan.verify(signing_key, now_ms)?;
        let stored = StoredActionPlan {
            storage_schema_version: ACTION_STORE_SCHEMA_VERSION.to_string(),
            plan,
        };
        let bytes = serde_json::to_vec(&stored).map_err(|_| ActionError::InvalidStoredPlan)?;
        if bytes.len() as u64 > MAX_PLAN_BYTES {
            return Err(ActionError::InvalidStoredPlan);
        }
        let final_name = plan_file_name(&stored.plan.unsigned.action_id)?;
        let mut output = AtomicPlanFile::create(self.plans_directory.clone())?;
        output.file.write_all(&bytes)?;
        self.validate_state_identity()?;
        output.commit(&final_name)?;
        self.validate_state_identity()?;
        Ok(self.actions_root.join("plans").join(final_name))
    }

    pub fn load_plan(&self, action_id: &str) -> Result<StoredActionPlan, ActionError> {
        self.validate_state_identity()?;
        let file_name = plan_file_name(action_id)?;
        let descriptor = rustix::fs::openat(
            &self.plans_directory,
            file_name.as_str(),
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|error| {
            if error == rustix::io::Errno::NOENT {
                ActionError::MissingState
            } else {
                ActionError::Platform(error)
            }
        })?;
        let stat = rustix::fs::fstat(&descriptor)?;
        validate_private_file(&stat)?;
        if stat.st_size < 0 || stat.st_size as u64 > MAX_PLAN_BYTES {
            return Err(ActionError::InvalidStoredPlan);
        }
        let mut file: File = descriptor.into();
        let mut bytes = Vec::with_capacity(stat.st_size as usize);
        file.read_to_end(&mut bytes)?;
        validate_stored_plan_json(&bytes)?;
        let stored: StoredActionPlan =
            serde_json::from_slice(&bytes).map_err(|_| ActionError::InvalidStoredPlan)?;
        validate_stored_plan(&stored, action_id)?;
        Ok(stored)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_audit_transition(
        &self,
        lock: &ActionWriteLock,
        plan: &ActionPlan,
        state: ActionState,
        result_code: SanitizedResultCode,
        timestamp_ms: i64,
        signing_key: &[u8],
    ) -> Result<AuditRecord, ActionError> {
        self.validate_lock(lock)?;
        let records = self.read_and_verify_audit(signing_key)?;
        let previous_action_state = records
            .iter()
            .rev()
            .find(|record| record.unsigned.action_id == plan.unsigned.action_id)
            .map(|record| record.unsigned.state);
        match previous_action_state {
            None if state == ActionState::IntentDurable => {}
            Some(previous) if previous.can_transition_to(state) => {}
            _ => return Err(ActionError::InvalidAudit),
        }
        if !result_code.matches_state(state) {
            return Err(ActionError::InvalidAudit);
        }
        let sequence = u64::try_from(records.len())
            .map_err(|_| ActionError::AuditLimitExceeded)?
            .checked_add(1)
            .ok_or(ActionError::AuditLimitExceeded)?;
        let record = AuditRecord::sign(
            UnsignedAuditRecord {
                schema_version: ACTION_AUDIT_SCHEMA_VERSION.to_string(),
                sequence,
                action_id: plan.unsigned.action_id.clone(),
                source_id: plan.unsigned.source_id.clone(),
                entity_id: plan.unsigned.entity_id.clone(),
                operation: plan.unsigned.operation,
                plan_digest: plan.plan_digest.clone(),
                state,
                result_code,
                timestamp_ms,
                previous_commitment: records.last().map(|record| record.commitment.clone()),
            },
            signing_key,
        )?;
        let mut encoded = serde_json::to_vec(&record).map_err(|_| ActionError::InvalidAudit)?;
        encoded.push(b'\n');
        let descriptor = rustix::fs::openat(
            &self.version_directory,
            "audit.jsonl",
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::APPEND
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        let stat = rustix::fs::fstat(&descriptor)?;
        validate_private_file(&stat)?;
        let current_size = u64::try_from(stat.st_size).map_err(|_| ActionError::InvalidAudit)?;
        if current_size
            .checked_add(encoded.len() as u64)
            .is_none_or(|size| size > MAX_AUDIT_BYTES)
        {
            return Err(ActionError::AuditLimitExceeded);
        }
        self.validate_state_identity()?;
        let mut file: File = descriptor.into();
        file.write_all(&encoded)?;
        file.sync_all()?;
        rustix::fs::fsync(&self.version_directory)?;
        self.validate_state_identity()?;
        Ok(record)
    }

    pub fn verify_audit(&self, signing_key: &[u8]) -> Result<u64, ActionError> {
        let records = self.read_and_verify_audit(signing_key)?;
        u64::try_from(records.len()).map_err(|_| ActionError::AuditLimitExceeded)
    }

    pub fn latest_audit_for_action(
        &self,
        action_id: &str,
        signing_key: &[u8],
    ) -> Result<Option<AuditRecord>, ActionError> {
        Ok(self
            .read_and_verify_audit(signing_key)?
            .into_iter()
            .rev()
            .find(|record| record.unsigned.action_id == action_id))
    }

    pub fn recover_incomplete_audit_tail(
        &self,
        lock: &ActionWriteLock,
        signing_key: &[u8],
    ) -> Result<bool, ActionError> {
        self.validate_lock(lock)?;
        let descriptor = match rustix::fs::openat(
            &self.version_directory,
            "audit.jsonl",
            rustix::fs::OFlags::RDWR | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error == rustix::io::Errno::NOENT => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let stat = rustix::fs::fstat(&descriptor)?;
        validate_private_file(&stat)?;
        if stat.st_size < 0 || stat.st_size as u64 > MAX_AUDIT_BYTES {
            return Err(ActionError::AuditLimitExceeded);
        }
        let mut file: File = descriptor.into();
        let mut bytes = Vec::with_capacity(stat.st_size as usize);
        file.read_to_end(&mut bytes)?;
        if bytes.is_empty()
            || bytes.ends_with(b"\n")
            || verify_audit_bytes(&bytes, signing_key).is_ok()
        {
            return Ok(false);
        }
        let prefix_len = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1);
        let tail = &bytes[prefix_len..];
        if tail.is_empty() || tail.len() > MAX_AUDIT_RECORD_BYTES || tail[0] != b'{' {
            return Err(ActionError::InvalidAudit);
        }
        let _ = verify_audit_bytes(&bytes[..prefix_len], signing_key)?;
        self.validate_state_identity()?;
        file.set_len(prefix_len as u64)?;
        file.sync_all()?;
        rustix::fs::fsync(&self.version_directory)?;
        self.validate_state_identity()?;
        Ok(true)
    }

    fn read_and_verify_audit(&self, signing_key: &[u8]) -> Result<Vec<AuditRecord>, ActionError> {
        self.validate_state_identity()?;
        let descriptor = match rustix::fs::openat(
            &self.version_directory,
            "audit.jsonl",
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        ) {
            Ok(descriptor) => descriptor,
            Err(error) if error == rustix::io::Errno::NOENT => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let stat = rustix::fs::fstat(&descriptor)?;
        validate_private_file(&stat)?;
        if stat.st_size < 0 || stat.st_size as u64 > MAX_AUDIT_BYTES {
            return Err(ActionError::AuditLimitExceeded);
        }
        let mut bytes = Vec::with_capacity(stat.st_size as usize);
        let mut file: File = descriptor.into();
        file.read_to_end(&mut bytes)?;
        verify_audit_bytes(&bytes, signing_key)
    }

    fn validate_lock(&self, lock: &ActionWriteLock) -> Result<(), ActionError> {
        if lock.released || !Arc::ptr_eq(&self.version_directory, &lock.version_directory) {
            return Err(ActionError::LockHeld);
        }
        self.validate_state_identity()
    }

    fn validate_state_identity(&self) -> Result<(), ActionError> {
        let current = rustix::fs::statat(
            rustix::fs::CWD,
            &self.state_root,
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        if identity(&current) != self.state_identity
            || identity(&rustix::fs::fstat(&self.state_directory)?) != self.state_identity
        {
            return Err(ActionError::StateChanged);
        }
        Ok(())
    }

    fn validate_layout(&self) -> Result<(), ActionError> {
        let allowed = ["OWNED_BY_AQL", "plans", "audit.jsonl"]
            .into_iter()
            .collect::<BTreeSet<_>>();
        for entry in std::fs::read_dir(&self.actions_root)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if !allowed.contains(name.as_str()) {
                return Err(ActionError::InvalidOwnershipMarker);
            }
        }
        for entry in std::fs::read_dir(self.actions_root.join("plans"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".action-plan") || name.starts_with('.') {
                return Err(ActionError::InvalidStoredPlan);
            }
            let metadata = entry.path().symlink_metadata()?;
            if !metadata.file_type().is_file() {
                return Err(ActionError::InvalidStoredPlan);
            }
        }
        Ok(())
    }
}

fn verify_audit_bytes(bytes: &[u8], signing_key: &[u8]) -> Result<Vec<AuditRecord>, ActionError> {
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        if records.len() >= MAX_AUDIT_RECORDS {
            return Err(ActionError::AuditLimitExceeded);
        }
        validate_audit_json(line)?;
        let record: AuditRecord =
            serde_json::from_slice(line).map_err(|_| ActionError::InvalidAudit)?;
        record.verify(signing_key)?;
        let expected_sequence =
            u64::try_from(records.len()).map_err(|_| ActionError::AuditLimitExceeded)? + 1;
        let previous = records
            .last()
            .map(|previous: &AuditRecord| previous.commitment.as_str());
        if record.unsigned.sequence != expected_sequence
            || record.unsigned.previous_commitment.as_deref() != previous
        {
            return Err(ActionError::AuditTampered);
        }
        records.push(record);
    }
    Ok(records)
}

impl ActionWriteLock {
    pub fn release(mut self) -> Result<(), ActionError> {
        rustix::fs::unlinkat(
            &self.lock_directory,
            ".aql-actions-v1.lock",
            rustix::fs::AtFlags::empty(),
        )?;
        rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for ActionWriteLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = rustix::fs::unlinkat(
                &self.lock_directory,
                ".aql-actions-v1.lock",
                rustix::fs::AtFlags::empty(),
            );
            let _ = rustix::fs::flock(&self.file, rustix::fs::FlockOperation::Unlock);
        }
    }
}

impl AtomicPlanFile {
    fn create(directory: Arc<OwnedFd>) -> Result<Self, ActionError> {
        let temporary_name = format!(".building-{:032x}.tmp", rand::random::<u128>());
        let descriptor = rustix::fs::openat(
            &directory,
            temporary_name.as_str(),
            rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        Ok(Self {
            directory,
            temporary_name,
            file: descriptor.into(),
            committed: false,
        })
    }

    fn prepare(&mut self) -> Result<(), ActionError> {
        self.file.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    fn commit(mut self, final_name: &str) -> Result<(), ActionError> {
        self.prepare()?;
        rustix::fs::renameat_with(
            &self.directory,
            self.temporary_name.as_str(),
            &self.directory,
            final_name,
            rustix::fs::RenameFlags::NOREPLACE,
        )?;
        rustix::fs::fsync(&self.directory)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for AtomicPlanFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = rustix::fs::unlinkat(
                &self.directory,
                self.temporary_name.as_str(),
                rustix::fs::AtFlags::empty(),
            );
        }
    }
}

fn validate_stored_plan(stored: &StoredActionPlan, action_id: &str) -> Result<(), ActionError> {
    if stored.storage_schema_version != ACTION_STORE_SCHEMA_VERSION
        || stored.plan.unsigned.action_id != action_id
    {
        return Err(ActionError::InvalidStoredPlan);
    }
    Ok(())
}

fn validate_stored_plan_json(bytes: &[u8]) -> Result<(), ActionError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| ActionError::InvalidStoredPlan)?;
    validate_object_keys(&value, &["storage_schema_version", "plan"])
        .map_err(|()| ActionError::InvalidStoredPlan)?;
    let plan = value.get("plan").ok_or(ActionError::InvalidStoredPlan)?;
    validate_object_keys(
        plan,
        &[
            "schema_version",
            "action_id",
            "idempotency_key",
            "adapter_id",
            "capability_version",
            "source_id",
            "entity_id",
            "operation",
            "arguments",
            "expected_revision",
            "created_at_ms",
            "expires_at_ms",
            "plan_digest",
        ],
    )
    .map_err(|()| ActionError::InvalidStoredPlan)?;
    validate_arguments_json(
        plan.get("arguments")
            .ok_or(ActionError::InvalidStoredPlan)?,
    )
}

fn validate_arguments_json(value: &serde_json::Value) -> Result<(), ActionError> {
    let kind = value
        .as_object()
        .and_then(|object| object.get("kind"))
        .and_then(serde_json::Value::as_str)
        .ok_or(ActionError::InvalidStoredPlan)?;
    let allowed = match kind {
        "none" => &["kind"][..],
        "rename_commitment" => &["kind", "commitment", "utf8_bytes"][..],
        _ => return Err(ActionError::InvalidStoredPlan),
    };
    validate_object_keys(value, allowed).map_err(|()| ActionError::InvalidStoredPlan)
}

fn validate_audit_json(bytes: &[u8]) -> Result<(), ActionError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| ActionError::InvalidAudit)?;
    validate_object_keys(
        &value,
        &[
            "schema_version",
            "sequence",
            "action_id",
            "source_id",
            "entity_id",
            "operation",
            "plan_digest",
            "state",
            "result_code",
            "timestamp_ms",
            "previous_commitment",
            "commitment",
        ],
    )
    .map_err(|()| ActionError::InvalidAudit)
}

fn validate_object_keys(value: &serde_json::Value, allowed: &[&str]) -> Result<(), ()> {
    let object = value.as_object().ok_or(())?;
    if object.len() != allowed.len() || !object.keys().all(|key| allowed.contains(&key.as_str())) {
        return Err(());
    }
    Ok(())
}

fn plan_file_name(action_id: &str) -> Result<String, ActionError> {
    if action_id.is_empty()
        || action_id.len() > 128
        || !action_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ActionError::InvalidStoredPlan);
    }
    Ok(format!("{action_id}.action-plan"))
}

fn open_or_create_private_root(path: &Path) -> Result<(PathBuf, OwnedFd), ActionError> {
    let normalized = prospective_private_root(path)?;
    let parent = normalized
        .parent()
        .ok_or(ActionError::UnsafeStateRoot)?
        .to_path_buf();
    let name = normalized
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(ActionError::UnsafeStateRoot)?;
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

fn prospective_private_root(path: &Path) -> Result<PathBuf, ActionError> {
    let parent = path
        .parent()
        .ok_or(ActionError::UnsafeStateRoot)?
        .canonicalize()?;
    let name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or(ActionError::UnsafeStateRoot)?;
    Ok(parent.join(name))
}

fn open_or_create_child_directory(parent: &OwnedFd, name: &str) -> Result<OwnedFd, ActionError> {
    match rustix::fs::mkdirat(parent, name, rustix::fs::Mode::from_raw_mode(0o700)) {
        Ok(()) => {}
        Err(error) if error == rustix::io::Errno::EXIST => {}
        Err(error) => return Err(error.into()),
    }
    open_child_directory(parent, name)
}

fn open_child_directory(parent: &OwnedFd, name: &str) -> Result<OwnedFd, ActionError> {
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
            ActionError::MissingState
        } else {
            ActionError::Platform(error)
        }
    })?;
    validate_private_directory(&rustix::fs::fstat(&directory)?)?;
    Ok(directory)
}

fn ensure_ownership_marker(directory: &OwnedFd) -> Result<(), ActionError> {
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
            ensure_existing_ownership_marker(directory)?;
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn ensure_existing_ownership_marker(directory: &OwnedFd) -> Result<(), ActionError> {
    let descriptor = rustix::fs::openat(
        directory,
        "OWNED_BY_AQL",
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|error| {
        if error == rustix::io::Errno::NOENT {
            ActionError::InvalidOwnershipMarker
        } else {
            ActionError::Platform(error)
        }
    })?;
    validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
    let mut marker = Vec::new();
    let mut file: File = descriptor.into();
    file.read_to_end(&mut marker)?;
    if marker != OWNERSHIP_MARKER {
        return Err(ActionError::InvalidOwnershipMarker);
    }
    Ok(())
}

fn validate_private_directory(stat: &rustix::fs::Stat) -> Result<(), ActionError> {
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::Directory
        || stat.st_mode & 0o077 != 0
    {
        return Err(ActionError::UnsafeStateRoot);
    }
    Ok(())
}

fn validate_private_file(stat: &rustix::fs::Stat) -> Result<(), ActionError> {
    if rustix::fs::FileType::from_raw_mode(stat.st_mode) != rustix::fs::FileType::RegularFile
        || stat.st_mode & 0o077 != 0
    {
        return Err(ActionError::UnsafeStateRoot);
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::time::{SystemTime, UNIX_EPOCH};

    use aql_model::{EntityId, SourceId};

    use super::*;
    use crate::{
        ACTION_PLAN_SCHEMA_VERSION, ActionArguments, ActionOperation, DEFAULT_PLAN_TTL_MS,
        UnsignedActionPlan,
    };

    const KEY: &[u8] = b"synthetic-phase-five-store-signing-key";

    fn test_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is available")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "aql-actions-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("test root is created");
        root
    }

    fn plan(action_id: &str) -> ActionPlan {
        ActionPlan::sign(
            UnsignedActionPlan {
                schema_version: ACTION_PLAN_SCHEMA_VERSION.to_string(),
                action_id: action_id.to_string(),
                idempotency_key: format!("{action_id}-idempotency"),
                adapter_id: "synthetic-official-channel".to_string(),
                capability_version: "synthetic-capability-v1".to_string(),
                source_id: SourceId::new("synthetic-source-opaque"),
                entity_id: EntityId::new("synthetic-entity-opaque"),
                operation: ActionOperation::SessionArchive,
                arguments: ActionArguments::None,
                expected_revision: "synthetic-revision-0001".to_string(),
                created_at_ms: 1_000,
                expires_at_ms: 1_000 + DEFAULT_PLAN_TTL_MS,
            },
            KEY,
        )
        .expect("synthetic plan signs")
    }

    #[test]
    fn store_is_private_atomic_and_audit_chain_detects_tampering() {
        let root = test_root("private");
        let data_root = root.join("agent-data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = ActionStore::create(&state_root, &data_root).expect("store creates");
        assert_eq!(
            fs::metadata(&state_root)
                .expect("state root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let lock = store.acquire_write_lock().expect("writer locks");
        let plan = plan("synthetic-action-0001");
        store
            .publish_plan(&lock, plan.clone(), 2_000, KEY)
            .expect("plan publishes");
        assert_eq!(
            fs::metadata(
                store
                    .actions_root()
                    .join("plans/synthetic-action-0001.action-plan")
            )
            .expect("plan metadata")
            .permissions()
            .mode()
                & 0o777,
            0o600
        );
        store
            .append_audit_transition(
                &lock,
                &plan,
                ActionState::IntentDurable,
                SanitizedResultCode::IntentRecorded,
                2_001,
                KEY,
            )
            .expect("intent audit appends");
        store
            .append_audit_transition(
                &lock,
                &plan,
                ActionState::Executing,
                SanitizedResultCode::DispatchStarted,
                2_002,
                KEY,
            )
            .expect("dispatch audit appends");
        assert_eq!(store.verify_audit(KEY).expect("audit verifies"), 2);
        let _ = store
            .load_plan("synthetic-action-0001")
            .expect("plan reloads");
        let plan_path = store
            .actions_root()
            .join("plans/synthetic-action-0001.action-plan");
        let original_plan = fs::read(&plan_path).expect("plan reads");
        let mut stored_json: serde_json::Value =
            serde_json::from_slice(&original_plan).expect("stored plan JSON parses");
        stored_json["state"] = serde_json::json!("succeeded");
        fs::write(
            &plan_path,
            serde_json::to_vec(&stored_json).expect("stored plan JSON serializes"),
        )
        .expect("stored state is tampered");
        assert!(matches!(
            store.load_plan("synthetic-action-0001"),
            Err(ActionError::InvalidStoredPlan)
        ));
        fs::write(&plan_path, &original_plan).expect("original plan is restored");
        stored_json = serde_json::from_slice(&original_plan).expect("stored plan JSON parses");
        stored_json["plan"]["plaintext_title"] = serde_json::json!("Synthetic secret title");
        fs::write(
            &plan_path,
            serde_json::to_vec(&stored_json).expect("stored plan JSON serializes"),
        )
        .expect("unknown plan field is injected");
        assert!(matches!(
            store.load_plan("synthetic-action-0001"),
            Err(ActionError::InvalidStoredPlan)
        ));
        fs::write(&plan_path, &original_plan).expect("original plan is restored");
        lock.release().expect("lock releases");
        assert!(!state_root.join(".aql-actions-v1.lock").exists());

        let audit_path = store.actions_root().join("audit.jsonl");
        let original_audit = fs::read(&audit_path).expect("audit reads");
        let mut interrupted_audit = original_audit.clone();
        interrupted_audit.extend_from_slice(b"{\"schema_version\":\"aql-action-audit-v1\"");
        fs::write(&audit_path, interrupted_audit).expect("partial audit tail is written");
        assert!(matches!(
            store.verify_audit(KEY),
            Err(ActionError::InvalidAudit)
        ));
        let recovery_lock = store.acquire_write_lock().expect("recovery writer locks");
        assert!(
            store
                .recover_incomplete_audit_tail(&recovery_lock, KEY)
                .expect("partial tail recovers")
        );
        recovery_lock.release().expect("recovery lock releases");
        assert_eq!(
            store.verify_audit(KEY).expect("recovered audit verifies"),
            2
        );

        let mut audit_lines = original_audit
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("audit parses"))
            .collect::<Vec<_>>();
        audit_lines[0]["plaintext_title"] = serde_json::json!("Synthetic secret title");
        let injected_audit = audit_lines
            .into_iter()
            .map(|line| serde_json::to_string(&line).expect("audit serializes"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(&audit_path, injected_audit).expect("unknown audit field is injected");
        assert!(matches!(
            store.verify_audit(KEY),
            Err(ActionError::InvalidAudit)
        ));
        fs::write(&audit_path, &original_audit).expect("original audit is restored");

        let mut bytes = original_audit;
        let marker = b"\"commitment\":\"";
        let position = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("audit contains a commitment")
            + marker.len();
        bytes[position] = if bytes[position] == b'0' { b'1' } else { b'0' };
        fs::write(&audit_path, bytes).expect("audit is tampered");
        assert!(matches!(
            store.verify_audit(KEY),
            Err(ActionError::AuditTampered)
        ));
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn store_rejects_overlap_symlink_unknown_files_and_lock_contention() {
        let root = test_root("boundaries");
        let data_root = root.join("agent-data");
        fs::create_dir(&data_root).expect("data root is created");
        let overlap = data_root.join("state");
        assert!(matches!(
            ActionStore::create(&overlap, &data_root),
            Err(ActionError::StateRootOverlap)
        ));
        assert!(!overlap.exists());

        let real_state = root.join("real-state");
        fs::create_dir(&real_state).expect("real state is created");
        fs::set_permissions(&real_state, fs::Permissions::from_mode(0o700))
            .expect("real state is private");
        let linked_state = root.join("linked-state");
        symlink(&real_state, &linked_state).expect("state symlink is created");
        assert!(ActionStore::create(&linked_state, &data_root).is_err());

        let state_root = root.join("state");
        let store = ActionStore::create(&state_root, &data_root).expect("store creates");
        let lock = store.acquire_write_lock().expect("first writer locks");
        assert!(matches!(
            store.acquire_write_lock(),
            Err(ActionError::LockHeld)
        ));
        lock.release().expect("lock releases");
        fs::write(store.actions_root().join("unknown-user-file"), b"preserve")
            .expect("unknown file is created");
        assert!(matches!(
            ActionStore::open_existing(&state_root, &data_root),
            Err(ActionError::InvalidOwnershipMarker)
        ));
        assert_eq!(
            fs::read(store.actions_root().join("unknown-user-file")).expect("unknown file remains"),
            b"preserve"
        );
        fs::remove_dir_all(root).expect("test root is removed");
    }

    #[test]
    fn root_replacement_prevents_plan_publication() {
        let root = test_root("replacement");
        let data_root = root.join("agent-data");
        fs::create_dir(&data_root).expect("data root is created");
        let state_root = root.join("state");
        let store = ActionStore::create(&state_root, &data_root).expect("store creates");
        let lock = store.acquire_write_lock().expect("writer locks");
        let moved = root.join("moved-state");
        fs::rename(&state_root, &moved).expect("state root moves");
        fs::create_dir(&state_root).expect("replacement root is created");
        assert!(matches!(
            store.publish_plan(&lock, plan("synthetic-action-0002"), 2_000, KEY),
            Err(ActionError::StateChanged)
        ));
        assert!(
            fs::read_dir(moved.join("actions/v1/plans"))
                .expect("original plans list")
                .all(|entry| !entry
                    .expect("plan entry reads")
                    .file_name()
                    .to_string_lossy()
                    .contains("building"))
        );
        fs::remove_dir_all(root).expect("test root is removed");
    }
}
