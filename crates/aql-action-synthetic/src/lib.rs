//! Deterministic synthetic official-channel reference Adapter.
//!
//! This crate accepts only an isolated root with an explicit synthetic marker.
//! It is protocol test infrastructure and cannot parse or mutate real Agent data.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use aql_actions::{
    ActionCapability, ActionError, ActionExecutionResult, ActionOperation, ActionReconciliation,
    ActionTargetState, AgentActionAdapter, ApprovedAction, OfficialChannelEvidence,
};
use aql_model::{EntityId, SourceId};
use serde::{Deserialize, Serialize};

pub const SYNTHETIC_CHANNEL_VERSION: &str = "aql-synthetic-official-channel-v1";
const SYNTHETIC_MARKER: &[u8] = b"aql-synthetic-action-channel-v1\n";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyntheticFault {
    #[default]
    None,
    RejectBeforeApply,
    DelayBeforeDispatch,
    DelayBeforeApply,
    DelayResponseAfterApply,
    LoseResponseAfterApply,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyntheticEntity {
    pub revision: u64,
    pub archived: bool,
    pub title: String,
    pub external_effects: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SyntheticOutcome {
    action_id: String,
    result: StoredExecutionResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredExecutionResult {
    Succeeded,
    Conflicted,
    Rejected,
}

impl StoredExecutionResult {
    fn execution(self) -> ActionExecutionResult {
        match self {
            Self::Succeeded => ActionExecutionResult::Succeeded,
            Self::Conflicted => ActionExecutionResult::Conflicted,
            Self::Rejected => ActionExecutionResult::Rejected,
        }
    }

    fn reconciliation(self) -> ActionReconciliation {
        match self {
            Self::Succeeded => ActionReconciliation::Succeeded,
            Self::Conflicted | Self::Rejected => ActionReconciliation::NotApplied,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SyntheticChannelState {
    schema_version: String,
    source_id: SourceId,
    entities: BTreeMap<EntityId, SyntheticEntity>,
    outcomes: BTreeMap<String, SyntheticOutcome>,
}

pub struct SyntheticActionAdapter {
    root: PathBuf,
    fault: SyntheticFault,
}

struct SyntheticChannelLock<'directory> {
    directory: &'directory OwnedFd,
    descriptor: OwnedFd,
    released: bool,
}

impl SyntheticActionAdapter {
    pub fn create_fixture(
        root: &Path,
        source_id: SourceId,
        entities: BTreeMap<EntityId, SyntheticEntity>,
    ) -> Result<Self, ActionError> {
        std::fs::create_dir(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        }
        let directory = open_root(root)?;
        write_new_private_file(&directory, "SYNTHETIC_ACTION_CHANNEL", SYNTHETIC_MARKER)?;
        let state = SyntheticChannelState {
            schema_version: SYNTHETIC_CHANNEL_VERSION.to_string(),
            source_id,
            entities,
            outcomes: BTreeMap::new(),
        };
        write_new_private_file(
            &directory,
            "channel.json",
            &serde_json::to_vec(&state).map_err(|_| ActionError::InvalidStoredPlan)?,
        )?;
        Ok(Self {
            root: root.to_path_buf(),
            fault: SyntheticFault::None,
        })
    }

    pub fn open(root: &Path) -> Result<Self, ActionError> {
        let directory = open_root(root)?;
        validate_marker(&directory)?;
        let _ = read_state(&directory)?;
        Ok(Self {
            root: root.to_path_buf(),
            fault: SyntheticFault::None,
        })
    }

    #[must_use]
    pub fn with_fault(mut self, fault: SyntheticFault) -> Self {
        self.fault = fault;
        self
    }

    pub fn entity(&self, entity_id: &EntityId) -> Result<SyntheticEntity, ActionError> {
        let directory = open_root(&self.root)?;
        validate_marker(&directory)?;
        read_state(&directory)?
            .entities
            .get(entity_id)
            .cloned()
            .ok_or(ActionError::InvalidPlan)
    }

    fn capabilities() -> Result<Vec<ActionCapability>, ActionError> {
        [
            ActionOperation::SessionArchive,
            ActionOperation::SessionUnarchive,
            ActionOperation::SessionRename,
        ]
        .into_iter()
        .map(|operation| {
            ActionCapability::admit(
                operation,
                SYNTHETIC_CHANNEL_VERSION,
                OfficialChannelEvidence {
                    official_channel_id: "synthetic-official-channel".to_string(),
                    official_channel_version: SYNTHETIC_CHANNEL_VERSION.to_string(),
                    target_binding: Some("canonical-entity-id".to_string()),
                    atomic_precondition: Some("exact-revision".to_string()),
                    idempotency_mechanism: Some("persistent-idempotency-key".to_string()),
                    outcome_lookup: Some("persistent-outcome-map".to_string()),
                    stable_result_mapping: Some("typed-result".to_string()),
                    disposable_profile: Some("synthetic-marker-root".to_string()),
                    inverse_operation: true,
                },
                true,
            )
        })
        .collect()
    }

    fn with_locked_state<T>(
        &self,
        operation: impl FnOnce(&mut SyntheticChannelState) -> Result<T, ActionError>,
    ) -> Result<T, ActionError> {
        let directory = open_root(&self.root)?;
        validate_marker(&directory)?;
        let root_identity = identity(&rustix::fs::fstat(&directory)?);
        let lock = SyntheticChannelLock::acquire(&directory)?;
        let mut state = read_state(&directory)?;
        let result = operation(&mut state)?;
        validate_root_identity(&self.root, &directory, root_identity)?;
        replace_state(&directory, &state)?;
        validate_root_identity(&self.root, &directory, root_identity)?;
        lock.release()?;
        Ok(result)
    }
}

impl<'directory> SyntheticChannelLock<'directory> {
    fn acquire(directory: &'directory OwnedFd) -> Result<Self, ActionError> {
        let descriptor = rustix::fs::openat(
            directory,
            ".synthetic-channel.lock",
            rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::from_raw_mode(0o600),
        )?;
        validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
        rustix::fs::flock(&descriptor, rustix::fs::FlockOperation::LockExclusive)?;
        Ok(Self {
            directory,
            descriptor,
            released: false,
        })
    }

    fn release(mut self) -> Result<(), ActionError> {
        rustix::fs::unlinkat(
            self.directory,
            ".synthetic-channel.lock",
            rustix::fs::AtFlags::empty(),
        )?;
        rustix::fs::flock(&self.descriptor, rustix::fs::FlockOperation::Unlock)?;
        self.released = true;
        Ok(())
    }
}

impl Drop for SyntheticChannelLock<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = rustix::fs::unlinkat(
                self.directory,
                ".synthetic-channel.lock",
                rustix::fs::AtFlags::empty(),
            );
            let _ = rustix::fs::flock(&self.descriptor, rustix::fs::FlockOperation::Unlock);
        }
    }
}

impl AgentActionAdapter for SyntheticActionAdapter {
    fn action_capabilities(
        &self,
        _source_id: &SourceId,
    ) -> Result<Vec<ActionCapability>, ActionError> {
        Self::capabilities()
    }

    fn observe_target(
        &self,
        source_id: &SourceId,
        entity_id: &EntityId,
    ) -> Result<ActionTargetState, ActionError> {
        let directory = open_root(&self.root)?;
        validate_marker(&directory)?;
        let state = read_state(&directory)?;
        if &state.source_id != source_id {
            return Err(ActionError::InvalidPlan);
        }
        let entity = state
            .entities
            .get(entity_id)
            .ok_or(ActionError::InvalidPlan)?;
        Ok(ActionTargetState {
            source_id: source_id.clone(),
            entity_id: entity_id.clone(),
            revision: revision(entity.revision),
        })
    }

    fn execute(&self, approved: &ApprovedAction) -> Result<ActionExecutionResult, ActionError> {
        if self.fault == SyntheticFault::DelayBeforeApply {
            std::thread::sleep(Duration::from_millis(250));
        }
        let plan = &approved.plan;
        let (result, newly_executed) = self.with_locked_state(|state| {
            if state.source_id != plan.unsigned.source_id {
                return Err(ActionError::InvalidPlan);
            }
            if let Some(outcome) = state.outcomes.get(&plan.unsigned.idempotency_key) {
                if outcome.action_id != plan.unsigned.action_id {
                    return Err(ActionError::InvalidPlan);
                }
                return Ok((outcome.result.execution(), false));
            }
            if self.fault == SyntheticFault::RejectBeforeApply {
                state.outcomes.insert(
                    plan.unsigned.idempotency_key.clone(),
                    SyntheticOutcome {
                        action_id: plan.unsigned.action_id.clone(),
                        result: StoredExecutionResult::Rejected,
                    },
                );
                return Ok((ActionExecutionResult::Rejected, true));
            }
            let entity = state
                .entities
                .get_mut(&plan.unsigned.entity_id)
                .ok_or(ActionError::InvalidPlan)?;
            if revision(entity.revision) != plan.unsigned.expected_revision {
                state.outcomes.insert(
                    plan.unsigned.idempotency_key.clone(),
                    SyntheticOutcome {
                        action_id: plan.unsigned.action_id.clone(),
                        result: StoredExecutionResult::Conflicted,
                    },
                );
                return Ok((ActionExecutionResult::Conflicted, true));
            }
            match plan.unsigned.operation {
                ActionOperation::SessionArchive => entity.archived = true,
                ActionOperation::SessionUnarchive => entity.archived = false,
                ActionOperation::SessionRename => {
                    entity.title = approved
                        .supplied_rename
                        .clone()
                        .ok_or(ActionError::InvalidArguments)?;
                }
            }
            entity.revision = entity
                .revision
                .checked_add(1)
                .ok_or(ActionError::InvalidPlan)?;
            entity.external_effects = entity
                .external_effects
                .checked_add(1)
                .ok_or(ActionError::InvalidPlan)?;
            state.outcomes.insert(
                plan.unsigned.idempotency_key.clone(),
                SyntheticOutcome {
                    action_id: plan.unsigned.action_id.clone(),
                    result: StoredExecutionResult::Succeeded,
                },
            );
            Ok((ActionExecutionResult::Succeeded, true))
        })?;
        if newly_executed
            && result == ActionExecutionResult::Succeeded
            && self.fault == SyntheticFault::DelayResponseAfterApply
        {
            std::thread::sleep(Duration::from_millis(250));
            return Ok(ActionExecutionResult::UnknownOutcome);
        }
        if newly_executed
            && result == ActionExecutionResult::Succeeded
            && self.fault == SyntheticFault::LoseResponseAfterApply
        {
            Ok(ActionExecutionResult::UnknownOutcome)
        } else {
            Ok(result)
        }
    }

    fn reconcile(
        &self,
        action_id: &str,
        idempotency_key: &str,
    ) -> Result<ActionReconciliation, ActionError> {
        let directory = open_root(&self.root)?;
        validate_marker(&directory)?;
        let state = read_state(&directory)?;
        let Some(outcome) = state.outcomes.get(idempotency_key) else {
            return Ok(ActionReconciliation::NotApplied);
        };
        if outcome.action_id != action_id {
            return Err(ActionError::InvalidPlan);
        }
        Ok(outcome.result.reconciliation())
    }
}

fn revision(value: u64) -> String {
    format!("synthetic-revision-{value:020}")
}

fn open_root(root: &Path) -> Result<OwnedFd, ActionError> {
    let directory = rustix::fs::openat(
        rustix::fs::CWD,
        root,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    validate_private_directory(&rustix::fs::fstat(&directory)?)?;
    Ok(directory)
}

fn validate_marker(directory: &OwnedFd) -> Result<(), ActionError> {
    let descriptor = rustix::fs::openat(
        directory,
        "SYNTHETIC_ACTION_CHANNEL",
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    validate_private_file(&rustix::fs::fstat(&descriptor)?)?;
    let mut marker = Vec::new();
    let mut file: File = descriptor.into();
    file.read_to_end(&mut marker)?;
    if marker != SYNTHETIC_MARKER {
        return Err(ActionError::InvalidOwnershipMarker);
    }
    Ok(())
}

fn read_state(directory: &OwnedFd) -> Result<SyntheticChannelState, ActionError> {
    let descriptor = rustix::fs::openat(
        directory,
        "channel.json",
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?;
    let stat = rustix::fs::fstat(&descriptor)?;
    validate_private_file(&stat)?;
    if stat.st_size < 0 || stat.st_size > 1024 * 1024 {
        return Err(ActionError::InvalidStoredPlan);
    }
    let mut bytes = Vec::with_capacity(stat.st_size as usize);
    let mut file: File = descriptor.into();
    file.read_to_end(&mut bytes)?;
    let state: SyntheticChannelState =
        serde_json::from_slice(&bytes).map_err(|_| ActionError::InvalidStoredPlan)?;
    if state.schema_version != SYNTHETIC_CHANNEL_VERSION {
        return Err(ActionError::InvalidStoredPlan);
    }
    Ok(state)
}

fn replace_state(directory: &OwnedFd, state: &SyntheticChannelState) -> Result<(), ActionError> {
    let bytes = serde_json::to_vec(state).map_err(|_| ActionError::InvalidStoredPlan)?;
    if bytes.len() > 1024 * 1024 {
        return Err(ActionError::InvalidStoredPlan);
    }
    let temporary_name = format!(".building-{:032x}.tmp", rand::random::<u128>());
    let descriptor = rustix::fs::openat(
        directory,
        temporary_name.as_str(),
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(0o600),
    )?;
    let mut file: File = descriptor.into();
    let result = (|| {
        file.write_all(&bytes)?;
        file.sync_all()?;
        rustix::fs::renameat(
            directory,
            temporary_name.as_str(),
            directory,
            "channel.json",
        )?;
        rustix::fs::fsync(directory)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = rustix::fs::unlinkat(
            directory,
            temporary_name.as_str(),
            rustix::fs::AtFlags::empty(),
        );
    }
    result
}

fn write_new_private_file(
    directory: &OwnedFd,
    name: &str,
    bytes: &[u8],
) -> Result<(), ActionError> {
    let descriptor = rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(0o600),
    )?;
    let mut file: File = descriptor.into();
    file.write_all(bytes)?;
    file.sync_all()?;
    rustix::fs::fsync(directory)?;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

fn identity(stat: &rustix::fs::Stat) -> FileIdentity {
    FileIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino,
    }
}

fn validate_root_identity(
    root: &Path,
    directory: &OwnedFd,
    expected: FileIdentity,
) -> Result<(), ActionError> {
    let current = rustix::fs::statat(rustix::fs::CWD, root, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)?;
    if identity(&current) != expected || identity(&rustix::fs::fstat(directory)?) != expected {
        return Err(ActionError::StateChanged);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use aql_actions::{
        ACTION_PLAN_SCHEMA_VERSION, ActionArguments, ActionPlan, DEFAULT_PLAN_TTL_MS,
        UnsignedActionPlan,
    };

    use super::*;

    const KEY: &[u8] = b"synthetic-reference-adapter-key";
    static ROOT_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time is available")
            .as_nanos();
        let counter = ROOT_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "aql-action-synthetic-{}-{nonce}-{counter}",
            std::process::id()
        ))
    }

    fn fixture() -> (PathBuf, SyntheticActionAdapter, SourceId, EntityId) {
        let root = root();
        let source = SourceId::new("synthetic-source-opaque");
        let entity = EntityId::new("synthetic-entity-opaque");
        let adapter = SyntheticActionAdapter::create_fixture(
            &root,
            source.clone(),
            BTreeMap::from([(
                entity.clone(),
                SyntheticEntity {
                    revision: 1,
                    archived: false,
                    title: "Synthetic original title".to_string(),
                    external_effects: 0,
                },
            )]),
        )
        .expect("synthetic fixture creates");
        (root, adapter, source, entity)
    }

    fn approved(
        source: SourceId,
        entity: EntityId,
        action_id: &str,
        operation: ActionOperation,
        expected_revision: &str,
        rename: Option<&str>,
    ) -> ApprovedAction {
        let arguments = match rename {
            Some(value) => ActionArguments::rename(value, KEY).expect("rename commitment builds"),
            None => ActionArguments::None,
        };
        ApprovedAction {
            plan: ActionPlan::sign(
                UnsignedActionPlan {
                    schema_version: ACTION_PLAN_SCHEMA_VERSION.to_string(),
                    action_id: action_id.to_string(),
                    idempotency_key: format!("{action_id}-idempotency"),
                    adapter_id: "synthetic-official-channel".to_string(),
                    capability_version: SYNTHETIC_CHANNEL_VERSION.to_string(),
                    source_id: source,
                    entity_id: entity,
                    operation,
                    arguments,
                    expected_revision: expected_revision.to_string(),
                    created_at_ms: 1_000,
                    expires_at_ms: 1_000 + DEFAULT_PLAN_TTL_MS,
                },
                KEY,
            )
            .expect("plan signs"),
            supplied_rename: rename.map(ToString::to_string),
        }
    }

    #[test]
    fn duplicate_apply_has_exactly_one_external_effect() {
        let (root, adapter, source, entity) = fixture();
        let action = approved(
            source,
            entity.clone(),
            "synthetic-action-archive",
            ActionOperation::SessionArchive,
            "synthetic-revision-00000000000000000001",
            None,
        );
        assert_eq!(
            adapter.execute(&action).expect("first execute succeeds"),
            ActionExecutionResult::Succeeded
        );
        assert_eq!(
            adapter.execute(&action).expect("duplicate resolves"),
            ActionExecutionResult::Succeeded
        );
        let entity = adapter.entity(&entity).expect("entity reads");
        assert!(entity.archived);
        assert_eq!(entity.revision, 2);
        assert_eq!(entity.external_effects, 1);
        std::fs::remove_dir_all(root).expect("fixture root is removed");
    }

    #[test]
    fn stale_revision_conflicts_without_effect() {
        let (root, adapter, source, entity) = fixture();
        let action = approved(
            source,
            entity.clone(),
            "synthetic-action-stale",
            ActionOperation::SessionArchive,
            "synthetic-revision-00000000000000000000",
            None,
        );
        assert_eq!(
            adapter.execute(&action).expect("stale execute resolves"),
            ActionExecutionResult::Conflicted
        );
        let entity = adapter.entity(&entity).expect("entity reads");
        assert!(!entity.archived);
        assert_eq!(entity.revision, 1);
        assert_eq!(entity.external_effects, 0);
        std::fs::remove_dir_all(root).expect("fixture root is removed");
    }

    #[test]
    fn response_loss_reconciles_success_without_duplicate() {
        let (root, adapter, source, entity) = fixture();
        let adapter = adapter.with_fault(SyntheticFault::LoseResponseAfterApply);
        let action = approved(
            source,
            entity.clone(),
            "synthetic-action-lost-response",
            ActionOperation::SessionRename,
            "synthetic-revision-00000000000000000001",
            Some("Synthetic renamed title"),
        );
        assert_eq!(
            adapter
                .execute(&action)
                .expect("lost response is represented"),
            ActionExecutionResult::UnknownOutcome
        );
        assert_eq!(
            adapter
                .reconcile(
                    &action.plan.unsigned.action_id,
                    &action.plan.unsigned.idempotency_key
                )
                .expect("outcome reconciles"),
            ActionReconciliation::Succeeded
        );
        assert_eq!(
            adapter.execute(&action).expect("retry is idempotent"),
            ActionExecutionResult::Succeeded
        );
        let entity = adapter.entity(&entity).expect("entity reads");
        assert_eq!(entity.title, "Synthetic renamed title");
        assert_eq!(entity.external_effects, 1);
        std::fs::remove_dir_all(root).expect("fixture root is removed");
    }

    #[test]
    fn marker_is_required_and_capabilities_are_fully_admitted() {
        let (root, adapter, source, _entity) = fixture();
        assert!(
            adapter
                .action_capabilities(&source)
                .expect("synthetic capabilities resolve")
                .iter()
                .all(ActionCapability::is_supported)
        );
        std::fs::remove_file(root.join("SYNTHETIC_ACTION_CHANNEL")).expect("marker is removed");
        assert!(SyntheticActionAdapter::open(&root).is_err());
        std::fs::remove_dir_all(root).expect("fixture root is removed");
    }
}
