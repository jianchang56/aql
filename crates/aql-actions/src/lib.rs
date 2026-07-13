//! Capability-gated, auditable Action contracts.

use std::fmt;

use aql_model::{EntityId, SourceId};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

mod store;

pub use store::{ActionStore, ActionWriteLock, StoredActionPlan};

pub const ACTION_PLAN_SCHEMA_VERSION: &str = "aql-action-plan-v1";
pub const ACTION_AUDIT_SCHEMA_VERSION: &str = "aql-action-audit-v1";
pub const DEFAULT_PLAN_TTL_MS: i64 = 5 * 60 * 1_000;
pub const MAX_PLAN_TTL_MS: i64 = 60 * 60 * 1_000;
pub const ACTION_STORE_SCHEMA_VERSION: &str = "aql-action-store-v1";

const PLAN_DIGEST_DOMAIN: &[u8] = b"aql/action-plan/v1\0";
const ARGUMENT_COMMITMENT_DOMAIN: &[u8] = b"aql/action-argument/v1\0";
const AUDIT_COMMITMENT_DOMAIN: &[u8] = b"aql/action-audit/v1\0";

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionOperation {
    SessionArchive,
    SessionUnarchive,
    SessionRename,
}

impl fmt::Display for ActionOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SessionArchive => "session.archive",
            Self::SessionUnarchive => "session.unarchive",
            Self::SessionRename => "session.rename",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionAccess {
    Safe,
    Content,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedReason {
    OfficialChannelUndocumented,
    TargetBindingUnavailable,
    AtomicPreconditionUnavailable,
    IdempotencyAndOutcomeUnavailable,
    StableResultUnavailable,
    DisposableProfileUnavailable,
    InverseOperationUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CapabilityStatus {
    Supported {
        official_channel_id: String,
        official_channel_version: String,
        atomic_precondition: String,
        idempotency_mechanism: Option<String>,
        outcome_lookup: Option<String>,
    },
    Unsupported {
        reason: UnsupportedReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActionCapability {
    pub operation: ActionOperation,
    pub capability_version: String,
    pub required_access: ActionAccess,
    pub reversible: bool,
    pub status: CapabilityStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialChannelEvidence {
    pub official_channel_id: String,
    pub official_channel_version: String,
    pub target_binding: Option<String>,
    pub atomic_precondition: Option<String>,
    pub idempotency_mechanism: Option<String>,
    pub outcome_lookup: Option<String>,
    pub stable_result_mapping: Option<String>,
    pub disposable_profile: Option<String>,
    pub inverse_operation: bool,
}

impl ActionCapability {
    #[must_use]
    pub fn unsupported(
        operation: ActionOperation,
        capability_version: impl Into<String>,
        reason: UnsupportedReason,
    ) -> Self {
        Self {
            operation,
            capability_version: capability_version.into(),
            required_access: required_access(operation),
            reversible: false,
            status: CapabilityStatus::Unsupported { reason },
        }
    }

    pub fn admit(
        operation: ActionOperation,
        capability_version: impl Into<String>,
        evidence: OfficialChannelEvidence,
        advertise_reversible: bool,
    ) -> Result<Self, ActionError> {
        validate_public_identifier(&evidence.official_channel_id)?;
        validate_public_identifier(&evidence.official_channel_version)?;
        let _ = evidence
            .target_binding
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or(ActionError::Unsupported(
                UnsupportedReason::TargetBindingUnavailable,
            ))?;
        let atomic_precondition = evidence
            .atomic_precondition
            .filter(|value| !value.is_empty())
            .ok_or(ActionError::Unsupported(
                UnsupportedReason::AtomicPreconditionUnavailable,
            ))?;
        if evidence.idempotency_mechanism.is_none() && evidence.outcome_lookup.is_none() {
            return Err(ActionError::Unsupported(
                UnsupportedReason::IdempotencyAndOutcomeUnavailable,
            ));
        }
        if evidence
            .stable_result_mapping
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(ActionError::Unsupported(
                UnsupportedReason::StableResultUnavailable,
            ));
        }
        if evidence
            .disposable_profile
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(ActionError::Unsupported(
                UnsupportedReason::DisposableProfileUnavailable,
            ));
        }
        if advertise_reversible && !evidence.inverse_operation {
            return Err(ActionError::Unsupported(
                UnsupportedReason::InverseOperationUnavailable,
            ));
        }
        Ok(Self {
            operation,
            capability_version: capability_version.into(),
            required_access: required_access(operation),
            reversible: advertise_reversible,
            status: CapabilityStatus::Supported {
                official_channel_id: evidence.official_channel_id,
                official_channel_version: evidence.official_channel_version,
                atomic_precondition,
                idempotency_mechanism: evidence.idempotency_mechanism,
                outcome_lookup: evidence.outcome_lookup,
            },
        })
    }

    #[must_use]
    pub fn is_supported(&self) -> bool {
        matches!(self.status, CapabilityStatus::Supported { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActionArguments {
    None,
    RenameCommitment { commitment: String, utf8_bytes: u64 },
}

impl ActionArguments {
    pub fn rename(title: &str, key: &[u8]) -> Result<Self, ActionError> {
        validate_title(title)?;
        Ok(Self::RenameCommitment {
            commitment: keyed_commitment(ARGUMENT_COMMITMENT_DOMAIN, title.as_bytes(), key)?,
            utf8_bytes: title.len() as u64,
        })
    }

    pub fn verify_rename(&self, title: &str, key: &[u8]) -> Result<(), ActionError> {
        validate_title(title)?;
        let Self::RenameCommitment {
            commitment,
            utf8_bytes,
        } = self
        else {
            return Err(ActionError::InvalidArguments);
        };
        if *utf8_bytes != title.len() as u64 {
            return Err(ActionError::ArgumentCommitmentMismatch);
        }
        verify_commitment(
            ARGUMENT_COMMITMENT_DOMAIN,
            title.as_bytes(),
            key,
            commitment,
        )
        .map_err(|_| ActionError::ArgumentCommitmentMismatch)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnsignedActionPlan {
    pub schema_version: String,
    pub action_id: String,
    pub idempotency_key: String,
    pub adapter_id: String,
    pub capability_version: String,
    pub source_id: SourceId,
    pub entity_id: EntityId,
    pub operation: ActionOperation,
    pub arguments: ActionArguments,
    pub expected_revision: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ActionPlan {
    #[serde(flatten)]
    pub unsigned: UnsignedActionPlan,
    pub plan_digest: String,
}

impl ActionPlan {
    pub fn sign(unsigned: UnsignedActionPlan, key: &[u8]) -> Result<Self, ActionError> {
        validate_unsigned_plan(&unsigned)?;
        let bytes = serde_json::to_vec(&unsigned).map_err(|_| ActionError::InvalidPlan)?;
        let plan_digest = keyed_commitment(PLAN_DIGEST_DOMAIN, &bytes, key)?;
        Ok(Self {
            unsigned,
            plan_digest,
        })
    }

    pub fn verify(&self, key: &[u8], now_ms: i64) -> Result<(), ActionError> {
        validate_unsigned_plan(&self.unsigned)?;
        if now_ms < self.unsigned.created_at_ms || now_ms >= self.unsigned.expires_at_ms {
            return Err(ActionError::PlanExpired);
        }
        self.verify_digest(key)
    }

    pub fn verify_digest(&self, key: &[u8]) -> Result<(), ActionError> {
        validate_unsigned_plan(&self.unsigned)?;
        let bytes = serde_json::to_vec(&self.unsigned).map_err(|_| ActionError::InvalidPlan)?;
        verify_commitment(PLAN_DIGEST_DOMAIN, &bytes, key, &self.plan_digest)
            .map_err(|_| ActionError::PlanDigestMismatch)
    }

    pub fn confirm(&self, supplied_digest: &str) -> Result<(), ActionError> {
        if supplied_digest.len() != 64 || supplied_digest != self.plan_digest {
            return Err(ActionError::ConfirmationMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    Planned,
    IntentDurable,
    Executing,
    Succeeded,
    Conflicted,
    Rejected,
    UnknownOutcome,
    ReconciledSucceeded,
    ReconciledNotApplied,
    ManualIntervention,
}

impl ActionState {
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Planned, Self::IntentDurable)
                | (Self::IntentDurable, Self::Executing)
                | (Self::IntentDurable, Self::ReconciledNotApplied)
                | (
                    Self::Executing,
                    Self::Succeeded | Self::Conflicted | Self::Rejected | Self::UnknownOutcome
                )
                | (
                    Self::UnknownOutcome,
                    Self::ReconciledSucceeded
                        | Self::ReconciledNotApplied
                        | Self::ManualIntervention
                )
        )
    }
}

impl SanitizedResultCode {
    #[must_use]
    pub fn matches_state(self, state: ActionState) -> bool {
        matches!(
            (self, state),
            (Self::IntentRecorded, ActionState::IntentDurable)
                | (Self::DispatchStarted, ActionState::Executing)
                | (Self::Applied, ActionState::Succeeded)
                | (Self::RevisionConflict, ActionState::Conflicted)
                | (Self::Rejected, ActionState::Rejected)
                | (Self::OutcomeUnknown, ActionState::UnknownOutcome)
                | (Self::ReconciledApplied, ActionState::ReconciledSucceeded)
                | (
                    Self::ReconciledNotApplied,
                    ActionState::ReconciledNotApplied
                )
                | (
                    Self::ManualInterventionRequired,
                    ActionState::ManualIntervention
                )
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SanitizedResultCode {
    IntentRecorded,
    DispatchStarted,
    Applied,
    RevisionConflict,
    Rejected,
    OutcomeUnknown,
    ReconciledApplied,
    ReconciledNotApplied,
    ManualInterventionRequired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnsignedAuditRecord {
    pub schema_version: String,
    pub sequence: u64,
    pub action_id: String,
    pub source_id: SourceId,
    pub entity_id: EntityId,
    pub operation: ActionOperation,
    pub plan_digest: String,
    pub state: ActionState,
    pub result_code: SanitizedResultCode,
    pub timestamp_ms: i64,
    pub previous_commitment: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    #[serde(flatten)]
    pub unsigned: UnsignedAuditRecord,
    pub commitment: String,
}

impl AuditRecord {
    pub fn sign(unsigned: UnsignedAuditRecord, key: &[u8]) -> Result<Self, ActionError> {
        validate_audit_record(&unsigned)?;
        let bytes = serde_json::to_vec(&unsigned).map_err(|_| ActionError::InvalidAudit)?;
        let commitment = keyed_commitment(AUDIT_COMMITMENT_DOMAIN, &bytes, key)?;
        Ok(Self {
            unsigned,
            commitment,
        })
    }

    pub fn verify(&self, key: &[u8]) -> Result<(), ActionError> {
        validate_audit_record(&self.unsigned)?;
        let bytes = serde_json::to_vec(&self.unsigned).map_err(|_| ActionError::InvalidAudit)?;
        verify_commitment(AUDIT_COMMITMENT_DOMAIN, &bytes, key, &self.commitment)
            .map_err(|_| ActionError::AuditTampered)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActionTargetState {
    pub source_id: SourceId,
    pub entity_id: EntityId,
    pub revision: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovedAction {
    pub plan: ActionPlan,
    pub supplied_rename: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionExecutionResult {
    Succeeded,
    Conflicted,
    Rejected,
    UnknownOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionReconciliation {
    Succeeded,
    NotApplied,
    ManualIntervention,
}

pub trait AgentActionAdapter: Send + Sync {
    fn action_capabilities(
        &self,
        source_id: &SourceId,
    ) -> Result<Vec<ActionCapability>, ActionError>;
    fn observe_target(
        &self,
        source_id: &SourceId,
        entity_id: &EntityId,
    ) -> Result<ActionTargetState, ActionError>;
    fn execute(&self, approved: &ApprovedAction) -> Result<ActionExecutionResult, ActionError>;
    fn reconcile(
        &self,
        action_id: &str,
        idempotency_key: &str,
    ) -> Result<ActionReconciliation, ActionError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ActionError {
    #[error("unsupported action capability")]
    Unsupported(UnsupportedReason),
    #[error("invalid action plan")]
    InvalidPlan,
    #[error("action plan schema is unsupported")]
    UnsupportedPlanSchema,
    #[error("action plan is expired")]
    PlanExpired,
    #[error("action plan digest does not match")]
    PlanDigestMismatch,
    #[error("action confirmation does not match the full plan digest")]
    ConfirmationMismatch,
    #[error("invalid action arguments")]
    InvalidArguments,
    #[error("action argument commitment does not match")]
    ArgumentCommitmentMismatch,
    #[error("invalid action audit record")]
    InvalidAudit,
    #[error("action audit schema is unsupported")]
    UnsupportedAuditSchema,
    #[error("action audit record was tampered with")]
    AuditTampered,
    #[error("action cryptographic commitment failed")]
    Commitment,
    #[error("action state root overlaps the Agent data root")]
    StateRootOverlap,
    #[error("action state root has unsafe permissions or type")]
    UnsafeStateRoot,
    #[error("action state is missing")]
    MissingState,
    #[error("action ownership marker is missing or invalid")]
    InvalidOwnershipMarker,
    #[error("another Action writer holds the lock")]
    LockHeld,
    #[error("action state changed during the operation")]
    StateChanged,
    #[error("stored action plan is invalid")]
    InvalidStoredPlan,
    #[error("action audit limit exceeded")]
    AuditLimitExceeded,
    #[error("action state I/O failed")]
    Io(#[from] std::io::Error),
    #[error("action platform operation failed")]
    Platform(#[from] rustix::io::Errno),
}

fn required_access(operation: ActionOperation) -> ActionAccess {
    match operation {
        ActionOperation::SessionArchive | ActionOperation::SessionUnarchive => ActionAccess::Safe,
        ActionOperation::SessionRename => ActionAccess::Content,
    }
}

fn validate_unsigned_plan(plan: &UnsignedActionPlan) -> Result<(), ActionError> {
    if plan.schema_version != ACTION_PLAN_SCHEMA_VERSION {
        return Err(ActionError::UnsupportedPlanSchema);
    }
    for value in [
        plan.action_id.as_str(),
        plan.idempotency_key.as_str(),
        plan.adapter_id.as_str(),
        plan.capability_version.as_str(),
        plan.expected_revision.as_str(),
    ] {
        validate_public_identifier(value)?;
    }
    let ttl = plan
        .expires_at_ms
        .checked_sub(plan.created_at_ms)
        .ok_or(ActionError::InvalidPlan)?;
    if ttl <= 0 || ttl > MAX_PLAN_TTL_MS {
        return Err(ActionError::InvalidPlan);
    }
    match (&plan.operation, &plan.arguments) {
        (ActionOperation::SessionRename, ActionArguments::RenameCommitment { .. })
        | (
            ActionOperation::SessionArchive | ActionOperation::SessionUnarchive,
            ActionArguments::None,
        ) => Ok(()),
        _ => Err(ActionError::InvalidArguments),
    }
}

fn validate_audit_record(record: &UnsignedAuditRecord) -> Result<(), ActionError> {
    if record.schema_version != ACTION_AUDIT_SCHEMA_VERSION {
        return Err(ActionError::UnsupportedAuditSchema);
    }
    validate_public_identifier(&record.action_id)?;
    if record.plan_digest.len() != 64
        || record
            .previous_commitment
            .as_ref()
            .is_some_and(|value| value.len() != 64)
    {
        return Err(ActionError::InvalidAudit);
    }
    Ok(())
}

fn validate_title(title: &str) -> Result<(), ActionError> {
    if title.is_empty()
        || title.len() > 4_096
        || title.chars().any(char::is_control)
        || title.trim() != title
    {
        return Err(ActionError::InvalidArguments);
    }
    Ok(())
}

fn validate_public_identifier(value: &str) -> Result<(), ActionError> {
    if value.is_empty()
        || value.len() > 512
        || value.chars().any(char::is_control)
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(ActionError::InvalidPlan);
    }
    Ok(())
}

fn keyed_commitment(domain: &[u8], bytes: &[u8], key: &[u8]) -> Result<String, ActionError> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| ActionError::Commitment)?;
    mac.update(domain);
    mac.update(bytes);
    Ok(hex(&mac.finalize().into_bytes()))
}

fn verify_commitment(
    domain: &[u8],
    bytes: &[u8],
    key: &[u8],
    expected: &str,
) -> Result<(), ActionError> {
    let expected = decode_hex(expected).ok_or(ActionError::Commitment)?;
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| ActionError::Commitment)?;
    mac.update(domain);
    mac.update(bytes);
    mac.verify_slice(&expected)
        .map_err(|_| ActionError::Commitment)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|chunk| {
            let text = std::str::from_utf8(chunk).ok()?;
            u8::from_str_radix(text, 16).ok()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"synthetic-phase-five-signing-key";

    fn unsigned(operation: ActionOperation, arguments: ActionArguments) -> UnsignedActionPlan {
        UnsignedActionPlan {
            schema_version: ACTION_PLAN_SCHEMA_VERSION.to_string(),
            action_id: "synthetic-action-0001".to_string(),
            idempotency_key: "synthetic-idempotency-0001".to_string(),
            adapter_id: "synthetic-official-channel".to_string(),
            capability_version: "synthetic-capability-v1".to_string(),
            source_id: SourceId::new("synthetic-source-opaque"),
            entity_id: EntityId::new("synthetic-entity-opaque"),
            operation,
            arguments,
            expected_revision: "synthetic-revision-0001".to_string(),
            created_at_ms: 1_000,
            expires_at_ms: 1_000 + DEFAULT_PLAN_TTL_MS,
        }
    }

    #[test]
    fn capability_admission_fails_closed_without_every_required_property() {
        let base = OfficialChannelEvidence {
            official_channel_id: "synthetic-official-api".to_string(),
            official_channel_version: "v1".to_string(),
            target_binding: Some("opaque-entity".to_string()),
            atomic_precondition: Some("if-revision".to_string()),
            idempotency_mechanism: Some("idempotency-key".to_string()),
            outcome_lookup: None,
            stable_result_mapping: Some("typed-result".to_string()),
            disposable_profile: Some("synthetic-profile".to_string()),
            inverse_operation: true,
        };
        assert!(
            ActionCapability::admit(ActionOperation::SessionArchive, "v1", base.clone(), true)
                .expect("complete official evidence admits")
                .is_supported()
        );
        let mut missing_atomic = base.clone();
        missing_atomic.atomic_precondition = None;
        assert!(matches!(
            ActionCapability::admit(ActionOperation::SessionArchive, "v1", missing_atomic, false),
            Err(ActionError::Unsupported(
                UnsupportedReason::AtomicPreconditionUnavailable
            ))
        ));
        let mut no_outcome = base;
        no_outcome.idempotency_mechanism = None;
        assert!(matches!(
            ActionCapability::admit(ActionOperation::SessionArchive, "v1", no_outcome, false),
            Err(ActionError::Unsupported(
                UnsupportedReason::IdempotencyAndOutcomeUnavailable
            ))
        ));
    }

    #[test]
    fn plan_digest_binds_every_field_and_requires_the_full_digest() {
        let plan = ActionPlan::sign(
            unsigned(ActionOperation::SessionArchive, ActionArguments::None),
            KEY,
        )
        .expect("plan signs");
        plan.verify(KEY, 2_000).expect("plan verifies");
        plan.confirm(&plan.plan_digest)
            .expect("full digest confirms");
        assert!(plan.confirm(&plan.plan_digest[..16]).is_err());

        let mut changed = plan.clone();
        changed.unsigned.expected_revision = "synthetic-revision-0002".to_string();
        assert!(matches!(
            changed.verify(KEY, 2_000),
            Err(ActionError::PlanDigestMismatch)
        ));
        assert!(matches!(
            plan.verify(KEY, plan.unsigned.expires_at_ms),
            Err(ActionError::PlanExpired)
        ));
    }

    #[test]
    fn rename_commitment_and_audit_never_serialize_plaintext() {
        let sensitive = "Synthetic private rename value";
        let arguments = ActionArguments::rename(sensitive, KEY).expect("rename commitment builds");
        arguments
            .verify_rename(sensitive, KEY)
            .expect("matching rename verifies");
        assert!(
            arguments
                .verify_rename("Synthetic other value", KEY)
                .is_err()
        );
        let plan = ActionPlan::sign(unsigned(ActionOperation::SessionRename, arguments), KEY)
            .expect("rename plan signs");
        let encoded = serde_json::to_string(&plan).expect("plan serializes");
        assert!(!encoded.contains(sensitive));

        let audit = AuditRecord::sign(
            UnsignedAuditRecord {
                schema_version: ACTION_AUDIT_SCHEMA_VERSION.to_string(),
                sequence: 1,
                action_id: plan.unsigned.action_id.clone(),
                source_id: plan.unsigned.source_id.clone(),
                entity_id: plan.unsigned.entity_id.clone(),
                operation: plan.unsigned.operation,
                plan_digest: plan.plan_digest.clone(),
                state: ActionState::IntentDurable,
                result_code: SanitizedResultCode::IntentRecorded,
                timestamp_ms: 2_000,
                previous_commitment: None,
            },
            KEY,
        )
        .expect("audit signs");
        audit.verify(KEY).expect("audit verifies");
        let encoded = serde_json::to_string(&audit).expect("audit serializes");
        assert!(!encoded.contains(sensitive));
        let mut tampered = audit;
        tampered.unsigned.sequence = 2;
        assert!(matches!(
            tampered.verify(KEY),
            Err(ActionError::AuditTampered)
        ));
    }

    #[test]
    fn schemas_expiry_and_state_transitions_fail_closed() {
        let mut future = unsigned(ActionOperation::SessionArchive, ActionArguments::None);
        future.schema_version = "aql-action-plan-v2".to_string();
        assert!(matches!(
            ActionPlan::sign(future, KEY),
            Err(ActionError::UnsupportedPlanSchema)
        ));
        let mut excessive = unsigned(ActionOperation::SessionArchive, ActionArguments::None);
        excessive.expires_at_ms = excessive.created_at_ms + MAX_PLAN_TTL_MS + 1;
        assert!(matches!(
            ActionPlan::sign(excessive, KEY),
            Err(ActionError::InvalidPlan)
        ));
        assert!(ActionState::Planned.can_transition_to(ActionState::IntentDurable));
        assert!(ActionState::Executing.can_transition_to(ActionState::UnknownOutcome));
        assert!(ActionState::UnknownOutcome.can_transition_to(ActionState::ReconciledSucceeded));
        assert!(!ActionState::Planned.can_transition_to(ActionState::Succeeded));
        assert!(!ActionState::Succeeded.can_transition_to(ActionState::Planned));
    }
}
