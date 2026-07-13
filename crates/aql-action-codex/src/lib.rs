//! Codex Action capability admission.
//!
//! The observed public Codex surfaces do not expose the atomic precondition and
//! idempotency/outcome protocol required by AQL Phase 5. This Adapter therefore
//! advertises only explicit unsupported capabilities and contains no writer.

use aql_actions::{
    ActionCapability, ActionError, ActionExecutionResult, ActionOperation, ActionReconciliation,
    ActionTargetState, AgentActionAdapter, ApprovedAction, UnsupportedReason,
};
use aql_model::{EntityId, SourceId};

pub const CODEX_ACTION_SURVEY_VERSION: &str = "codex-cli-0.144.1-action-survey-v1";

#[derive(Clone, Copy, Debug, Default)]
pub struct CodexActionAdapter;

impl CodexActionAdapter {
    #[must_use]
    pub fn capabilities_snapshot() -> Vec<ActionCapability> {
        [
            ActionOperation::SessionArchive,
            ActionOperation::SessionUnarchive,
            ActionOperation::SessionRename,
        ]
        .into_iter()
        .map(|operation| {
            ActionCapability::unsupported(
                operation,
                CODEX_ACTION_SURVEY_VERSION,
                UnsupportedReason::AtomicPreconditionUnavailable,
            )
        })
        .collect()
    }

    fn unsupported() -> ActionError {
        ActionError::Unsupported(UnsupportedReason::AtomicPreconditionUnavailable)
    }
}

impl AgentActionAdapter for CodexActionAdapter {
    fn action_capabilities(
        &self,
        _source_id: &SourceId,
    ) -> Result<Vec<ActionCapability>, ActionError> {
        Ok(Self::capabilities_snapshot())
    }

    fn observe_target(
        &self,
        _source_id: &SourceId,
        _entity_id: &EntityId,
    ) -> Result<ActionTargetState, ActionError> {
        Err(Self::unsupported())
    }

    fn execute(&self, _approved: &ApprovedAction) -> Result<ActionExecutionResult, ActionError> {
        Err(Self::unsupported())
    }

    fn reconcile(
        &self,
        _action_id: &str,
        _idempotency_key: &str,
    ) -> Result<ActionReconciliation, ActionError> {
        Err(Self::unsupported())
    }
}

#[cfg(test)]
mod tests {
    use aql_actions::{ActionAccess, CapabilityStatus};

    use super::*;

    #[test]
    fn codex_capability_snapshot_is_explicitly_unsupported() {
        let capabilities = CodexActionAdapter::capabilities_snapshot();
        assert_eq!(capabilities.len(), 3);
        assert_eq!(capabilities[0].operation, ActionOperation::SessionArchive);
        assert_eq!(capabilities[0].required_access, ActionAccess::Safe);
        assert_eq!(capabilities[1].operation, ActionOperation::SessionUnarchive);
        assert_eq!(capabilities[1].required_access, ActionAccess::Safe);
        assert_eq!(capabilities[2].operation, ActionOperation::SessionRename);
        assert_eq!(capabilities[2].required_access, ActionAccess::Content);
        assert!(capabilities.iter().all(|capability| {
            !capability.is_supported()
                && capability.capability_version == CODEX_ACTION_SURVEY_VERSION
                && matches!(
                    capability.status,
                    CapabilityStatus::Unsupported {
                        reason: UnsupportedReason::AtomicPreconditionUnavailable
                    }
                )
        }));
    }

    #[test]
    fn unsupported_codex_adapter_has_no_observe_execute_or_reconcile_path() {
        let adapter = CodexActionAdapter;
        let source = SourceId::new("synthetic-source-opaque");
        let entity = EntityId::new("synthetic-entity-opaque");
        assert!(matches!(
            adapter.observe_target(&source, &entity),
            Err(ActionError::Unsupported(
                UnsupportedReason::AtomicPreconditionUnavailable
            ))
        ));
        assert!(matches!(
            adapter.reconcile("synthetic-action", "synthetic-idempotency"),
            Err(ActionError::Unsupported(
                UnsupportedReason::AtomicPreconditionUnavailable
            ))
        ));
    }
}
