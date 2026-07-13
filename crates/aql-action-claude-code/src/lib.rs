//! Evidence-backed unsupported Action surface for Claude Code 2.x.

use aql_actions::{
    ActionCapability, ActionError, ActionExecutionResult, ActionOperation, ActionReconciliation,
    ActionTargetState, AgentActionAdapter, ApprovedAction, UnsupportedReason,
};
use aql_model::{EntityId, SourceId};

pub const CLAUDE_CODE_ACTION_SURVEY_VERSION: &str = "claude-code-2.x-action-survey-v1";

#[derive(Clone, Copy, Debug, Default)]
pub struct ClaudeCodeActionAdapter;

impl ClaudeCodeActionAdapter {
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
                CLAUDE_CODE_ACTION_SURVEY_VERSION,
                UnsupportedReason::AtomicPreconditionUnavailable,
            )
        })
        .collect()
    }

    fn unsupported() -> ActionError {
        ActionError::Unsupported(UnsupportedReason::AtomicPreconditionUnavailable)
    }
}

impl AgentActionAdapter for ClaudeCodeActionAdapter {
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
    use aql_actions::CapabilityStatus;

    use super::*;

    #[test]
    fn all_claude_mutations_are_versioned_unsupported_without_a_writer() {
        let capabilities = ClaudeCodeActionAdapter::capabilities_snapshot();
        assert_eq!(capabilities.len(), 3);
        assert!(capabilities.iter().all(|capability| {
            capability.capability_version == CLAUDE_CODE_ACTION_SURVEY_VERSION
                && matches!(
                    capability.status,
                    CapabilityStatus::Unsupported {
                        reason: UnsupportedReason::AtomicPreconditionUnavailable
                    }
                )
        }));
    }
}
