use super::*;

pub(super) fn execute_action_command(
    command: ActionCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ActionCommand::Capabilities {
            source_id,
            synthetic_channel_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), None)?;
            let source_id = SourceId::new(source_id);
            let adapter = action_adapter(
                &source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::None,
            )?;
            let capabilities = adapter.action_capabilities(&source_id)?;
            ensure_action_deadline(deadline)?;
            match output {
                ActionOutput::Text => {
                    for capability in capabilities {
                        println!("operation={}", capability.operation);
                        println!("capability_version={}", capability.capability_version);
                        println!(
                            "required_access={}",
                            match capability.required_access {
                                aql_actions::ActionAccess::Safe => "safe",
                                aql_actions::ActionAccess::Content => "content",
                            }
                        );
                        println!("reversible={}", capability.reversible);
                        match capability.status {
                            CapabilityStatus::Supported {
                                official_channel_id,
                                official_channel_version,
                                ..
                            } => {
                                println!("status=supported");
                                println!("official_channel_id={official_channel_id}");
                                println!("official_channel_version={official_channel_version}");
                            }
                            CapabilityStatus::Unsupported { reason } => {
                                println!("status=unsupported");
                                println!("reason={}", unsupported_reason_name(reason));
                            }
                        }
                    }
                }
                ActionOutput::Json => println!("{}", serde_json::to_string(&capabilities)?),
            }
        }
        ActionCommand::Plan {
            data_root,
            source_id,
            entity_id,
            operation,
            new_title,
            access,
            ttl,
            synthetic_channel_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            let operation = ActionOperation::from(operation);
            validate_action_arguments(operation, new_title.as_deref(), &access)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), Some(&data_root))?;
            let source_id = SourceId::new(source_id);
            let entity_id = EntityId::new(entity_id);
            let adapter = action_adapter(
                &source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::None,
            )?;
            let capability = supported_capability(&*adapter, &source_id, operation)?;
            let observed = adapter.observe_target(&source_id, &entity_id)?;
            ensure_action_deadline(deadline)?;
            if observed.source_id != source_id || observed.entity_id != entity_id {
                return Err("Action Adapter returned a mismatched target binding".into());
            }
            let ttl_ms = i64::try_from(ttl.as_millis())?;
            if ttl_ms <= 0 || ttl_ms > MAX_PLAN_TTL_MS {
                return Err("Action plan TTL must be between 1ms and 1h".into());
            }
            let signing_key = installation_salt()?;
            let now_ms = unix_time_ms()?;
            let arguments = match operation {
                ActionOperation::SessionRename => ActionArguments::rename(
                    new_title.as_deref().ok_or("rename requires --new-title")?,
                    &signing_key,
                )?,
                ActionOperation::SessionArchive | ActionOperation::SessionUnarchive => {
                    ActionArguments::None
                }
            };
            let action_id = format!("action-{:032x}", rand::random::<u128>());
            let plan = ActionPlan::sign(
                UnsignedActionPlan {
                    schema_version: ACTION_PLAN_SCHEMA_VERSION.to_string(),
                    action_id: action_id.clone(),
                    idempotency_key: format!("idempotency-{:032x}", rand::random::<u128>()),
                    adapter_id: capability_channel_id(&capability)?.to_string(),
                    capability_version: capability.capability_version,
                    source_id,
                    entity_id,
                    operation,
                    arguments,
                    expected_revision: observed.revision.clone(),
                    created_at_ms: now_ms,
                    expires_at_ms: now_ms
                        .checked_add(ttl_ms)
                        .ok_or("Action plan expiry overflow")?,
                },
                &signing_key,
            )?;
            let state_root = aql_state_root()?;
            ensure_action_deadline(deadline)?;
            let store = ActionStore::create(&state_root, &data_root)?;
            let lock = store.acquire_write_lock()?;
            let _ = store.publish_plan(&lock, plan.clone(), now_ms, &signing_key)?;
            lock.release()?;
            render_action_plan_summary(&plan, &observed.revision, &signing_key, output)?;
        }
        ActionCommand::Apply {
            data_root,
            action_id,
            confirm,
            new_title,
            access,
            synthetic_channel_root,
            synthetic_fault,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), Some(&data_root))?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let lock = store.acquire_write_lock()?;
            let stored = store.load_plan(&action_id)?;
            let now_ms = unix_time_ms()?;
            stored.plan.verify(&signing_key, now_ms)?;
            stored.plan.confirm(&confirm)?;
            validate_action_arguments(
                stored.plan.unsigned.operation,
                new_title.as_deref(),
                &access,
            )?;
            if stored.plan.unsigned.operation == ActionOperation::SessionRename {
                stored.plan.unsigned.arguments.verify_rename(
                    new_title.as_deref().ok_or("rename requires --new-title")?,
                    &signing_key,
                )?;
            }
            let _ = store.recover_incomplete_audit_tail(&lock, &signing_key)?;
            if store
                .latest_audit_for_action(&action_id, &signing_key)?
                .is_some()
            {
                return Err(
                    "Action plan has already been consumed; inspect or reconcile it".into(),
                );
            }
            let adapter = action_adapter(
                &stored.plan.unsigned.source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::from(synthetic_fault),
            )?;
            let capability = supported_capability(
                &*adapter,
                &stored.plan.unsigned.source_id,
                stored.plan.unsigned.operation,
            )?;
            if capability.capability_version != stored.plan.unsigned.capability_version
                || capability_channel_id(&capability)? != stored.plan.unsigned.adapter_id
            {
                return Err("Action capability changed after planning".into());
            }
            let observed = adapter.observe_target(
                &stored.plan.unsigned.source_id,
                &stored.plan.unsigned.entity_id,
            )?;
            if observed.revision != stored.plan.unsigned.expected_revision {
                return Err("Action target revision changed; create a new plan".into());
            }
            ensure_action_deadline(deadline)?;
            store.append_audit_transition(
                &lock,
                &stored.plan,
                ActionState::IntentDurable,
                SanitizedResultCode::IntentRecorded,
                now_ms,
                &signing_key,
            )?;
            if matches!(synthetic_fault, SyntheticFaultArg::DelayBeforeDispatch) {
                std::thread::sleep(Duration::from_millis(250));
            }
            if Instant::now() >= deadline {
                store.append_audit_transition(
                    &lock,
                    &stored.plan,
                    ActionState::ReconciledNotApplied,
                    SanitizedResultCode::ReconciledNotApplied,
                    unix_time_ms()?,
                    &signing_key,
                )?;
                lock.release()?;
                return Err("Action timed out before dispatch".into());
            }
            store.append_audit_transition(
                &lock,
                &stored.plan,
                ActionState::Executing,
                SanitizedResultCode::DispatchStarted,
                unix_time_ms()?,
                &signing_key,
            )?;
            let approved = ApprovedAction {
                plan: stored.plan.clone(),
                supplied_rename: new_title,
            };
            let result = match adapter.execute(&approved) {
                Ok(result) => result,
                Err(_) => ActionExecutionResult::UnknownOutcome,
            };
            let (state, code) = execution_audit(result);
            if let Err(error) = store.append_audit_transition(
                &lock,
                &stored.plan,
                state,
                code,
                unix_time_ms()?,
                &signing_key,
            ) {
                return Err(format!(
                    "Action outcome is unknown because durable outcome recording failed: {error}"
                )
                .into());
            }
            lock.release()?;
            render_action_result(&action_id, state, output)?;
        }
        ActionCommand::Inspect {
            data_root,
            action_id,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let stored = store.load_plan(&action_id)?;
            stored.plan.verify_digest(&signing_key)?;
            let latest = store.latest_audit_for_action(&action_id, &signing_key)?;
            ensure_action_deadline(deadline)?;
            let state = latest
                .as_ref()
                .map_or(ActionState::Planned, |record| record.unsigned.state);
            render_action_inspect(
                &stored.plan,
                externally_visible_action_state(state),
                &signing_key,
                output,
            )?;
        }
        ActionCommand::Reconcile {
            data_root,
            action_id,
            synthetic_channel_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            validate_synthetic_root_isolation(synthetic_channel_root.as_deref(), Some(&data_root))?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let lock = store.acquire_write_lock()?;
            let stored = store.load_plan(&action_id)?;
            stored.plan.verify_digest(&signing_key)?;
            let _ = store.recover_incomplete_audit_tail(&lock, &signing_key)?;
            let latest = store
                .latest_audit_for_action(&action_id, &signing_key)?
                .ok_or("Action has no durable intent to reconcile")?;
            let mut state = latest.unsigned.state;
            if matches!(
                state,
                ActionState::Succeeded
                    | ActionState::Conflicted
                    | ActionState::Rejected
                    | ActionState::ReconciledSucceeded
                    | ActionState::ReconciledNotApplied
                    | ActionState::ManualIntervention
            ) {
                lock.release()?;
                return render_action_result(&action_id, state, output);
            }
            if state == ActionState::Executing {
                store.append_audit_transition(
                    &lock,
                    &stored.plan,
                    ActionState::UnknownOutcome,
                    SanitizedResultCode::OutcomeUnknown,
                    unix_time_ms()?,
                    &signing_key,
                )?;
                state = ActionState::UnknownOutcome;
            }
            ensure_action_deadline(deadline)?;
            let adapter = action_adapter(
                &stored.plan.unsigned.source_id,
                synthetic_channel_root.as_deref(),
                SyntheticFault::None,
            )?;
            let reconciliation = adapter.reconcile(
                &stored.plan.unsigned.action_id,
                &stored.plan.unsigned.idempotency_key,
            )?;
            let (final_state, code) = reconciliation_audit(state, reconciliation)?;
            store.append_audit_transition(
                &lock,
                &stored.plan,
                final_state,
                code,
                unix_time_ms()?,
                &signing_key,
            )?;
            lock.release()?;
            render_action_result(&action_id, final_state, output)?;
        }
        ActionCommand::AuditVerify {
            data_root,
            output,
            timeout,
        } => {
            let deadline = action_deadline(timeout)?;
            let signing_key = installation_salt()?;
            let state_root = aql_state_root()?;
            let store = ActionStore::open_existing(&state_root, &data_root)?;
            let records = store.verify_audit(&signing_key)?;
            ensure_action_deadline(deadline)?;
            match output {
                ActionOutput::Text => println!("audit=valid\nrecords={records}"),
                ActionOutput::Json => println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "audit": "valid",
                        "records": records,
                    }))?
                ),
            }
        }
    }
    Ok(())
}

pub(super) fn action_adapter(
    source_id: &SourceId,
    synthetic_channel_root: Option<&std::path::Path>,
    fault: SyntheticFault,
) -> Result<Box<dyn AgentActionAdapter>, Box<dyn std::error::Error>> {
    Ok(match synthetic_channel_root {
        Some(root) => Box::new(SyntheticActionAdapter::open(root)?.with_fault(fault)),
        None if source_id.as_str().starts_with("claude-code:") => Box::new(ClaudeCodeActionAdapter),
        None if source_id.as_str().starts_with("kimi-code:") => Box::new(KimiCodeActionAdapter),
        None if source_id.as_str().starts_with("opencode:") => Box::new(OpenCodeActionAdapter),
        None => Box::new(CodexActionAdapter),
    })
}

pub(super) fn action_deadline(timeout: Duration) -> Result<Instant, Box<dyn std::error::Error>> {
    Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| "Action timeout is too large".into())
}

pub(super) fn ensure_action_deadline(deadline: Instant) -> Result<(), Box<dyn std::error::Error>> {
    if Instant::now() >= deadline {
        Err("Action timed out before dispatch".into())
    } else {
        Ok(())
    }
}

pub(super) fn validate_synthetic_root_isolation(
    synthetic_channel_root: Option<&std::path::Path>,
    data_root: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(synthetic_root) = synthetic_channel_root else {
        return Ok(());
    };
    let synthetic_root = synthetic_root.canonicalize()?;
    let state_root = canonical_or_prospective(&aql_state_root()?)?;
    if paths_overlap(&synthetic_root, &state_root) {
        return Err("synthetic Action channel must be isolated from AQL state".into());
    }
    if let Some(data_root) = data_root {
        let data_root = data_root.canonicalize()?;
        if paths_overlap(&synthetic_root, &data_root) {
            return Err("synthetic Action channel must be isolated from Agent data".into());
        }
    }
    Ok(())
}

pub(super) fn canonical_or_prospective(path: &std::path::Path) -> io::Result<PathBuf> {
    match path.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "path has no final component")
            })?;
            Ok(parent.canonicalize()?.join(name))
        }
        Err(error) => Err(error),
    }
}

pub(super) fn paths_overlap(left: &std::path::Path, right: &std::path::Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

pub(super) fn supported_capability(
    adapter: &dyn AgentActionAdapter,
    source_id: &SourceId,
    operation: ActionOperation,
) -> Result<ActionCapability, Box<dyn std::error::Error>> {
    let capability = adapter
        .action_capabilities(source_id)?
        .into_iter()
        .find(|capability| capability.operation == operation)
        .ok_or("Action capability is not declared")?;
    match capability.status {
        CapabilityStatus::Supported { .. } => Ok(capability),
        CapabilityStatus::Unsupported { reason } => Err(format!(
            "Action capability is unsupported: {}",
            unsupported_reason_name(reason)
        )
        .into()),
    }
}

pub(super) fn capability_channel_id(
    capability: &ActionCapability,
) -> Result<&str, Box<dyn std::error::Error>> {
    match &capability.status {
        CapabilityStatus::Supported {
            official_channel_id,
            ..
        } => Ok(official_channel_id),
        CapabilityStatus::Unsupported { .. } => Err("Action capability is unsupported".into()),
    }
}

pub(super) fn validate_action_arguments(
    operation: ActionOperation,
    new_title: Option<&str>,
    access: &[Access],
) -> Result<(), Box<dyn std::error::Error>> {
    match operation {
        ActionOperation::SessionRename => {
            if !access_grant(access).content {
                return Err("session.rename requires --access content".into());
            }
            let title = new_title.ok_or("session.rename requires --new-title")?;
            if title.is_empty()
                || title.len() > 4_096
                || title.chars().any(char::is_control)
                || title.trim() != title
            {
                return Err("session.rename title is invalid".into());
            }
        }
        ActionOperation::SessionArchive | ActionOperation::SessionUnarchive => {
            if new_title.is_some() {
                return Err("--new-title is valid only for session.rename".into());
            }
        }
    }
    Ok(())
}

pub(super) fn execution_audit(result: ActionExecutionResult) -> (ActionState, SanitizedResultCode) {
    match result {
        ActionExecutionResult::Succeeded => (ActionState::Succeeded, SanitizedResultCode::Applied),
        ActionExecutionResult::Conflicted => (
            ActionState::Conflicted,
            SanitizedResultCode::RevisionConflict,
        ),
        ActionExecutionResult::Rejected => (ActionState::Rejected, SanitizedResultCode::Rejected),
        ActionExecutionResult::UnknownOutcome => (
            ActionState::UnknownOutcome,
            SanitizedResultCode::OutcomeUnknown,
        ),
    }
}

pub(super) fn reconciliation_audit(
    prior: ActionState,
    result: ActionReconciliation,
) -> Result<(ActionState, SanitizedResultCode), Box<dyn std::error::Error>> {
    Ok(match (prior, result) {
        (ActionState::UnknownOutcome, ActionReconciliation::Succeeded) => (
            ActionState::ReconciledSucceeded,
            SanitizedResultCode::ReconciledApplied,
        ),
        (
            ActionState::UnknownOutcome | ActionState::IntentDurable,
            ActionReconciliation::NotApplied,
        ) => (
            ActionState::ReconciledNotApplied,
            SanitizedResultCode::ReconciledNotApplied,
        ),
        (ActionState::UnknownOutcome, ActionReconciliation::ManualIntervention) => (
            ActionState::ManualIntervention,
            SanitizedResultCode::ManualInterventionRequired,
        ),
        _ => return Err("Action reconciliation result is inconsistent with audit state".into()),
    })
}

pub(super) fn render_action_plan_summary(
    plan: &ActionPlan,
    revision: &str,
    signing_key: &[u8],
    output: ActionOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    let revision_commitment = installation_scoped_hmac("action-revision-v1", revision, signing_key);
    match output {
        ActionOutput::Text => {
            println!("action_id={}", plan.unsigned.action_id);
            println!("operation={}", plan.unsigned.operation);
            println!("source_id={}", plan.unsigned.source_id);
            println!("entity_id={}", plan.unsigned.entity_id);
            println!("revision_commitment={revision_commitment}");
            println!("expires_at_ms={}", plan.unsigned.expires_at_ms);
            println!("confirm={}", plan.plan_digest);
        }
        ActionOutput::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action_id": plan.unsigned.action_id,
                "operation": plan.unsigned.operation,
                "source_id": plan.unsigned.source_id,
                "entity_id": plan.unsigned.entity_id,
                "revision_commitment": revision_commitment,
                "expires_at_ms": plan.unsigned.expires_at_ms,
                "confirm": plan.plan_digest,
            }))?
        ),
    }
    Ok(())
}

pub(super) fn render_action_inspect(
    plan: &ActionPlan,
    state: ActionState,
    signing_key: &[u8],
    output: ActionOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    let revision_commitment = installation_scoped_hmac(
        "action-revision-v1",
        &plan.unsigned.expected_revision,
        signing_key,
    );
    match output {
        ActionOutput::Text => {
            println!("action_id={}", plan.unsigned.action_id);
            println!("operation={}", plan.unsigned.operation);
            println!("state={}", action_state_name(state));
            println!("revision_commitment={revision_commitment}");
            println!("expires_at_ms={}", plan.unsigned.expires_at_ms);
        }
        ActionOutput::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action_id": plan.unsigned.action_id,
                "operation": plan.unsigned.operation,
                "state": action_state_name(state),
                "revision_commitment": revision_commitment,
                "expires_at_ms": plan.unsigned.expires_at_ms,
            }))?
        ),
    }
    Ok(())
}

pub(super) fn render_action_result(
    action_id: &str,
    state: ActionState,
    output: ActionOutput,
) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        ActionOutput::Text => {
            println!("action_id={action_id}");
            println!("state={}", action_state_name(state));
        }
        ActionOutput::Json => println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "action_id": action_id,
                "state": action_state_name(state),
            }))?
        ),
    }
    Ok(())
}

pub(super) fn action_state_name(state: ActionState) -> &'static str {
    match state {
        ActionState::Planned => "planned",
        ActionState::IntentDurable => "intent_durable",
        ActionState::Executing => "executing",
        ActionState::Succeeded => "succeeded",
        ActionState::Conflicted => "conflicted",
        ActionState::Rejected => "rejected",
        ActionState::UnknownOutcome => "unknown_outcome",
        ActionState::ReconciledSucceeded => "reconciled_succeeded",
        ActionState::ReconciledNotApplied => "reconciled_not_applied",
        ActionState::ManualIntervention => "manual_intervention",
    }
}

pub(super) fn externally_visible_action_state(state: ActionState) -> ActionState {
    if state == ActionState::Executing {
        ActionState::UnknownOutcome
    } else {
        state
    }
}

pub(super) fn unsupported_reason_name(reason: aql_actions::UnsupportedReason) -> &'static str {
    match reason {
        aql_actions::UnsupportedReason::OfficialChannelUndocumented => {
            "official_channel_undocumented"
        }
        aql_actions::UnsupportedReason::TargetBindingUnavailable => "target_binding_unavailable",
        aql_actions::UnsupportedReason::AtomicPreconditionUnavailable => {
            "atomic_precondition_unavailable"
        }
        aql_actions::UnsupportedReason::IdempotencyAndOutcomeUnavailable => {
            "idempotency_and_outcome_unavailable"
        }
        aql_actions::UnsupportedReason::StableResultUnavailable => "stable_result_unavailable",
        aql_actions::UnsupportedReason::DisposableProfileUnavailable => {
            "disposable_profile_unavailable"
        }
        aql_actions::UnsupportedReason::InverseOperationUnavailable => {
            "inverse_operation_unavailable"
        }
    }
}

pub(super) fn unix_time_ms() -> Result<i64, Box<dyn std::error::Error>> {
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
