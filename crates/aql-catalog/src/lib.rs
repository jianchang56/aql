//! Source catalog and canonical-record reconciliation.
//!
//! Reconciliation merges observations of the same canonical session while
//! retaining provenance and reporting conflicts instead of silently choosing a
//! source value.

#![deny(missing_docs)]

use std::collections::BTreeMap;

use aql_model::{CanonicalRecord, EntityId, SessionRecord, SnapshotState};

pub use aql_adapter_api as adapter_api;
pub use aql_model as model;

/// Stable category for a reconciliation warning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogWarningKind {
    /// Two observations disagree about a canonical field.
    FieldConflict,
    /// At least one observation came from a stale source snapshot.
    StaleSnapshot,
}

/// Non-fatal issue detected while reconciling canonical entities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogWarning {
    /// Stable warning category.
    pub kind: CatalogWarningKind,
    /// Canonical entity affected by the warning.
    pub entity_id: EntityId,
    /// Conflicting canonical field, or `None` for entity-wide warnings.
    pub field: Option<String>,
}

/// Sessions and warnings produced by one reconciliation pass.
#[derive(Clone, Debug, PartialEq)]
pub struct ReconcileResult {
    /// One merged record per canonical session ID.
    pub records: Vec<SessionRecord>,
    /// Conflicts and stale-snapshot observations encountered during the pass.
    pub warnings: Vec<CatalogWarning>,
}

/// Stateless reconciler for canonical records from multiple bound sources.
#[derive(Default)]
pub struct Catalog;

impl Catalog {
    /// Reconciles session records by canonical ID and source authority.
    ///
    /// Non-session records are ignored. Conflicting values emit warnings; the
    /// value with the strongest recorded provenance is retained.
    #[must_use]
    pub fn reconcile_sessions(&self, records: Vec<CanonicalRecord>) -> ReconcileResult {
        let mut sessions: BTreeMap<EntityId, SessionRecord> = BTreeMap::new();
        let mut warnings = Vec::new();
        for record in records {
            let CanonicalRecord::Session(incoming) = record else {
                continue;
            };
            if incoming.snapshot_state == SnapshotState::Stale {
                warnings.push(CatalogWarning {
                    kind: CatalogWarningKind::StaleSnapshot,
                    entity_id: incoming.session_id.clone(),
                    field: None,
                });
            }
            match sessions.get_mut(&incoming.session_id) {
                Some(existing) => merge_session(existing, incoming, &mut warnings),
                None => {
                    sessions.insert(incoming.session_id.clone(), incoming);
                }
            }
        }
        ReconcileResult {
            records: sessions.into_values().collect(),
            warnings,
        }
    }
}

fn merge_session(
    existing: &mut SessionRecord,
    incoming: SessionRecord,
    warnings: &mut Vec<CatalogWarning>,
) {
    let existing_authority = authority_map(existing);
    let incoming_authority = authority_map(&incoming);
    let authority = |map: &BTreeMap<String, u8>, field: &str| *map.get(field).unwrap_or(&0);
    merge_field(
        &existing.session_id,
        "title",
        &mut existing.title,
        incoming.title,
        authority(&existing_authority, "title"),
        authority(&incoming_authority, "title"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "preview",
        &mut existing.preview,
        incoming.preview,
        authority(&existing_authority, "preview"),
        authority(&incoming_authority, "preview"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "cwd",
        &mut existing.cwd,
        incoming.cwd,
        authority(&existing_authority, "cwd"),
        authority(&incoming_authority, "cwd"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "project",
        &mut existing.project,
        incoming.project,
        authority(&existing_authority, "project"),
        authority(&incoming_authority, "project"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "model",
        &mut existing.model,
        incoming.model,
        authority(&existing_authority, "model"),
        authority(&incoming_authority, "model"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "provider",
        &mut existing.provider,
        incoming.provider,
        authority(&existing_authority, "provider"),
        authority(&incoming_authority, "provider"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "created_at",
        &mut existing.created_at,
        incoming.created_at,
        authority(&existing_authority, "created_at"),
        authority(&incoming_authority, "created_at"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "updated_at",
        &mut existing.updated_at,
        incoming.updated_at,
        authority(&existing_authority, "updated_at"),
        authority(&incoming_authority, "updated_at"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "archived",
        &mut existing.archived,
        incoming.archived,
        authority(&existing_authority, "archived"),
        authority(&incoming_authority, "archived"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "status",
        &mut existing.status,
        incoming.status,
        authority(&existing_authority, "status"),
        authority(&incoming_authority, "status"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "message_count",
        &mut existing.message_count,
        incoming.message_count,
        authority(&existing_authority, "message_count"),
        authority(&incoming_authority, "message_count"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "tool_call_count",
        &mut existing.tool_call_count,
        incoming.tool_call_count,
        authority(&existing_authority, "tool_call_count"),
        authority(&incoming_authority, "tool_call_count"),
        warnings,
    );
    merge_field(
        &existing.session_id,
        "tokens_used",
        &mut existing.tokens_used,
        incoming.tokens_used,
        authority(&existing_authority, "tokens_used"),
        authority(&incoming_authority, "tokens_used"),
        warnings,
    );
    existing.snapshot_state = match (existing.snapshot_state, incoming.snapshot_state) {
        (SnapshotState::Stale, _) | (_, SnapshotState::Stale) => SnapshotState::Stale,
        (SnapshotState::Weak, _) | (_, SnapshotState::Weak) => SnapshotState::Weak,
        _ => SnapshotState::Consistent,
    };
    for (field, provenance) in incoming.provenance {
        existing
            .provenance
            .entry(field)
            .or_default()
            .extend(provenance);
    }
    merge_extensions(
        &existing.session_id,
        &mut existing.extensions,
        incoming.extensions,
        warnings,
    );
}

/// Extension keys merge per key with the incoming value winning, but a key
/// present on both sides with a different value is a conflict like any other
/// field and must not be overwritten silently.
fn merge_extensions<T: PartialEq>(
    entity_id: &EntityId,
    existing: &mut BTreeMap<String, T>,
    incoming: BTreeMap<String, T>,
    warnings: &mut Vec<CatalogWarning>,
) {
    for (key, value) in incoming {
        if let Some(current) = existing.get(&key)
            && *current != value
        {
            warnings.push(CatalogWarning {
                kind: CatalogWarningKind::FieldConflict,
                entity_id: entity_id.clone(),
                field: Some(format!("extensions.{key}")),
            });
        }
        existing.insert(key, value);
    }
}

fn merge_field<T: Clone + PartialEq>(
    entity_id: &EntityId,
    field: &str,
    existing: &mut Option<T>,
    incoming: Option<T>,
    existing_authority: u8,
    incoming_authority: u8,
    warnings: &mut Vec<CatalogWarning>,
) {
    match (&*existing, incoming) {
        (None, Some(value)) => *existing = Some(value),
        (Some(current), Some(value)) if current != &value => {
            warnings.push(CatalogWarning {
                kind: CatalogWarningKind::FieldConflict,
                entity_id: entity_id.clone(),
                field: Some(field.to_string()),
            });
            if incoming_authority > existing_authority {
                *existing = Some(value);
            }
        }
        _ => {}
    }
}

fn authority_map(record: &SessionRecord) -> BTreeMap<String, u8> {
    record
        .provenance
        .iter()
        .map(|(field, items)| {
            let authority = items
                .iter()
                .map(|item| match item.source_kind.as_str() {
                    "state_database" => 30,
                    "session_index" => 20,
                    "rollout" => 10,
                    _ => 0,
                })
                .max()
                .unwrap_or(0);
            (field.clone(), authority)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aql_model::{IdentityConfidence, NativeId, Provenance, SnapshotState, SourceId};
    use chrono::Utc;

    use super::*;

    fn session(source: &str, native: &str, title: &str, source_kind: &str) -> SessionRecord {
        let source_id = SourceId::new(source);
        let session_id = EntityId::from_parts("codex", &source_id, &NativeId::new(native));
        SessionRecord {
            session_id,
            native_id: NativeId::new(native),
            source_id: source_id.clone(),
            agent_id: "codex".to_string(),
            title: Some(title.to_string()),
            preview: None,
            cwd: None,
            project: None,
            model: None,
            provider: None,
            created_at: None,
            updated_at: None,
            status: None,
            archived: Some(false),
            message_count: None,
            tool_call_count: None,
            tokens_used: None,
            identity_confidence: IdentityConfidence::Exact,
            snapshot_state: SnapshotState::Consistent,
            provenance: BTreeMap::from([(
                "title".to_string(),
                vec![Provenance {
                    source_id,
                    source_kind: source_kind.to_string(),
                    source_locator: "fixture".to_string(),
                    source_version: Some("fixture-v0".to_string()),
                    observed_at: Utc::now(),
                    watermark: None,
                    derived: false,
                }],
            )]),
            extensions: BTreeMap::new(),
        }
    }

    #[test]
    fn same_entity_is_reconciled_and_database_title_wins() {
        let index = session(
            "codex:root-a",
            "session-1",
            "Synthetic index",
            "session_index",
        );
        let database = session(
            "codex:root-a",
            "session-1",
            "Synthetic database",
            "state_database",
        );
        let result = Catalog.reconcile_sessions(vec![
            CanonicalRecord::Session(index),
            CanonicalRecord::Session(database),
        ]);
        assert_eq!(result.records.len(), 1);
        assert_eq!(
            result.records[0].title.as_deref(),
            Some("Synthetic database")
        );
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn matching_native_ids_in_different_roots_remain_distinct() {
        let first = session("codex:root-a", "same", "Synthetic A", "state_database");
        let second = session("codex:root-b", "same", "Synthetic B", "state_database");
        let result = Catalog.reconcile_sessions(vec![
            CanonicalRecord::Session(first),
            CanonicalRecord::Session(second),
        ]);
        assert_eq!(result.records.len(), 2);
    }

    #[test]
    fn stale_snapshot_is_preserved_and_reported() {
        let mut stale = session("codex:root-a", "session-1", "Synthetic", "state_database");
        stale.snapshot_state = SnapshotState::Stale;
        let result = Catalog.reconcile_sessions(vec![CanonicalRecord::Session(stale)]);
        assert_eq!(result.records[0].snapshot_state, SnapshotState::Stale);
        assert_eq!(result.warnings[0].kind, CatalogWarningKind::StaleSnapshot);
    }

    #[test]
    fn reconciliation_merges_every_public_session_field() {
        let mut sparse = session("codex:root-a", "session-1", "Synthetic", "session_index");
        sparse.project = None;
        sparse.status = None;
        sparse.message_count = None;
        sparse.tool_call_count = None;
        sparse.tokens_used = None;
        let mut complete = sparse.clone();
        complete.project = Some("synthetic-project".to_string());
        complete.status = Some("completed".to_string());
        complete.message_count = Some(3);
        complete.tool_call_count = Some(2);
        complete.tokens_used = Some(42);

        let result = Catalog.reconcile_sessions(vec![
            CanonicalRecord::Session(sparse),
            CanonicalRecord::Session(complete),
        ]);
        let merged = &result.records[0];
        assert_eq!(merged.project.as_deref(), Some("synthetic-project"));
        assert_eq!(merged.status.as_deref(), Some("completed"));
        assert_eq!(merged.message_count, Some(3));
        assert_eq!(merged.tool_call_count, Some(2));
        assert_eq!(merged.tokens_used, Some(42));
    }

    #[test]
    fn merged_authority_uses_the_highest_observed_provenance() {
        let index = session("codex:root-a", "session-1", "Index", "session_index");
        let database = session("codex:root-a", "session-1", "Database", "state_database");
        let rollout = session("codex:root-a", "session-1", "Rollout", "rollout");

        let result = Catalog.reconcile_sessions(vec![
            CanonicalRecord::Session(index),
            CanonicalRecord::Session(database),
            CanonicalRecord::Session(rollout),
        ]);
        assert_eq!(result.records[0].title.as_deref(), Some("Database"));
    }

    #[test]
    fn conflicting_extension_keys_warn_and_take_the_incoming_value() {
        let mut existing = session("codex:root-a", "session-1", "Synthetic", "state_database");
        existing
            .extensions
            .insert("codex.tone".to_string(), 1_i64.into());
        existing
            .extensions
            .insert("codex.same".to_string(), 7_i64.into());
        let mut incoming = session("codex:root-a", "session-1", "Synthetic", "state_database");
        incoming
            .extensions
            .insert("codex.tone".to_string(), 2_i64.into());
        incoming
            .extensions
            .insert("codex.same".to_string(), 7_i64.into());
        incoming
            .extensions
            .insert("codex.new".to_string(), 3_i64.into());
        let expected_tone = incoming.extensions.get("codex.tone").cloned();

        let result = Catalog.reconcile_sessions(vec![
            CanonicalRecord::Session(existing),
            CanonicalRecord::Session(incoming),
        ]);
        let merged = &result.records[0];
        assert_eq!(
            merged.extensions.get("codex.tone"),
            expected_tone.as_ref(),
            "the incoming value still wins the conflicting key"
        );
        assert!(
            merged.extensions.contains_key("codex.new"),
            "disjoint keys merge silently"
        );
        assert_eq!(
            result.warnings.len(),
            1,
            "only the differing shared key warns: {:?}",
            result.warnings
        );
        assert_eq!(result.warnings[0].kind, CatalogWarningKind::FieldConflict);
        assert_eq!(
            result.warnings[0].field.as_deref(),
            Some("extensions.codex.tone")
        );
    }
}
