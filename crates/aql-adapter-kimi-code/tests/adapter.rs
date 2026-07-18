use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aql_adapter_api::{
    AccessGrant, AdapterError, AgentAdapter, CancellationToken, ColumnName, Literal, Predicate,
    ProbeRequest, PushdownState, ResourceBudget, ScanRequest, TableName,
};
use aql_adapter_kimi_code::KimiCodeAdapter;
use aql_model::{CanonicalRecord, SourceManifest};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn fixtures() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("valid system time")
        .as_nanos();
    let output = std::env::temp_dir().join(format!(
        "aql-kimi-fixtures-{}-{nonce}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    aql_test_support::generate_kimi(&output).expect("fixture generator succeeds");
    output
}

fn manifest(adapter: &KimiCodeAdapter, root: &Path) -> SourceManifest {
    adapter
        .probe(&ProbeRequest {
            data_root: root.to_string_lossy().into_owned(),
        })
        .expect("fixture probes")
        .manifests
        .into_iter()
        .next()
        .expect("manifest exists")
}

fn request(source: SourceManifest, projection: &[&str]) -> ScanRequest {
    ScanRequest {
        source,
        table: TableName::Sessions,
        projection: projection
            .iter()
            .map(|name| ColumnName::new(*name))
            .collect(),
        predicates: Vec::new(),
        limit: None,
        order_hint: Vec::new(),
        access: AccessGrant::default(),
        budget: ResourceBudget::default(),
        cancellation: CancellationToken::default(),
        snapshot: None,
    }
}

fn table_request(source: SourceManifest, table: TableName, projection: &[&str]) -> ScanRequest {
    let mut request = request(source, projection);
    request.table = table;
    request
}

#[test]
fn safe_session_scan_does_not_project_sensitive_state() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let result = adapter
        .scan(request(source, &["session_id", "created_at"]))
        .expect("safe scan starts");
    let records = result
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("scan succeeds");
    assert_eq!(records.len(), 1);
    let CanonicalRecord::Session(session) = &records[0] else {
        panic!("session record expected")
    };
    assert_eq!(session.agent_id, "kimi-code");
    assert_eq!(session.title, None);
    assert_eq!(session.preview, None);
    assert_eq!(session.cwd, None);
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn session_inventory_obeys_the_shared_record_budget() {
    let fixtures = fixtures();
    let root = fixtures.join("minimal");
    fs::create_dir_all(root.join("sessions/wd_workspace_84b61923346a/session-inventory-overflow"))
        .expect("second synthetic session directory");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut scan = request(source, &["session_id"]);
    scan.budget.max_records = 1;
    let mut records = adapter.scan(scan).expect("bounded scan starts").records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::BudgetExceeded { resource, .. }))
            if resource == "kimi_session_inventory"
    ));
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn content_is_rejected_before_state_scan_without_grant() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("minimal"));
    let error = match adapter.scan(request(source, &["title"])) {
        Ok(_) => panic!("content must require a grant"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        AdapterError::AccessDenied {
            column: "title".to_string()
        }
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn missing_index_does_not_hide_self_describing_session() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("missing-index"));
    let result = adapter
        .scan(request(source, &["session_id"]))
        .expect("scan starts");
    assert_eq!(result.records.count(), 1);
    assert!(
        !result
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .is_empty()
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn stale_index_does_not_hide_current_session_and_warns() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("stale-index"));
    let result = adapter
        .scan(request(source, &["session_id"]))
        .expect("scan starts");
    assert_eq!(result.records.count(), 1);
    assert!(
        !result
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .is_empty()
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn symlinked_state_is_rejected() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("symlink-state"));
    let mut records = adapter
        .scan(request(source, &["session_id"]))
        .expect("lazy scan starts")
        .records;
    let error = match records.next() {
        Some(Err(error)) => error,
        _ => panic!("symlink must fail"),
    };
    assert_eq!(
        error,
        AdapterError::UnsupportedFormat {
            stage: "kimi_state_type_or_size".to_string()
        }
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn session_source_is_not_read_until_stream_is_polled() {
    let fixtures = fixtures();
    let root = fixtures.join("minimal");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut records = adapter
        .scan(request(source, &["session_id"]))
        .expect("scan starts")
        .records;
    fs::write(
        root.join("sessions/wd_workspace_84b61923346a/session-minimal/state.json"),
        b"{invalid after scan}",
    )
    .expect("fixture state changes");
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::CorruptSource { .. }))
    ));
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn root_replacement_after_scan_fails_closed() {
    let fixtures = fixtures();
    let root = fixtures.join("root-replacement");
    let replacement = fixtures.join("root-replacement-replacement");
    let displaced = fixtures.join("root-replacement-displaced");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut records = adapter
        .scan(request(source, &["session_id"]))
        .expect("scan starts")
        .records;
    fs::rename(&root, &displaced).expect("original root displaced");
    fs::rename(&replacement, &root).expect("replacement installed");
    assert_eq!(records.next(), Some(Err(AdapterError::SnapshotUnavailable)));
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn sensitive_value_budget_is_checked_before_string_projection() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("huge-sensitive"));
    let mut scan = request(source, &["title"]);
    scan.access.content = true;
    scan.budget.max_single_value_bytes = 32;
    let mut records = adapter.scan(scan).expect("authorized scan starts").records;
    assert_eq!(
        records.next(),
        Some(Err(AdapterError::BudgetExceeded {
            resource: "single_value_bytes".to_string(),
            actual: 70_000,
        }))
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn full_wire_projects_messages_tools_and_usage() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("full"));

    let mut messages = table_request(
        source.clone(),
        TableName::Messages,
        &["message_id", "role", "content"],
    );
    messages.access.content = true;
    let messages = adapter
        .scan(messages)
        .expect("message scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("messages parse");
    assert_eq!(messages.len(), 3);
    assert!(messages.iter().any(|record| matches!(record, CanonicalRecord::Message(message) if message.content.as_deref() == Some("Synthetic answer"))));

    let mut tools = table_request(
        source.clone(),
        TableName::ToolCalls,
        &["tool_call_id", "tool_name", "arguments", "output", "status"],
    );
    tools.access.tool_input = true;
    tools.access.tool_output = true;
    let tools = adapter
        .scan(tools)
        .expect("tool scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("tools parse");
    assert_eq!(tools.len(), 1);
    assert!(
        matches!(&tools[0], CanonicalRecord::ToolCall(call) if call.tool_name == "synthetic_tool" && call.output.as_deref() == Some("Synthetic tool output") && call.status.as_deref() == Some("completed"))
    );

    let usage = adapter
        .scan(table_request(
            source.clone(),
            TableName::Usage,
            &["usage_id", "input_tokens", "output_tokens", "cached_tokens"],
        ))
        .expect("usage scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("usage parses");
    assert_eq!(usage.len(), 1);
    assert!(
        matches!(&usage[0], CanonicalRecord::Usage(value) if value.input_tokens == Some(16) && value.output_tokens == Some(7) && value.cached_tokens == Some(3))
    );

    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn malformed_wire_fails_but_truncated_tail_preserves_prior_records() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let malformed = manifest(&adapter, &fixtures.join("malformed-record"));
    let malformed = adapter
        .scan(table_request(
            malformed,
            TableName::Messages,
            &["message_id"],
        ))
        .expect("lazy malformed scan starts")
        .records
        .collect::<Result<Vec<_>, _>>();
    assert!(matches!(malformed, Err(AdapterError::CorruptSource { .. })));

    let truncated = manifest(&adapter, &fixtures.join("truncated-tail"));
    let result = adapter
        .scan(table_request(
            truncated,
            TableName::Messages,
            &["message_id"],
        ))
        .expect("truncated scan starts");
    assert_eq!(result.records.count(), 0);
    assert!(
        result
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .iter()
            .any(|warning| warning.kind == aql_adapter_api::AdapterWarningKind::TruncatedRecord)
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn wire_boundary_excludes_appends_after_first_record() {
    let fixtures = fixtures();
    let root = fixtures.join("full");
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let mut records = adapter
        .scan(table_request(
            source.clone(),
            TableName::Messages,
            &["message_id", "role"],
        ))
        .expect("message scan starts")
        .records;
    assert!(matches!(
        records.next(),
        Some(Ok(CanonicalRecord::Message(_)))
    ));
    let wire = root.join("sessions/wd_workspace_84b61923346a/session-full/agents/main/wire.jsonl");
    use std::io::Write;
    writeln!(
        fs::OpenOptions::new()
            .append(true)
            .open(wire)
            .expect("wire opens for synthetic append"),
        "{{\"type\":\"context.append_message\",\"time\":1767225606000,\"message\":{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"Synthetic late append\"}}],\"toolCalls\":[]}}}}"
    )
    .expect("synthetic record appended");
    assert_eq!(
        records
            .collect::<Result<Vec<_>, _>>()
            .expect("scan completes")
            .len(),
        2
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn unknown_legacy_and_symlink_wire_have_explicit_outcomes() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());

    let unknown = manifest(&adapter, &fixtures.join("unknown-record"));
    let unknown = adapter
        .scan(table_request(unknown, TableName::Messages, &["message_id"]))
        .expect("unknown scan starts");
    assert_eq!(unknown.records.count(), 0);
    assert!(
        unknown
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .iter()
            .any(|warning| warning.kind == aql_adapter_api::AdapterWarningKind::UnknownEvent)
    );

    let legacy = manifest(&adapter, &fixtures.join("legacy-1.0"));
    let legacy = adapter
        .scan(table_request(
            legacy,
            TableName::Messages,
            &["message_id", "role"],
        ))
        .expect("legacy scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("fixture-backed legacy message migrates");
    assert_eq!(legacy.len(), 1);

    let symlink = manifest(&adapter, &fixtures.join("symlink-wire"));
    let mut symlink = adapter
        .scan(table_request(symlink, TableName::Messages, &["message_id"]))
        .expect("lazy symlink scan starts")
        .records;
    assert!(matches!(
        symlink.next(),
        Some(Err(AdapterError::UnsupportedFormat { .. }))
    ));
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn unprojected_wire_content_is_skipped_and_projected_content_is_bounded() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("huge-wire"));
    let mut safe = table_request(source.clone(), TableName::Messages, &["message_id", "role"]);
    safe.budget.max_single_value_bytes = 32;
    assert_eq!(
        adapter
            .scan(safe)
            .expect("safe scan starts")
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect("unprojected content is skipped")
            .len(),
        1
    );

    let mut content = table_request(source, TableName::Messages, &["content"]);
    content.access.content = true;
    content.budget.max_single_value_bytes = 32;
    let mut records = adapter.scan(content).expect("content scan starts").records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::BudgetExceeded { .. }))
    ));
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn subagent_is_a_namespaced_child_session_and_owns_its_messages() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("subagent"));
    let sessions = adapter
        .scan(request(
            source.clone(),
            &["session_id", "native_id", "status"],
        ))
        .expect("session scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("sessions parse");
    assert_eq!(sessions.len(), 2);
    let child_id = sessions
        .iter()
        .find_map(|record| match record {
            CanonicalRecord::Session(session) if session.status.as_deref() == Some("sub") => {
                Some(session.session_id.clone())
            }
            _ => None,
        })
        .expect("subagent session exists");
    let messages = adapter
        .scan(table_request(
            source.clone(),
            TableName::Messages,
            &["message_id", "session_id", "role"],
        ))
        .expect("message scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("messages parse");
    assert_eq!(messages.len(), 2);
    assert!(
        matches!(&messages[0], CanonicalRecord::Message(message) if message.session_id != child_id)
    );
    assert!(
        matches!(&messages[1], CanonicalRecord::Message(message) if message.session_id == child_id)
    );
    let edges = adapter
        .scan(table_request(
            source,
            TableName::SessionEdges,
            &[
                "edge_id",
                "parent_session_id",
                "child_session_id",
                "edge_kind",
            ],
        ))
        .expect("edge scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("edges parse");
    assert_eq!(edges.len(), 1);
    assert!(
        matches!(&edges[0], CanonicalRecord::SessionEdge(edge) if edge.child_session_id == child_id && edge.edge_kind == "subagent" && edge.parent_session_id != edge.child_session_id)
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn wire_limit_cancel_and_unpaired_tools_are_bounded_and_typed() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("full"));
    let mut limited = table_request(source.clone(), TableName::Messages, &["message_id"]);
    limited.limit = Some(1);
    let limited_budget = limited.budget.clone();
    assert_eq!(
        adapter
            .scan(limited)
            .expect("limited scan starts")
            .records
            .count(),
        1
    );
    let mut full = table_request(source, TableName::Messages, &["message_id"]);
    let full_budget = full.budget.clone();
    full.limit = None;
    assert_eq!(
        adapter
            .scan(full)
            .expect("full scan starts")
            .records
            .count(),
        3
    );
    assert!(limited_budget.bytes_read_used() < full_budget.bytes_read_used());

    let source = manifest(&adapter, &fixtures.join("full"));
    let cancelled = table_request(source, TableName::Messages, &["message_id"]);
    cancelled.cancellation.cancel();
    assert!(matches!(
        adapter.scan(cancelled),
        Err(AdapterError::Cancelled)
    ));

    let source = manifest(&adapter, &fixtures.join("unpaired-tools"));
    let result = adapter
        .scan(table_request(
            source,
            TableName::ToolCalls,
            &["tool_call_id", "status"],
        ))
        .expect("unpaired scan starts");
    let records = result
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("unpaired scan degrades");
    assert!(
        matches!(&records[0], CanonicalRecord::ToolCall(call) if call.status.as_deref() == Some("interrupted"))
    );
    assert!(
        result
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .iter()
            .any(|warning| warning.stage == "unpaired_tool_result")
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn symlinked_intermediate_directories_cannot_escape_the_allowlist() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    assert!(matches!(
        adapter.probe(&ProbeRequest {
            data_root: fixtures
                .join("symlink-sessions-dir")
                .to_string_lossy()
                .into_owned(),
        }),
        Err(AdapterError::UnsupportedFormat { .. })
    ));

    let source = manifest(&adapter, &fixtures.join("symlink-agent-dir"));
    let mut records = adapter
        .scan(table_request(source, TableName::Messages, &["message_id"]))
        .expect("lazy scan starts")
        .records;
    assert_eq!(records.next(), Some(Err(AdapterError::SnapshotUnavailable)));
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn multi_session_and_invalid_state_fixtures_have_closed_outcomes() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let multi = manifest(&adapter, &fixtures.join("multi-session"));
    assert_eq!(
        adapter
            .scan(request(multi, &["session_id"]))
            .expect("multi session scan starts")
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect("multi sessions parse")
            .len(),
        2
    );
    for fixture in ["missing-state", "invalid-state"] {
        let source = manifest(&adapter, &fixtures.join(fixture));
        let mut records = adapter
            .scan(request(source, &["session_id"]))
            .expect("lazy invalid scan starts")
            .records;
        assert!(matches!(records.next(), Some(Err(_))));
    }
    let mismatched = manifest(&adapter, &fixtures.join("mismatched-bucket"));
    let mut records = adapter
        .scan(request(mismatched, &["session_id"]))
        .expect("lazy mismatch scan starts")
        .records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::CorruptSource { .. }))
    ));
    let missing_workdir = manifest(&adapter, &fixtures.join("missing-workdir"));
    let result = adapter
        .scan(request(missing_workdir, &["session_id", "snapshot_state"]))
        .expect("missing workdir degrades");
    let records = result
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("safe identity remains queryable");
    assert!(
        matches!(&records[0], CanonicalRecord::Session(session) if session.cwd.is_none() && session.snapshot_state == aql_model::SnapshotState::Stale)
    );
    assert!(
        result
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .iter()
            .any(|warning| warning.stage == "missing_workdir_authority")
    );
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn exact_identity_predicate_pushes_limit_and_unsupported_predicate_does_not() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.join("multi-session"));
    let mut scan = request(source, &["session_id", "native_id"]);
    scan.predicates.push(Predicate::Eq(
        ColumnName::new("native_id"),
        Literal::Text("session-beta".to_string()),
    ));
    scan.limit = Some(1);
    let result = adapter.scan(scan).expect("scan starts");
    assert_eq!(result.pushdown.predicates, vec![PushdownState::Exact]);
    assert_eq!(result.pushdown.limit, Some(PushdownState::Exact));
    assert_eq!(
        result
            .records
            .collect::<Result<Vec<_>, _>>()
            .expect("adapter filters exact identity")
            .len(),
        1
    );

    let source = manifest(&adapter, &fixtures.join("multi-session"));
    let mut scan = request(source, &["session_id"]);
    scan.predicates
        .push(Predicate::Unsupported("synthetic".to_string()));
    scan.limit = Some(1);
    let result = adapter.scan(scan).expect("unsupported scan starts");
    assert_eq!(result.pushdown.predicates, vec![PushdownState::Unsupported]);
    assert_eq!(result.pushdown.limit, Some(PushdownState::Unsupported));
    assert_eq!(result.records.count(), 2);
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}

#[test]
fn future_or_missing_wire_protocol_fails_closed_and_writable_root_is_rejected() {
    let fixtures = fixtures();
    let adapter = KimiCodeAdapter::new(b"fixture-salt".to_vec());
    for fixture in ["future-protocol", "missing-protocol"] {
        let source = manifest(&adapter, &fixtures.join(fixture));
        let mut records = adapter
            .scan(table_request(source, TableName::Messages, &["message_id"]))
            .expect("lazy protocol scan starts")
            .records;
        assert!(matches!(
            records.next(),
            Some(Err(AdapterError::UnsupportedFormat { .. }))
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let root = fixtures.join("minimal");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o777))
            .expect("make synthetic root unsafe");
        assert!(matches!(
            adapter.probe(&ProbeRequest {
                data_root: root.to_string_lossy().into_owned(),
            }),
            Err(AdapterError::PermissionDenied { .. })
        ));
    }
    fs::remove_dir_all(fixtures).expect("fixtures removed");
}
