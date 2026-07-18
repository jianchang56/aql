use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use aql_adapter_api::{
    AccessGrant, AdapterError, AgentAdapter, CancellationToken, ColumnName, ProbeRequest,
    ResourceBudget, ScanRequest, TableName,
};
use aql_adapter_claude_code::ClaudeCodeAdapter;
use aql_model::{CanonicalRecord, SourceManifest};

fn fixtures() -> tempfile::TempDir {
    let output = tempfile::tempdir().expect("temporary fixture directory");
    aql_test_support::generate_claude(output.path()).expect("fixture generator succeeds");
    output
}

fn manifest(adapter: &ClaudeCodeAdapter, root: &Path) -> SourceManifest {
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

fn request(source: SourceManifest, table: TableName, projection: &[&str]) -> ScanRequest {
    ScanRequest {
        source,
        table,
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

#[test]
fn safe_sessions_are_discovered_without_exposing_sensitive_fields() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.path().join("minimal"));
    let records = adapter
        .scan(request(
            source,
            TableName::Sessions,
            &["session_id", "agent_id", "updated_at"],
        ))
        .expect("session scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("session scan succeeds");
    assert_eq!(records.len(), 1);
    let CanonicalRecord::Session(session) = &records[0] else {
        panic!("session expected")
    };
    assert_eq!(session.agent_id, "claude-code");
    assert_eq!(session.title, None);
    assert_eq!(session.preview, None);
    assert_eq!(session.cwd, None);
}

#[test]
fn sensitive_projection_is_rejected_before_transcript_scan() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.path().join("minimal"));
    let error = match adapter.scan(request(source, TableName::Messages, &["content"])) {
        Ok(_) => panic!("content must require a grant"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        AdapterError::AccessDenied {
            column: "content".to_string(),
        }
    );
}

#[test]
fn full_transcript_maps_messages_tools_and_deduplicated_usage() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.path().join("full"));

    let mut messages = request(
        source.clone(),
        TableName::Messages,
        &["message_id", "role", "kind", "content", "content_json"],
    );
    messages.access.content = true;
    let messages = adapter
        .scan(messages)
        .expect("message scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("messages parse");
    assert_eq!(messages.len(), 4);
    assert!(messages.iter().any(|record| matches!(record, CanonicalRecord::Message(message) if message.content.as_deref().is_some_and(|content| content.contains("Synthetic answer")))));
    assert!(messages.iter().all(|record| !matches!(record, CanonicalRecord::Message(message) if message.content.as_deref().is_some_and(|content| content.contains("Synthetic input") || content.contains("Synthetic tool output")))));
    assert!(messages.iter().all(|record| !matches!(record, CanonicalRecord::Message(message) if message.content_json.as_ref().is_some_and(|content| {
        let encoded = content.to_string();
        encoded.contains("Synthetic input") || encoded.contains("Synthetic tool output")
    }))));

    let mut tools = request(
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
        matches!(&tools[0], CanonicalRecord::ToolCall(tool) if tool.tool_name == "synthetic_tool" && tool.output.as_deref() == Some("Synthetic tool output") && tool.status.as_deref() == Some("completed"))
    );

    let usage = adapter
        .scan(request(
            source,
            TableName::Usage,
            &[
                "usage_id",
                "input_tokens",
                "output_tokens",
                "cached_tokens",
                "total_tokens",
            ],
        ))
        .expect("usage scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("usage parses");
    assert_eq!(usage.len(), 2);
    assert!(usage.iter().any(|record| matches!(record, CanonicalRecord::Usage(value) if value.input_tokens == Some(13) && value.output_tokens == Some(7) && value.cached_tokens == Some(3) && value.total_tokens == Some(23))));
}

#[test]
fn direct_agent_transcript_is_a_child_session_with_an_explicit_edge() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.path().join("subagent"));
    let sessions = adapter
        .scan(request(
            source.clone(),
            TableName::Sessions,
            &["session_id", "status"],
        ))
        .expect("session scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("sessions parse");
    assert_eq!(sessions.len(), 2);
    assert!(sessions.iter().any(|record| matches!(record, CanonicalRecord::Session(session) if session.status.as_deref() == Some("subagent"))));

    let edges = adapter
        .scan(request(
            source.clone(),
            TableName::SessionEdges,
            &["parent_session_id", "child_session_id", "edge_kind"],
        ))
        .expect("edge scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("edges parse");
    assert_eq!(edges.len(), 1);
    assert!(
        matches!(&edges[0], CanonicalRecord::SessionEdge(edge) if edge.edge_kind == "subagent" && edge.parent_session_id != edge.child_session_id)
    );

    let messages = adapter
        .scan(request(
            source,
            TableName::Messages,
            &["message_id", "session_id", "role"],
        ))
        .expect("message scan starts")
        .records
        .collect::<Result<Vec<_>, _>>()
        .expect("messages parse");
    assert_eq!(messages.len(), 2);
    let session_ids = messages
        .iter()
        .filter_map(|record| match record {
            CanonicalRecord::Message(message) => Some(message.session_id.clone()),
            _ => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(session_ids.len(), 2);
}

#[test]
fn malformed_complete_record_fails_and_truncated_tail_warns() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let malformed = manifest(&adapter, &fixtures.path().join("malformed"));
    let mut records = adapter
        .scan(request(malformed, TableName::Messages, &["message_id"]))
        .expect("lazy malformed scan starts")
        .records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::CorruptSource { .. }))
    ));

    let truncated = manifest(&adapter, &fixtures.path().join("truncated-tail"));
    let result = adapter
        .scan(request(truncated, TableName::Messages, &["message_id"]))
        .expect("truncated scan starts");
    assert_eq!(result.records.count(), 1);
    assert!(
        !result
            .diagnostics
            .snapshot()
            .expect("diagnostics")
            .is_empty()
    );
}

#[test]
fn identity_mismatch_and_symlink_fail_closed() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let mismatch = manifest(&adapter, &fixtures.path().join("mismatched-session"));
    let mut records = adapter
        .scan(request(mismatch, TableName::Messages, &["message_id"]))
        .expect("lazy mismatch scan starts")
        .records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::CorruptSource { .. }))
    ));

    assert!(matches!(
        adapter.probe(&ProbeRequest {
            data_root: fixtures
                .path()
                .join("symlink-transcript")
                .to_string_lossy()
                .into_owned(),
        }),
        Err(AdapterError::UnsupportedFormat { .. })
    ));
}

#[test]
fn sensitive_values_are_bounded_before_allocation() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &fixtures.path().join("huge-sensitive"));
    let mut scan = request(source, TableName::Messages, &["content"]);
    scan.access.content = true;
    scan.budget.max_single_value_bytes = 32;
    let mut records = adapter.scan(scan).expect("authorized scan starts").records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::BudgetExceeded {
            resource,
            actual: 70_000,
        })) if resource == "single_value_bytes"
    ));
}

#[test]
fn transcript_boundary_excludes_appends_and_root_replacement_is_detected() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let root = fixtures.path().join("active-boundary");
    let source = manifest(&adapter, &root);
    let records = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("scan starts");
    let transcript = find_main(&root);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(transcript)
        .expect("append fixture transcript");
    file.write_all(b"{\"type\":\"future-after-boundary\"}\n")
        .expect("append succeeds");
    assert_eq!(records.records.count(), 2);

    let root = fixtures.path().join("root-replacement");
    let replacement = fixtures.path().join("root-replacement-replacement");
    let displaced = fixtures.path().join("root-replacement-displaced");
    let source = manifest(&adapter, &root);
    let mut records = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("replacement scan starts")
        .records;
    fs::rename(&root, &displaced).expect("root displaced");
    fs::rename(&replacement, &root).expect("replacement installed");
    assert_eq!(records.next(), Some(Err(AdapterError::SnapshotUnavailable)));
}

#[test]
fn project_replacement_duplicate_session_and_future_version_fail_closed() {
    let fixtures = fixtures();
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());

    let root = fixtures.path().join("minimal");
    let source = manifest(&adapter, &root);
    let mut records = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("project replacement scan starts")
        .records;
    let project = root.join("projects/synthetic-project");
    let replacement = root.join("projects/replacement-project");
    fs::create_dir(&replacement).expect("replacement project created");
    let transcript = find_main(&root);
    fs::hard_link(
        &transcript,
        replacement.join(transcript.file_name().expect("transcript name")),
    )
    .expect("replacement keeps transcript identity");
    let displaced = root.join("displaced-project");
    fs::rename(&project, &displaced).expect("project displaced");
    fs::rename(&replacement, &project).expect("replacement project installed");
    assert_eq!(records.next(), Some(Err(AdapterError::SnapshotUnavailable)));

    let duplicate_root = fixtures.path().join("active-boundary");
    let duplicate_project = duplicate_root.join("projects/duplicate-project");
    fs::create_dir(&duplicate_project).expect("duplicate project created");
    let transcript = find_main(&duplicate_root);
    fs::hard_link(
        &transcript,
        duplicate_project.join(transcript.file_name().expect("transcript name")),
    )
    .expect("duplicate transcript linked");
    assert!(matches!(
        adapter.probe(&ProbeRequest {
            data_root: duplicate_root.to_string_lossy().into_owned(),
        }),
        Err(AdapterError::CorruptSource { .. })
    ));

    let version_root = fixtures.path().join("root-replacement-replacement");
    let transcript = find_main(&version_root);
    let contents = fs::read_to_string(&transcript).expect("transcript reads");
    fs::write(&transcript, contents.replace("2.1.207", "3.0.0"))
        .expect("future version fixture written");
    let source = manifest(&adapter, &version_root);
    let mut records = adapter
        .scan(request(source, TableName::Messages, &["message_id"]))
        .expect("future version scan starts")
        .records;
    assert!(matches!(
        records.next(),
        Some(Err(AdapterError::UnsupportedFormat { .. }))
    ));
}

#[test]
fn cancellation_is_observed_mid_file_during_session_summary() {
    let fixtures = fixtures();
    let root = fixtures.path().join("minimal");
    let transcript = find_main(&root);
    let session = transcript
        .file_stem()
        .and_then(|name| name.to_str())
        .expect("session name")
        .to_string();
    let mut contents = String::new();
    for index in 0..40_000_u32 {
        contents.push_str(&format!(
            "{{\"type\":\"user\",\"uuid\":\"{index:08x}-0000-4000-8000-000000000000\",\"sessionId\":\"{session}\",\"version\":\"2.1.207\",\"timestamp\":\"2026-01-01T00:00:00.000Z\",\"message\":{{\"role\":\"user\",\"content\":\"Synthetic cancellation line {index}\"}}}}\n"
        ));
    }
    fs::write(&transcript, contents).expect("large transcript written");
    let adapter = ClaudeCodeAdapter::new(b"fixture-salt".to_vec());
    let source = manifest(&adapter, &root);
    let scan = request(source, TableName::Sessions, &["message_count"]);
    let token = scan.cancellation.clone();
    let budget = scan.budget.clone();
    let watcher = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        while !token.is_cancelled() {
            if budget.bytes_read_used() >= 1_000_000
                || started.elapsed() > std::time::Duration::from_secs(30)
            {
                token.cancel();
                break;
            }
            std::hint::spin_loop();
        }
    });
    let mut records = adapter.scan(scan).expect("session scan starts").records;
    assert!(matches!(records.next(), Some(Err(AdapterError::Cancelled))));
    watcher.join().expect("watcher joins");
}

fn find_main(root: &Path) -> PathBuf {
    fs::read_dir(root.join("projects/synthetic-project"))
        .expect("project exists")
        .map(|entry| entry.expect("entry").path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| !name.starts_with("agent-") && name.ends_with(".jsonl"))
        })
        .expect("main transcript exists")
}
