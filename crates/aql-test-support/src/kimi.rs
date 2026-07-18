use std::fs;
use std::path::{Path, PathBuf};

use serde_json::json;

use crate::{TestResult, copy_tree, make_private_tree, reset_output};

const DEFAULT_BUCKET: &str = "wd_workspace_84b61923346a";
const DEFAULT_WORKDIR: &str = "/synthetic/workspace";

/// Generates all deterministic Kimi Code index, state, and wire fixture scenarios.
pub fn generate_kimi(output: &Path) -> TestResult {
    reset_output(output)?;

    basic(output, "minimal", "session-minimal")?;
    manifest(output, "minimal", "accepted", 0)?;

    basic(output, "full", "session-full")?;
    append_wire(
        output,
        "full",
        DEFAULT_BUCKET,
        "session-full",
        &[
            r#"{"type":"turn.prompt","time":1767225601000,"input":[{"type":"text","text":"Synthetic question"}],"origin":{"kind":"user"}}"#,
            r#"{"type":"context.append_message","time":1767225602000,"message":{"role":"user","content":[{"type":"text","text":"Synthetic question"}],"toolCalls":[],"origin":{"kind":"user"}}}"#,
            r#"{"type":"context.append_loop_event","time":1767225603000,"event":{"type":"step.begin","uuid":"step-synthetic-1","turnId":"turn-synthetic-1","step":1}}"#,
            r#"{"type":"context.append_loop_event","time":1767225603100,"event":{"type":"content.part","uuid":"part-synthetic-1","turnId":"turn-synthetic-1","step":1,"stepUuid":"step-synthetic-1","part":{"type":"text","text":"Synthetic answer"}}}"#,
            r#"{"type":"context.append_loop_event","time":1767225603200,"event":{"type":"tool.call","uuid":"event-synthetic-1","turnId":"turn-synthetic-1","step":1,"stepUuid":"step-synthetic-1","toolCallId":"tool-synthetic-1","name":"synthetic_tool","args":{"value":"synthetic input"}}}"#,
            r#"{"type":"context.append_loop_event","time":1767225603300,"event":{"type":"tool.result","parentUuid":"event-synthetic-1","toolCallId":"tool-synthetic-1","result":{"output":"Synthetic tool output","isError":false}}}"#,
            r#"{"type":"context.append_loop_event","time":1767225603400,"event":{"type":"step.end","uuid":"step-synthetic-1","turnId":"turn-synthetic-1","step":1,"usage":{"inputOther":11,"output":7,"inputCacheRead":3,"inputCacheCreation":2},"finishReason":"stop"}}"#,
            r#"{"type":"context.append_message","time":1767225604000,"message":{"role":"assistant","content":[{"type":"text","text":"Synthetic answer"}],"toolCalls":[{"type":"function","id":"tool-synthetic-1","name":"synthetic_tool","arguments":"{\"value\":\"synthetic input\"}"}]}}"#,
            r#"{"type":"context.append_message","time":1767225604100,"message":{"role":"tool","content":[{"type":"text","text":"Synthetic tool output"}],"toolCalls":[],"toolCallId":"tool-synthetic-1"}}"#,
            r#"{"type":"usage.record","time":1767225605000,"model":"synthetic-model","usage":{"inputOther":11,"output":7,"inputCacheRead":3,"inputCacheCreation":2},"usageScope":"turn"}"#,
        ],
    )?;
    manifest(output, "full", "accepted", 0)?;

    basic(output, "unpaired-tools", "session-unpaired-tools")?;
    append_wire(
        output,
        "unpaired-tools",
        DEFAULT_BUCKET,
        "session-unpaired-tools",
        &[
            r#"{"type":"context.append_loop_event","time":1767225601000,"event":{"type":"tool.call","uuid":"event-unpaired-call","turnId":"turn-unpaired","step":1,"stepUuid":"step-unpaired","toolCallId":"tool-unpaired-call","name":"synthetic_unpaired_tool","args":{}}}"#,
            r#"{"type":"context.append_loop_event","time":1767225602000,"event":{"type":"tool.result","parentUuid":"event-missing-call","toolCallId":"tool-missing-call","result":{"output":"Synthetic orphan output","isError":true}}}"#,
        ],
    )?;
    manifest(output, "unpaired-tools", "degraded", 1)?;

    create_root(output, "multi-session")?;
    create_session(
        output,
        "multi-session",
        "session-alpha",
        "wd_alpha_f916fe78c6c9",
        "/synthetic/alpha",
    )?;
    create_session(
        output,
        "multi-session",
        "session-beta",
        "wd_beta_93d6b04bf330",
        "/synthetic/beta",
    )?;
    index_session(
        output,
        "multi-session",
        "session-alpha",
        "wd_alpha_f916fe78c6c9",
        "/synthetic/alpha",
    )?;
    index_session(
        output,
        "multi-session",
        "session-beta",
        "wd_beta_93d6b04bf330",
        "/synthetic/beta",
    )?;
    manifest(output, "multi-session", "accepted", 0)?;

    basic_with(
        output,
        "mismatched-bucket",
        "session-mismatched",
        "wd_workspace_deadbeefdead",
        DEFAULT_WORKDIR,
    )?;
    manifest(output, "mismatched-bucket", "rejected", 1)?;

    basic(output, "missing-workdir", "session-missing-workdir")?;
    write_json(
        &state_path(
            output,
            "missing-workdir",
            DEFAULT_BUCKET,
            "session-missing-workdir",
        ),
        &json!({
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-01T00:00:05.000Z",
            "title": "Synthetic missing workdir",
            "agents": {"main": {"homedir": "/synthetic/main", "type": "main", "parentAgentId": null}},
            "custom": {}
        }),
    )?;
    manifest(output, "missing-workdir", "degraded", 1)?;

    basic(output, "subagent", "session-subagent")?;
    append_wire(
        output,
        "subagent",
        DEFAULT_BUCKET,
        "session-subagent",
        &[
            r#"{"type":"context.append_message","time":1767225601500,"message":{"role":"assistant","content":[{"type":"text","text":"Synthetic main answer"}],"toolCalls":[]}}"#,
        ],
    )?;
    let child = session_dir(output, "subagent", DEFAULT_BUCKET, "session-subagent")
        .join("agents/agent-synthetic-child");
    fs::create_dir_all(&child)?;
    fs::write(
        child.join("wire.jsonl"),
        concat!(
            "{\"type\":\"metadata\",\"protocol_version\":\"1.4\",\"created_at\":1767225601000}\n",
            "{\"type\":\"context.append_message\",\"time\":1767225602000,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"Synthetic subagent answer\"}],\"toolCalls\":[]}}\n"
        ),
    )?;
    write_json(
        &state_path(output, "subagent", DEFAULT_BUCKET, "session-subagent"),
        &json!({
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-01T00:00:05.000Z",
            "title": "Synthetic title",
            "workDir": DEFAULT_WORKDIR,
            "agents": {
                "main": {"homedir": "/synthetic/main", "type": "main", "parentAgentId": null},
                "agent-synthetic-child": {"homedir": "/synthetic/child", "type": "sub", "parentAgentId": "main"}
            },
            "custom": {}
        }),
    )?;
    manifest(output, "subagent", "accepted", 0)?;

    create_root(output, "stale-index")?;
    create_session(
        output,
        "stale-index",
        "session-current",
        DEFAULT_BUCKET,
        DEFAULT_WORKDIR,
    )?;
    fs::write(
        output.join("stale-index/session_index.jsonl"),
        "{\"sessionId\":\"session-stale\",\"sessionDir\":\"/synthetic/missing/session-stale\",\"workDir\":\"/synthetic/old\"}\n",
    )?;
    manifest(output, "stale-index", "degraded", 1)?;

    basic(output, "missing-index", "session-unindexed")?;
    fs::remove_file(output.join("missing-index/session_index.jsonl"))?;
    manifest(output, "missing-index", "degraded", 1)?;

    basic(output, "missing-state", "session-missing-state")?;
    fs::remove_file(state_path(
        output,
        "missing-state",
        DEFAULT_BUCKET,
        "session-missing-state",
    ))?;
    manifest(output, "missing-state", "rejected", 1)?;

    basic(output, "invalid-state", "session-invalid-state")?;
    write_json(
        &state_path(
            output,
            "invalid-state",
            DEFAULT_BUCKET,
            "session-invalid-state",
        ),
        &json!({"createdAt": false, "agents": []}),
    )?;
    manifest(output, "invalid-state", "rejected", 1)?;

    basic(output, "unknown-record", "session-unknown-record")?;
    append_wire(
        output,
        "unknown-record",
        DEFAULT_BUCKET,
        "session-unknown-record",
        &[
            r#"{"type":"future.synthetic.record","time":1767225601000,"payload":{"value":"Synthetic future value"}}"#,
        ],
    )?;
    manifest(output, "unknown-record", "degraded", 1)?;

    basic(output, "malformed-record", "session-malformed-record")?;
    append_wire(
        output,
        "malformed-record",
        DEFAULT_BUCKET,
        "session-malformed-record",
        &["{complete malformed json]"],
    )?;
    manifest(output, "malformed-record", "rejected", 1)?;

    basic(output, "truncated-tail", "session-truncated-tail")?;
    append_wire_raw(
        output,
        "truncated-tail",
        DEFAULT_BUCKET,
        "session-truncated-tail",
        br#"{"type":"context.append_message","message":"#,
    )?;
    manifest(output, "truncated-tail", "degraded", 1)?;

    basic(output, "active-boundary", "session-active-boundary")?;
    append_wire(
        output,
        "active-boundary",
        DEFAULT_BUCKET,
        "session-active-boundary",
        &[
            r#"{"type":"context.append_message","time":1767225601000,"message":{"role":"user","content":[{"type":"text","text":"Synthetic boundary record"}],"toolCalls":[]}}"#,
        ],
    )?;
    manifest(output, "active-boundary", "accepted", 0)?;

    basic(output, "legacy-1.0", "session-legacy")?;
    write_json(
        &state_path(output, "legacy-1.0", DEFAULT_BUCKET, "session-legacy"),
        &json!({
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-01T00:00:05.000Z",
            "title": "Synthetic legacy title",
            "agents": {"main": {"homedir": "/synthetic/main", "type": "main", "parentAgentId": null}},
            "custom": {"cwd": DEFAULT_WORKDIR}
        }),
    )?;
    fs::write(
        wire_path(output, "legacy-1.0", DEFAULT_BUCKET, "session-legacy"),
        concat!(
            "{\"type\":\"metadata\",\"protocol_version\":\"1.0\",\"created_at\":1767225600000}\n",
            "{\"type\":\"context.append_message\",\"message\":{\"role\":\"assistant\",\"content\":[],\"toolCalls\":[{\"type\":\"function\",\"id\":\"tool-legacy-synthetic\",\"function\":{\"name\":\"legacy_synthetic_tool\",\"arguments\":\"{}\"}}]}}\n"
        ),
    )?;
    manifest(output, "legacy-1.0", "degraded", 1)?;

    for (fixture, session, metadata, outcome) in [
        (
            "future-protocol",
            "session-future-protocol",
            r#"{"type":"metadata","protocol_version":"9.9","created_at":1767225600000}"#,
            "rejected",
        ),
        (
            "missing-protocol",
            "session-missing-protocol",
            r#"{"type":"context.append_message","message":{"role":"user","content":[],"toolCalls":[]}}"#,
            "rejected",
        ),
    ] {
        basic(output, fixture, session)?;
        fs::write(
            wire_path(output, fixture, DEFAULT_BUCKET, session),
            format!("{metadata}\n"),
        )?;
        manifest(output, fixture, outcome, 1)?;
    }

    let huge = "S".repeat(70_000);
    basic(output, "huge-sensitive", "session-huge-sensitive")?;
    write_json(
        &state_path(
            output,
            "huge-sensitive",
            DEFAULT_BUCKET,
            "session-huge-sensitive",
        ),
        &json!({
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-01T00:00:05.000Z",
            "title": huge,
            "lastPrompt": huge,
            "workDir": DEFAULT_WORKDIR,
            "agents": {"main": {"homedir": "/synthetic/main", "type": "main", "parentAgentId": null}},
            "custom": {}
        }),
    )?;
    manifest(output, "huge-sensitive", "rejected", 1)?;

    basic(output, "huge-wire", "session-huge-wire")?;
    append_wire(
        output,
        "huge-wire",
        DEFAULT_BUCKET,
        "session-huge-wire",
        &[&format!(
            "{{\"type\":\"context.append_message\",\"time\":1767225601000,\"message\":{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":{}}}],\"toolCalls\":[]}}}}",
            serde_json::to_string(&huge)?
        )],
    )?;
    manifest(output, "huge-wire", "accepted", 0)?;

    create_symlink_fixtures(output)?;

    basic(output, "root-replacement", "session-root-replacement")?;
    copy_tree(
        &output.join("root-replacement"),
        &output.join("root-replacement-replacement"),
    )?;
    manifest(output, "root-replacement", "accepted", 0)?;
    manifest(output, "root-replacement-replacement", "accepted", 0)?;

    make_private_tree(output)?;
    Ok(())
}

fn basic(output: &Path, fixture: &str, session: &str) -> TestResult {
    basic_with(output, fixture, session, DEFAULT_BUCKET, DEFAULT_WORKDIR)
}

fn basic_with(
    output: &Path,
    fixture: &str,
    session: &str,
    bucket: &str,
    workdir: &str,
) -> TestResult {
    create_root(output, fixture)?;
    create_session(output, fixture, session, bucket, workdir)?;
    index_session(output, fixture, session, bucket, workdir)
}

fn create_root(output: &Path, fixture: &str) -> TestResult {
    fs::create_dir_all(output.join(fixture).join("sessions").join(DEFAULT_BUCKET))?;
    fs::write(output.join(fixture).join("session_index.jsonl"), b"")?;
    Ok(())
}

fn create_session(
    output: &Path,
    fixture: &str,
    session: &str,
    bucket: &str,
    workdir: &str,
) -> TestResult {
    let directory = session_dir(output, fixture, bucket, session);
    fs::create_dir_all(directory.join("agents/main"))?;
    write_json(
        &directory.join("state.json"),
        &json!({
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-01T00:00:05.000Z",
            "title": "Synthetic title",
            "isCustomTitle": false,
            "lastPrompt": "Synthetic prompt",
            "workDir": workdir,
            "agents": {"main": {"homedir": "/synthetic/agent-home", "type": "main", "parentAgentId": null}},
            "custom": {}
        }),
    )?;
    fs::write(
        directory.join("agents/main/wire.jsonl"),
        "{\"type\":\"metadata\",\"protocol_version\":\"1.4\",\"created_at\":1767225600000}\n",
    )?;
    Ok(())
}

fn index_session(
    output: &Path,
    fixture: &str,
    session: &str,
    bucket: &str,
    workdir: &str,
) -> TestResult {
    use std::io::Write;
    // Emit the locator relative to the sessions root so fixture bytes are
    // host-independent; the adapter resolves it below the validated root.
    let directory = format!("{bucket}/{session}");
    let value = json!({"sessionId": session, "sessionDir": directory, "workDir": workdir});
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(output.join(fixture).join("session_index.jsonl"))?;
    writeln!(file, "{}", serde_json::to_string(&value)?)?;
    Ok(())
}

fn append_wire(
    output: &Path,
    fixture: &str,
    bucket: &str,
    session: &str,
    lines: &[&str],
) -> TestResult {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(wire_path(output, fixture, bucket, session))?;
    for line in lines {
        writeln!(file, "{line}")?;
    }
    Ok(())
}

fn append_wire_raw(
    output: &Path,
    fixture: &str,
    bucket: &str,
    session: &str,
    bytes: &[u8],
) -> TestResult {
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(wire_path(output, fixture, bucket, session))?
        .write_all(bytes)?;
    Ok(())
}

fn manifest(output: &Path, fixture: &str, outcome: &str, warnings: usize) -> TestResult {
    write_json(
        &output.join(fixture).join("fixture-manifest.json"),
        &json!({
            "format": "kimi-code-0.23.3-fixture",
            "protocol": "1.4",
            "fixture": fixture,
            "outcome": outcome,
            "warnings": warnings
        }),
    )
}

fn write_json(path: &Path, value: &serde_json::Value) -> TestResult {
    fs::write(path, format!("{}\n", serde_json::to_string(value)?))?;
    Ok(())
}

fn session_dir(output: &Path, fixture: &str, bucket: &str, session: &str) -> PathBuf {
    output
        .join(fixture)
        .join("sessions")
        .join(bucket)
        .join(session)
}

fn state_path(output: &Path, fixture: &str, bucket: &str, session: &str) -> PathBuf {
    session_dir(output, fixture, bucket, session).join("state.json")
}

fn wire_path(output: &Path, fixture: &str, bucket: &str, session: &str) -> PathBuf {
    session_dir(output, fixture, bucket, session).join("agents/main/wire.jsonl")
}

fn create_symlink_fixtures(output: &Path) -> TestResult {
    basic(output, "symlink-state", "session-symlink-state")?;
    fs::write(
        output.join("symlink-state/outside.json"),
        "{\"synthetic\":\"outside allowlist target\"}\n",
    )?;
    let state = state_path(
        output,
        "symlink-state",
        DEFAULT_BUCKET,
        "session-symlink-state",
    );
    fs::remove_file(&state)?;
    super::symlink_file(Path::new("../../../outside.json"), &state)?;
    manifest(output, "symlink-state", "rejected", 1)?;

    basic(output, "symlink-wire", "session-symlink-wire")?;
    fs::write(
        output.join("symlink-wire/outside-wire.jsonl"),
        "{\"type\":\"metadata\",\"protocol_version\":\"1.4\",\"created_at\":1767225600000}\n",
    )?;
    let wire = wire_path(
        output,
        "symlink-wire",
        DEFAULT_BUCKET,
        "session-symlink-wire",
    );
    fs::remove_file(&wire)?;
    super::symlink_file(Path::new("../../../../../outside-wire.jsonl"), &wire)?;
    manifest(output, "symlink-wire", "rejected", 1)?;

    basic(output, "symlink-agent-dir", "session-symlink-agent")?;
    let session = session_dir(
        output,
        "symlink-agent-dir",
        DEFAULT_BUCKET,
        "session-symlink-agent",
    );
    fs::rename(
        session.join("agents/main"),
        output.join("symlink-agent-dir/outside-agent"),
    )?;
    super::symlink_dir(
        Path::new("../../../../outside-agent"),
        &session.join("agents/main"),
    )?;
    manifest(output, "symlink-agent-dir", "rejected", 1)?;

    create_root(output, "symlink-sessions-dir")?;
    fs::rename(
        output.join("symlink-sessions-dir/sessions"),
        output.join("symlink-sessions-dir/outside-sessions"),
    )?;
    super::symlink_dir(
        Path::new("outside-sessions"),
        &output.join("symlink-sessions-dir/sessions"),
    )?;
    manifest(output, "symlink-sessions-dir", "rejected", 1)?;
    Ok(())
}
