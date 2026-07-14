use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};
use serde_json::json;

use crate::{TestResult, reset_output};

const SCHEMA: &str = include_str!("../assets/codex-schema.sql");
const USER_MESSAGE: &str = r#"{"timestamp":"2026-01-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Synthetic question"}]}}"#;
const ASSISTANT_MESSAGE: &str = r#"{"timestamp":"2026-01-01T00:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Synthetic answer"}]}}"#;
const TOOL_CALL: &str = r#"{"timestamp":"2026-01-01T00:00:03Z","type":"response_item","payload":{"type":"function_call","name":"example_tool","call_id":"call-fixture-1","arguments":"{\"value\":\"synthetic\"}"}}"#;
const TOOL_OUTPUT: &str = r#"{"timestamp":"2026-01-01T00:00:04Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-fixture-1","output":"Synthetic tool output"}}"#;

pub fn generate_codex(output: &Path, large_metadata_count: usize) -> TestResult {
    reset_output(output)?;

    create_standard_fixture(
        output,
        "minimal",
        "session-minimal",
        "Synthetic minimal session",
        "rollout-minimal.jsonl",
    )?;
    fs::write(
        output.join("minimal/session_index.jsonl"),
        "{\"id\":\"session-minimal\",\"thread_name\":\"Synthetic minimal session\",\"updated_at\":\"2026-01-01T00:00:04Z\"}\n",
    )?;
    write_manifest(output, "minimal", 1, 2, 1, 0)?;

    create_standard_fixture(
        output,
        "artifacts",
        "session-artifacts",
        "Synthetic artifacts session",
        "rollout-artifacts.jsonl",
    )?;
    append_line(
        &rollout_path(output, "artifacts", "rollout-artifacts.jsonl"),
        r#"{"timestamp":"2026-01-01T00:00:05Z","type":"event_msg","payload":{"type":"patch_apply_end","call_id":"call-fixture-1","changes":{"/workspace/example/new.txt":{"content":"Synthetic artifact content","type":"add"},"/workspace/example/old.txt":{"move_path":"/workspace/example/moved.txt","type":"update","unified_diff":"@@ synthetic diff @@"},"/workspace/example/unknown.bin":{}}}}"#,
    )?;
    write_manifest(output, "artifacts", 1, 2, 1, 0)?;

    create_standard_fixture(
        output,
        "multi-source",
        "session-shared",
        "Synthetic shared session",
        "rollout-shared.jsonl",
    )?;
    fs::write(
        output.join("multi-source/session_index.jsonl"),
        "{\"id\":\"session-shared\",\"thread_name\":\"Synthetic shared session\",\"updated_at\":\"2026-01-01T00:00:04Z\"}\n",
    )?;
    write_manifest(output, "multi-source", 1, 2, 1, 0)?;

    create_standard_fixture(
        output,
        "conflict",
        "session-conflict",
        "Synthetic database title",
        "rollout-conflict.jsonl",
    )?;
    fs::write(
        output.join("conflict/session_index.jsonl"),
        "{\"id\":\"session-conflict\",\"thread_name\":\"Synthetic index title\",\"updated_at\":\"2026-01-01T00:01:00Z\"}\n",
    )?;
    write_manifest(output, "conflict", 1, 2, 1, 1)?;

    create_standard_fixture(
        output,
        "unknown-event",
        "session-unknown",
        "Synthetic unknown event",
        "rollout-unknown.jsonl",
    )?;
    append_line(
        &rollout_path(output, "unknown-event", "rollout-unknown.jsonl"),
        r#"{"timestamp":"2026-01-01T00:00:05Z","type":"future_fixture_event","payload":{"future_fixture_field":true}}"#,
    )?;
    write_manifest(output, "unknown-event", 1, 2, 1, 1)?;

    create_standard_fixture(
        output,
        "truncated-jsonl",
        "session-truncated",
        "Synthetic truncated session",
        "rollout-truncated.jsonl",
    )?;
    append_raw(
        &rollout_path(output, "truncated-jsonl", "rollout-truncated.jsonl"),
        br#"{"timestamp":"2026-01-01T00:00:05Z","type":"response_item","payload":"#,
    )?;
    write_manifest(output, "truncated-jsonl", 1, 2, 1, 1)?;

    create_root(output, "empty")?;
    write_manifest(output, "empty", 0, 0, 0, 0)?;

    create_root(output, "edges")?;
    insert_thread(
        output,
        "edges",
        "session-edge-parent",
        "Synthetic edge parent",
        1_767_225_604,
        "sessions/2026/01/01/rollout-edge-parent.jsonl",
    )?;
    insert_thread(
        output,
        "edges",
        "session-edge-child",
        "Synthetic edge child",
        1_767_225_605,
        "sessions/2026/01/01/rollout-edge-child.jsonl",
    )?;
    write_rollout(
        &rollout_path(output, "edges", "rollout-edge-parent.jsonl"),
        "session-edge-parent",
    )?;
    write_rollout(
        &rollout_path(output, "edges", "rollout-edge-child.jsonl"),
        "session-edge-child",
    )?;
    connection(output, "edges")?.execute_batch(
        "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) VALUES
         ('session-edge-parent', 'session-edge-child', 'completed'),
         ('session-edge-child', 'session-edge-parent', 'running'),
         ('session-edge-parent', 'session-edge-missing', 'pending');",
    )?;
    write_manifest(output, "edges", 2, 4, 2, 1)?;

    for (fixture, title, rollout) in [
        (
            "separate-root-a",
            "Synthetic root A",
            "rollout-root-a.jsonl",
        ),
        (
            "separate-root-b",
            "Synthetic root B",
            "rollout-root-b.jsonl",
        ),
    ] {
        create_standard_fixture(output, fixture, "session-duplicate", title, rollout)?;
        write_manifest(output, fixture, 1, 2, 1, 0)?;
    }

    create_root(output, "large-metadata")?;
    if large_metadata_count > 0 {
        connection(output, "large-metadata")?.execute_batch(&format!(
            "WITH RECURSIVE seq(value) AS (
               SELECT 1 UNION ALL SELECT value + 1 FROM seq WHERE value < {large_metadata_count}
             )
             INSERT INTO threads (
               id, rollout_path, created_at, updated_at, source, model_provider, cwd,
               title, tokens_used, archived, cli_version, model, created_at_ms,
               updated_at_ms, preview
             )
             SELECT printf('session-large-%07d', value), '', 1767225600,
               1767225600 + value, 'fixture', 'example-provider', '/workspace/example',
               'Synthetic large session', 0, 0, '0.0.0-fixture', 'example-model',
               1767225600000, (1767225600 + value) * 1000, '' FROM seq;"
        ))?;
    }
    write_manifest(output, "large-metadata", large_metadata_count, 0, 0, 0)?;

    create_standard_fixture(
        output,
        "added-column",
        "session-added",
        "Synthetic added column",
        "rollout-added.jsonl",
    )?;
    connection(output, "added-column")?
        .execute_batch("ALTER TABLE threads ADD COLUMN future_optional TEXT;")?;
    write_manifest(output, "added-column", 1, 2, 1, 1)?;

    create_standard_fixture(
        output,
        "missing-optional",
        "session-missing-optional",
        "Synthetic missing optional",
        "rollout-missing-optional.jsonl",
    )?;
    connection(output, "missing-optional")?
        .execute_batch("ALTER TABLE threads DROP COLUMN preview;")?;
    write_manifest(output, "missing-optional", 1, 2, 1, 1)?;

    create_root(output, "missing-critical")?;
    connection(output, "missing-critical")?
        .execute_batch("ALTER TABLE threads DROP COLUMN rollout_path;")?;
    write_manifest(output, "missing-critical", 0, 0, 0, 1)?;

    create_standard_fixture(
        output,
        "unknown-version",
        "session-unknown-version",
        "Synthetic unknown version",
        "rollout-unknown-version.jsonl",
    )?;
    connection(output, "unknown-version")?.execute_batch("PRAGMA user_version = 99;")?;
    write_manifest(output, "unknown-version", 1, 2, 1, 1)?;
    Ok(())
}

fn create_root(output: &Path, fixture: &str) -> TestResult {
    let root = output.join(fixture);
    fs::create_dir_all(root.join("sqlite"))?;
    fs::create_dir_all(root.join("sessions/2026/01/01"))?;
    fs::create_dir_all(root.join("archived_sessions"))?;
    let connection = Connection::open(root.join("sqlite/state_5.sqlite"))?;
    connection.execute_batch(SCHEMA)?;
    fs::write(root.join("session_index.jsonl"), b"")?;
    Ok(())
}

fn create_standard_fixture(
    output: &Path,
    fixture: &str,
    id: &str,
    title: &str,
    rollout_file: &str,
) -> TestResult {
    create_root(output, fixture)?;
    let relative = format!("sessions/2026/01/01/{rollout_file}");
    insert_thread(output, fixture, id, title, 1_767_225_604, &relative)?;
    write_rollout(&rollout_path(output, fixture, rollout_file), id)
}

fn insert_thread(
    output: &Path,
    fixture: &str,
    id: &str,
    title: &str,
    updated: i64,
    rollout: &str,
) -> TestResult {
    connection(output, fixture)?.execute(
        "INSERT INTO threads (
           id, rollout_path, created_at, updated_at, source, model_provider, cwd,
           title, tokens_used, archived, cli_version, model, created_at_ms,
           updated_at_ms, preview
         ) VALUES (?1, ?2, 1767225600, ?3, 'fixture', 'example-provider',
           '/workspace/example', ?4, 42, 0, '0.0.0-fixture', 'example-model',
           1767225600000, ?5, 'Synthetic preview')",
        params![id, rollout, updated, title, updated * 1000],
    )?;
    Ok(())
}

fn connection(output: &Path, fixture: &str) -> TestResult<Connection> {
    Ok(Connection::open(
        output.join(fixture).join("sqlite/state_5.sqlite"),
    )?)
}

fn rollout_path(output: &Path, fixture: &str, file: &str) -> PathBuf {
    output.join(fixture).join("sessions/2026/01/01").join(file)
}

fn write_rollout(path: &Path, id: &str) -> TestResult {
    let metadata = json!({
        "timestamp": "2026-01-01T00:00:00Z",
        "type": "session_meta",
        "payload": {
            "id": id,
            "cwd": "/workspace/example",
            "cli_version": "0.0.0-fixture"
        }
    });
    fs::write(
        path,
        format!(
            "{}\n{USER_MESSAGE}\n{ASSISTANT_MESSAGE}\n{TOOL_CALL}\n{TOOL_OUTPUT}\n",
            serde_json::to_string(&metadata)?
        ),
    )?;
    Ok(())
}

fn append_line(path: &Path, line: &str) -> TestResult {
    append_raw(path, format!("{line}\n").as_bytes())
}

fn append_raw(path: &Path, bytes: &[u8]) -> TestResult {
    use std::io::Write;
    let mut file = fs::OpenOptions::new().append(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn write_manifest(
    output: &Path,
    fixture: &str,
    sessions: usize,
    messages: usize,
    tool_calls: usize,
    warnings: usize,
) -> TestResult {
    let manifest = json!({
        "format": "codex-state-v5-fixture",
        "fixture": fixture,
        "expected": {
            "sessions": sessions,
            "messages": messages,
            "tool_calls": tool_calls,
            "warnings": warnings
        }
    });
    fs::write(
        output.join(fixture).join("fixture-manifest.json"),
        format!("{}\n", serde_json::to_string(&manifest)?),
    )?;
    Ok(())
}
