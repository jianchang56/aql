use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::{TestResult, copy_tree, make_private_tree, reset_output, write_private};

const MAIN_SESSION: &str = "11111111-1111-4111-8111-111111111111";
const OTHER_SESSION: &str = "22222222-2222-4222-8222-222222222222";
const PROJECT: &str = "synthetic-project";

pub fn generate_claude(output: &Path) -> TestResult {
    reset_output(output)?;

    create_root(output, "minimal")?;
    write_transcript(
        &main_path(output, "minimal", MAIN_SESSION),
        &[
            user_event(
                MAIN_SESSION,
                "message-user-1",
                Value::String("Synthetic question".into()),
            ),
            assistant_event(
                MAIN_SESSION,
                "message-assistant-1",
                vec![json!({"type": "text", "text": "Synthetic answer"})],
                Some(json!({"input_tokens": 5, "output_tokens": 3})),
            ),
        ],
    )?;

    create_root(output, "full")?;
    write_transcript(
        &main_path(output, "full", MAIN_SESSION),
        &[
            user_event(
                MAIN_SESSION,
                "message-user-1",
                Value::String("Synthetic question".into()),
            ),
            assistant_event(
                MAIN_SESSION,
                "message-assistant-1",
                vec![
                    json!({"type": "thinking", "thinking": "Synthetic reasoning", "signature": "synthetic-signature"}),
                    json!({"type": "text", "text": "Synthetic answer"}),
                ],
                Some(json!({
                    "input_tokens": 11,
                    "cache_creation_input_tokens": 2,
                    "cache_read_input_tokens": 3,
                    "output_tokens": 7
                })),
            ),
            assistant_event(
                MAIN_SESSION,
                "message-assistant-tool",
                vec![json!({
                    "type": "tool_use",
                    "id": "tool-synthetic-1",
                    "name": "synthetic_tool",
                    "input": {"value": "Synthetic input"},
                    "caller": {"type": "direct"}
                })],
                Some(json!({"input_tokens": 4, "output_tokens": 1})),
            ),
            tool_result_event(MAIN_SESSION, "message-tool-result", false),
            json!({
                "type": "last-prompt",
                "sessionId": MAIN_SESSION,
                "leafUuid": "message-tool-result",
                "lastPrompt": "Synthetic preview"
            }),
            json!({"type": "future-synthetic-event", "sessionId": MAIN_SESSION}),
        ],
    )?;

    create_root(output, "subagent")?;
    write_transcript(
        &main_path(output, "subagent", MAIN_SESSION),
        &[assistant_event(
            MAIN_SESSION,
            "message-main",
            vec![json!({"type": "text", "text": "Synthetic main answer"})],
            None,
        )],
    )?;
    write_transcript(
        &agent_path(output, "subagent", "synthetic-child"),
        &[agent_assistant_event(
            MAIN_SESSION,
            "synthetic-child",
            "message-child",
        )],
    )?;

    create_root(output, "malformed")?;
    write_private(
        &main_path(output, "malformed", MAIN_SESSION),
        b"{complete malformed json]\n",
    )?;

    create_root(output, "truncated-tail")?;
    let mut valid = serde_json::to_vec(&user_event(
        MAIN_SESSION,
        "message-before-tail",
        Value::String("Synthetic complete message".into()),
    ))?;
    valid.extend_from_slice(b"\n{\"type\":\"assistant\",\"message\":");
    write_private(&main_path(output, "truncated-tail", MAIN_SESSION), valid)?;

    create_root(output, "mismatched-session")?;
    write_transcript(
        &main_path(output, "mismatched-session", MAIN_SESSION),
        &[user_event(
            OTHER_SESSION,
            "message-mismatch",
            Value::String("Synthetic mismatch".into()),
        )],
    )?;

    create_root(output, "huge-sensitive")?;
    write_transcript(
        &main_path(output, "huge-sensitive", MAIN_SESSION),
        &[user_event(
            MAIN_SESSION,
            "message-huge",
            Value::String("x".repeat(70_000)),
        )],
    )?;

    copy_tree(&output.join("minimal"), &output.join("active-boundary"))?;
    copy_tree(&output.join("minimal"), &output.join("root-replacement"))?;
    copy_tree(
        &output.join("minimal"),
        &output.join("root-replacement-replacement"),
    )?;

    create_root(output, "symlink-transcript")?;
    let target = output.join("symlink-transcript-target.jsonl");
    write_transcript(
        &target,
        &[user_event(
            MAIN_SESSION,
            "message-symlink",
            Value::String("Synthetic symlink".into()),
        )],
    )?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        &target,
        main_path(output, "symlink-transcript", MAIN_SESSION),
    )?;

    make_private_tree(output)?;
    Ok(())
}

fn create_root(output: &Path, scenario: &str) -> TestResult {
    fs::create_dir_all(project_dir(output, scenario))?;
    Ok(())
}

fn project_dir(output: &Path, scenario: &str) -> PathBuf {
    output.join(scenario).join("projects").join(PROJECT)
}

fn main_path(output: &Path, scenario: &str, session: &str) -> PathBuf {
    project_dir(output, scenario).join(format!("{session}.jsonl"))
}

fn agent_path(output: &Path, scenario: &str, agent: &str) -> PathBuf {
    project_dir(output, scenario).join(format!("agent-{agent}.jsonl"))
}

fn write_transcript(path: &Path, entries: &[Value]) -> TestResult {
    let mut bytes = Vec::new();
    for entry in entries {
        serde_json::to_writer(&mut bytes, entry)?;
        bytes.push(b'\n');
    }
    write_private(path, bytes)
}

fn user_event(session: &str, uuid: &str, content: Value) -> Value {
    json!({
        "type": "user",
        "uuid": uuid,
        "parentUuid": null,
        "sessionId": session,
        "timestamp": "2026-01-01T00:00:01.000Z",
        "cwd": "/synthetic/workspace",
        "gitBranch": "synthetic-branch",
        "version": "2.1.207",
        "isSidechain": false,
        "userType": "external",
        "message": {"role": "user", "content": content}
    })
}

fn assistant_event(session: &str, uuid: &str, content: Vec<Value>, usage: Option<Value>) -> Value {
    json!({
        "type": "assistant",
        "uuid": uuid,
        "parentUuid": "message-user-1",
        "sessionId": session,
        "timestamp": "2026-01-01T00:00:02.000Z",
        "cwd": "/synthetic/workspace",
        "gitBranch": "synthetic-branch",
        "version": "2.1.207",
        "isSidechain": false,
        "userType": "external",
        "message": {
            "id": format!("api-{uuid}"),
            "type": "message",
            "role": "assistant",
            "model": "synthetic-model",
            "content": content,
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": usage
        }
    })
}

fn tool_result_event(session: &str, uuid: &str, is_error: bool) -> Value {
    json!({
        "type": "user",
        "uuid": uuid,
        "parentUuid": "message-assistant-tool",
        "sessionId": session,
        "timestamp": "2026-01-01T00:00:03.000Z",
        "cwd": "/synthetic/workspace",
        "gitBranch": "synthetic-branch",
        "version": "2.1.207",
        "isSidechain": false,
        "userType": "external",
        "message": {
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "tool-synthetic-1",
                "content": "Synthetic tool output",
                "is_error": is_error
            }]
        },
        "toolUseResult": {
            "stdout": "Synthetic tool output",
            "stderr": "",
            "interrupted": false,
            "isImage": false,
            "noOutputExpected": false
        }
    })
}

fn agent_assistant_event(session: &str, agent: &str, uuid: &str) -> Value {
    let mut value = assistant_event(
        session,
        uuid,
        vec![json!({"type": "text", "text": "Synthetic child answer"})],
        None,
    );
    value["agentId"] = Value::String(agent.to_string());
    value
}
