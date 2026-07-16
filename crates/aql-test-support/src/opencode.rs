use std::fs;
use std::path::Path;

use rusqlite::{Connection, params};
use serde_json::json;

use crate::{TestResult, copy_tree, make_private_tree, reset_output};

const SCHEMA: &str = include_str!("../assets/opencode-schema.sql");

/// Generates all deterministic OpenCode SQLite and WAL fixture scenarios.
pub fn generate_opencode(output: &Path) -> TestResult {
    reset_output(output)?;

    create_root(output, "minimal")?;
    insert_session(output, "minimal", "ses_synthetic_minimal", None, None)?;
    manifest(output, "minimal", "accepted", 1, 0, 0, 0)?;

    create_root(output, "full")?;
    insert_session(output, "full", "ses_synthetic_full", None, None)?;
    insert_full_messages(output, "full", "ses_synthetic_full")?;
    manifest(output, "full", "accepted", 1, 2, 1, 0)?;

    create_root(output, "multi-session")?;
    insert_session(output, "multi-session", "ses_synthetic_alpha", None, None)?;
    insert_session(output, "multi-session", "ses_synthetic_beta", None, None)?;
    manifest(output, "multi-session", "accepted", 2, 0, 0, 0)?;

    create_root(output, "parent-child")?;
    insert_session(output, "parent-child", "ses_synthetic_parent", None, None)?;
    insert_session(
        output,
        "parent-child",
        "ses_synthetic_child",
        Some("ses_synthetic_parent"),
        None,
    )?;
    manifest(output, "parent-child", "accepted", 2, 0, 0, 0)?;

    create_root(output, "archived")?;
    insert_session(
        output,
        "archived",
        "ses_synthetic_archived",
        None,
        Some(1_767_225_606_000),
    )?;
    manifest(output, "archived", "accepted", 1, 0, 0, 0)?;

    create_root(output, "future-schema")?;
    insert_session(output, "future-schema", "ses_synthetic_future", None, None)?;
    connection(output, "future-schema")?.execute_batch(
        "INSERT INTO migration(id, time_completed) VALUES ('20990101000000_future_fixture', 1767225600000);
         ALTER TABLE session ADD COLUMN future_required TEXT NOT NULL DEFAULT 'synthetic';",
    )?;
    manifest(output, "future-schema", "rejected", 0, 0, 0, 1)?;

    create_root(output, "missing-migration")?;
    insert_session(
        output,
        "missing-migration",
        "ses_synthetic_missing_migration",
        None,
        None,
    )?;
    connection(output, "missing-migration")?.execute(
        "DELETE FROM migration WHERE id = '20260622202450_simplify_session_input'",
        [],
    )?;
    manifest(output, "missing-migration", "rejected", 0, 0, 0, 1)?;

    create_root(output, "malformed-json")?;
    insert_session(
        output,
        "malformed-json",
        "ses_synthetic_malformed",
        None,
        None,
    )?;
    connection(output, "malformed-json")?.execute(
        "INSERT INTO message(id, session_id, time_created, time_updated, data)
         VALUES (?1, ?2, 1767225601000, 1767225601000, ?3)",
        params![
            "msg_synthetic_malformed",
            "ses_synthetic_malformed",
            "{malformed fixture json]"
        ],
    )?;
    manifest(output, "malformed-json", "rejected", 1, 0, 0, 1)?;

    create_root(output, "unknown-part")?;
    insert_session(output, "unknown-part", "ses_synthetic_unknown", None, None)?;
    let db = connection(output, "unknown-part")?;
    db.execute(
        "INSERT INTO message(id, session_id, time_created, time_updated, data)
         VALUES (?1, ?2, 1767225601000, 1767225601000, ?3)",
        params![
            "msg_synthetic_unknown",
            "ses_synthetic_unknown",
            r#"{"role":"assistant","time":{"created":1767225601000},"parentID":"msg_synthetic_parent","modelID":"synthetic-model","providerID":"synthetic-provider","mode":"build","agent":"synthetic-agent","path":{"cwd":"/synthetic/workspace","root":"/synthetic/workspace"},"cost":0,"tokens":{"input":0,"output":0,"reasoning":0,"cache":{"read":0,"write":0}}}"#
        ],
    )?;
    db.execute(
        "INSERT INTO part(id, message_id, session_id, time_created, time_updated, data)
         VALUES (?1, ?2, ?3, 1767225601000, 1767225601000, ?4)",
        params![
            "prt_001_unknown",
            "msg_synthetic_unknown",
            "ses_synthetic_unknown",
            r#"{"type":"future-synthetic-part","value":"Synthetic future value"}"#
        ],
    )?;
    manifest(output, "unknown-part", "degraded", 1, 1, 0, 1)?;

    create_root(output, "duplicate-representations")?;
    insert_session(
        output,
        "duplicate-representations",
        "ses_synthetic_duplicate",
        None,
        None,
    )?;
    insert_full_messages(
        output,
        "duplicate-representations",
        "ses_synthetic_duplicate",
    )?;
    connection(output, "duplicate-representations")?.execute_batch(
        "INSERT INTO session_message(id, session_id, type, seq, time_created, time_updated, data)
         VALUES ('msg_synthetic_projection', 'ses_synthetic_duplicate', 'assistant', 1,
                 1767225602000, 1767225602000, '{\"text\":\"Synthetic OpenCode answer\"}');
         INSERT INTO event_sequence(aggregate_id, seq) VALUES ('ses_synthetic_duplicate', 1);
         INSERT INTO event(id, aggregate_id, seq, type, data)
         VALUES ('evt_synthetic_projection', 'ses_synthetic_duplicate', 1, 'message.updated',
                 '{\"text\":\"Synthetic OpenCode answer\"}');",
    )?;
    manifest(output, "duplicate-representations", "accepted", 1, 2, 1, 0)?;

    create_root(output, "forbidden-tables")?;
    insert_session(
        output,
        "forbidden-tables",
        "ses_synthetic_forbidden",
        None,
        None,
    )?;
    connection(output, "forbidden-tables")?.execute_batch(
        "INSERT INTO credential(id, data) VALUES ('credential-synthetic', 'synthetic forbidden credential value');
         INSERT INTO account(id, data) VALUES ('account-synthetic', 'synthetic forbidden account value');
         INSERT INTO control_account(id, data) VALUES ('control-synthetic', 'synthetic forbidden control value');
         INSERT INTO permission(id, data) VALUES ('permission-synthetic', 'synthetic forbidden permission value');",
    )?;
    manifest(output, "forbidden-tables", "accepted", 1, 0, 0, 0)?;

    create_oversized_fixtures(output)?;

    fs::create_dir_all(output.join("corrupt-db"))?;
    fs::write(
        output.join("corrupt-db/opencode.db"),
        "synthetic corrupt sqlite fixture\n",
    )?;
    manifest(output, "corrupt-db", "rejected", 0, 0, 0, 1)?;

    create_symlink_fixture(output)?;

    create_root(output, "root-replacement")?;
    insert_session(
        output,
        "root-replacement",
        "ses_synthetic_root_replacement",
        None,
        None,
    )?;
    copy_tree(
        &output.join("root-replacement"),
        &output.join("root-replacement-copy"),
    )?;
    manifest(output, "root-replacement", "accepted", 1, 0, 0, 0)?;
    manifest(output, "root-replacement-copy", "accepted", 1, 0, 0, 0)?;

    make_private_tree(output)?;
    Ok(())
}

fn create_root(output: &Path, fixture: &str) -> TestResult {
    let root = output.join(fixture);
    fs::create_dir_all(&root)?;
    Connection::open(root.join("opencode.db"))?.execute_batch(SCHEMA)?;
    Ok(())
}

fn connection(output: &Path, fixture: &str) -> TestResult<Connection> {
    Ok(Connection::open(output.join(fixture).join("opencode.db"))?)
}

fn insert_session(
    output: &Path,
    fixture: &str,
    id: &str,
    parent: Option<&str>,
    archived: Option<i64>,
) -> TestResult {
    connection(output, fixture)?.execute(
        "INSERT INTO session (
           id, project_id, workspace_id, parent_id, slug, directory, path, title,
           version, cost, tokens_input, tokens_output, tokens_reasoning,
           tokens_cache_read, tokens_cache_write, agent, model,
           time_created, time_updated, time_archived
         ) VALUES (?1, 'project-synthetic', 'workspace-synthetic', ?2,
           'synthetic-session', '/synthetic/workspace', 'relative/synthetic',
           'Synthetic OpenCode title', '1.17.18', 0.5, 11, 7, 2, 3, 1,
           'synthetic-agent', ?3, 1767225600000, 1767225605000, ?4)",
        params![
            id,
            parent,
            r#"{"id":"synthetic-model","providerID":"synthetic-provider"}"#,
            archived
        ],
    )?;
    Ok(())
}

fn insert_full_messages(output: &Path, fixture: &str, session: &str) -> TestResult {
    let mut connection = connection(output, fixture)?;
    let transaction = connection.transaction()?;
    transaction.execute(
        "INSERT INTO message(id, session_id, time_created, time_updated, data) VALUES
         (?1, ?2, 1767225601000, 1767225601000, ?3),
         (?4, ?2, 1767225602000, 1767225604000, ?5)",
        params![
            "msg_synthetic_user",
            session,
            r#"{"role":"user","time":{"created":1767225601000},"agent":"synthetic-agent","model":{"providerID":"synthetic-provider","modelID":"synthetic-model"}}"#,
            "msg_synthetic_assistant",
            r#"{"role":"assistant","time":{"created":1767225602000,"completed":1767225604000},"parentID":"msg_synthetic_user","modelID":"synthetic-model","providerID":"synthetic-provider","mode":"build","agent":"synthetic-agent","path":{"cwd":"/synthetic/workspace","root":"/synthetic/workspace"},"cost":0.5,"tokens":{"total":24,"input":11,"output":7,"reasoning":2,"cache":{"read":3,"write":1}},"finish":"stop"}"#
        ],
    )?;
    for (id, message, created, updated, data) in [
        (
            "prt_001_user_text",
            "msg_synthetic_user",
            1_767_225_601_000_i64,
            1_767_225_601_000_i64,
            r#"{"type":"text","text":"Synthetic OpenCode question"}"#,
        ),
        (
            "prt_002_assistant_text",
            "msg_synthetic_assistant",
            1_767_225_602_000,
            1_767_225_602_000,
            r#"{"type":"text","text":"Synthetic OpenCode answer","time":{"start":1767225602000,"end":1767225602500}}"#,
        ),
        (
            "prt_003_tool",
            "msg_synthetic_assistant",
            1_767_225_602_500,
            1_767_225_603_500,
            r#"{"type":"tool","callID":"call_synthetic_1","tool":"synthetic_tool","state":{"status":"completed","input":{"value":"synthetic input"},"output":"Synthetic OpenCode tool output","title":"Synthetic tool","metadata":{},"time":{"start":1767225602500,"end":1767225603500}}}"#,
        ),
        (
            "prt_004_finish",
            "msg_synthetic_assistant",
            1_767_225_603_500,
            1_767_225_603_500,
            r#"{"type":"step-finish","reason":"stop","cost":0.5,"tokens":{"total":24,"input":11,"output":7,"reasoning":2,"cache":{"read":3,"write":1}}}"#,
        ),
    ] {
        transaction.execute(
            "INSERT INTO part(id, message_id, session_id, time_created, time_updated, data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, message, session, created, updated, data],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn create_oversized_fixtures(output: &Path) -> TestResult {
    let huge = "S".repeat(70_000);
    create_root(output, "oversized-json")?;
    insert_session(
        output,
        "oversized-json",
        "ses_synthetic_oversized",
        None,
        None,
    )?;
    let db = connection(output, "oversized-json")?;
    db.execute(
        "INSERT INTO message(id, session_id, time_created, time_updated, data)
         VALUES (?1, ?2, 1767225601000, 1767225601000, ?3)",
        params![
            "msg_synthetic_oversized",
            "ses_synthetic_oversized",
            r#"{"role":"user","time":{"created":1767225601000},"agent":"synthetic-agent","model":{"providerID":"synthetic-provider","modelID":"synthetic-model"}}"#
        ],
    )?;
    db.execute(
        "INSERT INTO part(id, message_id, session_id, time_created, time_updated, data)
         VALUES (?1, ?2, ?3, 1767225601000, 1767225601000, ?4)",
        params![
            "prt_001_oversized",
            "msg_synthetic_oversized",
            "ses_synthetic_oversized",
            serde_json::to_string(&json!({"type": "text", "text": huge}))?
        ],
    )?;
    manifest(output, "oversized-json", "accepted", 1, 1, 0, 0)?;

    create_root(output, "oversized-session-sensitive")?;
    insert_session(
        output,
        "oversized-session-sensitive",
        "ses_synthetic_oversized_session",
        None,
        None,
    )?;
    let huge_path = format!("/synthetic/{huge}");
    connection(output, "oversized-session-sensitive")?.execute(
        "UPDATE session SET title = ?1, directory = ?2, model = ?3 WHERE id = ?4",
        params![
            huge,
            huge_path,
            "{malformed synthetic model json]",
            "ses_synthetic_oversized_session"
        ],
    )?;
    manifest(
        output,
        "oversized-session-sensitive",
        "accepted",
        1,
        0,
        0,
        0,
    )?;
    Ok(())
}

#[cfg(unix)]
fn create_symlink_fixture(output: &Path) -> TestResult {
    use std::os::unix::fs::symlink;

    create_root(output, "symlink-db-target")?;
    insert_session(
        output,
        "symlink-db-target",
        "ses_synthetic_symlink_target",
        None,
        None,
    )?;
    fs::create_dir_all(output.join("symlink-db"))?;
    symlink(
        "../symlink-db-target/opencode.db",
        output.join("symlink-db/opencode.db"),
    )?;
    manifest(output, "symlink-db", "rejected", 0, 0, 0, 1)
}

#[cfg(not(unix))]
fn create_symlink_fixture(_output: &Path) -> TestResult {
    Err("OpenCode symlink fixtures require Unix".into())
}

fn manifest(
    output: &Path,
    fixture: &str,
    outcome: &str,
    sessions: usize,
    messages: usize,
    tools: usize,
    warnings: usize,
) -> TestResult {
    let value = json!({
        "format": "opencode-1.17.18-schema-38-message-v1",
        "fixture": fixture,
        "outcome": outcome,
        "expected": {
            "sessions": sessions,
            "messages": messages,
            "tool_calls": tools,
            "warnings": warnings
        }
    });
    fs::write(
        output.join(fixture).join("fixture-manifest.json"),
        format!("{}\n", serde_json::to_string(&value)?),
    )?;
    Ok(())
}
