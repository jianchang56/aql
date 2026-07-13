PRAGMA foreign_keys = ON;

CREATE TABLE migration (
    id TEXT PRIMARY KEY,
    time_completed INTEGER NOT NULL
);

CREATE TABLE session (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    workspace_id TEXT,
    parent_id TEXT,
    slug TEXT NOT NULL,
    directory TEXT NOT NULL,
    path TEXT,
    title TEXT NOT NULL,
    version TEXT NOT NULL,
    share_url TEXT,
    summary_additions INTEGER,
    summary_deletions INTEGER,
    summary_files INTEGER,
    summary_diffs TEXT,
    metadata TEXT,
    cost REAL NOT NULL DEFAULT 0,
    tokens_input INTEGER NOT NULL DEFAULT 0,
    tokens_output INTEGER NOT NULL DEFAULT 0,
    tokens_reasoning INTEGER NOT NULL DEFAULT 0,
    tokens_cache_read INTEGER NOT NULL DEFAULT 0,
    tokens_cache_write INTEGER NOT NULL DEFAULT 0,
    revert TEXT,
    permission TEXT,
    agent TEXT,
    model TEXT,
    time_created INTEGER NOT NULL,
    time_updated INTEGER NOT NULL,
    time_compacting INTEGER,
    time_archived INTEGER
);

CREATE INDEX session_project_idx ON session(project_id);
CREATE INDEX session_workspace_idx ON session(workspace_id);
CREATE INDEX session_parent_idx ON session(parent_id);

CREATE TABLE message (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES session(id) ON DELETE CASCADE,
    time_created INTEGER NOT NULL,
    time_updated INTEGER NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX message_session_time_created_id_idx
    ON message(session_id, time_created, id);

CREATE TABLE part (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    time_created INTEGER NOT NULL,
    time_updated INTEGER NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX part_message_id_id_idx ON part(message_id, id);
CREATE INDEX part_session_idx ON part(session_id);

CREATE TABLE session_message (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES session(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL,
    time_created INTEGER NOT NULL,
    time_updated INTEGER NOT NULL,
    data TEXT NOT NULL
);

CREATE UNIQUE INDEX session_message_session_seq_idx ON session_message(session_id, seq);

CREATE TABLE event_sequence (
    aggregate_id TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    owner_id TEXT
);

CREATE TABLE event (
    id TEXT PRIMARY KEY,
    aggregate_id TEXT NOT NULL REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE UNIQUE INDEX event_aggregate_seq_idx ON event(aggregate_id, seq);

CREATE TABLE credential (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE account (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE control_account (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE permission (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

INSERT INTO migration(id, time_completed) VALUES
    ('20260127222353_familiar_lady_ursula', 1767225600000),
    ('20260211171708_add_project_commands', 1767225600000),
    ('20260213144116_wakeful_the_professor', 1767225600000),
    ('20260225215848_workspace', 1767225600000),
    ('20260227213759_add_session_workspace_id', 1767225600000),
    ('20260228203230_blue_harpoon', 1767225600000),
    ('20260303231226_add_workspace_fields', 1767225600000),
    ('20260309230000_move_org_to_state', 1767225600000),
    ('20260312043431_session_message_cursor', 1767225600000),
    ('20260323234822_events', 1767225600000),
    ('20260410174513_workspace-name', 1767225600000),
    ('20260413175956_chief_energizer', 1767225600000),
    ('20260423070820_add_icon_url_override', 1767225600000),
    ('20260427172553_slow_nightmare', 1767225600000),
    ('20260428004200_add_session_path', 1767225600000),
    ('20260501142318_next_venus', 1767225600000),
    ('20260504145000_add_sync_owner', 1767225600000),
    ('20260507164347_add_workspace_time', 1767225600000),
    ('20260510033149_session_usage', 1767225600000),
    ('20260511000411_data_migration_state', 1767225600000),
    ('20260511173437_session-metadata', 1767225600000),
    ('20260601010001_normalize_storage_paths', 1767225600000),
    ('20260601202201_amazing_prowler', 1767225600000),
    ('20260602002951_lowly_union_jack', 1767225600000),
    ('20260602182828_add_project_directories', 1767225600000),
    ('20260603001617_session_message_projection_indexes', 1767225600000),
    ('20260603040000_session_message_projection_order', 1767225600000),
    ('20260603141458_session_input_inbox', 1767225600000),
    ('20260603160727_jittery_ezekiel_stane', 1767225600000),
    ('20260604172448_event_sourced_session_input', 1767225600000),
    ('20260605003541_add_session_context_snapshot', 1767225600000),
    ('20260605042240_add_context_epoch_agent', 1767225600000),
    ('20260611035744_credential', 1767225600000),
    ('20260611192811_lush_chimera', 1767225600000),
    ('20260612174303_project_dir_strategy', 1767225600000),
    ('20260622142730_simplify_session_context_epoch', 1767225600000),
    ('20260622170816_reset_v2_session_state', 1767225600000),
    ('20260622202450_simplify_session_input', 1767225600000);
