PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;
PRAGMA user_version = 5;

CREATE TABLE _sqlx_migrations (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    success INTEGER NOT NULL
);

INSERT INTO _sqlx_migrations (version, description, success)
VALUES (35, 'fixture schema', 1);

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    rollout_path TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    model_provider TEXT NOT NULL,
    cwd TEXT NOT NULL,
    title TEXT NOT NULL,
    tokens_used INTEGER NOT NULL DEFAULT 0,
    archived INTEGER NOT NULL DEFAULT 0,
    cli_version TEXT NOT NULL DEFAULT '',
    model TEXT,
    created_at_ms INTEGER,
    updated_at_ms INTEGER,
    preview TEXT NOT NULL DEFAULT ''
);

CREATE TABLE thread_spawn_edges (
    parent_thread_id TEXT NOT NULL,
    child_thread_id TEXT PRIMARY KEY,
    status TEXT NOT NULL
);

CREATE INDEX idx_thread_spawn_edges_parent_status
ON thread_spawn_edges (parent_thread_id, status);
