# OpenCode Local Format Notes

## Status

Pinned contract for the installed OpenCode 1.17.18 local SQLite read projection. Production support remains limited to the schema and mappings evidenced below.

## Pinned official source

- Repository: `https://github.com/anomalyco/opencode.git`.
- Tag: `v1.17.18`.
- Commit: `b1fc8113948b518835c2a39ece49553cffe9b30c`.
- Package evidence: `packages/opencode/package.json`, `packages/core/package.json` and the installed plugin all declare `1.17.18`.
- The temporary official checkout is read-only: no dependency installation, package script, build, test, repository binary or plugin was executed.

## Observed installation

- OpenCode binary: `~/.opencode/bin/opencode`.
- Installed version: `1.17.18`.
- Installed `@opencode-ai/plugin` package: `1.17.18`.
- Candidate data root: `~/.local/share/opencode`.
- Active SQLite files: `opencode.db`, `opencode.db-wal`, `opencode.db-shm`.
- The binary is not currently on the non-interactive shell `PATH`; production discovery must not depend on PATH alone.

The survey read file metadata, package version, SQLite schema object names and selected table DDL only. It did not select any real row or read configuration, logs, credentials, prompts, responses, paths or native IDs.

## Database location and startup behavior

`packages/core/src/database/database.ts` and `packages/core/src/database/path.ts` establish the default production path as `${Global.Path.data}/opencode.db` for production channels. OpenCode's own database layer sets WAL mode, runs a passive checkpoint and applies migrations during startup. AQL must not reuse that startup layer because query-only AQL reads may not checkpoint or migrate the source.

The official migration list for 1.17.18 is `packages/core/src/database/migration.gen.ts`. It contains 38 TypeScript migrations ending in:

```text
20260622202450_simplify_session_input
```

The fixture fingerprint is `opencode-1.17.18-schema-38-message-v1`. Production probe must validate the required tables/columns/index authorities and pinned terminal migration without running migrations. A missing required migration/schema is rejected; an unknown future migration/schema is rejected rather than interpreted as 1.17.18.

## Pinned read projection

The supported local read projection uses:

- `session`
- `message`
- `part`

Official evidence:

- `packages/core/src/session/sql.ts` defines `SessionTable`, `MessageTable` and `PartTable`.
- `packages/opencode/src/session/message-v2.ts` implements the installed session-message read API directly over `message` and `part`.
- The public HTTP handler in `packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts` calls `MessageV2.page`/`MessageV2.get`.
- `MessageV2.page` orders messages by `(time_created DESC, id DESC)`, then reverses a page for ascending presentation. Its cursor is the explicit `(time_created, id)` pair.
- `MessageV2.hydrate` orders parts by `(message_id, id)`.
- `packages/core/src/session/projector.ts` writes the v1 message projection into `message` and `part` from explicit session events and updates session usage from `step-finish` parts.

Consequently the Adapter reads `message` + `part` as the pinned 1.17.18 public read projection. It does not read or UNION `session_message` or `event`.

The coexisting tables are deliberately unsupported in this Adapter version:

- `session_message`
- `event`
- `event_sequence`

They belong to the newer event-sourced session path. Official migration `20260603040000_session_message_projection_order` deleted pre-launch projection rows because truthful sequence could not be assigned, and `20260622170816_reset_v2_session_state` clears `session_message`, `event` and `event_sequence`. Their presence cannot be used to infer completeness and duplicate rows cannot be merged heuristically with `message`/`part`.

`project` and `workspace` are not needed for the canonical mapping. Session Path values come only from explicitly projected `session.directory`/`session.path`; AQL never opens project directories.

## Session mapping

`packages/core/src/session/sql.ts` and `packages/opencode/src/session/session.ts::fromRow` establish:

- `session.id`: native identity.
- `session.parent_id`: explicit parent session edge.
- `time_created`, `time_updated`, `time_archived`: explicit millisecond timestamps; archived is `time_archived IS NOT NULL`.
- `agent`: explicit Agent label when present.
- `model`: JSON `{id, providerID, variant?}`; model/provider are materialized only when selected and size-bounded.
- token aggregates: `tokens_input`, `tokens_output`, `tokens_reasoning`, `tokens_cache_read`, `tokens_cache_write`.
- `title`: Content; `directory`/`path`: Path.
- `metadata`, `summary_diffs`, `revert`, `permission`, `share_url`: excluded from canonical reads.

Session rows are ordered by `(time_updated, id)` for deterministic pagination. Parent edges use only `parent_id`; no project/path/title/time inference is allowed.

## Message, tool and usage mapping

The JSON unions are pinned by `packages/schema/src/v1/session.ts`:

- Message roles are explicit `user` and `assistant` discriminators in `message.data`.
- Canonical message sequence is the stable official read order `(message.time_created, message.id)` within a session.
- Only `part.data.type = text` contributes canonical message Content. Reasoning, file, patch, snapshot, subtask, compaction, retry and arbitrary future parts remain unsupported and are never reclassified as text.
- `part.data.type = tool` contains explicit `callID`, tool name and state. Pending/running map to interrupted/in-progress semantics; completed and error map from their explicit state only.
- Tool input is `state.input` and ToolOutput is `state.output` or `state.error`; each requires its matching grant and pre-allocation size check.
- Assistant message JSON supplies explicit model/provider, completion/error state and event-grain token fields.
- `step-finish` parts carry explicit cost/token usage and are the projector authority for session aggregates. The Adapter emits explicit message usage from assistant message token fields and does not double-count step-finish/session aggregates as additional event facts.
- Negative, non-integral or overflowing numeric values fail closed.

Explicitly forbidden or deferred tables include:

- `credential`
- `account`
- `account_state`
- `control_account`
- `permission`
- `session_share`
- `todo`
- migration/control tables except for fixed schema compatibility checks

Deterministic fixtures are generated by the Rust-only `aql-test-support` crate from AQL-owned literals and the pinned schema asset. They cover minimal/full/multi-session/parent-child/archive/future and missing migration/malformed and oversized JSON/unknown part/duplicate representations/forbidden tables/corrupt DB/symlink/root replacement. Active WAL is created at test runtime from a synthetic database so DB/WAL/SHM behavior can be observed without copying real data.

## Sensitivity observations

- `session` co-locates Safe timestamps/status/aggregate counters with Content title, Path directory/path and opaque metadata/revert/permission JSON.
- `project` and `workspace` include worktree/directory and other project metadata.
- `message`, `part`, `session_message` and `event` contain JSON text that may mix Content, reasoning, ToolInput, ToolOutput, Path and provider metadata.
- The same physical database contains credentials and account/control data. Filesystem allowlisting therefore does not replace SQLite table/column authorization.

## Required production boundary

- Exact root file allowlist: `opencode.db` and, when present and required, its WAL/SHM companions.
- A cleanly-closed WAL-mode database (no `opencode.db-wal` present) is opened with immutable semantics so SQLite never creates `-shm`/`-wal` sidecars and never attempts WAL recovery; a WAL appearing after binding fails closed as drift. An existing active WAL keeps the coordinated read-only open and is never opened immutable.
- SQLite read-only/query-only connection with fixed statements, extension loading disabled, trusted schema disabled where supported and an observable authorizer.
- Authorizer denial for every non-allowlisted table/column plus ATTACH/DETACH, DDL/DML, writable pragmas, temp writes and virtual-table creation.
- Safe projections must avoid selecting title/path/JSON columns. Unselected JSON must not be fetched or deserialized.
- Active WAL correctness must be proven with synthetic fixtures without checkpointing, copying or mutating the source. If correct WAL reading necessarily changes SHM/source state, the live capability is blocked rather than weakened.
- Root/database/WAL/SHM symlink, type and identity replacement checks fail closed.

## Mutation boundary

AQL does not invoke OpenCode CLI, server, SDK or database mutation surfaces. It never writes through the private SQLite store and does not depend on a running OpenCode process.

## Closed and deferred contract questions

Closed by pinned source: installed tag/commit, migration set, database path, session identity/parent/archive, public `message`/`part` read projection, message/part order, user/assistant/text/tool discriminators, explicit tool call ID/state and token fields.

Deferred to implementation evidence:

1. Correct active-WAL reads without DB/WAL business-data mutation; existing AQL policy permits only SQLite reader coordination changes in an already-existing SHM.
2. `session_message`/`event`, todos, permissions, shares, account/provider/credential data, reasoning and arbitrary parts/events remain unsupported by this Adapter version.

Any unresolved question keeps the affected canonical capability unsupported.
