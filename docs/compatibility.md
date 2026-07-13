# AQL Adapter Compatibility

## Interactive and database compatibility

- Interactive entrypoints are `aql` on a terminal and `aql shell`; redirected stdin/stdout is rejected.
- User-facing built-in database names are `claude`, `codex`, `kimi`, `opencode`, and explicit federation `all`. Configured database names take precedence on collision.
- `-d/--database` is the public selection form for doctor/query/export/report/search/index. Legacy `--data-root`, `--source`, `--profile`, `profile`, and `sources` remain hidden, mutually exclusive compatibility surfaces.
- `SHOW DATABASES`, `USE`, `SHOW TABLES`, `DESCRIBE`, session-only `GRANT`/`REVOKE`, `EXIT` and `QUIT` are Shell control statements, not claims of ANSI SQL support.
- Query SQL remains the existing read-only GenericDialect/DataFusion subset. DML, DDL, source attachment and unsafe database control statements remain rejected.
- The Shell has bounded in-memory history and completion but never writes a history file. Database selection and sensitive grants are process-local.
- `query` accepts exactly one inline SQL argument, `--file`, or `--stdin`; file/stdin input is bounded to 64 KiB and still passes the same one-statement read-only firewall.
- `EXPLAIN SELECT` and `EXPLAIN WITH ...` are plan-only aliases for the existing safe plan surface. `EXPLAIN ANALYZE` remains rejected because it would execute.
- `schema` and `examples` never open Agent data. `--error-format json` emits one stable stderr object with category, message, hint and exit code.

## Platform, database, output and release compatibility

- Supported operating systems: macOS and Linux. Release targets are exactly `aarch64-macos`, `x86_64-macos`, `aarch64-linux`, and `x86_64-linux`; each target requires executable runtime evidence. Windows is Deferred and is not advertised as supported.
- Discovery candidates are fixed to `$HOME/.claude`, `$HOME/.codex`, `$HOME/.kimi-code`, and `${XDG_DATA_HOME:-$HOME/.local/share}/opencode`. Discovery is explicit, non-recursive, process-free and non-persistent.
- Configured databases support only exact adapter IDs `claude-code`, `codex`, `kimi-code`, and `opencode`. There is no implicit/default database.
- Config schema is `aql-config-v1`; future/unknown security fields fail closed. Index remains `aql-index-v1`; Action plan/audit/store remain their existing v1 schemas.
- CSV is RFC 4180 UTF-8 with safe formula escaping by default. Raw formula emission is compatibility opt-in with a separate acknowledgement.
- Release archive schema is `aql-release-v1`; installer support requires Python 3, a local verified archive, and a new explicit prefix on macOS/Linux. Archive overwrite, remote URL/stdin installation and automatic shell setup are unsupported.
- Claude Code/Codex/Kimi/OpenCode production Actions remain Unsupported.

## Claude Code 2.x compatibility

Evidence was established from the official Claude Code 2.1.207 CLI and a user-authorized directory generated specifically for AQL format validation. Only structural keys, JSON types, event families and identity relationships were retained; real prompts, replies, tool payloads, UUIDs, paths and project names were not copied into fixtures.

| Capability | Status | Boundary |
|---|---|---|
| Sessions | Supported | direct `projects/<project>/<session-uuid>.jsonl` transcripts |
| Messages | Supported | top-level `user`/`assistant` entries; text/thinking only enter Content fields |
| Tool calls | Supported | verified `tool_use`/`tool_result` IDs; input/output retain separate grants |
| Usage | Supported | assistant usage, deduplicated by explicit API message ID |
| Session edges | Supported | direct `agent-<id>.jsonl` linked by explicit `sessionId` |
| Artifacts | Unsupported | attachment/file-history payloads remain opaque |
| Strong snapshot | Not available | append boundary is fixed per opened transcript; identity/truncation is revalidated |
| Production mutations | Unsupported | no admitted official CAS/idempotency/outcome channel |

The adapter opens only `$HOME/.claude/projects`, one project-directory level and direct `.jsonl` transcripts. It does not read auth, settings, history snapshots as canonical payload, plugins, memory, logs, hooks or project files. Main filenames must be UUIDs; child files must be `agent-<safe-id>.jsonl`. Symlinks, root/project/file replacement, oversized records, session-ID mismatch, negative/overflowing usage and complete malformed JSON fail closed. Unknown events warn and remain opaque; an incomplete final line warns while preserving prior records.

## Codex

### Observed format fingerprint: state schema generation 5

Observed on macOS using read-only inspection. This entry records structure only; it contains no source values.

| Capability | Status | Evidence |
|---|---|---|
| Session metadata | Supported for prototype | SQLite `threads` table |
| Session index | Supported for discovery/reconciliation | `session_index.jsonl` with `id`, `thread_name`, `updated_at` shape |
| Active rollouts | Located | `sessions/<year>/<month>/<day>/*.jsonl` |
| Archived rollouts | Located | `archived_sessions/*.jsonl` |
| Messages/tool calls | Shape survey required before parser support | rollout JSONL |
| Strong cross-file snapshot | Not available | SQLite and JSONL change independently |
| Private Agent-store write operations | Unsupported | AQL never writes Codex SQLite, rollout JSONL, session indexes, auth/config or project files |

Observed SQLite migration versions: `1..35`, all successful in the inspected format. The `threads` schema contains stable identity, rollout locator, timestamps, provider/model, archive state, and sensitive metadata fields. Sensitive fields such as title, preview, cwd, and first-user-message are not Safe projections.

### Source authority and linkage

- `threads.id` is the session native identity authority for the observed state schema.
- `threads.rollout_path` is the authoritative locator for that session's rollout event source.
- Session index entries reconcile by their explicit `id`; they are not merged by title or time proximity.
- Active and archived rollout locations are both discoverable and must be de-duplicated through the native session identity or authoritative rollout locator.
- Records from different configured databases are never merged, even when their native IDs match.

### Known rollout event families

The inspected format contains these top-level event families:

- `session_meta`
- `turn_context`
- `event_msg`
- `response_item`
- `compacted`

This list is an allowlist for structural reporting, not a claim that every nested payload shape is already supported. Unknown events must produce a warning and remain unread by the canonical parser until a synthetic fixture exists.

The value-discarding shape survey observed the following relevant paths without retaining their values: top-level `type`/`timestamp`; session metadata `payload.id`, `payload.cwd`, `payload.cli_version`; message content `payload.role`, `payload.content[].type`, `payload.content[].text`; tool-call linkage `payload.name`, `payload.call_id`, `payload.arguments`, `payload.output`; and model metadata `payload.model`, `payload.model_provider`.

### Downgrade behavior

- Missing rollout data: sessions remain queryable; messages/tool-calls capability is unavailable.
- Added nullable SQLite columns: continue with a format warning.
- Missing `threads.id` or `threads.rollout_path`: reject the format for session-to-rollout reconciliation.
- Truncated final JSONL record: keep preceding records and return a warning.
- Unknown event type: skip it and return a warning.

### Phase 1 automated compatibility matrix

| Synthetic format | Result |
|---|---|
| Known schema + user_version 5 | Supported |
| Added unknown nullable column | Supported with `unknown_optional_columns` warning |
| Missing optional canonical source column | Field returns NULL with `missing_optional_columns` warning |
| Missing `threads.id` or `threads.rollout_path` | `UnsupportedFormat` |
| Compatible schema + unknown user_version | Supported with `unrecognized_user_version` warning |
| Truncated rollout tail | Prior records retained; `TruncatedRecord` warning |
| Event appended after scan byte boundary | Deferred to next query |

Active WAL is supported with the policy recorded in `docs/privacy-threat-model.md`: SQLite may update reader marks in an already-existing SHM file, while DB/WAL bytes and business data remain unchanged and AQL creates no sidecars.

## Domain capabilities

| Capability | Codex status | Notes |
|---|---|---|
| Usage facts | Supported | Derived from canonical messages, tool calls and sessions; unknown tokens remain NULL |
| Session edges | Supported when `thread_spawn_edges(parent_thread_id, child_thread_id, status)` matches | Native relationships only; no title/path/time inference |
| Artifacts | Supported with table-level Path grant | Explicit `patch_apply_end.changes` entries only; paths are never opened and payloads additionally require Content |
| REDACT / MASK_PATH | Supported | Original Content/Path grant remains required |
| Portable JSON / Markdown reports | Supported | Same planner, lineage checks, budgets and cancellation |

## Optional index compatibility

| Capability | Codex status | Notes |
|---|---|---|
| Safe metadata generation | Supported | Canonical Adapter scan; no title, preview, path, native ID, message, tool or artifact payload |
| Content generation | Explicit opt-in | Session title/preview and message content only; FTS5 must be available before Content is opened |
| Metadata/Content reconciliation | Supported | Ordinary add/change/delete uses atomic full reconciliation; format/schema/tokenizer drift returns `rebuild_required` |
| Full-text search | Supported | Unicode words, quoted phrases and trailing prefix only; Content grant required; no snippets by default |
| Strong cross-file incremental watermark | Not available | Codex SQLite and rollout files do not expose one shared transaction boundary; unsafe continuity is never guessed |
| Repair | Supported | Removes only validated abandoned AQL building files under the owned generation directory |
| Clear | Supported | Opaque source or acknowledged all-index clear; no forensic-erasure claim |

The format fingerprint describes schema/source capabilities and is stable across ordinary session insertions and deletions. Added/removed schema columns, user-version changes, policy changes and tokenizer changes are incompatible with an existing generation and require rebuild.

`thread_spawn_edges` has no timestamp or foreign-key contract in the observed format. Consequently `created_at` is NULL, cycles are queryable without recursive traversal, and dangling children produce a warning.

## Action compatibility

| Operation | Codex CLI 0.144.1 | Admission result | Reason |
|---|---|---|---|
| `session.archive` | Public `codex archive <SESSION>` and experimental `thread/archive` exist | Unsupported | No atomic expected revision/CAS; no Action idempotency key or authoritative outcome query |
| `session.unarchive` | Public `codex unarchive <SESSION>` and experimental `thread/unarchive` exist | Unsupported | No atomic expected revision/CAS; no Action idempotency key or authoritative outcome query |
| `session.rename` | Experimental `thread/name/set` exists | Unsupported | No atomic expected revision/CAS; no Action idempotency key or authoritative outcome query |

The production `aql-action-codex` Adapter therefore exposes an unsupported capability snapshot only and contains no writer. The synthetic reference Adapter is admitted solely for isolated deterministic acceptance; it cannot be selected implicitly and cannot justify production support.

## Kimi Code 0.23.3 compatibility

Pinned official evidence: tag `@moonshot-ai/kimi-code@0.23.3`, commit `93c0b7bb7836fa990cd9cd35f6518ed55841d2fe`, wire protocol `1.4`.

| Capability | Status | Boundary |
|---|---|---|
| Sessions | Supported | self-describing `state.json`; index is a hint |
| Messages | Supported | completed `context.append_message` only |
| Tool calls | Supported | explicit `context.append_loop_event` call/result correlation |
| Usage | Supported | explicit `usage.record`, unioned with existing derived facts |
| Session edges | Supported | explicit `agents.<id>.parentAgentId`; subagents are namespaced logical sessions |
| Artifacts | Unsupported | no fixture-backed canonical artifact contract |
| Protocol 1.4 | Accepted | fixed per-file byte boundary |
| Protocol 1.0 | Degraded | only the pinned message migration fixture |
| Future/missing protocol | Rejected | fail closed before payload projection |
| Missing workDir authority | Degraded | identity remains explicit; cwd NULL and stale warning |
| Present workDir/bucket mismatch | Rejected | streaming decoded path hash mismatch |
| Production Actions | Unsupported | no atomic expected revision/CAS or admitted idempotency/outcome protocol |

The exact read allowlist is `session_index.jsonl`, `sessions/*/*/state.json`, and state-declared `agents/*/wire.jsonl`. Credentials, OAuth, config, logs, plans, tasks, skills, MCP configuration and project files are never sources. Content index excludes tool-role messages and all tool payloads. See `docs/kimi-code-format-notes.md`.

## OpenCode 1.17.18 compatibility

Pinned official evidence: tag `v1.17.18`, commit `b1fc8113948b518835c2a39ece49553cffe9b30c`, 38 migrations ending `20260622202450_simplify_session_input`, format fingerprint `opencode-1.17.18-schema-38-message-v1`.

| Capability | Status | Boundary |
|---|---|---|
| Sessions | Supported | explicit `session` columns; deterministic `(time_updated,id)` order |
| Messages | Supported | pinned public `message` + `part` projection only |
| Tool calls | Supported | explicit tool part `callID`, state and timestamps |
| Usage | Supported | explicit assistant token fields including reasoning in total; derived count facts carry no tokens |
| Session edges | Supported | explicit `session.parent_id` only |
| Artifacts | Unsupported | file/patch/reasoning/subtask parts are not reclassified |
| Active WAL | Supported | read-only snapshot includes committed WAL rows; DB/WAL business bytes remain unchanged |
| Future/missing migrations | Rejected | exact pinned migration/schema fingerprint |
| `session_message`/`event` projection | Unsupported | never UNIONed with the public read projection |
| Production Actions | Unsupported | no atomic expected revision/CAS or admitted idempotency/outcome protocol |

The exact filesystem allowlist is `opencode.db` plus existing SQLite-managed `opencode.db-wal` and `opencode.db-shm`. The hardened connection is read-only/query-only and uses a SQLite authorizer that denies credential/account/control/permission tables, ATTACH/DETACH, writes/DDL, unsafe pragmas, triggers/views and unapproved functions. Logs, repos, config, plugins, project trees and provider/auth data are never opened.

Safe session projection omits title/model/path/JSON unless explicitly selected with the matching grant. Mixed message/part JSON is bounded before materialization; unprojected sensitive payloads are not selected or deserialized. Content indexing is synthetic/explicit opt-in and excludes tool payloads, reasoning, permissions, shares, todos and account/credential data. See `docs/opencode-format-notes.md`.

The OpenCode Action survey found explicit session-ID target binding but unconditional update by ID, no expected revision/CAS, no idempotency key or authoritative outcome lookup, and no disposable-profile acceptance. Delete additionally catches internal removal errors before the HTTP handler returns success. `aql-action-opencode` therefore contains only a versioned unsupported snapshot and no writer.
