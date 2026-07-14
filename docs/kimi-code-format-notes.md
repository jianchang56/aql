# Kimi Code Format Notes

## Status and pinned evidence

Read-only contract recorded on 2026-07-12 (Asia/Shanghai). The installed CLI version `0.23.3` resolves to official tag `@moonshot-ai/kimi-code@0.23.3`, peeled commit `93c0b7bb7836fa990cd9cd35f6518ed55841d2fe`. The wire protocol emitted by that source is `1.4`.

Evidence was extracted from an unexecuted, dependency-free temporary clone of that exact commit. No package script, build, test, checked-in executable or plugin was run. No real `wire.jsonl`, plan, task, log, credential, OAuth or configuration payload was read. Real `state.json` inspection remained limited to structural key paths; session-tree inspection remained limited to names, types, permissions and sizes.

## Observed installation

- Product: Kimi Code CLI.
- Executable: `~/.kimi-code/bin/kimi` (Mach-O arm64).
- Installed CLI version: `0.23.3`.
- Official documentation: `https://moonshotai.github.io/kimi-code/`.
- Official source: `https://github.com/MoonshotAI/kimi-code`.
- Installed source tag: `@moonshot-ai/kimi-code@0.23.3`.
- Installed source commit: `93c0b7bb7836fa990cd9cd35f6518ed55841d2fe`.
- Official repository package version separately observed during the survey: `0.23.5`; it is compatibility evidence only and cannot replace the pinned installed contract.

The public CLI exposes multiple local and network surfaces, but AQL uses none of them. It must not invoke the CLI, server or migration tools against a real user session for fixture generation or acceptance.

## Observed root structure

The observed root is `~/.kimi-code`. Only these candidates are relevant to the read Adapter:

```text
~/.kimi-code/
├── session_index.jsonl
└── sessions/
    └── wd_<redacted-token>/
        └── session_<uuid>/
            ├── state.json
            ├── agents/
            │   ├── main/
            │   │   ├── wire.jsonl
            │   │   ├── plans/*.md
            │   │   └── tasks/*.json
            │   └── agent-*/wire.jsonl
            └── logs/kimi-code.log
```

`credentials/`, `oauth/`, `config.toml`, root/session logs, `plans/` and `tasks/` are excluded sources for the first Kimi Adapter. Discovery must use an exact allowlist and must never recursively traverse the whole Kimi root.

Observed directories are private (`0700`) while files inside those private parents may be `0644`. The Adapter must evaluate the complete containment chain rather than incorrectly requiring every contained file to be `0600`, and it must still reject symlinks, non-regular files and root replacement.

## Session index contract

Pinned source: `packages/agent-core/src/session/store/session-index.ts`.

Each complete JSONL line has exactly three required string fields for the supported contract:

```text
sessionId
sessionDir
workDir
```

The index is append-only. Later valid entries replace earlier entries with the same `sessionId`. Invalid JSON, wrong field types, relative `sessionDir`, paths outside the selected `sessions` root, and entries whose directory basename differs from `sessionId` are ignored by Kimi. AQL reports sanitized warnings for invalid entries but never uses them as authority.

`workDir` in the index is explicitly non-authoritative. `session_index.jsonl` is only a discovery hint: stale or missing entries cannot suppress a valid self-describing session. AQL scans only the two fixed directory levels `sessions/<bucket>/<session-id>` and never recursively searches arbitrary descendants.

## `state.json` contract and authority

The bounded structural survey observed these paths:

```text
agents.<agent-id>.homedir
agents.<agent-id>.parentAgentId
agents.<agent-id>.type
createdAt
lastPrompt
title
updatedAt
workDir
```

Classification:

- `createdAt`, `updatedAt`, explicit agent type and structural parent relation may contribute Safe canonical metadata after fixture-backed validation.
- `title` and `lastPrompt` are Content.
- `workDir` and agent `homedir` are Path.
- The work-directory bucket name is a locator, not session identity.
- The session UUID is a native identity candidate, but the exact authority and migration behavior must be proven from pinned official source and fixtures.

Official source comments state that `state.json` is self-describing and can recover sessions when `session_index.jsonl` is stale or relocated. The Adapter therefore treats the index as a discovery hint and `state.json` as the metadata authority instead of silently dropping unindexed sessions.

Pinned sources: `packages/agent-core/src/session/index.ts`, `packages/agent-core/src/session/store/session-store.ts`, and the legacy migration writer.

The session metadata authority is the validated `state.json` inside the candidate session directory. The supported metadata fields are:

```text
createdAt: ISO timestamp
updatedAt: ISO timestamp
title: string (Content)
isCustomTitle: boolean
lastPrompt?: string (Content)
workDir?: string (Path)
archived?: boolean
custom?: object
agents: object keyed by explicit agent ID
agents.<id>.homedir: string (Path)
agents.<id>.type: explicit agent type
agents.<id>.parentAgentId: string or null
```

The official session ID safety rule is `^[A-Za-z0-9._-]+$`, excluding `.` and `..`. The directory basename is the explicit native session ID. AQL additionally requires containment, a regular non-symlink state file, a matching work-directory bucket when `workDir` is present, and unique identity within a selected source.

The bucket encoding is `wd_<slug of basename>_<first 12 lowercase hex characters of SHA-256(normalized absolute workDir)>`. It is a one-way locator and never identity. Official `reindex()` recovers `workDir` from `state.workDir`, falling back only to legacy `state.custom.cwd`, and refuses a state whose recovered work directory does not encode to its containing bucket.

Agent identity and parentage come only from the `agents` object. `main` has type `main` and `parentAgentId: null`; a subagent must have an explicit ID, type and parent ID. `homedir` is never used as identity or opened as an arbitrary path: AQL constructs the only allowed wire path from the validated session directory and agent ID.

Archive is represented by optional `state.archived`. Official archive and rename rewrite the entire state file directly. The pinned implementation supplies no expected revision/CAS, idempotency key or authoritative outcome lookup, so these operations remain production-unsupported.

## `wire.jsonl` and official protocol 1.4

Pinned sources include `packages/agent-core/src/agent/records/{types,persistence,migration}.ts`, `packages/agent-core/src/agent/context/types.ts`, `packages/agent-core/src/loop/events.ts`, and `packages/kosong/src/{message,usage}.ts`.

The file is an ordered newline-delimited JSON event log. Its first record is:

```json
{"type":"metadata","protocol_version":"1.4","created_at":1767225600000}
```

The fixed reader boundary is the byte length observed when a scan opens the regular file. Records after that boundary belong to a later scan. A complete malformed record is fatal. A final line without a newline is accepted only if it is complete valid JSON; an incomplete final JSON value is ignored for that scan while all prior records remain valid. Unknown well-formed record types are skipped with sanitized warnings.

Official migrations form `1.0 -> 1.1 -> 1.2 -> 1.3 -> 1.4`. Version 1.1 flattens legacy tool calls from `function: {name, arguments}` to `{name, arguments}`; 1.2 is approval-record-only; 1.3 is a blob-reference bump; 1.4 changes goal records only. The Adapter supports exact 1.4 and only the fixture-backed 1.0 message migration needed by the synthetic legacy fixture. Newer protocols fail closed. Missing metadata or an unrecognized older migration path is rejected.

### Canonical record allowlist

- `context.append_message.message` is the canonical completed message source. Roles are exactly `system`, `user`, `assistant`, or `tool`. `content` is an ordered union of `text`, `think`, `image_url`, `audio_url`, and `video_url`; only explicitly supported text/think projections are materialized. `toolCalls` contain explicit `{type: "function", id, name, arguments}` and tool result messages correlate through `toolCallId`.
- `context.append_loop_event.event` is the canonical ordered loop/tool history source. Recorded events are exactly `step.begin`, `step.end`, `content.part`, `tool.call`, and `tool.result`. Live-only deltas/progress/interruption events are not assumed persisted canonical history.
- `usage.record` is the canonical session usage source. Its usage fields are `inputOther`, `output`, `inputCacheRead`, and `inputCacheCreation`; `model` is explicit and `usageScope` is optional.
- `turn.prompt` and `turn.steer` are retained as explicit prompt/steer provenance records where a canonical projection needs them. They are not used to fabricate a second completed message when a corresponding `context.append_message` exists.
- `metadata` supplies protocol compatibility only.

Tool calls and results pair only by explicit `toolCallId`; `parentUuid`, `turnId`, `stepUuid`, record order and explicit state parent IDs may be preserved as provenance but never replaced by time proximity or content similarity. Duplicate completed message and loop-event representations are projected according to canonical table semantics rather than double-counted.

The first Adapter must support lazy bounded projection of:

- canonical messages;
- tool calls and results;
- explicit token usage;
- main/subagent relationships where identity and parentage are explicit.

It must not infer roles, tool pairing, session edges, timestamps or identities by text similarity or file time. Unknown event families produce sanitized warnings. A final incomplete JSONL record preserves prior records and is deferred to the next scan.

## Official server/API boundary

The open-source Kimi server exposes network APIs, but AQL does not use them. AQL must not auto-start the server, read its token/configuration, rotate credentials or depend on a running daemon for local queries.

## Compatibility policy

- **Accepted:** exact protocol 1.4, required identity/authority fields present, only known optional fields absent.
- **Degraded:** extra optional state/index fields, unknown well-formed record types, stale/missing index, incomplete final JSON value, or the explicitly fixture-backed 1.0-to-1.4 migration. Degradation always emits sanitized warnings.
- **Rejected:** missing/invalid identity or agent authority, unsafe IDs, bucket mismatch, symlink/non-regular/escaped paths, complete malformed wire record, missing/invalid protocol metadata, unsupported future protocol, oversized records/values, or files changing identity during a scan.

## Deterministic fixture matrix

The Rust-only `aql-test-support` crate creates AQL-owned synthetic roots for minimal and full records, multiple sessions/workdir buckets, main/subagent state, stale and missing index, missing and invalid state, unknown and malformed records, truncated tails, append boundaries, legacy protocol migration, oversized sensitive values, symlink rejection and root replacement. All user-facing literals are visibly synthetic and all timestamps/IDs are deterministic.

The verifier scans fixture bytes for host home paths, emails and token/secret-shaped values. Fixtures never contain data copied from the real Kimi root.

## Remaining implementation questions (not format guesses)

- AQL's deterministic global sequence across multiple agent files must use explicit agent/state ordering plus per-file record order; the official format does not define a total wall-clock order across files. The implementation must expose this limitation rather than use file mtime.
- Active-file rotation/truncation and root replacement must be detected by file identity/size revalidation and cause a sanitized retry/failure; continuity must not be guessed.
- Non-text media content and provider-specific extras remain unsupported canonical payloads until a separate schema/privacy mapping is required.

No implementation may guess these semantics. Fixture and contract tests must close them or mark the affected canonical capability unsupported.
