# Claude Code Local Format Notes

> Status: read-only transcript contract admitted for Claude Code 2.x.  
> Evidence: official Claude Code CLI 2.1.207, official `@anthropic-ai/claude-agent-sdk` package 0.3.207, and a user-authorized local directory generated specifically for AQL validation. Inspected on 2026-07-13.

## Evidence handling

The local directory was used only to derive structural evidence:

- relative layout classes;
- JSON field names and JSON value types;
- event and content-block discriminants;
- equality relationships between filename, `sessionId` and `agentId`;
- counts and bounded file sizes.

AQL did not copy real prompts, assistant replies, reasoning, tool input/output, UUIDs, absolute paths, project names or credentials into source, tests, fixtures, documentation or logs. The committed fixture generator reconstructs the admitted shapes using fixed synthetic IDs, `/synthetic/workspace` and fixed synthetic payloads.

## Admitted layout

The selected data root is `~/.claude`. The Adapter opens only:

```text
projects/
  <opaque-project-key>/
    <session-uuid>.jsonl
    agent-<safe-agent-id>.jsonl
```

Observed main transcript invariants:

- filename stem is a UUID;
- every identity-bearing entry has the same `sessionId` as the filename;
- the top-level `uuid` is the stable transcript-entry identity;
- timestamps are RFC 3339 strings;
- `cwd` is Path data;
- main files are direct children of one project-key directory.

Observed child transcript invariants:

- filename is `agent-<safe-agent-id>.jsonl`;
- identity-bearing entries use that same `agentId`;
- their `sessionId` names an existing main transcript in the same project directory;
- the canonical child session identity is `<main-session>/agent/<agent-id>`;
- the parent/child edge is explicit and does not depend on title, path or time inference.

Older SDK documentation describes a sibling session directory with `subagents/agent-*.jsonl`. That layout was not present in the admitted Claude Code 2.x evidence and remains unsupported rather than guessed.

## Admitted event families

Observed top-level event types:

- `user`
- `assistant`
- `system`
- `attachment`
- `file-history-snapshot`
- `last-prompt`
- `mode`
- `permission-mode`
- `queue-operation`

Unknown event types remain opaque and emit `UnknownEvent`. A complete malformed JSON record fails the scan. An incomplete final record emits `TruncatedRecord` and preserves preceding records.

Only `user` and `assistant` map to canonical messages. `system` content, attachments, file-history snapshots, queue content and other control records are never promoted into canonical Content fields.

## Message mapping

Verified message envelope fields include:

```text
message.role
message.id
message.model
message.content
message.usage
```

Observed content forms:

- user string content;
- `text` blocks;
- `thinking` blocks;
- `tool_use` blocks with stable ID/name and JSON input;
- `tool_result` blocks with stable `tool_use_id`, content and error flag.

Canonical rules:

- top-level transcript `uuid` is used for `message_id`;
- text and thinking may enter `messages.content/content_json` only with Content access;
- `tool_use.input` never enters message Content and requires ToolInput through `tool_calls.arguments`;
- `tool_result.content` never enters message Content and requires ToolOutput through `tool_calls.output`;
- a tool-result-only entry uses canonical role `tool` with no payload in the message table;
- `isApiErrorMessage` and tool-result error flags map only to Safe boolean/error status, never to raw error text.

Claude Code can emit several consecutive assistant transcript entries for one API `message.id`. Each entry has its own top-level `uuid`, so message records remain distinct. Repeated usage for the same API message is emitted once by deduplicating on the explicit API message ID.

## Tool and usage mapping

`tool_use.id` and `tool_result.tool_use_id` are the only correlation authority. Unpaired results warn; calls still pending at the fixed file boundary are emitted as `interrupted`. AQL does not infer tools from text, shell syntax or `toolUseResult` helper fields.

Usage fields admitted from assistant messages:

- `input_tokens`
- `output_tokens`
- `cache_creation_input_tokens`
- `cache_read_input_tokens`

Canonical `input_tokens` is non-cached input plus cache-creation input. `cached_tokens` is cache-read input. `total_tokens` is their checked sum with output. Negative values and overflow fail closed. Other usage metadata such as service tier, inference geography, speed, iteration detail and server-tool metadata remains unsupported.

## Projection and access boundary

- Session filename, source identity, file mtime, model and token counts are Safe.
- `cwd/project` require Path.
- `last-prompt` may populate `preview` only with Content.
- No first prompt or transcript body is promoted to `title`.
- Message text/thinking require Content.
- Tool input and output retain independent ToolInput/ToolOutput grants.
- Secret has no grant.

When a sensitive projection is absent, the parser retains the corresponding JSON as borrowed opaque raw bytes and does not deserialize or return the value.

## File and snapshot boundary

- Discovery is process-free, fixed-depth and non-recursive.
- Root, `projects`, project directory and transcript are checked for type and symlink replacement.
- Transcript opens use no-follow semantics.
- Each open fixes the initial byte length; later appends are deferred to the next query.
- Identity change or truncation fails with `SnapshotUnavailable`.
- Individual records are bounded to 1 MiB and transcripts to 512 MiB.
- Source count, shared read budget, deadline and cancellation remain enforced.

The Adapter never writes beside Claude Code data and never opens auth, settings, plugins, hooks, logs, memory or project files.
