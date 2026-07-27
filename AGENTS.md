# AGENTS.md

This file is the operating guide for coding agents working in this repository. `README.md` is user-facing; do not turn it into a development ledger.

## Project status

AQL is a local-first Rust CLI that exposes read-only SQL over local Claude Code, Codex, Kimi Code, and OpenCode data. Windows is supported through source builds. The prebuilt release target matrix is macOS/Linux-only, and no official GitHub Release has been published yet. Agent mutation is outside the product.

Current user model:

```text
database -> canonical tables -> read-only SELECT
```

Built-in database names are `claude`, `codex`, `kimi`, `opencode`, and explicit federation `all`. Configured databases use the private `aql-databases-v1` schema and take precedence on a name collision. Public CLI selection uses `database` and `-d`; no alternate source-selection model exists.

## Repository map

| Path | Responsibility |
|---|---|
| `crates/aql-model` | Canonical records, IDs, access classes |
| `crates/aql-adapter-api` | Adapter, scan, budget, cancellation contracts |
| `crates/aql-adapter-*` | Read-only Claude Code/Codex/Kimi/OpenCode adapters |
| `crates/aql-catalog` | Reconciliation and source identity |
| `crates/aql-engine-datafusion` | SQL firewall, planning, authorization, execution |
| `crates/aql-fs` | Capability-based filesystem primitives: nofollow opens, atomic no-replace publish, mode/ownership helpers |
| `crates/aql` | AQL executable, Clap CLI, interactive shell, rendering, orchestration |
| `crates/aql-config` | Private configured-database storage |
| `crates/aql-test-support` | Deterministic synthetic Claude Code/Codex/Kimi/OpenCode fixture generators |
| `crates/aql-release` | Deterministic archive, verification, install, uninstall and formula tooling |
| `tools/xtask` | Fixture and capability-based verification entry point |
| `docs/` | Current guides, contracts, format evidence, historical audits |

Read [docs/architecture.md](docs/architecture.md) before making cross-crate changes.

## Non-negotiable safety boundaries

1. Never modify private Agent stores, auth/config files, logs, plugins, or project trees.
2. Never inspect real message/tool payloads unless the user explicitly authorizes a bounded real-data task. Prefer synthetic fixtures for all development and tests.
3. No implicit/default database. `all` must remain explicit.
4. Discovery is fixed-candidate, bounded, non-recursive, process-free, path-masked, and non-persistent.
5. Reject unknown, duplicate, overlapping, or invalid database members before probing or opening Agent data.
6. Enforce projection and access grants before sensitive source reads. Secret has no grant.
7. SQL remains exactly one read-only canonical query. Do not add DML, DDL, `ATTACH`, external files/URLs, arbitrary catalogs, or shell interpolation.
8. Preserve one shared query budget, deadline, cancellation token, and transactional publication across federated sources.
9. Do not persist SQL, shell history, query results, grants, credentials, or tool payloads.

Treat any regression in these boundaries as P0/P1.

## Development workflow

- Use `rg`/`rg --files` for search.
- Preserve unrelated work in a dirty worktree. Never reset or overwrite user changes.
- Use `apply_patch` for source and documentation edits.
- Keep changes surgical. Do not refactor unrelated code while fixing a narrow issue.
- Do not copy real usernames, paths, IDs, prompts, code, tool input/output, or credentials into fixtures, tests, docs, snapshots, or logs.
- Add deterministic synthetic fixtures for every new accepted format or edge case.
- Keep the focused public CLI small. Do not add compatibility aliases or migration layers for removed surfaces.
- User-facing concepts belong in `README.md` and `docs/user-guide.md`; internal source/adapter details belong here or in architecture/contracts.

## Validation ladder

Run the smallest relevant check while iterating, then broader checks in proportion to risk.

```bash
cargo fmt --all -- --check
cargo check -p <changed-package>
cargo test -p <changed-package>
cargo clippy -p <changed-package> --all-targets -- -D warnings
```

For cross-crate, CLI, adapter, security, release, or documentation-contract changes:

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo xtask verify
git diff --check
```

`cargo xtask verify` chains workspace, documentation, release and performance-related behavioral gates (lazy streaming, budget, cancellation; no timing or throughput assertions). Use only synthetic roots unless an explicitly authorized `AQL_RUN_REAL_SMOKE=1 cargo xtask real-smoke` task says otherwise.

Generated `target/`, logs, temporary archives, `__pycache__`, and test state are not source artifacts. Remove them after cleanup/documentation tasks or when the user asks for a clean workspace.

## Documentation contract

- `README.md`: concise final-user onboarding and common usage.
- `docs/user-guide.md`: complete daily CLI workflows.
- `docs/installation.md`: build, local verified release, upgrade, uninstall.
- `docs/architecture.md`: current component model and data flow.
- `docs/compatibility.md`: versioned Adapter support and known drift behavior.
- `docs/privacy-threat-model.md`: authoritative security controls.
- `docs/*-format-notes.md`: pinned source-format evidence.

When changing CLI syntax, update README, user guide, completions/man tests, compatibility, threat model when relevant, and the appropriate verifier.

## Review checklist

Before handoff, explicitly check:

- no new implicit discovery/default selection;
- no sensitive value read before grant;
- no path or host identity leak;
- no partial-success output;
- no budget multiplication across sources;
- no source-adjacent writes or SQLite checkpoint/migration;
- no ignored tests or production panic/unwrap/expect introduced;
- no stale documentation or broken relative links;
- no generated build/log/cache residue.
