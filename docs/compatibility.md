# AQL 兼容性

本文记录当前只读 Adapter 与 CLI 契约。AQL 不提供旧命令或旧配置迁移层。

## CLI 契约

- 唯一数据选择形式是 `-d/--database`。
- 内置数据库为 `claude`、`codex`、`kimi`、`opencode` 和显式 `all`。
- 配置数据库 schema 为 `aql-databases-v1`。
- 查询是单条 read-only GenericDialect/DataFusion SELECT/CTE 或 EXPLAIN SELECT。
- `--file` 只接受单条只读 `.aql` 查询；`.sql`、多语句和 mutation 不属于兼容入口。
- 输出为 table、JSON、JSONL、始终公式安全的 CSV，或通过 `--output-file` 原子写文件。
- 敏感授权只接受 `path`、`content`、`tool-input`、`tool-output`，且逐查询或逐 Shell session 生效。

## 平台支持

AQL 当前发布支持 macOS 和 Linux；Windows 仍是 deferred 验证平台，不提供正式发布或兼容性承诺。Windows 构建会运行 stdout 查询、installation salt、配置写入和 `--output-file` 测试：普通文件使用原子 hard-link claim 实现 no-replace 发布，目录 metadata sync 在非 Unix 平台是安全 no-op。无法维持 nofollow、identity 或 no-clobber 保证的操作仍必须 fail closed，不能静默降级。

## Canonical schema

公开 schema 版本为 `aql-canonical-v0`。`SELECT *` 只展开 Safe 字段；显式敏感列必须在 source read 前完成授权。`aql_tables`、`aql_columns`、`aql_sources` 和 `aql_capabilities` 是只读元数据表，不暴露 Agent 私有路径或 payload。

## Claude Code

允许读取固定深度：

```text
projects/<project>/<session-uuid>.jsonl
projects/<project>/agent-<safe-id>.jsonl
```

支持完整记录、截断尾部 warning、固定扫描边界和显式父子 session edge。auth、settings、plugins、hooks、logs、memory、file-history 和项目文件不属于 source。

未知完整事件、身份不匹配、symlink、root replacement 或无法证明的格式漂移 fail closed。

## Codex

支持：

- `sqlite/state_*.sqlite` session metadata（确定性选择最高 version；`user_version != 5` 产生 warning）；
- `session_index.jsonl` title evidence；
- state-declared rollout JSONL messages、tool calls 和 artifacts；
- active WAL read-only snapshot；cleanly checkpointed WAL 以 immutable 语义读取；hot WAL（`-wal` 缺 `-shm`）fail closed 且不创建 sidecar；
- append boundary、bounded record、byte/value budget 和 schema drift 检测。

未授权 Content 不打开 rollout；未投影 Content 不反序列化。未知或截断尾部事件产生 bounded warning，完整畸形记录 fail closed。hostile rollout locator（绝对路径、`..`、非 allowlist 目录、symlink 组件）、root/database replacement 或 shrink 一律 fail closed。

## Kimi Code

允许读取：

```text
session_index.jsonl
sessions/*/*/state.json
state-declared agents/*/wire.jsonl
```

支持 self-describing session、stale/missing index 降级、父子 Agent、messages、tools 和 usage。credentials、OAuth、配置、日志、计划、任务、skills、MCP 配置和项目文件不属于 source。

`session_index.jsonl` 的 `sessionDir` locator 接受两种形式：固定的落盘绝对路径，或严格两段相对路径 `<bucket>/<session>`（相对 sessions root 解析，两段都必须是安全 normal 组件：非空、非 `.`/`..`，只含 ASCII 字母数字和 `.`、`_`、`-`）。其他相对形状、prefix/root/`..` 组件一律 fail closed。两种形式都要求解析结果位于 sessions root 内且目录 basename 与 `sessionId` 一致，否则该条目被忽略并产生脱敏 warning。

## OpenCode

只允许根目录及：

```text
opencode.db
opencode.db-wal
opencode.db-shm
```

使用 pinned SQLite schema、query authorizer 和 active WAL snapshot。logs、repos、config、plugins、项目树和任意 sibling 文件不属于 discovery。

## 格式漂移策略

| 情况 | 行为 |
|---|---|
| 缺少已知 optional 字段 | 返回 NULL 并产生 warning |
| 未知但可安全跳过的 event | bounded warning |
| 截断 append tail | 保留已完成记录并 warning |
| schema/user-version/protocol 超出规则 | fail closed |
| symlink、root replacement、identity mismatch | fail closed |
| 单值、读取量、records、memory 或 deadline 超限 | 取消完整查询 |

升级 Agent 后若 `doctor` 报告 format drift，应升级 AQL Adapter，不要绕过校验或直接查询 Agent 私有数据库。
