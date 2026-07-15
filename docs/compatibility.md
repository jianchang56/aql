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

- pinned `state_5.sqlite` session metadata；
- `session_index.jsonl` title evidence；
- state-declared rollout JSONL messages、tool calls、usage 和 artifacts；
- active WAL read-only snapshot；
- append boundary、byte/value budget 和 schema drift 检测。

未授权 Content 不打开 rollout；未投影 Content 不反序列化。未知或截断尾部事件产生 bounded warning，完整畸形记录 fail closed。

## Kimi Code

允许读取：

```text
session_index.jsonl
sessions/*/*/state.json
state-declared agents/*/wire.jsonl
```

支持 self-describing session、stale/missing index 降级、父子 Agent、messages、tools 和 usage。credentials、OAuth、配置、日志、计划、任务、skills、MCP 配置和项目文件不属于 source。

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
