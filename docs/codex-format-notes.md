# Codex 本地数据格式勘察笔记

## 1. 文档用途

本文件是 `P0-T02` 的事实记录模板。它只记录格式结构、关联规则和兼容性结论，不提取、物化或记录任何真实会话正文或敏感字段值。结构 survey 可以流式跳过 value，但不得把完整事件解析为可 Debug/序列化的通用 JSON 对象。

状态标签：

- `Observed`：已在本机只读验证。
- `Fixture`：仅在合成 fixture 验证。
- `Hypothesis`：待验证，不得据此实现破坏性行为。
- `Unsupported`：Phase 0 明确不支持。

## 2. 已观察的数据根结构

当前本机环境观察到 Codex 数据根可能包含：

| 类别 | 路径模式 | 状态 | 角色 |
|---|---|---|---|
| 主状态库 | `~/.codex/sqlite/state_*.sqlite` 或 `~/.codex/state_*.sqlite` | Observed | thread metadata、关系、jobs 等 |
| WAL/SHM | 对应 `-wal`、`-shm` | Observed | SQLite 活跃事务视图 |
| Session edges | SQLite `thread_spawn_edges` | Observed | `parent_thread_id TEXT NOT NULL`, `child_thread_id TEXT PRIMARY KEY`, `status TEXT NOT NULL`; no declared foreign keys or timestamp |
| Session index | `~/.codex/session_index.jsonl` | Observed | session ID、名称、更新时间索引 |
| Archived rollout | `~/.codex/archived_sessions/*.jsonl` | Observed | session 事件流 |
| Active rollout | 待勘察 | Hypothesis | 活跃 session 事件流 |
| Legacy history | `~/.codex/history.jsonl` | Observed | 旧/辅助历史，角色待确认 |
| Auth/config | `~/.codex/auth.json`、`config.toml` | Observed / Unsupported | 禁止作为查询数据源 |

注意：文件名和位置不是稳定 API。Adapter 必须以 probe 和格式指纹为依据，不能只按固定路径假定。

## 3. 已观察 SQLite Schema

在当前环境的 `state_5.sqlite` 中观察到以下表：

| 表 | Phase 0 | 用途 |
|---|---:|---|
| `threads` | 是 | sessions metadata 主来源候选 |
| `thread_dynamic_tools` | 否 | 后续工具定义能力 |
| `thread_spawn_edges` | 否 | 后续 session edges |
| `agent_jobs` | 否 | 后续 jobs |
| `agent_job_items` | 否 | 后续 job items |
| `backfill_state` | 只诊断 | 数据回填状态 |
| `_sqlx_migrations` | 是 | format fingerprint 输入 |
| `remote_control_enrollments` | Unsupported | 可能含账户/服务信息，不查询 |

### 3.1 `threads` 已观察字段

只记录字段名和类型，不记录真实值：

| 字段 | SQLite 类型 | Canonical 候选 |
|---|---|---|
| `id` | TEXT | `native_id` |
| `rollout_path` | TEXT | source locator，仅内部使用 |
| `created_at` / `created_at_ms` | INTEGER | `created_at` |
| `updated_at` / `updated_at_ms` | INTEGER | `updated_at` |
| `source` / `thread_source` | TEXT | extension/source metadata |
| `model_provider` | TEXT | `provider` |
| `model` | TEXT | `model` |
| `cwd` | TEXT | `cwd`，Access=Path |
| `title` | TEXT | `title`，Access=Content |
| `preview` | TEXT | `preview`，Access=Content |
| `tokens_used` | INTEGER | `tokens_used` |
| `archived` / `archived_at` | INTEGER | `archived` |
| `first_user_message` | TEXT | Content，Phase 0 metadata 查询禁止读取 |
| `cli_version` | TEXT | format/record provenance |
| git 字段 | TEXT | Phase 0 可暂不暴露 |
| sandbox/approval/memory 字段 | TEXT | extension，Phase 0 可暂不暴露 |

### 3.2 SQLite Authority 初稿

- `title`、`cwd`、`created_at`、`updated_at`、`archived`：`threads` 为首选 authority；authority 不改变 title/cwd 的访问等级。
- `preview`、`first_user_message`：即使位于 metadata DB，也按 Content 分类。
- `rollout_path`：仅用于定位对应事件流，不直接输出。
- 重复秒/毫秒时间字段优先毫秒字段；需测试 migration 中 NULL/不一致情况。

## 4. 已观察 JSONL 结构

当前归档 rollout 中观察到的顶层事件类型：

- `session_meta`
- `turn_context`
- `event_msg`
- `response_item`
- `compacted`

这些枚举只说明类型存在，不说明其内部字段已完整稳定。

### 4.1 安全勘察方法

允许输出：

- 顶层 key 集合。
- `type` 枚举及计数。
- 每个 type 的嵌套 key 路径集合。
- JSON value 类型，如 string/object/array/null。
- 字符串长度分布，不输出字符串值。

禁止输出：

- `content`、`text`、`message`、`arguments`、`output` 等值。
- 命令、路径、URL、邮箱、token。
- 任意“前 100 字符”采样。

需要验证跨来源 ID 相等性时，只允许在进程内使用每次运行随机生成的 keyed hash 比较；输出仅包含 `matched_count`、`unmatched_count` 和布尔结论，禁止输出 ID、稳定 hash 或可跨运行关联的摘要。

### 4.2 待建立映射

| 原始事件 | Canonical | 状态 | 待确认 |
|---|---|---|---|
| `session_meta` | session metadata/provenance | Observed shape | `payload.id`、`payload.cwd`、`payload.cli_version`、timestamp |
| `turn_context` | message/turn metadata | Observed shape | model/provider、时间及上下文字段 |
| `event_msg` | message 或 usage event | Observed shape | `payload.type` 决定具体映射，值枚举仍待 fixture |
| `response_item` | message/tool call | Observed shape | 可含 role/content 或 name/call_id/arguments/output |
| `compacted` | session event | Hypothesis | 是否产生用户可查询 message |

任何 Hypothesis 都必须通过合成 fixture 和安全 shape survey 后才能进入实现。

## 5. Session Index

观察到单行包含字段：

- `id`
- `thread_name`
- `updated_at`

初步规则：

- `id` 可与 SQLite `threads.id` 关联。
- `thread_name` 可能与 `title` 重叠，authority 低于 SQLite `threads.title`。
- Index 可用于发现，但不能作为 messages 来源。
- JSONL 重复 ID、旧 entry 和 append-only 更新语义需要 fixture 验证。

## 6. Identity 与合并规则

### 6.1 已知关联

- SQLite `threads.id` ↔ session index `id`：Observed candidate，需自动化验证。
- SQLite `threads.rollout_path` ↔ rollout 文件：Observed candidate，路径本身不得输出。
- Rollout `session_meta.payload.id` 与 SQLite `threads.id` 使用同一 native-ID 候选空间；Phase 0 解析仍以 `threads.rollout_path` 作为权威定位关系，并在合成 fixture 中验证 ID 一致性。

### 6.2 禁止猜测

- 不按 title 合并。
- 不按 cwd + 时间相近合并。
- 不跨 data root/profile 合并。
- 不按 rollout 文件名截取 ID，除非格式指纹证明该规则稳定。

## 7. Format Fingerprint

建议由以下非敏感信息构成：

- SQLite `_sqlx_migrations` 成功版本列表的 hash。
- 关键表及列名/类型的排序 hash。
- JSONL 顶层 type 集合和必需 key shape 的 hash。
- Session index key shape hash。

不得使用：

- 数据内容 hash。
- 绝对 data root。
- installation/account ID。

示例标识：`codex-state-v5:<schema-hash>:<rollout-shape-hash>`。

## 8. Probe 决策表

| 情况 | 结果 |
|---|---|
| data root 不存在 | `NotFound` |
| SQLite 存在且 schema 已知 | metadata capability 可用 |
| SQLite schema 含新增 nullable 列 | warning，继续 |
| 关键表/ID 列缺失 | `UnsupportedFormat` |
| rollout 不存在 | messages/tool_calls capability 不可用，sessions 仍可查 |
| rollout 有未知事件 | warning，跳过未知事件 |
| JSONL 最后一行截断 | warning，保留此前记录 |
| auth/config 存在 | 忽略，不报告具体内容 |

已验证的 Phase 3 artifact 来源仅为 `event_msg.payload.type = patch_apply_end` 的 `payload.changes` object。object key 是 Path；value 仅按投影读取 `type`、`move_path`、`content`、`unified_diff`。未知 event/tool 参数不推断为 artifact，且 key 指向的文件永不打开。

## 9. Fixture 映射要求

每个 observed shape 必须有合成 fixture：

- SQLite schema 与关键 index。
- Session index append/重复行为。
- 每个已支持 rollout event 的最小 JSON。
- 未知 event 与新增字段。
- 时间字段 NULL/冲突。
- rollout path 不存在。
- 相同 native ID 位于两个 profile。

## 10. 待办清单（P0-T02）

- [ ] 确认 active rollout 默认路径或发现机制。
- [ ] 只读导出 `_sqlx_migrations` 版本，不导出其他值。
- [ ] 生成关键 SQLite schema hash。
- [x] 使用只保留 key/type 的流式 Shape Visitor 收集 rollout key path，不保留值。
- [x] 确认 `session_meta.payload.id` 字段存在；真实来源间以 `threads.rollout_path` 定位，ID 一致性放入合成 fixture 验证，避免提取真实 ID。
- [ ] 验证 session index 重复 ID 的最后写入/最新时间语义。
- [ ] 确认 archived 与 active session 是否可能同时出现。
- [ ] 记录未知/旧格式降级策略。
- [ ] 将结论写入 `docs/compatibility.md`。

## 11. 安全 Survey 输出示例

允许：

```text
source=state_sqlite
tables=threads,_sqlx_migrations
threads.columns=id:TEXT,title:TEXT,updated_at_ms:INTEGER
threads.rows=123
rollout.types=event_msg:42,response_item:105,session_meta:1
rollout.paths.response_item=payload.type:string,payload.content:array
```

禁止：

```text
title=Fix production API key ...
cwd=<USER_HOME>/company/secret-project
content=...
```
