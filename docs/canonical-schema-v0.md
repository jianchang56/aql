# AQL Canonical Schema v0

## 1. 目标与边界

Canonical Model 是 Adapter、Catalog 和查询引擎之间唯一稳定的数据边界。它不反映某个 Agent 的原始文件结构，也不依赖 GitQL、DataFusion、Arrow 或 SQLite。

v0 覆盖 source、session、message、tool call，以及 Phase 3 新增的 usage、session edge 和 artifact。Phase 1 另外提供由 `SourceManifest` 派生的只读 `agents` 查询视图；它不是新的 canonical record。Job、action 等实体延后。

### 1.1 `agents` 查询视图

`agents` 每个 source profile 一行，以 `source_id` 唯一。多个 profile 可以拥有相同 `agent_id`。该视图不暴露 `data_root_token`、真实路径或 Adapter-private locator。

| 字段 | 类型 | NULL | Access | 来源 |
|---|---|---:|---|---|
| `source_id` | text | 否 | Safe | manifest |
| `agent_id` | text | 否 | Safe | manifest |
| `display_name` | text | 否 | Safe | manifest |
| `format_fingerprint` | text | 否 | Safe | manifest |
| `snapshot_state` | text | 否 | Safe | probe/catalog |
| `capabilities` | json | 否 | Safe | manifest |

## 2. 通用规则

### 2.1 标识符

- `SourceId`：某个 Agent 的具体 data root/profile，稳定格式为 `<agent_id>:<source_fingerprint>`。
- `NativeId`：来源内部 ID，不保证跨 source 唯一。
- `EntityId`：AQL 逻辑 ID，稳定格式为 `<agent_id>:<source_fingerprint>:<native_id>`。
- `source_fingerprint` 使用安装级随机盐对规范化 data root 身份做 HMAC 后截断生成；不得使用可被字典枚举的裸路径 hash，也不得直接包含绝对路径。测试通过注入固定盐保证确定性。
- ID 比较大小写敏感，序列化为 UTF-8 text。

安装级盐由 AQL 自己管理，不写入 Agent data root。生产默认保存在 OS 对应的 AQL state 目录，文件权限仅当前用户可读写；创建失败时必须报错，不得退化为无盐 hash。测试和 fixture 通过依赖注入使用固定测试盐，不能读取生产盐。

### 2.2 时间

- 内部使用 UTC instant，精度至少毫秒。
- 原始时间无法确定时区时不得擅自套用本地时区；记录 parse warning 并返回 NULL。
- 保留 `observed_at` 作为 AQL 读取时间，它不能替代来源事件时间。

### 2.3 NULL

- `None/NULL` 表示来源未提供、无法可靠推导或没有访问权限之外的缺失。
- 未授权字段不得以 NULL 冒充正常查询结果；必须在计划/scan 前返回 access error。
- 空字符串是有效值，不等同 NULL。
- 推导值必须带 provenance 和 `derived = true`。

### 2.4 扩展字段

- 每条记录可带 `extensions: Map<String, JsonValue>`。
- key 使用 `<agent_id>.<field>` 命名空间。
- 核心查询和身份合并不得依赖未文档化 extension。
- Extension 默认 `AccessClass::Secret`，除非 canonical schema 明确赋予更低访问等级；来源 Adapter 无权自行降级。

## 3. 安全与成本分类

```rust
pub enum AccessClass {
    Safe,
    Path,
    Content,
    ToolInput,
    ToolOutput,
    Secret,
}

pub enum FieldCost {
    Metadata,
    Content,
    Heavy,
    Derived,
}
```

规则：

- `Secret` 永不通过通用查询暴露。
- `Path`、`Content`、`ToolInput`、`ToolOutput` 需要对应显式授权。
- `SELECT *` 只展开 `Safe` 字段。
- Cost 决定读取策略，不替代 access control。

## 4. 通用类型

### `Provenance`

| 字段 | 类型 | 必需 | 含义 |
|---|---|---:|---|
| `source_id` | SourceId | 是 | 物理来源实例 |
| `source_kind` | text | 是 | sqlite/index/rollout 等 |
| `source_locator` | redacted text | 是 | 不含用户绝对路径的定位信息 |
| `source_version` | text? | 否 | 格式指纹/版本 |
| `observed_at` | datetime | 是 | AQL 读取时间 |
| `watermark` | text? | 否 | 快照/变更标记 |
| `derived` | bool | 是 | 是否推导值 |

### `FieldValue<T>`

概念结构：

```rust
pub struct FieldValue<T> {
    pub value: Option<T>,
    pub provenance: Vec<Provenance>,
    pub conflicts: Vec<ConflictingValue<T>>,
}
```

Phase 0 实现可为减少复杂度采用 record-level provenance 加字段映射，但必须能回答“该字段来自哪个 source”。

### `SourceManifest`

| 字段 | 类型 | 必需 |
|---|---|---:|
| `source_id` | SourceId | 是 |
| `agent_id` | text | 是 |
| `display_name` | text | 是 |
| `data_root_token` | redacted text | 是 |
| `format_fingerprint` | text | 是 |
| `capabilities` | set | 是 |
| `snapshot` | SnapshotToken? | 否 |
| `warnings` | list | 是 |

## 5. `sessions` 表

| 字段 | 类型 | NULL | Access | Cost | Authority |
|---|---|---:|---|---|---|
| `session_id` | text | 否 | Safe | Metadata | Catalog |
| `native_id` | text | 否 | Safe | Metadata | Adapter |
| `source_id` | text | 否 | Safe | Metadata | Catalog |
| `agent_id` | text | 否 | Safe | Metadata | Adapter |
| `title` | text | 是 | Content | Metadata | metadata DB > index；禁止默认从正文推导 |
| `preview` | text | 是 | Content | Content | metadata DB/index |
| `cwd` | text | 是 | Path | Metadata | metadata DB > session meta |
| `project` | text | 是 | Path | Derived | AQL path normalizer |
| `model` | text | 是 | Safe | Metadata | latest explicit session/turn metadata |
| `provider` | text | 是 | Safe | Metadata | explicit source value |
| `created_at` | datetime | 是 | Safe | Metadata | earliest reliable explicit value |
| `updated_at` | datetime | 是 | Safe | Metadata | metadata DB > latest valid event |
| `status` | text | 是 | Safe | Derived | explicit source > AQL derivation |
| `archived` | bool | 是 | Safe | Metadata | metadata DB/archive manifest |
| `message_count` | int64 | 是 | Safe | Derived | only when requested |
| `tool_call_count` | int64 | 是 | Safe | Derived | only when requested |
| `tokens_used` | int64 | 是 | Safe | Metadata | explicit source only |
| `identity_confidence` | text | 否 | Safe | Metadata | Catalog |
| `snapshot_state` | text | 否 | Safe | Metadata | Catalog |

约束：

- `title` 即使来源已作为 metadata 保存，仍可能包含用户正文，因此始终需要 Content 授权。Phase 0 禁止从首条消息派生 title。
- `project` 仅基于 cwd/path metadata 推导，不读取项目文件。
- Count 字段不得为满足查询而无界扫描；预算不足时返回 resource error，而非错误计数。

## 6. `messages` 表

| 字段 | 类型 | NULL | Access | Cost |
|---|---|---:|---|---|
| `message_id` | text | 否 | Safe | Metadata |
| `session_id` | text | 否 | Safe | Metadata |
| `source_id` | text | 否 | Safe | Metadata |
| `sequence` | int64 | 否 | Safe | Metadata |
| `role` | text | 否 | Safe | Metadata |
| `kind` | text | 是 | Safe | Metadata |
| `content` | text | 是 | Content | Content |
| `content_json` | json | 是 | Content | Heavy |
| `model` | text | 是 | Safe | Metadata |
| `created_at` | datetime | 是 | Safe | Metadata |
| `input_tokens` | int64 | 是 | Safe | Metadata |
| `output_tokens` | int64 | 是 | Safe | Metadata |
| `cached_tokens` | int64 | 是 | Safe | Metadata |
| `is_error` | bool | 是 | Safe | Metadata |

约束：

- `sequence` 是 AQL 在该 session 内的稳定总序；原始序号放 provenance/extension。
- Reasoning 默认 `kind = reasoning` 且 content 仍受 Content 授权；未来可提高为更严格等级。
- 系统注入内容不得误标为 user。

## 7. `tool_calls` 表

| 字段 | 类型 | NULL | Access | Cost |
|---|---|---:|---|---|
| `tool_call_id` | text | 否 | Safe | Metadata |
| `session_id` | text | 否 | Safe | Metadata |
| `message_id` | text | 是 | Safe | Metadata |
| `source_id` | text | 否 | Safe | Metadata |
| `sequence` | int64 | 否 | Safe | Metadata |
| `tool_name` | text | 否 | Safe | Metadata |
| `namespace` | text | 是 | Safe | Metadata |
| `arguments` | json | 是 | ToolInput | Heavy |
| `output` | text | 是 | ToolOutput | Heavy |
| `status` | text | 是 | Safe | Metadata |
| `started_at` | datetime | 是 | Safe | Metadata |
| `ended_at` | datetime | 是 | Safe | Metadata |
| `duration_ms` | int64 | 是 | Safe | Derived |
| `exit_code` | int64 | 是 | Safe | Metadata |

约束：

- 命令行本身属于 ToolInput，不得复制到 Safe 字段。
- 工具输出摘要也属于 ToolOutput，除非摘要由明确的本地脱敏器产生并标记 derived。
- `duration_ms` 仅在起止时间可靠时生成。

## 7.1 `usage` 表

`usage` 包含 canonical sessions/messages/tool_calls 的事件粒度派生实体，也允许 Adapter 映射来源明确记录的 usage facts（例如 Kimi `usage.record` 或 OpenCode assistant message token fields）。两者使用 UNION，不按时间或模型猜测合并。字段包括 `usage_id`、`source_id`、`agent_id`、可选 `session_id/model/provider/bucket_start`、可选 token 分量和 `total_tokens`，以及 message/tool/error counts。更高层聚合由 SQL GROUP BY 完成。

- 所有字段为 Safe，但必须只依赖 Safe 输入。
- 未知 token 保持 NULL；只有至少一个可靠分量存在时才生成 `total_tokens`。
- 所有求和使用 checked arithmetic，负值、溢出和来源冲突不得静默修正。
- provenance 标记 `derived = true` 并列出被消费的 canonical 字段来源。
- OpenCode explicit usage 的 `total_tokens` 包含 reasoning；同一 message/tool 的 derived count facts 不再携带 token，避免 UNION 后双重计数。

## 7.2 `session_edges` 表

字段：`edge_id`、`source_id`、`parent_session_id`、`child_session_id`、`edge_kind`、可选 `created_at/native_edge_id`，均为 Safe metadata。

- 只接受来源明确声明的 native edge，不按标题、路径、时间或正文推断。
- 循环和悬空边可以作为数据返回并产生 warning，但查询层不得递归展开。
- 不同 profile/source 的 edge 默认拒绝，除非 Adapter contract 明确声明共享 identity namespace。
- Kimi main/subagent 各自是 namespaced logical session；edge 两端必须能在 `sessions` 中解析，禁止用同一 session 的 self-edge 代替 parentage。
- Claude Code main/direct-agent transcript 各自是 namespaced logical session；child 只接受同 project 目录中 `agentId`、`sessionId` 与 main UUID 明确一致的来源关系。
- OpenCode parent edge 只接受 `session.parent_id`；不同 source/profile 即使 native ID 相同也不得连接或合并。

## 7.3 `artifacts` 表

字段：Safe 的 `artifact_id/source_id/session_id/tool_call_id/kind/media_type/size_bytes/created_at`，Content 的 `name/content/content_json`，Path 的 `path`。

- 整张 `artifacts` 表具有额外的 Path gate：任何 projection（包括 Safe 字段和 `COUNT(*)`）都必须显式授予 Path，因为按文件枚举记录本身需要读取 `changes` 的路径 key。
- artifact 必须由 Agent source 明确记录；未知工具参数不能被猜测为 artifact。
- `path` 只是来源保存的引用。AQL 永不打开它指向的工作区文件。
- Path-only metadata projection 只解析路径 key 与 change type，不解析 `name/content/content_json` payload；payload 仍要求额外 Content grant。
- artifact identity 包含 source/session/native artifact identity；重复或冲突不得静默覆盖。

## 8. 合并与冲突规则

1. Catalog 先按 `source_id + native_id` 建立身份。
2. 不同物理 source kind 只有在 Adapter 明确声明共享 native ID namespace 时才合并。
3. Authority 表决定主值；非主值写入 conflicts，不静默丢弃。
4. 两个同 authority 值冲突时选择更新 watermark/observed_at 更可信者，并产生 warning。
5. 不同 profile/data root 永不自动合并。
6. identity 不确定时保留两个实体，优先重复而不是误合并。

## 9. Schema 演进

- v0 可新增 nullable 字段。
- 字段改名、类型改变、Access 降级/升级必须新增 schema version 和迁移说明。
- AccessClass 变得更严格属于安全修复，可立即应用。
- Adapter extension 不改变 canonical schema version。

### 9.1 Phase 5 Action identity binding

- Action target 只使用 canonical `source_id + entity_id`，一份 plan 只能绑定一个 source/entity/operation。
- CLI/plan/audit 不持久化或接受 native ID、rollout path、SQLite rowid 或任意文件系统路径作为写目标。
- production Action Adapter 可在内存中把 canonical identity 解析到官方 channel target，但 native identity 不得写入 plan、audit、错误或 confirmation。
- plan digest 同时绑定 adapter/capability version 和 expected revision；跨 profile、跨 source 或 capability 变化不能复用。
- rename 参数属于 Content，只以 keyed commitment/长度进入 plan；canonical session title 字段本身不被降级为 Safe。

## 10. Contract Tests

- ID 对 multi-profile 不碰撞。
- NULL 与空字符串区分。
- 未授权 Path/Content/ToolInput/ToolOutput 拒绝。
- `SELECT *` 展开列表不含敏感字段。
- 同一 session 多来源按 authority 合并。
- 冲突 provenance 可追溯。
- Extension round-trip 不丢失。
