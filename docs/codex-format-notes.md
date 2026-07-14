# Codex format notes

本文记录当前 Codex read Adapter 的格式证据和 fail-closed 边界。所有测试数据来自 `aql-test-support` 的确定性合成 fixture；不得把真实会话正文、工具载荷、路径、ID 或凭据复制到文档或测试。

## Read allowlist

Adapter 只使用显式选择的 Codex root 中的以下来源：

```text
sqlite/state_*.sqlite 或 root/state_*.sqlite
session_index.jsonl（存在时）
threads.rollout_path 明确指向的 rollout JSONL
```

SQLite 以 read-only 方式打开。Adapter 不读取 auth、config、history、logs、skills、plugins、项目树或任意未声明 sibling 文件，也不创建 sidecar、checkpoint、migration 或缓存。

## State database contract

数据库必须包含 `threads`，且至少包含：

```text
id
rollout_path
```

已支持的 optional session 字段包括 model/provider、created/updated time、title、preview、cwd、token count 和 archive state。新增未知列产生 sanitized warning；缺少 optional 列返回 NULL 并 warning。缺少 identity/locator authority 则拒绝格式。

`thread_spawn_edges` 存在时，必须包含：

```text
parent_thread_id
child_thread_id
status
```

只有这些显式字段可生成 `session_edges`。循环和悬空 child 可以作为数据返回并产生 warning；不得按 title、path、时间或正文推断关系。

Format fingerprint 由 SQLite user version、排序后的表/列 shape、session-index presence、rollout contract 和 edge contract 组成。普通 session 增删不改变 fingerprint。未识别 user version 或兼容 optional drift 产生 warning；缺少 required schema fail closed。

## Identity and authority

- `threads.id` 是 session native identity authority。
- `threads.rollout_path` 是该 session rollout 的唯一 locator authority。
- `session_index.jsonl` 只为 title reconciliation 提供 evidence，不是 session 或 message identity authority。
- 不同 data root 产生不同 `source_id`；相同 native ID 不跨 root 合并。
- source identity 使用 installation-scoped HMAC，不暴露绝对路径或 host identity。

## Projection and access

Safe session metadata 查询只选择所需 SQLite 列，不打开 rollout。

- `title`、`preview` 和 message content 需要 Content grant。
- `cwd`、project 和 artifact path 需要 Path grant。
- tool arguments 需要 ToolInput grant。
- tool result 需要 ToolOutput grant。
- `artifacts` 整表需要 Path grant；读取 artifact payload 还需要 Content grant。

未授权 projection 必须在 rollout 打开前失败。未投影的 sensitive JSON 字段通过流式 visitor 跳过，不物化为通用 JSON value。

## Rollout contract

支持的顶层 event family 为：

```text
session_meta
turn_context
event_msg
response_item
compacted
```

Canonical mapping 只使用明确字段：role、content、tool name、call ID、arguments、output、timestamp、model/provider 和显式 artifact changes。未知 event 产生 bounded warning，不按内容相似、时间邻近或文件名猜测语义。

每次 scan 固定 rollout 打开时的 byte boundary。边界后的 append 留到下次查询；完整 malformed record 失败；不完整尾部产生 warning，同时保留此前完整记录。

Artifact 只来自 `event_msg.payload.type = patch_apply_end` 的 `payload.changes`。object key 是 Path；Adapter 永不打开该路径指向的工作区文件。`type`、`move_path`、`content` 和 `unified_diff` 仅按 projection 与 grant 读取。

## Resource and snapshot behavior

- SQLite session scan 分页读取，不一次物化全部 metadata。
- rollout stream 惰性消费，并在读取前检查共享 byte/value budget。
- 安全 LIMIT 只在 predicate/order contract 允许时提前停止。
- cancellation 优先于 budget error。
- active WAL 查询不得改变 database 或 WAL business bytes，也不得 checkpoint、copy 或 repair source。
- root、database 和 rollout identity 在关键边界重新验证；symlink、replacement 或 shrink fail closed。

## Synthetic verification matrix

Fixture 覆盖：

- known schema、optional drift 和 missing required columns；
- active WAL read-only snapshot；
- metadata-only projection 不打开 rollout；
- 未授权 Content/Path/ToolInput/ToolOutput 在读取前失败；
- session index conflict；
- known、unknown、malformed、truncated 和 appended rollout records；
- byte budget、single-value budget、cancellation 和 safe LIMIT；
- explicit session edges、cycle 和 dangling child；
- artifact grants 与 payload projection；
- 不同 data root 中相同 native ID 保持独立；
- source tree 不可写时仍可查询。

兼容行为的用户级摘要见 [compatibility.md](compatibility.md)，安全控制见 [privacy-threat-model.md](privacy-threat-model.md)。
