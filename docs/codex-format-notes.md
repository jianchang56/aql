# Codex format notes

本文记录当前 Codex read Adapter 的格式证据和 fail-closed 边界。所有测试数据来自 `aql-test-support` 的确定性合成 fixture；不得把真实会话正文、工具载荷、路径、ID 或凭据复制到文档或测试。

## Read allowlist

Adapter 只使用显式选择的 Codex root 中的以下来源：

```text
sqlite/state_<version>.sqlite（多个候选时确定性选择最高 version）
session_index.jsonl（存在时）
threads.rollout_path 明确指向的 rollout JSONL
```

SQLite 以 read-only 方式打开，且永不创建、checkpoint、迁移或删除 sidecar。`-wal` 不存在时使用 `immutable=1` URI 打开：cleanly checkpointed 的 WAL 数据库无需 WAL recovery 即可读取，也不会产生任何 `-wal`/`-shm` 文件。`-wal` 与 `-shm` 同时存在时按 active WAL read-only snapshot 打开。`-wal` 存在而 `-shm` 缺失（需要 recovery 写入的 hot WAL）直接 fail closed（`SnapshotUnavailable`），不创建任何文件。Adapter 不读取 auth、config、history、logs、skills、plugins、项目树或任意未声明 sibling 文件。

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
- `threads.rollout_path` 是该 session rollout 的唯一 locator authority。locator 是 store-supplied hostile input：必须是 `sessions/` 或 `archived_sessions/` 下的规范化相对路径；绝对路径、prefix、`..`、`.` 组件或空值一律 fail closed（`CorruptSource`）。rollout 文件按 no-follow 打开，任何 symlink 组件失败；byte boundary 取自打开后的文件 metadata。
- `session_index.jsonl` 只为 title reconciliation 提供 evidence，不是 session 或 message identity authority。
- 不同 data root 产生不同 `source_id`；相同 native ID 不跨 root 合并。
- source identity 使用 installation-scoped HMAC，不暴露绝对路径或 host identity。
- probe 拒绝 symlink root，并在 unix 上拒绝 group/other 可写的 root（`mode & 0o022`）；root canonicalize 后才绑定。
- probe 绑定 root 目录、state database 和 `-wal`/`-shm` 的 dev+inode identity 以及 database 长度；每次 scan 打开连接前重新验证，replacement、shrink 或 sidecar drift 一律 fail closed（`SnapshotUnavailable`）。
- snapshot token 由绑定 identity 派生（`codex-snapshot:<hash>`），随 source 内容 identity 变化。

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

- SQLite session scan 分页读取，不一次物化全部 metadata；每次 scan 复用同一条连接。
- rollout stream 每次 scan 复用同一条连接，惰性消费，并在读取前检查共享 byte/value budget。
- 单条 rollout record 和单条 session_index record 均有 1 MB 读取上限，超限在物化前以 budget error 失败。
- `session_index.jsonl` 仅在 title 投影时每次 scan 读取一次，并按 no-follow 打开。
- 安全 LIMIT 只在 predicate/order contract 允许时提前停止。
- cancellation 优先于 budget error。
- active WAL 查询不得改变 database 或 WAL business bytes，也不得 checkpoint、copy 或 repair source。
- root、database 和 sidecar identity 在每次 scan 打开连接前重新验证；replacement、shrink、symlink 或 drift fail closed。

## Synthetic verification matrix

Fixture 覆盖：

- known schema、optional drift 和 missing required columns；
- active WAL read-only snapshot；cleanly checkpointed WAL 读取且零 sidecar 残留；hot WAL（`-wal` 缺 `-shm`）fail closed 且零残留；
- metadata-only projection 不打开 rollout；
- 未授权 Content/Path/ToolInput/ToolOutput 在读取前失败；
- hostile rollout locator（绝对路径、`..`、非 allowlist 目录、symlink 组件）fail closed；
- session index conflict 与 oversized index record；
- known、unknown、malformed、truncated、appended 和 oversized rollout records；
- byte budget、single-value budget、cancellation 和 safe LIMIT；
- explicit session edges、cycle 和 dangling child；
- artifact grants 与 payload projection；
- 不同 data root 中相同 native ID 保持独立；
- 多个 `state_*.sqlite` 候选时确定性选择最高 version；
- database replacement 或 shrink 使绑定 snapshot fail closed；
- source tree 不可写时仍可查询。

兼容行为的用户级摘要见 [compatibility.md](compatibility.md)，安全控制见 [privacy-threat-model.md](privacy-threat-model.md)。
