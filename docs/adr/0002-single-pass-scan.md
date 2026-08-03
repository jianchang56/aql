# ADR 0002: Claude Code adapter 单次查询单遍解析缓存

## Status

Accepted。

## Context

一次针对 `claude` source 的 usage 视图查询（`USAGE_VIEW_SQL` 在 messages/tool_calls/usage 等 UNION 分支中各 JOIN 一次 sessions，见 `crates/aql-engine-datafusion/src/lib.rs:85`）在改造前触发：

- 6 次目录枚举：probe 一次 + 五个表 scan 各一次，每次 scan 都完整执行 `enumerate_transcripts`（`crates/aql-adapter-claude-code/src/lib.rs:240`，行号以本次提交前为准）；
- 5 遍全量 transcript 读取与解析：messages/tool_calls/usage 三表各自按 `parse_line` 的单表 dispatch 全量解析一遍（`transcript.rs` 旧 489-501 行），sessions 被两个 UNION 分支扫描两次、每次 `summarize` 独立全量读取（`transcript.rs` 旧 171-334 行）。

同时已核实：

- 同一查询内同一 source 复用同一个 `Arc<dyn AgentAdapter>`；CLI 每次查询、shell 每条语句都重新 `bind_sources`（`crates/aql/src/main.rs:563`），因此 adapter 实例生命周期 == 单次查询，查询内缓存天然不跨查询；
- 多表 scan 的调用顺序无保证且可能并发（tokio 多线程执行），缓存必须 `Mutex` 保护、顺序无关；
- `SELECT *` 只展开 Safe 列，usage 视图恒为 Safe-only projection；
- 每次 scan 新建 `ScanDiagnostics`，同一 warning 在每表的 diagnostics 中重复（最多 5 份），engine 聚合进同一 metadata.warnings。

约束：Adapter Contract v0 §14 字面条文禁止「Adapter 自有缓存」（`docs/adapter-contract-v0.md`，本次随决策修订其措辞）；AGENTS.md 九条安全边界，重点是 #8（一次查询共享同一 budget/deadline/cancellation，不得按来源翻倍）与 #9（不持久化查询结果、授权与 payload）。

## Candidates

### A. 引擎级多表融合（一次读取喂多个 TableProvider）

在 engine 层把同一 source 的多表 scan 融合为一次流式读取再 demux 给各表。

- 各表消费速度不同（JOIN/聚合反压不同），demux 需要跨流缓冲；缓冲上界难以证明，慢流反压会卡死快流；
- 融合改变 Adapter Contract 的表级 scan 语义，下推报告、diagnostics 与 limit 语义都要重定义；
- 收益最大但对契约侵入最深。

推迟到契约 v1 再评估。

### B. Adapter 内 per-query 有界解析缓存（采纳）

adapter 实例内维护 `Mutex<ParseCache>`（`crates/aql-adapter-claude-code/src/cache.rs:179`）：每个 transcript 单遍解析（`transcript.rs:275` `parse_file`），per-envelope fan-out 同时产出 messages/tool_calls/usage 记录与 Safe sessions 聚合；同查询内后续表 scan 直接 replay。

- 对 engine/catalog/CLI/API 类型零改动；
- 缓存键钉住 scan-start watermark（source + 文件 identity + pinned 长度）与授权位（`cache.rs:123` `FileCacheKey`），append/replace/授权变化一律 miss；
- 内存上界 64 MiB（`cache.rs:28` `MAX_PARSE_CACHE_BYTES`），填满即停止缓存新文件并回退为现状重解析（fill-then-stop，不做 LRU）；
- 同查询两个 scan 并发解析同一文件允许重复解析，last-writer-wins。

### C. 仅共享 inventory（枚举结果）

跨表共享 `enumerate_transcripts` 的枚举结果。§9 要求每次 scan 以每文件 stat+identity 作为 scan-start watermark，共享后收益只剩 `read_dir` 级别，却要在 watermark 语义上开口子。暂缓。

### D. 不做优化

usage 视图的 5× 读取/解析在大会话目录上持续放大 I/O 与 CPU，且与「local-first 只读」的低干扰定位冲突。不取。

## Decision

采用 B：adapter 内 per-query 有界解析缓存。

- `ClaudeCodeAdapter` 持有 `Arc<Mutex<ParseCache>>`，随实例（查询）生命周期释放，不持久化；
- Safe 聚合（summary）恒算；preview/cwd/content/tool_input/tool_output 仅在授权且被请求时抽取，并记入 entry 的 `extracted` 位集（`cache.rs:39` `SensitiveClasses`）；
- 命中条件：entry 完整（只有读满 pinned 长度的解析才允许入缓存，limit 早停不入）&& 键全等 && 本次 projection 所需敏感类 ⊆ extracted（`transcript.rs:200` `load_parsed`）；未命中按 widening 重解析（bytes 正常 charge）并替换 entry；
- replay（`transcript.rs:695` `replay_records`）克隆缓存记录并按本次 projection 掩码敏感字段；emit 时 limit 截断、逐条 `charge_records(1)`、每 1024 条与文件间 `check_scan_state`（取消优先不变）；每个文件 replay 结束做一次 identity+len 重查（`lib.rs:746` `revalidate_transcript_path`，对齐 §9 scan-end 校验），失配返回 `SnapshotUnavailable`；
- warning 只在 parse 路径 push，replay 零 push，结构性保证同一 warning 每查询只产生一次；
- sessions 的纯 identity projection（不含任何 summary 列）保持零 transcript 读取（`lib.rs:767` `wants_session_summary` 短路，行为与改造前一致）。

## Behavior changes

- `bytes_read` 计费：同一查询内同一 transcript 由每表一遍降为解析时一次（`charge_records` 逐条不变）；预算方向只减不增。
- warning 计数：同类 warning 由每表一次降为每查询一次；engine 聚合进同一 metadata.warnings，种类不丢。
- 结构校验覆盖面前移：单遍解析对每个 envelope 运行全部 builder 的结构校验（如 usage conflict、message role 不一致、非 user/assistant 信封的时间戳解析）；原先只读单表时不会触发的结构性损坏现在对任意相关表 scan 都 fail closed。错误方向只增不减。
- limit 早停的 scan 不产生缓存，下一表仍全量解析并按常 charge bytes。
- append/replace/授权变化一律 cache miss，Weak snapshot 语义不变。
- replay 端新增每文件 scan-end identity/len 重查；原 parse 路径用 open handle revalidate，replay 不持有句柄，改为等价的路径重查。

## Safety boundary check

- #8（预算不翻倍）：bytes 计费只减不增，records 逐条不变，budget/deadline/cancellation 依旧全查询共享。
- #9（不持久化）：缓存只存在于 adapter 实例内存，随查询结束释放；无文件、无跨查询共享；64 MiB 上界 + fill-then-stop 回退；授权数据仅在授权且被请求时进入缓存，replay 按 projection 掩码到本次授权列。
- 其余边界不变：miss 路径完整保留 enumerate 的每文件 stat+identity scan-start watermark、`open_transcript` 的 nofollow/identity 校验链与固定格式白名单。

## Consequences

新增测试（全部合成 fixture）：

- `tests/parse_cache.rs::one_parse_charges_source_bytes_once_per_query`：四表连续 scan 后 `bytes_read_used()` == 1× 语料（对照组每表一个新 adapter，4×）；
- `cache_replay_is_byte_identical_to_uncached_scans`：缓存命中记录序列与冷缓存路径逐字节一致（含敏感列授权投影）；
- `parse_warnings_are_emitted_once_per_query`：unknown_event 全查询总数 == 1 且不丢；
- `limit_early_stop_is_never_cached`：limit 早停不入缓存、次表全量重解析、再次表命中；
- `growth_or_replacement_invalidates_the_cached_parse`：append/替换 → miss + 完整校验链 + replay 端 identity 重查 fail closed；
- `concurrent_scans_of_one_file_stay_correct`：双线程并发 scan 同文件正确性冒烟；
- `src/cache.rs` 单元测试：命中子集条件、widening 原位替换、fill-then-stop、零上界不缓存、adapter 级 cap 满回退（1 字节上界时两表 bytes == 2×）。

已知取舍：只查 sessions 聚合一类的查询会额外构建 messages/tools/usage 记录（受 64 MiB 上界约束）；agent transcript 在 cache miss 时仍有一次 `first_identity` head-read。

## Migration triggers

- 契约 v1 引入多表融合 scan（候选 A 的正当形态）时回迁；
- DataFusion 获得 scan 级公共子表达式消除、引擎层可天然合并同 source 多表读取时重估；
- profiling 出现新瓶颈（如 widening 重解析频率过高、agent transcript head-read 占比上升）时重估。
