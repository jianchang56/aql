# AQL 查询引擎 Spike 评分卡

## 1. 决策目标

公平比较 GitQL 与 DataFusion，并选择唯一查询引擎。评分卡不是长期功能愿望清单，而是可复现的准入测试。

## 2. 公平性规则

- 使用相同 Canonical Model、Catalog、Adapter API 和 fixtures。
- 使用相同 release profile、机器和测量脚本。
- 不允许某个 spike 使用索引/缓存而另一个不使用。
- 不修改/fork 候选引擎内核。
- 引擎专用转换代码单独计入实现复杂度。
- 每个失败必须附日志或测试证据，不凭主观判断。

## 3. 必测查询

### Q1：Metadata filter/order/limit

```sql
SELECT agent_id, session_id, model, updated_at
FROM sessions
WHERE updated_at >= TIMESTAMP '2026-01-01 00:00:00'
ORDER BY updated_at DESC
LIMIT 20;
```

### Q2：Aggregation

```sql
SELECT agent_id, COUNT(*) AS session_count
FROM sessions
GROUP BY agent_id
ORDER BY session_count DESC;
```

### Q3：Access rejection

```sql
SELECT session_id, content
FROM messages
LIMIT 1;
```

在没有 `--access content` 时必须在打开 rollout/content source 前失败。

### Q4：Cancellation

对 1M metadata 扫描启动查询，并在确定性检查点触发取消。

### Q5：Unsupported pushdown correctness

使用 Adapter 不支持的表达式过滤，验证引擎会重算，而不是返回未过滤结果。

## 4. 硬门槛

任何一项失败即淘汰：

| ID | 门槛 | 验证 |
|---|---|---|
| H1 | Adapter/Model 不暴露引擎类型 | crate dependency/API 检查 |
| H2 | 可在执行前获得 projection | file access observer |
| H3 | 未授权 sensitive column 读取前失败 | Q3，content source opens = 0 |
| H4 | Unsupported predicate 正确补算 | Q5 结果断言 |
| H5 | Limit 不被错误下推 | 混合 filter/order fixture |
| H6 | 可取消长查询 | Q4，取消后 1 秒内停止为目标，最多 3 秒 |
| H7 | 预算耗尽返回结构化错误 | max records/bytes 测试 |
| H8 | 1M metadata 峰值内存不超过 512 MiB | release benchmark |
| H9 | 不需要 fork 引擎内核 | dependency/source audit |
| H10 | 支持 macOS/Linux 单二进制或可接受动态依赖 | build/package 验证 |

512 MiB 是初始门槛，可根据 fixture record size 在 ADR 中调整，但两个候选必须使用同一门槛。

## 5. 加权评分

硬门槛全部通过后评分，总分 100。

| 维度 | 权重 | 评分说明 |
|---|---:|---|
| 正确性与 SQL 语义 | 20 | 查询结果、NULL、排序、聚合 |
| 下推与流式能力 | 20 | projection/predicate/limit、首行延迟 |
| 资源控制与取消 | 15 | budget、cancel、backpressure |
| 集成复杂度 | 15 | glue code、类型转换、生命周期 |
| 错误诊断 | 10 | 定位表/列/来源，且不泄密 |
| 性能 | 10 | Q1/Q2 时间与内存 |
| Schema 动态性 | 5 | 运行时表/列和 capability |
| 维护与生态风险 | 5 | release、文档、依赖稳定性 |

每项 0–5 分，维度得分计算：`权重 × 分数 / 5`。

评分锚点：

- 5：直接满足，代码清晰，无特殊绕行。
- 4：少量稳定适配。
- 3：可接受但存在明确限制。
- 2：需要脆弱绕行或较多自维护代码。
- 1：只能演示，难以进入 MVP。
- 0：不支持或结果错误。

## 6. 实现成本记录

每个 spike 记录：

- 新增 Rust LOC，不含 fixture/generated code。
- 引擎专用 crate 数量。
- 编译时间增量。
- 二进制 release 大小。
- 直接/间接依赖数量。
- 为实现 cancellation/budget 增加的特殊代码。
- 尚未解决的 unsafe、panic 或内存物化点。

LOC 不是单独决策依据，只用于解释维护成本。

## 7. 性能记录 JSON

统一输出结构：

```json
{
  "engine": "gitql-or-datafusion",
  "revision": "git-sha",
  "environment": {
    "os": "macos",
    "arch": "arm64",
    "rust": "version",
    "fixture_seed": 42
  },
  "queries": {
    "q1": {
      "median_ms": 0,
      "max_ms": 0,
      "first_row_ms": 0,
      "peak_rss_bytes": 0,
      "records_scanned": 0,
      "bytes_read": 0,
      "pushdowns": []
    }
  }
}
```

## 8. 决策规则

1. 先执行硬门槛，失败者不参与加权评分。
2. 仅一个通过，选择该引擎。
3. 两个都通过，选择加权总分高者。
4. 分差小于 5 分时，优先选择集成复杂度更低者；仍相同则选择资源控制得分高者。
5. 两个都失败，提出最小第三方案或缩减 SQL 功能，不得修改评分规则让候选勉强通过。

## 9. ADR 模板

`docs/adr/0001-query-engine.md` 必须包含：

- Context
- Candidates
- Hard-gate results
- Weighted score
- Benchmark environment
- Decision
- Rejected alternative
- Consequences
- Migration triggers
- Evidence file links

## 10. 迁移触发条件

选定后不因轻微差异保留双引擎。只有出现以下情况才重新评估：

- 无法在合理改动内修复 SQL 正确性问题。
- 典型用户数据超过已承诺资源预算。
- Adapter 下推被引擎结构性阻断。
- 关键平台无法交付。
- 上游停止维护并出现无法接受的安全/兼容风险。
