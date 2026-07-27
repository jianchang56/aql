---
name: aql
description: 使用 AQL 对本机 Claude Code、Codex、Kimi Code 和 OpenCode 数据执行显式数据库选择、严格只读的 SQL 查询。用于统计 Agent 会话、消息、工具调用、token 用量、会话关系和 artifacts，比较多个 Agent，导出查询结果，检查 canonical schema，或诊断本地 Agent 数据兼容性；涉及正文、路径或工具载荷时也使用此 skill 来实施最小临时授权和隐私保护。
---

# AQL

使用 AQL 的 canonical tables 查询本地 Agent 数据。优先调用 AQL CLI；不要绕过 AQL 直接打开 Agent 的私有文件、SQLite、认证配置或日志。

## 安全约束

- 只执行一条只读 `SELECT`、CTE 或 `EXPLAIN SELECT`。不要尝试 DML、DDL、`ATTACH`、外部文件、URL、任意 catalog 或 shell 插值。
- 始终显式选择数据库。内置名称为 `claude`、`codex`、`kimi`、`opencode` 和 `all`；只有用户明确要求联合查询时才选择 `all`。
- 默认只查询 Safe 字段。仅在任务确实需要时添加最小的 `path`、`content`、`tool-input` 或 `tool-output` 授权；Secret 永远不可授权。
- 先聚合、筛选和限制结果，再考虑读取正文或工具载荷。不要无界输出敏感字段。
- 不要把“AQL 在本机查询”描述成“AI 对话一定离线”。宿主 Agent 可能按自身配置把提示词和选入上下文的工具结果发送给云端模型；继续保持最小字段和有界输出。
- 除非用户明确要求，不要把 SQL、结果或授权持久化。使用 `--output-file` 时只写入用户指定的、不存在的新文件。
- `database add` 和 `database remove` 会修改 AQL 自己的配置。只在用户明确要求配置数据库时使用；它们不得修改 Agent 数据。

## 工作流

### 1. 确认命令

先运行：

```bash
aql --version
```

如果 `aql` 不在 `PATH`，但当前位于 AQL 源码仓库，可将后续命令的 `aql` 替换为：

```bash
cargo run --locked -p aql --
```

不要擅自全局安装工具。

### 2. 选择并检查数据库

```bash
aql database list
aql database discover
aql doctor -d codex
```

根据用户明确指定的 Agent 选择数据库。若用户没有提供足够信息，先列出数据库并请求选择，不要猜测默认数据库。

### 3. 先检查 schema 和示例

```bash
aql schema --list
aql schema sessions
aql examples --list
aql examples token-usage
```

不知道字段名、类型或访问级别时，先运行 `schema`，不要根据底层 Agent 格式猜列。优先复用与任务接近的内置示例。

### 4. 设计有界查询

- 明确列名，避免不必要的 `SELECT *`。
- 明细查询通常添加确定性的 `ORDER BY` 和合理的 `LIMIT`。
- 聚合查询按用户需要的维度分组，避免返回可识别的正文或路径。
- 把用户值绑定为参数，不拼接到 SQL 中：

```bash
aql query -d codex --param model=text:gpt-5 --param minimum=int:10 'SELECT session_id FROM sessions WHERE model = :model AND message_count >= :minimum ORDER BY updated_at DESC LIMIT 20'
```

参数只能替换值，不能替换表名、列名、函数或 SQL 片段。

### 5. 执行并选择输出

面向用户快速查看时使用 table；需要继续处理时优先使用 JSON：

```bash
aql query -d codex --output table 'SELECT model, COUNT(*) AS sessions FROM sessions GROUP BY model ORDER BY sessions DESC'
aql query -d all --output json 'SELECT agent_id, COUNT(*) AS sessions FROM sessions GROUP BY agent_id ORDER BY sessions DESC'
```

只有用户明确要求跨 Agent 联合查询时才使用第二种 `-d all` 形式。使用 JSONL 处理逐行结果，使用 CSV 交付电子表格数据；AQL 会始终进行 CSV 公式防护。

### 6. 按需授予敏感访问

遇到 `requires --access ...` 时，先检查对应表的 schema，再仅添加报错所需授权：

```bash
aql query -d codex --access content --output json 'SELECT role, content FROM messages ORDER BY created_at DESC LIMIT 10'
```

不要为了方便一次性添加所有授权。查询结束后不要把授权转化为环境默认或持久配置。

### 7. 诊断失败

```bash
aql doctor -d codex
aql --error-format json query -d codex 'SELECT COUNT(*) FROM sessions'
aql query -d codex --diagnostics 'SELECT session_id FROM sessions LIMIT 10'
```

数据库不可用时先用 `database discover` 和 `doctor`。预算或超时时先缩小投影、增加过滤和 `LIMIT`，确认确有必要后再调整上限。格式漂移时报告兼容性问题，不要绕过 schema 校验或直接读取底层文件。

## 配置命名数据库

仅在用户明确要求保存自定义 Agent 路径时运行：

```bash
aql database add work --member codex=/absolute/path/to/.codex --acknowledge-persistent-path
aql database show work
aql database remove work
```

使用当前平台的绝对路径，拒绝重复或重叠成员。默认保持路径遮罩；只有用户确实需要查看路径时才使用 `database show work --access path`。

## 交付结果

说明使用的数据库、查询目标、临时授权和返回规模。只呈现完成任务所需的信息；路径、正文和工具载荷不得超出用户明确授权的范围。
