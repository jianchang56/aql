# AQL

AQL 是一个本地优先、严格只读的 Agent 数据 SQL CLI。它把 Claude Code、Codex、Kimi Code 和 OpenCode 的本机数据映射为统一表，用户只需要：

```text
database → canonical tables → read-only SELECT
```

```sql
SHOW DATABASES;
USE codex;

SELECT model, COUNT(*) AS sessions
FROM sessions
GROUP BY model
ORDER BY sessions DESC;
```

## 支持范围

| 项目 | 状态 |
|---|---|
| macOS / Linux | 支持 |
| Claude Code / Codex / Kimi Code / OpenCode | 支持只读查询 |
| Windows | 暂缓 |
| 修改 Agent 原始数据 | 不支持 |

AQL 不上传数据，不调用 Agent 程序，不读取认证配置，也不会自动选择数据库或扫描未明确选择的位置。

## 安装

当前版本需要 Rust `1.96.1`：

```bash
cargo build --locked --release -p aql-cli
```

或者安装到 Cargo bin 目录：

```bash
cargo install --locked --path crates/aql-cli
```

下文假设 `aql` 已加入 `PATH`。GitHub release、Homebrew Formula、本地可验证 archive、升级和卸载见 [安装文档](docs/installation.md)。

## 快速开始

启动交互式 Shell：

```bash
aql
```

```sql
SHOW DATABASES;
USE codex;
SHOW TABLES;
DESCRIBE sessions;

SELECT session_id, model, updated_at
FROM sessions
ORDER BY updated_at DESC
LIMIT 20;
```

`all` 必须显式选择：

```sql
USE all;
SELECT agent_id, COUNT(*) FROM sessions GROUP BY agent_id;
```

未执行 `USE` 时，Shell 会拒绝查询。

## 非交互查询

```bash
aql query -d codex \
  'SELECT model, COUNT(*) FROM sessions GROUP BY model'

aql query -d all --output json \
  'SELECT agent_id, COUNT(*) AS sessions FROM sessions GROUP BY agent_id'
```

结果可以原子写入一个不存在的新文件：

```bash
aql query -d codex \
  --output json \
  --output-file ./sessions.json \
  'SELECT * FROM sessions'
```

文件使用 private 临时文件和 no-replace 原子发布，不提供 overwrite 或 force。

## 可查询表

| 表 | 内容 |
|---|---|
| `agents` | 当前选择的数据源及能力 |
| `sessions` | 会话元数据 |
| `messages` | 用户与助手消息 |
| `tool_calls` | 工具调用、参数和结果 |
| `usage` | 消息、工具调用和 token 使用事实 |
| `session_edges` | 父子会话和子 Agent 关系 |
| `artifacts` | 明确记录的 patch artifact |

完整字段、类型和访问级别见 [Canonical Schema](docs/canonical-schema-v0.md)。除 canonical tables 外，还可以查询只读元数据表 `aql_tables`、`aql_columns`、`aql_sources` 和 `aql_capabilities`。`SELECT *` 只展开 Safe 字段。

## 敏感字段授权

正文、路径和工具载荷默认不可读取。Shell 授权只在当前进程有效：

```sql
GRANT CONTENT FOR SESSION;
SELECT role, content FROM messages LIMIT 10;
REVOKE ALL FOR SESSION;
```

非交互查询逐次授权：

```bash
aql query -d codex --access content \
  'SELECT role, content FROM messages LIMIT 10'
```

可用授权为 `path`、`content`、`tool-input` 和 `tool-output`。Secret 永远不可授权。

## 输出和参数

```bash
aql query -d codex --output table 'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output json  'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output jsonl 'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output csv   'SELECT model FROM sessions LIMIT 10'
```

CSV 永远进行电子表格公式防护。

命名参数只绑定 SQL value placeholder：

```bash
aql query -d codex \
  --param project=text:demo \
  --param minimum=int:10 \
  'SELECT session_id FROM sessions WHERE project = :project AND message_count >= :minimum'
```

支持 `null`、布尔、带符号 64 位整数、有限浮点数和文本；`text:`、`int:`、`float:`、`bool:` 可显式指定类型。参数不能替换表名、列名、函数或 SQL 片段。

## 数据库

内置数据库为 `claude`、`codex`、`kimi`、`opencode` 和显式 federation `all`。

```bash
aql database list
aql database discover
aql doctor -d codex
```

自定义位置可以保存为命名数据库：

```bash
aql database add work \
  --member codex=/absolute/path/to/.codex \
  --acknowledge-persistent-path

aql database show work
aql query -d work 'SELECT COUNT(*) FROM sessions'
aql database remove work
```

配置只保存数据库名、Adapter ID 和绝对路径，不保存 SQL、结果、授权或凭据。

## Schema、示例和诊断

```bash
aql schema --list
aql schema sessions
aql examples --list
aql examples token-usage

aql query -d codex 'EXPLAIN SELECT model FROM sessions'
aql query -d codex --diagnostics 'SELECT model FROM sessions LIMIT 10'
```

`EXPLAIN` 只输出授权后的查询计划，并可通过 `--output-file` 原子写入；它只使用默认 table 输出。`--diagnostics` 向 stderr 输出去敏的来源、扫描、预算和阶段耗时。

## 安全边界

- 恰好一条只读 canonical query；拒绝 DML、DDL、外部文件、URL、catalog 和 shell 插值。
- 不递归扫描 HOME；内置发现只检查四个固定候选位置。
- 在敏感 source read 之前完成 SQL 校验、字段授权和投影检查。
- 全部来源共享一个预算、deadline 和 cancellation token。
- 任一来源失败时，不发布部分成功结果。
- 普通查询不在 Agent 数据旁写 sidecar，也不修改 SQLite、JSON 或 JSONL。

详细使用方法见 [用户指南](docs/user-guide.md)，安全控制见 [隐私与威胁模型](docs/privacy-threat-model.md)。

## 文档

- [用户指南](docs/user-guide.md)
- [安装、升级与卸载](docs/installation.md)
- [架构](docs/architecture.md)
- [兼容性](docs/compatibility.md)
- [Canonical Schema](docs/canonical-schema-v0.md)
- [隐私与威胁模型](docs/privacy-threat-model.md)
- [文档索引](docs/README.md)

## License

[MIT](LICENSE)
