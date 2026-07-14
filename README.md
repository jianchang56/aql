# AQL

AQL 是一个本地优先、只读为默认的 Agent 数据 SQL 查询工具。它将 Claude Code、Codex、Kimi Code 和 OpenCode 的本机数据映射为统一表，让你可以像查询数据库一样检索会话、消息、工具调用和 token 使用情况。

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
| Claude Code | 支持只读查询 |
| Codex | 支持只读查询 |
| Kimi Code | 支持只读查询 |
| OpenCode | 支持只读查询 |
| Windows | 暂缓 |
| 修改 Agent 原始数据 | 不支持 |

AQL 不上传数据，不调用 Agent 程序，不把认证配置作为查询来源，也不会自动选择或扫描未明确指定的数据源。

## 安装

当前版本需要从源码构建，要求 Rust `1.88.0`：

```bash
cd aql
cargo build --locked --release -p aql-cli
```

也可以安装到 Cargo 的 bin 目录：

```bash
cargo install --locked --path crates/aql-cli
```

生成的程序位于：

```text
target/release/aql
```

可以直接运行，或复制到你自己的 `PATH` 目录。AQL 不会自动使用 `sudo`、修改 shell 配置或从网络下载安装包。

下文假设 `aql` 已加入 `PATH`；如果没有，请替换为 `./target/release/aql`。

GitHub tag release、Homebrew Formula、本地可验证 archive、升级和卸载流程见 [安装文档](docs/installation.md)。

## 快速开始

启动交互式 Shell：

```bash
./target/release/aql
```

查看并选择数据库：

```sql
SHOW DATABASES;
USE codex;
SHOW TABLES;
DESCRIBE sessions;
```

Claude Code 使用内置数据库名 `claude`：

```sql
USE claude;
SELECT model, COUNT(*) AS messages FROM messages GROUP BY model;
```

执行查询：

```sql
SELECT session_id, model, updated_at
FROM sessions
ORDER BY updated_at DESC
LIMIT 20;
```

联合查询所有已安装且兼容的 Agent：

```sql
USE all;

SELECT agent_id, COUNT(*) AS sessions
FROM sessions
GROUP BY agent_id;
```

`all` 必须显式选择。未执行 `USE` 时，AQL 会拒绝查询，不会默认读取全部数据。

## 非交互查询

适合脚本和管道：

```bash
aql query -d codex \
  'SELECT model, COUNT(*) FROM sessions GROUP BY model'

aql query -d all --output json \
  'SELECT agent_id, COUNT(*) AS sessions FROM sessions GROUP BY agent_id'
```

如果数据不在默认位置，可以把绝对路径保存为命名数据库：

```bash
aql database add work \
  --member codex=/absolute/path/to/.codex \
  --acknowledge-persistent-path

aql query -d work 'SELECT COUNT(*) FROM sessions'
```

## 可查询的表

| 表 | 内容 |
|---|---|
| `agents` | 已选择的数据源及能力 |
| `sessions` | 会话元数据 |
| `messages` | 用户与助手消息 |
| `tool_calls` | 工具调用、参数和结果 |
| `usage` | 消息、工具调用和 token 使用事实 |
| `session_edges` | 父子会话、子 Agent 关系 |
| `artifacts` | Codex 明确记录的 patch artifact |

完整字段、类型和访问级别见 [Canonical Schema](docs/canonical-schema-v0.md)。`SELECT *` 只展开安全字段。

## 敏感字段授权

正文、路径和工具载荷默认不可查询。交互模式下，授权仅在当前进程有效：

```sql
GRANT CONTENT FOR SESSION;

SELECT role, content
FROM messages
LIMIT 10;

REVOKE ALL FOR SESSION;
```

非交互模式需要逐次传入参数：

```bash
aql query -d codex --access content \
  'SELECT role, content FROM messages LIMIT 10'
```

可用授权为 `path`、`content`、`tool-input` 和 `tool-output`。授权不会持久化；Secret 字段永远不可授权。

## 输出格式

```bash
aql query -d codex --output table 'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output json  'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output jsonl 'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output csv   'SELECT model FROM sessions LIMIT 10'
```

CSV 默认防止电子表格公式注入。原始公式模式需要同时传入 `--csv-formulas raw` 和 `--acknowledge-raw-csv-formulas`。

## 常用命令

```bash
# 检查 Claude Code、Codex、Kimi Code 和 OpenCode 的固定候选位置，不显示路径
aql database discover

# 有界诊断
aql doctor -d codex

# 标准 EXPLAIN、查询计划兼容参数和执行元数据
aql query -d codex 'EXPLAIN SELECT model FROM sessions'
aql query -d codex --plan 'SELECT model FROM sessions'
aql query -d codex --metadata 'SELECT model FROM sessions LIMIT 10'

# SQL 文件或 stdin
aql query -d codex --file ./query.sql
printf '%s\n' 'SELECT COUNT(*) FROM sessions' | aql query -d codex --stdin

# AST 级命名标量参数绑定
aql query -d codex --param project=demo \
  'SELECT session_id FROM sessions WHERE project = :project'

# 非交互 Schema 和纯 SQL 示例
aql schema sessions
aql schema --output json
aql examples token-usage

# 导出和报告
aql export -d codex \
  --file ./sessions.json 'SELECT * FROM sessions'
aql report -d codex summary

# 自动补全和 man page
aql completions zsh > ./_aql
aql man > ./aql.1
```

`--param NAME=VALUE` 支持 `null`、布尔值、带符号 64 位整数和文本。参数只能替换 `:name` value placeholder，不能替换表名、列名、函数或 SQL 片段；缺失、重复和未使用参数都会被拒绝。

命名数据库、自定义路径、索引、全文搜索、导出和故障排查见 [用户指南](docs/user-guide.md)。

## 安全边界

- 查询路径只读，不修改 Agent 的 SQLite、JSON、JSONL 或项目文件。
- 不进行递归 HOME 扫描；内置发现只检查四个固定候选位置。
- 字段授权、SQL 校验和预算检查在读取敏感内容前完成。
- 默认超时 30 秒，并限制记录数、读取字节、输出大小、单值大小和内存。
- 多数据源查询任一来源失败时，不发布看似成功的部分结果。
- Content 索引是显式 opt-in 的本机明文副本，需要额外确认。
- Claude Code、Codex、Kimi Code 和 OpenCode 的 production Actions 均不支持。

详细边界见 [隐私与威胁模型](docs/privacy-threat-model.md) 和 [兼容性矩阵](docs/compatibility.md)。

## 文档

- [用户指南](docs/user-guide.md)
- [安装、升级与卸载](docs/installation.md)
- [架构概览](docs/architecture.md)
- [兼容性矩阵](docs/compatibility.md)
- [Canonical Schema](docs/canonical-schema-v0.md)
- [隐私与威胁模型](docs/privacy-threat-model.md)
- [完整文档索引](docs/README.md)

开发 Agent 请先阅读 [AGENTS.md](AGENTS.md)。

## License

[MIT](LICENSE)
