# AQL 用户指南

AQL 的完整用户模型只有一条路径：选择数据库，查询 canonical tables，输出结果。

## 交互式 Shell

```bash
aql
aql shell
aql shell -d codex
```

无参数启动只接受真实终端；自动化任务使用 `aql query`。

| 命令 | 作用 |
|---|---|
| `SHOW DATABASES;` | 列出数据库，不显示路径 |
| `USE name;` | 显式选择数据库 |
| `SHOW TABLES;` | 列出 canonical tables |
| `DESCRIBE table;` | 显示字段、类型和访问级别 |
| `SHOW ACCESS;` | 显示当前临时授权 |
| `SHOW STATUS;` | 显示数据库、授权和预算状态 |
| `GRANT ... FOR SESSION;` | 授予当前进程内敏感访问 |
| `REVOKE ALL FOR SESSION;` | 清除临时授权 |
| `HELP;` | 显示 Shell 命令摘要 |
| `EXIT;` / `QUIT;` | 退出 Shell |

Shell 支持多行 SELECT/CTE、`EXPLAIN SELECT`、表/字段/数据库补全和进程内方向键 history。退出后 history、结果、授权和数据库选择全部丢弃。

## 数据库选择

内置数据库：

- `claude`：`$HOME/.claude`
- `codex`：`$HOME/.codex`
- `kimi`：`$HOME/.kimi-code`
- `opencode`：`${XDG_DATA_HOME:-$HOME/.local/share}/opencode`
- `all`：显式联合当前兼容的内置数据库

```bash
aql database list
aql database discover
aql doctor -d codex
```

`database discover` 只检查四个固定候选位置，不递归扫描 HOME、不调用 Agent 程序、不输出路径。

### 命名数据库

```bash
aql database add work \
  --member claude=/absolute/path/to/.claude \
  --member codex=/absolute/path/to/.codex \
  --acknowledge-persistent-path

aql database show work
aql database show work --access path
aql database remove work
```

配置格式为 `aql-databases-v1`，只保存名称、Adapter ID 和绝对路径。它不保存 SQL、结果、授权、凭据或 installation salt。名称冲突时，配置数据库优先于同名内置数据库。

## 查询

```bash
aql query -d codex \
  'SELECT model, COUNT(*) AS sessions FROM sessions GROUP BY model'
```

AQL 支持受限的只读 SQL：

- SELECT、CTE、WHERE、JOIN、GROUP BY、ORDER BY、LIMIT
- 固定聚合函数、标量函数和隐私函数
- `EXPLAIN SELECT ...`

AQL 拒绝多条 SQL、DML、DDL、COPY、ATTACH、外部文件或 URL、任意 catalog、table function 和 shell 插值。

只读元数据表包括 `aql_tables`、`aql_columns`、`aql_sources` 和 `aql_capabilities`，分别描述表、列、实际 source 和 source 能力。`SHOW TABLES;` 与 `DESCRIBE sessions;` 会重写到这些表，因此和普通查询共享授权、预算、deadline 与事务发布。

SQL 可以来自一个有界 regular file 或 stdin：

```bash
aql query -d codex --file ./query.aql
printf '%s\n' 'SELECT COUNT(*) FROM sessions' | aql query -d codex --stdin
```

直接参数、`--file` 和 `--stdin` 三者互斥；`--file` 只接受一个最大 64 KiB 的 `.aql` 只读脚本。

显式分页使用 `ORDER BY ... LIMIT ... OFFSET ...`。分页查询缺少 `ORDER BY` 时会提示结果顺序不稳定，AQL 不会添加隐式排序。

## Schema 和示例

```bash
aql schema --list
aql schema sessions
aql schema --output json sessions
aql schema

aql examples --list
aql examples sessions-by-model
aql examples token-usage
```

首次查看建议先 `schema --list`，再查看单表。不带表名的 `schema` 输出全部字段。

## 敏感字段

| 授权 | 示例字段 |
|---|---|
| `path` | cwd、project、artifact path |
| `content` | title、preview、message content、artifact payload |
| `tool-input` | 工具参数 |
| `tool-output` | 工具结果 |

Safe 字段无需授权；Secret 永远不可授权。

```bash
aql query -d codex --access content \
  'SELECT role, content FROM messages LIMIT 10'

aql query -d codex \
  --access tool-input \
  --access tool-output \
  'SELECT tool_name, arguments, output FROM tool_calls LIMIT 10'
```

授权只对当前查询有效，不支持环境默认，也不会持久化。

## 参数绑定

```bash
aql query -d codex \
  --param project=text:demo \
  --param minimum=int:10 \
  --param active=bool:true \
  'SELECT session_id FROM sessions WHERE project = :project AND message_count >= :minimum AND archived != :active'
```

未加前缀时，`null`、`true`、`false` 和整数形状会自动绑定对应类型，其余值为文本。显式 `text:`、`int:`、`float:`、`bool:` 可指定类型。缺失、重复、未使用参数和非命名 placeholder 都会被拒绝。

固定函数白名单包含 `lower`、`upper`、`length`、`substr`、`trim`、`replace`、`coalesce`、`nullif`、`date_trunc`、`date_part`、`round`、`sum`、`count`、`redact` 和 `mask_path`。隐私函数的参数策略固定，不能动态指定。

## 输出

```bash
aql query -d codex --output table 'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output json  'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output jsonl 'SELECT model FROM sessions LIMIT 10'
aql query -d codex --output csv   'SELECT model FROM sessions LIMIT 10'
```

JSON 保留 null、布尔、整数、timestamp 和 JSON 列类型。CSV 使用 RFC 4180，并始终转义电子表格公式形状文本；不存在 raw 模式。

原子文件输出：

```bash
aql query -d codex \
  --output json \
  --output-file ./result.json \
  'SELECT * FROM usage'
```

目标必须不存在。AQL 在同目录创建 mode `0600` 的临时文件，完成后 fsync 并 no-replace rename；失败不会留下目标文件或部分成功输出。

## EXPLAIN 和诊断

```bash
aql query -d codex \
  'EXPLAIN SELECT session_id FROM sessions WHERE model = '\''gpt-5'\'''

aql query -d codex --diagnostics \
  'SELECT session_id FROM sessions LIMIT 10'
```

`EXPLAIN` 输出授权后的表、列、访问需求、pushdown、预算和来源能力，不执行查询。计划使用默认 table 输出，可以通过 `--output-file` 原子写入；其他 `--output` 格式会被拒绝。`--diagnostics` 向 stderr 输出实际发生阶段的耗时、来源 ID、扫描 pushdown、读取量和 warning；不包含 SQL literal、参数值、真实路径或结果。

## 资源预算

公开参数：

```text
--timeout              默认 30s
--max-output-bytes     默认 64MiB
```

内部安全预算始终启用：

| 环境变量 | 默认值 |
|---|---:|
| `AQL_MAX_RECORDS` | 100,000 |
| `AQL_MAX_BYTES_READ` | 256 MiB |
| `AQL_MAX_SINGLE_VALUE_BYTES` | 16 MiB |
| `AQL_MAX_MEMORY_BYTES` | 256 MiB |

这些环境变量只调整上限，不选择数据库、不授予敏感访问。命令行仍可设置 `AQL_TIMEOUT` 和 `AQL_MAX_OUTPUT_BYTES` 对应参数。

## 错误和排障

- 缺少数据库：运行 `aql database list`，然后使用 `-d <database>`。
- 数据库不可用：运行 `aql database discover` 和 `aql doctor -d <database>`。
- `requires --access ...`：先用 `aql schema <table>` 确认访问级别，再添加最小授权。
- 格式漂移：参阅 [兼容性](compatibility.md)，不要绕过 schema 验证。
- timeout 或 budget exceeded：增加 WHERE/LIMIT，确认后再提高对应上限。

自动化可以请求单行 JSON 错误：

```bash
aql --error-format json query -d missing 'SELECT COUNT(*) FROM sessions'
```

JSON 包含 `category`、`stage`、`message`、`hint`、`location` 和 `exit_code`；没有可靠位置时 `location` 为 null。`--quiet` 只抑制非必要 warning 和 Shell 摘要，不隐藏错误或显式诊断。
