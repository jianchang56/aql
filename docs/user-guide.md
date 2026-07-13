# AQL 用户指南

本指南覆盖日常查询、命名数据库、输出、导出、索引和排障。首次使用可以先阅读根目录 [README](../README.md)。

## 交互式 Shell

```bash
aql
aql shell
aql shell -d codex
```

无参数启动只接受真实终端。stdin 或 stdout 被重定向时会立即失败；自动化任务请使用 `query`。

常用 Shell 命令：

| 命令 | 作用 |
|---|---|
| `SHOW DATABASES;` | 列出可用数据库，不显示路径 |
| `USE name;` | 显式选择数据库 |
| `SHOW TABLES;` | 列出 canonical tables |
| `DESCRIBE table;` | 显示字段、类型、nullable 和访问级别 |
| `SHOW ACCESS;` | 显示当前进程内授权 |
| `SHOW STATUS;` | 显示数据库、授权、预算和 history 状态 |
| `HELP;` | 显示 Shell 命令摘要 |
| `GRANT ... FOR SESSION;` | 临时授予敏感字段访问 |
| `REVOKE ALL FOR SESSION;` | 清除临时授权 |
| `SELECT ...;` | 执行只读 SQL |
| `EXIT;` / `QUIT;` | 退出并清除进程内状态 |

Shell 支持多行 SELECT/CTE、`EXPLAIN SELECT`、表/字段/数据库补全，以及仅在当前进程内存在的方向键 history。prompt 显示当前数据库，查询结束显示返回行数和耗时。`Ctrl-C` 清空未完成语句或取消当前查询，但不退出 Shell。语句最大 64 KiB；退出后 history、结果、授权和当前数据库全部丢弃。

## 数据库选择

内置数据库：

- `claude`：`$HOME/.claude`
- `codex`：`$HOME/.codex`
- `kimi`：`$HOME/.kimi-code`
- `opencode`：`${XDG_DATA_HOME:-$HOME/.local/share}/opencode`
- `all`：显式联合当前兼容的内置数据库

`SHOW DATABASES` 只检查这些固定候选，不递归扫描 HOME、不调用 Agent 程序、不输出路径。选择单个数据库只探测对应候选；只有 `SHOW DATABASES` 和显式 `all` 会检查全部候选。

### 自定义路径和命名数据库

保存命名数据库需要明确确认持久化路径：

确认后只会保存 adapter ID 与精确绝对路径；SQL、查询结果、访问授权和凭据不会保存。

```bash
aql database add work \
  --member claude=/absolute/path/to/.claude \
  --member codex=/absolute/path/to/.codex \
  --member opencode=/absolute/path/to/opencode \
  --acknowledge-persistent-path

aql database list
aql database show work
aql query -d work 'SELECT COUNT(*) FROM sessions'
```

保存后可以在 Shell 中直接选择：

```sql
USE work;
```

命名数据库只保存名称、Adapter 类型和绝对路径，不保存授权、SQL、正文、凭据或 installation salt。删除数据库配置不会删除 Agent 数据、索引或 Action audit：

```bash
aql database remove work
```

旧 `profile`、`--profile`、`--source` 和 `--data-root` 暂时保留为隐藏兼容入口，新用法统一使用 database 和 `-d`。

## SQL 能力

AQL 支持受限的只读 SQL，包括：

- `SELECT`、CTE
- `WHERE`
- `JOIN`
- `GROUP BY`、聚合函数
- `ORDER BY`
- `LIMIT`
- 固定白名单函数和隐私函数

AQL 拒绝多条 SQL、DML、DDL、`COPY`、`ATTACH`、unsafe pragma、外部表、文件/URL、任意 catalog、table function 和 shell 插值。

示例：

```sql
SELECT agent_id, model, COUNT(*) AS sessions
FROM sessions
WHERE archived = false
GROUP BY agent_id, model
ORDER BY sessions DESC;
```

```sql
WITH recent AS (
  SELECT session_id, agent_id, updated_at
  FROM sessions
  WHERE updated_at IS NOT NULL
)
SELECT agent_id, COUNT(*)
FROM recent
GROUP BY agent_id;
```

## 敏感字段

访问类别：

| 授权 | 示例字段 |
|---|---|
| `path` | cwd、artifact path |
| `content` | title、preview、message content、artifact payload |
| `tool-input` | 工具参数 |
| `tool-output` | 工具结果 |

交互模式：

```sql
GRANT CONTENT FOR SESSION;
GRANT TOOL OUTPUT FOR SESSION;

SELECT tool_name, output
FROM tool_calls
LIMIT 10;

REVOKE ALL FOR SESSION;
```

非交互模式：

```bash
aql query -d codex \
  --access content \
  --access tool-output \
  'SELECT tool_name, output FROM tool_calls LIMIT 10'
```

授权仅对当前进程或当前命令有效。Secret 永远不可授权。敏感输出被重定向时，AQL 会向 stderr 写 warning，不污染结构化 stdout。

## 输出、计划和元数据

输出格式：

```bash
aql query -d codex --output table 'SELECT model FROM sessions'
aql query -d codex --output json  'SELECT model FROM sessions'
aql query -d codex --output jsonl 'SELECT model FROM sessions'
aql query -d codex --output csv   'SELECT model FROM sessions'
```

JSON/JSONL/CSV 保留类型和 RFC 3339 时间。CSV 默认转义 `= + - @ TAB CR` 公式前缀；raw 模式必须明确确认：

```bash
aql query -d codex --output csv \
  --csv-formulas raw \
  --acknowledge-raw-csv-formulas \
  'SELECT model FROM sessions'
```

查看安全化查询计划和执行元数据：

```bash
aql query -d codex --plan \
  'SELECT session_id FROM sessions WHERE updated_at IS NOT NULL'

aql query -d codex --metadata \
  'SELECT session_id FROM sessions LIMIT 10'
```

也可以使用标准只读 `EXPLAIN`，它只生成脱敏计划而不执行查询：

```bash
aql query -d codex 'EXPLAIN SELECT session_id FROM sessions LIMIT 10'
```

复杂 SQL 可以从有界本地 regular file 或 stdin 读取；三种输入方式互斥：

```bash
aql query -d codex --file ./query.sql
printf '%s\n' 'SELECT COUNT(*) FROM sessions' | aql query -d codex --stdin
```

非交互 Schema 和示例：

```bash
aql schema
aql schema sessions
aql schema --output json
aql examples
aql examples sessions-by-model
```

计划和元数据写入 stderr，不包含 SQL literal 或原始路径。

## 导出和报告

portable JSON 导出：

```bash
aql export -d work \
  'SELECT agent_id, model, SUM(total_tokens) FROM usage GROUP BY agent_id, model'

aql export -d work --output-file ./usage.json \
  'SELECT * FROM usage'
```

文件目标必须不存在。AQL 使用同目录 private 临时文件和 atomic no-replace 发布，不提供 overwrite/force。

预定义 Markdown 报告：

```bash
aql report -d work summary
aql report -d work --access path project
```

stdout export/report 会在全部来源成功后一次发布，避免部分成功结果。

## 可选索引和全文搜索

Safe metadata 索引：

```bash
aql index build -d codex --policy metadata
aql index update -d codex --policy metadata
aql index status -d codex
```

Content 索引是 AQL state 中的本机明文副本，必须同时授权并确认：

```bash
aql index build -d codex \
  --policy content \
  --access content \
  --acknowledge-persistent-sensitive-copy

aql search -d codex \
  --access content \
  '"connection timeout"'
```

Content 索引不包含工具输入/输出、reasoning、permission/share/account/credential、日志、配置或项目文件。`index clear` 和 `index repair` 只操作 marker/catalog 验证后的 AQL-owned state，不承诺法证擦除。

## 资源预算

默认值：

| 资源 | 默认限制 |
|---|---:|
| timeout | 30 秒 |
| records | 100,000 |
| source bytes | 256 MiB |
| output | 64 MiB |
| single sensitive value | 16 MiB |
| DataFusion memory | 256 MiB |
| disk spill | 禁用 |

对应参数：

```text
--timeout
--max-records
--max-bytes-read
--max-output-bytes
--max-single-value-bytes
--max-memory-bytes
```

预算、超时、Ctrl-C 或 broken stdout 会传播 cancellation。失败不会返回看似成功的部分结果。

## 排障

- `No database selected`：运行 `SHOW DATABASES;` 和 `USE <name>;`。
- `unknown or unavailable database`：运行 `aql database discover` 和 `aql database list`；确认 Agent 使用默认位置，或创建命名数据库。
- `requires --access ...`：只在确实需要该字段时添加相应临时授权。
- `source path is unavailable`：确认路径存在、为绝对路径且没有 symlink/type/permission 问题。
- `future migration` / `protocol drift`：当前 Agent 格式超出兼容范围；不要绕过 schema 校验，参阅 [兼容性矩阵](compatibility.md)。
- `query timed out` / budget exceeded：增加过滤和 LIMIT；确认后再有界提高对应预算。
- `raw CSV formulas require ...`：保留默认 safe 模式，或同时传入 raw acknowledgement。
- `install prefix already exists`：升级应使用新的版本化 prefix，不要覆盖。

仍无法判断时先运行有界诊断：

```bash
aql doctor -d codex
```

诊断输出包含 format/capability/warning，不输出消息或工具载荷。

所有命令支持稳定文本错误；自动化可以请求单行 JSON 错误：

```bash
aql --error-format json query -d missing 'SELECT COUNT(*) FROM sessions'
```

脚本可以增加全局 `--quiet` 抑制非必要 warning 和 Shell 摘要；错误仍使用统一的
`--error-format json` 单行对象，显式请求的 `--plan` 与 `--metadata` 不会被隐藏。

JSON 包含 `category`、`message`、`hint` 和 `exit_code`。
