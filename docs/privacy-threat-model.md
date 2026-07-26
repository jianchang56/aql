# AQL 隐私与威胁模型

## 目标

AQL 允许用户对明确选择的本机 Agent 数据执行受限只读 SQL，同时防止：

- 未授权正文、路径或工具载荷读取；
- SQL 写入、外部文件或网络访问；
- 隐式数据库选择和无界 discovery；
- 跨来源预算倍增；
- 部分成功结果被误认为完整结果；
- 路径、主机身份、凭据或查询内容被持久化；
- 输出文件覆盖、symlink 跟随和目录替换攻击。

## 信任边界

不受信任输入包括 SQL、参数、数据库配置、Agent 数据文件、SQLite/JSON/JSONL 内容、文件名、时间戳和 schema version。

AQL 自己拥有的持久状态只有：

- `aql-databases-v1` 配置数据库；
- installation salt。

AQL 不持久化 SQL、shell history、查询结果、授权、诊断、凭据或敏感 payload。

## AI 与 AQL 的边界

AQL 进程不调用模型，也不把 Agent 数据上传到网络。通过 Skill 调用 AQL 的 Claude Code、Codex、Kimi Code、OpenCode 或其他宿主 Agent 不在这个进程边界内。

如果宿主 Agent 使用云端模型，它可能按照自身产品配置，把用户提示词、命令以及选入上下文的 AQL 工具输出发送给模型服务。AQL 的只读、字段授权和本地执行保证不能替代宿主 Agent 的隐私设置、企业数据策略或服务条款。面向 AI 的工作流应先使用聚合、Safe 字段和有界输出，仅在任务确实需要时扩大字段授权与返回范围。

## 显式数据库选择

没有默认数据库。非交互查询和 doctor 必须传 `-d`；Shell 必须先 `USE`。`all` 必须显式选择。

内置 discovery 只检查四个固定候选位置，不递归扫描 HOME、不启动 Agent 进程、不输出真实路径。配置数据库要求绝对路径和明确的 `--acknowledge-persistent-path`。

所有 database member 在 Adapter probe 前统一解析，拒绝未知 Adapter、相对路径、symlink、重复和重叠 root。discovery 与配置校验的结果在查询绑定时可能已过期，因此查询路径会以 nofollow 方式重新验证 member root 的全部路径组件，然后才 canonicalize。

## SQL firewall

只接受恰好一条 SELECT/CTE 或 EXPLAIN SELECT。拒绝：

- DML、DDL、COPY、ATTACH、PRAGMA；
- 多语句；
- 外部文件、URL、任意 catalog 和 table function；
- shell 插值；
- 非 canonical tables；
- 非白名单函数和过高 AST complexity。

命名参数只替换 AST value placeholder，不能引入 SQL fragment、identifier 或函数。

## 字段授权

访问类：Safe、Path、Content、ToolInput、ToolOutput、Secret。

查询计划在 Adapter scan 之前完成 column lineage 与 projection 授权。未授权字段必须在打开 heavy content source 前失败。Secret 没有授权形式。

`SELECT *` 只展开 Safe 列。授权只在当前查询或当前 Shell 进程内存在，不支持数据库级或环境级默认授权。

## Adapter 读取

每个 Adapter 有固定文件与 schema allowlist，并实施：

- no-follow path traversal；
- root/file identity validation；
- fixed append boundary 或 read-only SQLite snapshot；
- schema/protocol fingerprint；
- projection-aware parsing；
- 单值大小检查；
- cancellation 和 shared budget。

Agent auth、配置、日志、插件、项目树和未声明 sibling 文件不属于 source。

## Federation 与预算

全部来源共享一个 `ResourceBudget`、deadline 和 cancellation token。clone budget 共享计数，不为每个来源重新分配上限。

默认限制：

| 资源 | 默认值 |
|---|---:|
| records | 100,000 |
| source bytes | 256 MiB |
| output bytes | 64 MiB |
| single sensitive value | 16 MiB |
| execution memory | 256 MiB |
| timeout | 30 秒 |

查询引擎的执行 disk spill 禁用。事务输出只使用无路径、进程退出即关闭的 private 匿名临时文件，不创建可持久引用的查询结果；表格格式另用同类匿名行缓冲计算全局列宽。timeout、Ctrl-C、budget error 和 broken stdout 传播 cancellation。

## 结果发布

stdout 查询按批流式渲染到匿名事务缓冲，只有执行到 EOF、元数据完成且渲染成功后才发布结果；任一来源失败时没有部分结果。

`--output-file`：

- 在 source read 前验证目标目录和目标不存在；
- 不跟随目标或父目录 symlink；
- 临时文件 mode `0600`；
- 写完后 fsync；
- 验证目录 identity 未变化；
- no-replace rename；
- 失败清理临时文件。

CSV 始终转义公式形状文本，避免电子表格公式执行：字符串单元格和列头以 `= + - @ tab CR` 开头，或以前导空白（空格、tab、CR、LF）后接 `= + - @` 时，一律加 `'` 前缀。

## 配置数据库

配置 root 使用 private directory、ownership marker、known-file allowlist、writer lock、fsync 和原子替换。配置只包含数据库名、Adapter ID 和绝对 member path。

旧 schema、未知字段、未知文件、宽权限、root overlap、symlink 和 state replacement 全部 fail closed。配置 store 损坏或无法打开时，错误在内置数据库回退之前传播：包括内置名称在内的所有数据库解析都 fail closed。

## 诊断与错误

错误和 `--diagnostics` 可以包含 Adapter、source ID、table、stage、预算计数和 format fingerprint，但不得包含 SQL literal、参数值、授权值、正文、工具 payload、真实路径或 host identity。

JSON 错误固定包含 category、stage、message、hint、location 和 exit_code；location 无可靠来源时为 null。AQL 不从 parser 错误文本猜测位置。

## 明确不支持

- 修改、归档、重命名或删除 Agent 数据；
- 内容派生的持久化副本或查询缓存；
- 网络上传或远程 catalog；
- 自动选择数据库；
- 覆盖已有输出文件；
- Secret 授权。
