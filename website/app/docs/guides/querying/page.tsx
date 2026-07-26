import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "编写查询",
  description: "使用 canonical schema、命名参数和有界只读 SQL。",
};

export default function QueryingPage() {
  return (
    <DocsPage
      currentPath="/docs/guides/querying"
      title="从 schema 写出可靠查询"
      description="不要猜测底层 Agent 文件字段。AQL 只公开 canonical tables，并在 SQL engine 前执行只读防火墙与字段授权。"
    >
      <DocsSection title="先查看表与字段">
        <CodeBlock
          ai="使用 $aql 列出 canonical tables，查看 sessions schema，并列出内置查询示例与 token-usage 示例。"
          code={[
            "aql schema --list",
            "aql schema sessions",
            "aql examples --list",
            "aql examples token-usage",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
      </DocsSection>

      <DocsSection title="支持的查询形状">
        <p>
          支持 SELECT、CTE、WHERE、JOIN、GROUP BY、ORDER BY、LIMIT 和固定函数白名单。AQL
          拒绝多语句、DML、DDL、COPY、ATTACH、外部文件、URL 和 shell 插值。
        </p>
        <CodeBlock
          ai="使用 $aql 查询有更新时间的会话，按模型统计数量并降序返回。只使用只读 canonical SQL。"
          code={[
            "WITH recent AS (",
            "  SELECT session_id, model, updated_at",
            "  FROM sessions",
            "  WHERE updated_at IS NOT NULL",
            ")",
            "SELECT model, COUNT(*) AS sessions",
            "FROM recent",
            "GROUP BY model",
            "ORDER BY sessions DESC;",
          ].join("\n")}
          language="sql"
          label="sql"
        />
      </DocsSection>

      <DocsSection title="绑定用户参数">
        <CodeBlock
          ai="使用 $aql 在 codex 中查找模型为 gpt-5 且消息数至少为 10 的会话。把用户值作为参数绑定，按更新时间降序，最多返回 20 条。"
          code={[
            "aql query -d codex \\",
            "  --param model=text:gpt-5 \\",
            "  --param minimum=int:10 \\",
            "  'SELECT session_id FROM sessions",
            "   WHERE model = :model AND message_count >= :minimum",
            "   ORDER BY updated_at DESC LIMIT 20'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          参数只能替换值，不能替换表名、列名、函数或 SQL 片段。缺失、重复和未使用参数都会被拒绝。
        </p>
      </DocsSection>

      <DocsSection title="分页与顺序">
        <CodeBlock
          ai="使用 $aql 分页查询 sessions：按更新时间降序和 session_id 稳定排序，跳过前 100 条并返回 50 条。"
          code={[
            "SELECT session_id, model, updated_at",
            "FROM sessions",
            "ORDER BY updated_at DESC, session_id",
            "LIMIT 50 OFFSET 100;",
          ].join("\n")}
          language="sql"
          label="sql"
        />
        <DocsNote title="分页必须有 ORDER BY" tone="amber">
          AQL 不会添加隐式排序。分页查询缺少 <code>ORDER BY</code> 时会提示结果顺序不稳定。
        </DocsNote>
      </DocsSection>

      <DocsSection title="查看授权后的计划">
        <CodeBlock
          ai="使用 $aql 查看这个查询在授权检查后的执行计划：从 codex sessions 返回 session_id 和 model，限制 20 条。"
          code={[
            "aql query -d codex \\",
            "  'EXPLAIN SELECT session_id, model FROM sessions LIMIT 20'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
      </DocsSection>
    </DocsPage>
  );
}
