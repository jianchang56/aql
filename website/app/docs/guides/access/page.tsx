import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "敏感字段",
  description: "理解 Safe、Path、Content、工具载荷和 Secret 的访问规则。",
};

const accessClasses = [
  ["Safe", "无需授权，例如 session_id、model、时间和计数"],
  ["Path", "cwd、project、artifact path"],
  ["Content", "title、preview、消息正文和 artifact payload"],
  ["ToolInput", "工具参数"],
  ["ToolOutput", "工具结果"],
  ["Secret", "永远不可授权"],
];

export default function AccessPage() {
  return (
    <DocsPage
      currentPath="/docs/guides/access"
      title="只授予完成任务所需的字段"
      description="AQL 在敏感 source read 之前完成 SQL 校验、投影检查与访问授权。未授权字段不会用 NULL 伪装成正常结果。"
    >
      <DocsSection title="访问级别">
        <div className="overflow-hidden rounded-2xl border border-border bg-card">
          {accessClasses.map(([name, description]) => (
            <div
              key={name}
              className="grid gap-1 border-b border-border px-4 py-3 last:border-0 sm:grid-cols-[8rem_1fr]"
            >
              <code className="w-fit">{name}</code>
              <span className="text-sm leading-6">{description}</span>
            </div>
          ))}
        </div>
      </DocsSection>

      <DocsSection title="非交互查询授权">
        <CodeBlock
          ai="使用 $aql 查询 codex 最近 10 条消息的角色和正文。先确认 schema，只申请 content 这一项临时授权。"
          code={[
            "aql query -d codex --access content \\",
            "  'SELECT role, content",
            "   FROM messages",
            "   ORDER BY created_at DESC",
            "   LIMIT 10'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
      </DocsSection>

      <DocsSection title="Shell 临时授权">
        <CodeBlock
          ai="在 AQL Shell 中临时授予 Content 访问，查看当前授权，读取最多 10 条消息，然后立即撤销全部授权。"
          code={[
            "GRANT CONTENT FOR SESSION;",
            "SHOW ACCESS;",
            "SELECT role, content FROM messages LIMIT 10;",
            "REVOKE ALL FOR SESSION;",
          ].join("\n")}
          language="sql"
          label="aql shell"
        />
        <DocsNote title="授权不会持久化" tone="mint">
          非交互授权只对当前查询有效；Shell 授权只对当前进程有效。AQL
          不支持通过配置或环境设置永久默认授权。
        </DocsNote>
      </DocsSection>

      <DocsSection title="安全的查询顺序">
        <ol className="grid gap-3">
          {[
            "先使用 Safe 字段完成聚合和筛选。",
            "用 schema 确认真正需要的访问级别。",
            "添加最小授权与合理 LIMIT。",
            "不要把敏感结果写入未明确指定的位置。",
          ].map((item, index) => (
            <li
              key={item}
              className="grid grid-cols-[2rem_1fr] gap-3 rounded-2xl border border-border bg-card p-4"
            >
              <span className="grid size-8 place-items-center rounded-xl bg-muted font-mono text-xs font-bold text-primary">
                {index + 1}
              </span>
              <span className="text-sm leading-6 text-foreground/75">{item}</span>
            </li>
          ))}
        </ol>
      </DocsSection>
    </DocsPage>
  );
}
