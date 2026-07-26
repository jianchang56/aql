import { CheckCircle2 } from "lucide-react";

import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "5 分钟上手",
  description: "安装 AQL 与 Skill 后，用自然语言完成第一条安全查询。",
};

const steps = [
  "确认 aql 已安装并可从 PATH 调用。",
  "把 AQL Skill 安装到正在使用的 Agent。",
  "告诉 AI 数据库、查询目标和结果范围。",
  "需要审计或自动化时，再切换到代码视图。",
];

export default function GettingStartedPage() {
  return (
    <DocsPage
      currentPath="/docs/getting-started"
      title="5 分钟让 AI 完成第一条查询"
      description="推荐路径是先安装 AQL 与 Skill，再直接描述你想知道什么。AQL 负责把自然语言目标落实为显式数据库、只读 SQL 和最小授权。"
    >
      <DocsSection title="开始前检查">
        <div className="grid gap-3 sm:grid-cols-2">
          {steps.map((step) => (
            <div
              key={step}
              className="flex items-start gap-3 rounded-2xl border border-border bg-card p-4"
            >
              <CheckCircle2
                className="mt-1 size-4 shrink-0 text-primary"
                aria-hidden="true"
              />
              <span className="text-sm leading-6 text-foreground/75">{step}</span>
            </div>
          ))}
        </div>
        <CodeBlock
          ai="确认 AQL 是否已经安装；如果没有，请先打开安装页，按当前发布状态和操作系统选择可用方式。安装后运行版本检查。"
          code="aql --version"
          language="bash"
          label="安装检查"
        />
      </DocsSection>

      <DocsSection title="1. 让 AI 检查可用数据库">
        <p>
          AQL 没有默认数据库。先列出已经配置的数据库，再检查四个固定内置候选位置。
        </p>
        <CodeBlock
          ai="使用 $aql 列出已经配置和可以发现的数据库。不要替我选择默认数据库，也不要递归扫描 HOME。"
          code={["aql database list", "aql database discover"].join("\n")}
          language="bash"
          label="terminal"
        />
        <DocsNote title="不要直接从 all 开始">
          只有明确需要跨 Agent 联合统计时才选择 <code>all</code>。第一次查询建议选择一个具体数据库，例如{" "}
          <code>codex</code>。
        </DocsNote>
      </DocsSection>

      <DocsSection title="2. 让 AI 先确认 schema">
        <CodeBlock
          ai="使用 $aql 选择 codex 数据库，查看可用表，并说明 sessions 表有哪些 Safe 字段。先不要申请任何敏感字段授权。"
          code={[
            "aql",
            "",
            "SHOW DATABASES;",
            "USE codex;",
            "SHOW TABLES;",
            "DESCRIBE sessions;",
          ].join("\n")}
          language="sql"
          label="aql shell"
        />
        <p>
          <code>DESCRIBE</code> 会显示字段类型和访问级别。看到 Content、Path 或工具载荷字段时，不要先授予权限；只在查询确实需要时授权。
        </p>
      </DocsSection>

      <DocsSection title="3. 直接描述查询目标">
        <CodeBlock
          ai="使用 $aql 查询 codex 最近更新的 20 个会话，只返回 session_id、模型和更新时间，并按更新时间从新到旧排序。"
          code={[
            "SELECT session_id, model, updated_at",
            "FROM sessions",
            "ORDER BY updated_at DESC",
            "LIMIT 20;",
          ].join("\n")}
          language="sql"
          label="sql"
        />
        <p>
          这是只读取 Safe 字段的有界查询。退出 Shell 后，数据库选择、授权和 history
          都不会持久化。
        </p>
      </DocsSection>

      <DocsSection title="4. 需要自动化时使用命令">
        <CodeBlock
          ai="使用 $aql 统计 codex 会话数，按模型分组并降序返回。请同时给出可复用的命令行。"
          code={[
            "aql query -d codex \\",
            "  'SELECT model, COUNT(*) AS sessions",
            "   FROM sessions",
            "   GROUP BY model",
            "   ORDER BY sessions DESC'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          自动化、脚本和 Agent 调用应使用 <code>aql query</code>，并通过{" "}
          <code>-d</code> 显式选择数据库。
        </p>
      </DocsSection>
    </DocsPage>
  );
}
