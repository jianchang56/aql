import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "数据库",
  description: "理解内置数据库、显式 all federation 和命名数据库。",
};

const databases = [
  ["claude", "Claude Code 固定候选位置"],
  ["codex", "Codex 固定候选位置"],
  ["kimi", "Kimi Code 固定候选位置"],
  ["opencode", "OpenCode 固定候选位置"],
  ["all", "显式联合当前兼容的内置数据库"],
];

export default function DatabasesPage() {
  return (
    <DocsPage
      currentPath="/docs/guides/databases"
      title="先选择数据库，再查询"
      description="AQL 没有隐式数据库。Shell 使用 USE，非交互查询使用 -d；两者接受同一套数据库名称。"
    >
      <DocsSection title="内置数据库">
        <div className="overflow-hidden rounded-2xl border border-border bg-card">
          {databases.map(([name, description]) => (
            <div
              key={name}
              className="grid gap-1 border-b border-border px-4 py-3 last:border-0 sm:grid-cols-[8rem_1fr]"
            >
              <code className="w-fit">{name}</code>
              <span className="text-sm leading-6">{description}</span>
            </div>
          ))}
        </div>
        <DocsNote title="all 必须是明确选择">
          只有用户明确要求跨 Agent 联合查询时才使用 <code>all</code>。全部来源共享一个预算、deadline
          和取消信号。
        </DocsNote>
      </DocsSection>

      <DocsSection title="检查数据库状态">
        <CodeBlock
          ai="使用 $aql 列出数据库、检查固定候选来源，并诊断 codex 数据库是否可用。不要读取真实路径。"
          code={[
            "aql database list",
            "aql database discover",
            "aql doctor -d codex",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          <code>discover</code> 只检查四个固定候选位置，不递归扫描 HOME，不输出真实路径。
        </p>
      </DocsSection>

      <DocsSection title="在 Shell 中选择">
        <CodeBlock
          ai="使用 $aql 打开交互式 Shell，列出数据库，明确选择 codex，然后查看表和当前状态。"
          code={[
            "SHOW DATABASES;",
            "USE codex;",
            "SHOW TABLES;",
            "SHOW STATUS;",
          ].join("\n")}
          language="sql"
          label="aql shell"
        />
      </DocsSection>

      <DocsSection title="保存命名数据库">
        <p>只有确实需要保存自定义 Agent 路径时才创建命名数据库。</p>
        <CodeBlock
          ai="为这个自定义 Codex 数据目录创建名为 work 的 AQL 数据库，明确确认会持久化绝对路径；随后显示配置、执行一次计数查询，并告诉我如何删除它。"
          code={[
            "aql database add work \\",
            "  --member codex=/absolute/path/to/.codex \\",
            "  --acknowledge-persistent-path",
            "",
            "aql database show work",
            "aql query -d work 'SELECT COUNT(*) FROM sessions'",
            "aql database remove work",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          配置只保存名称、Adapter ID 和绝对路径，不保存 SQL、查询结果、授权或凭据。
        </p>
      </DocsSection>
    </DocsPage>
  );
}
