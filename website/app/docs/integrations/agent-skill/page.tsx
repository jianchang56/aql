import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "Agent Skill",
  description: "安装 AQL Skill，把自然语言作为推荐使用方式。",
};

const skillInstallCommand =
  "npx --yes skills add jianchang56/aql --skill aql --global --yes";

export default function AgentSkillPage() {
  return (
    <DocsPage
      currentPath="/docs/integrations/agent-skill"
      title="安装 Skill，然后直接提问"
      description="AQL 推荐由 AI 使用。Skill 会把显式数据库、schema 检查、有界查询和最小授权写进 Agent 的操作流程。"
    >
      <DocsSection title="1. 安装 AQL CLI">
        <CodeBlock
          ai="确认 AQL CLI 已安装并可调用，然后列出已经配置的数据库。不要替我选择默认数据库。"
          code={["aql --version", "aql database list"].join("\n")}
          language="bash"
          label="terminal"
        />
      </DocsSection>

      <DocsSection title="2. 安装 Skill">
        <CodeBlock
          ai="请从 GitHub 仓库 jianchang56/aql 安装完整的 aql Skill。优先使用 skills CLI 全局安装到当前 Agent；不要只复制 SKILL.md，安装后确认你能识别 $aql。"
          code={skillInstallCommand}
          language="bash"
          label="所有支持的 Agent"
        />
        <p>
          `skills` CLI 会把完整目录安装到当前用户的 Agent Skill 目录。若环境没有
          `npx`，可以先克隆 AQL 仓库，再复制整个 <code>skills/aql</code> 目录；不要只复制单个说明文件。
        </p>
      </DocsSection>

      <DocsSection title="3. 用自然语言查询">
        <CodeBlock
          ai="使用 $aql 查询 codex 最近 30 天的会话数，按模型分组并按数量降序返回 table。"
          code="aql query -d codex 'SELECT model, COUNT(*) AS sessions FROM sessions GROUP BY model ORDER BY sessions DESC'"
          language="bash"
          label="同一个查询"
        />
        <p>一个好的请求包含数据库、时间或数量范围、聚合目标和输出格式。</p>
      </DocsSection>

      <DocsSection title="Skill 会遵守什么">
        <ul className="grid gap-3">
          {[
            "不会猜测默认数据库，all 仍需明确要求。",
            "不会直接打开 Agent 私有 SQLite、日志、认证配置或项目树。",
            "正文、路径和工具载荷只申请任务所需的最小临时授权。",
            "明细查询先筛选并限制结果，不无界输出敏感字段。",
            "除非用户明确要求，不持久化 SQL、结果或授权。",
          ].map((item) => (
            <li
              key={item}
              className="rounded-2xl border border-border bg-card px-4 py-3 text-sm leading-6 text-foreground/75"
            >
              {item}
            </li>
          ))}
        </ul>
        <DocsNote title="Skill 不会扩大 AQL 权限" tone="mint">
          Skill 只是操作指南。SQL firewall、字段授权、共享预算和原子结果发布仍由 AQL
          本身强制执行。
        </DocsNote>
      </DocsSection>
    </DocsPage>
  );
}
