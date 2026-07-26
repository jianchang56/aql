import Link from "next/link";

import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "Agent Skill",
  description: "为 Claude Code、Codex、Kimi Code CLI 或 OpenCode 安装并验证 AQL Skill。",
};

const skillInstallCommand =
  "npx --yes skills add jianchang56/aql --skill aql --global --yes";

const supportedAgents = [
  ["Claude Code", "claude-code", "~/.claude/skills/aql"],
  ["Codex", "codex", "~/.codex/skills/aql"],
  ["Kimi Code CLI", "kimi-code-cli", "~/.agents/skills/aql"],
  ["OpenCode", "opencode", "~/.config/opencode/skills/aql"],
];

const inlineLinkClass =
  "font-semibold text-primary underline-offset-4 outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring";

export default function AgentSkillPage() {
  return (
    <DocsPage
      currentPath="/docs/integrations/agent-skill"
      title="安装 Skill，然后直接提问"
      description="Skill 是安装到 Agent 的使用说明：它教 AI 先确认数据来源和可用字段，再生成只读、有边界的 AQL 查询。它不会安装 AQL CLI，也不会获得额外权限。"
    >
      <DocsSection title="1. 先确认 AQL CLI">
        <CodeBlock
          ai="确认 AQL CLI 已安装并可调用，然后只做版本检查。不要修改配置，也不要替我选择数据库。"
          code="aql --version"
          language="bash"
          label="AQL CLI"
        />
        <p>
          如果命令不可用，先完成 <Link href="/docs/getting-started/installation" className={inlineLinkClass}>AQL 安装</Link>
          。Skill 和 CLI 是两件事：前者教 Agent 怎样操作，后者才真正执行查询。
        </p>
      </DocsSection>

      <DocsSection title="2. 检查 Node.js 与 npx">
        <p>
          推荐安装方式使用 <code>skills</code> CLI，它通过 <code>npx</code> 运行。当前
          <code>skills</code> CLI 要求 Node.js <code>22.20.0</code> 或更高版本；
          <code>npm</code> 与 <code>npx</code> 通常随 Node.js 一起安装。
        </p>
        <CodeBlock
          ai="检查 Node.js、npm 和 npx 是否可用，并确认 Node.js 至少为 22.20.0。只报告版本，不安装或升级任何软件。"
          code={["node --version", "npm --version", "npx --version"].join("\n")}
          language="bash"
          label="安装前检查"
        />
        <DocsNote title="没有 npx 时" tone="amber">
          可以先安装符合版本要求的 Node.js；如果不想安装 Node.js，则克隆 AQL 仓库，并把完整的
          <code>skills/aql</code> 目录复制到下表中对应 Agent 的全局目录。不要只复制
          <code>SKILL.md</code>。
        </DocsNote>
      </DocsSection>

      <DocsSection title="3. 选择 Agent 并安装">
        <p>
          下面的命令会自动检测已经安装的兼容 Agent，并把 <code>aql</code> Skill 安装到当前用户。若检测结果不符合预期，请使用表格中的
          <code>--agent</code> 值明确指定目标。
        </p>
        <CodeBlock
          ai="请从 GitHub 仓库 jianchang56/aql 安装完整的 aql Skill 到我当前使用的 Agent。先确认 Agent 类型，再使用 skills CLI 全局安装；不要只复制 SKILL.md。"
          code={skillInstallCommand}
          language="bash"
          label="自动检测已安装 Agent"
        />

        <div className="overflow-hidden rounded-2xl border border-border bg-card">
          <table className="w-full border-collapse text-left">
            <caption className="sr-only">
              AQL Skill 支持的 Agent、对应的 --agent ID 和全局安装目录
            </caption>
            <thead className="hidden border-b border-border bg-muted/45 font-mono text-[11px] font-bold uppercase tracking-[0.12em] text-muted-foreground sm:table-header-group">
              <tr>
                <th scope="col" className="w-[28%] px-4 py-3">
                  Agent
                </th>
                <th scope="col" className="w-[28%] px-4 py-3">
                  --agent
                </th>
                <th scope="col" className="px-4 py-3">
                  全局目录
                </th>
              </tr>
            </thead>
            <tbody>
              {supportedAgents.map(([name, agentId, globalPath]) => (
                <tr
                  key={agentId}
                  className="block border-b border-border last:border-0 sm:table-row"
                >
                  <td className="grid grid-cols-[5.5rem_minmax(0,1fr)] items-center gap-3 px-4 pt-4 sm:table-cell sm:py-4">
                    <span className="font-mono text-[10px] font-bold uppercase leading-none tracking-[0.12em] text-muted-foreground sm:hidden">
                      Agent
                    </span>
                    <strong className="text-foreground">{name}</strong>
                  </td>
                  <td className="grid grid-cols-[5.5rem_minmax(0,1fr)] items-center gap-3 px-4 pt-2 sm:table-cell sm:py-4">
                    <span className="font-mono text-[10px] font-bold uppercase leading-none tracking-[0.12em] text-muted-foreground sm:hidden">
                      --agent
                    </span>
                    <code className="w-fit max-w-full break-all">{agentId}</code>
                  </td>
                  <td className="grid grid-cols-[5.5rem_minmax(0,1fr)] items-center gap-3 px-4 pb-4 pt-2 sm:table-cell sm:py-4">
                    <span className="font-mono text-[10px] font-bold uppercase leading-none tracking-[0.12em] text-muted-foreground sm:hidden">
                      全局目录
                    </span>
                    <code className="w-fit max-w-full break-all">{globalPath}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <CodeBlock
          ai="把 aql Skill 明确安装到 Codex 的全局 Skill 目录，并在符号链接不可用时复制文件。"
          code="npx --yes skills add jianchang56/aql --skill aql --agent codex --global --yes --copy"
          language="bash"
          label="示例：明确安装到 Codex"
        />
      </DocsSection>

      <DocsSection title="4. 验证 Agent 已加载 Skill">
        <CodeBlock
          ai="确认当前 Agent 已安装并能识别 aql Skill，同时确认本机 aql CLI 可运行。不要执行真实数据查询。"
          code={[
            "npx --yes skills list --global --agent codex",
            "aql --version",
          ].join("\n")}
          language="bash"
          label="示例：验证 Codex"
        />
        <DocsNote title="$aql 不是终端命令" tone="blue">
          <code>$aql</code> 是提示词中用来点名 Skill 的写法；真正的终端程序叫
          <code>aql</code>。不要在 shell 中运行 <code>$aql</code>。安装后请新建一个 Agent
          会话，再输入“请使用 <code>$aql</code>”或“请使用 AQL Skill”。
        </DocsNote>
        <p>
          如果列表中有 <code>aql</code>，但 Agent 仍不识别，请完全重启或新建会话；仍然失败时查看
          <Link href="/docs/reference/troubleshooting" className={inlineLinkClass}>
            Skill 排障
          </Link>
          。
        </p>
      </DocsSection>

      <DocsSection title="5. 用自然语言查询">
        <CodeBlock
          ai="使用 $aql 查询 codex 的会话数，按模型分组并按数量降序返回 table。不要读取会话正文。"
          code="aql query -d codex 'SELECT model, COUNT(*) AS sessions FROM sessions GROUP BY model ORDER BY sessions DESC'"
          language="bash"
          label="同一个查询"
        />
        <p>一个好的请求包含具体数据库、查询目标、必要的范围和输出格式。</p>
      </DocsSection>

      <DocsSection title="Skill 会遵守什么">
        <ul className="grid gap-3">
          {[
            "不会猜测默认数据库，all 仍需用户明确要求。",
            "不会直接打开 Agent 私有 SQLite、日志、认证配置或项目树。",
            "正文、路径和工具载荷只申请任务所需的最小临时授权。",
            "明细查询先筛选并限制结果，不无界输出敏感字段。",
            "不会声称 AI 对话一定离线；云端 Agent 的提示词与工具结果仍受其产品设置约束。",
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
          Skill 只是操作指南。只读查询规则、字段授权、资源限制和完整结果发布仍由 AQL
          本身强制执行。
        </DocsNote>
      </DocsSection>
    </DocsPage>
  );
}
