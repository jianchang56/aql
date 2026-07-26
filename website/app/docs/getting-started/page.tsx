import Link from "next/link";
import { CheckCircle2 } from "lucide-react";

import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "5 分钟上手",
  description: "安装 AQL 与 Skill 后，找到一个数据库并完成第一条只读查询。",
};

const prerequisites = [
  "AQL CLI 已安装，终端可以运行 aql。",
  "AQL Skill 已安装到你正在使用的 Agent。",
  "至少使用过一次 Claude Code、Codex、Kimi Code 或 OpenCode。",
  "不需要先学 SQL；先用自然语言完成第一条查询。",
];

const inlineLinkClass =
  "font-semibold text-primary underline-offset-4 outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring";

export default function GettingStartedPage() {
  return (
    <DocsPage
      currentPath="/docs/getting-started"
      title="5 分钟完成第一条只读查询"
      description="这条路径只做三件事：确认安装、找到一个具体的数据来源、得到一个可以核对的结果。示例使用 codex；如果你使用其他 Agent，请替换成实际发现的数据库名称。"
    >
      <DocsSection title="开始前：确认两项安装">
        <div className="grid gap-3 sm:grid-cols-2">
          {prerequisites.map((item) => (
            <div
              key={item}
              className="flex items-start gap-3 rounded-2xl border border-border bg-card p-4"
            >
              <CheckCircle2
                className="mt-1 size-4 shrink-0 text-primary"
                aria-hidden="true"
              />
              <span className="text-sm leading-6 text-foreground/75">{item}</span>
            </div>
          ))}
        </div>
        <p>
          如果还没有完成，请先看{" "}
          <Link href="/docs/getting-started/installation" className={inlineLinkClass}>
            安装 AQL
          </Link>
          ，再看{" "}
          <Link href="/docs/integrations/agent-skill" className={inlineLinkClass}>
            安装 Agent Skill
          </Link>
          。
        </p>
        <CodeBlock
          ai="确认 AQL CLI 已安装并返回版本号。只做版本检查，不修改任何配置。"
          code="aql --version"
          language="bash"
          label="安装检查"
        />
        <DocsNote title="成功标志" tone="mint">
          终端打印 AQL 版本号，并且没有出现“命令找不到”。如果失败，先回到安装页处理 PATH 或源码安装问题。
        </DocsNote>
        <DocsNote title="本地查询不等于 AI 对话一定离线" tone="amber">
          AQL 本身不上传数据；但云端 Agent 可能把你的提示词和它读取到的工具结果发送给模型。开始时先做计数和聚合，不要读取正文、路径或工具载荷，并确认所用 AI 产品的隐私设置。
        </DocsNote>
      </DocsSection>

      <DocsSection title="1. 找到一个具体数据库">
        <p>
          在 AQL 中，“数据库”只是一个数据来源名称：<code>codex</code> 代表 Codex，
          <code>claude</code> 代表 Claude Code。你不需要另外安装数据库服务器。
        </p>
        <CodeBlock
          ai="使用 $aql 列出已经配置和可以发现的数据库。告诉我哪些具体数据库可用；如果有多个，先让我选择。不要替我选择默认数据库，也不要使用 all。"
          code={["aql database list", "aql database discover"].join("\n")}
          language="bash"
          label="查找数据库"
        />
        <DocsNote title="成功时会看到什么" tone="mint">
          结果中至少出现一个具体名称，例如 <code>codex</code>、<code>claude</code>、
          <code>kimi</code> 或 <code>opencode</code>。如果出现多个，请明确选一个；
          <code>all</code> 只用于你明确要求的跨 Agent 联合查询。
        </DocsNote>
      </DocsSection>

      <DocsSection title="如果没有发现数据库">
        <p>
          这通常不代表 AQL 安装失败。对应 Agent 可能还没有生成本地记录，或者数据位于非默认目录。
        </p>
        <CodeBlock
          ai="没有发现可用数据库。请先确认我实际使用的是 Claude Code、Codex、Kimi Code 还是 OpenCode，然后只对对应数据库运行 AQL doctor。不要扫描其他目录，也不要读取真实路径。"
          code="aql doctor -d codex"
          language="bash"
          label="示例：诊断 codex"
        />
        <ol className="list-decimal space-y-2 pl-5">
          <li>确认你确实使用过对应 Agent，并至少完成过一次会话。</li>
          <li>
            再运行一次 <code>aql database discover</code>。
          </li>
          <li>
            如果你主动把 Agent 数据移到了自定义目录，再阅读
            <Link href="/docs/guides/databases" className={inlineLinkClass}>
              命名数据库
            </Link>
            ；不要为了试错随意保存路径。
          </li>
        </ol>
      </DocsSection>

      <DocsSection title="2. 完成第一条查询">
        <p>
          下面只统计会话总数，不读取会话正文。示例使用 <code>codex</code>；请替换为上一步已经确认的具体数据库。
        </p>
        <CodeBlock
          ai="使用 $aql 查询刚才确认的具体数据库；如果有多个数据库而我还没有选择，请先问我。统计 sessions 的总数，只返回一个 sessions 数字，不读取正文、路径或工具载荷。"
          code="aql query -d codex 'SELECT COUNT(*) AS sessions FROM sessions'"
          language="bash"
          label="第一条查询 · 示例使用 codex"
        />
        <DocsNote title="预期结果" tone="mint">
          你会得到一列名为 <code>sessions</code> 的结果和一个数字。数字为 0 也是有效结果，表示当前来源没有可计数的会话，不代表查询失败。
        </DocsNote>
      </DocsSection>

      <DocsSection title="3. 查看一个更有用的汇总">
        <p>
          现在按模型统计会话数量。仍然只读取普通统计字段，并按数量从高到低返回。
        </p>
        <CodeBlock
          ai="使用 $aql 查询刚才选择的具体数据库，按模型统计会话数量并降序返回 table。不要读取会话正文；同时给出等价命令，方便我核对。"
          code={[
            "aql query -d codex \\",
            "  'SELECT model, COUNT(*) AS sessions",
            "   FROM sessions",
            "   GROUP BY model",
            "   ORDER BY sessions DESC'",
          ].join("\n")}
          language="bash"
          label="按模型汇总 · 示例使用 codex"
        />
        <DocsNote title="预期结果" tone="mint">
          结果包含 <code>model</code> 和 <code>sessions</code> 两列，每行代表一个模型。到这里，你已经完成了安装验证、数据库选择和第一条实际查询。
        </DocsNote>
        <p>
          下一步可以学习{" "}
          <Link href="/docs/guides/querying" className={inlineLinkClass}>
            编写查询
          </Link>
          、
          <Link href="/docs/guides/access" className={inlineLinkClass}>
            敏感字段
          </Link>{" "}
          和
          <Link href="/docs/guides/output" className={inlineLinkClass}>
            输出结果
          </Link>
          。只有确实需要跨 Agent 比较时，才使用显式数据库 <code>all</code>。
        </p>
      </DocsSection>
    </DocsPage>
  );
}
