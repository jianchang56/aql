import Link from "next/link";
import {
  ArrowRight,
  Bot,
  Check,
  Database,
  Download,
  FileOutput,
  KeyRound,
  MessageSquareText,
  ShieldCheck,
  Sparkles,
} from "lucide-react";

import { CodeBlock } from "@/components/docs/code-block";
import { GitHubIcon } from "@/components/github-icon";
import { QueryTabs } from "@/components/query-tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { publishedRelease } from "@/lib/project-status";

const builtInAgents = ["Claude Code", "Codex", "Kimi Code", "OpenCode"];

const guarantees = [
  {
    icon: Database,
    title: "统一的公开表",
    description: "Agent 私有格式由 Adapter 处理；AI 始终面对同一套 canonical tables。",
  },
  {
    icon: KeyRound,
    title: "授权先于读取",
    description: "正文、路径和工具载荷默认不可见，只在任务确实需要时申请最小临时授权。",
  },
  {
    icon: ShieldCheck,
    title: "数据库必须显式选择",
    description: "Skill 不会猜测默认来源；只有用户明确要求联合查询时才使用 all。",
  },
  {
    icon: FileOutput,
    title: "完整结果再发布",
    description: "任一来源失败都不会输出部分结果，文件采用 no-replace 原子发布。",
  },
];

const safetyRules = [
  "只接受一条只读 SELECT、CTE 或 EXPLAIN SELECT",
  "不递归扫描 HOME，也不调用任何 Agent 程序",
  "Secret 字段永远不可授权或读取",
  "跨源查询共享预算、deadline 与取消信号",
  "不在 Agent 数据旁创建缓存或 sidecar",
  "不提供 mutation、覆盖写入或部分成功输出",
];

const releaseInstall = publishedRelease
  ? 'tmp="$(mktemp -d)" && curl -fsSL https://github.com/jianchang56/aql/releases/latest/download/aql.rb -o "$tmp/aql.rb" && brew install --formula "$tmp/aql.rb" && aql --version'
  : [
      "git clone https://github.com/jianchang56/aql.git",
      "cd aql",
      "rustup toolchain install 1.97.0",
      "cargo +1.97.0 install --locked --path crates/aql",
      "aql --version",
    ].join("\n");

const skillInstallCommand =
  "npx --yes skills add jianchang56/aql --skill aql --global --yes";

const heroQuery = [
  "aql query -d all \\",
  "  'SELECT agent_id, COUNT(*) AS sessions",
  "   FROM sessions",
  "   GROUP BY agent_id",
  "   ORDER BY sessions DESC'",
].join("\n");

export default function HomePage() {
  return (
    <>
      <section className="relative isolate overflow-hidden border-b border-border">
        <div className="site-grid absolute inset-0 -z-10" aria-hidden="true" />
        <div className="mx-auto grid w-full max-w-7xl grid-cols-[minmax(0,1fr)] gap-12 px-5 pb-16 pt-14 sm:px-8 sm:pb-20 sm:pt-20 lg:grid-cols-[minmax(0,0.92fr)_minmax(0,1.08fr)] lg:items-center lg:gap-20 lg:pb-24 lg:pt-24">
          <div className="min-w-0">
            <Badge variant="mint">
              <Sparkles className="size-3.5" aria-hidden="true" />
              推荐路径：安装 → Skill → 提问
            </Badge>
            <h1 className="mt-6 text-balance font-display text-[clamp(2.75rem,6vw,5rem)] font-extrabold leading-[0.98] tracking-[-0.06em]">
              <span className="block sm:whitespace-nowrap">直接问 AI，</span>
              <span className="block text-primary">安全查询本地 Agent 数据。</span>
            </h1>
            <p className="mt-6 max-w-xl text-lg leading-8 text-muted-foreground sm:text-xl">
              AQL 推荐由 AI 使用。安装 CLI 与 Skill 后，只需描述问题；AQL 会把目标落实为显式数据库、只读 SQL 和最小授权。
            </p>

            <div className="mt-8 flex flex-col gap-3 sm:flex-row">
              <Button asChild size="lg">
                <Link href="/docs/getting-started/installation">
                  安装 AQL
                  <ArrowRight aria-hidden="true" />
                </Link>
              </Button>
              <Button asChild size="lg" variant="outline">
                <Link href="/docs/integrations/agent-skill">
                  <Bot aria-hidden="true" />
                  安装 Skill
                </Link>
              </Button>
            </div>

            <div className="mt-9 flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
              <span className="font-mono text-xs font-semibold text-foreground/75">
                natural language
              </span>
              <ArrowRight className="size-3.5 text-primary/60" aria-hidden="true" />
              <span className="font-mono text-xs font-semibold text-foreground/75">
                AQL Skill
              </span>
              <ArrowRight className="size-3.5 text-primary/60" aria-hidden="true" />
              <span className="font-mono text-xs font-semibold text-primary">
                read-only SQL
              </span>
            </div>
          </div>

          <div className="relative mx-auto min-w-0 w-full max-w-2xl lg:mx-0">
            <div
              className="absolute -inset-8 -z-10 rounded-[3rem] bg-primary/10 blur-3xl"
              aria-hidden="true"
            />
            <div className="code-panel overflow-hidden rounded-[1.75rem] border border-border text-card-foreground">
              <div className="flex items-center justify-between border-b border-border px-5 py-4 sm:px-6">
                <div className="flex items-center gap-2">
                  <span className="size-2 rounded-full bg-[#ff7868]" />
                  <span className="size-2 rounded-full bg-[#ffd45c]" />
                  <span className="size-2 rounded-full bg-[#76f3d7]" />
                </div>
                <span className="font-mono text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                  Ask AQL
                </span>
              </div>

              <CodeBlock
                ai="使用 $aql 联合查询所有可用 Agent 的会话数量，按 Agent 分组并降序返回。只返回聚合结果。"
                code={heroQuery}
                language="bash"
                label="同一个问题，两种方式"
                className="rounded-none border-0 border-b border-border bg-transparent"
              />

              <div className="px-5 py-4 sm:px-6 sm:py-5">
                <table className="w-full border-collapse font-mono text-sm tabular-nums">
                  <caption className="sr-only">示例查询结果</caption>
                  <thead>
                    <tr className="border-b border-border font-mono text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
                      <th scope="col" className="pb-2 text-left font-medium">
                        agent_id
                      </th>
                      <th scope="col" className="pb-2 text-right font-medium">
                        sessions
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      ["claude", "42"],
                      ["codex", "37"],
                      ["kimi", "18"],
                      ["opencode", "11"],
                    ].map(([agent, sessions]) => (
                      <tr key={agent} className="border-b border-border last:border-0">
                        <td className="py-2 text-foreground/75">{agent}</td>
                        <td className="py-2 text-right text-primary">{sessions}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="flex items-center justify-between border-t border-border bg-muted/30 px-5 py-3 font-mono text-[11px] uppercase tracking-[0.12em] sm:px-6">
                <span className="text-muted-foreground">query contract</span>
                <span className="inline-flex items-center gap-2 text-mint-foreground">
                  <span className="size-1.5 rounded-full bg-mint-foreground" />
                  read-only / complete
                </span>
              </div>
            </div>
          </div>
        </div>

        <div className="border-t border-border bg-card/65">
          <div className="mx-auto flex w-full max-w-7xl flex-col gap-3 px-5 py-4 sm:px-8 lg:flex-row lg:items-center lg:justify-between">
            <p className="font-mono text-[11px] font-bold uppercase tracking-[0.14em] text-muted-foreground">
              当前内置 Adapter
            </p>
            <div className="flex flex-wrap items-center gap-x-7 gap-y-2">
              {builtInAgents.map((agent) => (
                <span
                  key={agent}
                  className="font-display text-sm font-bold tracking-[-0.02em] text-foreground/70"
                >
                  {agent}
                </span>
              ))}
              <span className="font-mono text-[11px] font-bold tracking-[0.08em] text-primary">
                更多来源持续接入
              </span>
            </div>
          </div>
        </div>
      </section>

      <section id="install" className="scroll-mt-24 py-20 sm:py-24">
        <div className="mx-auto w-full max-w-7xl px-5 sm:px-8">
          <div className="grid gap-6 border-b border-border pb-9 lg:grid-cols-[0.78fr_1.22fr] lg:items-end">
            <div>
              <Badge variant="outline">Recommended setup</Badge>
              <h2 className="mt-4 text-balance font-display text-3xl font-extrabold tracking-[-0.05em] sm:text-5xl">
                先安装软件，再安装 Skill。
              </h2>
            </div>
            <p className="max-w-2xl text-lg leading-8 text-muted-foreground lg:justify-self-end">
              {publishedRelease
                ? "macOS 与 Linux 可从正式 Release 安装预编译版本；之后把 AQL Skill 交给 Agent。完成这两步，日常使用只需要自然语言。"
                : "当前先使用 Cargo 从源码安装；首个正式 Release 发布后，macOS 与 Linux 可改用预编译版本。之后安装 AQL Skill，日常使用只需要自然语言。"}
            </p>
          </div>

          <div className="mt-10 grid gap-6 lg:grid-cols-2">
            <article className="min-w-0 rounded-[1.75rem] border border-border bg-card p-5 sm:p-7">
              <div className="flex items-start justify-between gap-4">
                <span className="grid size-11 place-items-center rounded-2xl bg-primary/10 text-primary">
                  <Download className="size-5" aria-hidden="true" />
                </span>
                <span className="font-mono text-xs font-bold text-muted-foreground">01</span>
              </div>
              <h3 className="mt-5 font-display text-2xl font-extrabold tracking-[-0.04em]">
                安装 AQL CLI
              </h3>
              <p className="mt-2 leading-7 text-muted-foreground">
                {publishedRelease
                  ? "正式 Release 提供 macOS / Linux 预编译包；Homebrew 一行安装并校验固定 SHA256。Windows 当前使用 Cargo 安装。"
                  : "当前尚无正式预编译 Release，macOS、Linux 与 Windows 均使用 Rust 1.97.0 和 Cargo 从源码安装。"}
              </p>
              <CodeBlock
                ai={
                  publishedRelease
                    ? `请安装 AQL ${publishedRelease.tag}。macOS 或 Linux 使用正式 Release 的预编译包并验证 SHA256；Windows 使用 Rust 1.97.0 从源码安装。不要使用 sudo 或修改 shell 配置，最后运行 aql --version。`
                    : "当前还没有正式 AQL Release。请使用 Rust 1.97.0 和 locked 依赖从 GitHub 源码安装，不要使用 sudo 或修改 shell 配置，最后运行 aql --version。"
                }
                code={releaseInstall}
                language="bash"
                label={publishedRelease ? `安装 · ${publishedRelease.tag}` : "当前安装方式"}
                className="mt-6"
              />
              <Link
                href="/docs/getting-started/installation"
                className="mt-5 inline-flex min-h-11 items-center gap-2 rounded-lg font-semibold text-primary outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring"
              >
                查看所有安装方式
                <ArrowRight className="size-4" aria-hidden="true" />
              </Link>
            </article>

            <article id="skill" className="scroll-mt-24 min-w-0 rounded-[1.75rem] border border-border bg-card p-5 sm:p-7">
              <div className="flex items-start justify-between gap-4">
                <span className="grid size-11 place-items-center rounded-2xl bg-mint/60 text-mint-foreground">
                  <Bot className="size-5" aria-hidden="true" />
                </span>
                <span className="font-mono text-xs font-bold text-muted-foreground">02</span>
              </div>
              <h3 className="mt-5 font-display text-2xl font-extrabold tracking-[-0.04em]">
                安装 AQL Skill
              </h3>
              <p className="mt-2 leading-7 text-muted-foreground">
                Skill 教 Agent 先检查数据库与 schema，再生成有边界的查询，并只申请任务需要的最小临时授权。无需先克隆仓库，直接从 GitHub 安装。
              </p>
              <CodeBlock
                ai="请从 GitHub 仓库 jianchang56/aql 安装完整的 aql Skill。优先使用 skills CLI 全局安装到当前 Agent；安装后确认你能识别 $aql。"
                code={skillInstallCommand}
                language="bash"
                label="Skill"
                className="mt-6"
              />
              <Link
                href="/docs/integrations/agent-skill"
                className="mt-5 inline-flex min-h-11 items-center gap-2 rounded-lg font-semibold text-primary outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring"
              >
                查看 Skill 使用方式
                <ArrowRight className="size-4" aria-hidden="true" />
              </Link>
            </article>
          </div>

          <div className="mt-6 grid gap-6 rounded-[1.75rem] border border-border bg-muted/25 p-5 sm:p-7 lg:grid-cols-[0.72fr_1.28fr] lg:items-center">
            <div>
              <span className="grid size-11 place-items-center rounded-2xl bg-muted text-primary">
                <MessageSquareText className="size-5" aria-hidden="true" />
              </span>
              <p className="mt-5 font-mono text-xs font-bold text-muted-foreground">03</p>
              <h3 className="mt-2 font-display text-2xl font-extrabold tracking-[-0.04em]">
                现在，直接提问
              </h3>
              <p className="mt-2 max-w-xl leading-7 text-muted-foreground">
                说明数据库、目标、范围和输出格式。AI 是推荐入口，代码视图保留完整的可审计路径。
              </p>
            </div>
            <CodeBlock
              ai="使用 $aql 查询 codex 最近 30 天的会话数，按模型分组并降序返回 table。"
              code="aql query -d codex 'SELECT model, COUNT(*) AS sessions FROM sessions GROUP BY model ORDER BY sessions DESC'"
              language="bash"
              label="第一次查询"
            />
          </div>
        </div>
      </section>

      <section id="why-aql" className="scroll-mt-24 border-y border-border bg-card/60 py-20 sm:py-24">
        <div className="mx-auto w-full max-w-7xl px-5 sm:px-8">
          <div className="max-w-3xl">
            <Badge>Natural language → Canonical SQL</Badge>
            <h2 className="mt-4 text-balance font-display text-3xl font-extrabold tracking-[-0.05em] sm:text-5xl">
              自然语言在前，
              <span className="text-primary">SQL 始终可检查。</span>
            </h2>
            <p className="mt-5 text-lg leading-8 text-muted-foreground">
              你可以让 AI 完成日常查询，也可以随时切到代码视图审计 SQL、复用命令或接入自动化。
            </p>
          </div>

          <div className="mt-12 grid grid-cols-[minmax(0,1fr)] gap-12 lg:grid-cols-[minmax(0,0.82fr)_minmax(0,1.18fr)] lg:items-start lg:gap-16">
            <div className="divide-y divide-border border-y border-border">
              {guarantees.map((item) => {
                const Icon = item.icon;
                return (
                  <article key={item.title} className="grid grid-cols-[2.5rem_1fr] gap-4 py-5">
                    <span className="grid size-10 place-items-center rounded-xl bg-muted text-primary">
                      <Icon className="size-5" aria-hidden="true" />
                    </span>
                    <div>
                      <h3 className="font-display text-lg font-extrabold tracking-[-0.03em]">
                        {item.title}
                      </h3>
                      <p className="mt-1.5 leading-7 text-muted-foreground">
                        {item.description}
                      </p>
                    </div>
                  </article>
                );
              })}
            </div>

            <div className="min-w-0">
              <div className="mb-5 flex items-end justify-between gap-4">
                <div>
                  <p className="font-mono text-[11px] font-bold uppercase tracking-[0.14em] text-muted-foreground">
                    Common questions
                  </p>
                  <h3 className="mt-2 font-display text-2xl font-extrabold tracking-[-0.04em]">
                    先说需求，需要时再看代码
                  </h3>
                </div>
                <Link
                  href="/docs/guides/querying"
                  className="hidden min-h-11 items-center gap-1.5 rounded-lg text-sm font-semibold text-primary outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring sm:inline-flex"
                >
                  查询指南
                  <ArrowRight className="size-4" aria-hidden="true" />
                </Link>
              </div>
              <QueryTabs />
            </div>
          </div>
        </div>
      </section>

      <section className="bg-terminal py-20 text-terminal-foreground sm:py-24">
        <div className="mx-auto w-full max-w-7xl px-5 sm:px-8">
          <div className="grid gap-8 border-b border-white/10 pb-10 lg:grid-cols-[0.78fr_1.22fr] lg:items-end">
            <div>
              <Badge className="border-white/10 bg-white/5 text-mint-foreground">
                Safety is the interface
              </Badge>
              <h2 className="mt-5 text-balance font-display text-3xl font-extrabold tracking-[-0.05em] sm:text-5xl">
                AI 负责理解意图，AQL 负责守住边界。
              </h2>
            </div>
            <p className="max-w-2xl text-lg leading-8 text-white/58 lg:justify-self-end">
              Skill 不会扩大权限。数据库选择、SQL firewall、字段授权、共享预算和完整结果发布仍由 AQL 强制执行。
            </p>
          </div>

          <div className="mt-3 grid sm:grid-cols-2 lg:grid-cols-3">
            {safetyRules.map((rule) => (
              <div
                key={rule}
                className="flex items-start gap-3 border-b border-white/10 py-5 sm:px-5 sm:odd:border-r lg:border-r lg:px-6 lg:nth-[3n]:border-r-0"
              >
                <span className="mt-0.5 grid size-5 shrink-0 place-items-center rounded-full bg-mint-foreground/10 text-mint-foreground">
                  <Check className="size-3.5" aria-hidden="true" />
                </span>
                <span className="leading-7 text-white/72">{rule}</span>
              </div>
            ))}
          </div>

          <div className="mt-12 flex flex-col gap-6 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <p className="font-mono text-[11px] font-bold uppercase tracking-[0.14em] text-mint-foreground">
                install → skill → ask
              </p>
              <h2 className="mt-2 font-display text-2xl font-extrabold tracking-[-0.04em] sm:text-3xl">
                安装完成后，下一步就是提问。
              </h2>
            </div>
            <div className="flex flex-col gap-3 sm:flex-row">
              <Button asChild size="lg" className="w-full sm:w-auto">
                <Link href="/docs/getting-started/installation">
                  开始安装
                  <ArrowRight aria-hidden="true" />
                </Link>
              </Button>
              <Button
                asChild
                size="lg"
                variant="outline"
                className="w-full border-white/15 bg-white/5 text-white hover:bg-white/10 hover:text-white sm:w-auto"
              >
                <a
                  href="https://github.com/jianchang56/aql"
                  target="_blank"
                  rel="noreferrer"
                >
                  <GitHubIcon aria-hidden="true" />
                  GitHub
                </a>
              </Button>
            </div>
          </div>
        </div>
      </section>
    </>
  );
}
