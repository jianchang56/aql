import Link from "next/link";
import {
  ArrowRight,
  Bot,
  BookOpen,
  Download,
  KeyRound,
  MessageSquareText,
} from "lucide-react";

import { DocsNote, DocsPage, DocsSection } from "@/components/docs/docs-page";
import { Badge } from "@/components/ui/badge";
import { docsSections } from "@/lib/docs";
import { publishedRelease } from "@/lib/project-status";

export const metadata = {
  title: "使用文档",
  description: "从安装 AQL、安装 Skill 到完成第一条自然语言查询的多页使用文档。",
};

const learningPath = [
  {
    icon: Download,
    title: "安装 AQL",
    description: publishedRelease
      ? "让 AI 选择安装方式，或一行安装正式 Release 预编译版本。"
      : "先从源码安装；正式 Release 发布后可切换到预编译一行安装。",
    href: "/docs/getting-started/installation",
  },
  {
    icon: Bot,
    title: "安装 Agent Skill",
    description: "让 Agent 遵循显式数据库、只读查询和最小授权流程。",
    href: "/docs/integrations/agent-skill",
  },
  {
    icon: MessageSquareText,
    title: "直接用自然语言提问",
    description: "说明数据库、目标和范围；需要时再切换到等价代码。",
    href: "/docs/getting-started",
  },
];

const concepts = [
  {
    term: "Agent",
    description: "这里指 Claude Code、Codex、Kimi Code 或 OpenCode 等在本机保存工作记录的 AI 编程工具。",
  },
  {
    term: "数据库",
    description: "你要查询的具体数据来源，例如 codex。它不是要你另外安装一台数据库服务器。",
  },
  {
    term: "Skill",
    description: "安装到 Agent 的一组使用说明，教它怎样安全调用 AQL；Skill 本身不会获得额外权限。",
  },
];

export default function DocsIndexPage() {
  return (
    <DocsPage
      currentPath="/docs"
      title="先安装，再把问题交给 AI"
      description="推荐顺序是安装 AQL、安装 Skill、直接提问。每个操作示例都可以在 AI 自然语言与代码之间切换；需要审计或自动化时再查看代码。"
    >
      <DocsSection id="beginner-path" title="推荐的新手路径">
        <div className="grid gap-4 md:grid-cols-3">
          {learningPath.map((item, index) => {
            const Icon = item.icon;
            return (
              <Link
                key={item.href}
                href={item.href}
                className="group border-t-2 border-border bg-card px-1 py-5 outline-none transition-colors hover:border-primary focus-visible:ring-2 focus-visible:ring-ring"
              >
                <div className="flex items-center justify-between">
                  <span className="grid size-9 place-items-center rounded-xl bg-muted text-primary">
                    <Icon className="size-5" aria-hidden="true" />
                  </span>
                  <span className="font-mono text-xs font-bold text-muted-foreground">
                    {index + 1}
                  </span>
                </div>
                <h3 className="mt-5 text-balance font-display text-lg font-extrabold tracking-[-0.03em] text-foreground">
                  {item.title}
                </h3>
                <p className="mt-2 text-sm leading-6">{item.description}</p>
                <span className="mt-4 inline-flex items-center gap-2 text-sm font-semibold text-primary">
                  打开
                  <ArrowRight
                    className="size-4 transition-transform group-hover:translate-x-0.5"
                    aria-hidden="true"
                  />
                </span>
              </Link>
            );
          })}
        </div>
      </DocsSection>

      <DocsSection id="core-terms" title="先认识三个词">
        <div className="grid gap-4 md:grid-cols-3">
          {concepts.map((concept) => (
            <article
              key={concept.term}
              className="rounded-xl border border-border bg-card p-5"
            >
              <h3 className="font-display text-lg font-extrabold text-foreground">
                {concept.term}
              </h3>
              <p className="mt-2 text-sm leading-6">{concept.description}</p>
            </article>
          ))}
        </div>
        <DocsNote title="关于“本地优先”" tone="amber">
          AQL 的查询在本机执行，AQL 本身不上传数据；但如果你使用的是云端 Agent，它发送给模型的提示词和工具结果仍受该产品的隐私设置与服务条款约束。
        </DocsNote>
      </DocsSection>

      <DocsSection id="browse-by-topic" title="按主题查找">
        <div className="grid gap-6 sm:grid-cols-2">
          {docsSections.slice(1).map((section) => (
            <section
              key={section.title}
              className="rounded-xl border border-border bg-card p-5"
            >
              <div className="flex items-center gap-2">
                {section.title === "日常使用" ? (
                  <BookOpen className="size-4 text-primary" aria-hidden="true" />
                ) : (
                  <KeyRound className="size-4 text-primary" aria-hidden="true" />
                )}
                <h3 className="font-display font-extrabold text-foreground">
                  {section.title}
                </h3>
              </div>
              <div className="mt-4 grid gap-2">
                {section.pages.map((page) => (
                  <Link
                    key={page.href}
                    href={page.href}
                    className="border-t border-border px-1 py-3 outline-none transition-colors hover:text-primary focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    <span className="flex items-center justify-between gap-3 font-semibold text-foreground">
                      {page.title}
                      <ArrowRight className="size-4 text-primary" aria-hidden="true" />
                    </span>
                    <span className="mt-1 block text-sm leading-6">
                      {page.description}
                    </span>
                  </Link>
                ))}
              </div>
            </section>
          ))}
        </div>
      </DocsSection>

      <DocsSection id="platform-support" title="平台支持">
        <div className="flex flex-wrap gap-2">
          <Badge variant="mint">macOS</Badge>
          <Badge variant="mint">Linux</Badge>
          <Badge variant="mint">Windows</Badge>
        </div>
        <p>
          {publishedRelease ? (
            <>
              macOS 和 Linux 提供 {publishedRelease.tag} 官方预编译包；Windows
              已支持运行与源码安装，当前通过 Cargo 构建安装。
            </>
          ) : (
            <>
              macOS、Linux 和 Windows 当前都可以通过 Cargo 从源码安装。预编译
              Release 的目标平台是 macOS 与 Linux，但首个正式资产尚未发布。
            </>
          )}
        </p>
      </DocsSection>
    </DocsPage>
  );
}
