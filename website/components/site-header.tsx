import Link from "next/link";

import { GitHubIcon } from "@/components/github-icon";
import { ThemeToggle } from "@/components/theme-toggle";
import { buttonVariants } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export function LogoMark() {
  return (
    <span
      className="relative grid size-8 place-items-center overflow-hidden rounded-lg border border-foreground/10 bg-foreground font-display text-xs font-extrabold text-background sm:size-9 sm:rounded-xl sm:text-sm"
      aria-hidden="true"
    >
      A
      <span className="absolute -right-1 top-1/2 h-px w-6 -rotate-45 bg-mint" />
    </span>
  );
}

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-40 border-b border-border/70 bg-background/88 backdrop-blur-xl">
      <div className="mx-auto flex h-14 w-full max-w-7xl items-center justify-between px-4 sm:h-16 sm:px-8">
        <Link
          href="/"
          className="flex min-h-11 items-center gap-2.5 rounded-xl outline-none focus-visible:ring-2 focus-visible:ring-ring sm:gap-3"
          aria-label="AQL 首页"
        >
          <LogoMark />
          <span className="font-display text-lg font-extrabold tracking-[-0.04em]">
            AQL
          </span>
        </Link>

        <nav className="flex items-center gap-1" aria-label="主导航">
          <Link
            href="/docs/getting-started/installation"
            className="inline-flex min-h-11 shrink-0 items-center whitespace-nowrap rounded-full px-2 py-2 text-sm font-semibold text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:px-3"
          >
            安装
          </Link>
          <Link
            href="/docs/integrations/agent-skill"
            className="inline-flex min-h-11 shrink-0 items-center whitespace-nowrap rounded-full px-2 py-2 text-sm font-semibold text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:px-3"
          >
            Skill
          </Link>
          <Link
            href="/#why-aql"
            className="hidden min-h-11 items-center rounded-full px-3 py-2 text-sm font-semibold text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring md:inline-flex"
          >
            为什么 AQL
          </Link>
          <Link
            href="/docs"
            className="hidden min-h-11 shrink-0 items-center whitespace-nowrap rounded-full px-3 py-2 text-sm font-semibold text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:inline-flex sm:px-4"
          >
            使用文档
          </Link>
          <ThemeToggle />
          <a
            href="https://github.com/jianchang56/aql"
            target="_blank"
            rel="noreferrer"
            className={cn(
              buttonVariants({ variant: "outline", size: "sm" }),
              "ml-0.5 size-11 px-0 sm:ml-1 sm:w-auto sm:px-4",
            )}
            aria-label="在 GitHub 查看 AQL 源码"
          >
            <GitHubIcon aria-hidden="true" />
            <span className="hidden sm:inline">GitHub</span>
          </a>
        </nav>
      </div>
    </header>
  );
}
