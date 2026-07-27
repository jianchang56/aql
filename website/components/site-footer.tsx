import Link from "next/link";

import { GitHubIcon } from "@/components/github-icon";
import { LogoMark } from "@/components/site-header";

export function SiteFooter() {
  return (
    <footer className="border-t border-border bg-background">
      <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 py-8 sm:flex-row sm:items-center sm:justify-between sm:px-8">
        <div className="flex items-center gap-3">
          <LogoMark />
          <div>
            <p className="font-display font-extrabold tracking-[-0.03em]">AQL</p>
            <p className="text-xs text-muted-foreground sm:text-sm">
              Local-first · Read-only · Explicit by default
            </p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-sm font-semibold text-muted-foreground">
          <Link
            className="inline-flex min-h-11 items-center rounded-lg outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
            href="/docs/getting-started/installation"
          >
            安装
          </Link>
          <Link
            className="inline-flex min-h-11 items-center rounded-lg outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
            href="/docs/integrations/agent-skill"
          >
            Skill
          </Link>
          <Link
            className="inline-flex min-h-11 items-center rounded-lg outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
            href="/docs"
          >
            文档
          </Link>
          <a
            className="inline-flex min-h-11 items-center gap-1.5 rounded-lg outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
            href="https://github.com/jianchang56/aql"
            target="_blank"
            rel="noreferrer"
          >
            <GitHubIcon className="size-4" aria-hidden="true" />
            源码
          </a>
          <span>MIT License</span>
        </div>
      </div>
    </footer>
  );
}
