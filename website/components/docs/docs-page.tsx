import type { ReactNode } from "react";
import Link from "next/link";
import { ArrowLeft, ArrowRight } from "lucide-react";

import { getDocsNeighbors, getDocsPage } from "@/lib/docs";

export function DocsPage({
  currentPath,
  title,
  description,
  children,
}: {
  currentPath: string;
  title: string;
  description: string;
  children: ReactNode;
}) {
  const page = getDocsPage(currentPath);
  const { previous, next } = getDocsNeighbors(currentPath);

  return (
    <article className="doc-prose min-w-0">
      <header className="border-b border-border pb-9">
        <div className="flex flex-wrap items-center gap-2 font-mono text-[11px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
          <Link className="font-semibold hover:text-foreground" href="/docs">
            文档
          </Link>
          <span aria-hidden="true">/</span>
          <span>{page?.section}</span>
        </div>
        <h1 className="mt-5 text-balance font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] sm:text-5xl">
          {title}
        </h1>
        <p className="mt-4 max-w-3xl text-lg leading-8 text-muted-foreground">
          {description}
        </p>
      </header>

      <div className="mt-9 space-y-12">{children}</div>

      <nav
        className="mt-16 grid gap-3 border-t border-border pt-8 sm:grid-cols-2"
        aria-label="文档分页"
      >
        {previous ? (
          <Link
            href={previous.href}
            className="rounded-xl border border-border bg-card p-5 outline-none transition-colors hover:border-foreground/20 hover:bg-muted/30 focus-visible:ring-2 focus-visible:ring-ring"
          >
            <span className="flex items-center gap-2 text-sm font-semibold text-muted-foreground">
              <ArrowLeft className="size-4" aria-hidden="true" />
              上一篇
            </span>
            <strong className="mt-2 block font-display text-lg tracking-[-0.025em]">
              {previous.title}
            </strong>
          </Link>
        ) : (
          <span />
        )}
        {next ? (
          <Link
            href={next.href}
            className="rounded-xl border border-border bg-card p-5 text-right outline-none transition-colors hover:border-foreground/20 hover:bg-muted/30 focus-visible:ring-2 focus-visible:ring-ring"
          >
            <span className="flex items-center justify-end gap-2 text-sm font-semibold text-muted-foreground">
              下一篇
              <ArrowRight className="size-4" aria-hidden="true" />
            </span>
            <strong className="mt-2 block font-display text-lg tracking-[-0.025em]">
              {next.title}
            </strong>
          </Link>
        ) : null}
      </nav>
    </article>
  );
}

export function DocsSection({
  id,
  title,
  children,
}: {
  id?: string;
  title: string;
  children: ReactNode;
}) {
  return (
    <section id={id} className="scroll-mt-24">
      <h2 className="text-balance font-display text-2xl font-extrabold tracking-[-0.04em] sm:text-3xl">
        {title}
      </h2>
      <div className="mt-4 space-y-5 text-[17px] leading-8 text-muted-foreground">
        {children}
      </div>
    </section>
  );
}

export function DocsNote({
  title,
  children,
  tone = "blue",
}: {
  title: string;
  children: ReactNode;
  tone?: "blue" | "mint" | "amber";
}) {
  const styles = {
    blue: "border-primary/20 bg-primary/8",
    mint: "border-mint-foreground/20 bg-mint/45",
    amber: "border-warning/25 bg-warning-surface",
  };

  return (
    <aside className={"rounded-xl border p-5 " + styles[tone]}>
      <h3 className="font-display font-extrabold text-foreground">{title}</h3>
      <div className="mt-2 text-[15px] leading-7 text-foreground/70">{children}</div>
    </aside>
  );
}
