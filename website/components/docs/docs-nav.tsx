"use client";

import Link from "next/link";
import { BookOpen, ChevronDown } from "lucide-react";
import { usePathname } from "next/navigation";
import { useEffect, useRef } from "react";

import { docsSections, normalizeDocsPath } from "@/lib/docs";
import { cn } from "@/lib/utils";

function NavGroups({ pathname }: { pathname: string }) {
  return (
    <div className="space-y-7">
      {docsSections.map((section) => (
        <div key={section.title}>
          <p className="mb-2 px-2 font-mono text-[11px] font-bold uppercase tracking-[0.14em] text-muted-foreground">
            {section.title}
          </p>
          <div className="grid border-l border-border">
            {section.pages.map((page) => {
              const active = normalizeDocsPath(pathname) === page.href;
              return (
                <Link
                  key={page.href}
                  href={page.href}
                  className={cn(
                    "-ml-px inline-flex min-h-11 items-center border-l px-3 py-2 text-sm font-semibold outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring",
                    active
                      ? "border-primary bg-primary/[0.06] text-primary"
                      : "border-transparent text-muted-foreground hover:border-foreground/25 hover:text-foreground",
                  )}
                  aria-current={active ? "page" : undefined}
                >
                  {page.title}
                </Link>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

export function DocsNav() {
  const pathname = usePathname();
  const mobileNavRef = useRef<HTMLDetailsElement>(null);

  useEffect(() => {
    mobileNavRef.current?.removeAttribute("open");
  }, [pathname]);

  return (
    <>
      <details
        ref={mobileNavRef}
        className="group rounded-xl border border-border bg-card p-3 lg:hidden"
      >
        <summary className="flex min-h-11 cursor-pointer list-none items-center justify-between rounded-lg px-1 font-display font-extrabold outline-none focus-visible:ring-2 focus-visible:ring-ring">
          <span className="flex items-center gap-2">
            <BookOpen className="size-4 text-primary" aria-hidden="true" />
            文档目录
          </span>
          <ChevronDown
            className="size-4 text-muted-foreground transition-transform group-open:rotate-180"
            aria-hidden="true"
          />
        </summary>
        <div className="mt-5 border-t border-border pt-5">
          <NavGroups pathname={pathname} />
        </div>
      </details>

      <aside className="sticky top-24 hidden h-[calc(100vh-7rem)] overflow-y-auto pr-4 lg:block">
        <NavGroups pathname={pathname} />
      </aside>
    </>
  );
}
