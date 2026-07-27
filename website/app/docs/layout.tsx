import type { ReactNode } from "react";

import { DocsNav } from "@/components/docs/docs-nav";

export default function DocsLayout({ children }: { children: ReactNode }) {
  return (
    <div className="mx-auto grid w-full max-w-7xl grid-cols-[minmax(0,1fr)] gap-8 px-5 py-8 sm:px-8 lg:grid-cols-[13rem_minmax(0,1fr)] lg:gap-16 lg:py-14">
      <DocsNav />
      <div className="min-w-0 max-w-4xl">{children}</div>
    </div>
  );
}
