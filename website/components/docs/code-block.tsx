import { createHighlighter } from "shiki";
import { Bot, Code2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

export type CodeLanguage = "bash" | "powershell" | "sql" | "json";

const highlighterPromise = createHighlighter({
  themes: ["github-light", "github-dark"],
  langs: ["bash", "powershell", "sql", "json"],
});

async function renderCode(code: string, language: CodeLanguage) {
  const highlighter = await highlighterPromise;
  return highlighter.codeToHtml(code, {
    lang: language,
    themes: {
      light: "github-light",
      dark: "github-dark",
    },
  });
}

export async function HighlightedCode({
  code,
  language,
  className,
  appearance = "adaptive",
}: {
  code: string;
  language: CodeLanguage;
  className?: string;
  appearance?: "adaptive" | "dark";
}) {
  const html = await renderCode(code, language);

  return (
    <div
      className={cn(
        "syntax-code min-w-0",
        appearance === "dark" ? "syntax-code-dark" : undefined,
        className,
      )}
      translate="no"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}

export async function CodeBlock({
  ai,
  code,
  language,
  label,
  className,
}: {
  ai: string;
  code: string;
  language: CodeLanguage;
  label?: string;
  className?: string;
}) {
  return (
    <Tabs
      defaultValue="ai"
      className={cn(
        "min-w-0 gap-0 overflow-hidden rounded-2xl border border-border bg-card",
        className,
      )}
    >
      <div className="flex flex-col gap-2 border-b border-border bg-muted/45 px-3 py-2 sm:flex-row sm:items-center sm:justify-between sm:px-4">
        <span className="px-1 font-mono text-[11px] font-bold uppercase tracking-[0.14em] text-muted-foreground">
          {label ?? "example"}
        </span>
        <TabsList
          className="w-full flex-nowrap border-0 bg-background/70 p-1 sm:w-auto"
          aria-label={`${label ?? "示例"} · 查看方式`}
        >
          <TabsTrigger value="ai" className="gap-2 sm:min-w-24">
            <Bot className="size-4" aria-hidden="true" />
            AI
          </TabsTrigger>
          <TabsTrigger value="code" className="gap-2 sm:min-w-24">
            <Code2 className="size-4" aria-hidden="true" />
            代码
          </TabsTrigger>
        </TabsList>
      </div>
      <TabsContent value="ai" className="m-0">
        <div className="flex min-h-32 items-start gap-4 bg-primary/[0.035] p-5 sm:p-6">
          <span className="grid size-9 shrink-0 place-items-center rounded-xl bg-primary/10 text-primary">
            <Bot className="size-4.5" aria-hidden="true" />
          </span>
          <div className="min-w-0">
            <p className="font-mono text-[11px] font-bold uppercase tracking-[0.13em] text-primary">
              直接告诉 AI
            </p>
            <p className="mt-2 whitespace-pre-wrap text-pretty text-[16px] leading-7 text-foreground/80">
              {ai}
            </p>
          </div>
        </div>
      </TabsContent>
      <TabsContent value="code" className="m-0">
        <HighlightedCode code={code} language={language} />
      </TabsContent>
    </Tabs>
  );
}
