"use client";

import * as React from "react";
import { Bot, Check, Code2, Copy } from "lucide-react";

import type { CodeLanguage } from "@/components/docs/code-types";
import { useExampleView, type ExampleView } from "@/components/example-view-provider";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";

type CopyState = {
  view: ExampleView;
  status: "copied" | "error";
} | null;

async function copyText(text: string) {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return;
    } catch {
      // Fall back for browsers that expose the API but deny clipboard access.
    }
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  let copied = false;

  try {
    textarea.select();
    copied = document.execCommand("copy");
  } finally {
    textarea.remove();
  }

  if (!copied) {
    throw new Error("Copy command was rejected");
  }
}

function CopyButton({
  activeView,
  copyState,
  onCopy,
}: {
  activeView: ExampleView;
  copyState: CopyState;
  onCopy: () => void;
}) {
  const isCopied =
    copyState?.view === activeView && copyState.status === "copied";
  const hasError =
    copyState?.view === activeView && copyState.status === "error";
  const label = isCopied ? "已复制" : hasError ? "重试" : "复制";
  const contentLabel = activeView === "ai" ? "AI 提示词" : "代码";

  return (
    <button
      type="button"
      onClick={onCopy}
      className="inline-flex min-h-11 shrink-0 items-center justify-center gap-2 rounded-full border border-border bg-background/85 px-3 text-sm font-semibold text-foreground shadow-sm outline-none transition-colors hover:bg-muted focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background"
      aria-label={`${label}：${contentLabel}`}
    >
      {isCopied ? (
        <Check className="size-4 text-mint-foreground" aria-hidden="true" />
      ) : (
        <Copy className="size-4" aria-hidden="true" />
      )}
      <span>{label}</span>
    </button>
  );
}

export function CodeBlockClient({
  ai,
  code,
  highlightedHtml,
  language,
  label,
  className,
}: {
  ai: string;
  code: string;
  highlightedHtml: string;
  language: CodeLanguage;
  label?: string;
  className?: string;
}) {
  const { view, setView } = useExampleView();
  const [copyState, setCopyState] = React.useState<CopyState>(null);
  const resetTimer = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  React.useEffect(
    () => () => {
      if (resetTimer.current) {
        clearTimeout(resetTimer.current);
      }
    },
    [],
  );

  const handleCopy = async (activeView: ExampleView, text: string) => {
    if (resetTimer.current) {
      clearTimeout(resetTimer.current);
    }

    try {
      await copyText(text);
      setCopyState({ view: activeView, status: "copied" });
    } catch {
      setCopyState({ view: activeView, status: "error" });
    }

    resetTimer.current = setTimeout(() => setCopyState(null), 2200);
  };

  const liveMessage = copyState
    ? copyState.status === "copied"
      ? `${copyState.view === "ai" ? "AI 提示词" : "代码"}已复制。`
      : "复制失败，请重试。"
    : "";

  return (
    <Tabs
      value={view}
      onValueChange={(nextView) => {
        if (nextView === "ai" || nextView === "code") {
          setView(nextView);
        }
      }}
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
          <TabsTrigger value="ai" className="gap-2 sm:min-w-32">
            <Bot className="size-4" aria-hidden="true" />
            AI 提示词
          </TabsTrigger>
          <TabsTrigger value="code" className="gap-2 sm:min-w-24">
            <Code2 className="size-4" aria-hidden="true" />
            代码
          </TabsTrigger>
        </TabsList>
      </div>

      <TabsContent value="ai" className="m-0 rounded-none">
        <div className="min-h-32 bg-primary/[0.035] p-4 sm:p-6">
          <div className="flex items-center justify-between gap-3">
            <div className="flex min-w-0 items-center gap-3">
              <span className="grid size-9 shrink-0 place-items-center rounded-xl bg-primary/10 text-primary">
                <Bot className="size-4.5" aria-hidden="true" />
              </span>
              <p className="font-mono text-[11px] font-bold uppercase tracking-[0.13em] text-primary">
                直接告诉 AI
              </p>
            </div>
            <CopyButton
              activeView="ai"
              copyState={copyState}
              onCopy={() => handleCopy("ai", ai)}
            />
          </div>
          <p className="mt-4 whitespace-pre-wrap text-pretty text-[16px] leading-7 text-foreground/80">
            {ai}
          </p>
        </div>
      </TabsContent>

      <TabsContent value="code" className="m-0 rounded-none">
        <div className="bg-[var(--code-surface)]">
          <div className="flex min-h-14 items-center justify-between gap-3 border-b border-border bg-muted/25 px-3 py-2 sm:px-4">
            <span className="font-mono text-[11px] font-bold uppercase tracking-[0.13em] text-muted-foreground">
              可复制代码
            </span>
            <CopyButton
              activeView="code"
              copyState={copyState}
              onCopy={() => handleCopy("code", code)}
            />
          </div>
          <div
            className="syntax-code min-w-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
            role="region"
            aria-label={`${language} 代码`}
            tabIndex={0}
            translate="no"
            dangerouslySetInnerHTML={{ __html: highlightedHtml }}
          />
        </div>
      </TabsContent>

      <span className="sr-only" role="status" aria-live="polite" aria-atomic="true">
        {liveMessage}
      </span>
    </Tabs>
  );
}
