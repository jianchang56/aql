import { createHighlighter } from "shiki";

import { CodeBlockClient } from "@/components/docs/code-block-client";
import type { CodeLanguage } from "@/components/docs/code-types";

export type { CodeLanguage } from "@/components/docs/code-types";

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
  const highlightedHtml = await renderCode(code, language);

  return (
    <CodeBlockClient
      ai={ai}
      code={code}
      highlightedHtml={highlightedHtml}
      language={language}
      label={label}
      className={className}
    />
  );
}
