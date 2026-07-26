import type { Metadata, Viewport } from "next";
import "@fontsource-variable/manrope";
import "@fontsource-variable/source-sans-3";
import "@fontsource-variable/jetbrains-mono";

import { SiteFooter } from "@/components/site-footer";
import { SiteHeader } from "@/components/site-header";
import { ThemeProvider } from "@/components/theme-provider";
import { ExampleViewProvider } from "@/components/example-view-provider";

import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "AQL — 让 AI 查询本地 Agent 数据",
    template: "%s · AQL",
  },
  description:
    "AQL 是一个推荐由 AI 使用的本地优先、严格只读 SQL CLI。安装 AQL 与 Skill 后，直接用自然语言查询本机 Agent 数据。",
  keywords: ["AQL", "Agent", "SQL", "Agent 数据", "local-first", "read-only"],
};

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#f7f9fc" },
    { media: "(prefers-color-scheme: dark)", color: "#07101d" },
  ],
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="zh-CN"
      suppressHydrationWarning
      data-scroll-behavior="smooth"
    >
      <body className="min-h-dvh bg-background font-sans text-foreground antialiased">
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <ExampleViewProvider>
            <a
              href="#main-content"
              className="fixed left-4 top-3 z-50 -translate-y-20 rounded-full bg-foreground px-4 py-2 text-sm font-semibold text-background transition-transform focus:translate-y-0"
            >
              跳到正文
            </a>
            <SiteHeader />
            <main
              id="main-content"
              tabIndex={-1}
              className="outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
            >
              {children}
            </main>
            <SiteFooter />
          </ExampleViewProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
