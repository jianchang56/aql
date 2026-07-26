"use client";

import { Moon, Sun } from "lucide-react";
import { useTheme } from "next-themes";
import { useEffect, useSyncExternalStore } from "react";

import { Button } from "@/components/ui/button";

export function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  const mounted = useSyncExternalStore(
    () => () => undefined,
    () => true,
    () => false,
  );
  const nextTheme = resolvedTheme === "dark" ? "light" : "dark";
  const label = mounted
    ? nextTheme === "dark"
      ? "切换到暗色模式"
      : "切换到亮色模式"
    : "切换颜色模式";

  useEffect(() => {
    if (resolvedTheme !== "light" && resolvedTheme !== "dark") {
      return;
    }

    const color = resolvedTheme === "dark" ? "#07101d" : "#f7f9fc";
    document
      .querySelectorAll<HTMLMetaElement>('meta[name="theme-color"]')
      .forEach((meta) => meta.setAttribute("content", color));
  }, [resolvedTheme]);

  return (
    <Button
      type="button"
      variant="ghost"
      size="icon"
      className="relative"
      aria-label={label}
      title={label}
      onClick={() => setTheme(nextTheme)}
    >
      <Sun
        className="rotate-0 scale-100 transition-transform dark:rotate-90 dark:scale-0"
        aria-hidden="true"
      />
      <Moon
        className="absolute rotate-90 scale-0 transition-transform dark:rotate-0 dark:scale-100"
        aria-hidden="true"
      />
      <span className="sr-only">{label}</span>
    </Button>
  );
}
