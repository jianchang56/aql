"use client";

import * as React from "react";
import { usePathname } from "next/navigation";

export type ExampleView = "ai" | "code";

const STORAGE_KEY = "aql-example-view";

type ExampleViewContextValue = {
  view: ExampleView;
  setView: (view: ExampleView) => void;
};

const ExampleViewContext = React.createContext<ExampleViewContextValue | null>(
  null,
);

let currentView: ExampleView | null = null;
const listeners = new Set<() => void>();
let listeningToBrowser = false;

function readUrlView(): ExampleView | null {
  const value = new URL(window.location.href).searchParams.get("view");
  return value === "ai" || value === "code" ? value : null;
}

function readStoredView(): ExampleView | null {
  try {
    const value = window.localStorage.getItem(STORAGE_KEY);
    return value === "ai" || value === "code" ? value : null;
  } catch {
    return null;
  }
}

function persistView(view: ExampleView) {
  try {
    window.localStorage.setItem(STORAGE_KEY, view);
  } catch {
    // The URL still preserves the preference when storage is unavailable.
  }
}

function syncUrl(view: ExampleView) {
  const url = new URL(window.location.href);

  if (view === "code") {
    url.searchParams.set("view", "code");
  } else {
    url.searchParams.delete("view");
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  const currentUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`;

  if (nextUrl !== currentUrl) {
    window.history.replaceState(window.history.state, "", nextUrl);
  }
}

function emitChange() {
  for (const listener of listeners) {
    listener();
  }
}

function updateView(
  view: ExampleView,
  options: { persist?: boolean; sync?: boolean } = {},
) {
  const { persist = true, sync = true } = options;
  const changed = currentView !== view;
  currentView = view;

  if (persist) {
    persistView(view);
  }

  if (sync) {
    syncUrl(view);
  }

  if (changed) {
    emitChange();
  }
}

function handlePopState() {
  updateView(readUrlView() ?? "ai", { sync: false });
}

function handleStorage(event: StorageEvent) {
  if (event.key !== STORAGE_KEY) {
    return;
  }

  const nextView =
    event.newValue === "ai" || event.newValue === "code"
      ? event.newValue
      : "ai";
  updateView(nextView, { persist: false });
}

function subscribe(listener: () => void) {
  listeners.add(listener);

  if (!listeningToBrowser) {
    window.addEventListener("popstate", handlePopState);
    window.addEventListener("storage", handleStorage);
    listeningToBrowser = true;
  }

  return () => {
    listeners.delete(listener);

    if (listeners.size === 0 && listeningToBrowser) {
      window.removeEventListener("popstate", handlePopState);
      window.removeEventListener("storage", handleStorage);
      listeningToBrowser = false;
    }
  };
}

function getSnapshot(): ExampleView {
  currentView ??= readUrlView() ?? readStoredView() ?? "ai";
  return currentView;
}

function getServerSnapshot(): ExampleView {
  return "ai";
}

export function ExampleViewProvider({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const view = React.useSyncExternalStore(
    subscribe,
    getSnapshot,
    getServerSnapshot,
  );

  React.useEffect(() => {
    persistView(view);
    syncUrl(view);
  }, [pathname, view]);

  const value = React.useMemo(
    () => ({ view, setView: updateView }),
    [view],
  );

  return (
    <ExampleViewContext.Provider value={value}>
      {children}
    </ExampleViewContext.Provider>
  );
}

export function useExampleView() {
  const value = React.useContext(ExampleViewContext);

  if (!value) {
    throw new Error("useExampleView must be used within ExampleViewProvider");
  }

  return value;
}
