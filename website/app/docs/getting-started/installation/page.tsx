import Link from "next/link";

import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";
import { prebuiltPlatforms, publishedRelease } from "@/lib/project-status";

export const metadata = {
  title: "安装",
  description: publishedRelease
    ? `安装 AQL ${publishedRelease.tag} 预编译版本，或从源码构建。`
    : "从源码安装 AQL；首个正式 Release 发布后可使用预编译一行安装。",
};

const sourceInstallBash = [
  "git clone https://github.com/jianchang56/aql.git",
  "cd aql",
  "rustup toolchain install 1.97.0",
  "cargo +1.97.0 install --locked --path crates/aql",
  "aql --version",
].join("\n");

const sourceInstallPowerShell = [
  "git clone https://github.com/jianchang56/aql.git",
  "Set-Location aql",
  "rustup toolchain install 1.97.0",
  "cargo +1.97.0 install --locked --path crates/aql",
  "aql --version",
].join("\n");

const homebrewOneLine =
  'tmp="$(mktemp -d)" && curl -fsSL https://github.com/jianchang56/aql/releases/latest/download/aql.rb -o "$tmp/aql.rb" && brew install --formula "$tmp/aql.rb" && aql --version';

const bashInstall = publishedRelease
  ? [
      "set -euo pipefail",
      "",
      `VERSION=${publishedRelease.version}`,
      "REPO=https://github.com/jianchang56/aql",
      "",
      "case \"$(uname -s):$(uname -m)\" in",
      "  Darwin:arm64) TARGET=aarch64-macos ;;",
      "  Darwin:x86_64) TARGET=x86_64-macos ;;",
      "  Linux:aarch64|Linux:arm64) TARGET=aarch64-linux ;;",
      "  Linux:x86_64) TARGET=x86_64-linux ;;",
      "  *) echo \"Unsupported platform\" >&2; exit 1 ;;",
      "esac",
      "",
      "ARCHIVE=\"aql-$VERSION-$TARGET.tar.gz\"",
      `BASE="$REPO/releases/download/${publishedRelease.tag}"`,
      "curl -fLO \"$BASE/$ARCHIVE\"",
      "curl -fLO \"$BASE/$ARCHIVE.sha256\"",
      "",
      "EXPECTED=$(awk '{print $1}' \"$ARCHIVE.sha256\")",
      "if command -v sha256sum >/dev/null 2>&1; then",
      "  ACTUAL=$(sha256sum \"$ARCHIVE\" | awk '{print $1}')",
      "else",
      "  ACTUAL=$(shasum -a 256 \"$ARCHIVE\" | awk '{print $1}')",
      "fi",
      "test \"$EXPECTED\" = \"$ACTUAL\"",
      "",
      "PREFIX=\"$HOME/.local/aql/$VERSION\"",
      "mkdir -p \"$PREFIX\" \"$HOME/.local/bin\"",
      "tar -xzf \"$ARCHIVE\" -C \"$PREFIX\" --strip-components=1",
      "ln -s \"$PREFIX/bin/aql\" \"$HOME/.local/bin/aql\"",
      '"$HOME/.local/bin/aql" --version',
    ].join("\n")
  : null;

export default function InstallationPage() {
  return (
    <DocsPage
      currentPath="/docs/getting-started/installation"
      title="先选平台，再安装"
      description="AQL 当前可以在 macOS、Linux 和 Windows 上从源码安装。正式预编译 Release 发布后，macOS 与 Linux 会提供验证 SHA256 的一行安装。"
    >
      <DocsSection id="release-status" title="先确认当前发布状态">
        <DocsNote
          title={
            publishedRelease
              ? `正式 Release：${publishedRelease.tag}`
              : "当前尚无正式预编译 Release"
          }
          tone={publishedRelease ? "mint" : "amber"}
        >
          {publishedRelease ? (
            <>
              已验证的预编译包覆盖 {prebuiltPlatforms.join("、")}。Windows
              当前仍使用 Cargo 从源码安装。
            </>
          ) : (
            <>
              GitHub Releases 目前没有可下载的正式资产，因此不要运行指向
              <code>releases/latest</code> 的安装命令。当前请选择下面的 Cargo
              源码安装；发布完成后，本页会显示预编译命令。
            </>
          )}
        </DocsNote>

        <div className="grid gap-3 sm:grid-cols-2">
          <div className="rounded-2xl border border-border bg-card p-5">
            <p className="font-display font-extrabold text-foreground">macOS / Linux</p>
            <p className="mt-2 text-sm leading-6">
              当前使用 Git、rustup 与 Cargo。正式 Release 发布后，可改用 Homebrew
              Formula 或直接下载 archive。
            </p>
          </div>
          <div className="rounded-2xl border border-border bg-card p-5">
            <p className="font-display font-extrabold text-foreground">Windows</p>
            <p className="mt-2 text-sm leading-6">
              当前使用 PowerShell、Git、rustup 与 Cargo。核心 CLI、配置与文件输出均已支持。
            </p>
          </div>
        </div>
      </DocsSection>

      <DocsSection id="source-install" title="当前可用：Cargo 源码安装">
        <p>
          需要 Git、rustup 和 Rust <code>1.97.0</code>。命令使用 locked
          依赖，不使用 <code>sudo</code>，也不修改 shell 配置。
        </p>
        <CodeBlock
          ai="当前还没有正式 AQL Release。请在 macOS 或 Linux 上从 GitHub 源码安装 AQL：使用 Rust 1.97.0 和 locked 依赖，不使用 sudo，不修改 shell 配置，最后运行 aql --version。"
          code={sourceInstallBash}
          language="bash"
          label="macOS / Linux"
        />
        <CodeBlock
          ai="当前还没有正式 AQL Release。请在 Windows PowerShell 中从 GitHub 源码安装 AQL：使用 Rust 1.97.0 和 locked 依赖，不修改其他配置，最后运行 aql --version。"
          code={sourceInstallPowerShell}
          language="powershell"
          label="Windows PowerShell"
        />
        <DocsNote title="如果 aql 不在 PATH 中">
          Cargo 通常把程序安装到 <code>~/.cargo/bin</code>。先直接运行该目录中的
          AQL 验证安装，再由你明确决定是否把目录加入 PATH；AQL 不会自动修改
          <code>.zshrc</code>、<code>.bashrc</code> 或 PowerShell 配置。
        </DocsNote>
      </DocsSection>

      <DocsSection id="prebuilt-release" title="预编译 Release：发布后的一行安装">
        {publishedRelease && bashInstall ? (
          <>
            <p>
              已安装 Homebrew 的 macOS / Linux 用户可以一行完成下载、SHA256
              校验、安装和版本检查。
            </p>
            <CodeBlock
              ai={`请安装 AQL ${publishedRelease.tag}。在 macOS 或 Linux 上优先使用正式 Release 的预编译包，并验证 Formula 中固定的 SHA256；不要使用 sudo 或修改 shell 配置，完成后运行 aql --version。`}
              code={homebrewOneLine}
              language="bash"
              label={`Homebrew · ${publishedRelease.tag}`}
            />
            <p>
              没有 Homebrew 时，使用下面的完整流程直接下载 archive。验证命令通过
              <code>~/.local/bin/aql</code> 的绝对路径运行，不依赖当前 PATH。
            </p>
            <CodeBlock
              ai={`请从 AQL ${publishedRelease.tag} 安装预编译包：识别当前 macOS/Linux 架构，下载 archive 和 checksum，验证 SHA256 后安装到用户目录。不要使用 sudo，不要覆盖已有链接，也不要修改 shell 配置。`}
              code={bashInstall}
              language="bash"
              label="直接下载并校验"
            />
          </>
        ) : (
          <DocsNote title="这不是当前可运行的安装方式" tone="amber">
            首个正式 tag 发布并通过四个平台构建后，Release workflow 会生成 archive、
            checksum 和 <code>aql.rb</code>。在这些资产真实存在前，本页不会展示会返回
            404 的安装命令。
          </DocsNote>
        )}
      </DocsSection>

      <DocsSection id="verify-installation" title="验证安装并进入下一步">
        <CodeBlock
          ai="验证 AQL 安装，并列出已经配置与可以发现的数据库。只做只读检查，不替我选择默认数据库。"
          code={[
            "aql --version",
            "aql database list",
            "aql database discover",
          ].join("\n")}
          language="bash"
          label="安装验证"
        />
        <DocsNote title="下一步：安装 AQL Skill" tone="mint">
          CLI 验证通过后，继续前往
          <Link
            href="/docs/integrations/agent-skill"
            className="mx-1 rounded font-semibold text-primary outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring"
          >
            Skill 安装页
          </Link>
          。安装 Skill 后，日常查询可以直接使用自然语言。
        </DocsNote>
      </DocsSection>
    </DocsPage>
  );
}
