import Link from "next/link";

import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "排障",
  description: "按实际错误文本处理安装、Skill、数据库、授权、预算和格式问题。",
};

const issues = [
  [
    "aql: command not found / 无法识别 aql",
    "先从 Cargo bin 目录直接运行，再检查当前终端的 PATH。",
    "aql-command-not-found",
  ],
  [
    "no database selected",
    "在 Shell 中先运行 SHOW DATABASES;，再明确 USE 一个数据库。",
    "database-selection-errors",
  ],
  [
    "unknown database / unavailable database",
    "运行 database list、database discover 和对应的 doctor。",
    "database-selection-errors",
  ],
  [
    "requires --access content",
    "先检查字段，只添加错误要求的最小临时授权。",
    "access-required",
  ],
  [
    "resource budget exceeded",
    "先减少字段、增加 WHERE 与 LIMIT，不要先放大预算。",
    "resource-budget",
  ],
  [
    "unsupported source format at stage",
    "升级完整 AQL，不要绕过校验或直接读取底层文件。",
    "source-format",
  ],
];

const inlineLinkClass =
  "font-semibold text-primary underline-offset-4 outline-none hover:underline focus-visible:ring-2 focus-visible:ring-ring";

export default function TroubleshootingPage() {
  return (
    <DocsPage
      currentPath="/docs/reference/troubleshooting"
      title="按你看到的错误文本排查"
      description="先在下面找到错误中出现的短语，再执行对应的最小检查。不要因为排障而扩大扫描范围、读取私有文件或一次授予全部敏感权限。"
    >
      <DocsSection id="quick-reference" title="先查这张表">
        <dl className="overflow-hidden rounded-2xl border border-border bg-card">
          {issues.map(([issue, solution, target]) => (
            <div
              key={issue}
              className="grid gap-2 border-b border-border px-4 py-4 last:border-0 sm:grid-cols-[minmax(0,14rem)_1fr] sm:gap-5"
            >
              <dt>
                <a
                  href={`#${target}`}
                  className="group inline-flex max-w-full rounded-md outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  <code className="w-fit max-w-full break-words transition-colors group-hover:border-primary/50 group-hover:text-primary">
                    {issue}
                  </code>
                </a>
              </dt>
              <dd className="text-sm leading-6">{solution}</dd>
            </div>
          ))}
        </dl>
      </DocsSection>

      <DocsSection id="aql-command-not-found" title="aql: command not found / 无法识别 aql">
        <p>
          Cargo 默认把可执行文件放在用户目录下。先用绝对路径验证文件是否存在；确认可运行后，再只为当前终端补充 PATH。
        </p>
        <CodeBlock
          ai="在 macOS 或 Linux 上检查 Cargo 安装的 AQL 是否存在：先从 ~/.cargo/bin 直接运行版本检查；成功后只修改当前终端的 PATH，不改 shell 配置文件。"
          code={[
            '"$HOME/.cargo/bin/aql" --version',
            'export PATH="$HOME/.cargo/bin:$PATH"',
            "aql --version",
          ].join("\n")}
          language="bash"
          label="macOS / Linux"
        />
        <CodeBlock
          ai="在 Windows PowerShell 中从 GitHub 源码完整安装 AQL 1.97.0 toolchain 版本，使用 locked 依赖；先用绝对路径验证 aql.exe，再只修改当前 PowerShell 会话的 PATH。"
          code={[
            "git clone https://github.com/jianchang56/aql.git",
            "Set-Location aql",
            "rustup toolchain install 1.97.0",
            "cargo +1.97.0 install --locked --path crates/aql",
            '& "$HOME\\.cargo\\bin\\aql.exe" --version',
            '$env:Path = "$HOME\\.cargo\\bin;$env:Path"',
            "aql --version",
          ].join("\n")}
          language="powershell"
          label="Windows PowerShell · 完整安装与 PATH"
        />
        <DocsNote title="绝对路径也失败时" tone="amber">
          说明 AQL 尚未安装成功，而不只是 PATH 问题。回到
          <Link href="/docs/getting-started/installation" className={inlineLinkClass}>
            安装页
          </Link>
          检查 Git、rustup、Rust 版本和 Cargo 安装输出。
        </DocsNote>
      </DocsSection>

      <DocsSection id="brew-or-npx-not-found" title="brew 或 npx 找不到">
        <div className="grid gap-4 md:grid-cols-2">
          <DocsNote title="brew: command not found" tone="amber">
            当前尚无正式预编译 AQL Release，因此现在不需要为了 AQL 专门安装 Homebrew；请使用 Cargo 源码安装。正式 Release 发布后，Homebrew 仍只是 macOS/Linux 的可选方式。
          </DocsNote>
          <DocsNote title="npx: command not found" tone="amber">
            <code>npx</code> 用于安装 Skill，不用于运行 AQL。安装 Node.js
            <code>22.20.0</code> 或更高版本后，确认 <code>npm</code> 和
            <code>npx</code> 一起可用；也可以手动复制完整的 Skill 目录。
          </DocsNote>
        </div>
        <CodeBlock
          ai="检查 Node.js、npm 和 npx 的版本；如果缺失，只告诉我缺少哪一项和所需的 Node.js 最低版本，不要自动安装。"
          code={["node --version", "npm --version", "npx --version"].join("\n")}
          language="bash"
          label="Skill 安装工具检查"
        />
      </DocsSection>

      <DocsSection id="skill-not-recognized" title="Agent 不识别 $aql">
        <p>
          <code>$aql</code> 是提示词中的 Skill 名称，不是终端命令。CLI 验证与 Skill 验证必须分别完成。
        </p>
        <CodeBlock
          ai="检查 aql Skill 是否已经全局安装到 Codex，并确认 AQL CLI 可运行。不要查询真实 Agent 数据。"
          code={[
            "npx --yes skills list --global --agent codex",
            "aql --version",
          ].join("\n")}
          language="bash"
          label="示例：Codex"
        />
        <ol className="list-decimal space-y-2 pl-5">
          <li>
            把 <code>codex</code> 替换为实际 Agent ID：<code>claude-code</code>、
            <code>kimi-code-cli</code> 或 <code>opencode</code>。
          </li>
          <li>确认列表中出现 aql，然后完全重启 Agent 或新建会话。</li>
          <li>
            仍未出现时，返回
            <Link href="/docs/integrations/agent-skill" className={inlineLinkClass}>
              Skill 安装页
            </Link>
            ，使用显式 <code>--agent</code> 和必要时的 <code>--copy</code> 重新安装。
          </li>
        </ol>
      </DocsSection>

      <DocsSection id="database-selection-errors" title="no database selected / unknown database">
        <p>
          Shell 会显示 <code>no database selected; run SHOW DATABASES; and USE &lt;database&gt;;</code>。
          非交互查询可能显示 <code>unknown database; run SHOW DATABASES</code> 或来源不可用。
        </p>
        <CodeBlock
          ai="使用 $aql 列出已经配置和可以发现的数据库，然后只诊断我明确选择的 codex。不要选择 all，也不要递归扫描其他目录。"
          code={[
            "aql database list",
            "aql database discover",
            "aql doctor -d codex",
          ].join("\n")}
          language="bash"
          label="数据库检查"
        />
        <DocsNote title="仍然没有数据库" tone="amber">
          确认对应 Agent 至少生成过一次本地会话。只有你主动移动过数据目录时，才配置命名数据库；不要用
          <code>all</code> 掩盖单个来源不可用的问题。
        </DocsNote>
      </DocsSection>

      <DocsSection id="access-required" title="requires --access content">
        <p>
          错误 <code>query references a field that requires --access content</code> 表示查询选择了敏感字段。先确认该字段确实必要，再只添加错误要求的授权。
        </p>
        <CodeBlock
          ai="使用 $aql 检查 messages 表的字段与访问级别。只有在我确认确实需要正文后，才使用 content 临时授权返回最近 10 条消息的 role 和 content。"
          code={[
            "aql schema messages",
            "aql query -d codex --access content \\",
            "  'SELECT role, content FROM messages ORDER BY created_at DESC LIMIT 10'",
          ].join("\n")}
          language="bash"
          label="最小临时授权"
        />
        <DocsNote title="不要一次授予全部权限">
          <code>path</code>、<code>content</code>、<code>tool-input</code> 和
          <code>tool-output</code> 是彼此独立的临时授权；Secret 没有任何授权形式。
        </DocsNote>
      </DocsSection>

      <DocsSection id="resource-budget" title="resource budget exceeded / timeout">
        <CodeBlock
          ai="把查询缩小为 codex 最近更新的 20 个会话，只返回 session_id 和 model。不要先提高预算或超时。"
          code={[
            "aql query -d codex \\",
            "  'SELECT session_id, model",
            "   FROM sessions",
            "   ORDER BY updated_at DESC",
            "   LIMIT 20'",
          ].join("\n")}
          language="bash"
          label="先缩小查询"
        />
        <p>
          优先减少列、增加筛选与 <code>LIMIT</code>。只有缩小后仍不能完成明确任务，才考虑调整
          <code>--timeout</code> 或资源上限。
        </p>
      </DocsSection>

      <DocsSection id="source-format" title="unsupported source format at stage">
        <p>
          这表示 Agent 的本地格式与当前 AQL 版本不兼容。正确动作是升级完整 AQL，而不是寻找单独的组件更新、关闭校验或直接读取私有文件。
        </p>
        <CodeBlock
          ai="在现有 AQL 源码仓库中获取最新提交，使用 Rust 1.97.0 和 locked 依赖重新安装完整 AQL，然后验证版本。不要修改 Agent 数据。"
          code={[
            "git pull --ff-only",
            "rustup toolchain install 1.97.0",
            "cargo +1.97.0 install --locked --path crates/aql",
            "aql --version",
          ].join("\n")}
          language="bash"
          label="升级完整 AQL"
        />
      </DocsSection>

      <DocsSection id="diagnostics" title="运行去敏诊断">
        <CodeBlock
          ai="使用 $aql 检查可发现数据库，诊断 codex，并对一个最多返回 10 条 session_id 的查询开启去敏 diagnostics。"
          code={[
            "aql database discover",
            "aql doctor -d codex",
            "aql query -d codex --diagnostics \\",
            "  'SELECT session_id FROM sessions LIMIT 10'",
          ].join("\n")}
          language="bash"
          label="诊断命令"
        />
        <p>
          diagnostics 只输出去敏的来源、扫描、预算和阶段耗时，不包含 SQL literal、参数值、真实路径或查询结果。
        </p>
      </DocsSection>

      <DocsSection id="json-errors" title="自动化读取 JSON 错误">
        <CodeBlock
          ai="使用 $aql 对名为 missing 的数据库执行计数查询，并把错误输出为机器可读 JSON。"
          code={[
            "aql --error-format json query -d missing \\",
            "  'SELECT COUNT(*) FROM sessions'",
          ].join("\n")}
          language="bash"
          label="触发 JSON 错误"
        />
        <CodeBlock
          ai="解释 AQL 的 JSON 错误对象会返回哪些字段，以及自动化应该如何使用 category、stage、hint 和 exit_code。"
          code={[
            "{",
            '  "category": "not_found",',
            '  "stage": "resolve",',
            '  "message": "unknown database; run SHOW DATABASES",',
            '  "hint": "run `aql database list`, then select one with `-d <database>`",',
            '  "location": null,',
            '  "exit_code": 4',
            "}",
          ].join("\n")}
          language="json"
          label="error.json"
        />
      </DocsSection>

      <DocsNote title="Windows 仍会保持安全失败">
        Windows 上如果 AQL 无法确认文件没有被重定向、替换或覆盖，它会拒绝执行，而不会降低安全检查。
      </DocsNote>
    </DocsPage>
  );
}
