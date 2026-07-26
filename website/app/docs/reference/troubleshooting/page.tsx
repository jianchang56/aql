import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "排障",
  description: "诊断数据库不可用、授权、预算、格式漂移和安装问题。",
};

const issues = [
  ["缺少数据库", "运行 database list，然后用 -d 或 USE 显式选择。"],
  ["数据库不可用", "运行 database discover 和 doctor。"],
  ["requires --access", "查看 schema，再添加报错要求的最小授权。"],
  ["timeout / budget exceeded", "减少字段、增加 WHERE 与 LIMIT。"],
  ["format drift", "升级 AQL Adapter，不要绕过校验读取底层文件。"],
];

export default function TroubleshootingPage() {
  return (
    <DocsPage
      currentPath="/docs/reference/troubleshooting"
      title="从错误类别定位问题"
      description="AQL 的错误会说明阶段和恢复建议。优先缩小查询或检查数据库状态，不要绕过只读与格式校验。"
    >
      <DocsSection title="常见问题">
        <div className="overflow-hidden rounded-2xl border border-border bg-card">
          {issues.map(([issue, solution]) => (
            <div
              key={issue}
              className="grid gap-1 border-b border-border px-4 py-4 last:border-0 sm:grid-cols-[12rem_1fr]"
            >
              <strong className="text-foreground">{issue}</strong>
              <span className="text-sm leading-6">{solution}</span>
            </div>
          ))}
        </div>
      </DocsSection>

      <DocsSection title="运行诊断">
        <CodeBlock
          ai="使用 $aql 检查可发现数据库，诊断 codex，并对一个最多返回 10 条 session_id 的查询开启去敏 diagnostics。"
          code={[
            "aql database discover",
            "aql doctor -d codex",
            "aql query -d codex --diagnostics \\",
            "  'SELECT session_id FROM sessions LIMIT 10'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          diagnostics 只输出去敏的来源、扫描、预算和阶段耗时，不包含 SQL literal、参数值、真实路径或结果。
        </p>
      </DocsSection>

      <DocsSection title="自动化读取 JSON 错误">
        <CodeBlock
          ai="使用 $aql 对名为 missing 的数据库执行计数查询，并把错误输出为机器可读 JSON。"
          code={[
            "aql --error-format json query -d missing \\",
            "  'SELECT COUNT(*) FROM sessions'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <CodeBlock
          ai="解释 AQL 的 JSON 错误对象会返回哪些字段，以及自动化应该如何使用 category、stage、hint 和 exit_code。"
          code={[
            "{",
            "  \"category\": \"database\",",
            "  \"stage\": \"resolve\",",
            "  \"message\": \"...\",",
            "  \"hint\": \"...\",",
            "  \"location\": null,",
            "  \"exit_code\": 2",
            "}",
          ].join("\n")}
          language="json"
          label="error.json"
        />
      </DocsSection>

      <DocsSection title="Windows">
        <p>
          Windows 已支持通过 Cargo 构建和运行。如果命令找不到，请确认 Cargo bin
          目录在 PATH；预编译 Windows archive 当前尚未发布。
        </p>
        <CodeBlock
          ai="请在 Windows PowerShell 中检查 PATH；如果 AQL 尚未安装，使用当前仓库和 locked 依赖通过 Cargo 安装，然后验证版本。"
          code={[
            "$env:Path -split ';'",
            "cargo install --locked --path crates/aql",
            "aql --version",
          ].join("\n")}
          language="powershell"
          label="PowerShell"
        />
        <DocsNote title="无法维持安全保证时会失败">
          Windows 上无法维持 nofollow、identity 或 no-clobber 保证的文件操作仍会 fail
          closed，不会静默降级为不安全行为。
        </DocsNote>
      </DocsSection>
    </DocsPage>
  );
}
