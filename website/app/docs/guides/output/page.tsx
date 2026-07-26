import { CodeBlock } from "@/components/docs/code-block";
import {
  DocsNote,
  DocsPage,
  DocsSection,
} from "@/components/docs/docs-page";

export const metadata = {
  title: "输出结果",
  description: "选择 table、JSON、JSONL、CSV 或原子文件输出。",
};

export default function OutputPage() {
  return (
    <DocsPage
      currentPath="/docs/guides/output"
      title="选择适合下一步的输出"
      description="快速阅读使用 table，程序处理使用 JSON/JSONL，电子表格使用安全 CSV；只有完整结果才会发布。"
    >
      <DocsSection title="标准输出格式">
        <CodeBlock
          ai="使用 $aql 从 codex 查询最多 10 个模型，并分别说明如何输出为 table、JSON、JSONL 和安全 CSV。"
          code={[
            "aql query -d codex --output table 'SELECT model FROM sessions LIMIT 10'",
            "aql query -d codex --output json  'SELECT model FROM sessions LIMIT 10'",
            "aql query -d codex --output jsonl 'SELECT model FROM sessions LIMIT 10'",
            "aql query -d codex --output csv   'SELECT model FROM sessions LIMIT 10'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <DocsNote title="CSV 始终安全">
          CSV 使用 RFC 4180，并始终转义电子表格公式形状文本；不存在 raw CSV 模式。
        </DocsNote>
      </DocsSection>

      <DocsSection title="原子写入新文件">
        <CodeBlock
          ai="使用 $aql 把 codex usage 查询结果以 JSON 原子写入一个尚不存在的 ./result.json。不要覆盖现有文件。"
          code={[
            "aql query -d codex \\",
            "  --output json \\",
            "  --output-file ./result.json \\",
            "  'SELECT * FROM usage'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          目标必须不存在。AQL 在同目录写入 private 临时文件，完成后执行 no-replace
          原子发布；失败不会留下目标文件或部分成功结果。
        </p>
      </DocsSection>

      <DocsSection title="输出预算">
        <CodeBlock
          ai="使用 $aql 查询 codex 的最多 1000 个会话，只返回 session_id 和 model；把输出限制为 16 MiB，超时设为 10 秒。"
          code={[
            "aql query -d codex \\",
            "  --max-output-bytes 16MiB \\",
            "  --timeout 10s \\",
            "  'SELECT session_id, model FROM sessions LIMIT 1000'",
          ].join("\n")}
          language="bash"
          label="terminal"
        />
        <p>
          遇到预算错误时，先减少字段、增加 WHERE 和 LIMIT，再确认是否需要提高上限。
        </p>
      </DocsSection>
    </DocsPage>
  );
}
