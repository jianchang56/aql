import { CodeBlock } from "@/components/docs/code-block";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

const queryExamples = [
  {
    value: "models",
    label: "按模型统计",
    database: "codex",
    prompt: "使用 $aql 查询 codex 的会话数量，按模型分组并按数量从高到低返回。",
    query: [
      "SELECT model, COUNT(*) AS sessions",
      "FROM sessions",
      "GROUP BY model",
      "ORDER BY sessions DESC;",
    ].join("\n"),
  },
  {
    value: "usage",
    label: "Token 用量",
    database: "claude",
    prompt: "使用 $aql 统计 claude 的 Token 用量，按模型汇总输入与输出 Token，并按总量降序返回。",
    query: [
      "SELECT model, SUM(input_tokens + output_tokens) AS tokens",
      "FROM usage",
      "GROUP BY model",
      "ORDER BY tokens DESC;",
    ].join("\n"),
  },
  {
    value: "federation",
    label: "跨 Agent",
    database: "all",
    prompt: "使用 $aql 联合查询所有可用 Agent 的会话数量，按 Agent 分组；只返回聚合结果。",
    query: [
      "SELECT agent_id, COUNT(*) AS sessions",
      "FROM sessions",
      "GROUP BY agent_id",
      "ORDER BY sessions DESC;",
    ].join("\n"),
  },
];

export function QueryTabs() {
  return (
    <Tabs defaultValue="models" className="min-w-0">
      <TabsList aria-label="查询示例">
        {queryExamples.map((example) => (
          <TabsTrigger key={example.value} value={example.value}>
            {example.label}
          </TabsTrigger>
        ))}
      </TabsList>
      {queryExamples.map((example) => (
        <TabsContent key={example.value} value={example.value}>
          <CodeBlock
            ai={example.prompt}
            code={example.query}
            language="sql"
            label={`aql query · -d ${example.database}`}
          />
        </TabsContent>
      ))}
    </Tabs>
  );
}
