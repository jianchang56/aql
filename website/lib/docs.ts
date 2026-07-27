export type DocsPageInfo = {
  title: string;
  description: string;
  href: string;
};

export type DocsSectionInfo = {
  title: string;
  pages: DocsPageInfo[];
};

export const docsSections: DocsSectionInfo[] = [
  {
    title: "开始",
    pages: [
      {
        title: "文档首页",
        description: "选择适合你的学习路径。",
        href: "/docs",
      },
      {
        title: "安装",
        description: "AI 安装、Release 一行安装与 Windows。",
        href: "/docs/getting-started/installation",
      },
      {
        title: "安装 Agent Skill",
        description: "把自然语言变成推荐使用方式。",
        href: "/docs/integrations/agent-skill",
      },
      {
        title: "5 分钟上手",
        description: "用自然语言完成第一条安全查询。",
        href: "/docs/getting-started",
      },
    ],
  },
  {
    title: "日常使用",
    pages: [
      {
        title: "数据库",
        description: "选择内置、联合或命名数据库。",
        href: "/docs/guides/databases",
      },
      {
        title: "编写查询",
        description: "Schema、SELECT、参数与分页。",
        href: "/docs/guides/querying",
      },
      {
        title: "敏感字段",
        description: "理解访问级别与最小临时授权。",
        href: "/docs/guides/access",
      },
      {
        title: "输出结果",
        description: "Table、JSON、JSONL、CSV 与文件输出。",
        href: "/docs/guides/output",
      },
    ],
  },
  {
    title: "参考",
    pages: [
      {
        title: "排障",
        description: "处理数据库、授权、预算和格式问题。",
        href: "/docs/reference/troubleshooting",
      },
    ],
  },
];

export const docsPages = docsSections.flatMap((section) =>
  section.pages.map((page) => ({ ...page, section: section.title })),
);

export function normalizeDocsPath(pathname: string) {
  return pathname === "/" ? pathname : pathname.replace(/\/+$/, "");
}

export function getDocsPage(pathname: string) {
  const normalized = normalizeDocsPath(pathname);
  return docsPages.find((page) => page.href === normalized);
}

export function getDocsNeighbors(pathname: string) {
  const normalized = normalizeDocsPath(pathname);
  const index = docsPages.findIndex((page) => page.href === normalized);

  return {
    previous: index > 0 ? docsPages[index - 1] : undefined,
    next: index >= 0 && index < docsPages.length - 1 ? docsPages[index + 1] : undefined,
  };
}
