import { useState, useEffect, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { PageHeader } from "../components/PageHeader";
import { MermaidBlock } from "../components/MermaidBlock";

const API = import.meta.env.VITE_API_URL ?? "";

// Page definitions — order matters for sidebar
const DOC_PAGES = [
  { key: "overview", icon: "🌐" },
  { key: "pipeline", icon: "🔄" },
  { key: "engines", icon: "⚙️" },
  { key: "options", icon: "📈" },
  { key: "database", icon: "💾" },
  { key: "infrastructure", icon: "🏗️" },
] as const;

function useDocList() {
  return useQuery({
    queryKey: ["doc-list"],
    queryFn: async () => {
      const res = await fetch(`${API}/api/status/docs`);
      if (!res.ok) return [];
      return res.json() as Promise<{ key: string; title: string }[]>;
    },
    staleTime: 60_000,
  });
}

function useDoc(key: string) {
  return useQuery({
    queryKey: ["doc", key],
    queryFn: async () => {
      const res = await fetch(`${API}/api/status/docs/${key}`);
      if (!res.ok) throw new Error(`Failed to load doc: ${res.status}`);
      return res.json() as Promise<{ title: string; content: string }>;
    },
    staleTime: 5 * 60_000,
    enabled: !!key,
  });
}

// Extract headings from markdown for TOC
function extractHeadings(md: string): { level: number; text: string; id: string }[] {
  const headings: { level: number; text: string; id: string }[] = [];
  for (const line of md.split("\n")) {
    const m = line.match(/^(#{1,4})\s+(.+)/);
    if (m) {
      const text = m[2].replace(/[`*_~]/g, "");
      const id = text
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");
      headings.push({ level: m[1].length, text, id });
    }
  }
  return headings;
}

export default function Docs() {
  const { pageKey } = useParams<{ pageKey?: string }>();
  const navigate = useNavigate();
  const activeKey = pageKey || "overview";

  const docList = useDocList();
  const doc = useDoc(activeKey);
  const content = (doc.data?.content ?? "") as string;

  const headings = useMemo(() => extractHeadings(content), [content]);

  const [activeSection, setActiveSection] = useState("");

  // Track scroll position for active TOC highlight
  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setActiveSection(entry.target.id);
          }
        }
      },
      { rootMargin: "-80px 0px -70% 0px" },
    );

    const els = document.querySelectorAll("[data-heading-id]");
    els.forEach((el) => observer.observe(el));

    return () => observer.disconnect();
  }, [content]);

  // Build page list: merge API list with local icon defs
  const pages = useMemo(() => {
    const apiTitles = new Map((docList.data ?? []).map((d: { key: string; title: string }) => [d.key, d.title]));
    return DOC_PAGES.map((p) => ({
      ...p,
      title: apiTitles.get(p.key) ?? p.key,
    }));
  }, [docList.data]);

  return (
    <div className="flex h-full">
      {/* Left sidebar: page nav + section TOC */}
      <aside className="hidden w-56 shrink-0 overflow-y-auto border-r border-border-dim py-4 pr-2 lg:block">
        {/* Page selector */}
        <div className="mb-3 px-3">
          <div className="text-[9px] uppercase tracking-widest text-muted/50 mb-2">
            Documentation
          </div>
          {pages.map((p) => (
            <button
              key={p.key}
              onClick={() => navigate(`/docs/${p.key}`)}
              className={`flex items-center gap-2 w-full text-left rounded px-2 py-1.5 text-xs transition-colors ${
                activeKey === p.key
                  ? "bg-surface-overlay text-accent font-medium"
                  : "text-muted hover:text-zinc-200"
              }`}
            >
              <span className="text-[11px]">{p.icon}</span>
              <span className="truncate">{p.title}</span>
            </button>
          ))}
        </div>

        {/* Section TOC for current page */}
        {headings.length > 0 && (
          <div className="border-t border-border-dim pt-3 px-3">
            <div className="text-[9px] uppercase tracking-widest text-muted/50 mb-2">
              On this page
            </div>
            {headings.map((h) => (
              <a
                key={h.id}
                href={`#${h.id}`}
                className={`block truncate py-0.5 text-[11px] transition-colors ${
                  h.level === 1
                    ? "font-semibold text-zinc-300 mt-2"
                    : h.level === 2
                    ? "text-zinc-400 hover:text-zinc-200"
                    : "text-zinc-500 hover:text-zinc-300 pl-2"
                } ${activeSection === h.id ? "!text-accent" : ""}`}
                style={{ paddingLeft: h.level > 2 ? `${(h.level - 2) * 8 + 8}px` : undefined }}
              >
                {h.text}
              </a>
            ))}
          </div>
        )}
      </aside>

      {/* Main content */}
      <div className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-5xl px-6 py-4">
          <PageHeader
            title={doc.data?.title ?? "Documentation"}
            subtitle="Prometheus platform architecture, modules, and API reference"
            onRefresh={() => doc.refetch()}
          />

          {doc.isLoading && (
            <div className="flex h-40 items-center justify-center text-xs text-muted">
              Loading documentation...
            </div>
          )}

          {doc.isError && (
            <div className="rounded border border-red-800 bg-red-950/30 p-4 text-sm text-red-400">
              Failed to load documentation. Is the API running?
            </div>
          )}

          {content && (
            <article className="prose-prom mt-4">
              <Markdown
                remarkPlugins={[remarkGfm]}
                components={{
                  // Intercept <pre> to detect mermaid fenced blocks
                  pre({ children, ...props }) {
                    // react-markdown wraps fenced code in <pre><code>...
                    // Check if the child is a code element with language-mermaid
                    const child = Array.isArray(children) ? children[0] : children;
                    if (
                      child &&
                      typeof child === "object" &&
                      "props" in child &&
                      child.props?.className?.includes("language-mermaid")
                    ) {
                      const codeStr = String(child.props.children ?? "").replace(/\n$/, "");
                      return <MermaidBlock code={codeStr} />;
                    }
                    return (
                      <pre className="my-3 overflow-x-auto rounded-lg border border-border-dim bg-zinc-900 p-3 text-[11px]" {...props}>
                        {children}
                      </pre>
                    );
                  },
                  code({ className, children, ...props }) {
                    return (
                      <code
                        className={`${className ?? ""} rounded bg-zinc-800/60 px-1 py-0.5 text-[11px]`}
                        {...props}
                      >
                        {children}
                      </code>
                    );
                  },
                  h1({ children }) {
                    const id = String(children)
                      .toLowerCase()
                      .replace(/[^a-z0-9]+/g, "-")
                      .replace(/^-|-$/g, "");
                    return (
                      <h1 id={id} data-heading-id className="text-2xl font-bold text-zinc-100 mt-8 mb-3 pb-2 border-b border-border-dim">
                        {children}
                      </h1>
                    );
                  },
                  h2({ children }) {
                    const id = String(children)
                      .toLowerCase()
                      .replace(/[^a-z0-9]+/g, "-")
                      .replace(/^-|-$/g, "");
                    return (
                      <h2 id={id} data-heading-id className="text-lg font-semibold text-zinc-200 mt-8 mb-2">
                        {children}
                      </h2>
                    );
                  },
                  h3({ children }) {
                    const id = String(children)
                      .toLowerCase()
                      .replace(/[^a-z0-9]+/g, "-")
                      .replace(/^-|-$/g, "");
                    return (
                      <h3 id={id} data-heading-id className="text-sm font-semibold text-zinc-300 mt-6 mb-1.5">
                        {children}
                      </h3>
                    );
                  },
                  h4({ children }) {
                    return (
                      <h4 className="text-xs font-semibold text-zinc-400 mt-4 mb-1">
                        {children}
                      </h4>
                    );
                  },
                  p({ children }) {
                    return <p className="text-sm text-zinc-400 leading-relaxed mb-3">{children}</p>;
                  },
                  strong({ children }) {
                    return <strong className="text-zinc-200 font-semibold">{children}</strong>;
                  },
                  em({ children }) {
                    return <em className="text-zinc-300">{children}</em>;
                  },
                  ul({ children }) {
                    return <ul className="list-disc pl-5 text-sm text-zinc-400 space-y-1 mb-3">{children}</ul>;
                  },
                  ol({ children }) {
                    return <ol className="list-decimal pl-5 text-sm text-zinc-400 space-y-1 mb-3">{children}</ol>;
                  },
                  li({ children }) {
                    return <li className="leading-relaxed">{children}</li>;
                  },
                  hr() {
                    return <hr className="my-6 border-border-dim" />;
                  },
                  a({ href, children }) {
                    return <a href={href} className="text-accent hover:underline">{children}</a>;
                  },
                }}
              >
                {content}
              </Markdown>
            </article>
          )}
        </div>
      </div>
    </div>
  );
}
