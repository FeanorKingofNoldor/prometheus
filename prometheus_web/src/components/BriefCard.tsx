import { useState } from "react";
import { ChevronDown, ChevronUp, Eye, ExternalLink, X } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { SeverityBadge } from "./SeverityBadge";

export interface IntelBrief {
  id: string;
  brief_type: string;
  severity: string;
  domain: string;
  title: string;
  summary: string;
  content: string;
  entities: unknown[];  // may be strings or {type, id, name} objects
  predictions: unknown[];
  sources: unknown[];
  created_at: string;
  expires_at: string | null;
  is_read: boolean;
}

/** Safely coerce an unknown value to a display string. */
function toLabel(v: unknown): string {
  if (typeof v === "string") return v;
  if (v && typeof v === "object" && "name" in v) return String((v as Record<string, unknown>).name);
  if (v && typeof v === "object" && "id" in v) return String((v as Record<string, unknown>).id);
  return String(v);
}

/** Normalise a JSONB field that may arrive as null, an object, or an array. */
function asArray(v: unknown): unknown[] {
  if (Array.isArray(v)) return v;
  return [];
}

interface BriefCardProps {
  brief: IntelBrief;
  onMarkRead?: (id: string) => void;
  onDismiss?: (id: string) => void;
  showTimestamp?: boolean;
}

const domainColors: Record<string, string> = {
  nation: "text-blue-400",
  conflict: "text-red-400",
  maritime: "text-cyan-400",
  trade: "text-amber-400",
  synthesis: "text-violet-400",
};

const typeLabels: Record<string, string> = {
  flash_alert: "FLASH",
  daily_sitrep: "SITREP",
  weekly_assessment: "WEEKLY",
  domain_report: "DOMAIN",
};

export function BriefCard({ brief, onMarkRead, onDismiss, showTimestamp }: BriefCardProps) {
  const [expanded, setExpanded] = useState(false);

  const age = timeSince(brief.created_at);
  const fullTs = new Date(brief.created_at).toLocaleString();
  const domainCls = domainColors[brief.domain] ?? "text-zinc-400";

  return (
    <div
      className={`relative rounded-lg border transition-colors ${
        brief.is_read
          ? "border-border-dim bg-surface-raised/60"
          : "border-border-dim bg-surface-raised ring-1 ring-accent/20"
      }`}
    >
      {/* Dismiss button */}
      {onDismiss && (
        <button
          className="absolute right-2 top-2 z-10 rounded p-0.5 text-muted hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
          title="Dismiss from All Briefs"
          onClick={(e) => { e.stopPropagation(); onDismiss(brief.id); }}
        >
          <X size={14} />
        </button>
      )}

      {/* Header */}
      <button
        className="flex w-full items-center gap-3 px-4 py-3 text-left"
        onClick={() => {
          setExpanded((v) => !v);
          if (!brief.is_read && onMarkRead) onMarkRead(brief.id);
        }}
      >
        <SeverityBadge severity={brief.severity} />

        <span className={`text-[10px] font-bold uppercase tracking-wider ${domainCls}`}>
          {typeLabels[brief.brief_type] ?? brief.brief_type}
        </span>

        <span className="flex-1 truncate text-xs font-medium text-zinc-100">
          {brief.title}
        </span>

        <span className="shrink-0 text-[10px] text-muted" title={fullTs}>
          {showTimestamp ? fullTs : age}
        </span>

        {!brief.is_read && (
          <span className="h-2 w-2 shrink-0 rounded-full bg-accent" title="Unread" />
        )}

        {expanded ? <ChevronUp size={14} className="text-muted" /> : <ChevronDown size={14} className="text-muted" />}
      </button>

      {/* Summary (always visible) */}
      {!expanded && (
        <div className="px-4 pb-3">
          <p className="text-xs leading-relaxed text-zinc-300">{brief.summary}</p>
        </div>
      )}

      {/* Expanded content */}
      {expanded && (
        <div className="border-t border-border-dim px-4 py-3 space-y-3">
          {/* Full content — rich markdown rendering */}
          <div className="brief-markdown text-xs leading-relaxed text-zinc-300">
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={markdownComponents}
            >
              {brief.content}
            </ReactMarkdown>
          </div>

          {/* Entities */}
          {asArray(brief.entities).length > 0 && (
            <div>
              <span className="text-[10px] font-semibold uppercase text-muted">Entities</span>
              <div className="mt-1 flex flex-wrap gap-1.5">
                {asArray(brief.entities).map((e, i) => (
                  <span
                    key={i}
                    className="rounded-full bg-surface-overlay px-2.5 py-0.5 text-[10px] font-medium text-zinc-300 border border-border-dim"
                  >
                    {toLabel(e)}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Predictions */}
          {asArray(brief.predictions).length > 0 && (
            <div>
              <span className="text-[10px] font-semibold uppercase text-muted">Predictions</span>
              <ul className="mt-1 space-y-0.5">
                {asArray(brief.predictions).map((p, i) => (
                  <li key={i} className="text-xs text-zinc-400">
                    • {toLabel(p)}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Sources */}
          {asArray(brief.sources).length > 0 && (
            <div>
              <span className="text-[10px] font-semibold uppercase text-muted">Sources</span>
              <div className="mt-1 flex flex-wrap gap-1.5">
                {asArray(brief.sources).map((s, i) => {
                  const src = s as Record<string, unknown>;
                  const url = typeof src?.url === "string" ? src.url : null;
                  const label = typeof src?.outlet === "string" && src.outlet
                    ? src.outlet
                    : toLabel(s);
                  return url ? (
                    <a
                      key={i}
                      href={url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-0.5 text-[10px] text-accent hover:underline"
                    >
                      {label} <ExternalLink size={8} />
                    </a>
                  ) : (
                    <span key={i} className="text-[10px] text-muted">
                      [{label}]
                    </span>
                  );
                })}
              </div>
            </div>
          )}

          {/* Mark read */}
          {!brief.is_read && onMarkRead && (
            <button
              className="flex items-center gap-1 rounded bg-accent/10 px-2 py-1 text-[10px] font-semibold text-accent hover:bg-accent/20"
              onClick={(e) => { e.stopPropagation(); onMarkRead(brief.id); }}
            >
              <Eye size={12} /> Mark Read
            </button>
          )}
        </div>
      )}
    </div>
  );
}

/** Human-friendly time-since string. */
function timeSince(iso: string): string {
  const seconds = (Date.now() - new Date(iso).getTime()) / 1000;
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

/** Tailwind-styled component overrides for ReactMarkdown. */
const markdownComponents = {
  h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
    <h2 {...props} className="mt-3 text-sm font-bold text-zinc-100">{children}</h2>
  ),
  h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
    <h3 {...props} className="mt-2 text-xs font-bold text-zinc-100 uppercase tracking-wide">{children}</h3>
  ),
  h3: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
    <h4 {...props} className="mt-1.5 text-xs font-semibold text-zinc-200">{children}</h4>
  ),
  p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
    <p {...props} className="my-1">{children}</p>
  ),
  ul: ({ children, ...props }: React.HTMLAttributes<HTMLUListElement>) => (
    <ul {...props} className="my-1 ml-4 list-disc space-y-0.5 marker:text-muted">{children}</ul>
  ),
  ol: ({ children, ...props }: React.HTMLAttributes<HTMLOListElement>) => (
    <ol {...props} className="my-1 ml-4 list-decimal space-y-0.5 marker:text-muted">{children}</ol>
  ),
  li: ({ children, ...props }: React.HTMLAttributes<HTMLLIElement>) => (
    <li {...props} className="text-zinc-300">{children}</li>
  ),
  strong: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
    <strong {...props} className="font-semibold text-zinc-100">{children}</strong>
  ),
  a: ({ children, href, ...props }: React.AnchorHTMLAttributes<HTMLAnchorElement>) => (
    <a {...props} href={href} target="_blank" rel="noopener noreferrer" className="text-accent hover:underline">
      {children}
    </a>
  ),
  blockquote: ({ children, ...props }: React.HTMLAttributes<HTMLQuoteElement>) => (
    <blockquote {...props} className="my-1 border-l-2 border-accent/40 pl-3 text-zinc-400 italic">{children}</blockquote>
  ),
  table: ({ children, ...props }: React.HTMLAttributes<HTMLTableElement>) => (
    <div className="my-2 overflow-x-auto">
      <table {...props} className="w-full text-[11px] border-collapse">{children}</table>
    </div>
  ),
  thead: ({ children, ...props }: React.HTMLAttributes<HTMLTableSectionElement>) => (
    <thead {...props} className="border-b border-border-dim text-left text-muted">{children}</thead>
  ),
  th: ({ children, ...props }: React.HTMLAttributes<HTMLTableCellElement>) => (
    <th {...props} className="px-2 py-1 font-semibold">{children}</th>
  ),
  td: ({ children, ...props }: React.HTMLAttributes<HTMLTableCellElement>) => (
    <td {...props} className="px-2 py-1 border-t border-border-dim/50">{children}</td>
  ),
  hr: (props: React.HTMLAttributes<HTMLHRElement>) => (
    <hr {...props} className="my-2 border-border-dim" />
  ),
};
