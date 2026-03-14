import { useState } from "react";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import { useReports, useReport, useGenerateReport } from "../../api/hooks";
import { FileText, Loader2, Sparkles, AlertTriangle, CheckCircle, Calendar } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

interface ReportSummary {
  id: string;
  report_type: string;
  generated_at: string;
  as_of_date: string;
  title: string;
  summary: string;
}

interface ReportFull extends ReportSummary {
  content: string;
  metadata: Record<string, unknown>;
}

const TYPE_LABELS: Record<string, string> = {
  log_daily: "24H",
  log_weekly: "WEEKLY",
  log_custom: "CUSTOM",
  // Legacy types
  daily_evening: "DAILY",
  weekly_sunday: "WEEKLY",
};

const TYPE_VARIANTS: Record<string, "info" | "positive" | "warning" | "neutral"> = {
  log_daily: "info",
  log_weekly: "positive",
  log_custom: "warning",
  daily_evening: "info",
  weekly_sunday: "positive",
};

export default function ReportsTab() {
  const [typeFilter, setTypeFilter] = useState("");
  const [selectedId, setSelectedId] = useState("");
  const [customStart, setCustomStart] = useState("");
  const [customEnd, setCustomEnd] = useState("");

  const reports = useReports(typeFilter || undefined);
  const detail = useReport(selectedId);
  const generate = useGenerateReport();

  const list = (Array.isArray(reports.data) ? reports.data : []) as ReportSummary[];
  const full = detail.data as ReportFull | undefined;

  return (
    <div className="space-y-3">
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-2">
        <select
          value={typeFilter}
          onChange={(e) => setTypeFilter(e.target.value)}
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
        >
          <option value="">All types</option>
          <option value="log_daily">24h Health</option>
          <option value="log_weekly">Weekly Health</option>
          <option value="log_custom">Custom Range</option>
        </select>

        <div className="ml-auto flex flex-wrap items-center gap-2">
          {/* Custom date range inputs */}
          <div className="flex items-center gap-1">
            <Calendar size={11} className="text-muted" />
            <input
              type="date"
              value={customStart}
              onChange={(e) => setCustomStart(e.target.value)}
              className="rounded border border-border-dim bg-surface-overlay px-1.5 py-0.5 text-[11px] text-zinc-200"
            />
            <span className="text-[10px] text-muted">→</span>
            <input
              type="date"
              value={customEnd}
              onChange={(e) => setCustomEnd(e.target.value)}
              className="rounded border border-border-dim bg-surface-overlay px-1.5 py-0.5 text-[11px] text-zinc-200"
            />
          </div>

          <button
            onClick={() => generate.mutate({ report_type: "log_daily" })}
            disabled={generate.isPending}
            className="flex items-center gap-1.5 rounded bg-accent/20 px-3 py-1 text-xs font-medium text-accent hover:bg-accent/30 disabled:opacity-50"
          >
            {generate.isPending ? <Loader2 size={12} className="animate-spin" /> : <Sparkles size={12} />}
            24h Report
          </button>
          <button
            onClick={() => generate.mutate({ report_type: "log_weekly" })}
            disabled={generate.isPending}
            className="flex items-center gap-1.5 rounded bg-info/20 px-3 py-1 text-xs font-medium text-info hover:bg-info/30 disabled:opacity-50"
          >
            {generate.isPending ? <Loader2 size={12} className="animate-spin" /> : <Sparkles size={12} />}
            Weekly Report
          </button>
          <button
            onClick={() => generate.mutate({
              report_type: "log_custom",
              start_date: customStart || undefined,
              end_date: customEnd || undefined,
            })}
            disabled={generate.isPending || !customStart}
            className="flex items-center gap-1.5 rounded bg-warning/20 px-3 py-1 text-xs font-medium text-warning hover:bg-warning/30 disabled:opacity-50"
          >
            {generate.isPending ? <Loader2 size={12} className="animate-spin" /> : <Sparkles size={12} />}
            Custom
          </button>
        </div>
      </div>

      {/* Status messages */}
      {generate.isError && (
        <div className="flex items-center gap-2 rounded bg-negative/10 border border-negative/30 px-3 py-2 text-xs text-negative">
          <AlertTriangle size={12} />
          Report generation failed: {(generate.error as Error)?.message || "Unknown error"}
        </div>
      )}
      {generate.isSuccess && (
        <div className="flex items-center gap-2 rounded bg-positive/10 border border-positive/30 px-3 py-2 text-xs text-positive">
          <CheckCircle size={12} />
          Report generated successfully
        </div>
      )}

      <div className="grid gap-3 lg:grid-cols-[1fr_1.5fr]">
        {/* Report list */}
        <Panel title="Health Reports">
          <div className="max-h-[calc(100vh-280px)] overflow-auto space-y-1">
            {list.length === 0 && (
              <p className="py-8 text-center text-xs text-muted">
                No reports yet — click a Generate button to create one
              </p>
            )}
            {list.map((r) => (
              <button
                key={r.id}
                onClick={() => setSelectedId(r.id)}
                className={`w-full text-left rounded px-3 py-2 transition-colors ${
                  selectedId === r.id
                    ? "bg-accent/10 border border-accent/30"
                    : "hover:bg-surface-overlay/60 border border-transparent"
                }`}
              >
                <div className="flex items-center gap-2">
                  <FileText size={12} className="text-muted shrink-0" />
                  <span className="text-xs font-medium text-zinc-200 truncate">
                    {r.title || "Untitled Report"}
                  </span>
                </div>
                <div className="mt-1 flex items-center gap-2">
                  <StatusBadge
                    label={TYPE_LABELS[r.report_type] ?? r.report_type}
                    variant={TYPE_VARIANTS[r.report_type] ?? "neutral"}
                  />
                  <span className="text-[10px] text-muted">{r.as_of_date}</span>
                </div>
                {r.summary && (
                  <p className="mt-1 text-[11px] text-muted line-clamp-2">{r.summary}</p>
                )}
              </button>
            ))}
          </div>
        </Panel>

        {/* Reader pane */}
        <Panel title={full?.title || "Select a report"}>
          <div className="max-h-[calc(100vh-280px)] overflow-auto">
            {!selectedId && (
              <p className="py-12 text-center text-xs text-muted">
                Select a report from the list to read it
              </p>
            )}
            {selectedId && detail.isLoading && (
              <div className="flex items-center justify-center py-12">
                <Loader2 size={16} className="animate-spin text-muted" />
              </div>
            )}
            {full && (
              <div className="text-xs leading-relaxed text-zinc-300">
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={markdownComponents}
                >
                  {full.content}
                </ReactMarkdown>
              </div>
            )}
          </div>
        </Panel>
      </div>
    </div>
  );
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
