import { useState, useRef, useEffect } from "react";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import { useDaemonLogs, useDaemonCategories } from "../../api/hooks";
import { Pause, Play, ArrowDown, Search } from "lucide-react";

interface DaemonLogEntry {
  timestamp: string;
  level: string;
  category: string;
  source: string;
  message: string;
}

interface DaemonLogsResponse {
  entries: DaemonLogEntry[];
  file_size: number;
  log_path: string;
  available: boolean;
}

const LEVEL_OPTIONS = ["ALL", "DEBUG", "INFO", "WARNING", "ERROR"] as const;

const levelVariant = (lvl: string) => {
  switch (lvl) {
    case "ERROR":
    case "CRITICAL":
      return "negative" as const;
    case "WARNING":
      return "warning" as const;
    case "INFO":
      return "info" as const;
    default:
      return "neutral" as const;
  }
};

const levelColor = (lvl: string) => {
  switch (lvl) {
    case "ERROR":
    case "CRITICAL":
      return "text-red-400";
    case "WARNING":
      return "text-amber-400";
    case "INFO":
      return "text-sky-400";
    default:
      return "text-zinc-500";
  }
};

// Highlight important keywords in messages
function highlightMessage(msg: string) {
  // Don't render HTML — just apply category-based coloring
  return msg;
}

// Group categories into logical sections for the filter
const CATEGORY_GROUPS: Record<string, string[]> = {
  Pipeline: ["pipeline.tasks", "pipeline.state", "orchestration.dag", "orchestration.market_aware_daemon"],
  Execution: ["execution.", "scripts.run", "decisions.tracker"],
  Intel: ["ingest.social", "ingest.gdelt", "ingest.scheduler", "intel.living_profile", "graph.core"],
  Data: ["core.database", "core.time"],
  Options: ["run_derivatives", "options_strategy"],
};

function getCategoryGroup(cat: string): string {
  for (const [group, patterns] of Object.entries(CATEGORY_GROUPS)) {
    if (patterns.some((p) => cat.includes(p))) return group;
  }
  return "Other";
}

export default function DaemonTab() {
  const [level, setLevel] = useState("ALL");
  const [category, setCategory] = useState("");
  const [search, setSearch] = useState("");
  const [paused, setPaused] = useState(false);
  const [autoScroll, setAutoScroll] = useState(true);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const scrollRef = useRef<HTMLDivElement>(null);

  const logs = useDaemonLogs({
    level: level === "ALL" ? undefined : level,
    category: category || undefined,
    search: search || undefined,
    limit: 1000,
    enabled: !paused,
  });

  const categories = useDaemonCategories();
  const catList = (Array.isArray(categories.data) ? categories.data : []) as string[];

  const data = logs.data as DaemonLogsResponse | undefined;
  const entries = data?.entries ?? [];
  const available = data?.available ?? false;
  const fileSize = data?.file_size ?? 0;

  // Auto-scroll to bottom when new entries arrive
  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [entries.length, autoScroll]);

  const toggleExpand = (idx: number) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  };

  // Count by level
  const errorCount = entries.filter((e) => e.level === "ERROR" || e.level === "CRITICAL").length;
  const warnCount = entries.filter((e) => e.level === "WARNING").length;

  return (
    <div className="space-y-3">
      {/* Status bar */}
      <div className="flex items-center gap-3 text-xs">
        <div className="flex items-center gap-1.5">
          <div className={`w-2 h-2 rounded-full ${available ? "bg-positive animate-pulse" : "bg-zinc-600"}`} />
          <span className="text-muted">
            {available ? "Daemon log active" : "No daemon log file found"}
          </span>
        </div>
        {available && (
          <>
            <span className="text-zinc-600">|</span>
            <span className="text-muted">{(fileSize / 1024).toFixed(0)} KB</span>
            <span className="text-zinc-600">|</span>
            <span className="text-muted">{entries.length} entries</span>
            {errorCount > 0 && (
              <>
                <span className="text-zinc-600">|</span>
                <span className="text-red-400">{errorCount} errors</span>
              </>
            )}
            {warnCount > 0 && (
              <>
                <span className="text-zinc-600">|</span>
                <span className="text-amber-400">{warnCount} warnings</span>
              </>
            )}
          </>
        )}
      </div>

      {/* Filters & controls */}
      <div className="flex flex-wrap items-center gap-2">
        <select
          value={level}
          onChange={(e) => setLevel(e.target.value)}
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
        >
          {LEVEL_OPTIONS.map((l) => (
            <option key={l} value={l}>{l}</option>
          ))}
        </select>

        <select
          value={category}
          onChange={(e) => setCategory(e.target.value)}
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
        >
          <option value="">All categories</option>
          {catList.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>

        <div className="relative">
          <Search size={11} className="absolute left-2 top-1/2 -translate-y-1/2 text-muted" />
          <input
            type="text"
            placeholder="Search messages..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="rounded border border-border-dim bg-surface-overlay pl-6 pr-2 py-1 text-xs text-zinc-200 placeholder:text-muted w-52"
          />
        </div>

        <div className="ml-auto flex items-center gap-1.5">
          <button
            onClick={() => setPaused(!paused)}
            className={`flex items-center gap-1 rounded px-2 py-1 text-xs font-medium transition-colors ${
              paused
                ? "bg-amber-500/20 text-amber-400 border border-amber-500/30"
                : "bg-surface-overlay text-muted border border-border-dim hover:text-zinc-200"
            }`}
            title={paused ? "Resume auto-refresh" : "Pause auto-refresh"}
          >
            {paused ? <Play size={11} /> : <Pause size={11} />}
            {paused ? "Resume" : "Pause"}
          </button>
          <button
            onClick={() => {
              setAutoScroll(true);
              if (scrollRef.current) {
                scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
              }
            }}
            className="flex items-center gap-1 rounded px-2 py-1 text-xs text-muted border border-border-dim bg-surface-overlay hover:text-zinc-200 transition-colors"
            title="Scroll to bottom"
          >
            <ArrowDown size={11} />
          </button>
        </div>
      </div>

      {/* Log content */}
      <Panel>
        <div
          ref={scrollRef}
          className="max-h-[calc(100vh-280px)] overflow-auto font-mono text-[11px] leading-relaxed"
          onScroll={(e) => {
            const el = e.currentTarget;
            const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 50;
            setAutoScroll(atBottom);
          }}
        >
          {!available ? (
            <div className="py-12 text-center text-muted text-xs">
              <p className="mb-1">No daemon log file found</p>
              <p className="text-zinc-600">
                Restart with <code className="bg-surface-overlay px-1.5 py-0.5 rounded">./start.sh</code> to begin capturing daemon logs
              </p>
            </div>
          ) : entries.length === 0 ? (
            <div className="py-12 text-center text-muted text-xs">
              No log entries match filters
            </div>
          ) : (
            <table className="w-full">
              <tbody>
                {/* Entries are newest-first from API, display oldest-first (chronological) for tailing */}
                {[...entries].reverse().map((e, i) => {
                  const isMultiline = e.message.includes("\n");
                  const isExpanded = expanded.has(i);
                  const displayMsg = isMultiline && !isExpanded
                    ? e.message.split("\n")[0] + " ..."
                    : e.message;

                  return (
                    <tr
                      key={i}
                      onClick={() => isMultiline && toggleExpand(i)}
                      className={`border-b border-border-dim/30 ${
                        e.level === "ERROR" || e.level === "CRITICAL"
                          ? "bg-red-500/5"
                          : e.level === "WARNING"
                            ? "bg-amber-500/5"
                            : ""
                      } ${isMultiline ? "cursor-pointer" : ""} hover:bg-surface-overlay/40`}
                    >
                      <td className="px-1.5 py-0.5 text-zinc-600 whitespace-nowrap align-top w-[130px]">
                        {e.timestamp}
                      </td>
                      <td className={`px-1 py-0.5 whitespace-nowrap align-top w-[52px] ${levelColor(e.level)}`}>
                        {e.level.slice(0, 4)}
                      </td>
                      <td className="px-1 py-0.5 text-zinc-500 whitespace-nowrap align-top w-[160px] truncate max-w-[160px]" title={e.source}>
                        {e.category}
                      </td>
                      <td className={`px-1.5 py-0.5 text-zinc-300 ${isExpanded ? "whitespace-pre-wrap" : "truncate max-w-[600px]"}`} title={!isExpanded ? e.message : undefined}>
                        {displayMsg}
                        {isMultiline && !isExpanded && (
                          <span className="text-zinc-600 ml-1 text-[10px]">[+{e.message.split("\n").length - 1} lines]</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </Panel>
    </div>
  );
}
