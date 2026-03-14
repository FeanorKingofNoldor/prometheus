import { useState } from "react";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import {
  useSystemLogs,
  useLogCategories,
  useActivity,
  useEngineNames,
} from "../../api/hooks";

type Source = "backend" | "engine_decisions" | "regime_transitions" | "risk_actions";

const SOURCES: { key: Source; label: string }[] = [
  { key: "engine_decisions", label: "Engine Decisions" },
  { key: "regime_transitions", label: "Regime Changes" },
  { key: "risk_actions", label: "Risk Actions" },
  { key: "backend", label: "Backend Logs" },
];

const LEVEL_OPTIONS = ["ALL", "DEBUG", "INFO", "WARNING", "ERROR"] as const;

const levelVariant = (lvl: string) => {
  switch (lvl) {
    case "ERROR": return "negative" as const;
    case "WARNING": return "warning" as const;
    case "INFO": return "info" as const;
    default: return "neutral" as const;
  }
};

export default function SystemLogsTab() {
  const [source, setSource] = useState<Source>("engine_decisions");
  const [level, setLevel] = useState("ALL");
  const [category, setCategory] = useState("");
  const [engine, setEngine] = useState("");
  const [search, setSearch] = useState("");

  // Backend log buffer (only fetch when source=backend)
  const backendLogs = useSystemLogs(
    source === "backend"
      ? { level: level === "ALL" ? undefined : level, category: category || undefined, search: search || undefined, limit: 500 }
      : undefined,
  );
  const categories = useLogCategories();
  const catList = (Array.isArray(categories.data) ? categories.data : []) as string[];

  // DB activity (only fetch when source!=backend)
  const activity = useActivity(
    source !== "backend"
      ? { source, engine: engine || undefined, search: search || undefined, limit: 500 }
      : undefined,
  );
  const engineNames = useEngineNames();
  const engineList = (Array.isArray(engineNames.data) ? engineNames.data : []) as string[];

  const backendEntries = (Array.isArray(backendLogs.data) ? backendLogs.data : []) as Array<{
    timestamp: string; level: string; category: string; source: string; message: string;
  }>;
  const activityEntries = (Array.isArray(activity.data) ? activity.data : []) as Array<Record<string, string>>;

  const entryCount = source === "backend" ? backendEntries.length : activityEntries.length;
  const refreshRate = source === "backend" ? "5 s" : "15 s";

  return (
    <div className="space-y-3">
      {/* Source tabs */}
      <div className="flex gap-1 border-b border-border-dim">
        {SOURCES.map(({ key, label }) => (
          <button
            key={key}
            onClick={() => { setSource(key); setSearch(""); setEngine(""); setLevel("ALL"); setCategory(""); }}
            className={`px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              source === key
                ? "border-accent text-accent"
                : "border-transparent text-muted hover:text-zinc-200"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2">
        {source === "backend" && (
          <>
            <select
              value={level}
              onChange={(e) => setLevel(e.target.value)}
              className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
            >
              {LEVEL_OPTIONS.map((l) => <option key={l} value={l}>{l}</option>)}
            </select>
            <select
              value={category}
              onChange={(e) => setCategory(e.target.value)}
              className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
            >
              <option value="">All categories</option>
              {catList.map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </>
        )}

        {source === "engine_decisions" && (
          <select
            value={engine}
            onChange={(e) => setEngine(e.target.value)}
            className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
          >
            <option value="">All engines</option>
            {engineList.map((e) => <option key={e} value={e}>{e}</option>)}
          </select>
        )}

        <input
          type="text"
          placeholder="Search…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200 placeholder:text-muted w-48"
        />

        <span className="ml-auto text-[10px] text-muted">
          {entryCount} entries · auto-refresh {refreshRate}
        </span>
      </div>

      {/* Content */}
      <Panel>
        <div className="max-h-[calc(100vh-290px)] overflow-auto">
          {source === "backend" ? (
            <BackendLogTable entries={backendEntries} level={level} />
          ) : source === "engine_decisions" ? (
            <EngineDecisionTable entries={activityEntries} />
          ) : source === "regime_transitions" ? (
            <RegimeTable entries={activityEntries} />
          ) : (
            <RiskActionTable entries={activityEntries} />
          )}
        </div>
      </Panel>
    </div>
  );
}


/* ── Backend logs table ──────────────────────────────────────────── */

function BackendLogTable({ entries, level }: {
  entries: Array<{ timestamp: string; level: string; category: string; source: string; message: string }>;
  level: string;
}) {
  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 bg-surface-raised">
        <tr className="border-b border-border-dim text-left text-muted">
          <th className="px-2 py-1.5 w-20">Time</th>
          <th className="px-2 py-1.5 w-16">Level</th>
          <th className="px-2 py-1.5 w-32">Category</th>
          <th className="px-2 py-1.5">Message</th>
        </tr>
      </thead>
      <tbody>
        {entries.length === 0 && (
          <tr><td colSpan={4} className="px-2 py-8 text-center text-muted">
            No log entries{level !== "ALL" ? ` at ${level} level` : ""}
          </td></tr>
        )}
        {entries.map((e, i) => (
          <tr
            key={i}
            className={`border-b border-border-dim/50 hover:bg-surface-overlay/40 ${
              e.level === "ERROR" ? "bg-negative/5" : e.level === "WARNING" ? "bg-warning/5" : ""
            }`}
          >
            <td className="px-2 py-1 font-mono text-muted whitespace-nowrap">{e.timestamp?.slice(11, 19) || "—"}</td>
            <td className="px-2 py-1"><StatusBadge label={e.level} variant={levelVariant(e.level)} /></td>
            <td className="px-2 py-1 text-muted truncate max-w-[200px]" title={e.category}>{e.category}</td>
            <td className="px-2 py-1 text-zinc-300 truncate max-w-[500px]" title={e.message}>{e.message}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}


/* ── Engine decisions table ──────────────────────────────────────── */

function EngineDecisionTable({ entries }: { entries: Array<Record<string, string>> }) {
  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 bg-surface-raised">
        <tr className="border-b border-border-dim text-left text-muted">
          <th className="px-2 py-1.5 w-20">Time</th>
          <th className="px-2 py-1.5 w-36">Engine</th>
          <th className="px-2 py-1.5 w-20">Market</th>
          <th className="px-2 py-1.5 w-28">Strategy</th>
          <th className="px-2 py-1.5 w-24">Date</th>
          <th className="px-2 py-1.5">Run ID</th>
        </tr>
      </thead>
      <tbody>
        {entries.length === 0 && (
          <tr><td colSpan={6} className="px-2 py-8 text-center text-muted">No engine decisions found</td></tr>
        )}
        {entries.map((e, i) => (
          <tr key={i} className="border-b border-border-dim/50 hover:bg-surface-overlay/40">
            <td className="px-2 py-1 font-mono text-muted whitespace-nowrap">{e.timestamp?.slice(11, 19) || "—"}</td>
            <td className="px-2 py-1">
              <StatusBadge label={e.engine || "—"} variant="info" />
            </td>
            <td className="px-2 py-1 text-zinc-300">{e.market || "—"}</td>
            <td className="px-2 py-1 text-muted truncate max-w-[180px]" title={e.strategy}>{e.strategy || "—"}</td>
            <td className="px-2 py-1 text-muted">{e.as_of_date || "—"}</td>
            <td className="px-2 py-1 font-mono text-zinc-500 truncate max-w-[160px]" title={e.run_id}>{e.run_id?.slice(0, 8) || "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}


/* ── Regime transitions table ────────────────────────────────────── */

function RegimeTable({ entries }: { entries: Array<Record<string, string>> }) {
  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 bg-surface-raised">
        <tr className="border-b border-border-dim text-left text-muted">
          <th className="px-2 py-1.5 w-20">Time</th>
          <th className="px-2 py-1.5 w-16">Region</th>
          <th className="px-2 py-1.5 w-32">From</th>
          <th className="px-2 py-1.5">→</th>
          <th className="px-2 py-1.5 w-32">To</th>
          <th className="px-2 py-1.5 w-24">Date</th>
        </tr>
      </thead>
      <tbody>
        {entries.length === 0 && (
          <tr><td colSpan={6} className="px-2 py-8 text-center text-muted">No regime transitions found</td></tr>
        )}
        {entries.map((e, i) => (
          <tr key={i} className="border-b border-border-dim/50 hover:bg-surface-overlay/40">
            <td className="px-2 py-1 font-mono text-muted whitespace-nowrap">{e.timestamp?.slice(11, 19) || "—"}</td>
            <td className="px-2 py-1 text-zinc-300">{e.region || "—"}</td>
            <td className="px-2 py-1"><StatusBadge label={e.from_regime || "—"} variant="neutral" /></td>
            <td className="px-2 py-1 text-accent">→</td>
            <td className="px-2 py-1"><StatusBadge label={e.to_regime || "—"} variant="warning" /></td>
            <td className="px-2 py-1 text-muted">{e.as_of_date || "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}


/* ── Risk actions table ──────────────────────────────────────────── */

function RiskActionTable({ entries }: { entries: Array<Record<string, string>> }) {
  return (
    <table className="w-full text-xs">
      <thead className="sticky top-0 bg-surface-raised">
        <tr className="border-b border-border-dim text-left text-muted">
          <th className="px-2 py-1.5 w-20">Time</th>
          <th className="px-2 py-1.5 w-28">Action Type</th>
          <th className="px-2 py-1.5 w-24">Instrument</th>
          <th className="px-2 py-1.5">Strategy</th>
        </tr>
      </thead>
      <tbody>
        {entries.length === 0 && (
          <tr><td colSpan={4} className="px-2 py-8 text-center text-muted">No risk actions found</td></tr>
        )}
        {entries.map((e, i) => (
          <tr key={i} className="border-b border-border-dim/50 hover:bg-surface-overlay/40">
            <td className="px-2 py-1 font-mono text-muted whitespace-nowrap">{e.timestamp?.slice(11, 19) || "—"}</td>
            <td className="px-2 py-1"><StatusBadge label={e.action_type || "—"} variant="warning" /></td>
            <td className="px-2 py-1 text-zinc-300">{e.instrument || "—"}</td>
            <td className="px-2 py-1 text-muted">{e.strategy || "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
