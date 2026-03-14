import { useState } from "react";
import { Panel } from "../../components/Panel";
import { StatusBadge } from "../../components/StatusBadge";
import { KpiCard } from "../../components/KpiCard";
import { useEngineRuns, usePipelines, useRunDetail } from "../../api/hooks";
import { ChevronRight, X, Clock, Shield, AlertTriangle } from "lucide-react";

interface EngineRun {
  run_id: string;
  region: string;
  phase: string;
  as_of_date: string | null;
  created_at: string | null;
  updated_at: string | null;
  error: Record<string, unknown> | null;
}

interface PipelineStatus {
  market_id: string;
  status: string;
  [k: string]: unknown;
}

interface RunDetail {
  run_id: string;
  region: string;
  phase: string;
  as_of_date: string | null;
  created_at: string | null;
  updated_at: string | null;
  phase_started_at: string | null;
  phase_completed_at: string | null;
  duration_seconds: number | null;
  config_json: Record<string, unknown> | null;
  live_safe: boolean | null;
  error: Record<string, unknown> | null;
  decisions: Array<{
    decision_id: string;
    engine_name: string;
    strategy_id: string;
    market_id: string;
    as_of_date: string;
    config_id: string;
    created_at: string;
  }>;
}

const phaseVariant = (p: string) => {
  switch (p) {
    case "COMPLETED": case "BOOKS_DONE": return "positive" as const;
    case "FAILED": return "negative" as const;
    case "RUNNING": return "info" as const;
    default: return "neutral" as const;
  }
};

export default function PipelineRunsTab() {
  const [status, setStatus] = useState("");
  const [region, setRegion] = useState("");
  const [selectedRunId, setSelectedRunId] = useState("");

  const runs = useEngineRuns({ status: status || undefined, region: region || undefined, limit: 100 });
  const pipelines = usePipelines();
  const detail = useRunDetail(selectedRunId);

  const runList = (Array.isArray(runs.data) ? runs.data : []) as EngineRun[];
  const pipeList = (Array.isArray(pipelines.data) ? pipelines.data : []) as PipelineStatus[];
  const runDetail = detail.data as RunDetail | undefined;

  const completed = runList.filter((r) => r.phase === "COMPLETED" || r.phase === "BOOKS_DONE").length;
  const failed = runList.filter((r) => r.phase === "FAILED").length;

  return (
    <div className="space-y-3">
      {/* Pipeline status cards */}
      {pipeList.length > 0 && (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          {pipeList.map((p) => (
            <KpiCard
              key={p.market_id}
              label={p.market_id}
              value={p.status}
              sentiment={
                p.status === "IDLE" || p.status === "COMPLETED"
                  ? "positive" : p.status === "RUNNING" ? "neutral" : "warning"
              }
            />
          ))}
        </div>
      )}

      {/* Summary KPIs */}
      <div className="grid grid-cols-3 gap-2">
        <KpiCard label="Total Runs" value={String(runList.length)} />
        <KpiCard label="Completed" value={String(completed)} sentiment="positive" />
        <KpiCard label="Failed" value={String(failed)} sentiment={failed > 0 ? "warning" : "positive"} />
      </div>

      {/* Filters */}
      <div className="flex items-center gap-2">
        <select
          value={status}
          onChange={(e) => setStatus(e.target.value)}
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
        >
          <option value="">All phases</option>
          <option value="BOOKS_DONE">BOOKS_DONE</option>
          <option value="COMPLETED">COMPLETED</option>
          <option value="FAILED">FAILED</option>
          <option value="RUNNING">RUNNING</option>
        </select>
        <select
          value={region}
          onChange={(e) => setRegion(e.target.value)}
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-200"
        >
          <option value="">All regions</option>
          <option value="US">US</option>
          <option value="EU">EU</option>
          <option value="APAC">APAC</option>
        </select>
      </div>

      <div className={`grid gap-3 ${selectedRunId ? "lg:grid-cols-[1fr_1.2fr]" : ""}`}>
        {/* Runs table */}
        <Panel title="Engine Runs">
          <div className="max-h-[calc(100vh-400px)] overflow-auto">
            <table className="w-full text-xs">
              <thead className="sticky top-0 bg-surface-raised">
                <tr className="border-b border-border-dim text-left text-muted">
                  <th className="px-2 py-1.5">Run ID</th>
                  <th className="px-2 py-1.5 w-16">Region</th>
                  <th className="px-2 py-1.5 w-24">Phase</th>
                  <th className="px-2 py-1.5 w-24">Date</th>
                  <th className="px-2 py-1.5 w-20">Started</th>
                  <th className="px-2 py-1.5 w-6"></th>
                </tr>
              </thead>
              <tbody>
                {runList.length === 0 && (
                  <tr><td colSpan={6} className="px-2 py-8 text-center text-muted">No engine runs found</td></tr>
                )}
                {runList.map((r) => (
                  <tr
                    key={r.run_id}
                    onClick={() => setSelectedRunId(r.run_id === selectedRunId ? "" : r.run_id)}
                    className={`border-b border-border-dim/50 cursor-pointer transition-colors ${
                      selectedRunId === r.run_id
                        ? "bg-accent/10"
                        : "hover:bg-surface-overlay/40"
                    }`}
                  >
                    <td className="px-2 py-1.5 font-mono text-zinc-400 truncate max-w-[150px]" title={r.run_id}>
                      {r.run_id.slice(0, 8)}…
                    </td>
                    <td className="px-2 py-1.5">{r.region}</td>
                    <td className="px-2 py-1.5">
                      <StatusBadge label={r.phase} variant={phaseVariant(r.phase)} />
                    </td>
                    <td className="px-2 py-1.5 text-muted">{r.as_of_date || "—"}</td>
                    <td className="px-2 py-1.5 font-mono text-muted whitespace-nowrap">
                      {r.created_at?.slice(11, 19) || "—"}
                    </td>
                    <td className="px-2 py-1.5 text-muted">
                      <ChevronRight size={12} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        {/* Detail panel */}
        {selectedRunId && (
          <Panel
            title={`Run ${selectedRunId.slice(0, 12)}…`}
            actions={
              <button onClick={() => setSelectedRunId("")} className="text-muted hover:text-zinc-200">
                <X size={14} />
              </button>
            }
          >
            {detail.isLoading ? (
              <p className="py-8 text-center text-xs text-muted">Loading…</p>
            ) : !runDetail ? (
              <p className="py-8 text-center text-xs text-muted">Run not found</p>
            ) : (
              <RunDetailView detail={runDetail} />
            )}
          </Panel>
        )}
      </div>
    </div>
  );
}


function RunDetailView({ detail }: { detail: RunDetail }) {
  const fmtDuration = (s: number | null) => {
    if (s == null) return "—";
    if (s < 60) return `${s.toFixed(1)}s`;
    return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
  };

  return (
    <div className="space-y-4 max-h-[calc(100vh-400px)] overflow-auto">
      {/* Overview */}
      <div className="grid grid-cols-2 gap-2 text-xs">
        <div>
          <span className="text-muted">Region:</span>{" "}
          <span className="text-zinc-200">{detail.region}</span>
        </div>
        <div>
          <span className="text-muted">Phase:</span>{" "}
          <StatusBadge label={detail.phase} variant={phaseVariant(detail.phase)} />
        </div>
        <div>
          <span className="text-muted">As-of Date:</span>{" "}
          <span className="text-zinc-200">{detail.as_of_date || "—"}</span>
        </div>
        <div className="flex items-center gap-1">
          <Shield size={11} className="text-muted" />
          <span className="text-muted">Live Safe:</span>{" "}
          <StatusBadge
            label={detail.live_safe ? "YES" : "NO"}
            variant={detail.live_safe ? "positive" : "negative"}
          />
        </div>
      </div>

      {/* Timing */}
      <div className="rounded bg-surface-overlay/50 px-3 py-2 text-xs space-y-1">
        <div className="flex items-center gap-1.5 text-muted font-medium">
          <Clock size={11} /> Timing
        </div>
        <div className="grid grid-cols-2 gap-1 text-zinc-300">
          <div>Created: <span className="font-mono">{detail.created_at?.slice(11, 19) || "—"}</span></div>
          <div>Duration: <span className="font-mono">{fmtDuration(detail.duration_seconds)}</span></div>
          <div>Phase Start: <span className="font-mono">{detail.phase_started_at?.slice(11, 19) || "—"}</span></div>
          <div>Phase End: <span className="font-mono">{detail.phase_completed_at?.slice(11, 19) || "—"}</span></div>
        </div>
      </div>

      {/* Error */}
      {detail.error && (
        <div className="rounded bg-negative/10 border border-negative/30 px-3 py-2 text-xs">
          <div className="flex items-center gap-1.5 text-negative font-medium mb-1">
            <AlertTriangle size={11} /> Error
          </div>
          <pre className="text-negative/80 whitespace-pre-wrap font-mono text-[10px] max-h-32 overflow-auto">
            {JSON.stringify(detail.error, null, 2)}
          </pre>
        </div>
      )}

      {/* Config */}
      {detail.config_json && (
        <div className="rounded bg-surface-overlay/50 px-3 py-2 text-xs">
          <div className="text-muted font-medium mb-1">Config</div>
          <pre className="text-zinc-400 whitespace-pre-wrap font-mono text-[10px] max-h-40 overflow-auto">
            {JSON.stringify(detail.config_json, null, 2)}
          </pre>
        </div>
      )}

      {/* Decisions */}
      <div>
        <div className="text-xs text-muted font-medium mb-1">
          Engine Decisions ({detail.decisions.length})
        </div>
        {detail.decisions.length === 0 ? (
          <p className="text-[11px] text-muted">No decisions linked to this run</p>
        ) : (
          <table className="w-full text-[11px]">
            <thead>
              <tr className="border-b border-border-dim text-left text-muted">
                <th className="px-1.5 py-1">Engine</th>
                <th className="px-1.5 py-1">Market</th>
                <th className="px-1.5 py-1">Strategy</th>
                <th className="px-1.5 py-1">Date</th>
              </tr>
            </thead>
            <tbody>
              {detail.decisions.map((d) => (
                <tr key={d.decision_id} className="border-b border-border-dim/30">
                  <td className="px-1.5 py-1">
                    <StatusBadge label={d.engine_name} variant="info" />
                  </td>
                  <td className="px-1.5 py-1 text-zinc-300">{d.market_id || "—"}</td>
                  <td className="px-1.5 py-1 text-muted truncate max-w-[120px]">{d.strategy_id || "—"}</td>
                  <td className="px-1.5 py-1 text-muted">{d.as_of_date || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
