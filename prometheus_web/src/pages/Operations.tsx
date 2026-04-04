import { useState, useMemo } from "react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { useOpsOverview, useOpsDay } from "../api/hooks";
import {
  CheckCircle2, XCircle, AlertTriangle, Clock, ChevronRight,
  Activity, FileText, Database,
} from "lucide-react";

/* ── Types ──────────────────────────────��───────────── */

interface Service {
  name: string;
  description: string;
  active: string;
  sub_state: string;
  pid: number | null;
  started_at: string | null;
  restarts: number;
  memory_mb: number | null;
  healthy: boolean;
}

interface DagSummary {
  dag_id: string;
  total: number;
  success: number;
  failed: number;
  skipped: number;
  running: number;
  pending: number;
}

interface DaySummary {
  date: string;
  total: number;
  success: number;
  failed: number;
  skipped: number;
  running: number;
  pending: number;
  status: string;
  dags: Record<string, DagSummary>;
}

interface JobDetail {
  execution_id: string;
  job_id: string;
  job_type: string;
  dag_id: string;
  market_id: string | null;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  duration_s: number | null;
  attempt: number;
  error: string | null;
  created_at: string | null;
}

interface EngineRun {
  run_id: string;
  region: string;
  phase: string;
  created_at: string | null;
  updated_at: string | null;
  phase_started_at: string | null;
  phase_completed_at: string | null;
  error: string | Record<string, unknown> | null;
}

interface IntelBrief {
  id: string;
  type: string;
  severity: string;
  domain: string;
  title: string;
  created_at: string | null;
}

/* ── Helpers ─────────────────────────────────────────── */

const STATUS_COLORS: Record<string, string> = {
  ok: "bg-positive/20 text-positive border-positive/30",
  partial: "bg-warning/20 text-warning border-warning/30",
  failed: "bg-negative/20 text-negative border-negative/30",
  running: "bg-info/20 text-info border-info/30",
  idle: "bg-zinc-800 text-zinc-500 border-zinc-700",
  pending: "bg-zinc-800 text-zinc-400 border-zinc-700",
};

const JOB_STATUS_COLORS: Record<string, string> = {
  SUCCESS: "text-positive",
  FAILED: "text-negative",
  SKIPPED: "text-warning",
  RUNNING: "text-info",
  PENDING: "text-zinc-500",
  NOT_STARTED: "text-zinc-600",
};

const JOB_STATUS_ICONS: Record<string, typeof CheckCircle2> = {
  SUCCESS: CheckCircle2,
  FAILED: XCircle,
  SKIPPED: AlertTriangle,
  RUNNING: Activity,
  PENDING: Clock,
};

function formatTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatDuration(s: number | null): string {
  if (s == null) return "—";
  if (s < 60) return `${s.toFixed(1)}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function dagLabel(dagId: string): string {
  // "US_EQ_2026-04-04" → "US_EQ", "intel_daily_2026-04-04" → "INTEL", "kronos_daily_..." → "KRONOS"
  if (dagId.startsWith("intel_")) return "INTEL";
  if (dagId.startsWith("kronos_")) return "KRONOS";
  return dagId.replace(/_\d{4}-\d{2}-\d{2}$/, "");
}

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/* ── Main Page ──────────────────────────────────────── */

export default function Operations() {
  const overview = useOpsOverview();
  const [selectedDate, setSelectedDate] = useState<string>(new Date().toISOString().slice(0, 10));
  const dayDetail = useOpsDay(selectedDate);

  const data = overview.data as {
    services: Service[];
    daily: DaySummary[];
  } | undefined;

  const detail = dayDetail.data as {
    date: string;
    jobs: JobDetail[];
    engine_runs: EngineRun[];
    intel_briefs: IntelBrief[];
  } | undefined;

  // Group jobs by DAG
  const jobsByDag = useMemo(() => {
    if (!detail?.jobs) return new Map<string, JobDetail[]>();
    const m = new Map<string, JobDetail[]>();
    for (const j of detail.jobs) {
      const existing = m.get(j.dag_id) ?? [];
      existing.push(j);
      m.set(j.dag_id, existing);
    }
    return m;
  }, [detail?.jobs]);

  // Dedup jobs: show latest execution per job_id per dag
  const dedupedByDag = useMemo(() => {
    const result = new Map<string, JobDetail[]>();
    for (const [dagId, jobs] of jobsByDag) {
      const latest = new Map<string, JobDetail>();
      for (const j of jobs) {
        const existing = latest.get(j.job_id);
        if (!existing || (j.created_at && existing.created_at && j.created_at > existing.created_at)) {
          latest.set(j.job_id, j);
        }
      }
      result.set(dagId, [...latest.values()]);
    }
    return result;
  }, [jobsByDag]);

  return (
    <div className="space-y-4 p-4 overflow-y-auto h-[calc(100vh-3rem)]">
      <PageHeader
        title="Operations"
        subtitle="System health, service status, and pipeline execution history"
        onRefresh={() => { overview.refetch(); dayDetail.refetch(); }}
      />

      {/* ── Services ────────────────────────────────── */}
      <div className="grid grid-cols-4 gap-3">
        {data?.services.map((svc) => (
          <div
            key={svc.name}
            className={`rounded-lg border p-3 ${
              svc.healthy
                ? "border-positive/20 bg-positive/5"
                : "border-negative/20 bg-negative/5"
            }`}
          >
            <div className="flex items-center gap-2 mb-1">
              <div className={`w-2 h-2 rounded-full ${svc.healthy ? "bg-positive" : "bg-negative"} ${svc.healthy ? "animate-pulse" : ""}`} />
              <span className="text-[11px] font-semibold text-zinc-200">{svc.name}</span>
            </div>
            <div className="text-[9px] text-zinc-400 mb-1.5">{svc.description}</div>
            <div className="flex gap-3 text-[9px] text-zinc-500">
              <span>{svc.active}/{svc.sub_state}</span>
              {svc.pid != null && <span>PID {svc.pid}</span>}
              {svc.memory_mb != null && <span>{svc.memory_mb} MB</span>}
              {svc.restarts > 0 && <span className="text-warning">{svc.restarts} restarts</span>}
            </div>
          </div>
        ))}
      </div>

      {/* ── 14-Day History Grid ─────────────────────── */}
      <Panel title="Execution History" tooltip="Click a day to see detailed breakdown. Green = all jobs succeeded. Orange = partial failures. Red = all failed.">
        <div className="space-y-1.5">
          {data?.daily.map((day) => {
            const isSelected = day.date === selectedDate;
            const dayOfWeek = WEEKDAYS[new Date(day.date + "T12:00:00").getDay()];
            const isWeekend = dayOfWeek === "Sat" || dayOfWeek === "Sun";

            return (
              <button
                key={day.date}
                onClick={() => setSelectedDate(day.date)}
                className={`w-full flex items-center gap-3 rounded-lg px-3 py-2 text-left transition-colors ${
                  isSelected
                    ? "bg-accent/10 border border-accent/30"
                    : "hover:bg-surface-overlay border border-transparent"
                } ${isWeekend ? "opacity-60" : ""}`}
              >
                {/* Date */}
                <div className="w-24 shrink-0">
                  <div className="text-[11px] font-mono text-zinc-300">{day.date}</div>
                  <div className="text-[9px] text-zinc-500">{dayOfWeek}</div>
                </div>

                {/* Status badge */}
                <div className={`w-16 shrink-0 rounded-full border px-2 py-0.5 text-center text-[9px] font-semibold ${STATUS_COLORS[day.status] ?? STATUS_COLORS.idle}`}>
                  {day.status.toUpperCase()}
                </div>

                {/* Counts */}
                <div className="flex gap-4 text-[10px] flex-1">
                  {day.success > 0 && <span className="text-positive">{day.success} ok</span>}
                  {day.failed > 0 && <span className="text-negative">{day.failed} failed</span>}
                  {day.skipped > 0 && <span className="text-warning">{day.skipped} skipped</span>}
                  {day.running > 0 && <span className="text-info">{day.running} running</span>}
                  {day.total === 0 && <span className="text-zinc-600">No jobs</span>}
                </div>

                {/* Per-DAG mini bars */}
                <div className="flex gap-1.5 shrink-0">
                  {Object.entries(day.dags).map(([dagId, dag]) => {
                    const label = dagLabel(dagId);
                    const allOk = dag.failed === 0 && dag.success > 0;
                    const hasFail = dag.failed > 0;
                    return (
                      <div
                        key={dagId}
                        className={`rounded px-1.5 py-0.5 text-[8px] font-mono ${
                          allOk ? "bg-positive/15 text-positive"
                          : hasFail ? "bg-negative/15 text-negative"
                          : "bg-zinc-800 text-zinc-500"
                        }`}
                        title={`${label}: ${dag.success}/${dag.total} ok`}
                      >
                        {label} {dag.success}/{dag.total}
                      </div>
                    );
                  })}
                </div>

                <ChevronRight size={12} className={`shrink-0 ${isSelected ? "text-accent" : "text-zinc-600"}`} />
              </button>
            );
          })}
        </div>
      </Panel>

      {/* ── Day Detail ──────────────────────────────── */}
      {selectedDate && (
        <div className="space-y-3">
          <div className="text-xs font-semibold text-zinc-300">
            Detail: {selectedDate}
            {dayDetail.isLoading && <span className="text-zinc-500 ml-2">Loading...</span>}
          </div>

          {/* Engine Runs */}
          {detail?.engine_runs && detail.engine_runs.length > 0 && (
            <Panel title="Engine Runs">
              <div className="space-y-1">
                {detail.engine_runs.map((run) => {
                  const isComplete = run.phase === "COMPLETED";
                  const isFailed = run.phase === "FAILED";
                  return (
                    <div key={run.run_id} className="flex items-center gap-3 rounded px-2 py-1.5 bg-surface-overlay/30">
                      <Database size={12} className="text-zinc-500 shrink-0" />
                      <span className="text-[10px] font-mono text-zinc-300 w-12">{run.region}</span>
                      <span className={`text-[10px] font-semibold ${isComplete ? "text-positive" : isFailed ? "text-negative" : "text-info"}`}>
                        {run.phase}
                      </span>
                      <span className="text-[9px] text-zinc-500 flex-1">
                        {run.updated_at ? formatTime(run.updated_at) : ""}
                      </span>
                      {run.error && (
                        <span className="text-[9px] text-negative truncate max-w-64">{JSON.stringify(run.error)}</span>
                      )}
                    </div>
                  );
                })}
              </div>
            </Panel>
          )}

          {/* Jobs by DAG */}
          {[...dedupedByDag.entries()].map(([dagId, jobs]) => (
            <Panel key={dagId} title={dagLabel(dagId) + " Pipeline"} tooltip={`DAG: ${dagId}`}>
              <div className="space-y-0.5">
                {jobs.map((job) => {
                  const Icon = JOB_STATUS_ICONS[job.status] ?? Clock;
                  const color = JOB_STATUS_COLORS[job.status] ?? "text-zinc-500";
                  return (
                    <div key={job.execution_id} className="flex items-center gap-2 rounded px-2 py-1.5 hover:bg-surface-overlay/30">
                      <Icon size={12} className={`shrink-0 ${color}`} />
                      <span className="text-[10px] font-mono text-zinc-300 w-44 truncate" title={job.job_type}>
                        {job.job_type}
                      </span>
                      <span className={`text-[10px] font-semibold w-16 ${color}`}>{job.status}</span>
                      <span className="text-[9px] text-zinc-500 w-16">{formatTime(job.started_at)}</span>
                      <span className="text-[9px] text-zinc-500 w-16">{formatDuration(job.duration_s)}</span>
                      {job.attempt > 1 && (
                        <span className="text-[9px] text-warning">attempt {job.attempt}</span>
                      )}
                      {job.error && (
                        <span className="text-[9px] text-negative truncate flex-1 max-w-96" title={job.error}>
                          {job.error}
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
            </Panel>
          ))}

          {/* Intel Briefs */}
          {detail?.intel_briefs && detail.intel_briefs.length > 0 && (
            <Panel title="Intelligence Reports">
              <div className="space-y-0.5">
                {detail.intel_briefs.map((brief) => {
                  const sevColor = brief.severity === "critical" ? "text-negative"
                    : brief.severity === "high" ? "text-warning"
                    : brief.severity === "medium" ? "text-info"
                    : "text-zinc-400";
                  return (
                    <div key={brief.id} className="flex items-center gap-2 rounded px-2 py-1.5 hover:bg-surface-overlay/30">
                      <FileText size={12} className="text-zinc-500 shrink-0" />
                      <span className={`text-[9px] font-semibold uppercase w-14 ${sevColor}`}>{brief.severity}</span>
                      <span className="text-[9px] text-zinc-500 w-20">{brief.type}</span>
                      <span className="text-[9px] text-zinc-500 w-16">{brief.domain}</span>
                      <span className="text-[10px] text-zinc-300 truncate flex-1">{brief.title}</span>
                      <span className="text-[9px] text-zinc-500">{formatTime(brief.created_at)}</span>
                    </div>
                  );
                })}
              </div>
            </Panel>
          )}

          {/* Empty state */}
          {detail && detail.jobs.length === 0 && detail.engine_runs.length === 0 && detail.intel_briefs.length === 0 && (
            <div className="rounded-lg border border-border-dim bg-surface-raised p-8 text-center text-zinc-500 text-xs">
              No activity recorded for {selectedDate}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
