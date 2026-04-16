import { useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { useOpsOverview, useOpsDay } from "../api/hooks";
import {
  CheckCircle2, XCircle, AlertTriangle, Clock, ChevronRight, ChevronDown,
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
  // "US_EQ_2026-04-04" → "US_EQ", "intel_daily_2026-04-04" → "INTEL", "iris_daily_..." → "IRIS"
  if (dagId.startsWith("intel_")) return "INTEL";
  if (dagId.startsWith("iris_")) return "IRIS";
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
      <Panel title="Execution History" tooltip="Click a day to expand its breakdown. Green = all jobs succeeded. Orange = partial failures. Red = all failed.">
        <div className="space-y-1.5">
          {data?.daily.map((day) => {
            const isSelected = day.date === selectedDate;
            const dayOfWeek = WEEKDAYS[new Date(day.date + "T12:00:00").getDay()];
            const isWeekend = dayOfWeek === "Sat" || dayOfWeek === "Sun";

            return (
              <div key={day.date} className="space-y-0">
                {/* Row header — clickable to expand */}
                <button
                  onClick={() => setSelectedDate(isSelected ? "" : day.date)}
                  className={`w-full flex items-center gap-3 rounded-lg px-3 py-2 text-left transition-colors ${
                    isSelected
                      ? "bg-accent/10 border border-accent/30"
                      : "hover:bg-surface-overlay border border-transparent"
                  } ${isWeekend ? "opacity-60" : ""}`}
                >
                  {/* Expand chevron */}
                  {isSelected ? (
                    <ChevronDown size={12} className="shrink-0 text-accent" />
                  ) : (
                    <ChevronRight size={12} className="shrink-0 text-zinc-600" />
                  )}

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
                </button>

                {/* Inline expansion */}
                {isSelected && (
                  <div className="ml-6 mt-1 mb-2 rounded-lg border border-accent/20 bg-surface-base/50 overflow-hidden">
                    <ExpandedDayDetail
                      day={day}
                      detail={detail}
                      isLoading={dayDetail.isLoading}
                    />
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </Panel>

    </div>
  );
}

/* ── Expanded Day Detail (inline under each row) ──── */

interface ExpandedDayDetailProps {
  day: DaySummary;
  detail: {
    date: string;
    jobs: JobDetail[];
    engine_runs: EngineRun[];
    intel_briefs: IntelBrief[];
  } | undefined;
  isLoading: boolean;
}

function ExpandedDayDetail({ day, detail, isLoading }: ExpandedDayDetailProps) {
  if (isLoading) {
    return (
      <div className="px-4 py-3 text-[10px] text-zinc-500">
        Loading {day.date}…
      </div>
    );
  }
  if (!detail) {
    return (
      <div className="px-4 py-3 text-[10px] text-zinc-500">
        No data available for this day.
      </div>
    );
  }

  // Dedup jobs: latest execution per job_id
  const latest = new Map<string, JobDetail>();
  for (const j of detail.jobs) {
    const existing = latest.get(j.job_id);
    if (
      !existing ||
      (j.created_at && existing.created_at && j.created_at > existing.created_at)
    ) {
      latest.set(j.job_id, j);
    }
  }
  const allJobs = [...latest.values()];

  // Partition by status
  const failed = allJobs.filter((j) => j.status === "FAILED");
  const skipped = allJobs.filter((j) => j.status === "SKIPPED");
  const running = allJobs.filter((j) => j.status === "RUNNING");
  const succeeded = allJobs.filter((j) => j.status === "SUCCESS");

  // Group jobs by DAG for the success summary
  const byDag = new Map<string, JobDetail[]>();
  for (const j of allJobs) {
    const arr = byDag.get(j.dag_id) ?? [];
    arr.push(j);
    byDag.set(j.dag_id, arr);
  }

  return (
    <div className="divide-y divide-border-dim">
      {/* Summary line */}
      <div className="px-4 py-2 bg-surface-overlay/30 flex items-center gap-4 text-[10px]">
        <span className="font-semibold text-zinc-300">{detail.date}</span>
        <span className="text-positive">{succeeded.length} succeeded</span>
        {failed.length > 0 && <span className="text-negative">{failed.length} failed</span>}
        {skipped.length > 0 && <span className="text-warning">{skipped.length} skipped</span>}
        {running.length > 0 && <span className="text-info">{running.length} running</span>}
        <span className="text-zinc-500 ml-auto">
          {byDag.size} DAG{byDag.size !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Failed jobs section — most important */}
      {failed.length > 0 && (
        <div className="px-4 py-2">
          <div className="text-[10px] font-semibold text-negative mb-1.5 flex items-center gap-1">
            <XCircle size={11} /> FAILED ({failed.length})
          </div>
          <div className="space-y-1">
            {failed.map((j) => (
              <JobLine key={j.execution_id} job={j} highlightError />
            ))}
          </div>
        </div>
      )}

      {/* Skipped jobs section */}
      {skipped.length > 0 && (
        <div className="px-4 py-2">
          <div className="text-[10px] font-semibold text-warning mb-1.5 flex items-center gap-1">
            <AlertTriangle size={11} /> SKIPPED ({skipped.length})
          </div>
          <div className="space-y-1">
            {skipped.map((j) => (
              <JobLine key={j.execution_id} job={j} highlightError />
            ))}
          </div>
        </div>
      )}

      {/* Running jobs section */}
      {running.length > 0 && (
        <div className="px-4 py-2">
          <div className="text-[10px] font-semibold text-info mb-1.5 flex items-center gap-1">
            <Activity size={11} /> RUNNING ({running.length})
          </div>
          <div className="space-y-1">
            {running.map((j) => (
              <JobLine key={j.execution_id} job={j} />
            ))}
          </div>
        </div>
      )}

      {/* Succeeded jobs — collapsed by default, click to expand */}
      {succeeded.length > 0 && (
        <details className="px-4 py-2">
          <summary className="text-[10px] font-semibold text-positive cursor-pointer flex items-center gap-1">
            <CheckCircle2 size={11} /> SUCCEEDED ({succeeded.length}) — click to expand
          </summary>
          <div className="space-y-1 mt-2">
            {[...byDag.entries()].map(([dagId, jobs]) => {
              const dagSucceeded = jobs.filter((j) => j.status === "SUCCESS");
              if (dagSucceeded.length === 0) return null;
              return (
                <div key={dagId} className="mb-2">
                  <div className="text-[9px] font-mono text-zinc-400 mb-0.5">
                    {dagLabel(dagId)} ({dagSucceeded.length} jobs)
                  </div>
                  <div className="space-y-0.5 pl-2">
                    {dagSucceeded.map((j) => (
                      <JobLine key={j.execution_id} job={j} compact />
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </details>
      )}

      {/* Engine runs */}
      {detail.engine_runs.length > 0 && (
        <details className="px-4 py-2">
          <summary className="text-[10px] font-semibold text-zinc-400 cursor-pointer flex items-center gap-1">
            <Database size={11} /> Engine Runs ({detail.engine_runs.length})
          </summary>
          <div className="space-y-1 mt-2">
            {detail.engine_runs.map((run) => {
              const isComplete = run.phase === "COMPLETED";
              const isFailed = run.phase === "FAILED";
              return (
                <div
                  key={run.run_id}
                  className="flex items-center gap-2 rounded px-2 py-1 bg-surface-overlay/30 text-[10px]"
                >
                  <span className="font-mono text-zinc-300 w-12">{run.region}</span>
                  <span
                    className={`font-semibold w-20 ${
                      isComplete ? "text-positive" : isFailed ? "text-negative" : "text-info"
                    }`}
                  >
                    {run.phase}
                  </span>
                  <span className="text-[9px] text-zinc-500">
                    {run.updated_at ? formatTime(run.updated_at) : ""}
                  </span>
                  {run.error && (
                    <span className="text-[9px] text-negative truncate flex-1">
                      {JSON.stringify(run.error)}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        </details>
      )}

      {/* Intel briefs */}
      {detail.intel_briefs.length > 0 && (
        <details className="px-4 py-2">
          <summary className="text-[10px] font-semibold text-zinc-400 cursor-pointer flex items-center gap-1">
            <FileText size={11} /> Intelligence Briefs ({detail.intel_briefs.length})
          </summary>
          <div className="space-y-0.5 mt-2">
            {detail.intel_briefs.map((brief) => {
              const sevColor =
                brief.severity === "critical"
                  ? "text-negative"
                  : brief.severity === "high"
                  ? "text-warning"
                  : brief.severity === "medium"
                  ? "text-info"
                  : "text-zinc-400";
              return (
                <div
                  key={brief.id}
                  className="flex items-center gap-2 rounded px-2 py-1 bg-surface-overlay/30 text-[10px]"
                >
                  <span className={`font-semibold uppercase w-14 ${sevColor}`}>
                    {brief.severity}
                  </span>
                  <span className="text-zinc-500 w-20">{brief.type}</span>
                  <span className="text-zinc-500 w-16">{brief.domain}</span>
                  <span className="text-zinc-300 truncate flex-1">{brief.title}</span>
                  <span className="text-[9px] text-zinc-500">{formatTime(brief.created_at)}</span>
                </div>
              );
            })}
          </div>
        </details>
      )}

      {/* Empty state */}
      {allJobs.length === 0 && detail.engine_runs.length === 0 && detail.intel_briefs.length === 0 && (
        <div className="px-4 py-4 text-center text-[10px] text-zinc-500">
          No activity recorded for {detail.date}
        </div>
      )}
    </div>
  );
}

/* ── Single job line (used in expanded view) ─────── */

function JobLine({
  job,
  highlightError = false,
  compact = false,
}: {
  job: JobDetail;
  highlightError?: boolean;
  compact?: boolean;
}) {
  const Icon = JOB_STATUS_ICONS[job.status] ?? Clock;
  const color = JOB_STATUS_COLORS[job.status] ?? "text-zinc-500";
  return (
    <div
      className={`flex items-start gap-2 rounded px-2 py-1 text-[10px] ${
        highlightError ? "bg-negative/5" : "hover:bg-surface-overlay/30"
      }`}
    >
      <Icon size={11} className={`shrink-0 mt-0.5 ${color}`} />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="font-mono text-zinc-300 truncate" title={job.job_id}>
            {job.job_type}
          </span>
          {!compact && (
            <span className="text-[9px] text-zinc-600">
              {dagLabel(job.dag_id)}
            </span>
          )}
          <span className="text-[9px] text-zinc-500 ml-auto shrink-0">
            {formatTime(job.started_at)}
          </span>
          <span className="text-[9px] text-zinc-500 w-12 text-right shrink-0">
            {formatDuration(job.duration_s)}
          </span>
          {job.attempt > 1 && (
            <span className="text-[9px] text-warning shrink-0">×{job.attempt}</span>
          )}
        </div>
        {job.error && highlightError && (
          <div className="text-[9px] text-negative mt-0.5 font-mono whitespace-pre-wrap break-all">
            {job.error}
          </div>
        )}
      </div>
    </div>
  );
}
