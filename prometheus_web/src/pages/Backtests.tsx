import { useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { LineChart, ZOOM_STEPS, ZOOM_LABELS, fmtDateTick, ChartZoomBar } from "../components/Charts";
import { StatusBadge } from "../components/StatusBadge";
import { KpiCard } from "../components/KpiCard";
import { OptionsOverlaySection } from "../components/OptionsOverlaySection";
import {
  useBacktestRuns,
  useBacktestEquity,
  useRunBacktest,
  useJobStatus,
} from "../api/hooks";

interface RunRow extends Record<string, unknown> {
  run_id: string;
  label: string;
  sharpe: number;
  cagr: number;
  max_drawdown: number;
  start_date: string;
  end_date: string;
}

export default function Backtests() {
  const runs = useBacktestRuns();
  const runList = (runs.data ?? []) as RunRow[];
  const [selectedRun, setSelectedRun] = useState("");
  const [jobId, setJobId] = useState("");
  const [showForm, setShowForm] = useState(false);
  const [eqZoomIdx, setEqZoomIdx] = useState(ZOOM_STEPS.length - 1);
  const eqZoomDays = ZOOM_STEPS[eqZoomIdx];
  const [form, setForm] = useState({ book_id: "", sleeve_id: "", start_date: "", end_date: "", threads: "4", overrides: "{}" });

  const runId = selectedRun || runList[0]?.run_id || "";
  const equity = useBacktestEquity(runId);
  const equityData = (equity.data ?? []) as Record<string, unknown>[];
  const mutation = useRunBacktest();
  const job = useJobStatus(jobId);
  const jobData = (job.data ?? {}) as Record<string, unknown>;

  const selectedRunData = runList.find((r) => r.run_id === runId);

  const runCols: Column<RunRow>[] = [
    { key: "run_id", label: "Run", width: "180px" },
    { key: "label", label: "Label" },
    { key: "sharpe", label: "Sharpe", align: "right", render: (r) => Number(r.sharpe).toFixed(3) },
    { key: "cagr", label: "CAGR", align: "right", render: (r) => `${Number(r.cagr).toFixed(2)}%` },
    { key: "max_drawdown", label: "MaxDD", align: "right", render: (r) => `${Number(r.max_drawdown).toFixed(2)}%` },
    { key: "start_date", label: "Start" },
    { key: "end_date", label: "End" },
  ];

  const handleSubmit = () => {
    mutation.mutate(
      { ...form, threads: Number(form.threads), overrides: JSON.parse(form.overrides || "{}") },
      { onSuccess: (data) => { const d = data as Record<string, unknown>; if (d.job_id) setJobId(String(d.job_id)); setShowForm(false); } }
    );
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Backtests"
        subtitle={`${runList.length} runs available`}
        onRefresh={() => runs.refetch()}
        actions={
          <button
            className="rounded border border-accent bg-accent/10 px-3 py-1 text-xs font-semibold text-accent hover:bg-accent/20"
            onClick={() => setShowForm(!showForm)}
          >
            {showForm ? "Cancel" : "New Backtest"}
          </button>
        }
      />

      {/* New Backtest Form */}
      {showForm && (
        <Panel title="Launch Backtest">
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {(["book_id", "sleeve_id", "start_date", "end_date", "threads"] as const).map((field) => (
              <label key={field} className="block">
                <span className="text-[10px] uppercase text-muted">{field.replace(/_/g, " ")}</span>
                <input
                  className="mt-1 block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100"
                  value={form[field]}
                  onChange={(e) => setForm({ ...form, [field]: e.target.value })}
                  placeholder={field === "start_date" || field === "end_date" ? "YYYY-MM-DD" : ""}
                />
              </label>
            ))}
            <label className="block sm:col-span-2 lg:col-span-3">
              <span className="text-[10px] uppercase text-muted">Overrides (JSON)</span>
              <textarea
                className="mt-1 block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100 font-mono"
                rows={3}
                value={form.overrides}
                onChange={(e) => setForm({ ...form, overrides: e.target.value })}
              />
            </label>
          </div>
          <div className="mt-3 flex items-center gap-3">
            <button
              className="rounded bg-accent px-4 py-1.5 text-xs font-semibold text-zinc-950 hover:bg-accent/80 disabled:opacity-50"
              onClick={handleSubmit}
              disabled={mutation.isPending}
            >
              {mutation.isPending ? "Submitting..." : "Run Backtest"}
            </button>
            {mutation.isError && <span className="text-xs text-negative">Failed: {String(mutation.error)}</span>}
          </div>
        </Panel>
      )}

      {/* Job tracker */}
      {jobId && (
        <Panel title="Job Status">
          <div className="flex items-center gap-4">
            <StatusBadge label={String(jobData.status ?? "PENDING")} variant={jobData.status === "COMPLETED" ? "positive" : jobData.status === "FAILED" ? "negative" : "info"} />
            <span className="text-xs text-muted">Job: {jobId}</span>
            {jobData.progress != null && (
              <div className="flex-1">
                <div className="h-2 rounded-full bg-surface-overlay">
                  <div className="h-2 rounded-full bg-accent transition-all" style={{ width: `${Number(jobData.progress)}%` }} />
                </div>
              </div>
            )}
          </div>
        </Panel>
      )}

      {/* Selected run KPIs */}
      {selectedRunData && (
        <div className="grid grid-cols-3 gap-3">
          <KpiCard label="Sharpe Ratio" value={Number(selectedRunData.sharpe).toFixed(3)} sentiment={Number(selectedRunData.sharpe) > 1 ? "positive" : "neutral"} />
          <KpiCard label="CAGR" value={`${Number(selectedRunData.cagr).toFixed(2)}%`} sentiment={Number(selectedRunData.cagr) > 0 ? "positive" : "negative"} />
          <KpiCard label="Max Drawdown" value={`${Number(selectedRunData.max_drawdown).toFixed(2)}%`} sentiment={Number(selectedRunData.max_drawdown) > -20 ? "neutral" : "warning"} />
        </div>
      )}

      {/* Equity curve */}
      <Panel
        title={`Equity Curve (${ZOOM_LABELS[eqZoomDays] ?? `${eqZoomDays}d`})`}
        actions={<ChartZoomBar zoomIdx={eqZoomIdx} setZoomIdx={setEqZoomIdx} />}
      >
        {equityData.length > 0 ? (
          <LineChart
            data={equityData.slice(-eqZoomDays).map((d) => ({
              ...d,
              drawdown: d.drawdown != null ? Math.min(0, Number(d.drawdown)) : d.drawdown,
            }))}
            xKey="date"
            yKeys={["equity_curve_value", "drawdown"]}
            height={400}
            labels={{ equity_curve_value: "Equity", drawdown: "Drawdown" }}
            xTickFormatter={fmtDateTick}
          />
        ) : (
          <div className="flex h-64 items-center justify-center text-xs text-muted">Select a run to view equity curve</div>
        )}
      </Panel>

      {/* Options Overlay (collapsible) */}
      <OptionsOverlaySection />

      {/* Runs table */}
      <Panel title="All Runs">
        <DataTable columns={runCols} data={runList} onRowClick={(r) => setSelectedRun(r.run_id)} emptyMessage="No backtest runs" />
      </Panel>
    </div>
  );
}
