import { useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { BarChart } from "../components/Charts";
import { DataTable, Column } from "../components/DataTable";
import {
  useOptionsResults,
  useOptionsResult,
  useOptionsCampaigns,
  useOptionsCampaignSummary,
} from "../api/hooks";

interface StratRow extends Record<string, unknown> {
  strategy: string;
  median_pnl: number;
  mean_pnl: number;
  win_rate: number;
}

interface CampaignMetric extends Record<string, unknown> {
  metric: string;
  mean: number;
  median: number;
  p5: number;
  p95: number;
  worst: number;
}

export default function Options() {
  const results = useOptionsResults();
  const campaigns = useOptionsCampaigns();
  const resultList = (results.data ?? []) as { result_id: string; file?: string }[];
  const campaignList = (campaigns.data ?? []) as { campaign_id: string; n_realities?: number }[];

  const [selectedResult, setSelectedResult] = useState("");
  const [selectedCampaign, setSelectedCampaign] = useState("");

  // Auto-select first if available
  const resultId = selectedResult || resultList[0]?.result_id || "";
  const campaignId = selectedCampaign || campaignList[0]?.campaign_id || "";

  const result = useOptionsResult(resultId);
  const campaignSummary = useOptionsCampaignSummary(campaignId);

  const rd = (result.data ?? {}) as Record<string, unknown>;
  const summary = (rd.summary ?? rd) as Record<string, unknown>;
  const strategies = (rd.strategies ?? rd.strategy_attribution ?? []) as StratRow[];
  const guardrails = (rd.guardrails ?? {}) as Record<string, unknown>;
  const csData = (campaignSummary.data ?? {}) as Record<string, unknown>;
  const mcMetrics = (csData.metrics ?? csData.summary ?? []) as CampaignMetric[];

  // Strategy attribution chart data
  const chartData = strategies.map((s) => ({
    strategy: String(s.strategy ?? s.name ?? "").replace(/_/g, " "),
    pnl: Number(s.median_pnl ?? s.total_pnl ?? s.pnl ?? 0),
  }));

  const stratCols: Column<StratRow>[] = [
    { key: "strategy", label: "Strategy", render: (r) => String(r.strategy ?? r.name ?? "").replace(/_/g, " ") },
    { key: "median_pnl", label: "Median PnL", align: "right", render: (r) => `$${Number(r.median_pnl ?? r.pnl ?? 0).toLocaleString()}` },
    { key: "mean_pnl", label: "Mean PnL", align: "right", render: (r) => `$${Number(r.mean_pnl ?? 0).toLocaleString()}` },
    { key: "win_rate", label: "Win Rate", align: "right", render: (r) => `${(Number(r.win_rate ?? 0) * 100).toFixed(1)}%` },
  ];

  const mcCols: Column<CampaignMetric>[] = [
    { key: "metric", label: "Metric" },
    { key: "mean", label: "Mean", align: "right", render: (r) => fmt(r.mean, r.metric) },
    { key: "median", label: "Median", align: "right", render: (r) => fmt(r.median, r.metric) },
    { key: "p5", label: "P5", align: "right", render: (r) => fmt(r.p5, r.metric) },
    { key: "p95", label: "P95", align: "right", render: (r) => fmt(r.p95, r.metric) },
    { key: "worst", label: "Worst", align: "right", render: (r) => fmt(r.worst, r.metric) },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Options Overlay"
        subtitle="Strategy performance & Monte Carlo campaigns"
        onRefresh={() => { results.refetch(); campaigns.refetch(); result.refetch(); campaignSummary.refetch(); }}
        actions={
          <div className="flex gap-2">
            <select
              className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-100"
              value={resultId}
              onChange={(e) => setSelectedResult(e.target.value)}
            >
              {resultList.map((r) => (
                <option key={r.result_id} value={r.result_id}>{r.file ?? r.result_id}</option>
              ))}
              {resultList.length === 0 && <option value="">No results</option>}
            </select>
            <select
              className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-xs text-zinc-100"
              value={campaignId}
              onChange={(e) => setSelectedCampaign(e.target.value)}
            >
              {campaignList.map((c) => (
                <option key={c.campaign_id} value={c.campaign_id}>{`${c.campaign_id} (${c.n_realities ?? 0} runs)`}</option>
              ))}
              {campaignList.length === 0 && <option value="">No campaigns</option>}
            </select>
          </div>
        }
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        <KpiCard label="CAGR" value={summary.cagr != null ? `${Number(summary.cagr).toFixed(2)}%` : "—"} sentiment={Number(summary.cagr ?? 0) > 0 ? "positive" : "negative"} />
        <KpiCard label="Sharpe" value={summary.sharpe != null ? Number(summary.sharpe).toFixed(3) : "—"} sentiment={Number(summary.sharpe ?? 0) > 1 ? "positive" : "neutral"} />
        <KpiCard label="MaxDD" value={summary.max_drawdown != null ? `${Number(summary.max_drawdown).toFixed(2)}%` : "—"} sentiment={Number(summary.max_drawdown ?? 0) > -20 ? "neutral" : "warning"} />
        <KpiCard label="Options PnL" value={summary.total_options_pnl != null ? `$${Number(summary.total_options_pnl).toLocaleString()}` : "—"} sentiment={Number(summary.total_options_pnl ?? 0) > 0 ? "positive" : "negative"} />
        <KpiCard label="Guardrail Triggers" value={String(guardrails.total_triggers ?? guardrails.halt_count ?? "—")} sentiment={Number(guardrails.total_triggers ?? 0) > 5 ? "warning" : "neutral"} />
      </div>

      {/* Charts + Tables */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Strategy Attribution">
          {chartData.length > 0 ? (
            <BarChart data={chartData} xKey="strategy" yKeys={["pnl"]} height={400} />
          ) : (
            <div className="flex h-64 items-center justify-center text-xs text-muted">No strategy data</div>
          )}
        </Panel>
        <Panel title="Per-Strategy Breakdown">
          <DataTable columns={stratCols} data={strategies} compact emptyMessage="No strategies" />
        </Panel>
      </div>

      {/* MC Campaign Summary */}
      <Panel title="Monte Carlo Campaign Summary">
        <DataTable columns={mcCols} data={mcMetrics} compact emptyMessage="No campaign data" />
      </Panel>
    </div>
  );
}

function fmt(v: unknown, metric: string): string {
  const n = Number(v);
  if (isNaN(n)) return "—";
  if (String(metric).toLowerCase().includes("pnl")) return `$${n.toLocaleString()}`;
  if (String(metric).toLowerCase().includes("sharpe")) return n.toFixed(3);
  return `${n.toFixed(2)}%`;
}
