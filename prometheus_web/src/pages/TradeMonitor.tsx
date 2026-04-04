import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { KpiCard } from "../components/KpiCard";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import { useWeeklyReport, useTradeJournal, useMetaFeedback } from "../api/hooks";

// ── Types ───────────────────────────────────────────────

interface PosRow extends Record<string, unknown> {
  instrument_id: string;
  pnl_pct: number;
  pnl: number;
  sector: string;
}

interface InsightRow extends Record<string, unknown> {
  category: string;
  severity: string;
  message: string;
  metric_value: number;
  benchmark: number;
}

// ── Formatters ──────────────────────────────────────────

const fmtPct = (v: number) => `${(v * 100).toFixed(1)}%`;
const fmtUsd = (v: number) => `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;

const signalColors: Record<string, string> = {
  GREEN: "text-green-400",
  YELLOW: "text-yellow-400",
  ORANGE: "text-orange-400",
  RED: "text-red-400",
};

// ── Component ───────────────────────────────────────────

export default function TradeMonitor() {
  const weekly = useWeeklyReport();
  const journal = useTradeJournal(63);
  const feedback = useMetaFeedback(63);

  const w = (weekly.data ?? {}) as Record<string, unknown>;
  const j = (journal.data ?? {}) as Record<string, unknown>;
  const fb = (feedback.data ?? {}) as Record<string, unknown>;

  const anomalies = (w.anomalies ?? []) as string[];
  const winners = (w.top_winners ?? []) as PosRow[];
  const losers = (w.top_losers ?? []) as PosRow[];
  const sectorPnl = (w.sector_pnl ?? {}) as Record<string, number>;
  const insights = ((fb.insights ?? []) as InsightRow[]);

  const regime = String(w.regime ?? "—");
  const fwdSignal = String(w.forward_signal ?? "—");

  // Journal breakdown
  const journalByRegime = (j.by_regime ?? {}) as Record<string, { count: number; avg_return_5d: number }>;
  const journalBySector = (j.by_sector ?? {}) as Record<string, { count: number; avg_return_5d: number }>;
  const journalBySignal = (j.by_forward_signal ?? {}) as Record<string, { count: number; avg_return_5d: number }>;

  const posCols: Column<PosRow>[] = [
    { key: "instrument_id", label: "Instrument", render: (r) => <span className="font-mono text-zinc-200">{r.instrument_id}</span> },
    { key: "pnl_pct", label: "P&L %", align: "right", render: (r) => (
      <span className={r.pnl_pct >= 0 ? "text-positive" : "text-negative"}>{fmtPct(r.pnl_pct)}</span>
    )},
    { key: "pnl", label: "P&L $", align: "right", render: (r) => (
      <span className={r.pnl >= 0 ? "text-positive" : "text-negative"}>{fmtUsd(r.pnl)}</span>
    )},
    { key: "sector", label: "Sector" },
  ];

  const insightCols: Column<InsightRow>[] = [
    { key: "severity", label: "", render: (r) => (
      <StatusBadge label={r.severity} variant={r.severity === "critical" ? "negative" : r.severity === "warning" ? "warning" : "info"} />
    )},
    { key: "category", label: "Category", render: (r) => <span className="text-zinc-300">{r.category}</span> },
    { key: "message", label: "Message" },
    { key: "metric_value", label: "Value", align: "right", render: (r) => <span className="font-mono">{r.metric_value.toFixed(3)}</span> },
    { key: "benchmark", label: "Target", align: "right", render: (r) => <span className="font-mono text-muted">{r.benchmark.toFixed(3)}</span> },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Kronos Trade Monitor"
        subtitle="Live validation — every trade under scrutiny"
        onRefresh={() => { weekly.refetch(); journal.refetch(); feedback.refetch(); }}
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-6">
        <KpiCard label="NAV" value={w.nav ? fmtUsd(Number(w.nav)) : "—"} />
        <KpiCard label="Positions" value={String(w.n_positions ?? "—")} />
        <KpiCard label="Entries" value={String(w.n_entries ?? 0)} />
        <KpiCard label="Exits" value={String(w.n_exits ?? 0)} />
        <KpiCard label="Turnover" value={w.turnover_pct != null ? fmtPct(Number(w.turnover_pct)) : "—"} />
        <KpiCard label="Hit Rate" value={w.portfolio_hit_rate != null ? fmtPct(Number(w.portfolio_hit_rate)) : "—"}
          sentiment={Number(w.portfolio_hit_rate ?? 0.5) >= 0.5 ? "positive" : "negative"} />
      </div>

      {/* Regime + Forward Signal banner */}
      <div className="flex items-center gap-6 rounded bg-surface-raised px-4 py-2">
        <div className="flex items-center gap-2">
          <span className="text-[10px] uppercase text-muted">Regime</span>
          <StatusBadge label={regime} variant={regime === "CRISIS" ? "negative" : regime === "RISK_OFF" ? "warning" : "neutral"} />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[10px] uppercase text-muted">Forward Signal</span>
          <span className={`text-sm font-bold ${signalColors[fwdSignal] ?? "text-zinc-400"}`}>{fwdSignal}</span>
        </div>
        {anomalies.length > 0 && (
          <div className="ml-auto flex items-center gap-2">
            <StatusBadge label={`${anomalies.length} anomalies`} variant="warning" />
          </div>
        )}
      </div>

      {/* Anomalies */}
      {anomalies.length > 0 && (
        <Panel title="Anomalies">
          <div className="space-y-1">
            {anomalies.map((a, i) => (
              <div key={i} className="flex items-center gap-2 rounded bg-red-950/20 border border-red-900/30 px-3 py-1.5 text-xs text-red-300">
                <span className="text-red-400">!</span> {a}
              </div>
            ))}
          </div>
        </Panel>
      )}

      {/* Winners & Losers */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Top Winners">
          <DataTable columns={posCols} data={winners} compact emptyMessage="No positions" />
        </Panel>
        <Panel title="Top Losers">
          <DataTable columns={posCols} data={losers} compact emptyMessage="No positions" />
        </Panel>
      </div>

      {/* Sector P&L */}
      {Object.keys(sectorPnl).length > 0 && (
        <Panel title="Sector P&L Attribution">
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
            {Object.entries(sectorPnl)
              .sort((a, b) => b[1] - a[1])
              .map(([sector, pnl]) => (
                <div key={sector} className="flex items-center justify-between rounded bg-surface-overlay/30 px-3 py-1.5 text-xs">
                  <span className="text-zinc-300">{sector}</span>
                  <span className={pnl >= 0 ? "text-positive font-mono" : "text-negative font-mono"}>
                    {fmtUsd(pnl)}
                  </span>
                </div>
              ))}
          </div>
        </Panel>
      )}

      {/* Trade Journal Analysis */}
      {Object.keys(journalByRegime).length > 0 && (
        <Panel title="Trade Journal — 5-Day Returns by Context">
          <div className="grid gap-4 lg:grid-cols-3">
            {/* By Regime */}
            <div>
              <h4 className="mb-1 text-[10px] font-semibold uppercase text-muted">By Regime</h4>
              <div className="space-y-1">
                {Object.entries(journalByRegime).map(([regime, stats]) => (
                  <div key={regime} className="flex items-center justify-between text-xs rounded bg-surface-overlay/20 px-2 py-1">
                    <span>{regime} <span className="text-muted">({stats.count})</span></span>
                    <span className={stats.avg_return_5d >= 0 ? "text-positive font-mono" : "text-negative font-mono"}>
                      {fmtPct(stats.avg_return_5d)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
            {/* By Forward Signal */}
            {Object.keys(journalBySignal).length > 0 && (
              <div>
                <h4 className="mb-1 text-[10px] font-semibold uppercase text-muted">By Forward Signal</h4>
                <div className="space-y-1">
                  {Object.entries(journalBySignal).map(([sig, stats]) => (
                    <div key={sig} className="flex items-center justify-between text-xs rounded bg-surface-overlay/20 px-2 py-1">
                      <span className={signalColors[sig] ?? ""}>{sig} <span className="text-muted">({stats.count})</span></span>
                      <span className={stats.avg_return_5d >= 0 ? "text-positive font-mono" : "text-negative font-mono"}>
                        {fmtPct(stats.avg_return_5d)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            {/* By Sector (top 8) */}
            <div>
              <h4 className="mb-1 text-[10px] font-semibold uppercase text-muted">By Sector</h4>
              <div className="space-y-1">
                {Object.entries(journalBySector).slice(0, 8).map(([sector, stats]) => (
                  <div key={sector} className="flex items-center justify-between text-xs rounded bg-surface-overlay/20 px-2 py-1">
                    <span className="truncate">{sector} <span className="text-muted">({stats.count})</span></span>
                    <span className={stats.avg_return_5d >= 0 ? "text-positive font-mono" : "text-negative font-mono"}>
                      {fmtPct(stats.avg_return_5d)}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Panel>
      )}

      {/* Meta Feedback Insights */}
      {insights.length > 0 && (
        <Panel title="Meta Feedback Insights">
          <DataTable columns={insightCols} data={insights} compact emptyMessage="All metrics healthy" />
        </Panel>
      )}

      {/* Formatted Report */}
      {!!w.formatted_report && (
        <Panel title="Full Weekly Report (Text)">
          <pre className="whitespace-pre-wrap text-[11px] text-zinc-400 font-mono max-h-96 overflow-y-auto">
            {String(w.formatted_report)}
          </pre>
        </Panel>
      )}
    </div>
  );
}
