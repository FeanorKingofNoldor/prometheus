import { useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import { BarChart, LineChart, fmtDateTick } from "../components/Charts";
import { usePortfolio, usePositionPnlHistory, usePortfolioRiskComputed } from "../api/hooks";
import { usePortfolioContext } from "../context/PortfolioContext";
import TradingReportsTab from "./portfolio/TradingReportsTab";

interface PositionRow extends Record<string, unknown> {
  instrument_id: string;
  weight: number;
  market_value: number;
  quantity: number;
  avg_cost: number;
  unrealized_pnl: number;
  side: string;
}

interface RiskPositionRow extends Record<string, unknown> {
  instrument_id: string;
  weight: number;
  vol_20d: number | null;
  vol_60d: number | null;
  beta: number | null;
  fragility_score: number | null;
  last_risk_action: string | null;
}

type Tab = "overview" | "reports";

function fmtUsd(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtPct(n: number | null | undefined): string {
  if (n == null) return "—";
  return `${(n * 100).toFixed(2)}%`;
}

const TIP = {
  nlv: "Total portfolio value (cash + positions). Your account's mark-to-market worth.",
  cash: "Available cash not allocated to any position.",
  unrealPnl: "Sum of unrealized profit/loss across all open positions.",
  topWeight: "Weight of the largest single position. High concentration = higher single-name risk.",
  pnlHistory: "Unrealized P&L for each position over time. Shows how each holding's profit/loss has evolved.",
  positions: "All open positions with quantity, average cost, current value, weight, and unrealized P&L.",
  weights: "Visual breakdown of how your capital is allocated across positions.",
  portVol: "Annualized portfolio volatility (20-day). Measures how much daily returns vary, scaled to a year.",
  var95: "Value at Risk (95%, 1-day). The maximum expected daily loss 95% of the time, in dollar terms.",
  es95: "Expected Shortfall / CVaR. Average loss in the worst 5% of days. Worse than VaR — shows tail risk.",
  hhi: "Herfindahl-Hirschman Index. Concentration measure: 1.0 = single position, ~0.33 = perfectly spread across 3.",
  riskDetail: "Per-position risk metrics: 20d and 60d annualized vol, beta vs SPY, fragility score, and last risk engine action.",
};

export default function Portfolio() {
  const [tab, setTab] = useState<Tab>("overview");
  const { activePortfolioId } = usePortfolioContext();
  const portfolio = usePortfolio(activePortfolioId);
  const pnlHistory = usePositionPnlHistory(activePortfolioId);
  const riskComputed = usePortfolioRiskComputed(activePortfolioId);

  const port = (portfolio.data ?? {}) as Record<string, unknown>;
  const positions = (port.positions ?? []) as PositionRow[];
  const pnlData = (pnlHistory.data ?? []) as Record<string, unknown>[];
  const pnlInstruments = pnlData.length > 0
    ? Object.keys(pnlData[0]).filter((k) => k !== "date")
    : [];
  const risk = (riskComputed.data ?? {}) as Record<string, unknown>;
  const riskPositions = (risk.positions ?? []) as RiskPositionRow[];

  const nlv = Number(port.net_liquidation_value ?? 0);
  const cash = Number(port.total_cash ?? 0);
  const totalPnl = positions.reduce((s, p) => s + Number(p.unrealized_pnl ?? 0), 0);
  const topWeight = positions.length > 0
    ? Math.max(...positions.map((p) => Math.abs(Number(p.weight ?? 0))))
    : 0;

  // Position weight chart data
  const weightData = positions
    .map((p) => ({
      name: String(p.instrument_id).replace(".US", ""),
      weight: +(Number(p.weight ?? 0) * 100).toFixed(2),
    }))
    .sort((a, b) => b.weight - a.weight);

  const posCols: Column<PositionRow>[] = [
    { key: "instrument_id", label: "Instrument" },
    { key: "side", label: "Side", render: (r) => <StatusBadge label={String(r.side ?? (Number(r.quantity) >= 0 ? "LONG" : "SHORT"))} variant={Number(r.quantity) >= 0 ? "positive" : "negative"} /> },
    { key: "quantity", label: "Qty", align: "right", render: (r) => String(Number(r.quantity)) },
    { key: "avg_cost", label: "Avg Cost", align: "right", render: (r) => r.avg_cost != null ? `$${Number(r.avg_cost).toFixed(2)}` : "—" },
    { key: "market_value", label: "Mkt Value", align: "right", render: (r) => fmtUsd(Number(r.market_value)) },
    { key: "weight", label: "Weight", align: "right", render: (r) => `${(Number(r.weight) * 100).toFixed(2)}%` },
    { key: "unrealized_pnl", label: "Unreal P&L", align: "right", render: (r) => {
      const v = Number(r.unrealized_pnl ?? 0);
      const cls = v > 0 ? "text-positive" : v < 0 ? "text-negative" : "text-muted";
      return <span className={cls}>{fmtUsd(v)}</span>;
    }},
  ];

  const tabs: { key: Tab; label: string }[] = [
    { key: "overview", label: "Overview" },
    { key: "reports", label: "Reports" },
  ];

  return (
    <div className="space-y-4">
      <PageHeader title="Portfolio & Risk" subtitle={`${positions.length} positions`} onRefresh={() => { portfolio.refetch(); pnlHistory.refetch(); riskComputed.refetch(); }} />

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-border-dim">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-3 py-1.5 text-xs font-medium transition-colors ${
              tab === t.key
                ? "border-b-2 border-accent text-accent"
                : "text-muted hover:text-zinc-300"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "overview" && (
        <>
          {/* KPIs */}
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <KpiCard label="Net Liquidation" value={nlv > 0 ? fmtUsd(nlv) : "—"} tooltip={TIP.nlv} />
            <KpiCard label="Cash" value={fmtUsd(cash)} tooltip={TIP.cash} />
            <KpiCard
              label="Unrealized P&L"
              value={fmtUsd(totalPnl)}
              sentiment={totalPnl > 0 ? "positive" : totalPnl < 0 ? "negative" : "neutral"}
              tooltip={TIP.unrealPnl}
            />
            <KpiCard
              label="Top Position"
              value={topWeight > 0 ? `${(topWeight * 100).toFixed(1)}%` : "—"}
              sentiment={topWeight > 0.4 ? "warning" : "neutral"}
              tooltip={TIP.topWeight}
            />
          </div>

          {/* Risk KPIs */}
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <KpiCard
              label="Portfolio Vol (20d)"
              value={risk.portfolio_vol_20d != null ? fmtPct(Number(risk.portfolio_vol_20d)) : "—"}
              sentiment={Number(risk.portfolio_vol_20d ?? 0) > 0.3 ? "warning" : "neutral"}
              tooltip={TIP.portVol}
            />
            <KpiCard
              label="VaR 95% (1d)"
              value={risk.var_95 != null ? fmtUsd(Math.abs(Number(risk.var_95))) : "—"}
              sentiment="warning"
              tooltip={TIP.var95}
            />
            <KpiCard
              label="Exp. Shortfall"
              value={risk.expected_shortfall != null ? fmtUsd(Math.abs(Number(risk.expected_shortfall))) : "—"}
              sentiment="negative"
              tooltip={TIP.es95}
            />
            <KpiCard
              label="Concentration (HHI)"
              value={risk.hhi != null ? Number(risk.hhi).toFixed(3) : "—"}
              sentiment={Number(risk.hhi ?? 0) > 0.5 ? "warning" : "neutral"}
              tooltip={TIP.hhi}
            />
          </div>

          {/* Regime badge */}
          {risk.regime && (
            <div className="flex items-center gap-2 text-xs text-muted">
              <span>Market Regime:</span>
              <StatusBadge
                label={String(risk.regime)}
                variant={String(risk.regime) === "NEUTRAL" ? "neutral" : String(risk.regime).includes("EXPANSION") ? "positive" : "negative"}
              />
              {risk.regime_confidence != null && (
                <span className="text-zinc-500">({(Number(risk.regime_confidence) * 100).toFixed(0)}% confidence)</span>
              )}
            </div>
          )}

          {/* Position P&L History */}
          <Panel
            title="Position P&L History"
            tooltip={TIP.pnlHistory}
          >
            {pnlData.length > 0 ? (
              <LineChart
                data={pnlData}
                xKey="date"
                yKeys={pnlInstruments}
                height={280}
                labels={Object.fromEntries(pnlInstruments.map((k) => [k, k.replace(".US", "")]))}
                xTickFormatter={fmtDateTick}
              />
            ) : (
              <div className="flex h-40 items-center justify-center text-xs text-muted">
                No position history yet
              </div>
            )}
          </Panel>

          {/* Positions + Weights grid */}
          <div className="grid gap-4 lg:grid-cols-3">
            <Panel title="Positions" className="lg:col-span-2" tooltip={TIP.positions}>
              <DataTable columns={posCols} data={positions} pageSize={25} emptyMessage="No positions" />
            </Panel>

            <Panel title="Position Weights" tooltip={TIP.weights}>
              {weightData.length > 0 ? (
                <BarChart
                  data={weightData}
                  xKey="name"
                  yKeys={["weight"]}
                  height={Math.max(160, weightData.length * 50)}
                  labels={{ weight: "Weight %" }}
                />
              ) : (
                <div className="flex h-40 items-center justify-center text-xs text-muted">No positions</div>
              )}
            </Panel>
          </div>

          {/* Per-Position Risk Detail */}
          {riskPositions.length > 0 && (
            <Panel title="Position Risk Detail" tooltip={TIP.riskDetail}>
              <DataTable
                columns={[
                  { key: "instrument_id", label: "Instrument" },
                  { key: "weight", label: "Weight", align: "right" as const, render: (r: RiskPositionRow) => fmtPct(r.weight) },
                  { key: "vol_20d", label: "Vol 20d", align: "right" as const, render: (r: RiskPositionRow) => fmtPct(r.vol_20d) },
                  { key: "vol_60d", label: "Vol 60d", align: "right" as const, render: (r: RiskPositionRow) => fmtPct(r.vol_60d) },
                  { key: "beta", label: "Beta (SPY)", align: "right" as const, render: (r: RiskPositionRow) => r.beta != null ? r.beta.toFixed(2) : "—" },
                  { key: "fragility_score", label: "Fragility", align: "right" as const, render: (r: RiskPositionRow) => {
                    if (r.fragility_score == null) return "—";
                    const v = r.fragility_score;
                    const cls = v > 0.3 ? "text-negative" : v > 0.15 ? "text-yellow-400" : "text-positive";
                    return <span className={cls}>{v.toFixed(3)}</span>;
                  }},
                  { key: "last_risk_action", label: "Risk Action", render: (r: RiskPositionRow) => {
                    if (!r.last_risk_action) return "—";
                    return <StatusBadge label={r.last_risk_action} variant={r.last_risk_action === "OK" ? "positive" : "warning"} />;
                  }},
                ] as Column<RiskPositionRow>[]}
                data={riskPositions}
                compact
                emptyMessage="No risk data"
              />
            </Panel>
          )}
        </>
      )}

      {tab === "reports" && <TradingReportsTab />}
    </div>
  );
}
