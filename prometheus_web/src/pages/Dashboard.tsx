import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import { SeverityBadge } from "../components/SeverityBadge";
import { LineChart, ZOOM_STEPS, ZOOM_LABELS, fmtDateTick, ChartZoomBar } from "../components/Charts";
import {
  useOverview,
  usePortfolio,
  usePortfolioEquity,
  usePipelines,
  useRegime,
  useStability,
  useTradingReports,
  useIntelBriefs,
  useSystemLogs,
  useDivergenceSignals,
  useConvergenceSignals,
  useCompoundPressure,
  usePortfolioGeoRisk,
  useRiskDampener,
} from "../api/hooks";
import { usePortfolioContext } from "../context/PortfolioContext";

// ── Types ────────────────────────────────────────────────

interface PortfolioRow extends Record<string, unknown> {
  instrument_id: string;
  weight: number;
  market_value: number;
  quantity: number;
  avg_cost: number;
  unrealized_pnl: number;
  side: string;
}

interface PipelineRow extends Record<string, unknown> {
  market_id: string;
  state: string;
  next_run: string;
}

interface ReportRow extends Record<string, unknown> {
  id: string;
  report_type: string;
  generated_at: string;
  title: string;
  summary: string;
}

interface IntelRow extends Record<string, unknown> {
  id: string;
  brief_type: string;
  severity: string;
  domain: string;
  title: string;
  created_at: string;
}

interface LogRow extends Record<string, unknown> {
  timestamp: string;
  level: string;
  category: string;
  message: string;
}

interface DivergenceSignalRow {
  signal_id: string;
  as_of_date: string;
  entity_type: string;
  entity_id: string;
  behavioral_score: number;
  narrative_score: number;
  divergence: number;
  abs_divergence: number;
  direction: string;
  severity: "NONE" | "MILD" | "SIGNIFICANT" | "EXTREME";
  trading_signal: "NONE" | "FADE_NARRATIVE" | "FRONT_RUN_REALITY";
  decision_id: string | null;
  computed_at: string;
  rationale: string | null;
}

interface ConvergenceSignalRow {
  signal_id: string;
  entity_type: string;
  entity_id: string;
  days_to_hard_deadline: number | null;
  hard_deadline_reason: string | null;
  days_to_soft_signal: number | null;
  soft_signal_type: string | null;
  estimated_convergence_days: number | null;
  convergence_window_min: number | null;
  convergence_window_max: number | null;
  confidence: number;
  strategy: string | null;
  entry_windows: Array<{ label?: string; days_from_now?: number; allocation_pct?: number; trigger?: string }>;
  decision_id: string | null;
}

interface CompoundPressureRow {
  alert_id: string;
  target_entity_type: string;
  target_entity_id: string;
  total_pressure_points: number;
  pressure_points_moved: number;
  cluster_days: number;
  encirclement_score: number;
  severity: "LOW" | "MODERATE" | "HIGH" | "CRITICAL";
  likely_orchestrators: Array<Record<string, unknown>>;
  decision_id: string | null;
}

interface PortfolioGeoRiskRow {
  portfolio_id: string;
  as_of_date: string;
  overall_risk_score: number;
  conflict_risk: number;
  chokepoint_risk: number;
  sovereign_risk: number;
  sector_risk: number;
  ticker_count: number;
  decision_id: string | null;
}

interface RiskDampenerRow {
  portfolio_id: string;
  strategy_id: string;
  dampener: number;
  base_max_weight: number;
  effective_max_weight: number;
  overall_geo_risk: number | null;
  compound_severities: string[];
  exposed_isos: string[];
  strategy_disabled: boolean;
  strategy_floor: number;
}

// ── Column definitions ───────────────────────────────────

const positionCols: Column<PortfolioRow>[] = [
  { key: "instrument_id", label: "Instrument" },
  {
    key: "side",
    label: "Side",
    render: (r) => (
      <StatusBadge
        label={String(r.side ?? (Number(r.quantity) >= 0 ? "LONG" : "SHORT"))}
        variant={Number(r.quantity) >= 0 ? "positive" : "negative"}
      />
    ),
  },
  {
    key: "quantity",
    label: "Qty",
    align: "right",
    render: (r) => fmtQty(Number(r.quantity)),
  },
  {
    key: "avg_cost",
    label: "Avg Cost",
    align: "right",
    render: (r) => (r.avg_cost != null ? `$${Number(r.avg_cost).toFixed(2)}` : "—"),
  },
  {
    key: "market_value",
    label: "Mkt Value",
    align: "right",
    render: (r) => fmtUsd(Number(r.market_value)),
  },
  {
    key: "weight",
    label: "Weight",
    align: "right",
    render: (r) => `${(Number(r.weight) * 100).toFixed(2)}%`,
  },
  {
    key: "unrealized_pnl",
    label: "Unreal P&L",
    align: "right",
    render: (r) => {
      const v = Number(r.unrealized_pnl ?? 0);
      const cls = v > 0 ? "text-positive" : v < 0 ? "text-negative" : "text-muted";
      return <span className={cls}>{fmtUsd(v)}</span>;
    },
  },
];

const pipelineCols: Column<PipelineRow>[] = [
  { key: "market_id", label: "Market" },
  {
    key: "state",
    label: "State",
    render: (r) => (
      <StatusBadge
        label={String(r.state)}
        variant={r.state === "SESSION" ? "positive" : r.state === "PRE_OPEN" ? "warning" : "neutral"}
      />
    ),
  },
  { key: "next_run", label: "Next Run" },
];

// ── Helpers ──────────────────────────────────────────────

function fmtUsd(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

function fmtQty(n: number): string {
  if (n === 0) return "—";
  return n.toFixed(n % 1 === 0 ? 0 : 2);
}

function timeSince(iso: string): string {
  const seconds = (Date.now() - new Date(iso).getTime()) / 1000;
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

const typeLabels: Record<string, string> = {
  flash_alert: "FLASH",
  daily_sitrep: "SITREP",
  weekly_assessment: "WEEKLY",
  domain_report: "DOMAIN",
};

const domainColors: Record<string, string> = {
  nation: "text-blue-400",
  conflict: "text-red-400",
  maritime: "text-cyan-400",
  trade: "text-amber-400",
  synthesis: "text-violet-400",
};

// ── Tooltip text ─────────────────────────────────────────

const TIP = {
  nlv: "Total portfolio value (cash + positions). Your account's mark-to-market worth.",
  positions: "Number of open positions currently held in the portfolio.",
  netExposure: "(Long value − Short value) / NAV. Measures directional market risk. +100% = fully long, 0% = market neutral, −50% = net short betting on decline.",
  leverage: "(Long value + Short value) / NAV. Measures total risk amplification regardless of direction. 1.0x = no leverage, 1.5x = 50% extra exposure, 2.0x+ = significant amplification.",
  regime: "Market regime classification from ML model analyzing macro factors. CARRY = risk-on/yield-seeking, NEUTRAL = balanced conditions, CRISIS = risk-off/flight to safety.",
  stability: "Financial Stability Index (0–1) combining three components: Liquidity (1.0 = normal), Volatility (low = calm markets), Contagion (high = correlated selloffs spreading). Below 0.5 signals elevated systemic risk.",
  equity: "Portfolio NAV over time vs SPY benchmark (normalized to match starting value). Shows relative performance against the broad market.",
  positions_panel: "Current holdings with side, quantity, market value, weight, and unrealized P&L.",
  pipeline: "Market pipeline states and scheduled next runs. Shows which markets are actively being processed.",
  reports: "Most recent trading reports with date, type, and summary. Click to view full report.",
  intel: "Latest intelligence briefs from the AI briefing center. Shows severity, type, domain, and title.",
  logs: "Recent system log entries. Errors and warnings are highlighted for quick triage.",
  divergence:
    "Narrative-vs-Reality signals from Apatheon's intel layer. FADE_NARRATIVE = news overstates reality, short the headline. FRONT_RUN_REALITY = ships moving but media silent, long ahead of repricing. SIGNIFICANT/EXTREME signals are logged to engine_decisions for outcome tracking.",
  convergence:
    "Convergence-timing signals: when narrative will be forced to reprice based on physical depletion timelines, infrastructure lag, and leading indicators. Entry windows are laddered allocation suggestions; pair with the divergence direction for the trade.",
  compound_pressure:
    "Encirclement detection on watched sovereigns. HIGH/CRITICAL means multiple pressure points moved within a short window — consider trimming beta and raising tail-hedge sizing on names exposed to the target.",
  geo_risk:
    "Composite geopolitical risk for the live IBKR portfolio (0–100). Driven by conflict + chokepoint + sovereign + sector exposure. ≥40 logs a GEO_RISK decision; ≥60 = trim or hedge proactively.",
  risk_dampener:
    "Multiplicative shrink applied to per-name caps based on portfolio geo-risk + active compound-pressure alerts. Floors at the strategy's configured minimum (default 40%). Some strategies (hedge books) opt out — they're meant to concentrate during stress.",
};

// ── Component ────────────────────────────────────────────

export default function Dashboard() {
  const navigate = useNavigate();
  const overview = useOverview();
  const { activePortfolioId } = usePortfolioContext();
  const [eqZoomIdx, setEqZoomIdx] = useState(ZOOM_STEPS.length - 1);
  const eqZoomDays = ZOOM_STEPS[eqZoomIdx];

  const portfolio = usePortfolio(activePortfolioId);
  const equityCurve = usePortfolioEquity(activePortfolioId);
  const pipelines = usePipelines();
  const regime = useRegime();
  const stability = useStability();
  const tradingReports = useTradingReports();
  const intelBriefs = useIntelBriefs({ limit: 8 });
  const systemLogs = useSystemLogs({ level: "WARNING", limit: 20 });
  const divergence = useDivergenceSignals("SIGNIFICANT");
  const convergence = useConvergenceSignals(0.5);
  const compoundPressure = useCompoundPressure("MODERATE");
  const geoRisk = usePortfolioGeoRisk(activePortfolioId);
  const riskDampener = useRiskDampener(activePortfolioId);

  const ov = (overview.data ?? {}) as Record<string, unknown>;
  const port = (portfolio.data ?? {}) as Record<string, unknown>;
  const pips = ((pipelines.data ?? []) as Record<string, unknown>[]).map((p) => ({
    ...p,
    state: p.market_state ?? p.state,
    next_run: p.next_transition_time
      ? `${String(p.next_transition_state ?? "")} @ ${new Date(String(p.next_transition_time)).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`
      : "—",
  })) as PipelineRow[];
  const reg = (regime.data ?? {}) as Record<string, unknown>;
  const stab = (stability.data ?? {}) as Record<string, unknown>;

  const nlv = Number(port.net_liquidation_value ?? 0);
  const positions = ((port.positions ?? []) as PortfolioRow[]).slice(0, 30);
  const equityData = (equityCurve.data ?? []) as Record<string, unknown>[];
  const reports = ((tradingReports.data ?? []) as ReportRow[]).slice(0, 5);
  const briefs = ((Array.isArray(intelBriefs.data) ? intelBriefs.data : []) as IntelRow[]).slice(0, 8);
  const logs = ((Array.isArray(systemLogs.data) ? systemLogs.data : []) as LogRow[]).slice(0, 12);
  const divSignals = ((Array.isArray(divergence.data) ? divergence.data : []) as DivergenceSignalRow[]);
  const convSignals = ((Array.isArray(convergence.data) ? convergence.data : []) as ConvergenceSignalRow[]);
  const pressureAlerts = ((Array.isArray(compoundPressure.data) ? compoundPressure.data : []) as CompoundPressureRow[]);
  const geoRiskRow = (geoRisk.data ?? null) as PortfolioGeoRiskRow | null;
  const dampenerRow = (riskDampener.data ?? null) as RiskDampenerRow | null;

  return (
    <div className="space-y-4">
      <PageHeader
        title="Dashboard"
        subtitle="System overview"
        onRefresh={() => {
          overview.refetch();
          portfolio.refetch();
          pipelines.refetch();
          tradingReports.refetch();
          intelBriefs.refetch();
          systemLogs.refetch();
        }}
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <KpiCard
          label="Net Liquidation"
          value={nlv > 0 ? fmtUsd(nlv) : "—"}
          sentiment={"neutral"}
          tooltip={TIP.nlv}
        />
        <KpiCard
          label="Positions"
          value={String(positions.length)}
          tooltip={TIP.positions}
        />
        <KpiCard
          label="Net Exposure"
          value={ov.net_exposure != null ? `${(Number(ov.net_exposure) * 100).toFixed(1)}%` : "—"}
          tooltip={TIP.netExposure}
        />
        <KpiCard
          label="Leverage"
          value={ov.leverage != null ? `${Number(ov.leverage).toFixed(2)}x` : "—"}
          sentiment={Number(ov.leverage ?? 0) > 2 ? "warning" : "neutral"}
          tooltip={TIP.leverage}
        />
        <KpiCard
          label="Regime"
          value={String(reg.current_regime ?? "—")}
          sentiment={String(reg.current_regime ?? "").toLowerCase().includes("crisis") ? "negative" : "neutral"}
          tooltip={TIP.regime}
        />
        <KpiCard
          label="Stability"
          value={stab.current_index != null ? Number(stab.current_index).toFixed(3) : "—"}
          sentiment={Number(stab.current_index ?? 1) < 0.5 ? "warning" : "neutral"}
          tooltip={TIP.stability}
        />
      </div>

      {/* Equity Curve — portfolio vs benchmark */}
      <Panel
        title={`Equity Curve (${ZOOM_LABELS[eqZoomDays] ?? `${eqZoomDays}d`})`}
        actions={<ChartZoomBar zoomIdx={eqZoomIdx} setZoomIdx={setEqZoomIdx} />}
        tooltip={TIP.equity}
      >
        {equityData.length > 0 ? (
          <LineChart
            data={equityData.slice(-eqZoomDays)}
            xKey="date"
            yKeys={["portfolio", "benchmark"]}
            height={400}
            labels={{ portfolio: "Portfolio", benchmark: "SPY" }}
            xTickFormatter={fmtDateTick}
          />
        ) : (
          <div className="flex h-48 items-center justify-center text-xs text-muted">
            No equity history — sync IBKR data to populate
          </div>
        )}
      </Panel>

      {/* Positions + Pipeline grid */}
      <div className="grid gap-4 lg:grid-cols-3">
        <Panel title={`Positions (${positions.length})`} className="lg:col-span-2" tooltip={TIP.positions_panel}>
          <DataTable columns={positionCols} data={positions} compact pageSize={20} emptyMessage="No positions — run Sync or select a different portfolio" />
        </Panel>

        <Panel title="Pipeline Status" tooltip={TIP.pipeline}>
          <DataTable columns={pipelineCols} data={pips} compact pageSize={10} emptyMessage="No pipelines" />
        </Panel>
      </div>

      {/* Reports + Intelligence grid */}
      <div className="grid gap-4 lg:grid-cols-2">
        {/* Recent Reports */}
        <Panel
          title="Recent Reports"
          tooltip={TIP.reports}
          actions={
            <button
              className="text-[10px] text-accent hover:underline"
              onClick={() => navigate("/portfolio?tab=reports")}
            >
              View All →
            </button>
          }
        >
          {reports.length > 0 ? (
            <div className="space-y-1.5">
              {reports.map((r) => (
                <div
                  key={r.id}
                  className="flex items-center gap-3 rounded border border-border-dim/50 bg-surface-overlay/40 px-3 py-2 hover:bg-surface-overlay transition-colors cursor-pointer"
                  onClick={() => navigate(`/portfolio?tab=reports&report=${r.id}`)}
                >
                  <StatusBadge
                    label={r.report_type === "trading_daily" ? "DAILY" : r.report_type === "trading_weekly" ? "WEEKLY" : "CUSTOM"}
                    variant="info"
                  />
                  <span className="flex-1 truncate text-xs text-zinc-200">{r.title}</span>
                  <span className="shrink-0 text-[10px] text-muted">{timeSince(r.generated_at)}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">No trading reports yet</div>
          )}
        </Panel>

        {/* Intelligence Feed */}
        <Panel
          title="Intelligence Feed"
          tooltip={TIP.intel}
          actions={
            <button
              className="text-[10px] text-accent hover:underline"
              onClick={() => navigate("/intelligence")}
            >
              View All →
            </button>
          }
        >
          {briefs.length > 0 ? (
            <div className="space-y-1.5">
              {briefs.map((b) => (
                <div
                  key={b.id}
                  className="flex items-center gap-2 rounded border border-border-dim/50 bg-surface-overlay/40 px-3 py-2 hover:bg-surface-overlay transition-colors cursor-pointer"
                  onClick={() => navigate(`/intelligence?brief=${b.id}`)}
                >
                  <SeverityBadge severity={b.severity} />
                  <span className={`text-[9px] font-bold uppercase tracking-wider ${domainColors[b.domain] ?? "text-zinc-400"}`}>
                    {typeLabels[b.brief_type] ?? b.brief_type}
                  </span>
                  <span className="flex-1 truncate text-xs text-zinc-200">{b.title}</span>
                  <span className="shrink-0 text-[10px] text-muted">{timeSince(b.created_at)}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">No intelligence briefs yet</div>
          )}
        </Panel>
      </div>

      {/* Divergence Signals (Apatheon → Prometheus wiring) */}
      <Panel
        title={`Divergence Signals${divSignals.length > 0 ? ` (${divSignals.length})` : ""}`}
        tooltip={TIP.divergence}
      >
        {divSignals.length > 0 ? (
          <div className="space-y-1.5">
            {divSignals.slice(0, 8).map((d) => {
              const isFade = d.trading_signal === "FADE_NARRATIVE";
              const isFront = d.trading_signal === "FRONT_RUN_REALITY";
              const sigCls = isFade
                ? "border-amber-500/40 bg-amber-500/10 text-amber-300"
                : isFront
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300"
                  : "border-border-dim bg-surface-overlay text-zinc-400";
              const sevCls =
                d.severity === "EXTREME"
                  ? "bg-red-500/15 text-red-300 border-red-500/30"
                  : d.severity === "SIGNIFICANT"
                    ? "bg-orange-500/15 text-orange-300 border-orange-500/30"
                    : "bg-zinc-500/15 text-zinc-300 border-zinc-500/30";
              return (
                <div
                  key={d.signal_id}
                  className="flex items-center gap-3 rounded border border-border-dim/50 bg-surface-overlay/40 px-3 py-2"
                >
                  <span className={`shrink-0 rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider ${sevCls}`}>
                    {d.severity}
                  </span>
                  <span className="shrink-0 text-[10px] text-muted uppercase">{d.entity_type}</span>
                  <span className="font-mono text-xs text-zinc-200">{d.entity_id}</span>
                  <span className={`shrink-0 rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider ${sigCls}`}>
                    {isFade ? "FADE" : isFront ? "FRONT-RUN" : "—"}
                  </span>
                  <span className="flex-1 truncate text-[11px] text-zinc-300">
                    {d.rationale ?? `${d.direction} (Δ ${d.divergence.toFixed(2)})`}
                  </span>
                  <span className="shrink-0 font-mono text-[10px] text-muted">
                    b={d.behavioral_score.toFixed(2)} · n={d.narrative_score.toFixed(2)}
                  </span>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="py-6 text-center text-xs text-muted">
            No SIGNIFICANT/EXTREME divergence signals — narrative tracking reality.
          </div>
        )}
      </Panel>

      {/* Convergence + Compound Pressure grid */}
      <div className="grid gap-4 lg:grid-cols-2">
        {/* Convergence */}
        <Panel
          title={`Convergence Timing${convSignals.length > 0 ? ` (${convSignals.length})` : ""}`}
          tooltip={TIP.convergence}
        >
          {convSignals.length > 0 ? (
            <div className="space-y-2">
              {convSignals.slice(0, 5).map((c) => {
                const est = c.estimated_convergence_days;
                const win = c.convergence_window_min !== null && c.convergence_window_max !== null
                  ? `${Math.round(c.convergence_window_min)}–${Math.round(c.convergence_window_max)}d`
                  : "—";
                return (
                  <div
                    key={c.signal_id}
                    className="rounded border border-border-dim/50 bg-surface-overlay/40 p-2"
                  >
                    <div className="flex items-center gap-2">
                      <span className="shrink-0 text-[10px] uppercase text-muted">{c.entity_type}</span>
                      <span className="font-mono text-xs text-zinc-200">{c.entity_id}</span>
                      <span className="ml-auto font-mono text-[10px] text-emerald-300">
                        ~{est != null ? `${Math.round(est)}d` : "—"}
                      </span>
                      <span className="font-mono text-[10px] text-muted">({win})</span>
                      <span className="font-mono text-[10px] text-muted">conf {c.confidence.toFixed(2)}</span>
                    </div>
                    {c.strategy && (
                      <div className="mt-1 text-[11px] text-purple-200/90 leading-snug">
                        {c.strategy}
                      </div>
                    )}
                    {c.entry_windows?.length > 0 && (
                      <div className="mt-1 flex flex-wrap gap-1">
                        {c.entry_windows.slice(0, 4).map((w, i) => (
                          <span
                            key={i}
                            className="rounded bg-accent/10 border border-accent/30 px-1.5 py-0.5 text-[9px] font-semibold text-accent"
                          >
                            d{Math.round(w.days_from_now ?? 0)} · {w.allocation_pct ?? 0}%
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">
              No high-confidence convergence timelines.
            </div>
          )}
        </Panel>

        {/* Compound Pressure */}
        <Panel
          title={`Compound Pressure${pressureAlerts.length > 0 ? ` (${pressureAlerts.length})` : ""}`}
          tooltip={TIP.compound_pressure}
        >
          {pressureAlerts.length > 0 ? (
            <div className="space-y-1.5">
              {pressureAlerts.slice(0, 6).map((a) => {
                const sevCls =
                  a.severity === "CRITICAL"
                    ? "bg-red-500/15 text-red-300 border-red-500/30"
                    : a.severity === "HIGH"
                      ? "bg-orange-500/15 text-orange-300 border-orange-500/30"
                      : "bg-amber-500/15 text-amber-300 border-amber-500/30";
                return (
                  <div
                    key={a.alert_id}
                    className="flex items-center gap-3 rounded border border-border-dim/50 bg-surface-overlay/40 px-3 py-2"
                  >
                    <span className={`shrink-0 rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider ${sevCls}`}>
                      {a.severity}
                    </span>
                    <span className="font-mono text-xs text-zinc-200">{a.target_entity_id}</span>
                    <span className="text-[11px] text-zinc-300">
                      {a.pressure_points_moved}/{a.total_pressure_points} moved · {a.cluster_days.toFixed(1)}d cluster
                    </span>
                    <span className="ml-auto font-mono text-[10px] text-muted">
                      score {a.encirclement_score.toFixed(2)}
                    </span>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">
              No encirclement patterns above MODERATE.
            </div>
          )}
        </Panel>
      </div>

      {/* Portfolio Geo Risk */}
      {geoRiskRow && (
        <Panel title="Portfolio Geo Risk" tooltip={TIP.geo_risk}>
          <div className="flex flex-wrap items-center gap-4">
            <div>
              <div className="text-[10px] uppercase tracking-wider text-muted">Composite</div>
              <div className={`text-3xl font-bold ${
                geoRiskRow.overall_risk_score >= 70 ? "text-negative"
                  : geoRiskRow.overall_risk_score >= 40 ? "text-warning"
                    : "text-positive"
              }`}>
                {geoRiskRow.overall_risk_score.toFixed(0)}
              </div>
              <div className="text-[10px] text-muted">{geoRiskRow.ticker_count} positions</div>
            </div>
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 flex-1">
              {[
                { label: "Conflict", value: geoRiskRow.conflict_risk },
                { label: "Chokepoint", value: geoRiskRow.chokepoint_risk },
                { label: "Sovereign", value: geoRiskRow.sovereign_risk },
                { label: "Sector", value: geoRiskRow.sector_risk },
              ].map((d) => {
                const pct = Math.min(100, Math.max(0, d.value * 100));
                const color = pct > 70 ? "bg-negative" : pct > 40 ? "bg-warning" : "bg-positive";
                return (
                  <div key={d.label} className="rounded border border-border-dim bg-surface-overlay px-3 py-2">
                    <div className="text-[10px] uppercase text-muted">{d.label}</div>
                    <div className="mt-0.5 flex items-center gap-2">
                      <div className="h-1.5 flex-1 rounded-full bg-zinc-800 overflow-hidden">
                        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
                      </div>
                      <span className="font-mono text-xs text-zinc-200">{pct.toFixed(0)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </Panel>
      )}

      {/* Risk Dampener — shows why per-name caps were reduced today */}
      {dampenerRow && (dampenerRow.dampener < 0.999 || dampenerRow.strategy_disabled) && (
        <Panel
          title={`Risk Dampener — ${dampenerRow.strategy_id}`}
          tooltip={TIP.risk_dampener}
        >
          {dampenerRow.strategy_disabled ? (
            <div className="text-xs text-muted">
              Dampener disabled for <span className="font-mono text-zinc-200">{dampenerRow.strategy_id}</span>{" "}
              — this strategy is meant to concentrate during stress (hedge book).
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-6">
              <div>
                <div className="text-[10px] uppercase tracking-wider text-muted">Multiplier</div>
                <div className={`text-3xl font-bold ${
                  dampenerRow.dampener <= 0.55 ? "text-negative"
                    : dampenerRow.dampener <= 0.85 ? "text-warning"
                      : "text-positive"
                }`}>
                  {(dampenerRow.dampener * 100).toFixed(0)}%
                </div>
                <div className="text-[10px] text-muted">
                  floor {(dampenerRow.strategy_floor * 100).toFixed(0)}%
                </div>
              </div>
              <div>
                <div className="text-[10px] uppercase tracking-wider text-muted">Per-name cap</div>
                <div className="text-sm font-mono">
                  <span className="text-muted">{(dampenerRow.base_max_weight * 100).toFixed(2)}%</span>
                  <span className="mx-1.5 text-muted">→</span>
                  <span className="font-bold text-zinc-200">
                    {(dampenerRow.effective_max_weight * 100).toFixed(2)}%
                  </span>
                </div>
              </div>
              <div className="flex-1 min-w-[200px]">
                <div className="text-[10px] uppercase tracking-wider text-muted mb-1">Drivers</div>
                <div className="flex flex-wrap items-center gap-2 text-[11px]">
                  {dampenerRow.overall_geo_risk != null && (
                    <span className="rounded border border-border-dim bg-surface-overlay px-2 py-0.5">
                      <span className="text-muted">geo</span>{" "}
                      <span className={`font-bold ${
                        dampenerRow.overall_geo_risk >= 70 ? "text-negative"
                          : dampenerRow.overall_geo_risk >= 40 ? "text-warning"
                            : "text-positive"
                      }`}>
                        {dampenerRow.overall_geo_risk.toFixed(0)}
                      </span>
                    </span>
                  )}
                  {dampenerRow.compound_severities.length > 0 ? (
                    dampenerRow.compound_severities.map((s, i) => (
                      <span
                        key={i}
                        className={`rounded border px-2 py-0.5 font-bold uppercase tracking-wider text-[10px] ${
                          s === "CRITICAL"
                            ? "border-red-500/40 bg-red-500/10 text-red-300"
                            : "border-orange-500/40 bg-orange-500/10 text-orange-300"
                        }`}
                      >
                        compound {s}
                      </span>
                    ))
                  ) : (
                    <span className="rounded border border-border-dim bg-surface-overlay px-2 py-0.5 text-muted">
                      no active compound pressure
                    </span>
                  )}
                  {dampenerRow.exposed_isos.length > 0 && (
                    <span className="rounded border border-border-dim bg-surface-overlay px-2 py-0.5 text-[10px] text-muted">
                      exposed: {dampenerRow.exposed_isos.slice(0, 4).join(" · ")}
                      {dampenerRow.exposed_isos.length > 4 && ` +${dampenerRow.exposed_isos.length - 4}`}
                    </span>
                  )}
                </div>
              </div>
            </div>
          )}
        </Panel>
      )}

      {/* System Log */}
      <Panel title="System Log" tooltip={TIP.logs}>
        {logs.length > 0 ? (
          <div className="space-y-0.5 font-mono text-[11px]">
            {logs.map((l, i) => {
              const isErr = l.level === "ERROR" || l.level === "CRITICAL";
              const isWarn = l.level === "WARNING";
              const cls = isErr ? "text-red-400" : isWarn ? "text-amber-400" : "text-zinc-400";
              return (
                <div key={i} className={`flex gap-2 ${cls}`}>
                  <span className="shrink-0 text-muted">{String(l.timestamp).slice(11, 19)}</span>
                  <span className={`shrink-0 w-12 font-semibold ${isErr ? "text-red-400" : isWarn ? "text-amber-400" : "text-zinc-500"}`}>
                    {l.level?.slice(0, 4)}
                  </span>
                  <span className="truncate">{l.message}</span>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="py-6 text-center text-xs text-muted">No log entries</div>
        )}
      </Panel>
    </div>
  );
}
