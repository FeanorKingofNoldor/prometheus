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
