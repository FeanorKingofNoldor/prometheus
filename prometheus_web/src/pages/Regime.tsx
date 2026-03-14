import { useState, useMemo } from "react";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import { LineChart, fmtDateTick, ZOOM_STEPS, ChartZoomBar } from "../components/Charts";
import {
  useRegime,
  useStability,
  usePipelines,
  usePipeline,
  useMarketOverview,
} from "../api/hooks";

// ── Types ───────────────────────────────────────────────

interface HistoryRow {
  date: string;
  regime: string;
  confidence: number;
}

interface StabilityRow extends Record<string, unknown> {
  date: string;
  index: number;
  liquidity: number;
  volatility: number;
  contagion: number;
}

interface PipelineRow extends Record<string, unknown> {
  market_id: string;
  market_state: string;
  as_of_date: string;
  next_transition_state: string | null;
  next_transition_time: string | null;
  jobs: Record<string, unknown>[];
}

interface MarketData extends Record<string, unknown> {
  vix_current: number | null;
  vix_ma20: number | null;
  vix_percentile_1y: number | null;
  vix_history: { date: string; close: number }[];
  spy_current: number | null;
  spy_ma50: number | null;
  spy_ma200: number | null;
  spy_pct_from_high: number | null;
  spy_history: { date: string; close: number }[];
  breadth_above_50d: number | null;
  breadth_above_200d: number | null;
  breadth_total: number;
  hyg_current: number | null;
  hyg_ma200: number | null;
  hyg_relative_strength: number | null;
  fear_greed_score: number | null;
  fear_greed_label: string | null;
  fg_vix_component: number | null;
  fg_momentum_component: number | null;
  fg_breadth_component: number | null;
  fg_credit_component: number | null;
  regime_transitions: { date: string; from: string; to: string }[];
}

// ── Helpers ─────────────────────────────────────────────

const REGIME_COLORS: Record<string, string> = {
  NEUTRAL: "#a1a1aa",
  CARRY: "#facc15",
  EXPANSION: "#22c55e",
  STABLE_EXPANSION: "#22c55e",
  CONTRACTION: "#ef4444",
  CRISIS: "#ef4444",
  RISK_OFF: "#f97316",
  BEAR: "#ef4444",
  BULL: "#22c55e",
};

function regimeSentiment(r: string): "positive" | "negative" | "neutral" | "warning" {
  const l = r.toLowerCase();
  if (l.includes("crisis") || l.includes("bear") || l.includes("contraction")) return "negative";
  if (l.includes("bull") || l.includes("expansion")) return "positive";
  if (l.includes("carry") || l.includes("risk_off")) return "warning";
  return "neutral";
}

function fmtTs(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("en-US", {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false,
  });
}

function fgColor(score: number | null): string {
  if (score == null) return "#a1a1aa";
  if (score <= 20) return "#ef4444";
  if (score <= 40) return "#f97316";
  if (score <= 60) return "#eab308";
  if (score <= 80) return "#22c55e";
  return "#3b82f6";
}

function fgSentiment(score: number | null): "positive" | "negative" | "neutral" | "warning" {
  if (score == null) return "neutral";
  if (score <= 25) return "negative";
  if (score <= 45) return "warning";
  if (score <= 60) return "neutral";
  return "positive";
}

// ── Tooltips ────────────────────────────────────────────

const TIP = {
  regime: "Current market regime classification from the Regime Engine. Regimes drive allocation and risk policy.",
  stability: "Composite stability score (0–1). Below 0.5 = unstable. Combines liquidity, volatility, and contagion.",
  confidence: "Model confidence in the current regime classification (0–100%).",
  contagion: "Cross-asset contagion component. Measures how correlated sell-offs are across instruments.",
  regimeChart: "Regime confidence over time. Transition badges below show when the regime classification changed.",
  stabilityChart: "Stability index and its components over time. Index = weighted blend of liquidity, volatility, and contagion.",
  pipelines: "Pipeline status for each market. Shows current market state, scheduled transitions, and job execution status.",
  vix: "CBOE Volatility Index — measures 30-day expected S&P 500 volatility. Higher = more fear. Shows current level, 20d MA, and 1-year percentile rank.",
  fearGreed: "Composite Fear & Greed score (0–100). Combines VIX percentile, SPY momentum, market breadth, and credit spreads. 0 = Extreme Fear, 100 = Extreme Greed.",
  momentum: "SPY price relative to its 125-day moving average. Above = bullish momentum, below = bearish.",
  breadth: "Market breadth — percentage of S&P 500 constituents trading above their 50-day and 200-day moving averages.",
  credit: "HYG high-yield bond ETF relative to its 200-day MA. Above 1.0 = credit conditions normal (greed), below = stress (fear).",
  marketPulse: "Key market indicators computed from historical price data. VIX, momentum, breadth, and credit spread signals.",
  transitions: "All regime transitions detected by the engine. Most recent at top.",
};

// ── Fear & Greed Gauge ─────────────────────────────────

function FearGreedGauge({ score, label, components }: {
  score: number | null;
  label: string | null;
  components: { name: string; value: number | null; label: string }[];
}) {
  if (score == null) {
    return (
      <div className="flex h-28 items-center justify-center text-xs text-muted">
        No data — market overview endpoint not available
      </div>
    );
  }

  const segments = [
    { from: 0, to: 20, color: "#ef4444", label: "Extreme Fear" },
    { from: 20, to: 40, color: "#f97316", label: "Fear" },
    { from: 40, to: 60, color: "#eab308", label: "Neutral" },
    { from: 60, to: 80, color: "#22c55e", label: "Greed" },
    { from: 80, to: 100, color: "#3b82f6", label: "Extreme Greed" },
  ];

  return (
    <div>
      {/* Score + label */}
      <div className="mb-3 flex items-baseline gap-2">
        <span className="text-3xl font-bold" style={{ color: fgColor(score) }}>
          {Math.round(score)}
        </span>
        <span className="text-sm font-medium" style={{ color: fgColor(score) }}>
          {label}
        </span>
      </div>

      {/* Gauge bar */}
      <div className="relative h-5 w-full overflow-hidden rounded-full">
        <div className="flex h-full">
          {segments.map((s) => (
            <div
              key={s.label}
              className="h-full"
              style={{ width: `${s.to - s.from}%`, backgroundColor: s.color, opacity: 0.6 }}
            />
          ))}
        </div>
        {/* Needle */}
        <div
          className="absolute top-0 h-full w-0.5 bg-white shadow-[0_0_4px_rgba(255,255,255,0.6)]"
          style={{ left: `${score}%`, transform: "translateX(-50%)" }}
        />
      </div>

      {/* Segment labels */}
      <div className="mt-1 flex text-[9px] text-muted">
        {segments.map((s) => (
          <span key={s.label} style={{ width: `${s.to - s.from}%` }} className="text-center">
            {s.label}
          </span>
        ))}
      </div>

      {/* Component breakdown */}
      <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5">
        {components.map((c) => (
          <div key={c.name} className="flex items-center justify-between text-xs">
            <span className="text-muted">{c.label}</span>
            <span className="font-medium" style={{ color: fgColor(c.value) }}>
              {c.value != null ? Math.round(c.value) : "—"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Market Pulse Card ──────────────────────────────────

function PulseCard({ title, value, sub, sentiment }: {
  title: string;
  value: string;
  sub?: string;
  sentiment?: "positive" | "negative" | "neutral" | "warning";
}) {
  const colors = {
    positive: "text-positive",
    negative: "text-negative",
    neutral: "text-zinc-100",
    warning: "text-warning",
  };

  return (
    <div className="rounded-md border border-border-dim bg-surface-overlay p-2.5">
      <div className="text-[10px] uppercase tracking-wider text-muted">{title}</div>
      <div className={`mt-0.5 text-base font-semibold ${colors[sentiment ?? "neutral"]}`}>{value}</div>
      {sub ? <div className="mt-0.5 text-[10px] text-muted">{sub}</div> : null}
    </div>
  );
}

// ── Component ───────────────────────────────────────────

export default function Regime() {
  const regime = useRegime();
  const stability = useStability();
  const pipelines = usePipelines();
  const market = useMarketOverview();

  const reg = (regime.data ?? {}) as Record<string, unknown>;
  const stab = (stability.data ?? {}) as Record<string, unknown>;
  const pips = (pipelines.data ?? []) as PipelineRow[];
  const mkt = (market.data ?? {}) as MarketData;

  const regimeHistory = (reg.history ?? []) as HistoryRow[];
  const stabilityHistory = (stab.history ?? []) as StabilityRow[];

  const currentRegime = String(reg.current_regime ?? "—");
  const currentConf = reg.confidence != null ? Number(reg.confidence) : null;
  const currentStab = stab.current_index != null ? Number(stab.current_index) : null;
  const currentContagion = stab.contagion_component != null ? Number(stab.contagion_component) : null;

  // Regime chart data
  const regimeChartData = useMemo(() => {
    if (regimeHistory.length === 0) return [];
    return regimeHistory.map((h) => ({
      date: h.date,
      confidence: +(h.confidence * 100).toFixed(1),
    }));
  }, [regimeHistory]);

  // Detect regime transitions from history (for chart badges)
  const chartTransitions = useMemo(() => {
    const out: { date: string; from: string; to: string }[] = [];
    for (let i = 1; i < regimeHistory.length; i++) {
      if (regimeHistory[i].regime !== regimeHistory[i - 1].regime) {
        out.push({
          date: regimeHistory[i].date,
          from: regimeHistory[i - 1].regime,
          to: regimeHistory[i].regime,
        });
      }
    }
    return out;
  }, [regimeHistory]);

  // All-time transitions from market overview
  const allTransitions = (mkt.regime_transitions ?? []) as { date: string; from: string; to: string }[];

  // Zoom state
  const [regimeZoomIdx, setRegimeZoomIdx] = useState(ZOOM_STEPS.length - 1);
  const regimeZoomDays = ZOOM_STEPS[regimeZoomIdx];
  const [stabZoomIdx, setStabZoomIdx] = useState(ZOOM_STEPS.length - 1);
  const stabZoomDays = ZOOM_STEPS[stabZoomIdx];

  const [selectedMarket, setSelectedMarket] = useState("");
  const pipeline = usePipeline(selectedMarket);
  const pipeDetail = (pipeline.data ?? {}) as Record<string, unknown>;
  const pipeJobs = ((pipeDetail.jobs ?? []) as Record<string, unknown>[]);

  // Pipeline columns
  const pipCols: Column<PipelineRow>[] = [
    { key: "market_id", label: "Market" },
    {
      key: "market_state", label: "State",
      render: (r) => {
        const s = String(r.market_state);
        const v = s === "OPEN" ? "positive" as const : s === "CLOSED" || s === "HOLIDAY" ? "neutral" as const : s === "ERROR" ? "negative" as const : "warning" as const;
        return <StatusBadge label={s} variant={v} />;
      },
    },
    { key: "as_of_date", label: "As Of" },
    {
      key: "next_transition_state", label: "Next Transition",
      render: (r) => r.next_transition_state
        ? <span className="text-xs">{String(r.next_transition_state)} @ {fmtTs(r.next_transition_time as string)}</span>
        : "—",
    },
    {
      key: "jobs", label: "Jobs",
      render: (r) => {
        const jobs = (r.jobs ?? []) as Record<string, unknown>[];
        const done = jobs.filter((j) => j.last_run_status === "COMPLETED").length;
        const err = jobs.filter((j) => j.last_run_status === "FAILED").length;
        return (
          <span className="text-xs">
            {jobs.length} total
            {done > 0 && <span className="text-positive ml-1">({done} done)</span>}
            {err > 0 && <span className="text-negative ml-1">({err} failed)</span>}
          </span>
        );
      },
    },
  ];

  // SPY momentum sentiment
  const spyMomSentiment = (): "positive" | "negative" | "neutral" | "warning" => {
    if (mkt.spy_current == null || mkt.spy_ma50 == null) return "neutral";
    if (mkt.spy_current > (mkt.spy_ma50 ?? 0)) return "positive";
    return "negative";
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Regime & Market"
        subtitle={reg.as_of_date ? `As of ${reg.as_of_date}` : "Market regime, stability, and pipeline status"}
        onRefresh={() => { regime.refetch(); stability.refetch(); pipelines.refetch(); market.refetch(); }}
      />

      {/* KPI Cards — 8 across */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-8">
        <KpiCard
          label="Regime"
          value={currentRegime}
          sentiment={regimeSentiment(currentRegime)}
          tooltip={TIP.regime}
        />
        <KpiCard
          label="Confidence"
          value={currentConf != null ? `${(currentConf * 100).toFixed(1)}%` : "—"}
          sentiment={currentConf != null && currentConf < 0.6 ? "warning" : "neutral"}
          tooltip={TIP.confidence}
        />
        <KpiCard
          label="Stability"
          value={currentStab != null ? currentStab.toFixed(3) : "—"}
          sentiment={currentStab != null && currentStab < 0.5 ? "warning" : "neutral"}
          tooltip={TIP.stability}
        />
        <KpiCard
          label="VIX"
          value={mkt.vix_current != null ? mkt.vix_current.toFixed(1) : "—"}
          sentiment={
            mkt.vix_current != null && mkt.vix_current > 30 ? "negative"
              : mkt.vix_current != null && mkt.vix_current > 20 ? "warning"
              : "neutral"
          }
          delta={mkt.vix_ma20 != null ? `MA20: ${mkt.vix_ma20.toFixed(1)}` : undefined}
          tooltip={TIP.vix}
        />
        <KpiCard
          label="Fear & Greed"
          value={mkt.fear_greed_score != null ? `${Math.round(mkt.fear_greed_score)}` : "—"}
          sentiment={fgSentiment(mkt.fear_greed_score)}
          delta={mkt.fear_greed_label ?? undefined}
          tooltip={TIP.fearGreed}
        />
        <KpiCard
          label="SPY"
          value={mkt.spy_current != null ? `$${mkt.spy_current.toFixed(0)}` : "—"}
          sentiment={spyMomSentiment()}
          delta={mkt.spy_pct_from_high != null ? `${mkt.spy_pct_from_high.toFixed(1)}% from high` : undefined}
          tooltip={TIP.momentum}
        />
        <KpiCard
          label="Breadth 200d"
          value={mkt.breadth_above_200d != null ? `${mkt.breadth_above_200d.toFixed(0)}%` : "—"}
          sentiment={
            mkt.breadth_above_200d != null && mkt.breadth_above_200d < 40 ? "negative"
              : mkt.breadth_above_200d != null && mkt.breadth_above_200d < 60 ? "warning"
              : "neutral"
          }
          delta={mkt.breadth_total > 0 ? `${mkt.breadth_total} stocks` : undefined}
          tooltip={TIP.breadth}
        />
        <KpiCard
          label="Contagion"
          value={currentContagion != null ? currentContagion.toFixed(3) : "—"}
          sentiment={currentContagion != null && currentContagion > 0.7 ? "negative" : currentContagion != null && currentContagion > 0.5 ? "warning" : "neutral"}
          tooltip={TIP.contagion}
        />
      </div>

      {/* Fear & Greed + Market Pulse — side by side */}
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        <Panel title="Fear & Greed Index" tooltip={TIP.fearGreed}>
          <FearGreedGauge
            score={mkt.fear_greed_score}
            label={mkt.fear_greed_label}
            components={[
              { name: "vix", label: "VIX Sentiment", value: mkt.fg_vix_component },
              { name: "momentum", label: "Momentum", value: mkt.fg_momentum_component },
              { name: "breadth", label: "Market Breadth", value: mkt.fg_breadth_component },
              { name: "credit", label: "Credit Spread", value: mkt.fg_credit_component },
            ]}
          />
        </Panel>

        <Panel title="Market Pulse" tooltip={TIP.marketPulse}>
          <div className="grid grid-cols-2 gap-2">
            <PulseCard
              title="VIX Level"
              value={mkt.vix_current != null ? mkt.vix_current.toFixed(2) : "—"}
              sub={mkt.vix_percentile_1y != null ? `${mkt.vix_percentile_1y.toFixed(0)}th pctile (1Y)` : undefined}
              sentiment={
                mkt.vix_current != null && mkt.vix_current > 30 ? "negative"
                  : mkt.vix_current != null && mkt.vix_current > 20 ? "warning"
                  : "positive"
              }
            />
            <PulseCard
              title="SPY vs MAs"
              value={mkt.spy_current != null ? `$${mkt.spy_current.toFixed(2)}` : "—"}
              sub={
                mkt.spy_ma50 != null && mkt.spy_ma200 != null
                  ? `50d: $${mkt.spy_ma50.toFixed(0)} · 200d: $${mkt.spy_ma200.toFixed(0)}`
                  : undefined
              }
              sentiment={
                mkt.spy_current != null && mkt.spy_ma200 != null
                  ? mkt.spy_current > mkt.spy_ma200 ? "positive" : "negative"
                  : "neutral"
              }
            />
            <PulseCard
              title="Breadth (50d / 200d)"
              value={
                mkt.breadth_above_50d != null && mkt.breadth_above_200d != null
                  ? `${mkt.breadth_above_50d.toFixed(0)}% / ${mkt.breadth_above_200d.toFixed(0)}%`
                  : "—"
              }
              sub={mkt.breadth_total > 0 ? `${mkt.breadth_total} S&P constituents` : undefined}
              sentiment={
                mkt.breadth_above_200d != null
                  ? mkt.breadth_above_200d > 70 ? "positive" : mkt.breadth_above_200d < 40 ? "negative" : "warning"
                  : "neutral"
              }
            />
            <PulseCard
              title="Credit (HYG RS)"
              value={mkt.hyg_relative_strength != null ? mkt.hyg_relative_strength.toFixed(4) : "—"}
              sub={
                mkt.hyg_current != null && mkt.hyg_ma200 != null
                  ? `$${mkt.hyg_current.toFixed(2)} vs MA200 $${mkt.hyg_ma200.toFixed(2)}`
                  : undefined
              }
              sentiment={
                mkt.hyg_relative_strength != null
                  ? mkt.hyg_relative_strength >= 1.0 ? "positive" : mkt.hyg_relative_strength < 0.97 ? "negative" : "warning"
                  : "neutral"
              }
            />
          </div>
        </Panel>
      </div>

      {/* Charts — side by side */}
      <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
        {/* Regime History Chart */}
        <Panel
          title="Regime History"
          actions={<ChartZoomBar zoomIdx={regimeZoomIdx} setZoomIdx={setRegimeZoomIdx} />}
          tooltip={TIP.regimeChart}
        >
          {regimeChartData.length > 0 ? (
            <>
              <LineChart
                data={regimeChartData.slice(-regimeZoomDays)}
                xKey="date"
                yKeys={["confidence"]}
                height={220}
                labels={{ confidence: "Confidence %" }}
                xTickFormatter={fmtDateTick}
              />
              {chartTransitions.length > 0 && (
                <div className="mt-3 flex flex-wrap gap-2">
                  {chartTransitions.map((t, i) => (
                    <div key={i} className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-[10px]">
                      <span className="text-muted">{fmtDateTick(t.date)}</span>
                      {" "}
                      <span style={{ color: REGIME_COLORS[t.from] ?? "#a1a1aa" }}>{t.from}</span>
                      <span className="text-muted mx-1">→</span>
                      <span style={{ color: REGIME_COLORS[t.to] ?? "#a1a1aa" }}>{t.to}</span>
                    </div>
                  ))}
                </div>
              )}
            </>
          ) : (
            <div className="flex h-40 items-center justify-center text-xs text-muted">
              No regime history — run the regime engine to populate
            </div>
          )}
        </Panel>

        {/* Stability History Chart */}
        <Panel
          title="Stability Components"
          actions={<ChartZoomBar zoomIdx={stabZoomIdx} setZoomIdx={setStabZoomIdx} />}
          tooltip={TIP.stabilityChart}
        >
          {stabilityHistory.length > 0 ? (
            <LineChart
              data={stabilityHistory.slice(-stabZoomDays)}
              xKey="date"
              yKeys={["index", "volatility", "contagion", "liquidity"]}
              height={220}
              labels={{
                index: "Overall Index",
                volatility: "Volatility",
                contagion: "Contagion",
                liquidity: "Liquidity",
              }}
              xTickFormatter={fmtDateTick}
            />
          ) : (
            <div className="flex h-40 items-center justify-center text-xs text-muted">
              No stability history — run the stability engine to populate
            </div>
          )}
        </Panel>
      </div>

      {/* VIX + SPY mini charts — side by side */}
      {(mkt.vix_history?.length > 0 || mkt.spy_history?.length > 0) ? (
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
          {mkt.vix_history?.length > 0 ? (
            <Panel title="VIX (90d)" tooltip={TIP.vix}>
              <LineChart
                data={mkt.vix_history}
                xKey="date"
                yKeys={["close"]}
                height={180}
                labels={{ close: "VIX" }}
                xTickFormatter={fmtDateTick}
              />
            </Panel>
          ) : null}
          {mkt.spy_history?.length > 0 ? (
            <Panel title="SPY (90d)" tooltip={TIP.momentum}>
              <LineChart
                data={mkt.spy_history}
                xKey="date"
                yKeys={["close"]}
                height={180}
                labels={{ close: "SPY" }}
                xTickFormatter={fmtDateTick}
              />
            </Panel>
          ) : null}
        </div>
      ) : null}

      {/* Regime Transitions Timeline */}
      {allTransitions.length > 0 ? (
        <Panel title="Regime Transitions (All-Time)" tooltip={TIP.transitions}>
          <div className="max-h-52 overflow-y-auto">
            <div className="space-y-1">
              {allTransitions.map((t, i) => (
                <div
                  key={i}
                  className="flex items-center gap-3 rounded border border-border-dim bg-surface-overlay px-3 py-1.5 text-xs"
                >
                  <span className="min-w-[90px] text-muted">{fmtDateTick(t.date)}</span>
                  <span
                    className="min-w-[90px] font-medium"
                    style={{ color: REGIME_COLORS[t.from] ?? "#a1a1aa" }}
                  >
                    {t.from}
                  </span>
                  <span className="text-muted">→</span>
                  <span
                    className="min-w-[90px] font-medium"
                    style={{ color: REGIME_COLORS[t.to] ?? "#a1a1aa" }}
                  >
                    {t.to}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </Panel>
      ) : null}

      {/* Pipelines */}
      <Panel title="Market Pipelines" tooltip={TIP.pipelines}>
        <DataTable
          columns={pipCols}
          data={pips}
          compact
          onRowClick={(r) => setSelectedMarket(r.market_id === selectedMarket ? "" : r.market_id)}
          emptyMessage="No pipelines configured"
        />
      </Panel>

      {/* Pipeline job detail */}
      {selectedMarket && pipeJobs.length > 0 ? (
        <Panel title={`${selectedMarket} — Jobs (${pipeJobs.length})`}>
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            {pipeJobs.map((j, i) => {
              const status = String(j.last_run_status ?? "NOT_STARTED");
              const variant = status === "COMPLETED" ? "positive" as const
                : status === "FAILED" ? "negative" as const
                : status === "RUNNING" ? "warning" as const
                : "neutral" as const;
              return (
                <div key={i} className="rounded border border-border-dim bg-surface-overlay p-3 text-xs">
                  <div className="flex items-center justify-between">
                    <span className="font-medium text-zinc-200">{String(j.job_name)}</span>
                    <StatusBadge label={status} variant={variant} />
                  </div>
                  {j.latency_ms != null ? (
                    <div className="mt-1 text-[10px] text-muted">
                      Latency: {Number(j.latency_ms).toLocaleString()}ms
                      {j.slo_ms != null ? (
                        <span className={Number(j.latency_ms) > Number(j.slo_ms) ? " text-negative" : " text-positive"}>
                          {" "}(SLO: {Number(j.slo_ms).toLocaleString()}ms)
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                  {j.error_message ? (
                    <div className="mt-1 text-[10px] text-negative truncate" title={String(j.error_message)}>
                      {String(j.error_message)}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </Panel>
      ) : null}
    </div>
  );
}
