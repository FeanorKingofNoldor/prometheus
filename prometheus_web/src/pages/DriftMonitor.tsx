import { useMemo, useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { KpiCard } from "../components/KpiCard";
import { DataTable, Column } from "../components/DataTable";
import { SeverityBadge } from "../components/SeverityBadge";
import { useDrift, type DriftRow } from "../api/hooks";

const HORIZONS = [0, 1, 5, 21, 63] as const;

function fmtDelta(v: number | null, digits = 2): string {
  if (v == null || Number.isNaN(v)) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${v.toFixed(digits)}`;
}

function fmtPct(v: number | null): string {
  if (v == null || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(2)}%`;
}

function fmtSharpe(v: number | null): string {
  if (v == null || Number.isNaN(v)) return "—";
  return v.toFixed(2);
}

interface DriftTableRow extends Record<string, unknown> {
  drift_id: number;
  strategy_id: string;
  horizon_days: number;
  n_live_outcomes: number;
  live_sharpe: number | null;
  backtest_sharpe: number | null;
  sharpe_delta: number | null;
  live_return: number | null;
  backtest_return: number | null;
  return_delta: number | null;
  live_max_drawdown: number | null;
  backtest_max_drawdown: number | null;
  max_drawdown_delta: number | null;
  severity: string;
  notes: string | null;
  as_of_date: string;
}

export default function DriftMonitor() {
  const [horizon, setHorizon] = useState<number | undefined>(undefined);
  const [strategyFilter, setStrategyFilter] = useState<string>("");
  const { data, isLoading, refetch } = useDrift({
    horizonDays: horizon,
    strategyId: strategyFilter || undefined,
    latestOnly: true,
    limit: 500,
  });

  const rows: DriftRow[] = data?.items ?? [];
  const counts = useMemo(() => {
    const c = { critical: 0, warning: 0, info: 0 };
    for (const r of rows) {
      if (r.severity === "critical") c.critical += 1;
      else if (r.severity === "warning") c.warning += 1;
      else c.info += 1;
    }
    return c;
  }, [rows]);

  const tableRows: DriftTableRow[] = rows.map((r) => ({ ...r }));

  const columns: Column<DriftTableRow>[] = [
    { key: "strategy_id", label: "Strategy" },
    {
      key: "horizon_days",
      label: "Horizon",
      align: "right",
      render: (r) => `${r.horizon_days}d`,
    },
    {
      key: "severity",
      label: "Severity",
      render: (r) => <SeverityBadge severity={r.severity} />,
    },
    {
      key: "n_live_outcomes",
      label: "Live N",
      align: "right",
      render: (r) => r.n_live_outcomes.toLocaleString(),
    },
    {
      key: "live_sharpe",
      label: "Live Sharpe",
      align: "right",
      render: (r) => fmtSharpe(r.live_sharpe),
    },
    {
      key: "backtest_sharpe",
      label: "BT Sharpe",
      align: "right",
      render: (r) => fmtSharpe(r.backtest_sharpe),
    },
    {
      key: "sharpe_delta",
      label: "Δ Sharpe",
      align: "right",
      render: (r) => {
        const v = r.sharpe_delta;
        const cls =
          v == null
            ? "text-muted"
            : v > 0.2
              ? "text-positive"
              : v < -0.2
                ? "text-negative"
                : "text-zinc-100";
        return <span className={cls}>{fmtDelta(v)}</span>;
      },
    },
    {
      key: "return_delta",
      label: "Δ Return",
      align: "right",
      render: (r) => fmtPct(r.return_delta),
    },
    {
      key: "max_drawdown_delta",
      label: "Δ MaxDD",
      align: "right",
      render: (r) => fmtPct(r.max_drawdown_delta),
    },
    {
      key: "as_of_date",
      label: "As of",
      render: (r) => r.as_of_date,
    },
    {
      key: "notes",
      label: "Notes",
      render: (r) => (
        <span className="text-xs text-muted" title={r.notes ?? ""}>
          {r.notes ?? "—"}
        </span>
      ),
    },
  ];

  return (
    <div>
      <PageHeader
        title="Drift Monitor"
        subtitle={
          data?.latest_as_of_date
            ? `Backtest-vs-live comparison · latest run ${data.latest_as_of_date}`
            : "Backtest-vs-live comparison"
        }
        onRefresh={() => refetch()}
      />

      <div className="mb-4 grid grid-cols-1 gap-3 sm:grid-cols-4">
        <KpiCard
          label="Strategies (Latest)"
          value={rows.length}
          tooltip="Distinct strategy/horizon rows in the latest drift run."
        />
        <KpiCard
          label="Critical"
          value={counts.critical}
          sentiment={counts.critical > 0 ? "negative" : "neutral"}
        />
        <KpiCard
          label="Warning"
          value={counts.warning}
          sentiment={counts.warning > 0 ? "warning" : "neutral"}
        />
        <KpiCard
          label="Info / OK"
          value={counts.info}
          sentiment={rows.length > 0 && counts.info === rows.length ? "positive" : "neutral"}
        />
      </div>

      <Panel
        title="Latest drift per strategy / horizon"
        tooltip="From migration 0101 backtest_live_drift. Severity buckets: |ΔSharpe|<0.2 info, 0.2–0.5 warning, ≥0.5 critical."
        actions={
          <div className="flex items-center gap-2 text-[10px]">
            <input
              type="text"
              value={strategyFilter}
              onChange={(e) => setStrategyFilter(e.target.value)}
              placeholder="Filter strategy"
              className="rounded border border-border-dim bg-surface px-2 py-0.5 text-xs text-zinc-100"
            />
            <select
              value={horizon ?? ""}
              onChange={(e) =>
                setHorizon(e.target.value ? Number(e.target.value) : undefined)
              }
              className="rounded border border-border-dim bg-surface px-2 py-0.5 text-xs text-zinc-100"
            >
              <option value="">All horizons</option>
              {HORIZONS.filter((h) => h > 0).map((h) => (
                <option key={h} value={h}>
                  {h}d
                </option>
              ))}
            </select>
          </div>
        }
      >
        {isLoading ? (
          <div className="py-8 text-center text-xs text-muted">Loading…</div>
        ) : tableRows.length === 0 ? (
          <div className="py-8 text-center text-xs text-muted">
            No drift rows yet. The daily orchestrator populates this table after
            it has at least 5 live outcomes for a strategy/horizon pair.
          </div>
        ) : (
          <DataTable columns={columns} data={tableRows} pageSize={25} />
        )}
      </Panel>
    </div>
  );
}
