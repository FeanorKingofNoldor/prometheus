import { useEffect, useState } from "react";
import { Brain, RefreshCw } from "lucide-react";
import { useOverview, useSyncData, useIbkrStatus } from "../api/hooks";
import { StatusBadge } from "../components/StatusBadge";
import { ConnectionLed } from "../components/ConnectionLed";
import { NotificationsBell } from "../components/NotificationsBell";
import { useOptionalIris } from "../context/IrisContext";
import { usePortfolioContext } from "../context/PortfolioContext";

interface SyncResult {
  job_id?: string;
  status?: string;
  message?: string;
  sources_requested?: string[];
  ibkr?: { status: string; positions?: number; account_keys?: number; error?: string };
  engines?: { status: string; row_counts?: Record<string, number>; error?: string };
}

const MODE_VARIANT: Record<string, "positive" | "negative" | "warning" | "info" | "neutral"> = {
  LIVE: "positive",
  PAPER: "warning",
  BACKTEST: "info",
};

function fmtUsd(n: number): string {
  if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

// Returns a short "(stale 5h)" / "(stale 2d)" suffix if the snapshot is
// older than the freshness threshold; otherwise returns an empty string.
// Threshold is intentionally tight: a paper/live book whose latest
// snapshot is older than 15 min is almost certainly out-of-sync with
// IBKR (broker is disconnected, daemon paused, or sync failed).
function staleSuffix(ts: string | null | undefined): string {
  if (!ts) return " (no snapshot)";
  const ageMs = Date.now() - new Date(ts).getTime();
  if (ageMs < 15 * 60_000) return "";
  const ageMin = Math.floor(ageMs / 60_000);
  if (ageMin < 60) return ` (stale ${ageMin}m)`;
  const ageH = Math.floor(ageMin / 60);
  if (ageH < 24) return ` (stale ${ageH}h)`;
  const ageD = Math.floor(ageH / 24);
  return ` (stale ${ageD}d)`;
}

const DEFAULT_SYNC_PORTFOLIO_ID = "IBKR_PAPER";
const SYNC_SOURCES = ["ibkr", "engines"];

function isIbkrPortfolioId(portfolioId: string): boolean {
  return portfolioId.startsWith("IBKR_");
}

export function TopBar() {
  const [now, setNow] = useState(new Date());
  const { data: overview } = useOverview();
  const ov = overview as Record<string, unknown> | undefined;
  const sync = useSyncData();
  const iris = useOptionalIris();
  const { data: ibkrRaw, isLoading: ibkrLoading } = useIbkrStatus();
  const ibkr = ibkrRaw as { status: string; mode: string; account: string; endpoints: { label: string; port: number; reachable: boolean; latency_ms?: number; error?: string }[] } | undefined;
  const [syncDetail, setSyncDetail] = useState<string | null>(null);
  const [lastSyncTarget, setLastSyncTarget] = useState<string>(DEFAULT_SYNC_PORTFOLIO_ID);
  const { activePortfolioId, setActivePortfolioId, tradingPortfolios, backtestPortfolios, activeMode } = usePortfolioContext();

  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  // When sync succeeds, extract detail from the job result
  useEffect(() => {
    if (sync.isSuccess && sync.data) {
      const d = sync.data as SyncResult;
      const parts: string[] = [];
      if (d.job_id) parts.push(`Job: ${d.job_id}`);
      if (lastSyncTarget) parts.push(`Target: ${lastSyncTarget}`);
      if (d.ibkr) {
        if (d.ibkr.status === "ok") {
          parts.push(`IBKR: ${d.ibkr.positions ?? 0} positions`);
        } else {
          parts.push(`IBKR: ${d.ibkr.error ?? "error"}`);
        }
      }
      if (d.engines?.row_counts) {
        const rc = d.engines.row_counts;
        parts.push(`DB: ${rc.positions_snapshots ?? "?"} snaps, ${rc.orders ?? "?"} orders`);
      }
      setSyncDetail(parts.join(" | ") || "Synced");
      // Auto-clear after 8s
      const timer = setTimeout(() => setSyncDetail(null), 8000);
      return () => clearTimeout(timer);
    }
  }, [sync.isSuccess, sync.data, lastSyncTarget]);

  useEffect(() => {
    if (sync.isError) {
      console.error("[prometheus/sync] Sync failed:", sync.error);
      setSyncDetail(null);
    }
  }, [sync.isError, sync.error]);

  return (
    <header className="flex h-12 items-center justify-between border-b border-border-dim bg-surface-raised px-4">
      <div className="flex items-center gap-3">
        {/* Global portfolio selector */}
        <select
          value={activePortfolioId}
          onChange={(e) => setActivePortfolioId(e.target.value)}
          className="rounded border border-border-dim bg-surface px-2 py-1 text-xs text-zinc-100 max-w-[220px]"
        >
          {tradingPortfolios.length > 0 && (
            <optgroup label="Trading">
              {tradingPortfolios.map((p) => (
                <option key={p.portfolio_id} value={p.portfolio_id}>
                  {p.portfolio_id} — {p.num_positions} pos — {fmtUsd(p.total_market_value)}{staleSuffix(p.latest_timestamp)}
                </option>
              ))}
            </optgroup>
          )}
          {backtestPortfolios.length > 0 && (
            <optgroup label="Backtests">
              {backtestPortfolios.map((p) => (
                <option key={p.portfolio_id} value={p.portfolio_id}>
                  {p.portfolio_id} — {p.num_positions} pos
                </option>
              ))}
            </optgroup>
          )}
          {tradingPortfolios.length === 0 && backtestPortfolios.length === 0 && (
            <option value={activePortfolioId}>{activePortfolioId}</option>
          )}
        </select>
        <StatusBadge
          label={activeMode}
          variant={MODE_VARIANT[activeMode] ?? "neutral"}
        />
        {Array.isArray(ov?.regimes) && (ov.regimes as Record<string, unknown>[]).length > 0 && (
          <span className="text-xs text-muted">
            Regime:{" "}
            <span className="text-zinc-100">{String((ov.regimes as Record<string, unknown>[])[0]?.regime_label ?? "—")}</span>
          </span>
        )}
      </div>
      <div className="flex items-center gap-4 text-xs text-muted">
        <ConnectionLed
          status={ibkrLoading ? "loading" : (ibkr?.status ?? "disconnected")}
          label={`IB ${ibkr?.mode ?? ""}`}
          endpoints={ibkr?.endpoints}
        />
        {ov?.pnl_today !== undefined && (
          <span>
            P&L Today:{" "}
            <span
              className={
                Number(ov.pnl_today) >= 0 ? "text-positive" : "text-negative"
              }
            >
              {Number(ov.pnl_today) >= 0 ? "+" : ""}
              {fmtUsd(Number(ov.pnl_today))}
            </span>
          </span>
        )}
        <button
          onClick={() => {
            const requestedTarget = activePortfolioId;
            const syncTarget = isIbkrPortfolioId(requestedTarget)
              ? requestedTarget
              : DEFAULT_SYNC_PORTFOLIO_ID;

            setLastSyncTarget(syncTarget);
            if (syncTarget !== requestedTarget) {
              setSyncDetail(`Syncing ${syncTarget} (active ${requestedTarget} is non-IBKR)`);
            } else {
              setSyncDetail(`Syncing ${syncTarget}...`);
            }
            sync.mutate({ sources: SYNC_SOURCES, portfolioId: syncTarget });
          }}
          disabled={sync.isPending}
          className="flex items-center gap-1.5 rounded border border-border-dim px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-muted transition-colors hover:border-accent hover:text-accent disabled:opacity-50"
          title="Sync data from IBKR & engines (safe target: active IBKR portfolio, else IBKR_PAPER)"
        >
          <RefreshCw size={12} className={sync.isPending ? "animate-spin" : ""} />
          {sync.isPending ? "Syncing..." : "Sync"}
        </button>
        {syncDetail && (
          <span className="text-[10px] text-positive max-w-[320px] truncate" title={syncDetail}>
            {syncDetail}
          </span>
        )}
        {sync.isError && (
          <span className="text-[10px] text-negative" title={String(sync.error)}>
            Sync failed
          </span>
        )}
        {iris && (
          <button
            onClick={iris.toggle}
            className="flex h-7 items-center gap-1.5 rounded border border-border-dim bg-accent/10 px-2 text-[10px] font-semibold uppercase tracking-wider text-accent transition-colors hover:bg-accent/20 hover:border-accent"
            title="Open Iris (Ctrl+I)"
            aria-label="Open Iris"
          >
            <Brain size={13} />
            Iris
          </button>
        )}
        <NotificationsBell />
        <span className="tabular-nums">{now.toLocaleTimeString()}</span>
      </div>
    </header>
  );
}
