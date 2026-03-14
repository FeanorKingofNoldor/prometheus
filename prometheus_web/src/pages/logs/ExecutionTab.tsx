import { KpiCard } from "../../components/KpiCard";
import { Panel } from "../../components/Panel";
import { DataTable, Column } from "../../components/DataTable";
import { StatusBadge } from "../../components/StatusBadge";
import {
  useExecution,
  useExecutionDecisions,
  useRiskActions,
} from "../../api/hooks";
import { usePortfolioContext } from "../../context/PortfolioContext";

interface OrderRow extends Record<string, unknown> {
  timestamp: string;
  instrument: string;
  side: string;
  qty: number;
  filled_qty: number;
  price: number;
  status: string;
  order_type: string;
}

interface DecisionRow extends Record<string, unknown> {
  timestamp: string;
  instrument: string;
  action: string;
  reason: string;
  signal_strength: number;
  executed: boolean;
}

interface RiskActionRow extends Record<string, unknown> {
  timestamp: string;
  action: string;
  trigger: string;
  severity: string;
  instrument: string;
  resolved: boolean;
}

export default function ExecutionTab() {
  const { activePortfolioId } = usePortfolioContext();
  const execution = useExecution(activePortfolioId);
  const decisions = useExecutionDecisions(activePortfolioId);
  const riskActions = useRiskActions();

  const exec = (execution.data ?? {}) as Record<string, unknown>;
  const orders = (exec.orders ?? exec.recent_orders ?? []) as OrderRow[];
  const fills = orders.filter((o) => o.status === "FILLED");
  const rawDec = decisions.data;
  const decList = (Array.isArray(rawDec) ? rawDec : ((rawDec as Record<string, unknown> | undefined)?.decisions ?? [])) as DecisionRow[];
  const rawRisk = riskActions.data;
  const riskList = (Array.isArray(rawRisk) ? rawRisk : ((rawRisk as Record<string, unknown> | undefined)?.actions ?? [])) as RiskActionRow[];

  const totalVolume = orders.reduce((s, o) => s + Math.abs(Number(o.qty ?? 0) * Number(o.price ?? 0)), 0);
  const fillRate = orders.length > 0 ? fills.length / orders.length : 0;
  const pendingRisk = riskList.filter((r) => !r.resolved).length;

  const orderCols: Column<OrderRow>[] = [
    { key: "timestamp", label: "Time", render: (r) => String(r.timestamp ?? "").slice(11, 19) || "—" },
    { key: "instrument", label: "Instrument" },
    { key: "side", label: "Side", render: (r) => <StatusBadge label={String(r.side)} variant={r.side === "BUY" ? "positive" : "negative"} /> },
    { key: "order_type", label: "Type" },
    { key: "qty", label: "Qty", align: "right" },
    { key: "filled_qty", label: "Filled", align: "right" },
    { key: "price", label: "Price", align: "right", render: (r) => r.price != null ? `$${Number(r.price).toFixed(2)}` : "—" },
    { key: "status", label: "Status", render: (r) => (
      <StatusBadge
        label={String(r.status)}
        variant={
          r.status === "FILLED" ? "positive"
            : r.status === "REJECTED" || r.status === "CANCELLED" ? "negative"
              : r.status === "PARTIAL" ? "warning"
                : "info"
        }
      />
    )},
  ];

  const decisionCols: Column<DecisionRow>[] = [
    { key: "timestamp", label: "Time", render: (r) => String(r.timestamp ?? "").slice(11, 19) || "—" },
    { key: "instrument", label: "Instrument" },
    { key: "action", label: "Action", render: (r) => <StatusBadge label={String(r.action)} variant={String(r.action).includes("BUY") ? "positive" : String(r.action).includes("SELL") ? "negative" : "neutral"} /> },
    { key: "signal_strength", label: "Signal", align: "right", render: (r) => r.signal_strength != null ? Number(r.signal_strength).toFixed(3) : "—" },
    { key: "reason", label: "Reason" },
    { key: "executed", label: "Exec'd", render: (r) => <StatusBadge label={r.executed ? "YES" : "NO"} variant={r.executed ? "positive" : "neutral"} /> },
  ];

  const riskCols: Column<RiskActionRow>[] = [
    { key: "timestamp", label: "Time", render: (r) => String(r.timestamp ?? "").slice(0, 16) || "—" },
    { key: "action", label: "Action" },
    { key: "instrument", label: "Instrument" },
    { key: "trigger", label: "Trigger" },
    { key: "severity", label: "Severity", render: (r) => (
      <StatusBadge
        label={String(r.severity ?? "—")}
        variant={
          String(r.severity).toLowerCase() === "critical" ? "negative"
            : String(r.severity).toLowerCase() === "high" ? "warning"
              : "neutral"
        }
      />
    )},
    { key: "resolved", label: "Status", render: (r) => <StatusBadge label={r.resolved ? "RESOLVED" : "ACTIVE"} variant={r.resolved ? "neutral" : "warning"} /> },
  ];

  return (
    <div className="space-y-4">
      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <KpiCard label="Orders" value={String(orders.length)} />
        <KpiCard label="Fill Rate" value={`${(fillRate * 100).toFixed(1)}%`} sentiment={fillRate > 0.9 ? "positive" : fillRate > 0.7 ? "neutral" : "warning"} />
        <KpiCard label="Volume" value={fmtNum(totalVolume)} />
        <KpiCard label="Active Risk" value={String(pendingRisk)} sentiment={pendingRisk > 0 ? "warning" : "positive"} />
      </div>

      {/* Orders */}
      <Panel title="Orders">
        <DataTable columns={orderCols} data={orders} pageSize={20} emptyMessage="No orders yet — execute trades via IBKR Paper to see activity here" />
      </Panel>

      {/* Decisions + Risk side by side */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Execution Decisions">
          <DataTable columns={decisionCols} data={decList} compact pageSize={15} emptyMessage="No execution decisions recorded" />
        </Panel>
        <Panel title="Risk Actions">
          <DataTable columns={riskCols} data={riskList} compact pageSize={15} emptyMessage="No risk actions — risk engine will log actions during live trading" />
        </Panel>
      </div>
    </div>
  );
}

function fmtNum(n: number): string {
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}
