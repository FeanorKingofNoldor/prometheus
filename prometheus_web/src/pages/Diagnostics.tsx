import { useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { KpiCard } from "../components/KpiCard";
import { SeverityBadge } from "../components/SeverityBadge";
import { StatusBadge } from "../components/StatusBadge";
import {
  useDiagnosticReports,
  useDiagnosticReportDetail,
  useSignalValidations,
  type DiagnosticReportRow,
  type SignalValidationRow,
} from "../api/hooks";

function verdictVariant(v: string): "positive" | "negative" | "warning" | "info" | "neutral" {
  const up = v.toUpperCase();
  if (up === "PASS" || up === "OK") return "positive";
  if (up === "FAIL" || up === "BROKEN") return "negative";
  if (up === "DEGRADED" || up === "WARN" || up === "WARNING") return "warning";
  return "info";
}

export default function Diagnostics() {
  const [selectedReportId, setSelectedReportId] = useState<number | null>(null);
  const [onlyFindings, setOnlyFindings] = useState(false);
  const [days, setDays] = useState(30);

  const reports = useDiagnosticReports({
    days,
    onlyWithFindings: onlyFindings,
    limit: 200,
  });
  const detail = useDiagnosticReportDetail(selectedReportId);
  const validations = useSignalValidations({ days, limit: 300 });

  const reportRows: (DiagnosticReportRow & Record<string, unknown>)[] =
    (reports.data?.items ?? []) as (DiagnosticReportRow & Record<string, unknown>)[];
  const validationRows: (SignalValidationRow & Record<string, unknown>)[] =
    (validations.data?.items ?? []) as (SignalValidationRow & Record<string, unknown>)[];

  const withFindings = reportRows.filter(
    (r) => r.has_underperformers || r.has_high_risk,
  ).length;

  const reportCols: Column<DiagnosticReportRow & Record<string, unknown>>[] = [
    { key: "as_of_date", label: "Date" },
    { key: "strategy_id", label: "Strategy" },
    {
      key: "has_underperformers",
      label: "Underperformers",
      render: (r) =>
        r.has_underperformers ? (
          <SeverityBadge severity="warning" />
        ) : (
          <span className="text-muted">—</span>
        ),
    },
    {
      key: "has_high_risk",
      label: "High Risk",
      render: (r) =>
        r.has_high_risk ? (
          <SeverityBadge severity="critical" />
        ) : (
          <span className="text-muted">—</span>
        ),
    },
    {
      key: "num_runs_analysed",
      label: "Runs",
      align: "right",
      render: (r) => r.num_runs_analysed.toLocaleString(),
    },
  ];

  const validationCols: Column<SignalValidationRow & Record<string, unknown>>[] = [
    { key: "as_of_date", label: "Date" },
    {
      key: "signal_name",
      label: "Signal",
      render: (r) => <span className="font-mono text-zinc-200">{r.signal_name}</span>,
    },
    {
      key: "verdict",
      label: "Verdict",
      render: (r) => <StatusBadge label={r.verdict} variant={verdictVariant(r.verdict)} />,
    },
    {
      key: "metric_value",
      label: "Value",
      align: "right",
      render: (r) =>
        r.metric_value != null ? r.metric_value.toFixed(3) : "—",
    },
    {
      key: "threshold",
      label: "Threshold",
      align: "right",
      render: (r) =>
        r.threshold != null ? (
          <span className="text-muted">{r.threshold.toFixed(3)}</span>
        ) : (
          "—"
        ),
    },
    {
      key: "sample_size",
      label: "N",
      align: "right",
      render: (r) => (r.sample_size != null ? r.sample_size.toLocaleString() : "—"),
    },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Diagnostics & Signal Validations"
        subtitle="Persisted history of the autopilot loop's daily checks (migration 0099)."
        onRefresh={() => {
          reports.refetch();
          validations.refetch();
        }}
        actions={
          <div className="flex items-center gap-2 text-[10px]">
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="rounded border border-border-dim bg-surface px-2 py-0.5 text-xs text-zinc-100"
            >
              {[7, 14, 30, 60, 90].map((d) => (
                <option key={d} value={d}>
                  {d}d
                </option>
              ))}
            </select>
          </div>
        }
      />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <KpiCard label="Diagnostic Reports" value={reportRows.length} />
        <KpiCard
          label="With Findings"
          value={withFindings}
          sentiment={withFindings > 0 ? "warning" : "positive"}
        />
        <KpiCard label="Signal Validations" value={validationRows.length} />
        <KpiCard
          label="Distinct Signals"
          value={validations.data?.distinct_signals ?? 0}
        />
      </div>

      <Panel
        title="Diagnostic Reports"
        actions={
          <label className="flex items-center gap-1 text-[10px] text-muted">
            <input
              type="checkbox"
              checked={onlyFindings}
              onChange={(e) => setOnlyFindings(e.target.checked)}
            />
            Only reports with findings
          </label>
        }
      >
        <div className="grid gap-3 lg:grid-cols-2">
          <DataTable
            columns={reportCols}
            data={reportRows}
            onRowClick={(r) => setSelectedReportId(r.report_id)}
            compact
            pageSize={20}
            emptyMessage="No diagnostic reports in this window."
          />
          <div>
            {selectedReportId == null ? (
              <div className="rounded border border-dashed border-border-dim/50 px-4 py-8 text-center text-xs text-muted">
                Select a report to see its full JSON payload.
              </div>
            ) : detail.isLoading ? (
              <div className="px-4 py-8 text-center text-xs text-muted">Loading…</div>
            ) : (
              <pre className="max-h-96 overflow-y-auto rounded border border-border-dim bg-surface p-3 font-mono text-[11px] text-zinc-300">
                {JSON.stringify(detail.data?.report_json ?? {}, null, 2)}
              </pre>
            )}
          </div>
        </div>
      </Panel>

      <Panel
        title="Signal Validations"
        tooltip="Each row is one daily verdict per signal (e.g. divergence, convergence, beneficiary scoring)."
      >
        <DataTable
          columns={validationCols}
          data={validationRows}
          compact
          scrollable
          maxHeight="480px"
          emptyMessage="No signal validations in this window."
        />
      </Panel>
    </div>
  );
}
