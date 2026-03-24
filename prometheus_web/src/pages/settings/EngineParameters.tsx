import { Link } from "react-router-dom";
import { PageHeader } from "../../components/PageHeader";
import { Panel } from "../../components/Panel";
import { DataTable, type Column } from "../../components/DataTable";
import { useEngineParameters } from "../../api/hooks";

interface EngineParameterRow extends Record<string, unknown> {
  key: string;
  value: unknown;
  source: string;
  detrimental_reason: string;
}

interface EngineParameterGroup {
  engine_id: string;
  engine_label: string;
  parameters: EngineParameterRow[];
}

function renderValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return <span className="text-muted">—</span>;
  }
  if (typeof value === "object") {
    return (
      <span className="font-mono text-[11px] text-zinc-200">
        {JSON.stringify(value)}
      </span>
    );
  }
  return <span className="font-mono text-zinc-100">{String(value)}</span>;
}

export default function EngineParametersSettings() {
  const query = useEngineParameters();
  const raw = (query.data ?? {}) as { generated_at?: string; engines?: EngineParameterGroup[] };
  const engines = raw.engines ?? [];

  const columns: Column<EngineParameterRow>[] = [
    { key: "key", label: "Parameter", render: (r) => <span className="font-mono text-[11px]">{r.key}</span> },
    { key: "value", label: "Current Value", render: (r) => renderValue(r.value) },
    { key: "source", label: "Source", render: (r) => <span className="text-[11px] text-muted">{r.source}</span> },
    { key: "detrimental_reason", label: "Why it hurts when mis-set", sortable: false },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Settings · Engine Parameters"
        subtitle={`Live fetched from backend sources${raw.generated_at ? ` · updated ${raw.generated_at}` : ""}`}
        onRefresh={() => query.refetch()}
        actions={(
          <Link
            to="/settings"
            className="rounded border border-border-dim px-2 py-1 text-[11px] text-muted transition-colors hover:border-accent hover:text-accent"
          >
            Back to Settings
          </Link>
        )}
      />

      {engines.length === 0 ? (
        <Panel title="No engine parameters available">
          <div className="text-xs text-muted">
            {query.isLoading ? "Loading…" : "No engine parameter data returned."}
          </div>
        </Panel>
      ) : (
        engines.map((engine) => (
          <Panel
            key={engine.engine_id}
            title={`${engine.engine_label} (${engine.engine_id})`}
          >
            <DataTable
              columns={columns}
              data={engine.parameters ?? []}
              pageSize={50}
              scrollable
              maxHeight="420px"
              emptyMessage="No parameters"
            />
          </Panel>
        ))
      )}
    </div>
  );
}
