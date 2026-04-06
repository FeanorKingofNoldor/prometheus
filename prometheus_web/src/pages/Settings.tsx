import { useState } from "react";
import { Link } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import {
  useConfigs,
  useApplyConfig,
  usePerformance,
  useAllocatorRegistry,
  usePolicyDecisions,
  useLlmConfig,
  useSetLlmConfig,
} from "../api/hooks";

interface ConfigRow extends Record<string, unknown> {
  key: string;
  value: string;
  section: string;
  editable: boolean;
}

interface AllocatorRow extends Record<string, unknown> {
  id: string;
  name: string;
  type: string;
  status: string;
}

interface DecisionRow extends Record<string, unknown> {
  timestamp: string;
  market_id: string;
  decision: string;
  reason: string;
}

export default function Settings() {
  const configs = useConfigs();
  const performance = usePerformance();
  const allocators = useAllocatorRegistry();
  const policyDecisions = usePolicyDecisions();
  const applyConfig = useApplyConfig();
  const llmConfig = useLlmConfig();
  const setLlmConfig = useSetLlmConfig();

  const [editKey, setEditKey] = useState("");
  const [editValue, setEditValue] = useState("");

  // LLM config state
  const llmData = (llmConfig.data ?? {}) as Record<string, unknown>;
  const [llmProvider, setLlmProvider] = useState("");
  const [llmModel, setLlmModel] = useState("");
  const [llmApiKey, setLlmApiKey] = useState("");
  const [llmBaseUrl, setLlmBaseUrl] = useState("");
  const [llmStatus, setLlmStatus] = useState<string | null>(null);

  const rawConfigs = configs.data;
  const configList = (Array.isArray(rawConfigs) ? rawConfigs : ((rawConfigs as Record<string, unknown> | undefined)?.configs ?? [])) as ConfigRow[];
  const perf = (performance.data ?? {}) as Record<string, unknown>;
  const rawAlloc = allocators.data;
  const allocList = (Array.isArray(rawAlloc) ? rawAlloc : ((rawAlloc as Record<string, unknown> | undefined)?.allocators ?? [])) as AllocatorRow[];
  const rawDecisions = policyDecisions.data;
  const decisionList = (Array.isArray(rawDecisions) ? rawDecisions : ((rawDecisions as Record<string, unknown> | undefined)?.decisions ?? [])) as DecisionRow[];

  const configCols: Column<ConfigRow>[] = [
    { key: "section", label: "Section" },
    { key: "key", label: "Key" },
    { key: "value", label: "Value", render: (r) => <span className="font-mono">{String(r.value)}</span> },
    { key: "editable", label: "", sortable: false, width: "80px", render: (r) => r.editable !== false ? (
      <button
        className="rounded bg-accent/20 px-2 py-0.5 text-[10px] font-semibold text-accent hover:bg-accent/30"
        onClick={(e) => { e.stopPropagation(); setEditKey(String(r.key)); setEditValue(String(r.value)); }}
      >
        Edit
      </button>
    ) : null },
  ];

  const allocCols: Column<AllocatorRow>[] = [
    { key: "name", label: "Name" },
    { key: "type", label: "Type" },
    { key: "status", label: "Status", render: (r) => <StatusBadge label={String(r.status)} variant={r.status === "ACTIVE" ? "positive" : "neutral"} /> },
    { key: "id", label: "ID", render: (r) => <span className="font-mono text-muted">{String(r.id).slice(0, 12)}</span> },
  ];

  const decisionCols: Column<DecisionRow>[] = [
    { key: "timestamp", label: "Time", render: (r) => String(r.timestamp ?? "").slice(0, 16) || "—" },
    { key: "market_id", label: "Market" },
    { key: "decision", label: "Decision", render: (r) => <StatusBadge label={String(r.decision)} variant={String(r.decision).includes("ALLOW") ? "positive" : String(r.decision).includes("DENY") ? "negative" : "neutral"} /> },
    { key: "reason", label: "Reason" },
  ];

  const handleConfigSave = () => {
    if (editKey) {
      applyConfig.mutate({ key: editKey, value: editValue });
      setEditKey("");
      setEditValue("");
    }
  };

  return (
    <div className="space-y-4">
      <PageHeader
        title="Settings"
        subtitle="Configuration & system controls"
        onRefresh={() => { configs.refetch(); performance.refetch(); allocators.refetch(); policyDecisions.refetch(); }}
        actions={(
          <Link
            to="/settings/engine-parameters"
            className="rounded border border-border-dim px-2 py-1 text-[11px] text-muted transition-colors hover:border-accent hover:text-accent"
          >
            Engine Parameters
          </Link>
        )}
      />

      <Panel title="Engine Parameters">
        <div className="flex items-center justify-between gap-3 text-xs text-muted">
          <span>
            Review detrimental high-impact parameters split by engine, with current values fetched live from backend config sources.
          </span>
          <Link
            to="/settings/engine-parameters"
            className="rounded border border-border-dim px-2 py-1 text-[11px] text-muted transition-colors hover:border-accent hover:text-accent whitespace-nowrap"
          >
            Open Page
          </Link>
        </div>
      </Panel>

      {/* Performance KPIs */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        {Object.entries(perf)
          .filter(([k]) => !["_id", "id", "timestamp"].includes(k))
          .slice(0, 5)
          .map(([k, v]) => (
            <KpiCard key={k} label={k.replace(/_/g, " ")} value={typeof v === "number" ? (Math.abs(v) < 1 ? `${(v * 100).toFixed(2)}%` : v.toFixed(3)) : String(v ?? "—")} />
          ))}
      </div>

      {/* Config editor inline */}
      {editKey && (
        <Panel title={`Edit: ${editKey}`}>
          <div className="flex items-end gap-3">
            <label className="flex-1">
              <span className="text-[10px] uppercase text-muted">Value</span>
              <input
                className="mt-1 block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100 font-mono"
                value={editValue}
                onChange={(e) => setEditValue(e.target.value)}
              />
            </label>
            <button
              className="rounded bg-accent px-4 py-1.5 text-xs font-semibold text-zinc-950 hover:bg-accent/80 disabled:opacity-50"
              onClick={handleConfigSave}
              disabled={applyConfig.isPending}
            >
              {applyConfig.isPending ? "Saving..." : "Save"}
            </button>
            <button
              className="rounded border border-border-dim px-4 py-1.5 text-xs text-muted hover:text-zinc-100"
              onClick={() => setEditKey("")}
            >
              Cancel
            </button>
          </div>
        </Panel>
      )}

      {/* Configuration */}
      <Panel title="Configuration">
        <DataTable columns={configCols} data={configList} pageSize={20} emptyMessage="No config entries" />
      </Panel>

      {/* Allocators + Policy Decisions */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Allocator Registry">
          <DataTable columns={allocCols} data={allocList} compact emptyMessage="No allocators" />
        </Panel>
        <Panel title="Policy Decisions">
          <DataTable columns={decisionCols} data={decisionList} compact pageSize={10} emptyMessage="No decisions" />
        </Panel>
      </div>

      {/* LLM / Iris Configuration */}
      <Panel title="LLM / Iris">
        <div className="space-y-3">
          {/* Current status */}
          <div className="flex items-center gap-2 text-xs">
            <StatusBadge
              label={llmData.configured ? "Configured" : "Not Configured"}
              variant={llmData.configured ? "positive" : "neutral"}
            />
            {llmData.provider ? (
              <span className="text-muted">
                Provider: <span className="font-mono text-zinc-100">{String(llmData.provider)}</span>
                {llmData.model ? <> &middot; Model: <span className="font-mono text-zinc-100">{String(llmData.model)}</span></> : null}
              </span>
            ) : null}
          </div>

          {/* Config form */}
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <label className="space-y-1">
              <span className="text-[10px] uppercase text-muted">Provider</span>
              <select
                className="block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100"
                value={llmProvider || String(llmData.provider ?? "ollama")}
                onChange={(e) => setLlmProvider(e.target.value)}
              >
                <option value="ollama">Ollama (Local)</option>
                <option value="openai">OpenAI</option>
              </select>
            </label>
            <label className="space-y-1">
              <span className="text-[10px] uppercase text-muted">Model</span>
              <input
                className="block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100 font-mono"
                placeholder={llmProvider === "openai" ? "gpt-4o" : "llama3"}
                value={llmModel}
                onChange={(e) => setLlmModel(e.target.value)}
              />
            </label>
            <label className="space-y-1">
              <span className="text-[10px] uppercase text-muted">API Key {(llmProvider || String(llmData.provider ?? "")) !== "openai" && <span className="text-muted/50">(OpenAI only)</span>}</span>
              <input
                type="password"
                className="block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100 font-mono"
                placeholder="sk-..."
                value={llmApiKey}
                onChange={(e) => setLlmApiKey(e.target.value)}
                disabled={(llmProvider || String(llmData.provider ?? "")) !== "openai"}
              />
            </label>
            <label className="space-y-1">
              <span className="text-[10px] uppercase text-muted">Base URL <span className="text-muted/50">(optional)</span></span>
              <input
                className="block w-full rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100 font-mono"
                placeholder="http://localhost:11434"
                value={llmBaseUrl}
                onChange={(e) => setLlmBaseUrl(e.target.value)}
              />
            </label>
          </div>

          {/* Actions */}
          <div className="flex items-center gap-3">
            <button
              className="rounded bg-accent px-4 py-1.5 text-xs font-semibold text-zinc-950 hover:bg-accent/80 disabled:opacity-50"
              disabled={setLlmConfig.isPending}
              onClick={() => {
                setLlmStatus(null);
                const prov = llmProvider || String(llmData.provider ?? "ollama");
                setLlmConfig.mutate(
                  {
                    provider: prov,
                    model: llmModel || undefined,
                    api_key: llmApiKey || undefined,
                    base_url: llmBaseUrl || undefined,
                  },
                  {
                    onSuccess: (data: unknown) => {
                      const d = (data ?? {}) as Record<string, unknown>;
                      const h = (d?.health ?? d) as Record<string, unknown>;
                      setLlmStatus(h?.status === "ok" ? "Connected \u2714" : `Health: ${JSON.stringify(h)}`);
                    },
                    onError: (err: Error) => setLlmStatus(`Error: ${err.message}`),
                  }
                );
              }}
            >
              {setLlmConfig.isPending ? "Saving..." : "Save & Test"}
            </button>
            {llmStatus && (
              <span className={`text-xs ${llmStatus.startsWith("Connected") ? "text-green-400" : "text-red-400"}`}>
                {llmStatus}
              </span>
            )}
          </div>
        </div>
      </Panel>

      {/* Advanced Controls — placeholder */}
      <Panel title="Advanced Controls">
        <div className="flex items-center gap-3 text-xs text-muted">
          <span className="rounded bg-accent/10 px-2 py-0.5 text-[10px] font-semibold uppercase text-accent">Coming Soon</span>
          DAG scheduling, synthetic dataset creation, and pipeline orchestration.
        </div>
      </Panel>
    </div>
  );
}
