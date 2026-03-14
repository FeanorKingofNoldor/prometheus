import { useState, useMemo, useEffect, useCallback } from "react";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import { LineChart, ZOOM_STEPS, fmtDateTick, ChartZoomBar } from "../components/Charts";
import {
  useEntitySectors,
  useEntities,
  useEntityDetail,
  useEntityProfiles,
  useFragilityList,
  useCompareEntityPrices,
} from "../api/hooks";

// ── Types ───────────────────────────────────────────────

interface EntityRow extends Record<string, unknown> {
  issuer_id: string;
  issuer_type: string;
  name: string;
  country: string | null;
  sector: string | null;
  industry: string | null;
  fragility_score: number | null;
  soft_target_class: string | null;
  in_portfolio: boolean;
}

interface FragilityRow extends Record<string, unknown> {
  entity_id: string;
  name: string;
  score: number;
  classification: string;
  trend: string;
}

interface StructuredProfile {
  fundamentals?: Record<string, number | string | null>;
  numeric_features?: Record<string, number | string | null>;
  news_features?: Record<string, number | string | boolean | null>;
  issuer_metadata?: Record<string, unknown>;
  [key: string]: unknown;
}

// ── Helpers ─────────────────────────────────────────────

function useDebounce(value: string, delay: number) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(t);
  }, [value, delay]);
  return debounced;
}

function stcVariant(stc: string | null | undefined): "positive" | "negative" | "warning" | "info" | "neutral" {
  if (!stc) return "neutral";
  const s = stc.toLowerCase();
  if (s === "stable") return "positive";
  if (s === "fragile") return "negative";
  if (s === "targetable") return "warning";
  if (s === "watch") return "info";
  return "neutral";
}

function fmtVal(v: unknown): string {
  if (v == null || v === "") return "—";
  if (typeof v === "boolean") return v ? "Yes" : "No";
  if (typeof v === "number") {
    if (Math.abs(v) >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
    if (Math.abs(v) >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
    if (Math.abs(v) < 0.0001 && v !== 0) return v.toExponential(2);
    if (Number.isInteger(v)) return v.toLocaleString();
    return v.toFixed(4);
  }
  return String(v);
}

const COMPARE_COLORS = [
  "#facc15", "#3b82f6", "#22c55e", "#ef4444", "#a855f7",
  "#ec4899", "#14b8a6", "#f97316", "#06b6d4", "#84cc16",
];

const METRIC_GROUPS = [
  {
    label: "Price",
    metrics: [
      { value: "close", label: "Close Price" },
      { value: "normalized", label: "Normalized (Base 100)" },
      { value: "volume", label: "Volume" },
    ],
  },
  {
    label: "Performance",
    metrics: [
      { value: "cumulative_return", label: "Cumulative Return %" },
    ],
  },
  {
    label: "Risk",
    metrics: [
      { value: "fragility", label: "Fragility Score" },
      { value: "rolling_vol", label: "Rolling Volatility (20d)" },
    ],
  },
];

const TIP = {
  entities: "Real companies tracked by Prometheus (US equities). Click a row to see full profile, or select ☐ to add to comparison chart.",
  fragility: "Latest fragility scores from the Fragility Engine. Measures how vulnerable each entity is to market stress.",
  profile: "Structured entity profile including fundamentals, risk flags, price features, and news intelligence.",
  compare: "Compare price history or fragility scores for up to 10 companies. Select companies from the table below using the checkboxes.",
};

// ── Component ───────────────────────────────────────────

export default function Entities() {
  // Filters
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebounce(search, 300);
  const [sectorFilter, setSectorFilter] = useState("");
  const [portfolioOnly, setPortfolioOnly] = useState(false);

  // Selection + detail tab
  const [selectedEntity, setSelectedEntity] = useState("");
  const [detailTab, setDetailTab] = useState<"overview" | "fragility">("overview");
  const [fragZoomIdx, setFragZoomIdx] = useState(ZOOM_STEPS.length - 1);
  const fragZoomDays = ZOOM_STEPS[fragZoomIdx];

  // Comparison state
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [compareMetric, setCompareMetric] = useState("close");
  const [compareDays, setCompareDays] = useState(365);

  // Data hooks
  const entitySectors = useEntitySectors("COMPANY");
  const sectors = (entitySectors.data ?? []) as string[];

  // Always filter to COMPANY only
  const queryParams = useMemo(() => {
    const parts: string[] = ["issuer_type=COMPANY"];
    if (debouncedSearch) parts.push(`q=${encodeURIComponent(debouncedSearch)}`);
    if (sectorFilter) parts.push(`sector=${encodeURIComponent(sectorFilter)}`);
    if (portfolioOnly) parts.push("in_portfolio=IBKR_PAPER");
    parts.push("limit=1000");
    return parts.join("&");
  }, [debouncedSearch, sectorFilter, portfolioOnly]);

  const entities = useEntities(queryParams);
  const rawData = entities.data as Record<string, unknown> | undefined;
  const total = Number(rawData?.total ?? 0);
  const entityList = (Array.isArray(rawData?.entities) ? rawData!.entities : []) as EntityRow[];

  const fragility = useFragilityList();
  const rawFragility = fragility.data;
  const fragilityList = (
    Array.isArray(rawFragility) ? rawFragility : ((rawFragility as Record<string, unknown> | undefined)?.entities ?? [])
  ) as FragilityRow[];

  // Detail
  const detail = useEntityDetail(selectedEntity);
  const profiles = useEntityProfiles(selectedEntity);
  const det = (detail.data ?? {}) as Record<string, unknown>;
  const detInstruments = (det.instruments ?? []) as Record<string, unknown>[];
  const detFragHistory = (det.fragility_history ?? []) as { as_of_date: string; fragility_score: number }[];
  const detProfile = det.profile as Record<string, unknown> | null | undefined;
  const structured = (detProfile?.structured ?? null) as StructuredProfile | null;

  // Comparison prices
  const comparePrices = useCompareEntityPrices(compareIds, compareDays, compareMetric);
  const compareData = (comparePrices.data ?? {}) as { instruments?: string[]; series?: Record<string, { date: string; value: number }[]> };

  // Build chart data: pivot series into recharts format
  const compareChartData = useMemo(() => {
    const seriesMap = compareData.series ?? {};
    const allIds = Object.keys(seriesMap);
    if (allIds.length === 0) return [];

    // Build date-keyed map
    const dateMap = new Map<string, Record<string, unknown>>();
    for (const id of allIds) {
      const name = entityList.find((e) => e.issuer_id === id)?.name ?? id;
      for (const pt of seriesMap[id] ?? []) {
        if (!dateMap.has(pt.date)) dateMap.set(pt.date, { date: pt.date });
        const row = dateMap.get(pt.date)!;
        row[name] = pt.value;
      }
    }

    return Array.from(dateMap.values()).sort((a, b) =>
      String(a.date).localeCompare(String(b.date))
    );
  }, [compareData.series, entityList]);

  const compareYKeys = useMemo(() => {
    const seriesMap = compareData.series ?? {};
    return Object.keys(seriesMap).map(
      (id) => entityList.find((e) => e.issuer_id === id)?.name ?? id
    );
  }, [compareData.series, entityList]);

  // Reset tab on entity change
  useEffect(() => setDetailTab("overview"), [selectedEntity]);

  const handleRowClick = useCallback((r: EntityRow) => setSelectedEntity(r.issuer_id), []);

  const toggleCompare = useCallback((issuerId: string) => {
    setCompareIds((prev) => {
      if (prev.includes(issuerId)) return prev.filter((id) => id !== issuerId);
      if (prev.length >= 10) return prev;
      return [...prev, issuerId];
    });
  }, []);

  // ── Computed ───────────────────────────────────────

  const withFragility = entityList.filter((e) => e.fragility_score != null);
  const avgFragility = withFragility.length > 0
    ? withFragility.reduce((s, e) => s + Number(e.fragility_score ?? 0), 0) / withFragility.length
    : null;
  const inPortCount = entityList.filter((e) => e.in_portfolio).length;
  const sectorCounts = useMemo(() => {
    const m = new Map<string, number>();
    for (const e of entityList) {
      const s = e.sector || "Unknown";
      m.set(s, (m.get(s) ?? 0) + 1);
    }
    return m;
  }, [entityList]);
  const topSector = sectorCounts.size > 0
    ? [...sectorCounts.entries()].sort((a, b) => b[1] - a[1])[0]
    : null;

  // ── Columns ────────────────────────────────────────

  const entityCols: Column<EntityRow>[] = [
    {
      key: "issuer_id",
      label: "☐",
      sortable: false,
      width: "32px",
      render: (r) => (
        <input
          type="checkbox"
          className="accent-accent"
          checked={compareIds.includes(r.issuer_id)}
          onChange={(e) => { e.stopPropagation(); toggleCompare(r.issuer_id); }}
          onClick={(e) => e.stopPropagation()}
          title={compareIds.includes(r.issuer_id) ? "Remove from comparison" : "Add to comparison"}
        />
      ),
    },
    {
      key: "in_portfolio",
      label: "",
      sortable: false,
      width: "28px",
      render: (r) =>
        r.in_portfolio ? (
          <span title="In portfolio" className="inline-block h-2 w-2 rounded-full bg-accent" />
        ) : null,
    },
    { key: "name", label: "Name" },
    { key: "sector", label: "Sector" },
    { key: "industry", label: "Industry" },
    {
      key: "soft_target_class",
      label: "Class",
      render: (r) =>
        r.soft_target_class ? (
          <StatusBadge label={String(r.soft_target_class)} variant={stcVariant(r.soft_target_class)} />
        ) : (
          <span className="text-muted">—</span>
        ),
    },
    {
      key: "fragility_score",
      label: "Fragility",
      align: "right",
      render: (r) =>
        r.fragility_score != null ? (
          <span
            className={
              r.fragility_score > 0.2
                ? "text-red-400"
                : r.fragility_score > 0.1
                  ? "text-amber-400"
                  : "text-green-400"
            }
          >
            {r.fragility_score.toFixed(3)}
          </span>
        ) : (
          <span className="text-muted">—</span>
        ),
    },
  ];

  const fragCols: Column<FragilityRow>[] = [
    { key: "name", label: "Entity", render: (r) => String(r.name ?? r.entity_id) },
    {
      key: "classification",
      label: "Class",
      render: (r) => (
        <StatusBadge
          label={String(r.classification ?? "—")}
          variant={
            String(r.classification).toLowerCase() === "antifragile"
              ? "positive"
              : String(r.classification).toLowerCase() === "fragile"
                ? "negative"
                : "neutral"
          }
        />
      ),
    },
    {
      key: "score",
      label: "Score",
      align: "right",
      render: (r) => Number(r.score ?? 0).toFixed(3),
    },
    {
      key: "trend",
      label: "Trend",
      render: (r) => (
        <span
          className={
            String(r.trend).includes("improv")
              ? "text-positive"
              : String(r.trend).includes("degrad")
                ? "text-negative"
                : "text-muted"
          }
        >
          {String(r.trend ?? "—")}
        </span>
      ),
    },
  ];

  // ── Render ─────────────────────────────────────────

  return (
    <div className="space-y-4">
      <PageHeader
        title="Entities & Fragility"
        subtitle={`${total.toLocaleString()} companies`}
        onRefresh={() => { entities.refetch(); fragility.refetch(); }}
      />

      {/* Search + Filters */}
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="text"
          className="w-64 rounded border border-border-dim bg-surface-overlay px-3 py-1.5 text-xs text-zinc-100 placeholder:text-muted"
          placeholder="Search by name..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <select
          className="rounded border border-border-dim bg-surface-overlay px-2 py-1.5 text-xs text-zinc-100"
          value={sectorFilter}
          onChange={(e) => setSectorFilter(e.target.value)}
        >
          <option value="">All Sectors</option>
          {sectors.map((s) => (
            <option key={String(s)} value={String(s)}>
              {String(s)}
            </option>
          ))}
        </select>
        <label className="flex items-center gap-1.5 text-xs text-muted">
          <input
            type="checkbox"
            className="accent-accent"
            checked={portfolioOnly}
            onChange={(e) => setPortfolioOnly(e.target.checked)}
          />
          In Portfolio
        </label>
        {compareIds.length > 0 ? (
          <span className="ml-auto text-xs text-accent">
            {compareIds.length}/10 selected for comparison
            <button
              className="ml-2 text-muted hover:text-zinc-100"
              onClick={() => setCompareIds([])}
            >
              Clear
            </button>
          </span>
        ) : null}
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <KpiCard label="Companies" value={total.toLocaleString()} tooltip="Total real companies tracked (US equities)." />
        <KpiCard
          label="Avg Fragility"
          value={avgFragility != null ? avgFragility.toFixed(3) : "—"}
          sentiment={avgFragility != null && avgFragility > 0.15 ? "warning" : "neutral"}
          tooltip="Average fragility score across all companies with fragility data."
        />
        <KpiCard
          label="In Portfolio"
          value={String(inPortCount)}
          sentiment={inPortCount > 0 ? "positive" : "neutral"}
          tooltip="Number of entities you currently hold positions in."
        />
        <KpiCard
          label="Top Sector"
          value={topSector ? `${topSector[0]} (${topSector[1]})` : "—"}
          tooltip="Sector with the most entities in the current filtered view."
        />
      </div>

      {/* Comparison Chart */}
      {compareIds.length > 0 ? (
        <Panel
          title={`Comparing ${compareIds.length} ${compareIds.length === 1 ? "Company" : "Companies"}`}
          tooltip={TIP.compare}
          actions={
            <div className="flex items-center gap-2">
              <select
                className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-[10px] text-zinc-100"
                value={compareMetric}
                onChange={(e) => setCompareMetric(e.target.value)}
              >
                {METRIC_GROUPS.map((g) => (
                  <optgroup key={g.label} label={g.label}>
                    {g.metrics.map((m) => (
                      <option key={m.value} value={m.value}>{m.label}</option>
                    ))}
                  </optgroup>
                ))}
              </select>
              <select
                className="rounded border border-border-dim bg-surface-overlay px-2 py-1 text-[10px] text-zinc-100"
                value={compareDays}
                onChange={(e) => setCompareDays(Number(e.target.value))}
              >
                <option value={30}>1M</option>
                <option value={90}>3M</option>
                <option value={180}>6M</option>
                <option value={365}>1Y</option>
                <option value={730}>2Y</option>
                <option value={1826}>5Y</option>
              </select>
            </div>
          }
        >
          {/* Selected company badges */}
          <div className="mb-3 flex flex-wrap gap-1.5">
            {compareIds.map((id, i) => {
              const name = entityList.find((e) => e.issuer_id === id)?.name ?? id;
              return (
                <span
                  key={id}
                  className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium"
                  style={{ backgroundColor: `${COMPARE_COLORS[i % COMPARE_COLORS.length]}20`, color: COMPARE_COLORS[i % COMPARE_COLORS.length] }}
                >
                  {name}
                  <button
                    className="ml-0.5 hover:opacity-70"
                    onClick={() => toggleCompare(id)}
                  >
                    ✕
                  </button>
                </span>
              );
            })}
          </div>

          {comparePrices.isLoading ? (
            <div className="flex h-64 items-center justify-center text-xs text-muted">Loading price data...</div>
          ) : compareChartData.length > 0 ? (
            <LineChart
              data={compareChartData}
              xKey="date"
              yKeys={compareYKeys}
              height={340}
              xTickFormatter={fmtDateTick}
            />
          ) : (
            <div className="flex h-48 items-center justify-center text-xs text-muted">
              No data available for selected companies with this metric
            </div>
          )}
        </Panel>
      ) : (
        <Panel title="Company Comparison" tooltip={TIP.compare}>
          <div className="flex h-32 items-center justify-center text-xs text-muted">
            Select companies using the ☐ checkboxes in the table below to compare their price history
          </div>
        </Panel>
      )}

      {/* Entity table + Fragility table — scrollable */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Companies" tooltip={TIP.entities}>
          <DataTable
            columns={entityCols}
            data={entityList}
            scrollable
            maxHeight="520px"
            onRowClick={handleRowClick}
            compact
            emptyMessage={entities.isLoading ? "Loading..." : "No companies match your filters"}
          />
        </Panel>
        <Panel title="Fragility Scores" tooltip={TIP.fragility}>
          <DataTable
            columns={fragCols}
            data={fragilityList}
            scrollable
            maxHeight="520px"
            compact
            onRowClick={(r) => setSelectedEntity(String(r.entity_id))}
            emptyMessage="No fragility data"
          />
        </Panel>
      </div>

      {/* Entity Detail */}
      {selectedEntity ? (
        <Panel
          title={
            <div className="flex items-center gap-3">
              <span>{String(det.name ?? selectedEntity)}</span>
              {det.in_portfolio ? (
                <span className="rounded bg-accent/20 px-2 py-0.5 text-[10px] font-semibold text-accent">
                  IN PORTFOLIO
                </span>
              ) : null}
              {det.soft_target_class ? (
                <StatusBadge
                  label={String(det.soft_target_class)}
                  variant={stcVariant(String(det.soft_target_class))}
                />
              ) : null}
              <button
                className="ml-auto text-[10px] text-muted hover:text-zinc-100"
                onClick={() => setSelectedEntity("")}
              >
                ✕ Close
              </button>
            </div>
          }
          tooltip={TIP.profile}
        >
          {/* Tab bar */}
          <div className="mb-3 flex gap-1 border-b border-border-dim">
            {(["overview", "fragility"] as const).map((tab) => (
              <button
                key={tab}
                className={`px-3 py-1.5 text-xs font-medium capitalize ${
                  detailTab === tab
                    ? "border-b-2 border-accent text-accent"
                    : "text-muted hover:text-zinc-300"
                }`}
                onClick={() => setDetailTab(tab)}
              >
                {tab === "overview" ? "Profile" : "Fragility History"}
              </button>
            ))}
          </div>

          {/* Profile tab */}
          {detailTab === "overview" ? (
            <div className="space-y-4">
              {/* Basic info */}
              <div className="grid gap-3 sm:grid-cols-4">
                {[
                  { label: "Sector", value: det.sector },
                  { label: "Industry", value: det.industry },
                  { label: "Country", value: det.country },
                  { label: "Fragility", value: det.fragility_score != null ? Number(det.fragility_score).toFixed(4) : "—" },
                ].map((item) => (
                  <div key={item.label}>
                    <span className="text-[10px] uppercase text-muted">{item.label}</span>
                    <div className="text-sm font-semibold text-zinc-100">{fmtVal(item.value)}</div>
                  </div>
                ))}
              </div>

              {/* Instruments */}
              {detInstruments.length > 0 ? (
                <div>
                  <div className="mb-1.5 text-[10px] uppercase tracking-wider text-muted">Instruments</div>
                  <div className="flex flex-wrap gap-2">
                    {detInstruments.map((inst, i) => (
                      <div key={i} className="rounded border border-border-dim bg-surface-overlay px-2.5 py-1.5 text-xs">
                        <span className="font-mono font-medium text-zinc-100">{String(inst.instrument_id)}</span>
                        <span className="ml-2 text-muted">{String(inst.asset_class ?? "")} · {String(inst.currency ?? "")}</span>
                        {inst.status === "ACTIVE" ? <span className="ml-1.5 text-positive">•</span> : null}
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}

              {/* Metadata */}
              {det.issuer_metadata && typeof det.issuer_metadata === "object" ? (
                <div>
                  <div className="mb-1.5 text-[10px] uppercase tracking-wider text-muted">Metadata</div>
                  <div className="grid gap-2 sm:grid-cols-4">
                    {Object.entries(det.issuer_metadata as Record<string, unknown>)
                      .filter(([, v]) => v != null && v !== "")
                      .map(([k, v]) => (
                        <div key={k} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1 text-xs">
                          <span className="text-muted">{k.replace(/_/g, " ")}</span>
                          <span className="font-mono text-zinc-100">{fmtVal(v)}</span>
                        </div>
                      ))}
                  </div>
                </div>
              ) : null}

              {/* Risk Flags */}
              {detProfile?.risk_flags && typeof detProfile.risk_flags === "object" ? (
                <div>
                  <div className="mb-1.5 text-[10px] uppercase tracking-wider text-muted">Risk Flags</div>
                  <div className="grid gap-2 sm:grid-cols-4">
                    {Object.entries(detProfile.risk_flags as Record<string, number>).map(([k, v]) => (
                      <div key={k} className="rounded border border-border-dim bg-surface-overlay p-2">
                        <div className="text-[10px] text-muted">{k.replace(/_/g, " ")}</div>
                        <div className={`text-sm font-bold ${Number(v) > 0.5 ? "text-red-400" : Number(v) > 0.2 ? "text-amber-400" : "text-green-400"}`}>
                          {Number(v).toFixed(3)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}

              {/* Fundamentals */}
              {structured?.fundamentals && Object.values(structured.fundamentals).some((v) => v != null && v !== 0) ? (
                <div>
                  <div className="mb-1.5 text-[10px] uppercase tracking-wider text-muted">Fundamentals</div>
                  <div className="grid gap-2 sm:grid-cols-3 lg:grid-cols-4">
                    {Object.entries(structured.fundamentals)
                      .filter(([, v]) => v != null)
                      .map(([k, v]) => (
                        <div key={k} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1 text-xs">
                          <span className="text-muted">{k.replace(/_/g, " ")}</span>
                          <span className="font-mono text-zinc-100">{fmtVal(v)}</span>
                        </div>
                      ))}
                  </div>
                </div>
              ) : null}

              {/* Numeric Features */}
              {structured?.numeric_features ? (
                <div>
                  <div className="mb-1.5 text-[10px] uppercase tracking-wider text-muted">Price Features</div>
                  <div className="grid gap-2 sm:grid-cols-3">
                    {Object.entries(structured.numeric_features)
                      .filter(([k]) => k !== "instrument_id")
                      .map(([k, v]) => (
                        <div key={k} className="rounded border border-border-dim bg-surface-overlay p-2">
                          <div className="text-[10px] text-muted">{k.replace(/_/g, " ")}</div>
                          <div className="text-sm font-semibold text-zinc-100">{fmtVal(v)}</div>
                        </div>
                      ))}
                  </div>
                </div>
              ) : null}

              {/* News Features */}
              {structured?.news_features ? (
                <div>
                  <div className="mb-1.5 text-[10px] uppercase tracking-wider text-muted">News Intelligence</div>
                  <div className="grid gap-2 sm:grid-cols-3">
                    {Object.entries(structured.news_features)
                      .filter(([k]) => !["model_id", "embedding_source_id"].includes(k))
                      .map(([k, v]) => (
                        <div key={k} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1 text-xs">
                          <span className="text-muted">{k.replace(/_/g, " ")}</span>
                          <span className="font-mono text-zinc-100">{fmtVal(v)}</span>
                        </div>
                      ))}
                  </div>
                </div>
              ) : null}

              {/* Position value if in portfolio */}
              {det.position_value != null && Number(det.position_value) > 0 ? (
                <div className="rounded border border-accent/30 bg-accent/5 p-3">
                  <span className="text-xs text-muted">Position Value: </span>
                  <span className="text-sm font-bold text-accent">
                    ${Number(det.position_value).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                  </span>
                </div>
              ) : null}
            </div>
          ) : null}

          {/* Fragility tab */}
          {detailTab === "fragility" ? (
            <div>
              <div className="mb-2 flex justify-end">
                <ChartZoomBar zoomIdx={fragZoomIdx} setZoomIdx={setFragZoomIdx} />
              </div>
              {detFragHistory.length === 0 ? (
                <p className="text-xs text-muted">No fragility history for this entity.</p>
              ) : (
                <LineChart
                  data={detFragHistory.slice(-fragZoomDays)}
                  xKey="as_of_date"
                  yKeys={["fragility_score"]}
                  labels={{ fragility_score: "Fragility" }}
                  height={360}
                  xTickFormatter={fmtDateTick}
                />
              )}
            </div>
          ) : null}
        </Panel>
      ) : null}
    </div>
  );
}
