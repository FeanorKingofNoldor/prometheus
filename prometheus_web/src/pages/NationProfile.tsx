import { useState } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { DataTable, Column } from "../components/DataTable";
import { StatusBadge } from "../components/StatusBadge";
import { LineChart, ZOOM_STEPS, ZOOM_LABELS, fmtDateTick, ChartZoomBar } from "../components/Charts";
import {
  useNationList,
  useNationScores,
  useNationScoreHistory,
  useNationIndicators,
  useNationPersons,
  useResources,
  useChokepoints,
  useTradeRoutes,
  useNationIndustries,
  useNationIndustryHealth,
  useNationConflicts,
  useNationInfo,
} from "../api/hooks";
import { NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";

// ── helpers ──────────────────────────────────────────────

const FRED_NAMES: Record<string, string> = {
  BAMLC0A0CM: "US IG Credit Spread",
  BAMLH0A0HYM2: "US HY Credit Spread",
  CPIAUCSL: "CPI (All Urban Consumers)",
  CPILFESL: "Core CPI",
  FEDFUNDS: "Fed Funds Rate",
  DGS10: "10Y Treasury Yield",
  DGS2: "2Y Treasury Yield",
  T10Y2Y: "10Y-2Y Yield Spread",
  T10YIE: "10Y Breakeven Inflation",
  DCOILWTICO: "WTI Crude Oil",
  UNRATE: "Unemployment Rate",
  PAYEMS: "Nonfarm Payrolls",
  INDPRO: "Industrial Production",
  RSAFS: "Retail Sales",
  HOUST: "Housing Starts",
  MORTGAGE30US: "30Y Mortgage Rate",
  UMCSENT: "Consumer Sentiment",
  VIXCLS: "VIX",
  DTWEXBGS: "Trade-Weighted US Dollar",
  DEXUSEU: "USD/EUR Exchange Rate",
  DEXJPUS: "JPY/USD Exchange Rate",
  DEXCHUS: "CNY/USD Exchange Rate",
  GDP: "GDP",
  GDPC1: "Real GDP",
  PCE: "Personal Consumption Expenditures",
  PCEPILFE: "Core PCE Price Index",
  M2SL: "M2 Money Supply",
  WALCL: "Federal Reserve Total Assets",
  ICSA: "Initial Jobless Claims",
  JTSJOL: "Job Openings (JOLTS)",
  CSUSHPINSA: "Case-Shiller Home Price Index",
  GFDEBTN: "Federal Debt Total",
  TOTALSL: "Consumer Credit Outstanding",
  BUSLOANS: "Commercial & Industrial Loans",
  MICH: "Michigan Inflation Expectations",
  RECPROUSM156N: "US Recession Probability",
  STLFSI4: "St. Louis Financial Stress Index",
  NFCI: "Chicago Fed National Financial Conditions Index",
};

function fredLabel(seriesId: string): string {
  return FRED_NAMES[seriesId] ?? seriesId;
}

function pct(v: number | undefined | null): string {
  return v != null ? `${(v * 100).toFixed(1)}%` : "—";
}

function scoreSentiment(v: number | undefined | null): "positive" | "warning" | "negative" | "neutral" {
  if (v == null) return "neutral";
  if (v >= 0.7) return "positive";
  if (v >= 0.4) return "warning";
  return "negative";
}

function directionBadge(d: string | null | undefined) {
  if (!d) return "—";
  const map: Record<string, "positive" | "negative" | "neutral"> = {
    improving: "positive",
    deteriorating: "negative",
    stable: "neutral",
  };
  return <StatusBadge label={d} variant={map[d] ?? "neutral"} />;
}

// ── types ────────────────────────────────────────────────

interface Indicator extends Record<string, unknown> {
  series_id: string;
  observation_date: string;
  value: number;
  direction: string | null;
  rate_of_change: number | null;
  metadata: Record<string, unknown>;
}

interface WikipediaData {
  thumbnail_url?: string;
  extract?: string;
  page_url?: string;
}

interface Person {
  profile_id: string;
  person_name: string;
  role: string;
  role_tier: number;
  in_role_since: string;
  policy_stance: Record<string, number>;
  scores: Record<string, number>;
  confidence: number;
  metadata?: { wikipedia?: WikipediaData };
}

// Helper to build a display label from countryMapping
function nationLabel(iso3: string): string {
  const flag = NATION_FLAGS[iso3] ?? "";
  const name = NATION_NAMES[iso3] ?? iso3;
  return `${flag} ${name}`;
}

// ── component ────────────────────────────────────────────

export default function NationProfile() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const [nation, setNation] = useState(searchParams.get("n") ?? "USA");
  const [zoomIdx, setZoomIdx] = useState(ZOOM_STEPS.length - 1);
  const zoomDays = ZOOM_STEPS[zoomIdx];
  const nationList = useNationList();

  const scores = useNationScores(nation);
  const history = useNationScoreHistory(nation, 1826);
  const indicators = useNationIndicators(nation);
  const persons = useNationPersons(nation);
  const resourcesQuery = useResources(nation);
  const chokepointsQuery = useChokepoints();
  const tradeRoutesQuery = useTradeRoutes();
  const industriesQuery = useNationIndustries(nation);
  const industryHealthQuery = useNationIndustryHealth(nation);
  const conflictsQuery = useNationConflicts(nation);
  const nationInfoQuery = useNationInfo(nation);
  const nInfo = nationInfoQuery.data as { display_name?: string; region?: string; capital?: string; population?: string; gdp?: string; government?: string; key_sectors?: string[]; description?: string; strategic_notes?: string } | undefined;

  const nations = ((nationList.data ?? []) as { nation: string; composite_risk: number }[]);
  const scoredSet = new Set(nations.map((n) => n.nation));
  const s = (scores.data ?? {}) as Record<string, unknown>;
  const hasScores = s.composite_risk != null;
  const histData = ((history.data ?? []) as Record<string, unknown>[]).map((r) => ({
    ...r,
    date: String(r.as_of_date ?? ""),
  }));
  const indList = (indicators.data ?? []) as Indicator[];
  const personList = (persons.data ?? []) as Person[];
  const nationResources = ((resourcesQuery.data as any)?.resources ?? []) as Array<{ resource: string; global_share_pct: number; production: string; category: string; price_sensitivity: string; primary_buyers: string[] }>;
  const allChokepoints = (chokepointsQuery.data ?? []) as Array<{
    id: string;
    name: string;
    controlling_nations: string[];
    affected_nations: string[];
    status: string;
    category: string;
    daily_volume: string;
  }>;
  const allTradeRoutes = (tradeRoutesQuery.data ?? []) as Array<{
    id: string;
    name: string;
    source_nations: string[];
    dest_nations: string[];
    category: string;
    volume: string;
    status?: string;
  }>;

  const industries = (industriesQuery.data ?? []) as Array<{
    nation: string; industry: string; category: string; gdp_share_pct: number;
    employment_share_pct: number; global_rank: number; key_companies: string[];
    export_dependency: string; description: string;
  }>;
  const healthMap = new Map(
    ((industryHealthQuery.data ?? []) as Array<{
      industry: string; health_score: number; pmi_component: number;
      output_trend: string; regulatory_pressure: string; sentiment: string;
      growth_yoy_pct: number; as_of_date: string;
    }>).map((h) => [h.industry, h])
  );

  const nationConflicts = (conflictsQuery.data ?? []) as Array<{
    id: string; name: string; conflict_type: string; status: string;
    start_date: string; description: string; escalation_risk: number;
    parties: Array<{ name: string; nations: string[]; role: string; description: string }>;
    affected_nations: string[]; humanitarian: Record<string, string>;
    economic_impact: string;
  }>;

  // Filter chokepoints and trade routes relevant to this nation
  const relevantChokepoints = allChokepoints.filter(
    (c) => c.controlling_nations?.includes(nation) || c.affected_nations?.includes(nation)
  );
  const relevantRoutes = allTradeRoutes.filter(
    (r) => r.source_nations?.includes(nation) || r.dest_nations?.includes(nation)
  );

  const refetch = () => {
    scores.refetch();
    history.refetch();
    indicators.refetch();
    persons.refetch();
    resourcesQuery.refetch();
  };

  // ── indicator table columns ────────────────────────────

  const indCols: Column<Indicator>[] = [
    { key: "series_id", label: "Series", render: (r) => fredLabel(r.series_id) },
    {
      key: "value",
      label: "Value",
      align: "right",
      render: (r) => Number(r.value).toFixed(2),
    },
    { key: "direction", label: "Direction", render: (r) => directionBadge(r.direction) },
    {
      key: "rate_of_change",
      label: "Δ Rate",
      align: "right",
      render: (r) => (r.rate_of_change != null ? `${(r.rate_of_change * 100).toFixed(2)}%` : "—"),
    },
    { key: "observation_date", label: "As Of" },
  ];

  return (
    <div className="space-y-4">
      <PageHeader
        title="Nation Intel"
        subtitle={`Macro scores, indicators & leadership — ${nationLabel(nation)}`}
        onRefresh={refetch}
        actions={
          <select
            value={nation}
            onChange={(e) => setNation(e.target.value)}
            className="rounded border border-border-dim bg-surface-raised px-3 py-1.5 text-sm text-zinc-100 focus:border-accent focus:outline-none"
          >
            {/* Scored nations first, then all remaining */}
            {nations.length > 0 && (
              <optgroup label="Scored Nations">
                {nations.map((n) => (
                  <option key={n.nation} value={n.nation}>
                    {nationLabel(n.nation)} ({(n.composite_risk * 100).toFixed(0)}%)
                  </option>
                ))}
              </optgroup>
            )}
            <optgroup label="All Nations">
              {Object.entries(NATION_NAMES)
                .filter(([iso]) => !scoredSet.has(iso))
                .sort(([, a], [, b]) => a.localeCompare(b))
                .map(([iso]) => (
                  <option key={iso} value={iso}>
                    {nationLabel(iso)}
                  </option>
                ))}
            </optgroup>
          </select>
        }
      />

      {/* ── Country Overview ─────────────────────────── */}
      {nInfo?.description && (
        <Panel title="Country Overview">
          <div className="space-y-3">
            <p className="text-xs text-zinc-300 leading-relaxed">{nInfo.description}</p>
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
              {nInfo.capital && (
                <div className="rounded bg-surface px-2.5 py-1.5">
                  <div className="text-[9px] uppercase text-muted">Capital</div>
                  <div className="text-xs font-medium text-zinc-100">{nInfo.capital}</div>
                </div>
              )}
              {nInfo.population && (
                <div className="rounded bg-surface px-2.5 py-1.5">
                  <div className="text-[9px] uppercase text-muted">Population</div>
                  <div className="text-xs font-medium text-zinc-100">{nInfo.population}</div>
                </div>
              )}
              {nInfo.gdp && (
                <div className="rounded bg-surface px-2.5 py-1.5">
                  <div className="text-[9px] uppercase text-muted">GDP (Nominal)</div>
                  <div className="text-xs font-medium text-zinc-100">{nInfo.gdp}</div>
                </div>
              )}
              {nInfo.government && (
                <div className="rounded bg-surface px-2.5 py-1.5">
                  <div className="text-[9px] uppercase text-muted">Government</div>
                  <div className="text-xs font-medium text-zinc-100">{nInfo.government}</div>
                </div>
              )}
            </div>
            {nInfo.key_sectors && nInfo.key_sectors.length > 0 && (
              <div>
                <div className="text-[9px] uppercase text-muted mb-1">Key Sectors</div>
                <div className="flex flex-wrap gap-1">
                  {nInfo.key_sectors.map((s) => (
                    <span key={s} className="rounded px-2 py-0.5 text-[10px] bg-accent/10 text-accent">{s}</span>
                  ))}
                </div>
              </div>
            )}
            {nInfo.strategic_notes && (
              <div>
                <div className="text-[9px] uppercase text-muted mb-1">Strategic Notes</div>
                <p className="text-[11px] text-zinc-400 leading-relaxed">{nInfo.strategic_notes}</p>
              </div>
            )}
          </div>
        </Panel>
      )}

      {/* ── Score Gauges ───────────────────────────────── */}
      {hasScores && <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        <KpiCard
          label="Composite Risk"
          value={pct(s.composite_risk as number | undefined)}
          sentiment={scoreSentiment(s.composite_risk as number | undefined)}
        />
        <KpiCard
          label="Economic Stability"
          value={pct(s.economic_stability as number | undefined)}
          sentiment={scoreSentiment(s.economic_stability as number | undefined)}
        />
        <KpiCard
          label="Market Stability"
          value={pct(s.market_stability as number | undefined)}
          sentiment={scoreSentiment(s.market_stability as number | undefined)}
        />
        <KpiCard
          label="Political Stability"
          value={pct(s.political_stability as number | undefined)}
          sentiment={scoreSentiment(s.political_stability as number | undefined)}
        />
        <KpiCard
          label="Opportunity"
          value={pct(s.opportunity_score as number | undefined)}
          sentiment={scoreSentiment(s.opportunity_score as number | undefined)}
        />
      </div>}

      {hasScores && <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <KpiCard
          label="Currency Risk"
          value={pct(s.currency_risk as number | undefined)}
          sentiment={scoreSentiment(s.currency_risk as number | undefined)}
        />
        <KpiCard
          label="Contagion Risk"
          value={pct(s.contagion_risk as number | undefined)}
          sentiment={scoreSentiment(s.contagion_risk as number | undefined)}
        />
        <KpiCard
          label="Leadership Risk"
          value={pct(s.leadership_risk as number | undefined)}
          sentiment={scoreSentiment(s.leadership_risk as number | undefined)}
        />
        <KpiCard
          label="Leadership Comp."
          value={pct(s.leadership_composite as number | undefined)}
          sentiment={scoreSentiment(s.leadership_composite as number | undefined)}
        />
      </div>}

      {/* ── Score History Chart ────────────────────────── */}
      {histData.length > 0 && (
        <Panel
          title={`Score History (${ZOOM_LABELS[zoomDays] ?? `${zoomDays}d`})`}
          actions={<ChartZoomBar zoomIdx={zoomIdx} setZoomIdx={setZoomIdx} />}
        >
          <LineChart
            data={histData.slice(-zoomDays)}
            xKey="date"
            yKeys={[
              "composite_risk",
              "economic_stability",
              "political_stability",
              "opportunity_score",
            ]}
            labels={{
              composite_risk: "Composite",
              economic_stability: "Economic",
              political_stability: "Political",
              opportunity_score: "Opportunity",
            }}
            height={400}
            xTickFormatter={fmtDateTick}
          />
        </Panel>
      )}

      {/* ── Core Industries ─────────────────────────────── */}
      <Panel title={`Core Industries (${industries.length})`}>
        {industries.length > 0 ? (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {industries.map((ind) => {
              const h = healthMap.get(ind.industry);
              const hs = h?.health_score;
              const hsColor = hs == null ? "text-muted" : hs >= 0.7 ? "text-positive" : hs >= 0.4 ? "text-amber-400" : "text-negative";
              const regColor = !h?.regulatory_pressure ? "text-muted"
                : h.regulatory_pressure === "low" ? "text-positive"
                : h.regulatory_pressure === "moderate" ? "text-amber-400" : "text-negative";
              const sentColor = !h?.sentiment ? "text-muted"
                : h.sentiment === "positive" ? "text-positive"
                : h.sentiment === "neutral" ? "text-amber-400" : "text-negative";
              return (
                <div key={ind.industry} className="rounded-lg border border-border-dim bg-surface-overlay p-3 space-y-2">
                  {/* header */}
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <div className="text-sm font-semibold text-zinc-100">{ind.industry.replace(/_/g, " ")}</div>
                      <div className="text-[10px] text-muted">{ind.category}</div>
                    </div>
                    {hs != null && (
                      <div className="text-right shrink-0">
                        <div className={`text-lg font-bold tabular-nums ${hsColor}`}>{(hs * 100).toFixed(0)}</div>
                        <div className="text-[8px] text-muted">health</div>
                      </div>
                    )}
                  </div>
                  {/* metrics row */}
                  <div className="grid grid-cols-3 gap-1 text-center">
                    <div>
                      <div className="text-xs font-bold text-accent tabular-nums">{ind.gdp_share_pct.toFixed(1)}%</div>
                      <div className="text-[8px] text-muted">GDP</div>
                    </div>
                    <div>
                      <div className="text-xs font-bold tabular-nums text-zinc-200">#{ind.global_rank}</div>
                      <div className="text-[8px] text-muted">global rank</div>
                    </div>
                    <div>
                      <div className="text-xs font-bold tabular-nums text-zinc-200">{ind.employment_share_pct.toFixed(1)}%</div>
                      <div className="text-[8px] text-muted">employment</div>
                    </div>
                  </div>
                  {/* intelligence row */}
                  {h && (
                    <div className="grid grid-cols-3 gap-1 text-center">
                      <div>
                        <div className={`text-[11px] font-medium capitalize ${regColor}`}>{h.regulatory_pressure}</div>
                        <div className="text-[8px] text-muted">regulation</div>
                      </div>
                      <div>
                        <div className={`text-[11px] font-medium capitalize ${sentColor}`}>{h.sentiment}</div>
                        <div className="text-[8px] text-muted">sentiment</div>
                      </div>
                      <div>
                        <div className="text-[11px] font-medium capitalize text-zinc-200">{h.output_trend}</div>
                        <div className="text-[8px] text-muted">output</div>
                      </div>
                    </div>
                  )}
                  {/* companies */}
                  {ind.key_companies.length > 0 && (
                    <div className="text-[10px] text-muted truncate" title={ind.key_companies.join(", ")}>
                      {ind.key_companies.join(" · ")}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        ) : (
          <div className="py-8 text-center text-xs text-muted">
            No industry profiles for this nation
          </div>
        )}
      </Panel>

      {/* ── Conflicts & Disputes ─────────────────────── */}
      <Panel title={`Conflicts & Disputes (${nationConflicts.length})`}>
        {nationConflicts.length > 0 ? (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {nationConflicts.map((c) => {
              const sc: Record<string, string> = { ACTIVE: "#ef4444", ESCALATING: "#f97316", CEASEFIRE: "#f59e0b", FROZEN: "#6b7280" };
              const clr = sc[c.status] ?? "#888";
              const escClr = c.escalation_risk >= 0.7 ? "text-negative" : c.escalation_risk >= 0.4 ? "text-amber-400" : "text-positive";
              // Find this nation's role
              const myParty = c.parties.find((p) => p.nations.includes(nation));
              const roleLabel = myParty?.role ?? "affected";
              const roleBg: Record<string, string> = { belligerent: "#ef444422", ally: "#f59e0b22", proxy: "#a855f722", supporter: "#3b82f622" };
              const roleClr: Record<string, string> = { belligerent: "#ef4444", ally: "#f59e0b", proxy: "#a855f7", supporter: "#3b82f6" };
              // Opposing sides
              const mySideIdx = c.parties.findIndex((p) => p.nations.includes(nation));
              const opponents = c.parties.filter((_, i) => {
                if (mySideIdx < 0) return false;
                // Simple heuristic: parties at different "positions" are opponents
                // belligerents on opposite sides from our party
                return i !== mySideIdx && c.parties[i].role === "belligerent";
              });

              return (
                <div key={c.id} className="rounded-lg border border-border-dim bg-surface-overlay p-3 space-y-2">
                  {/* header */}
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="text-sm font-semibold text-zinc-100">{c.name}</div>
                      <div className="text-[10px] text-muted">{c.conflict_type.replace(/_/g, " ")} · since {c.start_date.slice(0, 4)}</div>
                    </div>
                    <span
                      className="rounded-full px-2 py-0.5 text-[8px] font-bold uppercase shrink-0"
                      style={{ backgroundColor: `${clr}22`, color: clr, border: `1px solid ${clr}44` }}
                    >
                      {c.status}
                    </span>
                  </div>
                  {/* nation role + escalation */}
                  <div className="flex items-center gap-2">
                    <span
                      className="rounded px-1.5 py-0.5 text-[8px] font-bold uppercase"
                      style={{ backgroundColor: roleBg[roleLabel] ?? "#88888822", color: roleClr[roleLabel] ?? "#888", border: `1px solid ${roleClr[roleLabel] ?? "#888"}44` }}
                    >
                      {roleLabel}
                    </span>
                    <span className="text-[10px] text-muted">via {myParty?.name ?? "affected nation"}</span>
                    <span className="ml-auto text-[10px] font-bold tabular-nums" title="Escalation risk">
                      <span className={escClr}>{(c.escalation_risk * 100).toFixed(0)}%</span>
                      <span className="text-[8px] text-muted ml-0.5">esc.</span>
                    </span>
                  </div>
                  {/* opponents */}
                  {opponents.length > 0 && (
                    <div className="text-[10px] text-muted">
                      <span className="text-[8px] uppercase text-muted">vs </span>
                      {opponents.map((p) => p.name).join(", ")}
                    </div>
                  )}
                  {/* allies */}
                  {myParty && (() => {
                    const allyParties = c.parties.filter((p, i) => i !== mySideIdx && p.role !== "belligerent" && p.nations.some((n) => myParty.nations.includes(n) || p.role === "supporter" || p.role === "ally"));
                    // Simpler: show all non-opponent parties that share our "side"
                    return allyParties.length > 0 ? (
                      <div className="text-[10px] text-muted">
                        <span className="text-[8px] uppercase text-muted">allies </span>
                        {allyParties.map((p) => p.name).join(", ")}
                      </div>
                    ) : null;
                  })()}
                  {/* description */}
                  <div className="text-[10px] text-zinc-400 leading-snug line-clamp-2">{c.description}</div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="py-8 text-center text-xs text-muted">
            No active conflicts or disputes
          </div>
        )}
      </Panel>

      {/* ── Leadership Profiles (full width, prominent) ──── */}
      <Panel title={`Leadership Profiles (${personList.length})`}>
        {personList.length > 0 ? (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {personList.map((p) => (
              <PersonCard key={p.profile_id} person={p} onNavigate={(id) => navigate(`/person?id=${id}&nation=${nation}`)} />
            ))}
          </div>
        ) : (
          <div className="py-8 text-center text-xs text-muted">
            No person profiles — run the seed script to populate
          </div>
        )}
      </Panel>

      {/* ── Resources + Chokepoints + Trade Routes ─────── */}
      <div className="grid gap-4 lg:grid-cols-3">
        {/* Resources */}
        <Panel title={`Resources (${nationResources.length})`}>
          {nationResources.length > 0 ? (
            <div className="space-y-2 max-h-64 overflow-auto">
              {nationResources.map((r) => (
                <div key={r.resource} className="flex items-center justify-between rounded bg-surface-overlay px-2.5 py-1.5">
                  <div>
                    <div className="text-xs font-medium text-zinc-200">{r.resource.replace(/_/g, " ")}</div>
                    <div className="text-[10px] text-muted">{r.category} · {r.production}</div>
                  </div>
                  <div className="text-right">
                    <div className="text-xs font-bold text-accent tabular-nums">
                      {r.global_share_pct.toFixed(1)}%
                    </div>
                    <div className="text-[8px] text-muted">global share</div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">No resource data</div>
          )}
        </Panel>

        {/* Chokepoints */}
        <Panel title={`Chokepoints (${relevantChokepoints.length})`}>
          {relevantChokepoints.length > 0 ? (
            <div className="space-y-2 max-h-64 overflow-auto">
              {relevantChokepoints.map((c) => (
                <div key={c.id} className="rounded bg-surface-overlay px-2.5 py-1.5">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-zinc-200">{c.name}</span>
                    <StatusBadge
                      label={c.status}
                      variant={c.status === "OPEN" ? "positive" : c.status === "THREATENED" ? "warning" : "negative"}
                    />
                  </div>
                  <div className="text-[10px] text-muted mt-0.5">
                    {c.category} · {c.daily_volume}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">No linked chokepoints</div>
          )}
        </Panel>

        {/* Trade Routes */}
        <Panel title={`Trade Routes (${relevantRoutes.length})`}>
          {relevantRoutes.length > 0 ? (
            <div className="space-y-2 max-h-64 overflow-auto">
              {relevantRoutes.map((r) => (
                <div key={r.id} className="rounded bg-surface-overlay px-2.5 py-1.5">
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-medium text-zinc-200">{r.name}</span>
                    <StatusBadge
                      label={r.status || "ACTIVE"}
                      variant={r.status === "DISRUPTED" ? "negative" : r.status === "THREATENED" ? "warning" : "positive"}
                    />
                  </div>
                  <div className="text-[10px] text-muted mt-0.5">
                    {r.category} · {r.volume}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="py-6 text-center text-xs text-muted">No linked trade routes</div>
          )}
        </Panel>
      </div>

      {/* ── Macro Indicators ──────────────────────────── */}
      <Panel title="Macro Indicators">
        <DataTable
          columns={indCols}
          data={indList}
          pageSize={12}
          compact
          emptyMessage="No indicator data — run nation ingestion to populate"
        />
      </Panel>
    </div>
  );
}

// ── Person card sub-component ────────────────────────────

function PersonCard({ person, onNavigate }: { person: Person; onNavigate: (profileId: string) => void }) {
  const tierColors: Record<number, string> = {
    1: "text-accent",
    2: "text-blue-400",
    3: "text-zinc-400",
  };
  const stances = Object.entries(person.policy_stance ?? {});
  const wiki = person.metadata?.wikipedia;

  return (
    <div
      className="rounded border border-border-dim bg-surface-overlay p-3 cursor-pointer hover:border-border-bright transition-colors"
      onClick={() => onNavigate(person.profile_id)}
    >
      <div className="flex gap-3">
        {/* Photo */}
        {wiki?.thumbnail_url ? (
          <img
            src={wiki.thumbnail_url}
            alt={person.person_name}
            className="h-14 w-14 shrink-0 rounded-full object-cover border border-border-dim"
          />
        ) : (
          <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-full bg-surface-raised text-lg text-muted">
            {person.person_name.split(" ").map((n) => n[0]).join("").slice(0, 2)}
          </div>
        )}

        <div className="min-w-0 flex-1">
          <div className="flex items-center justify-between">
            <div>
              <span className={`text-sm font-semibold hover:underline ${tierColors[person.role_tier] ?? "text-zinc-100"}`}>
                {person.person_name}
              </span>
              <span className="ml-2 text-[10px] uppercase text-muted">
                T{person.role_tier} · {person.role.replace(/_/g, " ")}
              </span>
            </div>
            <span className="text-[10px] text-muted">
              conf {(person.confidence * 100).toFixed(0)}%
            </span>
          </div>

          {/* Wikipedia bio */}
          {wiki?.extract && (
            <p className="mt-1 line-clamp-2 text-[11px] leading-relaxed text-zinc-400">
              {wiki.extract}
            </p>
          )}

          {/* Policy stances */}
          {stances.length > 0 && (
            <div className="mt-1.5 flex flex-wrap gap-1.5">
              {stances.map(([k, v]) => (
                <div key={k} className="rounded bg-surface-raised px-1.5 py-0.5 text-[10px]">
                  <span className="text-muted">{k.replace(/_/g, " ")}: </span>
                  <span className="text-zinc-100">{typeof v === "number" ? v.toFixed(2) : String(v)}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
