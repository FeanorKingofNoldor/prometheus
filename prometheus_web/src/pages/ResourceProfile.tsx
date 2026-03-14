import { useMemo } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { useResources, useResourceInfo } from "../api/hooks";
import { NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";
import type { ResourceData } from "../components/ResourceOverlay";

// ── Category styling ─────────────────────────────────────

const CATEGORY_COLORS: Record<string, string> = {
  energy:           "#f59e0b",
  metal:            "#94a3b8",
  critical_mineral: "#a855f7",
  agriculture:      "#22c55e",
  tech_material:    "#3b82f6",
};

const CATEGORY_ICONS: Record<string, string> = {
  energy:           "🛢️",
  metal:            "⛏",
  critical_mineral: "⚗️",
  agriculture:      "🌾",
  tech_material:    "🔬",
};

const CATEGORY_LABELS: Record<string, string> = {
  energy:           "Energy",
  metal:            "Metal",
  critical_mineral: "Critical Mineral",
  agriculture:      "Agriculture",
  tech_material:    "Tech Material",
};

// ── Component ────────────────────────────────────────────

export default function ResourceProfile() {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const resourceId = params.get("r") ?? "";
  const nationParam = params.get("n"); // null = global view, "SAU" = nation-specific

  const resourcesQuery = useResources();
  const allResources = ((resourcesQuery.data as any)?.resources ?? []) as ResourceData[];
  const infoQuery = useResourceInfo(resourceId || undefined);
  const rInfo = infoQuery.data as { display_name?: string; description?: string; uses?: string[]; sectors?: string[]; strategic_importance?: string; supply_chain_notes?: string } | undefined;

  // All entries for this resource, ranked by global share
  const entries = useMemo(
    () =>
      allResources
        .filter((r) => r.resource === resourceId)
        .sort((a, b) => b.global_share_pct - a.global_share_pct),
    [allResources, resourceId],
  );

  const sample = entries[0];
  const displayName = resourceId.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  const icon = sample ? (CATEGORY_ICONS[sample.category] ?? "") : "";
  const catColor = sample ? (CATEGORY_COLORS[sample.category] ?? "#888") : "#888";
  const catLabel = sample ? (CATEGORY_LABELS[sample.category] ?? sample.category) : "";

  if (resourcesQuery.isLoading) {
    return (
      <div className="flex h-64 items-center justify-center text-xs text-muted">
        Loading resource data…
      </div>
    );
  }

  if (!sample) {
    return (
      <div className="space-y-4">
        <PageHeader title="Resource Not Found" subtitle={resourceId || "no resource specified"} />
        <Panel>
          <div className="py-8 text-center text-sm text-muted">
            No data for "{displayName}".{" "}
            <button onClick={() => navigate("/geo")} className="text-accent hover:underline">
              Back to map →
            </button>
          </div>
        </Panel>
      </div>
    );
  }

  // ── Nation-specific view ──
  if (nationParam) {
    const nationEntry = entries.find((r) => r.nation === nationParam);
    if (!nationEntry) {
      return (
        <div className="space-y-4">
          <PageHeader title="Resource Not Found" subtitle={`${displayName} in ${NATION_NAMES[nationParam] ?? nationParam}`} />
          <Panel>
            <div className="py-8 text-center text-sm text-muted">
              No {displayName} data for {NATION_FLAGS[nationParam] ?? ""} {NATION_NAMES[nationParam] ?? nationParam}.{" "}
              <button onClick={() => navigate(`/resource?r=${resourceId}`)} className="text-accent hover:underline">
                View global profile →
              </button>
            </div>
          </Panel>
        </div>
      );
    }

    const flag = NATION_FLAGS[nationParam] ?? "";
    const nationName = NATION_NAMES[nationParam] ?? nationParam;
    const rank = entries.findIndex((r) => r.nation === nationParam) + 1;
    // Other resources this nation produces
    const otherNationResources = allResources
      .filter((r) => r.nation === nationParam && r.resource !== resourceId)
      .sort((a, b) => b.global_share_pct - a.global_share_pct);

    return (
      <div className="space-y-4">
        <PageHeader
          title={`${icon} ${displayName} — ${flag} ${nationName}`}
          subtitle={`${catLabel} · Rank #${rank} of ${entries.length} producers`}
        />

        {/* About this resource */}
        {rInfo?.description && <AboutResourcePanel rInfo={rInfo} catColor={catColor} />}

        {/* Key metrics */}
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard label="Global Share" value={`${nationEntry.global_share_pct}%`} color={catColor} />
          <StatCard label="Production" value={nationEntry.production} color={catColor} />
          <StatCard label="Reserves" value={nationEntry.proven_reserves !== "N/A" ? `${nationEntry.proven_reserves}${nationEntry.reserve_years ? ` (${nationEntry.reserve_years}yr)` : ""}` : "N/A"} color={catColor} />
          <StatCard label="Unit" value={nationEntry.unit || "N/A"} color={catColor} />
        </div>

        {/* Price sensitivity */}
        <Panel title="Price Sensitivity">
          <p className="text-xs text-zinc-300 leading-relaxed">{nationEntry.price_sensitivity}</p>
        </Panel>

        {/* Buyers */}
        {nationEntry.primary_buyers.length > 0 && (
          <Panel title={`Primary Buyers (${nationEntry.primary_buyers.length})`}>
            <div className="flex flex-wrap gap-1.5">
              {nationEntry.primary_buyers.map((b) => (
                <button
                  key={b}
                  onClick={() => navigate(`/nation?n=${b}`)}
                  className="rounded bg-surface-overlay px-2.5 py-1 text-[11px] text-zinc-300 hover:bg-surface-raised hover:text-zinc-100 transition-colors"
                >
                  {NATION_FLAGS[b] ?? ""} {NATION_NAMES[b] ?? b}
                </button>
              ))}
            </div>
          </Panel>
        )}

        {/* Ranking among producers */}
        <Panel title={`All Producers (${entries.length})`}>
          <div className="space-y-1.5">
            {entries.map((r, i) => {
              const isThis = r.nation === nationParam;
              return (
                <div
                  key={r.nation}
                  className={`flex items-center gap-3 rounded px-3 py-2 transition-colors cursor-pointer ${
                    isThis ? "bg-accent/10 border border-accent/30" : "bg-surface-overlay hover:bg-surface-raised"
                  }`}
                  onClick={() => isThis ? navigate(`/nation?n=${r.nation}`) : navigate(`/resource?r=${resourceId}&n=${r.nation}`)}
                >
                  <span className="w-5 text-[10px] text-muted text-right">#{i + 1}</span>
                  <span className="text-sm">{NATION_FLAGS[r.nation] ?? ""}</span>
                  <div className="min-w-0 flex-1">
                    <div className={`text-xs font-medium truncate ${isThis ? "text-accent" : "text-zinc-100"}`}>
                      {NATION_NAMES[r.nation] ?? r.nation}
                    </div>
                    <div className="text-[10px] text-muted">{r.production}</div>
                  </div>
                  <div className="w-24 flex items-center gap-1.5">
                    <div className="flex-1 h-1.5 rounded-full bg-surface overflow-hidden">
                      <div className="h-full rounded-full" style={{ width: `${Math.min(r.global_share_pct, 100)}%`, backgroundColor: catColor }} />
                    </div>
                    <span className="text-[10px] font-bold tabular-nums" style={{ color: catColor }}>{r.global_share_pct}%</span>
                  </div>
                </div>
              );
            })}
          </div>
        </Panel>

        {/* Other resources this nation produces */}
        {otherNationResources.length > 0 && (
          <Panel title={`Other Resources from ${flag} ${nationName} (${otherNationResources.length})`}>
            <div className="grid gap-1.5 sm:grid-cols-2 lg:grid-cols-3">
              {otherNationResources.map((r) => (
                <button
                  key={r.resource}
                  onClick={() => navigate(`/resource?r=${r.resource}&n=${nationParam}`)}
                  className="flex items-center justify-between rounded bg-surface-overlay px-3 py-2 hover:bg-surface-raised transition-colors text-left"
                >
                  <div>
                    <div className="text-[11px] font-medium text-zinc-200">
                      {CATEGORY_ICONS[r.category] ?? ""} {r.resource.replace(/_/g, " ")}
                    </div>
                    <div className="text-[9px] text-muted">{r.production}</div>
                  </div>
                  <span className="text-[10px] font-bold tabular-nums" style={{ color: CATEGORY_COLORS[r.category] ?? "#888" }}>
                    {r.global_share_pct}%
                  </span>
                </button>
              ))}
            </div>
          </Panel>
        )}

        {/* Links */}
        <div className="flex gap-4 justify-center pb-4">
          <button onClick={() => navigate(`/resource?r=${resourceId}`)} className="text-[11px] text-accent hover:underline">
            ← Global {displayName} Profile
          </button>
          <button onClick={() => navigate(`/nation?n=${nationParam}`)} className="text-[11px] text-accent hover:underline">
            {flag} {nationName} Profile →
          </button>
          <button onClick={() => navigate("/geo")} className="text-[11px] text-muted hover:underline">
            Back to Map
          </button>
        </div>
      </div>
    );
  }

  // ── Global view (original) ──
  const totalSharePct = entries.reduce((s, r) => s + r.global_share_pct, 0);
  const allBuyers = [...new Set(entries.flatMap((r) => r.primary_buyers))];
  const priceSensitivities = [...new Set(entries.map((r) => r.price_sensitivity))];

  return (
    <div className="space-y-4">
      <PageHeader
        title={`${icon} ${displayName}`}
        subtitle={`${catLabel} · ${entries.length} producer${entries.length !== 1 ? "s" : ""} tracked`}
      />

      {/* About this resource */}
      {rInfo?.description && <AboutResourcePanel rInfo={rInfo} catColor={catColor} />}

      {/* Overview cards */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <StatCard label="Category" value={`${icon} ${catLabel}`} color={catColor} />
        <StatCard label="Tracked Production" value={`${totalSharePct.toFixed(1)}% of world`} color={catColor} />
        <StatCard label="Top Producer" value={`${NATION_FLAGS[entries[0].nation] ?? ""} ${NATION_NAMES[entries[0].nation] ?? entries[0].nation}`} sub={`${entries[0].global_share_pct}% share`} color={catColor} />
        <StatCard label="Unit" value={sample.unit || "N/A"} color={catColor} />
      </div>

      {/* Producer rankings */}
      <Panel title="Producers by Global Share">
        <div className="space-y-1.5">
          {entries.map((r, i) => {
            const flag = NATION_FLAGS[r.nation] ?? "";
            const name = NATION_NAMES[r.nation] ?? r.nation;
            return (
              <div
                key={r.nation}
                className="flex items-center gap-3 rounded bg-surface-overlay px-3 py-2 hover:bg-surface-raised transition-colors cursor-pointer"
                onClick={() => navigate(`/resource?r=${resourceId}&n=${r.nation}`)}
              >
                <span className="w-5 text-[10px] text-muted text-right">#{i + 1}</span>
                <span className="text-sm">{flag}</span>
                <div className="min-w-0 flex-1">
                  <div className="text-xs font-medium text-zinc-100 truncate">{name}</div>
                  <div className="text-[10px] text-muted">{r.production}</div>
                </div>
                {/* Share bar */}
                <div className="w-24 flex items-center gap-1.5">
                  <div className="flex-1 h-1.5 rounded-full bg-surface overflow-hidden">
                    <div
                      className="h-full rounded-full"
                      style={{ width: `${Math.min(r.global_share_pct, 100)}%`, backgroundColor: catColor }}
                    />
                  </div>
                  <span className="text-[10px] font-bold tabular-nums" style={{ color: catColor }}>
                    {r.global_share_pct}%
                  </span>
                </div>
                {/* Reserves */}
                <div className="w-20 text-right">
                  {r.proven_reserves !== "N/A" ? (
                    <>
                      <div className="text-[10px] text-zinc-300">{r.proven_reserves}</div>
                      {r.reserve_years && <div className="text-[8px] text-muted">{r.reserve_years}yr left</div>}
                    </>
                  ) : (
                    <span className="text-[9px] text-muted">—</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </Panel>

      {/* Buyer network + price sensitivity */}
      <div className="grid gap-3 sm:grid-cols-2">
        <Panel title="Primary Buyers">
          {allBuyers.length > 0 ? (
            <div className="flex flex-wrap gap-1.5">
              {allBuyers.map((b) => (
                <button
                  key={b}
                  onClick={() => navigate(`/nation?n=${b}`)}
                  className="rounded bg-surface-overlay px-2.5 py-1 text-[11px] text-zinc-300 hover:bg-surface-raised hover:text-zinc-100 transition-colors"
                >
                  {NATION_FLAGS[b] ?? ""} {NATION_NAMES[b] ?? b}
                </button>
              ))}
            </div>
          ) : (
            <div className="py-4 text-center text-[10px] text-muted">No buyer data</div>
          )}
        </Panel>

        <Panel title="Price Sensitivity">
          <div className="space-y-2">
            {priceSensitivities.map((ps, i) => (
              <div key={i} className="rounded bg-surface-overlay px-3 py-2">
                <p className="text-[11px] text-zinc-300 leading-relaxed">{ps}</p>
              </div>
            ))}
          </div>
        </Panel>
      </div>

      {/* Back link */}
      <div className="text-center pb-4">
        <button
          onClick={() => navigate("/geo")}
          className="text-[11px] text-accent hover:underline"
        >
          ← Back to Geopolitical Risk Map
        </button>
      </div>
    </div>
  );
}

// ── About Resource panel ─────────────────────────────────

function AboutResourcePanel({ rInfo, catColor }: { rInfo: { description?: string; uses?: string[]; sectors?: string[]; strategic_importance?: string; supply_chain_notes?: string }; catColor: string }) {
  return (
    <Panel title="About This Resource">
      <div className="space-y-3">
        <p className="text-xs text-zinc-300 leading-relaxed">{rInfo.description}</p>
        {rInfo.uses && rInfo.uses.length > 0 && (
          <div>
            <div className="text-[9px] uppercase text-muted mb-1">Primary Uses</div>
            <div className="flex flex-wrap gap-1">
              {rInfo.uses.map((u) => (
                <span key={u} className="rounded px-2 py-0.5 text-[10px] bg-surface text-zinc-300">{u}</span>
              ))}
            </div>
          </div>
        )}
        {rInfo.sectors && rInfo.sectors.length > 0 && (
          <div>
            <div className="text-[9px] uppercase text-muted mb-1">Key Sectors</div>
            <div className="flex flex-wrap gap-1">
              {rInfo.sectors.map((s) => (
                <span key={s} className="rounded px-2 py-0.5 text-[10px] font-medium" style={{ backgroundColor: catColor + "22", color: catColor }}>{s}</span>
              ))}
            </div>
          </div>
        )}
        {rInfo.strategic_importance && (
          <div>
            <div className="text-[9px] uppercase text-muted mb-1">Strategic Importance</div>
            <p className="text-[11px] text-zinc-300 leading-relaxed">{rInfo.strategic_importance}</p>
          </div>
        )}
        {rInfo.supply_chain_notes && (
          <div>
            <div className="text-[9px] uppercase text-muted mb-1">Supply Chain Notes</div>
            <p className="text-[11px] text-zinc-400 leading-relaxed">{rInfo.supply_chain_notes}</p>
          </div>
        )}
      </div>
    </Panel>
  );
}

// ── Stat card ────────────────────────────────────────────

function StatCard({ label, value, sub, color }: { label: string; value: string; sub?: string; color: string }) {
  return (
    <div className="rounded-lg border border-border-dim bg-surface-overlay p-3">
      <div className="text-[9px] uppercase text-muted mb-1">{label}</div>
      <div className="text-sm font-semibold text-zinc-100">{value}</div>
      {sub && <div className="text-[10px] mt-0.5" style={{ color }}>{sub}</div>}
    </div>
  );
}
