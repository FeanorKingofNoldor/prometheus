import { useMemo, memo } from "react";
import { Marker, Line } from "react-simple-maps";
import { COUNTRY_CENTROIDS, NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";

// ── Types ────────────────────────────────────────────────

export interface ResourceData {
  nation: string;
  resource: string;
  category: string;
  production: string;
  global_share_pct: number;
  proven_reserves: string;
  reserve_years: number | null;
  primary_buyers: string[];
  price_sensitivity: string;
  unit: string;
}

interface ResourceOverlayProps {
  resources: ResourceData[];
  hoveredNation: string | null;
  selectedResource?: string | null;
  onClickResource?: (resource: string, nation: string) => void;
}

// ── Category colors + icons ──────────────────────────────

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

// ── Component ────────────────────────────────────────────

export const ResourceOverlay = memo(function ResourceOverlay({ resources, hoveredNation, selectedResource, onClickResource }: ResourceOverlayProps) {
  // Filter by selected resource if set
  const filtered = useMemo(
    () => selectedResource ? resources.filter((r) => r.resource === selectedResource) : resources,
    [resources, selectedResource],
  );

  // Group resources by nation, pick top-2 by global share
  const nationTopResources = useMemo(() => {
    const byNation = new Map<string, ResourceData[]>();
    for (const r of filtered) {
      if (!byNation.has(r.nation)) byNation.set(r.nation, []);
      byNation.get(r.nation)!.push(r);
    }
    // Sort each nation's resources and take top 2
    const result = new Map<string, ResourceData[]>();
    for (const [nation, res] of byNation) {
      const sorted = [...res].sort((a, b) => b.global_share_pct - a.global_share_pct);
      result.set(nation, sorted.slice(0, 2));
    }
    return result;
  }, [filtered]);

  // Export flow lines for hovered/selected nation (or all producers of selectedResource)
  const exportFlows = useMemo(() => {
    // When a resource is selected, show flows for ALL producers of that resource
    const producerNations = selectedResource
      ? [...new Set(filtered.map((r) => r.nation))]
      : hoveredNation ? [hoveredNation] : [];
    if (producerNations.length === 0) return [];

    const flows: { from: [number, number]; to: [number, number]; buyer: string; weight: number; resources: string[] }[] = [];
    for (const nat of producerNations) {
      const nationRes = filtered.filter((r) => r.nation === nat);
      const fromCoords = COUNTRY_CENTROIDS[nat];
      if (!fromCoords || nationRes.length === 0) continue;
      for (const r of nationRes) {
        for (const buyer of r.primary_buyers) {
          const toCoords = COUNTRY_CENTROIDS[buyer];
          if (!toCoords) continue;
          flows.push({ from: fromCoords, to: toCoords, buyer, weight: r.global_share_pct, resources: [r.resource] });
        }
      }
    }
    return flows;
  }, [hoveredNation, filtered, selectedResource]);

  return (
    <>
      {/* Export flow lines from hovered producer to buyers */}
      {exportFlows.map((flow) => (
        <Line
          key={`export-${hoveredNation}-${flow.buyer}`}
          from={flow.from}
          to={flow.to}
          stroke="#22c55e"
          strokeWidth={Math.max(0.4, Math.min(flow.weight / 30, 1.5))}
          strokeLinecap="round"
          strokeDasharray="3 2"
          className="animate-flow-forward"
          style={{
            opacity: 0.5,
            markerEnd: "url(#arrow-resource)",
          }}
        />
      ))}

      {/* Resource badges on nation markers */}
      {Array.from(nationTopResources).map(([nation, topRes]) => {
        const coords = COUNTRY_CENTROIDS[nation];
        if (!coords) return null;
        const isHovered = nation === hoveredNation;

        return (
          <Marker key={`res-${nation}`} coordinates={coords}>
            {/* Resource badges, offset below the nation marker */}
            {topRes.map((r, i) => {
              const catColor = CATEGORY_COLORS[r.category] ?? "#888";
              const icon = CATEGORY_ICONS[r.category] ?? "•";
              return (
                <g
                  key={r.resource}
                  transform={`translate(${i * 14 - 7}, 8)`}
                  style={{ cursor: onClickResource ? "pointer" : "default" }}
                  onClick={(e) => {
                    e.stopPropagation();
                    onClickResource?.(r.resource, nation);
                  }}
                >
                  <rect
                    x={-6}
                    y={-4}
                    width={12}
                    height={8}
                    rx={2}
                    fill={`${catColor}33`}
                    stroke={catColor}
                    strokeWidth={0.3}
                  />
                  <text
                    textAnchor="middle"
                    y={2.5}
                    style={{
                      fontSize: "5px",
                      fill: catColor,
                      fontWeight: 600,
                      pointerEvents: "none",
                    }}
                  >
                    {icon}
                  </text>
                </g>
              );
            })}
          </Marker>
        );
      })}
    </>
  );
});


// ── Tooltip (rendered outside SVG) ───────────────────────

export function ResourceTooltip({
  nation,
  resources,
  x,
  y,
}: {
  nation: string;
  resources: ResourceData[];
  x: number;
  y: number;
}) {
  const flag = NATION_FLAGS[nation] ?? "";
  const name = NATION_NAMES[nation] ?? nation;
  const nationRes = resources
    .filter((r) => r.nation === nation)
    .sort((a, b) => b.global_share_pct - a.global_share_pct);

  if (nationRes.length === 0) return null;

  const style: React.CSSProperties = {
    position: "fixed",
    left: Math.min(x + 16, window.innerWidth - 300),
    top: Math.min(y - 10, window.innerHeight - 350),
    zIndex: 50,
    pointerEvents: "none",
  };

  return (
    <div style={style} className="animate-fade-in">
      <div className="w-72 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl p-3">
        <div className="text-xs font-semibold text-zinc-100 mb-2">
          {flag} {name} — Resources
        </div>

        <div className="space-y-1.5 max-h-60 overflow-y-auto">
          {nationRes.map((r) => {
            const catColor = CATEGORY_COLORS[r.category] ?? "#888";
            const icon = CATEGORY_ICONS[r.category] ?? "•";
            return (
              <div
                key={r.resource}
                className="rounded bg-surface-overlay px-2 py-1.5"
              >
                <div className="flex items-center justify-between mb-0.5">
                  <span className="text-[10px] font-medium text-zinc-200">
                    {icon} {r.resource.replace(/_/g, " ")}
                  </span>
                  <span
                    className="text-[10px] font-bold tabular-nums"
                    style={{ color: catColor }}
                  >
                    {r.global_share_pct}% world
                  </span>
                </div>
                <div className="text-[9px] text-zinc-400">{r.production}</div>
                {r.proven_reserves !== "N/A" && (
                  <div className="text-[9px] text-zinc-500">
                    Reserves: {r.proven_reserves}
                    {r.reserve_years && ` (${r.reserve_years}yr)`}
                  </div>
                )}
                <div className="text-[8px] text-zinc-500 mt-0.5">
                  {r.price_sensitivity}
                </div>
                {r.primary_buyers.length > 0 && (
                  <div className="text-[8px] text-muted mt-0.5">
                    Buyers: {r.primary_buyers.map((n) => `${NATION_FLAGS[n] ?? ""} ${n}`).join(", ")}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
