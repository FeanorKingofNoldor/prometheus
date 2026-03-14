import { memo, useState } from "react";
import { Marker } from "react-simple-maps";

// ── Types ────────────────────────────────────────────────────────

export interface NavalDeploymentData {
  id: string;
  name: string;
  hull_number: string;
  ship_type: string;
  nation: string;
  region: string;
  coordinates: [number, number];
  operation: string;
  strike_group: string;
  status: string;
  homeport: string;
  last_updated: string;
  source: string;
  conflict_ids: string[];
}

interface NavalDeploymentOverlayProps {
  deployments: NavalDeploymentData[];
  zoom: number;
}

// ── Colors by ship type ──────────────────────────────────────────

const TYPE_COLORS: Record<string, string> = {
  carrier:    "#ef4444", // red
  destroyer:  "#3b82f6", // blue
  amphibious: "#22c55e", // green
  lcs:        "#06b6d4", // cyan
  cruiser:    "#a855f7", // purple
  submarine:  "#6b7280", // gray
  icebreaker: "#e0e7ff", // ice-white
  command:    "#f59e0b", // amber
};

const TYPE_LABELS: Record<string, string> = {
  carrier:    "Aircraft Carrier",
  destroyer:  "Destroyer",
  amphibious: "Amphibious",
  lcs:        "Littoral Combat Ship",
  cruiser:    "Cruiser",
  submarine:  "Submarine",
  icebreaker: "Icebreaker",
  command:    "Command Ship",
};

// ── Ship Marker SVG ──────────────────────────────────────────────

function ShipIcon({ color, size }: { color: string; size: number }) {
  const half = size / 2;
  return (
    <g>
      {/* Hull shape — diamond/shield */}
      <polygon
        points={`0,${-size} ${half},0 0,${half} ${-half},0`}
        fill={color}
        fillOpacity={0.85}
        stroke={color}
        strokeWidth={size * 0.15}
        strokeOpacity={0.5}
      />
      <circle r={size * 0.25} fill="#fff" fillOpacity={0.7} />
    </g>
  );
}

// ── Timestamp formatter ──────────────────────────────────────────

function formatDate(iso: string): string {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    return d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
  } catch { return iso; }
}

// ── Component ────────────────────────────────────────────────────

export const NavalDeploymentOverlay = memo(function NavalDeploymentOverlay({
  deployments,
  zoom,
}: NavalDeploymentOverlayProps) {
  const [hovered, setHovered] = useState<string | null>(null);
  const markerSize = Math.max(2, 4 / zoom);
  const s = 1 / zoom;

  return (
    <>
      {deployments.map((d) => {
        const isHov = hovered === d.id;
        const color = TYPE_COLORS[d.ship_type] ?? "#f59e0b";
        const sz = isHov ? markerSize * 1.5 : markerSize;

        return (
          <Marker
            key={d.id}
            coordinates={d.coordinates}
            onMouseEnter={() => setHovered(d.id)}
            onMouseLeave={() => setHovered(null)}
            style={{
              default: { cursor: "pointer" },
              hover: { cursor: "pointer" },
              pressed: { cursor: "pointer" },
            }}
          >
            {/* Pulsing ring for carriers */}
            {d.ship_type === "carrier" && (
              <circle
                r={sz * 2.5}
                fill="none"
                stroke={color}
                strokeWidth={sz * 0.2}
                opacity={0.3}
                className="animate-map-pulse"
              />
            )}
            <ShipIcon color={color} size={sz} />

            {/* Hover tooltip — scaled inversely with zoom */}
            {isHov && (
              <g transform={`scale(${s})`}>
                <foreignObject x={14} y={-60} width={260} height={155} style={{ pointerEvents: "none", overflow: "visible" }}>
                  <div
                    style={{
                      background: "rgba(10,10,15,0.95)",
                      border: `1px solid ${color}55`,
                      borderRadius: 6,
                      padding: "5px 8px",
                      fontSize: 10,
                      color: "#e4e4e7",
                      whiteSpace: "nowrap",
                      lineHeight: 1.45,
                    }}
                  >
                    <div style={{ fontWeight: 700, color, fontSize: 11 }}>
                      {d.name}
                      <span style={{ fontWeight: 400, color: "#a1a1aa", marginLeft: 6, fontSize: 10 }}>{d.hull_number}</span>
                    </div>
                    <div>{TYPE_LABELS[d.ship_type] ?? d.ship_type} · 🇺🇸 {d.nation}</div>
                    <div style={{ color: "#d4d4d8" }}>{d.region}</div>
                    {d.strike_group && <div style={{ color: "#a1a1aa" }}>🛡️ {d.strike_group}</div>}
                    {d.operation && (
                      <div style={{ color: "#f87171", fontWeight: 600 }}>⚔️ Op. {d.operation}</div>
                    )}
                    {d.conflict_ids.length > 0 && (
                      <div style={{ color: "#ef4444", fontSize: 9 }}>
                        🔗 Linked to {d.conflict_ids.length} conflict{d.conflict_ids.length > 1 ? "s" : ""}
                      </div>
                    )}
                    <div style={{ color: "#52525b", fontSize: 8, marginTop: 1 }}>
                      {d.status.toUpperCase()} · Updated {formatDate(d.last_updated)} · {d.source}
                    </div>
                  </div>
                </foreignObject>
              </g>
            )}
          </Marker>
        );
      })}
    </>
  );
});
