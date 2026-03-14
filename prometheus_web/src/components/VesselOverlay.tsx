import { memo, useState } from "react";
import { Marker } from "react-simple-maps";

// ── Types ────────────────────────────────────────────────────

export interface VesselData {
  mmsi: number;
  name: string;
  lat: number;
  lon: number;
  speed: number;
  heading: number | null;
  course: number;
  vessel_type: number;
  type_label: string;
  category: "military" | "commercial" | "law_enforcement";
  subcategory: string;
  flag_country: string;
  flag_iso3: string;
  destination: string | null;
  draught: number | null;
  last_seen: string;
  status?: "active" | "dark";
  dark_since?: string | null;
}

interface VesselOverlayProps {
  vessels: VesselData[];
  zoom: number;
}

// ── Color scheme ─────────────────────────────────────────────

const CATEGORY_COLORS: Record<string, string> = {
  military: "#ef4444",        // red
  law_enforcement: "#f97316", // orange
  cargo: "#94a3b8",           // slate-400
  tanker: "#f59e0b",          // amber
  container: "#60a5fa",       // blue-400
  bulk: "#a1a1aa",            // zinc-400
  passenger: "#22d3ee",       // cyan-400
  other: "#6b7280",           // gray-500
};

function vesselColor(v: VesselData): string {
  if (v.category === "military") return CATEGORY_COLORS.military;
  if (v.category === "law_enforcement") return CATEGORY_COLORS.law_enforcement;
  return CATEGORY_COLORS[v.subcategory] ?? CATEGORY_COLORS.other;
}

// ── SVG ship marker (triangle pointing in heading direction) ─

function ShipMarker({ heading, color, size }: { heading: number; color: string; size: number }) {
  const rot = heading ?? 0;
  return (
    <g transform={`rotate(${rot})`}>
      <polygon
        points={`0,${-size} ${size * 0.6},${size * 0.5} ${-size * 0.6},${size * 0.5}`}
        fill={color}
        stroke="#0a0a0f"
        strokeWidth={0.4}
        opacity={0.85}
      />
    </g>
  );
}

// ── Helpers ───────────────────────────────────────────────────

function formatTimestamp(iso: string): string {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    return d.toLocaleString("en-GB", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "UTC" }) + " UTC";
  } catch { return iso; }
}

// ── Component ────────────────────────────────────────────────

export const VesselOverlay = memo(function VesselOverlay({ vessels, zoom }: VesselOverlayProps) {
  const [hovered, setHovered] = useState<number | null>(null);
  const markerSize = Math.max(1.5, 3 / zoom);
  const s = 1 / zoom; // scale factor for tooltip

  return (
    <>
      {vessels.map((v) => {
        const isHov = hovered === v.mmsi;
        const isDark = v.status === "dark";
        const color = vesselColor(v);
        const sz = isHov ? markerSize * 1.6 : markerSize;

        return (
          <Marker
            key={v.mmsi}
            coordinates={[v.lon, v.lat]}
            onMouseEnter={() => setHovered(v.mmsi)}
            onMouseLeave={() => setHovered(null)}
            style={{
              default: { cursor: "pointer" },
              hover: { cursor: "pointer" },
              pressed: { cursor: "pointer" },
            }}
          >
            <g opacity={isDark ? 0.3 : 1}>
              <ShipMarker heading={v.heading ?? v.course} color={color} size={sz} />
              {/* Dashed ring for dark vessels */}
              {isDark && (
                <circle r={sz * 2} fill="none" stroke={color} strokeWidth={0.3} strokeDasharray="1.5 1" opacity={0.6} />
              )}
            </g>

            {/* Hover tooltip — scaled inversely with zoom */}
            {isHov && (
              <g transform={`scale(${s})`}>
                <foreignObject x={12} y={-48} width={220} height={150} style={{ pointerEvents: "none", overflow: "visible" }}>
                  <div
                    style={{
                      background: "rgba(10,10,15,0.95)",
                      border: `1px solid ${isDark ? "#92400e" : "#333"}`,
                      borderRadius: 6,
                      padding: "4px 8px",
                      fontSize: 10,
                      color: "#e4e4e7",
                      whiteSpace: "nowrap",
                      lineHeight: 1.4,
                    }}
                  >
                    <div style={{ fontWeight: 700, color }}>{v.name}</div>
                    {isDark && (
                      <div style={{ color: "#f59e0b", fontSize: 9, fontWeight: 700 }}>
                        ⚠ GONE DARK {v.dark_since ? `since ${formatTimestamp(v.dark_since)}` : ""}
                      </div>
                    )}
                    <div>{v.type_label} · {v.flag_iso3}</div>
                    <div>{v.speed} kn · {v.heading ?? "—"}°</div>
                    {v.destination && <div>→ {v.destination}</div>}
                    <div style={{ color: "#71717a", fontSize: 9 }}>{v.category} / {v.subcategory}</div>
                    <div style={{ color: "#52525b", fontSize: 8, marginTop: 1 }}>Last seen: {formatTimestamp(v.last_seen)}</div>
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
