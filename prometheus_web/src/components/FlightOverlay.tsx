import { memo, useState } from "react";
import { Marker } from "react-simple-maps";

// ── Types ────────────────────────────────────────────────────

export interface FlightData {
  icao24: string;
  callsign: string | null;
  lat: number;
  lon: number;
  altitude: number | null;
  velocity: number | null;
  heading: number | null;
  on_ground: boolean;
  category: "military" | "cargo" | "passenger" | "other";
  subcategory: string;
  operator: string | null;
  country: string;
  flag_iso3: string;
  last_seen: string;
  status?: "active" | "dark";
  dark_since?: string | null;
}

interface FlightOverlayProps {
  flights: FlightData[];
  zoom: number;
}

// ── Color scheme ─────────────────────────────────────────────

const CATEGORY_COLORS: Record<string, string> = {
  military: "#ef4444",       // red
  cargo: "#94a3b8",          // slate-400
  cargo_airline: "#60a5fa",  // blue-400
  passenger: "#22d3ee",      // cyan-400
  transport: "#f59e0b",      // amber (mil transport)
  fighter: "#dc2626",        // red-600
  tanker: "#fb923c",         // orange-400
  surveillance: "#a855f7",   // purple
  other: "#6b7280",          // gray-500
};

function flightColor(f: FlightData): string {
  if (f.category === "military") {
    return CATEGORY_COLORS[f.subcategory] ?? CATEGORY_COLORS.military;
  }
  if (f.category === "cargo") return CATEGORY_COLORS.cargo_airline;
  return CATEGORY_COLORS[f.category] ?? CATEGORY_COLORS.other;
}

// ── SVG aircraft marker (simplified plane shape) ─────────────

function PlaneMarker({ heading, color, size }: { heading: number; color: string; size: number }) {
  const rot = heading ?? 0;
  // Simple aircraft silhouette: fuselage + wings
  return (
    <g transform={`rotate(${rot})`}>
      {/* Fuselage */}
      <line x1={0} y1={-size * 1.2} x2={0} y2={size * 0.8} stroke={color} strokeWidth={size * 0.25} strokeLinecap="round" />
      {/* Wings */}
      <line x1={-size} y1={0} x2={size} y2={0} stroke={color} strokeWidth={size * 0.2} strokeLinecap="round" />
      {/* Tail */}
      <line x1={-size * 0.5} y1={size * 0.6} x2={size * 0.5} y2={size * 0.6} stroke={color} strokeWidth={size * 0.15} strokeLinecap="round" />
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

export const FlightOverlay = memo(function FlightOverlay({ flights, zoom }: FlightOverlayProps) {
  const [hovered, setHovered] = useState<string | null>(null);
  const markerSize = Math.max(1.2, 2.5 / zoom);
  const s = 1 / zoom; // scale factor for tooltip

  return (
    <>
      {flights.map((f) => {
        const isHov = hovered === f.icao24;
        const isDark = f.status === "dark";
        const color = flightColor(f);
        const sz = isHov ? markerSize * 1.6 : markerSize;

        return (
          <Marker
            key={f.icao24}
            coordinates={[f.lon, f.lat]}
            onMouseEnter={() => setHovered(f.icao24)}
            onMouseLeave={() => setHovered(null)}
            style={{
              default: { cursor: "pointer" },
              hover: { cursor: "pointer" },
              pressed: { cursor: "pointer" },
            }}
          >
            <g opacity={isDark ? 0.3 : 1}>
              <PlaneMarker heading={f.heading ?? 0} color={color} size={sz} />
              {/* Dashed ring for dark aircraft */}
              {isDark && (
                <circle r={sz * 2} fill="none" stroke={color} strokeWidth={0.3} strokeDasharray="1.5 1" opacity={0.6} />
              )}
            </g>

            {/* Hover tooltip — scaled inversely with zoom */}
            {isHov && (
              <g transform={`scale(${s})`}>
                <foreignObject x={12} y={-52} width={230} height={150} style={{ pointerEvents: "none", overflow: "visible" }}>
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
                    <div style={{ fontWeight: 700, color }}>
                      {f.callsign || f.icao24}
                      {f.operator && <span style={{ fontWeight: 400, color: "#a1a1aa" }}> · {f.operator}</span>}
                    </div>
                    {isDark && (
                      <div style={{ color: "#f59e0b", fontSize: 9, fontWeight: 700 }}>
                        ⚠ GONE DARK {f.dark_since ? `since ${formatTimestamp(f.dark_since)}` : ""}
                      </div>
                    )}
                    <div>{f.country}</div>
                    <div>
                      {f.altitude != null ? `FL${Math.round(f.altitude / 30.48).toString().padStart(3, "0")}` : "—"}
                      {" · "}
                      {f.velocity != null ? `${Math.round(f.velocity * 1.944)} kts` : "—"}
                      {" · "}
                      {f.heading != null ? `${Math.round(f.heading)}°` : "—"}
                    </div>
                    <div style={{ color: "#71717a", fontSize: 9 }}>{f.category} / {f.subcategory}</div>
                    <div style={{ color: "#52525b", fontSize: 8, marginTop: 1 }}>Last seen: {formatTimestamp(f.last_seen)}</div>
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
