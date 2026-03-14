import { memo } from "react";
import { Marker, Line } from "react-simple-maps";
import { NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";

// ── Types ────────────────────────────────────────────────

export interface PortData {
  id: string;
  name: string;
  port_type: "seaport" | "cargo_airport";
  coordinates: [number, number];
  nation: string;
  iata_or_locode: string;
  annual_volume: string;
  volume_unit: string;
  key_commodities: string[];
  connected_routes: string[];
  connected_chokepoints: string[];
  status: "OPERATIONAL" | "CONGESTED" | "DISRUPTED";
  description: string;
}

interface PortMarkersProps {
  ports: PortData[];
  onHover: (id: string | null) => void;
  hoveredId: string | null;
  /** Chokepoint coordinates for drawing connection lines */
  chokepointCoords?: Record<string, [number, number]>;
}

// ── Status colors ────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  OPERATIONAL: "#8b5cf6",
  CONGESTED:   "#f59e0b",
  DISRUPTED:   "#ef4444",
};

const STATUS_LABELS: Record<string, string> = {
  OPERATIONAL: "Operational",
  CONGESTED:   "Congested",
  DISRUPTED:   "Disrupted",
};

// ── Component ────────────────────────────────────────────

export const PortMarkers = memo(function PortMarkers({
  ports,
  onHover,
  hoveredId,
  chokepointCoords = {},
}: PortMarkersProps) {
  return (
    <>
      {ports.map((p) => {
        const color = STATUS_COLORS[p.status] ?? "#8b5cf6";
        const isHovered = p.id === hoveredId;
        const isPulsing = p.status !== "OPERATIONAL";
        const emoji = p.port_type === "seaport" ? "🚢" : "✈️";

        return (
          <Marker
            key={p.id}
            coordinates={p.coordinates}
            onMouseEnter={() => onHover(p.id)}
            onMouseLeave={() => onHover(null)}
          >
            {/* Pulse ring for non-operational status */}
            {isPulsing && (
              <circle
                r={isHovered ? 12 : 8}
                fill="none"
                stroke={color}
                strokeWidth={0.8}
                opacity={0.4}
                className="animate-map-pulse"
              />
            )}
            {/* Emoji marker */}
            <text
              textAnchor="middle"
              dominantBaseline="central"
              style={{
                fontSize: isHovered ? "11px" : "8px",
                cursor: "pointer",
                transition: "font-size 0.15s ease",
                filter: `drop-shadow(0 0 2px ${color})`,
              }}
            >
              {emoji}
            </text>
            {/* Label — only on hover, with dark halo */}
            {isHovered && (
              <text
                textAnchor="middle"
                y={-12}
                style={{
                  fontFamily: "inherit",
                  fontSize: "7px",
                  fill: "#f4f4f5",
                  fontWeight: 700,
                  pointerEvents: "none",
                  paintOrder: "stroke",
                  stroke: "#0a0a0f",
                  strokeWidth: "3px",
                  strokeLinejoin: "round",
                }}
              >
                {p.name}
              </text>
            )}
          </Marker>
        );
      })}

      {/* Lines to connected chokepoints on hover */}
      {hoveredId && (() => {
        const p = ports.find((x) => x.id === hoveredId);
        if (!p) return null;
        const color = STATUS_COLORS[p.status] ?? "#8b5cf6";
        return p.connected_chokepoints
          .filter((cpId) => chokepointCoords[cpId])
          .map((cpId) => (
            <Line
              key={`${p.id}-${cpId}`}
              from={p.coordinates}
              to={chokepointCoords[cpId]}
              stroke={color}
              strokeWidth={0.5}
              strokeLinecap="round"
              strokeDasharray="3 2"
              style={{ opacity: 0.45 }}
            />
          ));
      })()}
    </>
  );
});


// ── Tooltip (rendered outside SVG) ───────────────────────

export function PortTooltip({
  port,
  x,
  y,
}: {
  port: PortData;
  x?: number;
  y?: number;
}) {
  const color = STATUS_COLORS[port.status] ?? "#8b5cf6";
  const statusLabel = STATUS_LABELS[port.status] ?? port.status;
  const emoji = port.port_type === "seaport" ? "🚢" : "✈️";
  const typeLabel = port.port_type === "seaport" ? "Seaport" : "Cargo Airport";
  const flag = NATION_FLAGS[port.nation] ?? "";
  const nationName = NATION_NAMES[port.nation] ?? port.nation;

  const style: React.CSSProperties = x != null && y != null
    ? {
        position: "fixed",
        left: Math.min(x + 16, window.innerWidth - 300),
        top: Math.min(y - 10, window.innerHeight - 280),
        zIndex: 50,
        pointerEvents: "none",
      }
    : {};

  return (
    <div style={style} className="animate-fade-in">
      <div className="w-72 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl p-3">
        {/* Header */}
        <div className="flex items-center justify-between mb-2">
          <div className="min-w-0 flex-1">
            <div className="text-xs font-semibold text-zinc-100">
              {emoji} {port.name}
            </div>
            <div className="text-[10px] text-muted mt-0.5">
              {typeLabel} · {flag} {nationName} · {port.iata_or_locode}
            </div>
          </div>
          <span
            className="rounded-full px-2 py-0.5 text-[9px] font-bold uppercase shrink-0"
            style={{ backgroundColor: `${color}22`, color, border: `1px solid ${color}44` }}
          >
            {statusLabel}
          </span>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 gap-1 mb-2">
          <div className="rounded bg-surface-overlay px-2 py-1">
            <div className="text-[8px] text-muted">Annual Volume</div>
            <div className="text-[11px] text-zinc-200 font-medium">{port.annual_volume}</div>
          </div>
          <div className="rounded bg-surface-overlay px-2 py-1">
            <div className="text-[8px] text-muted">Unit</div>
            <div className="text-[11px] text-zinc-200 font-medium">{port.volume_unit}</div>
          </div>
        </div>

        {/* Description */}
        <p className="text-[10px] text-zinc-400 mb-2 leading-relaxed">
          {port.description}
        </p>

        {/* Key commodities */}
        <div className="flex flex-wrap gap-1 mb-1.5">
          {port.key_commodities.map((c) => (
            <span key={c} className="rounded bg-surface-overlay px-1.5 py-0.5 text-[8px] text-zinc-400">
              {c.replace(/_/g, " ")}
            </span>
          ))}
        </div>

        {/* Connected routes */}
        {port.connected_routes.length > 0 && (
          <div className="text-[8px] text-muted mt-1">
            🔗 Routes: {port.connected_routes.map((r) => r.replace(/_/g, " ")).join(", ")}
          </div>
        )}
        {/* Connected chokepoints */}
        {port.connected_chokepoints.length > 0 && (
          <div className="text-[8px] text-warning mt-0.5">
            ⚠ Chokepoints: {port.connected_chokepoints.map((c) => c.replace(/_/g, " ")).join(", ")}
          </div>
        )}
      </div>
    </div>
  );
}
