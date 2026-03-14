import { useState, useCallback, memo } from "react";
import { Marker, Line } from "react-simple-maps";
import { COUNTRY_CENTROIDS, NATION_FLAGS } from "../data/countryMapping";

// ── Types ────────────────────────────────────────────────

export interface ChokepointData {
  id: string;
  name: string;
  coordinates: [number, number];
  description: string;
  daily_volume: string;
  world_share: string;
  commodities: string[];
  controlling_nations: string[];
  affected_nations: string[];
  market_impact: string;
  status: "OPEN" | "THREATENED" | "DISRUPTED" | "CLOSED";
  category: string;
}

interface ChokepointMarkersProps {
  chokepoints: ChokepointData[];
  onHover: (id: string | null) => void;
  hoveredId: string | null;
}

// ── Status colors ────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  OPEN:      "#22c55e",
  THREATENED: "#f59e0b",
  DISRUPTED: "#f97316",
  CLOSED:    "#ef4444",
};

const STATUS_LABELS: Record<string, string> = {
  OPEN: "Open",
  THREATENED: "Threatened",
  DISRUPTED: "Disrupted",
  CLOSED: "Closed",
};

// ── Component ────────────────────────────────────────────

export const ChokepointMarkers = memo(function ChokepointMarkers({ chokepoints, onHover, hoveredId }: ChokepointMarkersProps) {
  return (
    <>
      {chokepoints.map((cp) => {
        const color = STATUS_COLORS[cp.status] ?? "#888";
        const isHovered = cp.id === hoveredId;
        const isPulsing = cp.status !== "OPEN";

        return (
          <Marker
            key={cp.id}
            coordinates={cp.coordinates}
            onMouseEnter={() => onHover(cp.id)}
            onMouseLeave={() => onHover(null)}
          >
            {/* Pulse ring for non-OPEN status */}
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
            {/* Anchor emoji marker */}
            <text
              textAnchor="middle"
              dominantBaseline="central"
              style={{
                fontSize: isHovered ? "12px" : "9px",
                cursor: "pointer",
                transition: "font-size 0.15s ease",
                filter: `drop-shadow(0 0 2px ${color})`,
              }}
            >
              ⚓
            </text>
            {/* Label — only on hover, with dark halo for readability */}
            {isHovered && (
              <text
                textAnchor="middle"
                y={-14}
                style={{
                  fontFamily: "inherit",
                  fontSize: "8px",
                  fill: "#f4f4f5",
                  fontWeight: 700,
                  pointerEvents: "none",
                  paintOrder: "stroke",
                  stroke: "#0a0a0f",
                  strokeWidth: "3px",
                  strokeLinejoin: "round",
                }}
              >
                {cp.name}
              </text>
            )}
          </Marker>
        );
      })}

      {/* Affected-nation lines for hovered chokepoint */}
      {hoveredId && (() => {
        const cp = chokepoints.find((c) => c.id === hoveredId);
        if (!cp) return null;
        const color = STATUS_COLORS[cp.status] ?? "#888";
        return cp.affected_nations
          .filter((n) => COUNTRY_CENTROIDS[n])
          .map((n) => (
            <Line
              key={`${cp.id}-${n}`}
              from={cp.coordinates}
              to={COUNTRY_CENTROIDS[n]}
              stroke={color}
              strokeWidth={0.6}
              strokeLinecap="round"
              strokeDasharray="3 2"
              style={{ opacity: 0.5 }}
            />
          ));
      })()}
    </>
  );
});


// ── Tooltip (rendered outside SVG) ───────────────────────

export function ChokepointTooltip({
  chokepoint,
  x,
  y,
}: {
  chokepoint: ChokepointData;
  x?: number;
  y?: number;
}) {
  const color = STATUS_COLORS[chokepoint.status] ?? "#888";
  const statusLabel = STATUS_LABELS[chokepoint.status] ?? chokepoint.status;

  // When x/y provided, position as fixed; otherwise parent handles positioning
  const style: React.CSSProperties = x != null && y != null
    ? {
        position: "fixed",
        left: Math.min(x + 16, window.innerWidth - 320),
        top: Math.min(y - 10, window.innerHeight - 300),
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
              ⚓ {chokepoint.name}
            </div>
            <div className="text-[10px] text-muted mt-0.5">
              {chokepoint.category === "supply_chain" ? "Supply Chokepoint" : "Maritime Chokepoint"}
            </div>
          </div>
          <span
            className="rounded-full px-2 py-0.5 text-[9px] font-bold uppercase"
            style={{ backgroundColor: `${color}22`, color, border: `1px solid ${color}44` }}
          >
            {statusLabel}
          </span>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 gap-1 mb-2">
          <div className="rounded bg-surface-overlay px-2 py-1">
            <div className="text-[8px] text-muted">Daily Volume</div>
            <div className="text-[11px] text-zinc-200 font-medium">{chokepoint.daily_volume}</div>
          </div>
          <div className="rounded bg-surface-overlay px-2 py-1">
            <div className="text-[8px] text-muted">World Share</div>
            <div className="text-[11px] text-zinc-200 font-medium">{chokepoint.world_share}</div>
          </div>
        </div>

        {/* Description */}
        <p className="text-[10px] text-zinc-400 mb-2 leading-relaxed">
          {chokepoint.description}
        </p>

        {/* Market impact */}
        <div className="rounded bg-surface-overlay px-2 py-1.5 mb-2">
          <div className="text-[8px] text-negative font-medium mb-0.5">⚠ Market Impact</div>
          <div className="text-[10px] text-zinc-300 leading-relaxed">
            {chokepoint.market_impact}
          </div>
        </div>

        {/* Commodities */}
        <div className="flex flex-wrap gap-1 mb-1.5">
          {chokepoint.commodities.map((c) => (
            <span key={c} className="rounded bg-surface-overlay px-1.5 py-0.5 text-[8px] text-zinc-400">
              {c.replace(/_/g, " ")}
            </span>
          ))}
        </div>

        {/* Affected nations */}
        <div className="text-[8px] text-muted">
          Affects: {chokepoint.affected_nations.map((n) => `${NATION_FLAGS[n] ?? ""} ${n}`).join(", ")}
        </div>
      </div>
    </div>
  );
}
