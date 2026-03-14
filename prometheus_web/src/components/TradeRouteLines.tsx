import { useState, memo } from "react";
import { Line, Marker } from "react-simple-maps";

// ── Types ────────────────────────────────────────────────

export interface TradeRouteData {
  id: string;
  name: string;
  category: string;
  waypoints: [number, number][];
  commodities: string[];
  volume: string;
  source_nations: string[];
  dest_nations: string[];
  chokepoints: string[];
  color: string;
  description: string;
}

interface TradeRouteLinesProps {
  routes: TradeRouteData[];
  hoveredRouteId: string | null;
  onHoverRoute: (id: string | null) => void;
}

// ── Category colors (fallback) ───────────────────────────

const CATEGORY_COLORS: Record<string, string> = {
  oil:       "#f59e0b",
  gas:       "#06b6d4",
  shipping:  "#22c55e",
  commodity: "#d97706",
};

// ── Component ────────────────────────────────────────────

export const TradeRouteLines = memo(function TradeRouteLines({ routes, hoveredRouteId, onHoverRoute }: TradeRouteLinesProps) {
  return (
    <>
      {routes.map((route) => {
        const isHovered = route.id === hoveredRouteId;
        const color = route.color || CATEGORY_COLORS[route.category] || "#888";

        // Render connected line segments between waypoints with directional flow
        const isLast = (i: number) => i === route.waypoints.length - 2;
        return route.waypoints.slice(0, -1).map((wp, i) => (
          <Line
            key={`${route.id}-${i}`}
            from={wp}
            to={route.waypoints[i + 1]}
            stroke={color}
            strokeWidth={isHovered ? 1.4 : 0.6}
            strokeLinecap="round"
            strokeDasharray={route.category === "gas" ? "2 2" : "4 2"}
            className={isHovered ? "animate-flow-forward" : "animate-flow-slow"}
            style={{
              opacity: isHovered ? 0.85 : 0.3,
              cursor: "pointer",
              pointerEvents: "visibleStroke",
              markerEnd: isHovered && isLast(i) ? "url(#arrow-route)" : undefined,
            }}
          />
        ));
      })}

      {/* Invisible wider hit-target for each route (midpoint marker) */}
      {routes.map((route) => {
        const mid = route.waypoints[Math.floor(route.waypoints.length / 2)];
        const isHovered = route.id === hoveredRouteId;
        const color = route.color || CATEGORY_COLORS[route.category] || "#888";

        return (
          <Marker
            key={`label-${route.id}`}
            coordinates={mid}
            onMouseEnter={() => onHoverRoute(route.id)}
            onMouseLeave={() => onHoverRoute(null)}
          >
            {/* Invisible hit area */}
            <circle r={8} fill="transparent" style={{ cursor: "pointer" }} />
            {/* Small route indicator dot */}
            <circle r={isHovered ? 3 : 1.5} fill={color} opacity={isHovered ? 0.9 : 0.5} />
            {/* Label on hover */}
            {isHovered && (
              <text
                textAnchor="middle"
                y={-8}
                style={{
                  fontFamily: "inherit",
                  fontSize: "5.5px",
                  fill: "#f4f4f5",
                  fontWeight: 600,
                  pointerEvents: "none",
                }}
              >
                {route.name}
              </text>
            )}
          </Marker>
        );
      })}
    </>
  );
});


// ── Tooltip (rendered outside SVG) ───────────────────────

export function TradeRouteTooltip({
  route,
  x,
  y,
}: {
  route: TradeRouteData;
  x?: number;
  y?: number;
}) {
  const color = route.color || CATEGORY_COLORS[route.category] || "#888";

  // When x/y provided, position as fixed; otherwise parent handles positioning
  const style: React.CSSProperties = x != null && y != null
    ? {
        position: "fixed",
        left: Math.min(x + 16, window.innerWidth - 300),
        top: Math.min(y - 10, window.innerHeight - 200),
        zIndex: 50,
        pointerEvents: "none",
      }
    : {};

  const categoryLabel: Record<string, string> = {
    oil: "🛢 Oil Route",
    gas: "💨 Gas Pipeline",
    shipping: "🚢 Shipping Lane",
    commodity: "📦 Commodity Corridor",
  };

  return (
    <div style={style} className="animate-fade-in">
      <div className="w-64 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl p-3">
        <div className="flex items-center gap-2 mb-1.5">
          <div
            className="h-2 w-2 rounded-full"
            style={{ backgroundColor: color }}
          />
          <div className="text-xs font-semibold text-zinc-100">{route.name}</div>
        </div>
        <div className="text-[10px] text-muted mb-2">
          {categoryLabel[route.category] ?? route.category} · {route.volume}
        </div>
        <p className="text-[10px] text-zinc-400 mb-2 leading-relaxed">
          {route.description}
        </p>
        <div className="flex flex-wrap gap-1 mb-1.5">
          {route.commodities.map((c) => (
            <span key={c} className="rounded bg-surface-overlay px-1.5 py-0.5 text-[8px] text-zinc-400">
              {c.replace(/_/g, " ")}
            </span>
          ))}
        </div>
        {route.chokepoints.length > 0 && (
          <div className="text-[8px] text-warning">
            ⚠ Passes through: {route.chokepoints.join(", ")}
          </div>
        )}
        <div className="text-[8px] text-muted mt-1">
          {route.source_nations.join(",")} → {route.dest_nations.join(",")}
        </div>
      </div>
    </div>
  );
}
