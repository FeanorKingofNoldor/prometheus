import { useMemo, useCallback, useRef, useEffect, memo } from "react";
import { Marker, Line } from "react-simple-maps";
import type { ChokepointData } from "./ChokepointMarkers";
import type { PortData } from "./PortMarkers";
import { COUNTRY_CENTROIDS } from "../data/countryMapping";

// ── Unified marker item ──────────────────────────────────

export interface TradeMarkerItem {
  id: string;
  type: "chokepoint" | "seaport" | "cargo_airport";
  name: string;
  coordinates: [number, number];
  status: string;
  emoji: string;
  color: string;
}

export interface ClusterData {
  key: string;
  center: [number, number];
  items: TradeMarkerItem[];
}

interface ClusteredTradeMarkersProps {
  chokepoints: ChokepointData[];
  ports: PortData[];
  zoom: number;
  hoveredId: string | null;
  onHover: (id: string | null) => void;
  onClick?: (item: TradeMarkerItem) => void;
  selectedId?: string | null;
  expandedClusterKey?: string | null;
  onExpandCluster?: (cluster: ClusterData | null) => void;
}

// ── Status colors ────────────────────────────────────────

const CP_STATUS_COLORS: Record<string, string> = {
  OPEN: "#22c55e", THREATENED: "#f59e0b", DISRUPTED: "#f97316", CLOSED: "#ef4444",
};
const PORT_STATUS_COLORS: Record<string, string> = {
  OPERATIONAL: "#8b5cf6", CONGESTED: "#f59e0b", DISRUPTED: "#ef4444",
};

// ── Build flat marker list ───────────────────────────────

function buildMarkers(chokepoints: ChokepointData[], ports: PortData[]): TradeMarkerItem[] {
  const items: TradeMarkerItem[] = [];
  for (const cp of chokepoints) {
    items.push({
      id: cp.id,
      type: "chokepoint",
      name: cp.name,
      coordinates: cp.coordinates,
      status: cp.status,
      emoji: "⚓",
      color: CP_STATUS_COLORS[cp.status] ?? "#888",
    });
  }
  for (const p of ports) {
    items.push({
      id: p.id,
      type: p.port_type,
      name: p.name,
      coordinates: p.coordinates,
      status: p.status,
      emoji: p.port_type === "seaport" ? "🚢" : "✈️",
      color: PORT_STATUS_COLORS[p.status] ?? "#8b5cf6",
    });
  }
  return items;
}

// ── Clustering ───────────────────────────────────────────

interface Cluster {
  key: string;
  center: [number, number];
  items: TradeMarkerItem[];
}

function clusterMarkers(markers: TradeMarkerItem[], zoom: number): Cluster[] {
  // Cell size shrinks as zoom increases → fewer clusters when zoomed in
  const cellSize = 18 / Math.max(zoom, 1);
  const buckets = new Map<string, TradeMarkerItem[]>();

  for (const m of markers) {
    const cx = Math.floor(m.coordinates[0] / cellSize);
    const cy = Math.floor(m.coordinates[1] / cellSize);
    const key = `${cx},${cy}`;
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(m);
  }

  const clusters: Cluster[] = [];
  for (const [key, items] of buckets) {
    const lon = items.reduce((s, m) => s + m.coordinates[0], 0) / items.length;
    const lat = items.reduce((s, m) => s + m.coordinates[1], 0) / items.length;
    clusters.push({ key, center: [lon, lat], items });
  }
  return clusters;
}

// ── Component ────────────────────────────────────────────

export const ClusteredTradeMarkers = memo(function ClusteredTradeMarkers({
  chokepoints,
  ports,
  zoom,
  hoveredId,
  onHover,
  onClick,
  selectedId,
  expandedClusterKey,
  onExpandCluster,
}: ClusteredTradeMarkersProps) {
  const markers = useMemo(() => buildMarkers(chokepoints, ports), [chokepoints, ports]);
  const clusters = useMemo(() => clusterMarkers(markers, zoom), [markers, zoom]);

  // Close expanded cluster when zoom changes
  useEffect(() => { onExpandCluster?.(null); }, [zoom]);

  const handleClusterClick = useCallback((cluster: Cluster) => {
    if (cluster.items.length === 1) {
      onClick?.(cluster.items[0]);
    } else {
      const isAlreadyOpen = expandedClusterKey === cluster.key;
      onExpandCluster?.(isAlreadyOpen ? null : { key: cluster.key, center: cluster.center, items: cluster.items });
    }
  }, [onClick, expandedClusterKey, onExpandCluster]);

  return (
    <>
      {clusters.map((cluster) => {
        const isSingle = cluster.items.length === 1;
        const item = cluster.items[0];

        if (isSingle) {
          // ── Single marker ──
          const isHovered = item.id === hoveredId;
          const isSelected = item.id === selectedId;
          const isActive = isHovered || isSelected;
          const isPulsing = item.type === "chokepoint"
            ? item.status !== "OPEN"
            : item.status !== "OPERATIONAL";

          return (
            <Marker
              key={item.id}
              coordinates={item.coordinates}
              onMouseEnter={() => onHover(item.id)}
              onMouseLeave={() => onHover(null)}
              onClick={() => onClick?.(item)}
            >
              {isPulsing && (
                <circle
                  r={isActive ? 12 : 8}
                  fill="none"
                  stroke={item.color}
                  strokeWidth={0.8}
                  opacity={0.4}
                  className="animate-map-pulse"
                />
              )}
              {isSelected && (
                <circle
                  r={14}
                  fill="none"
                  stroke="#06b6d4"
                  strokeWidth={1.2}
                  opacity={0.7}
                  className="animate-map-pulse"
                />
              )}
              <text
                textAnchor="middle"
                dominantBaseline="central"
                style={{
                  fontSize: isActive ? "12px" : "9px",
                  cursor: "pointer",
                  transition: "font-size 0.15s ease",
                  filter: `drop-shadow(0 0 2px ${item.color})`,
                }}
              >
                {item.emoji}
              </text>
              {isActive && (
                <text
                  textAnchor="middle"
                  y={-14}
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
                  {item.name}
                </text>
              )}
            </Marker>
          );
        }

        // ── Cluster marker ──
        const isExpanded = expandedClusterKey === cluster.key;
        const hasSelected = cluster.items.some((i) => i.id === selectedId);
        // Pick dominant emoji for cluster badge
        const typeCount: Record<string, number> = {};
        for (const i of cluster.items) typeCount[i.type] = (typeCount[i.type] ?? 0) + 1;
        const dominantType = Object.entries(typeCount).sort((a, b) => b[1] - a[1])[0][0];
        const dominantEmoji = cluster.items.find((i) => i.type === dominantType)?.emoji ?? "📍";

        return (
          <Marker
            key={`cluster-${cluster.key}`}
            coordinates={cluster.center}
            onClick={() => handleClusterClick(cluster)}
          >
            {hasSelected && (
              <circle
                r={18}
                fill="none"
                stroke="#06b6d4"
                strokeWidth={1}
                opacity={0.6}
                className="animate-map-pulse"
              />
            )}
            {/* Cluster background */}
            <circle
              r={isExpanded ? 14 : 11}
              fill={isExpanded ? "#1e1e2e" : "#18181b"}
              stroke={isExpanded ? "#06b6d4" : "#3f3f46"}
              strokeWidth={isExpanded ? 1.2 : 0.8}
              style={{ cursor: "pointer" }}
            />
            {/* Dominant emoji */}
            <text
              textAnchor="middle"
              dominantBaseline="central"
              y={-1}
              style={{ fontSize: "8px", pointerEvents: "none" }}
            >
              {dominantEmoji}
            </text>
            {/* Count badge */}
            <text
              textAnchor="middle"
              y={7}
              style={{
                fontSize: "5px",
                fill: "#a1a1aa",
                fontWeight: 700,
                pointerEvents: "none",
              }}
            >
              +{cluster.items.length}
            </text>
          </Marker>
        );
      })}

      {/* Chokepoint connection lines for hovered single marker */}
      {hoveredId && (() => {
        const cp = chokepoints.find((c) => c.id === hoveredId);
        if (!cp) return null;
        const color = CP_STATUS_COLORS[cp.status] ?? "#888";
        return cp.affected_nations
          .filter((n: string) => COUNTRY_CENTROIDS[n])
          .map((n: string) => (
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


// ── Cluster popup (rendered outside SVG, in parent div) ──

export function ClusterPopup({
  cluster,
  onSelect,
  onClose,
}: {
  cluster: { key: string; items: TradeMarkerItem[] } | null;
  onSelect: (item: TradeMarkerItem) => void;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!cluster) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [cluster, onClose]);

  if (!cluster) return null;

  return (
    <div
      ref={ref}
      className="absolute z-50 w-56 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl overflow-hidden animate-fade-in"
      style={{ left: "50%", top: "50%", transform: "translate(-50%, -50%)" }}
    >
      <div className="px-2.5 py-1.5 border-b border-border-dim text-[10px] text-muted font-medium">
        {cluster.items.length} entities at this location
      </div>
      <div className="max-h-48 overflow-y-auto py-1">
        {cluster.items.map((item) => (
          <button
            key={item.id}
            onClick={() => onSelect(item)}
            className="w-full text-left px-3 py-1.5 text-[11px] text-zinc-300 hover:bg-surface-overlay hover:text-zinc-100 transition-colors flex items-center gap-2"
          >
            <span>{item.emoji}</span>
            <span className="truncate flex-1">{item.name}</span>
            <span
              className="h-1.5 w-1.5 rounded-full shrink-0"
              style={{ backgroundColor: item.color }}
            />
          </button>
        ))}
      </div>
    </div>
  );
}
