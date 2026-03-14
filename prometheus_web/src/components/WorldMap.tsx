import { useState, useCallback, useMemo, memo } from "react";
import {
  ComposableMap,
  ZoomableGroup,
  Geographies,
  Geography,
  Line,
  Marker,
  Graticule,
  Sphere,
} from "react-simple-maps";
import { ZoomIn, ZoomOut, Maximize2 } from "lucide-react";
import {
  featureIdToISO3,
  resolveISO3,
  COUNTRY_CENTROIDS,
  NATION_FLAGS,
  NATION_NAMES,
} from "../data/countryMapping";
import type { ChokepointData } from "./ChokepointMarkers";
import { TradeRouteLines, type TradeRouteData } from "./TradeRouteLines";
import { ResourceOverlay, type ResourceData } from "./ResourceOverlay";
import { ConflictMarkers, type ConflictData } from "./ConflictMarkers";
import type { PortData } from "./PortMarkers";
import { ClusteredTradeMarkers, ClusterPopup, type TradeMarkerItem, type ClusterData } from "./ClusteredTradeMarkers";
import { VesselOverlay, type VesselData } from "./VesselOverlay";
import { FlightOverlay, type FlightData } from "./FlightOverlay";
import { NavalDeploymentOverlay, type NavalDeploymentData } from "./NavalDeploymentOverlay";
import type { MapLayer } from "./MapLayerToggle";

// ── Types ────────────────────────────────────────────────────────

export interface MapNation {
  nation: string;
  composite_risk: number;
  economic_stability: number;
  market_stability: number;
  political_stability: number;
  contagion_risk: number;
  currency_risk: number;
  opportunity_score: number;
  leadership_risk: number;
  leader?: { name: string; role: string; thumbnail_url?: string | null; profile_id?: string | null } | null;
  dependencies: { nation: string; weight: number; flow?: string; channel?: string }[];
}

interface WorldMapProps {
  nations: MapNation[];
  selectedNation: string | null;
  onSelectNation: (nation: string | null) => void;
  // Overlay data
  activeOverlays: Set<MapLayer>;
  chokepoints?: ChokepointData[];
  tradeRoutes?: TradeRouteData[];
  resources?: ResourceData[];
  selectedResource?: string | null;
  onClickResource?: (resource: string, nation: string) => void;
  conflicts?: ConflictData[];
  ports?: PortData[];
  vessels?: VesselData[];
  flights?: FlightData[];
  deployments?: NavalDeploymentData[];
  // Overlay hover/click state (managed by parent)
  hoveredTradeMarker: string | null;
  onHoverTradeMarker: (id: string | null) => void;
  onClickTradeMarker?: (item: TradeMarkerItem) => void;
  selectedTradeMarkerId?: string | null;
  hoveredRoute: string | null;
  onHoverRoute: (id: string | null) => void;
  hoveredConflict: string | null;
  onHoverConflict: (id: string | null) => void;
  onClickConflict?: (id: string) => void;
  pinnedConflict?: string | null;
  // External zoom control
  externalCenter?: [number, number] | null;
  externalZoom?: number | null;
}

// ── Constants ────────────────────────────────────────────────────

const GEO_URL = "https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json";

const COLORS = {
  ocean: "#0a0a0f",
  land: "#1a1a22",
  landStroke: "#2a2a35",
  graticule: "#1a1a22",
  tracked: {
    high: "#22c55e",   // composite >= 0.65
    mid: "#facc15",    // composite 0.45–0.65
    low: "#ef4444",    // composite < 0.45
  },
  hover: "#3b82f6",
  contagionLine: "#ef4444",
  exportLine: "#22c55e",   // green = exports flowing out
  importLine: "#3b82f6",   // blue = imports flowing in
  balancedLine: "#a1a1aa", // grey = balanced / financial
  marker: "#facc15",
};

// ── Helpers ──────────────────────────────────────────────────────

function riskColor(composite: number): string {
  if (composite >= 0.65) return COLORS.tracked.high;
  if (composite >= 0.45) return COLORS.tracked.mid;
  return COLORS.tracked.low;
}

function riskColorWithAlpha(composite: number, alpha: number): string {
  if (composite >= 0.65) return `rgba(34, 197, 94, ${alpha})`;
  if (composite >= 0.45) return `rgba(250, 204, 21, ${alpha})`;
  return `rgba(239, 68, 68, ${alpha})`;
}

function pct(v: number): string {
  return `${(v * 100).toFixed(0)}%`;
}

// ── Component ────────────────────────────────────────────────────

export const WorldMap = memo(function WorldMap({
  nations,
  selectedNation,
  onSelectNation,
  activeOverlays,
  chokepoints = [],
  tradeRoutes = [],
  resources = [],
  selectedResource,
  onClickResource,
  conflicts = [],
  ports = [],
  vessels = [],
  flights = [],
  deployments = [],
  hoveredTradeMarker,
  onHoverTradeMarker,
  onClickTradeMarker,
  selectedTradeMarkerId,
  hoveredRoute,
  onHoverRoute,
  hoveredConflict,
  onHoverConflict,
  onClickConflict,
  pinnedConflict,
  externalCenter,
  externalZoom,
}: WorldMapProps) {
  const [hoveredNation, setHoveredNation] = useState<string | null>(null);
  const [zoom, setZoom] = useState(1);
  const [center, setCenter] = useState<[number, number]>([10, 5]);
  const [expandedCluster, setExpandedCluster] = useState<ClusterData | null>(null);

  // Honour external zoom/center from parent (e.g. country dropdown)
  const prevExtCenter = useState<string | null>(null);
  useMemo(() => {
    if (externalCenter) {
      setCenter(externalCenter);
      if (externalZoom) setZoom(externalZoom);
    }
  }, [externalCenter, externalZoom]);

  // The "active" nation for contagion lines + highlights = selected (persisted click)
  const activeNation = selectedNation;

  // Index nations by ISO3 for fast lookup.
  const nationMap = useMemo(() => {
    const m = new Map<string, MapNation>();
    for (const n of nations) m.set(n.nation, n);
    return m;
  }, [nations]);

  // Set of all tracked ISO3 codes (including unscored placeholders)
  const trackedSet = useMemo(
    () => new Set(nations.map((n) => n.nation)),
    [nations],
  );

  const activeData = activeNation ? nationMap.get(activeNation) : null;

  // Contagion lines: directed from exporter → importer (bound to selected nation).
  const contagionLines = useMemo(() => {
    if (!activeNation || !activeData) return [];
    const coords = COUNTRY_CENTROIDS[activeNation];
    if (!coords) return [];

    return activeData.dependencies
      .filter((d) => COUNTRY_CENTROIDS[d.nation])
      .map((d) => {
        const partnerCoords = COUNTRY_CENTROIDS[d.nation];
        const flow = d.flow ?? "balanced";
        const isExport = flow === "export";
        const isImport = flow === "import";
        return {
          from: isImport ? partnerCoords : coords,
          to: isImport ? coords : partnerCoords,
          weight: d.weight,
          target: d.nation,
          flow,
          color: isExport ? COLORS.exportLine : isImport ? COLORS.importLine : COLORS.balancedLine,
          animClass: isExport || isImport ? "animate-flow-forward" : "animate-contagion-dash",
        };
      });
  }, [activeNation, activeData]);

  const handleGeoClick = useCallback(
    (iso3: string | undefined) => {
      if (!iso3 || !trackedSet.has(iso3)) {
        onSelectNation(null);
        return;
      }
      // Toggle: click same nation = deselect
      onSelectNation(iso3 === selectedNation ? null : iso3);
    },
    [trackedSet, selectedNation, onSelectNation],
  );

  const handleBackgroundClick = useCallback(() => {
    onSelectNation(null);
  }, [onSelectNation]);

  const handleZoomEnd = useCallback(
    (pos: { coordinates: [number, number]; zoom: number }) => {
      setCenter(pos.coordinates);
      setZoom(pos.zoom);
    },
    [],
  );

  return (
    <div className="relative w-full">
      {/* Zoom controls */}
      <div className="absolute top-2 right-2 z-10 flex flex-col gap-1">
        <button
          onClick={() => setZoom((z) => Math.min(z * 1.5, 8))}
          className="flex h-7 w-7 items-center justify-center rounded border border-border-dim bg-surface-overlay/90 text-zinc-300 hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
          title="Zoom in"
        >
          <ZoomIn size={14} />
        </button>
        <button
          onClick={() => setZoom((z) => Math.max(z / 1.5, 1))}
          className="flex h-7 w-7 items-center justify-center rounded border border-border-dim bg-surface-overlay/90 text-zinc-300 hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
          title="Zoom out"
        >
          <ZoomOut size={14} />
        </button>
        <button
          onClick={() => { setZoom(1); setCenter([10, 5]); }}
          className="flex h-7 w-7 items-center justify-center rounded border border-border-dim bg-surface-overlay/90 text-zinc-300 hover:bg-surface-overlay hover:text-zinc-100 transition-colors"
          title="Reset view"
        >
          <Maximize2 size={14} />
        </button>
      </div>

      <ComposableMap
        projection="geoNaturalEarth1"
        projectionConfig={{ scale: 155 }}
        width={960}
        height={480}
        style={{ width: "100%", height: "auto", background: COLORS.ocean }}
      >
        {/* SVG arrow marker definitions for directional flow lines */}
        <defs>
          <marker id="arrow-export" markerWidth="6" markerHeight="4" refX="5" refY="2" orient="auto">
            <path d="M0,0 L6,2 L0,4 Z" fill={COLORS.exportLine} opacity="0.8" />
          </marker>
          <marker id="arrow-import" markerWidth="6" markerHeight="4" refX="5" refY="2" orient="auto">
            <path d="M0,0 L6,2 L0,4 Z" fill={COLORS.importLine} opacity="0.8" />
          </marker>
          <marker id="arrow-route" markerWidth="6" markerHeight="4" refX="5" refY="2" orient="auto">
            <path d="M0,0 L6,2 L0,4 Z" fill="#06b6d4" opacity="0.7" />
          </marker>
          <marker id="arrow-resource" markerWidth="6" markerHeight="4" refX="5" refY="2" orient="auto">
            <path d="M0,0 L6,2 L0,4 Z" fill="#22c55e" opacity="0.7" />
          </marker>
        </defs>
        <ZoomableGroup
          center={center}
          zoom={zoom}
          minZoom={1}
          maxZoom={8}
          onMoveEnd={handleZoomEnd}
        >
          <Sphere
            id="globe-sphere"
            fill={COLORS.ocean}
            stroke="#1a1a2e"
            strokeWidth={0.5}
          />
          <Graticule stroke={COLORS.graticule} strokeWidth={0.3} />

          <Geographies geography={GEO_URL}>
            {({ geographies }) =>
              geographies.map((geo) => {
                const rawIso3 = featureIdToISO3(geo.id);
                const iso3 = rawIso3 ? resolveISO3(rawIso3) : undefined;
                const nationData = iso3 ? nationMap.get(iso3) : undefined;
                const isTracked = iso3 ? trackedSet.has(iso3) : false;
                const isScored = !!nationData && nationData.composite_risk >= 0;
                const isSelected = iso3 === activeNation;
                const isHov = iso3 === hoveredNation && iso3 !== activeNation;
                const isDependency = activeData?.dependencies.some(
                  (d) => d.nation === iso3
                );

                let fill = COLORS.land;
                if (isScored && nationData) {
                  fill = isSelected
                    ? COLORS.hover
                    : isHov
                      ? riskColorWithAlpha(nationData.composite_risk, 0.95)
                      : riskColorWithAlpha(nationData.composite_risk, isDependency ? 0.9 : 0.7);
                } else if (isTracked) {
                  fill = isSelected ? COLORS.hover : isHov ? "#2a2a45" : "#1e1e2a";
                } else if (isDependency) {
                  fill = "#2a2a45";
                } else if (isHov) {
                  fill = "#222230"; // subtle highlight for non-tracked hover
                }

                return (
                  <Geography
                    key={geo.rsmKey}
                    geography={geo}
                    fill={fill}
                    stroke={isSelected ? COLORS.hover : isScored ? riskColor(nationData!.composite_risk) : isTracked ? "#3a3a4a" : COLORS.landStroke}
                    strokeWidth={isSelected ? 1.5 : isHov ? 1.0 : isTracked ? 0.5 : 0.25}
                    onMouseEnter={() => {
                      if (iso3) setHoveredNation(iso3);
                    }}
                    onMouseLeave={() => setHoveredNation(null)}
                    onClick={() => {
                      if (iso3 && trackedSet.has(iso3)) handleGeoClick(iso3);
                    }}
                    style={{
                      default: { outline: "none", transition: "fill 0.2s, stroke-width 0.2s" },
                      hover: { outline: "none", cursor: isTracked ? "pointer" : "default" },
                      pressed: { outline: "none" },
                    }}
                  />
                );
              })
            }
          </Geographies>

          {/* ── Trade Routes overlay (render below markers) ── */}
          {activeOverlays.has("routes") && tradeRoutes.length > 0 && (
            <TradeRouteLines
              routes={tradeRoutes}
              hoveredRouteId={hoveredRoute}
              onHoverRoute={onHoverRoute}
            />
          )}

          {/* Directional contagion arcs — only when nations overlay is active */}
          {activeOverlays.has("nations") && contagionLines.map((line) => (
            <Line
              key={`contagion-${activeNation}-${line.target}`}
              from={line.from}
              to={line.to}
              stroke={line.color}
              strokeWidth={Math.max(0.6, line.weight * 2.2)}
              strokeLinecap="round"
              strokeDasharray="4 3"
              className={line.animClass}
              style={{
                opacity: 0.4 + line.weight * 0.5,
                markerEnd: line.flow === "export" ? "url(#arrow-export)"
                         : line.flow === "import" ? "url(#arrow-import)"
                         : undefined,
              }}
            />
          ))}

          {/* Nation markers — all tracked (scored + unscored) */}
          {nations.map((n) => {
            const coords = COUNTRY_CENTROIDS[n.nation];
            if (!coords) return null;
            const isSel = n.nation === activeNation;
            const isHov = n.nation === hoveredNation && !isSel;
            const isScored = n.composite_risk >= 0;
            const color = isScored ? riskColor(n.composite_risk) : "#555";
            const highlight = isSel || isHov;

            return (
              <Marker
                key={n.nation}
                coordinates={coords}
                onClick={() => handleGeoClick(n.nation)}
                style={{
                  default: { cursor: "pointer" },
                  hover: { cursor: "pointer" },
                  pressed: { cursor: "pointer" },
                }}
              >
                {/* Selected ring */}
                {isSel && (
                  <circle
                    r={8}
                    fill="none"
                    stroke={COLORS.hover}
                    strokeWidth={1.2}
                    opacity={0.7}
                    className="animate-map-pulse"
                  />
                )}
                {/* Outer pulse ring */}
                <circle
                  r={highlight ? 6 : isScored ? 4 : 2.5}
                  fill="none"
                  stroke={color}
                  strokeWidth={isScored ? 0.8 : 0.5}
                  opacity={isScored ? 0.4 : 0.25}
                  className={isScored ? "animate-map-pulse" : undefined}
                />
                {/* Inner dot */}
                <circle
                  r={highlight ? 3 : isScored ? 2 : 1.5}
                  fill={color}
                  opacity={isScored ? 0.9 : 0.5}
                />
                {/* Label — only visible when zoomed in, scales inversely with zoom */}
                {(zoom >= 1.6 || highlight) && (
                  <text
                    textAnchor="middle"
                    y={-8 / zoom}
                    style={{
                      fontFamily: "inherit",
                      fontSize: `${(highlight ? 8 : isScored ? 6 : 5) / zoom}px`,
                      fill: isSel ? "#60a5fa" : highlight ? "#ffffff" : isScored ? "#e4e4e7" : "#a1a1aa",
                      fontWeight: highlight ? 700 : 500,
                      pointerEvents: "none",
                      paintOrder: "stroke",
                      stroke: "#0a0a0f",
                      strokeWidth: `${2.5 / zoom}px`,
                      strokeLinejoin: "round",
                    }}
                  >
                    {n.nation}
                  </text>
                )}
              </Marker>
            );
          })}

          {/* ── Resources overlay (badges on nation markers) ── */}
          {activeOverlays.has("resources") && resources.length > 0 && (
            <ResourceOverlay
              resources={resources}
              hoveredNation={activeNation}
              selectedResource={selectedResource ?? null}
              onClickResource={onClickResource}
            />
          )}

          {/* ── Conflicts overlay ── */}
          {activeOverlays.has("conflicts") && conflicts.length > 0 && (
            <ConflictMarkers
              conflicts={conflicts}
              hoveredId={hoveredConflict}
              onHover={onHoverConflict}
              onClick={onClickConflict}
              pinnedId={pinnedConflict}
            />
          )}

          {/* ── Naval deployments overlay ── */}
          {activeOverlays.has("deployments") && deployments.length > 0 && (
            <NavalDeploymentOverlay deployments={deployments} zoom={zoom} />
          )}

          {/* ── Vessels overlay ── */}
          {activeOverlays.has("vessels") && vessels.length > 0 && (
            <VesselOverlay vessels={vessels} zoom={zoom} />
          )}

          {/* ── Flights overlay ── */}
          {activeOverlays.has("flights") && flights.length > 0 && (
            <FlightOverlay flights={flights} zoom={zoom} />
          )}

          {/* ── Clustered trade markers (chokepoints + ports) ── */}
          {activeOverlays.has("routes") && (chokepoints.length > 0 || ports.length > 0) && (
            <ClusteredTradeMarkers
              chokepoints={chokepoints}
              ports={ports}
              zoom={zoom}
              hoveredId={hoveredTradeMarker}
              onHover={onHoverTradeMarker}
              onClick={(item) => { setExpandedCluster(null); onClickTradeMarker?.(item); }}
              selectedId={selectedTradeMarkerId}
              expandedClusterKey={expandedCluster?.key ?? null}
              onExpandCluster={setExpandedCluster}
            />
          )}
        </ZoomableGroup>
      </ComposableMap>

      {/* Cluster expand popup */}
      <ClusterPopup
        cluster={expandedCluster}
        onSelect={(item) => { setExpandedCluster(null); onClickTradeMarker?.(item); }}
        onClose={() => setExpandedCluster(null)}
      />

      {/* Minimal hover label for non-tracked nations */}
      {hoveredNation && !trackedSet.has(hoveredNation) && NATION_NAMES[hoveredNation] && (
        <div className="absolute top-2 left-2 z-10 pointer-events-none animate-fade-in">
          <div className="rounded border border-border-dim bg-surface-raised/90 backdrop-blur-sm px-2 py-1 text-xs text-zinc-300">
            {NATION_FLAGS[hoveredNation] ?? ""} {NATION_NAMES[hoveredNation]}
            <span className="ml-1.5 text-[9px] text-muted">not tracked</span>
          </div>
        </div>
      )}

      {/* Pinned info panel rendered by parent (GeoRisk) */}
    </div>
  );
});


// ── Tooltip sub-component

function MapTooltip({ nation, x, y }: { nation: MapNation; x: number; y: number }) {
  const flag = NATION_FLAGS[nation.nation] ?? "";
  const name = NATION_NAMES[nation.nation] ?? nation.nation;
  const color = riskColor(nation.composite_risk);

  // Position tooltip — offset right/below cursor, clamp to viewport.
  const style: React.CSSProperties = {
    position: "fixed",
    left: Math.min(x + 16, window.innerWidth - 280),
    top: Math.min(y - 10, window.innerHeight - 300),
    zIndex: 50,
    pointerEvents: "none",
  };

  return (
    <div style={style} className="animate-fade-in">
      <div className="w-64 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl p-3">
        {/* Header */}
        <div className="flex items-center gap-2 mb-2">
          {nation.leader?.thumbnail_url ? (
            <img
              src={nation.leader.thumbnail_url}
              alt={nation.leader.name}
              className="h-9 w-9 rounded-full object-cover border border-border-dim"
            />
          ) : (
            <div className="flex h-9 w-9 items-center justify-center rounded-full bg-surface-overlay text-sm">
              {flag}
            </div>
          )}
          <div className="min-w-0 flex-1">
            <div className="text-xs font-semibold text-zinc-100 truncate">
              {flag} {name}
            </div>
            {nation.leader && (
              <div className="text-[10px] text-muted truncate">
                {nation.leader.name} · {nation.leader.role.replace(/_/g, " ")}
              </div>
            )}
          </div>
          <div className="text-right">
            <div className="text-lg font-bold" style={{ color }}>
              {pct(nation.composite_risk)}
            </div>
            <div className="text-[9px] text-muted">composite</div>
          </div>
        </div>

        {/* Score bars */}
        <div className="space-y-1">
          <ScoreBar label="Economic" value={nation.economic_stability} />
          <ScoreBar label="Market" value={nation.market_stability} />
          <ScoreBar label="Political" value={nation.political_stability} />
          <ScoreBar label="Currency" value={nation.currency_risk} />
          <ScoreBar label="Opportunity" value={nation.opportunity_score} />
        </div>

        {/* Trade flow links */}
        {nation.dependencies.length > 0 && (
          <div className="mt-2 pt-1.5 border-t border-border-dim">
            <div className="text-[9px] text-muted mb-0.5">Trade flow</div>
            <div className="flex flex-wrap gap-1">
              {nation.dependencies.slice(0, 6).map((d) => {
                const flow = d.flow ?? "balanced";
                const arrow = flow === "export" ? "→" : flow === "import" ? "←" : "↔";
                const flowColor = flow === "export" ? "#22c55e"
                               : flow === "import" ? "#3b82f6"
                               : "#a1a1aa";
                return (
                  <span
                    key={d.nation}
                    className="rounded bg-surface-overlay px-1.5 py-0.5 text-[9px]"
                  >
                    <span style={{ color: flowColor }}>{arrow}</span>
                    {" "}{NATION_FLAGS[d.nation] ?? ""} {d.nation}
                    <span className="text-muted ml-0.5">{pct(d.weight)}</span>
                  </span>
                );
              })}
            </div>
            <div className="flex gap-3 mt-1 text-[8px] text-muted">
              <span><span style={{ color: "#22c55e" }}>→</span> export</span>
              <span><span style={{ color: "#3b82f6" }}>←</span> import</span>
              <span><span style={{ color: "#a1a1aa" }}>↔</span> balanced</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}


function ScoreBar({ label, value }: { label: string; value: number }) {
  const color = riskColor(value);
  return (
    <div className="flex items-center gap-1.5">
      <span className="w-16 text-[9px] text-muted truncate">{label}</span>
      <div className="flex-1 h-1.5 rounded-full bg-surface-overlay overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{ width: `${value * 100}%`, backgroundColor: color }}
        />
      </div>
      <span className="w-7 text-right text-[9px] text-zinc-300">{pct(value)}</span>
    </div>
  );
}
