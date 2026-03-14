import { useState, useMemo, useCallback, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { WorldMap, MapNation } from "../components/WorldMap";
import { MapLayerToggle, type MapLayer, type LayerItem } from "../components/MapLayerToggle";
import { ChokepointTooltip, type ChokepointData } from "../components/ChokepointMarkers";
import { TradeRouteTooltip, type TradeRouteData } from "../components/TradeRouteLines";
import { ResourceTooltip, type ResourceData } from "../components/ResourceOverlay";
import { ConflictTooltipContent, type ConflictData } from "../components/ConflictMarkers";
import { PortTooltip, type PortData } from "../components/PortMarkers";
import type { TradeMarkerItem } from "../components/ClusteredTradeMarkers";
import type { VesselData } from "../components/VesselOverlay";
import type { FlightData } from "../components/FlightOverlay";
import type { NavalDeploymentData } from "../components/NavalDeploymentOverlay";
import { useNationMapSummary, useChokepoints, useTradeRoutes, useResources, useConflicts, usePorts, useVessels, useFlights, useNavalDeployments } from "../api/hooks";
import { NATION_FLAGS, NATION_NAMES, COUNTRY_CENTROIDS } from "../data/countryMapping";

// ── Helpers ──────────────────────────────────────────────────────

function pct(v: number): string {
  return `${(v * 100).toFixed(0)}%`;
}

function riskSentiment(v: number): "text-positive" | "text-warning" | "text-negative" {
  if (v >= 0.65) return "text-positive";
  if (v >= 0.45) return "text-warning";
  return "text-negative";
}

function riskGrade(v: number): { letter: string; label: string; color: string } {
  if (v >= 0.80) return { letter: "A", label: "Strong",   color: "#22c55e" };
  if (v >= 0.65) return { letter: "B", label: "Stable",   color: "#4ade80" };
  if (v >= 0.45) return { letter: "C", label: "Moderate", color: "#facc15" };
  if (v >= 0.30) return { letter: "D", label: "Fragile",  color: "#f97316" };
  return                { letter: "F", label: "Critical", color: "#ef4444" };
}

type SortKey = "composite_risk" | "economic_stability" | "political_stability" | "opportunity_score" | "nation";

// ── Component ────────────────────────────────────────────────────

export default function GeoRisk() {
  const navigate = useNavigate();
  const mapQuery = useNationMapSummary();
  const scoredNations = (mapQuery.data ?? []) as MapNation[];

  // Merge API scores with all tracked nations from countryMapping so
  // unscored nations still appear on the map with placeholder dots.
  const allNations: MapNation[] = useMemo(() => {
    const byIso = new Map(scoredNations.map((n) => [n.nation, n]));
    const list: MapNation[] = [...scoredNations];
    for (const iso3 of Object.keys(NATION_NAMES)) {
      if (!byIso.has(iso3) && COUNTRY_CENTROIDS[iso3]) {
        list.push({
          nation: iso3,
          composite_risk: -1,        // sentinel: not scored
          economic_stability: 0,
          market_stability: 0,
          political_stability: 0,
          contagion_risk: 0,
          currency_risk: 0,
          opportunity_score: 0,
          leadership_risk: 0,
          leader: null,
          dependencies: [],
        });
      }
    }
    return list;
  }, [scoredNations]);

  // Separate scored subset for cards / rankings
  const nations = allNations;

  // Click-to-select (persistent)
  const [selectedNation, setSelectedNation] = useState<string | null>(null);
  const [popupPage, setPopupPage] = useState(0);
  const [expandedCard, setExpandedCard] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("composite_risk");

  // Overlay state
  const [activeOverlays, setActiveOverlays] = useState<Set<MapLayer>>(
    () => new Set<MapLayer>(["nations"])
  );
  const [hoveredTradeMarker, setHoveredTradeMarker] = useState<string | null>(null);
  const [hoveredRoute, setHoveredRoute] = useState<string | null>(null);
  const [hoveredConflict, setHoveredConflict] = useState<string | null>(null);
  const [selectedTradeEntity, setSelectedTradeEntity] = useState<{ type: "chokepoint" | "port"; id: string } | null>(null);
  const [pinnedConflict, setPinnedConflict] = useState<string | null>(null);
  const [pinnedPos, setPinnedPos] = useState({ x: 200, y: 100 });
  const lastMousePos = useRef({ x: 300, y: 200 });
  // Tooltip position: use ref + direct DOM update to avoid re-rendering the map on every mouse move
  const tooltipRef = useRef<HTMLDivElement>(null);

  // Resource/commodity filter
  const [selectedResource, setSelectedResource] = useState<string | null>(null);

  // Vessel / flight / deployment category filter
  const [vesselFilter, setVesselFilter] = useState<string | null>(null);
  const [flightFilter, setFlightFilter] = useState<string | null>(null);
  const [deploymentFilter, setDeploymentFilter] = useState<string | null>(null);

  // External zoom target (set by country dropdown)
  const [mapCenter, setMapCenter] = useState<[number, number] | null>(null);
  const [mapZoom, setMapZoom] = useState<number | null>(null);

  // Overlay data queries
  const chokepointsQuery = useChokepoints();
  const tradeRoutesQuery = useTradeRoutes();
  const resourcesQuery = useResources();
  const conflictsQuery = useConflicts();
  const portsQuery = usePorts();
  const vesselsQuery = useVessels();
  const flightsQuery = useFlights();
  const deploymentsQuery = useNavalDeployments();

  const chokepoints = (chokepointsQuery.data ?? []) as ChokepointData[];
  const tradeRoutes = (tradeRoutesQuery.data ?? []) as TradeRouteData[];
  const allResources = ((resourcesQuery.data as any)?.resources ?? []) as ResourceData[];
  const conflicts = (conflictsQuery.data ?? []) as ConflictData[];
  const ports = (portsQuery.data ?? []) as PortData[];
  const vessels = (vesselsQuery.data ?? []) as VesselData[];
  const flights = (flightsQuery.data ?? []) as FlightData[];
  const deployments = (deploymentsQuery.data ?? []) as NavalDeploymentData[];

  // Unique resource names for dropdown
  const resourceNames = useMemo(() => {
    const names = [...new Set(allResources.map((r) => r.resource))].sort();
    return names;
  }, [allResources]);

  // ── Status color maps for dropdown items ──
  const CP_STATUS_COLORS: Record<string, string> = { OPEN: "#22c55e", THREATENED: "#f59e0b", DISRUPTED: "#f97316", CLOSED: "#ef4444" };
  const CONF_STATUS_COLORS: Record<string, string> = { ACTIVE: "#ef4444", ESCALATING: "#f97316", CEASEFIRE: "#f59e0b", FROZEN: "#6b7280" };
  const PORT_STATUS_COLORS: Record<string, string> = { OPERATIONAL: "#8b5cf6", CONGESTED: "#f59e0b", DISRUPTED: "#ef4444" };
  const RES_CATEGORY_ICONS: Record<string, string> = { energy: "🛢️", metal: "⛏", critical_mineral: "⚗️", agriculture: "🌾", tech_material: "🔬" };

  // ── Layer dropdown items ──
  const layerItems = useMemo<Partial<Record<MapLayer, LayerItem[]>>>(() => ({
    nations: Object.keys(NATION_NAMES)
      .filter((iso) => COUNTRY_CENTROIDS[iso])
      .sort((a, b) => (NATION_NAMES[a] ?? a).localeCompare(NATION_NAMES[b] ?? b))
      .map((iso) => ({ id: iso, label: `${NATION_FLAGS[iso] ?? ""} ${NATION_NAMES[iso] ?? iso}` })),
    routes: [
      // Chokepoints
      ...chokepoints.map((c) => ({ id: `cp:${c.id}`, label: `⚓ ${c.name}`, color: CP_STATUS_COLORS[c.status] })),
      // Trade corridors
      ...tradeRoutes.map((r) => ({ id: `rt:${r.id}`, label: `🛣️ ${r.name}` })),
      // Seaports
      ...ports.filter((p) => p.port_type === "seaport").map((p) => ({
        id: `pt:${p.id}`, label: `🚢 ${p.name}`, color: PORT_STATUS_COLORS[p.status],
      })),
      // Cargo airports
      ...ports.filter((p) => p.port_type === "cargo_airport").map((p) => ({
        id: `pt:${p.id}`, label: `✈️ ${p.name}`, color: PORT_STATUS_COLORS[p.status],
      })),
    ],
    resources: resourceNames.map((r) => {
      const sample = allResources.find((x) => x.resource === r);
      const icon = sample ? (RES_CATEGORY_ICONS[sample.category] ?? "") : "";
      return { id: r, label: `${icon} ${r.replace(/_/g, " ")}` };
    }),
    conflicts: conflicts.map((c) => ({ id: c.id, label: c.name, color: CONF_STATUS_COLORS[c.status] })),
    deployments: [
      { id: "carrier", label: "🚢 Carriers", color: "#ef4444" },
      { id: "destroyer", label: "🛡️ Destroyers", color: "#3b82f6" },
      { id: "amphibious", label: "🚁 Amphibious", color: "#22c55e" },
      { id: "lcs", label: "⚡ LCS / MCM", color: "#06b6d4" },
      // Country filters — derived from data
      ...(() => {
        const nations = [...new Set(deployments.map((d) => d.nation))].sort();
        if (nations.length > 1) {
          return nations.map((n) => ({ id: `nation:${n}`, label: `🏴 ${n}`, color: "#f59e0b" }));
        }
        return [];
      })(),
    ],
    vessels: [
      { id: "military", label: "⚔️ Military", color: "#ef4444" },
      { id: "commercial", label: "📦 Commercial Cargo", color: "#94a3b8" },
      { id: "tanker", label: "🛢️ Tankers", color: "#f59e0b" },
      // Country filters from live vessel data
      ...(() => {
        const flags = [...new Set(vessels.map((v) => v.flag_iso3).filter(Boolean))].sort();
        return flags.slice(0, 40).map((f) => ({ id: `nation:${f}`, label: `🏴 ${f}`, color: "#0ea5e9" }));
      })(),
    ],
    flights: [
      { id: "military", label: "⚔️ Military", color: "#ef4444" },
      { id: "cargo", label: "📦 Cargo Airlines", color: "#60a5fa" },
      { id: "passenger", label: "✈️ Passenger Airlines", color: "#22d3ee" },
      // Country filters from live flight data
      ...(() => {
        const countries = [...new Set(flights.map((f) => f.flag_iso3).filter(Boolean))].sort();
        return countries.slice(0, 40).map((c) => ({ id: `nation:${c}`, label: `🏴 ${c}`, color: "#d946ef" }));
      })(),
    ],
  }), [chokepoints, tradeRoutes, resourceNames, allResources, conflicts, ports, deployments, vessels, flights]);

  // ── Layer selection state (derived from existing state) ──
  const layerSelection = useMemo<Partial<Record<MapLayer, string | null>>>(() => ({
    nations: selectedNation,
    resources: selectedResource,
    vessels: vesselFilter,
    flights: flightFilter,
    deployments: deploymentFilter,
  }), [selectedNation, selectedResource, vesselFilter, flightFilter, deploymentFilter]);

  const handleToggleLayer = useCallback((layer: MapLayer) => {
    // Clear selection when turning off
    if (activeOverlays.has(layer)) {
      if (layer === "nations") { setSelectedNation(null); setExpandedCard(null); setMapCenter(null); setMapZoom(null); }
      if (layer === "resources") setSelectedResource(null);
      if (layer === "conflicts") setPinnedConflict(null);
      if (layer === "vessels") setVesselFilter(null);
      if (layer === "flights") setFlightFilter(null);
      if (layer === "deployments") setDeploymentFilter(null);
    }
    setActiveOverlays((prev) => {
      const next = new Set(prev);
      if (next.has(layer)) next.delete(layer);
      else next.add(layer);
      return next;
    });
  }, [activeOverlays]);

  // ── Layer item selection handler ──
  const handleLayerSelect = useCallback((layer: MapLayer, itemId: string | null) => {
    switch (layer) {
      case "nations":
        setSelectedNation(itemId);
        setExpandedCard(itemId);
        setPopupPage(0);
        if (itemId) {
          const coords = COUNTRY_CENTROIDS[itemId];
          if (coords) { setMapCenter(coords as [number, number]); setMapZoom(3); }
        } else {
          setMapCenter(null); setMapZoom(null);
        }
        break;
      case "resources":
        setSelectedResource(itemId);
        if (itemId) {
          setActiveOverlays((prev) => { const next = new Set(prev); next.add("resources"); return next; });
        }
        break;
      case "routes": {
        if (itemId) {
          const [prefix, ...rest] = itemId.split(":");
          const rawId = rest.join(":");
          if (prefix === "cp") {
            const cp = chokepoints.find((c) => c.id === rawId);
            if (cp) { setMapCenter(cp.coordinates); setMapZoom(4); }
          } else if (prefix === "rt") {
            const r = tradeRoutes.find((rt) => rt.id === rawId);
            if (r && r.waypoints.length > 0) {
              const mid = r.waypoints[Math.floor(r.waypoints.length / 2)];
              setMapCenter(mid); setMapZoom(2);
            }
          } else if (prefix === "pt") {
            const p = ports.find((pt) => pt.id === rawId);
            if (p) { setMapCenter(p.coordinates); setMapZoom(5); }
          }
        }
        break;
      }
      case "conflicts": {
        if (itemId) {
          const c = conflicts.find((cf) => cf.id === itemId);
          if (c) {
            setPinnedConflict(itemId);
            setPinnedPos({ x: window.innerWidth / 2 - 160, y: 100 });
            setMapCenter(c.coordinates); setMapZoom(4);
          }
        } else {
          setPinnedConflict(null);
        }
        break;
      }
      case "vessels":
        setVesselFilter(itemId);
        break;
      case "flights":
        setFlightFilter(itemId);
        break;
      case "deployments":
        setDeploymentFilter(itemId);
        break;
    }
  }, [chokepoints, tradeRoutes, conflicts, ports]);

  // Track mouse for overlay tooltips (direct DOM, no state)
  const handleMapMouseMove = useCallback((e: React.MouseEvent) => {
    lastMousePos.current = { x: e.clientX, y: e.clientY };
    if (tooltipRef.current) {
      tooltipRef.current.style.left = `${Math.min(e.clientX + 16, window.innerWidth - 320)}px`;
      tooltipRef.current.style.top = `${Math.min(e.clientY - 10, window.innerHeight - 300)}px`;
    }
  }, []);

  // Click conflict → pin tooltip at current mouse position
  const handleConflictClick = useCallback((id: string) => {
    setPinnedConflict((prev) => {
      if (prev === id) return null; // toggle off
      setPinnedPos({
        x: Math.min(lastMousePos.current.x + 16, window.innerWidth - 340),
        y: Math.max(lastMousePos.current.y - 200, 10),
      });
      return id;
    });
  }, []);

  // Drag handler for pinned tooltip
  const handlePinnedDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startY = e.clientY;
    const origX = pinnedPos.x;
    const origY = pinnedPos.y;
    const onMove = (ev: MouseEvent) => {
      setPinnedPos({
        x: origX + (ev.clientX - startX),
        y: origY + (ev.clientY - startY),
      });
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [pinnedPos]);

  // Derive hovered chokepoint/port data from unified trade marker hover
  const hoveredChokepointData = hoveredTradeMarker
    ? chokepoints.find((c) => c.id === hoveredTradeMarker) ?? null
    : null;
  const hoveredPortData = !hoveredChokepointData && hoveredTradeMarker
    ? ports.find((p) => p.id === hoveredTradeMarker) ?? null
    : null;
  const hoveredRouteData = hoveredRoute
    ? tradeRoutes.find((r) => r.id === hoveredRoute) ?? null
    : null;
  const hoveredConflictData = hoveredConflict
    ? conflicts.find((c) => c.id === hoveredConflict) ?? null
    : null;
  const pinnedConflictData = pinnedConflict
    ? conflicts.find((c) => c.id === pinnedConflict) ?? null
    : null;

  // Compute linked military assets for a conflict
  const getLinkedAssets = useCallback((conflict: ConflictData | null) => {
    if (!conflict) return { deployments: 0, flights: 0, darkVessels: 0, darkFlights: 0 };
    const partyNations = new Set<string>();
    conflict.parties.forEach((p) => p.nations.forEach((n) => partyNations.add(n)));
    return {
      deployments: deployments.filter((d) => d.conflict_ids.includes(conflict.id)).length,
      flights: flights.filter((f) => f.category === "military" && partyNations.has(f.flag_iso3)).length,
      darkVessels: vessels.filter((v) => v.status === "dark" && partyNations.has(v.flag_iso3)).length,
      darkFlights: flights.filter((f) => f.status === "dark" && partyNations.has(f.flag_iso3)).length,
    };
  }, [deployments, flights, vessels]);

  const hoveredConflictAssets = useMemo(() => getLinkedAssets(hoveredConflictData), [hoveredConflictData, getLinkedAssets]);
  const pinnedConflictAssets = useMemo(() => getLinkedAssets(pinnedConflictData), [pinnedConflictData, getLinkedAssets]);

  // Resolve selected trade entity data
  const selectedTradeEntityData = useMemo(() => {
    if (!selectedTradeEntity) return null;
    if (selectedTradeEntity.type === "chokepoint") {
      return { type: "chokepoint" as const, data: chokepoints.find((c) => c.id === selectedTradeEntity.id) ?? null };
    }
    return { type: "port" as const, data: ports.find((p) => p.id === selectedTradeEntity.id) ?? null };
  }, [selectedTradeEntity, chokepoints, ports]);

  // Click handler for trade markers — mutual exclusion with nation selection
  const handleClickTradeMarker = useCallback((item: TradeMarkerItem) => {
    const isSame = selectedTradeEntity?.id === item.id;
    if (isSame) {
      setSelectedTradeEntity(null);
    } else {
      const entityType = item.type === "chokepoint" ? "chokepoint" as const : "port" as const;
      setSelectedTradeEntity({ type: entityType, id: item.id });
      setSelectedNation(null);
      setExpandedCard(null);
    }
  }, [selectedTradeEntity]);

  // Nation selection from map or dropdown — deselect trade entity
  const handleSelectNation = useCallback((iso3: string | null) => {
    setSelectedNation(iso3);
    setExpandedCard(iso3);
    setPopupPage(0);
    if (iso3) setSelectedTradeEntity(null);
  }, []);


  // Only scored nations appear in the rankings grid
  const sorted = useMemo(() => {
    const arr = scoredNations.filter((n) => n.composite_risk >= 0);
    if (sortKey === "nation") {
      arr.sort((a, b) => a.nation.localeCompare(b.nation));
    } else {
      arr.sort((a, b) => (b[sortKey] as number) - (a[sortKey] as number));
    }
    return arr;
  }, [scoredNations, sortKey]);

  // Card click: toggle inline expand + sync map selection
  const handleCardClick = useCallback((iso3: string) => {
    const next = expandedCard === iso3 ? null : iso3;
    setExpandedCard(next);
    setSelectedNation(next);
  }, [expandedCard]);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Geopolitical Risk Map"
        subtitle={`${scoredNations.length} scored · ${allNations.length} tracked · click to select`}
        onRefresh={() => mapQuery.refetch()}
      />

      {/* Map */}
      <Panel className="overflow-hidden !p-0">
        {/* Control bar */}
        <div className="flex flex-wrap items-center gap-2 px-3 py-2 border-b border-border-dim">
          <MapLayerToggle
            active={activeOverlays}
            onToggle={handleToggleLayer}
            layerItems={layerItems}
            selectedItems={layerSelection}
            onSelectItem={handleLayerSelect}
          />
        </div>

        {nations.length > 0 ? (
          <div onMouseMove={handleMapMouseMove}>
            <WorldMap
              nations={nations}
              selectedNation={selectedNation}
              onSelectNation={handleSelectNation}
              activeOverlays={activeOverlays}
              chokepoints={chokepoints}
              tradeRoutes={tradeRoutes}
              resources={allResources}
              selectedResource={selectedResource}
              onClickResource={(r, n) => navigate(`/resource?r=${r}&n=${n}`)}
              conflicts={conflicts}
              ports={ports}
              deployments={deploymentFilter
                ? deploymentFilter.startsWith("nation:")
                  ? deployments.filter((d) => d.nation === deploymentFilter.slice(7))
                  : deployments.filter((d) => d.ship_type === deploymentFilter)
                : deployments}
              vessels={vesselFilter
                ? vesselFilter.startsWith("nation:")
                  ? vessels.filter((v) => v.flag_iso3 === vesselFilter.slice(7))
                  : vesselFilter === "tanker" ? vessels.filter((v) => v.subcategory === "tanker") : vessels.filter((v) => v.category === vesselFilter)
                : vessels}
              flights={flightFilter
                ? flightFilter.startsWith("nation:")
                  ? flights.filter((f) => f.flag_iso3 === flightFilter.slice(7))
                  : flights.filter((f) => f.category === flightFilter)
                : flights}
              hoveredTradeMarker={hoveredTradeMarker}
              onHoverTradeMarker={setHoveredTradeMarker}
              onClickTradeMarker={handleClickTradeMarker}
              selectedTradeMarkerId={selectedTradeEntity?.id ?? null}
              hoveredRoute={hoveredRoute}
              onHoverRoute={setHoveredRoute}
              hoveredConflict={hoveredConflict}
              onHoverConflict={setHoveredConflict}
              onClickConflict={handleConflictClick}
              pinnedConflict={pinnedConflict}
              externalCenter={mapCenter}
              externalZoom={mapZoom}
            />
          </div>
        ) : (
          <div className="flex h-64 items-center justify-center text-xs text-muted">
            {mapQuery.isLoading
              ? "Loading nation data…"
              : "No scored nations — run the nation scoring pipeline to populate"}
          </div>
        )}

        {/* Paginated nation detail popup */}
        {selectedNation && (() => {
          const nd = scoredNations.find((n) => n.nation === selectedNation);
          if (!nd || nd.composite_risk < 0) return null;
          const flag = NATION_FLAGS[nd.nation] ?? "";
          const name = NATION_NAMES[nd.nation] ?? nd.nation;
          const rc = (v: number) => v >= 0.65 ? "#22c55e" : v >= 0.45 ? "#facc15" : "#ef4444";
          const p = (v: number) => `${(v * 100).toFixed(0)}%`;

          // Resources for this nation
          const natResources = allResources.filter((r: any) => r.nation === nd.nation);
          // Chokepoints for this nation
          const natChokepoints = chokepoints.filter((c: any) =>
            c.controlling_nations?.includes(nd.nation) || c.affected_nations?.includes(nd.nation)
          );
          // Trade routes for this nation
          const natRoutes = tradeRoutes.filter((r: any) =>
            r.source_nations?.includes(nd.nation) || r.dest_nations?.includes(nd.nation)
          );

          // Conflicts for this nation
          const natConflicts = conflicts.filter((c: ConflictData) => {
            const allNations = new Set(c.affected_nations);
            c.parties.forEach((p) => p.nations.forEach((n) => allNations.add(n)));
            return allNations.has(nd.nation);
          });

          const PAGE_LABELS = ["Scores", "Resources", "Strategic", "Trade", "Conflicts"];
          const totalPages = PAGE_LABELS.length;

          return (
            <div className="absolute bottom-2 left-2 z-10 animate-fade-in">
              <div className="w-80 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl">
                {/* Header */}
                <div className="flex items-center gap-2 p-3 pb-2">
                  {nd.leader?.thumbnail_url ? (
                    <img src={nd.leader.thumbnail_url} alt="" className="h-10 w-10 rounded-full object-cover border border-border-dim" />
                  ) : (
                    <div className="flex h-10 w-10 items-center justify-center rounded-full bg-surface-overlay text-base">{flag}</div>
                  )}
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-semibold text-zinc-100 truncate">{flag} {name}</div>
                    {nd.leader && <div className="text-[10px] text-muted truncate">{nd.leader.name} · {nd.leader.role.replace(/_/g, " ")}</div>}
                  </div>
                  <div className="text-right">
                    <div className="text-xl font-bold" style={{ color: riskGrade(nd.composite_risk).color }}>{riskGrade(nd.composite_risk).letter}</div>
                    <div className="text-[8px] text-muted">{riskGrade(nd.composite_risk).label}</div>
                  </div>
                </div>

                {/* Page navigation */}
                <div className="flex items-center justify-between px-3 pb-1">
                  <button onClick={() => setPopupPage((i) => Math.max(0, i - 1))} disabled={popupPage === 0}
                    className="rounded p-0.5 text-muted hover:text-zinc-100 disabled:opacity-20"><ChevronLeft size={14} /></button>
                  <div className="flex gap-1">
                    {PAGE_LABELS.map((lbl, i) => (
                      <button key={lbl} onClick={() => setPopupPage(i)}
                        className={`rounded px-2 py-0.5 text-[9px] font-medium transition-colors ${popupPage === i ? "bg-accent text-black" : "text-muted hover:text-zinc-100"}`}>
                        {lbl}
                      </button>
                    ))}
                  </div>
                  <button onClick={() => setPopupPage((i) => Math.min(totalPages - 1, i + 1))} disabled={popupPage === totalPages - 1}
                    className="rounded p-0.5 text-muted hover:text-zinc-100 disabled:opacity-20"><ChevronRight size={14} /></button>
                </div>

                {/* Page content */}
                <div className="px-3 pb-3 pt-1 min-h-[140px]">
                  {popupPage === 0 && (
                    <div className="space-y-1.5">
                      {(["economic_stability", "market_stability", "political_stability", "currency_risk", "opportunity_score", "contagion_risk", "leadership_risk"] as const).map((key) => {
                        const v = (nd as any)[key] as number;
                        const label = key.replace(/_/g, " ").replace(/\b\w/g, (c: string) => c.toUpperCase());
                        const g = riskGrade(v);
                        return (
                          <div key={key} className="flex items-center gap-1.5">
                            <span className="w-24 text-[9px] text-muted truncate">{label}</span>
                            <div className="flex-1 h-1.5 rounded-full bg-surface-overlay overflow-hidden">
                              <div className="h-full rounded-full" style={{ width: `${v * 100}%`, backgroundColor: g.color }} />
                            </div>
                            <span className="w-8 text-right text-[9px] font-bold" style={{ color: g.color }}>{g.letter}</span>
                          </div>
                        );
                      })}
                    </div>
                  )}

                  {popupPage === 1 && (
                    <div className="space-y-1.5 max-h-[180px] overflow-auto">
                      {natResources.length > 0 ? natResources.map((r: any) => (
                        <div key={r.resource} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1">
                          <div>
                            <div className="text-[10px] font-medium text-zinc-200">{(r.resource as string).replace(/_/g, " ")}</div>
                            <div className="text-[8px] text-muted">{r.category} · {r.production}</div>
                          </div>
                          <div className="text-right">
                            <div className="text-[10px] font-bold text-accent tabular-nums">{Number(r.global_share_pct).toFixed(1)}%</div>
                            <div className="text-[7px] text-muted">global share</div>
                          </div>
                        </div>
                      )) : <div className="py-4 text-center text-[10px] text-muted">No resource data</div>}
                    </div>
                  )}

                  {popupPage === 2 && (
                    <div className="space-y-2 max-h-[180px] overflow-auto">
                      {natChokepoints.length > 0 && (
                        <div>
                          <div className="text-[8px] uppercase text-muted mb-1">Chokepoints ({natChokepoints.length})</div>
                          {natChokepoints.map((c: any) => (
                            <div key={c.id} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1 mb-1">
                              <span className="text-[10px] text-zinc-200">{c.name}</span>
                              <span className={`text-[8px] font-bold ${c.status === "OPEN" ? "text-positive" : c.status === "THREATENED" ? "text-warning" : "text-negative"}`}>{c.status}</span>
                            </div>
                          ))}
                        </div>
                      )}
                      {natRoutes.length > 0 && (
                        <div>
                          <div className="text-[8px] uppercase text-muted mb-1">Trade Routes ({natRoutes.length})</div>
                          {natRoutes.map((r: any) => (
                            <div key={r.id} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1 mb-1">
                              <div>
                                <div className="text-[10px] text-zinc-200">{r.name}</div>
                                <div className="text-[8px] text-muted">{r.category} · {r.volume}</div>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                      {natChokepoints.length === 0 && natRoutes.length === 0 && (
                        <div className="py-4 text-center text-[10px] text-muted">No strategic assets</div>
                      )}
                    </div>
                  )}

                  {popupPage === 3 && (
                    <div className="space-y-2 max-h-[180px] overflow-auto">
                      {nd.dependencies.length > 0 ? (
                        <>
                          <div className="text-[8px] uppercase text-muted mb-1">Contagion Links ({nd.dependencies.length})</div>
                          {nd.dependencies.map((d) => {
                            const flow = d.flow ?? "balanced";
                            const arrow = flow === "export" ? "→" : flow === "import" ? "←" : "↔";
                            const flowColor = flow === "export" ? "#22c55e" : flow === "import" ? "#3b82f6" : "#a1a1aa";
                            return (
                              <div key={d.nation} className="flex items-center justify-between rounded bg-surface-overlay px-2 py-1">
                                <span className="text-[10px]">
                                  <span style={{ color: flowColor }}>{arrow}</span> {NATION_FLAGS[d.nation] ?? ""} {NATION_NAMES[d.nation] ?? d.nation}
                                </span>
                                <span className="text-[9px] text-muted">{d.channel ?? flow} · {pct(d.weight)}</span>
                              </div>
                            );
                          })}
                          <div className="flex gap-3 mt-1 text-[8px] text-muted">
                            <span><span style={{ color: "#22c55e" }}>→</span> export</span>
                            <span><span style={{ color: "#3b82f6" }}>←</span> import</span>
                            <span><span style={{ color: "#a1a1aa" }}>↔</span> balanced</span>
                          </div>
                        </>
                      ) : <div className="py-4 text-center text-[10px] text-muted">No trade links</div>}
                    </div>
                  )}

                  {popupPage === 4 && (
                    <div className="space-y-1.5 max-h-[180px] overflow-auto">
                      {natConflicts.length > 0 ? (
                        <>
                          <div className="text-[8px] uppercase text-muted mb-1">Conflicts ({natConflicts.length})</div>
                          {natConflicts.map((c: ConflictData) => {
                            const sc: Record<string, string> = { ACTIVE: "#ef4444", ESCALATING: "#f97316", CEASEFIRE: "#f59e0b", FROZEN: "#6b7280" };
                            const clr = sc[c.status] ?? "#888";
                            return (
                              <div key={c.id} className="rounded bg-surface-overlay px-2 py-1.5 mb-1">
                                <div className="flex items-center justify-between mb-0.5">
                                  <span className="text-[10px] font-medium text-zinc-200 truncate">{c.name}</span>
                                  <span className="text-[8px] font-bold uppercase shrink-0" style={{ color: clr }}>{c.status}</span>
                                </div>
                                <div className="text-[8px] text-muted">{c.conflict_type.replace(/_/g, " ")} · esc. risk {(c.escalation_risk * 100).toFixed(0)}%</div>
                              </div>
                            );
                          })}
                        </>
                      ) : <div className="py-4 text-center text-[10px] text-muted">No active conflicts</div>}
                    </div>
                  )}
                </div>

                {/* Footer actions */}
                <div className="flex gap-2 px-3 pb-3">
                  <button onClick={() => navigate(`/nation?n=${nd.nation}`)}
                    className="flex-1 rounded bg-accent/10 border border-accent/30 px-2 py-1 text-[10px] font-semibold text-accent hover:bg-accent/20 transition-colors">
                    Full Profile →
                  </button>
                  <button onClick={() => { setSelectedNation(null); setExpandedCard(null); }}
                    className="rounded bg-surface-overlay px-2 py-1 text-[9px] text-muted hover:text-zinc-100 transition-colors">
                    ✕
                  </button>
                </div>
              </div>
            </div>
          );
        })()}

        {/* Persistent trade entity popup */}
        {selectedTradeEntityData?.data && (() => {
          const { type, data } = selectedTradeEntityData;
          if (!data) return null;
          const isChokepoint = type === "chokepoint";
          const cp = isChokepoint ? (data as ChokepointData) : null;
          const pt = !isChokepoint ? (data as PortData) : null;
          const name = cp?.name ?? pt?.name ?? "";
          const status = cp?.status ?? pt?.status ?? "";
          const statusColor = cp ? (CP_STATUS_COLORS[status] ?? "#888") : (PORT_STATUS_COLORS[status] ?? "#888");
          const emoji = isChokepoint ? "⚓" : pt?.port_type === "cargo_airport" ? "✈️" : "🚢";
          const typeLabel = isChokepoint ? "Chokepoint" : pt?.port_type === "cargo_airport" ? "Cargo Airport" : "Seaport";
          return (
            <div className="absolute bottom-2 left-2 z-10 animate-fade-in">
              <div className="w-72 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl">
                <div className="flex items-center gap-2 p-3 pb-2">
                  <div className="flex h-10 w-10 items-center justify-center rounded-full bg-surface-overlay text-lg">{emoji}</div>
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-semibold text-zinc-100 truncate">{name}</div>
                    <div className="text-[10px] text-muted">{typeLabel}</div>
                  </div>
                  <span className="text-[10px] font-bold uppercase" style={{ color: statusColor }}>{status}</span>
                </div>
                <div className="px-3 pb-2 space-y-1">
                  {cp && (
                    <>
                      <div className="text-[9px] text-muted">Volume: <span className="text-zinc-300">{cp.daily_volume}</span></div>
                      <div className="text-[9px] text-muted">World Share: <span className="text-zinc-300">{cp.world_share}</span></div>
                      {cp.controlling_nations?.length > 0 && (
                        <div className="text-[9px] text-muted">Controls: <span className="text-zinc-300">{cp.controlling_nations.map((n) => `${NATION_FLAGS[n] ?? ""} ${NATION_NAMES[n] ?? n}`).join(", ")}</span></div>
                      )}
                    </>
                  )}
                  {pt && (
                    <>
                      <div className="text-[9px] text-muted">Annual Volume: <span className="text-zinc-300">{pt.annual_volume} {pt.volume_unit}</span></div>
                      <div className="text-[9px] text-muted">Nation: <span className="text-zinc-300">{NATION_FLAGS[pt.nation] ?? ""} {NATION_NAMES[pt.nation] ?? pt.nation}</span></div>
                      {pt.key_commodities?.length > 0 && (
                        <div className="text-[9px] text-muted">Commodities: <span className="text-zinc-300">{pt.key_commodities.join(", ")}</span></div>
                      )}
                    </>
                  )}
                </div>
                <div className="flex gap-2 px-3 pb-3">
                  <button onClick={() => navigate(`/trade?type=${type}&id=${data.id}`)}
                    className="flex-1 rounded bg-accent/10 border border-accent/30 px-2 py-1 text-[10px] font-semibold text-accent hover:bg-accent/20 transition-colors">
                    Full Profile →
                  </button>
                  <button onClick={() => setSelectedTradeEntity(null)}
                    className="rounded bg-surface-overlay px-2 py-1 text-[9px] text-muted hover:text-zinc-100 transition-colors">
                    ✕
                  </button>
                </div>
              </div>
            </div>
          );
        })()}

        {/* Overlay tooltips — positioned via ref to avoid re-rendering map */}
        {(hoveredChokepointData || hoveredRouteData || (hoveredConflictData && !pinnedConflict) || hoveredPortData) && (
          <div
            ref={tooltipRef}
            className="fixed z-50 pointer-events-none"
            style={{ left: 0, top: 0 }}
          >
            {hoveredChokepointData && (
              <ChokepointTooltip chokepoint={hoveredChokepointData} />
            )}
            {hoveredRouteData && (
              <TradeRouteTooltip route={hoveredRouteData} />
            )}
            {hoveredConflictData && !pinnedConflict && (
              <div className="animate-fade-in">
                <div className="w-80 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl p-3">
                  <ConflictTooltipContent
                    conflict={hoveredConflictData}
                    linkedDeployments={hoveredConflictAssets.deployments}
                    linkedFlights={hoveredConflictAssets.flights}
                    linkedDarkVessels={hoveredConflictAssets.darkVessels}
                    linkedDarkFlights={hoveredConflictAssets.darkFlights}
                  />
                </div>
              </div>
            )}
            {hoveredPortData && (
              <PortTooltip port={hoveredPortData} />
            )}
          </div>
        )}

        {/* Pinned conflict tooltip — draggable + scrollable */}
        {pinnedConflictData && (
          <div
            className="fixed z-[60] w-80 flex flex-col rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl animate-fade-in"
            style={{ left: pinnedPos.x, top: pinnedPos.y, maxHeight: "70vh" }}
          >
            {/* Drag handle */}
            <div
              className="flex items-center justify-between px-3 py-1.5 cursor-grab active:cursor-grabbing border-b border-border-dim shrink-0 select-none"
              onMouseDown={handlePinnedDragStart}
            >
              <span className="text-[10px] text-muted">⚔️ Drag to move</span>
              <button
                onClick={() => setPinnedConflict(null)}
                className="rounded p-0.5 text-muted hover:text-zinc-100 transition-colors"
              >
                ✕
              </button>
            </div>
            {/* Scrollable content */}
            <div className="overflow-y-auto p-3">
              <ConflictTooltipContent
                conflict={pinnedConflictData}
                linkedDeployments={pinnedConflictAssets.deployments}
                linkedFlights={pinnedConflictAssets.flights}
                linkedDarkVessels={pinnedConflictAssets.darkVessels}
                linkedDarkFlights={pinnedConflictAssets.darkFlights}
              />
            </div>
          </div>
        )}
      </Panel>

      {/* Nation summary grid */}
      <Panel
        title="Nation Rankings"
        actions={
          <select
            value={sortKey}
            onChange={(e) => setSortKey(e.target.value as SortKey)}
            className="rounded border border-border-dim bg-surface-overlay px-2 py-0.5 text-[10px] text-zinc-100 focus:border-accent focus:outline-none"
          >
            <option value="composite_risk">Composite Risk</option>
            <option value="economic_stability">Economic</option>
            <option value="political_stability">Political</option>
            <option value="opportunity_score">Opportunity</option>
            <option value="nation">Name</option>
          </select>
        }
      >
        {sorted.length > 0 ? (
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5">
            {sorted.map((n) => (
              <NationCard
                key={n.nation}
                nation={n}
                isSelected={n.nation === selectedNation}
                isExpanded={n.nation === expandedCard}
                onClick={handleCardClick}
                onNavigate={(iso3) => navigate(`/nation?n=${iso3}`)}
                onNavigatePerson={(id, nat) => navigate(`/person?id=${id}&nation=${nat}`)}
              />
            ))}
          </div>
        ) : (
          <div className="py-6 text-center text-xs text-muted">No data</div>
        )}
      </Panel>
    </div>
  );
}


// ── Nation card sub-component (expandable) ────────────────────────

function NationCard({
  nation,
  isSelected,
  isExpanded,
  onClick,
  onNavigate,
  onNavigatePerson,
}: {
  nation: MapNation;
  isSelected: boolean;
  isExpanded: boolean;
  onClick: (n: string) => void;
  onNavigate: (n: string) => void;
  onNavigatePerson: (profileId: string, nation: string) => void;
}) {
  const flag = NATION_FLAGS[nation.nation] ?? "";
  const name = NATION_NAMES[nation.nation] ?? nation.nation;

  return (
    <div
      className={`cursor-pointer rounded border p-2.5 transition-all ${
        isSelected
          ? "border-accent bg-surface-overlay shadow-lg shadow-accent/5"
          : "border-border-dim bg-surface-overlay/50 hover:border-border-bright"
      }`}
      onClick={() => onClick(nation.nation)}
    >
      {/* Header row */}
      <div className="flex items-center justify-between mb-1.5">
        <div className="flex items-center gap-1.5 min-w-0">
          {nation.leader?.thumbnail_url ? (
            <img
              src={nation.leader.thumbnail_url}
              alt=""
              className="h-6 w-6 rounded-full object-cover border border-border-dim"
            />
          ) : (
            <span className="text-sm">{flag}</span>
          )}
          <div className="min-w-0">
            <div className="text-[11px] font-semibold text-zinc-100 truncate">{name}</div>
            <div className="text-[9px] text-muted truncate">{nation.nation}</div>
          </div>
        </div>
        <span className="text-sm font-bold tabular-nums" style={{ color: riskGrade(nation.composite_risk).color }}>
          {riskGrade(nation.composite_risk).letter}
        </span>
      </div>

      {/* Mini score bars */}
      <div className="space-y-0.5">
        <MiniBar label="Econ" value={nation.economic_stability} />
        <MiniBar label="Mkt" value={nation.market_stability} />
        <MiniBar label="Pol" value={nation.political_stability} />
      </div>

      {/* Expanded detail panel */}
      {isExpanded && (
        <div className="mt-2 pt-2 border-t border-border-dim animate-fade-in">
          <div className="space-y-0.5 mb-2">
            <MiniBar label="Curr" value={nation.currency_risk} />
            <MiniBar label="Oppty" value={nation.opportunity_score} />
            <MiniBar label="Lead" value={nation.leadership_risk} />
          </div>

          {nation.leader && (
            <div className="mb-1.5">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  if (nation.leader?.profile_id) {
                    onNavigatePerson(nation.leader.profile_id, nation.nation);
                  }
                }}
                className={`text-[10px] font-medium ${nation.leader.profile_id ? "text-accent hover:underline" : "text-muted cursor-default"}`}
              >
                {nation.leader.name}
              </button>
              <span className="text-[9px] text-muted ml-1">· {nation.leader.role.replace(/_/g, " ")}</span>
            </div>
          )}

          {nation.dependencies.length > 0 && (
            <div className="mb-2">
              <div className="text-[8px] uppercase text-muted mb-0.5">Trade links</div>
              <div className="flex flex-wrap gap-1">
                {nation.dependencies.slice(0, 4).map((d) => {
                  const flow = d.flow ?? "balanced";
                  const arrow = flow === "export" ? "→" : flow === "import" ? "←" : "↔";
                  const flowColor = flow === "export" ? "#22c55e" : flow === "import" ? "#3b82f6" : "#a1a1aa";
                  return (
                    <span key={d.nation} className="rounded bg-surface-raised px-1 py-0.5 text-[8px]">
                      <span style={{ color: flowColor }}>{arrow}</span>{" "}
                      {NATION_FLAGS[d.nation] ?? ""} {d.nation}
                    </span>
                  );
                })}
              </div>
            </div>
          )}

          <button
            onClick={(e) => {
              e.stopPropagation();
              onNavigate(nation.nation);
            }}
            className="w-full rounded bg-accent/10 border border-accent/30 px-2 py-1 text-[10px] font-semibold text-accent hover:bg-accent/20 transition-colors"
          >
            View Full Profile →
          </button>
        </div>
      )}

      {/* Dependency count (collapsed only) */}
      {!isExpanded && nation.dependencies.length > 0 && (
        <div className="mt-1.5 text-[9px] text-muted">
          {nation.dependencies.length} contagion link{nation.dependencies.length !== 1 ? "s" : ""}
        </div>
      )}
    </div>
  );
}


function MiniBar({ label, value }: { label: string; value: number }) {
  const g = riskGrade(value);

  return (
    <div className="flex items-center gap-1">
      <span className="w-7 text-[8px] text-muted">{label}</span>
      <div className="flex-1 h-1 rounded-full bg-surface">
        <div
          className="h-full rounded-full"
          style={{ width: `${value * 100}%`, backgroundColor: g.color }}
        />
      </div>
      <span className="w-3 text-[7px] font-bold" style={{ color: g.color }}>{g.letter}</span>
    </div>
  );
}
