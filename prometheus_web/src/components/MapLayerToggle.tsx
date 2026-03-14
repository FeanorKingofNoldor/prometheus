import { useState, useRef, useEffect, useCallback } from "react";
import { Globe, Route, Gem, Swords, Ship, Plane, Anchor, ChevronDown } from "lucide-react";

export type MapLayer = "nations" | "routes" | "resources" | "conflicts" | "vessels" | "flights" | "deployments";

export interface LayerItem {
  id: string;
  label: string;
  color?: string;
}

const LAYERS: { id: MapLayer; label: string; icon: typeof Globe; color: string }[] = [
  { id: "nations",      label: "Nations",      icon: Globe,   color: "#3b82f6" },
  { id: "routes",       label: "Trade Routes", icon: Route,   color: "#06b6d4" },
  { id: "resources",    label: "Resources",    icon: Gem,     color: "#22c55e" },
  { id: "conflicts",    label: "Conflicts",    icon: Swords,  color: "#ef4444" },
  { id: "deployments",  label: "Deployments",  icon: Anchor,  color: "#f59e0b" },
  { id: "vessels",      label: "Vessels",      icon: Ship,    color: "#0ea5e9" },
  { id: "flights",      label: "Flights",      icon: Plane,   color: "#d946ef" },
];

interface MapLayerToggleProps {
  active: Set<MapLayer>;
  onToggle: (layer: MapLayer) => void;
  layerItems?: Partial<Record<MapLayer, LayerItem[]>>;
  selectedItems?: Partial<Record<MapLayer, string | null>>;
  onSelectItem?: (layer: MapLayer, itemId: string | null) => void;
}

export function MapLayerToggle({ active, onToggle, layerItems, selectedItems, onSelectItem }: MapLayerToggleProps) {
  const [openLayer, setOpenLayer] = useState<MapLayer | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const containerRef = useRef<HTMLDivElement>(null);

  // Reset search when dropdown changes
  useEffect(() => { setSearchTerm(""); }, [openLayer]);

  // Close dropdown on outside click
  useEffect(() => {
    if (!openLayer) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpenLayer(null);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [openLayer]);

  const handlePillClick = useCallback((id: MapLayer) => {
    if (!active.has(id)) {
      onToggle(id);
      setOpenLayer(null);
    } else {
      const items = layerItems?.[id];
      if (items && items.length > 0) {
        setOpenLayer((prev) => (prev === id ? null : id));
      } else {
        onToggle(id);
      }
    }
  }, [active, onToggle, layerItems]);

  const handleSelect = useCallback((layer: MapLayer, itemId: string | null) => {
    onSelectItem?.(layer, itemId);
    setOpenLayer(null);
  }, [onSelectItem]);

  const handleHide = useCallback((layer: MapLayer) => {
    onToggle(layer);
    onSelectItem?.(layer, null);
    setOpenLayer(null);
  }, [onToggle, onSelectItem]);

  return (
    <div ref={containerRef} className="flex flex-wrap gap-1.5">
      {LAYERS.map(({ id, label, icon: Icon, color }) => {
        const isActive = active.has(id);
        const isOpen = openLayer === id;
        const items = layerItems?.[id] ?? [];
        const selId = selectedItems?.[id] ?? null;
        const selLabel = selId ? items.find((i) => i.id === selId)?.label : null;

        // Filter items by search term
        const filtered = searchTerm
          ? items.filter((i) => i.label.toLowerCase().includes(searchTerm.toLowerCase()))
          : items;

        return (
          <div key={id} className="relative">
            <button
              onClick={() => handlePillClick(id)}
              className={`flex items-center gap-1.5 rounded-full px-3 py-1 text-[11px] font-medium transition-all
                ${isActive
                  ? "border border-transparent text-zinc-100 shadow-sm"
                  : "border border-border-dim text-muted hover:border-border-bright hover:text-zinc-300"
                }
              `}
              style={isActive ? { backgroundColor: `${color}22`, borderColor: `${color}66`, color } : {}}
              title={isActive ? `Filter ${label}` : `Show ${label}`}
            >
              <Icon size={13} />
              {selLabel ? (
                <span className="max-w-[100px] truncate">{selLabel}</span>
              ) : (
                label
              )}
              {isActive && items.length > 0 && (
                <ChevronDown size={10} className={`ml-0.5 transition-transform ${isOpen ? "rotate-180" : ""}`} />
              )}
            </button>

            {/* Dropdown */}
            {isOpen && items.length > 0 && (
              <div className="absolute top-full left-0 mt-1 z-50 w-60 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl overflow-hidden animate-fade-in">
                {/* Search input for large lists */}
                {items.length > 15 && (
                  <div className="px-2 py-1.5 border-b border-border-dim">
                    <input
                      type="text"
                      placeholder={`Search ${label}…`}
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      className="w-full rounded border border-border-dim bg-surface-overlay px-2 py-1 text-[11px] text-zinc-100 placeholder:text-muted focus:border-accent focus:outline-none"
                      autoFocus
                      onClick={(e) => e.stopPropagation()}
                    />
                  </div>
                )}
                <div className="max-h-64 overflow-y-auto py-1">
                  {/* Hide layer option — top */}
                  <button
                    onClick={() => handleHide(id)}
                    className="w-full text-left px-3 py-1.5 text-[11px] text-muted hover:text-negative hover:bg-surface-overlay transition-colors"
                  >
                    ✕ Hide {label}
                  </button>
                  <div className="border-t border-border-dim my-0.5" />
                  {/* Show all option */}
                  <button
                    onClick={() => handleSelect(id, null)}
                    className={`w-full text-left px-3 py-1.5 text-[11px] hover:bg-surface-overlay transition-colors
                      ${!selId ? "text-zinc-100 font-medium" : "text-muted"}`}
                  >
                    All {label}
                  </button>
                  <div className="border-t border-border-dim my-0.5" />
                  {/* Entity items */}
                  {filtered.map((item) => (
                    <button
                      key={item.id}
                      onClick={() => handleSelect(id, item.id)}
                      className={`w-full text-left px-3 py-1.5 text-[11px] hover:bg-surface-overlay transition-colors flex items-center gap-2
                        ${item.id === selId ? "text-zinc-100 font-medium" : "text-zinc-400"}`}
                    >
                      {item.color && (
                        <span className="h-1.5 w-1.5 rounded-full shrink-0" style={{ backgroundColor: item.color }} />
                      )}
                      <span className="truncate">{item.label}</span>
                    </button>
                  ))}
                  {filtered.length === 0 && (
                    <div className="px-3 py-2 text-[10px] text-muted">No matches</div>
                  )}
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
