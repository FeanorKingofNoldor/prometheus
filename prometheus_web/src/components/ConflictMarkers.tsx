import { memo } from "react";
import { Marker, Line } from "react-simple-maps";
import { COUNTRY_CENTROIDS, NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";

// ── Types ────────────────────────────────────────────────

export interface ConflictPartyData {
  name: string;
  nations: string[];
  role: string;   // belligerent | ally | proxy | supporter
  description: string;
}

export interface ConflictData {
  id: string;
  name: string;
  conflict_type: string;
  status: "ACTIVE" | "CEASEFIRE" | "FROZEN" | "ESCALATING";
  start_date: string;
  coordinates: [number, number];
  description: string;
  parties: ConflictPartyData[];
  affected_nations: string[];
  humanitarian: { displaced?: string; casualties_est?: string; aid_status?: string };
  economic_impact: string;
  escalation_risk: number;
}

interface ConflictMarkersProps {
  conflicts: ConflictData[];
  onHover: (id: string | null) => void;
  hoveredId: string | null;
  onClick?: (id: string) => void;
  pinnedId?: string | null;
}

// ── Status colors ────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  ACTIVE:     "#ef4444",
  ESCALATING: "#f97316",
  CEASEFIRE:  "#f59e0b",
  FROZEN:     "#6b7280",
};

const STATUS_LABELS: Record<string, string> = {
  ACTIVE:     "Active",
  ESCALATING: "Escalating",
  CEASEFIRE:  "Ceasefire",
  FROZEN:     "Frozen",
};

const TYPE_LABELS: Record<string, string> = {
  interstate_war:      "Interstate War",
  civil_war:           "Civil War",
  insurgency:          "Insurgency",
  frozen_conflict:     "Frozen Conflict",
  territorial_dispute: "Territorial Dispute",
};

// ── Component ────────────────────────────────────────────

export const ConflictMarkers = memo(function ConflictMarkers({
  conflicts,
  onHover,
  hoveredId,
  onClick,
  pinnedId,
}: ConflictMarkersProps) {
  return (
    <>
      {conflicts.map((c) => {
        const color = STATUS_COLORS[c.status] ?? "#888";
        const isHovered = c.id === hoveredId;
        const isPinned = c.id === pinnedId;
        const isActive = isHovered || isPinned;
        const isPulsing = c.status === "ACTIVE" || c.status === "ESCALATING";

        return (
          <Marker
            key={c.id}
            coordinates={c.coordinates}
            onMouseEnter={() => onHover(c.id)}
            onMouseLeave={() => onHover(null)}
            onClick={() => onClick?.(c.id)}
          >
            {/* Outer pulse ring for active/escalating */}
            {isPulsing && (
              <circle
                r={isActive ? 14 : 10}
                fill="none"
                stroke={color}
                strokeWidth={0.8}
                opacity={0.35}
                className="animate-map-pulse"
              />
            )}
            {/* Swords emoji marker */}
            <text
              textAnchor="middle"
              dominantBaseline="central"
              style={{
                fontSize: isActive ? "14px" : "10px",
                cursor: "pointer",
                transition: "font-size 0.15s ease",
                filter: `drop-shadow(0 0 3px ${color})`,
              }}
            >
              ⚔️
            </text>
            {/* Label — only on hover/pinned, with dark halo */}
            {isActive && (
              <text
                textAnchor="middle"
                y={-16}
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
                {c.name}
              </text>
            )}
          </Marker>
        );
      })}

      {/* Lines to all involved nations on hover/pinned */}
      {(() => {
        const activeIds = [...new Set([hoveredId, pinnedId].filter(Boolean) as string[])];
        return activeIds.map((id) => {
          const c = conflicts.find((x) => x.id === id);
          if (!c) return null;
          const color = STATUS_COLORS[c.status] ?? "#888";
          const allNations = new Set<string>(c.affected_nations);
          c.parties.forEach((p) => p.nations.forEach((n) => allNations.add(n)));
          return [...allNations]
            .filter((n) => COUNTRY_CENTROIDS[n])
            .map((n) => (
              <Line
                key={`${id}-${n}`}
                from={c.coordinates}
                to={COUNTRY_CENTROIDS[n]}
                stroke={color}
                strokeWidth={0.5}
                strokeLinecap="round"
                strokeDasharray="3 2"
                style={{ opacity: 0.4 }}
              />
            ));
        });
      })()}
    </>
  );
});


// ── Tooltip (rendered outside SVG) ───────────────────────

const ROLE_COLORS: Record<string, string> = {
  belligerent: "#ef4444",
  ally:        "#f59e0b",
  proxy:       "#a855f7",
  supporter:   "#3b82f6",
};

interface ConflictTooltipContentProps {
  conflict: ConflictData;
  linkedDeployments?: number;
  linkedFlights?: number;
  linkedDarkVessels?: number;
  linkedDarkFlights?: number;
}

export function ConflictTooltipContent({ conflict, linkedDeployments, linkedFlights, linkedDarkVessels, linkedDarkFlights }: ConflictTooltipContentProps) {
  const color = STATUS_COLORS[conflict.status] ?? "#888";
  const statusLabel = STATUS_LABELS[conflict.status] ?? conflict.status;
  const typeLabel = TYPE_LABELS[conflict.conflict_type] ?? conflict.conflict_type.replace(/_/g, " ");
  const escPct = `${(conflict.escalation_risk * 100).toFixed(0)}%`;
  const escColor = conflict.escalation_risk >= 0.7 ? "#ef4444" : conflict.escalation_risk >= 0.4 ? "#f59e0b" : "#22c55e";
  const hasAssets = (linkedDeployments ?? 0) + (linkedFlights ?? 0) + (linkedDarkVessels ?? 0) + (linkedDarkFlights ?? 0) > 0;

  return (
    <>
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div className="min-w-0 flex-1">
          <div className="text-xs font-semibold text-zinc-100">
            ⚔️ {conflict.name}
          </div>
          <div className="text-[10px] text-muted mt-0.5">
            {typeLabel} · since {conflict.start_date.slice(0, 4)}
          </div>
        </div>
        <span
          className="rounded-full px-2 py-0.5 text-[9px] font-bold uppercase shrink-0"
          style={{ backgroundColor: `${color}22`, color, border: `1px solid ${color}44` }}
        >
          {statusLabel}
        </span>
      </div>

      {/* Description */}
      <p className="text-[10px] text-zinc-400 mb-2 leading-relaxed">
        {conflict.description}
      </p>

      {/* Parties */}
      <div className="space-y-1.5 mb-2">
        <div className="text-[8px] uppercase text-muted">Parties & Allies</div>
        {conflict.parties.map((p, i) => {
          const rc = ROLE_COLORS[p.role] ?? "#888";
          return (
            <div key={i} className="rounded bg-surface-overlay px-2 py-1.5">
              <div className="flex items-center gap-1.5 mb-0.5">
                <span
                  className="rounded px-1.5 py-0.5 text-[7px] font-bold uppercase"
                  style={{ backgroundColor: `${rc}22`, color: rc, border: `1px solid ${rc}44` }}
                >
                  {p.role}
                </span>
                <span className="text-[10px] font-medium text-zinc-200 truncate">{p.name}</span>
              </div>
              {p.nations.length > 0 && (
                <div className="text-[9px] text-muted mb-0.5">
                  {p.nations.map((n) => `${NATION_FLAGS[n] ?? ""} ${NATION_NAMES[n] ?? n}`).join(", ")}
                </div>
              )}
              <div className="text-[9px] text-zinc-400 leading-snug">{p.description}</div>
            </div>
          );
        })}
      </div>

      {/* Humanitarian */}
      {(conflict.humanitarian.displaced || conflict.humanitarian.casualties_est) && (
        <div className="grid grid-cols-2 gap-1 mb-2">
          {conflict.humanitarian.displaced && (
            <div className="rounded bg-surface-overlay px-2 py-1">
              <div className="text-[8px] text-muted">Displaced</div>
              <div className="text-[10px] text-zinc-200">{conflict.humanitarian.displaced}</div>
            </div>
          )}
          {conflict.humanitarian.casualties_est && (
            <div className="rounded bg-surface-overlay px-2 py-1">
              <div className="text-[8px] text-muted">Casualties Est.</div>
              <div className="text-[10px] text-zinc-200">{conflict.humanitarian.casualties_est}</div>
            </div>
          )}
        </div>
      )}

      {/* Economic impact */}
      <div className="rounded bg-surface-overlay px-2 py-1.5 mb-2">
        <div className="text-[8px] text-negative font-medium mb-0.5">⚠ Economic Impact</div>
        <div className="text-[10px] text-zinc-300 leading-relaxed">{conflict.economic_impact}</div>
      </div>

      {/* Linked military assets */}
      {hasAssets && (
        <div className="rounded bg-surface-overlay px-2 py-1.5 mb-2">
          <div className="text-[8px] uppercase text-muted mb-1">🎯 Linked Military Assets</div>
          <div className="flex flex-wrap gap-x-3 gap-y-0.5">
            {(linkedDeployments ?? 0) > 0 && (
              <span className="text-[9px] text-zinc-300">⚓ <span className="font-bold text-amber-400">{linkedDeployments}</span> naval deployments</span>
            )}
            {(linkedFlights ?? 0) > 0 && (
              <span className="text-[9px] text-zinc-300">✈️ <span className="font-bold text-red-400">{linkedFlights}</span> mil. aircraft</span>
            )}
            {(linkedDarkVessels ?? 0) > 0 && (
              <span className="text-[9px] text-zinc-400">🚢 <span className="font-bold text-amber-600">{linkedDarkVessels}</span> dark vessels</span>
            )}
            {(linkedDarkFlights ?? 0) > 0 && (
              <span className="text-[9px] text-zinc-400">✈️ <span className="font-bold text-amber-600">{linkedDarkFlights}</span> dark aircraft</span>
            )}
          </div>
        </div>
      )}

      {/* Escalation risk bar */}
      <div className="flex items-center gap-2">
        <span className="text-[8px] text-muted shrink-0">Escalation Risk</span>
        <div className="flex-1 h-1.5 rounded-full bg-surface-overlay overflow-hidden">
          <div
            className="h-full rounded-full transition-all"
            style={{ width: escPct, backgroundColor: escColor }}
          />
        </div>
        <span className="text-[9px] font-bold tabular-nums" style={{ color: escColor }}>{escPct}</span>
      </div>
    </>
  );
}

export function ConflictTooltip({ conflict }: { conflict: ConflictData }) {
  return (
    <div className="animate-fade-in">
      <div className="w-80 rounded-lg border border-border-dim bg-surface-raised/95 backdrop-blur-sm shadow-xl p-3">
        <ConflictTooltipContent conflict={conflict} />
      </div>
    </div>
  );
}
