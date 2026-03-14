import { useSearchParams, useNavigate } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { StatusBadge } from "../components/StatusBadge";
import { useChokepoints, usePorts, useTradeRoutes } from "../api/hooks";
import type { ChokepointData } from "../components/ChokepointMarkers";
import type { PortData } from "../components/PortMarkers";
import type { TradeRouteData } from "../components/TradeRouteLines";
import { NATION_FLAGS, NATION_NAMES } from "../data/countryMapping";

// ── helpers ──────────────────────────────────────────────

const CP_STATUS_VARIANT: Record<string, "positive" | "warning" | "negative"> = {
  OPEN: "positive", THREATENED: "warning", DISRUPTED: "negative", CLOSED: "negative",
};
const PORT_STATUS_VARIANT: Record<string, "positive" | "warning" | "negative"> = {
  OPERATIONAL: "positive", CONGESTED: "warning", DISRUPTED: "negative",
};

function nationLabel(iso3: string): string {
  return `${NATION_FLAGS[iso3] ?? ""} ${NATION_NAMES[iso3] ?? iso3}`;
}

// ── component ────────────────────────────────────────────

export default function TradeProfile() {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const type = params.get("type") ?? "chokepoint"; // "chokepoint" | "port"
  const id = params.get("id") ?? "";

  const chokepointsQuery = useChokepoints();
  const portsQuery = usePorts();
  const tradeRoutesQuery = useTradeRoutes();

  const chokepoints = (chokepointsQuery.data ?? []) as ChokepointData[];
  const ports = (portsQuery.data ?? []) as PortData[];
  const tradeRoutes = (tradeRoutesQuery.data ?? []) as TradeRouteData[];

  const isLoading = chokepointsQuery.isLoading || portsQuery.isLoading || tradeRoutesQuery.isLoading;

  if (isLoading) {
    return (
      <div className="flex h-64 items-center justify-center text-xs text-muted">Loading…</div>
    );
  }

  if (type === "chokepoint") {
    const cp = chokepoints.find((c) => c.id === id);
    if (!cp) return <NotFound navigate={navigate} />;
    return <ChokepointProfile cp={cp} tradeRoutes={tradeRoutes} navigate={navigate} />;
  }

  const port = ports.find((p) => p.id === id);
  if (!port) return <NotFound navigate={navigate} />;
  return <PortProfile port={port} chokepoints={chokepoints} tradeRoutes={tradeRoutes} navigate={navigate} />;
}

// ── Not found ────────────────────────────────────────────

function NotFound({ navigate }: { navigate: ReturnType<typeof useNavigate> }) {
  return (
    <div className="space-y-4">
      <PageHeader title="Trade Entity Not Found" subtitle="The requested entity could not be found." />
      <Panel>
        <button onClick={() => navigate("/geo")} className="text-accent text-sm hover:underline">
          ← Back to GeoRisk Map
        </button>
      </Panel>
    </div>
  );
}

// ── Chokepoint profile ───────────────────────────────────

function ChokepointProfile({
  cp,
  tradeRoutes,
  navigate,
}: {
  cp: ChokepointData;
  tradeRoutes: TradeRouteData[];
  navigate: ReturnType<typeof useNavigate>;
}) {
  const relatedRoutes = tradeRoutes.filter(
    (r) => r.waypoints?.some((w) => Math.abs(w[0] - cp.coordinates[0]) < 5 && Math.abs(w[1] - cp.coordinates[1]) < 5)
  );

  return (
    <div className="space-y-4">
      <PageHeader
        title={`⚓ ${cp.name}`}
        subtitle={`${cp.category === "supply_chain" ? "Supply Chokepoint" : "Maritime Chokepoint"} · ${cp.id}`}
      />

      {/* Key metrics */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <MetricCard label="Status" value={cp.status} badge={CP_STATUS_VARIANT[cp.status]} />
        <MetricCard label="Daily Volume" value={cp.daily_volume} />
        <MetricCard label="World Share" value={cp.world_share} />
        <MetricCard label="Category" value={cp.category.replace(/_/g, " ")} />
      </div>

      {/* Description & market impact */}
      <div className="grid gap-3 lg:grid-cols-2">
        <Panel title="Description">
          <p className="text-xs text-zinc-300 leading-relaxed">{cp.description}</p>
        </Panel>
        <Panel title="Market Impact">
          <p className="text-xs text-zinc-300 leading-relaxed">{cp.market_impact}</p>
        </Panel>
      </div>

      {/* Commodities */}
      {cp.commodities.length > 0 && (
        <Panel title={`Commodities (${cp.commodities.length})`}>
          <div className="flex flex-wrap gap-1.5">
            {cp.commodities.map((c) => (
              <span key={c} className="rounded bg-surface-overlay px-2 py-1 text-[10px] text-zinc-200 border border-border-dim">
                {c}
              </span>
            ))}
          </div>
        </Panel>
      )}

      {/* Nations */}
      <div className="grid gap-3 lg:grid-cols-2">
        {cp.controlling_nations.length > 0 && (
          <Panel title={`Controlling Nations (${cp.controlling_nations.length})`}>
            <div className="space-y-1">
              {cp.controlling_nations.map((n) => (
                <button
                  key={n}
                  onClick={() => navigate(`/nation?n=${n}`)}
                  className="flex w-full items-center gap-2 rounded bg-surface-overlay px-3 py-1.5 text-xs text-zinc-200 hover:bg-surface-raised transition-colors"
                >
                  {nationLabel(n)}
                </button>
              ))}
            </div>
          </Panel>
        )}
        {cp.affected_nations.length > 0 && (
          <Panel title={`Affected Nations (${cp.affected_nations.length})`}>
            <div className="space-y-1">
              {cp.affected_nations.map((n) => (
                <button
                  key={n}
                  onClick={() => navigate(`/nation?n=${n}`)}
                  className="flex w-full items-center gap-2 rounded bg-surface-overlay px-3 py-1.5 text-xs text-zinc-200 hover:bg-surface-raised transition-colors"
                >
                  {nationLabel(n)}
                </button>
              ))}
            </div>
          </Panel>
        )}
      </div>

      {/* Related trade routes */}
      {relatedRoutes.length > 0 && (
        <Panel title={`Related Trade Routes (${relatedRoutes.length})`}>
          <div className="space-y-1.5">
            {relatedRoutes.map((r) => (
              <div key={r.id} className="flex items-center justify-between rounded bg-surface-overlay px-3 py-2">
                <div>
                  <div className="text-xs font-medium text-zinc-200">🛣️ {r.name}</div>
                  <div className="text-[10px] text-muted">{r.category} · {r.volume}</div>
                </div>
              </div>
            ))}
          </div>
        </Panel>
      )}

      <BackLink navigate={navigate} />
    </div>
  );
}

// ── Port profile ─────────────────────────────────────────

function PortProfile({
  port,
  chokepoints,
  tradeRoutes,
  navigate,
}: {
  port: PortData;
  chokepoints: ChokepointData[];
  tradeRoutes: TradeRouteData[];
  navigate: ReturnType<typeof useNavigate>;
}) {
  const emoji = port.port_type === "seaport" ? "🚢" : "✈️";
  const typeLabel = port.port_type === "seaport" ? "Seaport" : "Cargo Airport";

  const connectedCps = chokepoints.filter((c) => port.connected_chokepoints.includes(c.id));
  const connectedRts = tradeRoutes.filter((r) => port.connected_routes.includes(r.id));

  return (
    <div className="space-y-4">
      <PageHeader
        title={`${emoji} ${port.name}`}
        subtitle={`${typeLabel} · ${port.iata_or_locode} · ${nationLabel(port.nation)}`}
      />

      {/* Key metrics */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <MetricCard label="Status" value={port.status} badge={PORT_STATUS_VARIANT[port.status]} />
        <MetricCard label="Annual Volume" value={`${port.annual_volume} ${port.volume_unit}`} />
        <MetricCard label="Type" value={typeLabel} />
        <MetricCard label="Nation" value={nationLabel(port.nation)} />
      </div>

      {/* Description */}
      <Panel title="Description">
        <p className="text-xs text-zinc-300 leading-relaxed">{port.description}</p>
      </Panel>

      {/* Key commodities */}
      {port.key_commodities.length > 0 && (
        <Panel title={`Key Commodities (${port.key_commodities.length})`}>
          <div className="flex flex-wrap gap-1.5">
            {port.key_commodities.map((c) => (
              <span key={c} className="rounded bg-surface-overlay px-2 py-1 text-[10px] text-zinc-200 border border-border-dim">
                {c}
              </span>
            ))}
          </div>
        </Panel>
      )}

      {/* Connected chokepoints */}
      {connectedCps.length > 0 && (
        <Panel title={`Connected Chokepoints (${connectedCps.length})`}>
          <div className="space-y-1.5">
            {connectedCps.map((c) => (
              <button
                key={c.id}
                onClick={() => navigate(`/trade?type=chokepoint&id=${c.id}`)}
                className="flex w-full items-center justify-between rounded bg-surface-overlay px-3 py-2 hover:bg-surface-raised transition-colors"
              >
                <div>
                  <div className="text-xs font-medium text-zinc-200">⚓ {c.name}</div>
                  <div className="text-[10px] text-muted">{c.daily_volume} · {c.world_share} world share</div>
                </div>
                <StatusBadge label={c.status} variant={CP_STATUS_VARIANT[c.status] ?? "neutral"} />
              </button>
            ))}
          </div>
        </Panel>
      )}

      {/* Connected trade routes */}
      {connectedRts.length > 0 && (
        <Panel title={`Connected Trade Routes (${connectedRts.length})`}>
          <div className="space-y-1.5">
            {connectedRts.map((r) => (
              <div key={r.id} className="flex items-center justify-between rounded bg-surface-overlay px-3 py-2">
                <div>
                  <div className="text-xs font-medium text-zinc-200">🛣️ {r.name}</div>
                  <div className="text-[10px] text-muted">{r.category} · {r.volume}</div>
                </div>
              </div>
            ))}
          </div>
        </Panel>
      )}

      <BackLink navigate={navigate} />
    </div>
  );
}

// ── Shared sub-components ────────────────────────────────

function MetricCard({ label, value, badge }: { label: string; value: string; badge?: "positive" | "warning" | "negative" }) {
  return (
    <div className="rounded-lg border border-border-dim bg-surface-overlay p-3">
      <div className="text-[10px] text-muted mb-1">{label}</div>
      {badge ? (
        <StatusBadge label={value} variant={badge} />
      ) : (
        <div className="text-sm font-semibold text-zinc-100">{value}</div>
      )}
    </div>
  );
}

function BackLink({ navigate }: { navigate: ReturnType<typeof useNavigate> }) {
  return (
    <div className="pt-2">
      <button onClick={() => navigate("/geo")} className="text-accent text-xs hover:underline">
        ← Back to GeoRisk Map
      </button>
    </div>
  );
}
