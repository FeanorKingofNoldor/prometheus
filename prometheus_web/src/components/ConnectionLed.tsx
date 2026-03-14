interface Endpoint {
  label: string;
  port: number;
  reachable: boolean;
  latency_ms?: number;
  error?: string;
}

interface ConnectionLedProps {
  /** "connected" | "degraded" | "disconnected" | "loading" */
  status: string;
  label?: string;
  endpoints?: Endpoint[];
}

const LED_STYLES: Record<string, { bg: string; ring: string; pulse: boolean }> = {
  connected:    { bg: "bg-green-500",  ring: "ring-green-500/30",  pulse: false },
  degraded:     { bg: "bg-yellow-400", ring: "ring-yellow-400/30", pulse: true },
  disconnected: { bg: "bg-red-500",    ring: "ring-red-500/30",    pulse: false },
  loading:      { bg: "bg-zinc-500",   ring: "ring-zinc-500/30",   pulse: true },
};

export function ConnectionLed({ status, label, endpoints }: ConnectionLedProps) {
  const s = LED_STYLES[status] ?? LED_STYLES.loading;

  const tooltipLines: string[] = [];
  if (label) tooltipLines.push(label);
  tooltipLines.push(`Status: ${status.toUpperCase()}`);
  if (endpoints?.length) {
    for (const ep of endpoints) {
      const state = ep.reachable ? `✓ ${ep.latency_ms?.toFixed(0) ?? "?"}ms` : `✗ ${ep.error ?? "unreachable"}`;
      tooltipLines.push(`  ${ep.label} :${ep.port} — ${state}`);
    }
  }

  return (
    <span
      className="inline-flex items-center gap-1.5 cursor-default"
      title={tooltipLines.join("\n")}
    >
      <span className="relative flex h-2.5 w-2.5">
        {s.pulse && (
          <span
            className={`absolute inset-0 rounded-full ${s.bg} opacity-75 animate-ping`}
          />
        )}
        <span
          className={`relative inline-flex h-2.5 w-2.5 rounded-full ${s.bg} ring-2 ${s.ring}`}
        />
      </span>
      {label && (
        <span className="text-[10px] font-medium text-muted uppercase tracking-wider">
          {label}
        </span>
      )}
    </span>
  );
}
