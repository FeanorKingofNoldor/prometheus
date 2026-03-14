import { useEffect, useState } from "react";
import { AlertTriangle } from "lucide-react";
import type { IntelBrief } from "./BriefCard";

interface FlashAlertTickerProps {
  alerts: IntelBrief[];
  onSelect?: (id: string) => void;
}

const severityBg: Record<string, string> = {
  critical: "bg-red-500/15 border-red-500/40 text-red-300",
  high: "bg-orange-500/15 border-orange-500/40 text-orange-300",
  medium: "bg-yellow-500/15 border-yellow-500/40 text-yellow-300",
  low: "bg-blue-500/15 border-blue-500/40 text-blue-300",
  info: "bg-zinc-700/50 border-zinc-600/40 text-zinc-300",
};

export function FlashAlertTicker({ alerts, onSelect }: FlashAlertTickerProps) {
  const [idx, setIdx] = useState(0);

  useEffect(() => {
    if (alerts.length <= 1) return;
    const iv = setInterval(() => setIdx((i) => (i + 1) % alerts.length), 6000);
    return () => clearInterval(iv);
  }, [alerts.length]);

  if (!alerts.length) return null;

  const current = alerts[idx % alerts.length];
  if (!current) return null;

  const cls = severityBg[current.severity?.toLowerCase()] ?? severityBg.info;

  return (
    <button
      className={`flex w-full items-center gap-3 rounded-lg border px-4 py-2.5 text-left transition-all ${cls}`}
      onClick={() => onSelect?.(current.id)}
    >
      <AlertTriangle size={16} className="shrink-0 animate-pulse" />

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-bold uppercase tracking-wider opacity-70">
            FLASH ALERT
          </span>
          {alerts.length > 1 && (
            <span className="text-[10px] opacity-50">
              {idx + 1}/{alerts.length}
            </span>
          )}
        </div>
        <p className="truncate text-xs font-medium">{current.title}</p>
      </div>

      <span className="shrink-0 text-[10px] opacity-60">
        {current.domain?.toUpperCase()}
      </span>
    </button>
  );
}
