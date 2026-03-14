import { Info } from "lucide-react";
import { useState } from "react";

interface KpiCardProps {
  label: string;
  value: string | number;
  delta?: string | number | null;
  sentiment?: "positive" | "negative" | "neutral" | "warning";
  unit?: string;
  tooltip?: string;
}

const sentimentColor = {
  positive: "text-positive",
  negative: "text-negative",
  neutral: "text-zinc-100",
  warning: "text-warning",
};

export function KpiCard({
  label,
  value,
  delta,
  sentiment = "neutral",
  unit = "",
  tooltip,
}: KpiCardProps) {
  const [showTip, setShowTip] = useState(false);

  return (
    <div className="relative rounded-lg border border-border-dim bg-surface-raised p-3">
      <div className="flex items-center gap-1 text-[10px] uppercase tracking-wider text-muted">
        {label}
        {tooltip && (
          <span
            className="relative cursor-help"
            onMouseEnter={() => setShowTip(true)}
            onMouseLeave={() => setShowTip(false)}
          >
            <Info size={10} className="text-muted/60 hover:text-zinc-300 transition-colors" />
            {showTip && (
              <span className="absolute top-full left-1/2 -translate-x-1/2 mt-1.5 z-50 w-56 rounded-md border border-border-dim bg-zinc-900 px-3 py-2 text-[11px] normal-case tracking-normal leading-relaxed text-zinc-300 shadow-lg">
                {tooltip}
              </span>
            )}
          </span>
        )}
      </div>
      <div className={`mt-1 text-lg font-semibold ${sentimentColor[sentiment]}`}>
        {value}
        {unit && <span className="ml-1 text-xs text-muted">{unit}</span>}
      </div>
      {delta != null && (
        <div
          className={`mt-0.5 text-xs ${
            Number(delta) > 0
              ? "text-positive"
              : Number(delta) < 0
                ? "text-negative"
                : "text-muted"
          }`}
        >
          {Number(delta) > 0 ? "+" : ""}
          {delta}
        </div>
      )}
    </div>
  );
}
