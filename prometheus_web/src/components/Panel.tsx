import { Info } from "lucide-react";
import { useState } from "react";

interface PanelProps {
  title?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  actions?: React.ReactNode;
  tooltip?: string;
}

export function Panel({ title, children, className = "", actions, tooltip }: PanelProps) {
  const [showTip, setShowTip] = useState(false);

  return (
    <div
      className={`rounded-lg border border-border-dim bg-surface-raised ${className}`}
    >
      {title && (
        <div className="flex items-center justify-between border-b border-border-dim px-4 py-2">
          <div className="flex items-center gap-1.5">
            <h2 className="text-xs font-semibold uppercase tracking-wider text-muted">
              {title}
            </h2>
            {tooltip && (
              <span
                className="relative cursor-help"
                onMouseEnter={() => setShowTip(true)}
                onMouseLeave={() => setShowTip(false)}
              >
                <Info size={11} className="text-muted/60 hover:text-zinc-300 transition-colors" />
                {showTip && (
                  <span className="absolute top-full left-1/2 -translate-x-1/2 mt-1.5 z-50 w-64 rounded-md border border-border-dim bg-zinc-900 px-3 py-2 text-[11px] normal-case tracking-normal leading-relaxed text-zinc-300 shadow-lg">
                    {tooltip}
                  </span>
                )}
              </span>
            )}
          </div>
          {actions}
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}
