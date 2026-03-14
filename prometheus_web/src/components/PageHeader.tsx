import { RefreshCw } from "lucide-react";

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  onRefresh?: () => void;
  actions?: React.ReactNode;
}

export function PageHeader({
  title,
  subtitle,
  onRefresh,
  actions,
}: PageHeaderProps) {
  return (
    <div className="mb-4 flex items-center justify-between">
      <div>
        <h1 className="text-lg font-semibold text-zinc-100">{title}</h1>
        {subtitle && (
          <p className="mt-0.5 text-xs text-muted">{subtitle}</p>
        )}
      </div>
      <div className="flex items-center gap-2">
        {actions}
        {onRefresh && (
          <button
            onClick={onRefresh}
            className="rounded p-1.5 text-muted transition-colors hover:bg-surface-overlay hover:text-zinc-100"
            title="Refresh"
          >
            <RefreshCw size={14} />
          </button>
        )}
      </div>
    </div>
  );
}
