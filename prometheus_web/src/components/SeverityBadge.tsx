interface SeverityBadgeProps {
  severity: string;
}

const styles: Record<string, string> = {
  critical: "bg-red-500/20 text-red-400 border-red-500/40",
  high: "bg-orange-500/20 text-orange-400 border-orange-500/40",
  medium: "bg-yellow-500/20 text-yellow-400 border-yellow-500/40",
  low: "bg-blue-500/20 text-blue-400 border-blue-500/40",
  info: "bg-zinc-700/50 text-zinc-400 border-zinc-600/40",
};

export function SeverityBadge({ severity }: SeverityBadgeProps) {
  const s = severity?.toLowerCase() ?? "info";
  return (
    <span
      className={`inline-flex items-center rounded border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider ${styles[s] ?? styles.info}`}
    >
      {severity}
    </span>
  );
}
