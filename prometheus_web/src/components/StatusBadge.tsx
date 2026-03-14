interface StatusBadgeProps {
  label: string;
  variant?: "positive" | "negative" | "warning" | "info" | "neutral";
}

const styles = {
  positive: "bg-positive/15 text-positive border-positive/30",
  negative: "bg-negative/15 text-negative border-negative/30",
  warning: "bg-warning/15 text-warning border-warning/30",
  info: "bg-info/15 text-info border-info/30",
  neutral: "bg-zinc-800 text-zinc-400 border-zinc-700",
};

export function StatusBadge({ label, variant = "neutral" }: StatusBadgeProps) {
  return (
    <span
      className={`inline-flex items-center rounded border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${styles[variant]}`}
    >
      {label}
    </span>
  );
}
