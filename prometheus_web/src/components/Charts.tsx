import type { Dispatch, SetStateAction } from "react";
import {
  ResponsiveContainer,
  LineChart as RLineChart,
  Line,
  BarChart as RBarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Brush,
  ReferenceArea,
} from "recharts";

// ── Shared zoom utilities ────────────────────────────────

export const ZOOM_STEPS = [14, 30, 90, 180, 365, 730, 1826] as const;
export const ZOOM_LABELS: Record<number, string> = {
  14: "2w", 30: "1m", 90: "3m", 180: "6m", 365: "1y", 730: "2y", 1826: "5y",
};

/** Format a date string (YYYY-MM-DD) as "Sep 5 '25" */
export function fmtDateTick(v: string): string {
  const d = new Date(v + "T00:00:00");
  if (isNaN(d.getTime())) return v;
  const mon = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][d.getMonth()];
  return `${mon} ${d.getDate()} '${String(d.getFullYear()).slice(2)}`;
}

/** Reusable zoom-preset toolbar (2w … 5y, +/−) */
export function ChartZoomBar({
  zoomIdx,
  setZoomIdx,
}: {
  zoomIdx: number;
  setZoomIdx: Dispatch<SetStateAction<number>>;
}) {
  return (
    <div className="flex items-center gap-1.5">
      {ZOOM_STEPS.map((d, i) => (
        <button
          key={d}
          onClick={() => setZoomIdx(i)}
          className={`rounded px-2 py-0.5 text-[10px] font-medium transition-colors ${
            zoomIdx === i
              ? "bg-accent text-black"
              : "bg-surface-raised text-muted hover:text-zinc-100"
          }`}
        >
          {ZOOM_LABELS[d] ?? `${d}d`}
        </button>
      ))}
      <span className="mx-1 text-border-dim">|</span>
      <button
        onClick={() => setZoomIdx((i) => Math.max(0, i - 1))}
        disabled={zoomIdx === 0}
        className="rounded bg-surface-raised px-2 py-0.5 text-xs font-bold text-muted hover:text-zinc-100 disabled:opacity-30"
        title="Zoom in"
      >
        +
      </button>
      <button
        onClick={() => setZoomIdx((i) => Math.min(ZOOM_STEPS.length - 1, i + 1))}
        disabled={zoomIdx === ZOOM_STEPS.length - 1}
        className="rounded bg-surface-raised px-2 py-0.5 text-xs font-bold text-muted hover:text-zinc-100 disabled:opacity-30"
        title="Zoom out"
      >
        −
      </button>
    </div>
  );
}

// ── Chart palette ────────────────────────────────────────

const COLORS = [
  "#facc15",
  "#3b82f6",
  "#22c55e",
  "#ef4444",
  "#a855f7",
  "#ec4899",
  "#14b8a6",
  "#f97316",
];

interface ChartProps {
  data: Record<string, unknown>[];
  xKey: string;
  yKeys: string[];
  height?: number;
  labels?: Record<string, string>;
  zoomable?: boolean;
  xTickFormatter?: (value: string) => string;
}

export function LineChart({
  data,
  xKey,
  yKeys,
  height = 300,
  labels,
  zoomable = false,
  xTickFormatter,
}: ChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RLineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#3f3f46" />
        <XAxis
          dataKey={xKey}
          tick={{ fill: "#a1a1aa", fontSize: 11 }}
          stroke="#3f3f46"
          tickFormatter={xTickFormatter}
        />
        <YAxis
          tick={{ fill: "#a1a1aa", fontSize: 11 }}
          stroke="#3f3f46"
          domain={["auto", "auto"]}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: "#18181b",
            border: "1px solid #3f3f46",
            borderRadius: 6,
            fontSize: 12,
          }}
          labelStyle={{ color: "#a1a1aa" }}
        />
        {yKeys.length > 1 && (
          <Legend
            wrapperStyle={{ fontSize: 11 }}
            formatter={(v: string) => labels?.[v] ?? v}
          />
        )}
        {yKeys.map((key, i) => (
          <Line
            key={key}
            type="monotone"
            dataKey={key}
            name={labels?.[key] ?? key}
            stroke={COLORS[i % COLORS.length]}
            strokeWidth={1.5}
            dot={false}
            animationDuration={300}
          />
        ))}
        {data.length > 1 && (
          <Brush
            dataKey={xKey}
            height={24}
            stroke="#facc15"
            fill="#18181b"
            travellerWidth={10}
            tickFormatter={xTickFormatter}
          />
        )}
      </RLineChart>
    </ResponsiveContainer>
  );
}

export function BarChart({
  data,
  xKey,
  yKeys,
  height = 300,
  labels,
}: ChartProps) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <RBarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#3f3f46" />
        <XAxis
          dataKey={xKey}
          tick={{ fill: "#a1a1aa", fontSize: 11 }}
          stroke="#3f3f46"
        />
        <YAxis tick={{ fill: "#a1a1aa", fontSize: 11 }} stroke="#3f3f46" />
        <Tooltip
          contentStyle={{
            backgroundColor: "#18181b",
            border: "1px solid #3f3f46",
            borderRadius: 6,
            fontSize: 12,
          }}
          labelStyle={{ color: "#a1a1aa" }}
        />
        {yKeys.length > 1 && (
          <Legend
            wrapperStyle={{ fontSize: 11 }}
            formatter={(v: string) => labels?.[v] ?? v}
          />
        )}
        {yKeys.map((key, i) => (
          <Bar
            key={key}
            dataKey={key}
            name={labels?.[key] ?? key}
            fill={COLORS[i % COLORS.length]}
            radius={[2, 2, 0, 0]}
            animationDuration={300}
          />
        ))}
      </RBarChart>
    </ResponsiveContainer>
  );
}
