import { useEffect } from "react";
import { Loader2, CheckCircle2, XCircle } from "lucide-react";
import { useIntelJob } from "../api/hooks";
import { useQueryClient } from "@tanstack/react-query";

interface SitrepProgressProps {
  jobId: string | null;
  onDone?: () => void;
}

export function SitrepProgress({ jobId, onDone }: SitrepProgressProps) {
  const job = useIntelJob(jobId);
  const qc = useQueryClient();
  const data = (job.data ?? {}) as Record<string, unknown>;

  const status = data.status as string | undefined;
  const step = data.step_label as string | undefined;
  const stepIdx = (data.step_index as number) ?? 0;
  const total = (data.total_steps as number) ?? 7;
  const pct = total > 0 ? Math.round((stepIdx / total) * 100) : 0;
  const elapsed = data.elapsed as number | undefined;
  const error = data.error as string | undefined;

  const jobType = data.type as string | undefined;
  const jobLabel = jobType === "weekly" ? "Weekly Assessment" : "SITREP";

  const isDone = status === "done";
  const isError = status === "error";
  // Job expired (server restarted and in-memory jobs were lost)
  const isExpired = job.isError;

  useEffect(() => {
    if (isDone) {
      // Refresh brief list + unread count
      qc.invalidateQueries({ queryKey: ["intel"] });
      onDone?.();
    }
  }, [isDone, qc, onDone]);

  useEffect(() => {
    if (isExpired) {
      // Server lost the job (restart) — clear stale state
      onDone?.();
    }
  }, [isExpired, onDone]);

  if (!jobId) return null;

  return (
    <div className="rounded-lg border border-border-dim bg-surface-raised px-4 py-3">
      <div className="flex items-center gap-3">
        {isDone ? (
          <CheckCircle2 size={16} className="text-positive shrink-0" />
        ) : isError ? (
          <XCircle size={16} className="text-negative shrink-0" />
        ) : (
          <Loader2 size={16} className="text-accent shrink-0 animate-spin" />
        )}

        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between">
            <span className="text-xs font-medium text-zinc-100">
              {isDone ? `${jobLabel} Complete` : isError ? "Generation Failed" : `Generating ${jobLabel}…`}
            </span>
            <span className="text-[10px] text-muted">
              {isDone && elapsed ? `${elapsed}s` : `${pct}%`}
            </span>
          </div>

          {/* Progress bar */}
          <div className="mt-1.5 h-1.5 w-full rounded-full bg-surface-overlay overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-500 ${
                isDone ? "bg-positive" : isError ? "bg-negative" : "bg-accent"
              }`}
              style={{ width: `${isDone ? 100 : pct}%` }}
            />
          </div>

          {/* Step label */}
          <p className="mt-1 text-[10px] text-muted truncate">
            {isError ? error : step}
          </p>
        </div>
      </div>
    </div>
  );
}
