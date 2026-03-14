import { useCallback, useState } from "react";
import { PageHeader } from "../components/PageHeader";
import { KpiCard } from "../components/KpiCard";
import { Panel } from "../components/Panel";
import { BriefCard, type IntelBrief } from "../components/BriefCard";
import { FlashAlertTicker } from "../components/FlashAlertTicker";
import { SitrepProgress } from "../components/SitrepProgress";
import {
  useIntelBriefs,
  useFlashAlerts,
  useIntelUnreadCount,
  useMarkBriefRead,
  useGenerateSitrep,
  useGenerateFlashCheck,
  useGenerateWeekly,
} from "../api/hooks";

type Tab = "all" | "sitrep" | "alerts" | "nation" | "conflict" | "maritime" | "trade";

const TABS: { key: Tab; label: string }[] = [
  { key: "all", label: "All Briefs" },
  { key: "sitrep", label: "SITREP" },
  { key: "alerts", label: "Flash Alerts" },
  { key: "nation", label: "Nations" },
  { key: "conflict", label: "Conflicts" },
  { key: "maritime", label: "Maritime" },
  { key: "trade", label: "Trade" },
];

const DISMISSED_KEY = "intel_dismissed_flash";
function loadDismissed(): Set<string> {
  try {
    return new Set(JSON.parse(localStorage.getItem(DISMISSED_KEY) || "[]"));
  } catch { return new Set(); }
}
function saveDismissed(ids: Set<string>) {
  localStorage.setItem(DISMISSED_KEY, JSON.stringify([...ids]));
}

function tabToQuery(tab: Tab): { brief_type?: string; domain?: string } {
  switch (tab) {
    case "sitrep": return { brief_type: "daily_sitrep" };
    case "alerts": return { brief_type: "flash_alert" };
    case "nation": return { domain: "nation" };
    case "conflict": return { domain: "conflict" };
    case "maritime": return { domain: "maritime" };
    case "trade": return { domain: "trade" };
    default: return {};
  }
}

export default function Intelligence() {
  const [tab, setTab] = useState<Tab>("all");
  const [activeJobId, setActiveJobId] = useState<string | null>(
    () => sessionStorage.getItem("intel_job_id"),
  );

  // Persist job ID across page navigations
  const setJobId = (id: string | null) => {
    setActiveJobId(id);
    if (id) sessionStorage.setItem("intel_job_id", id);
    else sessionStorage.removeItem("intel_job_id");
  };

  const query = tabToQuery(tab);
  const briefs = useIntelBriefs({ ...query, limit: 50 });
  const flashAlerts = useFlashAlerts(10);
  const unread = useIntelUnreadCount();
  const markRead = useMarkBriefRead();
  const [dismissed, setDismissed] = useState<Set<string>>(loadDismissed);
  const genSitrep = useGenerateSitrep();
  const genFlash = useGenerateFlashCheck();
  const genWeekly = useGenerateWeekly();

  const handleDismiss = useCallback((id: string) => {
    setDismissed((prev) => {
      const next = new Set(prev);
      next.add(id);
      saveDismissed(next);
      return next;
    });
  }, []);

  const briefListRaw = (Array.isArray(briefs.data) ? briefs.data : []) as IntelBrief[];
  // On "all" tab, hide dismissed flash alerts
  const briefList = tab === "all"
    ? briefListRaw.filter((b) => b.brief_type !== "flash_alert" || !dismissed.has(b.id))
    : briefListRaw;
  const alertList = (Array.isArray(flashAlerts.data) ? flashAlerts.data : []) as IntelBrief[];
  const unreadData = (unread.data ?? {}) as Record<string, number>;
  const totalUnread = Object.values(unreadData).reduce((a, b) => a + (b ?? 0), 0);

  const isGenerating = genSitrep.isPending || genFlash.isPending || genWeekly.isPending || !!activeJobId;

  const handleGenSitrep = () => {
    genSitrep.mutate(undefined, {
      onSuccess: (data) => {
        const d = data as Record<string, unknown>;
        if (d?.job_id) setJobId(d.job_id as string);
      },
    });
  };

  const handleGenWeekly = () => {
    genWeekly.mutate(undefined, {
      onSuccess: (data) => {
        const d = data as Record<string, unknown>;
        if (d?.job_id) setJobId(d.job_id as string);
      },
    });
  };

  const handleJobDone = useCallback(() => {
    // Clear job after a short delay so the user sees 100%
    setTimeout(() => setJobId(null), 5000);
  }, []);

  return (
    <div className="space-y-4">
      <PageHeader
        title="Intelligence Briefing Center"
        subtitle="AI-powered geo-intelligence reports & flash alerts"
        onRefresh={() => { briefs.refetch(); flashAlerts.refetch(); unread.refetch(); }}
        actions={
          <div className="flex gap-2">
            <button
              className="rounded border border-accent bg-accent/10 px-3 py-1 text-xs font-semibold text-accent hover:bg-accent/20 disabled:opacity-50"
              onClick={handleGenSitrep}
              disabled={isGenerating}
            >
              {genSitrep.isPending ? "Starting…" : "Generate SITREP"}
            </button>
            <button
              className="rounded border border-warning bg-warning/10 px-3 py-1 text-xs font-semibold text-warning hover:bg-warning/20 disabled:opacity-50"
              onClick={() => genFlash.mutate()}
              disabled={isGenerating}
            >
              {genFlash.isPending ? "Checking…" : "Flash Check"}
            </button>
            <button
              className="rounded border border-info bg-info/10 px-3 py-1 text-xs font-semibold text-info hover:bg-info/20 disabled:opacity-50"
              onClick={handleGenWeekly}
              disabled={isGenerating}
            >
              {genWeekly.isPending ? "Starting…" : "Weekly Report"}
            </button>
          </div>
        }
      />

      {/* KPI Cards */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <KpiCard label="Total Briefs" value={String(briefList.length)} />
        <KpiCard
          label="Unread"
          value={String(totalUnread)}
          sentiment={totalUnread > 0 ? "warning" : "neutral"}
        />
        <KpiCard
          label="Flash Alerts"
          value={String(alertList.length)}
          sentiment={alertList.length > 0 ? "negative" : "neutral"}
        />
        <KpiCard
          label="Domains"
          value={String(new Set(briefList.map((b) => b.domain)).size)}
        />
      </div>

      {/* Progress Bar */}
      {activeJobId && (
        <SitrepProgress jobId={activeJobId} onDone={handleJobDone} />
      )}

      {/* Flash Alert Ticker */}
      {alertList.length > 0 && (
        <FlashAlertTicker
          alerts={alertList}
          onSelect={(id) => {
            setTab("alerts");
            markRead.mutate(id);
          }}
        />
      )}

      {/* Tab Bar */}
      <div className="flex gap-1 overflow-x-auto border-b border-border-dim pb-px">
        {TABS.map((t) => (
          <button
            key={t.key}
            className={`shrink-0 rounded-t px-3 py-1.5 text-xs font-semibold transition-colors ${
              tab === t.key
                ? "bg-surface-raised text-zinc-100 border-b-2 border-accent"
                : "text-muted hover:text-zinc-300 hover:bg-surface-overlay"
            }`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
            {t.key === "alerts" && alertList.length > 0 && (
              <span className="ml-1.5 inline-flex h-4 w-4 items-center justify-center rounded-full bg-red-500/30 text-[9px] font-bold text-red-300">
                {alertList.length}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Brief List */}
      <Panel title={TABS.find((t) => t.key === tab)?.label ?? "Briefs"}>
        {briefs.isLoading ? (
          <div className="py-8 text-center text-xs text-muted">Loading briefs…</div>
        ) : briefList.length === 0 ? (
          <div className="py-8 text-center text-xs text-muted">
            No briefs yet — click <span className="text-accent font-semibold">Generate SITREP</span> or{" "}
            <span className="text-warning font-semibold">Flash Check</span> to create the first briefing.
            <br />
            <span className="text-[10px] opacity-70">Requires Kronos LLM setup in Settings for SITREP generation.</span>
          </div>
        ) : (
          <div className="space-y-2">
            {briefList.map((b) => (
              <BriefCard
                key={b.id}
                brief={b}
                onMarkRead={(id) => markRead.mutate(id)}
                onDismiss={tab === "all" && b.brief_type === "flash_alert" ? handleDismiss : undefined}
                showTimestamp={b.brief_type === "flash_alert"}
              />
            ))}
          </div>
        )}
      </Panel>
    </div>
  );
}
