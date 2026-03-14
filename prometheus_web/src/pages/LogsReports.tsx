import { useSearchParams } from "react-router-dom";
import { PageHeader } from "../components/PageHeader";
import { ScrollText, Activity, FileText, ArrowRightLeft } from "lucide-react";
import SystemLogsTab from "./logs/SystemLogsTab";
import PipelineRunsTab from "./logs/PipelineRunsTab";
import ReportsTab from "./logs/ReportsTab";
import ExecutionTab from "./logs/ExecutionTab";

const TABS = [
  { key: "system", label: "System Logs", icon: ScrollText },
  { key: "runs", label: "Pipeline Runs", icon: Activity },
  { key: "reports", label: "Reports", icon: FileText },
  { key: "execution", label: "Execution", icon: ArrowRightLeft },
] as const;

type TabKey = (typeof TABS)[number]["key"];

export default function LogsReports() {
  const [params, setParams] = useSearchParams();
  const activeTab = (params.get("tab") as TabKey) || "system";

  const setTab = (t: TabKey) => setParams({ tab: t });

  return (
    <div className="space-y-3">
      <PageHeader
        title="Logs"
        subtitle="System logs, pipeline runs, LLM reports, and execution activity"
      />

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-border-dim">
        {TABS.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setTab(key)}
            className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors border-b-2 -mb-px ${
              activeTab === key
                ? "border-accent text-accent"
                : "border-transparent text-muted hover:text-zinc-200"
            }`}
          >
            <Icon size={13} />
            {label}
          </button>
        ))}
      </div>

      {/* Active tab content */}
      {activeTab === "system" && <SystemLogsTab />}
      {activeTab === "runs" && <PipelineRunsTab />}
      {activeTab === "reports" && <ReportsTab />}
      {activeTab === "execution" && <ExecutionTab />}
    </div>
  );
}
