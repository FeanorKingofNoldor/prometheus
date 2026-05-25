import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { CheckCheck, X, ExternalLink, Check } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Panel } from "../components/Panel";
import { SeverityBadge } from "../components/SeverityBadge";
import {
  useNotifications,
  useMarkNotificationRead,
  useDismissNotification,
  useMarkAllNotificationsRead,
  type NotificationItem,
} from "../api/hooks";

const SEVERITIES = ["all", "critical", "warning", "info"] as const;
const KINDS = [
  { value: "all", label: "All kinds" },
  { value: "proposal_pending", label: "Proposals pending" },
  { value: "critical_insight", label: "Critical insights" },
  { value: "signal_degradation", label: "Signal degradation" },
  { value: "diagnostic_warning", label: "Diagnostics" },
  { value: "drift_alert", label: "Drift alerts" },
];

function fmtDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString();
}

export default function Notifications() {
  const navigate = useNavigate();
  const [severity, setSeverity] = useState<string>("all");
  const [kind, setKind] = useState<string>("all");
  const [showDismissed, setShowDismissed] = useState(false);
  const [unreadOnly, setUnreadOnly] = useState(false);

  const { data, isLoading, refetch } = useNotifications({
    severity: severity === "all" ? undefined : severity,
    kind: kind === "all" ? undefined : kind,
    unreadOnly,
    includeDismissed: showDismissed,
    limit: 200,
  });

  const markRead = useMarkNotificationRead();
  const dismiss = useDismissNotification();
  const markAll = useMarkAllNotificationsRead();

  const items = data?.items ?? [];
  const unread = data?.unread_count ?? 0;
  const total = data?.total ?? 0;

  function handleRowClick(n: NotificationItem) {
    if (!n.read_at) markRead.mutate(n.notification_id);
    if (n.link_path) navigate(n.link_path);
  }

  return (
    <div>
      <PageHeader
        title="Notifications"
        subtitle={`${unread} unread of ${total} active alerts. The autopilot loop emits these after each daily run.`}
        onRefresh={() => refetch()}
        actions={
          unread > 0 ? (
            <button
              onClick={() => markAll.mutate()}
              disabled={markAll.isPending}
              className="flex items-center gap-1.5 rounded border border-border-dim px-2 py-1 text-[10px] uppercase tracking-wider text-muted hover:border-accent hover:text-accent disabled:opacity-50"
            >
              <CheckCheck size={11} /> Mark all read
            </button>
          ) : null
        }
      />

      <Panel
        title="Inbox"
        actions={
          <div className="flex items-center gap-2 text-[10px]">
            <select
              value={severity}
              onChange={(e) => setSeverity(e.target.value)}
              className="rounded border border-border-dim bg-surface px-2 py-0.5 text-xs text-zinc-100"
            >
              {SEVERITIES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
            <select
              value={kind}
              onChange={(e) => setKind(e.target.value)}
              className="rounded border border-border-dim bg-surface px-2 py-0.5 text-xs text-zinc-100"
            >
              {KINDS.map((k) => (
                <option key={k.value} value={k.value}>
                  {k.label}
                </option>
              ))}
            </select>
            <label className="flex items-center gap-1 text-muted">
              <input
                type="checkbox"
                checked={unreadOnly}
                onChange={(e) => setUnreadOnly(e.target.checked)}
              />
              Unread only
            </label>
            <label className="flex items-center gap-1 text-muted">
              <input
                type="checkbox"
                checked={showDismissed}
                onChange={(e) => setShowDismissed(e.target.checked)}
              />
              Show dismissed
            </label>
          </div>
        }
      >
        {isLoading ? (
          <div className="py-8 text-center text-xs text-muted">Loading…</div>
        ) : items.length === 0 ? (
          <div className="py-8 text-center text-xs text-muted">
            No notifications match the current filters.
          </div>
        ) : (
          <div className="divide-y divide-border-dim/50">
            {items.map((n) => (
              <div
                key={n.notification_id}
                className={`group flex items-start gap-3 px-2 py-3 transition-colors hover:bg-surface-overlay/40 ${
                  !n.read_at ? "bg-surface-overlay/20" : ""
                }`}
              >
                <div className="flex-shrink-0 pt-0.5">
                  <SeverityBadge severity={n.severity} />
                </div>
                <button
                  className="flex-1 text-left"
                  onClick={() => handleRowClick(n)}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-zinc-100">
                      {n.title}
                    </span>
                    <span className="rounded bg-surface px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-muted">
                      {n.kind}
                    </span>
                    {n.dismissed_at && (
                      <span className="rounded bg-zinc-700/40 px-1.5 py-0.5 text-[9px] uppercase tracking-wider text-muted">
                        dismissed
                      </span>
                    )}
                  </div>
                  {n.body && (
                    <div className="mt-1 text-[11px] text-muted whitespace-pre-wrap">
                      {n.body}
                    </div>
                  )}
                  <div className="mt-1 flex items-center gap-3 text-[10px] text-muted">
                    <span>{fmtDate(n.created_at)}</span>
                    {n.source_table && (
                      <span>
                        source: {n.source_table}
                        {n.source_id ? `#${n.source_id}` : ""}
                      </span>
                    )}
                    {n.link_path && (
                      <span className="flex items-center gap-0.5 text-accent">
                        {n.link_path} <ExternalLink size={9} />
                      </span>
                    )}
                  </div>
                </button>
                <div className="flex flex-shrink-0 items-center gap-1 opacity-0 transition-opacity group-hover:opacity-100">
                  {!n.read_at && (
                    <button
                      onClick={() => markRead.mutate(n.notification_id)}
                      title="Mark read"
                      className="rounded p-1 text-muted hover:bg-surface hover:text-zinc-100"
                    >
                      <Check size={12} />
                    </button>
                  )}
                  {!n.dismissed_at && (
                    <button
                      onClick={() => dismiss.mutate(n.notification_id)}
                      title="Dismiss"
                      className="rounded p-1 text-muted hover:bg-surface hover:text-zinc-100"
                    >
                      <X size={12} />
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </Panel>
    </div>
  );
}
