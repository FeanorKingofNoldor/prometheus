import { useEffect, useRef, useState } from "react";
import { Bell, CheckCheck, ExternalLink, X } from "lucide-react";
import { useNavigate } from "react-router-dom";
import {
  useNotifications,
  useMarkNotificationRead,
  useDismissNotification,
  useMarkAllNotificationsRead,
  type NotificationItem,
} from "../api/hooks";
import { SeverityBadge } from "./SeverityBadge";

function formatRelative(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diffMs / 60_000);
  if (m < 1) return "just now";
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h`;
  const d = Math.floor(h / 24);
  return `${d}d`;
}

export function NotificationsBell() {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const navigate = useNavigate();
  const { data } = useNotifications({ unreadOnly: true, limit: 5 });
  const markRead = useMarkNotificationRead();
  const dismiss = useDismissNotification();
  const markAll = useMarkAllNotificationsRead();

  const unread = data?.unread_count ?? 0;
  const items = data?.items ?? [];

  useEffect(() => {
    function onClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    if (open) {
      document.addEventListener("mousedown", onClick);
      return () => document.removeEventListener("mousedown", onClick);
    }
  }, [open]);

  function handleItemClick(n: NotificationItem) {
    markRead.mutate(n.notification_id);
    setOpen(false);
    if (n.link_path) navigate(n.link_path);
    else navigate("/notifications");
  }

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="relative flex h-7 w-7 items-center justify-center rounded text-muted transition-colors hover:bg-surface-overlay hover:text-zinc-100"
        title="Notifications"
        aria-label={`Notifications (${unread} unread)`}
      >
        <Bell size={14} />
        {unread > 0 && (
          <span className="absolute -right-0.5 -top-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-red-500 px-1 text-[9px] font-bold leading-none text-white">
            {unread > 99 ? "99+" : unread}
          </span>
        )}
      </button>

      {open && (
        <div className="absolute right-0 top-9 z-50 w-96 rounded-lg border border-border-dim bg-surface-raised shadow-xl">
          <div className="flex items-center justify-between border-b border-border-dim px-3 py-2">
            <span className="text-[11px] font-semibold uppercase tracking-wider text-muted">
              Notifications
            </span>
            <div className="flex items-center gap-2">
              {unread > 0 && (
                <button
                  onClick={() => markAll.mutate()}
                  disabled={markAll.isPending}
                  className="flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] text-muted transition-colors hover:bg-surface-overlay hover:text-zinc-100 disabled:opacity-50"
                  title="Mark all as read"
                >
                  <CheckCheck size={11} /> Mark all
                </button>
              )}
              <button
                onClick={() => {
                  setOpen(false);
                  navigate("/notifications");
                }}
                className="flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] text-accent transition-colors hover:bg-surface-overlay"
              >
                View all <ExternalLink size={10} />
              </button>
            </div>
          </div>

          <div className="max-h-96 overflow-y-auto">
            {items.length === 0 ? (
              <div className="px-3 py-8 text-center text-xs text-muted">
                No unread notifications.
              </div>
            ) : (
              items.map((n) => (
                <div
                  key={n.notification_id}
                  className="group flex items-start gap-2 border-b border-border-dim/50 px-3 py-2 hover:bg-surface-overlay/50"
                >
                  <button
                    className="flex-1 text-left"
                    onClick={() => handleItemClick(n)}
                  >
                    <div className="flex items-center gap-2">
                      <SeverityBadge severity={n.severity} />
                      <span className="text-[10px] text-muted">
                        {formatRelative(n.created_at)}
                      </span>
                    </div>
                    <div className="mt-1 text-xs font-medium text-zinc-100">
                      {n.title}
                    </div>
                    {n.body && (
                      <div className="mt-0.5 text-[11px] text-muted line-clamp-2">
                        {n.body}
                      </div>
                    )}
                  </button>
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      dismiss.mutate(n.notification_id);
                    }}
                    className="opacity-0 transition-opacity group-hover:opacity-100"
                    title="Dismiss"
                    aria-label="Dismiss notification"
                  >
                    <X size={12} className="text-muted hover:text-zinc-100" />
                  </button>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
