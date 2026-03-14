import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Briefcase,
  FlaskConical,
  Activity,
  Database,
  ScrollText,
  Brain,
  Settings,
  MessageSquare,
  Globe,
  Map,
} from "lucide-react";
import { useIntelUnreadCount } from "../api/hooks";

const links = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/portfolio", icon: Briefcase, label: "Portfolio & Risk" },
  { to: "/backtests", icon: FlaskConical, label: "Backtests" },
  { to: "/regime", icon: Activity, label: "Regime & Market" },
  { to: "/entities", icon: Database, label: "Entities" },
  { to: "/logs", icon: ScrollText, label: "Logs" },
  { to: "/nation", icon: Globe, label: "Nation Intel" },
  { to: "/geo", icon: Map, label: "Geo Risk Map" },
  { to: "/intelligence", icon: Brain, label: "Intelligence" },
  { to: "/settings", icon: Settings, label: "Settings" },
  { to: "/chat", icon: MessageSquare, label: "Kronos" },
];

export function Sidebar() {
  const unread = useIntelUnreadCount();
  const unreadData = (unread.data ?? {}) as Record<string, number>;
  const totalUnread = Object.values(unreadData).reduce((a, b) => a + (b ?? 0), 0);

  return (
    <aside className="flex w-52 flex-col border-r border-border-dim bg-surface-raised">
      <div className="flex h-12 items-center gap-2 border-b border-border-dim px-4">
        <div className="h-6 w-6 rounded bg-accent" />
        <span className="text-sm font-bold tracking-wide text-accent">
          PROMETHEUS
        </span>
      </div>
      <nav className="flex-1 overflow-y-auto py-2">
        {links.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-3 px-4 py-2 text-xs transition-colors ${
                isActive
                  ? "bg-surface-overlay text-accent border-r-2 border-accent"
                  : "text-muted hover:text-zinc-100 hover:bg-surface-overlay/50"
              }`
            }
          >
            <Icon size={16} />
            {label}
            {to === "/intelligence" && totalUnread > 0 && (
              <span className="ml-auto inline-flex h-4 min-w-[16px] items-center justify-center rounded-full bg-accent/25 px-1 text-[9px] font-bold text-accent">
                {totalUnread > 99 ? "99+" : totalUnread}
              </span>
            )}
          </NavLink>
        ))}
      </nav>
      <div className="border-t border-border-dim px-4 py-3 text-[10px] text-muted">
        v0.1.0 · C2 Web
      </div>
    </aside>
  );
}
