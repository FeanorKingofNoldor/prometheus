import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Briefcase,
  ArrowRightLeft,
  FlaskConical,
  LineChart,
  ScrollText,
  BookOpen,
  Settings,
  MessageSquare,
  Brain,
  Activity,
  Server,
} from "lucide-react";

const links = [
  { to: "/", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/portfolio", icon: Briefcase, label: "Portfolio" },
  { to: "/execution", icon: ArrowRightLeft, label: "Execution" },
  { to: "/monitor", icon: Activity, label: "Trade Monitor" },
  { to: "/operations", icon: Server, label: "Operations" },
  { to: "/backtests", icon: FlaskConical, label: "Backtests" },
  { to: "/options", icon: LineChart, label: "Options" },
  { to: "/intelligence", icon: Brain, label: "Intelligence" },
  { to: "/logs", icon: ScrollText, label: "Logs & Reports" },
  { to: "/docs", icon: BookOpen, label: "Documentation" },
  { to: "/settings", icon: Settings, label: "Settings" },
  { to: "/kronos", icon: MessageSquare, label: "Kronos" },
];

export function Sidebar() {
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
          </NavLink>
        ))}
      </nav>
      <div className="border-t border-border-dim px-4 py-3 text-[10px] text-muted">
        v0.1.0 · C2 Web
      </div>
    </aside>
  );
}
