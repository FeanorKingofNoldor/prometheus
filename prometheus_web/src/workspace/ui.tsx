import { useEffect, useRef, useState, type ReactNode, Component } from "react";
import { NavLink, Link, useLocation } from "react-router-dom";
import {
  Activity,
  ArrowUpRight,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  Flame,
  Inbox,
  Menu,
  RefreshCw,
  Search,
  Sparkles,
  X,
  type LucideIcon,
} from "lucide-react";
export const money = (v?: number | null) =>
  v == null || !Number.isFinite(v)
    ? "—"
    : v.toLocaleString(undefined, {
        style: "currency",
        currency: "USD",
        maximumFractionDigits: 2,
      });
export const pct = (v?: number | null) =>
  v == null || !Number.isFinite(v) ? "—" : `${(v * 100).toFixed(1)}%`;
export const num = (v?: number | null) =>
  v == null || !Number.isFinite(v) ? "—" : v.toLocaleString();
export const words = (v?: string | null) =>
  (v ?? "unknown").replace(/_/g, " ").toLowerCase();
export const date = (v?: string | null) =>
  !v || !Number.isFinite(Date.parse(v))
    ? "Not recorded"
    : new Date(v).toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
export const age = (v?: string | null) =>
  !v || !Number.isFinite(Date.parse(v))
    ? Infinity
    : Math.max(0, (Date.now() - Date.parse(v)) / 60000);
export const safeLink = (v?: string) =>
  v && /^https?:\/\//i.test(v) ? v : undefined;
export function Badge({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: string;
}) {
  return <span className={`badge ${tone}`}>{children}</span>;
}
export function Metric({
  label,
  value,
  note,
  tone,
}: {
  label: string;
  value: ReactNode;
  note: string;
  tone?: string;
}) {
  return (
    <div className="metric">
      <div className="metric-label">
        {label}
        <ArrowUpRight size={14} />
      </div>
      <div className={`metric-value ${tone ?? ""}`}>{value}</div>
      <div className="metric-note">{note}</div>
    </div>
  );
}
export function Panel({
  title,
  action,
  children,
  body = false,
}: {
  title: string;
  action?: ReactNode;
  children: ReactNode;
  body?: boolean;
}) {
  return (
    <section className="panel">
      <div className="panel-head">
        <h2>{title}</h2>
        {action}
      </div>
      <div className={body ? "panel-body" : undefined}>{children}</div>
    </section>
  );
}
export function Heading({
  title,
  subtitle,
  kicker = "Workspace",
  refresh,
  busy,
}: {
  title: string;
  subtitle: string;
  kicker?: string;
  refresh?: () => void;
  busy?: boolean;
}) {
  return (
    <header className="page-head">
      <div>
        <div className="eyebrow">{kicker}</div>
        <h1>{title}</h1>
        <p>{subtitle}</p>
      </div>
      {refresh && (
        <button className="button" onClick={refresh} disabled={busy}>
          <RefreshCw size={14} className={busy ? "spin" : ""} />
          Refresh data
        </button>
      )}
    </header>
  );
}
export function Empty({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="empty">
      <Inbox size={28} strokeWidth={1.4} />
      <h3>{title}</h3>
      <p>{children}</p>
    </div>
  );
}
export function QueryState({
  query,
}: {
  query: {
    isPending?: boolean;
    isLoading?: boolean;
    isError: boolean;
    data?: unknown;
    refetch: () => unknown;
  };
}) {
  if (query.isError)
    return (
      <div className="notice error" role="alert">
        <CircleAlert size={18} />
        <div>
          <strong>Data unavailable</strong>
          <p>
            {query.data
              ? "Showing the last successful response. It may be out of date."
              : "The server could not return this data. No values have been substituted."}
          </p>
        </div>
        <button className="button" onClick={() => query.refetch()}>
          Retry
        </button>
      </div>
    );
  if (query.isPending || query.isLoading)
    return (
      <div className="loading" role="status">
        <RefreshCw size={16} className="spin" />
        Loading current data…
      </div>
    );
  return null;
}
export function SearchBox({
  value,
  onChange,
  placeholder = "Search…",
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}) {
  return (
    <label className="search">
      <Search size={16} />
      <input
        aria-label={placeholder}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      {value && (
        <button aria-label="Clear search" onClick={() => onChange("")}>
          <X size={14} />
        </button>
      )}
    </label>
  );
}
export function Pagination({
  page,
  total,
  size = 15,
  onChange,
}: {
  page: number;
  total: number;
  size?: number;
  onChange: (p: number) => void;
}) {
  const pages = Math.max(1, Math.ceil(total / size));
  return (
    <div className="pagination">
      <span>
        {total
          ? `${page * size + 1}–${Math.min((page + 1) * size, total)} of ${num(total)}`
          : "0 results"}
      </span>
      <div className="pagination-buttons">
        <button
          className="icon-button"
          aria-label="Previous page"
          disabled={page === 0}
          onClick={() => onChange(page - 1)}
        >
          <ChevronLeft size={16} />
        </button>
        <span style={{ alignSelf: "center" }}>
          {" "}
          {page + 1} / {pages}{" "}
        </span>
        <button
          className="icon-button"
          aria-label="Next page"
          disabled={page + 1 >= pages}
          onClick={() => onChange(page + 1)}
        >
          <ChevronRight size={16} />
        </button>
      </div>
    </div>
  );
}
export function Drawer({
  title,
  kicker,
  children,
  onClose,
}: {
  title: string;
  kicker: string;
  children: ReactNode;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const d = ref.current;
    d?.showModal();
    const previous = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      d?.close();
      document.body.style.overflow = previous;
    };
  }, []);
  return (
    <dialog
      ref={ref}
      className="detail-dialog"
      aria-label={title}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
      onClick={(e) => {
        if (
          e.target === ref.current &&
          e.clientX < ref.current.getBoundingClientRect().left
        )
          onClose();
      }}
    >
      <div className="detail-header">
        <div>
          <div className="eyebrow">{kicker}</div>
          <h2>{title}</h2>
        </div>
        <button
          className="icon-button"
          aria-label="Close details"
          onClick={onClose}
        >
          <X size={18} />
        </button>
      </div>
      <div className="detail-content">{children}</div>
    </dialog>
  );
}
export function Shell({
  name,
  subtitle,
  nav,
  children,
  status,
  updated,
}: {
  name: string;
  subtitle: string;
  nav: { to: string; label: string; icon: LucideIcon }[];
  children: ReactNode;
  status: ReactNode;
  updated?: number;
}) {
  const [open, setOpen] = useState(false);
  const location = useLocation();
  useEffect(() => {
    setOpen(false);
    window.scrollTo(0, 0);
  }, [location.pathname]);
  useEffect(() => {
    const listener = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", listener);
    return () => window.removeEventListener("keydown", listener);
  }, []);
  const current = nav.find((n) =>
    n.to === "/"
      ? location.pathname === "/"
      : location.pathname.startsWith(n.to),
  );
  return (
    <div className="shell">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      {open && (
        <button
          className="menu-scrim"
          aria-label="Close navigation"
          onClick={() => setOpen(false)}
        />
      )}
      <aside className={`rail ${open ? "open" : ""}`}>
        <Link className="brand" to="/">
          <span className="brand-mark">
            {name === "Prometheus" ? (
              <Flame size={21} />
            ) : (
              <Sparkles size={21} />
            )}
          </span>
          {name}
        </Link>
        <button
          className="icon-button nav-close"
          aria-label="Close menu"
          onClick={() => setOpen(false)}
        >
          <X size={16} />
        </button>
        <div className="eyebrow rail-label">{subtitle}</div>
        <nav aria-label="Main navigation">
          {nav.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.to === "/"}
              className={({ isActive }) =>
                `nav-item ${isActive ? "active" : ""}`
              }
            >
              <n.icon size={17} />
              {n.label}
            </NavLink>
          ))}
        </nav>
        <div className="rail-footer">
          <div className="eyebrow">Research in motion</div>
          <strong>Observe. Decide. Measure.</strong>
          <span>Powered by Apatheon intelligence</span>
        </div>
      </aside>
      <div className="main-shell">
        <div className="topline">
          <div className="topline-left">
            <button
              className="icon-button mobile-menu"
              aria-label="Open navigation"
              aria-expanded={open}
              onClick={() => setOpen(true)}
            >
              <Menu size={17} />
            </button>
            <span className="breadcrumb">
              Workspace <span className="muted"> / </span>{" "}
              <span style={{ color: "#dce5f2" }}>
                {current?.label ?? "Details"}
              </span>
            </span>
          </div>
          <div className="topline-right">
            <span className="update-time">
              {updated
                ? `Fetched ${new Date(updated).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}`
                : "Connecting"}
            </span>
            {status}
          </div>
        </div>
        <main id="main" className="content">
          {children}
        </main>
      </div>
    </div>
  );
}
export class ErrorBoundary extends Component<
  { children: ReactNode },
  { failed: boolean }
> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    return this.state.failed ? (
      <main className="content">
        <Empty title="This view could not be displayed">
          Reload the workspace to try again.
        </Empty>
        <button className="button" onClick={() => window.location.reload()}>
          Reload workspace
        </button>
      </main>
    ) : (
      this.props.children
    );
  }
}
