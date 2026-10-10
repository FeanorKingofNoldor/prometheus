import Account from "./Account";
import Chat from "./Chat";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Link,
  Navigate,
  Route,
  Routes,
  useSearchParams,
} from "react-router-dom";
import {
  Activity,
  ArrowRight,
  BookOpen,
  ChartNoAxesCombined,
  CircleAlert,
  MessageSquare,
  Crosshair,
  FileText,
  Layers,
  LayoutDashboard,
  Radar,
  ShieldCheck,
  TrendingUp,
  Wallet,
} from "lucide-react";
import { api } from "../api/client";
import type { Candidate, Desk, Event as DeskEvent } from "./types";
import {
  age,
  Badge,
  date,
  Drawer,
  Empty,
  ErrorBoundary,
  Heading,
  Metric,
  money,
  num,
  Panel,
  Pagination,
  pct,
  QueryState,
  safeLink,
  SearchBox,
  Shell,
  words,
} from "./ui";

const archived = new Set(["REJECTED", "EXPIRED", "NO_FILL", "CLOSED"]);
const active = (c: Candidate) => !archived.has(c.state);
const trade = (c: Candidate) =>
  c.state !== "WATCHING" &&
  !["REJECTED", "EXPIRED", "NO_FILL"].includes(c.state);
const tone = (state: string) =>
  ["OPEN", "CLOSED", "FILLED"].includes(state)
    ? "good"
    : ["REVIEW", "ERROR"].includes(state)
      ? "bad"
      : state === "WATCHING"
        ? "accent"
        : "warn";
const nav = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/opportunities", label: "Opportunities", icon: Crosshair },
  { to: "/trades", label: "Paper trades", icon: Wallet },
  { to: "/account", label: "Broker account", icon: ShieldCheck },
  { to: "/research", label: "Results & research", icon: ChartNoAxesCombined },
  { to: "/events", label: "Source events", icon: Layers },
  { to: "/operations", label: "System health", icon: Activity },
  { to: "/iris", label: "Iris assistant", icon: MessageSquare },
  { to: "/method", label: "Strategy & limits", icon: BookOpen },
];
const useDesk = () =>
  useQuery({
    queryKey: ["workspace", "event-desk"],
    queryFn: () => api.get<Desk>("/event-desk"),
    refetchInterval: 30000,
  });
type DeskQuery = ReturnType<typeof useDesk>;
function Evidence({ event }: { event: DeskEvent }) {
  return (
    <>
      <h3>Source evidence</h3>
      <p>
        {event.evidence_state.sources} independent sources ·{" "}
        {words(event.evidence_state.label)}. Repeated or unresolved aggregator
        reports do not count as independent confirmation.
      </p>
      {event.evidence.map((e, i) => (
        <article className="source" key={`${e.url}${i}`}>
          <a href={safeLink(e.url)} target="_blank" rel="noreferrer">
            {e.title}
          </a>
          <div className="cell-sub">
            {e.source} · {date(e.published_at)}
          </div>
          {e.quote && <p>“{e.quote}”</p>}
        </article>
      ))}
    </>
  );
}
function CandidateDetail({
  candidate: c,
  onClose,
}: {
  candidate: Candidate;
  onClose: () => void;
}) {
  const p = c.payload.plan ?? c.payload.proposed_plan;
  return (
    <Drawer
      title={c.instrument_id}
      kicker={`${words(c.strategy)} · event-v1`}
      onClose={onClose}
    >
      <Badge tone={tone(c.state)}>{words(c.state)}</Badge>
      <h3>
        {words(c.event.predicate)} · {c.event.entity[1]}
      </h3>
      <p>{c.event.title}</p>
      <div className="notice">
        <CircleAlert size={18} />
        <div>
          <strong>Current decision</strong>
          <p>{c.reason}</p>
        </div>
      </div>
      <dl className="definition">
        <div>
          <dt>First observed</dt>
          <dd>{date(c.created_at)}</dd>
        </div>
        <div>
          <dt>Economic exposure</dt>
          <dd>{c.payload.sign > 0 ? "Positive" : "Negative"}</dd>
        </div>
        <div>
          <dt>Observed return</dt>
          <dd>{pct(c.payload.last_mark?.return)}</dd>
        </div>
        <div>
          <dt>Excess over SPY</dt>
          <dd>{pct(c.payload.last_mark?.excess_return)}</dd>
        </div>
      </dl>
      <p>
        Quote timing:{" "}
        {words(c.payload.last_mark?.quote?.timing ?? "awaiting quote")}
        {c.payload.last_mark?.quote?.observed_at &&
          ` · ${date(c.payload.last_mark.quote.observed_at)}`}
      </p>
      <h3>Why this stock?</h3>
      {c.payload.path.length ? (
        c.payload.path.map((path, i) => (
          <div className="method" key={i}>
            <h3>
              {path.from[1]} → {path.to[1]}
            </h3>
            <p>{words(path.relation)}</p>
          </div>
        ))
      ) : (
        <p>Direct exposure to the company named in the event.</p>
      )}
      <h3>Trade plan</h3>
      {p ? (
        <>
          <dl className="definition">
            <div>
              <dt>Order</dt>
              <dd>
                {p.side > 0 ? "Buy" : "Sell short"} {num(p.quantity)} shares
              </dd>
            </div>
            <div>
              <dt>Entry limit</dt>
              <dd>{money(p.entry_limit)}</dd>
            </div>
            <div>
              <dt>Initial stop / target</dt>
              <dd>
                {money(p.stop)} / {money(p.target)}
              </dd>
            </div>
            <div>
              <dt>Maximum holding time</dt>
              <dd>{p.hold_hours} hours</dd>
            </div>
          </dl>
          <p>
            Estimated round-trip costs {money(p.estimated_round_trip_cost)}.
            Target payoff after estimated costs{" "}
            {money(p.target_payoff_after_estimated_cost)}. This is a conditional
            trade plan, not a forecast of expected profit.
          </p>
        </>
      ) : (
        <p>
          No order planned. Waiting for price, evidence and risk conditions.
        </p>
      )}
      <h3>Broker execution</h3>
      <dl className="definition">
        <div>
          <dt>Filled entry price</dt>
          <dd>{money(c.payload.entry_vwap)}</dd>
        </div>
        <div>
          <dt>Shares remaining</dt>
          <dd>{num(c.payload.remaining_quantity)}</dd>
        </div>
        <div>
          <dt>Closed trade-fill P&L</dt>
          <dd>{money(c.payload.net_pnl)}</dd>
        </div>
      </dl>
      <Evidence event={c.event} />
      <details className="small muted" style={{ marginTop: 24 }}>
        <summary>Record identifiers</summary>
        <p className="break">
          Candidate {c.candidate_id}
          <br />
          Event {c.event_id}
        </p>
      </details>
    </Drawer>
  );
}
function CandidateTable({
  rows,
  onSelect,
  compact = false,
}: {
  rows: Candidate[];
  onSelect: (c: Candidate) => void;
  compact?: boolean;
}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Asset / strategy</th>
            <th>Event & evidence</th>
            {!compact && <th>Market observation</th>}
            <th>Decision</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((c) => (
            <tr key={c.candidate_id}>
              <td>
                <button
                  className="text-link cell-title"
                  onClick={() => onSelect(c)}
                >
                  {c.instrument_id}
                </button>
                <span className="cell-sub">{words(c.strategy)}</span>
              </td>
              <td className="primary-cell">
                <span className="cell-title">
                  {words(c.event.predicate)} · {c.event.entity[1]}
                </span>
                <span className="cell-sub clamp">{c.event.title}</span>
                <span className="cell-sub">
                  {words(c.event.evidence_state.label)} ·{" "}
                  {c.event.evidence_state.sources} sources
                </span>
              </td>
              {!compact && (
                <td>
                  <span className="mono">
                    {pct(c.payload.last_mark?.return)}
                  </span>
                  <span className="cell-sub">
                    {pct(c.payload.last_mark?.excess_return)} vs SPY
                  </span>
                  <span className="cell-sub">
                    {words(
                      c.payload.last_mark?.quote?.timing ?? "awaiting quote",
                    )}
                  </span>
                </td>
              )}
              <td style={{ maxWidth: 270 }}>
                <Badge tone={tone(c.state)}>{words(c.state)}</Badge>
                <span className="cell-sub">{c.reason}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
function Summary({ q }: { q: DeskQuery }) {
  const d = q.data;
  const run = d?.last_run;
  const policy = run?.payload.policy;
  const waiting = d?.candidates.filter((c) => c.state === "WATCHING");
  const open = d?.candidates.filter((c) => trade(c) && c.state !== "CLOSED");
  return (
    <div className="metrics">
      <Metric
        label="Watching"
        value={num(waiting?.length)}
        note="Candidates in the current response"
        tone="accent-text"
      />
      <Metric
        label="Active paper trades"
        value={num(open?.length)}
        note="Includes orders awaiting reconciliation"
      />
      <Metric
        label="Paper allocation"
        value={money(policy?.book_usd)}
        note={`Gross limit ${money(policy?.max_gross)}`}
      />
      <Metric
        label="Completed event groups"
        value={num(
          d
            ? d.paper_results.reduce((n, r) => n + r.event_groups, 0)
            : undefined,
        )}
        note="Strategy × event groups with closed trades"
      />
    </div>
  );
}
function ScannerNotice({ q }: { q: DeskQuery }) {
  const r = q.data?.last_run;
  if (!r) return null;
  const problems = [
    age(r.completed_at) > 10
      ? "The last scanner completion is over 10 minutes old."
      : null,
    r.payload.error,
    r.payload.drawdown_halt
      ? "Drawdown limit reached. New entries are blocked."
      : null,
    r.payload.unresolved_orders
      ? "An order needs reconciliation. New entries are blocked."
      : null,
  ].filter(Boolean);
  return problems.length ? (
    <div className="notice error" role="alert">
      <CircleAlert size={18} />
      <div>
        {problems.map((p, i) => (
          <p key={i}>{p}</p>
        ))}
      </div>
    </div>
  ) : null;
}
function Overview({ q }: { q: DeskQuery }) {
  const d = q.data;
  const [selected, setSelected] = useState<Candidate>();
  const run = d?.last_run;
  const watching = d?.candidates.filter(active) ?? [];
  return (
    <>
      <Heading
        title="The event desk"
        kicker="Prometheus / Overview"
        subtitle="From economic evidence to a measured paper trade."
        refresh={() => void q.refetch()}
        busy={q.isFetching}
      />
      <QueryState query={q} />
      <ScannerNotice q={q} />
      <Summary q={q} />
      <section className="hero">
        <div>
          <Badge
            tone={
              run?.payload.status === "OK" && age(run.completed_at) < 10
                ? "good"
                : "warn"
            }
          >
            <span className="dot" />
            {run ? words(run.payload.market_state) : "Awaiting scanner"}
          </Badge>
          <h2>
            {run?.payload.paper_entries_enabled
              ? "Watching for the next qualified setup."
              : "Observe first. Trade when enabled."}
          </h2>
          <p>
            Continuation follows a corroborated event. Reversal tests a fading
            reaction to weak evidence. Every candidate must pass price,
            liquidity and risk checks before an order.
          </p>
          <div style={{ marginTop: 18 }}>
            <Link className="button primary" to="/opportunities">
              Explore opportunities <ArrowRight size={14} />
            </Link>
          </div>
        </div>
        <div className="hero-symbol">
          <Radar size={38} strokeWidth={1.2} />
        </div>
      </section>
      <div className="stack">
        <Panel
          title="On the watchlist"
          action={
            <Link className="text-link small" to="/opportunities">
              View all →
            </Link>
          }
        >
          {watching.length ? (
            <CandidateTable
              rows={watching.slice(0, 5)}
              onSelect={setSelected}
              compact
            />
          ) : (
            <Empty
              title={
                q.isError ? "Watchlist unavailable" : "No active candidates"
              }
            >
              New candidates appear when source events map to a supported listed
              company.
            </Empty>
          )}
        </Panel>
        <div className="grid-2">
          <Panel title="Decision pipeline" body>
            <div className="flow">
              {[
                { name: "Source events", value: d?.events.length },
                {
                  name: "Mapped groups",
                  value: d
                    ? new Set(watching.map((c) => c.event_id)).size
                    : undefined,
                },
                {
                  name: "Watching",
                  value: d?.candidates.filter((c) => c.state === "WATCHING")
                    .length,
                },
                {
                  name: "Trade records",
                  value: d?.candidates.filter(trade).length,
                },
              ].map((s, i) => (
                <div className="flow-step" key={s.name}>
                  <div className="eyebrow">0{i + 1}</div>
                  <div className="flow-number mono">{num(s.value)}</div>
                  <div className="tiny muted">{s.name}</div>
                </div>
              ))}
            </div>
            <p className="small muted" style={{ marginTop: 18 }}>
              Current response: up to 50 recent events and 250 candidates. One
              event can produce multiple stock and strategy candidates.
            </p>
          </Panel>
          <Panel title="Experiment status" body>
            <div className="row-line">
              <span className="muted">Strategy version</span>
              <Badge tone="accent">event-v1</Badge>
            </div>
            <div className="row-line">
              <span className="muted">New paper entries</span>
              <span>
                {run
                  ? run.payload.paper_entries_enabled
                    ? "Enabled"
                    : "Observations only"
                  : "Unknown"}
              </span>
            </div>
            <div className="row-line">
              <span className="muted">Last completed scan</span>
              <span>{date(run?.completed_at)}</span>
            </div>
            <div className="row-line">
              <span className="muted">Evidence of an edge</span>
              <Link className="text-link" to="/research">
                Review forward results →
              </Link>
            </div>
          </Panel>
        </div>
      </div>
      {selected && (
        <CandidateDetail
          candidate={
            d?.candidates.find(
              (c) => c.candidate_id === selected.candidate_id,
            ) ?? selected
          }
          onClose={() => setSelected(undefined)}
        />
      )}
    </>
  );
}
function Opportunities({
  q,
  trades = false,
}: {
  q: DeskQuery;
  trades?: boolean;
}) {
  const [params, setParams] = useSearchParams();
  const search = params.get("q") ?? "";
  const strategy = params.get("strategy") ?? "ALL";
  const showArchive = params.get("archive") === "1";
  const [page, setPage] = useState(0);
  const [selected, setSelected] = useState<Candidate>();
  const set = (key: string, value: string) => {
    const p = new URLSearchParams(params);
    value ? p.set(key, value) : p.delete(key);
    setParams(p, { replace: true });
    setPage(0);
  };
  const rows = (q.data?.candidates ?? []).filter(
    (c) =>
      (trades ? trade(c) : showArchive || active(c)) &&
      (strategy === "ALL" || c.strategy === strategy) &&
      `${c.instrument_id} ${c.event.title} ${c.event.predicate} ${c.reason} ${c.state}`
        .toLowerCase()
        .includes(search.toLowerCase()),
  );
  const current = Math.min(page, Math.max(0, Math.ceil(rows.length / 15) - 1));
  return (
    <>
      <Heading
        title={trades ? "Paper trades" : "Opportunities"}
        kicker="Prometheus / Execution"
        subtitle={
          trades
            ? "Orders, positions and closed trades belonging to the event experiment."
            : "Trace every stock candidate back to its economic event and entry conditions."
        }
        refresh={() => void q.refetch()}
        busy={q.isFetching}
      />
      <QueryState query={q} />
      <ScannerNotice q={q} />
      {trades && (
        <div className="notice">
          <ShieldCheck size={18} />
          <p>
            This book is isolated from existing broker holdings. Quotes may be
            delayed; stops operate during market sessions.
          </p>
        </div>
      )}
      <Panel
        title={trades ? "Event book" : "Candidate watchlist"}
        action={<Badge>{rows.length} records</Badge>}
      >
        <div className="toolbar">
          <SearchBox
            value={search}
            onChange={(v) => set("q", v)}
            placeholder="Search assets, events or decisions…"
          />
          <select
            className="select"
            aria-label="Strategy"
            value={strategy}
            onChange={(e) => set("strategy", e.target.value)}
          >
            <option value="ALL">Both strategies</option>
            <option value="CONTINUATION">Continuation</option>
            <option value="REVERSAL">Reversal</option>
          </select>
          {!trades && (
            <label className="check-label">
              <input
                type="checkbox"
                checked={showArchive}
                onChange={(e) => set("archive", e.target.checked ? "1" : "")}
              />
              Include archived
            </label>
          )}
        </div>
        {rows.length ? (
          <CandidateTable
            rows={rows.slice(current * 15, (current + 1) * 15)}
            onSelect={setSelected}
          />
        ) : (
          <Empty
            title={
              q.isError
                ? "Records unavailable"
                : search
                  ? "No matching records"
                  : trades
                    ? "No event trades yet"
                    : "No active opportunities"
            }
          >
            {trades
              ? "Orders will appear here after a candidate clears its entry conditions during the US market session."
              : "Try another filter, or wait for a source event with supported economic exposure."}
          </Empty>
        )}
        <Pagination page={current} total={rows.length} onChange={setPage} />
      </Panel>
      <p className="page-footer">
        Latest 250 candidate records. Archived candidates preserve the original
        decision history.
      </p>
      {selected && (
        <CandidateDetail
          candidate={
            q.data?.candidates.find(
              (c) => c.candidate_id === selected.candidate_id,
            ) ?? selected
          }
          onClose={() => setSelected(undefined)}
        />
      )}
    </>
  );
}
function Results({ q }: { q: DeskQuery }) {
  const d = q.data;
  if (!d)
    return (
      <>
        <Heading
          title="Results & research"
          kicker="Prometheus / Evaluation"
          subtitle="Waiting for the measured record."
          refresh={() => void q.refetch()}
          busy={q.isFetching}
        />
        <QueryState query={q} />
      </>
    );
  return (
    <>
      <Heading
        title="Results & research"
        kicker="Prometheus / Evaluation"
        subtitle="Two separate measurements: what the broker filled, and how the thesis developed."
        refresh={() => void q.refetch()}
        busy={q.isFetching}
      />
      <QueryState query={q} />
      <div className="stack">
        <Panel
          title="Broker paper results"
          action={<Badge tone="accent">Executed trades</Badge>}
        >
          {d?.paper_results.length ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Strategy</th>
                    <th>Closed trades</th>
                    <th>Event groups</th>
                    <th>Trade-fill P&L</th>
                  </tr>
                </thead>
                <tbody>
                  {d.paper_results.map((r) => (
                    <tr key={r.strategy}>
                      <td>{words(r.strategy)}</td>
                      <td className="mono">{r.closed_trades}</td>
                      <td className="mono">{r.event_groups}</td>
                      <td
                        className={`mono ${r.pnl_after_recorded_fees_and_estimated_borrow >= 0 ? "positive" : "negative"}`}
                      >
                        {money(r.pnl_after_recorded_fees_and_estimated_borrow)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <Empty title="The first closed trade will start the record">
              There are no closed event trades to evaluate yet. No return or win
              rate is inferred from the watchlist.
            </Empty>
          )}
          <div className="panel-body small muted">
            Includes recorded commissions and estimated commission / borrow
            reserves. Excludes dividends, corporate actions and financing cash
            postings; this is not account total return.
          </div>
        </Panel>
        <Panel
          title="Forward research observations"
          action={<Badge>Not trading P&L</Badge>}
        >
          {d?.research_outcomes.length ? (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Strategy</th>
                    <th>Horizon</th>
                    <th>Observations / events</th>
                    <th>Signed return</th>
                    <th>Excess over SPY</th>
                  </tr>
                </thead>
                <tbody>
                  {d.research_outcomes.map((r) => (
                    <tr key={`${r.strategy}${r.horizon_hours}`}>
                      <td>{words(r.strategy)}</td>
                      <td>{r.horizon_hours}h</td>
                      <td>
                        {r.observations} / {r.event_groups}
                      </td>
                      <td className="mono">{pct(r.mean_signed_return)}</td>
                      <td className="mono">
                        {pct(r.mean_signed_excess_return)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <Empty title="Building the forward sample">
              Measurements are collected 1, 4, 24 and 72 hours after
              observation. Closed-market gaps are left ungraded.
            </Empty>
          )}
          <div className="panel-body small muted">
            {d?.measurement_note} Stocks are averaged within their event, then
            events receive equal weight. A positive early average alone does not
            establish an edge.
          </div>
        </Panel>
      </div>
    </>
  );
}
function Events({ q }: { q: DeskQuery }) {
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<DeskEvent>();
  const rows = (q.data?.events ?? []).filter((e) =>
    `${e.payload.title} ${e.payload.entity.join(" ")} ${e.payload.predicate}`
      .toLowerCase()
      .includes(search.toLowerCase()),
  );
  return (
    <>
      <Heading
        title="Source events"
        kicker="Prometheus / Intelligence"
        subtitle="The evidence entering the strategy, including events that cannot map to a stock."
        refresh={() => void q.refetch()}
        busy={q.isFetching}
      />
      <QueryState query={q} />
      <Panel title="Recent event groups" action={<Badge>Latest 50</Badge>}>
        <div className="toolbar">
          <SearchBox
            value={search}
            onChange={setSearch}
            placeholder="Search source events…"
          />
        </div>
        {rows.length ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Economic event</th>
                  <th>Evidence</th>
                  <th>Stock mapping</th>
                  <th>First observed</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((e) => (
                  <tr key={e.event_id}>
                    <td className="primary-cell">
                      <button
                        className="text-link cell-title"
                        onClick={() => setSelected(e.payload)}
                      >
                        {words(e.payload.predicate)} · {e.payload.entity[1]}
                      </button>
                      <span className="cell-sub clamp">{e.payload.title}</span>
                    </td>
                    <td>
                      <Badge
                        tone={
                          e.payload.evidence_state.label === "confirmed"
                            ? "good"
                            : "warn"
                        }
                      >
                        {words(e.payload.evidence_state.label)}
                      </Badge>
                      <span className="cell-sub">
                        {e.payload.evidence_state.sources} independent sources
                      </span>
                    </td>
                    <td style={{ maxWidth: 250 }}>
                      {e.payload.mapping_reason}
                    </td>
                    <td className="muted">{date(e.first_seen)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty title="No matching source events">
            Change the search or wait for fresh intelligence.
          </Empty>
        )}
      </Panel>
      {selected && (
        <Drawer
          title={`${words(selected.predicate)} · ${selected.entity[1]}`}
          kicker="Source event"
          onClose={() => setSelected(undefined)}
        >
          <p>{selected.mapping_reason}</p>
          <Evidence event={selected} />
        </Drawer>
      )}
    </>
  );
}
type Ops = {
  services: {
    name: string;
    description: string;
    active: string;
    healthy: boolean;
    restarts: number;
    memory_mb: number | null;
  }[];
  daily: {
    date: string;
    total: number;
    success: number;
    failed: number;
    skipped: number;
    status: string;
  }[];
};
type Health = {
  status: string;
  checks: Record<
    string,
    { ok: boolean; status?: string; last_update?: string }
  >;
};
type Jobs = {
  jobs: {
    execution_id: string;
    job_id: string;
    market_id: string | null;
    status: string;
    duration_s: number | null;
    error: string | null;
    started_at: string | null;
  }[];
};
function Operations({ q }: { q: DeskQuery }) {
  const ops = useQuery({
    queryKey: ["workspace", "ops"],
    queryFn: () => api.get<Ops>("/ops/overview"),
    refetchInterval: 30000,
  });
  const health = useQuery({
    queryKey: ["workspace", "health"],
    queryFn: async () => {
      const r = await fetch("/health");
      if (!r.ok && r.status !== 503) throw Error("Health unavailable");
      return r.json() as Promise<Health>;
    },
    refetchInterval: 30000,
  });
  const [day, setDay] = useState("");
  const jobs = useQuery({
    queryKey: ["workspace", "jobs", day],
    queryFn: () => api.get<Jobs>(`/ops/day/${day}`),
    enabled: !!day,
  });
  return (
    <>
      <Heading
        title="System health"
        kicker="Prometheus / Operations"
        subtitle="Service readiness, scanner freshness and completed job history."
        refresh={() => {
          void ops.refetch();
          void health.refetch();
          void q.refetch();
        }}
        busy={ops.isFetching || health.isFetching}
      />
      <QueryState query={ops} />
      <QueryState query={health} />
      <ScannerNotice q={q} />
      <div className="stack">
        <div className="grid-2">
          <Panel title="Data dependencies" body>
            {health.data &&
              Object.entries(health.data.checks).map(([name, c]) => (
                <div className="row-line" key={name}>
                  <span>{words(name)}</span>
                  <Badge tone={c.ok ? "good" : "bad"}>
                    {c.ok ? "Ready" : "Unhealthy"}
                  </Badge>
                </div>
              ))}
          </Panel>
          <Panel title="Application services" body>
            {ops.data?.services
              .filter((s) => s.name !== "postgresql")
              .map((s) => (
                <div className="row-line" key={s.name}>
                  <div>
                    {s.name}
                    <span className="cell-sub">
                      {s.restarts} restarts · {s.memory_mb ?? "—"} MB
                    </span>
                  </div>
                  <Badge tone={s.healthy ? "good" : "bad"}>{s.active}</Badge>
                </div>
              ))}
          </Panel>
        </div>
        <Panel
          title="Job history"
          action={<Badge>Shared stack · 14 days</Badge>}
        >
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Total</th>
                  <th>Succeeded</th>
                  <th>Failed</th>
                  <th>Skipped</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {ops.data?.daily.map((d) => (
                  <tr key={d.date}>
                    <td>
                      <button
                        className="text-link"
                        onClick={() => setDay(d.date)}
                      >
                        {d.date}
                      </button>
                    </td>
                    <td>{d.total}</td>
                    <td>{d.success}</td>
                    <td className={d.failed ? "negative" : ""}>{d.failed}</td>
                    <td>{d.skipped}</td>
                    <td>
                      <Badge
                        tone={
                          d.failed
                            ? "bad"
                            : d.status === "ok"
                              ? "good"
                              : "neutral"
                        }
                      >
                        {d.status}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      </div>
      {day && (
        <Drawer
          title={day}
          kicker="Job execution history"
          onClose={() => setDay("")}
        >
          <QueryState query={jobs} />
          {jobs.data?.jobs.map((j) => (
            <div className="source" key={j.execution_id}>
              <div className="row-line">
                <strong>{words(j.job_id)}</strong>
                <Badge
                  tone={
                    j.status === "SUCCESS"
                      ? "good"
                      : j.status === "FAILED"
                        ? "bad"
                        : "neutral"
                  }
                >
                  {words(j.status)}
                </Badge>
              </div>
              <div className="cell-sub">
                {j.market_id ?? "Shared stack"} · {date(j.started_at)} ·{" "}
                {j.duration_s ?? "—"}s
              </div>
              {j.error && <p className="break negative">{j.error}</p>}
            </div>
          ))}
          {jobs.data?.jobs.length === 0 && (
            <Empty title="No recorded jobs">
              No executions were recorded for this date.
            </Empty>
          )}
        </Drawer>
      )}
    </>
  );
}
function Method({ q }: { q: DeskQuery }) {
  const p = q.data?.last_run?.payload.policy;
  return (
    <>
      <Heading
        title="Strategy & limits"
        kicker="Prometheus / Method"
        subtitle="The rules currently governing the event-v1 paper experiment."
      />
      <QueryState query={q} />
      <div className="grid-2">
        <Panel title="Two testable hypotheses" body>
          <div className="method">
            <h3>01 / Continuation</h3>
            <p>
              Corroborated evidence, at least 15 minutes of observation, a
              0.2–2% move in the expected direction and at least 0.1% excess
              over SPY. Initial stop 2%, target 4%, maximum hold 72 hours.
            </p>
          </div>
          <div className="method">
            <h3>02 / Reversal</h3>
            <p>
              Unconfirmed or contradicted evidence, a reaction of at least 1.5%,
              and a pullback of at least 0.3% while the original move remains at
              least 0.5%. Trade the opposite direction. Initial stop 1.5%,
              target 3%, maximum hold 24 hours.
            </p>
          </div>
          <p className="small muted">
            These are fixed experimental rules. Their forward results determine
            whether they deserve continuation.
          </p>
        </Panel>
        <Panel title="Current risk policy" body>
          {[
            ["Paper allocation", money(p?.book_usd)],
            ["Maximum gross exposure", money(p?.max_gross)],
            ["Per-position cap", money(p?.max_position)],
            ["Per-event cap", money(p?.max_event)],
            ["Maximum positions", num(p?.max_positions)],
            ["Planned risk per trade", money(p?.risk_per_trade)],
            ["Drawdown entry stop", money(p?.drawdown_limit)],
          ].map(([k, v]) => (
            <div className="row-line" key={k}>
              <span className="muted">{k}</span>
              <span className="mono">{v}</span>
            </div>
          ))}
        </Panel>
        <Panel title="Evidence to exposure" body>
          <p className="small muted">
            Fresh structural claims from Apatheon are grouped into economic
            events. Explicit graph relationships connect a company, resource or
            infrastructure event to listed US stocks, with at most two
            relationship steps. Broad country claims cannot substitute for a
            specific supply shock.
          </p>
          <div className="method">
            <h3>Corroboration</h3>
            <p>
              Two independent publishers with distinct evidence, or one
              qualifying tier-one asserted source. Duplicates and unresolved
              aggregator URLs do not qualify. Contradictions can invalidate an
              open trade.
            </p>
          </div>
        </Panel>
        <Panel title="Execution & measurement" body>
          <div className="method">
            <h3>Paper broker only</h3>
            <p>
              IBKR paper account. Entries use IOC limit orders, exits use market
              orders in the US session. Software stops cannot guarantee a
              maximum loss. Existing unrelated holdings are kept separate.
            </p>
          </div>
          <div className="method">
            <h3>No invented performance</h3>
            <p>
              Research observations use wall-clock horizons and event weighting.
              Broker results require fills. Missing prices remain ungraded, and
              delayed quotes remain labelled.
            </p>
          </div>
        </Panel>
      </div>
    </>
  );
}
export default function Workspace() {
  const q = useDesk();
  return (
    <ErrorBoundary>
      <Shell
        name="Prometheus"
        subtitle="Event trading"
        nav={nav}
        updated={q.dataUpdatedAt}
        status={
          <Badge tone={q.isError ? "bad" : "accent"}>
            <span className="dot" />
            {q.isError ? "Connection issue" : "Paper experiment"}
          </Badge>
        }
      >
        <Routes>
          <Route path="/" element={<Overview q={q} />} />
          <Route path="/opportunities" element={<Opportunities q={q} />} />
          <Route path="/trades" element={<Opportunities q={q} trades />} />
          <Route path="/account" element={<Account />} />
          <Route path="/research" element={<Results q={q} />} />
          <Route path="/events" element={<Events q={q} />} />
          <Route path="/operations" element={<Operations q={q} />} />
          <Route path="/iris" element={<Chat />} />
          <Route path="/method" element={<Method q={q} />} />
          {["dashboard", "portfolio", "execution", "monitor", "options"].map(
            (p) => (
              <Route
                key={p}
                path={p}
                element={
                  <Navigate
                    replace
                    to={
                      p === "dashboard"
                        ? "/"
                        : p === "portfolio"
                          ? "/account"
                          : "/trades"
                    }
                  />
                }
              />
            ),
          )}
          {["backtests", "drift"].map((p) => (
            <Route
              key={p}
              path={p}
              element={<Navigate replace to="/research" />}
            />
          ))}
          <Route
            path="intelligence"
            element={<Navigate replace to="/events" />}
          />
          <Route
            path="diagnostics"
            element={<Navigate replace to="/operations" />}
          />
          <Route path="logs" element={<Navigate replace to="/operations" />} />
          <Route
            path="settings/*"
            element={<Navigate replace to="/method" />}
          />
          <Route path="docs/*" element={<Navigate replace to="/method" />} />
          <Route
            path="*"
            element={
              <Empty title="Page not found">
                <Link className="text-link" to="/">
                  Return to the event desk
                </Link>
              </Empty>
            }
          />
        </Routes>
        <footer className="page-footer">
          Prometheus · Event-driven paper research · All timestamps shown in
          your local timezone
        </footer>
      </Shell>
    </ErrorBoundary>
  );
}
