from __future__ import annotations

import math
import os
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from psycopg2.extras import Json

from .model import PORTFOLIO, Policy, entry_signal, stable_id, trade_plan
from .storage import Store

TERMINAL = {"FILLED", "CANCELLED", "REJECTED"}
ACTIVE = {"OPEN", "ENTRY_PENDING", "EXIT_PENDING", "REVIEW"}
_graph_cache = None
_graph_loaded_at = 0.0


def current_graph():
    global _graph_cache, _graph_loaded_at
    if _graph_cache is None or time.monotonic() - _graph_loaded_at >= 900:
        from apatheon.graph.bootstrap import bootstrap_graph

        _graph_cache = bootstrap_graph()
        _graph_loaded_at = time.monotonic()
    return _graph_cache


def utcnow():
    return datetime.now(timezone.utc)


def parsed(value):
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def lifecycle(c, fills, orders, broker_states, now, policy):
    p = c["payload"]
    entries = [f for f in fills if f["leg"] == "ENTRY"]
    exits = [f for f in fills if f["leg"] == "EXIT"]
    eq = sum(float(f["quantity"]) for f in entries)
    xq = sum(float(f["quantity"]) for f in exits)
    p["filled_quantity"] = eq
    p["remaining_quantity"] = eq - xq
    if xq > eq + 1e-6:
        c.update(state="REVIEW", reason="Exit fills exceed owned entry fills")
        return
    if entries:
        p["entry_vwap"] = sum(float(f["price"]) * float(f["quantity"]) for f in entries) / eq
        p["entered_at"] = min(f["timestamp"] for f in entries).isoformat()
    for order in orders:
        broker_status = broker_states.get(order["order_id"])
        status = broker_status or order["status"]
        known_quantity = sum(float(f["quantity"]) for f in fills if f["order_id"] == order["order_id"])
        if status == "FILLED" and known_quantity < float(order["quantity"]) - 1e-6:
            c.update(state="REVIEW", reason="Broker fill confirmed but execution ledger is incomplete")
            return
        if status not in TERMINAL:
            c.update(
                state="ENTRY_PENDING" if order["metadata"]["event_leg"] == "ENTRY" else "EXIT_PENDING",
                reason="Awaiting broker confirmation; resubmission blocked",
            )
            if now - parsed(order["timestamp"]) > timedelta(minutes=10):
                c.update(state="REVIEW", reason="Unresolved broker order; entries paused until reconciled")
            return
    if p.get("entry_order") and not orders:
        c.update(state="REVIEW", reason="Persisted intent has no order record; inspect before retry")
    elif eq and abs(eq - xq) < 1e-6:
        gross = int(p["plan"]["side"]) * (
            sum(float(f["price"]) * float(f["quantity"]) for f in exits)
            - sum(float(f["price"]) * float(f["quantity"]) for f in entries)
        )
        fees = sum(
            float(f["commission"])
            for f in fills
            if f["commission"] is not None
            and math.isfinite(float(f["commission"]))
            and 0 <= float(f["commission"]) <= 100
        )
        estimated_fees = sum(
            max(
                policy.min_commission,
                sum(float(f["quantity"]) for f in fills if f["order_id"] == o["order_id"])
                * policy.commission_per_share,
            )
            for o in orders
            if any(f["order_id"] == o["order_id"] for f in fills)
        )
        fee_estimate = max(0, estimated_fees - fees)
        exit_time = max(f["timestamp"] for f in exits)
        hours = max(0, (exit_time - parsed(p["entered_at"])).total_seconds() / 3600)
        borrow = (
            (eq * p["entry_vwap"] * policy.short_borrow_annual * hours / (365 * 24)) if p["plan"]["side"] < 0 else 0
        )
        p.update(
            gross_pnl=gross,
            recorded_commissions=fees,
            commission_reserve=fee_estimate,
            estimated_borrow=borrow,
            net_pnl=gross - fees - fee_estimate - borrow,
            accounting_kind="broker_fills_with_cost_reserves",
            cash_flow_scope="Trade fills only; excludes dividends, corporate actions and financing cash postings",
            closed_at=exit_time.isoformat(),
        )
        c.update(state="CLOSED", reason=p.get("exit_reason", "Closed"))
    elif eq:
        c.update(state="OPEN", reason="Managing broker-confirmed position")
    elif orders:
        c.update(state="NO_FILL", reason="Entry completed without a fill")


def observe(store, c, quote, benchmark, now):
    p = c["payload"]
    if "baseline" not in p:
        p["baseline"] = {
            "price": quote.mid,
            "benchmark": benchmark.mid,
            "at": now.isoformat(),
            "quote": quote.payload(),
            "benchmark_quote": benchmark.payload(),
        }
    baseline = p["baseline"]
    if baseline["quote"]["market_data_type"] != quote.market_data_type:
        c["reason"] = "Market data mode changed; waiting for comparable observations"
        return None
    move = quote.mid / baseline["price"] - 1
    market = benchmark.mid / baseline["benchmark"] - 1
    age = (now - parsed(baseline["at"])).total_seconds() / 3600
    p["peak_signed_move"] = max(p.get("peak_signed_move", 0), p["sign"] * move)
    mark = {
        "quote": quote.payload(),
        "benchmark_quote": benchmark.payload(),
        "return": move,
        "benchmark_return": market,
        "excess_return": move - market,
        "elapsed_hours": age,
    }
    p["last_mark"] = mark
    store.query(
        "INSERT INTO event_desk_marks VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
        (c["candidate_id"], now, Json(mark)),
        True,
    )
    for horizon in (1, 4, 24, 72):
        if age < horizon:
            continue
        # Only grade a horizon when the observation is near it. Closed-market gaps stay visible.
        if age - horizon > 1:
            continue
        side = p["sign"] if c["strategy"] == "CONTINUATION" else -p["sign"]
        outcome = {
            **mark,
            "signed_return": side * move,
            "signed_excess_return": side * (move - market),
            "evaluation_kind": "forward_research",
            "event_id": c["event_id"],
        }
        store.query(
            "INSERT INTO event_desk_outcomes VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (c["candidate_id"], horizon, now, Json(outcome)),
            True,
        )
    return move, move - market, age * 60


def submit_leg(store, session, c, plan, leg, now):
    from prometheus.execution.broker_interface import Order, OrderSide, OrderTimeInForce, OrderType
    from prometheus.execution.storage import record_orders, update_order_statuses

    p = c["payload"]
    attempt = (
        0
        if leg == "ENTRY"
        else len([o for o in store.order_rows(c["candidate_id"]) if o["metadata"]["event_leg"] == "EXIT"])
    )
    oid = "EV_" + stable_id(c["candidate_id"], leg, attempt)
    order = Order(
        oid,
        c["instrument_id"],
        OrderSide.BUY if plan["side"] > 0 else OrderSide.SELL,
        OrderType.LIMIT if leg == "ENTRY" else OrderType.MARKET,
        plan["quantity"],
        limit_price=round(plan["entry_limit"], 2) if leg == "ENTRY" else None,
        tif=OrderTimeInForce.IOC if leg == "ENTRY" else OrderTimeInForce.DAY,
        metadata={
            "candidate_id": c["candidate_id"],
            "event_id": c["event_id"],
            "event_leg": leg,
            "strategy": c["strategy"],
            "version": c["version"],
        },
    )
    c.update(
        state="ENTRY_PENDING" if leg == "ENTRY" else "EXIT_PENDING", reason="Intent persisted before broker submission"
    )
    p["entry_order" if leg == "ENTRY" else "exit_order"] = oid
    store.save(c, now)
    record_orders(store.db, portfolio_id=PORTFOLIO, orders=[order], mode="PAPER")
    status = session.submit(order)
    update_order_statuses(store.db, statuses={oid: status})
    session.reconcile(store.db)
    states = session.order_states()
    store.sync_order_states(states)
    lifecycle(
        c,
        store.fills(c["candidate_id"]),
        store.order_rows(c["candidate_id"]),
        states,
        utcnow(),
        Policy(),
    )
    store.save(c, utcnow())


def process(store, session, now, policy, allow_entries=True):
    candidates = store.candidates(False)
    states = session.order_states()
    store.sync_order_states(states)
    for c in candidates:
        if c["state"] in ACTIVE or (c["state"] == "CLOSED" and now - c["updated_at"] < timedelta(days=7)):
            lifecycle(c, store.fills(c["candidate_id"]), store.order_rows(c["candidate_id"]), states, now, policy)
            store.save(c, now)
    active = [c for c in candidates if c["state"] in ACTIVE]
    watchers = [
        c
        for c in candidates
        if c["state"] == "WATCHING" and now - c["first_seen"] <= timedelta(hours=policy.entry_age_hours)
    ]
    for c in candidates:
        if c["state"] == "WATCHING" and now - c["first_seen"] > timedelta(hours=policy.entry_age_hours):
            c.update(state="EXPIRED", reason="Entry window expired")
            store.save(c, now)
    research = [
        c
        for c in candidates
        if c["state"] in ("CLOSED", "NO_FILL", "EXPIRED")
        and "baseline" in c["payload"]
        and now - parsed(c["payload"]["baseline"]["at"]) <= timedelta(hours=73)
    ]
    watchers.sort(key=lambda c: c["payload"].get("last_mark", {}).get("quote", {}).get("observed_at", ""))
    watched = active + watchers + research
    symbols = list(dict.fromkeys(c["instrument_id"] for c in watched))[:30]
    quotes = session.quotes(symbols + ["SPY.US"])
    now = max(now, utcnow())
    liquidity_error = None
    try:
        liquid = store.liquid_instruments(symbols, now)
    except Exception as exc:
        liquid = set()
        liquidity_error = f"Liquidity history unavailable: {type(exc).__name__}"
    benchmark = quotes.get("SPY.US")
    positions = session.positions()
    gross = sum(
        max(
            c["payload"].get("remaining_quantity", 0),
            c["payload"].get("plan", {}).get("quantity", 0) if c["state"] != "OPEN" else 0,
        )
        * c["payload"].get("entry_vwap", c["payload"].get("plan", {}).get("entry_limit", 0))
        for c in active
    )
    net_pnl = sum(c["payload"].get("net_pnl", 0) for c in candidates if c["state"] == "CLOSED")
    for c in active:
        p = c["payload"]
        q = quotes.get(c["instrument_id"])
        if q and q.valid(now, float("inf")) and p.get("entry_vwap"):
            net_pnl += p["plan"]["side"] * p.get("remaining_quantity", 0) * (q.mid - p["entry_vwap"])
    previous = store.query("SELECT max((payload->>'peak_pnl')::float) AS peak FROM event_desk_runs")
    peak = max(0, net_pnl, float(previous[0]["peak"] or 0))
    halted = peak - net_pnl >= policy.drawdown_limit
    unresolved = any(c["state"] in ("REVIEW", "ENTRY_PENDING", "EXIT_PENDING") for c in active)
    unpriced = any(
        not quotes.get(c["instrument_id"]) or not quotes[c["instrument_id"]].valid(now, policy.max_spread_bps)
        for c in active
    )
    submissions = 0
    for c in watched:
        p = c["payload"]
        q = quotes.get(c["instrument_id"])
        # Exits never depend on benchmark availability or evidence confirmation.
        if c["state"] == "OPEN":
            side = p["plan"]["side"]
            px = (q.bid if side > 0 else q.ask) if q and q.valid(now, float("inf")) else None
            broker_quantity = getattr(positions.get(c["instrument_id"]), "quantity", 0)
            if side * broker_quantity < p["remaining_quantity"] - 1e-6:
                c.update(state="REVIEW", reason="Broker holdings disagree with event fill ledger")
                store.save(c, now)
                unresolved = True
                continue
            ret = side * (px / p["entry_vwap"] - 1) if px is not None else None
            age = (now - parsed(p["entered_at"])).total_seconds() / 3600
            ev = c["event"]["evidence_state"]
            current_path = c["event"].get("exposures", {}).get(p.get("company"))
            mapping_invalid = "exposures" in c["event"] and (not current_path or current_path["sign"] != p["sign"])
            invalid = (
                mapping_invalid
                or (c["strategy"] == "CONTINUATION" and ev["contradicted"])
                or (c["strategy"] == "REVERSAL" and ev["confirmed"])
            )
            reason = (
                "Book drawdown stop"
                if halted
                else "Evidence invalidated"
                if invalid
                else "Stop loss"
                if ret is not None and ret <= -p["plan"]["stop_fraction"]
                else "Profit target"
                if ret is not None and ret >= p["plan"]["target_fraction"]
                else "Time exit"
                if age >= p["plan"]["hold_hours"]
                else None
            )
            if reason:
                p["exit_reason"] = reason
                submit_leg(
                    store,
                    session,
                    c,
                    {"side": -side, "quantity": p["remaining_quantity"], "entry_limit": px},
                    "EXIT",
                    now,
                )
                submissions += 1
                continue
        if not q or not q.valid(now, policy.max_spread_bps):
            c["reason"] = "Waiting for a valid bid/ask quote"
            store.save(c, now)
            continue
        if (
            not benchmark
            or not benchmark.valid(now, policy.max_spread_bps)
            or benchmark.market_data_type != q.market_data_type
        ):
            c["reason"] = "Waiting for benchmark quote"
            store.save(c, now)
            continue
        observation = observe(store, c, q, benchmark, now)
        if c["state"] != "WATCHING" or observation is None:
            store.save(c, now)
            continue
        move, excess, age = observation
        side, reason = entry_signal(
            c["strategy"], p["sign"], c["event"]["evidence_state"], move, excess, p["peak_signed_move"], age
        )
        c["reason"] = reason
        if side:
            plan = trade_plan(c["strategy"], side, q, policy)
            p["proposed_plan"] = plan
            notional = plan["quantity"] * plan["entry_limit"]
            event_gross = sum(
                a["payload"].get("plan", {}).get("quantity", 0) * a["payload"].get("plan", {}).get("entry_limit", 0)
                for a in active
                if a["event_id"] == c["event_id"]
            )
            occupied = (
                any(a["instrument_id"] == c["instrument_id"] for a in active)
                or abs(getattr(positions.get(c["instrument_id"]), "quantity", 0)) > 1e-6
            )
            if not allow_entries:
                c["reason"] = "Paper entry switch is off"
            elif halted:
                c["reason"] = "Book drawdown limit reached"
            elif unresolved:
                c["reason"] = "Unresolved order blocks new entries"
            elif unpriced:
                c["reason"] = "Existing exposure lacks a valid quote"
            elif occupied:
                c["reason"] = "Instrument already held or reserved"
            elif c["instrument_id"] not in liquid:
                c["reason"] = "Requires recent USD price history and $5m average daily turnover"
            elif (
                len(active) >= policy.max_positions
                or gross + notional > policy.max_gross
                or event_gross + notional > policy.max_event
            ):
                c["reason"] = "Paper book or event exposure limit"
            elif side < 0 and q.shortable < plan["quantity"]:
                c["reason"] = "Borrow availability not confirmed"
            elif plan["quantity"] <= 0 or plan["target_payoff_after_estimated_cost"] <= 0:
                c["reason"] = "Trade does not clear estimated costs"
            else:
                p["plan"] = plan
                p["entry_evidence"] = c["event"]
                p["entry_quote"] = q.payload()
                p["benchmark_at_entry"] = benchmark.mid
                submit_leg(store, session, c, plan, "ENTRY", now)
                submissions += 1
                gross += notional
                active.append(c)
                unresolved = c["state"] in ("REVIEW", "ENTRY_PENDING", "EXIT_PENDING")
        store.save(c, now)
    return {
        "observed_instruments": len(quotes),
        "submissions": submissions,
        "gross_reserved": gross,
        "net_pnl": net_pnl,
        "peak_pnl": peak,
        "drawdown_halt": halted,
        "unresolved_orders": unresolved,
        "liquidity_error": liquidity_error,
    }


def run_tick(db, *, now=None, session_factory=None):
    from apatheon.core.market_state import MarketState, get_market_state

    from .broker import PaperSession

    now = now or utcnow()
    store = Store(db)
    # A transaction lock works through PgBouncer and fences manual invocations too.
    with db.get_runtime_connection() as lock_conn:
        with lock_conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_xact_lock(710109)")
            if not cursor.fetchone()[0]:
                return {"skipped": "Another event tick is active"}
            try:
                summary = {
                    "mode": "PAPER",
                    "policy": asdict(Policy()),
                    "status": "RUNNING",
                    "paper_entries_enabled": os.environ.get("PROMETHEUS_EVENT_PAPER_ENABLED") == "1",
                }
                store.query("INSERT INTO event_desk_runs VALUES(%s,NULL,%s)", (now, Json(summary)), True)
                try:
                    summary.update(store.ingest(current_graph(), now))
                except Exception as exc:
                    summary["ingestion_error"] = f"Event ingestion unavailable: {type(exc).__name__}: {str(exc)[:300]}"
                market_state = get_market_state("US_EQ", now)
                summary["market_state"] = market_state.value
                if market_state == MarketState.SESSION:
                    with (session_factory or PaperSession)() as session:
                        session.reconcile(db)
                        summary.update(
                            process(
                                store,
                                session,
                                utcnow(),
                                Policy(),
                                allow_entries=summary["paper_entries_enabled"] and not summary.get("ingestion_error"),
                            )
                        )
                summary["error"] = summary.get("ingestion_error") or summary.get("liquidity_error")
                summary["status"] = "DEGRADED" if summary["error"] else "OK"
                store.query(
                    "UPDATE event_desk_runs SET completed_at=%s,payload=%s WHERE started_at=%s",
                    (utcnow(), Json(summary), now),
                    True,
                )
                return summary
            except Exception as exc:
                store.query(
                    "UPDATE event_desk_runs SET completed_at=%s,payload=payload || %s WHERE started_at=%s",
                    (utcnow(), Json({"status": "ERROR", "error": str(exc)[:500]}), now),
                    True,
                )
                raise
            finally:
                lock_conn.commit()


if __name__ == "__main__":
    import json

    from apatheon.core.database import get_db_manager

    print(json.dumps(run_tick(get_db_manager()), default=str))
