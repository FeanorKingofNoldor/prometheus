"""Core+wheel daily runner — the I/O shell around ``planner.build_plan``.

Runs as the ``run_wheel`` daemon job at POST_CLOSE (~16:01 ET): SPY /
VIX closes are final (exactly the inputs the strategy was validated
on) and SPY options quote until 16:15 ET, so limit-at-mid fills are
still live. Fully self-contained: needs no pipeline phase, no
universes, no books — just the gateway, the wheel config, and the DB.

Submission is gated on ``PROMETHEUS_WHEEL_ENABLED`` alone. Deliberately
independent of ``PROMETHEUS_EXECUTION_HALT``: the halt flag is the
*legacy* retirement switch (it keeps the V12 equity execution and the
old options pipeline short-circuited permanently) — coupling the wheel
to it would force waking the retired paths just to trade the wheel.
To emergency-stop the wheel, unset ``PROMETHEUS_WHEEL_ENABLED``.
While unset, every run is a shadow: full plan persisted to
``engine_decisions`` (engine_name=WHEEL), zero orders.

Error taxonomy matches ``run_derivatives_daily``: ``errors`` = fatal
pre-submission (daemon may retry safely); ``warnings`` = anything at or
after submission (never retried).
"""

from __future__ import annotations

import math
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional

from apatheon.core.logging import get_logger

from prometheus.wheel.config import WheelStrategyConfig, load_wheel_config
from prometheus.wheel.planner import (
    OpenShortOptionView,
    PlannedOrder,
    WheelAccountView,
    WheelPlan,
    build_plan,
)

logger = get_logger(__name__)

WHEEL_CLIENT_ID = 16

# Give-up thresholds for the limit-at-mid walk.
_WALK_FIRST_WAIT_S = 60.0     # rest at mid before conceding
_WALK_SECOND_WAIT_S = 120.0   # rest at the touch before giving up the day

_STRATEGY_BY_CATEGORY = {
    "csp": "wheel.csp",
    "cc": "wheel.cc",
    "profit_take": "wheel.btc",
}


def _record_warning(summary: dict[str, Any], label: str, exc: BaseException) -> None:
    logger.error("run_wheel [%s] failed (non-fatal): %s", label, exc, exc_info=True)
    summary.setdefault("warnings", []).append(f"{label}: {exc}")


def _f(x: Any) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    return v if math.isfinite(v) else 0.0


# ── Market-data helpers ──────────────────────────────────────────────


def _snapshot_quote(ib: Any, contract: Any, wait_s: float = 2.0) -> tuple[float, float, float]:
    """Return (bid, ask, last_or_close) for a qualified contract.

    Tries live data first, then delayed (``reqMarketDataType(3)``) — the
    paper account has no live equity/OPRA API entitlement (error 10089),
    same fallback the legacy derivatives pipeline uses. Snapshot requests
    auto-cancel; no ``cancelMktData`` needed.
    """
    for mdt in (1, 3):
        try:
            ib.reqMarketDataType(mdt)
        except Exception:
            pass
        ticker = ib.reqMktData(contract, "", snapshot=True)
        ib.sleep(wait_s)
        bid = _f(getattr(ticker, "bid", 0))
        ask = _f(getattr(ticker, "ask", 0))
        last = _f(getattr(ticker, "last", 0)) or _f(getattr(ticker, "close", 0))
        if bid > 0 or ask > 0 or last > 0:
            return bid, ask, last
    return 0.0, 0.0, 0.0


def _db_latest_close(db_manager: Any, instrument_id: str) -> float:
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT close FROM prices_daily WHERE instrument_id = %s "
                "ORDER BY trade_date DESC LIMIT 1",
                (instrument_id,),
            )
            row = cur.fetchone()
    return _f(row[0]) if row else 0.0


def _spot_and_vix(ib: Any, db_manager: Any, cfg: WheelStrategyConfig) -> tuple[float, float]:
    """Underlying spot + VIX, live quote first, DB close fallback."""
    from prometheus.execution.ib_compat import Index, Stock

    spot = 0.0
    try:
        stk = Stock(cfg.underlying_symbol, "SMART", "USD")
        if ib.qualifyContracts(stk):
            bid, ask, last = _snapshot_quote(ib, stk)
            spot = last or ((bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0)
    except Exception as exc:
        logger.warning("run_wheel: live spot lookup failed: %s", exc)
    if spot <= 0:
        spot = _db_latest_close(db_manager, cfg.underlying_instrument_id)

    vix = 0.0
    try:
        idx = Index("VIX", "CBOE")
        if ib.qualifyContracts(idx):
            _, _, last = _snapshot_quote(ib, idx)
            vix = last
    except Exception as exc:
        logger.warning("run_wheel: live VIX lookup failed: %s", exc)
    if vix <= 0:
        vix = _db_latest_close(db_manager, "VIX.INDX")

    return spot, vix


# ── State reconstruction ─────────────────────────────────────────────


def _load_managed_flags(
    db_manager: Any, *, portfolio_id: str, mode: str, today: date,
) -> dict[tuple[str, str, float], bool]:
    """(right, expiryYYYYMMDD, strike) → managed, from SUBMIT events.

    Later submissions win. Contracts the wheel never submitted (manual
    trades) default to unmanaged — hold-to-expiry is the safe base rule.
    """
    out: dict[tuple[str, str, float], bool] = {}
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT "right", expiry, strike, metadata_json
                FROM options_position_events
                WHERE portfolio_id = %s AND mode = %s AND event_type = 'SUBMIT'
                  AND as_of_date >= %s
                ORDER BY event_at
                """,
                (portfolio_id, mode, today - timedelta(days=90)),
            )
            for right, expiry, strike, meta in cur.fetchall():
                managed = bool((meta or {}).get("managed", False))
                out[(str(right).upper(), str(expiry)[:8], float(strike))] = managed
    return out


def _build_account_view(
    ib: Any,
    db_manager: Any,
    cfg: WheelStrategyConfig,
    *,
    mode: str,
    today: date,
) -> WheelAccountView:
    """Distill broker truth into the planner's input."""
    # Account values — brief pause so IBKR streams them post-connect.
    ib.sleep(2)
    account: dict[str, str] = {}
    for av in ib.accountValues():
        if av.currency in ("USD", "BASE", ""):
            account[av.tag] = av.value
    nav = _f(account.get("NetLiquidation") or account.get("NetLiquidationByCurrency"))
    total_cash = _f(account.get("TotalCashValue"))

    portfolio_mv: dict[int, float] = {}
    for item in ib.portfolio():
        con_id = getattr(item.contract, "conId", None)
        if con_id:
            portfolio_mv[con_id] = _f(getattr(item, "marketValue", 0))

    managed_flags = _load_managed_flags(
        db_manager, portfolio_id=cfg.portfolio_id, mode=mode, today=today,
    )

    symbol = cfg.underlying_symbol
    # Symbol → canonical leg id; covers the US originals AND their UCITS
    # twins (the broker holds DTLA, the planner thinks in TLT.US). The
    # generic mapper can't do this — it renders every stock as SYMBOL.US.
    ballast_symbols = cfg.ballast_symbol_map
    shares = 0
    short_puts: list[OpenShortOptionView] = []
    short_calls: list[OpenShortOptionView] = []
    ballast_values: dict[str, float] = {}

    for p in ib.positions():
        contract = p.contract
        sec_type = getattr(contract, "secType", "")
        qty = _f(p.position)
        if sec_type == "STK":
            con_symbol = getattr(contract, "symbol", "")
            if con_symbol == symbol:
                shares += int(qty)
            elif con_symbol in ballast_symbols:
                iid = ballast_symbols[con_symbol]
                mv = portfolio_mv.get(getattr(contract, "conId", None) or -1, 0.0)
                ballast_values[iid] = ballast_values.get(iid, 0.0) + (
                    mv or qty * _f(p.avgCost)
                )
        elif sec_type == "OPT" and getattr(contract, "symbol", "") == symbol and qty < 0:
            expiry_raw = str(getattr(contract, "lastTradeDateOrContractMonth", ""))[:8]
            try:
                expiry_d = datetime.strptime(expiry_raw, "%Y%m%d").date()
            except ValueError:
                logger.warning("run_wheel: unparseable option expiry %r — skipped", expiry_raw)
                continue
            mult = int(_f(getattr(contract, "multiplier", 100)) or 100)
            right = str(getattr(contract, "right", "")).upper()[:1]
            view = OpenShortOptionView(
                right=right,
                strike=_f(getattr(contract, "strike", 0)),
                expiry=expiry_d,
                contracts=int(abs(qty)),
                credit_per_share=_f(p.avgCost) / mult,
                managed=managed_flags.get(
                    (right, expiry_raw, _f(getattr(contract, "strike", 0))), False,
                ),
            )
            (short_puts if right == "P" else short_calls).append(view)

    # Marks for managed short puts (profit-take inputs) — a few snapshots.
    priced_puts: list[OpenShortOptionView] = []
    for put in short_puts:
        if not put.managed:
            priced_puts.append(put)
            continue
        mark: Optional[float] = None
        try:
            from prometheus.execution.ib_compat import Option

            opt = Option(
                symbol=symbol,
                lastTradeDateOrContractMonth=put.expiry.strftime("%Y%m%d"),
                strike=put.strike,
                right=put.right,
                exchange="SMART",
            )
            if ib.qualifyContracts(opt):
                bid, ask, last = _snapshot_quote(ib, opt)
                if bid > 0 and ask > 0:
                    mark = (bid + ask) / 2.0
                elif last > 0:
                    mark = last
        except Exception as exc:
            logger.warning("run_wheel: mark lookup failed for %s: %s", put, exc)
        priced_puts.append(
            OpenShortOptionView(
                right=put.right, strike=put.strike, expiry=put.expiry,
                contracts=put.contracts, credit_per_share=put.credit_per_share,
                managed=put.managed, mark_per_share=mark,
            )
        )

    spot, vix = _spot_and_vix(ib, db_manager, cfg)

    ballast_prices = {
        leg.instrument_id: _db_latest_close(db_manager, leg.instrument_id)
        for leg in cfg.ballast
    }

    return WheelAccountView(
        nav=nav,
        total_cash=total_cash,
        underlying_shares=shares,
        underlying_spot=spot,
        vix=vix,
        short_puts=tuple(priced_puts),
        short_calls=tuple(short_calls),
        ballast_values=ballast_values,
        ballast_prices=ballast_prices,
        peak_nav=_peak_nav(db_manager, nav),
    )


def _peak_nav(db_manager: Any, nav: float) -> float:
    """High-water mark since the last account reset (breaker basis)."""
    peak = 0.0
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT reset_at, starting_nav_usd FROM account_resets "
                "ORDER BY reset_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            reset_at, starting = (row[0], _f(row[1])) if row else (None, 0.0)
            peak = starting
            try:
                if reset_at is not None:
                    cur.execute(
                        "SELECT COALESCE(MAX(nav), 0) FROM portfolio_equity_history "
                        "WHERE as_of_date >= %s",
                        (reset_at.date() if hasattr(reset_at, "date") else reset_at,),
                    )
                    peak = max(peak, _f(cur.fetchone()[0]))
            except Exception:
                conn.rollback()  # table/column optional — starting NAV suffices
    return max(peak, nav)


def _ballast_rebalance_due(db_manager: Any, *, portfolio_id: str, mode: str, today: date) -> bool:
    """Quarterly: first wheel run of Jan/Apr/Jul/Oct that hasn't rebalanced yet."""
    if today.month not in (1, 4, 7, 10):
        return False
    month_start = today.replace(day=1)
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM orders
                WHERE portfolio_id = %s AND UPPER(mode) = %s
                  AND metadata->>'category' = 'ballast'
                  AND timestamp >= %s
                LIMIT 1
                """,
                (portfolio_id, mode, month_start),
            )
            return cur.fetchone() is None


def _existing_order_ids(db_manager: Any, order_ids: list[str], mode: str) -> set[str]:
    """Refs from THIS cycle already recorded (crash-retry idempotency)."""
    if not order_ids:
        return set()
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            # CANCELLED and REJECTED attempts never rested at the broker —
            # a rerun may retry them (same deterministic ref; the orders
            # row insert is ON CONFLICT DO NOTHING and status is updated
            # by ref as the retry settles).
            cur.execute(
                "SELECT order_id FROM orders WHERE order_id = ANY(%s) "
                "AND UPPER(mode) = %s AND status NOT IN ('CANCELLED', 'REJECTED')",
                (order_ids, mode),
            )
            return {str(r[0]) for r in cur.fetchall()}


def _update_order_status(db_manager: Any, order_id: str, status: str) -> None:
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (status, order_id),
            )
        conn.commit()


# ── Contract resolution + refs ───────────────────────────────────────


def _pin_expiry(discovery: Any, symbol: str, target: date, today: date) -> str | None:
    """Resolve the engine's target Friday to a listed expiration."""
    chains = discovery.discover_option_chain(symbol)
    if not chains:
        return None
    chain = chains[0]
    target_dte = (target - today).days
    for lo, hi in ((target_dte - 3, target_dte + 10), (target_dte - 7, target_dte + 21)):
        expirations = chain.filter_expirations(max(lo, 1), hi, today=today)
        if expirations:
            return min(
                expirations,
                key=lambda e: abs(
                    (datetime.strptime(e[:8], "%Y%m%d").date() - target).days
                ),
            )
    return None


def _option_instrument_id(symbol: str, expiry: str, strike: float, right: str) -> str:
    strike_txt = f"{strike:g}"
    return f"{symbol}_{expiry[2:8]}_{strike_txt}{right}.US"


def _option_order_ref(
    po: PlannedOrder, *, portfolio_id: str, symbol: str, expiry: str, today: date,
) -> str:
    from prometheus.derivatives.order_refs import deterministic_option_order_ref

    return deterministic_option_order_ref(
        portfolio_id=portfolio_id,
        strategy=_STRATEGY_BY_CATEGORY[po.category],
        underlying=symbol,
        right=po.right,
        expiry=expiry,
        strike=po.strike,
        side=po.side,
        as_of_date=today,
    )


# ── Submission ───────────────────────────────────────────────────────


class _WorkingOrder:
    """One live order being walked from mid toward the touch."""

    def __init__(self, *, planned: PlannedOrder, contract: Any, trade: Any,
                 order_ref: str, instrument_id: str, mid: float,
                 bid: float, ask: float) -> None:
        self.planned = planned
        self.contract = contract
        self.trade = trade
        self.order_ref = order_ref
        self.instrument_id = instrument_id
        self.mid = mid
        self.bid = bid
        self.ask = ask
        self.walked = False
        self.outcome = "working"   # filled | cancelled | working

    @property
    def status(self) -> str:
        return str(getattr(getattr(self.trade, "orderStatus", None), "status", "") or "")

    @property
    def is_filled(self) -> bool:
        return self.status == "Filled"

    @property
    def is_dead(self) -> bool:
        return self.status in ("Cancelled", "ApiCancelled", "Inactive")


def _submit_plan(
    ib: Any,
    db_manager: Any,
    cfg: WheelStrategyConfig,
    plan: WheelPlan,
    *,
    mode: str,
    today: date,
    spot: float,
    summary: dict[str, Any],
) -> None:
    """Place, walk, and settle every planned order. Post-submission zone."""
    from prometheus.execution.broker_interface import (
        Order,
        OrderSide,
        OrderType,
    )
    from prometheus.execution.contract_discovery import ContractDiscoveryService
    from prometheus.execution.executed_actions import (
        ExecutedActionContext,
        record_executed_actions_for_fills,
    )
    from prometheus.execution.fill_reconciliation import _fill_from_ib
    from prometheus.execution.ib_compat import LimitOrder, Option, Stock
    from prometheus.execution.options_storage import record_order_submission
    from prometheus.execution.order_planner import deterministic_order_id
    from prometheus.execution.storage import record_fills, record_orders

    discovery = ContractDiscoveryService(ib)
    symbol = cfg.underlying_symbol
    concession_cap = spot * cfg.limit_walk_bps / 10_000.0

    working: list[_WorkingOrder] = []
    submitted_rows: list[Order] = []

    # Resolve refs up front so the idempotency check covers everything.
    resolved: list[tuple[PlannedOrder, str, str, str]] = []  # (po, ref, expiry, iid)
    for po in plan.orders:
        if po.category == "ballast":
            sub = cfg.ballast_substitute(po.instrument_id)
            iid = sub.instrument_id if sub else po.instrument_id
            side = OrderSide.BUY if po.side == "BUY" else OrderSide.SELL
            ref = deterministic_order_id(cfg.portfolio_id, iid, side, today)
            resolved.append((po, ref, "", iid))
            continue
        expiry = (
            _pin_expiry(discovery, symbol, po.target_expiry, today)
            if po.category in ("csp", "cc") and po.target_expiry
            else (po.target_expiry.strftime("%Y%m%d") if po.target_expiry else None)
        )
        if not expiry:
            summary.setdefault("warnings", []).append(
                f"{po.category}: no listed expiration near {po.target_expiry}"
            )
            continue
        iid = _option_instrument_id(symbol, expiry, po.strike, po.right)
        ref = _option_order_ref(
            po, portfolio_id=cfg.portfolio_id, symbol=symbol, expiry=expiry, today=today,
        )
        resolved.append((po, ref, expiry, iid))

    already = _existing_order_ids(db_manager, [r[1] for r in resolved], mode)

    for po, ref, expiry, iid in resolved:
        if ref in already:
            logger.warning(
                "run_wheel: %s %s already recorded this cycle (ref=%s) — skipping",
                po.category, iid, ref,
            )
            continue

        try:
            if po.category == "ballast":
                sub = cfg.ballast_substitute(po.instrument_id)
                if sub is not None:
                    # SMART routing with a primaryExchange hint: the bare
                    # ticker is ambiguous to SMART, but direct routing to
                    # LSEETF trips the gateway's API precautionary block
                    # (error 10311, instant cancel — bitten 2026-08-07).
                    contract = Stock(
                        sub.symbol, "SMART", sub.currency, primaryExchange=sub.exchange
                    )
                else:
                    contract = Stock(iid.split(".")[0], "SMART", "USD")
                if not ib.qualifyContracts(contract):
                    summary.setdefault("warnings", []).append(f"ballast: cannot qualify {iid}")
                    continue
                # Price off a live/delayed quote, not the (possibly stale)
                # DB close in limit_hint. The order rests to the next open,
                # so pad 0.5% in the fill direction; a bigger overnight gap
                # leaves it unfilled and the bootstrap re-plans tomorrow.
                bid, ask, last = _snapshot_quote(ib, contract)
                base = last or ((bid + ask) / 2.0 if bid > 0 and ask > 0 else 0.0)
                quantity = po.quantity
                if sub is not None:
                    # The planner sized in shares of the US original
                    # (limit_hint = its price). Re-size to the twin's own
                    # quote so the DOLLAR notional is preserved — twin
                    # prices differ wildly from the originals'.
                    if base <= 0 or not po.limit_hint:
                        summary.setdefault("warnings", []).append(
                            f"ballast: no quote to size substitute {iid} — skipped"
                        )
                        continue
                    quantity = int((po.quantity * po.limit_hint) // base)
                    if quantity <= 0:
                        summary.setdefault("warnings", []).append(
                            f"ballast: notional too small for one {iid} unit"
                        )
                        continue
                else:
                    base = base or po.limit_hint or 0.0
                pad = 1.005 if po.side == "BUY" else 0.995
                price = round(base * pad, 2)
                if price <= 0:
                    summary.setdefault("warnings", []).append(f"ballast: no price for {iid}")
                    continue
                lo = LimitOrder(po.side, quantity, price, tif="DAY", orderRef=ref)
                trade = ib.placeOrder(contract, lo)
                w = _WorkingOrder(
                    planned=po, contract=contract, trade=trade, order_ref=ref,
                    instrument_id=iid, mid=price, bid=price, ask=price,
                )
                w.walked = True  # ballast orders rest at limit; never walked
                working.append(w)
            else:
                contract = Option(
                    symbol=symbol, lastTradeDateOrContractMonth=expiry,
                    strike=po.strike, right=po.right, exchange="SMART",
                )
                qualified = ib.qualifyContracts(contract)
                if not qualified:
                    summary.setdefault("warnings", []).append(
                        f"{po.category}: cannot qualify {iid}"
                    )
                    continue
                contract = qualified[0]
                bid, ask, last = _snapshot_quote(ib, contract)
                if bid <= 0 or ask <= 0:
                    summary.setdefault("warnings", []).append(
                        f"{po.category}: no NBBO for {iid} (bid={bid} ask={ask}) — "
                        "giving up the day"
                    )
                    continue
                mid = round((bid + ask) / 2.0, 2)
                half_spread = (ask - bid) / 2.0
                if half_spread > concession_cap:
                    summary.setdefault("warnings", []).append(
                        f"{po.category}: spread too wide for {iid} "
                        f"(half={half_spread:.2f} > cap={concession_cap:.2f})"
                    )
                    continue
                lo = LimitOrder(po.side, po.quantity, mid, tif="DAY", orderRef=ref)
                trade = ib.placeOrder(contract, lo)
                working.append(
                    _WorkingOrder(
                        planned=po, contract=contract, trade=trade, order_ref=ref,
                        instrument_id=iid, mid=mid, bid=bid, ask=ask,
                    )
                )

            summary["orders_submitted"] = summary.get("orders_submitted", 0) + 1

            side = OrderSide.BUY if po.side == "BUY" else OrderSide.SELL
            submitted_rows.append(
                Order(
                    order_id=ref,
                    instrument_id=iid,
                    side=side,
                    order_type=OrderType.LIMIT,
                    quantity=float(working[-1].trade.order.totalQuantity)
                    if working
                    else float(po.quantity),
                    limit_price=working[-1].mid if working else None,
                    metadata={
                        "category": po.category,
                        "strategy": _STRATEGY_BY_CATEGORY.get(po.category, "wheel.ballast"),
                        "reason": po.reason,
                        "managed": po.manage_with_profit_take,
                    },
                )
            )
        except Exception as exc:
            _record_warning(summary, f"submit:{po.category}", exc)

    # Persist order rows + option SUBMIT provenance immediately.
    try:
        record_orders(
            db_manager, portfolio_id=cfg.portfolio_id, orders=submitted_rows,
            mode=mode, as_of_date=today,
        )
    except Exception as exc:
        _record_warning(summary, "record_orders", exc)

    for w in working:
        if w.planned.category == "ballast":
            continue
        try:
            qty = w.planned.quantity if w.planned.side == "BUY" else -w.planned.quantity
            record_order_submission(
                db_manager,
                portfolio_id=cfg.portfolio_id,
                mode=mode,
                instrument_id=w.instrument_id,
                symbol=symbol,
                right=w.planned.right,
                expiry=str(getattr(w.contract, "lastTradeDateOrContractMonth", ""))[:8],
                strike=w.planned.strike,
                quantity=qty,
                strategy=_STRATEGY_BY_CATEGORY[w.planned.category],
                order_id=w.order_ref,
                limit_price=w.mid,
                as_of_date=today,
                metadata={
                    "managed": w.planned.manage_with_profit_take,
                    "credit_per_share": w.mid,
                    "category": w.planned.category,
                },
            )
        except Exception as exc:
            _record_warning(summary, "record_submission", exc)

    # ── The walk: mid → touch → give up ──────────────────────────────
    def _option_orders() -> list[_WorkingOrder]:
        return [w for w in working if w.planned.category != "ballast"]

    deadline = _WALK_FIRST_WAIT_S
    waited = 0.0
    while waited < deadline and any(
        not (w.is_filled or w.is_dead) for w in _option_orders()
    ):
        ib.sleep(5)
        waited += 5

    for w in _option_orders():
        if w.is_filled or w.is_dead or w.walked:
            continue
        touch = w.bid if w.planned.side == "SELL" else w.ask
        try:
            w.trade.order.lmtPrice = round(touch, 2)
            ib.placeOrder(w.contract, w.trade.order)  # same orderId → modify
            w.walked = True
            logger.info(
                "run_wheel: walked %s %s from %.2f to touch %.2f",
                w.planned.category, w.instrument_id, w.mid, touch,
            )
        except Exception as exc:
            _record_warning(summary, f"walk:{w.instrument_id}", exc)

    waited = 0.0
    while waited < _WALK_SECOND_WAIT_S and any(
        not (w.is_filled or w.is_dead) for w in _option_orders()
    ):
        ib.sleep(5)
        waited += 5

    # Give up the day on anything still unfilled (options only — ballast
    # DAY orders rest for the next session by design).
    for w in _option_orders():
        if w.is_filled or w.is_dead:
            continue
        try:
            ib.cancelOrder(w.trade.order)
            ib.sleep(2)
        except Exception as exc:
            _record_warning(summary, f"cancel:{w.instrument_id}", exc)

    # Give the broker a beat to report async rejections (they arrive as a
    # silent Cancelled a few seconds after placement) before settling.
    if any(w.planned.category == "ballast" for w in working):
        ib.sleep(5)

    # Settle statuses into the orders table.
    fills = 0
    for w in working:
        if w.is_filled:
            w.outcome = "filled"
            fills += 1
            try:
                _update_order_status(db_manager, w.order_ref, "FILLED")
            except Exception as exc:
                _record_warning(summary, f"status:{w.order_ref}", exc)
        elif w.is_dead and w.planned.category != "ballast":
            w.outcome = "cancelled"
            try:
                _update_order_status(db_manager, w.order_ref, "CANCELLED")
            except Exception as exc:
                _record_warning(summary, f"status:{w.order_ref}", exc)
        elif w.is_dead:
            # Ballast orders are never cancelled by us — a dead one was
            # rejected by IBKR (permissions/price band; these arrive as a
            # silent Cancelled). Surface it instead of reporting "working".
            w.outcome = "rejected"
            summary.setdefault("warnings", []).append(
                f"ballast: {w.instrument_id} rejected by broker "
                f"(status={w.status})"
            )
            try:
                _update_order_status(db_manager, w.order_ref, "REJECTED")
            except Exception as exc:
                _record_warning(summary, f"status:{w.order_ref}", exc)

    # Record executions straight from the trade objects. reqExecutions is
    # cleared by the nightly IBC restart, so when the 23:30 EOD reconcile
    # lane misses a session (box shut down) that evening's executions are
    # unrecoverable next morning — the runner is the only component
    # guaranteed to have seen them (bitten 2026-08-07: the first CSP fill
    # never reached `fills`). Rows are keyed on IBKR execIds exactly like
    # the reconcile (ON CONFLICT DO NOTHING + reconcile's novelty gate),
    # so a later reconcile pass records nothing twice.
    fill_rows = []
    for w in working:
        for ib_fill in list(getattr(w.trade, "fills", None) or []):
            try:
                row = _fill_from_ib(ib_fill)
            except Exception as exc:
                _record_warning(summary, f"fill_map:{w.order_ref}", exc)
                continue
            if row is not None:
                row.metadata["source"] = "wheel_runner"
                fill_rows.append(row)
    if fill_rows:
        try:
            record_fills(db_manager, fills=fill_rows, mode=mode)
            record_executed_actions_for_fills(
                db_manager,
                fills=fill_rows,
                context=ExecutedActionContext(
                    portfolio_id=cfg.portfolio_id, mode=mode,
                ),
            )
        except Exception as exc:
            _record_warning(summary, "record_fills", exc)

    summary["orders_filled"] = fills
    summary["order_outcomes"] = [
        {
            "category": w.planned.category,
            "instrument_id": w.instrument_id,
            "side": w.planned.side,
            "quantity": _f(getattr(w.trade.order, "totalQuantity", w.planned.quantity)),
            "limit": w.mid,
            "walked": w.walked and w.planned.category != "ballast",
            "outcome": w.outcome,
            "avg_fill": _f(getattr(getattr(w.trade, "orderStatus", None), "avgFillPrice", 0)),
        }
        for w in working
    ]


# ── Decision logging ─────────────────────────────────────────────────


def _persist_decision(
    db_manager: Any, plan: WheelPlan, summary: dict[str, Any], *, shadow: bool,
) -> None:
    from prometheus.meta import EngineDecision, MetaStorage

    decision = EngineDecision(
        decision_id=f"WHEEL-{plan.as_of.isoformat()}-{uuid.uuid4().hex[:8]}",
        engine_name="WHEEL",
        run_id=None,
        strategy_id="US_WHEEL",
        market_id="US_EQ",
        as_of_date=plan.as_of,
        config_id=None,
        metadata={
            "shadow": shadow,
            "plan": plan.summary(),
            "order_outcomes": summary.get("order_outcomes", []),
            "nav": summary.get("nav"),
            "vix": summary.get("vix"),
            "spot": summary.get("spot"),
        },
    )
    MetaStorage(db_manager).save_engine_decision(decision)


def _notify_breaker(db_manager: Any, plan: WheelPlan) -> None:
    from prometheus.meta.notifications import record_notification

    record_notification(
        db_manager,
        as_of_date=plan.as_of,
        kind="wheel_drawdown_breaker",
        severity="critical",
        title=f"Wheel drawdown breaker: {plan.drawdown:.1%} from peak",
        body=(
            "NAV is more than 40% below the post-reset high-water mark. "
            "Per the 2026-08 spec CSP re-entry stays active (the wheel is "
            "the recovery mechanism) — this alert is the escalation."
        ),
        source_table="engine_decisions",
        source_id=f"US_WHEEL:{plan.as_of.isoformat()}:breaker",
    )


# ── Entry point ──────────────────────────────────────────────────────


def run_wheel_daily(
    *,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = WHEEL_CLIENT_ID,
    submit_override: bool | None = None,
) -> dict[str, Any]:
    """Run one day of the core+wheel strategy.

    ``submit_override`` forces submission on/off regardless of env flags
    (CLI/testing only — the daemon never passes it).
    """
    from apatheon.core.database import get_db_manager

    from prometheus.env_utils import env_flag
    from prometheus.execution.ib_compat import IB

    today = date.today()
    mode = "LIVE" if port == 4001 else "PAPER"

    if submit_override is not None:
        submit = submit_override
    else:
        # Independent of PROMETHEUS_EXECUTION_HALT by design — see module
        # docstring. The wheel's own kill switch is this flag alone.
        submit = env_flag("PROMETHEUS_WHEEL_ENABLED", default=False)

    summary: dict[str, Any] = {
        "date": today.isoformat(),
        "mode": mode,
        "shadow": not submit,
        "errors": [],
        "warnings": [],
    }

    try:
        cfg = load_wheel_config()
    except Exception as exc:
        summary["errors"].append(f"config: {exc}")
        return summary

    db_manager = get_db_manager()
    ib = IB()
    try:
        import asyncio

        try:
            asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
        ib.connect(host=host, port=port, clientId=client_id, timeout=30)
    except Exception as exc:
        summary["errors"].append(f"connect: {exc}")
        return summary

    try:
        try:
            view = _build_account_view(ib, db_manager, cfg, mode=mode, today=today)
        except Exception as exc:
            summary["errors"].append(f"account_view: {exc}")
            return summary

        if view.nav <= 0:
            summary["errors"].append(f"account_view: NAV={view.nav}")
            return summary

        summary["nav"] = view.nav
        summary["spot"] = view.underlying_spot
        summary["vix"] = view.vix

        try:
            from prometheus.calendar import near_iv_event

            event = near_iv_event(cfg.underlying_symbol, today, window_days=2)
            if event is None:
                iv_event = None
            else:
                kind = getattr(getattr(event, "kind", None), "value", None) or "event"
                iv_event = f"{kind} on {getattr(event, 'event_date', '?')}"
        except Exception as exc:
            logger.warning("run_wheel: iv-event lookup failed (continuing): %s", exc)
            iv_event = None

        try:
            rebalance_due = _ballast_rebalance_due(
                db_manager, portfolio_id=cfg.portfolio_id, mode=mode, today=today,
            )
        except Exception as exc:
            logger.warning("run_wheel: rebalance-due check failed: %s", exc)
            rebalance_due = False

        plan = build_plan(
            cfg, view, today=today, iv_event=iv_event,
            ballast_rebalance_due=rebalance_due,
        )
        summary["plan"] = plan.summary()
        summary["orders_planned"] = len(plan.orders)

        if plan.breaker_triggered:
            try:
                _notify_breaker(db_manager, plan)
            except Exception as exc:
                _record_warning(summary, "breaker_notification", exc)

        if submit and plan.orders:
            _submit_plan(
                ib, db_manager, cfg, plan,
                mode=mode, today=today, spot=view.underlying_spot,
                summary=summary,
            )

        try:
            _persist_decision(db_manager, plan, summary, shadow=not submit)
        except Exception as exc:
            _record_warning(summary, "decision_log", exc)

        logger.info(
            "run_wheel: %s planned=%d submitted=%d filled=%d shadow=%s "
            "skips=%s",
            today, len(plan.orders), summary.get("orders_submitted", 0),
            summary.get("orders_filled", 0), not submit, plan.skips,
        )
        return summary
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


__all__ = ["run_wheel_daily", "WHEEL_CLIENT_ID"]
