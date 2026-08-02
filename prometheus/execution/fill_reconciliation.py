"""Prometheus v2 – morning fill reconciliation.

Equity orders are submitted POST_CLOSE while the market is closed, so
they only fill at the *next* session's open — hours after the submitting
process has polled for fills and exited.  Nothing used to reconcile
those next-morning fills, which left every paper order stuck at status
``SUBMITTED`` forever and recorded zero fills.

:func:`reconcile_fills` closes that gap.  It connects to the IBKR
gateway (dedicated ``client_id=13`` so it never collides with the
equity/options/snapshot clients), requests all executions from the last
``lookback_hours`` via ``reqExecutions`` + ``ExecutionFilter``, maps
each execution back to a Prometheus order through ``execution.orderRef``
(the deterministic order id stored there at submit time), and backfills:

* ``fills`` rows (``fill_id`` = IBKR ``execId``; the ``ON CONFLICT DO
  NOTHING`` in :func:`prometheus.execution.storage.record_fills` makes
  this idempotent),
* ``executed_actions`` rows (only for fills that were not already in the
  DB — that insert is *not* idempotent),
* ``orders.status`` (FILLED / PARTIALLY_FILLED per cumulative filled
  quantity vs. order quantity).

It also expires stale orders: SUBMITTED/PENDING orders older than the
previous trading session with no execution are set to CANCELLED (the
:class:`OrderStatus` enum has no EXPIRED value) with a metadata note, so
they stop looking like live exposure.

Designed to run from the market-aware daemon during PRE_OPEN/SESSION and
during morning catch-up.  It is read-mostly and idempotent — safe to run
multiple times per day.
"""

from __future__ import annotations

import importlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Sequence

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.execution.broker_interface import Fill, OrderSide, OrderStatus
from prometheus.execution.executed_actions import (
    ExecutedActionContext,
    record_executed_actions_for_fills,
)
from prometheus.execution.instrument_mapper import InstrumentMapper
from prometheus.execution.storage import record_fills, update_order_statuses

logger = get_logger(__name__)

#: Dedicated IBKR API client id for reconciliation.  Must not collide with
#: equity execution (10), options (11), or position snapshots (12).
RECONCILE_CLIENT_ID = 13

#: Quantity tolerance when comparing cumulative filled qty vs. order qty.
_QTY_EPSILON = 1e-6


@dataclass(frozen=True)
class _OrderRow:
    """Slim projection of an ``orders`` row used during reconciliation."""

    order_id: str
    quantity: float
    status: str
    portfolio_id: str | None
    decision_id: str | None


# ----------------------------------------------------------------------
# Pure helpers (unit-testable without IBKR or a database)
# ----------------------------------------------------------------------


def _normalize_exec_time(ts_raw: Any) -> datetime:
    """Normalize an IBKR execution timestamp to an aware UTC datetime."""
    if isinstance(ts_raw, datetime):
        ts = ts_raw
    else:
        ts = datetime.fromisoformat(str(ts_raw))
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


def _fill_from_ib(ib_fill: Any) -> Fill | None:
    """Map an ib_async/ib_insync ``Fill`` to a Prometheus :class:`Fill`.

    Returns ``None`` when the execution carries no usable ``orderRef``
    (manual TWS orders, other systems' orders) — those are not ours to
    reconcile.
    """
    execution = ib_fill.execution
    order_ref = str(getattr(execution, "orderRef", "") or "").strip()
    if not order_ref:
        return None

    side = OrderSide.BUY if execution.side == "BOT" else OrderSide.SELL
    commission = 0.0
    commission_report = getattr(ib_fill, "commissionReport", None)
    if commission_report is not None:
        try:
            commission = float(commission_report.commission or 0.0)
        except (TypeError, ValueError):
            commission = 0.0

    return Fill(
        fill_id=str(execution.execId),
        order_id=order_ref,
        instrument_id=InstrumentMapper.contract_to_instrument_id(ib_fill.contract),
        side=side,
        quantity=float(execution.shares),
        price=float(execution.price),
        timestamp=_normalize_exec_time(execution.time),
        commission=commission,
        metadata={
            "exchange": getattr(execution, "exchange", None),
            "exec_id": str(execution.execId),
            "order_id_ibkr": str(getattr(execution, "orderId", "")),
            "source": "fill_reconciliation",
        },
    )


def _derive_order_status(total_filled: float, order_quantity: float) -> OrderStatus | None:
    """Derive an order status from cumulative filled qty vs. order qty."""
    if total_filled <= _QTY_EPSILON:
        return None
    if total_filled + _QTY_EPSILON >= order_quantity:
        return OrderStatus.FILLED
    return OrderStatus.PARTIALLY_FILLED


#: Pseudo-markets that have no trading calendar and must never affect the
#: stale-order cutoff.
_NON_TRADING_MARKETS = frozenset({"IRIS", "INTEL"})


def _default_market_ids() -> tuple[str, ...]:
    """Markets whose calendars gate stale-order expiry.

    Reads ``PROMETHEUS_ACTIVE_MARKETS`` (comma-separated, the daemon's
    active-market env var), dropping the IRIS/INTEL pseudo-markets.
    Falls back to ``("US_EQ",)`` when unset/empty — identical to the
    historical single-market behaviour.
    """
    raw = os.environ.get("PROMETHEUS_ACTIVE_MARKETS", "")
    markets = tuple(
        m.strip().upper()
        for m in raw.split(",")
        if m.strip() and m.strip().upper() not in _NON_TRADING_MARKETS
    )
    return markets or ("US_EQ",)


def _previous_trading_day(as_of: date, market_ids: Iterable[str] | None = None) -> date:
    """Oldest previous trading day across ``market_ids``, strictly before ``as_of``.

    Multi-market conservatism: each market's previous trading day is
    computed with its own :class:`TradingCalendar` and the OLDEST is
    returned, so stale-order expiry never expires an order whose market
    has not had its next session yet (e.g. a HK holiday while the US
    traded).

    ``market_ids=None`` defaults to ``PROMETHEUS_ACTIVE_MARKETS`` (minus
    the IRIS/INTEL pseudo-markets), falling back to ``("US_EQ",)`` — so
    existing single-argument callers keep today's behaviour on a US-only
    deployment.

    Falls back to a conservative calendar-day offset (``as_of - 3d``)
    for any market whose calendar cannot be loaded (e.g. DB unavailable)
    so expiry still errs on the side of keeping orders alive.
    """
    if market_ids is None:
        markets = _default_market_ids()
    else:
        markets = tuple(
            str(m).strip().upper()
            for m in market_ids
            if str(m).strip() and str(m).strip().upper() not in _NON_TRADING_MARKETS
        ) or ("US_EQ",)

    fallback = as_of - timedelta(days=3)
    candidates: list[date] = []
    for market in dict.fromkeys(markets):
        try:
            from apatheon.core.time import TradingCalendar, TradingCalendarConfig

            cal = TradingCalendar(TradingCalendarConfig(market=market))
            days = cal.trading_days_between(as_of - timedelta(days=10), as_of - timedelta(days=1))
            if days:
                candidates.append(days[-1])
                continue
        except Exception:
            logger.debug(
                "_previous_trading_day: calendar unavailable for %s, using fallback",
                market, exc_info=True,
            )
        # Unknown previous session for this market — contribute the
        # conservative fallback so the cutoff cannot move forward.
        candidates.append(fallback)

    return min(candidates) if candidates else fallback


def _execution_filter(cutoff_utc: datetime) -> Any:
    """Build an ``ExecutionFilter`` for executions since ``cutoff_utc``.

    The class is resolved through the active ib_compat backend
    (ib_async preferred, ib_insync fallback) — both expose an identical
    dataclass.  The TWS API expects ``time`` as ``'yyyymmdd hh:mm:ss'``;
    we pass UTC and additionally filter client-side by timestamp, so a
    gateway interpreting the filter in another timezone can only
    over-fetch, never miss fills.
    """
    from prometheus.execution.ib_compat import IB_BACKEND

    execution_filter_cls = getattr(importlib.import_module(IB_BACKEND), "ExecutionFilter")
    exec_filter = execution_filter_cls()
    exec_filter.time = cutoff_utc.strftime("%Y%m%d %H:%M:%S")
    return exec_filter


# ----------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------


def _load_order_rows(
    db_manager: DatabaseManager, order_ids: Sequence[str], mode_db: str,
) -> Dict[str, _OrderRow]:
    """Load the orders (in the given mode) matching ``order_ids``."""
    if not order_ids:
        return {}

    sql = """
        SELECT order_id, quantity, status, portfolio_id, decision_id
        FROM orders
        WHERE order_id = ANY(%s) AND UPPER(mode) = %s
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (list(order_ids), mode_db))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return {
        row[0]: _OrderRow(
            order_id=row[0],
            quantity=float(row[1]),
            status=str(row[2] or ""),
            portfolio_id=row[3],
            decision_id=row[4],
        )
        for row in rows
    }


def _existing_fill_ids(db_manager: DatabaseManager, fill_ids: Sequence[str]) -> set[str]:
    """Return the subset of ``fill_ids`` already present in ``fills``."""
    if not fill_ids:
        return set()

    sql = "SELECT fill_id FROM fills WHERE fill_id = ANY(%s)"
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (list(fill_ids),))
            rows = cursor.fetchall()
        finally:
            cursor.close()
    return {row[0] for row in rows}


def _expire_stale_orders(
    db_manager: DatabaseManager,
    *,
    mode_db: str,
    cutoff_utc: datetime,
    exclude_order_ids: Iterable[str],
) -> List[str]:
    """CANCEL stale SUBMITTED/PENDING orders with no execution.

    An order timestamped before ``cutoff_utc`` (start of the previous
    trading session) had a full session to fill; if IBKR reported no
    execution for it and no fill row exists, it is dead — a DAY order
    the gateway already discarded.  The status update carries a metadata
    note (``update_order_statuses`` does not support metadata, so this
    is a direct UPDATE).
    """
    note = json.dumps(
        {
            "expired_by": "fill_reconciliation",
            "expired_at": datetime.now(timezone.utc).isoformat(),
            "reason": "no execution reported after the previous trading session",
        }
    )
    sql = """
        UPDATE orders
        SET status = %s,
            metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb
        WHERE UPPER(mode) = %s
          AND status IN (%s, %s)
          AND timestamp < %s
          AND NOT (order_id = ANY(%s))
          AND NOT EXISTS (SELECT 1 FROM fills f WHERE f.order_id = orders.order_id)
        RETURNING order_id
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql,
                (
                    OrderStatus.CANCELLED.value,
                    note,
                    mode_db,
                    OrderStatus.SUBMITTED.value,
                    OrderStatus.PENDING.value,
                    cutoff_utc,
                    list(exclude_order_ids),
                ),
            )
            expired = [row[0] for row in cursor.fetchall()]
            conn.commit()
        finally:
            cursor.close()
    return expired


# ----------------------------------------------------------------------
# IBKR connection plumbing
# ----------------------------------------------------------------------


def _default_client_factory(mode: str) -> Any:
    """Connect a dedicated reconciliation IBKR client (client_id=13).

    Reuses the same connection plumbing as the daemon's
    ``snapshot_positions`` handler, including the per-thread asyncio
    event loop required by ib_async/eventkit in daemon worker threads.
    """
    import asyncio

    from prometheus.execution.ibkr_client_impl import IbkrClientImpl
    from prometheus.execution.ibkr_config import (
        IbkrGatewayType,
        IbkrMode,
        create_connection_config,
    )

    ibkr_mode = IbkrMode.LIVE if mode == "live" else IbkrMode.PAPER
    conn_config = create_connection_config(
        mode=ibkr_mode,
        gateway_type=IbkrGatewayType.GATEWAY,
        client_id=RECONCILE_CLIENT_ID,
    )

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    client = IbkrClientImpl(config=conn_config)
    client.connect()
    return client


# ----------------------------------------------------------------------
# Public entry point
# ----------------------------------------------------------------------


def reconcile_fills(
    db_manager: DatabaseManager,
    *,
    mode: str = "paper",
    lookback_hours: int = 48,
    client_factory: Callable[[str], Any] | None = None,
    expire_stale: bool = True,
    stale_cutoff_utc: datetime | None = None,
) -> dict:
    """Reconcile IBKR fills into the Prometheus DB.

    IBKR's ``reqExecutions`` only reports executions from the *current*
    gateway day (the overnight IBC restart clears it), so a capture pass
    only sees fills of the session it runs after.  The daemon therefore
    runs this twice: a POST_CLOSE pass the same day (capture + expiry —
    the only pass that can have seen the session's executions) and a
    capture-only morning pass (``expire_stale=False``) that exists for
    intraday-restart stragglers and must NEVER expire orders, because
    at 08:30 ET yesterday-evening's orders haven't had their session yet.

    Args:
        db_manager: Runtime database manager.
        mode: ``"paper"`` or ``"live"`` — selects the gateway port,
            credentials, and the ``orders.mode`` scope.
        lookback_hours: How far back to request executions.
        client_factory: Test seam — callable returning a *connected*
            client exposing ``.ib`` and ``.disconnect()``.  Defaults to
            a real :class:`IbkrClientImpl` on ``client_id=13``.
        expire_stale: Whether to CANCEL stale unfilled orders.  Only
            pass True from a pass that ran AFTER the session those
            orders traded in, with that session's executions captured.
        stale_cutoff_utc: Orders timestamped before this instant are
            expiry candidates.  ``None`` keeps the legacy
            previous-trading-day cutoff.

    Returns:
        Summary dict ``{fills_recorded, orders_updated, orders_expired,
        errors}``.
    """
    summary: dict = {
        "fills_recorded": 0,
        "orders_updated": 0,
        "orders_expired": 0,
        "errors": [],
    }
    mode = str(mode).lower()
    if mode not in ("paper", "live"):
        summary["errors"].append(f"unsupported mode: {mode!r} (expected 'paper' or 'live')")
        return summary
    mode_db = "PAPER" if mode == "paper" else "LIVE"

    factory = client_factory or _default_client_factory
    try:
        client = factory(mode)
    except Exception as exc:
        summary["errors"].append(f"IBKR connect failed: {type(exc).__name__}: {exc}")
        logger.warning("reconcile_fills: could not connect to IBKR (%s): %s", mode, exc)
        return summary

    try:
        cutoff_utc = datetime.now(timezone.utc) - timedelta(hours=int(lookback_hours))
        try:
            ib_fills = list(client.ib.reqExecutions(_execution_filter(cutoff_utc)))
        except Exception as exc:
            summary["errors"].append(f"reqExecutions failed: {type(exc).__name__}: {exc}")
            logger.warning("reconcile_fills: reqExecutions failed: %s", exc)
            return summary

        # Map executions → Prometheus fills, dropping empty orderRefs and
        # anything outside the lookback window (the server-side time
        # filter's timezone semantics vary across gateway versions).
        fills_by_id: Dict[str, Fill] = {}
        for ib_fill in ib_fills:
            try:
                fill = _fill_from_ib(ib_fill)
            except Exception:
                logger.warning("reconcile_fills: could not map execution, skipping", exc_info=True)
                continue
            if fill is None or fill.timestamp < cutoff_utc:
                continue
            fills_by_id[fill.fill_id] = fill

        # Resolve which orderRefs actually belong to Prometheus orders in
        # this mode; unknown refs are ignored.
        order_ids = sorted({f.order_id for f in fills_by_id.values()})
        order_rows = _load_order_rows(db_manager, order_ids, mode_db)
        matched_fills = [f for f in fills_by_id.values() if f.order_id in order_rows]
        unknown_refs = len(fills_by_id) - len(matched_fills)
        if unknown_refs:
            logger.info(
                "reconcile_fills: ignoring %d execution(s) with unknown orderRef", unknown_refs,
            )

        # Backfill fills + executed_actions for fills not yet in the DB.
        # record_fills is idempotent (ON CONFLICT DO NOTHING), but the
        # executed_actions insert is not — so gate both on novelty.
        known_fill_ids = _existing_fill_ids(db_manager, [f.fill_id for f in matched_fills])
        new_fills = sorted(
            (f for f in matched_fills if f.fill_id not in known_fill_ids),
            key=lambda f: f.timestamp,
        )
        if new_fills:
            record_fills(db_manager, fills=new_fills, mode=mode_db)
            summary["fills_recorded"] = len(new_fills)

            by_context: Dict[tuple, List[Fill]] = {}
            for fill in new_fills:
                row = order_rows[fill.order_id]
                by_context.setdefault((row.portfolio_id, row.decision_id), []).append(fill)
            for (portfolio_id, decision_id), context_fills in by_context.items():
                try:
                    record_executed_actions_for_fills(
                        db_manager,
                        fills=context_fills,
                        context=ExecutedActionContext(
                            portfolio_id=portfolio_id,
                            decision_id=decision_id,
                            mode=mode_db,
                        ),
                    )
                except Exception as exc:
                    summary["errors"].append(
                        f"executed_actions insert failed for portfolio={portfolio_id}: {exc}"
                    )
                    logger.warning(
                        "reconcile_fills: executed_actions insert failed for portfolio=%s",
                        portfolio_id, exc_info=True,
                    )

        # Update order statuses per cumulative filled quantity.
        filled_qty: Dict[str, float] = {}
        for fill in matched_fills:
            filled_qty[fill.order_id] = filled_qty.get(fill.order_id, 0.0) + fill.quantity

        statuses: Dict[str, OrderStatus] = {}
        for order_id, total in filled_qty.items():
            row = order_rows[order_id]
            status = _derive_order_status(total, row.quantity)
            if status is not None and status.value != row.status:
                statuses[order_id] = status
        if statuses:
            update_order_statuses(db_manager, statuses=statuses)
            summary["orders_updated"] = len(statuses)

        # Expire stale SUBMITTED/PENDING orders with no execution — but only
        # when this pass could actually have seen the relevant session's
        # executions (expire_stale=True).  A pre-open pass must never expire:
        # yesterday-evening's orders fill TODAY, and their executions are
        # gone from reqExecutions by tomorrow, so expiring here silently
        # cancels orders that go on to fill at IBKR.
        if expire_stale:
            if stale_cutoff_utc is not None:
                stale_cutoff = stale_cutoff_utc
            else:
                prev_session = _previous_trading_day(datetime.now(timezone.utc).date())
                stale_cutoff = datetime(
                    prev_session.year, prev_session.month, prev_session.day, tzinfo=timezone.utc,
                )
            try:
                expired = _expire_stale_orders(
                    db_manager,
                    mode_db=mode_db,
                    cutoff_utc=stale_cutoff,
                    exclude_order_ids=list(filled_qty.keys()),
                )
                summary["orders_expired"] = len(expired)
                if expired:
                    logger.info(
                        "reconcile_fills: expired %d stale order(s) older than %s (first: %s)",
                        len(expired), stale_cutoff.isoformat(), expired[0],
                    )
            except Exception as exc:
                summary["errors"].append(f"stale-order expiry failed: {type(exc).__name__}: {exc}")
                logger.warning("reconcile_fills: stale-order expiry failed", exc_info=True)

        logger.info(
            "reconcile_fills[%s]: executions=%d matched=%d fills_recorded=%d "
            "orders_updated=%d orders_expired=%d errors=%d",
            mode_db,
            len(ib_fills),
            len(matched_fills),
            summary["fills_recorded"],
            summary["orders_updated"],
            summary["orders_expired"],
            len(summary["errors"]),
        )
        return summary
    finally:
        try:
            client.disconnect()
        except Exception:
            logger.debug("reconcile_fills: disconnect failed", exc_info=True)


__all__ = ["reconcile_fills", "RECONCILE_CLIENT_ID"]
