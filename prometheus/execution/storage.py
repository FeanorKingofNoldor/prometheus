"""Prometheus v2 – Execution storage helpers.

This module provides small helpers for persisting execution activity into
the runtime database:

- ``orders`` – logical orders produced by the order planner/router.
- ``fills`` – concrete executions of those orders.
- ``positions_snapshots`` – point-in-time portfolio holdings.

The helpers are intentionally minimal and mode-agnostic so they can be
used in LIVE, PAPER, and BACKTEST environments.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Mapping, Sequence

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.execution.broker_interface import Fill, Order, OrderStatus, Position

logger = get_logger(__name__)


@dataclass(frozen=True)
class ExecutionMode:
    """Simple constants for execution modes.

    Using a dataclass for namespacing rather than an Enum keeps the
    values easy to serialise into the database.
    """

    LIVE: str = "LIVE"
    PAPER: str = "PAPER"
    BACKTEST: str = "BACKTEST"


def _default_timestamp(as_of_date: date | None) -> datetime:
    """Return a sensible default timestamp for an execution event.

    For backtests we typically align events to end-of-day in UTC for the
    given ``as_of_date``; if ``as_of_date`` is None we fall back to
    ``datetime.utcnow()``.
    """

    if as_of_date is None:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    return datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 0, tzinfo=timezone.utc)


def record_orders(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str | None,
    orders: Sequence[Order],
    mode: str,
    decision_id: str | None = None,
    as_of_date: date | None = None,
) -> None:
    """Insert a batch of orders into the ``orders`` table.

    Args:
        db_manager: Runtime database manager.
        portfolio_id: Logical portfolio identifier associated with the
            orders (may be ``None`` for some strategies).
        orders: Sequence of :class:`Order` objects to persist.
        mode: Execution mode (e.g. ``"BACKTEST"``, ``"LIVE"``).
        decision_id: Optional engine decision id that produced the
            orders.
        as_of_date: Optional simulation date used to derive a default
            timestamp when order.metadata does not carry one.
    """

    if not orders:
        return

    sql = """
        INSERT INTO orders (
            order_id,
            timestamp,
            instrument_id,
            side,
            order_type,
            quantity,
            limit_price,
            stop_price,
            status,
            mode,
            portfolio_id,
            decision_id,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    mode_up = str(mode).upper()

    # Backtests align events to end-of-day UTC on the simulated date.
    # For LIVE/PAPER we want a real wall-clock timestamp.
    if mode_up == ExecutionMode.BACKTEST:
        ts_default = _default_timestamp(as_of_date)
    else:
        ts_default = datetime.utcnow().replace(tzinfo=timezone.utc)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for order in orders:
                # Allow a custom timestamp in order.metadata["timestamp"]
                meta = order.metadata or {}
                ts_val = meta.get("timestamp") if isinstance(meta, dict) else None
                if isinstance(ts_val, str):
                    try:
                        ts = datetime.fromisoformat(ts_val)
                    except ValueError:
                        ts = ts_default
                elif isinstance(ts_val, datetime):
                    ts = ts_val
                else:
                    ts = ts_default

                payload = Json(meta if isinstance(meta, dict) else {})

                cursor.execute(
                    sql,
                    (
                        order.order_id,
                        ts,
                        order.instrument_id,
                        order.side.value,
                        order.order_type.value,
                        float(order.quantity),
                        float(order.limit_price) if order.limit_price is not None else None,
                        float(order.stop_price) if order.stop_price is not None else None,
                        "SUBMITTED",
                        mode,
                        portfolio_id,
                        decision_id,
                        payload,
                    ),
                )
            conn.commit()
        finally:
            cursor.close()

    logger.info("Recorded %d orders in mode=%s", len(orders), mode)


def record_fills(
    db_manager: DatabaseManager,
    *,
    fills: Sequence[Fill],
    mode: str,
) -> None:
    """Insert a batch of fills into the ``fills`` table."""

    if not fills:
        return

    sql = """
        INSERT INTO fills (
            fill_id,
            order_id,
            timestamp,
            instrument_id,
            side,
            quantity,
            price,
            commission,
            mode,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for fill in fills:
                meta = fill.metadata or {}
                payload = Json(meta if isinstance(meta, dict) else {})
                cursor.execute(
                    sql,
                    (
                        fill.fill_id,
                        fill.order_id,
                        fill.timestamp,
                        fill.instrument_id,
                        fill.side.value,
                        float(fill.quantity),
                        float(fill.price),
                        float(fill.commission),
                        mode,
                        payload,
                    ),
                )
            conn.commit()
        finally:
            cursor.close()

    logger.info("Recorded %d fills in mode=%s", len(fills), mode)


def record_positions_snapshot(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    positions: Mapping[str, Position],
    as_of_date: date,
    mode: str,
    timestamp: datetime | None = None,
) -> None:
    """Insert a positions snapshot into ``positions_snapshots``.

    Args:
        db_manager: Runtime database manager.
        portfolio_id: Logical portfolio identifier.
        positions: Mapping from instrument_id to :class:`Position`.
        as_of_date: Trading date the snapshot represents.
        mode: Execution mode (LIVE/PAPER/BACKTEST).
        timestamp: Optional wall-clock timestamp; if omitted, a default
            end-of-day UTC timestamp for ``as_of_date`` is used.
    """

    if not positions:
        return

    ts = timestamp or _default_timestamp(as_of_date)

    sql = """
        INSERT INTO positions_snapshots (
            portfolio_id,
            timestamp,
            as_of_date,
            instrument_id,
            quantity,
            avg_cost,
            market_value,
            unrealized_pnl,
            mode
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for inst_id, pos in positions.items():
                cursor.execute(
                    sql,
                    (
                        portfolio_id,
                        ts,
                        as_of_date,
                        inst_id,
                        float(pos.quantity),
                        float(pos.avg_cost),
                        float(pos.market_value),
                        float(pos.unrealized_pnl),
                        mode,
                    ),
                )
            conn.commit()
        finally:
            cursor.close()

    logger.info(
        "Recorded positions snapshot for portfolio_id=%s instruments=%d mode=%s",
        portfolio_id,
        len(positions),
        mode,
    )


def update_order_statuses(
    db_manager: DatabaseManager,
    *,
    statuses: Mapping[str, OrderStatus | str],
) -> None:
    """Persist latest order statuses for known order_ids.

    Args:
        db_manager: Runtime database manager.
        statuses: Mapping of ``order_id -> status`` where status is either
            :class:`OrderStatus` or an already-normalized string.
    """

    if not statuses:
        return

    sql = """
        UPDATE orders
        SET status = %s
        WHERE order_id = %s
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for order_id, status in statuses.items():
                status_value = status.value if isinstance(status, OrderStatus) else str(status)
                cursor.execute(sql, (status_value, str(order_id)))
            conn.commit()
        finally:
            cursor.close()

    logger.info("Updated %d order statuses", len(statuses))
