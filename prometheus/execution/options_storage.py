"""Prometheus v2 — Options positions storage.

Persists options position state and events to the runtime database so
the picture survives IBKR disconnects, daemon restarts, and gives us
proper attribution. Two tables:

- ``options_positions`` carries the mutable state of currently open
  positions (one row per ``(instrument_id, portfolio_id, mode)``).
- ``options_position_events`` is the immutable log of OPEN/CLOSE/ROLL/
  EXPIRE/MARK events. Survives position closure.

Helpers here mirror ``prometheus.execution.storage`` in style: small,
mode-agnostic, no business logic. The in-memory ``OptionsPortfolio``
calls these to write through.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


# Event types — kept as plain strings to match the column type and make
# ad-hoc SQL filters straightforward.
EVENT_OPEN = "OPEN"
EVENT_CLOSE = "CLOSE"
EVENT_ROLL = "ROLL"
EVENT_EXPIRE = "EXPIRE"
EVENT_ADJUST = "ADJUST"
EVENT_MARK = "MARK"


@dataclass(frozen=True)
class OptionPositionRow:
    """Snapshot of a row in ``options_positions``."""

    position_id: str
    instrument_id: str
    portfolio_id: str
    mode: str
    sleeve: str | None
    template: str | None
    strategy: str | None
    symbol: str
    right: str
    expiry: str
    strike: float
    multiplier: int
    sec_type: str
    quantity: int
    avg_cost: float
    opened_at: datetime
    market_price: float | None
    market_value: float | None
    unrealized_pnl: float | None
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    implied_vol: float | None
    underlying_price: float | None


def _new_position_id() -> str:
    return uuid.uuid4().hex[:16]


def _as_of(as_of_date: date | None) -> date:
    return as_of_date if as_of_date is not None else datetime.now(timezone.utc).date()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def record_position_open(
    db_manager: DatabaseManager,
    *,
    instrument_id: str,
    portfolio_id: str,
    mode: str,
    symbol: str,
    right: str,
    expiry: str,
    strike: float,
    quantity: int,
    avg_cost: float,
    multiplier: int = 100,
    sec_type: str = "OPT",
    sleeve: str | None = None,
    template: str | None = None,
    strategy: str | None = None,
    decision_id: str | None = None,
    order_id: str | None = None,
    fill_id: str | None = None,
    greeks: Mapping[str, float] | None = None,
    as_of_date: date | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    """Insert an open options position and log the OPEN event.

    Idempotent on ``(instrument_id, portfolio_id, mode)``: if a row
    already exists, this returns the existing ``position_id`` without
    inserting again (the caller is expected to use ``record_position_event``
    for subsequent fills on the same position).

    Returns the ``position_id`` of the (existing or newly created) row.
    """

    now = _now()
    opened_at = now
    as_of = _as_of(as_of_date)
    md_json = Json(dict(metadata)) if metadata else None

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT position_id
                FROM options_positions
                WHERE instrument_id = %s AND portfolio_id = %s AND mode = %s
                """,
                (instrument_id, portfolio_id, mode),
            )
            existing = cur.fetchone()
            if existing:
                return existing[0]

            position_id = _new_position_id()
            cur.execute(
                """
                INSERT INTO options_positions (
                    position_id, instrument_id, portfolio_id, mode,
                    sleeve, template, strategy,
                    symbol, right, expiry, strike, multiplier, sec_type,
                    quantity, avg_cost, opened_at, opened_decision_id,
                    delta, gamma, theta, vega, implied_vol, underlying_price,
                    greeks_updated_at, metadata_json,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s
                )
                """,
                (
                    position_id, instrument_id, portfolio_id, mode,
                    sleeve, template, strategy,
                    symbol, right.upper(), expiry, float(strike),
                    int(multiplier), sec_type,
                    int(quantity), float(avg_cost), opened_at, decision_id,
                    _g(greeks, "delta"), _g(greeks, "gamma"),
                    _g(greeks, "theta"), _g(greeks, "vega"),
                    _g(greeks, "implied_vol"), _g(greeks, "underlying_price"),
                    now if greeks else None, md_json,
                    now, now,
                ),
            )
            _insert_event(
                cur,
                position_id=position_id,
                portfolio_id=portfolio_id,
                mode=mode,
                event_type=EVENT_OPEN,
                event_at=now,
                as_of_date=as_of,
                instrument_id=instrument_id,
                symbol=symbol,
                right=right,
                expiry=expiry,
                strike=strike,
                multiplier=multiplier,
                quantity_delta=int(quantity),
                price=float(avg_cost),
                realized_pnl=None,
                sleeve=sleeve,
                template=template,
                strategy=strategy,
                decision_id=decision_id,
                order_id=order_id,
                fill_id=fill_id,
                greeks=greeks,
                metadata=metadata,
            )

    return position_id


def record_position_close(
    db_manager: DatabaseManager,
    *,
    position_id: str,
    quantity_delta: int,
    price: float,
    realized_pnl: float | None = None,
    event_type: str = EVENT_CLOSE,
    decision_id: str | None = None,
    order_id: str | None = None,
    fill_id: str | None = None,
    as_of_date: date | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> bool:
    """Apply a close/expire/roll-out leg to an open position.

    If ``quantity_delta`` zeroes the position, the parent row is
    deleted; otherwise the quantity is decremented. The event row is
    always written. Returns ``True`` if the parent row was removed,
    ``False`` if only updated.

    ``quantity_delta`` is signed in the *direction of the close*: closing
    a long position passes a negative delta, closing a short position
    passes a positive delta. The caller is responsible for sign.
    """

    now = _now()
    as_of = _as_of(as_of_date)

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT instrument_id, portfolio_id, mode,
                       symbol, right, expiry, strike, multiplier,
                       quantity, sleeve, template, strategy
                FROM options_positions
                WHERE position_id = %s
                """,
                (position_id,),
            )
            row = cur.fetchone()
            if row is None:
                logger.warning(
                    "record_position_close: position_id=%s not found", position_id
                )
                return False

            (
                instrument_id, portfolio_id, mode,
                symbol, right, expiry, strike, multiplier,
                cur_qty, sleeve, template, strategy,
            ) = row

            new_qty = int(cur_qty) + int(quantity_delta)
            removed = new_qty == 0

            if removed:
                cur.execute(
                    "DELETE FROM options_positions WHERE position_id = %s",
                    (position_id,),
                )
            else:
                cur.execute(
                    """
                    UPDATE options_positions
                    SET quantity = %s, updated_at = %s
                    WHERE position_id = %s
                    """,
                    (new_qty, now, position_id),
                )

            _insert_event(
                cur,
                position_id=position_id,
                portfolio_id=portfolio_id,
                mode=mode,
                event_type=event_type,
                event_at=now,
                as_of_date=as_of,
                instrument_id=instrument_id,
                symbol=symbol,
                right=right,
                expiry=expiry,
                strike=float(strike),
                multiplier=int(multiplier),
                quantity_delta=int(quantity_delta),
                price=float(price),
                realized_pnl=realized_pnl,
                sleeve=sleeve,
                template=template,
                strategy=strategy,
                decision_id=decision_id,
                order_id=order_id,
                fill_id=fill_id,
                greeks=None,
                metadata=metadata,
            )

    return removed


def update_position_mark(
    db_manager: DatabaseManager,
    *,
    position_id: str,
    market_price: float | None = None,
    market_value: float | None = None,
    unrealized_pnl: float | None = None,
    greeks: Mapping[str, float] | None = None,
    write_mark_event: bool = False,
    as_of_date: date | None = None,
) -> None:
    """Update mark-to-market and greeks for an open position.

    Optionally writes a MARK event row when ``write_mark_event=True`` —
    use this for the daily snapshot, not for transient refreshes during
    a strategy run.
    """

    now = _now()
    sets: list[str] = []
    args: list[Any] = []

    if market_price is not None:
        sets.append("market_price = %s")
        args.append(float(market_price))
    if market_value is not None:
        sets.append("market_value = %s")
        args.append(float(market_value))
    if unrealized_pnl is not None:
        sets.append("unrealized_pnl = %s")
        args.append(float(unrealized_pnl))
    if greeks:
        for key, col in (
            ("delta", "delta"), ("gamma", "gamma"),
            ("theta", "theta"), ("vega", "vega"),
            ("implied_vol", "implied_vol"),
            ("underlying_price", "underlying_price"),
        ):
            val = greeks.get(key)
            if val is not None:
                sets.append(f"{col} = %s")
                args.append(float(val))
        sets.append("greeks_updated_at = %s")
        args.append(now)

    if not sets:
        return

    sets.append("updated_at = %s")
    args.append(now)
    args.append(position_id)

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE options_positions SET {', '.join(sets)} WHERE position_id = %s",
                args,
            )

            if write_mark_event and cur.rowcount > 0:
                cur.execute(
                    """
                    SELECT instrument_id, portfolio_id, mode,
                           symbol, right, expiry, strike, multiplier,
                           sleeve, template, strategy
                    FROM options_positions
                    WHERE position_id = %s
                    """,
                    (position_id,),
                )
                row = cur.fetchone()
                if row is not None:
                    (
                        instrument_id, portfolio_id, mode,
                        symbol, right, expiry, strike, multiplier,
                        sleeve, template, strategy,
                    ) = row
                    _insert_event(
                        cur,
                        position_id=position_id,
                        portfolio_id=portfolio_id,
                        mode=mode,
                        event_type=EVENT_MARK,
                        event_at=now,
                        as_of_date=_as_of(as_of_date),
                        instrument_id=instrument_id,
                        symbol=symbol,
                        right=right,
                        expiry=expiry,
                        strike=float(strike),
                        multiplier=int(multiplier),
                        quantity_delta=0,
                        price=market_price,
                        realized_pnl=None,
                        sleeve=sleeve,
                        template=template,
                        strategy=strategy,
                        decision_id=None,
                        order_id=None,
                        fill_id=None,
                        greeks=greeks,
                        metadata=None,
                    )


@dataclass(frozen=True)
class ReconcileSummary:
    """Result of reconciling an in-memory snapshot with the table."""

    opened: int     # rows newly inserted
    updated: int    # existing rows whose mark was refreshed
    closed: int     # rows present in DB but missing from snapshot (deleted + CLOSE)


def reconcile_positions(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    mode: str,
    snapshot: Mapping[str, Any],
    as_of_date: date | None = None,
    default_strategy: str | None = None,
    sleeve_by_iid: Mapping[str, str] | None = None,
    template_by_iid: Mapping[str, str] | None = None,
) -> ReconcileSummary:
    """Reconcile an in-memory positions snapshot with ``options_positions``.

    Three cases per position:

    * **In snapshot only** — INSERT new row + OPEN event.
    * **In both** — UPDATE mark/greeks; no event written (use
      ``update_position_mark(write_mark_event=True)`` separately if a
      daily MARK row is wanted).
    * **In table only** — DELETE row + CLOSE event (assumes the
      position closed externally between syncs).

    ``snapshot`` is a mapping ``instrument_id → entry`` where each
    entry has the duck-typed attributes ``symbol, right, expiry,
    strike, quantity, multiplier, avg_cost, market_price,
    market_value, unrealized_pnl, strategy, greeks`` (greeks may be
    ``None`` or an object with ``delta/gamma/theta/vega/implied_vol``).

    ``sleeve_by_iid`` / ``template_by_iid`` let callers tag positions
    opened by the new pipeline with their sleeve+template provenance.
    Legacy strategies pass only ``default_strategy`` (or rely on the
    entry's own ``strategy`` attribute).
    """

    sleeve_map = dict(sleeve_by_iid or {})
    template_map = dict(template_by_iid or {})

    opened = 0
    updated = 0
    closed = 0

    existing = {row.instrument_id: row for row in get_open_positions(
        db_manager, portfolio_id=portfolio_id, mode=mode,
    )}

    snapshot_iids = set()
    for iid, entry in snapshot.items():
        snapshot_iids.add(iid)
        strategy = getattr(entry, "strategy", "") or default_strategy or None
        greeks_dict = _greeks_from_entry(entry)

        if iid in existing:
            update_position_mark(
                db_manager,
                position_id=existing[iid].position_id,
                market_price=_safe_float(getattr(entry, "market_price", None)),
                market_value=_safe_float(getattr(entry, "market_value", None)),
                unrealized_pnl=_safe_float(getattr(entry, "unrealized_pnl", None)),
                greeks=greeks_dict,
                write_mark_event=False,
            )
            updated += 1
        else:
            record_position_open(
                db_manager,
                instrument_id=iid,
                portfolio_id=portfolio_id,
                mode=mode,
                symbol=str(getattr(entry, "symbol", "")),
                right=str(getattr(entry, "right", "")),
                expiry=str(getattr(entry, "expiry", "")),
                strike=float(getattr(entry, "strike", 0.0)),
                quantity=int(getattr(entry, "quantity", 0)),
                avg_cost=float(getattr(entry, "avg_cost", 0.0)),
                multiplier=int(getattr(entry, "multiplier", 100)),
                sleeve=sleeve_map.get(iid),
                template=template_map.get(iid),
                strategy=strategy,
                greeks=greeks_dict,
                as_of_date=as_of_date,
            )
            opened += 1

    # Positions in DB but not in the snapshot → assume closed.
    for iid, row in existing.items():
        if iid in snapshot_iids:
            continue
        record_position_close(
            db_manager,
            position_id=row.position_id,
            quantity_delta=-int(row.quantity),
            price=float(row.market_price or row.avg_cost),
            realized_pnl=None,    # external close; P&L unknown here
            event_type=EVENT_CLOSE,
            as_of_date=as_of_date,
            metadata={"source": "reconcile_external_close"},
        )
        closed += 1

    logger.info(
        "reconcile portfolio=%s mode=%s: opened=%d updated=%d closed=%d",
        portfolio_id, mode, opened, updated, closed,
    )
    return ReconcileSummary(opened=opened, updated=updated, closed=closed)


def _safe_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _greeks_from_entry(entry: Any) -> Mapping[str, float] | None:
    g = getattr(entry, "greeks", None)
    if g is None:
        return None
    out: dict[str, float] = {}
    for attr in ("delta", "gamma", "theta", "vega", "implied_vol", "underlying_price"):
        val = getattr(g, attr, None)
        if val is None:
            continue
        try:
            out[attr] = float(val)
        except (TypeError, ValueError):
            continue
    return out or None


def get_open_positions(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    mode: str,
) -> list[OptionPositionRow]:
    """Return all currently open positions for a portfolio + mode."""

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT position_id, instrument_id, portfolio_id, mode,
                       sleeve, template, strategy,
                       symbol, right, expiry, strike, multiplier, sec_type,
                       quantity, avg_cost, opened_at,
                       market_price, market_value, unrealized_pnl,
                       delta, gamma, theta, vega, implied_vol, underlying_price
                FROM options_positions
                WHERE portfolio_id = %s AND mode = %s
                ORDER BY opened_at
                """,
                (portfolio_id, mode),
            )
            return [
                OptionPositionRow(
                    position_id=r[0], instrument_id=r[1], portfolio_id=r[2], mode=r[3],
                    sleeve=r[4], template=r[5], strategy=r[6],
                    symbol=r[7], right=r[8], expiry=r[9], strike=float(r[10]),
                    multiplier=int(r[11]), sec_type=r[12],
                    quantity=int(r[13]), avg_cost=float(r[14]), opened_at=r[15],
                    market_price=r[16], market_value=r[17], unrealized_pnl=r[18],
                    delta=r[19], gamma=r[20], theta=r[21], vega=r[22],
                    implied_vol=r[23], underlying_price=r[24],
                )
                for r in cur.fetchall()
            ]


def _g(greeks: Mapping[str, float] | None, key: str) -> float | None:
    if greeks is None:
        return None
    val = greeks.get(key)
    return float(val) if val is not None else None


def _insert_event(
    cur: Any,
    *,
    position_id: str,
    portfolio_id: str,
    mode: str,
    event_type: str,
    event_at: datetime,
    as_of_date: date,
    instrument_id: str,
    symbol: str,
    right: str,
    expiry: str,
    strike: float,
    multiplier: int,
    quantity_delta: int,
    price: float | None,
    realized_pnl: float | None,
    sleeve: str | None,
    template: str | None,
    strategy: str | None,
    decision_id: str | None,
    order_id: str | None,
    fill_id: str | None,
    greeks: Mapping[str, float] | None,
    metadata: Mapping[str, Any] | None,
) -> None:
    cur.execute(
        """
        INSERT INTO options_position_events (
            position_id, portfolio_id, mode, event_type,
            event_at, as_of_date,
            instrument_id, symbol, right, expiry, strike, multiplier,
            quantity_delta, price, realized_pnl,
            sleeve, template, strategy,
            decision_id, order_id, fill_id,
            greeks_json, metadata_json
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s
        )
        """,
        (
            position_id, portfolio_id, mode, event_type,
            event_at, as_of_date,
            instrument_id, symbol, right.upper(), expiry,
            float(strike), int(multiplier),
            int(quantity_delta),
            float(price) if price is not None else None,
            float(realized_pnl) if realized_pnl is not None else None,
            sleeve, template, strategy,
            decision_id, order_id, fill_id,
            Json(dict(greeks)) if greeks else None,
            Json(dict(metadata)) if metadata else None,
        ),
    )


__all__ = [
    "OptionPositionRow",
    "ReconcileSummary",
    "EVENT_OPEN",
    "EVENT_CLOSE",
    "EVENT_ROLL",
    "EVENT_EXPIRE",
    "EVENT_ADJUST",
    "EVENT_MARK",
    "record_position_open",
    "record_position_close",
    "update_position_mark",
    "get_open_positions",
    "reconcile_positions",
]
