"""Prometheus v2 – Run execution plan for a portfolio via IBKR broker.

This script bridges the daily portfolio targets produced by the
Portfolio & Risk Engine (``target_portfolios`` table) to the unified
execution bridge and a concrete broker implementation (Paper/Live
IBKR).

Initial focus is on PAPER mode for safe testing. LIVE mode wiring is
present but should be used with extreme care and typically in readonly
mode until fully validated.

Usage examples
--------------

Note: with Meta book routing enabled, the daily BOOKS phase typically
writes targets under book_ids like `US_EQ_LONG` / `US_EQ_LONG_DEFENSIVE`
/ `US_EQ_HEDGE_ETF`.

To execute the *meta-selected* book automatically, prefer:

    python -m prometheus.scripts.run.run_execution_for_market \
        --market-id US_EQ \
        --mode PAPER \
        --notional 100000 \
        --dry-run

Run a PAPER execution for the latest targets of a specific portfolio:

    python -m prometheus.scripts.run.run_execution_for_portfolio \
        --portfolio-id US_EQ_LONG \
        --mode PAPER \
        --notional 100000

Run for a specific date:

    python -m prometheus.scripts.run.run_execution_for_portfolio \
        --portfolio-id US_EQ_LONG \
        --mode PAPER \
        --notional 100000 \
        --as-of 2025-12-02

LIVE mode (readonly by default, no orders submitted):

    python -m prometheus.scripts.run.run_execution_for_portfolio \
        --portfolio-id US_EQ_LONG \
        --mode LIVE \
        --readonly \
        --notional 100000
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger

from prometheus.decisions import DecisionTracker
from prometheus.execution.api import apply_execution_plan
from prometheus.execution.broker_factory import create_live_broker, create_paper_broker
from prometheus.execution.broker_interface import OrderStatus
from prometheus.execution.ibkr_config import IbkrGatewayType
from prometheus.execution.order_planner import plan_orders
from prometheus.execution.policy import (
    AccountMode,
    build_constrained_execution_plan,
    load_execution_policy_artifact,
)

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers: parsing and DB access
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string for CLI arguments."""

    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _find_latest_as_of(db_manager: DatabaseManager, portfolio_id: str) -> Optional[date]:
    """Return the most recent as_of_date for which targets exist.

    If no rows are present for the portfolio, returns None.
    """

    sql = """
        SELECT as_of_date
        FROM target_portfolios
        WHERE portfolio_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return None

    as_of_date: date = row[0]
    return as_of_date


def _load_target_snapshot(
    db_manager: DatabaseManager,
    portfolio_id: str,
    as_of: date,
) -> Tuple[Dict[str, float], Dict[str, object]]:
    """Load target weights + metadata for a portfolio/date.

    Returns:
        (weights, metadata)

    Where:
        weights: mapping ``instrument_id -> weight``
        metadata: JSON metadata dict (empty if missing/malformed)

    If no row is found, returns ({}, {}).
    """

    sql = """
        SELECT target_positions, metadata
        FROM target_portfolios
        WHERE portfolio_id = %s
          AND as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (portfolio_id, as_of))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return {}, {}

    positions, metadata_raw = row

    if not isinstance(metadata_raw, dict):
        metadata: Dict[str, object] = {}
    else:
        metadata = {str(k): v for k, v in metadata_raw.items()}

    if not isinstance(positions, dict):
        logger.warning(
            "run_execution_for_portfolio: target_positions payload is not a dict for portfolio_id=%s as_of=%s",
            portfolio_id,
            as_of,
        )
        return {}, metadata

    raw_weights = positions.get("weights") or {}
    try:
        weights: Dict[str, float] = {str(k): float(v) for k, v in raw_weights.items()}
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(
            "run_execution_for_portfolio: failed to parse weights for portfolio_id=%s as_of=%s: %s",
            portfolio_id,
            as_of,
            exc,
        )
        return {}, metadata

    return weights, metadata


def _extract_market_situation(metadata: Dict[str, object]) -> Optional[str]:
    """Best-effort extraction of market_situation from target metadata."""

    direct = metadata.get("market_situation")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    meta_budget = metadata.get("meta_budget")
    if isinstance(meta_budget, dict):
        ms = meta_budget.get("market_situation")
        if isinstance(ms, str) and ms.strip():
            return ms.strip()

    return None


def _find_latest_portfolio_decision(
    db_manager: DatabaseManager,
    *,
    strategy_id: str,
    as_of: date,
) -> Tuple[Optional[str], Optional[str]]:
    """Best-effort: fetch latest PORTFOLIO decision for (strategy_id, as_of).

    Returns:
        (decision_id, market_id)
    """

    sql = """
        SELECT decision_id, market_id
        FROM engine_decisions
        WHERE engine_name = 'PORTFOLIO'
          AND strategy_id = %s
          AND as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (strategy_id, as_of))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return None, None

    decision_id_db, market_id_db = row
    return str(decision_id_db), (str(market_id_db) if market_id_db is not None else None)


def _load_recent_open_order_ids(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    mode: str,
    lookback_days: int,
    stale_before: datetime | None = None,
    limit: int = 2000,
) -> List[str]:
    """Return recent order_ids that are plausibly still open.

    We only have best-effort status tracking in the DB, so we use
    `status IN (...)` and a lookback window to avoid scanning the full
    table.
    """

    lookback_days_eff = int(lookback_days)
    if lookback_days_eff <= 0:
        lookback_days_eff = 1

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days_eff)

    sql = """
        SELECT order_id
        FROM orders
        WHERE portfolio_id = %s
          AND mode = %s
          AND status IN ('PENDING', 'SUBMITTED', 'PARTIALLY_FILLED')
          AND timestamp >= %s
    """

    params: list[object] = [str(portfolio_id), str(mode).upper(), cutoff]

    if stale_before is not None:
        sql += "  AND timestamp <= %s\n"
        params.append(stale_before)

    sql += """ORDER BY timestamp DESC
        LIMIT %s
    """
    params.append(int(limit))

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [str(r[0]) for r in rows if r and r[0] is not None]


def _mark_orders_status(db_manager: DatabaseManager, order_ids: List[str], *, status: OrderStatus) -> None:
    if not order_ids:
        return

    sql = """
        UPDATE orders
        SET status = %s
        WHERE order_id = ANY(%s)
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (status.value, order_ids))
            conn.commit()
        finally:
            cursor.close()


def _cancel_open_orders(
    *,
    db_manager: DatabaseManager,
    broker: object,
    portfolio_id: str,
    mode: str,
    lookback_days: int,
    order_ttl_seconds: int | None,
) -> int:
    """Best-effort: cancel stale open orders recorded in DB for a portfolio/mode."""

    stale_before: datetime | None = None
    if order_ttl_seconds is not None:
        ttl = int(order_ttl_seconds)
        if ttl > 0:
            stale_before = datetime.now(timezone.utc) - timedelta(seconds=ttl)

    order_ids = _load_recent_open_order_ids(
        db_manager,
        portfolio_id=portfolio_id,
        mode=mode,
        lookback_days=lookback_days,
        stale_before=stale_before,
    )

    if not order_ids:
        return 0

    cancellable = {
        OrderStatus.PENDING,
        OrderStatus.SUBMITTED,
        OrderStatus.PARTIALLY_FILLED,
    }

    cancelled_ids: List[str] = []
    terminal_updates: dict[OrderStatus, List[str]] = {}

    for oid in order_ids:
        status: OrderStatus | None = None
        try:
            status = broker.get_order_status(oid)
        except Exception:
            # If we can't query status, attempt cancel anyway.
            status = None

        if status is not None and status not in cancellable:
            terminal_updates.setdefault(status, []).append(oid)
            continue

        try:
            ok = bool(broker.cancel_order(oid))
        except Exception:
            logger.exception("run_execution_for_portfolio: cancel_order failed for %s", oid)
            ok = False

        if ok:
            cancelled_ids.append(oid)

    if cancelled_ids:
        try:
            _mark_orders_status(db_manager, cancelled_ids, status=OrderStatus.CANCELLED)
        except Exception:  # pragma: no cover - defensive
            logger.exception("run_execution_for_portfolio: failed to mark cancelled orders in DB")

    for st, ids in terminal_updates.items():
        try:
            _mark_orders_status(db_manager, ids, status=st)
        except Exception:  # pragma: no cover - defensive
            logger.exception("run_execution_for_portfolio: failed to update order statuses in DB")

    return len(cancelled_ids)


def _load_latest_closes(
    db_manager: DatabaseManager,
    instrument_ids: List[str],
    as_of: date,
) -> Dict[str, float]:
    """Load latest close price on/before ``as_of`` for each instrument.

    Returns mapping ``instrument_id -> close``. Instruments without a
    price are omitted from the result.
    """

    if not instrument_ids:
        return {}

    sql = """
        SELECT instrument_id, trade_date, close
        FROM prices_daily
        WHERE instrument_id = ANY(%s)
          AND trade_date <= %s
        ORDER BY instrument_id ASC, trade_date DESC
    """

    prices: Dict[str, float] = {}

    try:
        with db_manager.get_historical_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (instrument_ids, as_of))
                rows = cursor.fetchall()
            finally:
                cursor.close()
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(
            "run_execution_for_portfolio: failed to load prices from prices_daily: %s",
            exc,
            exc_info=True,
        )
        return {}

    for inst_id_db, trade_date, close in rows:
        inst = str(inst_id_db)
        # First row per instrument_id is the most recent because of ORDER BY.
        if inst not in prices:
            try:
                prices[inst] = float(close)
            except Exception:
                continue

    missing = sorted(set(instrument_ids) - set(prices.keys()))
    if missing:
        logger.warning(
            "run_execution_for_portfolio: missing prices for %d instruments on/before %s (e.g. %s)",
            len(missing),
            as_of,
            ", ".join(missing[:5]),
        )

    return prices


def _compute_target_quantities(
    weights: Dict[str, float],
    prices: Dict[str, float],
    notional: float,
) -> Dict[str, float]:
    """Convert weights + prices into absolute share quantities.

    For each instrument:

    - target_value = weight * notional
    - quantity = floor(target_value / price)

    Instruments with missing prices or non-positive quantities are
    skipped. Quantities are returned as floats but are always
    integer-valued.
    """

    from math import floor

    targets: Dict[str, float] = {}

    for inst_id, weight in weights.items():
        if weight <= 0.0:
            continue
        price = prices.get(inst_id)
        if price is None or price <= 0.0:
            continue

        target_value = notional * float(weight)
        qty = floor(target_value / float(price))
        if qty <= 0:
            continue

        targets[inst_id] = float(qty)

    if not targets:
        logger.warning(
            "run_execution_for_portfolio: no non-zero target quantities after sizing; check weights/prices/notional",
        )

    return targets


# ---------------------------------------------------------------------------
# Broker creation
# ---------------------------------------------------------------------------


def _create_broker(mode: str, readonly: bool) -> object:
    """Create a Live or Paper broker based on mode.

    PAPER mode always uses IB Gateway by default. LIVE mode is created in
    readonly mode unless explicitly disabled. This function returns a
    ``LiveBroker`` or ``PaperBroker`` instance.
    """

    mode_up = mode.upper()

    if mode_up == "PAPER":
        logger.info("Creating PaperBroker (IBKR paper trading)")
        return create_paper_broker(
            gateway_type=IbkrGatewayType.GATEWAY,
            client_id=1,
            readonly=False,
            auto_connect=True,
        )
    elif mode_up == "LIVE":
        logger.info("Creating LiveBroker (IBKR live account), readonly=%s", readonly)
        return create_live_broker(
            gateway_type=IbkrGatewayType.GATEWAY,
            client_id=1,
            readonly=readonly,
            auto_connect=True,
        )
    else:
        raise ValueError(f"Unsupported mode {mode!r}; expected PAPER or LIVE")


def _disconnect_broker(broker: object) -> None:
    """Best-effort disconnect for brokers that wrap an IbkrClient."""

    client = getattr(broker, "client", None)
    if client is not None:
        try:
            client.disconnect()
        except Exception:  # pragma: no cover - defensive
            logger.exception("run_execution_for_portfolio: error while disconnecting broker client")


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the run_execution_for_portfolio CLI."""

    parser = argparse.ArgumentParser(
        description=(
            "Apply an execution plan for a portfolio using IBKR PAPER/LIVE broker "
            "based on target_portfolios weights."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Portfolio identifier (e.g. US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=False,
        help=(
            "As-of date for the snapshot (YYYY-MM-DD). If omitted, uses the latest "
            "available date in target_portfolios."
        ),
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["PAPER", "LIVE"],
        default="PAPER",
        help="Execution mode: PAPER (default) or LIVE",
    )
    parser.add_argument(
        "--notional",
        type=float,
        required=True,
        help="Total notional to allocate according to target weights (account currency)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan constrained target quantities and print a JSON preview without submitting orders",
    )
    parser.add_argument(
        "--cancel-open-orders",
        action="store_true",
        help=(
            "Before submitting new orders, attempt to cancel recent open orders for this "
            "portfolio/mode (best-effort). Ignored in --dry-run."
        ),
    )
    parser.add_argument(
        "--cancel-lookback-days",
        type=int,
        default=7,
        help="How many days of order history to scan when cancelling open orders (default: 7)",
    )
    parser.add_argument(
        "--readonly",
        action="store_true",
        help=(
            "For LIVE mode: create broker in readonly mode (no order submission). "
            "Has no effect in PAPER mode and defaults to True when mode=LIVE."
        ),
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    as_of: Optional[date] = args.as_of
    if as_of is None:
        as_of = _find_latest_as_of(db_manager, args.portfolio_id)
        if as_of is None:
            logger.error(
                "run_execution_for_portfolio: no target_portfolios rows found for portfolio %r",
                args.portfolio_id,
            )
            return

    logger.info(
        "run_execution_for_portfolio: portfolio_id=%s mode=%s as_of=%s notional=%.2f",
        args.portfolio_id,
        args.mode,
        as_of,
        args.notional,
    )

    # Load target weights (+ metadata) for this portfolio/date.
    weights, metadata = _load_target_snapshot(db_manager, args.portfolio_id, as_of)
    if not weights:
        logger.error(
            "run_execution_for_portfolio: no weights found for portfolio_id=%s as_of=%s",
            args.portfolio_id,
            as_of,
        )
        return

    market_situation = _extract_market_situation(metadata)

    # Create broker and apply execution plan.
    readonly = args.readonly or args.mode.upper() == "LIVE"
    broker = _create_broker(args.mode, readonly=readonly)

    try:
        # Pull latest positions before planning.
        try:
            broker.sync()
        except Exception:  # pragma: no cover - defensive
            logger.exception("run_execution_for_portfolio: broker.sync failed; continuing")

        current_positions = broker.get_positions()

        # Load execution policy (also used for optional stale-order cancellation).
        policy_artifact = load_execution_policy_artifact()
        policy = policy_artifact.policy

        mode_up = args.mode.upper()
        should_cancel = bool(args.cancel_open_orders) or bool(policy.order_staleness.cancel_stale_orders)
        if should_cancel and mode_up in {"LIVE", "PAPER"}:
            if args.dry_run:
                logger.warning(
                    "run_execution_for_portfolio: stale-order cancellation skipped in --dry-run mode",
                )
            else:
                lookback_days = (
                    int(args.cancel_lookback_days)
                    if args.cancel_open_orders
                    else int(policy.order_staleness.lookback_days)
                )
                ttl_seconds = int(policy.order_staleness.order_ttl_seconds)

                cancelled = _cancel_open_orders(
                    db_manager=db_manager,
                    broker=broker,
                    portfolio_id=args.portfolio_id,
                    mode=args.mode.upper(),
                    lookback_days=lookback_days,
                    order_ttl_seconds=ttl_seconds,
                )
                if cancelled:
                    logger.info("run_execution_for_portfolio: cancelled %d stale open orders", cancelled)
                    try:
                        broker.sync()
                    except Exception:  # pragma: no cover - defensive
                        logger.exception("run_execution_for_portfolio: broker.sync failed after cancellations")
                    current_positions = broker.get_positions()

        # Load latest close prices for instruments in either the current or target state.
        instrument_ids = sorted(set(weights.keys()) | set(current_positions.keys()))
        prices = _load_latest_closes(db_manager, instrument_ids, as_of)
        if not prices:
            logger.warning(
                "run_execution_for_portfolio: no prices available; using synthetic price=100.0 for all instruments",
            )
            prices = {inst_id: 100.0 for inst_id in instrument_ids}

        # Build constrained plan.

        constrained = build_constrained_execution_plan(
            current_positions=current_positions,
            target_weights=weights,
            prices=prices,
            equity=float(args.notional),
            policy=policy,
            market_situation=market_situation,
        )

        # Preview orders (decision logging + dry-run output).
        preview_orders = plan_orders(
            current_positions=current_positions,
            target_positions=constrained.target_positions,
        )
        orders_generated = [
            {
                "instrument_id": o.instrument_id,
                "side": o.side.value,
                "quantity": float(o.quantity),
                "order_type": o.order_type.value,
            }
            for o in preview_orders
        ]

        policy_ctx: Dict[str, Any] = {
            "artifact": {
                "version": policy_artifact.version,
                "updated_at": policy_artifact.updated_at,
                "updated_by": policy_artifact.updated_by,
            },
            "policy": {
                "account_mode": policy.account_mode.value,
                "turnover": {"one_way_limit": float(policy.turnover.one_way_limit)},
                "no_trade_band_bps": float(policy.no_trade_band_bps),
                "min_trade_notional": {
                    "buy_min_notional": float(policy.min_trade_notional.buy_min_notional),
                    "sells_exempt": bool(policy.min_trade_notional.sells_exempt),
                },
                "cash_buffer_weight": float(policy.cash_buffer_weight),
                "crisis": {
                    "turnover_override_sells": bool(policy.crisis.turnover_override_sells),
                    "keep_no_trade_band": bool(policy.crisis.keep_no_trade_band),
                    "keep_buy_min_notional": bool(policy.crisis.keep_buy_min_notional),
                    "keep_cash_buffer_weight": bool(policy.crisis.keep_cash_buffer_weight),
                },
                "order_staleness": {
                    "cancel_stale_orders": bool(policy.order_staleness.cancel_stale_orders),
                    "order_ttl_seconds": int(policy.order_staleness.order_ttl_seconds),
                    "lookback_days": int(policy.order_staleness.lookback_days),
                },
            },
        }

        sells_first = policy.account_mode == AccountMode.CASH

        if args.dry_run:
            report = {
                "portfolio_id": args.portfolio_id,
                "mode": args.mode.upper(),
                "as_of": as_of.isoformat(),
                "notional": float(args.notional),
                "readonly": bool(readonly),
                "market_situation": market_situation,
                "policy": policy_ctx,
                "plan_summary": constrained.summary,
                "orders_preview": orders_generated,
            }
            print(json.dumps(report, indent=2, sort_keys=True))
            return

        # Record an EXECUTION decision (best-effort) and submit orders.
        decision_id: Optional[str] = None
        try:
            portfolio_decision_id, market_id = _find_latest_portfolio_decision(
                db_manager,
                strategy_id=args.portfolio_id,
                as_of=as_of,
            )

            tracker = DecisionTracker(db_manager=db_manager)
            decision_id = tracker.record_execution_decision(
                strategy_id=args.portfolio_id,
                market_id=market_id or "UNKNOWN",
                as_of_date=as_of,
                portfolio_id=args.portfolio_id,
                orders_generated=orders_generated,
                portfolio_decision_id=portfolio_decision_id,
                current_positions={
                    inst_id: float(pos.quantity) for inst_id, pos in current_positions.items()
                },
                plan_summary=constrained.summary,
                execution_policy=policy_ctx,
                run_id=None,
                config_id=policy_artifact.version,
                metadata={
                    "mode": args.mode.upper(),
                    "readonly": bool(readonly),
                    "sells_first": bool(sells_first),
                },
            )
        except Exception:  # pragma: no cover - defensive
            logger.exception("run_execution_for_portfolio: failed to record execution decision")
            decision_id = None

        summary = apply_execution_plan(
            db_manager=db_manager,
            broker=broker,
            portfolio_id=args.portfolio_id,
            target_positions=constrained.target_positions,
            mode=args.mode.upper(),
            as_of_date=as_of,
            decision_id=decision_id,
            record_positions=True,
            sells_first=bool(sells_first),
        )

        logger.info(
            "run_execution_for_portfolio: completed execution – orders=%d fills=%d",
            summary.num_orders,
            summary.num_fills,
        )
    finally:
        _disconnect_broker(broker)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
