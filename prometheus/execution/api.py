"""Prometheus v2 – Execution API.

This module provides a small, mode-agnostic helper for applying an
execution plan given target positions and a :class:`BrokerInterface`.

The core entrypoint :func:`apply_execution_plan`:

- Computes required orders via :func:`order_planner.plan_orders`.
- Submits them through the provided broker.
- In BACKTEST mode, calls ``BacktestBroker.process_fills`` for the
  current date and records fills.
- Persists orders, fills, and an optional positions snapshot into the
  runtime database using :mod:`prometheus.execution.storage`.

This helper is designed to be used by both backtesting code and future
live/paper execution flows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from time import monotonic
from time import sleep as _blocking_sleep
from typing import Dict, List, Mapping

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

from prometheus.execution.backtest_broker import BacktestBroker
from prometheus.execution.broker_interface import (
    BrokerInterface,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from prometheus.execution.executed_actions import (
    ExecutedActionContext,
    record_executed_actions_for_fills,
)
from prometheus.execution.fx import FxConverter, FxRateUnavailable
from prometheus.execution.order_planner import plan_orders
from prometheus.execution.storage import (
    ExecutionMode,
    record_fills,
    record_orders,
    record_positions_snapshot,
    update_order_statuses,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class ExecutionSummary:
    """Lightweight summary of an execution step."""

    num_orders: int
    num_fills: int


class ExecutionSafetyError(RuntimeError):
    """Raised when planned orders breach a hard safety limit.

    Raised BEFORE any order is submitted, so callers can treat it as a
    hard abort (e.g. mark the pipeline run FAILED) knowing nothing
    reached the broker.
    """


def apply_execution_plan(
    db_manager: DatabaseManager,
    *,
    broker: BrokerInterface,
    portfolio_id: str | None,
    target_positions: Mapping[str, float],
    mode: str,
    as_of_date: date | None = None,
    decision_id: str | None = None,
    run_id: str | None = None,
    record_positions: bool = True,
    sells_first: bool = False,
    order_type: "OrderType | None" = None,
    prices: Mapping[str, float] | None = None,
    prices_currency: Mapping[str, str] | None = None,
    fx: FxConverter | None = None,
    max_orders: int | None = None,
    max_single_order_value: float | None = None,
    status_poll_timeout_sec: float = 30.0,
    status_poll_interval_sec: float = 2.0,
    cancel_unfilled_on_timeout: bool = True,
) -> ExecutionSummary:
    """Apply an execution plan for ``target_positions`` via ``broker``.

    Args:
        db_manager: Runtime database manager.
        broker: Concrete :class:`BrokerInterface` implementation.
        portfolio_id: Logical portfolio identifier associated with the
            orders (may be ``None`` for some strategies).
        target_positions: Mapping from instrument_id to desired absolute
            quantity.
        mode: Execution mode (``"LIVE"``, ``"PAPER"``, or ``"BACKTEST"``).
        as_of_date: Optional trading date for the step. Required for
            BACKTEST mode to process fills at the correct date.
        decision_id: Optional engine decision id that produced the
            orders.
        record_positions: If True, also persist a positions snapshot
            after fills are processed.
        prices_currency: Optional mapping of instrument_id → currency
            code for the ``prices`` mapping. When provided (with ``fx``),
            the ``max_single_order_value`` check converts local reference
            prices to USD, and limit prices are rounded to the local
            tick convention. Omitted → prices are treated as USD
            (historical behavior).
        fx: Optional :class:`~prometheus.execution.fx.FxConverter` used
            for the USD conversion above.
        status_poll_timeout_sec: For PAPER/LIVE mode, how long to poll
            broker statuses after submission.
        status_poll_interval_sec: Poll interval in seconds for
            PAPER/LIVE status updates.
        cancel_unfilled_on_timeout: If True (default), orders still
            non-terminal at the poll deadline are cancelled. Post-close
            pipelines MUST pass False: nothing can fill while the market
            is closed, so cancelling here would guarantee zero fills —
            DAY orders are left working for the next session instead and
            the morning reconcile captures their fills.

    Returns:
        :class:`ExecutionSummary` with counts of orders and fills.
    """

    # 1) Sync broker state so positions reflect the latest IBKR data.
    if hasattr(broker, "sync"):
        broker.sync()
    current_positions = broker.get_positions()
    plan_kwargs: dict = {}
    if order_type is not None:
        plan_kwargs["order_type"] = order_type
        if prices is not None:
            plan_kwargs["prices"] = prices
            if prices_currency is not None:
                plan_kwargs["currency_by_instrument"] = prices_currency
    orders = plan_orders(
        current_positions=current_positions,
        target_positions=target_positions,
        portfolio_id=portfolio_id,
        as_of_date=as_of_date,
        **plan_kwargs,
    )

    # Hard safety limits — checked BEFORE anything is submitted.
    if max_orders is not None and len(orders) > max_orders:
        raise ExecutionSafetyError(
            f"planned {len(orders)} orders exceeds max_orders={max_orders}"
        )
    if max_single_order_value is not None and max_single_order_value > 0:
        check_date = as_of_date or datetime.now(timezone.utc).date()
        for order in orders:
            ref_price = float((prices or {}).get(order.instrument_id, 0.0)) or float(
                order.limit_price or 0.0
            )
            # Non-USD reference prices must be expressed in USD before
            # comparing against the (USD) limit. Without prices_currency/fx
            # the price is treated as USD, preserving historical behavior.
            currency = (prices_currency or {}).get(order.instrument_id)
            if fx is not None and currency and currency.upper() != "USD" and ref_price > 0:
                try:
                    ref_price = fx.price_to_usd(
                        ref_price, currency, order.instrument_id, check_date
                    )
                except FxRateUnavailable as exc:
                    raise ExecutionSafetyError(
                        f"order {order.instrument_id}: FX rate unavailable — cannot "
                        f"verify max_single_order_value in USD: {exc}"
                    ) from exc
            notional = abs(order.quantity * ref_price)
            if notional > max_single_order_value:
                raise ExecutionSafetyError(
                    f"order {order.instrument_id} notional ${notional:,.0f} exceeds "
                    f"max_single_order_value ${max_single_order_value:,.0f}"
                )

    if not orders:
        logger.info("apply_execution_plan: no orders generated; nothing to do")
        # Optionally record a positions snapshot even if no orders.
        if record_positions and portfolio_id is not None and current_positions and as_of_date is not None:
            record_positions_snapshot(
                db_manager=db_manager,
                portfolio_id=portfolio_id,
                positions=current_positions,
                as_of_date=as_of_date,
                mode=mode,
            )
        return ExecutionSummary(num_orders=0, num_fills=0)

    if sells_first:
        orders = sorted(
            orders,
            key=lambda o: (
                0 if o.side == OrderSide.SELL else 1,
                o.instrument_id,
            ),
        )

    # 2) Submit orders via broker.
    #
    # Each submission is isolated: a single order that trips a risk limit
    # (RiskLimitExceeded) or is otherwise rejected by the broker MUST NOT
    # abort the rest of the batch. We log and skip the offending order and
    # continue submitting the remainder. Only orders that were actually
    # accepted are persisted and tracked for status/fill reconciliation.
    submission_started_at = datetime.now(timezone.utc)
    submitted_orders: List[Order] = []
    rejected = 0
    for order in orders:
        try:
            broker.submit_order(order)
        except Exception as exc:
            rejected += 1
            logger.warning(
                "apply_execution_plan: skipping order %s (%s %s qty=%s) — submission "
                "rejected: %s; continuing with remaining %d order(s)",
                order.order_id,
                order.instrument_id,
                order.side.value,
                order.quantity,
                exc,
                len(orders) - len(submitted_orders) - rejected,
            )
            continue
        submitted_orders.append(order)

    if rejected:
        logger.warning(
            "apply_execution_plan: %d of %d orders rejected/skipped; %d submitted",
            rejected,
            len(orders),
            len(submitted_orders),
        )

    submitted_order_ids = [order.order_id for order in submitted_orders]
    submitted_order_ids_set = set(submitted_order_ids)

    if not submitted_orders:
        logger.warning("apply_execution_plan: no orders accepted by broker; nothing persisted")
        return ExecutionSummary(num_orders=0, num_fills=0)

    # 3) Persist orders to DB (only those actually accepted).
    record_orders(
        db_manager=db_manager,
        portfolio_id=portfolio_id,
        orders=submitted_orders,
        mode=mode,
        decision_id=decision_id,
        as_of_date=as_of_date,
    )

    # 4) Process fills and reconcile statuses.
    fills: List[Fill] = []
    order_statuses: Dict[str, OrderStatus] = {}
    mode_up = mode.upper()
    if mode.upper() == ExecutionMode.BACKTEST and isinstance(broker, BacktestBroker):
        if as_of_date is None:
            raise ValueError("apply_execution_plan: as_of_date is required for BACKTEST mode")
        fills = broker.process_fills(as_of_date)
        if fills:
            record_fills(db_manager=db_manager, fills=fills, mode=mode)
        for order_id in submitted_order_ids:
            try:
                order_statuses[order_id] = broker.get_order_status(order_id)
            except Exception:  # pragma: no cover - defensive
                logger.debug("apply_execution_plan: failed to fetch backtest status for order_id=%s", order_id)
    else:
        # PAPER/LIVE: poll broker statuses for submitted orders and
        # persist any fills emitted during this submission window.
        # Use ib.sleep() instead of time.sleep() so the ib_async event
        # loop can process fill callbacks (execDetailsEvent).
        pending = set(submitted_order_ids)
        deadline = monotonic() + max(0.0, float(status_poll_timeout_sec))
        poll_interval = max(0.2, float(status_poll_interval_sec))

        # Resolve ib.sleep — walk broker wrappers to find the IB instance.
        _sleep = _blocking_sleep
        _inner = getattr(broker, "inner", broker)
        _client = getattr(_inner, "client", getattr(_inner, "_client", None))
        _ib = getattr(_client, "_ib", None)
        if _ib is not None and hasattr(_ib, "sleep"):
            _sleep = _ib.sleep

        terminal_statuses = {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
        }

        while pending and monotonic() < deadline:
            for order_id in list(pending):
                try:
                    status = broker.get_order_status(order_id)
                except Exception:  # pragma: no cover - defensive
                    continue
                order_statuses[order_id] = status
                if status in terminal_statuses:
                    pending.discard(order_id)
            if pending:
                _sleep(poll_interval)

        # Final status refresh (including any still-pending ids).
        for order_id in submitted_order_ids:
            try:
                order_statuses[order_id] = broker.get_order_status(order_id)
            except Exception:  # pragma: no cover - defensive
                pass

        # Orders that turned terminal by the final refresh are done — only
        # genuinely working orders are candidates for the cancel decision.
        pending = {oid for oid in pending if order_statuses.get(oid) not in terminal_statuses}

        if pending and cancel_unfilled_on_timeout:
            logger.warning(
                "apply_execution_plan: %d orders still non-terminal after %.1fs timeout — "
                "attempting to cancel remaining orders",
                len(pending),
                float(status_poll_timeout_sec),
            )
            # Cancel any still-pending orders to prevent them from filling
            # unexpectedly after we've moved on.
            for order_id in pending:
                try:
                    broker.cancel_order(order_id)
                    logger.info("apply_execution_plan: cancelled pending order %s", order_id)
                except Exception:
                    logger.warning("apply_execution_plan: failed to cancel order %s", order_id)
            # Give the broker a beat to acknowledge, then refresh so the
            # persisted statuses say CANCELLED rather than a stale SUBMITTED.
            _sleep(2.0)
            for order_id in pending:
                try:
                    order_statuses[order_id] = broker.get_order_status(order_id)
                except Exception:  # pragma: no cover - defensive
                    pass
        elif pending:
            logger.info(
                "apply_execution_plan: %d orders still working at poll deadline — "
                "left open for the next session (cancel_unfilled_on_timeout=False); "
                "morning reconcile will capture their fills",
                len(pending),
            )

        try:
            broker_fills = broker.get_fills(since=submission_started_at)
        except Exception:  # pragma: no cover - defensive
            logger.exception("apply_execution_plan: failed to fetch broker fills")
            broker_fills = []

        # Keep only fills corresponding to this submission batch.
        seen_fill_ids: set[str] = set()
        for fill in broker_fills:
            if fill.order_id not in submitted_order_ids_set:
                continue
            if fill.fill_id in seen_fill_ids:
                continue
            seen_fill_ids.add(fill.fill_id)
            fills.append(fill)

        if fills:
            record_fills(db_manager=db_manager, fills=fills, mode=mode)
            try:
                record_executed_actions_for_fills(
                    db_manager=db_manager,
                    fills=fills,
                    context=ExecutedActionContext(
                        run_id=run_id,
                        portfolio_id=portfolio_id,
                        decision_id=decision_id,
                        mode=mode_up,
                    ),
                )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "apply_execution_plan: failed to record executed_actions for portfolio_id=%s mode=%s",
                    portfolio_id,
                    mode_up,
                )

    if order_statuses:
        update_order_statuses(db_manager=db_manager, statuses=order_statuses)

    # 5) Optionally record a positions snapshot after execution.
    if record_positions and portfolio_id is not None and as_of_date is not None:
        positions_after = broker.get_positions()
        if positions_after:
            record_positions_snapshot(
                db_manager=db_manager,
                portfolio_id=portfolio_id,
                positions=positions_after,
                as_of_date=as_of_date,
                mode=mode,
            )

    logger.info(
        "apply_execution_plan: mode=%s portfolio_id=%s orders=%d fills=%d",
        mode,
        portfolio_id,
        len(submitted_orders),
        len(fills),
    )

    return ExecutionSummary(num_orders=len(submitted_orders), num_fills=len(fills))
