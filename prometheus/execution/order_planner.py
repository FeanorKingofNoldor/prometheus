"""Prometheus v2 – Order planning utilities.

This module provides helpers to convert desired target positions into
executable :class:`~prometheus.execution.broker_interface.Order` objects.

Key features:
- **Turnover filter**: Suppresses small delta trades that add cost but
  negligible alpha.
- **Sells-first ordering**: Frees cash before buying new positions.
- **Limit order support**: When prices are provided, generates LIMIT
  orders with a configurable buffer instead of MARKET orders.
"""

from __future__ import annotations

import math
import os
import time
from typing import Dict, List, Optional, Tuple

from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger

from prometheus.execution.broker_interface import Order, OrderSide, OrderType, Position

logger = get_logger(__name__)


# Minimum absolute quantity before an order is emitted.
MIN_ABS_QUANTITY: float = 1e-6

# Duplicate order prevention: if the same (instrument_id, side) was
# ordered within this window (seconds), the order is suppressed.
_DEDUP_WINDOW_SECONDS: float = 60.0

# Module-level dedup ledger: {(instrument_id, side): timestamp}.
# Stored as a mutable attribute on plan_orders so tests can reliably
# clear it regardless of how many module copies exist.
_recent_orders: Dict[Tuple[str, str], float] = {}

# Compiled defaults — kept as constants for backward compatibility.
_COMPILED_MIN_REBALANCE_PCT: float = 0.02
_COMPILED_LIMIT_BUFFER_PCT: float = 0.001

# Default minimum rebalance threshold as a fraction of position size.
# Orders where |delta| / max(current, target) < this threshold are
# suppressed to reduce churn.  2% means a 100-share position won't
# generate an order for < 2 shares of delta.
DEFAULT_MIN_REBALANCE_PCT: float = _COMPILED_MIN_REBALANCE_PCT

# Default limit order buffer: how far above/below the reference price
# to place limit orders.  0.001 = 0.1% (10 bps).
DEFAULT_LIMIT_BUFFER_PCT: float = _COMPILED_LIMIT_BUFFER_PCT


def _resolve_min_rebalance_pct() -> float:
    """Return min rebalance pct from env var or compiled default (0.02)."""
    raw = os.environ.get("PROMETHEUS_MIN_REBALANCE_PCT")
    if raw is not None:
        try:
            return float(raw)
        except (ValueError, TypeError):
            pass
    return _COMPILED_MIN_REBALANCE_PCT


def _resolve_limit_buffer_pct() -> float:
    """Return limit buffer pct from env var or compiled default (0.001)."""
    raw = os.environ.get("PROMETHEUS_LIMIT_BUFFER_PCT")
    if raw is not None:
        try:
            return float(raw)
        except (ValueError, TypeError):
            pass
    return _COMPILED_LIMIT_BUFFER_PCT


# Sentinel to signal "use env-var-aware default at call time".
_USE_ENV_DEFAULT = object()


def plan_orders(
    current_positions: Dict[str, Position],
    target_positions: Dict[str, float],
    order_type: OrderType = OrderType.MARKET,
    min_abs_quantity: float = MIN_ABS_QUANTITY,
    *,
    min_rebalance_pct: float | object = _USE_ENV_DEFAULT,
    prices: Optional[Dict[str, float]] = None,
    limit_buffer_pct: float | object = _USE_ENV_DEFAULT,
    sells_first: bool = True,
    long_only: bool = False,
) -> List[Order]:
    """Compute orders required to move from current to target positions.

    Args:
        current_positions: Mapping from instrument_id to current
            :class:`Position` objects.
        target_positions: Mapping from instrument_id to desired absolute
            quantities (same units as ``Position.quantity``).
        order_type: Order type to use.  When ``LIMIT`` and ``prices``
            are provided, limit prices are computed automatically.
        min_abs_quantity: Minimum absolute quantity change.
        min_rebalance_pct: Minimum fractional change before an order is
            emitted.  Set to 0.0 to disable turnover filtering.
        prices: Optional mapping of instrument_id → reference price.
            Used to compute limit prices when ``order_type`` is LIMIT.
        limit_buffer_pct: Buffer added/subtracted from reference price
            for limit orders (as a fraction, e.g. 0.001 = 10 bps).
        sells_first: When True, SELL orders are placed before BUY orders
            in the returned list to free cash before buying.

    Returns:
        A list of :class:`Order` objects representing the required trades.
    """

    # Resolve env-var-aware defaults at call time.
    if min_rebalance_pct is _USE_ENV_DEFAULT:
        min_rebalance_pct = _resolve_min_rebalance_pct()
    if limit_buffer_pct is _USE_ENV_DEFAULT:
        limit_buffer_pct = _resolve_limit_buffer_pct()
    min_rebalance_pct = float(min_rebalance_pct)  # type: ignore[arg-type]
    limit_buffer_pct = float(limit_buffer_pct)  # type: ignore[arg-type]

    orders: List[Order] = []
    suppressed = 0

    all_instruments = set(current_positions.keys()) | set(target_positions.keys())

    for instrument_id in sorted(all_instruments):
        current = current_positions.get(
            instrument_id,
            Position(
                instrument_id=instrument_id,
                quantity=0.0,
                avg_cost=0.0,
                market_value=0.0,
                unrealized_pnl=0.0,
            ),
        )
        current_qty = float(current.quantity)
        target_qty = float(target_positions.get(instrument_id, 0.0))
        delta = target_qty - current_qty

        # Reject NaN/inf deltas that would produce invalid orders.
        if math.isnan(delta) or math.isinf(delta):
            logger.warning(
                "OrderPlanner: skipping instrument %s — delta is %s (current=%s, target=%s)",
                instrument_id, delta, current_qty, target_qty,
            )
            continue

        if abs(delta) < min_abs_quantity:
            continue

        # Turnover filter: skip noise trades where the change is a tiny
        # fraction of the position size.
        if min_rebalance_pct > 0:
            ref_qty = max(abs(current_qty), abs(target_qty))
            if ref_qty > 0 and abs(delta) / ref_qty < min_rebalance_pct:
                suppressed += 1
                continue

        side = OrderSide.BUY if delta > 0 else OrderSide.SELL

        # Compute limit price when applicable.
        limit_price: Optional[float] = None
        effective_type = order_type
        if order_type == OrderType.LIMIT and prices is not None:
            ref_price = prices.get(instrument_id)
            if ref_price is not None and ref_price > 0:
                if side == OrderSide.BUY:
                    limit_price = round(ref_price * (1 + limit_buffer_pct), 2)
                else:
                    limit_price = round(ref_price * (1 - limit_buffer_pct), 2)
            else:
                # No price available — fall back to MARKET.
                effective_type = OrderType.MARKET

        # Guard against NaN or non-positive limit prices from bad inputs.
        if limit_price is not None and (math.isnan(limit_price) or limit_price <= 0):
            logger.warning(
                "OrderPlanner: invalid limit_price=%s for %s — falling back to MARKET",
                limit_price, instrument_id,
            )
            effective_type = OrderType.MARKET
            limit_price = None

        # Duplicate order prevention: skip if the same (instrument, side)
        # was ordered within the dedup window. The dedup state is stored
        # as a function attribute so it travels with the function reference
        # even if the module is reloaded.
        dedup_key = (instrument_id, side.value)
        now_ts = time.monotonic()
        dedup_ledger = plan_orders._dedup_ledger  # type: ignore[attr-defined]
        last_ts = dedup_ledger.get(dedup_key)
        if last_ts is not None and (now_ts - last_ts) < _DEDUP_WINDOW_SECONDS:
            logger.warning(
                "OrderPlanner: suppressing duplicate order for %s %s (last ordered %.1fs ago)",
                instrument_id, side.value, now_ts - last_ts,
            )
            continue

        order = Order(
            order_id=generate_uuid(),
            instrument_id=instrument_id,
            side=side,
            order_type=effective_type,
            quantity=abs(delta),
            limit_price=limit_price,
        )
        orders.append(order)
        dedup_ledger[dedup_key] = now_ts

    # Sort: sells before buys to free cash first.
    if sells_first:
        orders.sort(
            key=lambda o: (0 if o.side == OrderSide.SELL else 1, o.instrument_id),
        )

    # Validate: long-only strategies must not have SELL orders that would
    # create short positions.
    if long_only:
        for order in orders:
            if order.side == OrderSide.SELL:
                current_qty = float(
                    current_positions[order.instrument_id].quantity
                ) if order.instrument_id in current_positions else 0.0
                if order.quantity > current_qty:
                    logger.error(
                        "Long-only violation: SELL %s qty=%f exceeds position %f — clamping",
                        order.instrument_id, order.quantity, current_qty,
                    )
                    order.quantity = max(0.0, current_qty)
        # Remove orders with zero quantity after clamping
        orders = [o for o in orders if o.quantity > min_abs_quantity]

    if orders or suppressed:
        logger.info(
            "OrderPlanner.plan_orders: %d orders (%d suppressed by turnover filter) for %d instruments",
            len(orders),
            suppressed,
            len(all_instruments),
        )

    return orders


# Attach the dedup ledger as a function attribute so it can be reliably
# cleared in tests regardless of module reloading.
plan_orders._dedup_ledger = _recent_orders  # type: ignore[attr-defined]
