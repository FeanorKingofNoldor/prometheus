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

from typing import Dict, List, Optional

from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger

from prometheus.execution.broker_interface import Order, OrderSide, OrderType, Position


logger = get_logger(__name__)


# Minimum absolute quantity before an order is emitted.
MIN_ABS_QUANTITY: float = 1e-6

# Default minimum rebalance threshold as a fraction of position size.
# Orders where |delta| / max(current, target) < this threshold are
# suppressed to reduce churn.  2% means a 100-share position won't
# generate an order for < 2 shares of delta.
DEFAULT_MIN_REBALANCE_PCT: float = 0.02

# Default limit order buffer: how far above/below the reference price
# to place limit orders.  0.001 = 0.1% (10 bps).
DEFAULT_LIMIT_BUFFER_PCT: float = 0.001


def plan_orders(
    current_positions: Dict[str, Position],
    target_positions: Dict[str, float],
    order_type: OrderType = OrderType.MARKET,
    min_abs_quantity: float = MIN_ABS_QUANTITY,
    *,
    min_rebalance_pct: float = DEFAULT_MIN_REBALANCE_PCT,
    prices: Optional[Dict[str, float]] = None,
    limit_buffer_pct: float = DEFAULT_LIMIT_BUFFER_PCT,
    sells_first: bool = True,
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

        order = Order(
            order_id=generate_uuid(),
            instrument_id=instrument_id,
            side=side,
            order_type=effective_type,
            quantity=abs(delta),
            limit_price=limit_price,
        )
        orders.append(order)

    # Sort: sells before buys to free cash first.
    if sells_first:
        orders.sort(
            key=lambda o: (0 if o.side == OrderSide.SELL else 1, o.instrument_id),
        )

    if orders or suppressed:
        logger.info(
            "OrderPlanner.plan_orders: %d orders (%d suppressed by turnover filter) for %d instruments",
            len(orders),
            suppressed,
            len(all_instruments),
        )

    return orders
