"""Prometheus v2 – Pre-staged Crisis Orders.

Uses IBKR conditional orders and OCA (One-Cancels-All) groups for
server-side crisis response:

- On every rebalance, place conditional orders: "if SPY drops X% from
  today's close, sell all equity positions."
- These execute server-side with no round-trip delay.
- Cancel and replace daily as the reference price updates.
- OCA groups ensure all exit orders fire together.

Usage
-----
    from prometheus.execution.crisis_orders import CrisisOrderManager

    mgr = CrisisOrderManager(ibkr_client, mapper)
    mgr.stage_crisis_orders(positions, spy_close=500.0, drop_pct=0.05)
    mgr.cancel_all()  # Before re-staging
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger

from prometheus.execution.broker_interface import Position
from prometheus.execution.instrument_mapper import InstrumentMapper

logger = get_logger(__name__)


@dataclass
class CrisisConfig:
    """Configuration for crisis order staging."""
    drop_pct: float = 0.05           # 5% drop triggers liquidation
    reference_symbol: str = "SPY"    # Price reference instrument
    order_type: str = "MKT"          # MKT or LMT (MKT for guaranteed fill)
    use_adaptive: bool = True        # Use Adaptive algo with Urgent priority
    oca_group_prefix: str = "PROMETHEUS_CRISIS"
    # Second tier: deeper drop triggers options hedges too
    deep_drop_pct: float = 0.08      # 8% drop
    enabled: bool = True


@dataclass
class StagedOrder:
    """Record of a staged conditional order."""
    order_id: str
    instrument_id: str
    ib_order_id: Optional[int] = None
    oca_group: str = ""
    trigger_price: float = 0.0
    quantity: float = 0.0
    staged_at: Optional[datetime] = None


class CrisisOrderManager:
    """Manage pre-staged conditional orders for crisis response.

    Parameters
    ----------
    ib : Any
        Connected ``ib_insync.IB`` instance.
    mapper : InstrumentMapper
        For contract resolution.
    config : CrisisConfig, optional
        Override default crisis parameters.
    """

    def __init__(
        self,
        ib: Any,
        mapper: InstrumentMapper,
        config: Optional[CrisisConfig] = None,
    ) -> None:
        self._ib = ib
        self._mapper = mapper
        self._config = config or CrisisConfig()

        # Tracking
        self._staged_orders: List[StagedOrder] = []
        self._current_oca_group: str = ""

    # ── Stage crisis orders ──────────────────────────────────────────

    def stage_crisis_orders(
        self,
        positions: Dict[str, Position],
        spy_close: float,
        drop_pct: Optional[float] = None,
    ) -> List[StagedOrder]:
        """Stage conditional sell orders for all equity positions.

        Parameters
        ----------
        positions : dict
            Current equity positions keyed by instrument_id.
        spy_close : float
            Today's SPY closing price (reference for condition).
        drop_pct : float, optional
            Override the configured drop percentage.

        Returns
        -------
        list[StagedOrder]
            Records of all staged orders.
        """
        if not self._config.enabled:
            logger.info("Crisis orders disabled")
            return []

        # Cancel existing staged orders first
        self.cancel_all()

        pct = drop_pct or self._config.drop_pct
        trigger_price = round(spy_close * (1 - pct), 2)

        # Generate OCA group ID for this batch
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        oca_group = f"{self._config.oca_group_prefix}_{ts}"
        self._current_oca_group = oca_group

        staged: List[StagedOrder] = []

        for instrument_id, pos in positions.items():
            if pos.quantity <= 0:
                continue  # Only close long positions

            try:
                order_record = self._stage_one(
                    instrument_id=instrument_id,
                    quantity=pos.quantity,
                    trigger_price=trigger_price,
                    oca_group=oca_group,
                )
                if order_record is not None:
                    staged.append(order_record)
            except Exception as exc:
                logger.error(
                    "Failed to stage crisis order for %s: %s",
                    instrument_id, exc,
                )

        self._staged_orders = staged
        logger.info(
            "Staged %d crisis orders (trigger: SPY <= $%.2f, OCA: %s)",
            len(staged), trigger_price, oca_group,
        )

        return staged

    def _stage_one(
        self,
        instrument_id: str,
        quantity: float,
        trigger_price: float,
        oca_group: str,
    ) -> Optional[StagedOrder]:
        """Stage a single conditional sell order."""
        from prometheus.execution.ib_compat import (
            MarketOrder,
            PriceCondition,
        )

        # Build the contract
        contract = self._mapper.get_contract(instrument_id)

        # Qualify
        try:
            qualified = self._ib.qualifyContracts(contract)
            if qualified:
                contract = qualified[0]
        except Exception as exc:
            logger.warning("Could not qualify %s: %s", instrument_id, exc)

        # Build the SPY reference contract for condition
        from prometheus.execution.ib_compat import Stock
        spy_contract = Stock("SPY", "SMART", "USD")
        try:
            spy_qualified = self._ib.qualifyContracts(spy_contract)
            if spy_qualified:
                spy_contract = spy_qualified[0]
        except Exception:
            pass

        # Build conditional SELL order
        sell_order = MarketOrder("SELL", quantity)
        sell_order.orderRef = f"crisis_{instrument_id}_{generate_uuid()[:8]}"

        # Add price condition: SPY last price <= trigger
        condition = PriceCondition(
            price=trigger_price,
            conId=spy_contract.conId,
            exch="SMART",
            isMore=False,  # Trigger when price is LESS than or equal
            conjunction="a",  # AND with other conditions
        )
        sell_order.conditions = [condition]

        # Transmit immediately (live on server)
        sell_order.transmit = True

        # OCA group: all orders in this batch fire together
        sell_order.ocaGroup = oca_group
        sell_order.ocaType = 1  # Cancel remaining on first fill

        # Use Adaptive algo for better fills during crisis
        if self._config.use_adaptive:
            sell_order.algoStrategy = "Adaptive"
            sell_order.algoParams = [
                {"tag": "adaptivePriority", "value": "Urgent"},
            ]

        # Place the order
        try:
            trade = self._ib.placeOrder(contract, sell_order)
            ib_order_id = trade.order.orderId if trade.order else None

            record = StagedOrder(
                order_id=sell_order.orderRef,
                instrument_id=instrument_id,
                ib_order_id=ib_order_id,
                oca_group=oca_group,
                trigger_price=trigger_price,
                quantity=quantity,
                staged_at=datetime.now(timezone.utc),
            )

            logger.info(
                "Staged crisis order: SELL %s x%.0f if SPY <= $%.2f "
                "(IBKR orderId=%s, OCA=%s)",
                instrument_id, quantity, trigger_price,
                ib_order_id, oca_group,
            )

            return record

        except Exception as exc:
            logger.error(
                "Failed to place conditional order for %s: %s",
                instrument_id, exc,
            )
            return None

    # ── Cancel all staged orders ─────────────────────────────────────

    def cancel_all(self) -> int:
        """Cancel all currently staged crisis orders.

        Returns the number of orders successfully cancelled.
        """
        cancelled = 0

        for record in self._staged_orders:
            try:
                # Find the trade by orderRef
                for trade in self._ib.trades():
                    order = getattr(trade, "order", None)
                    if order is None:
                        continue
                    ref = getattr(order, "orderRef", "")
                    if ref == record.order_id:
                        self._ib.cancelOrder(order)
                        cancelled += 1
                        break
                else:
                    # Try by orderId
                    if record.ib_order_id is not None:
                        for trade in self._ib.trades():
                            if (trade.order and
                                    trade.order.orderId == record.ib_order_id):
                                self._ib.cancelOrder(trade.order)
                                cancelled += 1
                                break

            except Exception as exc:
                logger.warning(
                    "Failed to cancel staged order %s: %s",
                    record.order_id, exc,
                )

        if cancelled > 0:
            logger.info("Cancelled %d/%d staged crisis orders",
                       cancelled, len(self._staged_orders))

        self._staged_orders.clear()
        return cancelled

    # ── Daily refresh ────────────────────────────────────────────────

    def refresh(
        self,
        positions: Dict[str, Position],
        spy_close: float,
    ) -> List[StagedOrder]:
        """Cancel existing and re-stage with today's reference price.

        Call this at end-of-day after each rebalance.
        """
        logger.info("Refreshing crisis orders (SPY close=$%.2f)", spy_close)
        return self.stage_crisis_orders(positions, spy_close)

    # ── Status ───────────────────────────────────────────────────────

    @property
    def staged_count(self) -> int:
        return len(self._staged_orders)

    @property
    def current_oca_group(self) -> str:
        return self._current_oca_group

    def get_status(self) -> Dict[str, Any]:
        """Return crisis order manager status."""
        return {
            "enabled": self._config.enabled,
            "staged_orders": len(self._staged_orders),
            "oca_group": self._current_oca_group,
            "drop_pct": self._config.drop_pct,
            "trigger_prices": {
                r.instrument_id: r.trigger_price
                for r in self._staged_orders
            },
        }

    def get_staged_orders(self) -> List[StagedOrder]:
        """Return list of all staged crisis orders."""
        return list(self._staged_orders)


__all__ = [
    "CrisisConfig",
    "StagedOrder",
    "CrisisOrderManager",
]
