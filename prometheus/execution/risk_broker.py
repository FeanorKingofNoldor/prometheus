"""Risk-checking broker wrapper for live/paper execution.

This module defines :class:`RiskCheckingBroker`, a ``BrokerInterface``
implementation that wraps another broker and enforces configurable
execution risk limits before forwarding orders to the underlying
implementation.

All limits are driven by environment variables exposed via
:class:`prometheus.core.config.PrometheusConfig` and its
``execution_risk`` property. No numerical thresholds are hardcoded
in this module; a value of ``0`` means that a particular check is
disabled.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from apathis.core.config import ExecutionRiskConfig, get_config
from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

from prometheus.execution.broker_interface import (
    BrokerInterface,
    Fill,
    Order,
    OrderSide,
    OrderStatus,
    Position,
)
from prometheus.risk.engine import RiskActionType
from prometheus.risk.storage import RiskAction, insert_risk_actions

logger = get_logger(__name__)


class RiskLimitExceeded(RuntimeError):
    """Raised when an order violates a configured risk limit."""


@dataclass
class RiskCheckingBroker(BrokerInterface):
    """Broker wrapper that enforces simple, configurable risk limits.

    The wrapper is intentionally conservative and only blocks orders
    when a configured limit would be exceeded. When a limit is not set
    (e.g. ``max_order_notional == 0``), the corresponding check is
    skipped.
    """

    inner: BrokerInterface
    config: ExecutionRiskConfig

    def __init__(self, inner: BrokerInterface, config: Optional[ExecutionRiskConfig] = None) -> None:
        self.inner = inner
        self.config = config or get_config().execution_risk
        # Optional context for logging to risk_actions; these attributes
        # may be populated by the caller.
        self.strategy_id: Optional[str] = getattr(inner, "strategy_id", None)
        self.portfolio_id: Optional[str] = getattr(inner, "portfolio_id", None)

    # --- BrokerInterface delegation -------------------------------------------------

    def submit_order(self, order: Order) -> str:
        """Apply risk checks and, if they pass, forward to inner broker."""

        if not self.config.enabled:
            return self.inner.submit_order(order)

        self._enforce_limits(order)
        return self.inner.submit_order(order)

    def cancel_order(self, order_id: str) -> bool:
        return bool(self.inner.cancel_order(order_id))

    def get_order_status(self, order_id: str) -> OrderStatus:
        return self.inner.get_order_status(order_id)

    def get_fills(self, since: datetime | None = None) -> List[Fill]:
        return list(self.inner.get_fills(since=since))

    def get_positions(self) -> Dict[str, Position]:
        return self.inner.get_positions()

    def get_account_state(self) -> Dict[str, float]:
        return self.inner.get_account_state()

    def sync(self) -> None:
        return self.inner.sync()

    # --- Attribute delegation -------------------------------------------------------

    def __getattr__(self, name: str):
        """Delegate unknown attributes to the inner broker.

        This allows callers that know about attributes on concrete
        broker implementations (e.g. ``client`` on ``LiveBroker``) to
        keep working when a :class:`RiskCheckingBroker` is inserted in
        between.
        """

        return getattr(self.inner, name)

    # --- Risk logic -----------------------------------------------------------------

    def _enforce_limits(self, order: Order) -> None:
        positions = self.inner.get_positions()
        account_state = self.inner.get_account_state()

        est_price = self._estimate_price(order.instrument_id, positions)
        est_notional = abs(est_price * order.quantity)

        # Per-order notional limit
        if self.config.max_order_notional > 0 and est_notional > self.config.max_order_notional:
            reason = (
                f"order notional {est_notional:.2f} exceeds max_order_notional "
                f"{self.config.max_order_notional:.2f} for {order.instrument_id}"
            )
            self._block(order, reason)

        # Per-position notional limit
        if self.config.max_position_notional > 0:
            current_pos = positions.get(order.instrument_id)
            current_qty = current_pos.quantity if current_pos is not None else 0.0
            signed_qty = order.quantity if order.side == OrderSide.BUY else -order.quantity
            new_qty = current_qty + signed_qty
            new_notional = abs(new_qty * est_price)

            if new_notional > self.config.max_position_notional:
                reason = (
                    f"resulting position notional {new_notional:.2f} exceeds "
                    f"max_position_notional {self.config.max_position_notional:.2f} "
                    f"for {order.instrument_id}"
                )
                self._block(order, reason)

        # Leverage limit (gross exposure / equity)
        if self.config.max_leverage > 0:
            equity = float(account_state.get("equity") or 0.0)
            if equity > 0:
                gross = self._gross_exposure(positions) + est_notional
                leverage = gross / equity
                if leverage > self.config.max_leverage:
                    reason = (
                        f"leverage {leverage:.3f} would exceed max_leverage "
                        f"{self.config.max_leverage:.3f}"
                    )
                    self._block(order, reason)

    def _estimate_price(self, instrument_id: str, positions: Dict[str, Position]) -> float:
        """Best-effort price estimate for risk checks.

        For now we use the current position's market value when
        available. If there is no existing position or the market value
        is not available, we fall back to a conservative synthetic
        price of ``100.0`` and log a warning.
        """

        pos = positions.get(instrument_id)
        if pos is not None and pos.quantity:
            try:
                # Avoid division by zero; quantity sign is irrelevant here.
                price = abs(pos.market_value) / abs(pos.quantity)
                if price > 0:
                    return price
            except Exception:  # pragma: no cover - defensive
                logger.exception("Failed to infer price from position for %s", instrument_id)

        logger.warning(
            "RiskCheckingBroker: using synthetic price 100.0 for %s; "
            "configure tighter limits or provide real-time pricing if needed.",
            instrument_id,
        )
        return 100.0

    @staticmethod
    def _gross_exposure(positions: Dict[str, Position]) -> float:
        return float(sum(abs(p.market_value) for p in positions.values()))

    def _block(self, order: Order, reason: str) -> None:
        logger.error("RiskCheckingBroker: blocking order %s: %s", order, reason)

        # Best-effort logging into risk_actions so UI and operators can see
        # why the order was rejected at the execution layer. We treat
        # these as generic EXECUTION_* actions tied to the strategy
        # (if known) and instrument.
        try:
            db_manager = get_db_manager()
            action = RiskAction(
                strategy_id=self.strategy_id,
                instrument_id=order.instrument_id,
                decision_id=None,
                action_type=RiskActionType.EXECUTION_REJECT,  # generic execution-level rejection
                details={
                    "reason": reason,
                    "order_id": order.order_id,
                    "side": order.side.value,
                    "quantity": float(order.quantity),
                    "order_type": order.order_type.value,
                    "portfolio_id": self.portfolio_id,
                },
            )
            insert_risk_actions(db_manager, [action])
        except Exception:  # pragma: no cover - defensive logging path
            logger.exception("RiskCheckingBroker: failed to insert risk_actions row for blocked order")

        raise RiskLimitExceeded(reason)
