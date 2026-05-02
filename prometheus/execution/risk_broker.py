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

from apatheon.core.config import ExecutionRiskConfig, get_config
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

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

        # Drawdown circuit breaker — block ALL new orders when book is in
        # excessive drawdown. Trailing peak is read from
        # ``portfolio_equity_history`` if available; otherwise we fall back
        # to the broker's reported ``high_water_mark`` field. If neither is
        # available the check is skipped (operator should be alerted via
        # missing-data check elsewhere).
        if self.config.max_drawdown_pct > 0:
            equity = float(account_state.get("equity") or account_state.get("NetLiquidation") or 0.0)
            peak = float(account_state.get("high_water_mark") or 0.0)
            if peak <= 0:
                peak = self._lookup_trailing_peak(equity)
            if equity > 0 and peak > 0:
                drawdown = max(0.0, 1.0 - equity / peak)
                if drawdown > self.config.max_drawdown_pct:
                    reason = (
                        f"drawdown circuit breaker tripped: equity={equity:.0f} "
                        f"peak={peak:.0f} dd={drawdown:.2%} > "
                        f"max_drawdown_pct={self.config.max_drawdown_pct:.2%}"
                    )
                    self._block(order, reason)

        # Sector concentration cap
        if self.config.max_sector_concentration_pct > 0:
            equity = float(account_state.get("equity") or account_state.get("NetLiquidation") or 0.0)
            if equity > 0:
                sector = self._lookup_sector(order.instrument_id)
                if sector:
                    sector_gross = self._sector_gross_exposure(positions, sector) + est_notional
                    sector_pct = sector_gross / equity
                    if sector_pct > self.config.max_sector_concentration_pct:
                        reason = (
                            f"sector concentration {sector_pct:.2%} for sector "
                            f"{sector!r} would exceed max_sector_concentration_pct "
                            f"{self.config.max_sector_concentration_pct:.2%}"
                        )
                        self._block(order, reason)

    def _estimate_price(self, instrument_id: str, positions: Dict[str, Position]) -> float:
        """Best-effort price estimate for risk checks.

        Uses the current position's implied price when available. Falls
        back to the latest close price from the historical DB. As a last
        resort, returns a high synthetic price ($1,000) to be conservative
        — this ensures oversized orders are blocked rather than allowed.
        """

        pos = positions.get(instrument_id)
        if pos is not None and pos.quantity:
            try:
                price = abs(pos.market_value) / abs(pos.quantity)
                if price > 0:
                    return price
            except Exception:  # pragma: no cover - defensive
                logger.exception("Failed to infer price from position for %s", instrument_id)

        # Try latest close from DB
        try:
            from apatheon.core.database import get_db_manager

            db = get_db_manager()
            with db.get_historical_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT close FROM prices_daily WHERE instrument_id = %s AND close > 0 "
                        "ORDER BY trade_date DESC LIMIT 1",
                        (instrument_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        return float(row[0])
                finally:
                    cur.close()
        except Exception:
            logger.exception("RiskCheckingBroker: DB price lookup failed for %s", instrument_id)

        # Conservative fallback: high price means notional checks are STRICT
        logger.warning(
            "RiskCheckingBroker: no price available for %s — using conservative $1000 fallback",
            instrument_id,
        )
        return 1000.0

    @staticmethod
    def _gross_exposure(positions: Dict[str, Position]) -> float:
        return float(sum(abs(p.market_value) for p in positions.values()))

    def _lookup_trailing_peak(self, current_equity: float) -> float:
        """Return the trailing peak NAV from runtime DB (last 252 trading days).

        Returns ``0`` when the history table is missing or empty so the
        caller can decide to skip the check rather than incorrectly trip
        the drawdown breaker.
        """
        try:
            db = get_db_manager()
            with db.get_runtime_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT MAX(equity) FROM portfolio_equity_history "
                        "WHERE as_of_date >= (CURRENT_DATE - INTERVAL '252 days')"
                    )
                    row = cur.fetchone()
                finally:
                    cur.close()
            peak = float(row[0]) if row and row[0] else 0.0
            # If we observe a higher *current* equity than the recorded peak
            # (e.g. fresh deploy with empty history), use current equity so
            # the breaker doesn't trip from missing data.
            return max(peak, current_equity)
        except Exception:
            logger.exception("RiskCheckingBroker: trailing-peak lookup failed")
            return 0.0

    def _lookup_sector(self, instrument_id: str) -> Optional[str]:
        """Return the GICS sector for an instrument, or ``None`` if unknown."""
        try:
            db = get_db_manager()
            with db.get_historical_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT sector FROM instruments "
                        "WHERE instrument_id = %s AND sector IS NOT NULL "
                        "LIMIT 1",
                        (instrument_id,),
                    )
                    row = cur.fetchone()
                finally:
                    cur.close()
            return str(row[0]) if row and row[0] else None
        except Exception:
            logger.exception(
                "RiskCheckingBroker: sector lookup failed for %s", instrument_id,
            )
            return None

    def _sector_gross_exposure(
        self, positions: Dict[str, Position], sector: str,
    ) -> float:
        """Sum absolute market values of positions in the given sector."""
        if not positions:
            return 0.0
        instruments = list(positions.keys())
        # Cheap single-query batch lookup; falls back to 0 on failure.
        try:
            db = get_db_manager()
            with db.get_historical_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT instrument_id FROM instruments "
                        "WHERE instrument_id = ANY(%s) AND sector = %s",
                        (instruments, sector),
                    )
                    matching_ids = {row[0] for row in cur.fetchall()}
                finally:
                    cur.close()
        except Exception:
            logger.exception("RiskCheckingBroker: sector exposure lookup failed")
            return 0.0
        return float(sum(
            abs(p.market_value)
            for iid, p in positions.items()
            if iid in matching_ids
        ))

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
