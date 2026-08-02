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

from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Dict, List, NoReturn, Optional

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
from prometheus.execution.fx import FxConverter, FxRateUnavailable
from prometheus.risk.engine import RiskActionType
from prometheus.risk.storage import RiskAction, insert_risk_actions

logger = get_logger(__name__)


class RiskLimitExceeded(RuntimeError):
    """Raised when an order violates a configured risk limit."""


# Minimum tradeable quantity; orders clamped below this are rejected
# (skipped) rather than submitted as a near-zero, pointless trade.
_MIN_CLAMP_QUANTITY: float = 1e-6


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

    def __init__(
        self,
        inner: BrokerInterface,
        config: Optional[ExecutionRiskConfig] = None,
        fx: Optional[FxConverter] = None,
        equity_history_portfolio_id: Optional[str] = None,
    ) -> None:
        self.inner = inner
        self.config = config or get_config().execution_risk
        # Scopes the drawdown breaker's trailing-peak lookup to this
        # account's equity curve ('IBKR_PAPER' / 'IBKR_LIVE'); unset →
        # unfiltered across portfolios (historical behavior).
        self.equity_history_portfolio_id = equity_history_portfolio_id
        # FX converter used to express non-USD price estimates in USD so
        # all notional/leverage/sector math stays in one currency. Lazily
        # constructed from the default DB manager on first non-USD order
        # when not injected.
        self._fx: Optional[FxConverter] = fx
        # instrument_id → currency memo for _lookup_currency.
        self._currency_cache: Dict[str, Optional[str]] = {}
        # Optional context for logging to risk_actions; these attributes
        # may be populated by the caller.
        self.strategy_id: Optional[str] = getattr(inner, "strategy_id", None)
        self.portfolio_id: Optional[str] = getattr(inner, "portfolio_id", None)

    # --- BrokerInterface delegation -------------------------------------------------

    def submit_order(self, order: Order) -> str:
        """Apply risk checks and, if they pass, forward to inner broker.

        Where a notional cap is exceeded the order quantity is CLAMPED
        down to the largest size that fits the limit (and the clamped
        order is forwarded) rather than hard-rejected. Limits that cannot
        be satisfied by resizing a single order (leverage, drawdown,
        sector concentration) still raise :class:`RiskLimitExceeded`; the
        batch submit loop in :mod:`prometheus.execution.api` catches that
        so one bad order skips itself without killing the rest of the batch.
        """

        if not self.config.enabled:
            return self.inner.submit_order(order)

        checked = self._enforce_limits(order)
        return self.inner.submit_order(checked)

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

    def _enforce_limits(self, order: Order) -> Order:
        """Validate ``order`` against configured limits.

        Returns the order to actually submit. Per-order and per-position
        notional caps CLAMP the quantity down to fit; the other limits
        raise :class:`RiskLimitExceeded` on violation.
        """
        positions = self.inner.get_positions()
        account_state = self.inner.get_account_state()

        try:
            est_price = self._estimate_price(order.instrument_id, positions)
        except FxRateUnavailable as exc:
            # Without a rate we cannot express the order in USD, so no
            # notional/leverage/sector limit can be checked — block it.
            self._block(
                order,
                f"FX rate unavailable for {order.instrument_id} — cannot "
                f"compute USD notional for risk checks: {exc}",
            )
        est_notional = abs(est_price * order.quantity)

        # Per-order notional limit — clamp quantity down to fit.
        if (
            self.config.max_order_notional > 0
            and est_notional > self.config.max_order_notional
            and est_price > 0
        ):
            max_qty = self.config.max_order_notional / est_price
            order = self._clamp_quantity(
                order,
                max_qty,
                reason=(
                    f"order notional {est_notional:.2f} exceeds max_order_notional "
                    f"{self.config.max_order_notional:.2f} for {order.instrument_id}"
                ),
            )
            est_notional = abs(est_price * order.quantity)

        # Per-position notional limit — clamp the order so the resulting
        # position notional fits. A BUY that would overshoot is reduced; a
        # SELL/short that would overshoot is reduced too.
        if self.config.max_position_notional > 0 and est_price > 0:
            current_pos = positions.get(order.instrument_id)
            current_qty = current_pos.quantity if current_pos is not None else 0.0
            signed_qty = order.quantity if order.side == OrderSide.BUY else -order.quantity
            new_qty = current_qty + signed_qty
            new_notional = abs(new_qty * est_price)

            if new_notional > self.config.max_position_notional:
                max_pos_qty = self.config.max_position_notional / est_price
                if order.side == OrderSide.BUY:
                    allowed_delta = max_pos_qty - abs(current_qty)
                else:
                    # Selling reduces a long toward zero, or builds a short.
                    allowed_delta = max_pos_qty - max(0.0, -current_qty)
                allowed_delta = max(0.0, allowed_delta)
                order = self._clamp_quantity(
                    order,
                    allowed_delta,
                    reason=(
                        f"resulting position notional {new_notional:.2f} exceeds "
                        f"max_position_notional {self.config.max_position_notional:.2f} "
                        f"for {order.instrument_id}"
                    ),
                )
                est_notional = abs(est_price * order.quantity)

        reduces_exposure = self._reduces_exposure(order, positions)

        # Leverage limit (gross exposure / equity). Exposure-REDUCING
        # orders are exempt: a sell that shrinks a long lowers gross, and
        # blocking it would prevent exactly the de-risking the limit is
        # meant to force. (The old code also ADDED sell notional to gross,
        # overstating post-trade leverage.)
        if self.config.max_leverage > 0 and not reduces_exposure:
            equity = self._resolve_equity(account_state)
            if equity > 0:
                gross = self._gross_exposure(positions) + est_notional
                leverage = gross / equity
                if leverage > self.config.max_leverage:
                    reason = (
                        f"leverage {leverage:.3f} would exceed max_leverage "
                        f"{self.config.max_leverage:.3f}"
                    )
                    self._block(order, reason)

        # Drawdown circuit breaker — block new EXPOSURE-INCREASING orders
        # when the book is in excessive drawdown. Exposure-reducing orders
        # must always pass: when the breaker trips, selling down is the
        # one thing the system must still be able to do. Trailing peak is
        # read from ``portfolio_equity_history`` if available; otherwise we
        # fall back to the broker's reported ``high_water_mark`` field.
        if self.config.max_drawdown_pct > 0 and not reduces_exposure:
            equity = self._resolve_equity(account_state)
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
            else:
                # The breaker is configured but has no data to act on —
                # that is a silently-dead safety control. Shout once per
                # day instead of skipping quietly.
                self._warn_breaker_dataless()

        # Sector concentration cap (exposure-reducing orders shrink the
        # sector position and are exempt).
        if self.config.max_sector_concentration_pct > 0 and not reduces_exposure:
            equity = self._resolve_equity(account_state)
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

        return order

    def _clamp_quantity(self, order: Order, max_quantity: float, *, reason: str) -> Order:
        """Return ``order`` resized to ``max_quantity`` when it overshoots.

        If the allowed quantity collapses to ~0 the order cannot be made
        safe by resizing, so we hard-reject it (skip) via :meth:`_block`.
        """
        max_quantity = max(0.0, float(max_quantity))
        if max_quantity <= _MIN_CLAMP_QUANTITY:
            self._block(order, f"{reason}; clamp would zero the order — skipping")
        if order.quantity <= max_quantity:
            return order

        logger.warning(
            "RiskCheckingBroker: clamping order %s %s qty %.6f -> %.6f (%s)",
            order.instrument_id, order.side.value, order.quantity, max_quantity, reason,
        )
        clamped = replace(order, quantity=max_quantity)
        self._record_risk_action(clamped, f"CLAMPED: {reason}")
        return clamped

    def _estimate_price(self, instrument_id: str, positions: Dict[str, Position]) -> float:
        """Best-effort **USD** price estimate for risk checks.

        Uses the current position's implied price when available. Falls
        back to the latest close price from the historical DB. Both are
        quoted in the instrument's local currency (pence for LSE), so the
        result is converted to USD via :class:`FxConverter` — every
        downstream notional/leverage/sector computation therefore stays
        in USD. Raises :class:`FxRateUnavailable` when the instrument is
        non-USD and no usable rate exists.

        As a last resort, returns a high synthetic price (USD 1,000) to
        be conservative — this ensures oversized orders are blocked
        rather than allowed. The fallback is already USD; no conversion
        is applied to it.
        """

        local_price: Optional[float] = None

        pos = positions.get(instrument_id)
        if pos is not None and pos.quantity:
            try:
                price = abs(pos.market_value) / abs(pos.quantity)
                if price > 0:
                    local_price = price
            except Exception:  # pragma: no cover - defensive
                logger.exception("Failed to infer price from position for %s", instrument_id)

        # Try latest close from DB
        if local_price is None:
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
                            local_price = float(row[0])
                    finally:
                        cur.close()
            except Exception:
                logger.exception("RiskCheckingBroker: DB price lookup failed for %s", instrument_id)

        if local_price is not None:
            currency = self._lookup_currency(instrument_id) or "USD"
            if currency == "USD":
                return local_price
            # May raise FxRateUnavailable — the caller blocks the order.
            return self._get_fx().price_to_usd(
                local_price, currency, instrument_id, date.today()
            )

        # Conservative fallback: high USD price means notional checks are STRICT
        logger.warning(
            "RiskCheckingBroker: no price available for %s — using conservative $1000 (USD) fallback",
            instrument_id,
        )
        return 1000.0

    def _get_fx(self) -> FxConverter:
        """Return the injected converter, constructing one lazily if needed."""
        if self._fx is None:
            self._fx = FxConverter(get_db_manager())
        return self._fx

    def _lookup_currency(self, instrument_id: str) -> Optional[str]:
        """Return the instrument's currency, or ``None`` if unknown.

        Mirrors :meth:`_lookup_sector`, but reads the RUNTIME db — that is
        where the ``instruments`` table is actually populated (the
        historical copy is empty). Results are memoised; failures are not,
        so a transient DB error can recover on the next order.
        """
        if instrument_id in self._currency_cache:
            return self._currency_cache[instrument_id]
        try:
            db = get_db_manager()
            with db.get_runtime_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "SELECT currency FROM instruments "
                        "WHERE instrument_id = %s AND currency IS NOT NULL "
                        "LIMIT 1",
                        (instrument_id,),
                    )
                    row = cur.fetchone()
                finally:
                    cur.close()
        except Exception:
            logger.exception(
                "RiskCheckingBroker: currency lookup failed for %s", instrument_id,
            )
            return None
        # Only accept a real string — a mocked/absent row means "unknown",
        # which downstream treats as USD (best-effort, matching the other
        # best-effort lookups in this class).
        currency = row[0].upper() if row and isinstance(row[0], str) else None
        self._currency_cache[instrument_id] = currency
        return currency

    @staticmethod
    def _gross_exposure(positions: Dict[str, Position]) -> float:
        return float(sum(abs(p.market_value) for p in positions.values()))

    @staticmethod
    def _resolve_equity(account_state: Dict) -> float:
        """Account equity from whichever key the broker populated."""
        return float(
            account_state.get("equity")
            or account_state.get("NetLiquidation")
            or 0.0
        )

    @staticmethod
    def _reduces_exposure(order: Order, positions: Dict[str, Position]) -> bool:
        """True when the order shrinks an existing position toward zero.

        A SELL against a long (or a BUY covering a short) reduces gross
        exposure; portfolio-level blocks (leverage, drawdown, sector
        concentration) must never stop the system from de-risking.
        Only the shrinking portion counts: a SELL bigger than the long it
        closes would open a short, so it is NOT treated as reducing.
        """
        pos = positions.get(order.instrument_id)
        current_qty = float(pos.quantity) if pos is not None else 0.0
        if order.side == OrderSide.SELL and current_qty > 0:
            return order.quantity <= current_qty + 1e-9
        if order.side == OrderSide.BUY and current_qty < 0:
            return order.quantity <= -current_qty + 1e-9
        return False

    def _warn_breaker_dataless(self) -> None:
        """Log (once per day) that the drawdown breaker has no data."""
        from datetime import date as _date

        today = _date.today()
        if getattr(self, "_breaker_warned_on", None) == today:
            return
        self._breaker_warned_on = today
        logger.warning(
            "RiskCheckingBroker: drawdown circuit breaker is CONFIGURED but has "
            "NO data (no positive equity/high_water_mark and no rows in "
            "portfolio_equity_history) — the safety control is inactive. "
            "Ensure the daily equity snapshot job is writing portfolio_equity_history."
        )

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
                    sql = (
                        "SELECT MAX(equity) FROM portfolio_equity_history "
                        "WHERE as_of_date >= (CURRENT_DATE - INTERVAL '252 days')"
                    )
                    params: tuple = ()
                    if self.equity_history_portfolio_id:
                        sql += " AND portfolio_id = %s"
                        params = (self.equity_history_portfolio_id,)
                    cur.execute(sql, params)
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

    def _record_risk_action(self, order: Order, reason: str) -> None:
        """Best-effort write into ``risk_actions`` so operators can see why
        an order was rejected or clamped at the execution layer."""
        try:
            db_manager = get_db_manager()
            action = RiskAction(
                strategy_id=self.strategy_id,
                instrument_id=order.instrument_id,
                decision_id=None,
                action_type=RiskActionType.EXECUTION_REJECT,  # generic execution-level action
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
            logger.exception("RiskCheckingBroker: failed to insert risk_actions row")

    def _block(self, order: Order, reason: str) -> NoReturn:
        logger.error("RiskCheckingBroker: blocking order %s: %s", order, reason)
        self._record_risk_action(order, reason)
        raise RiskLimitExceeded(reason)
