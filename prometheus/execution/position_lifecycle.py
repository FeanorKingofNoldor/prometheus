"""Prometheus v2 – Position Lifecycle Manager.

Centralized roll, close, adjust, and assignment-risk logic that works
across ALL option strategies.  Replaces the per-strategy roll/close code
that was previously duplicated in each strategy's ``evaluate()`` method.

Strategies own OPEN logic; the lifecycle manager owns:
- ROLL: any position approaching expiry
- CLOSE: profit targets, stop losses, regime-driven exits
- ADJUST: delta-neutral rebalancing
- ASSIGNMENT_RISK: short options approaching ITM near expiry

Usage::

    from prometheus.execution.position_lifecycle import PositionLifecycleManager

    lifecycle = PositionLifecycleManager()
    directives = lifecycle.evaluate(
        positions=option_positions,
        signals=signals,
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger
from prometheus.execution.broker_interface import OrderType
from prometheus.execution.options_strategy import (
    OptionTradeDirective,
    TradeAction,
)

logger = get_logger(__name__)


# ── Lifecycle config ─────────────────────────────────────────────────

@dataclass
class LifecycleConfig:
    """Configuration for position lifecycle management."""
    # Roll thresholds
    default_roll_dte: int = 14           # Default: roll at 14 DTE
    leaps_roll_dte: int = 90             # LEAPS: roll at 90 DTE

    # Profit targets (by strategy category)
    default_profit_target: float = 0.50  # Close at 50% profit
    directional_profit_target: float = 0.60
    income_profit_target: float = 0.50
    vol_profit_target: float = 1.00       # Vol plays: 100% profit (double)

    # Stop losses
    default_stop_loss: float = 1.00       # Close at 100% loss (debit paid)
    income_stop_loss: float = 2.00        # Income: close at 2x credit
    vol_stop_loss: float = 0.50           # Vol: close at 50% loss

    # Assignment risk
    assignment_risk_dte: int = 7          # Check ITM risk within 7 DTE
    assignment_risk_itm_pct: float = 0.02 # Flag if < 2% OTM

    # Gamma risk
    gamma_risk_dte: int = 7              # Close short options at 7 DTE


# ── Strategy → config mapping ────────────────────────────────────────

# Profit targets per strategy
STRATEGY_PROFIT_TARGETS: Dict[str, float] = {
    "protective_put": 0.0,        # Never profit-take hedges early
    "covered_call": 0.80,
    "sector_put_spread": 0.0,
    "vix_tail_hedge": 0.0,
    "short_put": 0.50,
    "futures_overlay": 0.0,
    "futures_option": 0.0,
    "bull_call_spread": 0.60,
    "leaps": 0.0,                 # Hold LEAPS, don't profit-take
    "iron_condor": 0.50,
    "iron_butterfly": 0.40,
    "collar": 0.0,                # Don't profit-take hedges
    "calendar_spread": 0.50,
    "straddle_strangle": 1.00,
    "wheel": 0.50,
}

# Stop losses per strategy (0.0 = no stop loss)
STRATEGY_STOP_LOSSES: Dict[str, float] = {
    "protective_put": 0.0,
    "covered_call": 0.0,
    "sector_put_spread": 0.0,
    "vix_tail_hedge": 0.0,
    "short_put": 2.00,
    "futures_overlay": 0.0,
    "futures_option": 0.0,
    "bull_call_spread": 1.00,
    "leaps": 0.50,
    "iron_condor": 2.00,
    "iron_butterfly": 2.00,
    "collar": 0.0,
    "calendar_spread": 0.50,
    "straddle_strangle": 0.50,
    "wheel": 2.00,
}

# Roll DTE per strategy
STRATEGY_ROLL_DTE: Dict[str, int] = {
    "protective_put": 14,
    "covered_call": 14,
    "sector_put_spread": 14,
    "vix_tail_hedge": 14,
    "short_put": 14,
    "futures_overlay": 0,   # Futures rolling handled by FuturesManager
    "futures_option": 14,
    "bull_call_spread": 0,  # Close, don't roll
    "leaps": 90,
    "iron_condor": 14,
    "iron_butterfly": 14,
    "collar": 14,
    "calendar_spread": 7,
    "straddle_strangle": 7,
    "wheel": 14,
}


# ── Lifecycle Manager ────────────────────────────────────────────────

class PositionLifecycleManager:
    """Centralized position lifecycle management.

    Parameters
    ----------
    config : LifecycleConfig, optional
        Override default thresholds.
    """

    def __init__(self, config: Optional[LifecycleConfig] = None) -> None:
        self._config = config or LifecycleConfig()

    def evaluate(
        self,
        positions: List[Dict[str, Any]],
        signals: Dict[str, Any],
    ) -> List[OptionTradeDirective]:
        """Run all lifecycle checks and return management directives.

        Parameters
        ----------
        positions : list
            Current option positions (from OptionsPortfolio.get_positions_as_dicts()).
        signals : dict
            Current market signals.

        Returns
        -------
        list[OptionTradeDirective]
            ROLL, CLOSE, and ADJUST directives.
        """
        directives: List[OptionTradeDirective] = []
        as_of = signals.get("as_of_date") or date.today()

        directives.extend(self.check_rolls(positions, as_of))
        directives.extend(self.check_profit_targets(positions))
        directives.extend(self.check_stop_losses(positions))
        directives.extend(self.check_assignment_risk(positions, signals, as_of))
        directives.extend(self.check_gamma_risk(positions, as_of))

        if directives:
            logger.info(
                "Lifecycle: %d management directives (%d ROLL, %d CLOSE)",
                len(directives),
                sum(1 for d in directives if d.action == TradeAction.ROLL),
                sum(1 for d in directives if d.action == TradeAction.CLOSE),
            )

        return directives

    # ── Roll check ───────────────────────────────────────────────────

    def check_rolls(
        self,
        positions: List[Dict[str, Any]],
        as_of: Optional[date] = None,
    ) -> List[OptionTradeDirective]:
        """Emit ROLL directives for positions approaching expiry."""
        directives: List[OptionTradeDirective] = []
        today = as_of or date.today()

        for pos in positions:
            strategy = pos.get("strategy", "")
            roll_dte = STRATEGY_ROLL_DTE.get(strategy, self._config.default_roll_dte)
            if roll_dte <= 0:
                continue  # Strategy doesn't roll (e.g. bull call spread)

            dte = self._compute_dte(pos.get("expiry", ""), today)
            if dte > roll_dte:
                continue

            directives.append(OptionTradeDirective(
                strategy=strategy,
                action=TradeAction.ROLL,
                symbol=pos["symbol"],
                right=pos.get("right", ""),
                expiry=pos.get("expiry", ""),
                strike=pos.get("strike", 0.0),
                quantity=-pos.get("quantity", 0),
                reason=f"Lifecycle roll: {pos['symbol']} {dte} DTE "
                       f"(threshold={roll_dte})",
                metadata={"lifecycle": "roll", "dte": dte},
            ))

        return directives

    # ── Profit target check ──────────────────────────────────────────

    def check_profit_targets(
        self,
        positions: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        """Close positions that hit their profit target."""
        directives: List[OptionTradeDirective] = []

        for pos in positions:
            strategy = pos.get("strategy", "")
            target = STRATEGY_PROFIT_TARGETS.get(
                strategy, self._config.default_profit_target,
            )
            if target <= 0:
                continue  # Strategy doesn't profit-take

            entry_price = pos.get("entry_price", 0)
            current_price = pos.get("current_price", entry_price)
            qty = pos.get("quantity", 0)

            if entry_price <= 0 or current_price <= 0:
                continue

            # For short positions (negative qty): profit = entry - current
            # For long positions (positive qty): profit = current - entry
            if qty < 0:
                profit_pct = (entry_price - current_price) / max(entry_price, 0.01)
            else:
                profit_pct = (current_price - entry_price) / max(entry_price, 0.01)

            if profit_pct >= target:
                directives.append(OptionTradeDirective(
                    strategy=strategy,
                    action=TradeAction.CLOSE,
                    symbol=pos["symbol"],
                    right=pos.get("right", ""),
                    expiry=pos.get("expiry", ""),
                    strike=pos.get("strike", 0.0),
                    quantity=-qty,
                    reason=f"Lifecycle profit target: {pos['symbol']} "
                           f"{profit_pct:.0%} (target={target:.0%})",
                    metadata={"lifecycle": "profit_target", "profit_pct": profit_pct},
                ))

        return directives

    # ── Stop loss check ──────────────────────────────────────────────

    def check_stop_losses(
        self,
        positions: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        """Close positions that exceeded their max loss."""
        directives: List[OptionTradeDirective] = []

        for pos in positions:
            strategy = pos.get("strategy", "")
            max_loss = STRATEGY_STOP_LOSSES.get(
                strategy, self._config.default_stop_loss,
            )
            if max_loss <= 0:
                continue

            entry_price = pos.get("entry_price", 0)
            current_price = pos.get("current_price", entry_price)
            qty = pos.get("quantity", 0)

            if entry_price <= 0 or current_price <= 0:
                continue

            # For short positions: loss = current - entry (price went up)
            # For long positions: loss = entry - current (price went down)
            if qty < 0:
                loss_pct = (current_price - entry_price) / max(entry_price, 0.01)
            else:
                loss_pct = (entry_price - current_price) / max(entry_price, 0.01)

            if loss_pct >= max_loss:
                directives.append(OptionTradeDirective(
                    strategy=strategy,
                    action=TradeAction.CLOSE,
                    symbol=pos["symbol"],
                    right=pos.get("right", ""),
                    expiry=pos.get("expiry", ""),
                    strike=pos.get("strike", 0.0),
                    quantity=-qty,
                    reason=f"Lifecycle stop loss: {pos['symbol']} "
                           f"loss {loss_pct:.0%} (max={max_loss:.0%})",
                    metadata={"lifecycle": "stop_loss", "loss_pct": loss_pct},
                ))

        return directives

    # ── Assignment risk check ────────────────────────────────────────

    def check_assignment_risk(
        self,
        positions: List[Dict[str, Any]],
        signals: Dict[str, Any],
        as_of: Optional[date] = None,
    ) -> List[OptionTradeDirective]:
        """Flag short options approaching ITM near expiry."""
        directives: List[OptionTradeDirective] = []
        today = as_of or date.today()
        equity_prices: Dict[str, float] = signals.get("equity_prices", {})
        spy_price = signals.get("spy_price", 0.0)

        for pos in positions:
            qty = pos.get("quantity", 0)
            if qty >= 0:
                continue  # Only check short positions

            dte = self._compute_dte(pos.get("expiry", ""), today)
            if dte > self._config.assignment_risk_dte:
                continue

            symbol = pos["symbol"]
            strike = pos.get("strike", 0.0)
            right = pos.get("right", "")

            # Get underlying price
            price = equity_prices.get(symbol, 0.0)
            if price <= 0 and symbol == "SPY":
                price = spy_price
            if price <= 0:
                continue

            # Check how close to ITM
            if right == "P":
                # Short put: ITM when price < strike
                otm_pct = (price - strike) / max(price, 1)
            elif right == "C":
                # Short call: ITM when price > strike
                otm_pct = (strike - price) / max(price, 1)
            else:
                continue

            if otm_pct < self._config.assignment_risk_itm_pct:
                directives.append(OptionTradeDirective(
                    strategy=pos.get("strategy", ""),
                    action=TradeAction.ROLL,
                    symbol=symbol,
                    right=right,
                    expiry=pos.get("expiry", ""),
                    strike=strike,
                    quantity=-qty,  # Close the short
                    reason=f"Assignment risk: {symbol} {strike}{right} "
                           f"only {otm_pct:.1%} OTM at {dte} DTE",
                    metadata={
                        "lifecycle": "assignment_risk",
                        "otm_pct": otm_pct,
                        "dte": dte,
                    },
                ))

        return directives

    # ── Gamma risk check ─────────────────────────────────────────────

    def check_gamma_risk(
        self,
        positions: List[Dict[str, Any]],
        as_of: Optional[date] = None,
    ) -> List[OptionTradeDirective]:
        """Close short options approaching expiry (gamma risk)."""
        directives: List[OptionTradeDirective] = []
        today = as_of or date.today()

        for pos in positions:
            qty = pos.get("quantity", 0)
            if qty >= 0:
                continue  # Only short positions have gamma risk

            dte = self._compute_dte(pos.get("expiry", ""), today)
            if dte > self._config.gamma_risk_dte:
                continue

            strategy = pos.get("strategy", "")
            # Skip strategies that already handle their own near-expiry logic
            if strategy in ("futures_overlay", "futures_option"):
                continue

            directives.append(OptionTradeDirective(
                strategy=strategy,
                action=TradeAction.CLOSE,
                symbol=pos["symbol"],
                right=pos.get("right", ""),
                expiry=pos.get("expiry", ""),
                strike=pos.get("strike", 0.0),
                quantity=-qty,
                reason=f"Gamma risk: closing short {pos['symbol']} "
                       f"{pos.get('strike', 0)}{pos.get('right', '')} "
                       f"at {dte} DTE",
                metadata={"lifecycle": "gamma_risk", "dte": dte},
            ))

        return directives

    # ── Delta adjustment ─────────────────────────────────────────────

    def compute_delta_adjustment(
        self,
        portfolio_delta: float,
        target_delta: float,
        spy_price: float,
    ) -> Optional[OptionTradeDirective]:
        """Compute a delta-neutral adjustment using SPY options.

        Returns a directive to buy/sell SPY options to bring portfolio
        delta closer to target.  Returns None if adjustment is too small.
        """
        delta_gap = target_delta - portfolio_delta
        if abs(delta_gap) < 5000:  # Threshold: 5000 share-equivalents
            return None

        # Use SPY options: 1 ATM contract ≈ 50 delta
        n_contracts = int(abs(delta_gap) / 5000)
        if n_contracts < 1:
            return None

        if delta_gap > 0:
            # Need more delta: buy calls
            strike = round(spy_price, 0)
            return OptionTradeDirective(
                strategy="lifecycle_delta_adjust",
                action=TradeAction.HEDGE,
                symbol="SPY",
                right="C",
                expiry="",  # Will be filled by discovery
                strike=strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Delta adjust: +{n_contracts} SPY calls "
                       f"(gap={delta_gap:.0f})",
                metadata={"lifecycle": "delta_adjust", "delta_gap": delta_gap},
            )
        else:
            # Too much delta: buy puts
            strike = round(spy_price, 0)
            return OptionTradeDirective(
                strategy="lifecycle_delta_adjust",
                action=TradeAction.HEDGE,
                symbol="SPY",
                right="P",
                expiry="",
                strike=strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Delta adjust: +{n_contracts} SPY puts "
                       f"(gap={delta_gap:.0f})",
                metadata={"lifecycle": "delta_adjust", "delta_gap": delta_gap},
            )

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _compute_dte(expiry: str, today: date) -> int:
        """Compute days to expiry."""
        try:
            exp_date = datetime.strptime(expiry[:8], "%Y%m%d").date()
            return (exp_date - today).days
        except (ValueError, IndexError):
            return 999  # Unknown expiry → don't trigger


__all__ = [
    "LifecycleConfig",
    "PositionLifecycleManager",
    "STRATEGY_PROFIT_TARGETS",
    "STRATEGY_STOP_LOSSES",
    "STRATEGY_ROLL_DTE",
]
