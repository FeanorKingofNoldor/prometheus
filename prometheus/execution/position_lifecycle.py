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

from apatheon.core.logging import get_logger

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

    # Default profit target / stop loss — applied to any strategy not
    # explicitly listed in STRATEGY_PROFIT_TARGETS / STRATEGY_STOP_LOSSES.
    # Per-strategy entries in those tables override these defaults.
    default_profit_target: float = 0.50  # Close at 50% profit
    default_stop_loss: float = 1.00       # Close at 100% loss (debit paid)

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
    "iron_condor": 0.50,
    "iron_butterfly": 0.50,  # matches IronButterflyConfig.profit_target (v36)
    "crisis_alpha": 2.50,         # matches CrisisAlphaStrategyConfig.profit_target — multi-bagger by design
    # COMMODITY sleeve templates — match TemplateConfig.profit_target_pct
    "commodity.crude_chokepoint_call": 1.50,
    "commodity.natgas_supply_call": 1.50,
    "commodity.gold_sanctions_call": 2.00,
    "commodity.wheat_blacksea_call": 1.50,
}

# Stop losses per strategy (0.0 = no stop loss)
STRATEGY_STOP_LOSSES: Dict[str, float] = {
    "protective_put": 0.0,
    "covered_call": 0.0,
    "sector_put_spread": 0.0,
    "vix_tail_hedge": 0.0,
    "iron_condor": 2.00,
    "iron_butterfly": 2.00,
    "crisis_alpha": 1.00,         # full debit on the put — let it run via min_hold_days
    # COMMODITY sleeve templates — match TemplateConfig.stop_loss_multiplier
    "commodity.crude_chokepoint_call": 1.00,
    "commodity.natgas_supply_call": 1.00,
    "commodity.gold_sanctions_call": 1.00,
    "commodity.wheat_blacksea_call": 1.00,
}

# Roll DTE per strategy
STRATEGY_ROLL_DTE: Dict[str, int] = {
    "protective_put": 14,
    "covered_call": 14,
    "sector_put_spread": 14,
    "vix_tail_hedge": 14,
    "iron_condor": 14,
    "iron_butterfly": 14,
    "crisis_alpha": 0,      # Close, don't roll — driven by signal + cooldown_days
    # COMMODITY sleeve templates — TemplateConfig.close_at_dte semantics.
    # legacy lifecycle emits ROLL at this DTE; for FOP we want a CLOSE at
    # the same DTE. Phase 4.5 work: extend lifecycle to consult sleeve
    # close_at_dte for sleeve-tagged positions. For now, roll-at-DTE is
    # a no-op for FOPs the broker can't roll anyway, so positions just
    # ride until expiry. Acceptable for v1.
    "commodity.crude_chokepoint_call": 14,
    "commodity.natgas_supply_call": 14,
    "commodity.gold_sanctions_call": 21,
    "commodity.wheat_blacksea_call": 14,
}


# Trailing-stop give-back fraction per strategy (0.0 / missing = no
# trailing stop). When current unrealized gain has retraced more than
# this fraction of the peak gain seen so far, emit CLOSE.
STRATEGY_TRAILING_STOPS: Dict[str, float] = {
    # CONVEX sleeve — let winners run but stop the give-back
    "convex.thematic_sector_put": 0.30,
    "convex.vix_escalation_call": 0.30,
    "convex.convergence_straddle": 0.30,
    # COMMODITY sleeve — same intent on the directional commodity bets
    "commodity.crude_chokepoint_call": 0.30,
    "commodity.natgas_supply_call": 0.30,
    "commodity.gold_sanctions_call": 0.30,
    "commodity.wheat_blacksea_call": 0.30,
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
        # Per-position peak unrealized PnL%, keyed by stable position
        # tuple. In-memory only — resets on daemon restart. Re-seeds
        # conservatively from current PnL% on first observation after
        # restart (peak may understate true historical max, which means
        # trailing fires slightly later, never earlier).
        self._peak_gains: Dict[tuple, float] = {}

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
        directives.extend(self.check_trailing_stops(positions))
        directives.extend(self.check_assignment_risk(positions, signals, as_of))
        directives.extend(self.check_gamma_risk(positions, as_of))
        directives.extend(self.check_barrier_stops(positions, signals))

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

        from prometheus.execution.futures_option_specs import (
            is_commodity_fop_symbol,
        )

        for pos in positions:
            strategy = pos.get("strategy", "")
            roll_dte = STRATEGY_ROLL_DTE.get(strategy, self._config.default_roll_dte)
            if roll_dte <= 0:
                continue  # Strategy doesn't roll (e.g. bull call spread)

            dte = self._compute_dte(pos.get("expiry", ""), today)
            if dte > roll_dte:
                continue

            # Commodity FOPs aren't rolled by our broker — emit CLOSE
            # instead so the position exits at the template's close_at_dte.
            # Detected via strategy name prefix OR symbol being a
            # registered commodity FOP underlying.
            is_fop = (
                strategy.startswith("commodity.")
                or is_commodity_fop_symbol(pos.get("symbol", ""))
            )
            action = TradeAction.CLOSE if is_fop else TradeAction.ROLL
            verb = "close" if is_fop else "roll"

            directives.append(OptionTradeDirective(
                strategy=strategy,
                action=action,
                symbol=pos["symbol"],
                right=pos.get("right", ""),
                expiry=pos.get("expiry", ""),
                strike=pos.get("strike", 0.0),
                quantity=-pos.get("quantity", 0),
                reason=f"Lifecycle {verb}: {pos['symbol']} {dte} DTE "
                       f"(threshold={roll_dte})",
                metadata={"lifecycle": verb, "dte": dte},
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

    # ── Trailing stop check ─────────────────────────────────────────

    @staticmethod
    def _position_key(pos: Dict[str, Any]) -> tuple:
        """Stable identity for peak tracking. Strategy + contract.

        Includes a sign-of-quantity tag so a long and short on the same
        contract get separate peaks (rare but possible during rolls).
        """
        qty = pos.get("quantity", 0) or 0
        return (
            pos.get("strategy", ""),
            pos.get("symbol", ""),
            pos.get("expiry", ""),
            pos.get("strike", 0.0),
            pos.get("right", ""),
            1 if qty > 0 else -1,
        )

    def check_trailing_stops(
        self,
        positions: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        """Close positions that gave back too much of their peak gain.

        Tracks peak unrealized PnL% per position in an instance-scoped
        dict. A position with no entry in ``STRATEGY_TRAILING_STOPS`` is
        skipped — the existing profit_target / stop_loss machinery owns
        those exits.
        """
        directives: List[OptionTradeDirective] = []

        for pos in positions:
            strategy = pos.get("strategy", "")
            trailing_pct = STRATEGY_TRAILING_STOPS.get(strategy, 0.0)
            if trailing_pct <= 0:
                continue

            entry_price = pos.get("entry_price", 0) or 0
            current_price = pos.get("current_price", entry_price) or entry_price
            qty = pos.get("quantity", 0) or 0
            if entry_price <= 0 or current_price <= 0 or qty == 0:
                continue

            if qty > 0:
                pnl_pct = (current_price - entry_price) / entry_price
            else:
                pnl_pct = (entry_price - current_price) / entry_price

            key = self._position_key(pos)
            peak = self._peak_gains.get(key, pnl_pct)
            if pnl_pct > peak:
                peak = pnl_pct
            self._peak_gains[key] = peak

            # Only fire when we had a real gain to trail and have now
            # given back more than trailing_pct of that peak.
            if peak <= 0:
                continue
            give_back_threshold = peak * (1.0 - trailing_pct)
            if pnl_pct >= give_back_threshold:
                continue

            directives.append(OptionTradeDirective(
                strategy=strategy,
                action=TradeAction.CLOSE,
                symbol=pos["symbol"],
                right=pos.get("right", ""),
                expiry=pos.get("expiry", ""),
                strike=pos.get("strike", 0.0),
                quantity=-qty,
                reason=(
                    f"Lifecycle trailing stop: {pos['symbol']} "
                    f"peak={peak:.0%} current={pnl_pct:.0%} "
                    f"(give back ≥ {trailing_pct:.0%})"
                ),
                metadata={
                    "lifecycle": "trailing_stop",
                    "peak_pnl_pct": peak,
                    "current_pnl_pct": pnl_pct,
                    "trailing_pct": trailing_pct,
                },
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

    # ── Wing-barrier stop ────────────────────────────────────────────

    def check_barrier_stops(
        self,
        positions: List[Dict[str, Any]],
        signals: Dict[str, Any],
    ) -> List[OptionTradeDirective]:
        """Close iron-butterfly / iron-condor short legs when the underlying
        reaches the long-wing strike.

        The ATM short legs of a butterfly (or the OTM short legs of a condor)
        are delta-hedged by the long wings, but once the underlying blows
        through the wing the spread has no more protection — max loss is
        effectively locked in.  Closing at the barrier avoids the assignment
        risk and further P&L deterioration.

        ``wing_strike`` must be stored in ``pos['metadata']`` when the
        position is opened (done by IronButterflyStrategy and
        IronCondorStrategy after the Bug-1 fix).
        """
        directives: List[OptionTradeDirective] = []

        spy_price  = signals.get("spy_price", 0.0)
        eq_prices  = signals.get("equity_prices") or {}

        for pos in positions:
            strategy = pos.get("strategy", "")
            if strategy not in ("iron_butterfly", "iron_condor"):
                continue

            qty = pos.get("quantity", 0)
            if qty >= 0:
                continue  # Only short legs carry the risk

            wing_strike = pos.get("metadata", {}).get("wing_strike", 0.0)
            if not wing_strike:
                continue

            right   = pos.get("right", "")
            symbol  = pos.get("symbol", "")
            underlying = spy_price if symbol == "SPY" else eq_prices.get(symbol, 0.0)
            if underlying <= 0:
                continue

            # Put wing: price breaks below the long-put strike
            # Call wing: price breaks above the long-call strike
            breached = (
                (right == "P" and underlying <= wing_strike)
                or (right == "C" and underlying >= wing_strike)
            )
            if not breached:
                continue

            directives.append(OptionTradeDirective(
                strategy=strategy,
                action=TradeAction.CLOSE,
                symbol=symbol,
                right=right,
                expiry=pos.get("expiry", ""),
                strike=pos.get("strike", 0.0),
                quantity=-qty,  # Buy-to-close
                order_type=OrderType.LIMIT,
                reason=(
                    f"Wing barrier breached: {symbol} {right} {pos.get('strike', 0):.1f} "
                    f"– underlying {underlying:.2f} through wing {wing_strike:.1f}"
                ),
                metadata={
                    "lifecycle": "barrier_stop",
                    "wing_strike": wing_strike,
                    "underlying": underlying,
                },
            ))
            logger.info(
                "Barrier stop: closing %s %s %s%.1f "
                "(underlying=%.2f, wing=%.1f)",
                strategy, symbol, right, pos.get("strike", 0),
                underlying, wing_strike,
            )

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
    "STRATEGY_TRAILING_STOPS",
]
