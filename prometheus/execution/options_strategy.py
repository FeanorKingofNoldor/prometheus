"""Prometheus v2 – Options & Derivatives Strategy Layer.

Translates high-level allocator directives into concrete option, futures,
and futures-option trades submitted via the broker.

Strategies
----------
1. **Protective Puts** – Buy SPY puts when MHI drops below threshold.
2. **Covered Calls** – Sell calls on largest equity positions in
   RISK_ON/NEUTRAL states.
3. **Sector Put Spreads** – Buy put spreads on sector ETFs when sector
   SHI is in the "reduce" zone (below reduce, above kill).
4. **VIX Tail Hedge** – Always-on OTM VIX calls as catastrophe insurance.
5. **Short Puts** – Sell cash-secured puts on high-conviction equity signals.
6. **Futures Overlay** – ES/NQ futures for portfolio-level beta management.
7. **Futures Options** – Defined-risk plays on VX and ES via FOP.

Each strategy is a self-contained class implementing :class:`OptionStrategy`.
The :class:`OptionsStrategyManager` orchestrates them.

Usage
-----
    from prometheus.execution.options_strategy import OptionsStrategyManager

    mgr = OptionsStrategyManager(broker, mapper, discovery=discovery)
    mgr.evaluate_all(portfolio, signals)
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger

from prometheus.execution.broker_interface import (
    BrokerInterface,
    Order,
    OrderSide,
    OrderType,
    Position,
)
from prometheus.execution.instrument_mapper import InstrumentMapper

logger = get_logger(__name__)


# ── Inline Black-Scholes pricer (no external deps) ──────────────────
# Used internally by IronButterflyStrategy and IronCondorStrategy to
# estimate the true net credit at sizing time so that max_loss per
# contract reflects (wing_width - net_credit) rather than wing_width.

def _bs_price(
    S: float, K: float, T: float, r: float, sigma: float, right: str,
) -> float:
    """Minimal Black-Scholes option price using math.erf (no scipy).

    Returns intrinsic value for degenerate inputs (T≤0, sigma≤0, etc.).
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return max((S - K if right.upper() == "C" else K - S), 0.0)
    sqrt2 = math.sqrt(2.0)
    sqrtT = math.sqrt(T)

    def _n(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / sqrt2))

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrtT)
    d2 = d1 - sigma * sqrtT
    disc = math.exp(-r * T)
    if right.upper() == "C":
        return S * _n(d1) - K * disc * _n(d2)
    else:
        return K * disc * _n(-d2) - S * _n(-d1)


# ── Configuration dataclasses ────────────────────────────────────────

@dataclass
class ProtectivePutConfig:
    """Configuration for protective put strategy."""
    enabled: bool = True
    mhi_threshold: float = 0.4         # MHI below this triggers puts
    otm_pct: float = 0.05             # 5% OTM
    target_dte_min: int = 45           # Minimum days to expiration
    target_dte_max: int = 90           # Maximum days to expiration
    nav_pct: float = 0.03             # Spend up to 3% of NAV on premium
    roll_dte: int = 14                 # Roll when DTE drops below this
    underlying: str = "SPY"


@dataclass
class CoveredCallConfig:
    """Configuration for covered call strategy."""
    enabled: bool = True
    target_delta: float = 0.20         # Sell ~0.20 delta calls (further OTM)
    target_dte_min: int = 30
    target_dte_max: int = 45
    coverage_ratio: float = 0.20       # Cover up to 20% of position (tightened from 30%)
    min_position_days: int = 5         # Only on positions held > 5 days
    profit_target: float = 0.80        # Buy back at 80% profit
    roll_dte: int = 14
    min_position_shares: int = 100     # Must hold at least 100 shares
    min_vix_for_entry: float = 22.0    # Only sell calls when VIX >= 22 (high-premium only)


@dataclass
class SectorPutSpreadConfig:
    """Configuration for sector put spread strategy."""
    enabled: bool = True
    shi_reduce_threshold: float = 0.25  # SHI below this triggers spread (tightened from 0.35)
    shi_kill_threshold: float = 0.15    # Below this, don't hedge — liquidate
    spread_width_pct: float = 0.07      # 7% between long and short strikes (reduced from 10%)
    target_dte_min: int = 30
    target_dte_max: int = 60
    max_nav_pct: float = 0.01           # Cap at 1% of NAV per sector hedge


@dataclass
class VixTailHedgeConfig:
    """Configuration for VIX tail hedge strategy."""
    enabled: bool = True
    nav_pct: float = 0.03             # 3% of NAV (doubled from 1.5% for v9)
    strike_premium_pct: float = 0.50   # Strike = VIX + 50%
    target_dte_min: int = 45
    target_dte_max: int = 90
    roll_dte: int = 14
    # VIX options trade on VIX index, settled in cash
    underlying: str = "VIX"
    exchange: str = "CBOE"


@dataclass
class ShortPutConfig:
    """Configuration for short (cash-secured) put strategy."""
    # Disabled: IV engine uses VIX as base vol for single stocks, understating
    # individual IV — short_put is consistently -EV in backtest.  Valid for live
    # trading with real IV feeds but not enabled in the model-driven pipeline.
    enabled: bool = False
    target_delta: float = 0.25        # Sell 0.20–0.30 delta puts
    target_dte_min: int = 30
    target_dte_max: int = 45
    max_buying_power_pct: float = 0.05  # Max 5% of buying power per underlying
    profit_target: float = 0.50       # Buy back at 50% profit
    roll_dte: int = 14
    min_lambda_score: float = 0.60    # Min λ score (real DB scores [-1,1] post-2015)
    min_stab_score: float = 0.50      # Must be stable
    max_positions: int = 10           # Max concurrent short put positions
    min_vix: float = 15.0             # Only sell when IV is meaningful
    max_vix: float = 35.0             # Raised from 30 — sell into mildly elevated vol
    max_loss_stop: float = 3.0        # Close at 3x credit received


@dataclass
class FuturesOverlayConfig:
    """Configuration for ES/NQ futures overlay strategy."""
    enabled: bool = True
    # FRAG-based hedging
    frag_hedge_threshold: float = 0.65   # Short ES when FRAG above this
    frag_max_hedge_ratio: float = 0.30   # Max 30% of portfolio notional hedged
    # Lambda-based leverage
    lambda_leverage_threshold: float = 0.70  # Add ES longs when lambda > this
    lambda_max_leverage_pct: float = 0.15    # Max 15% additional ES exposure
    # Product
    product: str = "ES"                # Default to E-mini S&P 500
    exchange: str = "CME"
    target_beta: float = 1.0           # Target portfolio beta


@dataclass
class FuturesOptionConfig:
    """Configuration for futures option (FOP) strategy."""
    enabled: bool = True
    # VX call spreads — expect vol expansion when FRAG is low
    vx_frag_low_threshold: float = 0.30  # FRAG below this → buy VX call spread
    vx_spread_width: float = 5.0         # $5 wide VX call spread
    vx_nav_pct: float = 0.005            # 0.5% of NAV budget
    vx_target_dte_min: int = 30
    vx_target_dte_max: int = 60
    # ES put spreads — cheaper downside protection than SPY puts
    es_put_spread_enabled: bool = True
    es_spread_width_pct: float = 0.05    # 5% wide
    es_target_dte_min: int = 30
    es_target_dte_max: int = 60
    es_mhi_threshold: float = 0.45       # Buy ES put spread when MHI below this
    es_nav_pct: float = 0.01             # 1% of NAV budget


@dataclass
class BullCallSpreadConfig:
    """Configuration for bull call spread strategy."""
    enabled: bool = True
    min_lambda_score: float = 0.65    # Min momentum proxy score (see momentum_scores in signals)
    min_stab_score: float = 0.50
    spread_width_pct: float = 0.07    # 7% between long and short strikes
    target_dte_min: int = 30
    target_dte_max: int = 60
    max_risk_per_trade_pct: float = 0.04   # 4% NAV risk per trade (raised from 3%)
    max_positions: int = 12           # Raised from 9 — more slots for momentum names
    profit_target: float = 0.60       # Close at 60% of max profit
    long_delta: float = 0.55          # Slightly ITM long leg
    short_delta: float = 0.30         # OTM short leg


@dataclass
class MomentumCallConfig:
    """Configuration for momentum call overlay strategy.

    Buys SPY ATM call spreads during RISK_ON when 63-day momentum is
    positive.  Designed to capture bull-market upside that the
    vol-harvesting strategies miss.
    """
    enabled: bool = True
    underlying: str = "SPY"
    max_vix: float = 22.0              # Raised from 20 — allow mildly elevated vol
    min_momentum_63d: float = 0.01     # Lowered from 0.02 — SPY up ≥ 1% over 63 days
    spread_width_pct: float = 0.05     # 5% wide call spread
    target_dte_min: int = 30
    target_dte_max: int = 60
    nav_pct: float = 0.03              # Kept at 0.03 — larger size degrades Sharpe (directional debit, no calm-market gate)
    max_positions: int = 3             # Max concurrent positions
    profit_target: float = 0.70        # Close at 70% of max profit
    roll_dte: int = 14                 # Close/roll when DTE < 14
    long_delta: float = 0.55           # Slightly ITM long leg


@dataclass
class LEAPSConfig:
    """Configuration for LEAPS (stock replacement) strategy."""
    enabled: bool = True
    min_lambda_score: float = 0.70
    min_stab_score: float = 0.60
    target_delta: float = 0.75        # Deep ITM
    target_dte_min: int = 180         # 6 months minimum
    target_dte_max: int = 365         # Up to 1 year
    roll_dte: int = 90                # Roll when DTE < 90
    max_replacement_pct: float = 0.30 # Replace up to 30% of position
    min_position_value: float = 50_000  # Only on large positions
    max_positions: int = 5


@dataclass
class IronCondorConfig:
    """Configuration for iron condor strategy."""
    enabled: bool = True
    underlying: str = "SPY"           # Default underlying
    min_vix: float = 14.0             # Only when VIX > 14 (meaningful premium)
    max_vix: float = 18.0             # Reverted: condor at VIX 18-20 underperforms vs butterfly
    max_frag: float = 0.30            # Only when FRAG < 0.30
    put_delta: float = 0.18           # Short put delta
    call_delta: float = 0.18          # Short call delta
    wing_width: float = 5.0           # $5 wide wings
    target_dte_min: int = 30
    target_dte_max: int = 45
    nav_pct: float = 0.04             # 4% of NAV risk budget
    profit_target: float = 0.50       # Close at 50% profit
    max_loss_multiple: float = 2.0    # Close at 2x credit received
    max_positions: int = 5            # Raised from 4


@dataclass
class IronButterflyConfig:
    """Configuration for iron butterfly strategy."""
    enabled: bool = True
    underlying: str = "SPY"
    max_vix: float = 20.0             # Raised from 18 — more entry windows for #1 strategy
    max_frag: float = 0.20            # Keep tight: frag > 0.20 = fragile market, avoid short premium
    wing_width: float = 10.0          # $10 wide wings
    target_dte_min: int = 30
    target_dte_max: int = 45
    nav_pct: float = 0.20             # v34 test
    profit_target: float = 0.50       # Hold winners longer (was 0.40)
    max_loss_multiple: float = 2.0    # Close short legs at 2x credit received
    max_positions: int = 6            # Raised from 5


@dataclass
class CollarConfig:
    """Configuration for collar strategy."""
    enabled: bool = True
    put_delta: float = 0.25           # Further OTM put (cheaper protection)
    call_delta: float = 0.25          # Further OTM call (less cap on upside)
    target_dte_min: int = 45
    target_dte_max: int = 90
    roll_dte: int = 14
    min_position_shares: int = 100
    min_position_value: float = 25_000
    max_positions: int = 5


@dataclass
class CalendarSpreadConfig:
    """Configuration for calendar spread strategy."""
    enabled: bool = True
    underlying: str = "SPY"
    front_dte_min: int = 25
    front_dte_max: int = 35
    back_dte_min: int = 55
    back_dte_max: int = 90
    nav_pct: float = 0.01             # 1% of NAV
    profit_target: float = 0.50
    max_loss_pct: float = 0.50        # Close at 50% loss of debit paid
    max_positions: int = 2
    min_vix_contango: float = 0.08    # Min 8% term structure slope (tightened from 5%)


@dataclass
class StraddleStrangleConfig:
    """Configuration for straddle/strangle strategy."""
    enabled: bool = True
    max_entry_vix: float = 18.0       # Only buy vol when it's cheap
    strangle_otm_pct: float = 0.05    # 5% OTM for strangle legs
    target_dte_min: int = 14
    target_dte_max: int = 30
    nav_pct: float = 0.01             # 1% of NAV
    profit_target: float = 1.00       # Close at 100% profit (double)
    max_loss_pct: float = 0.50        # Close at 50% loss
    max_positions: int = 2
    prefer_strangle: bool = True      # Default to strangle (cheaper)


@dataclass
class WheelConfig:
    """Configuration for wheel strategy (CSP → assignment → CC cycle)."""
    enabled: bool = True
    min_lambda_score: float = 0.60
    min_stab_score: float = 0.55
    csp_target_delta: float = 0.28    # CSP: ~0.25-0.30 delta
    cc_target_delta: float = 0.30     # CC: ~0.30 delta
    target_dte_min: int = 30
    target_dte_max: int = 45
    profit_target: float = 0.50       # Close at 50% profit
    roll_dte: int = 14
    max_nav_pct_per_position: float = 0.06  # 6% NAV per wheel position (doubled for v9)
    max_positions: int = 5


# ── Trade directive (output of strategies) ───────────────────────────

class TradeAction(str, Enum):
    """What the strategy wants to do."""
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    ROLL = "ROLL"
    HOLD = "HOLD"
    HEDGE = "HEDGE"


@dataclass
class OptionTradeDirective:
    """A concrete option trade recommendation from a strategy.

    The OptionsStrategyManager converts these into Orders.
    """
    strategy: str              # e.g. "protective_put", "covered_call"
    action: TradeAction
    symbol: str                # Underlying symbol
    right: str                 # "C" or "P"
    expiry: str                # YYYYMMDD
    strike: float
    quantity: int              # Positive = buy, negative = sell
    order_type: OrderType = OrderType.LIMIT
    limit_price: Optional[float] = None
    reason: str = ""
    # For spreads: the other leg
    spread_leg: Optional["OptionTradeDirective"] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# ── Abstract strategy interface ──────────────────────────────────────

class OptionStrategy(ABC):
    """Base class for option strategies."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Strategy identifier."""

    @abstractmethod
    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        """Evaluate strategy and return trade directives.

        Parameters
        ----------
        portfolio : dict
            Current equity positions keyed by instrument_id.
        signals : dict
            Current market signals.  Expected keys vary by strategy:
            - "mhi": market health index ∈ [0, 1]
            - "nav": portfolio net asset value
            - "sector_shi": dict of sector → SHI score
            - "vix_level": current VIX
            - "sector_exposures": dict of sector → notional exposure
        existing_options : list
            Currently open option positions (from OptionsPortfolio).

        Returns
        -------
        list[OptionTradeDirective]
        """


# ── Protective Puts ──────────────────────────────────────────────────

class ProtectivePutStrategy(OptionStrategy):
    """Buy SPY puts when MHI drops below threshold."""

    def __init__(self, config: Optional[ProtectivePutConfig] = None) -> None:
        self._config = config or ProtectivePutConfig()

    @property
    def name(self) -> str:
        return "protective_put"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        mhi = signals.get("mhi", 1.0)
        nav = signals.get("nav", 0.0)
        directives: List[OptionTradeDirective] = []

        # Check existing protective puts
        existing_puts = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        if mhi >= self._config.mhi_threshold:
            # MHI healthy — close any existing protective puts
            for opt in existing_puts:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right="P",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],  # Sell to close
                    reason=f"MHI recovered to {mhi:.2f}, closing protection",
                ))
            return directives

        # MHI below threshold — need protection
        # Check if we need to roll existing puts
        today = signals.get("as_of_date", date.today())
        for opt in existing_puts:
            dte = self._days_to_expiry(opt["expiry"], today)
            if dte <= self._config.roll_dte:
                # Roll: close old, open new
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=opt["symbol"],
                    right="P",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling: {dte} DTE remaining",
                ))
            else:
                # Existing put is fine, hold
                return []

        # Open new protective put if no active position or rolling
        if nav <= 0:
            return directives

        spy_price = signals.get("spy_price", 500.0)
        strike = round(spy_price * (1 - self._config.otm_pct), 0)

        # Target expiry
        target_expiry = self._find_target_expiry(
            today,
            self._config.target_dte_min,
            self._config.target_dte_max,
        )

        # Size: NAV * pct / (strike * 100)
        max_premium = nav * self._config.nav_pct
        # Rough estimate: each contract controls 100 shares
        notional_per_contract = strike * 100
        # Premium is ~2-5% of notional for ATM puts, scale for OTM
        estimated_premium_per_contract = notional_per_contract * 0.02
        n_contracts = max(1, int(max_premium / max(estimated_premium_per_contract, 1)))

        directives.append(OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=self._config.underlying,
            right="P",
            expiry=target_expiry,
            strike=strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"MHI={mhi:.2f} < {self._config.mhi_threshold}, "
                   f"buying {n_contracts} puts @ {strike}",
            metadata={"mhi": mhi, "nav_pct": self._config.nav_pct},
        ))

        return directives

    @staticmethod
    def _days_to_expiry(expiry: str, today: date) -> int:
        exp_date = datetime.strptime(expiry[:8], "%Y%m%d").date()
        return (exp_date - today).days

    @staticmethod
    def _find_target_expiry(today: date, min_dte: int, max_dte: int) -> str:
        """Return YYYYMMDD string for a monthly expiry in the DTE range.

        Uses third Friday of the month as standard options expiry.
        """
        target_date = today + timedelta(days=(min_dte + max_dte) // 2)
        # Find third Friday of target month
        first_day = target_date.replace(day=1)
        # weekday(): 0=Mon, 4=Fri
        days_to_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_to_friday)
        third_friday = first_friday + timedelta(weeks=2)
        return third_friday.strftime("%Y%m%d")


# ── Covered Calls ────────────────────────────────────────────────────

class CoveredCallStrategy(OptionStrategy):
    """Sell calls on large equity positions."""

    def __init__(self, config: Optional[CoveredCallConfig] = None) -> None:
        self._config = config or CoveredCallConfig()

    @property
    def name(self) -> str:
        return "covered_call"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        market_state = signals.get("market_state", "NEUTRAL")
        if market_state not in ("RISK_ON", "NEUTRAL"):
            return []

        # Only sell new calls when VIX is elevated (more premium to collect)
        vix = signals.get("vix_level", 15.0)

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Check for profit-taking and rolling on existing calls
        existing_calls = {
            opt["symbol"]: opt for opt in existing_options
            if opt.get("strategy") == self.name
        }

        for symbol, opt in existing_calls.items():
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            # Profit target: buy back
            if entry_price > 0 and current_price > 0:
                profit_pct = (entry_price - current_price) / entry_price
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=symbol,
                        right="C",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],  # Buy to close (was short)
                        reason=f"Profit target reached: {profit_pct:.0%}",
                    ))
                    continue

            # Roll at low DTE
            if dte <= self._config.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=symbol,
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling: {dte} DTE remaining",
                ))

        # Find new covered call candidates — only when VIX provides enough premium
        if vix < self._config.min_vix_for_entry:
            return directives

        covered_symbols = set(existing_calls.keys())

        for iid, pos in portfolio.items():
            if pos.quantity < self._config.min_position_shares:
                continue

            # Extract symbol from instrument_id
            symbol = iid.split(".")[0] if "." in iid else iid

            if symbol in covered_symbols:
                continue

            # Determine how many contracts to sell
            coverable = int(pos.quantity * self._config.coverage_ratio)
            n_contracts = coverable // 100
            if n_contracts < 1:
                continue

            # Strike: approximate using delta target
            # In practice, we'd query the option chain and pick by delta.
            # For now, use a heuristic: 0.30 delta ≈ 1 std dev out
            current_price = pos.market_value / max(pos.quantity, 1)
            if current_price <= 0:
                continue

            # ~0.20 delta ≈ ~8-12% OTM for 30-45 DTE
            # Round to nearest whole dollar — fractional strikes (e.g. 29.9) are
            # not listed and cause Error 200 from IBKR qualifyContracts.
            strike = float(round(current_price * 1.10))

            target_expiry = ProtectivePutStrategy._find_target_expiry(
                today,
                self._config.target_dte_min,
                self._config.target_dte_max,
            )

            # Limit price: sell at or above 90% of BS mid so the order fills
            # promptly while avoiding giving away premium at market.
            _T_cc = (self._config.target_dte_min + self._config.target_dte_max) / 2 / 365.0
            _cc_prem = _bs_price(current_price, strike, _T_cc, 0.04,
                                 max(vix, 10.0) / 100.0, "C")
            cc_limit = max(round(_cc_prem * 0.9, 2), 0.01)

            directives.append(OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="C",
                expiry=target_expiry,
                strike=strike,
                quantity=-n_contracts,  # Negative = sell
                order_type=OrderType.LIMIT,
                limit_price=cc_limit,
                reason=f"Covered call on {symbol}: {n_contracts} contracts "
                       f"@ {strike} (lmt={cc_limit:.2f}, {pos.quantity:.0f} shares held)",
                metadata={"position_qty": pos.quantity, "coverage": self._config.coverage_ratio},
            ))

        return directives


# ── Sector Put Spreads ───────────────────────────────────────────────

class SectorPutSpreadStrategy(OptionStrategy):
    """Buy put spreads on sector ETFs with deteriorating health."""

    def __init__(self, config: Optional[SectorPutSpreadConfig] = None) -> None:
        self._config = config or SectorPutSpreadConfig()

    @property
    def name(self) -> str:
        return "sector_put_spread"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        sector_shi: Dict[str, float] = signals.get("sector_shi", {})
        sector_exposures: Dict[str, float] = signals.get("sector_exposures", {})
        nav = signals.get("nav", 0.0)

        # Map sector → ETF symbol
        from apathis.sector.health import SECTOR_NAME_TO_ETF

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        existing_sectors = {
            opt.get("sector"): opt for opt in existing_options
            if opt.get("strategy") == self.name
        }

        for sector_name, shi in sector_shi.items():
            etf_id = SECTOR_NAME_TO_ETF.get(sector_name)
            if not etf_id:
                continue
            etf_symbol = etf_id.replace(".US", "")

            # In the "reduce" zone: between kill and reduce thresholds
            if shi >= self._config.shi_reduce_threshold:
                # Sector healthy — close any existing hedges
                if sector_name in existing_sectors:
                    opt = existing_sectors[sector_name]
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=etf_symbol,
                        right="P",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"{sector_name} SHI recovered to {shi:.2f}",
                    ))
                continue

            if shi < self._config.shi_kill_threshold:
                # Below kill — allocator handles liquidation, not us
                continue

            if sector_name in existing_sectors:
                # Already hedged — check for roll
                opt = existing_sectors[sector_name]
                dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
                if dte > self._config.target_dte_min // 2:
                    continue  # Hold
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=etf_symbol,
                    right="P",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling {sector_name} hedge: {dte} DTE",
                ))

            # Open new spread
            sector_exposure = sector_exposures.get(sector_name, 0.0)
            if sector_exposure <= 0:
                continue

            # Get ETF price from signals
            etf_prices = signals.get("etf_prices", {})
            etf_price = etf_prices.get(etf_symbol, 0.0)
            if etf_price <= 0:
                continue

            # ATM long put
            long_strike = round(etf_price, 0)
            # Short put: spread_width_pct below
            short_strike = round(etf_price * (1 - self._config.spread_width_pct), 0)

            # Size: hedge the full sector exposure, capped by max_nav_pct
            n_contracts = max(1, int(sector_exposure / (etf_price * 100)))

            # Cap sizing: don't spend more than max_nav_pct of NAV per sector
            if nav > 0 and self._config.max_nav_pct > 0:
                spread_cost_per_contract = (long_strike - short_strike) * 100
                if spread_cost_per_contract > 0:
                    max_contracts = max(1, int(
                        (nav * self._config.max_nav_pct) / spread_cost_per_contract
                    ))
                    n_contracts = min(n_contracts, max_contracts)

            target_expiry = ProtectivePutStrategy._find_target_expiry(
                today,
                self._config.target_dte_min,
                self._config.target_dte_max,
            )

            long_leg = OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=etf_symbol,
                right="P",
                expiry=target_expiry,
                strike=long_strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"{sector_name} SHI={shi:.2f}: buy put @ {long_strike}",
                metadata={"sector": sector_name, "leg": "long"},
            )

            short_leg = OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=etf_symbol,
                right="P",
                expiry=target_expiry,
                strike=short_strike,
                quantity=-n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"{sector_name} SHI={shi:.2f}: sell put @ {short_strike}",
                metadata={"sector": sector_name, "leg": "short"},
            )

            long_leg.spread_leg = short_leg
            directives.append(long_leg)

        return directives


# ── VIX Tail Hedge ───────────────────────────────────────────────────

class VixTailHedgeStrategy(OptionStrategy):
    """Always-on OTM VIX calls as tail risk insurance."""

    def __init__(self, config: Optional[VixTailHedgeConfig] = None) -> None:
        self._config = config or VixTailHedgeConfig()

    @property
    def name(self) -> str:
        return "vix_tail_hedge"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        vix_level = signals.get("vix_level", 20.0)
        nav = signals.get("nav", 0.0)

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        existing_vix = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        # Check for rolls
        for opt in existing_vix:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            if dte <= self._config.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=self._config.underlying,
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling VIX hedge: {dte} DTE remaining",
                ))
            else:
                # Active hedge exists, no need to open new
                return directives

        if nav <= 0:
            return directives

        # Open new VIX call
        # VIX option strike increments: 0.5-pt below ~25, 1-pt from 25-40,
        # 2.5-pt above 40.  Snap to the appropriate grid so IBKR qualifyContracts
        # doesn't return Error 200 for a non-listed strike (e.g. 41 doesn't exist).
        raw_strike = vix_level * (1 + self._config.strike_premium_pct)
        if raw_strike > 40:
            strike = float(round(raw_strike / 2.5) * 2.5)
        else:
            strike = float(round(raw_strike))

        # VIX options expire on Wednesdays (30 days before the 3rd Friday of
        # the following month), NOT on standard equity 3rd-Friday expiries.
        target_expiry = VixTailHedgeStrategy._find_vix_expiry(
            today,
            self._config.target_dte_min,
            self._config.target_dte_max,
        )

        # Size: nav_pct of NAV / estimated premium per contract
        budget = nav * self._config.nav_pct
        # VIX options: multiplier = 100, very OTM calls are cheap
        estimated_premium = max(vix_level * 0.03, 0.5) * 100  # ~$0.50-$2 per contract
        n_contracts = max(1, int(budget / max(estimated_premium, 1)))

        # Generous limit: 1.5× estimated premium to ensure a fill while
        # still avoiding runaway debit on a spike.  VIX options REQUIRE a
        # limit order (no market orders accepted at CFE).
        vix_limit_price = round(max(vix_level * 0.03, 0.5) * 1.5, 2)

        directives.append(OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=self._config.underlying,
            right="C",
            expiry=target_expiry,
            strike=strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=vix_limit_price,
            reason=f"VIX tail hedge: {n_contracts} calls @ {strike} "
                   f"(VIX={vix_level:.1f}, lmt={vix_limit_price:.2f})",
            metadata={
                "vix_level": vix_level,
                "budget": budget,
                "nav_pct": self._config.nav_pct,
            },
        ))

        return directives

    @staticmethod
    def _find_vix_expiry(today: date, min_dte: int, max_dte: int) -> str:
        """Return YYYYMMDD for a VIX option expiry within [min_dte, max_dte].

        VIX options expire on the Wednesday that is exactly 30 days before
        the 3rd Friday of the following calendar month (i.e. the day used
        as the settlement reference for SPX standard monthly options).
        """
        candidates = []
        for month_offset in range(1, 9):
            year = today.year
            month = today.month + month_offset
            while month > 12:
                month -= 12
                year += 1
            # 3rd Friday of (year, month)
            first_day = date(year, month, 1)
            days_to_fri = (4 - first_day.weekday()) % 7
            third_friday = first_day + timedelta(days=days_to_fri + 14)
            # VIX settlement Wednesday is 30 calendar days before the 3rd Friday.
            # IBKR's lastTradeDateOrContractMonth is the calendar day BEFORE the
            # settlement date (i.e. the last day orders are accepted).
            settlement_wed = third_friday - timedelta(days=30)
            vix_exp = settlement_wed - timedelta(days=1)
            candidates.append(vix_exp)

        # Return first candidate within the DTE window
        for exp in candidates:
            dte = (exp - today).days
            if min_dte <= dte <= max_dte:
                return exp.strftime("%Y%m%d")

        # Fallback: nearest future candidate
        for exp in candidates:
            if exp > today:
                return exp.strftime("%Y%m%d")

        return candidates[0].strftime("%Y%m%d")


# ── Short Puts (cash-secured) ─────────────────────────────────────────

class ShortPutStrategy(OptionStrategy):
    """Sell cash-secured puts on high-conviction equities.

    Triggered by lambda universe scores + STAB.  Targets 0.20–0.30 delta
    puts at 30–45 DTE.  Position sizing: max N% of buying power per
    underlying.  Rolls/closes at 14 DTE or 50% profit.
    """

    def __init__(
        self,
        config: Optional[ShortPutConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or ShortPutConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "short_put"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        nav = signals.get("nav", 0.0)
        buying_power = signals.get("buying_power", nav)
        lambda_scores: Dict[str, float] = signals.get("lambda_scores", {})
        stab_scores: Dict[str, float] = signals.get("stab_scores", {})
        equity_prices: Dict[str, float] = signals.get("equity_prices", {})

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Manage existing short puts — profit target, stop-loss & roll
        existing_puts = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]
        managed_symbols: set = set()

        for opt in existing_puts:
            managed_symbols.add(opt["symbol"])
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)

            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            if entry_price > 0 and current_price > 0:
                # Profit target: buy back at 50% decay
                profit_pct = (entry_price - current_price) / entry_price
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right="P",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Profit target {profit_pct:.0%} on {opt['symbol']}",
                    ))
                    continue

                # Stop-loss: close when loss exceeds max_loss_stop x credit received
                loss_multiple = (current_price - entry_price) / max(entry_price, 0.01)
                if loss_multiple >= self._config.max_loss_stop:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right="P",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Short put stop-loss: {loss_multiple:.1f}x on {opt['symbol']}",
                    ))
                    continue

            # Roll at low DTE
            if dte <= self._config.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=opt["symbol"],
                    right="P",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling {opt['symbol']} short put: {dte} DTE",
                ))

        # Open new positions — find high-conviction names
        current_count = len(existing_puts)
        if current_count >= self._config.max_positions:
            return directives

        if buying_power <= 0:
            return directives

        # VIX gate: only sell puts when there's real premium and market isn't already pricing in collapse
        vix = signals.get("vix_level", 20.0)
        if vix < self._config.min_vix or vix > self._config.max_vix:
            return directives

        max_per_underlying = buying_power * self._config.max_buying_power_pct

        # Score candidates by lambda + stab
        candidates: List[tuple] = []
        for symbol, lam_score in lambda_scores.items():
            if symbol in managed_symbols:
                continue
            stab = stab_scores.get(symbol, 0.0)
            if lam_score < self._config.min_lambda_score:
                continue
            if stab < self._config.min_stab_score:
                continue
            price = equity_prices.get(symbol, 0.0)
            if price <= 0:
                continue
            candidates.append((symbol, lam_score + stab, price))

        candidates.sort(key=lambda x: x[1], reverse=True)

        slots = self._config.max_positions - current_count
        for symbol, score, price in candidates[:slots]:
            # Determine expiry and strike via discovery or heuristic
            expiry = self._find_expiry(symbol, today)
            strike = self._find_strike(symbol, expiry, price)

            if strike is None or expiry is None:
                continue

            # Position sizing: notional = strike * 100; max N% of buying power
            notional_per_contract = strike * 100
            n_contracts = max(1, int(max_per_underlying / max(notional_per_contract, 1)))

            directives.append(OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="P",
                expiry=expiry,
                strike=strike,
                quantity=-n_contracts,  # Short
                order_type=OrderType.LIMIT,
                reason=(
                    f"Short put {symbol}: {n_contracts}x {strike}P "
                    f"(lambda={score:.2f})"
                ),
                metadata={"lambda_score": score, "stab": stab_scores.get(symbol, 0)},
            ))

        return directives

    def _find_expiry(self, symbol: str, as_of: Optional[date] = None) -> Optional[str]:
        """Find target expiry via discovery service or heuristic."""
        if self._discovery is not None:
            exp = self._discovery.get_best_expiry(
                symbol,
                min_dte=self._config.target_dte_min,
                max_dte=self._config.target_dte_max,
            )
            if exp:
                return exp
        return ProtectivePutStrategy._find_target_expiry(
            as_of or date.today(),
            self._config.target_dte_min,
            self._config.target_dte_max,
        )

    def _find_strike(self, symbol: str, expiry: Optional[str], price: float) -> Optional[float]:
        """Find target strike via discovery delta selection or heuristic."""
        if self._discovery is not None and expiry is not None:
            strike = self._discovery.get_option_by_delta(
                symbol, expiry, "P",
                target_delta=self._config.target_delta,
                underlying_price=price,
            )
            if strike is not None:
                return strike
        # Heuristic: 0.25 delta put ≈ 5-8% OTM
        return round(price * 0.93, 1)


# ── Futures Overlay ──────────────────────────────────────────────────

class FuturesOverlayStrategy(OptionStrategy):
    """Use ES/NQ futures for portfolio-level beta management.

    * When FRAG signal is elevated → short ES to hedge equity exposure.
    * When lambda scores are strongly positive → add ES longs for leverage.
    * Position sizing targets portfolio beta via delta-equivalent.
    """

    def __init__(
        self,
        config: Optional[FuturesOverlayConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or FuturesOverlayConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "futures_overlay"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        frag = signals.get("frag", 0.0)
        lambda_agg = signals.get("lambda_aggregate", 0.0)
        nav = signals.get("nav", 0.0)
        futures_positions: Dict[str, Any] = signals.get("futures_positions", {})
        es_price = signals.get("es_price", 0.0)

        directives: List[OptionTradeDirective] = []

        if nav <= 0 or es_price <= 0:
            return directives

        from prometheus.execution.futures_manager import PRODUCTS
        product = PRODUCTS.get(self._config.product)
        if product is None:
            return directives

        multiplier = product.multiplier  # ES = $50 per point

        # Current futures delta exposure
        current_futures_notional = sum(
            p.get("quantity", 0) * es_price * multiplier
            for p in futures_positions.values()
            if p.get("symbol") == self._config.product
        ) if futures_positions else 0.0

        # Determine target
        target_notional = 0.0
        reason = ""

        if frag >= self._config.frag_hedge_threshold:
            # FRAG elevated → hedge (short)
            hedge_ratio = min(
                (frag - self._config.frag_hedge_threshold) / (1.0 - self._config.frag_hedge_threshold),
                1.0,
            ) * self._config.frag_max_hedge_ratio
            target_notional = -nav * hedge_ratio
            reason = f"FRAG={frag:.2f}: hedge {hedge_ratio:.0%} of NAV"

        elif lambda_agg >= self._config.lambda_leverage_threshold:
            # Lambda strong → add leverage (long)
            lev_ratio = min(
                (lambda_agg - self._config.lambda_leverage_threshold)
                / (1.0 - self._config.lambda_leverage_threshold),
                1.0,
            ) * self._config.lambda_max_leverage_pct
            target_notional = nav * lev_ratio
            reason = f"Lambda={lambda_agg:.2f}: leverage {lev_ratio:.0%} of NAV"

        # Delta to target
        delta_notional = target_notional - current_futures_notional
        contract_value = es_price * multiplier
        if contract_value <= 0:
            return directives

        delta_contracts = int(delta_notional / contract_value)
        if delta_contracts == 0:
            return directives

        # Find front-month expiry
        expiry = self._get_front_expiry(signals.get('as_of_date'))

        side_label = "BUY" if delta_contracts > 0 else "SELL"
        directives.append(OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.HEDGE if frag >= self._config.frag_hedge_threshold else TradeAction.OPEN,
            symbol=self._config.product,
            right="",  # N/A for futures
            expiry=expiry or "",
            strike=0.0,  # N/A for futures
            quantity=delta_contracts,
            order_type=OrderType.MARKET,
            reason=f"{side_label} {abs(delta_contracts)} {self._config.product}: {reason}",
            metadata={
                "frag": frag,
                "lambda_agg": lambda_agg,
                "target_notional": target_notional,
                "current_notional": current_futures_notional,
                "is_futures": True,
            },
        ))

        return directives

    def _get_front_expiry(self, as_of: Optional[date] = None) -> Optional[str]:
        """Get front-month futures expiry via discovery or heuristic."""
        if self._discovery is not None:
            front = self._discovery.get_front_month_future(
                self._config.product, self._config.exchange,
            )
            if front is not None:
                return front.last_trade_date
        # Heuristic: next quarterly expiry (3rd Friday of Mar/Jun/Sep/Dec)
        from prometheus.execution.futures_manager import PRODUCTS, FuturesManager
        product = PRODUCTS.get(self._config.product)
        if product is None:
            return None
        today_str = (as_of or date.today()).strftime("%Y%m%d")
        return FuturesManager._find_next_expiry(product, today_str)


# ── Futures Options (FOP) Strategy ───────────────────────────────────

class FuturesOptionStrategy(OptionStrategy):
    """Defined-risk plays on VX and ES via futures options.

    * **VX call spreads**: When FRAG is low (expect vol expansion), buy
      call spreads on VX futures.
    * **ES put spreads**: Cheaper downside protection than SPY puts,
      triggered by low MHI.
    """

    def __init__(
        self,
        config: Optional[FuturesOptionConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or FuturesOptionConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "futures_option"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        directives: List[OptionTradeDirective] = []
        directives.extend(self._evaluate_vx_call_spreads(signals, existing_options))
        directives.extend(self._evaluate_es_put_spreads(signals, existing_options))
        return directives

    def _evaluate_vx_call_spreads(
        self,
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        """Buy VX call spreads when FRAG is low (expect vol expansion)."""
        frag = signals.get("frag", 0.5)
        nav = signals.get("nav", 0.0)
        vix_level = signals.get("vix_level", 20.0)
        today = signals.get("as_of_date", date.today())

        existing_vx = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name and opt.get("symbol") == "VX"
        ]

        directives: List[OptionTradeDirective] = []

        # Roll existing positions if needed
        for opt in existing_vx:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            if dte <= 14:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol="VX",
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling VX call spread: {dte} DTE",
                    metadata={"is_fop": True},
                ))
            else:
                return directives  # Active position exists, hold

        # Only open new if FRAG is low
        if frag >= self._config.vx_frag_low_threshold:
            return directives

        if nav <= 0:
            return directives

        # Determine strikes
        long_strike = round(vix_level + 2, 0)
        short_strike = long_strike + self._config.vx_spread_width

        # Expiry
        expiry = self._get_vx_expiry(signals.get('as_of_date'))

        # Size
        budget = nav * self._config.vx_nav_pct
        max_loss_per_spread = self._config.vx_spread_width * 1000  # VX multiplier = 1000
        n_contracts = max(1, int(budget / max(max_loss_per_spread, 1)))

        long_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol="VX",
            right="C",
            expiry=expiry or "",
            strike=long_strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"VX call spread: FRAG={frag:.2f}, VIX={vix_level:.1f}",
            metadata={"is_fop": True, "leg": "long"},
        )

        short_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol="VX",
            right="C",
            expiry=expiry or "",
            strike=short_strike,
            quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"VX call spread short leg @ {short_strike}",
            metadata={"is_fop": True, "leg": "short"},
        )

        long_leg.spread_leg = short_leg
        directives.append(long_leg)

        return directives

    def _evaluate_es_put_spreads(
        self,
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        """Buy ES put spreads for downside protection when MHI is low."""
        if not self._config.es_put_spread_enabled:
            return []

        mhi = signals.get("mhi", 1.0)
        nav = signals.get("nav", 0.0)
        es_price = signals.get("es_price", 0.0)
        today = signals.get("as_of_date", date.today())

        existing_es = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name and opt.get("symbol") == "ES"
        ]

        directives: List[OptionTradeDirective] = []

        # Manage existing
        for opt in existing_es:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            if mhi >= self._config.es_mhi_threshold + 0.10:
                # MHI recovered — close
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol="ES",
                    right="P",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"MHI recovered to {mhi:.2f}, closing ES put spread",
                    metadata={"is_fop": True},
                ))
            elif dte <= 14:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol="ES",
                    right="P",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling ES put spread: {dte} DTE",
                    metadata={"is_fop": True},
                ))
            else:
                return directives  # Hold

        if mhi >= self._config.es_mhi_threshold:
            return directives

        if nav <= 0 or es_price <= 0:
            return directives

        # Strikes
        long_strike = round(es_price * 0.97, 0)   # ~3% OTM
        short_strike = round(es_price * (0.97 - self._config.es_spread_width_pct), 0)

        expiry = self._get_es_expiry(signals.get('as_of_date'))

        # Size: budget / max_loss_per_spread
        budget = nav * self._config.es_nav_pct
        spread_width = long_strike - short_strike
        from prometheus.execution.futures_manager import PRODUCTS
        es_product = PRODUCTS.get("ES")
        mult = es_product.multiplier if es_product else 50.0
        max_loss = spread_width * mult
        n_contracts = max(1, int(budget / max(max_loss, 1)))

        long_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol="ES",
            right="P",
            expiry=expiry or "",
            strike=long_strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"ES put spread: MHI={mhi:.2f}, ES={es_price:.0f}",
            metadata={"is_fop": True, "leg": "long"},
        )

        short_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol="ES",
            right="P",
            expiry=expiry or "",
            strike=short_strike,
            quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"ES put spread short leg @ {short_strike}",
            metadata={"is_fop": True, "leg": "short"},
        )

        long_leg.spread_leg = short_leg
        directives.append(long_leg)

        return directives

    def _get_vx_expiry(self, as_of: Optional[date] = None) -> Optional[str]:
        """Get VX futures option expiry."""
        if self._discovery is not None:
            front = self._discovery.get_front_month_future("VX", "CFE")
            if front is not None:
                return front.last_trade_date
        # Heuristic
        from prometheus.execution.futures_manager import PRODUCTS, FuturesManager
        product = PRODUCTS.get("VX")
        if product is None:
            return None
        return FuturesManager._find_next_expiry(product, (as_of or date.today()).strftime("%Y%m%d"))

    def _get_es_expiry(self, as_of: Optional[date] = None) -> Optional[str]:
        """Get ES futures option expiry."""
        if self._discovery is not None:
            front = self._discovery.get_front_month_future("ES", "CME")
            if front is not None:
                return front.last_trade_date
        from prometheus.execution.futures_manager import PRODUCTS, FuturesManager
        product = PRODUCTS.get("ES")
        if product is None:
            return None
        return FuturesManager._find_next_expiry(product, (as_of or date.today()).strftime("%Y%m%d"))


# ── Bull Call Spread ──────────────────────────────────────────────────

class BullCallSpreadStrategy(OptionStrategy):
    """Buy call spreads on high-conviction names in RISK_ON.

    Long ATM/slightly-ITM call + short OTM call for defined-risk
    directional exposure.  Triggered by lambda scores.
    """

    def __init__(
        self,
        config: Optional[BullCallSpreadConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or BullCallSpreadConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "bull_call_spread"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        market_state = signals.get("market_state", "NEUTRAL")
        if market_state != "RISK_ON":
            return []

        nav = signals.get("nav", 0.0)
        # Use momentum_scores (63d proxy) for directional conviction;
        # fall back to lambda_scores when momentum_scores not in signals.
        lambda_scores: Dict[str, float] = (
            signals.get("momentum_scores")
            or signals.get("lambda_scores", {})
        )
        stab_scores: Dict[str, float] = signals.get("stab_scores", {})
        equity_prices: Dict[str, float] = signals.get("equity_prices", {})

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Manage existing
        existing_spreads = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]
        managed_symbols: set = set()

        for opt in existing_spreads:
            managed_symbols.add(opt["symbol"])
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            if entry_price > 0 and current_price > 0:
                profit_pct = (current_price - entry_price) / max(entry_price, 0.01)
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right="C",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Profit target {profit_pct:.0%} on {opt['symbol']} bull call",
                    ))
                    continue

            if dte <= 14:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Closing {opt['symbol']} bull call: {dte} DTE",
                ))

        # Open new positions
        current_count = len({opt["symbol"] for opt in existing_spreads})
        if current_count >= self._config.max_positions or nav <= 0:
            return directives

        max_risk = nav * self._config.max_risk_per_trade_pct

        candidates: List[tuple] = []
        for symbol, lam_score in lambda_scores.items():
            if symbol in managed_symbols:
                continue
            stab = stab_scores.get(symbol, 0.0)
            if lam_score < self._config.min_lambda_score:
                continue
            if stab < self._config.min_stab_score:
                continue
            price = equity_prices.get(symbol, 0.0)
            if price <= 0:
                continue
            candidates.append((symbol, lam_score + stab, price))

        candidates.sort(key=lambda x: x[1], reverse=True)
        slots = self._config.max_positions - current_count

        for symbol, score, price in candidates[:slots]:
            # Long leg: slightly ITM
            long_strike = round(price * (1 - (1 - self._config.long_delta) * 0.15), 1)
            # Short leg: OTM
            short_strike = round(price * (1 + self._config.spread_width_pct), 1)

            expiry = self._find_expiry(symbol, today)
            if expiry is None:
                continue

            # Size: max risk / max loss per spread
            spread_width = short_strike - long_strike
            if spread_width <= 0:
                continue
            max_loss_per_contract = spread_width * 100
            n_contracts = max(1, int(max_risk / max(max_loss_per_contract, 1)))

            long_leg = OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="C",
                expiry=expiry,
                strike=long_strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Bull call spread {symbol}: {long_strike}/{short_strike} "
                       f"(lambda={score:.2f})",
                metadata={"lambda_score": score, "leg": "long"},
            )

            short_leg = OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="C",
                expiry=expiry,
                strike=short_strike,
                quantity=-n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Bull call spread {symbol} short leg @ {short_strike}",
                metadata={"lambda_score": score, "leg": "short"},
            )

            long_leg.spread_leg = short_leg
            directives.append(long_leg)

        return directives

    def _find_expiry(self, symbol: str, as_of: Optional[date] = None) -> Optional[str]:
        if self._discovery is not None:
            exp = self._discovery.get_best_expiry(
                symbol,
                min_dte=self._config.target_dte_min,
                max_dte=self._config.target_dte_max,
            )
            if exp:
                return exp
        return ProtectivePutStrategy._find_target_expiry(
            as_of or date.today(),
            self._config.target_dte_min,
            self._config.target_dte_max,
        )


# ── Momentum Call Overlay ─────────────────────────────────────────────

class MomentumCallStrategy(OptionStrategy):
    """Buy SPY ATM call spreads during confirmed bull markets.

    Addresses the bull-year drag problem: when equity rips +30-40%,
    vol-harvesting strategies contribute but can't keep pace.  This
    strategy adds directional upside participation via defined-risk
    call spreads on SPY, gated by regime (RISK_ON only), VIX (< 20),
    and 63-day momentum (positive trend confirmation).
    """

    def __init__(
        self,
        config: Optional[MomentumCallConfig] = None,
    ) -> None:
        self._config = config or MomentumCallConfig()

    @property
    def name(self) -> str:
        return "momentum_call"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        cfg = self._config
        market_state = signals.get("market_state", "NEUTRAL")
        vix = signals.get("vix_level", 20.0)
        nav = signals.get("nav", 0.0)
        spy_price = signals.get("spy_price", 0.0)
        today = signals.get("as_of_date", date.today())

        directives: List[OptionTradeDirective] = []

        # ── Manage existing positions ──
        existing = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        for opt in existing:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            # Profit target
            if entry_price > 0 and current_price > 0:
                profit_pct = (current_price - entry_price) / max(entry_price, 0.01)
                if profit_pct >= cfg.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right="C",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Momentum call profit target {profit_pct:.0%}",
                    ))
                    continue

            # Close on low DTE
            if dte <= cfg.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Closing momentum call: {dte} DTE",
                ))
                continue

            # Close on regime break — if we're no longer RISK_ON, exit
            if market_state != "RISK_ON":
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Regime break: {market_state}, closing momentum call",
                ))

        # ── Entry gating ──
        if market_state != "RISK_ON":
            return directives
        if vix >= cfg.max_vix:
            return directives
        if nav <= 0 or spy_price <= 0:
            return directives

        current_count = len([
            opt for opt in existing
            if opt.get("strategy") == self.name
        ])
        if current_count >= cfg.max_positions:
            return directives

        # ── Momentum check ──
        # Use pre-computed momentum from signals (backtest computes from price cache)
        momentum = signals.get("spy_momentum_63d", 0.0)

        if momentum < cfg.min_momentum_63d:
            return directives

        # ── Open new call spread ──
        # Long leg: slightly ITM (~0.55 delta)
        long_strike = round(spy_price * (1 - (1 - cfg.long_delta) * 0.10), 0)
        # Short leg: OTM
        short_strike = round(spy_price * (1 + cfg.spread_width_pct), 0)

        expiry = ProtectivePutStrategy._find_target_expiry(
            today, cfg.target_dte_min, cfg.target_dte_max,
        )

        # Size: nav_pct / max_loss_per_contract
        spread_width = short_strike - long_strike
        if spread_width <= 0:
            return directives

        budget = nav * cfg.nav_pct
        max_loss_per_contract = spread_width * 100
        n_contracts = max(1, int(budget / max(max_loss_per_contract, 1)))

        long_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=cfg.underlying,
            right="C",
            expiry=expiry,
            strike=long_strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"Momentum call SPY {long_strike}/{short_strike} "
                   f"(mom={momentum:.1%}, VIX={vix:.1f})",
            metadata={
                "momentum": round(momentum, 4),
                "vix": vix,
                "leg": "long",
            },
        )

        short_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=cfg.underlying,
            right="C",
            expiry=expiry,
            strike=short_strike,
            quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"Momentum call SPY short leg @ {short_strike}",
            metadata={"leg": "short"},
        )

        long_leg.spread_leg = short_leg
        directives.append(long_leg)

        return directives


# ── LEAPS (Stock Replacement) ────────────────────────────────────────

class LEAPSStrategy(OptionStrategy):
    """Buy deep ITM LEAPS calls for capital-efficient long exposure.

    Replaces a fraction of large equity positions with deep ITM calls
    (0.70–0.80 delta, 6–12 month expiry), freeing capital while
    maintaining upside participation.
    """

    def __init__(
        self,
        config: Optional[LEAPSConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or LEAPSConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "leaps"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        market_state = signals.get("market_state", "NEUTRAL")
        if market_state != "RISK_ON":
            return []

        lambda_scores: Dict[str, float] = signals.get("lambda_scores", {})
        stab_scores: Dict[str, float] = signals.get("stab_scores", {})

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Manage existing LEAPS — roll when DTE < 90
        existing_leaps = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]
        managed_symbols: set = set()

        for opt in existing_leaps:
            managed_symbols.add(opt["symbol"])
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            if dte <= self._config.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=opt["symbol"],
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling LEAPS {opt['symbol']}: {dte} DTE",
                ))

        # Open new LEAPS on large positions with strong conviction
        current_count = len(managed_symbols)
        if current_count >= self._config.max_positions:
            return directives

        for iid, pos in portfolio.items():
            symbol = iid.split(".")[0] if "." in iid else iid
            if symbol in managed_symbols:
                continue

            qty = pos.quantity
            mv = pos.market_value
            if mv < self._config.min_position_value:
                continue

            lam_score = lambda_scores.get(symbol, 0.0)
            stab = stab_scores.get(symbol, 0.0)
            if lam_score < self._config.min_lambda_score:
                continue
            if stab < self._config.min_stab_score:
                continue

            price = mv / max(qty, 1)
            if price <= 0:
                continue

            # How many shares to replace with LEAPS
            replace_shares = int(qty * self._config.max_replacement_pct)
            n_contracts = replace_shares // 100
            if n_contracts < 1:
                continue

            # Deep ITM strike: ~0.75 delta ≈ roughly 10-15% ITM
            strike = round(price * 0.85, 1)

            expiry = self._find_expiry(symbol, today)
            if expiry is None:
                continue

            directives.append(OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="C",
                expiry=expiry,
                strike=strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"LEAPS {symbol}: {n_contracts} contracts @ {strike} "
                       f"(replacing {replace_shares} shares, lambda={lam_score:.2f})",
                metadata={
                    "lambda_score": lam_score,
                    "stab": stab,
                    "shares_replaced": replace_shares,
                },
            ))

            managed_symbols.add(symbol)
            if len(managed_symbols) >= self._config.max_positions:
                break

        return directives

    def _find_expiry(self, symbol: str, as_of: Optional[date] = None) -> Optional[str]:
        if self._discovery is not None:
            exp = self._discovery.get_best_expiry(
                symbol,
                min_dte=self._config.target_dte_min,
                max_dte=self._config.target_dte_max,
            )
            if exp:
                return exp
        return ProtectivePutStrategy._find_target_expiry(
            as_of or date.today(),
            self._config.target_dte_min,
            self._config.target_dte_max,
        )


# ── Iron Condor ────────────────────────────────────────────────────────

class IronCondorStrategy(OptionStrategy):
    """Sell iron condors for income in low-vol, range-bound markets.

    Sell OTM put spread + OTM call spread on index ETFs.  Four legs:
    long put wing, short put, short call, long call wing.
    Profits from theta decay when underlying stays within the short strikes.
    """

    def __init__(self, config: Optional[IronCondorConfig] = None) -> None:
        self._config = config or IronCondorConfig()

    @property
    def name(self) -> str:
        return "iron_condor"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        vix = signals.get("vix_level", 25.0)
        frag = signals.get("frag", 0.5)
        nav = signals.get("nav", 0.0)
        regime_hostile = vix > self._config.max_vix or frag > self._config.max_frag

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Always manage existing condors regardless of current regime —
        # positions opened in calm markets must be exited when conditions deteriorate.
        existing_condors = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        for opt in existing_condors:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            if entry_price > 0 and current_price > 0:
                # Profit target: buy back at configured % of max credit
                profit_pct = (entry_price - current_price) / max(entry_price, 0.01)
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right=opt.get("right", "P"),
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Iron condor profit target: {profit_pct:.0%}",
                    ))
                    continue

                # Stop-loss on short legs: exit when loss exceeds max_loss_multiple x credit
                if opt.get("quantity", 0) < 0:
                    loss_multiple = (current_price - entry_price) / max(entry_price, 0.01)
                    if loss_multiple >= self._config.max_loss_multiple:
                        directives.append(OptionTradeDirective(
                            strategy=self.name,
                            action=TradeAction.CLOSE,
                            symbol=opt["symbol"],
                            right=opt.get("right", "P"),
                            expiry=opt["expiry"],
                            strike=opt["strike"],
                            quantity=-opt["quantity"],
                            reason=f"Iron condor stop-loss: {loss_multiple:.1f}x credit",
                        ))
                        continue

            # Close at 14 DTE to avoid gamma risk
            if dte <= 14:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right=opt.get("right", "P"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Closing iron condor: {dte} DTE (gamma risk)",
                ))
                continue

            # Emergency exit: regime turned hostile after entry
            if regime_hostile:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right=opt.get("right", "P"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Iron condor regime exit: VIX={vix:.1f} FRAG={frag:.2f}",
                ))

        # No new positions when regime is hostile or VIX floor not met (premium too thin)
        if regime_hostile or vix < self._config.min_vix:
            return directives

        # Count unique expiries to track position count
        existing_expiries = {opt["expiry"] for opt in existing_condors}
        if len(existing_expiries) >= self._config.max_positions or nav <= 0:
            return directives

        underlying = self._config.underlying
        spy_price = signals.get("spy_price", 0.0)
        if underlying == "SPY" and spy_price <= 0:
            return directives
        price = spy_price if underlying == "SPY" else signals.get("equity_prices", {}).get(underlying, 0.0)
        if price <= 0:
            return directives

        # Strikes based on delta approximation
        # ~0.18 delta ≈ ~1 standard deviation out (~6-8% OTM for 30-45 DTE)
        short_put = round(price * 0.93, 0)
        long_put = short_put - self._config.wing_width
        short_call = round(price * 1.07, 0)
        long_call = short_call + self._config.wing_width

        target_expiry = ProtectivePutStrategy._find_target_expiry(
            today, self._config.target_dte_min, self._config.target_dte_max,
        )

        # Bug fix: same as butterfly — use (wing_width - net_credit) as max-loss.
        # For condors the OTM spreads collect less credit so the error is smaller,
        # but the principle is identical.
        _r_c   = 0.04
        _sig_c = max(vix, 1.0) / 100.0
        _T_c   = max(self._config.target_dte_min + self._config.target_dte_max, 2) / 2 / 365.0
        # Individual leg premiums (needed for per-leg limit prices)
        _sp_prem = _bs_price(price, short_put,  _T_c, _r_c, _sig_c, "P")
        _lp_prem = _bs_price(price, long_put,   _T_c, _r_c, _sig_c, "P")
        _sc_prem = _bs_price(price, short_call, _T_c, _r_c, _sig_c, "C")
        _lc_prem = _bs_price(price, long_call,  _T_c, _r_c, _sig_c, "C")
        _put_spread_credit  = _sp_prem - _lp_prem
        _call_spread_credit = _sc_prem - _lc_prem
        _condor_net_credit = _put_spread_credit + _call_spread_credit
        _condor_max_loss   = max(
            self._config.wing_width * 0.10,
            self._config.wing_width - _condor_net_credit,
        ) * 100
        budget                    = nav * self._config.nav_pct
        n_by_max_loss             = int(budget / max(_condor_max_loss, 1))
        _condor_credit_per_contract = max(_condor_net_credit, 0.01) * 100
        n_by_credit               = int(budget / _condor_credit_per_contract)
        n_contracts               = max(1, min(n_by_max_loss, n_by_credit))

        # Book-level margin cap (same logic as butterfly)
        _deriv_budget_c = signals.get("buying_power", nav * 0.15)
        _margin_used_c  = signals.get("butterfly_condor_margin_used", 0.0)
        _margin_avail_c = max(0.0, _deriv_budget_c - _margin_used_c)
        if _margin_avail_c <= 0:
            return directives
        _n_by_margin_c = int(_margin_avail_c / max(_condor_credit_per_contract, 1))
        n_contracts    = max(1, min(n_contracts, _n_by_margin_c))

        # Put spread (short put + long put wing)
        # Store wing_strike so lifecycle barrier-stop can fire when price breaches wing.
        short_put_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying,
            right="P",
            expiry=target_expiry,
            strike=short_put,
            quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_sp_prem * 0.9, 2), 0.01),  # SELL: accept 10% below BS mid
            reason=f"Iron condor {underlying}: sell {short_put}P (VIX={vix:.1f})",
            metadata={"leg": "short_put", "wing_strike": long_put},
        )

        long_put_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying,
            right="P",
            expiry=target_expiry,
            strike=long_put,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_lp_prem * 1.1, 2), 0.01),  # BUY: pay up to 10% above BS mid
            reason=f"Iron condor {underlying}: buy {long_put}P wing",
            metadata={"leg": "long_put_wing"},
        )

        # Call spread (short call + long call wing)
        short_call_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying,
            right="C",
            expiry=target_expiry,
            strike=short_call,
            quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_sc_prem * 0.9, 2), 0.01),  # SELL: accept 10% below BS mid
            reason=f"Iron condor {underlying}: sell {short_call}C",
            metadata={"leg": "short_call", "wing_strike": long_call},
        )

        long_call_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying,
            right="C",
            expiry=target_expiry,
            strike=long_call,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_lc_prem * 1.1, 2), 0.01),  # BUY: pay up to 10% above BS mid
            reason=f"Iron condor {underlying}: buy {long_call}C wing",
            metadata={"leg": "long_call_wing"},
        )

        # Chain legs: put side
        short_put_leg.spread_leg = long_put_leg
        # Chain legs: call side
        short_call_leg.spread_leg = long_call_leg

        directives.append(short_put_leg)
        directives.append(short_call_leg)

        return directives


# ── Iron Butterfly ─────────────────────────────────────────────────────

class IronButterflyStrategy(OptionStrategy):
    """Sell iron butterflies for premium in very low vol environments.

    Sell ATM straddle (same-strike put+call) + buy OTM wings.
    Higher credit than condor but narrower profit zone.
    Only deployed in very calm markets (VIX < 16).
    """

    def __init__(self, config: Optional[IronButterflyConfig] = None) -> None:
        self._config = config or IronButterflyConfig()

    @property
    def name(self) -> str:
        return "iron_butterfly"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        vix = signals.get("vix_level", 25.0)
        frag = signals.get("frag", 0.5)
        nav = signals.get("nav", 0.0)
        regime_hostile = vix > self._config.max_vix or frag > self._config.max_frag

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Always manage existing butterflies regardless of current regime.
        existing_flies = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        for opt in existing_flies:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            if entry_price > 0 and current_price > 0:
                profit_pct = (entry_price - current_price) / max(entry_price, 0.01)
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right=opt.get("right", "P"),
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Iron butterfly profit target: {profit_pct:.0%}",
                    ))
                    continue

                # Stop-loss on short legs
                if opt.get("quantity", 0) < 0:
                    loss_multiple = (current_price - entry_price) / max(entry_price, 0.01)
                    if loss_multiple >= self._config.max_loss_multiple:
                        directives.append(OptionTradeDirective(
                            strategy=self.name,
                            action=TradeAction.CLOSE,
                            symbol=opt["symbol"],
                            right=opt.get("right", "P"),
                            expiry=opt["expiry"],
                            strike=opt["strike"],
                            quantity=-opt["quantity"],
                            reason=f"Iron butterfly stop-loss: {loss_multiple:.1f}x credit",
                        ))
                        continue

            if dte <= 14:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right=opt.get("right", "P"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Closing iron butterfly: {dte} DTE",
                ))
                continue

            # Emergency exit: regime turned hostile
            if regime_hostile:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right=opt.get("right", "P"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Iron butterfly regime exit: VIX={vix:.1f} FRAG={frag:.2f}",
                ))

        if regime_hostile:
            return directives

        existing_expiries = {opt["expiry"] for opt in existing_flies}
        if len(existing_expiries) >= self._config.max_positions or nav <= 0:
            return directives

        underlying = self._config.underlying
        spy_price = signals.get("spy_price", 0.0)
        price = spy_price if underlying == "SPY" else signals.get("equity_prices", {}).get(underlying, 0.0)
        if price <= 0:
            return directives

        # ATM body, OTM wings
        atm_strike = round(price, 0)
        long_put_wing = atm_strike - self._config.wing_width
        long_call_wing = atm_strike + self._config.wing_width

        target_expiry = ProtectivePutStrategy._find_target_expiry(
            today, self._config.target_dte_min, self._config.target_dte_max,
        )

        # Bug fix: use actual max-loss per contract = (wing_width - net_credit) × 100.
        # The old formula used wing_width × 100, which overstates max-loss by 5–10× in
        # low-VIX environments because ATM credit ≈ wing_width.  That inflated n_contracts
        # by the same factor, creating unrealistic P&L.
        _r   = 0.04
        _sig = max(vix, 1.0) / 100.0
        _T   = max(self._config.target_dte_min + self._config.target_dte_max, 2) / 2 / 365.0
        # Individual leg premiums (needed for per-leg limit prices)
        _atm_c_prem = _bs_price(price, atm_strike,    _T, _r, _sig, "C")
        _atm_p_prem = _bs_price(price, atm_strike,    _T, _r, _sig, "P")
        _lc_w_prem  = _bs_price(price, long_call_wing, _T, _r, _sig, "C")
        _lp_w_prem  = _bs_price(price, long_put_wing,  _T, _r, _sig, "P")
        _net_credit = _atm_c_prem + _atm_p_prem - _lc_w_prem - _lp_w_prem
        # Floor at 10 % of wing so we never divide by near-zero
        _max_loss_per_contract = max(
            self._config.wing_width * 0.10,
            self._config.wing_width - _net_credit,
        ) * 100
        budget        = nav * self._config.nav_pct
        n_by_max_loss = int(budget / max(_max_loss_per_contract, 1))
        # Second constraint: total premium written ≤ budget.
        # In low-VIX envs the 10 % max-loss floor is binding, which lets us write
        # credit ≈ 9× budget — unrealistic.  Capping here ensures the amount of
        # premium collected is proportional to the stated risk allocation.
        _credit_per_contract = max(_net_credit, 0.01) * 100
        n_by_credit   = int(budget / _credit_per_contract)
        n_contracts   = max(1, min(n_by_max_loss, n_by_credit))

        # Book-level margin cap ── prevent total butterfly/condor credit from
        # exceeding the derivatives budget.  Without this, max_positions × nav_pct
        # can far exceed the stated budget, producing unrealistic compounding.
        _deriv_budget = signals.get("buying_power", nav * 0.15)
        _margin_used  = signals.get("butterfly_condor_margin_used", 0.0)
        _margin_avail = max(0.0, _deriv_budget - _margin_used)
        if _margin_avail <= 0:
            return directives  # Book is full — no room for a new position
        # Scale down n_contracts to fit remaining budget
        _n_by_margin = int(_margin_avail / max(_credit_per_contract, 1))
        n_contracts  = max(1, min(n_contracts, _n_by_margin))

        # Short ATM put — store wing_strike so lifecycle barrier-stop can use it
        short_put = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying, right="P", expiry=target_expiry,
            strike=atm_strike, quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_atm_p_prem * 0.9, 2), 0.01),  # SELL: accept 10% below BS mid
            reason=f"Iron butterfly {underlying}: sell ATM {atm_strike}P (VIX={vix:.1f})",
            metadata={"leg": "short_put", "wing_strike": long_put_wing},
        )
        # Short ATM call
        short_call = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying, right="C", expiry=target_expiry,
            strike=atm_strike, quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_atm_c_prem * 0.9, 2), 0.01),  # SELL: accept 10% below BS mid
            reason=f"Iron butterfly {underlying}: sell ATM {atm_strike}C",
            metadata={"leg": "short_call", "wing_strike": long_call_wing},
        )
        # Long put wing
        lp_wing = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying, right="P", expiry=target_expiry,
            strike=long_put_wing, quantity=n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_lp_w_prem * 1.1, 2), 0.01),  # BUY: pay up to 10% above BS mid
            reason=f"Iron butterfly {underlying}: buy {long_put_wing}P wing",
            metadata={"leg": "long_put_wing"},
        )
        # Long call wing
        lc_wing = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying, right="C", expiry=target_expiry,
            strike=long_call_wing, quantity=n_contracts,
            order_type=OrderType.LIMIT,
            limit_price=max(round(_lc_w_prem * 1.1, 2), 0.01),  # BUY: pay up to 10% above BS mid
            reason=f"Iron butterfly {underlying}: buy {long_call_wing}C wing",
            metadata={"leg": "long_call_wing"},
        )

        short_put.spread_leg = lp_wing
        short_call.spread_leg = lc_wing
        directives.append(short_put)
        directives.append(short_call)

        return directives


# ── Collar ─────────────────────────────────────────────────────────────

class CollarStrategy(OptionStrategy):
    """Protect large equity positions with costless collars.

    Buy protective put + sell covered call at approximately equal premium
    (zero-cost collar).  Deployed in RECOVERY or early RISK_OFF to protect
    gains without selling the equity position.
    """

    def __init__(
        self,
        config: Optional[CollarConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or CollarConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "collar"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        market_state = signals.get("market_state", "NEUTRAL")
        if market_state not in ("RECOVERY", "RISK_OFF"):
            return []

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Manage existing collars
        existing_collars = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]
        collared_symbols: set = set()

        for opt in existing_collars:
            collared_symbols.add(opt["symbol"])
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)

            if dte <= self._config.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=opt["symbol"],
                    right=opt.get("right", "P"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Rolling collar {opt['symbol']}: {dte} DTE",
                ))

        # Find new collar candidates from portfolio
        if len(collared_symbols) >= self._config.max_positions:
            return directives

        for iid, pos in portfolio.items():
            if pos.quantity < self._config.min_position_shares:
                continue

            symbol = iid.split(".")[0] if "." in iid else iid
            if symbol in collared_symbols:
                continue

            if pos.market_value < self._config.min_position_value:
                continue

            price = pos.market_value / max(pos.quantity, 1)
            if price <= 0:
                continue

            n_contracts = pos.quantity // 100
            if n_contracts < 1:
                continue

            # Put: ~0.32 delta ≈ ~5-7% OTM
            put_strike = round(price * 0.94, 1)
            # Call: ~0.32 delta ≈ ~5-7% OTM
            call_strike = round(price * 1.06, 1)

            target_expiry = ProtectivePutStrategy._find_target_expiry(
                today,
                self._config.target_dte_min,
                self._config.target_dte_max,
            )

            # Long put (protective)
            put_leg = OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="P",
                expiry=target_expiry,
                strike=put_strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Collar {symbol}: buy {put_strike}P "
                       f"(protecting {pos.quantity:.0f} shares)",
                metadata={"leg": "protective_put", "position_qty": pos.quantity},
            )

            # Short call (financing)
            call_leg = OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="C",
                expiry=target_expiry,
                strike=call_strike,
                quantity=-n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Collar {symbol}: sell {call_strike}C (financing)",
                metadata={"leg": "financing_call", "position_qty": pos.quantity},
            )

            put_leg.spread_leg = call_leg
            directives.append(put_leg)

            collared_symbols.add(symbol)
            if len(collared_symbols) >= self._config.max_positions:
                break

        return directives


# ── Calendar Spread ───────────────────────────────────────────────────

class CalendarSpreadStrategy(OptionStrategy):
    """Exploit vol term structure with calendar spreads.

    Sell near-term option + buy longer-term option at same strike.
    Profits from faster theta decay of the front month and/or
    normalisation of a steep vol term structure.
    """

    def __init__(self, config: Optional[CalendarSpreadConfig] = None) -> None:
        self._config = config or CalendarSpreadConfig()

    @property
    def name(self) -> str:
        return "calendar_spread"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        nav = signals.get("nav", 0.0)
        vix = signals.get("vix_level", 20.0)

        # Check for VIX contango (term structure slope)
        # In practice, use VIX vs VIX3M; here use a proxy: if VIX < 20,
        # term structure is likely in contango.
        vix_contango = signals.get("vix_contango", max(0, (20 - vix) / 100))
        if vix_contango < self._config.min_vix_contango:
            return []

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Manage existing calendars
        existing_cals = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        for opt in existing_cals:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            if entry_price > 0 and current_price > 0:
                profit_pct = (current_price - entry_price) / max(entry_price, 0.01)
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right="C",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Calendar spread profit target: {profit_pct:.0%}",
                    ))
                    continue

                loss_pct = (entry_price - current_price) / max(entry_price, 0.01)
                if loss_pct >= self._config.max_loss_pct:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right="C",
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Calendar spread stop loss: {loss_pct:.0%}",
                    ))
                    continue

            # Close front leg at 7 DTE
            if dte <= 7:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right="C",
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Calendar spread: closing at {dte} DTE",
                ))

        existing_count = len({opt["expiry"] for opt in existing_cals})
        if existing_count >= self._config.max_positions or nav <= 0:
            return directives

        underlying = self._config.underlying
        spy_price = signals.get("spy_price", 0.0)
        price = spy_price if underlying == "SPY" else signals.get("equity_prices", {}).get(underlying, 0.0)
        if price <= 0:
            return directives

        atm_strike = round(price, 0)

        # Front month: ~30 DTE
        front_expiry = ProtectivePutStrategy._find_target_expiry(
            today, self._config.front_dte_min, self._config.front_dte_max,
        )
        # Back month: ~60-90 DTE
        back_expiry = ProtectivePutStrategy._find_target_expiry(
            today, self._config.back_dte_min, self._config.back_dte_max,
        )

        if front_expiry == back_expiry:
            return directives  # Need different expirations

        budget = nav * self._config.nav_pct
        # Calendar debit is typically ~$2-5 per spread on SPY
        estimated_debit = max(price * 0.008, 1.0) * 100
        n_contracts = max(1, int(budget / max(estimated_debit, 1)))

        # Sell front month (short)
        front_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying,
            right="C",
            expiry=front_expiry,
            strike=atm_strike,
            quantity=-n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"Calendar spread {underlying}: sell {atm_strike}C "
                   f"front ({front_expiry})",
            metadata={"leg": "front_short"},
        )

        # Buy back month (long)
        back_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol=underlying,
            right="C",
            expiry=back_expiry,
            strike=atm_strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"Calendar spread {underlying}: buy {atm_strike}C "
                   f"back ({back_expiry})",
            metadata={"leg": "back_long"},
        )

        front_leg.spread_leg = back_leg
        directives.append(front_leg)

        return directives


# ── Straddle / Strangle ───────────────────────────────────────────────

class StraddleStrangleStrategy(OptionStrategy):
    """Buy volatility around regime transitions or vol-cheap environments.

    Straddle: buy ATM put + ATM call (max gamma, higher cost).
    Strangle: buy OTM put + OTM call (cheaper, needs bigger move).
    Deployed when vol is cheap (VIX < threshold) and a regime shift
    is expected.
    """

    def __init__(self, config: Optional[StraddleStrangleConfig] = None) -> None:
        self._config = config or StraddleStrangleConfig()

    @property
    def name(self) -> str:
        return "straddle_strangle"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        vix = signals.get("vix_level", 25.0)
        nav = signals.get("nav", 0.0)
        frag = signals.get("frag", 0.0)
        market_state = signals.get("market_state", "NEUTRAL")

        # In RECOVERY, allow higher VIX (betting on continued vol movement);
        # otherwise only buy vol when it's cheap.
        vix_cap = 25.0 if market_state == "RECOVERY" else self._config.max_entry_vix
        if vix > vix_cap:
            return []

        # Need a catalyst: elevated fragility suggests regime transition
        # approaching, or market_state is RECOVERY (transitional)
        regime_transition_signal = (
            frag >= 0.40  # Fragility rising → vol expansion expected
            or market_state == "RECOVERY"
        )
        if not regime_transition_signal:
            return []

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Manage existing positions
        existing_vols = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]

        for opt in existing_vols:
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            if entry_price > 0 and current_price > 0:
                profit_pct = (current_price - entry_price) / max(entry_price, 0.01)
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right=opt.get("right", "C"),
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Vol play profit target: {profit_pct:.0%}",
                    ))
                    continue

                loss_pct = (entry_price - current_price) / max(entry_price, 0.01)
                if loss_pct >= self._config.max_loss_pct:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right=opt.get("right", "C"),
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Vol play stop loss: {loss_pct:.0%}",
                    ))
                    continue

            if dte <= 7:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.CLOSE,
                    symbol=opt["symbol"],
                    right=opt.get("right", "C"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Vol play: closing at {dte} DTE (theta burn)",
                ))

        existing_count = len({opt.get("expiry") for opt in existing_vols})
        if existing_count >= self._config.max_positions or nav <= 0:
            return directives

        spy_price = signals.get("spy_price", 0.0)
        if spy_price <= 0:
            return directives

        target_expiry = ProtectivePutStrategy._find_target_expiry(
            today, self._config.target_dte_min, self._config.target_dte_max,
        )

        budget = nav * self._config.nav_pct

        if self._config.prefer_strangle:
            # Strangle: OTM put + OTM call
            put_strike = round(spy_price * (1 - self._config.strangle_otm_pct), 0)
            call_strike = round(spy_price * (1 + self._config.strangle_otm_pct), 0)
            # Estimated cost: ~$2-4 per strangle on SPY
            est_cost = max(spy_price * 0.006, 0.5) * 100
        else:
            # Straddle: ATM
            put_strike = round(spy_price, 0)
            call_strike = put_strike
            est_cost = max(spy_price * 0.015, 1.0) * 100

        n_contracts = max(1, int(budget / max(est_cost, 1)))

        # Long put
        put_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol="SPY",
            right="P",
            expiry=target_expiry,
            strike=put_strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"{'Strangle' if self._config.prefer_strangle else 'Straddle'} "
                   f"SPY: buy {put_strike}P (VIX={vix:.1f}, FRAG={frag:.2f})",
            metadata={"leg": "long_put", "vix": vix, "frag": frag},
        )

        # Long call
        call_leg = OptionTradeDirective(
            strategy=self.name,
            action=TradeAction.OPEN,
            symbol="SPY",
            right="C",
            expiry=target_expiry,
            strike=call_strike,
            quantity=n_contracts,
            order_type=OrderType.LIMIT,
            reason=f"{'Strangle' if self._config.prefer_strangle else 'Straddle'} "
                   f"SPY: buy {call_strike}C",
            metadata={"leg": "long_call", "vix": vix, "frag": frag},
        )

        put_leg.spread_leg = call_leg
        directives.append(put_leg)

        return directives


# ── Wheel Strategy ───────────────────────────────────────────────────

class WheelStrategy(OptionStrategy):
    """Systematic wheel: sell CSP → assignment → covered call → repeat.

    Phase 1: Sell cash-secured puts on high-conviction names.
    Phase 2: If assigned, immediately write covered calls.
    Phase 3: If called away, restart with CSP.
    Tracks wheel state per symbol in metadata.
    """

    def __init__(
        self,
        config: Optional[WheelConfig] = None,
        discovery: Any = None,
    ) -> None:
        self._config = config or WheelConfig()
        self._discovery = discovery

    @property
    def name(self) -> str:
        return "wheel"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        nav = signals.get("nav", 0.0)
        signals.get("buying_power", nav)
        lambda_scores: Dict[str, float] = signals.get("lambda_scores", {})
        stab_scores: Dict[str, float] = signals.get("stab_scores", {})
        equity_prices: Dict[str, float] = signals.get("equity_prices", {})

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        # Track existing wheel positions
        existing_wheel = [
            opt for opt in existing_options
            if opt.get("strategy") == self.name
        ]
        wheel_symbols: set = set()

        for opt in existing_wheel:
            wheel_symbols.add(opt["symbol"])
            dte = ProtectivePutStrategy._days_to_expiry(opt["expiry"], today)
            entry_price = opt.get("entry_price", 0)
            current_price = opt.get("current_price", entry_price)

            # Profit target on existing options
            if entry_price > 0 and current_price > 0:
                profit_pct = (entry_price - current_price) / max(entry_price, 0.01)
                if profit_pct >= self._config.profit_target:
                    directives.append(OptionTradeDirective(
                        strategy=self.name,
                        action=TradeAction.CLOSE,
                        symbol=opt["symbol"],
                        right=opt.get("right", "P"),
                        expiry=opt["expiry"],
                        strike=opt["strike"],
                        quantity=-opt["quantity"],
                        reason=f"Wheel profit target {profit_pct:.0%} on {opt['symbol']}",
                    ))
                    continue

            # Roll at low DTE
            if dte <= self._config.roll_dte:
                directives.append(OptionTradeDirective(
                    strategy=self.name,
                    action=TradeAction.ROLL,
                    symbol=opt["symbol"],
                    right=opt.get("right", "P"),
                    expiry=opt["expiry"],
                    strike=opt["strike"],
                    quantity=-opt["quantity"],
                    reason=f"Wheel roll {opt['symbol']}: {dte} DTE",
                ))

        # Phase 2: Check for assignments — if we hold shares from a
        # wheel symbol, write covered calls
        for iid, pos in portfolio.items():
            symbol = iid.split(".")[0] if "." in iid else iid
            if pos.quantity < 100:
                continue

            # Check if this symbol is a wheel candidate (was assigned)
            # by looking for metadata or matching lambda/stab criteria
            wheel_assigned = symbol in wheel_symbols
            if not wheel_assigned:
                # Also check if this is a name we'd wheel on
                lam = lambda_scores.get(symbol, 0.0)
                stab = stab_scores.get(symbol, 0.0)
                if lam < self._config.min_lambda_score or stab < self._config.min_stab_score:
                    continue

            # Check if we already have a CC on this symbol
            has_cc = any(
                opt["symbol"] == symbol and opt.get("right") == "C"
                for opt in existing_wheel
            )
            if has_cc:
                continue

            price = pos.market_value / max(pos.quantity, 1)
            if price <= 0:
                continue

            n_contracts = pos.quantity // 100
            if n_contracts < 1:
                continue

            # Write covered call
            call_strike = round(price * 1.06, 1)
            expiry = self._find_expiry(symbol, today)
            if expiry is None:
                continue

            directives.append(OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="C",
                expiry=expiry,
                strike=call_strike,
                quantity=-n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Wheel CC {symbol}: sell {call_strike}C "
                       f"({pos.quantity:.0f} shares assigned)",
                metadata={"wheel_phase": "covered_call"},
            ))

        # Phase 1: Sell new CSPs on high-conviction names
        if len(wheel_symbols) >= self._config.max_positions or nav <= 0:
            return directives

        max_capital_per = nav * self._config.max_nav_pct_per_position

        candidates: List[tuple] = []
        for symbol, lam_score in lambda_scores.items():
            if symbol in wheel_symbols:
                continue
            stab = stab_scores.get(symbol, 0.0)
            if lam_score < self._config.min_lambda_score:
                continue
            if stab < self._config.min_stab_score:
                continue
            price = equity_prices.get(symbol, 0.0)
            if price <= 0:
                continue
            candidates.append((symbol, lam_score + stab, price))

        candidates.sort(key=lambda x: x[1], reverse=True)
        slots = self._config.max_positions - len(wheel_symbols)

        for symbol, score, price in candidates[:slots]:
            # CSP strike: ~0.28 delta ≈ ~6-7% OTM
            strike = round(price * 0.93, 1)

            expiry = self._find_expiry(symbol, today)
            if expiry is None:
                continue

            # Size: max capital / (strike * 100)
            notional = strike * 100
            n_contracts = max(1, int(max_capital_per / max(notional, 1)))

            directives.append(OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol=symbol,
                right="P",
                expiry=expiry,
                strike=strike,
                quantity=-n_contracts,
                order_type=OrderType.LIMIT,
                reason=f"Wheel CSP {symbol}: sell {strike}P x{n_contracts} "
                       f"(lambda={score:.2f})",
                metadata={"wheel_phase": "csp", "lambda_score": score},
            ))

        return directives

    def _find_expiry(self, symbol: str, as_of: Optional[date] = None) -> Optional[str]:
        if self._discovery is not None:
            exp = self._discovery.get_best_expiry(
                symbol,
                min_dte=self._config.target_dte_min,
                max_dte=self._config.target_dte_max,
            )
            if exp:
                return exp
        return ProtectivePutStrategy._find_target_expiry(
            as_of or date.today(),
            self._config.target_dte_min,
            self._config.target_dte_max,
        )


# ── Strategy Manager ───────────────────────────────────────────────────────

class OptionsStrategyManager:
    """Orchestrates all option and derivatives strategies.

    Collects directives from each strategy, converts them to Orders,
    and submits them via the broker (with optional dry-run).

    Parameters
    ----------
    broker : BrokerInterface
        For order submission.
    mapper : InstrumentMapper
        For contract building and instrument_id generation.
    discovery : ContractDiscoveryService, optional
        Wired into strategies that need live chain data.
    strategies : list, optional
        Override the default strategy set.
    dry_run : bool
        If True, log directives without submitting orders.
    """

    def __init__(
        self,
        broker: BrokerInterface,
        mapper: InstrumentMapper,
        discovery: Any = None,
        strategies: Optional[List[OptionStrategy]] = None,
        dry_run: bool = False,
    ) -> None:
        self._broker = broker
        self._mapper = mapper
        self._discovery = discovery
        self._dry_run = dry_run

        if strategies is not None:
            self._strategies = strategies
        else:
            self._strategies = self._build_default_strategies(discovery)

        # Name → strategy lookup for allocator integration
        self._strategy_map: Dict[str, OptionStrategy] = {
            s.name: s for s in self._strategies
        }

    @staticmethod
    def _build_default_strategies(
        discovery: Any = None,
    ) -> List[OptionStrategy]:
        """Construct the full set of 15 strategies."""
        return [
            # Original 7
            ProtectivePutStrategy(),
            CoveredCallStrategy(),
            SectorPutSpreadStrategy(),
            VixTailHedgeStrategy(),
            ShortPutStrategy(discovery=discovery),
            FuturesOverlayStrategy(discovery=discovery),
            FuturesOptionStrategy(discovery=discovery),
            # New 8
            BullCallSpreadStrategy(discovery=discovery),
            LEAPSStrategy(discovery=discovery),
            IronCondorStrategy(),
            IronButterflyStrategy(),
            CollarStrategy(discovery=discovery),
            CalendarSpreadStrategy(),
            StraddleStrangleStrategy(),
            WheelStrategy(discovery=discovery),
        ]

    def apply_allocations(
        self,
        allocations: Dict[str, Any],
    ) -> None:
        """Enable/disable strategies based on allocator directives.

        Parameters
        ----------
        allocations : dict
            strategy_name → AllocationDirective (from StrategyAllocator).
        """
        for strat_name, alloc in allocations.items():
            strategy = self._strategy_map.get(strat_name)
            if strategy is None:
                continue
            # Each strategy stores its config as _config with an enabled field
            if hasattr(strategy, "_config") and hasattr(strategy._config, "enabled"):
                strategy._config.enabled = alloc.enabled

        enabled = [s.name for s in self._strategies
                   if hasattr(s, "_config") and getattr(s._config, "enabled", True)]
        logger.info(
            "OptionsStrategyManager: %d/%d strategies enabled: %s",
            len(enabled), len(self._strategies), ", ".join(enabled),
        )

    def evaluate_all(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: Optional[List[Dict[str, Any]]] = None,
        allocations: Optional[Dict[str, Any]] = None,
    ) -> List[OptionTradeDirective]:
        """Run all strategies and return combined directives.

        Parameters
        ----------
        portfolio : dict
            Current equity positions.
        signals : dict
            Market signals.
        existing_options : list, optional
            Open option positions.
        allocations : dict, optional
            Strategy allocations from StrategyAllocator.  If provided,
            strategies are enabled/disabled before evaluation.
        """
        if allocations is not None:
            self.apply_allocations(allocations)

        if existing_options is None:
            existing_options = []

        all_directives: List[OptionTradeDirective] = []

        # Sort strategies by priority if allocations are provided
        strategies = self._strategies
        if allocations:
            strategies = sorted(
                self._strategies,
                key=lambda s: getattr(allocations.get(s.name), "priority", 0),
                reverse=True,
            )

        for strategy in strategies:
            try:
                directives = strategy.evaluate(
                    portfolio, signals, existing_options,
                )
                if directives:
                    logger.info(
                        "Strategy %s: %d directives",
                        strategy.name, len(directives),
                    )
                all_directives.extend(directives)
            except Exception as exc:
                logger.error(
                    "Strategy %s failed: %s", strategy.name, exc,
                    exc_info=True,
                )

        if self._dry_run:
            for d in all_directives:
                logger.info(
                    "[DRY RUN] %s %s %s %s %.1f x%d — %s",
                    d.strategy, d.action.value, d.symbol, d.right,
                    d.strike, d.quantity, d.reason,
                )
        else:
            self._submit_directives(all_directives)

        return all_directives

    def _submit_directives(
        self,
        directives: List[OptionTradeDirective],
    ) -> None:
        """Convert directives to Orders and submit."""
        from apathis.core.ids import generate_uuid

        for directive in directives:
            if directive.action == TradeAction.HOLD:
                continue

            try:
                instrument_id = InstrumentMapper.option_instrument_id(
                    directive.symbol,
                    directive.expiry,
                    directive.strike,
                    directive.right,
                )

                side = OrderSide.BUY if directive.quantity > 0 else OrderSide.SELL

                order = Order(
                    order_id=generate_uuid(),
                    instrument_id=instrument_id,
                    side=side,
                    order_type=directive.order_type,
                    quantity=abs(directive.quantity),
                    limit_price=directive.limit_price,
                    metadata={
                        "strategy": directive.strategy,
                        "action": directive.action.value,
                        "reason": directive.reason,
                        **directive.metadata,
                    },
                )

                logger.info(
                    "Submitting option order: %s %s %s x%d (%s)",
                    side.value, instrument_id,
                    directive.order_type.value, abs(directive.quantity),
                    directive.reason,
                )

                self._broker.submit_order(order)

                # Handle spread legs
                if directive.spread_leg is not None:
                    self._submit_directives([directive.spread_leg])

            except Exception as exc:
                logger.error(
                    "Failed to submit directive %s %s: %s",
                    directive.strategy, directive.symbol, exc,
                    exc_info=True,
                )


__all__ = [
    # Configs
    "ProtectivePutConfig",
    "CoveredCallConfig",
    "SectorPutSpreadConfig",
    "VixTailHedgeConfig",
    "ShortPutConfig",
    "FuturesOverlayConfig",
    "FuturesOptionConfig",
    "BullCallSpreadConfig",
    "LEAPSConfig",
    "IronCondorConfig",
    "IronButterflyConfig",
    "CollarConfig",
    "CalendarSpreadConfig",
    "StraddleStrangleConfig",
    "WheelConfig",
    # Core types
    "TradeAction",
    "OptionTradeDirective",
    "OptionStrategy",
    # Strategies
    "ProtectivePutStrategy",
    "CoveredCallStrategy",
    "SectorPutSpreadStrategy",
    "VixTailHedgeStrategy",
    "ShortPutStrategy",
    "FuturesOverlayStrategy",
    "FuturesOptionStrategy",
    "BullCallSpreadStrategy",
    "LEAPSStrategy",
    "IronCondorStrategy",
    "IronButterflyStrategy",
    "CollarStrategy",
    "CalendarSpreadStrategy",
    "StraddleStrangleStrategy",
    "WheelStrategy",
    # Manager
    "OptionsStrategyManager",
]
