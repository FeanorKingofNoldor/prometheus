"""Prometheus v2 – Options & Derivatives Strategy Layer.

Translates high-level allocator directives into concrete option trades
submitted via the broker.

Strategies (legacy pre-cutover shadow/live set)
-----------------------------------------------
1. **Protective Puts** – Buy SPY puts when MHI drops below threshold.
2. **Covered Calls** – Sell calls on largest equity positions in
   RISK_ON/NEUTRAL states.
3. **Sector Put Spreads** – Buy put spreads on sector ETFs when sector
   SHI is in the "reduce" zone (below reduce, above kill).
4. **VIX Tail Hedge** – Always-on OTM VIX calls as catastrophe insurance.
5. **Iron Condor / Iron Butterfly** – Short-premium when vol is rich.
6. **Crisis Alpha** – Offensive puts during broad sector deterioration.

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

from apatheon.core.logging import get_logger

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
    coverage_ratio: float = 0.50       # Cover up to 50% of position
    min_position_days: int = 5         # Only on positions held > 5 days
    profit_target: float = 0.80        # Buy back at 80% profit
    roll_dte: int = 14
    min_position_shares: int = 100     # Must hold at least 100 shares
    min_vix_for_entry: float = 22.0    # Only sell calls when VIX >= 22 (high-premium only)


@dataclass
class SectorPutSpreadConfig:
    """Configuration for sector put spread strategy.

    Tuned based on root-cause analysis (docs/sector_put_vs_sh_analysis.md):
    - Wider activation (0.30 vs 0.25) to hedge earlier before IV spikes
    - No floor threshold — hedge even when SHI is very low (allocator
      liquidation + put payoff are complementary, not exclusive)
    - Wider spreads (15%) to capture more tail
    - OTM long strike (3% OTM) to reduce premium cost
    - Larger position size (3% NAV) for meaningful hedge notional
    """
    enabled: bool = True
    shi_reduce_threshold: float = 0.30  # SHI below this triggers spread (was 0.25)
    shi_kill_threshold: float = 0.0     # Disabled — always hedge if SHI < reduce (was 0.15)
    spread_width_pct: float = 0.15      # 15% between long and short strikes (was 7%)
    otm_pct: float = 0.03              # Long strike 3% OTM to reduce premium (was ATM)
    target_dte_min: int = 30
    target_dte_max: int = 60
    max_nav_pct: float = 0.03           # 3% of NAV per sector hedge (was 1%)
    max_total_nav_pct: float = 0.20     # Total across all sectors capped at 20% NAV


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
    nav_pct: float = 0.03             # 3% of NAV risk budget (was 4%)
    profit_target: float = 0.50
    max_loss_multiple: float = 2.0
    max_positions: int = 3            # Max 3 concurrent (was 5)


@dataclass
class IronButterflyConfig:
    """Configuration for iron butterfly strategy."""
    enabled: bool = True
    underlying: str = "SPY"
    max_vix: float = 20.0
    max_frag: float = 0.20
    wing_width: float = 10.0          # $10 wide wings
    target_dte_min: int = 30
    target_dte_max: int = 45
    nav_pct: float = 0.05             # 5% of NAV per position (was 20% — way too much)
    profit_target: float = 0.50
    max_loss_multiple: float = 2.0
    max_positions: int = 2            # Max 2 simultaneous (was 6 — 120% exposure)










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
        from apatheon.sector.health import SECTOR_NAME_TO_ETF

        directives: List[OptionTradeDirective] = []
        today = signals.get("as_of_date", date.today())

        existing_sectors = {
            opt.get("sector"): opt for opt in existing_options
            if opt.get("strategy") == self.name
        }

        # Track total sector hedge allocation to enforce portfolio-level cap.
        total_sector_hedge_cost = 0.0

        for sector_name, shi in sector_shi.items():
            etf_id = SECTOR_NAME_TO_ETF.get(sector_name)
            if not etf_id:
                continue
            etf_symbol = etf_id.replace(".US", "")

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

            # No floor threshold — hedge even when SHI is very low.
            # The put spread payoff is complementary to the allocator's
            # equity liquidation (you still benefit from further downside).
            if self._config.shi_kill_threshold > 0 and shi < self._config.shi_kill_threshold:
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

            # OTM long put (reduces premium vs ATM)
            otm_pct = getattr(self._config, "otm_pct", 0.0)
            long_strike = round(etf_price * (1 - otm_pct), 0)
            # Short put: spread_width_pct below the long strike
            short_strike = round(etf_price * (1 - otm_pct - self._config.spread_width_pct), 0)

            # Size: hedge the full sector exposure, capped by max_nav_pct
            n_contracts = max(1, int(sector_exposure / (etf_price * 100)))

            # Cap sizing: per-sector and total portfolio caps
            spread_cost_per_contract = (long_strike - short_strike) * 100
            if nav > 0 and self._config.max_nav_pct > 0 and spread_cost_per_contract > 0:
                per_sector_budget = nav * self._config.max_nav_pct
                max_contracts = max(1, int(per_sector_budget / spread_cost_per_contract))
                n_contracts = min(n_contracts, max_contracts)

                # Enforce total portfolio hedge cap across all sectors
                max_total = getattr(self._config, "max_total_nav_pct", 1.0)
                remaining_budget = (nav * max_total) - total_sector_hedge_cost
                if remaining_budget <= 0:
                    continue  # Total hedge budget exhausted
                max_from_total = max(1, int(remaining_budget / spread_cost_per_contract))
                n_contracts = min(n_contracts, max_from_total)
                total_sector_hedge_cost += n_contracts * spread_cost_per_contract

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

            # Emergency exit FIRST — regime hostile takes priority over profit/stop
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
                continue

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

        # No new positions when regime is hostile or VIX floor not met
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










# ── Crisis Alpha Strategy ──────────────────────────────────────────────────


@dataclass
class CrisisAlphaStrategyConfig:
    """Configuration for crisis alpha strategy (offensive puts during crises).

    Two trigger modes:
    1. SUSTAINED: ≥5 sectors SHI<0.25 for 3+ days → 7% NAV in SPY puts
    2. FLASH: ≥5 sectors drop SHI >0.10 in one day → 10% NAV (instant)

    Backtested 2007-2024: 7 trades, 57% win, +48% NAV, +88% ROI.
    """
    enabled: bool = True
    shi_threshold: float = 0.25
    sustained_count: int = 5
    sustained_days: int = 3
    sustained_nav_pct: float = 0.07
    flash_count: int = 5
    flash_drop: float = 0.10
    flash_min_sick: int = 3
    flash_nav_pct: float = 0.10
    otm_pct: float = 0.05
    target_dte_min: int = 45
    target_dte_max: int = 60
    profit_target: float = 2.5
    min_hold_days: int = 10
    cooldown_days: int = 30


class CrisisAlphaStrategy(OptionStrategy):
    """Buy SPY puts when multiple sectors deteriorate simultaneously.

    This is an OFFENSIVE strategy that profits from market declines,
    not a hedge. Uses the sector health system as a directional signal.
    """

    def __init__(self, config: CrisisAlphaStrategyConfig | None = None) -> None:
        self._config = config or CrisisAlphaStrategyConfig()
        self._prev_sector_shi: Dict[str, float] = {}
        self._consecutive_sick_days: int = 0
        self._last_close_date: date | None = None

    @property
    def name(self) -> str:
        return "crisis_alpha"

    def evaluate(
        self,
        portfolio: Dict[str, Position],
        signals: Dict[str, Any],
        existing_options: List[Dict[str, Any]],
    ) -> List[OptionTradeDirective]:
        if not self._config.enabled:
            return []

        sector_shi: Dict[str, float] = signals.get("sector_shi", {})
        if not sector_shi:
            return []

        nav = signals.get("nav", 0.0)
        today = signals.get("as_of_date", date.today())
        spy_price = signals.get("spy_price", 0.0)
        if spy_price <= 0 or nav <= 0:
            return []

        cfg = self._config
        directives: List[OptionTradeDirective] = []

        # Count sick sectors
        n_sick = sum(1 for s in sector_shi.values() if s < cfg.shi_threshold)

        # Update consecutive sick days
        if n_sick >= cfg.sustained_count:
            self._consecutive_sick_days += 1
        else:
            self._consecutive_sick_days = 0

        # Flash detection: sharp single-day multi-sector SHI drop
        flash_drops = 0
        if self._prev_sector_shi:
            for sector in sector_shi:
                prev = self._prev_sector_shi.get(sector, 1.0)
                curr = sector_shi[sector]
                if prev - curr > cfg.flash_drop:
                    flash_drops += 1
        self._prev_sector_shi = dict(sector_shi)

        is_flash = flash_drops >= cfg.flash_count and n_sick >= cfg.flash_min_sick
        is_sustained = self._consecutive_sick_days >= cfg.sustained_days

        # Check if we already have a crisis position
        has_position = any(
            opt.get("strategy") == self.name for opt in existing_options
        )

        # Cooldown check
        in_cooldown = (
            self._last_close_date is not None
            and (today - self._last_close_date).days < cfg.cooldown_days
        )

        # EXIT: close when crisis subsides
        if has_position and n_sick < 2:
            # Check min hold
            for opt in existing_options:
                if opt.get("strategy") == self.name:
                    open_date = opt.get("open_date", today)
                    if isinstance(open_date, str):
                        open_date = date.fromisoformat(open_date)
                    held = (today - open_date).days
                    if held >= cfg.min_hold_days:
                        directives.append(OptionTradeDirective(
                            strategy=self.name,
                            action=TradeAction.CLOSE,
                            symbol="SPY",
                            right="P",
                            expiry=opt.get("expiry", today),
                            strike=opt.get("strike", 0),
                            quantity=-opt.get("quantity", 0),
                            reason=f"CRISIS ALPHA EXIT: {n_sick} sectors sick, held {held}d",
                        ))
                        self._last_close_date = today
            return directives

        # ENTER: open new position
        if not has_position and not in_cooldown and (is_flash or is_sustained):
            trigger = "FLASH" if is_flash else "SUSTAINED"
            alloc = cfg.flash_nav_pct if is_flash else cfg.sustained_nav_pct
            budget = nav * alloc

            strike = round(spy_price * (1 - cfg.otm_pct))
            est_premium_per = strike * 0.035 * 100  # ~3.5% for 5% OTM, 45-60 DTE
            n_contracts = max(1, int(budget / est_premium_per)) if est_premium_per > 0 else 1

            target_expiry = ProtectivePutStrategy._find_target_expiry(
                today, cfg.target_dte_min, cfg.target_dte_max,
            )

            sick_names = sorted(s for s, v in sector_shi.items() if v < cfg.shi_threshold)

            directives.append(OptionTradeDirective(
                strategy=self.name,
                action=TradeAction.OPEN,
                symbol="SPY",
                right="P",
                expiry=target_expiry,
                strike=strike,
                quantity=n_contracts,
                order_type=OrderType.LIMIT,
                reason=(
                    f"CRISIS ALPHA [{trigger}]: {n_sick} sectors sick "
                    f"({', '.join(sick_names[:4])}). "
                    f"{n_contracts} SPY puts @ {strike} ({alloc:.0%} NAV)"
                ),
                metadata={
                    "trigger": trigger,
                    "sick_count": n_sick,
                    "sick_sectors": sick_names,
                    "nav_pct": alloc,
                    "flash_drops": flash_drops,
                    "consecutive_days": self._consecutive_sick_days,
                },
            ))

            logger.info(
                "CRISIS ALPHA %s: %d sectors sick, buying %d SPY puts @ %d (%.0f%% NAV = $%.0f)",
                trigger, n_sick, n_contracts, strike, alloc * 100, budget,
            )

        return directives


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
        submission_recorder: Optional[Any] = None,
    ) -> None:
        self._broker = broker
        self._mapper = mapper
        self._discovery = discovery
        self._dry_run = dry_run
        # Optional callback ``(directive, instrument_id, order_id) -> None``
        # invoked after each successful submission — used to persist
        # strategy provenance so positions coming back from the broker
        # can be re-tagged (see options_storage.record_order_submission).
        # Must never raise into the submission path; failures are logged.
        self._submission_recorder = submission_recorder
        # Failure descriptions from the most recent _submit_directives
        # call (for callers that use evaluate_all's internal submission).
        self.last_submission_failures: List[str] = []

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
        """Construct the live set of legacy (shadow) strategies."""
        return [
            ProtectivePutStrategy(),
            CoveredCallStrategy(),
            SectorPutSpreadStrategy(),
            VixTailHedgeStrategy(),
            IronCondorStrategy(),
            IronButterflyStrategy(),
            # Crisis alpha: offensive puts during broad sector deterioration
            CrisisAlphaStrategy(),
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

        # Book-level long-debit cap: sum existing + proposed long-debit
        # gross premium; drop OPEN directives that push total past the
        # legacy derivatives budget (NAV × 30%). Prevents the failure mode
        # where many uncoordinated long-debit strategies pile into the same
        # underlying — observed 2026-06-05 with 73% NAV stacked on SPY 7/17.
        all_directives = self._apply_long_debit_cap(
            all_directives, existing_options, signals,
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

    @staticmethod
    def _apply_long_debit_cap(
        directives: List[OptionTradeDirective],
        existing_options: List[Dict[str, Any]],
        signals: Dict[str, Any],
        cap_pct: float = 0.30,
    ) -> List[OptionTradeDirective]:
        """Drop OPEN long-debit directives that push the book past cap.

        The cap is on **gross long premium paid** (max loss on long
        options). Existing long positions count against the cap; CLOSE/
        ROLL directives pass through unchecked; OPEN directives that fit
        are kept in arrival order, the rest are logged and dropped.
        """
        nav = float(signals.get("nav", 0.0) or 0.0)
        if nav <= 0:
            return directives  # no NAV → can't enforce, pass through

        cap_usd = nav * cap_pct

        existing_long_premium = 0.0
        for pos in existing_options:
            qty = pos.get("quantity", 0) or 0
            entry_price = pos.get("entry_price", 0.0) or 0.0
            if qty > 0 and entry_price > 0:
                existing_long_premium += entry_price * qty * 100.0

        running = existing_long_premium
        kept: List[OptionTradeDirective] = []
        dropped: List[OptionTradeDirective] = []
        for d in directives:
            if d.action != TradeAction.OPEN or d.quantity <= 0:
                kept.append(d)
                continue
            price = d.limit_price
            if price is None or price <= 0:
                # No usable price → can't size; let the broker layer
                # reject if needed.
                kept.append(d)
                continue
            this_premium = float(price) * int(d.quantity) * 100.0
            if running + this_premium > cap_usd:
                dropped.append(d)
                continue
            running += this_premium
            kept.append(d)

        if dropped:
            logger.warning(
                "Long-debit cap dropped %d OPEN directive(s): "
                "existing=$%.0f, cap=$%.0f (%.0f%% of NAV $%.0f)",
                len(dropped), existing_long_premium, cap_usd,
                cap_pct * 100, nav,
            )
            for d in dropped:
                logger.warning(
                    "  dropped %s %s %s %s strike=%.1f qty=%d @ $%.2f",
                    d.strategy, d.action.value, d.symbol, d.right,
                    d.strike, d.quantity, d.limit_price or 0.0,
                )
        return kept

    @staticmethod
    def _describe_directive(directive: OptionTradeDirective) -> str:
        return (
            f"{directive.strategy} {directive.action.value} {directive.symbol} "
            f"{directive.right}{directive.strike:g} {directive.expiry} "
            f"x{directive.quantity}"
        )

    def _submit_directives(
        self,
        directives: List[OptionTradeDirective],
    ) -> List[str]:
        """Convert directives to Orders and submit.

        Returns a list of human-readable failure descriptions so the
        caller can surface them (run_derivatives_daily threads them into
        the run summary's ``warnings``). An empty list means every leg
        went out.

        Multi-leg spreads (chained via ``spread_leg``) are submitted
        parent-first; if a chained leg fails AFTER earlier legs were
        submitted, the already-submitted legs are cancelled
        (best-effort) so no unpaired leg — e.g. a naked short — is left
        working overnight. Every leg failure and every cancel outcome is
        recorded loudly; nothing fails silently.
        """
        failures: List[str] = []
        for directive in directives:
            if directive.action == TradeAction.HOLD:
                continue

            ok, parent_order_id = self._submit_single(directive, failures)
            if not ok:
                if directive.spread_leg is not None:
                    msg = (
                        f"spread aborted: parent leg "
                        f"{self._describe_directive(directive)} failed — "
                        f"chained leg(s) NOT submitted"
                    )
                    logger.error(msg)
                    failures.append(msg)
                continue

            # Submit chained legs; unwind on failure.
            submitted: List[tuple] = [(directive, parent_order_id)]
            leg = directive.spread_leg
            while leg is not None:
                if leg.action == TradeAction.HOLD:
                    leg = leg.spread_leg
                    continue
                ok, leg_order_id = self._submit_single(leg, failures)
                if not ok:
                    self._cancel_submitted_legs(
                        submitted, failed_leg=leg, failures=failures,
                    )
                    break
                submitted.append((leg, leg_order_id))
                leg = leg.spread_leg

        self.last_submission_failures = failures
        return failures

    def _submit_single(
        self,
        directive: OptionTradeDirective,
        failures: List[str],
    ) -> "tuple[bool, Optional[str]]":
        """Submit one directive. Returns ``(ok, order_id)``.

        On failure the error is logged AND appended to ``failures`` —
        never swallowed silently.
        """
        from apatheon.core.ids import generate_uuid

        try:
            # Commodity FOPs (CL/BZ/NG/ZW/GC/HG) take the .FOP
            # instrument_id format; equity options take .US.
            from prometheus.execution.futures_option_specs import (
                is_commodity_fop_symbol,
            )
            if is_commodity_fop_symbol(directive.symbol):
                instrument_id = InstrumentMapper.futures_option_instrument_id(
                    directive.symbol,
                    directive.expiry,
                    directive.strike,
                    directive.right,
                )
            else:
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

            broker_order_id = self._broker.submit_order(order)
            order_id = str(broker_order_id) if broker_order_id is not None else None

        except Exception as exc:
            logger.error(
                "Failed to submit directive %s %s: %s",
                directive.strategy, directive.symbol, exc,
                exc_info=True,
            )
            failures.append(
                f"submit failed: {self._describe_directive(directive)}: {exc}"
            )
            return False, None

        # Persist strategy provenance (defect: positions came back from
        # IBKR untagged, blinding every tag-filtered check). The order is
        # already out — a recorder failure must not look like a
        # submission failure.
        if self._submission_recorder is not None:
            try:
                self._submission_recorder(directive, instrument_id, order_id)
            except Exception as exc:
                logger.error(
                    "submission recorder failed for %s (order submitted; "
                    "provenance NOT persisted): %s",
                    instrument_id, exc, exc_info=True,
                )

        return True, order_id

    def _cancel_submitted_legs(
        self,
        submitted: List[tuple],
        *,
        failed_leg: OptionTradeDirective,
        failures: List[str],
    ) -> None:
        """A chained leg failed after earlier legs went out — cancel the
        already-submitted legs so no unpaired (possibly naked short) leg
        is left working. Warn loudly either way."""
        for prev, prev_order_id in reversed(submitted):
            desc = self._describe_directive(prev)
            cancelled = False
            cancel_err: Optional[str] = None
            if prev_order_id:
                try:
                    cancelled = bool(self._broker.cancel_order(prev_order_id))
                except Exception as exc:
                    cancel_err = str(exc)
            if cancelled:
                msg = (
                    f"NAKED-LEG GUARD: leg "
                    f"{self._describe_directive(failed_leg)} failed after "
                    f"{desc} was submitted — cancel requested for "
                    f"order_id={prev_order_id}"
                )
            else:
                msg = (
                    f"NAKED-LEG RISK: leg "
                    f"{self._describe_directive(failed_leg)} failed after "
                    f"{desc} was submitted and the cancel attempt FAILED "
                    f"(order_id={prev_order_id}"
                    + (f", error: {cancel_err}" if cancel_err else "")
                    + ") — an unpaired leg may be working; "
                    "MANUAL INTERVENTION REQUIRED"
                )
            logger.error(msg)
            failures.append(msg)


__all__ = [
    # Configs
    "ProtectivePutConfig",
    "CoveredCallConfig",
    "SectorPutSpreadConfig",
    "VixTailHedgeConfig",
    "IronCondorConfig",
    "IronButterflyConfig",
    # Core types
    "TradeAction",
    "OptionTradeDirective",
    "OptionStrategy",
    # Strategies
    "ProtectivePutStrategy",
    "CoveredCallStrategy",
    "SectorPutSpreadStrategy",
    "VixTailHedgeStrategy",
    "IronCondorStrategy",
    "IronButterflyStrategy",
    # Manager
    "OptionsStrategyManager",
]
