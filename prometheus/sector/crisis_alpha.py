"""Prometheus – Crisis Alpha Strategy.

Generates OFFENSIVE short positions (via puts or inverse ETFs) when the
sector health system detects broad deterioration. This is NOT hedging —
it's a directional alpha strategy that profits from market declines.

Signal: When ≥N sectors have SHI below threshold simultaneously, the
signal fires. Historical edge (2007-2024):
- ≥5 sectors SHI<0.25: SPY avg -9.1% in 21d, 71% win rate
- ≥7 sectors SHI<0.25: SPY avg -9.3% in 21d, 73% win rate

The key insight: SINGLE sector triggers are noise (mean-reverts).
MULTIPLE sector triggers are real crises (persistent trend).

Position structure:
- Buy outright puts on SPY (not spreads — we want UNLIMITED downside capture)
- Size: scale with conviction (more sick sectors → bigger position)
- Duration: 45-60 DTE, hold through the crisis
- Exit: take profit at 2x premium, or when sick count drops below threshold

Why outright puts instead of SH.US:
- Convexity: puts appreciate non-linearly as market drops
- Leverage: $1 of premium controls $100+ of exposure
- Limited risk: max loss = premium paid
- Vol expansion: puts gain from IV rising during crisis
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Dict, List, Optional, Tuple

from apathis.core.logging import get_logger

logger = get_logger(__name__)


class CrisisSignal(str, Enum):
    """Crisis alpha signal strength."""
    NONE = "NONE"
    WATCH = "WATCH"           # 3+ sectors sick — monitor
    ENGAGE = "ENGAGE"         # 5+ sectors sick — open positions
    FULL_CRISIS = "FULL_CRISIS"  # 7+ sectors sick — max size


@dataclass
class CrisisAlphaConfig:
    """Configuration for crisis alpha strategy.

    Tuned from 2007-2024 backtest:
    - ≥5 sick sectors at SHI<0.25 → 71% win rate, avg -9.1% SPY in 21d
    - ≥7 sick sectors → 73% win rate, avg -9.3% in 21d
    """
    # SHI threshold for counting "sick" sectors
    shi_threshold: float = 0.25

    # Sector count thresholds for signal levels
    watch_count: int = 3          # Start monitoring
    engage_count: int = 5         # Open positions
    full_crisis_count: int = 7    # Maximum conviction

    # Position sizing (fraction of NAV allocated to crisis puts)
    engage_nav_pct: float = 0.05       # 5% of NAV at ENGAGE
    full_crisis_nav_pct: float = 0.10  # 10% of NAV at FULL_CRISIS

    # Put parameters
    target_dte_min: int = 45
    target_dte_max: int = 60
    otm_pct: float = 0.05         # 5% OTM strikes (cheaper, more convex)
    profit_target_multiple: float = 2.0  # Take profit at 2x premium paid
    stop_loss_pct: float = 0.80   # Cut loss if put loses 80% of premium

    # Exit: close when sick count drops below this
    exit_sick_count: int = 2

    # Instrument: SPY puts for broadest market exposure
    underlying: str = "SPY"


@dataclass
class CrisisAlphaSignalResult:
    """Output of crisis signal evaluation."""
    signal: CrisisSignal
    sick_count: int
    sick_sectors: List[str]
    sector_scores: Dict[str, float]
    target_nav_pct: float
    as_of_date: date


def evaluate_crisis_signal(
    sector_scores: Dict[str, float],
    as_of_date: date,
    config: CrisisAlphaConfig | None = None,
) -> CrisisAlphaSignalResult:
    """Evaluate the crisis alpha signal from sector health scores.

    Parameters
    ----------
    sector_scores : dict
        sector_name → SHI score for the current date
    as_of_date : date
        Current evaluation date
    config : CrisisAlphaConfig, optional
        Strategy configuration

    Returns
    -------
    CrisisAlphaSignalResult with signal strength and sizing
    """
    if config is None:
        config = CrisisAlphaConfig()

    sick = [s for s, score in sector_scores.items() if score < config.shi_threshold]
    n_sick = len(sick)

    if n_sick >= config.full_crisis_count:
        signal = CrisisSignal.FULL_CRISIS
        nav_pct = config.full_crisis_nav_pct
    elif n_sick >= config.engage_count:
        signal = CrisisSignal.ENGAGE
        nav_pct = config.engage_nav_pct
    elif n_sick >= config.watch_count:
        signal = CrisisSignal.WATCH
        nav_pct = 0.0  # Watch only, no position
    else:
        signal = CrisisSignal.NONE
        nav_pct = 0.0

    return CrisisAlphaSignalResult(
        signal=signal,
        sick_count=n_sick,
        sick_sectors=sorted(sick),
        sector_scores=sector_scores,
        target_nav_pct=nav_pct,
        as_of_date=as_of_date,
    )


def generate_crisis_trades(
    signal_result: CrisisAlphaSignalResult,
    current_spy_price: float,
    nav: float,
    existing_crisis_position: bool = False,
    config: CrisisAlphaConfig | None = None,
) -> List[Dict]:
    """Generate trade directives for the crisis alpha strategy.

    Returns a list of trade directives (dicts) that can be passed to the
    options execution layer.
    """
    if config is None:
        config = CrisisAlphaConfig()

    trades = []

    if signal_result.signal in (CrisisSignal.ENGAGE, CrisisSignal.FULL_CRISIS):
        if existing_crisis_position:
            # Already positioned — check if we should scale up
            if signal_result.signal == CrisisSignal.FULL_CRISIS:
                logger.info(
                    "Crisis alpha: FULL_CRISIS with existing position — "
                    "consider scaling up to %.1f%% NAV",
                    config.full_crisis_nav_pct * 100,
                )
            return trades  # Hold existing position

        # OPEN new crisis position
        budget = nav * signal_result.target_nav_pct
        strike = round(current_spy_price * (1 - config.otm_pct))

        # Estimate premium (~3-5% of strike for 5% OTM, 45-60 DTE puts)
        est_premium_pct = 0.035  # ~3.5% of strike
        premium_per_share = strike * est_premium_pct
        premium_per_contract = premium_per_share * 100
        n_contracts = max(1, int(budget / premium_per_contract))

        trades.append({
            "strategy": "crisis_alpha",
            "action": "OPEN",
            "symbol": config.underlying,
            "right": "P",
            "strike": strike,
            "quantity": n_contracts,
            "dte_target": (config.target_dte_min + config.target_dte_max) // 2,
            "reason": (
                f"CRISIS ALPHA: {signal_result.sick_count} sectors sick "
                f"({', '.join(signal_result.sick_sectors[:5])}). "
                f"Buying {n_contracts} SPY puts @ {strike} "
                f"(budget ${budget:,.0f}, {signal_result.target_nav_pct:.0%} NAV)"
            ),
            "metadata": {
                "signal": signal_result.signal.value,
                "sick_count": signal_result.sick_count,
                "sick_sectors": signal_result.sick_sectors,
                "nav_pct": signal_result.target_nav_pct,
                "budget": budget,
            },
        })

        logger.info(
            "Crisis alpha ENGAGE: %d sectors sick, buying %d SPY puts @ %d "
            "(%.1f%% NAV = $%,.0f)",
            signal_result.sick_count, n_contracts, strike,
            signal_result.target_nav_pct * 100, budget,
        )

    elif signal_result.signal == CrisisSignal.NONE and existing_crisis_position:
        # Exit signal: sick count dropped below threshold
        if signal_result.sick_count < config.exit_sick_count:
            trades.append({
                "strategy": "crisis_alpha",
                "action": "CLOSE",
                "symbol": config.underlying,
                "right": "P",
                "reason": (
                    f"CRISIS ALPHA EXIT: only {signal_result.sick_count} sectors sick, "
                    f"below exit threshold {config.exit_sick_count}"
                ),
            })
            logger.info(
                "Crisis alpha EXIT: %d sectors sick (below %d threshold)",
                signal_result.sick_count, config.exit_sick_count,
            )

    return trades
