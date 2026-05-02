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

import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List

import yaml

from apatheon.core.logging import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CRISIS_ALPHA_CONFIG_PATH = PROJECT_ROOT / "configs" / "sector" / "crisis_alpha.yaml"


class CrisisSignal(str, Enum):
    """Crisis alpha signal strength."""
    NONE = "NONE"
    WATCH = "WATCH"           # 3+ sectors sick — monitor
    ENGAGE = "ENGAGE"         # 5+ sectors sick — open positions
    FULL_CRISIS = "FULL_CRISIS"  # 7+ sectors sick — max size


@dataclass
class CrisisAlphaConfig:
    """Configuration for crisis alpha strategy.

    Two trigger modes:
    1. SUSTAINED: ≥5 sectors SHI<0.25 for 3+ consecutive days → 7% NAV
    2. FLASH: ≥5 sectors drop SHI by >0.10 in a single day → 10% NAV (instant)

    Backtested 2007-2024: 7 trades, 57% win rate, +48% NAV, +88% ROI.
    Flash trades (Lehman, COVID): 2/2 = 100% win rate.
    """
    # SHI threshold for counting "sick" sectors
    shi_threshold: float = 0.25

    # ── Sustained signal ──────────────────────────────────────────
    sustained_engage_count: int = 5    # sectors sick for sustained
    sustained_days: int = 3            # consecutive days required
    sustained_nav_pct: float = 0.07    # 7% of NAV

    # ── Flash override (mega crisis — instant response) ───────────
    # Fires when N+ sectors experience a sharp SHI drop in a single
    # day AND at least 3 sectors are already sick. Catches events
    # like Lehman (2008-09-15) and COVID (2020-03-09) on day one.
    flash_sector_count: int = 5        # sectors with sharp drop
    flash_drop_threshold: float = 0.10 # min SHI drop per sector
    flash_min_sick: int = 3            # also require N sectors sick
    flash_nav_pct: float = 0.10        # 10% NAV (higher conviction)

    # ── Put parameters ────────────────────────────────────────────
    target_dte_min: int = 45
    target_dte_max: int = 60
    otm_pct: float = 0.05             # 5% OTM (cheaper, more convex)
    profit_target_multiple: float = 2.5 # Take profit at 2.5x premium
    min_hold_days: int = 10            # Don't exit before 10 days

    # ── Exit & risk management ────────────────────────────────────
    exit_sick_count: int = 2           # Close when sick count drops below this
    cooldown_days: int = 30            # Don't re-enter within 30 days

    # Instruments
    underlying: str = "SPY"

    # ── Sector-specific puts (targeted on weakest sectors) ────────
    # Instead of only buying broad SPY puts, also buy puts on the
    # individual sector ETFs with the worst SHI scores. This gives
    # concentrated exposure where the damage is actually happening.
    sector_puts_enabled: bool = True
    sector_puts_max_sectors: int = 3          # target the N weakest sectors
    sector_puts_shi_threshold: float = 0.20   # only sectors below this
    sector_puts_nav_pct_per_sector: float = 0.025  # 2.5% NAV each
    sector_puts_otm_pct: float = 0.05         # 5% OTM


# ── Environment variable mapping for the most critical parameters ────
_CRISIS_ALPHA_ENV_OVERRIDES: Dict[str, tuple[str, type]] = {
    "shi_threshold": ("PROMETHEUS_CRISIS_SHI_THRESHOLD", float),
    "sustained_engage_count": ("PROMETHEUS_CRISIS_SUSTAINED_COUNT", int),
    "sustained_nav_pct": ("PROMETHEUS_CRISIS_SUSTAINED_NAV_PCT", float),
    "flash_nav_pct": ("PROMETHEUS_CRISIS_FLASH_NAV_PCT", float),
    "profit_target_multiple": ("PROMETHEUS_CRISIS_PROFIT_TARGET", float),
    "cooldown_days": ("PROMETHEUS_CRISIS_COOLDOWN_DAYS", int),
    "sector_puts_enabled": ("PROMETHEUS_CRISIS_SECTOR_PUTS_ENABLED", lambda v: v.lower() in ("1", "true", "yes")),
    "sector_puts_max_sectors": ("PROMETHEUS_CRISIS_SECTOR_PUTS_MAX", int),
    "sector_puts_nav_pct_per_sector": ("PROMETHEUS_CRISIS_SECTOR_PUTS_NAV_PCT", float),
}


def load_crisis_alpha_config(
    path: str | Path | None = None,
) -> CrisisAlphaConfig:
    """Load a :class:`CrisisAlphaConfig` from YAML + env overrides.

    Resolution order (last wins):
    1. Dataclass defaults
    2. YAML file at *path* (or ``configs/sector/crisis_alpha.yaml`` if exists)
    3. Environment variable overrides for critical parameters

    If the YAML file does not exist or is malformed, the dataclass defaults
    are used without error.
    """
    cfg_path = Path(path) if path is not None else DEFAULT_CRISIS_ALPHA_CONFIG_PATH
    kwargs: Dict[str, Any] = {}

    # ── Step 1: Load from YAML if available ──────────────────────────
    if cfg_path.exists():
        try:
            raw = yaml.safe_load(cfg_path.read_text())
            if isinstance(raw, dict):
                valid_fields = {f.name for f in CrisisAlphaConfig.__dataclass_fields__.values()}
                for key, value in raw.items():
                    if key in valid_fields and value is not None:
                        kwargs[key] = value
                logger.info("Loaded crisis alpha config from %s (%d fields)", cfg_path, len(kwargs))
        except Exception as exc:
            logger.warning("Failed to load crisis alpha config from %s: %s", cfg_path, exc)
    elif path is not None:
        # Explicit path was given but doesn't exist — warn loudly.
        import sys
        msg = f"WARNING: crisis alpha config file not found: {cfg_path}"
        print(msg, file=sys.stderr)
        logger.warning(msg)

    # ── Step 2: Environment variable overrides ───────────────────────
    for field_name, (env_var, field_type) in _CRISIS_ALPHA_ENV_OVERRIDES.items():
        env_val = os.environ.get(env_var)
        if env_val is not None:
            try:
                kwargs[field_name] = field_type(env_val)
                logger.info("Crisis alpha config override: %s=%s (from %s)", field_name, kwargs[field_name], env_var)
            except (ValueError, TypeError) as exc:
                logger.warning("Invalid env override %s=%r: %s", env_var, env_val, exc)

    config = CrisisAlphaConfig(**kwargs)

    # ── Step 3: Range validation for key parameters ─────────────────
    _range_checks = {
        "shi_threshold": (0.0, 1.0),
        "sustained_nav_pct": (0.0, 1.0),
        "flash_nav_pct": (0.0, 1.0),
        "flash_drop_threshold": (0.0, 1.0),
        "otm_pct": (0.0, 0.50),
        "profit_target_multiple": (1.0, 20.0),
        "sector_puts_nav_pct_per_sector": (0.0, 0.10),
        "sector_puts_otm_pct": (0.0, 0.30),
        "sector_puts_shi_threshold": (0.0, 1.0),
    }
    for field_name, (lo, hi) in _range_checks.items():
        val = getattr(config, field_name)
        if val < lo or val > hi:
            logger.warning(
                "Crisis alpha config: %s=%s out of range [%s, %s], clamping",
                field_name, val, lo, hi,
            )
            object.__setattr__(config, field_name, max(lo, min(hi, val)))

    return config


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
    prev_sector_scores: Dict[str, float] | None = None,
    consecutive_sick_days: int = 0,
    config: CrisisAlphaConfig | None = None,
) -> CrisisAlphaSignalResult:
    """Evaluate the crisis alpha signal from sector health scores.

    Two trigger modes:
    1. **Flash** (instant): ≥N sectors drop SHI sharply in a single day
       while ≥3 are already sick. Catches mega-crises on day one.
    2. **Sustained**: ≥5 sectors sick for 3+ consecutive days. Filters
       noise while catching real crises.

    Parameters
    ----------
    sector_scores : dict
        sector_name → SHI score for the current date
    as_of_date : date
        Current evaluation date
    prev_sector_scores : dict, optional
        Previous day's scores (needed for flash detection)
    consecutive_sick_days : int
        How many consecutive days ≥engage_count sectors have been sick
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

    # ── Flash detection: sharp single-day multi-sector drop ───────
    # Require BOTH a sharp drop (delta) AND the sector being below the
    # sick threshold (absolute floor). This prevents false flash signals
    # when sectors drop sharply but from a high starting level (e.g.
    # 0.90 -> 0.78 is a large drop but the sector is still healthy).
    flash_drops = 0
    if prev_sector_scores:
        for sector in sector_scores:
            prev = prev_sector_scores.get(sector, 1.0)
            curr = sector_scores[sector]
            if (prev - curr > config.flash_drop_threshold
                    and curr < config.shi_threshold):
                flash_drops += 1

    is_flash = (
        flash_drops >= config.flash_sector_count
        and n_sick >= config.flash_min_sick
    )

    # ── Signal determination ──────────────────────────────────────
    if is_flash:
        signal = CrisisSignal.FULL_CRISIS
        nav_pct = config.flash_nav_pct
    elif consecutive_sick_days >= config.sustained_days and n_sick >= config.sustained_engage_count:
        signal = CrisisSignal.ENGAGE
        nav_pct = config.sustained_nav_pct
    elif n_sick >= 3:
        signal = CrisisSignal.WATCH
        nav_pct = 0.0
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
    sector_prices: Dict[str, float] | None = None,
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
                    config.flash_nav_pct * 100,
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

        # ── Sector-specific puts on the weakest sectors ──────────
        if config.sector_puts_enabled:
            sector_trades = _generate_sector_puts(
                signal_result, nav, config, sector_prices,
            )
            trades.extend(sector_trades)

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


# ── Sector-specific put generation ────────────────────────────────────


# Sector ETF → underlying symbol for options (strip .US suffix)
_SECTOR_ETF_TO_OPTION_SYMBOL: Dict[str, str] = {
    "XLK.US": "XLK",
    "XLF.US": "XLF",
    "XLV.US": "XLV",
    "XLI.US": "XLI",
    "XLY.US": "XLY",
    "XLP.US": "XLP",
    "XLE.US": "XLE",
    "XLU.US": "XLU",
    "XLRE.US": "XLRE",
    "XLC.US": "XLC",
    "XLB.US": "XLB",
}


def _generate_sector_puts(
    signal_result: CrisisAlphaSignalResult,
    nav: float,
    config: CrisisAlphaConfig,
    sector_prices: Dict[str, float] | None = None,
) -> List[Dict]:
    """Generate put trades on the weakest individual sector ETFs.

    Instead of only buying broad SPY puts, this targets the specific
    sectors identified by SHI as most damaged. Concentrated exposure
    where the pain actually is, cheaper premium (sector ETFs have
    lower IV than SPY during sector-specific stress).

    Parameters
    ----------
    signal_result : CrisisAlphaSignalResult
        Must have sick_sectors and sector_scores populated.
    nav : float
        Portfolio NAV for sizing.
    config : CrisisAlphaConfig
        Strategy configuration.
    sector_prices : dict, optional
        ETF instrument_id -> current price. If None, sector puts are skipped.
    """
    from apatheon.sector.health import SECTOR_NAME_TO_ETF

    if not sector_prices:
        logger.debug("Sector puts: no sector prices provided, skipping")
        return []

    trades: List[Dict] = []

    # Rank sick sectors by SHI score (worst first)
    sector_ranking = [
        (sector, score)
        for sector, score in signal_result.sector_scores.items()
        if score < config.sector_puts_shi_threshold
    ]
    sector_ranking.sort(key=lambda x: x[1])

    # Take the N weakest
    targets = sector_ranking[:config.sector_puts_max_sectors]

    for sector_name, shi_score in targets:
        etf_id = SECTOR_NAME_TO_ETF.get(sector_name)
        if not etf_id:
            continue

        option_symbol = _SECTOR_ETF_TO_OPTION_SYMBOL.get(etf_id)
        if not option_symbol:
            continue

        etf_price = sector_prices.get(etf_id)
        if not etf_price or etf_price <= 0:
            continue

        budget = nav * config.sector_puts_nav_pct_per_sector
        strike = round(etf_price * (1 - config.sector_puts_otm_pct))

        # Estimate premium (~3% of strike for sector ETF puts)
        est_premium_pct = 0.03
        premium_per_contract = strike * est_premium_pct * 100
        if premium_per_contract <= 0:
            continue
        n_contracts = max(1, int(budget / premium_per_contract))

        trades.append({
            "strategy": "crisis_alpha_sector",
            "action": "OPEN",
            "symbol": option_symbol,
            "right": "P",
            "strike": strike,
            "quantity": n_contracts,
            "dte_target": (config.target_dte_min + config.target_dte_max) // 2,
            "reason": (
                f"SECTOR PUT: {sector_name} SHI={shi_score:.2f} "
                f"(below {config.sector_puts_shi_threshold}). "
                f"Buying {n_contracts} {option_symbol} puts @ {strike} "
                f"(${budget:,.0f}, {config.sector_puts_nav_pct_per_sector:.1%} NAV)"
            ),
            "metadata": {
                "signal": signal_result.signal.value,
                "sector": sector_name,
                "etf": etf_id,
                "shi_score": shi_score,
                "nav_pct": config.sector_puts_nav_pct_per_sector,
                "budget": budget,
            },
        })

        logger.info(
            "Sector put: %s (SHI=%.2f) — buying %d %s puts @ %d (%.1f%% NAV = $%,.0f)",
            sector_name, shi_score, n_contracts, option_symbol, strike,
            config.sector_puts_nav_pct_per_sector * 100, budget,
        )

    return trades
