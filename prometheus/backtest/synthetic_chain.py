"""Prometheus v2 – Synthetic Option Chain Generator.

Generates realistic option chains (expirations + strikes) for any
underlying on any historical date.  Mimics the structure of real
US equity and index option markets:

- Monthly options: 3rd Friday of each month (standard since 1973)
- Weekly options: every Friday for liquid ETFs (from ~2005)
- VIX options: Wednesday settlement
- Strike spacing: $1 / $2.50 / $5 / $10 based on underlying price
- Strike range: ±20% for monthlies, ±10% for weeklies

Usage::

    from prometheus.backtest.synthetic_chain import SyntheticChainGenerator

    gen = SyntheticChainGenerator()
    chain = gen.generate_chain("SPY", 450.0, date(2024, 6, 15))
    expiry = gen.get_best_expiry(date(2024, 6, 15), min_dte=30, max_dte=60)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import FrozenSet, List, Optional, Set


# ── Constants ────────────────────────────────────────────────────────

# Symbols that get weekly options (historically available from ~2005-2010)
_WEEKLY_ELIGIBLE = frozenset({
    "SPY", "QQQ", "IWM", "DIA", "EEM", "GLD", "TLT", "HYG",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA",
})

# Year weeklies became widely available
_WEEKLY_START_YEAR = 2005

# VIX-style symbols (Wednesday settlement)
_VIX_SYMBOLS = frozenset({"VIX", "VX"})


# ── Data containers ──────────────────────────────────────────────────

@dataclass(frozen=True)
class SyntheticOptionChain:
    """A synthetic option chain for one underlying on one date.

    Mirrors the structure of ``OptionChainParams`` from contract_discovery.
    """
    symbol: str
    underlying_price: float
    as_of_date: date
    expirations: FrozenSet[str]       # Set of YYYYMMDD strings
    strikes: FrozenSet[float]
    multiplier: int = 100
    exchange: str = "SYNTH"
    trading_class: str = ""

    def filter_expirations(
        self,
        min_dte: int = 0,
        max_dte: int = 365,
    ) -> List[str]:
        """Return sorted expirations within a DTE range."""
        result = []
        for exp_str in sorted(self.expirations):
            exp_date = _parse_expiry(exp_str)
            if exp_date is None:
                continue
            dte = (exp_date - self.as_of_date).days
            if min_dte <= dte <= max_dte:
                result.append(exp_str)
        return result

    def filter_strikes(
        self,
        center: float,
        width_pct: float = 0.20,
    ) -> List[float]:
        """Return sorted strikes within center ± width_pct."""
        lo = center * (1 - width_pct)
        hi = center * (1 + width_pct)
        return sorted(s for s in self.strikes if lo <= s <= hi)

    def get_best_expiry(
        self,
        min_dte: int = 30,
        max_dte: int = 60,
        target_dte: Optional[int] = None,
    ) -> Optional[str]:
        """Return the expiry closest to target_dte within range."""
        candidates = self.filter_expirations(min_dte, max_dte)
        if not candidates:
            return None
        if target_dte is None:
            target_dte = (min_dte + max_dte) // 2
        best = min(candidates, key=lambda e: abs(
            (_parse_expiry(e) - self.as_of_date).days - target_dte  # type: ignore
        ))
        return best


# ── Chain Generator ──────────────────────────────────────────────────

class SyntheticChainGenerator:
    """Generate synthetic option chains for backtesting.

    Parameters
    ----------
    monthly_count : int
        Number of monthly expirations to generate (forward).
    weekly_count : int
        Number of weekly expirations (forward) for eligible symbols.
    strike_range_monthly_pct : float
        Strike range as fraction of underlying for monthlies.
    strike_range_weekly_pct : float
        Strike range as fraction of underlying for weeklies.
    """

    def __init__(
        self,
        *,
        monthly_count: int = 12,
        weekly_count: int = 8,
        strike_range_monthly_pct: float = 0.20,
        strike_range_weekly_pct: float = 0.10,
    ) -> None:
        self._monthly_count = monthly_count
        self._weekly_count = weekly_count
        self._strike_range_monthly = strike_range_monthly_pct
        self._strike_range_weekly = strike_range_weekly_pct

    def generate_chain(
        self,
        symbol: str,
        underlying_price: float,
        as_of_date: date,
    ) -> SyntheticOptionChain:
        """Generate a complete synthetic option chain.

        Parameters
        ----------
        symbol : str
            Underlying symbol (e.g. "SPY", "AAPL").
        underlying_price : float
            Current underlying price.
        as_of_date : date
            Current backtest date.

        Returns
        -------
        SyntheticOptionChain
            Chain with all available expirations and strikes.
        """
        symbol_upper = symbol.upper()

        # ── Expirations ──────────────────────────────────────────────
        expirations: Set[str] = set()

        # Monthly expirations
        monthly_dates = _get_monthly_expirations(
            as_of_date, self._monthly_count, symbol_upper,
        )
        for d in monthly_dates:
            expirations.add(d.strftime("%Y%m%d"))

        # Weekly expirations (for eligible symbols, after _WEEKLY_START_YEAR)
        if (symbol_upper in _WEEKLY_ELIGIBLE
                and as_of_date.year >= _WEEKLY_START_YEAR):
            weekly_dates = _get_weekly_expirations(
                as_of_date, self._weekly_count, symbol_upper,
            )
            for d in weekly_dates:
                expirations.add(d.strftime("%Y%m%d"))

        # ── Strikes ──────────────────────────────────────────────────
        # Use the wider monthly range for the combined chain
        strikes = _generate_strike_grid(
            underlying_price, self._strike_range_monthly,
        )

        multiplier = 100
        if symbol_upper in _VIX_SYMBOLS:
            multiplier = 100  # VIX options: $100 per point

        return SyntheticOptionChain(
            symbol=symbol_upper,
            underlying_price=underlying_price,
            as_of_date=as_of_date,
            expirations=frozenset(expirations),
            strikes=frozenset(strikes),
            multiplier=multiplier,
            exchange="SYNTH",
            trading_class=symbol_upper,
        )

    def get_best_expiry(
        self,
        symbol: str,
        underlying_price: float,
        as_of_date: date,
        min_dte: int = 30,
        max_dte: int = 60,
        target_dte: Optional[int] = None,
    ) -> Optional[str]:
        """Convenience: generate chain and pick best expiry."""
        chain = self.generate_chain(symbol, underlying_price, as_of_date)
        return chain.get_best_expiry(min_dte, max_dte, target_dte)

    def get_strike_near_delta(
        self,
        underlying_price: float,
        target_delta: float,
        right: str,
    ) -> float:
        """Approximate a strike for a given delta target.

        Uses the rough rule: for 30-45 DTE with ~20% vol,
        delta ≈ N(ln(S/K) / (σ√T)) for calls.

        This is an approximation for chain generation; the actual delta
        is computed by the pricer when pricing the option.
        """
        # Rough moneyness for target delta
        # For ATM: delta ≈ 0.50.  For 0.25 delta put, K/S ≈ 0.95
        if right.upper() == "P":
            # Put delta is negative; we use absolute value
            abs_delta = abs(target_delta)
            # Rough: moneyness ≈ 1 - 2*(0.5 - abs_delta) for near-ATM
            moneyness = 1.0 + 2.0 * (abs_delta - 0.5)
        else:
            moneyness = 1.0 - 2.0 * (target_delta - 0.5)

        moneyness = max(moneyness, 0.70)
        moneyness = min(moneyness, 1.30)

        raw_strike = underlying_price * moneyness
        spacing = _strike_spacing(underlying_price)
        return round(raw_strike / spacing) * spacing


# ── Expiration helpers ───────────────────────────────────────────────

def _third_friday(year: int, month: int) -> date:
    """Return the 3rd Friday of the given month."""
    # First day of month
    first = date(year, month, 1)
    # Day of week: Monday=0, Friday=4
    dow = first.weekday()
    # Days until first Friday
    days_to_friday = (4 - dow) % 7
    first_friday = first + timedelta(days=days_to_friday)
    # Third Friday = first Friday + 14 days
    return first_friday + timedelta(days=14)


def _next_wednesday(ref: date) -> date:
    """Return the next Wednesday on or after ref."""
    dow = ref.weekday()
    days_to_wed = (2 - dow) % 7
    if days_to_wed == 0:
        days_to_wed = 7
    return ref + timedelta(days=days_to_wed)


def _get_monthly_expirations(
    as_of_date: date,
    count: int,
    symbol: str,
) -> List[date]:
    """Generate the next `count` monthly expiration dates."""
    is_vix = symbol.upper() in _VIX_SYMBOLS
    result: List[date] = []

    year = as_of_date.year
    month = as_of_date.month

    for _ in range(count + 3):  # Overshoot to ensure we get enough
        if is_vix:
            # VIX options expire on the Wednesday before 3rd Friday
            third_fri = _third_friday(year, month)
            expiry = third_fri - timedelta(days=30)  # VIX: ~30 days before
            # Actually VIX settlement is the Wednesday before 3rd Friday
            # of the following month.  Simplified: Wednesday before 3rd Friday.
            expiry = third_fri - timedelta(days=2)  # Wed before Fri
            if expiry.weekday() != 2:  # Ensure it's Wednesday
                expiry = third_fri - timedelta(days=(third_fri.weekday() - 2) % 7)
        else:
            expiry = _third_friday(year, month)

        if expiry > as_of_date:
            result.append(expiry)

        if len(result) >= count:
            break

        # Advance month
        month += 1
        if month > 12:
            month = 1
            year += 1

    return result[:count]


def _get_weekly_expirations(
    as_of_date: date,
    count: int,
    symbol: str,
) -> List[date]:
    """Generate the next `count` weekly expiration dates (Fridays).

    Excludes dates that coincide with monthly expirations (3rd Friday).
    """
    is_vix = symbol.upper() in _VIX_SYMBOLS
    result: List[date] = []

    # Start from next Friday (or Wednesday for VIX)
    if is_vix:
        cursor = _next_wednesday(as_of_date + timedelta(days=1))
        step = timedelta(days=7)
    else:
        # Next Friday
        dow = as_of_date.weekday()
        days_to_friday = (4 - dow) % 7
        if days_to_friday == 0:
            days_to_friday = 7
        cursor = as_of_date + timedelta(days=days_to_friday)
        step = timedelta(days=7)

    # Collect monthly expirations to exclude
    monthly = set()
    for m_date in _get_monthly_expirations(as_of_date, 6, symbol):
        monthly.add(m_date)

    attempts = 0
    while len(result) < count and attempts < count * 3:
        if cursor not in monthly and cursor > as_of_date:
            result.append(cursor)
        cursor += step
        attempts += 1

    return result[:count]


# ── Strike grid helpers ──────────────────────────────────────────────

def _strike_spacing(underlying_price: float) -> float:
    """Determine strike spacing based on underlying price.

    Follows standard US exchange conventions.
    """
    if underlying_price < 25:
        return 0.50
    elif underlying_price < 50:
        return 1.0
    elif underlying_price < 200:
        return 2.50
    elif underlying_price < 500:
        return 5.0
    else:
        return 10.0


def _generate_strike_grid(
    underlying_price: float,
    range_pct: float,
) -> List[float]:
    """Generate a grid of strikes around the underlying price."""
    if underlying_price <= 0:
        return []

    spacing = _strike_spacing(underlying_price)
    lo = underlying_price * (1 - range_pct)
    hi = underlying_price * (1 + range_pct)

    # Round lo down and hi up to spacing
    lo = max(spacing, (lo // spacing) * spacing)
    hi = ((hi // spacing) + 1) * spacing

    strikes: List[float] = []
    k = lo
    while k <= hi:
        strikes.append(round(k, 2))
        k += spacing

    return strikes


def _parse_expiry(exp_str: str) -> Optional[date]:
    """Parse YYYYMMDD expiry string to date."""
    try:
        y = int(exp_str[:4])
        m = int(exp_str[4:6])
        d = int(exp_str[6:8])
        return date(y, m, d)
    except (ValueError, IndexError):
        return None


__all__ = [
    "SyntheticOptionChain",
    "SyntheticChainGenerator",
]
