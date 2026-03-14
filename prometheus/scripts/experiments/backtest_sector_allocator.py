"""Backtest the sector-aware graduated response allocator (v2 — realistic).

Runs a daily backtest over 2007-2024 comparing:
1. **Baseline**: buy-and-hold SPY.
2. **Old system**: binary sleeve switch (Growth ↔ Defensive) using MHI.
3. **Sector system**: graduated sector-aware allocation.

Realistic assumptions applied:
- **Actual hedge ETF returns**: uses real SH.US daily returns, not perfect
  inverse SPY.  Falls back to -0.95×SPY when SH data unavailable.
- **Transaction costs**: 10 bps per unit of weight traded (one-way).
- **Turnover limits**: max 25% one-way turnover per day; excess is deferred.
- **Point-in-time universe**: only instruments with price data on the
  current date are eligible.  Addresses survivorship bias.
- **Periodic rebalancing**: equal-weight base portfolio is rebuilt monthly
  from the current available universe, not held statically.

Usage:
    python -m prometheus.scripts.experiments.backtest_sector_allocator
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

import numpy as np

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from prometheus.sector.allocator import SectorAllocator, SectorAllocatorConfig, StressLevel
from apathis.sector.health import SectorHealthEngine, SectorHealthResult
from apathis.sector.mapper import SectorMapper

logger = get_logger(__name__)

# ── Configuration ────────────────────────────────────────────────────

TRANSACTION_COST_BPS = 10          # bps per unit of weight traded
MAX_TURNOVER_ONE_WAY = 0.25        # max daily weight change as fraction of NAV
PORTFOLIO_MAX_NAMES = 40           # max number of long positions
REBALANCE_FREQUENCY_DAYS = 21      # rebuild base portfolio every N trading days
MAX_DAILY_RETURN = 1.00            # cap single-stock daily return at ±100%
MIN_PRICE_THRESHOLD = 1.00         # exclude instruments trading below $1

EVENTS = {
    "GFC_start": (date(2007, 10, 9), date(2009, 3, 9)),
    "GFC_recovery": (date(2009, 3, 10), date(2010, 4, 23)),
    "COVID_crash": (date(2020, 2, 19), date(2020, 3, 23)),
    "COVID_recovery": (date(2020, 3, 24), date(2020, 8, 18)),
    "Rate_shock_2022": (date(2022, 1, 3), date(2022, 10, 12)),
}


# ── Data loading ─────────────────────────────────────────────────────

def load_prices(instrument_ids: List[str]) -> Dict[str, Dict[date, float]]:
    """Load daily close prices for instruments."""
    db = get_db_manager()
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT instrument_id, trade_date, close FROM prices_daily
            WHERE instrument_id = ANY(%s)
              AND trade_date BETWEEN '2005-01-01' AND '2025-12-31'
              AND close > 0
            ORDER BY instrument_id, trade_date
        """, (instrument_ids,))
        rows = cur.fetchall()
        cur.close()

    closes: Dict[str, Dict[date, float]] = defaultdict(dict)
    for iid, td, c in rows:
        closes[iid][td] = float(c)
    return dict(closes)


def prices_to_returns(
    closes: Dict[str, Dict[date, float]],
    max_return: float = MAX_DAILY_RETURN,
) -> Dict[str, Dict[date, float]]:
    """Convert close prices to daily returns with data quality filters.

    Returns are capped at ±max_return to guard against ticker-reuse
    data corruption (e.g. EODHD mixing a bankrupt stock's penny-stock
    prices with a different instrument that later reused the same ticker).
    """
    returns: Dict[str, Dict[date, float]] = {}
    for iid, price_dict in closes.items():
        dates_sorted = sorted(price_dict.keys())
        rets = {}
        for i in range(1, len(dates_sorted)):
            d_prev, d = dates_sorted[i - 1], dates_sorted[i]
            c_prev, c = price_dict[d_prev], price_dict[d]
            if c_prev > 0:
                r = c / c_prev - 1.0
                # Cap to filter ticker-reuse data corruption.
                r = max(-max_return, min(max_return, r))
                rets[d] = r
        returns[iid] = rets
    return returns


def load_universe_instruments() -> List[str]:
    """Load all US_EQ equity instruments (ACTIVE + DELISTED).

    Including delisted instruments is essential for avoiding survivorship
    bias: companies that went bankrupt or were acquired during the
    backtest period must be present so the portfolio can hold them
    (and lose money on them) on historical dates.
    """
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT instrument_id FROM instruments
            WHERE market_id = 'US_EQ'
              AND asset_class = 'EQUITY'
              AND status IN ('ACTIVE', 'DELISTED')
        """)
        ids = [r[0] for r in cur.fetchall()]
        cur.close()
    return ids


# ── Simple MHI ───────────────────────────────────────────────────────

def compute_spy_mhi(spy_closes: Dict[date, float]) -> Dict[date, float]:
    dates_sorted = sorted(spy_closes.keys())
    n = len(dates_sorted)
    closes = np.array([spy_closes[d] for d in dates_sorted])

    sma200 = np.full(n, np.nan)
    cs = np.cumsum(closes)
    sma200[199:] = (cs[199:] - np.concatenate([[0.0], cs[:-200]])) / 200

    ret_21d = np.full(n, np.nan)
    ret_63d = np.full(n, np.nan)
    ret_126d = np.full(n, np.nan)
    for i in range(21, n):
        if closes[i - 21] > 0:
            ret_21d[i] = closes[i] / closes[i - 21] - 1.0
    for i in range(63, n):
        if closes[i - 63] > 0:
            ret_63d[i] = closes[i] / closes[i - 63] - 1.0
    for i in range(126, n):
        if closes[i - 126] > 0:
            ret_126d[i] = closes[i] / closes[i - 126] - 1.0

    log_ret = np.zeros(n)
    log_ret[1:] = np.log(closes[1:] / closes[:-1])
    rvol = np.full(n, np.nan)
    for i in range(20, n):
        rvol[i] = np.std(log_ret[i - 20: i + 1], ddof=1) * math.sqrt(252)
    rvol_pctile = np.full(n, np.nan)
    for i in range(251, n):
        seg = rvol[i - 251: i + 1]
        valid = seg[~np.isnan(seg)]
        if len(valid) >= 2:
            rvol_pctile[i] = float(np.sum(valid <= rvol[i])) / len(valid)

    rolling_high = np.full(n, np.nan)
    for i in range(251, n):
        rolling_high[i] = np.max(closes[i - 251: i + 1])
    dd = np.where(rolling_high > 0, closes / rolling_high - 1.0, 0.0)

    mhi: Dict[date, float] = {}
    for i in range(252, n):
        if np.isnan(sma200[i]) or sma200[i] <= 0:
            continue
        trend_s = max(-1.0, min(1.0, (closes[i] / sma200[i] - 0.95) / 0.10))
        m1 = ret_21d[i] if not np.isnan(ret_21d[i]) else 0.0
        m3 = ret_63d[i] if not np.isnan(ret_63d[i]) else 0.0
        m6 = ret_126d[i] if not np.isnan(ret_126d[i]) else 0.0
        mom_s = max(-1.0, min(1.0, (0.4 * m1 + 0.35 * m3 + 0.25 * m6) / 0.15))
        vp = rvol_pctile[i] if not np.isnan(rvol_pctile[i]) else 0.5
        vol_s = 1.0 - 2.0 * vp
        dd_s = max(-1.0, min(0.0, dd[i] / 0.20)) * 2.0 + 1.0
        mhi[dates_sorted[i]] = 0.25 * trend_s + 0.25 * mom_s + 0.25 * vol_s + 0.25 * dd_s

    return mhi


# ── Turnover-constrained weight transition ───────────────────────────

def apply_turnover_limit(
    current_weights: Dict[str, float],
    target_weights: Dict[str, float],
    max_turnover: float,
) -> Dict[str, float]:
    """Move current weights toward target, respecting max one-way turnover.

    Returns the new weights after applying the turnover constraint.
    """
    all_ids = set(current_weights) | set(target_weights)
    deltas: Dict[str, float] = {}
    total_turnover = 0.0

    for iid in all_ids:
        cur = current_weights.get(iid, 0.0)
        tgt = target_weights.get(iid, 0.0)
        deltas[iid] = tgt - cur
        total_turnover += abs(tgt - cur)

    one_way = total_turnover / 2.0  # buys ≈ sells in a rebalance

    if one_way <= max_turnover:
        # Within limit: apply fully.
        return {iid: target_weights.get(iid, 0.0) for iid in all_ids if target_weights.get(iid, 0.0) != 0.0}

    # Scale deltas to fit within limit.
    scale = max_turnover / one_way if one_way > 0 else 0.0
    new_weights: Dict[str, float] = {}
    for iid in all_ids:
        cur = current_weights.get(iid, 0.0)
        new_w = cur + deltas[iid] * scale
        if abs(new_w) > 1e-8:
            new_weights[iid] = new_w

    return new_weights


def compute_transaction_cost(
    current_weights: Dict[str, float],
    new_weights: Dict[str, float],
    cost_bps: float,
) -> float:
    """Return transaction cost as fraction of NAV."""
    all_ids = set(current_weights) | set(new_weights)
    total_traded = sum(
        abs(new_weights.get(iid, 0.0) - current_weights.get(iid, 0.0))
        for iid in all_ids
    )
    return total_traded * cost_bps / 10_000.0


# ── Backtest engine ──────────────────────────────────────────────────

@dataclass
class BacktestResult:
    name: str
    dates: List[date] = field(default_factory=list)
    nav: List[float] = field(default_factory=list)
    drawdown: List[float] = field(default_factory=list)
    stress_levels: List[str] = field(default_factory=list)
    sector_kills: Dict[date, List[str]] = field(default_factory=dict)
    total_tx_cost: float = 0.0
    total_turnover: float = 0.0

    @property
    def cumulative_return(self) -> float:
        return (self.nav[-1] / self.nav[0] - 1.0) if len(self.nav) > 1 else 0.0

    @property
    def max_drawdown(self) -> float:
        return min(self.drawdown) if self.drawdown else 0.0

    @property
    def annualised_vol(self) -> float:
        if len(self.nav) < 2:
            return 0.0
        rets = [self.nav[i] / self.nav[i - 1] - 1.0 for i in range(1, len(self.nav))]
        return float(np.std(rets, ddof=1) * math.sqrt(252))

    @property
    def sharpe(self) -> float:
        vol = self.annualised_vol
        if vol <= 0:
            return 0.0
        n_years = len(self.nav) / 252
        ann_ret = (self.nav[-1] / self.nav[0]) ** (1.0 / n_years) - 1.0 if n_years > 0 else 0.0
        return ann_ret / vol


def build_equal_weight_portfolio(
    available_instruments: Set[str],
    max_names: int,
    sector_mapper: Optional[SectorMapper] = None,
) -> Dict[str, float]:
    """Build equal-weight portfolio from available instruments.

    Uses a deterministic sort so the portfolio is reproducible.
    """
    candidates = sorted(available_instruments)
    selected = candidates[:max_names]
    if not selected:
        return {}
    w = 1.0 / len(selected)
    return {iid: w for iid in selected}


def run_backtest(
    name: str,
    trading_dates: List[date],
    instrument_returns: Dict[str, Dict[date, float]],
    all_closes: Dict[str, Dict[date, float]],
    hedge_returns: Dict[date, float],
    spy_returns: Dict[date, float],
    sector_allocator: Optional[SectorAllocator],
    sector_mapper: Optional[SectorMapper],
    mhi_series: Dict[date, float],
    use_sector_system: bool,
) -> BacktestResult:
    """Run a daily backtest with realistic constraints."""
    result = BacktestResult(name=name)
    nav = 1_000_000.0
    peak = nav

    # Current portfolio weights (carried forward daily).
    current_weights: Dict[str, float] = {}
    current_hedge_alloc: float = 0.0
    days_since_rebalance = 0

    for d in trading_dates:
        # ── Point-in-time universe: only instruments with data today ──
        # Also exclude penny stocks (price < $1) to avoid bankrupt
        # companies that continue trading as zombie tickers.
        available = set()
        for iid, rets in instrument_returns.items():
            if d not in rets:
                continue
            # Check previous close is above penny-stock threshold.
            price = all_closes.get(iid, {}).get(d, 0.0)
            if price >= MIN_PRICE_THRESHOLD:
                available.add(iid)

        # ── Periodic rebalancing: rebuild base portfolio monthly ──────
        days_since_rebalance += 1
        if days_since_rebalance >= REBALANCE_FREQUENCY_DAYS or not current_weights:
            base_weights = build_equal_weight_portfolio(
                available, PORTFOLIO_MAX_NAMES, sector_mapper,
            )
            days_since_rebalance = 0
        else:
            # Keep previous base, but remove delisted instruments.
            base_weights = {
                iid: w for iid, w in current_weights.items()
                if iid in available
            }
            # Renormalise after removing stale names.
            total = sum(base_weights.values())
            if total > 0:
                for iid in base_weights:
                    base_weights[iid] /= total

        # ── Compute target weights ───────────────────────────────────
        if use_sector_system and sector_allocator is not None:
            mhi = mhi_series.get(d)
            decision = sector_allocator.adjust_weights(base_weights, d, market_mhi=mhi)
            target_weights = decision.adjusted_weights
            target_hedge = decision.hedge_allocation
            stress = decision.stress_level.value
            if decision.sick_sectors:
                result.sector_kills[d] = list(decision.sick_sectors)
        else:
            mhi = mhi_series.get(d)
            if mhi is not None and mhi < -0.1:
                target_weights = {k: v * 0.5 for k, v in base_weights.items()}
                target_hedge = 0.5
                stress = "DEFENSIVE"
            else:
                target_weights = dict(base_weights)
                target_hedge = 0.0
                stress = "GROWTH"

        # ── Apply turnover limit ─────────────────────────────────────
        new_weights = apply_turnover_limit(
            current_weights, target_weights, MAX_TURNOVER_ONE_WAY,
        )

        # Hedge transitions are also subject to turnover (simplified).
        hedge_delta = abs(target_hedge - current_hedge_alloc)
        if hedge_delta > MAX_TURNOVER_ONE_WAY:
            if target_hedge > current_hedge_alloc:
                new_hedge = current_hedge_alloc + MAX_TURNOVER_ONE_WAY
            else:
                new_hedge = current_hedge_alloc - MAX_TURNOVER_ONE_WAY
        else:
            new_hedge = target_hedge

        # ── Transaction costs ────────────────────────────────────────
        tx_cost = compute_transaction_cost(
            current_weights, new_weights, TRANSACTION_COST_BPS,
        )
        # Hedge transaction cost.
        tx_cost += abs(new_hedge - current_hedge_alloc) * TRANSACTION_COST_BPS / 10_000.0

        result.total_tx_cost += tx_cost * nav
        turnover = sum(
            abs(new_weights.get(iid, 0.0) - current_weights.get(iid, 0.0))
            for iid in set(current_weights) | set(new_weights)
        )
        result.total_turnover += turnover

        # ── Compute returns ──────────────────────────────────────────
        # Long leg.
        port_ret = 0.0
        for iid, w in new_weights.items():
            r = instrument_returns.get(iid, {}).get(d, 0.0)
            port_ret += w * r

        # Hedge leg: use ACTUAL SH.US returns.
        sh_ret = hedge_returns.get(d)
        if sh_ret is not None:
            hedge_ret = sh_ret * new_hedge
        else:
            # Fallback: imperfect inverse (-0.95x SPY to model tracking error).
            spy_ret = spy_returns.get(d, 0.0)
            hedge_ret = -0.95 * spy_ret * new_hedge

        total_ret = port_ret + hedge_ret - tx_cost
        nav *= (1.0 + total_ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0

        # Update state.
        current_weights = new_weights
        current_hedge_alloc = new_hedge

        result.dates.append(d)
        result.nav.append(nav)
        result.drawdown.append(dd)
        result.stress_levels.append(stress)

    return result


def main() -> None:
    logger.info("Loading data...")

    instruments = load_universe_instruments()
    logger.info("Universe: %d instruments", len(instruments))

    # Load prices for all instruments + hedge ETFs.
    all_ids = instruments + ["SPY.US", "SH.US"]
    all_closes = load_prices(all_ids)
    logger.info("Loaded prices for %d instruments", len(all_closes))

    spy_closes = all_closes.get("SPY.US", {})
    sh_closes = all_closes.get("SH.US", {})

    # Compute returns.
    all_returns = prices_to_returns(all_closes)
    spy_returns = all_returns.get("SPY.US", {})
    sh_returns = all_returns.get("SH.US", {})

    # Remove non-equity instruments from the returns used for portfolio.
    equity_returns = {
        iid: rets for iid, rets in all_returns.items()
        if iid not in ("SPY.US", "SH.US")
    }
    logger.info("Equity returns for %d instruments", len(equity_returns))

    mhi_series = compute_spy_mhi(spy_closes)
    logger.info("Computed MHI for %d dates", len(mhi_series))

    # Sector health.
    engine = SectorHealthEngine()
    engine.load(start=date(2007, 1, 1), end=date(2024, 12, 31), load_breadth=True)
    shi = engine.compute(start=date(2007, 1, 1), end=date(2024, 12, 31))

    mapper = SectorMapper()
    mapper.load()

    config = SectorAllocatorConfig()
    sector_alloc = SectorAllocator(config, mapper, shi)

    # Trading dates.
    trading_dates = sorted(
        d for d in spy_returns
        if date(2007, 1, 2) <= d <= date(2024, 12, 31)
    )
    logger.info("Trading dates: %d (%s to %s)",
                len(trading_dates), trading_dates[0], trading_dates[-1])

    # ── Run backtests ────────────────────────────────────────────────
    logger.info("Running baseline (SPY buy-and-hold)...")
    baseline = BacktestResult(name="SPY Buy & Hold")
    nav = 1_000_000.0
    peak = nav
    for d in trading_dates:
        r = spy_returns.get(d, 0.0)
        nav *= (1.0 + r)
        peak = max(peak, nav)
        baseline.dates.append(d)
        baseline.nav.append(nav)
        baseline.drawdown.append(nav / peak - 1.0)
        baseline.stress_levels.append("N/A")

    logger.info("Running old system (binary MHI + turnover + costs)...")
    old_system = run_backtest(
        "Old System (Binary MHI)",
        trading_dates, equity_returns, all_closes, sh_returns, spy_returns,
        None, mapper, mhi_series, use_sector_system=False,
    )

    logger.info("Running sector system (graduated + turnover + costs)...")
    sector_system = run_backtest(
        "Sector System (Graduated)",
        trading_dates, equity_returns, all_closes, sh_returns, spy_returns,
        sector_alloc, mapper, mhi_series, use_sector_system=True,
    )

    # ── Results ──────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("BACKTEST RESULTS (2007-01 to 2024-12) — REALISTIC")
    print(f"  Transaction cost: {TRANSACTION_COST_BPS} bps | "
          f"Max turnover: {MAX_TURNOVER_ONE_WAY:.0%}/day | "
          f"Rebalance: every {REBALANCE_FREQUENCY_DAYS}d | "
          f"Hedge: actual SH.US returns")
    print("=" * 80)

    for bt in [baseline, old_system, sector_system]:
        print(f"\n{bt.name}")
        print(f"  Cumulative Return: {bt.cumulative_return:+.1%}")
        print(f"  Max Drawdown:      {bt.max_drawdown:.1%}")
        print(f"  Annualised Vol:    {bt.annualised_vol:.1%}")
        print(f"  Sharpe Ratio:      {bt.sharpe:.3f}")
        if bt.total_tx_cost > 0:
            print(f"  Total Tx Cost:     ${bt.total_tx_cost:,.0f}")
            print(f"  Total Turnover:    {bt.total_turnover:,.1f}x NAV")

    # Stress level distribution for sector system.
    print(f"\nSector System Stress Distribution")
    stress_counts = Counter(sector_system.stress_levels)
    total_days = len(sector_system.stress_levels)
    for level in ["NORMAL", "SECTOR_STRESS", "BROAD_STRESS", "SYSTEMIC_CRISIS"]:
        count = stress_counts.get(level, 0)
        print(f"  {level:20s}: {count:5d} days ({count/total_days:.1%})")

    # Event analysis.
    print(f"\nEvent Analysis")
    for event_name, (ev_start, ev_end) in EVENTS.items():
        event_levels = []
        event_kills: Dict[str, int] = Counter()
        for i, d in enumerate(sector_system.dates):
            if ev_start <= d <= ev_end:
                event_levels.append(sector_system.stress_levels[i])
                if d in sector_system.sector_kills:
                    for s in sector_system.sector_kills[d]:
                        event_kills[s] += 1

        if event_levels:
            level_counts = Counter(event_levels)
            dominant = level_counts.most_common(1)[0][0]
            first_stress = None
            for i, d in enumerate(sector_system.dates):
                if ev_start <= d <= ev_end and sector_system.stress_levels[i] != "NORMAL":
                    first_stress = d
                    break

            spy_start_p = spy_closes.get(ev_start, spy_closes.get(ev_start - timedelta(days=1), 0))
            spy_end_p = spy_closes.get(ev_end, 0)
            spy_ev_ret = (spy_end_p / spy_start_p - 1.0) if spy_start_p > 0 else 0.0

            nav_start = nav_end = None
            for i, d in enumerate(sector_system.dates):
                if d >= ev_start and nav_start is None:
                    nav_start = sector_system.nav[i]
                if d <= ev_end:
                    nav_end = sector_system.nav[i]
            sec_ev_ret = (nav_end / nav_start - 1.0) if nav_start and nav_end else 0.0

            print(f"\n  {event_name} ({ev_start} to {ev_end})")
            print(f"    SPY return:      {spy_ev_ret:+.1%}")
            print(f"    Sector sys ret:  {sec_ev_ret:+.1%}")
            print(f"    Dominant level:  {dominant}")
            print(f"    First stress:    {first_stress}")
            if event_kills:
                top_kills = event_kills.most_common(5)
                print(f"    Top sector kills: {', '.join(f'{s}({n}d)' for s, n in top_kills)}")


if __name__ == "__main__":
    main()
