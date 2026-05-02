"""Compare sector put spread hedge vs SH.US hedge — focused backtest.

This is a fast, targeted backtest that compares hedging strategies:
1. SH.US only (current production approach)
2. Sector puts (OLD config: narrow window, small size, tight spread)
3. Sector puts (NEW config: wider window, larger size, wider spread)

Uses only sector ETFs + SPY + SH.US prices — no need to load the full
equity universe. Runs in <30 seconds.

Usage:
    python -m prometheus.scripts.experiments.compare_sector_puts_vs_sh
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple
from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.sector.health import SectorHealthEngine, SECTOR_NAME_TO_ETF

logger = get_logger(__name__)

# Sector ETF mapping (reverse of SECTOR_NAME_TO_ETF)
ETF_TO_SECTOR = {v: k for k, v in SECTOR_NAME_TO_ETF.items()}

# ── Config for old vs new put strategy ──────────────────────────────

@dataclass
class PutConfig:
    name: str
    shi_threshold: float      # SHI below this → open put
    shi_floor: float          # SHI below this → skip (0 = no floor)
    spread_width: float       # % between long and short strikes
    otm_pct: float           # long strike OTM %
    max_nav_pct: float       # max % of NAV per sector
    max_total_pct: float     # max total across all sectors

OLD_CONFIG = PutConfig(
    name="OLD puts (narrow/small)",
    shi_threshold=0.25,
    shi_floor=0.15,
    spread_width=0.07,
    otm_pct=0.0,
    max_nav_pct=0.01,
    max_total_pct=0.11,
)

NEW_CONFIG = PutConfig(
    name="NEW puts (wide/large)",
    shi_threshold=0.30,
    shi_floor=0.0,
    spread_width=0.15,
    otm_pct=0.03,
    max_nav_pct=0.03,
    max_total_pct=0.20,
)


# ── Data loading ────────────────────────────────────────────────────

def load_etf_prices() -> Dict[str, Dict[date, float]]:
    """Load daily close prices for sector ETFs + SPY + SH.US."""
    db = get_db_manager()
    etf_ids = list(SECTOR_NAME_TO_ETF.values()) + ["SPY.US", "SH.US"]
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT instrument_id, trade_date, close
            FROM prices_daily
            WHERE instrument_id = ANY(%s)
              AND close > 0
            ORDER BY instrument_id, trade_date
        """, (etf_ids,))
        rows = cur.fetchall()
        cur.close()
    prices: Dict[str, Dict[date, float]] = defaultdict(dict)
    for iid, td, c in rows:
        prices[iid][td] = float(c)
    return dict(prices)


def compute_returns(prices: Dict[str, Dict[date, float]]) -> Dict[str, Dict[date, float]]:
    returns: Dict[str, Dict[date, float]] = {}
    for iid, pd in prices.items():
        dates = sorted(pd.keys())
        rets = {}
        for i in range(1, len(dates)):
            if pd[dates[i-1]] > 0:
                rets[dates[i]] = pd[dates[i]] / pd[dates[i-1]] - 1.0
        returns[iid] = rets
    return returns


# ── Put spread P&L simulation ───────────────────────────────────────

@dataclass
class SpreadPosition:
    sector: str
    etf_id: str
    open_date: date
    expiry: date
    long_strike: float
    short_strike: float
    n_contracts: int
    cost: float  # total debit paid

    @property
    def max_payoff(self) -> float:
        return (self.long_strike - self.short_strike) * 100 * self.n_contracts

    def payoff_at_price(self, price: float) -> float:
        """Compute spread payoff at a given underlying price."""
        long_payoff = max(0, self.long_strike - price) * 100 * self.n_contracts
        short_payoff = max(0, self.short_strike - price) * 100 * self.n_contracts
        return long_payoff - short_payoff - self.cost


def simulate_put_strategy(
    config: PutConfig,
    sector_shi: Dict[str, Dict[date, float]],
    etf_prices: Dict[str, Dict[date, float]],
    trading_dates: List[date],
    nav: float = 1_000_000.0,
) -> Tuple[List[float], Dict[str, Any]]:
    """Simulate sector put spread strategy, return daily P&L list + stats."""
    daily_pnl: List[float] = []
    positions: List[SpreadPosition] = []
    total_premium_paid = 0.0
    total_payoff = 0.0
    trades_opened = 0
    trades_expired = 0

    for d in trading_dates:
        day_pnl = 0.0

        # Check for expiring positions
        expired = [p for p in positions if p.expiry <= d]
        for p in expired:
            etf_price = etf_prices.get(p.etf_id, {}).get(d, 0.0)
            if etf_price > 0:
                payoff = p.payoff_at_price(etf_price)
                day_pnl += payoff
                total_payoff += max(0, payoff + p.cost)  # gross payoff
            trades_expired += 1
        positions = [p for p in positions if p.expiry > d]

        # Check for new positions to open
        total_allocated = sum(p.cost for p in positions)
        hedged_sectors = {p.sector for p in positions}

        for sector_name, shi_series in sector_shi.items():
            shi = shi_series.get(d)
            if shi is None:
                continue

            etf_id = SECTOR_NAME_TO_ETF.get(sector_name)
            if not etf_id or etf_id not in etf_prices:
                continue

            if sector_name in hedged_sectors:
                continue  # Already hedged

            etf_price = etf_prices[etf_id].get(d, 0.0)
            if etf_price <= 0:
                continue

            # Check thresholds
            if shi >= config.shi_threshold:
                continue
            if config.shi_floor > 0 and shi < config.shi_floor:
                continue

            # Size the spread
            long_strike = round(etf_price * (1 - config.otm_pct))
            short_strike = round(etf_price * (1 - config.otm_pct - config.spread_width))
            if short_strike >= long_strike:
                continue

            spread_cost_per = (long_strike - short_strike) * 100
            budget = nav * config.max_nav_pct
            remaining_total = (nav * config.max_total_pct) - total_allocated
            budget = min(budget, max(0, remaining_total))

            n_contracts = max(1, int(budget / spread_cost_per)) if spread_cost_per > 0 else 0
            if n_contracts <= 0:
                continue

            # Estimate premium cost (simplified: ~30% of max spread width for OTM puts)
            premium_pct = 0.25 if config.otm_pct > 0 else 0.35
            premium = spread_cost_per * premium_pct * n_contracts
            cost = premium

            expiry = d + timedelta(days=45)  # ~45 DTE

            positions.append(SpreadPosition(
                sector=sector_name,
                etf_id=etf_id,
                open_date=d,
                expiry=expiry,
                long_strike=long_strike,
                short_strike=short_strike,
                n_contracts=n_contracts,
                cost=cost,
            ))
            total_premium_paid += cost
            total_allocated += cost
            trades_opened += 1
            day_pnl -= cost  # Debit

        daily_pnl.append(day_pnl)

    stats = {
        "total_premium_paid": total_premium_paid,
        "total_payoff": total_payoff,
        "net_pnl": sum(daily_pnl),
        "trades_opened": trades_opened,
        "trades_expired": trades_expired,
        "open_positions_remaining": len(positions),
    }
    return daily_pnl, stats


def simulate_sh_strategy(
    sh_returns: Dict[date, float],
    spy_returns: Dict[date, float],
    fragility_series: Dict[date, float],
    trading_dates: List[date],
    nav: float = 1_000_000.0,
) -> Tuple[List[float], Dict[str, Any]]:
    """Simulate SH.US hedge strategy sized by fragility."""
    daily_pnl: List[float] = []
    total_hedge_days = 0

    for d in trading_dates:
        frag = fragility_series.get(d, 0.0)

        # SH.US allocation ramps linearly from 0% at frag=0.3 to 50% at frag=0.7
        if frag < 0.30:
            sh_alloc = 0.0
        elif frag > 0.70:
            sh_alloc = 0.50
        else:
            sh_alloc = (frag - 0.30) / 0.40 * 0.50

        sh_ret = sh_returns.get(d, 0.0)
        if sh_ret == 0.0:
            # Fallback: -0.95x SPY
            spy_ret = spy_returns.get(d, 0.0)
            sh_ret = -0.95 * spy_ret

        pnl = nav * sh_alloc * sh_ret
        daily_pnl.append(pnl)
        if sh_alloc > 0:
            total_hedge_days += 1

    stats = {
        "net_pnl": sum(daily_pnl),
        "total_hedge_days": total_hedge_days,
        "avg_allocation": sum(daily_pnl) / len(daily_pnl) if daily_pnl else 0,
    }
    return daily_pnl, stats


def main() -> None:
    logger.info("Loading sector ETF prices...")
    etf_prices = load_etf_prices()
    logger.info("Loaded %d instruments", len(etf_prices))

    etf_returns = compute_returns(etf_prices)
    spy_returns = etf_returns.get("SPY.US", {})
    sh_returns = etf_returns.get("SH.US", {})

    # Compute sector health
    logger.info("Computing sector health...")
    engine = SectorHealthEngine()
    engine.load(start=date(2015, 1, 1), end=date(2024, 12, 31), load_breadth=True)
    shi = engine.compute(start=date(2015, 1, 1), end=date(2024, 12, 31))
    logger.info("Sector health computed for %d sectors", len(shi.scores))

    # Simple fragility proxy from SPY drawdown
    spy_prices = etf_prices.get("SPY.US", {})
    frag_series: Dict[date, float] = {}
    spy_dates = sorted(spy_prices.keys())
    peak = 0.0
    for d in spy_dates:
        p = spy_prices[d]
        peak = max(peak, p)
        dd = 1.0 - p / peak if peak > 0 else 0.0
        frag_series[d] = dd  # drawdown as fragility proxy

    # Trading dates
    trading_dates = sorted(
        d for d in spy_returns
        if date(2015, 1, 2) <= d <= date(2024, 12, 31)
    )
    logger.info("Trading dates: %d", len(trading_dates))

    # Run simulations
    logger.info("Running SH.US hedge...")
    sh_pnl, sh_stats = simulate_sh_strategy(
        sh_returns, spy_returns, frag_series, trading_dates,
    )

    logger.info("Running OLD sector puts...")
    old_pnl, old_stats = simulate_put_strategy(
        OLD_CONFIG, shi.scores, etf_prices, trading_dates,
    )

    logger.info("Running NEW sector puts...")
    new_pnl, new_stats = simulate_put_strategy(
        NEW_CONFIG, shi.scores, etf_prices, trading_dates,
    )

    # Results
    nav = 1_000_000.0
    print("\n" + "=" * 70)
    print("HEDGE STRATEGY COMPARISON (2015-2024)")
    print("=" * 70)

    for name, pnl, stats in [
        ("SH.US (fragility-sized)", sh_pnl, sh_stats),
        (OLD_CONFIG.name, old_pnl, old_stats),
        (NEW_CONFIG.name, new_pnl, new_stats),
    ]:
        total_pnl = sum(pnl)
        cum_returns = []
        cum = 0.0
        for p in pnl:
            cum += p
            cum_returns.append(cum)

        print(f"\n{name}")
        print(f"  Net P&L:       ${total_pnl:>+15,.0f}")
        print(f"  As % of NAV:   {total_pnl/nav:>+10.2%}")
        if "total_premium_paid" in stats:
            print(f"  Premium Paid:  ${stats['total_premium_paid']:>15,.0f}")
            print(f"  Gross Payoff:  ${stats['total_payoff']:>15,.0f}")
            print(f"  Trades:        {stats['trades_opened']:>5} opened, {stats['trades_expired']:>5} expired")
        if "total_hedge_days" in stats:
            print(f"  Hedge Days:    {stats['total_hedge_days']:>5} / {len(trading_dates)}")
        for k, v in stats.items():
            if k not in ("net_pnl", "total_premium_paid", "total_payoff",
                        "trades_opened", "trades_expired", "total_hedge_days",
                        "avg_allocation", "open_positions_remaining"):
                print(f"  {k}: {v}")

    # Event-specific analysis
    EVENTS = {
        "COVID crash (Feb-Mar 2020)": (date(2020, 2, 19), date(2020, 3, 23)),
        "2022 rate shock (Jan-Oct)": (date(2022, 1, 3), date(2022, 10, 12)),
        "2018 Q4 selloff": (date(2018, 10, 1), date(2018, 12, 24)),
    }

    print("\n" + "-" * 70)
    print("EVENT-SPECIFIC HEDGE P&L")
    print("-" * 70)

    for event_name, (start, end) in EVENTS.items():
        print(f"\n{event_name} ({start} → {end})")
        for strat_name, pnl_list in [
            ("SH.US", sh_pnl),
            ("OLD puts", old_pnl),
            ("NEW puts", new_pnl),
        ]:
            event_pnl = sum(
                p for d, p in zip(trading_dates, pnl_list)
                if start <= d <= end
            )
            print(f"  {strat_name:20s}  ${event_pnl:>+12,.0f}")


if __name__ == "__main__":
    main()
