"""Backtest the conviction-based position lifecycle manager.

Runs a daily backtest over 2007-2024 comparing:
1. **SPY buy-and-hold**: passive benchmark.
2. **Sector system + conviction**: graduated sector-aware allocator with
   conviction-based entry/exit logic.

The conviction system receives a daily selection signal (which instruments
the inner model would pick today after sector adjustments) and uses a
conviction score to smooth entries and exits:
- New entries start with +5 credit at half weight; scale to full after
  3 consecutive days of selection.
- Selected positions build +1/day; non-selected decay at -2/day
  (regime-adjusted: x1.5 SECTOR_STRESS, x2.0 BROAD_STRESS).
- Positions exit only when conviction <= 0 or a -20% hard stop fires.

Realistic assumptions:
- Actual SH.US hedge returns (fallback: -0.95 x SPY).
- Transaction costs: 10 bps per unit of weight traded.
- Turnover limits: max 25% one-way per day.
- Point-in-time universe with survivorship-bias handling.

Usage:
    python -m prometheus.scripts.experiments.backtest_conviction
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

import numpy as np

from apathis.core.logging import get_logger
from prometheus.portfolio.conviction import (
    ConvictionConfig,
    ConvictionTracker,
    PositionConviction,
)
from prometheus.sector.allocator import (
    SectorAllocator,
    SectorAllocatorConfig,
    StressLevel,
)
from apathis.sector.health import SectorHealthEngine
from apathis.sector.mapper import SectorMapper

# Shared utilities from the sector allocator backtest.
from prometheus.scripts.experiments.backtest_sector_allocator import (
    load_prices,
    prices_to_returns,
    load_universe_instruments,
    compute_spy_mhi,
    apply_turnover_limit,
    compute_transaction_cost,
    BacktestResult,
    TRANSACTION_COST_BPS,
    MAX_TURNOVER_ONE_WAY,
    PORTFOLIO_MAX_NAMES,
    MIN_PRICE_THRESHOLD,
    EVENTS,
)

logger = get_logger(__name__)


# ── Sector-balanced portfolio construction ───────────────────────────

def build_sector_balanced_portfolio(
    available_instruments: Set[str],
    max_names: int,
    sector_mapper: SectorMapper,
) -> Dict[str, float]:
    """Build equal-weight portfolio via round-robin across sectors.

    Instead of alphabetical top-N (which always picks the same A-names),
    this selects instruments spread across all sectors.  Within each
    sector, instruments are sorted alphabetically for determinism.

    With ~11 sectors and 40 target positions, each sector contributes
    roughly 3-4 names.
    """
    by_sector: Dict[str, List[str]] = defaultdict(list)
    for iid in available_instruments:
        sector = sector_mapper.get_sector(iid) or "UNKNOWN"
        by_sector[sector].append(iid)

    # Sort within each sector for determinism.
    for sector in by_sector:
        by_sector[sector].sort()

    # Round-robin: pick one from each sector, cycle until full.
    sectors = sorted(by_sector.keys())
    if not sectors:
        return {}

    selected: List[str] = []
    idx = {s: 0 for s in sectors}

    while len(selected) < max_names:
        added_any = False
        for s in sectors:
            if len(selected) >= max_names:
                break
            if idx[s] < len(by_sector[s]):
                selected.append(by_sector[s][idx[s]])
                idx[s] += 1
                added_any = True
        if not added_any:
            break  # all sectors exhausted

    if not selected:
        return {}
    w = 1.0 / len(selected)
    return {iid: w for iid in selected}


# ── Conviction-specific statistics ───────────────────────────────────

@dataclass
class ExitRecord:
    """Single exit event for detailed analysis."""
    holding_days: int
    reason: str
    conviction_score: float
    stress_level: str


@dataclass
class ConvictionStats:
    """Aggregated statistics from a conviction backtest run."""

    total_entries: int = 0
    total_exits: int = 0
    exits_by_reason: Dict[str, int] = field(default_factory=dict)
    holding_periods: List[int] = field(default_factory=list)
    exit_scores: List[float] = field(default_factory=list)
    daily_position_count: List[int] = field(default_factory=list)
    daily_avg_conviction: List[float] = field(default_factory=list)
    exit_records: List[ExitRecord] = field(default_factory=list)

    @property
    def avg_holding_days(self) -> float:
        return (
            sum(self.holding_periods) / len(self.holding_periods)
            if self.holding_periods else 0.0
        )

    @property
    def avg_position_count(self) -> float:
        return (
            sum(self.daily_position_count) / len(self.daily_position_count)
            if self.daily_position_count else 0.0
        )

    @property
    def avg_exit_score(self) -> float:
        return (
            sum(self.exit_scores) / len(self.exit_scores)
            if self.exit_scores else 0.0
        )


# ── Conviction backtest engine ───────────────────────────────────────

def run_backtest_conviction(
    name: str,
    trading_dates: List[date],
    instrument_returns: Dict[str, Dict[date, float]],
    all_closes: Dict[str, Dict[date, float]],
    hedge_returns: Dict[date, float],
    spy_returns: Dict[date, float],
    sector_allocator: SectorAllocator,
    sector_mapper: SectorMapper,
    mhi_series: Dict[date, float],
    conviction_config: ConvictionConfig,
) -> Tuple[BacktestResult, ConvictionStats]:
    """Run a daily backtest with conviction-based position lifecycle.

    Daily flow:
    1. Build point-in-time universe (instruments with data, price >= $1).
    2. Compute daily selection signal: equal-weight top-N from universe.
    3. Apply sector allocator -> adjusted weights + stress level + hedge.
    4. Feed selection signal into ConvictionTracker -> entries/exits/holds.
    5. Build conviction-adjusted weights with weight fractions.
    6. Apply turnover limits and compute transaction costs.
    7. Compute portfolio + hedge returns.
    """
    result = BacktestResult(name=name)
    stats = ConvictionStats()
    tracker = ConvictionTracker(conviction_config)
    prior_states: Dict[str, PositionConviction] = {}

    nav = 1_000_000.0
    peak = nav
    current_weights: Dict[str, float] = {}
    current_hedge_alloc: float = 0.0

    for d in trading_dates:
        # ── 1. Point-in-time universe ────────────────────────────────
        available: Set[str] = set()
        for iid, rets in instrument_returns.items():
            if d not in rets:
                continue
            price = all_closes.get(iid, {}).get(d, 0.0)
            if price >= MIN_PRICE_THRESHOLD:
                available.add(iid)

        # ── 2. Daily selection signal ────────────────────────────────
        base_weights = build_sector_balanced_portfolio(
            available, PORTFOLIO_MAX_NAMES, sector_mapper,
        )

        # ── 3. Sector allocator: stress + weight adjustments ─────────
        mhi = mhi_series.get(d)
        sector_decision = sector_allocator.adjust_weights(
            base_weights, d, market_mhi=mhi,
        )
        selection_weights = sector_decision.adjusted_weights
        selected_set = {
            iid for iid, w in selection_weights.items() if w > 0
        }
        stress = sector_decision.stress_level
        target_hedge = sector_decision.hedge_allocation

        if sector_decision.sick_sectors:
            result.sector_kills[d] = list(sector_decision.sick_sectors)

        # ── 4. Prices for hard-stop evaluation ───────────────────────
        prices: Dict[str, float] = {}
        for iid in set(prior_states) | selected_set:
            p = all_closes.get(iid, {}).get(d)
            if p is not None:
                prices[iid] = p

        # ── 5. Conviction update ─────────────────────────────────────
        decision = tracker.update(
            current_selection=selected_set,
            prior_states=prior_states,
            prices=prices,
            as_of_date=d,
            stress_level=stress,
        )

        # ── Track conviction statistics ──────────────────────────────
        stats.total_entries += len(decision.entries)
        stats.total_exits += len(decision.exits)

        for iid in decision.exits:
            reason = decision.exit_reasons.get(iid, "unknown")
            stats.exits_by_reason[reason] = (
                stats.exits_by_reason.get(reason, 0) + 1
            )
            prior = prior_states.get(iid)
            if prior:
                hdays = (d - prior.entry_date).days
                stats.holding_periods.append(hdays)
                stats.exit_scores.append(prior.conviction_score)
                stats.exit_records.append(ExitRecord(
                    holding_days=hdays,
                    reason=reason,
                    conviction_score=prior.conviction_score,
                    stress_level=stress.value,
                ))

        n_pos = len(decision.entries) + len(decision.holds)
        stats.daily_position_count.append(n_pos)

        if decision.position_states:
            avg_conv = (
                sum(s.conviction_score for s in decision.position_states.values())
                / len(decision.position_states)
            )
            stats.daily_avg_conviction.append(avg_conv)

        # ── 6. Build conviction-adjusted weights ─────────────────────
        conviction_weights: Dict[str, float] = {}

        # Entries: use sector-adjusted weight * entry fraction.
        for iid, frac in decision.entries.items():
            w = selection_weights.get(iid, 0.0)
            if w > 0:
                conviction_weights[iid] = w * frac

        # Holds: sector-adjusted weight * hold fraction, or average
        # weight if the instrument dropped from today's selection but
        # is still held by conviction.
        for iid, frac in decision.holds.items():
            w = selection_weights.get(iid, 0.0)
            if w > 0:
                conviction_weights[iid] = w * frac
            else:
                avg_w = (
                    sum(selection_weights.values()) / len(selection_weights)
                    if selection_weights else 0.0
                )
                conviction_weights[iid] = avg_w * frac

        # Renormalise: equity weight <= (1 - hedge).
        total = sum(conviction_weights.values())
        max_equity = 1.0 - target_hedge
        if total > max_equity and total > 0:
            scale = max_equity / total
            conviction_weights = {
                iid: w * scale for iid, w in conviction_weights.items()
            }

        target_weights = conviction_weights

        # ── 7. Apply turnover limit ──────────────────────────────────
        new_weights = apply_turnover_limit(
            current_weights, target_weights, MAX_TURNOVER_ONE_WAY,
        )

        hedge_delta = abs(target_hedge - current_hedge_alloc)
        if hedge_delta > MAX_TURNOVER_ONE_WAY:
            if target_hedge > current_hedge_alloc:
                new_hedge = current_hedge_alloc + MAX_TURNOVER_ONE_WAY
            else:
                new_hedge = current_hedge_alloc - MAX_TURNOVER_ONE_WAY
        else:
            new_hedge = target_hedge

        # ── 8. Transaction costs ─────────────────────────────────────
        tx_cost = compute_transaction_cost(
            current_weights, new_weights, TRANSACTION_COST_BPS,
        )
        tx_cost += (
            abs(new_hedge - current_hedge_alloc)
            * TRANSACTION_COST_BPS / 10_000.0
        )
        result.total_tx_cost += tx_cost * nav

        turnover = sum(
            abs(new_weights.get(iid, 0.0) - current_weights.get(iid, 0.0))
            for iid in set(current_weights) | set(new_weights)
        )
        result.total_turnover += turnover

        # ── 9. Compute returns ───────────────────────────────────────
        port_ret = 0.0
        for iid, w in new_weights.items():
            r = instrument_returns.get(iid, {}).get(d, 0.0)
            port_ret += w * r

        sh_ret = hedge_returns.get(d)
        if sh_ret is not None:
            hedge_ret = sh_ret * new_hedge
        else:
            spy_r = spy_returns.get(d, 0.0)
            hedge_ret = -0.95 * spy_r * new_hedge

        total_ret = port_ret + hedge_ret - tx_cost
        nav *= (1.0 + total_ret)
        peak = max(peak, nav)
        dd = nav / peak - 1.0

        # ── Update state ─────────────────────────────────────────────
        current_weights = new_weights
        current_hedge_alloc = new_hedge
        prior_states = decision.position_states

        result.dates.append(d)
        result.nav.append(nav)
        result.drawdown.append(dd)
        result.stress_levels.append(stress.value)

    return result, stats


# ── Results display ──────────────────────────────────────────────────

def print_results(
    baseline: BacktestResult,
    conviction: BacktestResult,
    stats: ConvictionStats,
    spy_closes: Dict[date, float],
    trading_dates: List[date],
    conviction_config: ConvictionConfig,
) -> None:
    """Print formatted backtest results and conviction analysis."""
    n_years = len(trading_dates) / 252.0

    print("\n" + "=" * 80)
    print("BACKTEST RESULTS (2007-01 to 2024-12)")
    print(f"  Tx cost: {TRANSACTION_COST_BPS} bps | "
          f"Max turnover: {MAX_TURNOVER_ONE_WAY:.0%}/day | "
          f"Hedge: actual SH.US returns")
    print("=" * 80)

    for bt in [baseline, conviction]:
        cagr = (
            (bt.nav[-1] / bt.nav[0]) ** (1.0 / n_years) - 1.0
            if n_years > 0 else 0.0
        )
        print(f"\n{bt.name}")
        print(f"  Cumulative Return: {bt.cumulative_return:+.1%}")
        print(f"  CAGR:              {cagr:+.1%}")
        print(f"  Max Drawdown:      {bt.max_drawdown:.1%}")
        print(f"  Annualised Vol:    {bt.annualised_vol:.1%}")
        print(f"  Sharpe Ratio:      {bt.sharpe:.3f}")
        if bt.total_tx_cost > 0:
            print(f"  Total Tx Cost:     ${bt.total_tx_cost:,.0f}")
            print(f"  Total Turnover:    {bt.total_turnover:,.1f}x NAV")

    # ── Conviction parameters ────────────────────────────────────────
    cfg = conviction_config
    print(f"\n{'─' * 80}")
    print("CONVICTION PARAMETERS")
    print(f"{'─' * 80}")
    print(f"  Entry credit:    {cfg.entry_credit}")
    print(f"  Build rate:      +{cfg.build_rate}/day")
    print(f"  Base decay rate: -{cfg.base_decay_rate}/day")
    print(f"  Score cap:       {cfg.score_cap}")
    print(f"  Sell threshold:  {cfg.sell_threshold}")
    print(f"  Hard stop:       -{cfg.hard_stop_pct:.0%}")
    print(f"  Scale-up days:   {cfg.scale_up_days}")
    print(f"  Entry weight:    {cfg.entry_weight_fraction:.0%}")
    for level, mult in sorted(cfg.decay_multipliers.items()):
        print(f"  Decay x{mult:.1f} during {level}")

    # ── Conviction lifecycle stats ───────────────────────────────────
    print(f"\n{'─' * 80}")
    print("CONVICTION LIFECYCLE STATS")
    print(f"{'─' * 80}")
    print(f"  Total Entries:        {stats.total_entries:,}")
    print(f"  Total Exits:          {stats.total_exits:,}")
    print(f"  Avg Holding Days:     {stats.avg_holding_days:.1f}")
    print(f"  Avg Position Count:   {stats.avg_position_count:.1f}")
    print(f"  Avg Conviction @Exit: {stats.avg_exit_score:.1f}")

    if stats.daily_avg_conviction:
        overall = sum(stats.daily_avg_conviction) / len(stats.daily_avg_conviction)
        print(f"  Avg Daily Conviction: {overall:.1f}")

    if stats.exits_by_reason:
        print(f"\n  Exit Reasons:")
        for reason, count in sorted(
            stats.exits_by_reason.items(), key=lambda x: -x[1],
        ):
            pct = count / stats.total_exits * 100 if stats.total_exits else 0.0
            print(f"    {reason:20s}: {count:5d} ({pct:.1f}%)")

    if stats.holding_periods:
        hp = stats.holding_periods
        print(f"\n  Holding Period Distribution (all exits):")
        print(f"    Min:    {min(hp):4d} days")
        print(f"    p10:    {int(np.percentile(hp, 10)):4d} days")
        print(f"    p25:    {int(np.percentile(hp, 25)):4d} days")
        print(f"    Median: {int(np.median(hp)):4d} days")
        print(f"    p75:    {int(np.percentile(hp, 75)):4d} days")
        print(f"    p90:    {int(np.percentile(hp, 90)):4d} days")
        print(f"    Max:    {max(hp):4d} days")

    # ── Duration buckets ─────────────────────────────────────────────
    if stats.exit_records:
        buckets = [
            ("1-3 days", 1, 3),
            ("4-7 days", 4, 7),
            ("8-21 days", 8, 21),
            ("22-63 days", 22, 63),
            ("64-252 days", 64, 252),
            ("253+ days", 253, 999999),
        ]
        total_exits = len(stats.exit_records)

        print(f"\n  Holding Period Buckets:")
        print(f"    {'Bucket':<16s} {'Count':>6s} {'%':>6s}  "
              f"{'Decay':>6s} {'HStop':>6s}  {'AvgScore':>8s}")
        for label, lo, hi in buckets:
            recs = [r for r in stats.exit_records if lo <= r.holding_days <= hi]
            if not recs:
                continue
            n = len(recs)
            n_decay = sum(1 for r in recs if r.reason == "conviction_decay")
            n_hard = sum(1 for r in recs if r.reason == "hard_stop")
            avg_sc = sum(r.conviction_score for r in recs) / n
            print(f"    {label:<16s} {n:6d} {n / total_exits:5.1%}  "
                  f"{n_decay:6d} {n_hard:6d}  {avg_sc:+8.1f}")

        # ── By exit reason ───────────────────────────────────────────
        for reason in ["conviction_decay", "hard_stop"]:
            recs = [r for r in stats.exit_records if r.reason == reason]
            if not recs:
                continue
            hp_r = [r.holding_days for r in recs]
            print(f"\n  Holding Period — {reason}:")
            print(f"    Count:  {len(recs):5d}")
            print(f"    Min:    {min(hp_r):4d} days")
            print(f"    p25:    {int(np.percentile(hp_r, 25)):4d} days")
            print(f"    Median: {int(np.median(hp_r)):4d} days")
            print(f"    Mean:   {sum(hp_r)/len(hp_r):6.1f} days")
            print(f"    p75:    {int(np.percentile(hp_r, 75)):4d} days")
            print(f"    Max:    {max(hp_r):4d} days")
            avg_sc = sum(r.conviction_score for r in recs) / len(recs)
            print(f"    Avg conviction @exit: {avg_sc:+.1f}")

        # ── Exits by stress regime ───────────────────────────────────
        print(f"\n  Exits by Stress Regime:")
        for level in ["NORMAL", "SECTOR_STRESS", "BROAD_STRESS", "SYSTEMIC_CRISIS"]:
            recs = [r for r in stats.exit_records if r.stress_level == level]
            if not recs:
                continue
            n = len(recs)
            n_decay = sum(1 for r in recs if r.reason == "conviction_decay")
            n_hard = sum(1 for r in recs if r.reason == "hard_stop")
            avg_hold = sum(r.holding_days for r in recs) / n
            print(f"    {level:20s}: {n:5d} exits  "
                  f"(decay={n_decay}, hstop={n_hard}, avg_hold={avg_hold:.0f}d)")

    # ── Stress distribution ──────────────────────────────────────────
    print(f"\n{'─' * 80}")
    print("STRESS LEVEL DISTRIBUTION")
    print(f"{'─' * 80}")
    stress_counts = Counter(conviction.stress_levels)
    total_days = len(conviction.stress_levels)
    for level in ["NORMAL", "SECTOR_STRESS", "BROAD_STRESS", "SYSTEMIC_CRISIS"]:
        count = stress_counts.get(level, 0)
        print(f"  {level:20s}: {count:5d} days ({count / total_days:.1%})")

    # ── Event analysis ───────────────────────────────────────────────
    print(f"\n{'─' * 80}")
    print("EVENT ANALYSIS")
    print(f"{'─' * 80}")

    for event_name, (ev_start, ev_end) in EVENTS.items():
        c_nav_start = c_nav_end = None
        for i, d in enumerate(conviction.dates):
            if d >= ev_start and c_nav_start is None:
                c_nav_start = conviction.nav[i]
            if d <= ev_end:
                c_nav_end = conviction.nav[i]

        if c_nav_start and c_nav_end:
            c_ret = c_nav_end / c_nav_start - 1.0

            spy_start_p = spy_closes.get(ev_start)
            if spy_start_p is None:
                spy_start_p = spy_closes.get(
                    ev_start - timedelta(days=1), 0.0,
                )
            spy_end_p = spy_closes.get(ev_end, 0.0)
            spy_ret = (
                (spy_end_p / spy_start_p - 1.0) if spy_start_p > 0 else 0.0
            )

            # Sector kills during this event.
            event_kills: Counter = Counter()
            for d_ev in conviction.dates:
                if ev_start <= d_ev <= ev_end and d_ev in conviction.sector_kills:
                    for s in conviction.sector_kills[d_ev]:
                        event_kills[s] += 1

            print(f"\n  {event_name} ({ev_start} to {ev_end})")
            print(f"    SPY return:        {spy_ret:+.1%}")
            print(f"    Conviction return: {c_ret:+.1%}")
            print(f"    Alpha vs SPY:      {c_ret - spy_ret:+.2%}")
            if event_kills:
                top_kills = event_kills.most_common(3)
                kills_str = ", ".join(f"{s}({n}d)" for s, n in top_kills)
                print(f"    Sector kills:      {kills_str}")


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("Loading data...")

    instruments = load_universe_instruments()
    logger.info("Universe: %d instruments", len(instruments))

    all_ids = instruments + ["SPY.US", "SH.US"]
    all_closes = load_prices(all_ids)
    logger.info("Loaded prices for %d instruments", len(all_closes))

    spy_closes = all_closes.get("SPY.US", {})

    all_returns = prices_to_returns(all_closes)
    spy_returns = all_returns.get("SPY.US", {})
    sh_returns = all_returns.get("SH.US", {})

    equity_returns = {
        iid: rets
        for iid, rets in all_returns.items()
        if iid not in ("SPY.US", "SH.US")
    }
    logger.info("Equity returns for %d instruments", len(equity_returns))

    mhi_series = compute_spy_mhi(spy_closes)
    logger.info("Computed MHI for %d dates", len(mhi_series))

    # Sector health.
    engine = SectorHealthEngine()
    engine.load(
        start=date(2007, 1, 1), end=date(2024, 12, 31), load_breadth=True,
    )
    shi = engine.compute(start=date(2007, 1, 1), end=date(2024, 12, 31))

    mapper = SectorMapper()
    mapper.load()

    sa_config = SectorAllocatorConfig()
    sector_alloc = SectorAllocator(sa_config, mapper, shi)

    # Trading dates.
    trading_dates = sorted(
        d for d in spy_returns
        if date(2007, 1, 2) <= d <= date(2024, 12, 31)
    )
    logger.info(
        "Trading dates: %d (%s to %s)",
        len(trading_dates), trading_dates[0], trading_dates[-1],
    )

    # ── SPY buy-and-hold baseline ────────────────────────────────────
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

    # ── Sector system + conviction ───────────────────────────────────
    logger.info("Running sector system + conviction...")
    conviction_config = ConvictionConfig()
    conviction_result, conviction_stats = run_backtest_conviction(
        "Sector System + Conviction",
        trading_dates,
        equity_returns,
        all_closes,
        sh_returns,
        spy_returns,
        sector_alloc,
        mapper,
        mhi_series,
        conviction_config,
    )

    # ── Display ──────────────────────────────────────────────────────
    print_results(
        baseline,
        conviction_result,
        conviction_stats,
        spy_closes,
        trading_dates,
        conviction_config,
    )


if __name__ == "__main__":
    main()
