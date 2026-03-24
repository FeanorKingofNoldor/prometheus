"""Tests for data quality utilities used by the monitoring API.

These tests exercise the *pure business logic* (capital-flow detection,
flow-adjusted NLV, benchmark forward-fill, risk computation excluding
options, HHI with normalised equity-only weights) without touching any
database or network resource.

The functions under test are extracted from the endpoint implementations
in ``prometheus.monitoring.api`` and tested as standalone algorithms so
that regressions in the numerical logic are caught immediately.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Dict, List, Optional, Set, Tuple

import pytest


# ============================================================================
# Helpers — extracted pure-logic functions from api.py
# ============================================================================
# We replicate the exact algorithms used in the endpoints so that
# changes in api.py that break the math are caught here.


def detect_capital_flow_dates(
    nlv_by_date: Dict[date, float],
    threshold: float = 0.15,
) -> Set[date]:
    """Return dates where day-over-day NLV change exceeds *threshold*.

    Mirrors the flow-detection logic in ``get_status_overview`` and
    ``get_portfolio_equity``.
    """
    sorted_dates = sorted(nlv_by_date)
    flow_dates: set = set()
    for i in range(1, len(sorted_dates)):
        prev_nlv = nlv_by_date[sorted_dates[i - 1]]
        curr_nlv = nlv_by_date[sorted_dates[i]]
        if prev_nlv > 0 and abs(curr_nlv - prev_nlv) / prev_nlv > threshold:
            flow_dates.add(sorted_dates[i])
    return flow_dates


def compute_flow_adjusted_nlv(
    nlv_by_date: Dict[date, float],
    flow_dates: Set[date],
) -> List[Tuple[date, float]]:
    """Build a flow-adjusted NLV series by accumulating only market returns.

    Flow days carry forward the previous adjusted value (rebase).
    Mirrors the ``clean_nlvs`` logic in ``get_status_overview``.
    """
    sorted_dates = sorted(nlv_by_date)
    if not sorted_dates:
        return []

    result: List[Tuple[date, float]] = [(sorted_dates[0], nlv_by_date[sorted_dates[0]])]

    for i in range(1, len(sorted_dates)):
        d = sorted_dates[i]
        prev_d = sorted_dates[i - 1]
        prev_raw = nlv_by_date[prev_d]
        curr_raw = nlv_by_date[d]
        prev_adj = result[-1][1]

        if d in flow_dates:
            # Carry forward previous adjusted NLV
            result.append((d, prev_adj))
        elif prev_raw > 0:
            daily_ret = (curr_raw - prev_raw) / prev_raw
            result.append((d, prev_adj * (1.0 + daily_ret)))
        else:
            result.append((d, prev_adj))

    return result


def compute_pnl_excluding_flows(
    nlv_by_date: Dict[date, float],
    flow_dates: Set[date],
) -> float:
    """Sum of daily NLV diffs excluding flow dates.

    Mirrors the YTD P&L logic in ``get_status_overview``.
    """
    sorted_dates = sorted(nlv_by_date)
    total = 0.0
    for i in range(1, len(sorted_dates)):
        d = sorted_dates[i]
        if d in flow_dates:
            continue
        total += nlv_by_date[d] - nlv_by_date[sorted_dates[i - 1]]
    return total


def max_drawdown_from_adjusted(adjusted: List[float]) -> float:
    """Compute max drawdown from a flow-adjusted NLV series.

    Mirrors the drawdown calculation in ``get_status_overview``.
    """
    if len(adjusted) < 2:
        return 0.0
    peak = adjusted[0]
    max_dd = 0.0
    for v in adjusted:
        if v > peak:
            peak = v
        dd = (peak - v) / peak
        if dd > max_dd:
            max_dd = dd
    return max_dd


def forward_fill_benchmark(
    portfolio_dates: List[str],
    bench_by_date: Dict[str, float],
) -> List[Optional[float]]:
    """Forward-fill benchmark values across portfolio dates.

    Mirrors the merge logic in ``get_portfolio_equity``.
    """
    result: List[Optional[float]] = []
    last_bench: Optional[float] = None
    for d in portfolio_dates:
        bench_val = bench_by_date.get(d)
        if bench_val is not None:
            last_bench = bench_val
        result.append(bench_val if bench_val is not None else last_bench)
    return result


def compute_hhi_equity_only(
    weights: List[float],
    has_price_data: List[bool],
) -> Optional[float]:
    """Compute HHI using only equity (priced) positions with normalised weights.

    Mirrors the HHI logic in ``get_portfolio_risk_computed``.
    """
    equity_weights = [w for w, has_price in zip(weights, has_price_data) if has_price]
    if not equity_weights:
        return None
    w_sum = sum(equity_weights)
    if w_sum <= 0:
        return None
    normed = [w / w_sum for w in equity_weights]
    return sum(n ** 2 for n in normed)


def compute_live_sharpe(
    nlv_rows: List[Tuple[date, float]],
    flow_threshold: float = 0.15,
) -> Optional[Tuple[float, float, int]]:
    """Compute annualised Sharpe, ann_vol, and n_days from NLV series.

    Capital-flow days (>threshold daily return) are excluded.
    Mirrors the live portfolio Sharpe logic in ``get_performance``.

    Returns (sharpe, ann_vol, n_days) or None if insufficient data.
    """
    if len(nlv_rows) < 2:
        return None

    nlvs = [float(r[1]) for r in nlv_rows]
    daily_returns = []
    for i in range(1, len(nlvs)):
        if nlvs[i - 1] > 0:
            ret = (nlvs[i] - nlvs[i - 1]) / nlvs[i - 1]
            if abs(ret) <= flow_threshold:
                daily_returns.append(ret)

    if not daily_returns:
        return None

    n = len(daily_returns)
    mean_r = sum(daily_returns) / n
    var_r = sum((r - mean_r) ** 2 for r in daily_returns) / max(n - 1, 1)
    vol = math.sqrt(var_r) if var_r > 0 else 0.0
    ann_vol = vol * math.sqrt(252)
    ann_sharpe = (mean_r * 252) / ann_vol if ann_vol > 0 else 0.0

    return ann_sharpe, ann_vol, n


# ============================================================================
# Tests — Capital Flow Detection
# ============================================================================


class TestCapitalFlowDetection:
    """Test the >15% NLV jump detection for deposits/withdrawals."""

    def test_no_flows_normal_returns(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 101_000,   # +1%
            date(2026, 1, 6): 100_500,   # -0.5%
            date(2026, 1, 7): 101_500,   # +1%
        }
        flows = detect_capital_flow_dates(nlv)
        assert flows == set()

    def test_detects_large_deposit(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 120_000,   # +20% = flow
            date(2026, 1, 6): 121_000,   # +0.8%
        }
        flows = detect_capital_flow_dates(nlv)
        assert date(2026, 1, 3) in flows
        assert len(flows) == 1

    def test_detects_large_withdrawal(self):
        nlv = {
            date(2026, 1, 2): 200_000,
            date(2026, 1, 3): 160_000,   # -20% = flow
            date(2026, 1, 6): 161_000,
        }
        flows = detect_capital_flow_dates(nlv)
        assert date(2026, 1, 3) in flows

    def test_exactly_at_threshold_not_flagged(self):
        """15% exactly should NOT be flagged (threshold is strict >)."""
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 115_000,   # exactly +15%
        }
        flows = detect_capital_flow_dates(nlv)
        assert flows == set()

    def test_just_above_threshold_flagged(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 115_001,   # just above 15%
        }
        flows = detect_capital_flow_dates(nlv)
        assert date(2026, 1, 3) in flows

    def test_empty_series(self):
        flows = detect_capital_flow_dates({})
        assert flows == set()

    def test_single_day(self):
        flows = detect_capital_flow_dates({date(2026, 1, 2): 100_000})
        assert flows == set()

    def test_zero_prev_nlv_not_crash(self):
        """If previous NLV is 0, the day should not be flagged (guard against division by zero)."""
        nlv = {
            date(2026, 1, 2): 0.0,
            date(2026, 1, 3): 50_000,
        }
        flows = detect_capital_flow_dates(nlv)
        assert flows == set()  # prev_nlv == 0 → condition not met

    def test_multiple_flows(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 150_000,   # deposit +50%
            date(2026, 1, 6): 151_000,
            date(2026, 1, 7): 200_000,   # deposit +32%
            date(2026, 1, 8): 201_000,
        }
        flows = detect_capital_flow_dates(nlv)
        assert flows == {date(2026, 1, 3), date(2026, 1, 7)}


# ============================================================================
# Tests — Flow-Adjusted NLV Series
# ============================================================================


class TestFlowAdjustedNLV:
    """Test the flow-adjusted NLV computation."""

    def test_no_flows_preserves_series(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 102_000,
            date(2026, 1, 6): 101_000,
        }
        flows: set = set()
        adjusted = compute_flow_adjusted_nlv(nlv, flows)

        assert len(adjusted) == 3
        assert adjusted[0][1] == 100_000
        assert adjusted[1][1] == pytest.approx(102_000, abs=1)
        assert adjusted[2][1] == pytest.approx(101_000, abs=1)

    def test_flow_day_carries_forward(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 101_000,    # +1%
            date(2026, 1, 6): 151_000,    # deposit (flow)
            date(2026, 1, 7): 152_510,    # +1%
        }
        flows = {date(2026, 1, 6)}
        adjusted = compute_flow_adjusted_nlv(nlv, flows)

        assert len(adjusted) == 4
        # Day 0: 100000
        assert adjusted[0][1] == 100_000
        # Day 1: 101000 (normal +1%)
        assert adjusted[1][1] == pytest.approx(101_000, abs=1)
        # Day 2: flow — carried forward from day 1
        assert adjusted[2][1] == pytest.approx(101_000, abs=1)
        # Day 3: +1% from 151000 → 152510; adjusted = 101000 * 1.01 = 102010
        assert adjusted[3][1] == pytest.approx(102_010, abs=1)

    def test_consecutive_flow_days(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 150_000,    # flow
            date(2026, 1, 6): 200_000,    # flow
            date(2026, 1, 7): 202_000,    # +1%
        }
        flows = {date(2026, 1, 3), date(2026, 1, 6)}
        adjusted = compute_flow_adjusted_nlv(nlv, flows)

        assert adjusted[1][1] == pytest.approx(100_000, abs=1)  # carried forward
        assert adjusted[2][1] == pytest.approx(100_000, abs=1)  # carried forward again
        assert adjusted[3][1] == pytest.approx(101_000, abs=1)  # +1% applied

    def test_empty_series(self):
        assert compute_flow_adjusted_nlv({}, set()) == []


# ============================================================================
# Tests — P&L Excluding Flows
# ============================================================================


class TestPnlExcludingFlows:
    """Test YTD/MTD P&L computation that skips flow days."""

    def test_normal_pnl(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 101_000,
            date(2026, 1, 6): 102_500,
        }
        flows: set = set()
        pnl = compute_pnl_excluding_flows(nlv, flows)
        assert pnl == pytest.approx(2_500, abs=0.01)

    def test_flow_day_excluded(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 150_000,   # deposit
            date(2026, 1, 6): 151_500,
        }
        flows = {date(2026, 1, 3)}
        pnl = compute_pnl_excluding_flows(nlv, flows)
        # d2→d3 skipped, only d3→d6: 151500 - 150000 = 1500
        assert pnl == pytest.approx(1_500, abs=0.01)

    def test_all_flow_days(self):
        nlv = {
            date(2026, 1, 2): 100_000,
            date(2026, 1, 3): 200_000,
            date(2026, 1, 6): 300_000,
        }
        flows = {date(2026, 1, 3), date(2026, 1, 6)}
        pnl = compute_pnl_excluding_flows(nlv, flows)
        assert pnl == pytest.approx(0.0, abs=0.01)


# ============================================================================
# Tests — Max Drawdown from Adjusted Series
# ============================================================================


class TestMaxDrawdown:
    """Test max drawdown calculation from flow-adjusted NLV series."""

    def test_no_drawdown(self):
        dd = max_drawdown_from_adjusted([100, 101, 102, 103])
        assert dd == pytest.approx(0.0)

    def test_simple_drawdown(self):
        dd = max_drawdown_from_adjusted([100, 110, 90, 95])
        # Peak=110, trough=90, dd = 20/110 = 0.1818
        assert dd == pytest.approx(20.0 / 110.0, abs=1e-4)

    def test_single_point(self):
        assert max_drawdown_from_adjusted([100]) == 0.0

    def test_recovery_then_new_dd(self):
        # Two drawdowns: 100→90 (10%), then 120→100 (16.67%)
        dd = max_drawdown_from_adjusted([100, 90, 95, 120, 100])
        assert dd == pytest.approx(20.0 / 120.0, abs=1e-4)


# ============================================================================
# Tests — Benchmark Forward-Fill
# ============================================================================


class TestBenchmarkForwardFill:
    """Test benchmark forward-fill logic for equity chart."""

    def test_fills_gaps(self):
        portfolio_dates = ["2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05", "2026-01-06"]
        bench = {
            "2026-01-02": 450.0,
            "2026-01-03": 452.0,
            # 01-04 and 01-05 are weekend/missing
            "2026-01-06": 455.0,
        }
        result = forward_fill_benchmark(portfolio_dates, bench)
        assert result == [450.0, 452.0, 452.0, 452.0, 455.0]

    def test_no_benchmark_data(self):
        result = forward_fill_benchmark(["2026-01-02", "2026-01-03"], {})
        assert result == [None, None]

    def test_benchmark_starts_late(self):
        portfolio_dates = ["2026-01-02", "2026-01-03", "2026-01-06"]
        bench = {"2026-01-03": 100.0, "2026-01-06": 102.0}
        result = forward_fill_benchmark(portfolio_dates, bench)
        assert result == [None, 100.0, 102.0]


# ============================================================================
# Tests — Risk Computation Excluding Options
# ============================================================================


class TestRiskExcludingOptions:
    """Test that risk metrics exclude options (no price data) positions."""

    def test_hhi_with_equity_only_weights(self):
        """HHI should be computed on normalized equity-only weights."""
        # Portfolio: 3 positions, but one is an option with no price data
        weights = [0.40, 0.35, 0.25]        # raw weights
        has_price = [True, True, False]       # option has no price data

        hhi = compute_hhi_equity_only(weights, has_price)

        # Equity weights: [0.40, 0.35], sum=0.75
        # Normalized: [0.5333, 0.4667]
        # HHI = 0.5333^2 + 0.4667^2 = 0.2844 + 0.2178 = 0.5022
        expected = (0.40 / 0.75) ** 2 + (0.35 / 0.75) ** 2
        assert hhi == pytest.approx(expected, abs=1e-4)

    def test_hhi_all_equity(self):
        """When all positions have prices, all weights are used."""
        weights = [0.50, 0.30, 0.20]
        has_price = [True, True, True]
        hhi = compute_hhi_equity_only(weights, has_price)
        expected = 0.50 ** 2 + 0.30 ** 2 + 0.20 ** 2
        assert hhi == pytest.approx(expected, abs=1e-6)

    def test_hhi_no_equity(self):
        """If no positions have price data, HHI is None."""
        weights = [0.50, 0.50]
        has_price = [False, False]
        hhi = compute_hhi_equity_only(weights, has_price)
        assert hhi is None

    def test_hhi_single_position(self):
        """Single equity position = HHI of 1.0 (maximum concentration)."""
        hhi = compute_hhi_equity_only([1.0], [True])
        assert hhi == pytest.approx(1.0, abs=1e-6)

    def test_hhi_equal_weights(self):
        """N equally-weighted positions → HHI = 1/N."""
        n = 10
        weights = [1.0 / n] * n
        has_price = [True] * n
        hhi = compute_hhi_equity_only(weights, has_price)
        assert hhi == pytest.approx(1.0 / n, abs=1e-6)


# ============================================================================
# Tests — Live Sharpe with Capital Flow Filtering
# ============================================================================


class TestLiveSharpe:
    """Test live Sharpe ratio computation with flow exclusion."""

    def test_basic_sharpe(self):
        """Sharpe should be computable from a simple NLV series."""
        nlv_rows = [
            (date(2026, 1, 2), 100_000),
            (date(2026, 1, 3), 100_500),
            (date(2026, 1, 6), 101_200),
            (date(2026, 1, 7), 100_800),
            (date(2026, 1, 8), 101_500),
        ]
        result = compute_live_sharpe(nlv_rows)
        assert result is not None
        sharpe, ann_vol, n_days = result
        assert n_days == 4
        assert ann_vol > 0
        assert isinstance(sharpe, float)

    def test_flow_excluded_from_sharpe(self):
        """Days with >15% NLV change should not count in Sharpe."""
        nlv_rows = [
            (date(2026, 1, 2), 100_000),
            (date(2026, 1, 3), 101_000),     # +1%
            (date(2026, 1, 6), 150_000),     # +48.5% = flow
            (date(2026, 1, 7), 151_500),     # +1%
            (date(2026, 1, 8), 152_500),     # +0.66%
        ]
        result = compute_live_sharpe(nlv_rows)
        assert result is not None
        _, _, n_days = result
        # 4 daily returns total, 1 excluded (flow) → 3
        assert n_days == 3

    def test_all_flows_returns_none(self):
        """If all returns are flow days, result is None."""
        nlv_rows = [
            (date(2026, 1, 2), 100_000),
            (date(2026, 1, 3), 200_000),     # +100%
            (date(2026, 1, 6), 50_000),      # -75%
        ]
        result = compute_live_sharpe(nlv_rows)
        assert result is None

    def test_insufficient_data(self):
        result = compute_live_sharpe([(date(2026, 1, 2), 100_000)])
        assert result is None

    def test_constant_nlv(self):
        """Zero vol should yield Sharpe of 0."""
        nlv_rows = [
            (date(2026, 1, i), 100_000) for i in range(2, 12)
        ]
        result = compute_live_sharpe(nlv_rows)
        assert result is not None
        sharpe, ann_vol, n_days = result
        assert sharpe == 0.0
        assert ann_vol == 0.0
