"""Prometheus v2 – Backtest analyzers.

This module provides a small set of helpers for computing standard
backtest metrics from an equity curve, such as cumulative return,
max drawdown, and annualised volatility/Sharpe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import sqrt
from typing import Dict, List, Sequence

import numpy as np
from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class EquityCurvePoint:
    """Single point on an equity curve."""

    date: date
    equity: float


@dataclass
class EquityCurveAnalyzer:
    """Compute summary statistics for an equity curve.

    The implementation assumes daily sampling with ``trading_days_per_year``
    trading days per calendar year. It is robust to short curves and
    non-positive starting equity, in which case it returns zeros.
    """

    trading_days_per_year: int = 252

    def compute_metrics(self, curve: Sequence[EquityCurvePoint]) -> Dict[str, float]:
        """Return a metrics dictionary for the given equity curve."""

        if not curve:
            return {
                "cumulative_return": 0.0,
                "max_drawdown": 0.0,
                "annualised_vol": 0.0,
                "annualised_sharpe": 0.0,
            }

        curve_sorted = sorted(curve, key=lambda p: p.date)
        start_value = float(curve_sorted[0].equity)
        end_value = float(curve_sorted[-1].equity)

        if start_value <= 0.0:
            logger.warning(
                "EquityCurveAnalyzer.compute_metrics: non-positive starting equity %.4f",
                start_value,
            )
            return {
                "cumulative_return": 0.0,
                "max_drawdown": 0.0,
                "annualised_vol": 0.0,
                "annualised_sharpe": 0.0,
            }

        cumulative_return = end_value / start_value - 1.0

        # Daily returns and max drawdown in a single pass.
        daily_returns: List[float] = []
        peak_equity = start_value
        max_drawdown = 0.0
        prev_equity = start_value

        for point in curve_sorted[1:]:
            eq = float(point.equity)
            if prev_equity > 0.0:
                daily_returns.append(eq / prev_equity - 1.0)
            if eq > peak_equity:
                peak_equity = eq
            drawdown = eq / peak_equity - 1.0
            if drawdown < max_drawdown:
                max_drawdown = drawdown
            prev_equity = eq

        if daily_returns:
            mean_daily = float(np.mean(daily_returns))
            vol_daily = float(np.std(daily_returns, ddof=1)) if len(daily_returns) > 1 else 0.0
        else:
            mean_daily = 0.0
            vol_daily = 0.0

        if vol_daily > 0.0:
            annualised_vol = vol_daily * sqrt(self.trading_days_per_year)
            annualised_sharpe = (
                mean_daily * self.trading_days_per_year / annualised_vol
            )
        else:
            annualised_vol = 0.0
            annualised_sharpe = 0.0

        return {
            "cumulative_return": cumulative_return,
            "max_drawdown": max_drawdown,
            "annualised_vol": annualised_vol,
            "annualised_sharpe": annualised_sharpe,
        }
