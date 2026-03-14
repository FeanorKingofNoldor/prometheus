"""Prometheus v2 – Portfolio & Risk Engine core types.

This module defines in-memory representations for target portfolios and
risk reports produced by the Portfolio & Risk Engine. The shapes are
aligned with spec 150 but intentionally minimal for the first
implementation iteration.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict


Number = float


@dataclass(frozen=True)
class TargetPortfolio:
    """Target portfolio produced by the PortfolioEngine.

    Attributes:
        portfolio_id: Logical portfolio or book identifier
            (e.g. "US_CORE_LONG_EQ").
        as_of_date: Date of the optimization snapshot.
        weights: Mapping from instrument_id to target portfolio weight in
            NAV terms. For fully-invested long-only portfolios these sum
            to ~1.0 and are non-negative. When per-name caps bind and cash
            is allowed, they may sum to < 1.0.
        expected_return: Aggregate expected portfolio return over the
            chosen horizon. For v1 this may be a simple heuristic (e.g.
            weighted average of underlying scores).
        expected_volatility: Aggregate risk proxy for the portfolio. In
            v1 this is a placeholder that can be derived from simple STAB
            metrics or left at 0.0.
        risk_metrics: Flat dictionary of scalar risk metrics such as
            gross_exposure, net_exposure, fragility_exposure,
            number_of_names, etc.
        factor_exposures: Aggregate exposures by factor, sector, or
            other grouping keys. For v1 we primarily use sector buckets.
        constraints_status: Boolean flags indicating which constraints
            are active/binding (e.g. per-name max weight, fragility
            limits).
        metadata: Free-form diagnostics and model identifiers.
    """

    portfolio_id: str
    as_of_date: date
    weights: Dict[str, Number]
    expected_return: Number
    expected_volatility: Number
    risk_metrics: Dict[str, Number]
    factor_exposures: Dict[str, Number]
    constraints_status: Dict[str, bool]
    metadata: Dict[str, object]


@dataclass(frozen=True)
class RiskReport:
    """Basic risk report for a portfolio.

    This mirrors spec 150 but focuses on aggregated exposures and simple
    risk metrics. Scenario P&L and richer risk models can be added in a
    later iteration.
    """

    portfolio_id: str
    as_of_date: date
    exposures: Dict[str, Number]
    risk_metrics: Dict[str, Number]
    scenario_pnl: Dict[str, Number]
    metadata: Dict[str, object]