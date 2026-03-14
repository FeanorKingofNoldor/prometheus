"""Prometheus v2 – Portfolio & Risk Engine configuration models.

This module defines Pydantic models describing configuration for
Portfolio & Risk Engine instances. The shapes are aligned with the
150_portfolio_and_risk_engine specification but scoped to the current
iteration, which focuses on a simple long-only equity book.
"""

from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel


class PortfolioConfig(BaseModel):
    """Configuration for a portfolio or book optimisation problem.

    Attributes:
        portfolio_id: Identifier of the portfolio/book (e.g.
            "US_CORE_LONG_EQ").
        strategies: Strategy identifiers feeding this portfolio.
        markets: List of markets this portfolio draws instruments from.
        base_currency: Base currency for reporting and risk metrics.
        risk_model_id: Identifier of the risk model used for this
            portfolio (e.g. "basic-longonly-v1").
        optimizer_type: Optimiser type (e.g. "SIMPLE_LONG_ONLY").
        risk_aversion_lambda: Risk aversion parameter; not fully used in
            the initial heuristic model but kept for future QP/SOCP
            optimisers.
        leverage_limit: Maximum allowed leverage; for long-only this is
            typically 1.0.
        gross_exposure_limit: Maximum gross exposure; for long-only this
            is typically 1.0 as well.
        per_instrument_max_weight: Maximum allowed weight per
            instrument; enforced as a simple cap in the basic model.
        max_names: Optional cap on the number of instruments that may
            receive non-zero weights. If None, no explicit top-K culling
            is applied at the portfolio stage.
        hysteresis_buffer: Optional rank buffer used when max_names is
            set to reduce churn. If > 0 and holdings information is
            available, names already held are allowed to remain in the
            portfolio until they fall below rank (max_names + buffer).
        sector_limits: Optional per-sector exposure caps.
        country_limits: Optional per-country exposure caps.
        factor_limits: Optional factor exposure bounds.
        fragility_exposure_limit: Maximum allowed aggregate weight in
            fragile / soft-target names; currently used as a diagnostic
            threshold.
        turnover_limit: Maximum one-day turnover; not yet enforced in
            the v1 model.
        cost_model_id: Identifier for the trading cost model used in
            more advanced optimisers.
        scenario_risk_scenario_set_ids: Optional list of scenario_set_id
            values that should be used when computing scenario-based
            portfolio risk. If empty, scenario P&L is not computed by the
            core model (it can still be backfilled via dedicated
            research CLIs).
    """

    portfolio_id: str
    strategies: List[str]
    markets: List[str]
    base_currency: str

    risk_model_id: str
    optimizer_type: str
    risk_aversion_lambda: float

    leverage_limit: float
    gross_exposure_limit: float
    per_instrument_max_weight: float

    # Optional concentration control.
    max_names: int | None = None

    # Optional turnover control for top-K portfolios.
    hysteresis_buffer: int | None = None

    sector_limits: Dict[str, float]
    country_limits: Dict[str, float]
    factor_limits: Dict[str, float]

    fragility_exposure_limit: float
    turnover_limit: float
    cost_model_id: str

    # Score concentration: raise raw scores to this power before
    # normalising into weights.  1.0 = linear (original behaviour),
    # 2.0 = quadratic (top names get disproportionately more weight).
    score_concentration_power: float = 1.0

    # When True, after per-instrument caps are applied the residual
    # weight (that would otherwise sit in cash) is redistributed
    # proportionally to the uncapped names.  This ensures the portfolio
    # is fully invested up to the gross_exposure_limit.
    redistribute_capped_residual: bool = True

    # Scenario-based risk is optional and off by default; scenarios can
    # also be applied via offline backfill scripts if preferred.
    scenario_risk_scenario_set_ids: List[str] = []

    # ── Conviction-based position lifecycle ───────────────────────────
    # When enabled, the ConvictionPortfolioModel wraps the inner model
    # and applies conviction-based entry/exit logic.
    conviction_enabled: bool = False
    conviction_entry_credit: float = 5.0
    conviction_build_rate: float = 1.0
    conviction_decay_rate: float = 2.0
    conviction_score_cap: float = 20.0
    conviction_sell_threshold: float = 0.0
    conviction_hard_stop_pct: float = 0.20
    conviction_scale_up_days: int = 3
    conviction_entry_weight_fraction: float = 0.50
