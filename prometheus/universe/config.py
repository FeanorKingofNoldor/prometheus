"""Prometheus v2 – Universe Engine configuration models.

This module defines Pydantic models describing configuration for
Universe Engine instances. The primary consumer is the basic equity
universe model, which uses these settings to enforce liquidity and
quality constraints, global/sector caps, and tiering behaviour.

The shapes are aligned with the 140_universe_engine specification but do
not yet handle persistence in ``engine_configs``; for this iteration we
primarily use them as strongly-typed in-memory configs constructed by the
pipeline.
"""

from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field


class UniverseConfig(BaseModel):
    """Configuration for a Universe Engine instance.

    Attributes:
        strategy_id: Strategy identifier this config applies to (e.g.
            ``"US_CORE_LONG_EQ"``).
        markets: List of market identifiers to include (e.g.
            ``["US_EQ"]``).
        max_universe_size: Maximum number of instruments allowed in the
            effective universe (CORE + SATELLITE tiers). A non-positive
            value disables the global cap.
        min_liquidity_adv: Minimum average daily volume (ADV) required
            for inclusion, expressed in units of shares traded.
        min_price: Minimum allowed last closing price; instruments
            trading below this value are hard-excluded.
        sector_max_names: Maximum number of instruments per sector.
            A non-positive value disables sector caps.
        hard_exclusion_list: Explicit instrument_ids to always exclude
            from the universe.
        issuer_exclusion_list: Explicit issuer_ids to always exclude from
            the universe.
        allow_soft_targets_long: Whether soft-target / fragile names are
            allowed on the long side; this primarily influences how STAB
            filters are configured for long-only universes.
        allow_soft_targets_short: Whether soft-target / fragile names may
            appear on the short side universe; kept for completeness even
            though short universes are not yet implemented in this
            iteration.
        universe_model_id: Identifier of the universe selection model or
            rule-set (e.g. ``"basic-equity-v1"``). Useful for auditing
            and for distinguishing future model variants.
        regime_region: Optional region identifier to use when querying
            regime risk (e.g. "US", "GLOBAL"). If omitted, the pipeline
            or BasicUniverseModel defaults will be used.
        regime_risk_alpha: Strength of the global regime risk modifier.
            A value of 0.0 disables regime risk integration. Positive
            values shrink scores when the regime risk score is high;
            negative values would upweight scores in stressed regimes.
        regime_risk_horizon_steps: Horizon in regime transition steps for
            the risk forecast (typically trading days).
        stability_risk_alpha: Strength of the per-instrument STAB
            state-change risk modifier. A value of 0.0 disables STAB risk
            integration. Positive values shrink scores when the STAB
            state-change risk score is high.
        stability_risk_horizon_steps: Horizon in soft-target transition
            steps for the STAB risk forecast (typically trading days).
    """

    strategy_id: str
    markets: List[str]
    max_universe_size: int
    min_liquidity_adv: float
    min_price: float
    sector_max_names: int
    hard_exclusion_list: List[str] = Field(default_factory=list)
    issuer_exclusion_list: List[str] = Field(default_factory=list)
    allow_soft_targets_long: bool = False
    allow_soft_targets_short: bool = True
    universe_model_id: str
    regime_region: str | None = None
    regime_risk_alpha: float = 0.0
    regime_risk_horizon_steps: int = 1
    stability_risk_alpha: float = 0.0
    stability_risk_horizon_steps: int = 1
