"""Prometheus v2 – Portfolio & Risk Engine configuration models.

This module defines Pydantic models describing configuration for
Portfolio & Risk Engine instances. The shapes are aligned with the
150_portfolio_and_risk_engine specification but scoped to the current
iteration, which focuses on a simple long-only equity book.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml

from apathis.core.logging import get_logger
from pydantic import BaseModel

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONVICTION_CONFIG_PATH = PROJECT_ROOT / "configs" / "portfolio" / "conviction.yaml"


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

    # ── Assessment conviction scaling ────────────────────────────────
    # When enabled, target weights are scaled by the assessment engine's
    # confidence score (0-1).  High-confidence instruments get up to
    # max_conviction_scale * base weight; low-confidence get min_conviction_scale.
    conviction_scaling_enabled: bool = False
    conviction_scaling_min: float = 0.5   # scale at confidence=0
    conviction_scaling_max: float = 1.5   # scale at confidence=1


# ── Conviction configuration (YAML + env override) ──────────────────


@dataclass
class ConvictionDefaults:
    """Conviction mechanic defaults, loadable from YAML + env overrides.

    These defaults are applied when constructing a :class:`PortfolioConfig`
    and no per-sleeve override is present.  The :func:`load_conviction_config`
    loader follows the same 3-layer resolution pattern as
    ``load_allocator_config``:

    1. Dataclass defaults (this class)
    2. YAML file (``configs/portfolio/conviction.yaml``)
    3. Environment variable overrides for critical parameters
    """

    entry_credit: float = 5.0
    build_rate: float = 1.0
    decay_rate: float = 2.0
    score_cap: float = 20.0
    sell_threshold: float = 0.0
    hard_stop_pct: float = 0.20
    scale_up_days: int = 3
    entry_weight_fraction: float = 0.50


# ── Environment variable mapping for conviction parameters ──────────
_CONVICTION_ENV_OVERRIDES: Dict[str, tuple[str, type]] = {
    "entry_credit": ("PROMETHEUS_CONVICTION_ENTRY_CREDIT", float),
    "build_rate": ("PROMETHEUS_CONVICTION_BUILD_RATE", float),
    "decay_rate": ("PROMETHEUS_CONVICTION_DECAY_RATE", float),
    "score_cap": ("PROMETHEUS_CONVICTION_SCORE_CAP", float),
    "sell_threshold": ("PROMETHEUS_CONVICTION_SELL_THRESHOLD", float),
    "hard_stop_pct": ("PROMETHEUS_CONVICTION_HARD_STOP_PCT", float),
    "scale_up_days": ("PROMETHEUS_CONVICTION_SCALE_UP_DAYS", int),
    "entry_weight_fraction": ("PROMETHEUS_CONVICTION_ENTRY_WEIGHT_FRACTION", float),
}


def load_conviction_config(
    path: str | Path | None = None,
) -> ConvictionDefaults:
    """Load a :class:`ConvictionDefaults` from YAML + env overrides.

    Resolution order (last wins):
    1. Dataclass defaults
    2. YAML file at *path* (or ``configs/portfolio/conviction.yaml`` if exists)
    3. Environment variable overrides for critical parameters

    If the YAML file does not exist or is malformed, the dataclass defaults
    are used without error.
    """
    cfg_path = Path(path) if path is not None else DEFAULT_CONVICTION_CONFIG_PATH
    kwargs: Dict[str, Any] = {}

    # ── Step 1: Load from YAML if available ──────────────────────────
    if cfg_path.exists():
        try:
            raw = yaml.safe_load(cfg_path.read_text())
            if isinstance(raw, dict):
                valid_fields = {f.name for f in ConvictionDefaults.__dataclass_fields__.values()}
                for key, value in raw.items():
                    if key in valid_fields and value is not None:
                        kwargs[key] = value
                logger.info("Loaded conviction config from %s (%d fields)", cfg_path, len(kwargs))
        except Exception as exc:
            logger.warning("Failed to load conviction config from %s: %s", cfg_path, exc)

    # ── Step 2: Environment variable overrides ───────────────────────
    for field_name, (env_var, field_type) in _CONVICTION_ENV_OVERRIDES.items():
        env_val = os.environ.get(env_var)
        if env_val is not None:
            try:
                kwargs[field_name] = field_type(env_val)
                logger.info("Conviction config override: %s=%s (from %s)", field_name, kwargs[field_name], env_var)
            except (ValueError, TypeError) as exc:
                logger.warning("Invalid env override %s=%r: %s", env_var, env_val, exc)

    return ConvictionDefaults(**kwargs)
