"""Prometheus v2 – Synthetic Scenario Engine types.

This module defines request/response types used by the Synthetic
Scenario Engine for generating and managing synthetic scenario sets
and full synthetic market realities.

The shapes are aligned with the 170_synthetic_scenarios specification.
"""

from __future__  import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Scenario-level types (original)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScenarioRequest:
    """Request describing a scenario set to be generated.

    Attributes:
        name: Human-readable name of the scenario set.
        description: Longer free-form description of purpose and
            construction.
        category: Scenario category, e.g. "HISTORICAL" or "BOOTSTRAP".
        horizon_days: Number of days in each scenario path (H).
        num_paths: Number of distinct paths to generate.
        markets: Market identifiers (e.g. ["US_EQ"]) used to resolve the
            base universe of instruments.
        base_date_start: Optional start date for the historical window
            used as the sampling base.
        base_date_end: Optional end date for the historical window used
            as the sampling base.
        regime_filter: Optional regime labels to condition sampling on;
            unused in the first iteration but reserved for future
            extensions.
        universe_filter: Optional free-form filter description (e.g.
            sector/asset-class constraints).
        generator_spec: Additional generator parameters (e.g. block
            length for bootstraps); for simple historical windows this
            can remain empty.
    """

    name: str
    description: str
    category: str
    horizon_days: int
    num_paths: int
    markets: List[str]
    base_date_start: Optional[date] = None
    base_date_end: Optional[date] = None
    regime_filter: Optional[List[str]] = None
    universe_filter: Optional[Dict[str, object]] = None
    generator_spec: Optional[Dict[str, object]] = None


@dataclass(frozen=True)
class ScenarioSetRef:
    """Lightweight reference to a stored scenario set."""

    scenario_set_id: str
    name: str
    category: str
    horizon_days: int
    num_paths: int


# ---------------------------------------------------------------------------
# Full market-reality types (new)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RealityConfig:
    """Configuration for generating a synthetic market reality.

    A *reality* is a complete alternative market history that the C++
    backtester (or Python sleeve pipeline) can consume directly.  It
    contains full OHLCV price panels, sector ETF prices, derived
    fragility scores, and optionally lambda score tables.

    Attributes:
        name: Human-readable label for the reality set.
        category: Generation method.  Supported values:
            ``BLOCK_BOOTSTRAP`` – block-bootstrap of daily returns with
            cross-sectional preservation.
        horizon_days: Number of trading days per synthetic reality.
        num_realities: How many distinct realities to generate.
        block_length: Block length (trading days) for the bootstrap.
        markets: Market identifiers used to resolve instruments.
        base_date_start: Start of historical window to sample from.
        base_date_end: End of historical window to sample from.
        seed: Optional RNG seed for reproducibility.
        include_fragility: Whether to derive synthetic fragility scores.
        lambda_mode: How to handle lambda scores:
            ``passthrough`` – use real lambda CSV unchanged.
            ``noisy`` – add Gaussian noise (controlled by
            ``lambda_noise_std``).
            ``shuffle`` – permute dates within each cluster.
            ``none`` – omit lambda scores entirely.
        lambda_noise_std: Std-dev of noise added when lambda_mode is
            ``noisy``.
        lambda_csv_path: Path to the real lambda scores CSV used as
            basis for passthrough / noisy / shuffle modes.
        base_price: Starting price level for synthetic instruments.
    """

    name: str
    category: str = "BLOCK_BOOTSTRAP"
    horizon_days: int = 252 * 5  # ~5 years
    num_realities: int = 10
    block_length: int = 20
    markets: List[str] = field(default_factory=lambda: ["US_EQ"])
    base_date_start: Optional[date] = None
    base_date_end: Optional[date] = None
    seed: Optional[int] = None
    include_fragility: bool = True
    lambda_mode: str = "passthrough"  # passthrough | noisy | shuffle | none
    lambda_noise_std: float = 0.5
    lambda_csv_path: Optional[str] = None
    base_price: float = 100.0


@dataclass
class SyntheticReality:
    """A complete synthetic market reality.

    Contains all data artefacts the C++ backtester needs to run a full
    factorial backtest on an alternative universe.

    Attributes:
        reality_id: Unique identifier for this reality.
        config: The :class:`RealityConfig` that produced this reality.
        prices_df: Full price panel with columns
            ``[instrument_id, trade_date, open, high, low, close,
            adjusted_close, volume, currency, metadata]``.
        instrument_ids: Ordered list of synthetic instrument IDs
            (including sector ETF IDs).
        sector_etf_ids: Mapping from synthetic ETF instrument_id to
            sector name.
        real_to_synth: Mapping from real instrument_id to synthetic
            instrument_id.
        fragility_df: Optional DataFrame with columns
            ``[as_of_date, fragility_score]``.
        lambda_df: Optional DataFrame matching the C++ lambda CSV
            schema.
        metadata: Arbitrary metadata (generation stats, etc.).
    """

    reality_id: str
    config: RealityConfig
    prices_df: Optional[pd.DataFrame] = None  # None when using C++ path
    instrument_ids: List[str] = field(default_factory=list)
    sector_etf_ids: Dict[str, str] = field(default_factory=dict)  # synth_etf_id -> sector_name
    real_to_synth: Dict[str, str] = field(default_factory=dict)  # real_id -> synth_id
    fragility_df: Optional[pd.DataFrame] = None
    lambda_df: Optional[pd.DataFrame] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # C++ acceleration fields (set when generated via C++ engine).
    cpp_arrays: Optional[Dict[str, Any]] = field(default=None, repr=False)
    trade_dates: Optional[List] = None  # date objects for the synthetic timeline
    trade_dates_int: Optional[List[int]] = None  # YYYYMMDD ints for C++ DB writer
    panel_ids: Optional[List[str]] = None  # real instrument IDs in panel order
    panel_sectors: Optional[List[str]] = None  # sectors per instrument
