"""Prometheus v2 – Canonical sleeve catalog helpers.

This module defines small helpers for constructing canonical
``SleeveConfig`` grids for common strategies/markets. These helpers are
intended for use by backtest campaigns and Meta-Orchestrator routines
when sweeping a space of sleeves.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from prometheus.backtest.config import SleeveConfig


DEFAULT_CORE_HORIZONS: Sequence[int] = (5, 21, 63)


@dataclass(frozen=True)
class SleeveTemplate:
    """Template for generating a sleeve within a grid.

    Attributes:
        horizon_days: Assessment horizon in trading days.
        suffix: Short identifier used to distinguish sleeves in IDs.
    """

    horizon_days: int
    suffix: str


def build_core_long_sleeves(strategy_id: str, market_id: str) -> List[SleeveConfig]:
    """Return a small grid of core long-only sleeves for a strategy/market.

    The generated sleeves share the same ``strategy_id`` and ``market_id``
    but differ in assessment horizon and identifiers. For example, for
    ``strategy_id="US_EQ_CORE_LONG_EQ"`` and ``market_id="US_EQ"`` this
    will construct sleeves such as::

        US_EQ_CORE_LONG_EQ_H5
        US_EQ_CORE_LONG_EQ_H21
        US_EQ_CORE_LONG_EQ_H63

    The exact IDs are not coupled to any external config tables; they are
    simply used as keys in the backtesting and portfolio/universe tables.
    """

    templates: Sequence[SleeveTemplate] = (
        SleeveTemplate(horizon_days=5, suffix="H5"),
        SleeveTemplate(horizon_days=21, suffix="H21"),
        SleeveTemplate(horizon_days=63, suffix="H63"),
    )

    sleeves: List[SleeveConfig] = []
    for tpl in templates:
        base = f"{strategy_id}_{tpl.suffix}"
        sleeves.append(
            SleeveConfig(
                sleeve_id=base,
                strategy_id=strategy_id,
                market_id=market_id,
                universe_id=f"{base}_UNIVERSE",
                portfolio_id=f"{base}_PORTFOLIO",
                assessment_strategy_id=f"{base}_ASSESS",
                assessment_horizon_days=tpl.horizon_days,
            )
        )

    return sleeves
