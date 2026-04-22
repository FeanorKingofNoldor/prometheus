"""Prometheus v2 – Risk constraints and configuration.

This module defines small, in-code risk configuration structures and
helpers for applying simple constraints such as per-name weight caps.

Later iterations can extend this to load configs from dedicated
``risk_configs`` / ``strategy_configs`` tables as described in the
planning documents.

# TODO(issue-21): Risk constraints are per-name only — add sector-level,
# gross/net exposure, correlation, and drawdown constraints. Currently the
# only constraint is max_abs_weight_per_name which misses portfolio-level
# risk limits (e.g. sector concentration, factor exposure, beta).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class StrategyRiskConfig:
    """Static risk configuration for a single strategy.

    Attributes:
        strategy_id: Logical strategy identifier.
        max_abs_weight_per_name: Maximum absolute portfolio weight per
            instrument for this strategy. A value of 0.05 corresponds to a
            5% per-name cap in a fully-invested portfolio.
    """

    strategy_id: str
    max_abs_weight_per_name: float = 0.05


_DEFAULT_CONFIGS: Dict[str, StrategyRiskConfig] = {
    # Example: conservative default for a core long-only equity strategy.
    "US_EQ_CORE_LONG_EQ": StrategyRiskConfig(
        strategy_id="US_EQ_CORE_LONG_EQ",
        max_abs_weight_per_name=0.05,
    ),

    # Allocator and hedge books often require concentrated weights in a small
    # number of hedge instruments (e.g. SH.US). We rely on the portfolio model's
    # own per-instrument cap for equities; for these strategies we allow larger
    # per-name weights so the hedge leg can actually express the intended sizing.
    "US_EQ_ALLOCATOR": StrategyRiskConfig(
        strategy_id="US_EQ_ALLOCATOR",
        max_abs_weight_per_name=1.0,
    ),
    "US_EQ_HEDGE_ETF": StrategyRiskConfig(
        strategy_id="US_EQ_HEDGE_ETF",
        max_abs_weight_per_name=1.0,
    ),

    # V12 lambda-driven long-only book: 10% per-name cap matches the
    # sleeve config (portfolio_per_instrument_max_weight: 0.10).
    "US_EQ_LONG_V12": StrategyRiskConfig(
        strategy_id="US_EQ_LONG_V12",
        max_abs_weight_per_name=0.10,
    ),
}


def _env_max_weight_per_name() -> float | None:
    """Read ``PROMETHEUS_MAX_WEIGHT_PER_NAME`` env var override.

    Returns the float value if set and valid, otherwise ``None`` so the
    caller falls back to the per-strategy default.
    """
    raw = os.environ.get("PROMETHEUS_MAX_WEIGHT_PER_NAME")
    if raw is not None:
        try:
            return float(raw)
        except (ValueError, TypeError):
            pass
    return None


def get_strategy_risk_config(strategy_id: str) -> StrategyRiskConfig:
    """Return a :class:`StrategyRiskConfig` for ``strategy_id``.

    For now this looks up a small in-code mapping and falls back to a
    generic configuration if no specific entry is found.

    If the ``PROMETHEUS_MAX_WEIGHT_PER_NAME`` environment variable is set,
    it overrides the ``max_abs_weight_per_name`` for **all** strategies.
    """

    cfg = _DEFAULT_CONFIGS.get(strategy_id)
    if cfg is None:
        cfg = StrategyRiskConfig(strategy_id=strategy_id)

    env_cap = _env_max_weight_per_name()
    if env_cap is not None:
        cfg = StrategyRiskConfig(
            strategy_id=cfg.strategy_id,
            max_abs_weight_per_name=env_cap,
        )
    return cfg


def apply_per_name_limit(
    weight: float,
    config: StrategyRiskConfig,
    *,
    eps: float = 1e-9,
) -> Tuple[float, str | None]:
    """Apply a simple per-name absolute weight cap.

    Args:
        weight: Proposed portfolio weight for a single instrument.
        config: Strategy-level risk configuration.
        eps: Numerical tolerance for comparisons.

    Returns:
        A tuple ``(adjusted_weight, reason)`` where ``reason`` is
        ``None`` if the weight was unchanged, or a short string such as
        ``"REJECTED_PER_NAME_CAP"`` or ``"CAPPED_PER_NAME"`` when the
        proposed weight violates the configured cap.
    """

    cap = abs(config.max_abs_weight_per_name)

    # If cap is effectively zero, reject any non-zero position.
    if cap <= eps:
        if abs(weight) <= eps:
            return 0.0, None
        return 0.0, "REJECTED_PER_NAME_CAP"

    if abs(weight) <= cap + eps:
        return weight, None

    adjusted = cap if weight > 0.0 else -cap
    return adjusted, "CAPPED_PER_NAME"
