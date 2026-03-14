"""Prometheus v2 – Meta-Orchestrator core types.

This module defines small dataclasses used by the Meta-Orchestrator for
representing engine decisions, outcomes, and sleeve evaluations. The
initial implementation focuses on backtest-based sleeve evaluation; the
full decision/outcome logging surface can be extended later.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from prometheus.backtest.config import SleeveConfig


@dataclass(frozen=True)
class EngineDecision:
    """Logical decision taken by an engine (e.g. Meta-Orchestrator).

    For v1 this dataclass is thin and primarily mirrors the
    ``engine_decisions`` table schema.
    """

    decision_id: str
    engine_name: str
    run_id: str | None
    strategy_id: str | None
    market_id: str | None
    as_of_date: date
    config_id: str | None
    input_refs: Dict[str, Any] | None = None
    output_refs: Dict[str, Any] | None = None
    metadata: Dict[str, Any] | None = None


@dataclass(frozen=True)
class DecisionOutcome:
    """Realised outcome for a previously recorded decision."""

    decision_id: str
    horizon_days: int
    realized_return: float | None = None
    realized_pnl: float | None = None
    realized_drawdown: float | None = None
    realized_vol: float | None = None
    metadata: Dict[str, Any] | None = None


@dataclass(frozen=True)
class BacktestRunRecord:
    """Thin wrapper around a ``backtest_runs`` row used for meta-analysis."""

    run_id: str
    strategy_id: str
    universe_id: str | None
    config: Dict[str, Any]
    metrics: Dict[str, float]


@dataclass(frozen=True)
class SleeveEvaluation:
    """Backtest evaluation of a sleeve configuration.

    Attributes:
        run_id: Identifier of the backtest run.
        sleeve_config: Parsed :class:`SleeveConfig` reconstructed from the
            run's ``config_json``.
        metrics: Metrics dictionary from ``backtest_runs.metrics_json``.
    """

    run_id: str
    sleeve_config: "SleeveConfig"
    metrics: Dict[str, float]
