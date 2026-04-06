"""Prometheus v2 – Meta-Orchestrator (Iris) package.

This package contains the minimal Meta-Orchestrator implementation used
for evaluating and selecting sleeves based on backtest results.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import BacktestRunRecord, DecisionOutcome, EngineDecision, SleeveEvaluation

if TYPE_CHECKING:  # pragma: no cover
    # Imported lazily at runtime to avoid circular imports (meta -> backtest -> decisions -> meta).
    from prometheus.meta.engine import MetaOrchestrator as MetaOrchestrator


def __getattr__(name: str):  # pragma: no cover - exercised implicitly
    if name == "MetaOrchestrator":
        from prometheus.meta.engine import MetaOrchestrator

        return MetaOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "EngineDecision",
    "DecisionOutcome",
    "BacktestRunRecord",
    "SleeveEvaluation",
    "MetaStorage",
    "MetaOrchestrator",
]
