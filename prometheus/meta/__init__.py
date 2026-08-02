"""Prometheus v2 – Meta-Orchestrator (Iris) package.

Decision tracking, outcome evaluation, proposal generation, and the
daily autopilot loop.  (The legacy sleeve-grid ``MetaOrchestrator``
engine was removed 2026-08 with the core+wheel simplification.)
"""

from __future__ import annotations

from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import BacktestRunRecord, DecisionOutcome, EngineDecision, SleeveEvaluation

__all__ = [
    "EngineDecision",
    "DecisionOutcome",
    "BacktestRunRecord",
    "SleeveEvaluation",
    "MetaStorage",
]
