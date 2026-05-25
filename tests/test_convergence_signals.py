"""Tests for prometheus.signals.convergence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from prometheus.signals.convergence import (
    DECISION_MIN_CONFIDENCE,
    ConvergenceSignal,
    _to_signal,
    run_convergence_scan,
)


@dataclass
class _FakeTimeline:
    entity_id: str
    entity_type: str
    days_to_hard_deadline: float | None = 30.0
    hard_deadline_reason: str = "SPR depletion"
    days_to_soft_signal: float | None = 7.0
    soft_signal_type: str = "freight rates"
    infrastructure_lag_days: float = 14.0
    lag_reason: str = "reroute around Africa"
    buffer_days: float = 18.0
    buffer_source: str = "SPR"
    estimated_convergence_days: float | None = 22.0
    convergence_window: tuple[float, float] = (10.0, 35.0)
    confidence: float = 0.8
    strategy: str = "LADDER_EARLY"
    entry_windows: list[dict[str, Any]] = field(default_factory=list)
    computed_at: str = "2026-05-05T12:00:00+00:00"


@dataclass
class _FakeDB:
    pass


class _CapturingStorage:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.upserts: list[ConvergenceSignal] = []
        self.divergence_decisions: dict[tuple[date, str, str], str | None] = {}
        self.existing: dict[tuple[date, str, str], str | None] = {}

    def upsert(self, signal: ConvergenceSignal) -> None:
        self.upserts.append(signal)
        self.existing[(signal.as_of_date, signal.entity_type, signal.entity_id)] = signal.decision_id

    def existing_decision_id(self, *, as_of_date: date, entity_type: str, entity_id: str) -> str | None:
        return self.existing.get((as_of_date, entity_type, entity_id))

    def divergence_decision_id(self, *, as_of_date: date, entity_type: str, entity_id: str) -> str | None:
        return self.divergence_decisions.get((as_of_date, entity_type, entity_id))


class _CapturingMeta:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.decisions: list[Any] = []

    def save_engine_decision(self, decision: Any) -> None:
        self.decisions.append(decision)


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    storage = _CapturingStorage()
    meta = _CapturingMeta()
    monkeypatch.setattr("prometheus.signals.convergence.ConvergenceStorage", lambda db_manager: storage)
    monkeypatch.setattr("prometheus.signals.convergence.MetaStorage", lambda db_manager: meta)
    return storage, meta


def test_to_signal_extracts_window() -> None:
    upstream = _FakeTimeline(entity_id="hormuz", entity_type="chokepoint")
    sig = _to_signal(upstream, as_of_date=date(2026, 5, 5), decision_id=None)
    assert sig.entity_id == "hormuz"
    assert sig.convergence_window_min == 10.0
    assert sig.convergence_window_max == 35.0
    assert sig.confidence == 0.8


def test_confident_logs_decision(patched) -> None:
    storage, meta = patched
    res = run_convergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream=[_FakeTimeline(entity_id="hormuz", entity_type="chokepoint", confidence=0.85)],
    )
    assert res.rows_persisted == 1
    assert res.decisions_logged == 1
    assert len(res.confident) == 1
    assert meta.decisions[0].engine_name == "CONVERGENCE"
    assert storage.upserts[0].decision_id is not None


def test_low_confidence_persists_no_decision(patched) -> None:
    storage, meta = patched
    res = run_convergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream=[_FakeTimeline(entity_id="cp1", entity_type="chokepoint", confidence=0.30)],
    )
    assert res.rows_persisted == 1
    assert res.decisions_logged == 0
    assert len(res.confident) == 0
    assert meta.decisions == []


def test_links_to_existing_divergence_decision(patched) -> None:
    """When a divergence decision exists for the same entity/day, the
    convergence signal should reuse that decision_id rather than logging
    a new one — so the Meta-Orchestrator scores them as a joint hypothesis."""
    storage, meta = patched
    storage.divergence_decisions[(date(2026, 5, 5), "chokepoint", "hormuz")] = "div-decision-xyz"

    res = run_convergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream=[_FakeTimeline(entity_id="hormuz", entity_type="chokepoint", confidence=0.85)],
    )

    assert res.decisions_logged == 0  # reused divergence decision_id
    assert storage.upserts[0].decision_id == "div-decision-xyz"


def test_threshold_constant_consistent() -> None:
    assert 0.0 < DECISION_MIN_CONFIDENCE < 1.0
