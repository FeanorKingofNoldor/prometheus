"""Tests for prometheus.signals.compound_pressure."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from prometheus.signals.compound_pressure import (
    DECISION_MIN_SEVERITY,
    CompoundPressureAlert,
    _meets_threshold,
    run_compound_pressure_scan,
)


@dataclass
class _FakeAlert:
    target_entity: tuple[str, str]
    lookback_days: int = 14
    total_pressure_points: int = 12
    pressure_points_moved: int = 5
    cluster_days: float = 4.0
    encirclement_score: float = 0.8
    severity: str = "HIGH"
    adversarial_movements: list[Any] = field(default_factory=list)
    likely_orchestrators: list[Any] = field(default_factory=list)


@dataclass
class _FakeDB:
    pass


class _CapturingStorage:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.upserts: list[CompoundPressureAlert] = []
        self.existing: dict[tuple[date, str, str], str | None] = {}

    def upsert(self, alert: CompoundPressureAlert) -> None:
        self.upserts.append(alert)
        self.existing[(alert.as_of_date, alert.target_entity_type, alert.target_entity_id)] = alert.decision_id

    def existing_decision_id(self, *, as_of_date: date, target_entity_type: str, target_entity_id: str) -> str | None:
        return self.existing.get((as_of_date, target_entity_type, target_entity_id))


class _CapturingMeta:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.decisions: list[Any] = []

    def save_engine_decision(self, decision: Any) -> None:
        self.decisions.append(decision)


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    storage = _CapturingStorage()
    meta = _CapturingMeta()
    monkeypatch.setattr("prometheus.signals.compound_pressure.CompoundPressureStorage", lambda db_manager: storage)
    monkeypatch.setattr("prometheus.signals.compound_pressure.MetaStorage", lambda db_manager: meta)
    return storage, meta


def test_threshold_helper() -> None:
    assert DECISION_MIN_SEVERITY == "HIGH"
    assert not _meets_threshold("LOW")
    assert not _meets_threshold("MODERATE")
    assert _meets_threshold("HIGH")
    assert _meets_threshold("CRITICAL")


def test_high_severity_logs_decision(patched) -> None:
    storage, meta = patched
    res = run_compound_pressure_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream_results=[_FakeAlert(target_entity=("SOVEREIGN", "IRN"), severity="HIGH")],
    )
    assert res.rows_persisted == 1
    assert res.decisions_logged == 1
    assert len(res.high_or_above) == 1
    assert meta.decisions[0].engine_name == "COMPOUND_PRESSURE"
    assert meta.decisions[0].output_refs["severity"] == "HIGH"


def test_moderate_persists_without_decision(patched) -> None:
    storage, meta = patched
    res = run_compound_pressure_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream_results=[_FakeAlert(target_entity=("SOVEREIGN", "USA"), severity="MODERATE")],
    )
    assert res.rows_persisted == 1
    assert res.decisions_logged == 0
    assert len(res.high_or_above) == 0


def test_handles_string_target(patched) -> None:
    """Some upstream code may return a string 'SOVEREIGN:IRN' instead of a tuple."""
    storage, meta = patched

    @dataclass
    class _Alert:
        target_entity: str = "SOVEREIGN:IRN"
        lookback_days: int = 14
        total_pressure_points: int = 10
        pressure_points_moved: int = 4
        cluster_days: float = 3.0
        encirclement_score: float = 0.9
        severity: str = "CRITICAL"
        adversarial_movements: list[Any] = field(default_factory=list)
        likely_orchestrators: list[Any] = field(default_factory=list)

    res = run_compound_pressure_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream_results=[_Alert()],
    )
    assert res.rows_persisted == 1
    persisted = storage.upserts[0]
    assert persisted.target_entity_type == "SOVEREIGN"
    assert persisted.target_entity_id == "IRN"


def test_idempotent_rerun(patched) -> None:
    storage, meta = patched
    upstream = [_FakeAlert(target_entity=("SOVEREIGN", "RUS"), severity="CRITICAL")]
    first = run_compound_pressure_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream_results=upstream,
    )
    assert first.decisions_logged == 1

    second = run_compound_pressure_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream_results=upstream,
    )
    assert second.decisions_logged == 0
    assert len(meta.decisions) == 1
