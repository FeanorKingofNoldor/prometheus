"""Tests for prometheus.signals.beneficiary."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from prometheus.signals.beneficiary import (
    BeneficiaryRow,
    TOP_K,
    run_beneficiary_scan,
)


@dataclass
class _FakeCandidate:
    entity_id: tuple[str, str]
    entity_name: str
    entity_type: str
    composite_score: float
    motive_score: float
    means_score: float
    opportunity_score: float
    pattern_match_score: float
    direct_benefits: list[str] = field(default_factory=list)
    indirect_benefits: list[str] = field(default_factory=list)


@dataclass
class _FakeAnalysis:
    candidates: list[_FakeCandidate]
    asymmetry_detected: bool = False
    claimed_perpetrator: tuple[str, str] | None = None


@dataclass
class _FakeDB:
    pass


class _CapturingStorage:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.upserts: list[BeneficiaryRow] = []
        self.existing: dict[tuple[date, str, str], str | None] = {}

    def upsert(self, row: BeneficiaryRow) -> None:
        self.upserts.append(row)
        if row.decision_id:
            self.existing[(row.as_of_date, row.victim_entity_type, row.victim_entity_id)] = row.decision_id

    def existing_decision_id(self, *, as_of_date: date, victim_entity_type: str, victim_entity_id: str) -> str | None:
        return self.existing.get((as_of_date, victim_entity_type, victim_entity_id))


class _CapturingMeta:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.decisions: list[Any] = []

    def save_engine_decision(self, decision: Any) -> None:
        self.decisions.append(decision)


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    storage = _CapturingStorage()
    meta = _CapturingMeta()
    monkeypatch.setattr("prometheus.signals.beneficiary.BeneficiaryStorage", lambda db_manager: storage)
    monkeypatch.setattr("prometheus.signals.beneficiary.MetaStorage", lambda db_manager: meta)
    return storage, meta


def _make_candidates(n: int = 3) -> list[_FakeCandidate]:
    return [
        _FakeCandidate(
            entity_id=("SOVEREIGN", f"X{i}"),
            entity_name=f"Country X{i}",
            entity_type="SOVEREIGN",
            composite_score=0.8 - i * 0.1,
            motive_score=0.7,
            means_score=0.6,
            opportunity_score=0.5,
            pattern_match_score=0.5,
        )
        for i in range(n)
    ]


def test_persists_top_k_per_victim(patched) -> None:
    storage, meta = patched
    analysis = _FakeAnalysis(candidates=_make_candidates(7))
    res = run_beneficiary_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        analyses=[(("CONFLICT", "iran_war_2026"), analysis)],
    )
    # Capped at TOP_K
    assert res.rows_persisted == TOP_K
    assert res.victims_scanned == 1
    assert res.decisions_logged == 1
    assert all(r.victim_entity_id == "iran_war_2026" for r in storage.upserts)
    assert [r.rank for r in storage.upserts] == list(range(1, TOP_K + 1))


def test_asymmetry_recorded(patched) -> None:
    storage, meta = patched
    analysis = _FakeAnalysis(
        candidates=_make_candidates(2),
        asymmetry_detected=True,
        claimed_perpetrator=("SOVEREIGN", "RUS"),
    )
    res = run_beneficiary_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        analyses=[(("CONFLICT", "nord_stream"), analysis)],
    )
    assert ("CONFLICT", "nord_stream") in res.asymmetric
    assert all(r.asymmetry_detected for r in storage.upserts)
    assert meta.decisions[0].engine_name == "BENEFICIARY"
    assert meta.decisions[0].output_refs["asymmetry_detected"] is True


def test_skips_none_analysis(patched) -> None:
    storage, meta = patched
    res = run_beneficiary_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        analyses=[(("CONFLICT", "broken"), None)],
    )
    assert res.rows_persisted == 0
    assert res.victims_scanned == 1


def test_idempotent_rerun(patched) -> None:
    storage, meta = patched
    analysis = _FakeAnalysis(candidates=_make_candidates(3))
    first = run_beneficiary_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        analyses=[(("CONFLICT", "iran_war_2026"), analysis)],
    )
    assert first.decisions_logged == 1
    second = run_beneficiary_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        analyses=[(("CONFLICT", "iran_war_2026"), analysis)],
    )
    assert second.decisions_logged == 0
    assert len(meta.decisions) == 1
