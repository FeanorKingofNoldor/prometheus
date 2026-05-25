"""Tests for prometheus.signals.scenarios."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from prometheus.signals.scenarios import (
    DECISION_MIN_PROBABILITY,
    PersistedBranch,
    run_scenario_tree,
    weight_greeks_by_scenarios,
)


@dataclass
class _FakeBranch:
    branch_id: str | None = None
    description: str = ""
    probability: float = 0.0
    affected_entities: list[Any] = field(default_factory=list)
    branch_type: str | None = None
    rationale: str | None = None
    children: list["_FakeBranch"] = field(default_factory=list)


@dataclass
class _FakeTree:
    tree_id: str
    root: _FakeBranch


@dataclass
class _FakeDB:
    pass


class _CapturingStorage:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.upserts: list[PersistedBranch] = []

    def upsert_branch(self, branch: PersistedBranch) -> None:
        self.upserts.append(branch)


class _CapturingMeta:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.decisions: list[Any] = []

    def save_engine_decision(self, decision: Any) -> None:
        self.decisions.append(decision)


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    storage = _CapturingStorage()
    meta = _CapturingMeta()
    monkeypatch.setattr("prometheus.signals.scenarios.ScenarioStorage", lambda db_manager: storage)
    monkeypatch.setattr("prometheus.signals.scenarios.MetaStorage", lambda db_manager: meta)
    return storage, meta


def test_persists_all_branches_and_logs_high_probability(patched) -> None:
    storage, meta = patched
    tree = _FakeTree(
        tree_id="tree-1",
        root=_FakeBranch(
            branch_id="root",
            description="Hormuz closure",
            probability=1.0,
            affected_entities=["XOM.US"],
            children=[
                _FakeBranch(
                    branch_id="b1",
                    description="48h escalation",
                    probability=0.4,  # ≥ threshold
                    affected_entities=["BNO.US", "XOM.US"],
                ),
                _FakeBranch(
                    branch_id="b2",
                    description="diplomatic resolution",
                    probability=0.1,  # below threshold
                    affected_entities=["AAPL.US"],
                ),
            ],
        ),
    )

    res = run_scenario_tree(
        trigger_event="Iran closes Strait of Hormuz",
        trigger_entity=("CHOKEPOINT", "hormuz"),
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        upstream_tree=tree,
    )

    # 3 branches persisted (root + 2 children)
    assert len(storage.upserts) == 3
    # Root probability=1.0 and b1=0.4 → both above threshold (0.30)
    assert res.decisions_logged == 2
    assert all(d.engine_name == "SCENARIO" for d in meta.decisions)


def test_threshold_constant_consistent() -> None:
    assert 0.0 < DECISION_MIN_PROBABILITY < 1.0


def test_weight_greeks_normalises() -> None:
    branches = [
        PersistedBranch(
            branch_id="x",
            tree_id="t",
            as_of_date=date(2026, 5, 5),
            trigger_event="evt",
            trigger_entity_type="CHOKEPOINT",
            trigger_entity_id="hormuz",
            depth=1,
            parent_branch_id="root",
            description="d",
            probability=0.6,
            affected_entities=["XOM.US", "BNO.US"],
        ),
        PersistedBranch(
            branch_id="y",
            tree_id="t",
            as_of_date=date(2026, 5, 5),
            trigger_event="evt",
            trigger_entity_type="CHOKEPOINT",
            trigger_entity_id="hormuz",
            depth=1,
            parent_branch_id="root",
            description="d",
            probability=0.2,
            affected_entities=["AAPL.US"],
        ),
    ]
    weights = weight_greeks_by_scenarios(branches)
    assert pytest.approx(sum(weights.values()), rel=1e-6) == 1.0
    assert weights["XOM.US"] > weights["AAPL.US"]


def test_weight_greeks_empty() -> None:
    assert weight_greeks_by_scenarios([]) == {}
