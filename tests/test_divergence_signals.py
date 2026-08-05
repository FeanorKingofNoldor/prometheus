"""Tests for prometheus.signals.divergence.

These tests run with the stubbed apatheon modules from conftest.py, so we
fake out the DB layer entirely (the divergence scanner and storage take
the DatabaseManager as an injected dependency).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from prometheus.signals.divergence import (
    DECISION_MIN_SEVERITY,
    DivergenceSignal,
    _meets_decision_threshold,
    _to_signal,
    run_divergence_scan,
)

# ─────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────


@dataclass
class _FakeDivergenceResult:
    """Mirrors apatheon.intel.signal_classifier.DivergenceResult."""

    entity_id: str
    entity_type: str
    behavioral_score: float
    narrative_score: float
    divergence: float
    abs_divergence: float
    direction: str
    severity: str
    trading_signal: str
    behavioral: Any = None
    narrative: Any = None
    computed_at: str = "2026-05-05T12:00:00+00:00"


@dataclass
class _FakeDB:
    """In-memory replacement for DatabaseManager.

    We don't actually want to talk to Postgres in these tests — instead we
    replace ``DivergenceStorage`` and ``MetaStorage`` calls with a
    capture-only implementation when wiring tests.
    """


class _CapturingStorage:
    """Records upserts + existing-decision lookups in plain dicts."""

    def __init__(self, *, db_manager: Any = None) -> None:
        self.upserts: list[DivergenceSignal] = []
        self.decisions_by_key: dict[tuple[date, str, str], str | None] = {}

    def upsert(self, signal: DivergenceSignal) -> None:
        # Index latest by key so existing_decision_id reflects prior upserts.
        self.upserts.append(signal)
        self.decisions_by_key[(signal.as_of_date, signal.entity_type, signal.entity_id)] = (
            signal.decision_id
        )

    def existing_decision_id(self, *, as_of_date: date, entity_type: str, entity_id: str) -> str | None:
        return self.decisions_by_key.get((as_of_date, entity_type, entity_id))


class _CapturingMeta:
    """Records every engine_decision row written."""

    def __init__(self, *, db_manager: Any = None) -> None:
        self.decisions: list[Any] = []

    def save_engine_decision(self, decision: Any) -> None:
        self.decisions.append(decision)


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────


def _make_result(
    *,
    entity_id: str = "hormuz",
    entity_type: str = "chokepoint",
    severity: str = "SIGNIFICANT",
    direction: str = "REALITY_UNDERSTATED",
    trading_signal: str = "FRONT_RUN_REALITY",
    behavioral: float = 0.80,
    narrative: float = 0.40,
) -> _FakeDivergenceResult:
    div = narrative - behavioral
    return _FakeDivergenceResult(
        entity_id=entity_id,
        entity_type=entity_type,
        behavioral_score=behavioral,
        narrative_score=narrative,
        divergence=round(div, 4),
        abs_divergence=round(abs(div), 4),
        direction=direction,
        severity=severity,
        trading_signal=trading_signal,
    )


@pytest.fixture
def patched_storage(monkeypatch: pytest.MonkeyPatch):
    """Replace DivergenceStorage / MetaStorage so run_divergence_scan never touches Postgres."""
    cap_storage = _CapturingStorage()
    cap_meta = _CapturingMeta()

    monkeypatch.setattr(
        "prometheus.signals.divergence.DivergenceStorage",
        lambda db_manager: cap_storage,
    )
    monkeypatch.setattr(
        "prometheus.signals.divergence.MetaStorage",
        lambda db_manager: cap_meta,
    )
    return cap_storage, cap_meta


# ─────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────


def test_threshold_helper_uses_constant() -> None:
    assert DECISION_MIN_SEVERITY == "SIGNIFICANT"
    assert not _meets_decision_threshold("NONE")
    assert not _meets_decision_threshold("MILD")
    assert _meets_decision_threshold("SIGNIFICANT")
    assert _meets_decision_threshold("EXTREME")


def test_to_signal_copies_upstream_fields() -> None:
    upstream = _make_result()
    signal = _to_signal(upstream, as_of_date=date(2026, 5, 5), decision_id=None)

    assert signal.entity_id == "hormuz"
    assert signal.entity_type == "chokepoint"
    assert signal.severity == "SIGNIFICANT"
    assert signal.trading_signal == "FRONT_RUN_REALITY"
    assert signal.decision_id is None
    assert signal.as_of_date == date(2026, 5, 5)
    # Computed_at is parsed to a tz-aware datetime
    assert signal.computed_at.tzinfo is not None


def test_significant_signal_logs_decision_and_persists(patched_storage) -> None:
    storage, meta = patched_storage

    result = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[_make_result(severity="SIGNIFICANT")],
        conflict_results=[],
    )

    # Persistence
    assert result.rows_persisted == 1
    assert len(storage.upserts) == 1
    persisted = storage.upserts[0]
    assert persisted.severity == "SIGNIFICANT"
    assert persisted.trading_signal == "FRONT_RUN_REALITY"
    assert persisted.decision_id is not None  # decision was logged

    # Decision logged with engine_name=DIVERGENCE
    assert result.decisions_logged == 1
    assert len(meta.decisions) == 1
    decision = meta.decisions[0]
    assert decision.engine_name == "DIVERGENCE"
    assert decision.market_id == "INTEL"
    assert decision.output_refs["trading_signal"] == "FRONT_RUN_REALITY"
    assert decision.input_refs["entity_id"] == "hormuz"

    # Result classification
    assert len(result.significant) == 1
    assert len(result.extreme) == 0


def test_extreme_signal_classified_correctly(patched_storage) -> None:
    storage, meta = patched_storage

    res = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[],
        conflict_results=[
            _make_result(
                entity_id="iran_war_2026",
                entity_type="conflict",
                severity="EXTREME",
                direction="NARRATIVE_OVERSTATES",
                trading_signal="FADE_NARRATIVE",
                behavioral=0.20,
                narrative=0.85,
            )
        ],
    )

    assert res.decisions_logged == 1
    assert len(res.extreme) == 1
    assert res.extreme[0].trading_signal == "FADE_NARRATIVE"


def test_mild_signal_persists_without_decision(patched_storage) -> None:
    storage, meta = patched_storage

    res = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[
            _make_result(severity="MILD", trading_signal="NONE", direction="ALIGNED")
        ],
        conflict_results=[],
    )

    # Persisted but no engine_decision written
    assert res.rows_persisted == 1
    assert res.decisions_logged == 0
    assert len(meta.decisions) == 0
    assert storage.upserts[0].decision_id is None
    assert len(res.significant) == 0
    assert len(res.extreme) == 0


def test_idempotent_rerun_does_not_double_log(patched_storage) -> None:
    """Re-running the same scan reuses the existing decision_id."""
    storage, meta = patched_storage

    sig_result = _make_result(severity="SIGNIFICANT")

    first = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[sig_result],
        conflict_results=[],
    )
    assert first.decisions_logged == 1

    # Second run on the same day with the same entity should find the
    # existing decision_id and skip re-logging.
    second = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[sig_result],
        conflict_results=[],
    )
    assert second.decisions_logged == 0
    assert second.rows_persisted == 1
    assert len(meta.decisions) == 1  # still only the original


def test_handles_upstream_failure_gracefully(patched_storage) -> None:
    """A bad upstream row must not crash the scan."""
    storage, meta = patched_storage

    @dataclass
    class _Bad:
        # Missing required attributes; access should raise.
        entity_id: str = "broken"
        entity_type: str = "chokepoint"

        def __getattr__(self, name: str) -> Any:
            raise RuntimeError(f"explosive attr {name}")

    res = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[_Bad(), _make_result(severity="SIGNIFICANT")],
        conflict_results=[],
    )

    # Bad row is skipped; the good row still persists + logs.
    assert res.rows_persisted == 1
    assert res.decisions_logged == 1


def test_scan_result_counts_inputs(patched_storage) -> None:
    storage, meta = patched_storage

    res = run_divergence_scan(
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        chokepoint_results=[
            _make_result(entity_id=f"cp_{i}", severity="MILD", trading_signal="NONE", direction="ALIGNED")
            for i in range(3)
        ],
        conflict_results=[
            _make_result(entity_id="iran", entity_type="conflict", severity="EXTREME"),
            _make_result(entity_id="ukr", entity_type="conflict", severity="SIGNIFICANT"),
        ],
    )

    assert res.chokepoints_scanned == 3
    assert res.conflicts_scanned == 2
    assert res.rows_persisted == 5
    assert res.decisions_logged == 2  # only the EXTREME and SIGNIFICANT
    assert len(res.extreme) == 1
    assert len(res.significant) == 1
