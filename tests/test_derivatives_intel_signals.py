"""Tests for prometheus.derivatives.intel_signals."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any

from prometheus.derivatives import intel_signals

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []

    def execute(self, sql: str, args: Any = ()) -> None:
        sql_norm = " ".join(sql.split()).upper()
        if "FROM DIVERGENCE_SIGNALS" in sql_norm:
            d = args[0]
            self._result = [tuple(r.get(c) for c in (
                "signal_id", "entity_type", "entity_id",
                "behavioral_score", "narrative_score", "divergence",
                "abs_divergence", "direction", "severity", "trading_signal",
            )) for r in self._db.divergence if r["as_of_date"] == d]
        elif "FROM CONVERGENCE_SIGNALS" in sql_norm:
            d = args[0]
            self._result = [tuple(r.get(c) for c in (
                "signal_id", "entity_type", "entity_id",
                "estimated_convergence_days", "confidence",
                "strategy", "days_to_hard_deadline", "days_to_soft_signal",
            )) for r in self._db.convergence if r["as_of_date"] == d]
        elif "FROM COMPOUND_PRESSURE_ALERTS" in sql_norm:
            d = args[0]
            self._result = [tuple(r.get(c) for c in (
                "alert_id", "target_entity_type", "target_entity_id",
                "encirclement_score", "severity",
                "pressure_points_moved", "total_pressure_points",
            )) for r in self._db.compound if r["as_of_date"] == d]
        elif "FROM PORTFOLIO_GEO_RISK" in sql_norm:
            d = args[0]
            pid = args[1] if len(args) > 1 else None
            matching = [
                r for r in self._db.geo
                if r["as_of_date"] == d
                and (pid is None or r["portfolio_id"] == pid)
            ]
            self._result = [tuple(r.get(c) for c in (
                "portfolio_id", "overall_risk_score",
                "conflict_risk", "chokepoint_risk",
                "sovereign_risk", "sector_risk", "ticker_count",
            )) for r in matching]
        else:
            raise AssertionError(f"unhandled SQL: {sql_norm[:60]}")

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeConnection:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._db)

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeDb:
    def __init__(self) -> None:
        self.divergence: list[dict[str, Any]] = []
        self.convergence: list[dict[str, Any]] = []
        self.compound: list[dict[str, Any]] = []
        self.geo: list[dict[str, Any]] = []

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


# ── Loader tests ─────────────────────────────────────────────────────


def test_load_intel_signals_empty_day():
    snap = intel_signals.load_intel_signals(_FakeDb(), as_of_date=date(2026, 5, 22))
    assert snap.divergence == []
    assert snap.convergence == []
    assert snap.compound_pressure == []
    assert snap.geo_risk is None


def test_load_divergence_signals_returned_as_dicts():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.divergence.append({
        "signal_id": "div-1", "as_of_date": today,
        "entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
        "behavioral_score": 0.85, "narrative_score": 0.40,
        "divergence": 0.45, "abs_divergence": 0.45,
        "direction": "REALITY_OVER_NARRATIVE", "severity": "EXTREME",
        "trading_signal": "FRONT_RUN_REALITY",
    })
    snap = intel_signals.load_intel_signals(db, as_of_date=today)
    assert len(snap.divergence) == 1
    d = snap.divergence[0]
    assert d["entity_id"] == "HORMUZ"
    assert d["severity"] == "EXTREME"


def test_load_geo_risk_filters_by_portfolio_id():
    db = _FakeDb()
    today = date(2026, 5, 22)
    db.geo.append({
        "as_of_date": today, "portfolio_id": "US_OPTIONS_LIVE",
        "overall_risk_score": 72.0, "conflict_risk": 80.0,
        "chokepoint_risk": 70.0, "sovereign_risk": 50.0,
        "sector_risk": 45.0, "ticker_count": 12,
    })
    snap = intel_signals.load_intel_signals(
        db, as_of_date=today, portfolio_id="US_OPTIONS_LIVE",
    )
    assert snap.geo_risk is not None
    assert snap.geo_risk["overall_risk_score"] == 72.0


# ── Snapshot helper methods ──────────────────────────────────────────


def test_overall_geo_risk_score_returns_zero_when_missing():
    snap = intel_signals.IntelSignalsSnapshot(as_of_date=date(2026, 5, 22))
    assert snap.overall_geo_risk_score() == 0.0


def test_critical_compound_pressure_filters_by_min_severity():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        compound_pressure=[
            {"severity": "HIGH", "target_entity_id": "IRN"},
            {"severity": "MODERATE", "target_entity_id": "VEN"},
            {"severity": "CRITICAL", "target_entity_id": "CHN"},
        ],
    )
    high = snap.critical_compound_pressure(min_severity="HIGH")
    assert {a["target_entity_id"] for a in high} == {"IRN", "CHN"}


def test_extreme_divergences_filters_by_severity():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        divergence=[
            {"severity": "EXTREME", "entity_id": "HORMUZ"},
            {"severity": "SIGNIFICANT", "entity_id": "TAIWAN"},
            {"severity": "MODERATE", "entity_id": "BAB"},
        ],
    )
    assert len(snap.extreme_divergences()) == 1


def test_imminent_convergences_by_days_and_confidence():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        convergence=[
            {"entity_id": "A", "estimated_convergence_days": 15.0, "confidence": 0.8},
            {"entity_id": "B", "estimated_convergence_days": 90.0, "confidence": 0.8},
            {"entity_id": "C", "estimated_convergence_days": 20.0, "confidence": 0.3},
            {"entity_id": "D", "estimated_convergence_days": None, "confidence": 0.9},
        ],
    )
    imminent = snap.imminent_convergences(max_days=30, min_confidence=0.5)
    assert [c["entity_id"] for c in imminent] == ["A"]


def test_divergences_for_entity_case_insensitive():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        divergence=[
            {"entity_type": "CHOKEPOINT", "entity_id": "HORMUZ", "severity": "EXTREME"},
            {"entity_type": "CONFLICT", "entity_id": "IRAN_WAR", "severity": "HIGH"},
        ],
    )
    assert len(snap.divergences_for_entity("chokepoint", "hormuz")) == 1


# ── merge_into_signals tests ─────────────────────────────────────────


def test_merge_into_signals_folds_intel_under_well_known_keys():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        divergence=[{"entity_id": "HORMUZ", "severity": "EXTREME"}],
        convergence=[],
        compound_pressure=[],
        geo_risk={"overall_risk_score": 50.0},
    )
    merged = intel_signals.merge_into_signals(
        {"vix_level": 18.0}, snap,
    )
    # Original keys preserved
    assert merged["vix_level"] == 18.0
    # Intel keys added
    assert merged["divergence"] == snap.divergence
    assert merged["geo_risk_score"] == 50.0
    assert merged["intel"] is snap


def test_merge_into_signals_populates_compound_pressure_for_legacy_template():
    """The existing convex.thematic_sector_put template reads
    signals[compound_pressure] as a single dict — merge should
    surface the highest-severity target there."""
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        compound_pressure=[
            {"target_entity_id": "IRN", "severity": "HIGH",
             "encirclement_score": 0.75},
            {"target_entity_id": "CHN", "severity": "CRITICAL",
             "encirclement_score": 0.90},
        ],
    )
    merged = intel_signals.merge_into_signals({}, snap)
    assert "compound_pressure" in merged
    cp = merged["compound_pressure"]
    assert cp["severity"] == "CRITICAL"
    assert cp["target_entity_id"] == "CHN"
    # CHN → XLK sector mapping
    assert cp["target_sector_etf"] == "XLK"


def test_merge_into_signals_omits_compound_pressure_when_all_below_threshold():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        compound_pressure=[
            {"target_entity_id": "VEN", "severity": "MODERATE"},
        ],
    )
    merged = intel_signals.merge_into_signals({}, snap)
    assert "compound_pressure" not in merged or merged.get("compound_pressure") is None


def test_severity_rank_treats_equivalent_levels_equally():
    assert intel_signals.SEVERITY_RANK["EXTREME"] == intel_signals.SEVERITY_RANK["CRITICAL"]
    assert intel_signals.SEVERITY_RANK["SIGNIFICANT"] == intel_signals.SEVERITY_RANK["HIGH"]
