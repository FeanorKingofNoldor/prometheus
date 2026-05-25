"""Tests for prometheus.risk.geo_exposure."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from prometheus.risk.geo_exposure import (
    DECISION_MIN_SCORE,
    PortfolioGeoRiskSnapshot,
    _top_driver,
    run_geo_exposure_scan,
)


@dataclass
class _RiskTypes:
    conflict: float = 0.0
    chokepoint: float = 0.0
    sovereign: float = 0.0
    sector: float = 0.0


@dataclass
class _FakeExposure:
    overall_risk_score: float = 50.0
    tickers: list[str] = field(default_factory=lambda: ["XOM.US", "TSM.US", "AAPL.US"])
    risk_type_breakdown: _RiskTypes = field(default_factory=_RiskTypes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_risk_score": self.overall_risk_score,
            "tickers": self.tickers,
        }


@dataclass
class _FakeDB:
    pass


class _CapturingStorage:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.upserts: list[PortfolioGeoRiskSnapshot] = []
        self.existing: dict[tuple[str, date], str | None] = {}

    def upsert(self, snap: PortfolioGeoRiskSnapshot) -> None:
        self.upserts.append(snap)
        self.existing[(snap.portfolio_id, snap.as_of_date)] = snap.decision_id

    def existing_decision_id(self, *, portfolio_id: str, as_of_date: date) -> str | None:
        return self.existing.get((portfolio_id, as_of_date))

    def latest(self, portfolio_id: str) -> PortfolioGeoRiskSnapshot | None:
        return self.upserts[-1] if self.upserts else None


class _CapturingMeta:
    def __init__(self, *, db_manager: Any = None) -> None:
        self.decisions: list[Any] = []

    def save_engine_decision(self, decision: Any) -> None:
        self.decisions.append(decision)


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    storage = _CapturingStorage()
    meta = _CapturingMeta()
    monkeypatch.setattr("prometheus.risk.geo_exposure.PortfolioGeoRiskStorage", lambda db_manager: storage)
    monkeypatch.setattr("prometheus.risk.geo_exposure.MetaStorage", lambda db_manager: meta)
    return storage, meta


def test_no_holdings_short_circuits(patched) -> None:
    storage, meta = patched
    res = run_geo_exposure_scan(
        portfolio_id="IBKR_PAPER",
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        holdings={},
    )
    assert res.snapshot is None
    assert res.decision_logged is False
    assert storage.upserts == []
    assert meta.decisions == []


def test_low_score_persists_no_decision(patched) -> None:
    storage, meta = patched
    exposure = _FakeExposure(overall_risk_score=20.0)
    res = run_geo_exposure_scan(
        portfolio_id="IBKR_PAPER",
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        holdings={"AAPL.US": 0.5, "MSFT.US": 0.5},
        exposure_result=exposure,
    )
    assert res.snapshot is not None
    assert res.snapshot.overall_risk_score == 20.0
    assert res.decision_logged is False
    assert meta.decisions == []
    assert storage.upserts[0].decision_id is None


def test_high_score_logs_decision(patched) -> None:
    storage, meta = patched
    exposure = _FakeExposure(
        overall_risk_score=72.0,
        risk_type_breakdown=_RiskTypes(
            conflict=0.6, chokepoint=0.4, sovereign=0.3, sector=0.2,
        ),
    )
    res = run_geo_exposure_scan(
        portfolio_id="IBKR_PAPER",
        as_of_date=date(2026, 5, 5),
        db_manager=_FakeDB(),
        holdings={"XOM.US": 1.0},
        exposure_result=exposure,
    )
    assert res.snapshot is not None
    assert res.decision_logged is True
    assert meta.decisions[0].engine_name == "GEO_RISK"
    assert meta.decisions[0].market_id == "US_EQ"
    assert meta.decisions[0].output_refs["overall_risk_score"] == 72.0


def test_top_driver_picks_max() -> None:
    snap = PortfolioGeoRiskSnapshot(
        snapshot_id="x",
        portfolio_id="P",
        as_of_date=date(2026, 5, 5),
        overall_risk_score=50.0,
        conflict_risk=0.2,
        chokepoint_risk=0.6,
        sovereign_risk=0.3,
        sector_risk=0.1,
        ticker_count=5,
    )
    assert _top_driver(snap) == "chokepoint"


def test_decision_threshold_constant() -> None:
    assert 0 <= DECISION_MIN_SCORE <= 100
