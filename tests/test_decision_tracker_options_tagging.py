"""Pin OPTIONS vs OPTIONS_SHADOW engine_name tagging in record_options_decision.

Shadow / not-yet-cutover sleeve PnL must never be tagged live "OPTIONS",
otherwise it pollutes the live scorecard (live_performance / evaluator filter
engine_name IN ('PORTFOLIO','OPTIONS')).
"""

from __future__ import annotations

from datetime import date

import pytest

from prometheus.decisions.tracker import DecisionTracker

_DERIV_ENVS = (
    "PROMETHEUS_DERIVATIVES_SHADOW",
    "PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER",
    "PROMETHEUS_DERIVATIVES_INCOME_CUTOVER",
    "PROMETHEUS_DERIVATIVES_CONVEX_CUTOVER",
    "PROMETHEUS_DERIVATIVES_COMMODITY_CUTOVER",
)


class _CaptureStorage:
    def __init__(self) -> None:
        self.saved = []

    def save_engine_decision(self, decision) -> None:
        self.saved.append(decision)


@pytest.fixture
def tracker(monkeypatch):
    for k in _DERIV_ENVS:
        monkeypatch.delenv(k, raising=False)
    t = DecisionTracker.__new__(DecisionTracker)
    t._storage = _CaptureStorage()
    return t


def _record(tracker, orders):
    return tracker.record_options_decision(
        strategy_id="US_OPTIONS",
        market_id="US_EQ",
        as_of_date=date(2026, 6, 10),
        orders=orders,
    )


def _engine(tracker) -> str:
    return tracker._storage.saved[-1].engine_name


def test_all_sleeves_cutover_tags_live(tracker, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER", "1")
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_INCOME_CUTOVER", "1")
    orders = [
        {"strategy": "hedge.collar"},
        {"strategy": "income.spy_iron_condor"},
    ]
    _record(tracker, orders)
    assert _engine(tracker) == "OPTIONS"
    assert tracker._storage.saved[-1].input_refs["mode"] == "live"


def test_partial_cutover_tags_shadow(tracker, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER", "1")
    # INCOME still in shadow -> whole decision excluded from live.
    orders = [
        {"strategy": "hedge.collar"},
        {"strategy": "income.spy_iron_condor"},
    ]
    _record(tracker, orders)
    assert _engine(tracker) == "OPTIONS_SHADOW"
    assert tracker._storage.saved[-1].input_refs["mode"] == "shadow"


def test_global_shadow_no_cutover_tags_shadow(tracker, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    orders = [{"strategy": "hedge.collar"}]
    _record(tracker, orders)
    assert _engine(tracker) == "OPTIONS_SHADOW"


def test_legacy_strategy_names_tag_live_under_shadow_env(tracker, monkeypatch):
    """PROMETHEUS_DERIVATIVES_SHADOW gates only the new sleeve pipeline —
    the legacy strategies REALLY submit while it is set. Orders carrying
    legacy names (no sleeve prefix) must classify live, otherwise real
    submitted P&L is excluded from live metrics by the OPTIONS_SHADOW
    guard (2026-07 audit defect B)."""
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    for name in ("vix_tail_hedge", "iron_condor", "iron_butterfly",
                 "covered_call", "protective_put", "crisis_alpha"):
        _record(tracker, [{"strategy": name}])
        assert _engine(tracker) == "OPTIONS", name
        assert tracker._storage.saved[-1].input_refs["mode"] == "live", name


def test_legacy_plus_cutover_sleeve_tags_live(tracker, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER", "1")
    orders = [
        {"strategy": "vix_tail_hedge"},
        {"strategy": "hedge.collar"},
    ]
    _record(tracker, orders)
    assert _engine(tracker) == "OPTIONS"


def test_legacy_plus_shadow_sleeve_tags_shadow(tracker, monkeypatch):
    """Safety stays intact for sleeve-named orders: a decision touching a
    sleeve that has NOT been cut over is still excluded from live."""
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    orders = [
        {"strategy": "vix_tail_hedge"},
        {"strategy": "income.spy_iron_condor"},
    ]
    _record(tracker, orders)
    assert _engine(tracker) == "OPTIONS_SHADOW"


def test_shadow_globally_off_tags_live(tracker, monkeypatch):
    # No PROMETHEUS_DERIVATIVES_SHADOW -> system fully live.
    orders = [{"strategy": "hedge.collar"}]
    _record(tracker, orders)
    assert _engine(tracker) == "OPTIONS"


def test_explicit_shadow_arg_wins(tracker, monkeypatch):
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_SHADOW", "1")
    monkeypatch.setenv("PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER", "1")
    decision_id = tracker.record_options_decision(
        strategy_id="US_OPTIONS",
        market_id="US_EQ",
        as_of_date=date(2026, 6, 10),
        orders=[{"strategy": "hedge.collar"}],
        shadow=True,
    )
    assert decision_id
    assert _engine(tracker) == "OPTIONS_SHADOW"
