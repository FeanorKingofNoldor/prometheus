"""Tests for the static event calendar."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from prometheus.calendar.event_calendar import (
    EventKind,
    cpi_dates_for_year,
    days_to_iv_event,
    eia_natgas_dates_for_year,
    eia_petroleum_dates_for_year,
    event_kinds_for_symbol,
    fomc_dates,
    near_iv_event,
    next_iv_events,
    nfp_dates_for_year,
    opec_dates,
    upcoming_events,
    wasde_dates_for_year,
)


# ── Rule-based generators ───────────────────────────────────────────


def test_nfp_first_friday_of_each_month():
    dates = nfp_dates_for_year(2026)
    assert len(dates) == 12
    for d in dates:
        assert d.weekday() == 4   # Friday
        assert d.day <= 7         # first Friday is always 1-7


def test_eia_petroleum_all_wednesdays():
    dates = eia_petroleum_dates_for_year(2026)
    assert 50 <= len(dates) <= 53   # 52 or 53 Wednesdays in a year
    for d in dates:
        assert d.weekday() == 2   # Wednesday


def test_eia_natgas_all_thursdays():
    dates = eia_natgas_dates_for_year(2026)
    for d in dates:
        assert d.weekday() == 3   # Thursday


def test_cpi_and_wasde_monthly():
    assert len(cpi_dates_for_year(2026)) == 12
    assert len(wasde_dates_for_year(2026)) == 12


# ── Hardcoded schedules ─────────────────────────────────────────────


def test_fomc_2026_has_eight_meetings():
    """Fed holds 8 FOMC meetings per year; check 2026 set."""
    f2026 = [d for d in fomc_dates() if d.year == 2026]
    assert len(f2026) == 8


def test_opec_dates_present():
    assert len(opec_dates()) >= 4


# ── Symbol sensitivity map ──────────────────────────────────────────


def test_spy_sensitive_to_macro_releases():
    kinds = event_kinds_for_symbol("SPY")
    assert EventKind.FOMC in kinds
    assert EventKind.CPI in kinds
    assert EventKind.NFP in kinds


def test_crude_sensitive_to_eia_and_opec():
    for sym in ("CL", "BZ"):
        kinds = event_kinds_for_symbol(sym)
        assert EventKind.EIA_PETROLEUM in kinds
        assert EventKind.OPEC in kinds


def test_natgas_sensitive_only_to_eia_storage():
    kinds = event_kinds_for_symbol("NG")
    assert kinds == frozenset({EventKind.EIA_NATGAS})


def test_wheat_sensitive_to_wasde():
    kinds = event_kinds_for_symbol("ZW")
    assert EventKind.USDA_WASDE in kinds


def test_gold_sensitive_to_fed_and_cpi():
    kinds = event_kinds_for_symbol("GC")
    assert EventKind.FOMC in kinds
    assert EventKind.CPI in kinds


def test_copper_has_no_v1_coverage():
    """HG has no clean IV-event mapping in v1; documented gap."""
    assert event_kinds_for_symbol("HG") == frozenset()


def test_unknown_symbol_returns_empty():
    assert event_kinds_for_symbol("XXXX") == frozenset()


def test_symbol_lookup_case_insensitive():
    assert event_kinds_for_symbol("spy") == event_kinds_for_symbol("SPY")


# ── Event lookup helpers ────────────────────────────────────────────


def test_upcoming_events_filters_to_horizon():
    today = date(2026, 1, 1)
    events = upcoming_events(today, horizon_days=60)
    # All events should be in (today, today + 60]
    for e in events:
        assert today < e.event_date <= today + timedelta(days=60)


def test_upcoming_events_sorted_by_date():
    events = upcoming_events(date(2026, 1, 1), horizon_days=120)
    dates = [e.event_date for e in events]
    assert dates == sorted(dates)


def test_next_iv_events_for_natgas_only_returns_eia_thursdays():
    today = date(2026, 6, 1)   # Monday
    events = next_iv_events("NG", today, horizon_days=30)
    assert events  # there's at least one Thursday in 30 days
    for e in events:
        assert e.kind == EventKind.EIA_NATGAS
        assert e.event_date.weekday() == 3


def test_days_to_iv_event_for_crude_returns_wed_or_opec():
    # Pick a Monday; nearest IV event for CL is the upcoming Wed EIA.
    monday = date(2026, 6, 1)
    days = days_to_iv_event("CL", monday)
    assert days is not None and 0 < days <= 7


def test_days_to_iv_event_none_for_uncovered_symbol():
    assert days_to_iv_event("HG", date(2026, 6, 1)) is None


def test_near_iv_event_inside_window():
    # On a Tuesday, EIA petroleum (Wed) is 1 day away — within window=2
    tuesday = date(2026, 6, 2)
    event = near_iv_event("CL", tuesday, window_days=2)
    assert event is not None
    assert event.kind == EventKind.EIA_PETROLEUM


def test_near_iv_event_outside_window():
    # On a Thursday, next EIA petroleum is ~6 days away — outside window=2
    thursday = date(2026, 6, 4)
    event = near_iv_event("CL", thursday, window_days=2)
    assert event is None


def test_near_iv_event_for_uncovered_symbol_is_none():
    assert near_iv_event("HG", date(2026, 6, 2), window_days=5) is None


# ── Cross-symbol sanity ─────────────────────────────────────────────


@pytest.mark.parametrize("symbol", ["SPY", "VIX", "CL", "BZ", "NG", "ZW", "GC"])
def test_every_covered_symbol_has_at_least_one_event_in_30d(symbol):
    """All covered symbols should see at least one IV-relevant event
    within any 30-day window (FOMC ~6 weeks, monthlies, weeklies)."""
    today = date(2026, 6, 1)
    assert days_to_iv_event(symbol, today, horizon_days=45) is not None
