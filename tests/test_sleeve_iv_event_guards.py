"""Tests for the Phase 5.2 IV-event guards across COMMODITY + CONVEX
templates. Each long-debit trigger should skip when within 2 days of
an IV-sensitive event for the chosen underlying.
"""

from __future__ import annotations

from datetime import date

import pytest

from prometheus.derivatives import sleeves
from prometheus.derivatives.intel_signals import IntelSignalsSnapshot


def _quiet() -> date:
    """Friday 2026-06-05 — far from any tracked event."""
    return date(2026, 6, 5)


def _eia_wed() -> date:
    """Tuesday 2026-06-09 — 1 day before EIA petroleum Wed."""
    return date(2026, 6, 9)


def _eia_thu() -> date:
    """Wednesday 2026-06-10 — 1 day before EIA natgas Thu."""
    return date(2026, 6, 10)


def _fomc_eve() -> date:
    """Tuesday 2026-06-16 — 1 day before FOMC Jun 17."""
    return date(2026, 6, 16)


def _wasde_eve() -> date:
    """Thursday 2026-06-11 — 1 day before WASDE Jun 12."""
    return date(2026, 6, 11)


def _intel(rows: list[dict], today: date) -> IntelSignalsSnapshot:
    return IntelSignalsSnapshot(as_of_date=today, divergence=rows)


def _div_row(et: str, eid: str, *, severity: str = "EXTREME",
             trading_signal: str = "FRONT_RUN_REALITY",
             abs_divergence: float = 0.5) -> dict:
    return {
        "entity_type": et, "entity_id": eid,
        "severity": severity, "trading_signal": trading_signal,
        "abs_divergence": abs_divergence,
    }


def _tmpl(name: str):
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.COMMODITY] if name.startswith("commodity") \
        else sleeves.default_sleeves()[sleeves.Sleeve.CONVEX]
    return next(t for t in cfg.templates if t.name == name)


# ── Each guarded template skips on event eve ─────────────────────────


def test_crude_chokepoint_skips_on_eia_eve():
    intel = _intel([_div_row("CHOKEPOINT", "hormuz", abs_divergence=0.6)], _eia_wed())
    r = _tmpl("commodity.crude_chokepoint_call").trigger({"intel": intel})
    assert r.fire is False
    assert r.metadata.get("iv_event_skipped") == "EIA_PETROLEUM"
    assert r.metadata.get("iv_event_underlying") == "BZ"


def test_crude_chokepoint_fires_on_quiet_day():
    intel = _intel([_div_row("CHOKEPOINT", "hormuz", abs_divergence=0.6)], _quiet())
    r = _tmpl("commodity.crude_chokepoint_call").trigger({"intel": intel})
    assert r.fire is True
    assert r.metadata["underlying"] == "BZ"


def test_natgas_skips_on_eia_storage_eve():
    intel = _intel([_div_row("CONFLICT", "russia_ukraine")], _eia_thu())
    r = _tmpl("commodity.natgas_supply_call").trigger({"intel": intel})
    assert r.fire is False
    assert r.metadata.get("iv_event_skipped") == "EIA_NATGAS"


def test_natgas_fires_on_quiet_day():
    intel = _intel([_div_row("CONFLICT", "russia_ukraine")], _quiet())
    r = _tmpl("commodity.natgas_supply_call").trigger({"intel": intel})
    assert r.fire is True


def test_gold_skips_on_fomc_eve():
    intel = _intel([_div_row("SOVEREIGN", "rus")], _fomc_eve())
    r = _tmpl("commodity.gold_sanctions_call").trigger({"intel": intel})
    assert r.fire is False
    assert r.metadata.get("iv_event_skipped") == "FOMC"


def test_gold_fires_on_quiet_day():
    intel = _intel([_div_row("SOVEREIGN", "rus")], _quiet())
    r = _tmpl("commodity.gold_sanctions_call").trigger({"intel": intel})
    assert r.fire is True


def test_wheat_skips_on_wasde_eve():
    intel = _intel([_div_row("CONFLICT", "russia_ukraine")], _wasde_eve())
    r = _tmpl("commodity.wheat_blacksea_call").trigger({"intel": intel})
    assert r.fire is False
    assert r.metadata.get("iv_event_skipped") == "USDA_WASDE"


def test_wheat_fires_on_quiet_day():
    intel = _intel([_div_row("CONFLICT", "russia_ukraine")], _quiet())
    r = _tmpl("commodity.wheat_blacksea_call").trigger({"intel": intel})
    assert r.fire is True


# ── CONVEX templates ─────────────────────────────────────────────────


def test_convex_thematic_put_skips_xle_on_eia_eve():
    sig = {
        "compound_pressure": {"severity": "HIGH", "target_sector_etf": "XLE"},
        "as_of_date": _eia_wed(),
    }
    r = _tmpl("convex.thematic_sector_put").trigger(sig)
    assert r.fire is False
    assert r.metadata.get("iv_event_skipped") == "EIA_PETROLEUM"


def test_convex_vix_skips_on_fomc_eve():
    # convex.vix_escalation_call has more prerequisites than the others —
    # need geo_risk + vix + vix_5d_change_pct to satisfy the inner trigger.
    sig = {
        "geo_risk_score": 60.0,
        "vix_level": 16.0,
        "vix_5d_change_pct": 0.05,
        "as_of_date": _fomc_eve(),
    }
    r = _tmpl("convex.vix_escalation_call").trigger(sig)
    assert r.fire is False
    assert r.metadata.get("iv_event_skipped") == "FOMC"


def test_convex_vix_fires_on_quiet_day():
    sig = {
        "geo_risk_score": 60.0,
        "vix_level": 16.0,
        "vix_5d_change_pct": 0.05,
        "as_of_date": _quiet(),
    }
    r = _tmpl("convex.vix_escalation_call").trigger(sig)
    assert r.fire is True


# ── Date-string fallback ─────────────────────────────────────────────


def test_iso_string_date_handled():
    """Triggers should accept as_of_date as an ISO string too."""
    intel = _intel([_div_row("CHOKEPOINT", "hormuz", abs_divergence=0.6)], _quiet())
    r = _tmpl("commodity.crude_chokepoint_call").trigger({
        "intel": intel,
        "as_of_date": "2026-06-05",
    })
    assert r.fire is True


# ── HG has no calendar coverage in v1 — guard returns None ─────────


def test_copper_template_not_blocked_by_calendar():
    """HG has no event mapping in v1. The guard should pass through.

    There's no commodity.copper template yet; cover it via a direct
    helper call to confirm the guard returns None for HG.
    """
    from prometheus.derivatives.sleeves import _check_iv_event_guard
    # Tuesday near EIA Wed — would block CL/BZ, must not touch HG.
    result = _check_iv_event_guard("HG", {"as_of_date": _eia_wed()})
    assert result is None
