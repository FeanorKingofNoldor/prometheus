"""Tests for prometheus.derivatives.sleeves config surface."""

from __future__ import annotations

import pytest

from prometheus.derivatives import sleeves
from prometheus.derivatives.selection import TargetSpec


def test_default_sleeves_with_expected_budgets():
    d = sleeves.default_sleeves()
    assert set(d.keys()) == {
        sleeves.Sleeve.HEDGE, sleeves.Sleeve.INCOME,
        sleeves.Sleeve.CONVEX, sleeves.Sleeve.COMMODITY,
    }
    assert d[sleeves.Sleeve.HEDGE].nav_pct == 0.10
    assert d[sleeves.Sleeve.INCOME].nav_pct == 0.15
    assert d[sleeves.Sleeve.CONVEX].nav_pct == 0.05
    assert d[sleeves.Sleeve.COMMODITY].nav_pct == 0.05


def test_sleeve_budgets_sum_to_35_percent_of_nav():
    d = sleeves.default_sleeves()
    nav = 200_000.0
    total = sum(s.budget(nav) for s in d.values())
    assert total == pytest.approx(nav * 0.35)


def test_sleeve_budget_clamps_negative_nav_to_zero():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    assert cfg.budget(-100.0) == 0.0


def test_each_default_sleeve_has_at_least_one_template():
    d = sleeves.default_sleeves()
    for sleeve, cfg in d.items():
        assert len(cfg.templates) >= 1, f"{sleeve} has no templates"
        for tmpl in cfg.templates:
            assert tmpl.sleeve == sleeve
            assert tmpl.name.startswith(sleeve.value.lower() + ".")
            assert 0 < tmpl.sizing_pct_of_sleeve <= 1.0


def test_hedge_protective_put_fires_below_mhi_threshold():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    tmpl = next(t for t in cfg.templates if t.name == "hedge.spy_protective_put")
    r = tmpl.trigger({"mhi": 0.30})
    assert r.fire is True
    assert r.metadata["mhi"] == 0.30

    r2 = tmpl.trigger({"mhi": 0.50})
    assert r2.fire is False


def test_hedge_protective_put_target_is_spy_put_45_to_90_dte():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    tmpl = next(t for t in cfg.templates if t.name == "hedge.spy_protective_put")
    spec = tmpl.target_spec_factory({"mhi": 0.30}, {"mhi": 0.30})
    assert isinstance(spec, TargetSpec)
    assert spec.underlying == "SPY"
    assert spec.right == "P"
    assert spec.target_delta == 0.25
    assert (spec.min_dte, spec.max_dte) == (45, 90)


def test_hedge_template_has_no_profit_target_or_stop():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    tmpl = next(t for t in cfg.templates if t.name == "hedge.spy_protective_put")
    assert tmpl.profit_target_pct is None
    assert tmpl.stop_loss_multiplier is None


def test_income_short_put_fires_in_vix_band():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.INCOME]
    tmpl = next(t for t in cfg.templates if t.name == "income.spy_short_put")
    assert tmpl.trigger({"vix_level": 20.0}).fire is True
    assert tmpl.trigger({"vix_level": 12.0}).fire is False
    assert tmpl.trigger({"vix_level": 35.0}).fire is False


def test_income_template_has_profit_target_and_stop():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.INCOME]
    tmpl = next(t for t in cfg.templates if t.name == "income.spy_short_put")
    assert tmpl.profit_target_pct == 0.50
    assert tmpl.stop_loss_multiplier == 2.0


def test_convex_thematic_put_requires_high_compound_pressure_and_named_sector():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.CONVEX]
    tmpl = next(t for t in cfg.templates if t.name == "convex.thematic_sector_put")

    # Low severity → no fire
    r = tmpl.trigger({"compound_pressure": {"severity": "MODERATE", "target_sector_etf": "XLE"}})
    assert r.fire is False

    # HIGH but no target → no fire
    r2 = tmpl.trigger({"compound_pressure": {"severity": "HIGH"}})
    assert r2.fire is False

    # HIGH + named sector → fires. Inject a quiet date so the IV-event
    # guard added in Phase 5.2 doesn't block on a calendar event.
    from datetime import date as _date
    quiet = _date(2026, 6, 5)   # Friday, far from EIA Wed/Thu, FOMC, etc.
    r3 = tmpl.trigger({
        "compound_pressure": {"severity": "HIGH", "target_sector_etf": "XLE"},
        "as_of_date": quiet,
    })
    assert r3.fire is True
    assert r3.metadata["sector_etf"] == "XLE"


def test_convex_target_uses_sector_from_trigger_metadata():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.CONVEX]
    tmpl = next(t for t in cfg.templates if t.name == "convex.thematic_sector_put")
    trigger_meta = {"sector_etf": "XLE", "severity": "CRITICAL"}
    spec = tmpl.target_spec_factory({}, trigger_meta)
    assert spec.underlying == "XLE"
    assert spec.right == "P"


def test_convex_template_has_high_fallback_iv():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.CONVEX]
    tmpl = next(t for t in cfg.templates if t.name == "convex.thematic_sector_put")
    # Sector ETFs are vol-ier than SPY — fallback should be higher.
    assert tmpl.fallback_iv > 0.25


def test_trigger_result_metadata_defaults_to_empty_mapping():
    r = sleeves.TriggerResult(fire=False, reason="test")
    assert dict(r.metadata) == {}


def test_dataclasses_are_frozen():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]
    with pytest.raises(Exception):  # FrozenInstanceError
        cfg.nav_pct = 0.20  # type: ignore[misc]


# ── COMMODITY sleeve tests ───────────────────────────────────────────


def _intel(divergence_rows):
    """Build a minimal IntelSignalsSnapshot for trigger tests."""
    from datetime import date

    from prometheus.derivatives.intel_signals import IntelSignalsSnapshot
    return IntelSignalsSnapshot(as_of_date=date(2026, 6, 5), divergence=divergence_rows)


def _div_row(entity_type, entity_id, *, severity="EXTREME",
             trading_signal="FRONT_RUN_REALITY", abs_divergence=0.50):
    return {
        "entity_type": entity_type, "entity_id": entity_id,
        "severity": severity, "trading_signal": trading_signal,
        "abs_divergence": abs_divergence,
    }


def _commodity_template(name):
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.COMMODITY]
    return next(t for t in cfg.templates if t.name == name)


def test_commodity_crude_chokepoint_no_intel_returns_false():
    tmpl = _commodity_template("commodity.crude_chokepoint_call")
    assert tmpl.trigger({}).fire is False


def test_commodity_crude_chokepoint_no_relevant_div_returns_false():
    tmpl = _commodity_template("commodity.crude_chokepoint_call")
    # Severe divergence but on an unrelated entity.
    intel = _intel([_div_row("CONFLICT", "haiti_gang_war")])
    assert tmpl.trigger({"intel": intel}).fire is False


def test_commodity_crude_chokepoint_hormuz_picks_brent():
    tmpl = _commodity_template("commodity.crude_chokepoint_call")
    intel = _intel([_div_row("CHOKEPOINT", "hormuz", abs_divergence=0.65)])
    r = tmpl.trigger({"intel": intel})
    assert r.fire is True
    assert r.metadata["underlying"] == "BZ"
    assert r.metadata["chokepoint"] == "HORMUZ"


def test_commodity_crude_chokepoint_malacca_picks_wti():
    tmpl = _commodity_template("commodity.crude_chokepoint_call")
    intel = _intel([_div_row("CHOKEPOINT", "malacca", abs_divergence=0.55)])
    r = tmpl.trigger({"intel": intel})
    assert r.fire is True
    assert r.metadata["underlying"] == "CL"


def test_commodity_crude_chokepoint_ignores_fade_signal():
    tmpl = _commodity_template("commodity.crude_chokepoint_call")
    # Hormuz extreme but trading_signal says fade (narrative overstates).
    intel = _intel([_div_row("CHOKEPOINT", "hormuz", trading_signal="FADE_NARRATIVE")])
    assert tmpl.trigger({"intel": intel}).fire is False


def test_commodity_natgas_supply_fires_on_russia_ukraine():
    tmpl = _commodity_template("commodity.natgas_supply_call")
    intel = _intel([_div_row("CONFLICT", "russia_ukraine", abs_divergence=0.70)])
    assert tmpl.trigger({"intel": intel}).fire is True


def test_commodity_natgas_supply_fires_on_lng_chokepoint():
    tmpl = _commodity_template("commodity.natgas_supply_call")
    intel = _intel([_div_row("CHOKEPOINT", "suez")])
    assert tmpl.trigger({"intel": intel}).fire is True


def test_commodity_gold_sanctions_fires_on_sanctioned_sovereign():
    tmpl = _commodity_template("commodity.gold_sanctions_call")
    intel = _intel([_div_row("SOVEREIGN", "rus", abs_divergence=0.60)])
    assert tmpl.trigger({"intel": intel}).fire is True


def test_commodity_gold_sanctions_fires_on_iran_war_2026():
    tmpl = _commodity_template("commodity.gold_sanctions_call")
    intel = _intel([_div_row("CONFLICT", "iran_war_2026", abs_divergence=0.80)])
    assert tmpl.trigger({"intel": intel}).fire is True


def test_commodity_gold_sanctions_ignores_unrelated_sovereign():
    tmpl = _commodity_template("commodity.gold_sanctions_call")
    intel = _intel([_div_row("SOVEREIGN", "deu")])  # Germany not on the list
    assert tmpl.trigger({"intel": intel}).fire is False


def test_commodity_wheat_blacksea_fires_on_russia_ukraine():
    tmpl = _commodity_template("commodity.wheat_blacksea_call")
    intel = _intel([_div_row("CONFLICT", "russia_ukraine")])
    assert tmpl.trigger({"intel": intel}).fire is True


def test_commodity_wheat_blacksea_fires_on_bosporus():
    tmpl = _commodity_template("commodity.wheat_blacksea_call")
    intel = _intel([_div_row("CHOKEPOINT", "bosporus")])
    assert tmpl.trigger({"intel": intel}).fire is True


def test_commodity_targets_are_futures_options():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.COMMODITY]
    for tmpl in cfg.templates:
        spec = tmpl.target_spec_factory(
            {}, {"underlying": "BZ", "sector_etf": None, "source_entity": ("X", "Y")},
        )
        assert spec.sec_type == "FOP", f"{tmpl.name}: expected FOP"
        assert spec.right == "C", f"{tmpl.name}: expected long calls"


def test_commodity_sleeve_total_budget_under_5pct():
    cfg = sleeves.default_sleeves()[sleeves.Sleeve.COMMODITY]
    total = sum(t.sizing_pct_of_sleeve for t in cfg.templates)
    # Sum of per-template sizing must be ≤ 1.0 (i.e. fits inside sleeve budget).
    assert total <= 1.0
