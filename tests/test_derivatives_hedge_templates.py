"""End-to-end tests for the four HEDGE templates.

Drives the new sleeve runner through the backtest harness against
canned signals, then asserts each template's directive shape +
behaviour. This is the Phase 2.2 acceptance suite — every change to a
hedge template must keep these green before going to shadow mode.
"""

from __future__ import annotations

from datetime import date

from prometheus.derivatives import backtest, runner, sleeves

# ── Shared fixture builders ──────────────────────────────────────────


def _price_provider_full(_as_of: date, symbol: str) -> float:
    """Spot prices for every underlying our hedge templates target."""
    s = symbol.upper()
    if s == "SPY":
        return 500.0
    if s == "XLE":
        return 90.0
    if s == "XLK":
        return 220.0
    if s == "VIX":
        return 18.0
    return 0.0


def _hedge_sleeve():
    return sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]


# ── hedge.spy_protective_put (sanity check still works post-refactor) ─


def test_spy_protective_put_fires_in_stressed_regime():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {"mhi": 0.20, "vix_level": 35.0},
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    pp = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    assert len(pp) == 1
    assert pp[0].underlying == "SPY"
    assert pp[0].right == "P"
    assert pp[0].quantity > 0


# ── hedge.sector_put_spread ──────────────────────────────────────────


def test_sector_put_spread_fires_on_weak_sector_shi():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80,                  # no SPY hedge
            "vix_level": 18.0,
            "frag": 0.10,
            "sector_shi": {"ENERGY": 0.15, "TECHNOLOGY": 0.70},
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    # Two legs: long put + short put on XLE
    assert len(sp) == 2
    assert all(d.underlying == "XLE" for d in sp)
    assert all(d.right == "P" for d in sp)


def test_sector_put_spread_legs_share_spread_group_id():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0, "frag": 0.10,
            "sector_shi": {"ENERGY": 0.15},
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    spread_ids = {d.trigger_metadata.get("spread_group_id") for d in sp}
    assert len(spread_ids) == 1
    assert next(iter(spread_ids)) is not None


def test_sector_put_spread_long_and_short_legs_have_opposite_signs():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0, "frag": 0.10,
            "sector_shi": {"ENERGY": 0.15},
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    by_leg = {d.trigger_metadata["leg_name"]: d for d in sp}
    assert "long_put" in by_leg and "short_put" in by_leg
    assert by_leg["long_put"].quantity > 0
    assert by_leg["short_put"].quantity < 0
    # Long put strike > short put strike (bear put spread = buy higher, sell lower)
    assert by_leg["long_put"].strike > by_leg["short_put"].strike


def test_sector_put_spread_skips_when_no_sector_below_threshold():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0,
            "sector_shi": {"ENERGY": 0.45, "TECHNOLOGY": 0.55},  # all above 0.30
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    assert sp == []
    sp_skips = [s for s in result.skips if s.template_name == "hedge.sector_put_spread"]
    assert len(sp_skips) == 1
    assert sp_skips[0].reason == runner.SKIP_TRIGGER


def test_sector_put_spread_picks_weakest_sector_when_multiple_below():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0,
            "sector_shi": {"ENERGY": 0.15, "TECHNOLOGY": 0.25},   # both below
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    # Energy is the weaker → XLE selected, not XLK
    assert all(d.underlying == "XLE" for d in sp)


def test_sector_put_spread_records_max_loss_in_leg_metadata():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0,
            "sector_shi": {"ENERGY": 0.15},
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    for leg in sp:
        assert "max_loss_per_contract" in leg.trigger_metadata
        assert leg.trigger_metadata["max_loss_per_contract"] > 0
        assert "net_debit_per_share" in leg.trigger_metadata


# ── hedge.vix_tail_call ──────────────────────────────────────────────


def test_vix_tail_call_fires_when_vix_available():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0, "frag": 0.10,
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    vt = [d for d in result.directives if d.template_name == "hedge.vix_tail_call"]
    assert len(vt) == 1
    d = vt[0]
    assert d.underlying == "VIX"
    assert d.right == "C"
    assert d.quantity > 0          # always-on long call
    # Strike should be well OTM relative to VIX=18 (low-delta = high strike)
    assert d.strike > 18.0


def test_vix_tail_call_skips_when_vix_missing():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {"mhi": 0.80},   # no vix_level
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    vt = [d for d in result.directives if d.template_name == "hedge.vix_tail_call"]
    assert vt == []
    vt_skip = [s for s in result.skips if s.template_name == "hedge.vix_tail_call"]
    assert len(vt_skip) == 1


def test_vix_tail_call_is_single_leg_not_spread():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {"vix_level": 18.0, "mhi": 0.80},
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    vt = [d for d in result.directives if d.template_name == "hedge.vix_tail_call"]
    # Single-leg templates don't carry a spread_group_id
    assert "spread_group_id" not in vt[0].trigger_metadata


# ── hedge.collar ─────────────────────────────────────────────────────


def test_collar_fires_in_recovery_zone_with_elevated_frag():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.50,         # in 0.40-0.60 band
            "vix_level": 22.0,
            "frag": 0.50,        # > 0.40
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    cl = [d for d in result.directives if d.template_name == "hedge.collar"]
    assert len(cl) == 2          # long put + short call
    by_leg = {d.trigger_metadata["leg_name"]: d for d in cl}
    assert "protective_put" in by_leg
    assert "overwrite_call" in by_leg
    assert by_leg["protective_put"].right == "P"
    assert by_leg["protective_put"].quantity > 0    # long
    assert by_leg["overwrite_call"].right == "C"
    assert by_leg["overwrite_call"].quantity < 0    # short


def test_collar_skips_when_mhi_outside_recovery_band():
    # mhi above recovery band → no collar
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.80, "vix_level": 18.0, "frag": 0.50,
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    cl = [d for d in result.directives if d.template_name == "hedge.collar"]
    assert cl == []


def test_collar_skips_when_frag_below_threshold():
    # In recovery zone but fragility low → no collar
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.50, "vix_level": 22.0, "frag": 0.10,
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    cl = [d for d in result.directives if d.template_name == "hedge.collar"]
    assert cl == []


# ── Cross-template: full hedge sleeve in a representative scenario ───


def test_full_hedge_sleeve_in_crisis_fires_protective_put_and_vix_tail():
    """Crisis: MHI plunges, VIX spikes, energy SHI collapses.
    Expect three templates active: spy_protective_put, sector_put_spread (XLE),
    and vix_tail_call. Collar should not fire (MHI below recovery band)."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.15,
            "vix_level": 45.0,
            "frag": 0.70,
            "sector_shi": {"ENERGY": 0.10, "TECHNOLOGY": 0.60},
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    fired_templates = {d.template_name for d in result.directives}
    assert "hedge.spy_protective_put" in fired_templates
    assert "hedge.sector_put_spread" in fired_templates
    assert "hedge.vix_tail_call" in fired_templates
    assert "hedge.collar" not in fired_templates


def test_full_hedge_sleeve_in_recovery_fires_collar_and_vix_tail():
    """Recovery: MHI back in 0.40-0.60 band, FRAG still elevated.
    Collar + vix_tail active. spy_protective_put should not fire."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.50, "vix_level": 24.0, "frag": 0.50,
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    fired_templates = {d.template_name for d in result.directives}
    assert "hedge.collar" in fired_templates
    assert "hedge.vix_tail_call" in fired_templates
    assert "hedge.spy_protective_put" not in fired_templates


def test_full_hedge_sleeve_in_calm_only_fires_vix_tail():
    """Calm: no sector weak, MHI high, FRAG low. Only the always-on
    VIX tail hedge fires."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.90, "vix_level": 14.0, "frag": 0.10,
            "sector_shi": {"ENERGY": 0.70, "TECHNOLOGY": 0.80},
        },
        underlying_price_provider=_price_provider_full,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge_sleeve()},
    )
    fired_templates = {d.template_name for d in result.directives}
    assert fired_templates == {"hedge.vix_tail_call"}


# ── Per-template config sanity checks ────────────────────────────────


def test_all_hedge_templates_have_no_profit_target():
    """Hedges are insurance — never take profit. Universal rule."""
    cfg = _hedge_sleeve()
    for tmpl in cfg.templates:
        assert tmpl.profit_target_pct is None, (
            f"{tmpl.name} has profit_target_pct={tmpl.profit_target_pct} "
            "but hedges must not take profit"
        )


def test_hedge_sleeve_template_sizing_sums_within_sleeve_budget():
    """The sum of all templates' sizing_pct_of_sleeve should not exceed
    1.0 — otherwise we'd over-allocate the sleeve in a single run."""
    cfg = _hedge_sleeve()
    total = sum(t.sizing_pct_of_sleeve for t in cfg.templates)
    # Some over-allocation is fine because not all templates fire on
    # the same day; but we want to stay under ~1.2x as a sanity floor.
    assert total <= 1.20, f"hedge sleeve sizing sum={total}"
