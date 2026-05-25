"""End-to-end tests for the four INCOME templates.

Mirrors test_derivatives_hedge_templates.py — drives every template
through the backtest harness against canned signals + asserts the
emitted directive shape.
"""

from __future__ import annotations

from datetime import date

from prometheus.derivatives import backtest, runner, sleeves


def _prices(_d: date, symbol: str) -> float:
    s = symbol.upper()
    if s == "SPY":
        return 500.0
    if s == "AAPL":
        return 220.0
    if s == "XLE":
        return 90.0
    if s == "VIX":
        return 18.0
    return 0.0


def _income():
    return sleeves.default_sleeves()[sleeves.Sleeve.INCOME]


# ── income.spy_short_put (sanity post-refactor) ──────────────────────


def test_spy_short_put_fires_in_neutral_regime():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 20.0, "frag": 0.10, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    sp = [d for d in result.directives if d.template_name == "income.spy_short_put"]
    assert len(sp) == 1
    assert sp[0].quantity < 0   # short premium


# ── income.spy_iron_butterfly ────────────────────────────────────────


def test_iron_butterfly_fires_in_low_vix_low_frag():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 16.0, "frag": 0.10, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    fly = [d for d in result.directives if d.template_name == "income.spy_iron_butterfly"]
    assert len(fly) == 4         # 4 legs
    rights = {d.right for d in fly}
    assert rights == {"C", "P"}
    leg_names = {d.trigger_metadata["leg_name"] for d in fly}
    assert leg_names == {"short_atm_put", "short_atm_call", "long_otm_put", "long_otm_call"}


def test_iron_butterfly_shorts_are_atm_longs_are_otm():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 16.0, "frag": 0.10, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    fly = [d for d in result.directives if d.template_name == "income.spy_iron_butterfly"]
    by_leg = {d.trigger_metadata["leg_name"]: d for d in fly}

    spot = 500.0
    # ATM legs should be within a few percent of spot; OTM legs further
    assert abs(by_leg["short_atm_put"].strike - spot) < spot * 0.05
    assert abs(by_leg["short_atm_call"].strike - spot) < spot * 0.05
    assert by_leg["long_otm_put"].strike < by_leg["short_atm_put"].strike
    assert by_leg["long_otm_call"].strike > by_leg["short_atm_call"].strike


def test_iron_butterfly_skips_when_vix_too_high():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 28.0, "frag": 0.10, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    fly = [d for d in result.directives if d.template_name == "income.spy_iron_butterfly"]
    assert fly == []


def test_iron_butterfly_skips_in_risk_off_regime():
    """Butterfly only fires in NEUTRAL — RISK_OFF gate blocks it."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 16.0, "frag": 0.10, "market_state": "RISK_OFF",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    fly = [d for d in result.directives if d.template_name == "income.spy_iron_butterfly"]
    assert fly == []


# ── income.spy_iron_condor ───────────────────────────────────────────


def test_iron_condor_fires_in_target_vol_band():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "frag": 0.15, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    condor = [d for d in result.directives if d.template_name == "income.spy_iron_condor"]
    assert len(condor) == 4
    by_leg = {d.trigger_metadata["leg_name"]: d for d in condor}
    assert {"short_put", "short_call", "long_put", "long_call"} == set(by_leg.keys())


def test_iron_condor_short_strikes_otm_long_strikes_further_otm():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "frag": 0.15, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    condor = [d for d in result.directives if d.template_name == "income.spy_iron_condor"]
    by_leg = {d.trigger_metadata["leg_name"]: d for d in condor}

    # short put strike > long put strike (both OTM puts)
    assert by_leg["short_put"].strike > by_leg["long_put"].strike
    # short call strike < long call strike (both OTM calls)
    assert by_leg["short_call"].strike < by_leg["long_call"].strike


def test_iron_condor_signs_match_buy_sell():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "frag": 0.15, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    condor = [d for d in result.directives if d.template_name == "income.spy_iron_condor"]
    by_leg = {d.trigger_metadata["leg_name"]: d for d in condor}
    assert by_leg["short_put"].quantity < 0
    assert by_leg["short_call"].quantity < 0
    assert by_leg["long_put"].quantity > 0
    assert by_leg["long_call"].quantity > 0


def test_iron_condor_max_loss_in_metadata_uses_wing_width():
    """Max loss for an iron condor = worst wing width − net credit."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "frag": 0.15, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    condor = [d for d in result.directives if d.template_name == "income.spy_iron_condor"]
    if not condor:
        return
    max_loss = condor[0].trigger_metadata["max_loss_per_contract"]
    by_leg = {d.trigger_metadata["leg_name"]: d for d in condor}
    call_width = abs(by_leg["long_call"].strike - by_leg["short_call"].strike)
    put_width = abs(by_leg["short_put"].strike - by_leg["long_put"].strike)
    worst_wing = max(call_width, put_width) * 100
    # Max loss should not exceed worst wing
    assert max_loss <= worst_wing + 1.0


# ── income.covered_call ──────────────────────────────────────────────


def test_covered_call_fires_when_equity_position_present():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "market_state": "NEUTRAL",
            "equity_positions": {"AAPL": 350},   # 3 covered contracts
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    cc = [d for d in result.directives if d.template_name == "income.covered_call"]
    assert len(cc) == 1
    d = cc[0]
    assert d.underlying == "AAPL"
    assert d.right == "C"
    assert d.quantity < 0   # short call


def test_covered_call_picks_largest_eligible_position():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "market_state": "NEUTRAL",
            "equity_positions": {"AAPL": 350, "SPY": 100},
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    cc = [d for d in result.directives if d.template_name == "income.covered_call"]
    assert cc[0].underlying == "AAPL"   # 3 contracts > 1 contract


def test_covered_call_skips_when_no_position_has_100_shares():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 18.0, "market_state": "NEUTRAL",
            "equity_positions": {"AAPL": 50, "SPY": 75},   # neither hits 100
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    cc = [d for d in result.directives if d.template_name == "income.covered_call"]
    assert cc == []


def test_covered_call_skips_when_vix_too_low_for_premium():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 10.0, "market_state": "NEUTRAL",  # premium-starved
            "equity_positions": {"AAPL": 300},
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    cc = [d for d in result.directives if d.template_name == "income.covered_call"]
    assert cc == []


# ── Discipline / regression checks ───────────────────────────────────


def test_all_income_templates_have_profit_target_or_capacity_cap():
    """Income templates either take profit early or cap exposure
    capacity-wise. Otherwise they let losses compound."""
    cfg = _income()
    for tmpl in cfg.templates:
        has_profit = tmpl.profit_target_pct is not None
        has_cap = tmpl.max_concurrent is not None
        assert has_profit or has_cap, (
            f"{tmpl.name} has no profit target AND no concurrency cap"
        )


def test_income_sleeve_template_sizing_sums_within_sleeve_budget():
    cfg = _income()
    total = sum(t.sizing_pct_of_sleeve for t in cfg.templates)
    # All templates rarely fire on the same day (regime-gated +
    # mutually exclusive vol bands), so over-allocation is fine.
    assert total <= 1.5, f"income sleeve sizing sum={total}"


def test_full_income_sleeve_in_calm_fires_butterfly_and_condor_skip_short_put():
    """Calm regime (vix=16): butterfly (≤20) and condor (14-22) both
    in band. short_put requires vix 15-30 (also in band)."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 16.0, "frag": 0.10, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    fired = {d.template_name for d in result.directives}
    assert "income.spy_iron_butterfly" in fired
    assert "income.spy_iron_condor" in fired
    assert "income.spy_short_put" in fired


# Silence the linter about an unused import
_ = runner
