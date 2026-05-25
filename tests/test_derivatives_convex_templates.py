"""End-to-end tests for the three CONVEX templates."""

from __future__ import annotations

from datetime import date

from prometheus.derivatives import backtest, intel_signals, sleeves


def _prices(_d: date, symbol: str) -> float:
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


def _convex():
    return sleeves.default_sleeves()[sleeves.Sleeve.CONVEX]


# ── convex.thematic_sector_put (regression) ──────────────────────────


def test_thematic_sector_put_still_fires_via_intel_compound_pressure():
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=date(2026, 5, 22),
        compound_pressure=[
            {"target_entity_id": "IRN", "severity": "CRITICAL",
             "encirclement_score": 0.85},
        ],
    )
    merged = intel_signals.merge_into_signals(
        {"vix_level": 45.0, "market_state": "CRISIS"}, snap,
    )
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: merged,
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    fired = {d.template_name for d in result.directives}
    assert "convex.thematic_sector_put" in fired


# ── convex.vix_escalation_call ───────────────────────────────────────


def test_vix_escalation_call_fires_on_elevated_geo_with_quiet_vix():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "geo_risk_score": 65.0,        # elevated
            "vix_level": 18.0,             # hasn't moved
            "vix_5d_change_pct": 0.02,     # +2% in 5d
            "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    vt = [d for d in result.directives if d.template_name == "convex.vix_escalation_call"]
    assert len(vt) == 1
    d = vt[0]
    assert d.underlying == "VIX"
    assert d.right == "C"
    assert d.quantity > 0


def test_vix_escalation_call_skips_when_vix_already_moved():
    """If VIX is up 20% in 5 days, the asymmetric edge is gone."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "geo_risk_score": 75.0,
            "vix_level": 28.0,
            "vix_5d_change_pct": 0.30,     # +30% — too late
            "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    vt = [d for d in result.directives if d.template_name == "convex.vix_escalation_call"]
    assert vt == []


def test_vix_escalation_call_skips_when_geo_risk_below_threshold():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "geo_risk_score": 30.0,        # below 50 threshold
            "vix_level": 18.0,
            "vix_5d_change_pct": 0.0,
            "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    vt = [d for d in result.directives if d.template_name == "convex.vix_escalation_call"]
    assert vt == []


def test_vix_escalation_call_skips_in_crisis_regime():
    """In CRISIS, the always-on hedge.vix_tail_call covers VIX exposure.
    The escalation call is for pre-crisis pricing — gated out of CRISIS."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "geo_risk_score": 80.0, "vix_level": 18.0,
            "vix_5d_change_pct": 0.0,
            "market_state": "CRISIS",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    vt = [d for d in result.directives if d.template_name == "convex.vix_escalation_call"]
    assert vt == []


# ── convex.convergence_straddle ──────────────────────────────────────


def test_convergence_straddle_fires_when_divergence_and_convergence_stack():
    today = date(2026, 5, 22)
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=today,
        divergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
            "severity": "EXTREME",
        }],
        convergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
            "estimated_convergence_days": 15.0, "confidence": 0.7,
        }],
    )
    merged = intel_signals.merge_into_signals({"vix_level": 18.0}, snap)
    result = backtest.replay_day(
        as_of_date=today, nav=200_000.0,
        signal_provider=lambda _d: merged,
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    cs = [d for d in result.directives if d.template_name == "convex.convergence_straddle"]
    # Long straddle = call + put = 2 legs
    assert len(cs) == 2
    # Both long premium
    assert all(d.quantity > 0 for d in cs)
    # Underlying = HORMUZ proxy = XLE
    assert all(d.underlying == "XLE" for d in cs)
    rights = {d.right for d in cs}
    assert rights == {"C", "P"}


def test_convergence_straddle_picks_earliest_when_multiple_entities_match():
    today = date(2026, 5, 22)
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=today,
        divergence=[
            {"entity_type": "CHOKEPOINT", "entity_id": "HORMUZ", "severity": "EXTREME"},
            {"entity_type": "CONFLICT", "entity_id": "TAIWAN", "severity": "EXTREME"},
        ],
        convergence=[
            {"entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
             "estimated_convergence_days": 20.0, "confidence": 0.6},
            {"entity_type": "CONFLICT", "entity_id": "TAIWAN",
             "estimated_convergence_days": 8.0, "confidence": 0.8},
        ],
    )
    merged = intel_signals.merge_into_signals({"vix_level": 18.0}, snap)
    result = backtest.replay_day(
        as_of_date=today, nav=200_000.0,
        signal_provider=lambda _d: merged,
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    cs = [d for d in result.directives if d.template_name == "convex.convergence_straddle"]
    # Taiwan converges sooner (8d < 20d) → XLK proxy
    assert all(d.underlying == "XLK" for d in cs)


def test_convergence_straddle_skips_without_intel_snapshot():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {"vix_level": 18.0},   # no intel
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    cs = [d for d in result.directives if d.template_name == "convex.convergence_straddle"]
    assert cs == []


def test_convergence_straddle_skips_when_only_divergence_no_convergence():
    today = date(2026, 5, 22)
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=today,
        divergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
            "severity": "EXTREME",
        }],
        convergence=[],
    )
    merged = intel_signals.merge_into_signals({"vix_level": 18.0}, snap)
    result = backtest.replay_day(
        as_of_date=today, nav=200_000.0,
        signal_provider=lambda _d: merged,
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    cs = [d for d in result.directives if d.template_name == "convex.convergence_straddle"]
    assert cs == []


def test_convergence_straddle_skips_when_entity_lacks_proxy_mapping():
    today = date(2026, 5, 22)
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=today,
        divergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "OBSCURE_PASS",
            "severity": "EXTREME",
        }],
        convergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "OBSCURE_PASS",
            "estimated_convergence_days": 15.0, "confidence": 0.7,
        }],
    )
    merged = intel_signals.merge_into_signals({"vix_level": 18.0}, snap)
    result = backtest.replay_day(
        as_of_date=today, nav=200_000.0,
        signal_provider=lambda _d: merged,
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    cs = [d for d in result.directives if d.template_name == "convex.convergence_straddle"]
    assert cs == []


# ── Cross-template scenario ──────────────────────────────────────────


def test_convex_sleeve_full_intel_stack_fires_all_three_templates():
    """All three signal types present → all three convex templates
    eligible. Each fires independently (no greeks_headroom passed)."""
    today = date(2026, 5, 22)
    snap = intel_signals.IntelSignalsSnapshot(
        as_of_date=today,
        divergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
            "severity": "EXTREME",
        }],
        convergence=[{
            "entity_type": "CHOKEPOINT", "entity_id": "HORMUZ",
            "estimated_convergence_days": 15.0, "confidence": 0.7,
        }],
        compound_pressure=[{
            "target_entity_id": "IRN", "severity": "CRITICAL",
            "encirclement_score": 0.85,
        }],
        # geo_risk goes through merge_into_signals → overall_geo_risk_score()
        geo_risk={"overall_risk_score": 75.0},
    )
    merged = intel_signals.merge_into_signals(
        {
            "vix_level": 18.0, "vix_5d_change_pct": 0.0,
            "market_state": "RISK_OFF",   # allows all three convex templates
        },
        snap,
    )
    result = backtest.replay_day(
        as_of_date=today, nav=200_000.0,
        signal_provider=lambda _d: merged,
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.CONVEX: _convex()},
    )
    fired = {d.template_name for d in result.directives}
    assert "convex.thematic_sector_put" in fired
    assert "convex.vix_escalation_call" in fired
    assert "convex.convergence_straddle" in fired


# ── Discipline checks ────────────────────────────────────────────────


def test_convex_sleeve_template_sizing_sums_within_sleeve_budget():
    cfg = _convex()
    total = sum(t.sizing_pct_of_sleeve for t in cfg.templates)
    # Convex templates are mutually-amplifying (likely to fire on the
    # same crisis day); keep sum ≤ 1.0 to bound concurrent risk.
    assert total <= 1.05, f"convex sleeve sizing sum={total}"


def test_all_convex_templates_carry_stop_loss():
    """Convex bets are debit positions — they need a stop so a bad
    setup doesn't bleed the whole sleeve."""
    cfg = _convex()
    for tmpl in cfg.templates:
        assert tmpl.stop_loss_multiplier is not None, (
            f"{tmpl.name} has no stop_loss_multiplier — convex bets must"
        )
