"""Tests for the regime-gate check on TemplateConfig.allowed_market_states."""

from __future__ import annotations

from datetime import date

from prometheus.derivatives import backtest, runner, sleeves


def _prices(_d: date, symbol: str) -> float:
    s = symbol.upper()
    if s == "SPY":
        return 500.0
    if s == "XLE":
        return 90.0
    if s == "VIX":
        return 18.0
    return 0.0


def _hedge():
    return sleeves.default_sleeves()[sleeves.Sleeve.HEDGE]


def _income():
    return sleeves.default_sleeves()[sleeves.Sleeve.INCOME]


# ── Hedge gating ─────────────────────────────────────────────────────


def test_protective_put_skips_when_market_state_is_risk_on():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.20,                   # would normally fire
            "vix_level": 35.0,
            "market_state": "RISK_ON",     # but regime-blocked
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    pp_skips = [s for s in result.skips if s.template_name == "hedge.spy_protective_put"]
    assert len(pp_skips) == 1
    assert pp_skips[0].reason == runner.SKIP_REGIME
    assert "RISK_ON" in pp_skips[0].detail


def test_protective_put_fires_when_market_state_is_risk_off():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.20, "vix_level": 35.0, "market_state": "RISK_OFF",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    pp = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    assert len(pp) == 1


def test_sector_put_spread_skips_in_recovery_only_protective_put_fires():
    """Sector spread is RISK_OFF/CRISIS only — RECOVERY skips it,
    but protective_put still fires (it's allowed in RECOVERY)."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.20, "vix_level": 24.0,
            "sector_shi": {"ENERGY": 0.15},
            "market_state": "RECOVERY",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    sp = [d for d in result.directives if d.template_name == "hedge.sector_put_spread"]
    pp = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    assert sp == []   # gated out
    assert len(pp) == 1   # still fires

    sp_skips = [s for s in result.skips if s.template_name == "hedge.sector_put_spread"]
    assert sp_skips[0].reason == runner.SKIP_REGIME


def test_vix_tail_call_fires_in_every_regime():
    """Always-on hedge has empty allowed_market_states tuple."""
    for state in ("RISK_ON", "NEUTRAL", "RECOVERY", "RISK_OFF", "CRISIS"):
        result = backtest.replay_day(
            as_of_date=date(2026, 5, 22),
            nav=200_000.0,
            signal_provider=lambda _d, st=state: {
                "vix_level": 18.0, "market_state": st,
            },
            underlying_price_provider=_prices,
            sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
        )
        vt = [d for d in result.directives if d.template_name == "hedge.vix_tail_call"]
        assert len(vt) == 1, f"vix_tail_call should fire in {state}, got {vt}"


def test_collar_skips_in_crisis_keeps_protective_put():
    """Collar is RECOVERY/RISK_OFF only. CRISIS gates it out but
    protective_put still fires."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.10, "vix_level": 45.0, "frag": 0.70,
            "market_state": "CRISIS",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    cl = [d for d in result.directives if d.template_name == "hedge.collar"]
    pp = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    assert cl == []
    assert len(pp) == 1


# ── Income gating ────────────────────────────────────────────────────


def test_income_short_put_skips_in_risk_off_regime():
    """Income templates are RISK_ON/NEUTRAL only — stress regimes
    pause them so we're not selling premium into a tail."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 22.0, "market_state": "RISK_OFF",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    sp = [d for d in result.directives if d.template_name == "income.spy_short_put"]
    assert sp == []
    sk = [s for s in result.skips if s.template_name == "income.spy_short_put"]
    assert sk[0].reason == runner.SKIP_REGIME


def test_income_short_put_fires_in_neutral_regime():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "vix_level": 20.0, "market_state": "NEUTRAL",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.INCOME: _income()},
    )
    sp = [d for d in result.directives if d.template_name == "income.spy_short_put"]
    assert len(sp) == 1


# ── Back-compat ──────────────────────────────────────────────────────


def test_regime_gate_does_not_fire_when_signals_have_no_market_state():
    """If signals doesn't carry market_state, the gate is permissive
    (back-compat for callers that don't classify regime)."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.20, "vix_level": 35.0,    # no market_state
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    pp = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    # Without market_state, the gate is bypassed and the trigger logic
    # alone decides — mhi=0.20 fires.
    assert len(pp) == 1


def test_market_state_is_case_insensitive():
    """signal lookup uppercases before comparing."""
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=lambda _d: {
            "mhi": 0.20, "vix_level": 35.0, "market_state": "risk_off",
        },
        underlying_price_provider=_prices,
        sleeves_cfg={sleeves.Sleeve.HEDGE: _hedge()},
    )
    pp = [d for d in result.directives if d.template_name == "hedge.spy_protective_put"]
    assert len(pp) == 1
