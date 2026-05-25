"""Tests for prometheus.derivatives.backtest.

End-to-end: feed the harness historical-style signals + price providers,
get back daily sleeve directives via the *production* selection +
sizing code (only the IV / liquidity / chain adapters are synthetic).
"""

from __future__ import annotations

from datetime import date

import pytest

from prometheus.derivatives import backtest, runner, sleeves
from prometheus.derivatives.selection import SelectionTrace
from prometheus.derivatives.sizing import SizingResult

# ── Stub providers ──────────────────────────────────────────────────


def _spy_price_provider(_as_of: date, symbol: str) -> float:
    if symbol.upper() == "SPY":
        return 500.0
    if symbol.upper() == "XLE":
        return 90.0
    return 0.0


def _signal_provider_quiet(_as_of: date):
    """Calm regime: hedge trigger off, income trigger on, convex off."""
    return {
        "mhi": 0.80,        # well above 0.40 hedge threshold
        "vix_level": 18.0,  # inside 15-30 income band
        "nav": 200_000.0,
    }


def _signal_provider_stressed(_as_of: date):
    """Stressed regime: hedge fires, income off, convex off."""
    return {
        "mhi": 0.20,        # below 0.40
        "vix_level": 40.0,  # above 30 — income out
        "nav": 200_000.0,
    }


def _signal_provider_crisis(_as_of: date):
    """Crisis: hedge + convex fire on Energy compound pressure."""
    return {
        "mhi": 0.15,
        "vix_level": 45.0,
        "nav": 200_000.0,
        "compound_pressure": {
            "severity": "CRITICAL", "target_sector_etf": "XLE",
        },
    }


# ── Adapter shape tests ──────────────────────────────────────────────


def test_backtest_discovery_returns_synthetic_chain_with_strikes():
    d = backtest.BacktestDiscovery(
        as_of_date=date(2026, 5, 22),
        underlying_price_provider=_spy_price_provider,
    )
    chains = d.discover_option_chain("SPY")
    assert len(chains) == 1
    chain = chains[0]
    assert chain.trading_class == "SPY"
    assert len(chain.strikes) > 5
    assert len(chain.expirations) > 0


def test_backtest_discovery_returns_empty_when_price_unknown():
    d = backtest.BacktestDiscovery(
        as_of_date=date(2026, 5, 22),
        underlying_price_provider=lambda _d, _s: 0.0,
    )
    assert d.discover_option_chain("UNKNOWN") == []


def test_backtest_liquidity_filter_accepts_atm_strikes():
    f = backtest.BacktestLiquidityFilter(
        as_of_date=date(2026, 5, 22),
        signal_provider=_signal_provider_quiet,
        underlying_price_provider=_spy_price_provider,
    )

    class _C:
        def __init__(self, strike):
            self.symbol = "SPY"
            self.strike = strike
            self.right = "P"
            self.lastTradeDateOrContractMonth = "20260619"

    result = f.filter([_C(500), _C(490), _C(480)])
    assert result.accepted_count >= 1


def test_backtest_iv_lookup_returns_iv_for_every_contract():
    iv = backtest.BacktestIvLookup(
        as_of_date=date(2026, 5, 22),
        signal_provider=_signal_provider_quiet,
        underlying_price_provider=_spy_price_provider,
    )

    class _C:
        def __init__(self, strike):
            self.symbol = "SPY"
            self.strike = strike
            self.right = "P"
            self.lastTradeDateOrContractMonth = "20260619"

    results = iv.get_iv_batch([_C(500), _C(480)], fallback_iv=0.22)
    assert len(results) == 2
    for key, res in results.items():
        assert 0.05 <= res.iv <= 1.0   # reasonable equity IV
        assert res.underlying_price > 0


# ── replay_day ───────────────────────────────────────────────────────


def test_replay_day_quiet_regime_fires_income_only():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=_signal_provider_quiet,
        underlying_price_provider=_spy_price_provider,
    )
    template_names = [d.template_name for d in result.directives]
    assert "income.spy_short_put" in template_names
    assert "hedge.spy_protective_put" not in template_names
    assert "convex.thematic_sector_put" not in template_names


def test_replay_day_stressed_regime_fires_hedge_only():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=_signal_provider_stressed,
        underlying_price_provider=_spy_price_provider,
    )
    template_names = [d.template_name for d in result.directives]
    assert "hedge.spy_protective_put" in template_names
    assert "income.spy_short_put" not in template_names


def test_replay_day_crisis_fires_hedge_plus_convex_sector_put():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=_signal_provider_crisis,
        underlying_price_provider=_spy_price_provider,
    )
    template_names = [d.template_name for d in result.directives]
    assert "hedge.spy_protective_put" in template_names
    assert "convex.thematic_sector_put" in template_names

    convex = next(
        d for d in result.directives
        if d.template_name == "convex.thematic_sector_put"
    )
    assert convex.underlying == "XLE"


def test_replay_day_directive_quantity_signs_match_template_is_long():
    result = backtest.replay_day(
        as_of_date=date(2026, 5, 22),
        nav=200_000.0,
        signal_provider=_signal_provider_crisis,
        underlying_price_provider=_spy_price_provider,
    )
    hedge = next(
        d for d in result.directives
        if d.template_name == "hedge.spy_protective_put"
    )
    # Hedges are long premium → positive qty
    assert hedge.quantity > 0


# ── replay_sleeve_pipeline ───────────────────────────────────────────


def test_replay_pipeline_walks_weekdays_only():
    start = date(2026, 5, 18)   # Monday
    end = date(2026, 5, 24)     # Sunday
    result = backtest.replay_sleeve_pipeline(
        start_date=start, end_date=end,
        nav=200_000.0,
        signal_provider=_signal_provider_quiet,
        underlying_price_provider=_spy_price_provider,
    )
    # Mon-Fri = 5 weekdays
    assert result.total_days == 5


def test_replay_pipeline_aggregates_by_template():
    start = date(2026, 5, 18)
    end = date(2026, 5, 22)     # 5 weekdays
    result = backtest.replay_sleeve_pipeline(
        start_date=start, end_date=end,
        nav=200_000.0,
        signal_provider=_signal_provider_quiet,
        underlying_price_provider=_spy_price_provider,
    )
    by_template = result.by_template()
    assert "income.spy_short_put" in by_template
    assert by_template["income.spy_short_put"]["fired"] == 5
    # Hedge never fired in quiet regime — only skipped
    assert by_template["hedge.spy_protective_put"]["fired"] == 0
    assert by_template["hedge.spy_protective_put"]["skipped"] == 5


def test_replay_pipeline_explicit_trading_days_honored():
    explicit = [date(2026, 5, 18), date(2026, 5, 19), date(2026, 5, 20)]
    result = backtest.replay_sleeve_pipeline(
        start_date=explicit[0], end_date=explicit[-1],
        nav=200_000.0,
        signal_provider=_signal_provider_quiet,
        underlying_price_provider=_spy_price_provider,
        trading_days=explicit,
    )
    assert result.total_days == 3


# ── diff_against_legacy ──────────────────────────────────────────────


def _mk_directive(
    template_name="hedge.spy_protective_put",
    underlying="SPY", right="P", strike=480.0,
    expiry="20260815", quantity=3,
):
    return runner.SleeveDirective(
        sleeve=sleeves.Sleeve.HEDGE,
        template_name=template_name, action="OPEN",
        underlying=underlying, right=right, expiry=expiry,
        strike=strike, quantity=quantity, limit_price=4.20,
        iv_used=0.20, iv_source="ibkr_live", delta=-0.27,
        estimated_premium_per_contract=420.0,
        trigger_reason="mhi=0.30 below 0.40",
        trigger_metadata={},
        selection_trace=SelectionTrace(
            underlying=underlying, underlying_price=500.0, expiry=expiry,
            chain_strikes_total=10, chain_strikes_in_window=5,
            liquidity_rejections={}, candidates=[], chosen_index=0,
        ),
        sizing=SizingResult(
            contracts=quantity, capacity_bound=False,
            budget_bound=True, skipped_reason=None,
        ),
        reason="…",
    )


def _wrap_directive(d, as_of):
    return backtest.BacktestDayResult(
        as_of_date=as_of, nav=200_000.0,
        sleeve_results=[
            runner.SleeveRunResult(
                sleeve=sleeves.Sleeve.HEDGE,
                directives=[d], skips=[],
            )
        ],
    )


def test_diff_pairs_matching_new_and_legacy_decisions():
    as_of = date(2026, 5, 22)
    new = _mk_directive(strike=480.0)
    legacy = backtest.LegacyOption(
        as_of_date=as_of, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )
    backtest_result = backtest.BacktestReplayResult(
        start_date=as_of, end_date=as_of, days=[_wrap_directive(new, as_of)],
    )
    diff = backtest.diff_against_legacy(
        backtest_result=backtest_result,
        legacy_by_date={as_of: [legacy]},
    )
    assert len(diff.entries) == 1
    e = diff.entries[0]
    assert e.kind == "both"
    assert e.new_side is not None and e.legacy_side is not None


def test_diff_strike_divergence_count_picks_up_different_strikes():
    as_of = date(2026, 5, 22)
    new = _mk_directive(strike=480.0)
    legacy = backtest.LegacyOption(
        as_of_date=as_of, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )
    backtest_result = backtest.BacktestReplayResult(
        start_date=as_of, end_date=as_of, days=[_wrap_directive(new, as_of)],
    )
    diff = backtest.diff_against_legacy(
        backtest_result=backtest_result,
        legacy_by_date={as_of: [legacy]},
    )
    assert diff.strike_divergence_count == 1


def test_diff_classifies_new_only_when_legacy_silent():
    as_of = date(2026, 5, 22)
    new = _mk_directive()
    backtest_result = backtest.BacktestReplayResult(
        start_date=as_of, end_date=as_of, days=[_wrap_directive(new, as_of)],
    )
    diff = backtest.diff_against_legacy(
        backtest_result=backtest_result,
        legacy_by_date={},
    )
    assert diff.by_kind == {"new_only": 1}


def test_diff_classifies_legacy_only_when_new_silent():
    as_of = date(2026, 5, 22)
    legacy = backtest.LegacyOption(
        as_of_date=as_of, symbol="SPY", right="P", strike=485.0,
        expiry="20260815", quantity=3, strategy="protective_put",
    )
    backtest_result = backtest.BacktestReplayResult(
        start_date=as_of, end_date=as_of,
        days=[backtest.BacktestDayResult(
            as_of_date=as_of, nav=200_000.0, sleeve_results=[],
        )],
    )
    diff = backtest.diff_against_legacy(
        backtest_result=backtest_result,
        legacy_by_date={as_of: [legacy]},
    )
    assert diff.by_kind == {"legacy_only": 1}


def test_diff_respects_custom_template_to_strategy_mapping():
    as_of = date(2026, 5, 22)
    # Custom template name not in default mapping
    new = _mk_directive(template_name="custom.weird_hedge")
    legacy = backtest.LegacyOption(
        as_of_date=as_of, symbol="SPY", right="P", strike=480.0,
        expiry="20260815", quantity=3, strategy="weird_legacy",
    )
    backtest_result = backtest.BacktestReplayResult(
        start_date=as_of, end_date=as_of, days=[_wrap_directive(new, as_of)],
    )
    diff = backtest.diff_against_legacy(
        backtest_result=backtest_result,
        legacy_by_date={as_of: [legacy]},
        template_to_strategy={"custom.weird_hedge": "weird_legacy"},
    )
    assert diff.by_kind == {"both": 1}


def test_diff_summary_handles_multi_day_replay():
    days = [date(2026, 5, 18), date(2026, 5, 19), date(2026, 5, 20)]
    bt_days = [_wrap_directive(_mk_directive(), d) for d in days]
    backtest_result = backtest.BacktestReplayResult(
        start_date=days[0], end_date=days[-1], days=bt_days,
    )
    legacy_map = {
        days[0]: [backtest.LegacyOption(
            as_of_date=days[0], symbol="SPY", right="P", strike=480.0,
            expiry="20260815", quantity=3, strategy="protective_put",
        )],
        # days[1] and days[2] have no legacy decisions
    }
    diff = backtest.diff_against_legacy(
        backtest_result=backtest_result, legacy_by_date=legacy_map,
    )
    assert diff.by_kind == {"both": 1, "new_only": 2}


# Used implicitly for the type imports above
_ = pytest
