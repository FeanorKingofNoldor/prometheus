"""Planner + config + DAG wiring tests for the core+wheel book.

The strategy RULES are pinned in test_wheel_engine.py; this file pins
the account-level layer: sizing caps, aggregation, the IV-event guard,
ballast targets, the breaker, and the run_wheel job wiring.
"""

from __future__ import annotations

from datetime import date

from prometheus.wheel.config import load_wheel_config
from prometheus.wheel.planner import (
    BREAKER_DRAWDOWN,
    OpenShortOptionView,
    WheelAccountView,
    build_plan,
)

CFG = load_wheel_config()
D = date(2026, 8, 5)  # a Wednesday, not a quarter month


def _view(**kw) -> WheelAccountView:
    base = dict(
        nav=500_000.0,
        total_cash=500_000.0,
        underlying_shares=0,
        underlying_spot=630.0,
        vix=17.0,
        peak_nav=500_000.0,
    )
    base.update(kw)
    return WheelAccountView(**base)


def _short_put(strike=617.0, contracts=1, credit=6.0, managed=False, mark=None):
    return OpenShortOptionView(
        right="P", strike=strike, expiry=date(2026, 9, 4), contracts=contracts,
        credit_per_share=credit, managed=managed, mark_per_share=mark,
    )


def _short_call(strike=680.0, contracts=1):
    return OpenShortOptionView(
        right="C", strike=strike, expiry=date(2026, 9, 4), contracts=contracts,
        credit_per_share=4.0, managed=False,
    )


# ── Config ───────────────────────────────────────────────────────────


def test_config_loads_validated_spec():
    assert CFG.wheel_allocation == 0.80
    assert {leg.instrument_id for leg in CFG.ballast} == {"TLT.US", "GLD.US"}
    assert sum(leg.weight for leg in CFG.ballast) == 0.20
    assert CFG.params.put_otm == 0.02
    assert CFG.params.call_otm == 0.08
    assert CFG.underlying_symbol == "SPY"
    assert CFG.rebalance == "quarterly"


def test_live_substitutions_only_apply_live():
    assert CFG.ballast_instrument("TLT.US", live=False) == "TLT.US"
    assert CFG.ballast_instrument("TLT.US", live=True) == "DTLA.LSE"
    assert CFG.ballast_instrument("GLD.US", live=True) == "SGLN.LSE"


# ── CSP sizing ───────────────────────────────────────────────────────


def test_csp_count_capped_by_wheel_budget():
    # 500k NAV → 400k wheel budget; strike 617 → $61.7k/block → 6 blocks.
    plan = build_plan(CFG, _view(), today=D)
    csp = [o for o in plan.orders if o.category == "csp"]
    assert len(csp) == 1
    assert csp[0].quantity == 6
    assert csp[0].side == "SELL"
    assert csp[0].strike == 617.0
    assert csp[0].manage_with_profit_take is False


def test_csp_count_capped_by_actual_cash():
    # Budget allows 6 but only $130k unreserved cash → 2 contracts.
    plan = build_plan(CFG, _view(total_cash=130_000.0), today=D)
    csp = [o for o in plan.orders if o.category == "csp"]
    assert csp[0].quantity == 2


def test_csp_respects_max_contracts_per_day():
    plan = build_plan(CFG, _view(nav=20_000_000.0, total_cash=20_000_000.0), today=D)
    csp = [o for o in plan.orders if o.category == "csp"]
    assert csp[0].quantity == CFG.max_contracts_per_day


def test_no_cash_no_puts():
    plan = build_plan(CFG, _view(total_cash=10_000.0), today=D)
    assert [o for o in plan.orders if o.category == "csp"] == []


def test_open_reserves_reduce_new_csp_capacity():
    # 3 open CSPs reserve 3×61.7k; budget 400k → floor(214.9k/61.7k)=3 more.
    puts = tuple(_short_put(contracts=3) for _ in range(1))
    plan = build_plan(CFG, _view(short_puts=puts), today=D)
    csp = [o for o in plan.orders if o.category == "csp"]
    assert csp[0].quantity == 3


def test_rich_vol_csp_is_managed():
    plan = build_plan(CFG, _view(vix=31.0), today=D)
    csp = [o for o in plan.orders if o.category == "csp"]
    assert csp[0].manage_with_profit_take is True
    assert csp[0].strike == round(630.0 * 0.95)


# ── Covered calls ────────────────────────────────────────────────────


def test_uncovered_lots_get_one_aggregate_call():
    plan = build_plan(
        CFG,
        _view(underlying_shares=300, short_calls=(_short_call(contracts=1),)),
        today=D,
    )
    cc = [o for o in plan.orders if o.category == "cc"]
    assert len(cc) == 1
    assert cc[0].quantity == 2  # 3 lots − 1 covered
    assert cc[0].strike == round(630.0 * 1.08)


def test_fully_covered_shares_write_nothing():
    plan = build_plan(
        CFG,
        _view(underlying_shares=200, short_calls=(_short_call(contracts=2),)),
        today=D,
    )
    assert [o for o in plan.orders if o.category == "cc"] == []


def test_dead_vol_skips_the_call():
    plan = build_plan(CFG, _view(underlying_shares=100, vix=12.0), today=D)
    assert [o for o in plan.orders if o.category == "cc"] == []
    assert any("dead_vol" in s for s in plan.skips)


# ── IV-event guard ───────────────────────────────────────────────────


def test_iv_event_blocks_opens_but_not_profit_take():
    puts = (_short_put(managed=True, credit=6.0, mark=2.5),)
    plan = build_plan(
        CFG,
        _view(underlying_shares=100, short_puts=puts),
        today=D,
        iv_event="FOMC",
    )
    categories = {o.category for o in plan.orders}
    assert "csp" not in categories
    assert "cc" not in categories
    assert "profit_take" in categories
    assert any("iv_event" in s for s in plan.skips)


# ── Profit take ──────────────────────────────────────────────────────


def test_managed_put_at_half_credit_closes_full_size():
    puts = (_short_put(contracts=2, managed=True, credit=6.0, mark=2.9),)
    plan = build_plan(CFG, _view(short_puts=puts), today=D)
    pt = [o for o in plan.orders if o.category == "profit_take"]
    assert len(pt) == 1
    assert pt[0].side == "BUY"
    assert pt[0].quantity == 2


def test_unmanaged_put_never_profit_taken():
    puts = (_short_put(managed=False, credit=6.0, mark=0.5),)
    plan = build_plan(CFG, _view(short_puts=puts), today=D)
    assert [o for o in plan.orders if o.category == "profit_take"] == []


def test_managed_put_without_mark_holds():
    puts = (_short_put(managed=True, credit=6.0, mark=None),)
    plan = build_plan(CFG, _view(short_puts=puts), today=D)
    assert [o for o in plan.orders if o.category == "profit_take"] == []


# ── Ballast ──────────────────────────────────────────────────────────


def test_ballast_bootstrap_buys_to_target_without_rebalance_flag():
    view = _view(ballast_prices={"TLT.US": 82.0, "GLD.US": 371.0})
    plan = build_plan(CFG, view, today=D)
    ballast = {o.instrument_id: o for o in plan.orders if o.category == "ballast"}
    assert set(ballast) == {"TLT.US", "GLD.US"}
    assert ballast["TLT.US"].side == "BUY"
    # 10% of 500k = 50k target → 609 shares at $82.
    assert ballast["TLT.US"].quantity == int(50_000 // 82)


def test_ballast_inside_band_untouched_on_rebalance_day():
    view = _view(
        ballast_values={"TLT.US": 49_000.0, "GLD.US": 50_500.0},
        ballast_prices={"TLT.US": 82.0, "GLD.US": 371.0},
    )
    plan = build_plan(CFG, view, today=D, ballast_rebalance_due=True)
    assert [o for o in plan.orders if o.category == "ballast"] == []


def test_ballast_overweight_sells_on_rebalance_day_only():
    view = _view(
        ballast_values={"TLT.US": 70_000.0, "GLD.US": 50_000.0},
        ballast_prices={"TLT.US": 82.0, "GLD.US": 371.0},
    )
    plan_off = build_plan(CFG, view, today=D, ballast_rebalance_due=False)
    assert [o for o in plan_off.orders if o.category == "ballast"] == []

    plan_on = build_plan(CFG, view, today=D, ballast_rebalance_due=True)
    sells = [o for o in plan_on.orders if o.category == "ballast"]
    assert len(sells) == 1
    assert sells[0].instrument_id == "TLT.US"
    assert sells[0].side == "SELL"
    assert sells[0].quantity == int(20_000 // 82)


# ── Breaker ──────────────────────────────────────────────────────────


def test_breaker_triggers_at_40pct_from_peak_but_wheel_continues():
    view = _view(nav=290_000.0, total_cash=290_000.0, peak_nav=500_000.0)
    plan = build_plan(CFG, view, today=D)
    assert plan.breaker_triggered is True
    assert plan.drawdown >= BREAKER_DRAWDOWN
    # CSP re-entry exempt: puts still planned.
    assert [o for o in plan.orders if o.category == "csp"]


def test_no_breaker_inside_40pct():
    plan = build_plan(CFG, _view(nav=350_000.0, peak_nav=500_000.0), today=D)
    assert plan.breaker_triggered is False


# ── Degenerate inputs ────────────────────────────────────────────────


def test_missing_market_inputs_produce_empty_plan_with_skip():
    plan = build_plan(CFG, _view(underlying_spot=0.0), today=D)
    assert plan.orders == []
    assert any("no_market_inputs" in s for s in plan.skips)


def test_plan_summary_is_json_shaped():
    import json

    plan = build_plan(CFG, _view(underlying_shares=100), today=D)
    encoded = json.dumps(plan.summary())
    assert "csp" in encoded


# ── DAG + daemon wiring ──────────────────────────────────────────────


def test_run_wheel_in_us_dag_post_close_no_deps():
    from apatheon.core.market_state import MarketState

    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("US_EQ", D)
    job = dag.jobs[f"us_eq_run_wheel_{D.isoformat()}"]
    assert job.required_state == MarketState.POST_CLOSE
    assert job.dependencies == ()


def test_run_wheel_absent_from_non_us_dags():
    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("EU_EQ", D)
    assert not any(j.job_type == "run_wheel" for j in dag.jobs.values())


def test_run_wheel_holds_the_ibkr_token():
    from prometheus.orchestration.market_aware_daemon import IBKR_EXCLUSIVE_JOB_TYPES

    assert "run_wheel" in IBKR_EXCLUSIVE_JOB_TYPES
