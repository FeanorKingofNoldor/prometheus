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


def test_ballast_substitutions_apply_in_every_mode():
    # PRIIPs blocks US ETFs on paper too (rejection observed 2026-08-07),
    # so substitution is unconditional and carries explicit routing.
    tlt = CFG.ballast_substitute("TLT.US")
    gld = CFG.ballast_substitute("GLD.US")
    assert tlt is not None and tlt.instrument_id == "DTLA.LSE"
    assert tlt.exchange == "LSEETF" and tlt.currency == "USD"
    assert gld is not None and gld.instrument_id == "IGLN.LSE"
    assert gld.exchange == "LSEETF" and gld.currency == "USD"
    assert CFG.ballast_substitute("SPY.US") is None


def test_ballast_symbol_map_covers_originals_and_twins():
    m = CFG.ballast_symbol_map
    assert m["TLT"] == "TLT.US"
    assert m["DTLA"] == "TLT.US"
    assert m["GLD"] == "GLD.US"
    assert m["IGLN"] == "GLD.US"


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


def test_run_wheel_in_us_dag_clock_gated_no_deps():
    from apatheon.core.market_state import MarketState

    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("US_EQ", D)
    job = dag.jobs[f"us_eq_run_wheel_{D.isoformat()}"]
    # US POST_CLOSE starts 17:30 ET (EODHD delay) — after SPY options
    # stop quoting at 16:15 — so the wheel rides OVERNIGHT (the close
    # gap's state) fenced to the 16:00-16:16 ET options window.
    # POST_CLOSE stays in the set only for forced catch-up runs.
    assert job.required_states == (MarketState.OVERNIGHT, MarketState.POST_CLOSE)
    assert job.dispatch_window_local == ("16:00", "16:16")
    assert job.dependencies == ()


def test_run_wheel_dispatch_window_gating():
    """Window blocks the 17:30+ POST_CLOSE poll but admits the 16:0x gap;
    the clockless (catch-up) call bypasses the window entirely."""
    from datetime import datetime, timezone

    from apatheon.core.market_state import MarketState

    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("US_EQ", D)
    wheel_id = f"us_eq_run_wheel_{D.isoformat()}"

    def runnable_ids(state, now_utc):
        return {
            j.job_id
            for j in dag.get_runnable_jobs(set(), set(), state, now_utc=now_utc)
        }

    # 16:01 ET (20:01 UTC in August) — inside the window, state OVERNIGHT.
    in_window = datetime(2026, 8, 7, 20, 1, tzinfo=timezone.utc)
    assert wheel_id in runnable_ids(MarketState.OVERNIGHT, in_window)

    # 17:35 ET — POST_CLOSE has begun but the options market is closed.
    post_close = datetime(2026, 8, 7, 21, 35, tzinfo=timezone.utc)
    assert wheel_id not in runnable_ids(MarketState.POST_CLOSE, post_close)

    # 03:05 ET OVERNIGHT — way off-window.
    small_hours = datetime(2026, 8, 7, 7, 5, tzinfo=timezone.utc)
    assert wheel_id not in runnable_ids(MarketState.OVERNIGHT, small_hours)

    # Catch-up: forced POST_CLOSE with no clock — window bypassed.
    assert wheel_id in runnable_ids(MarketState.POST_CLOSE, None)


def test_run_wheel_absent_from_non_us_dags():
    from prometheus.orchestration.dag import build_market_dag

    dag = build_market_dag("EU_EQ", D)
    assert not any(j.job_type == "run_wheel" for j in dag.jobs.values())


def test_run_wheel_holds_the_ibkr_token():
    from prometheus.orchestration.market_aware_daemon import IBKR_EXCLUSIVE_JOB_TYPES

    assert "run_wheel" in IBKR_EXCLUSIVE_JOB_TYPES
