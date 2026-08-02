#!/usr/bin/env python3.14
"""
Wheel v2: three candidate improvements vs the validated BASELINE wheel,
SPY 1998-01-02 .. 2026-07-31, $250k, fair daily BS marking, both iv_factor 0.85/1.0.

Variants (all cash-secured SPY wheel):
  BASELINE  : put_otm 0.02, call_otm 0.08, 30cd options, hold to expiry.
              Must reproduce wheel_sim_managed HOLD (assert final equity within 0.5%).
  VIXCOND   : parameters chosen at each WRITE from that day's VIX close (no forecasting):
              - VIX > 25 : put written at put_otm 0.05, that option managed with PT50
                           (buy back at <=50% of per-share credit, re-write NEXT day,
                           buy-back cost = BS value x 1.05 + $0.65/ct, as in managed sim)
              - VIX < 13 : covered call SKIPPED this cycle (shares held uncovered,
                           re-check at next cycle date ~target_dte later); puts still 0.02
              - else     : baseline parameters, hold to expiry
  DTE45     : baseline parameters, 45cd options, hold to expiry.
  LADDER3   : 3 equal sub-sleeves ($250k/3 each), each an independent baseline 30d
              wheel, staggered starts at day 0 / +10cd / +20cd. Lots per sub-sleeve =
              floor(sub_budget/(strike*100)); aggregate equity = sum of sleeves.
  VIXCOND45 : VIXCOND rules on 45cd options.

Everything else identical to wheel_sim_managed.py: r=0,q=0 BS; VIX same-day close as
sigma (x iv_factor) at write/mark time only (no look-ahead); sell credit = BS premium
x (1-5%) - $0.65/ct; European assignment at raw close on expiry; expiry = trading day
nearest trade_date+target_dte cd; integer 100-share lots; CSP reserves all cash;
dividends credited daily from adjusted/raw close drift while holding shares.

Deployed capital fraction (daily) = (shares x S + put_strike_reserve) / equity,
averaged over the window; surfaces LADDER3's lot-rounding drag.
"""

import json
import math
import os
import sys
from bisect import bisect_left
from datetime import timedelta

SCRATCH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRATCH)

from wheel_sim_managed import (  # noqa: E402
    COMMISSION, INIT_CASH, PT_FRAC, SLIPPAGE, PUT_OTM, CALL_OTM,
    bs_call, bs_put, bs_value, era_indices, load_data, slice_metrics,
)

VIX_RICH = 25.0        # write puts wider + PT50-managed above this
VIX_LEAN = 13.0        # skip covered call below this
RICH_PUT_OTM = 0.05
LADDER_OFFSETS_CD = (0, 10, 20)


def nearest_expiry_idx(dates, i, target_dte):
    target = dates[i] + timedelta(days=target_dte)
    j = bisect_left(dates, target)
    cands = [k for k in (j - 1, j) if i < k < len(dates)]
    if not cands:
        return None
    return min(cands, key=lambda k: abs((dates[k] - target).days))


# ---------------------------------------------------------------- generalized wheel engine

def run_wheel(dates, close, adj, vix, iv_factor, *, target_dte=30, vix_cond=False,
              init_cash=INIT_CASH, start_idx=0):
    n = len(dates)
    cash = init_cash
    shares = 0
    opt = None           # dict(kind, K, contracts, expiry, credit_ps, pt)
    next_write = start_idx
    equity = [0.0] * n
    deployed = [0.0] * n     # dollars: shares*S + put strike reserve

    prem_events, buyback_events = [], []
    put_assign_idx, call_assign_idx = [], []
    rich_put_idx, pt_fire_idx, call_skip_idx = [], [], []
    put_sales = call_sales = 0

    for i in range(n):
        S = close[i]

        # 1) dividend credit while holding (total-return-implied)
        if shares > 0 and i > 0:
            div_ps = close[i - 1] * adj[i] / adj[i - 1] - close[i]
            cash += div_ps * shares

        # 2) natural expiry settlement (raw close, European); same-day re-write allowed
        if opt is not None and i == opt["expiry"]:
            K, c = opt["K"], opt["contracts"]
            if opt["kind"] == "put":
                if S < K:
                    cash -= K * 100 * c
                    shares += 100 * c
                    put_assign_idx.append(i)
            else:
                if S > K:
                    cash += K * 100 * c
                    shares -= 100 * c
                    call_assign_idx.append(i)
            opt = None

        # 3) PT50 management — only on options written with the pt flag (VIXCOND rich-vol puts)
        elif opt is not None and opt["pt"]:
            sigma = vix[i] / 100.0 * iv_factor
            dte = (dates[opt["expiry"]] - dates[i]).days
            val_ps = bs_value(opt["kind"], S, opt["K"], sigma, dte / 365.0)
            if val_ps <= PT_FRAC * opt["credit_ps"]:
                c = opt["contracts"]
                cost = val_ps * 100 * c * (1.0 + SLIPPAGE) + COMMISSION * c
                cash -= cost
                buyback_events.append((i, cost))
                pt_fire_idx.append(i)
                opt = None
                next_write = i + 1   # re-write NEXT day

        # 4) write next option (VIX read same-day close, at write time only)
        if opt is None and i >= next_write:
            exp = nearest_expiry_idx(dates, i, target_dte)
            if exp is not None:
                sigma = vix[i] / 100.0 * iv_factor
                T = (dates[exp] - dates[i]).days / 365.0
                if shares == 0:
                    put_otm, pt_flag = PUT_OTM, False
                    if vix_cond and vix[i] > VIX_RICH:
                        put_otm, pt_flag = RICH_PUT_OTM, True
                    K = S * (1.0 - put_otm)
                    c = int(cash // (K * 100.0))
                    if c > 0:
                        prem_ps = bs_put(S, K, sigma, T)
                        net = prem_ps * 100 * c * (1.0 - SLIPPAGE) - COMMISSION * c
                        cash += net
                        put_sales += 1
                        opt = {"kind": "put", "K": K, "contracts": c, "expiry": exp,
                               "credit_ps": prem_ps * (1.0 - SLIPPAGE), "pt": pt_flag}
                        prem_events.append((i, net))
                        if pt_flag:
                            rich_put_idx.append(i)
                    # c == 0: stay in cash, retry next day
                else:
                    if vix_cond and vix[i] < VIX_LEAN:
                        # hold shares uncovered; re-check at next cycle date
                        call_skip_idx.append(i)
                        next_write = exp
                    else:
                        K = S * (1.0 + CALL_OTM)
                        c = shares // 100
                        prem_ps = bs_call(S, K, sigma, T)
                        net = prem_ps * 100 * c * (1.0 - SLIPPAGE) - COMMISSION * c
                        cash += net
                        call_sales += 1
                        opt = {"kind": "call", "K": K, "contracts": c, "expiry": exp,
                               "credit_ps": prem_ps * (1.0 - SLIPPAGE), "pt": False}
                        prem_events.append((i, net))

        # 5) mark-to-market at fair BS value (today's VIX sigma, shrinking T)
        opt_mark = 0.0
        reserve = 0.0
        if opt is not None:
            sigma = vix[i] / 100.0 * iv_factor
            T = (dates[opt["expiry"]] - dates[i]).days / 365.0
            opt_mark = bs_value(opt["kind"], S, opt["K"], sigma, T) * 100 * opt["contracts"]
            if opt["kind"] == "put":
                reserve = opt["K"] * 100 * opt["contracts"]
        equity[i] = cash + shares * S - opt_mark
        deployed[i] = shares * S + reserve
        assert shares % 100 == 0, f"non-lot share count {shares} on {dates[i]}"
        assert equity[i] > 0, f"equity non-positive {equity[i]:.2f} on {dates[i]}"

    return {
        "equity": equity,
        "deployed": deployed,
        "prem_events": prem_events,
        "buyback_events": buyback_events,
        "put_assign_idx": put_assign_idx,
        "call_assign_idx": call_assign_idx,
        "rich_put_idx": rich_put_idx,
        "pt_fire_idx": pt_fire_idx,
        "call_skip_idx": call_skip_idx,
        "put_sales": put_sales,
        "call_sales": call_sales,
    }


def merge_runs(runs, n):
    """Aggregate independent sub-sleeve runs (LADDER3): sum equity/deployed, concat events."""
    out = {
        "equity": [sum(r["equity"][i] for r in runs) for i in range(n)],
        "deployed": [sum(r["deployed"][i] for r in runs) for i in range(n)],
        "put_sales": sum(r["put_sales"] for r in runs),
        "call_sales": sum(r["call_sales"] for r in runs),
    }
    for key in ("prem_events", "buyback_events"):
        out[key] = sorted([ev for r in runs for ev in r[key]])
    for key in ("put_assign_idx", "call_assign_idx", "rich_put_idx",
                "pt_fire_idx", "call_skip_idx"):
        out[key] = sorted([i for r in runs for i in r[key]])
    return out


# ---------------------------------------------------------------- window stats

def window_stats(dates, res, a, b):
    prem = sum(v for i, v in res["prem_events"] if a <= i <= b)
    bb = sum(v for i, v in res["buyback_events"] if a <= i <= b)
    dep = sum(res["deployed"][i] / res["equity"][i] for i in range(a, b + 1)) / (b - a + 1)
    return {
        "net_premium_after_costs": prem - bb,
        "premium_credits": prem,
        "buyback_cost": bb,
        "puts_assigned": sum(1 for i in res["put_assign_idx"] if a <= i <= b),
        "calls_assigned": sum(1 for i in res["call_assign_idx"] if a <= i <= b),
        "rich_vol_puts_written": sum(1 for i in res["rich_put_idx"] if a <= i <= b),
        "pt50_buybacks": sum(1 for i in res["pt_fire_idx"] if a <= i <= b),
        "calls_skipped": sum(1 for i in res["call_skip_idx"] if a <= i <= b),
        "avg_deployed_fraction": dep,
    }


# ---------------------------------------------------------------- main

def main():
    dates, close, adj, vix = load_data()
    n = len(dates)
    print(f"# SPY rows {n}  {dates[0]} .. {dates[-1]}   VIX range {min(vix):.2f}..{max(vix):.2f}")
    print(f"# wheel v2: BASELINE / VIXCOND / DTE45 / LADDER3 / VIXCOND45  x  iv_factor (0.85, 1.0)")
    print(f"# VIXCOND: VIX>{VIX_RICH:g} -> put_otm {RICH_PUT_OTM:g}+PT50; VIX<{VIX_LEAN:g} -> skip call")

    windows = era_indices(dates)
    bench_eq = [INIT_CASH * adj[i] / adj[0] for i in range(n)]
    ladder_starts = [bisect_left(dates, dates[0] + timedelta(days=off))
                     for off in LADDER_OFFSETS_CD]

    def build(iv_factor, variant):
        if variant == "BASELINE":
            return run_wheel(dates, close, adj, vix, iv_factor, target_dte=30)
        if variant == "VIXCOND":
            return run_wheel(dates, close, adj, vix, iv_factor, target_dte=30, vix_cond=True)
        if variant == "DTE45":
            return run_wheel(dates, close, adj, vix, iv_factor, target_dte=45)
        if variant == "LADDER3":
            subs = [run_wheel(dates, close, adj, vix, iv_factor, target_dte=30,
                              init_cash=INIT_CASH / 3.0, start_idx=s)
                    for s in ladder_starts]
            return merge_runs(subs, n)
        if variant == "VIXCOND45":
            return run_wheel(dates, close, adj, vix, iv_factor, target_dte=45, vix_cond=True)
        raise ValueError(variant)

    VARIANTS = ["BASELINE", "VIXCOND", "DTE45", "LADDER3", "VIXCOND45"]

    results = {"meta": {
        "window": [str(dates[0]), str(dates[-1])],
        "init_cash": INIT_CASH,
        "put_otm": PUT_OTM, "call_otm": CALL_OTM,
        "commission_per_contract": COMMISSION,
        "slippage_pct_of_premium": SLIPPAGE,
        "pt_threshold": PT_FRAC,
        "vix_rich": VIX_RICH, "vix_lean": VIX_LEAN, "rich_put_otm": RICH_PUT_OTM,
        "ladder_offsets_calendar_days": list(LADDER_OFFSETS_CD),
        "ladder_start_dates": [str(dates[s]) for s in ladder_starts],
        "rate": 0.0,
        "notes": [
            "engine identical to wheel_sim_managed.py HOLD path; fair daily BS marks",
            "VIX read at same-day close, at write/mark time only (no look-ahead)",
            "VIXCOND branches decided per WRITE; PT50 armed only on rich-vol puts",
            "call skip: shares uncovered until next cycle date (nearest +target_dte trading day)",
            "LADDER3: 3 x $83.3k independent baseline 30d wheels, starts day0/+10cd/+20cd, equity summed",
            "avg_deployed_fraction = mean of (shares*S + put strike reserve)/equity",
            "BASELINE asserted to reproduce wheel_managed_results.json HOLD final equity within 0.5%",
        ],
    }, "benchmark": {}, "variants": []}

    for label, a, b in windows:
        results["benchmark"][label] = slice_metrics(dates, bench_eq, a, b)

    # prior HOLD final equity for the sanity assert
    prior_hold = {}
    prior_path = os.path.join(SCRATCH, "wheel_managed_results.json")
    with open(prior_path) as fh:
        for e in json.load(fh)["variants"]:
            if e["variant"] == "HOLD":
                prior_hold[e["iv_factor"]] = e["final_equity"]

    rows = {}
    for variant in VARIANTS:
        for f in (0.85, 1.0):
            res = build(f, variant)
            name = f"{variant}_iv{f:g}"
            if variant == "BASELINE":
                prior = prior_hold[f]
                diff = abs(res["equity"][-1] - prior) / prior
                assert diff < 0.005, (
                    f"BASELINE iv{f:g} final equity {res['equity'][-1]:.2f} vs "
                    f"managed HOLD {prior:.2f} (diff {diff:.4%})")
                print(f"# sanity OK: BASELINE_iv{f:g} final equity {res['equity'][-1]:,.2f} "
                      f"vs managed HOLD {prior:,.2f} (diff {diff:.5%})")
            entry = {"variant": variant, "iv_factor": f, "name": name,
                     "put_sales": res["put_sales"], "call_sales": res["call_sales"],
                     "final_equity": res["equity"][-1], "windows": {}}
            for label, a, b in windows:
                m = slice_metrics(dates, res["equity"], a, b)
                m.update(window_stats(dates, res, a, b))
                entry["windows"][label] = m
            results["variants"].append(entry)
            rows[name] = entry

    hdr = (f"{'variant':<16} {'totret':>9} {'cagr':>7} {'sharpe':>7} {'maxDD':>7} "
           f"{'netprem$':>11} {'putsA':>5} {'callsA':>6} "
           f"{'rvP':>4} {'PT':>4} {'cSk':>4} {'dep%':>6}")
    for label, a, b in windows:
        print(f"\n== {label} ({dates[a]} .. {dates[b]}) ==")
        print(hdr)
        bm = results["benchmark"][label]
        print(f"{'BUY_AND_HOLD_SPY':<16} {bm['total_return']:>8.1%} {bm['cagr']:>7.2%} "
              f"{bm['sharpe']:>7.2f} {bm['max_dd']:>7.1%} {'-':>11} {'-':>5} {'-':>6} "
              f"{'-':>4} {'-':>4} {'-':>4} {'-':>6}")
        for name, entry in rows.items():
            w = entry["windows"][label]
            print(f"{name:<16} {w['total_return']:>8.1%} {w['cagr']:>7.2%} "
                  f"{w['sharpe']:>7.2f} {w['max_dd']:>7.1%} "
                  f"{w['net_premium_after_costs']:>11,.0f} "
                  f"{w['puts_assigned']:>5} {w['calls_assigned']:>6} "
                  f"{w['rich_vol_puts_written']:>4} {w['pt50_buybacks']:>4} "
                  f"{w['calls_skipped']:>4} {w['avg_deployed_fraction']:>6.1%}")

    out_path = os.path.join(SCRATCH, "wheel_v2_results.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"\n# results -> {out_path}")

    # verdict vs BASELINE, FULL window, both iv factors
    print("\n# vs BASELINE (FULL window):")
    verdict = {}
    for variant in VARIANTS[1:]:
        wins_cagr = wins_sharpe = 0
        for f in (0.85, 1.0):
            base = rows[f"BASELINE_iv{f:g}"]["windows"]["FULL"]
            w = rows[f"{variant}_iv{f:g}"]["windows"]["FULL"]
            dc, ds = w["cagr"] - base["cagr"], w["sharpe"] - base["sharpe"]
            wins_cagr += dc > 0
            wins_sharpe += ds > 0
            print(f"#   iv{f:g} {variant:<10}: dCAGR {dc:+.2%}  dSharpe {ds:+.3f}  "
                  f"dMaxDD {w['max_dd'] - base['max_dd']:+.1%}")
        verdict[variant] = (wins_cagr, wins_sharpe)
    robust = [v for v, (c, s) in verdict.items() if c == 2 or s == 2]
    fragile = [v for v, (c, s) in verdict.items() if v not in robust and (c == 1 or s == 1)]
    losers = [v for v, (c, s) in verdict.items() if c + s == 0]
    print(f"\n# RANKED: robust winners (beat BASELINE CAGR and/or Sharpe at BOTH iv): "
          f"{robust or 'none'}; fragile (one iv only): {fragile or 'none'}; "
          f"losers: {losers or 'none'}")
    for v, (c, s) in verdict.items():
        print(f"#   {v}: CAGR wins {c}/2, Sharpe wins {s}/2")


if __name__ == "__main__":
    main()
