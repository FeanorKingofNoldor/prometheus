#!/usr/bin/env python3.14
"""
Wheel (CSP + covered-call) on SPY with POSITION MANAGEMENT, 1998-01-02 .. 2026-07-31.

Extends wheel_sim.py (put_otm=0.02, call_otm=0.08 — the winning shape) with:

1. Daily Black-Scholes marking of the open short option:
   sigma = VIX[current day]/100 * iv_factor, T = remaining calendar days / 365.
   Equity curve uses this fair mark (no more intrinsic-only flat phases), so the
   daily Sharpe is honest.
2. Management variants (same underlying wheel logic for all):
   - HOLD      : hold to expiry (prior behavior, but fair daily marking).
   - PT50      : buy back when BS value <= 50% of the per-share credit received
                 (credit = BS premium at sale x (1-SLIPPAGE)); re-write fresh 30d
                 option the NEXT day.
   - DTE7      : buy back unconditionally once <=7 calendar days to expiry
                 (never hold gamma week); re-write next day.
   - PT50+DTE7 : whichever triggers first (a day where both hold counts as PT).
   Buy-back cost = BS value x 1.05 + $0.65/contract (slippage against you both ways).
3. Per-variant tracking: early closes (profit-takes vs gamma-week), average
   calendar days held per option, total buy-back dollars paid.

Everything else identical to wheel_sim.py: r=0, q=0 BS; total-return-implied
dividend credit while holding shares; sell credit = BS premium x (1-5%) - $0.65/ct;
European assignment at raw close on expiry; expiry = trading day nearest +30cd;
integer 100-share lots; CSP reserves all cash.
"""

import json
import math
import os
from bisect import bisect_left
from datetime import date, timedelta

import psycopg2

SCRATCH = os.path.dirname(os.path.abspath(__file__))
START = date(1998, 1, 2)
END = date(2026, 7, 31)
INIT_CASH = 250_000.0
COMMISSION = 0.65          # per contract
SLIPPAGE = 0.05            # fraction of premium (both selling and buying back)
TARGET_DTE = 30            # calendar days
PUT_OTM = 0.02
CALL_OTM = 0.08
PT_FRAC = 0.50             # profit-take threshold: BS value <= 50% of credit
DTE_CLOSE = 7              # unconditional close at <=7 calendar days to expiry

ERAS = [
    ("1998-2009", date(1998, 1, 1), date(2009, 12, 31)),
    ("2010-2019", date(2010, 1, 1), date(2019, 12, 31)),
    ("2020-2026", date(2020, 1, 1), date(2026, 12, 31)),
]

VARIANTS = [
    ("HOLD", False, False),
    ("PT50", True, False),
    ("DTE7", False, True),
    ("PT50+DTE7", True, True),
]

# ---------------------------------------------------------------- data


def load_data():
    conn = psycopg2.connect(
        host="localhost", port=6432, user="prometheus",
        dbname="prometheus_historical", password=os.environ["HISTORICAL_DB_PASSWORD"],
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_date, close, adjusted_close FROM prices_daily "
        "WHERE instrument_id='SPY.US' AND trade_date BETWEEN %s AND %s "
        "ORDER BY trade_date", (START, END))
    spy = cur.fetchall()
    cur.execute(
        "SELECT trade_date, close FROM prices_daily "
        "WHERE instrument_id='VIX.INDX' AND trade_date <= %s ORDER BY trade_date", (END,))
    vix_rows = cur.fetchall()
    conn.close()

    vix_map = {d: float(c) for d, c in vix_rows if c is not None}
    dates, close, adj, vix = [], [], [], []
    last_vix = None
    vix_dates_sorted = sorted(vix_map)
    for d, c, a in spy:
        dates.append(d)
        close.append(float(c))
        adj.append(float(a))
        if d in vix_map:
            last_vix = vix_map[d]
        elif last_vix is None:
            j = bisect_left(vix_dates_sorted, d) - 1
            last_vix = vix_map[vix_dates_sorted[j]]
        vix.append(last_vix)
    return dates, close, adj, vix


# ---------------------------------------------------------------- black-scholes (r=0, q=0)

def _phi(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_put(S, K, sigma, T):
    if sigma <= 0 or T <= 0:
        return max(K - S, 0.0)
    st = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T) / st
    d2 = d1 - st
    return K * _phi(-d2) - S * _phi(-d1)


def bs_call(S, K, sigma, T):
    if sigma <= 0 or T <= 0:
        return max(S - K, 0.0)
    st = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + 0.5 * sigma * sigma * T) / st
    d2 = d1 - st
    return S * _phi(d1) - K * _phi(d2)


def bs_value(kind, S, K, sigma, T):
    return bs_put(S, K, sigma, T) if kind == "put" else bs_call(S, K, sigma, T)


# ---------------------------------------------------------------- helpers

def nearest_expiry_idx(dates, i):
    target = dates[i] + timedelta(days=TARGET_DTE)
    j = bisect_left(dates, target)
    cands = [k for k in (j - 1, j) if i < k < len(dates)]
    if not cands:
        return None
    return min(cands, key=lambda k: abs((dates[k] - target).days))


def month_key(d):
    return f"{d.year:04d}-{d.month:02d}"


def months_between(d0, d1):
    out = []
    y, m = d0.year, d0.month
    while (y, m) <= (d1.year, d1.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


# ---------------------------------------------------------------- managed wheel simulation

def run_wheel_managed(dates, close, adj, vix, iv_factor, use_pt, use_dte):
    n = len(dates)
    cash = INIT_CASH
    shares = 0
    opt = None      # dict(kind, K, contracts, expiry, open_i, credit_ps)
    next_write = 0  # earliest index allowed to write (i+1 after an early buy-back)
    equity = [0.0] * n
    holding = [False] * n

    prem_credit_by_month = {}   # net premium credited at sale (after costs), by trade month
    buyback_by_month = {}       # buy-back dollars paid, by close month
    settle_loss_by_month = {}   # intrinsic paid at expiry settlement, by settlement month
    prem_events = []            # (write_idx, net_credit)
    buyback_events = []         # (close_idx, dollars_paid)
    close_events = []           # (open_idx, close_idx, type)  type in {expiry, pt, dte}
    put_assign_idx, call_assign_idx = [], []
    put_sales = call_sales = 0

    for i in range(n):
        S = close[i]

        # 1) dividend credit while holding (total-return-implied)
        if shares > 0 and i > 0:
            div_ps = close[i - 1] * adj[i] / adj[i - 1] - close[i]
            cash += div_ps * shares

        # 2) natural expiry settlement (raw close, European)
        if opt is not None and i == opt["expiry"]:
            K, c = opt["K"], opt["contracts"]
            if opt["kind"] == "put":
                if S < K:
                    cash -= K * 100 * c
                    shares += 100 * c
                    put_assign_idx.append(i)
                    mk = month_key(dates[i])
                    settle_loss_by_month[mk] = settle_loss_by_month.get(mk, 0.0) + (K - S) * 100 * c
            else:
                if S > K:
                    cash += K * 100 * c
                    shares -= 100 * c
                    call_assign_idx.append(i)
                    mk = month_key(dates[i])
                    settle_loss_by_month[mk] = settle_loss_by_month.get(mk, 0.0) + (S - K) * 100 * c
            close_events.append((opt["open_i"], i, "expiry"))
            opt = None
            # same-day re-write allowed after natural expiry (as in wheel_sim.py)

        # 3) management check (before expiry only)
        elif opt is not None:
            sigma = vix[i] / 100.0 * iv_factor
            dte = (dates[opt["expiry"]] - dates[i]).days
            val_ps = bs_value(opt["kind"], S, opt["K"], sigma, dte / 365.0)
            trig_pt = use_pt and val_ps <= PT_FRAC * opt["credit_ps"]
            trig_dte = use_dte and dte <= DTE_CLOSE
            if trig_pt or trig_dte:
                c = opt["contracts"]
                cost = val_ps * 100 * c * (1.0 + SLIPPAGE) + COMMISSION * c
                cash -= cost
                mk = month_key(dates[i])
                buyback_by_month[mk] = buyback_by_month.get(mk, 0.0) + cost
                buyback_events.append((i, cost))
                close_events.append((opt["open_i"], i, "pt" if trig_pt else "dte"))
                opt = None
                next_write = i + 1  # re-write a fresh 30d option the NEXT day

        # 4) write next option
        if opt is None and i >= next_write:
            exp = nearest_expiry_idx(dates, i)
            if exp is not None:
                sigma = vix[i] / 100.0 * iv_factor
                T = (dates[exp] - dates[i]).days / 365.0
                if shares == 0:
                    K = S * (1.0 - PUT_OTM)
                    c = int(cash // (K * 100.0))
                    if c > 0:
                        prem_ps = bs_put(S, K, sigma, T)
                        net = prem_ps * 100 * c * (1.0 - SLIPPAGE) - COMMISSION * c
                        cash += net
                        put_sales += 1
                        opt = {"kind": "put", "K": K, "contracts": c, "expiry": exp,
                               "open_i": i, "credit_ps": prem_ps * (1.0 - SLIPPAGE)}
                        prem_events.append((i, net))
                        mk = month_key(dates[i])
                        prem_credit_by_month[mk] = prem_credit_by_month.get(mk, 0.0) + net
                    # c == 0: stay in cash, retry next day
                else:
                    K = S * (1.0 + CALL_OTM)
                    c = shares // 100
                    prem_ps = bs_call(S, K, sigma, T)
                    net = prem_ps * 100 * c * (1.0 - SLIPPAGE) - COMMISSION * c
                    cash += net
                    call_sales += 1
                    opt = {"kind": "call", "K": K, "contracts": c, "expiry": exp,
                           "open_i": i, "credit_ps": prem_ps * (1.0 - SLIPPAGE)}
                    prem_events.append((i, net))
                    mk = month_key(dates[i])
                    prem_credit_by_month[mk] = prem_credit_by_month.get(mk, 0.0) + net

        # 5) mark-to-market: short option at FAIR BS value (today's VIX sigma, shrinking T)
        opt_mark = 0.0
        if opt is not None:
            sigma = vix[i] / 100.0 * iv_factor
            T = (dates[opt["expiry"]] - dates[i]).days / 365.0
            opt_mark = bs_value(opt["kind"], S, opt["K"], sigma, T) * 100 * opt["contracts"]
        equity[i] = cash + shares * S - opt_mark
        holding[i] = shares > 0
        assert shares % 100 == 0, f"non-lot share count {shares} on {dates[i]}"
        assert equity[i] > 0, f"equity non-positive {equity[i]:.2f} on {dates[i]}"

    return {
        "equity": equity,
        "holding": holding,
        "prem_credit_by_month": prem_credit_by_month,
        "buyback_by_month": buyback_by_month,
        "settle_loss_by_month": settle_loss_by_month,
        "prem_events": prem_events,
        "buyback_events": buyback_events,
        "close_events": close_events,
        "put_assign_idx": put_assign_idx,
        "call_assign_idx": call_assign_idx,
        "put_sales": put_sales,
        "call_sales": call_sales,
    }


# ---------------------------------------------------------------- metrics

def slice_metrics(dates, equity, a, b):
    eq = equity[a:b + 1]
    rets = [eq[j] / eq[j - 1] - 1.0 for j in range(1, len(eq))]
    total_ret = eq[-1] / eq[0] - 1.0
    years = (dates[b] - dates[a]).days / 365.25
    cagr = (eq[-1] / eq[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    if len(rets) > 1:
        mu = sum(rets) / len(rets)
        var = sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)
        sd = math.sqrt(var)
        sharpe = mu / sd * math.sqrt(252) if sd > 0 else 0.0
    else:
        sharpe = 0.0
    peak, mdd = eq[0], 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    return {"total_return": total_ret, "cagr": cagr, "sharpe": sharpe, "max_dd": mdd}


def era_indices(dates):
    out = [("FULL", 0, len(dates) - 1)]
    for label, lo, hi in ERAS:
        idxs = [i for i, d in enumerate(dates) if lo <= d <= hi]
        if idxs:
            out.append((label, idxs[0], idxs[-1]))
    return out


def wheel_window_stats(dates, res, a, b):
    d0, d1 = dates[a], dates[b]
    prem = sum(v for i, v in res["prem_events"] if a <= i <= b)
    bb = sum(v for i, v in res["buyback_events"] if a <= i <= b)
    puts_a = sum(1 for i in res["put_assign_idx"] if a <= i <= b)
    calls_a = sum(1 for i in res["call_assign_idx"] if a <= i <= b)
    closes = [(oi, ci, t) for oi, ci, t in res["close_events"] if a <= ci <= b]
    n_pt = sum(1 for _, _, t in closes if t == "pt")
    n_dte = sum(1 for _, _, t in closes if t == "dte")
    avg_days = (sum((dates[ci] - dates[oi]).days for oi, ci, _ in closes) / len(closes)
                if closes else 0.0)
    hold_pct = sum(1 for i in range(a, b + 1) if res["holding"][i]) / (b - a + 1)
    months = months_between(d0, d1)
    net_pos = 0
    for m in months:
        net = (res["prem_credit_by_month"].get(m, 0.0)
               - res["buyback_by_month"].get(m, 0.0)
               - res["settle_loss_by_month"].get(m, 0.0))
        if net > 0:
            net_pos += 1
    return {
        "premium_credits": prem,
        "buyback_cost": bb,
        "net_premium_after_costs": prem - bb,
        "puts_assigned": puts_a,
        "calls_assigned": calls_a,
        "early_closes_profit_take": n_pt,
        "early_closes_gamma_week": n_dte,
        "options_closed": len(closes),
        "avg_days_held": avg_days,
        "pct_days_holding_shares": hold_pct,
        "pct_months_net_premium_income": net_pos / len(months),
        "n_months": len(months),
    }


# ---------------------------------------------------------------- main

def main():
    dates, close, adj, vix = load_data()
    n = len(dates)
    print(f"# SPY rows {n}  {dates[0]} .. {dates[-1]}   VIX range "
          f"{min(vix):.2f}..{max(vix):.2f}")
    print(f"# base wheel: put_otm={PUT_OTM:g} call_otm={CALL_OTM:g}; "
          f"variants x iv_factor in (0.85, 1.0); fair daily BS marking")

    windows = era_indices(dates)
    bench_eq = [INIT_CASH * adj[i] / adj[0] for i in range(n)]

    results = {"meta": {
        "window": [str(dates[0]), str(dates[-1])],
        "init_cash": INIT_CASH,
        "put_otm": PUT_OTM, "call_otm": CALL_OTM,
        "commission_per_contract": COMMISSION,
        "slippage_pct_of_premium": SLIPPAGE,
        "target_dte_calendar": TARGET_DTE,
        "pt_threshold": PT_FRAC, "dte_close_calendar": DTE_CLOSE,
        "rate": 0.0,
        "notes": [
            "r=0,q=0 in BS; CSP cash earns 0 (understates wheel in high-rate eras)",
            "short option marked DAILY at fair BS value: sigma=VIX[today]/100*iv_factor, T shrinking",
            "PT50: buy back when BS value <= 50% of per-share credit (credit = sale BS premium x 0.95)",
            "DTE7: buy back when <=7 calendar days to expiry; both rules re-write fresh 30d option NEXT day",
            "buy-back cost = BS value x 1.05 + $0.65/contract",
            "net_premium_after_costs = sale credits (after 5% haircut + commission) - buy-back dollars paid",
            "pct_months_net_premium_income: months where credits - buybacks - settlement intrinsic > 0",
            "dividends credited daily from adjusted/raw close drift while holding shares",
            "expiry = trading day nearest trade_date+30cd; European exercise at raw close",
        ],
    }, "benchmark": {}, "variants": []}

    for label, a, b in windows:
        results["benchmark"][label] = slice_metrics(dates, bench_eq, a, b)

    variant_rows = {}
    for vname, use_pt, use_dte in VARIANTS:
        for f in (0.85, 1.0):
            res = run_wheel_managed(dates, close, adj, vix, f, use_pt, use_dte)
            name = f"{vname}_iv{f:g}"
            entry = {"variant": vname, "iv_factor": f, "name": name,
                     "put_sales": res["put_sales"], "call_sales": res["call_sales"],
                     "final_equity": res["equity"][-1], "windows": {}}
            for label, a, b in windows:
                m = slice_metrics(dates, res["equity"], a, b)
                m.update(wheel_window_stats(dates, res, a, b))
                entry["windows"][label] = m
            results["variants"].append(entry)
            variant_rows[name] = entry

    hdr = (f"{'variant':<16} {'totret':>9} {'cagr':>7} {'sharpe':>7} {'maxDD':>7} "
           f"{'netprem$':>11} {'bb$':>11} {'putsA':>5} {'callsA':>6} "
           f"{'ePT':>5} {'eDTE':>5} {'avgD':>5} {'%mo+':>6}")
    for label, a, b in windows:
        print(f"\n== {label} ({dates[a]} .. {dates[b]}) ==")
        print(hdr)
        bm = results["benchmark"][label]
        print(f"{'BUY_AND_HOLD_SPY':<16} {bm['total_return']:>8.1%} {bm['cagr']:>7.2%} "
              f"{bm['sharpe']:>7.2f} {bm['max_dd']:>7.1%} {'-':>11} {'-':>11} "
              f"{'-':>5} {'-':>6} {'-':>5} {'-':>5} {'-':>5} {'-':>6}")
        for name, entry in variant_rows.items():
            w = entry["windows"][label]
            print(f"{name:<16} {w['total_return']:>8.1%} {w['cagr']:>7.2%} "
                  f"{w['sharpe']:>7.2f} {w['max_dd']:>7.1%} "
                  f"{w['net_premium_after_costs']:>11,.0f} {w['buyback_cost']:>11,.0f} "
                  f"{w['puts_assigned']:>5} {w['calls_assigned']:>6} "
                  f"{w['early_closes_profit_take']:>5} {w['early_closes_gamma_week']:>5} "
                  f"{w['avg_days_held']:>5.1f} {w['pct_months_net_premium_income']:>6.1%}")

    out_path = os.path.join(SCRATCH, "wheel_managed_results.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"\n# results -> {out_path}")

    # sanity: HOLD vs prior intrinsic-marked run (expiry P&L identical; only final-day
    # open-option time value differs)
    prior_path = os.path.join(SCRATCH, "wheel_results.json")
    if os.path.exists(prior_path):
        with open(prior_path) as fh:
            prior = json.load(fh)
        print("\n# sanity: HOLD FULL total return vs prior put2_call8 (intrinsic-marked)")
        for e in prior["combos"]:
            if e["put_otm"] == PUT_OTM and e["call_otm"] == CALL_OTM:
                name = f"HOLD_iv{e['iv_factor']:g}"
                new_tr = variant_rows[name]["windows"]["FULL"]["total_return"]
                old_tr = e["windows"]["FULL"]["total_return"]
                print(f"#   iv{e['iv_factor']:g}: prior {old_tr:+.1%}  now {new_tr:+.1%}  "
                      f"diff {new_tr - old_tr:+.2%}")

    # verdict: management vs HOLD, per iv_factor, FULL window
    print("\n# Management vs HOLD (FULL window):")
    for f in (0.85, 1.0):
        h = variant_rows[f"HOLD_iv{f:g}"]["windows"]["FULL"]
        for vname in ("PT50", "DTE7", "PT50+DTE7"):
            w = variant_rows[f"{vname}_iv{f:g}"]["windows"]["FULL"]
            print(f"#   iv{f:g} {vname:<10}: dCAGR {w['cagr'] - h['cagr']:+.2%}  "
                  f"dSharpe {w['sharpe'] - h['sharpe']:+.2f}  "
                  f"dMaxDD {w['max_dd'] - h['max_dd']:+.1%}")


if __name__ == "__main__":
    main()
