#!/usr/bin/env python3.14
"""
Wheel (CSP + covered-call) overlay backtest on SPY, 1998-01-02 .. 2026-07-31.

Strategy A: buy-and-hold SPY total return (adjusted_close), $250k.
Strategy B: wheel, $250k, integer 100-share lots, European-style, expiry-only decisions.

Modeling notes (documented caveats):
- r = 0, q = 0 in Black-Scholes (understates put premia / overstates call premia in
  high-rate eras; CSP cash also earns 0 -> understates wheel in high-rate eras).
- sigma = VIX/100 * iv_factor at trade date (VIX is 30d SPX IV, SPY proxy).
- Short option marked daily at INTRINSIC value only (understates interim vol drag;
  exact at expiry). Consequence: equity is flat during OTM CSP phases, which
  suppresses measured daily vol and overstates wheel Sharpe.
- Dividends: while holding shares, daily cash credit per share of
  close[t-1]*adj[t]/adj[t-1] - close[t]  (total-return-implied dividend; ~0 noise
  on non-ex days, ~the dividend on ex days). While in cash: no interest.
- Costs: premium haircut 5% (slippage) then $0.65/contract commission.
- Assignment: put assigned iff raw close < strike at expiry; call iff raw close > strike.
  Contracts sold = floor(cash / (strike*100)) for puts (reserve ALL cash);
  = shares//100 for calls (every lot covered).
- Expiry = trading day nearest (trade date + 30 calendar days), strictly after trade date.
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
SLIPPAGE = 0.05            # fraction of premium
TARGET_DTE = 30            # calendar days

ERAS = [
    ("1998-2009", date(1998, 1, 1), date(2009, 12, 31)),
    ("2010-2019", date(2010, 1, 1), date(2019, 12, 31)),
    ("2020-2026", date(2020, 1, 1), date(2026, 12, 31)),
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
            # forward-fill from most recent VIX before window start
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


# ---------------------------------------------------------------- helpers

def nearest_expiry_idx(dates, i):
    """Index of trading day nearest dates[i]+30cd, strictly > i. None if none exists."""
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


# ---------------------------------------------------------------- wheel simulation

def run_wheel(dates, close, adj, vix, put_otm, call_otm, iv_factor):
    n = len(dates)
    cash = INIT_CASH
    shares = 0
    opt = None  # dict(kind, K, contracts, expiry)
    equity = [0.0] * n

    prem_credit_by_month = {}   # net premium credited (after costs), by trade month
    settle_loss_by_month = {}   # intrinsic paid at settlement, by settlement month
    prem_events = []            # (idx, net_premium)
    put_assign_idx, call_assign_idx = [], []
    put_sales = call_sales = 0
    max_prem_ratio = 0.0
    holding = [False] * n

    for i in range(n):
        S = close[i]

        # 1) dividend credit while holding (total-return-implied)
        if shares > 0 and i > 0:
            div_ps = close[i - 1] * adj[i] / adj[i - 1] - close[i]
            cash += div_ps * shares

        # 2) settle option at expiry (raw close)
        if opt is not None and i == opt["expiry"]:
            K, c = opt["K"], opt["contracts"]
            if opt["kind"] == "put":
                if S < K:
                    cash -= K * 100 * c
                    shares += 100 * c
                    put_assign_idx.append(i)
                    settle_loss_by_month[month_key(dates[i])] = (
                        settle_loss_by_month.get(month_key(dates[i]), 0.0) + (K - S) * 100 * c)
            else:  # call
                if S > K:
                    cash += K * 100 * c
                    shares -= 100 * c
                    call_assign_idx.append(i)
                    settle_loss_by_month[month_key(dates[i])] = (
                        settle_loss_by_month.get(month_key(dates[i]), 0.0) + (S - K) * 100 * c)
            opt = None

        # 3) write next option (same day as settlement, at close)
        if opt is None:
            exp = nearest_expiry_idx(dates, i)
            if exp is not None:
                sigma = vix[i] / 100.0 * iv_factor
                T = (dates[exp] - dates[i]).days / 365.0
                if shares == 0:
                    K = S * (1.0 - put_otm)
                    c = int(cash // (K * 100.0))
                    if c > 0:
                        prem_ps = bs_put(S, K, sigma, T)
                        net = prem_ps * 100 * c * (1.0 - SLIPPAGE) - COMMISSION * c
                        cash += net
                        put_sales += 1
                        opt = {"kind": "put", "K": K, "contracts": c, "expiry": exp}
                        max_prem_ratio = max(max_prem_ratio, prem_ps / S)
                        prem_events.append((i, net))
                        mk = month_key(dates[i])
                        prem_credit_by_month[mk] = prem_credit_by_month.get(mk, 0.0) + net
                    # c == 0: stay in cash, retry next day
                else:
                    K = S * (1.0 + call_otm)
                    c = shares // 100
                    prem_ps = bs_call(S, K, sigma, T)
                    net = prem_ps * 100 * c * (1.0 - SLIPPAGE) - COMMISSION * c
                    cash += net
                    call_sales += 1
                    opt = {"kind": "call", "K": K, "contracts": c, "expiry": exp}
                    max_prem_ratio = max(max_prem_ratio, prem_ps / S)
                    prem_events.append((i, net))
                    mk = month_key(dates[i])
                    prem_credit_by_month[mk] = prem_credit_by_month.get(mk, 0.0) + net

        # 4) mark-to-market (short option at intrinsic only)
        intrinsic = 0.0
        if opt is not None:
            if opt["kind"] == "put":
                intrinsic = max(opt["K"] - S, 0.0) * 100 * opt["contracts"]
            else:
                intrinsic = max(S - opt["K"], 0.0) * 100 * opt["contracts"]
        equity[i] = cash + shares * S - intrinsic
        holding[i] = shares > 0
        assert shares % 100 == 0, f"non-lot share count {shares} on {dates[i]}"
        assert equity[i] > 0, f"equity non-positive {equity[i]:.2f} on {dates[i]}"

    return {
        "equity": equity,
        "holding": holding,
        "prem_credit_by_month": prem_credit_by_month,
        "settle_loss_by_month": settle_loss_by_month,
        "prem_events": prem_events,
        "put_assign_idx": put_assign_idx,
        "call_assign_idx": call_assign_idx,
        "put_sales": put_sales,
        "call_sales": call_sales,
        "max_prem_ratio": max_prem_ratio,
    }


# ---------------------------------------------------------------- metrics

def slice_metrics(dates, equity, a, b):
    """Metrics on equity[a..b] inclusive; returns computed within the slice."""
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
    """(label, a, b) per era + full. a is the era's first trading index (base day)."""
    out = [("FULL", 0, len(dates) - 1)]
    for label, lo, hi in ERAS:
        idxs = [i for i, d in enumerate(dates) if lo <= d <= hi]
        if idxs:
            out.append((label, idxs[0], idxs[-1]))
    return out


def wheel_window_stats(dates, res, a, b):
    d0, d1 = dates[a], dates[b]
    prem = sum(v for i, v in res["prem_events"] if a <= i <= b)
    puts_a = sum(1 for i in res["put_assign_idx"] if a <= i <= b)
    calls_a = sum(1 for i in res["call_assign_idx"] if a <= i <= b)
    hold_pct = sum(1 for i in range(a, b + 1) if res["holding"][i]) / (b - a + 1)
    months = months_between(d0, d1)
    net_pos = 0
    for m in months:
        net = res["prem_credit_by_month"].get(m, 0.0) - res["settle_loss_by_month"].get(m, 0.0)
        if net > 0:
            net_pos += 1
    return {
        "total_premium_net": prem,
        "puts_assigned": puts_a,
        "calls_assigned": calls_a,
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

    windows = era_indices(dates)

    # benchmark
    bench_eq = [INIT_CASH * adj[i] / adj[0] for i in range(n)]

    grid = [(p, c, f) for p in (0.02, 0.05) for c in (0.03, 0.05, 0.08) for f in (0.85, 1.0)]

    results = {"meta": {
        "window": [str(dates[0]), str(dates[-1])],
        "init_cash": INIT_CASH,
        "commission_per_contract": COMMISSION,
        "slippage_pct_of_premium": SLIPPAGE,
        "target_dte_calendar": TARGET_DTE,
        "rate": 0.0,
        "notes": [
            "r=0,q=0 in BS; CSP cash earns 0 (understates wheel in high-rate eras)",
            "short option marked at intrinsic only (flat equity in OTM CSP phases -> wheel Sharpe overstated)",
            "sigma = VIX/100*iv_factor (VIX = 30d SPX IV as SPY proxy)",
            "dividends credited daily from adjusted/raw close drift while holding shares",
            "pct_months_net_premium_income: months where (net premium credited - intrinsic paid at settlements) > 0",
            "expiry = trading day nearest trade_date+30cd; European exercise at raw close",
        ],
    }, "benchmark": {}, "combos": []}

    for label, a, b in windows:
        results["benchmark"][label] = slice_metrics(dates, bench_eq, a, b)

    lines = []
    hdr = (f"{'combo':<26} {'window':<9} {'totret':>9} {'cagr':>7} {'sharpe':>7} "
           f"{'maxDD':>7} {'prem$':>12} {'putsA':>5} {'callsA':>6} {'%hold':>6} {'%mo+':>6}")

    combo_rows = {}
    for p, c, f in grid:
        res = run_wheel(dates, close, adj, vix, p, c, f)
        name = f"put{int(p*100)}_call{int(c*100)}_iv{f:g}"
        entry = {"put_otm": p, "call_otm": c, "iv_factor": f, "name": name,
                 "max_premium_pct_of_spot": res["max_prem_ratio"],
                 "put_sales": res["put_sales"], "call_sales": res["call_sales"],
                 "windows": {}}
        for label, a, b in windows:
            m = slice_metrics(dates, res["equity"], a, b)
            m.update(wheel_window_stats(dates, res, a, b))
            entry["windows"][label] = m
        entry["final_equity"] = res["equity"][-1]
        results["combos"].append(entry)
        combo_rows[name] = entry
        if res["max_prem_ratio"] > 0.10:
            print(f"# NOTE {name}: max premium/spot = {res['max_prem_ratio']:.2%} "
                  f"(peak-VIX days; check)")

    # ------------- print table, grouped by window
    for label, a, b in windows:
        print(f"\n== {label} ({dates[a]} .. {dates[b]}) ==")
        print(hdr)
        bm = results["benchmark"][label]
        print(f"{'BUY_AND_HOLD_SPY':<26} {label:<9} {bm['total_return']:>8.1%} "
              f"{bm['cagr']:>7.2%} {bm['sharpe']:>7.2f} {bm['max_dd']:>7.1%} "
              f"{'-':>12} {'-':>5} {'-':>6} {'-':>6} {'-':>6}")
        for name, entry in combo_rows.items():
            w = entry["windows"][label]
            print(f"{name:<26} {label:<9} {w['total_return']:>8.1%} {w['cagr']:>7.2%} "
                  f"{w['sharpe']:>7.2f} {w['max_dd']:>7.1%} {w['total_premium_net']:>12,.0f} "
                  f"{w['puts_assigned']:>5} {w['calls_assigned']:>6} "
                  f"{w['pct_days_holding_shares']:>6.1%} "
                  f"{w['pct_months_net_premium_income']:>6.1%}")

    out_path = os.path.join(SCRATCH, "wheel_results.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"\n# results -> {out_path}")

    # sharpe verdict per era
    print("\n# Wheel beats B&H on Sharpe?")
    for label, _, _ in windows:
        bs_ = results["benchmark"][label]["sharpe"]
        beat = sum(1 for e in results["combos"] if e["windows"][label]["sharpe"] > bs_)
        print(f"#   {label}: {beat}/12 combos beat benchmark Sharpe ({bs_:.2f})")


if __name__ == "__main__":
    main()
