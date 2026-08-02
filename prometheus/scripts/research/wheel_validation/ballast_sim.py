#!/usr/bin/env python3.14
"""
Ballast blend: VIXCOND wheel (the robust winner of wheel_sim_v2) + bonds/T-bills/gold.

Wheel sleeve = VIXCOND from wheel_sim_v2.run_wheel (put_otm 0.02 -> 0.05 with PT50 when
VIX>25, skip CC when VIX<13, else CC 8% OTM, 30cd, hold-to-expiry), run standalone on its
own $250k notional over the FULL 1998-2026 history at iv_factor 0.85 and 1.0, then
converted to a DAILY RETURN stream. Ballast sleeves = TLT.US / IEF.US / SHY.US / GLD.US
daily adjusted_close returns from prices_daily (IEF loaded for the correlation table
only; the allocation grid uses TLT/GLD/SHY).

Blending is done at the daily-returns level with QUARTERLY rebalancing to fixed target
weights (rebalance executed at the close of the first trading day of each calendar
quarter, using that day's closes only -- no look-ahead). Rebalance cost: 5bps paid on
every traded dollar (sum of |sleeve trade| across sleeves, i.e. both the sell and the
buy leg pay 5bps). NOTE / fidelity caveat: returns-level blending abstracts away lot
mechanics inside the wheel sleeve (a real quarterly top-up/down of the wheel would
perturb its contract counts and cash reserve); acceptable at this fidelity because
quarterly drift turnover is a few % of NAV and the wheel engine already carries its own
option-level costs.

Windows
  PRIMARY  2004-11-18 .. 2026-07-31 (GLD inception; all sleeves live), eras
           2004-2009 / 2010-2019 / 2020-2026, spotlight 2022 calendar year, and the
           GFC drawdown window 2008-09-01 .. 2009-03-31. Era/spotlight metrics are
           slices of ONE continuously-rebalanced primary run (weights drift across
           window edges realistically; year returns are close-to-close).
  SECONDARY 2002-07-26 .. 2026-07-31 (TLT/IEF/SHY inception) for the no-gold rows.

Sanity: weights sum to 1 (asserted); VIXCOND wheel reproduces wheel_v2_results.json
final equity (<0.1%); the 100% wheel and 100% SPY "blends" reproduce their sleeves'
sliced metrics (<1e-6 rel.); every rebalance date is verified to be the first trading
day of a calendar quarter and the count matches the number of quarter boundaries.
"""

import json
import math
import os
import sys
from datetime import date

import psycopg2

SCRATCH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRATCH)

from wheel_sim_managed import INIT_CASH, load_data  # noqa: E402
from wheel_sim_v2 import run_wheel  # noqa: E402

END = date(2026, 7, 31)
PRIMARY_START = date(2004, 11, 18)     # GLD first print
SECONDARY_START = date(2002, 7, 26)    # TLT/IEF/SHY first print
REB_COST = 0.0005                      # 5bps on every traded dollar (both legs)
IV_FACTORS = (0.85, 1.0)
BALLAST_IDS = ("TLT.US", "IEF.US", "SHY.US", "GLD.US")

# (label, w_wheel, w_TLT, w_GLD, w_SHY) -- wheel-based grid
GRID = [
    ("WHEEL100",              1.00, 0.00, 0.00, 0.00),
    ("W80/TLT20",             0.80, 0.20, 0.00, 0.00),
    ("W80/TLT10/GLD10",       0.80, 0.10, 0.10, 0.00),
    ("W70/TLT20/GLD10",       0.70, 0.20, 0.10, 0.00),
    ("W70/TLT15/GLD10/SHY5",  0.70, 0.15, 0.10, 0.05),
    ("W60/TLT25/GLD10/SHY5",  0.60, 0.25, 0.10, 0.05),
    ("W60/TLT20/GLD20",       0.60, 0.20, 0.20, 0.00),
    ("PERM40/TLT30/GLD20/SHY10", 0.40, 0.30, 0.20, 0.10),
]
# classic benchmarks with SPY total-return as the equity sleeve
SPY_ROWS = [
    ("SPY100",      1.00, 0.00, 0.00, 0.00),
    ("SPY60/TLT40", 0.60, 0.40, 0.00, 0.00),
]


# ---------------------------------------------------------------- data

def load_ballast(dates):
    """Adjusted-close returns for each ballast ETF, forward-filled onto the SPY
    trading calendar. rets[iid][i] is None until the instrument's second print."""
    conn = psycopg2.connect(
        host="localhost", port=6432, user="prometheus",
        dbname="prometheus_historical", password=os.environ["HISTORICAL_DB_PASSWORD"],
    )
    cur = conn.cursor()
    rets, first_dates = {}, {}
    for iid in BALLAST_IDS:
        cur.execute(
            "SELECT trade_date, adjusted_close FROM prices_daily "
            "WHERE instrument_id=%s AND trade_date <= %s ORDER BY trade_date",
            (iid, END))
        pmap = {d: float(a) for d, a in cur.fetchall() if a is not None}
        first_dates[iid] = min(pmap)
        px, last = [None] * len(dates), None
        for i, d in enumerate(dates):
            if d in pmap:
                last = pmap[d]
            px[i] = last                       # ffill (None before inception)
        r = [None] * len(dates)
        for i in range(1, len(dates)):
            if px[i] is not None and px[i - 1] is not None:
                r[i] = px[i] / px[i - 1] - 1.0
        rets[iid] = r
    conn.close()
    return rets, first_dates


# ---------------------------------------------------------------- blend engine

def _quarter(d):
    return (d.year, (d.month - 1) // 3)


def run_blend(dates, sleeves, a0, cost=REB_COST):
    """sleeves: list of (weight, rets_full_calendar). V[a0]=1.0; quarterly rebalance
    to target weights at the close of the first trading day of each new quarter;
    5bps on total traded dollars. Returns full-length V (None before a0)."""
    wsum = sum(w for w, _ in sleeves)
    assert abs(wsum - 1.0) < 1e-12, f"weights sum {wsum}"
    n = len(dates)
    V = [None] * n
    vals = [w for w, _ in sleeves]
    V[a0] = 1.0
    reb_dates, turns = [], []
    cum_fee = 0.0
    for i in range(a0 + 1, n):
        for k, (_, r) in enumerate(sleeves):
            ri = r[i]
            assert ri is not None, f"missing sleeve return on {dates[i]}"
            vals[k] *= 1.0 + ri
        if _quarter(dates[i]) != _quarter(dates[i - 1]):
            tot = sum(vals)
            traded = sum(abs(w * tot - v) for (w, _), v in zip(sleeves, vals))
            fee = traded * cost
            cum_fee += fee
            turns.append(traded / tot)
            tot -= fee
            vals = [w * tot for w, _ in sleeves]
            reb_dates.append(dates[i])
        V[i] = sum(vals)
        assert V[i] > 0.0
    return V, reb_dates, cum_fee, turns


# ---------------------------------------------------------------- metrics

def pf_metrics(dates, V, base, wb):
    """Close-to-close over [base..wb]: total ret uses V[base] as the pre-window close."""
    eq = V[base:wb + 1]
    rets = [eq[j] / eq[j - 1] - 1.0 for j in range(1, len(eq))]
    total = eq[-1] / eq[0] - 1.0
    years = (dates[wb] - dates[base]).days / 365.25
    cagr = (eq[-1] / eq[0]) ** (1.0 / years) - 1.0 if years > 0 else 0.0
    if len(rets) > 1:
        mu = sum(rets) / len(rets)
        sd = math.sqrt(sum((r - mu) ** 2 for r in rets) / (len(rets) - 1))
        sharpe = mu / sd * math.sqrt(252) if sd > 0 else 0.0
    else:
        sharpe = 0.0
    peak, mdd = eq[0], 0.0
    for v in eq:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    return {"total_return": total, "cagr": cagr, "sharpe": sharpe, "max_dd": mdd}


def year_bounds(dates):
    fb = {}
    for i, d in enumerate(dates):
        if d.year not in fb:
            fb[d.year] = [i, i]
        fb[d.year][1] = i
    return fb


def year_return(V, ybounds, y, a0):
    fi, li = ybounds[y]
    base = fi - 1
    if base < a0 or V[base] is None or V[li] is None:
        return None
    return V[li] / V[base] - 1.0


def full_years_in(ybounds, a0, wa, wb):
    out = []
    for y, (fi, li) in sorted(ybounds.items()):
        if fi >= wa and li <= wb and fi - 1 >= a0:
            out.append(y)
    return out


def corr(xs, ys):
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    if sxx <= 0 or syy <= 0:
        return 0.0
    return sxy / math.sqrt(sxx * syy)


# ---------------------------------------------------------------- main

def main():
    dates, close, adj, vix = load_data()
    n = len(dates)
    idx = {d: i for i, d in enumerate(dates)}
    ybounds = year_bounds(dates)
    print(f"# SPY calendar {n} rows  {dates[0]} .. {dates[-1]}")

    ballast, first_dates = load_ballast(dates)
    for iid in BALLAST_IDS:
        print(f"# {iid}: first print {first_dates[iid]}")
    assert first_dates["GLD.US"] == PRIMARY_START
    assert first_dates["TLT.US"] == SECONDARY_START

    # --- wheel sleeve (VIXCOND) on its own notional, full history, both iv factors
    with open(os.path.join(SCRATCH, "wheel_v2_results.json")) as fh:
        prior = {e["iv_factor"]: e["final_equity"]
                 for e in json.load(fh)["variants"] if e["variant"] == "VIXCOND"}
    wheel_rets, wheel_eq = {}, {}
    for f in IV_FACTORS:
        res = run_wheel(dates, close, adj, vix, f, target_dte=30, vix_cond=True)
        eq = res["equity"]
        diff = abs(eq[-1] - prior[f]) / prior[f]
        assert diff < 1e-3, f"VIXCOND iv{f:g} mismatch vs wheel_v2_results ({diff:.4%})"
        print(f"# sanity OK: VIXCOND iv{f:g} final equity {eq[-1]:,.0f} matches "
              f"wheel_v2_results.json (diff {diff:.5%})")
        wheel_eq[f] = eq
        r = [None] * n
        for i in range(1, n):
            r[i] = eq[i] / eq[i - 1] - 1.0
        wheel_rets[f] = r

    spy_rets = [None] * n
    for i in range(1, n):
        spy_rets[i] = adj[i] / adj[i - 1] - 1.0

    a_pri, a_sec, b_end = idx[PRIMARY_START], idx[SECONDARY_START], idx[END]

    def widx(d0, d1, a0):
        """(base, wa, wb): wa = first trading day >= d0 within run, base = day before
        (close-to-close), clamped to the run start a0; wb = last trading day <= d1."""
        wa = next(i for i in range(a0, n) if dates[i] >= d0)
        wb = max(i for i in range(a0, b_end + 1) if dates[i] <= d1)
        base = max(a0, wa - 1)
        return base, wa, wb

    pri_windows = [
        ("FULL",       widx(PRIMARY_START, END, a_pri)),
        ("2004-2009",  widx(PRIMARY_START, date(2009, 12, 31), a_pri)),
        ("2010-2019",  widx(date(2010, 1, 1), date(2019, 12, 31), a_pri)),
        ("2020-2026",  widx(date(2020, 1, 1), END, a_pri)),
        ("Y2022",      widx(date(2022, 1, 1), date(2022, 12, 31), a_pri)),
        ("GFC_0809-0903", widx(date(2008, 9, 1), date(2009, 3, 31), a_pri)),
    ]
    sec_windows = [("FULL_2002", widx(SECONDARY_START, END, a_sec))]

    # --- per-asset correlation vs wheel/SPY, per primary window
    asset_corr = {}
    for label, (base, wa, wb) in pri_windows:
        lo = max(base + 1, a_pri + 1)
        rows = {}
        for iid in BALLAST_IDS:
            br = [ballast[iid][i] for i in range(lo, wb + 1)]
            rows[iid] = {
                "vs_wheel_iv0.85": corr(br, [wheel_rets[0.85][i] for i in range(lo, wb + 1)]),
                "vs_wheel_iv1": corr(br, [wheel_rets[1.0][i] for i in range(lo, wb + 1)]),
                "vs_spy": corr(br, [spy_rets[i] for i in range(lo, wb + 1)]),
            }
        asset_corr[label] = rows

    # --- build portfolio list
    def sleeves_for(row, eq_rets):
        _, ww, wt, wg, ws = row
        out = [(ww, eq_rets)]
        for w, iid in ((wt, "TLT.US"), (wg, "GLD.US"), (ws, "SHY.US")):
            if w > 0:
                out.append((w, ballast[iid]))
        return out

    def ballast_composite(row):
        _, _, wt, wg, ws = row
        tot = wt + wg + ws
        if tot == 0:
            return None
        comp = [None] * n
        for i in range(1, n):
            parts = []
            for w, iid in ((wt, "TLT.US"), (wg, "GLD.US"), (ws, "SHY.US")):
                if w > 0:
                    ri = ballast[iid][i]
                    if ri is None:
                        parts = None
                        break
                    parts.append(w * ri)
            comp[i] = sum(parts) / tot if parts is not None else None
        return comp

    def run_portfolio(row, eq_rets, a0, windows, run_a0):
        V, reb, fee, turns = run_blend(dates, sleeves_for(row, eq_rets), a0)
        comp = ballast_composite(row)
        entry = {"windows": {}, "n_rebalances": len(reb),
                 "cum_rebalance_fee_frac_of_start": fee,
                 "avg_quarterly_turnover": (sum(turns) / len(turns)) if turns else 0.0}
        for label, (base, wa, wb) in windows:
            m = pf_metrics(dates, V, base, wb)
            fy = full_years_in(ybounds, run_a0, wa, wb)
            yrets = {y: year_return(V, ybounds, y, run_a0) for y in fy}
            worst = min(yrets.items(), key=lambda kv: kv[1]) if yrets else None
            m["worst_calendar_year"] = ({"year": worst[0], "ret": worst[1]}
                                        if worst else None)
            m["ret_2022"] = yrets.get(2022)
            if comp is not None:
                lo = max(base + 1, a0 + 1)
                m["corr_ballast_vs_equity_sleeve"] = corr(
                    [comp[i] for i in range(lo, wb + 1)],
                    [eq_rets[i] for i in range(lo, wb + 1)])
            else:
                m["corr_ballast_vs_equity_sleeve"] = None
            entry["windows"][label] = m
        return V, reb, entry

    results = {"meta": {
        "wheel_variant": "VIXCOND (wheel_sim_v2): VIX>25 -> put_otm 0.05 + PT50; "
                         "VIX<13 -> skip covered call; else put 0.02 / call 0.08, "
                         "30cd, hold-to-expiry; iv_factors 0.85 & 1.0",
        "init_cash_wheel_sleeve": INIT_CASH,
        "primary_window": [str(PRIMARY_START), str(END)],
        "secondary_window": [str(SECONDARY_START), str(END)],
        "rebalance": "quarterly, close of first trading day of each calendar quarter",
        "rebalance_cost": "5bps on every traded dollar (sum |sleeve trade|, both legs)",
        "notes": [
            "blend at daily-returns level: abstracts away lot mechanics inside the "
            "wheel sleeve (real top-ups would perturb contract counts); acceptable "
            "at this fidelity",
            "ballast returns from prices_daily adjusted_close, ffilled onto the SPY "
            "trading calendar; no look-ahead anywhere (VIX/prices same-day close)",
            "era/spotlight metrics are slices of one continuously-rebalanced run; "
            "year returns are close-to-close (prior-year last close as base)",
            "worst_calendar_year covers FULL calendar years inside the window only",
            "Sharpe rf=0, daily mean/sd * sqrt(252), matching wheel_sim conventions",
            "IEF.US loaded for the correlation table only; grid uses TLT/GLD/SHY",
        ],
    }, "asset_correlations_primary": asset_corr,
       "primary": {}, "secondary": {}}

    # --- run everything
    def portfolio_set(rows, a0, windows, run_a0):
        out = {}
        reb_ref = None
        for row in rows:
            label = row[0]
            if label.startswith(("SPY",)):
                V, reb, entry = run_portfolio(row, spy_rets, a0, windows, run_a0)
                entry.update({"weights_wheel_tlt_gld_shy": list(row[1:]),
                              "equity_sleeve": "SPY", "iv_factor": None})
                out[label] = (V, entry)
                reb_ref = reb
            else:
                for f in IV_FACTORS:
                    V, reb, entry = run_portfolio(row, wheel_rets[f], a0, windows, run_a0)
                    entry.update({"weights_wheel_tlt_gld_shy": list(row[1:]),
                                  "equity_sleeve": "VIXCOND_wheel", "iv_factor": f})
                    out[f"{label}_iv{f:g}"] = (V, entry)
                    reb_ref = reb
        return out, reb_ref

    pri_rows = GRID + SPY_ROWS
    sec_rows = [r for r in GRID if r[3] == 0.0] + [r for r in SPY_ROWS if r[3] == 0.0]
    pri, reb_pri = portfolio_set(pri_rows, a_pri, pri_windows, a_pri)
    sec, reb_sec = portfolio_set(sec_rows, a_sec, sec_windows, a_sec)

    # --- sanity: rebalance dates are exactly the first trading days of quarters
    for tag, reb, a0 in (("primary", reb_pri, a_pri), ("secondary", reb_sec, a_sec)):
        expected = [dates[i] for i in range(a0 + 1, b_end + 1)
                    if _quarter(dates[i]) != _quarter(dates[i - 1])]
        assert reb == expected, f"{tag} rebalance dates mismatch"
        assert all(d.month in (1, 4, 7, 10) and d.day <= 5 for d in reb)
        print(f"# sanity OK: {tag} rebalances {len(reb)} "
              f"(first {reb[0]}, {reb[1]} .. last {reb[-1]}), all Q-first trading days")

    # --- sanity: 100% single-sleeve blends reproduce their sleeve
    for f in IV_FACTORS:
        V = pri[f"WHEEL100_iv{f:g}"][0]
        direct = wheel_eq[f][b_end] / wheel_eq[f][a_pri] - 1.0
        blend = V[b_end] / V[a_pri] - 1.0
        assert abs(blend - direct) / abs(direct) < 1e-9
    Vs = pri["SPY100"][0]
    direct = adj[b_end] / adj[a_pri] - 1.0
    assert abs((Vs[b_end] / Vs[a_pri] - 1.0) - direct) / direct < 1e-9
    print("# sanity OK: WHEEL100 & SPY100 blends reproduce sleeve returns exactly")

    for name, (V, entry) in pri.items():
        results["primary"][name] = entry
    for name, (V, entry) in sec.items():
        results["secondary"][name] = entry
    results["meta"]["rebalance_dates_primary_first5"] = [str(d) for d in reb_pri[:5]]
    results["meta"]["n_rebalances_primary"] = len(reb_pri)

    # --- tables
    def fmt_worst(w):
        return f"{w['ret']:>7.1%}'{w['year'] % 100:02d}" if w else "      -"

    hdr = (f"{'portfolio':<28} {'cagr':>7} {'sharpe':>7} {'maxDD':>7} "
           f"{'totret':>9} {'worstYr':>10} {'ret2022':>8} {'corrB/E':>8}")
    for label, (base, wa, wb) in pri_windows:
        print(f"\n== PRIMARY {label}  ({dates[wa]} .. {dates[wb]}, base close {dates[base]}) ==")
        print(hdr)
        for name, (V, e) in pri.items():
            w = e["windows"][label]
            c = w["corr_ballast_vs_equity_sleeve"]
            r22 = f"{w['ret_2022']:>8.1%}" if w["ret_2022"] is not None else f"{'-':>8}"
            print(f"{name:<28} {w['cagr']:>7.2%} {w['sharpe']:>7.2f} {w['max_dd']:>7.1%} "
                  f"{w['total_return']:>8.1%} {fmt_worst(w['worst_calendar_year']):>10} "
                  f"{r22} {(f'{c:>8.2f}' if c is not None else f'{chr(45):>8}')}")

    for label, (base, wa, wb) in sec_windows:
        print(f"\n== SECONDARY {label}  ({dates[wa]} .. {dates[wb]}) — no-gold rows ==")
        print(hdr)
        for name, (V, e) in sec.items():
            w = e["windows"][label]
            c = w["corr_ballast_vs_equity_sleeve"]
            r22 = f"{w['ret_2022']:>8.1%}" if w["ret_2022"] is not None else f"{'-':>8}"
            print(f"{name:<28} {w['cagr']:>7.2%} {w['sharpe']:>7.2f} {w['max_dd']:>7.1%} "
                  f"{w['total_return']:>8.1%} {fmt_worst(w['worst_calendar_year']):>10} "
                  f"{r22} {(f'{c:>8.2f}' if c is not None else f'{chr(45):>8}')}")

    print("\n== ballast-asset correlations vs wheel/SPY daily returns (primary windows) ==")
    print(f"{'window':<14} {'asset':<8} {'vs_whl.85':>10} {'vs_whl1':>9} {'vs_SPY':>8}")
    for label, rows in asset_corr.items():
        for iid, cc in rows.items():
            print(f"{label:<14} {iid:<8} {cc['vs_wheel_iv0.85']:>10.2f} "
                  f"{cc['vs_wheel_iv1']:>9.2f} {cc['vs_spy']:>8.2f}")

    # --- 2022 sleeve returns (the ballast stress test)
    print("\n== 2022 sleeve returns (close-to-close) ==")
    fi, li = ybounds[2022]
    for f in IV_FACTORS:
        print(f"  VIXCOND wheel iv{f:g}: {wheel_eq[f][li] / wheel_eq[f][fi - 1] - 1.0:+.1%}")
    print(f"  SPY: {adj[li] / adj[fi - 1] - 1.0:+.1%}")
    for iid in BALLAST_IDS:
        r = ballast[iid]
        v = 1.0
        for i in range(fi, li + 1):
            v *= 1.0 + r[i]
        print(f"  {iid}: {v - 1.0:+.1%}")

    # --- verdicts
    print("\n# (a) Sharpe vs pure wheel (PRIMARY FULL), per mix, both iv factors:")
    improves_both = []
    for row in GRID[1:]:
        label = row[0]
        wins = 0
        deltas = []
        for f in IV_FACTORS:
            s = pri[f"{label}_iv{f:g}"][1]["windows"]["FULL"]["sharpe"]
            s0 = pri[f"WHEEL100_iv{f:g}"][1]["windows"]["FULL"]["sharpe"]
            deltas.append(s - s0)
            wins += s > s0
        print(f"#   {label:<26} dSharpe iv0.85 {deltas[0]:+.3f}  iv1 {deltas[1]:+.3f}"
              f"  -> {'BOTH' if wins == 2 else ('one' if wins == 1 else 'neither')}")
        if wins == 2:
            improves_both.append(label)
    print(f"# (a) mixes improving Sharpe at BOTH iv factors: {improves_both or 'NONE'}")

    print("\n# (b) 2022 (both stocks and bonds fell):")
    for f in IV_FACTORS:
        pw = pri[f"WHEEL100_iv{f:g}"][1]["windows"]["FULL"]["ret_2022"]
        print(f"#   pure wheel iv{f:g} 2022: {pw:+.1%}")
        for row in GRID[1:]:
            label = row[0]
            r22 = pri[f"{label}_iv{f:g}"][1]["windows"]["FULL"]["ret_2022"]
            print(f"#     {label:<26} 2022 {r22:+.1%}  (delta vs pure {r22 - pw:+.1%})")

    best = None
    for row in GRID[1:]:
        label = row[0]
        avg_s = sum(pri[f"{label}_iv{f:g}"][1]["windows"]["FULL"]["sharpe"]
                    for f in IV_FACTORS) / 2
        if best is None or avg_s > best[1]:
            best = (label, avg_s)
    print(f"\n# (c) best avg FULL Sharpe across both iv factors: {best[0]} ({best[1]:.3f})")

    out_path = os.path.join(SCRATCH, "ballast_results.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"\n# results -> {out_path}")


if __name__ == "__main__":
    main()
