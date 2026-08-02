#!/usr/bin/env python3.14
"""
Shotgun test of three cross-sectional equity strategies on prometheus_historical.

  SECTORMOM   9 SPDR sector ETFs (XLK/XLF/XLE/XLV/XLI/XLP/XLY/XLU/XLB, live 1998-12-22),
              monthly: rank by 126d total return (adjusted_close), hold top-3 EW.
              Variant TREND: same top-3 but a slot is invested only if that sector is
              above its own 200d SMA (else cash). Window 1999-07-01 .. 2026-07-31.
  LOWVOL      top-300 by 1999 avg dollar volume (ETFs blacklisted out), quarterly:
              hold the 30 lowest trailing-252d-realized-vol names EW.
              Window 2000-01-01 .. 2026-07-31.  SURVIVORSHIP-BIASED universe.
  EQUALWEIGHT top-100 of the same ranking, EW, monthly. Window 2000-01 .. 2026-07.
              SURVIVORSHIP-BIASED universe.

Mechanics (all strategies):
  - signals from close[t] (month-/quarter-end trading day); trades at close[t+1]
    (positions effective t+1); new weights earn returns from t+2 onward.
  - daily equity on adjusted_close total-return relatives; cash earns 0.
  - costs: 10bps on two-way traded notional (sum |target - drifted weight|).
  - benchmark: SPY buy-and-hold at close of the first window day, no costs.

Data hygiene: clean_segment() reused verbatim from dip_wheel_sim.py — trims
ticker-reuse tails, wrong-scale segments and adjustment-factor breaks by keeping
the longest clean segment (breaks: calendar gap >45d, any one-day |ln adj ret|>ln3,
adj-vs-raw return disagreement >1.5x with adj move >1.5x).

Known biases (also flagged in the JSON):
  - LOWVOL / EQUALWEIGHT universe is today's-survivors backfilled; delisted losers
    (Enron, WorldCom, Lehman, Bear...) simply are not in the DB under '%.US' with
    1999 volume, so both stock strategies AND their universe are inflated.
  - volume is split-adjusted while close is raw, so the 1999 close*volume ranking
    tilts the universe further toward future splitters (AMZN/AAPL/NVDA effect).
  - names whose clean segment ends mid-window are held flat (last price) until the
    next rebalance drops them - mild, favourable, rare.
  - SECTORMOM TREND: before the ETFs have 200d of history (until ~1999-10) the
    trend filter counts as a pass.
  - cash earns 0: understates the TREND variant in high-rate eras.
"""

import json
import math
import os
import time
from bisect import bisect_left, bisect_right
from datetime import date

import numpy as np
import psycopg2

SCRATCH = os.path.dirname(os.path.abspath(__file__))
HIST_START = date(1998, 6, 1)
END = date(2026, 7, 31)
COST = 0.0010                 # 10bps on two-way traded notional
TOP_N = 300
EW_N = 100
LOWVOL_N = 30
VOL_WIN = 252
MOM_WIN = 126
SMA_WIN = 200
STALE_DAYS = 5                # must have traded within N master days to be tradeable

SECTORS = ["XLK.US", "XLF.US", "XLE.US", "XLV.US", "XLI.US",
           "XLP.US", "XLY.US", "XLU.US", "XLB.US"]
ETF_BLACKLIST = {
    "SPY.US", "QQQ.US", "QQQQ.US", "DIA.US", "MDY.US", "IWM.US", "SMH.US",
    "IYR.US", "EEM.US", "EFA.US", "EWJ.US", "TLT.US", "GLD.US", "IBB.US",
    "XBI.US", "OIH.US", "RTH.US", "EWZ.US", "FXI.US", "SSO.US", "SPXL.US",
} | set(SECTORS)

SECTORMOM_START = date(1999, 7, 1)
STOCK_START = date(2000, 1, 1)

# ------------------------------------------------------------------ data


def load_data():
    conn = psycopg2.connect(
        host="localhost", port=6432, user="prometheus",
        dbname="prometheus_historical", password=os.environ["HISTORICAL_DB_PASSWORD"],
    )
    cur = conn.cursor()
    t0 = time.time()
    cur.execute(
        "SELECT instrument_id FROM prices_daily "
        "WHERE instrument_id LIKE '%%.US' AND trade_date BETWEEN %s AND %s "
        "  AND close > 0 AND volume > 0 "
        "GROUP BY instrument_id HAVING COUNT(*) >= 100 "
        "ORDER BY AVG(close*volume) DESC LIMIT %s",
        (date(1999, 1, 1), date(1999, 12, 31), TOP_N + 30))
    ranked = [r[0] for r in cur.fetchall()]
    excluded = [n for n in ranked if n in ETF_BLACKLIST]
    universe = [n for n in ranked if n not in ETF_BLACKLIST][:TOP_N]
    cur.close()
    print(f"# universe: top {len(universe)} stocks by 1999 ADV "
          f"(ETFs excluded from ranking: {excluded})  ({time.time()-t0:.1f}s)")

    ids = sorted(set(universe) | set(SECTORS) | {"SPY.US"})
    scur = conn.cursor(name="px_stream")
    scur.itersize = 100_000
    scur.execute(
        "SELECT instrument_id, trade_date, open, close, adjusted_close "
        "FROM prices_daily WHERE instrument_id = ANY(%s) "
        "AND trade_date BETWEEN %s AND %s AND close > 0 AND adjusted_close > 0 "
        "ORDER BY instrument_id, trade_date", (ids, HIST_START, END))
    raw = {}
    cur_id, buf = None, None
    nrows = 0
    for iid, d, o, c, a in scur:
        if iid != cur_id:
            if cur_id is not None:
                raw[cur_id] = buf
            cur_id, buf = iid, ([], [], [], [])
        buf[0].append(d)
        buf[1].append(o)
        buf[2].append(c)
        buf[3].append(a)
        nrows += 1
    if cur_id is not None:
        raw[cur_id] = buf
    scur.close()
    conn.close()
    print(f"# loaded {nrows:,} price rows for {len(raw)} instruments "
          f"({time.time()-t0:.1f}s)")
    return universe, raw


# ------------------------------------------------ hygiene (verbatim from dip_wheel_sim.py)

MIN_SEGMENT_DAYS = 300
MAX_GAP_CAL_DAYS = 45


def clean_segment(dts, op, cl, ad):
    """Data hygiene: EODHD backfill contains ticker-reuse tails, wrong-scale price
    segments (e.g. RAL_old jumping $44 -> $21,000) and adjustment-factor breaks where
    adjusted_close jumps while raw close is flat (e.g. KLAC adj /10 on 2026-06-09).
    Untreated, these produce fake 1000x P&L or fake splits in the sim.

    Split the series at break transitions and keep the LONGEST clean segment:
      - calendar gap > 45 days (ticker reuse / long halts)
      - any one-day |ln adj_ret| > ln(3)
      - adj return and raw return disagree by >1.5x while adj moved >1.5x
        (adjustment-factor break; real splits keep adj continuous and are untouched)
    """
    n = len(dts)
    if n < 2:
        return None, n
    a, c = np.asarray(ad), np.asarray(cl)
    radj = a[1:] / a[:-1]
    lab = np.abs(np.log(radj))
    lf = np.abs(np.log(radj / (c[1:] / c[:-1])))
    gaps = np.array([(dts[k + 1] - dts[k]).days for k in range(n - 1)])
    log3, log15 = math.log(3.0), math.log(1.5)
    brk = (gaps > MAX_GAP_CAL_DAYS) | (lab > log3) | ((lab > log15) & (lf > log15))
    cuts = np.nonzero(brk)[0]
    bounds = [0] + [int(k) + 1 for k in cuts] + [n]
    j = max(range(len(bounds) - 1), key=lambda k: bounds[k + 1] - bounds[k])
    lo, hi = bounds[j], bounds[j + 1]
    trimmed = n - (hi - lo)
    if hi - lo < MIN_SEGMENT_DAYS:
        return None, trimmed
    return (dts[lo:hi], op[lo:hi], cl[lo:hi], ad[lo:hi]), trimmed


class Inst:
    __slots__ = ("name", "midx", "adj", "pos", "lpos", "n")


def build_instrument(name, buf, mdate_to_idx, M):
    seg, trimmed = clean_segment(*buf)
    if seg is None:
        return None, trimmed
    dts, _, _, ad = seg
    keep = [(mdate_to_idx[d], a) for d, a in zip(dts, ad) if d in mdate_to_idx]
    if len(keep) < MIN_SEGMENT_DAYS:
        return None, trimmed
    ins = Inst()
    ins.name = name
    ins.midx = np.array([k[0] for k in keep], dtype=np.int32)
    ins.adj = np.array([k[1] for k in keep])
    ins.n = len(keep)
    pos = np.full(M, -1, dtype=np.int32)
    pos[ins.midx] = np.arange(ins.n, dtype=np.int32)
    ins.pos = pos
    ins.lpos = np.maximum.accumulate(np.where(pos >= 0, pos, -1))
    return ins, trimmed


def aligned_ffill(ins, M):
    """Master-calendar adjusted_close, forward-filled from first trade to END
    (flat past segment end - delisted names are held flat until dropped)."""
    P = np.full(M, np.nan)
    P[ins.midx] = ins.adj
    idx = np.where(~np.isnan(P), np.arange(M), 0)
    np.maximum.accumulate(idx, out=idx)
    P = P[idx]
    P[: ins.midx[0]] = np.nan
    return P


# ------------------------------------------------------------------ portfolio engine


def simulate(targets, ret, rows, sim_a, sim_b):
    """targets: list of (signal_master_idx, {name: weight}) sorted by idx; a signal
    at master day t is traded at close[t+1]. ret: (n_rows, M) daily return matrix
    (NaN -> 0 handled by caller). Returns equity (len sim_b-sim_a+1), tno_events."""
    nav = 1.0
    hold = {}
    ti = 0
    eq = np.zeros(sim_b - sim_a + 1)
    tno = []
    for m in range(sim_a, sim_b + 1):
        rp = 0.0
        for nm, w in hold.items():
            rp += w * ret[rows[nm], m]
        nav *= 1.0 + rp
        if hold:
            g = 1.0 + rp
            hold = {nm: w * (1.0 + ret[rows[nm], m]) / g for nm, w in hold.items()}
        if ti < len(targets) and targets[ti][0] == m - 1:
            tgt = targets[ti][1]
            ti += 1
            two = 0.0
            for nm in set(tgt) | set(hold):
                two += abs(tgt.get(nm, 0.0) - hold.get(nm, 0.0))
            nav *= 1.0 - COST * two
            hold = {nm: w for nm, w in tgt.items() if w > 0}
            tno.append((m, two))
        eq[m - sim_a] = nav
    return eq, tno


# ------------------------------------------------------------------ metrics


def slice_metrics(sim_dates, eq, a, b):
    e = eq[a:b + 1]
    rets = e[1:] / e[:-1] - 1.0
    years = (sim_dates[b] - sim_dates[a]).days / 365.25
    cagr = float((e[-1] / e[0]) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    sd = float(np.std(rets, ddof=1)) if len(rets) > 1 else 0.0
    sharpe = float(np.mean(rets)) / sd * math.sqrt(252) if sd > 0 else 0.0
    peak = np.maximum.accumulate(e)
    mdd = float(np.min(e / peak - 1.0))
    return {"total_return": float(e[-1] / e[0] - 1.0), "cagr": cagr,
            "sharpe": sharpe, "max_dd": mdd, "years": round(years, 2)}


def worst_year(sim_dates, eq, a, b):
    """Calendar-year returns inside [a,b]; base = last equity of prior year (or eq[a]).
    Years not fully covered by the window are flagged partial."""
    out = []
    y0, y1 = sim_dates[a].year, sim_dates[b].year
    for y in range(y0, y1 + 1):
        lo = bisect_left(sim_dates, date(y, 1, 1), a, b + 1)
        hi = bisect_right(sim_dates, date(y, 12, 31), a, b + 1) - 1
        if lo > hi:
            continue
        base = eq[lo - 1] if lo - 1 >= a else eq[a]
        partial = (y == y0 and sim_dates[a] > date(y, 1, 7)) or \
                  (y == y1 and sim_dates[b] < date(y, 12, 24))
        out.append((y, float(eq[hi] / base - 1.0), partial))
    w = min(out, key=lambda t: t[1])
    return {"year": w[0], "return": w[1], "partial_year": w[2]}


def window_turnover(tno, sim_a, a, b, years):
    two = sum(t for m, t in tno if a <= m - sim_a <= b)
    return two / 2.0 / years if years > 0 else 0.0


# ------------------------------------------------------------------ signal schedules


def period_end_idx(mdates, months):
    """Master indices of the last trading day of each month whose month is in
    `months` (None = all)."""
    out = []
    for i in range(len(mdates) - 1):
        d = mdates[i]
        if mdates[i + 1].month != d.month and (months is None or d.month in months):
            out.append(i)
    d = mdates[-1]
    if months is None or d.month in months:
        out.append(len(mdates) - 1)
    return out


def eligible(ins, mt):
    lp = int(ins.lpos[mt])
    return lp >= 0 and mt - int(ins.midx[lp]) <= STALE_DAYS, lp


# ------------------------------------------------------------------ main


def main():
    t0 = time.time()
    universe, raw = load_data()

    spy_dates = raw["SPY.US"][0]
    mdates = [d for d in spy_dates if HIST_START <= d <= END]
    mdate_to_idx = {d: i for i, d in enumerate(mdates)}
    M = len(mdates)

    insts = {}
    hygiene = {"rows_trimmed": 0, "instruments_trimmed": [], "instruments_dropped": []}
    for nm in set(universe) | set(SECTORS) | {"SPY.US"}:
        if nm not in raw:
            continue
        ins, dr = build_instrument(nm, raw[nm], mdate_to_idx, M)
        hygiene["rows_trimmed"] += dr
        if dr > 0:
            hygiene["instruments_trimmed"].append(nm)
        if ins is None:
            hygiene["instruments_dropped"].append(nm)
        else:
            insts[nm] = ins
    del raw
    universe = [nm for nm in universe if nm in insts]
    hygiene["instruments_trimmed"].sort()
    n_dead = sum(1 for nm in universe if int(insts[nm].midx[-1]) < M - 5)
    print(f"# usable: {len(universe)} stocks + {sum(1 for s in SECTORS if s in insts)}"
          f"/9 sector ETFs; hygiene trimmed {hygiene['rows_trimmed']:,} rows across "
          f"{len(hygiene['instruments_trimmed'])} names; dropped "
          f"{hygiene['instruments_dropped']}")
    print(f"# stocks whose clean data ends before window end (truncated/delisted): {n_dead}")

    rows = {nm: k for k, nm in enumerate(insts)}
    price = np.vstack([aligned_ffill(insts[nm], M) for nm in insts])
    with np.errstate(invalid="ignore", divide="ignore"):
        ret = price[:, 1:] / price[:, :-1] - 1.0
    ret = np.concatenate([np.zeros((len(insts), 1)), np.nan_to_num(ret)], axis=1)

    m_ends = period_end_idx(mdates, None)
    q_ends = period_end_idx(mdates, {3, 6, 9, 12})

    # -------- SECTORMOM signals
    sm_a = bisect_left(mdates, SECTORMOM_START)
    sm_b = M - 1
    smom_base, smom_trend = [], []
    for mt in m_ends:
        if mt < sm_a - 1 or mt >= sm_b:
            continue
        scores = []
        for nm in SECTORS:
            ins = insts[nm]
            i = int(ins.pos[mt])
            if i >= MOM_WIN:
                scores.append((float(ins.adj[i] / ins.adj[i - MOM_WIN] - 1.0), nm, i))
        if len(scores) < 3:
            continue
        top3 = sorted(scores, reverse=True)[:3]
        smom_base.append((mt, {nm: 1.0 / 3.0 for _, nm, _ in top3}))
        tgt = {}
        for _, nm, i in top3:
            ins = insts[nm]
            if i >= SMA_WIN - 1:
                if ins.adj[i] >= ins.adj[i - SMA_WIN + 1:i + 1].mean():
                    tgt[nm] = 1.0 / 3.0
            else:
                tgt[nm] = 1.0 / 3.0   # SMA not yet defined -> pass (noted)
        smom_trend.append((mt, tgt))

    # -------- LOWVOL signals
    st_a = bisect_left(mdates, STOCK_START)
    lv_targets = []
    for mt in q_ends:
        if mt < st_a - 1 or mt >= sm_b:
            continue
        cands = []
        for nm in universe:
            ins = insts[nm]
            ok, i = eligible(ins, mt)
            if not ok or i < VOL_WIN:
                continue
            r = np.diff(np.log(ins.adj[i - VOL_WIN:i + 1]))
            vol = float(r.std(ddof=1)) * math.sqrt(252.0)
            if vol > 0:
                cands.append((vol, nm))
        cands.sort()
        sel = cands[:LOWVOL_N]
        if sel:
            lv_targets.append((mt, {nm: 1.0 / len(sel) for _, nm in sel}))

    # -------- EQUALWEIGHT signals
    ew_members = universe[:EW_N]
    ew_targets = []
    for mt in m_ends:
        if mt < st_a - 1 or mt >= sm_b:
            continue
        avail = [nm for nm in ew_members if eligible(insts[nm], mt)[0]]
        if avail:
            ew_targets.append((mt, {nm: 1.0 / len(avail) for nm in avail}))

    print(f"# signals: sectormom {len(smom_base)} months, lowvol {len(lv_targets)} "
          f"quarters, equalweight {len(ew_targets)} months  ({time.time()-t0:.1f}s)")

    # -------- run sims
    runs = {
        "SECTORMOM":       (simulate(smom_base, ret, rows, sm_a, sm_b), sm_a, sm_b),
        "SECTORMOM_TREND": (simulate(smom_trend, ret, rows, sm_a, sm_b), sm_a, sm_b),
        "LOWVOL":          (simulate(lv_targets, ret, rows, st_a, sm_b), st_a, sm_b),
        "EQUALWEIGHT":     (simulate(ew_targets, ret, rows, st_a, sm_b), st_a, sm_b),
    }

    # -------- benchmark + windows
    spy = price[rows["SPY.US"]]

    def bench_eq(a, b):
        return spy[a:b + 1] / spy[a]

    def eras(win_start_idx):
        d0 = mdates[win_start_idx]
        out = [("FULL", d0, END)]
        for lab, lo, hi in ((f"{d0.year}-2009", d0, date(2009, 12, 31)),
                            ("2010-2019", date(2010, 1, 1), date(2019, 12, 31)),
                            ("2020-2026", date(2020, 1, 1), END)):
            out.append((lab, lo, hi))
        return out

    survivorship = {
        "SECTORMOM": "clean (real ETF history from launch; no survivorship)",
        "SECTORMOM_TREND": "clean (real ETF history from launch; no survivorship)",
        "LOWVOL": "SURVIVORSHIP-INFLATED: universe is today's-survivors backfilled; "
                  "delisted low-vol blowups (utilities/financials that died) are absent, "
                  "and the DD/vol profile of a real-time universe would be worse",
        "EQUALWEIGHT": "SURVIVORSHIP-INFLATED: basket = 1999's top-100 dollar-volume "
                       "names that still exist in today's DB, further tilted to future "
                       "splitters (split-adjusted volume x raw close ranking)",
    }

    results = {"meta": {
        "generated": str(date.today()),
        "cost_bps_on_traded_notional": COST * 1e4,
        "execution": "signal close[t] (period-end), trade close[t+1], "
                     "new weights earn from t+2; cash earns 0",
        "turnover_convention": "annual one-way = sum(|dw|)/2 per year",
        "universe": f"top {TOP_N} stocks by 1999 avg close*volume (>=100 days, "
                    f"'%.US', ETFs blacklisted); EQUALWEIGHT uses the top {EW_N}",
        "n_stocks_usable": len(universe),
        "n_stocks_truncated_before_end": n_dead,
        "data_hygiene": dict(hygiene, rules="longest clean segment; breaks = gap>45cd, "
                             "any |ln adj_ret|>ln3, or adj-vs-raw return disagreement "
                             ">1.5x with adj move >1.5x (verbatim dip_wheel_sim.py)"),
        "caveats": [
            "volume in DB is split-adjusted, close raw -> 1999 ADV ranking tilts to future splitters",
            "delisted/truncated names held flat at last price until next rebalance drops them",
            "SECTORMOM_TREND: 200d SMA unavailable before ~1999-10 counts as pass",
            "r=0 on cash understates SECTORMOM_TREND in high-rate eras",
            "worst_year entries flagged partial_year=true cover only part of that calendar year",
        ],
    }, "strategies": {}}

    lines = []
    for name, ((eq, tno), a, b) in runs.items():
        sim_dates = mdates[a:b + 1]
        sd = {"survivorship": survivorship[name], "windows": {}}
        for lab, lo, hi in eras(a):
            wa = bisect_left(sim_dates, lo)
            wb = bisect_right(sim_dates, hi) - 1
            if wa >= wb:
                continue
            mtr = slice_metrics(sim_dates, eq, wa, wb)
            mtr["ann_turnover_oneway"] = window_turnover(tno, a, wa, wb, mtr["years"])
            mtr["worst_year"] = worst_year(sim_dates, eq, wa, wb)
            beq = bench_eq(a, b)
            bm = slice_metrics(sim_dates, beq, wa, wb)
            bm["worst_year"] = worst_year(sim_dates, beq, wa, wb)
            mtr["spy"] = bm
            sd["windows"][lab] = mtr
        results["strategies"][name] = sd

        # table
        print(f"\n== {name}  ({sim_dates[0]} .. {sim_dates[-1]})  "
              f"[{'CLEAN' if 'clean' in survivorship[name] else 'SURVIVORSHIP-BIASED'}] ==")
        print(f"{'window':<10} {'cagr':>7} {'sharpe':>7} {'maxDD':>7} {'turn/yr':>8} "
              f"{'worst-year':>16} | {'SPY cagr':>8} {'SPY shp':>7} {'SPY DD':>7}")
        for lab, w in sd["windows"].items():
            wy = w["worst_year"]
            wys = f"{wy['year']}{'*' if wy['partial_year'] else ' '} {wy['return']:>7.1%}"
            s = w["spy"]
            print(f"{lab:<10} {w['cagr']:>7.2%} {w['sharpe']:>7.2f} {w['max_dd']:>7.1%} "
                  f"{w['ann_turnover_oneway']:>8.2f} {wys:>16} | {s['cagr']:>8.2%} "
                  f"{s['sharpe']:>7.2f} {s['max_dd']:>7.1%}")

        full = sd["windows"]["FULL"]
        beats = full["sharpe"] > full["spy"]["sharpe"]
        fail_eras = [lab for lab, w in sd["windows"].items()
                     if lab != "FULL" and w["sharpe"] <= w["spy"]["sharpe"]]
        lines.append((name, beats, fail_eras))

    with open(os.path.join(SCRATCH, "shotgun_xs_results.json"), "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"\n# results -> {os.path.join(SCRATCH, 'shotgun_xs_results.json')}")
    print("\n# one-liners (Sharpe vs SPY buy-and-hold, net of 10bps):")
    for name, beats, fails in lines:
        tag = "" if "clean" in survivorship[name] else "  [survivorship-inflated]"
        print(f"#   {name}: {'BEATS' if beats else 'does NOT beat'} SPY full-window; "
              f"era Sharpe losses: {fails or 'none'}{tag}")
    print(f"# total {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
