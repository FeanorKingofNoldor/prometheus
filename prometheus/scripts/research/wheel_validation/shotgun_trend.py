#!/usr/bin/python3.14
"""Shotgun test of three classic trend/timing strategies on prometheus_historical.prices_daily.

Strategies
  FABER     : SPY vs 200d SMA, monthly (last trading day). Above -> SPY, below -> SHY
              (cash at 0% before SHY exists, 2002-07-26). Window 1998-01 .. 2026-07.
  DUALMOM   : monthly 12-1 momentum rank of {SPY,QQQ,TLT,GLD}; top-1 (and top-2 EW variant)
              with absolute-momentum filter vs SHY. Window 2005-01 .. 2026-07.
  VOLTARGET : SPY weight = min(1, 0.10 / realized 20d ann. vol), daily, 5pp no-trade band,
              remainder cash at 0%. Window 1998-01 .. 2026-07.

Conventions (honest, documented)
  * All signals AND returns use adjusted_close (total-return proxy incl. dividends).
  * Fill model: signal computed at the close of day t; trade fills at the close of day t+1
    (task spec: next day's open equivalent, approximated by next day's adjusted_close).
    Therefore the NEW position first earns the return of day t+2 (close t+1 -> close t+2).
    Implemented as a 2-day shift of the decided-weight series.  This is slightly
    conservative vs. "position effective for day t+1's return".
  * Costs: 5 bps * sum_assets |delta weight|, charged when the new position takes effect.
    A full switch SPY -> SHY trades 200% notional -> 10 bps.  Moves to/from cash cost
    5 bps * |delta| (cash leg is free).
  * Weights are held constant at target between rebalances (implicit daily rebalancing to
    target between signal changes; immaterial for the 0/1 books, minor for top-2 / voltarget).
  * Sharpe: rf = 0, mean(daily)/std(daily) * sqrt(252). Same definition for the SPY benchmark.
  * CAGR: calendar-based, (end_equity/start_equity)^(365.25/days) - 1.
  * Benchmark = SPY buy-and-hold over the same window/era, cost-free.
  * pct_time_invested = average weight in RISK assets * 100 (SHY/cash count as not invested).
  * Worst year: worst calendar-year compounded return inside the slice; first/last years of a
    slice may be partial years (they are included; the year label makes this auditable).

Data notes
  * SPY.US in this DB starts 1996-12-09 (not 1993).  200d SMA is fully warmed up before the
    1998-01 window start, so FABER/VOLTARGET are unaffected.
  * GLD.US starts 2004-11-18, so it lacks a full 12-1 lookback until 2005-11; until then it is
    excluded from the DUALMOM ranking (the window still starts 2005-01 as specified).
  * Master trading calendar = SPY's trade dates; other ETFs reindexed to it and forward-filled.
"""

import json
import math
import os
import sys

import numpy as np
import pandas as pd
import psycopg2

SCRATCH = os.path.dirname(os.path.abspath(__file__))
OUT_JSON = os.path.join(SCRATCH, "shotgun_trend_results.json")

COST_BPS = 5e-4          # 5 bps per unit of turnover (sum |dw|)
TRADE_EPS = 1e-9

SYMS = ["SPY.US", "QQQ.US", "TLT.US", "GLD.US", "SHY.US"]


# ----------------------------------------------------------------------------- data
def load_prices() -> pd.DataFrame:
    conn = psycopg2.connect(
        host="localhost", port=6432, user="prometheus",
        password=os.environ["HISTORICAL_DB_PASSWORD"], dbname="prometheus_historical",
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT instrument_id, trade_date, adjusted_close FROM prices_daily "
            "WHERE instrument_id = ANY(%s) ORDER BY trade_date",
            (SYMS,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=["sym", "date", "adj"])
    wide = df.pivot(index="date", columns="sym", values="adj").astype(float)
    wide.index = pd.to_datetime(wide.index)
    wide = wide.sort_index()
    # master calendar = SPY trading days; ffill others onto it
    wide = wide[wide["SPY.US"].notna()]
    wide = wide.ffill()
    return wide


# ----------------------------------------------------------------------------- engine
def month_ends(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby(idx.to_period("M")).last().values)


def run_book(weights: pd.DataFrame, rets: pd.DataFrame, start: str, end: str):
    """weights: DECIDED weights per asset per day (decided at that day's close).
    Applies the 2-day effectiveness shift, costs on turnover, clips to window.
    Returns dict with daily strategy returns, risk weight, turnover."""
    held = weights.shift(2).fillna(0.0)
    r = rets.reindex(columns=held.columns).fillna(0.0)
    gross = (held * r).sum(axis=1)
    turnover = held.diff().abs().sum(axis=1).fillna(0.0)
    net = gross - COST_BPS * turnover
    sl = slice(pd.Timestamp(start), pd.Timestamp(end))
    return {
        "ret": net.loc[sl],
        "turnover": turnover.loc[sl],
        "held": held.loc[sl],
    }


def max_drawdown(equity: pd.Series) -> float:
    return float((equity / equity.cummax() - 1.0).min())


def slice_metrics(ret: pd.Series, risk_w: pd.Series, turnover: pd.Series) -> dict:
    if len(ret) < 20:
        return {"n_days": int(len(ret)), "note": "too few observations"}
    eq = (1.0 + ret).cumprod()
    days = (ret.index[-1] - ret.index[0]).days
    years = days / 365.25
    cagr = float(eq.iloc[-1] ** (1.0 / years) - 1.0) if years > 0 else float("nan")
    sd = float(ret.std(ddof=1))
    sharpe = float(ret.mean() / sd * math.sqrt(252)) if sd > 0 else float("nan")
    yearly = (1.0 + ret).groupby(ret.index.year).prod() - 1.0
    wy = yearly.idxmin()
    return {
        "n_days": int(len(ret)),
        "start": str(ret.index[0].date()),
        "end": str(ret.index[-1].date()),
        "cagr_pct": round(cagr * 100, 2),
        "sharpe": round(sharpe, 2),
        "max_dd_pct": round(max_drawdown(eq) * 100, 2),
        "trades": int((turnover > TRADE_EPS).sum()),
        "pct_time_invested": round(float(risk_w.mean()) * 100, 1),
        "worst_year": {"year": int(wy), "return_pct": round(float(yearly.loc[wy]) * 100, 2)},
    }


def full_and_eras(ret, risk_w, turnover, window_start, window_end) -> dict:
    eras = {
        "full": (window_start, window_end),
        "era_to_2009": (window_start, "2009-12-31"),
        "2010_2019": ("2010-01-01", "2019-12-31"),
        "2020_2026": ("2020-01-01", window_end),
    }
    out = {}
    for name, (a, b) in eras.items():
        sl = slice(pd.Timestamp(a), pd.Timestamp(b))
        out[name] = slice_metrics(ret.loc[sl], risk_w.loc[sl], turnover.loc[sl])
    return out


# ----------------------------------------------------------------------------- strategies
def strat_faber(px: pd.DataFrame, rets: pd.DataFrame):
    spy = px["SPY.US"]
    sma = spy.rolling(200).mean()
    mes = month_ends(px.index)
    shy_live = pd.Timestamp("2002-07-26")

    w = pd.DataFrame(0.0, index=px.index, columns=["SPY.US", "SHY.US"])
    tgt = pd.DataFrame(np.nan, index=px.index, columns=w.columns)
    for d in mes:
        if np.isnan(sma.loc[d]):
            continue
        risk_on = spy.loc[d] > sma.loc[d]
        tgt.loc[d, "SPY.US"] = 1.0 if risk_on else 0.0
        tgt.loc[d, "SHY.US"] = 0.0 if risk_on else (1.0 if d >= shy_live else 0.0)
    w = tgt.ffill().fillna(0.0)
    book = run_book(w, rets, "1998-01-01", "2026-07-31")
    return book, book["held"]["SPY.US"]


def strat_dualmom(px: pd.DataFrame, rets: pd.DataFrame, top_n: int):
    risk = ["SPY.US", "QQQ.US", "TLT.US", "GLD.US"]
    mom = px.shift(21) / px.shift(252) - 1.0          # 12-1: t-252 .. t-21
    # eligibility: asset must genuinely have 252d of its own history (ffill would fake it)
    first = {s: px[s].first_valid_index() for s in SYMS}
    mes = month_ends(px.index)
    cols = risk + ["SHY.US"]
    tgt = pd.DataFrame(np.nan, index=px.index, columns=cols)
    slot_w = 1.0 / top_n
    for d in mes:
        if np.isnan(mom.loc[d, "SHY.US"]):
            continue
        elig = [s for s in risk
                if not np.isnan(mom.loc[d, s])
                and (d - first[s]).days >= 370]        # ~252 trading days of real history
        if len(elig) < top_n:
            continue
        ranked = sorted(elig, key=lambda s: mom.loc[d, s], reverse=True)
        wrow = {c: 0.0 for c in cols}
        for s in ranked[:top_n]:                       # per-slot absolute momentum filter
            if mom.loc[d, s] > mom.loc[d, "SHY.US"]:
                wrow[s] += slot_w
            else:
                wrow["SHY.US"] += slot_w
        tgt.loc[d] = pd.Series(wrow)
    w = tgt.ffill().fillna(0.0)
    book = run_book(w, rets, "2005-01-01", "2026-07-31")
    return book, book["held"][risk].sum(axis=1)


def strat_voltarget(px: pd.DataFrame, rets: pd.DataFrame):
    spy_ret = rets["SPY.US"]
    vol = spy_ret.rolling(20).std(ddof=1) * math.sqrt(252)
    raw = (0.10 / vol).clip(upper=1.0)
    decided = pd.Series(np.nan, index=px.index)
    cur = np.nan
    for d, v in raw.items():
        if np.isnan(v):
            continue
        if np.isnan(cur) or abs(v - cur) > 0.05:      # 5pp no-trade band
            cur = v
        decided.loc[d] = cur
    w = pd.DataFrame({"SPY.US": decided.ffill().fillna(0.0)})
    book = run_book(w, rets, "1998-01-01", "2026-07-31")
    return book, book["held"]["SPY.US"]


def spy_benchmark(rets: pd.DataFrame, window_start: str, window_end: str) -> dict:
    r = rets["SPY.US"].loc[pd.Timestamp(window_start):pd.Timestamp(window_end)].dropna()
    ones = pd.Series(1.0, index=r.index)
    zeros = pd.Series(0.0, index=r.index)
    return full_and_eras(r, ones, zeros, window_start, window_end)


# ----------------------------------------------------------------------------- report
def fmt_table(name: str, res: dict, bench: dict) -> str:
    hdr = f"{'slice':<14}{'CAGR%':>8}{'Sharpe':>8}{'MaxDD%':>9}{'trades':>8}{'%inv':>7}{'worst yr':>16}   | SPY: {'CAGR%':>7}{'Sharpe':>8}{'MaxDD%':>9}"
    lines = [f"== {name} ==", hdr]
    for k in ["full", "era_to_2009", "2010_2019", "2020_2026"]:
        m, b = res[k], bench[k]
        if "note" in m:
            lines.append(f"{k:<14}{m['note']}")
            continue
        wy = f"{m['worst_year']['year']}: {m['worst_year']['return_pct']:+.1f}%"
        lines.append(
            f"{k:<14}{m['cagr_pct']:>8.2f}{m['sharpe']:>8.2f}{m['max_dd_pct']:>9.2f}"
            f"{m['trades']:>8d}{m['pct_time_invested']:>7.1f}{wy:>16}   |      "
            f"{b['cagr_pct']:>7.2f}{b['sharpe']:>8.2f}{b['max_dd_pct']:>9.2f}"
        )
    return "\n".join(lines)


def main():
    px = load_prices()
    rets = px.pct_change(fill_method=None)

    results = {"meta": {
        "generated": pd.Timestamp.now().isoformat(timespec="seconds"),
        "data_source": "prometheus_historical.prices_daily (adjusted_close)",
        "fill_model": "signal close t -> fill close t+1 (next-day-open proxy) -> earns from day t+2",
        "costs": "5 bps * sum|dw| on tradeable legs; cash leg free; SPY->SHY switch = 10 bps",
        "sharpe_rf": 0.0,
        "notes": [
            "SPY history in DB starts 1996-12-09 (not 1993); irrelevant for the 1998-01 windows.",
            "GLD enters DUALMOM ranking only once it has a full 12-1 lookback (~2005-11).",
            "First/last calendar years of a slice may be partial for worst_year.",
            "pct_time_invested = mean risk-asset weight x 100 (SHY/cash = not invested).",
        ],
    }, "strategies": {}}

    windows = {}

    faber, faber_rw = strat_faber(px, rets)
    windows["FABER"] = ("1998-01-01", "2026-07-31", faber, faber_rw)

    dm1, dm1_rw = strat_dualmom(px, rets, top_n=1)
    windows["DUALMOM_top1"] = ("2005-01-01", "2026-07-31", dm1, dm1_rw)

    dm2, dm2_rw = strat_dualmom(px, rets, top_n=2)
    windows["DUALMOM_top2ew"] = ("2005-01-01", "2026-07-31", dm2, dm2_rw)

    vt, vt_rw = strat_voltarget(px, rets)
    windows["VOLTARGET"] = ("1998-01-01", "2026-07-31", vt, vt_rw)

    bench_cache = {}
    for name, (a, b, book, rw) in windows.items():
        res = full_and_eras(book["ret"], rw, book["turnover"], a, b)
        if (a, b) not in bench_cache:
            bench_cache[(a, b)] = spy_benchmark(rets, a, b)
        bench = bench_cache[(a, b)]
        results["strategies"][name] = {"window": [a, b], "metrics": res, "spy_benchmark": bench}
        print(fmt_table(name, res, bench))
        print()

    # ---- sanity: FABER textbook profile (2008 shallow DD, CAGR near SPY)
    fres = results["strategies"]["FABER"]
    f2008 = (1.0 + faber["ret"].loc["2008"]).prod() - 1.0
    s2008 = (1.0 + rets["SPY.US"].loc["2008"]).prod() - 1.0
    sanity = {
        "faber_2008_return_pct": round(float(f2008) * 100, 2),
        "spy_2008_return_pct": round(float(s2008) * 100, 2),
        "faber_full_cagr_pct": fres["metrics"]["full"]["cagr_pct"],
        "spy_full_cagr_pct": fres["spy_benchmark"]["full"]["cagr_pct"],
        "faber_full_maxdd_pct": fres["metrics"]["full"]["max_dd_pct"],
        "spy_full_maxdd_pct": fres["spy_benchmark"]["full"]["max_dd_pct"],
    }
    sanity["passes"] = bool(
        f2008 > s2008 + 0.20
        and fres["metrics"]["full"]["max_dd_pct"] > fres["spy_benchmark"]["full"]["max_dd_pct"] + 15
        and abs(fres["metrics"]["full"]["cagr_pct"] - fres["spy_benchmark"]["full"]["cagr_pct"]) < 5
    )
    results["sanity_faber"] = sanity
    print("SANITY (FABER textbook profile):", json.dumps(sanity))

    with open(OUT_JSON, "w") as f:
        json.dump(results, f, indent=1)
    print(f"\nwrote {OUT_JSON}")


if __name__ == "__main__":
    sys.exit(main())
