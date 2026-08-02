#!/usr/bin/env python3.14
"""
Combined "dip-buyer core + wheel overlay" backtest, 1998-01-02 .. 2026-07-31, $250k.

Variants (identical core rules, same universe/slots/budget):
  dip-plain : entries buy shares at open[t+1] (integer shares), exits sell at open[t+1].
  dip+wheel : entries sell a 30cd cash-secured put (strike = raw_close*0.98) instead of
              buying; assignment -> own shares -> 30cd covered calls (strike = raw*1.08);
              recovery exit while call open -> buy back call at intrinsic, sell at open[t+1].

Core rules (replicating the C++ core_wheel engine):
  - Universe: top-300 by 1998 avg dollar volume (close*volume, >=100 trading days),
    instruments LIKE '%.US'. Decisions from close[t] only (no look-ahead).
  - Dip entry: adj_close <= (1-entry_dd) * trailing 126-trading-day high of adj_close,
    entry_dd in {0.15, 0.20}. Quality gate: at the date of that 126d high, adj_close >=
    its 200d SMA (>=200d history at the high date, else ineligible).
  - Slots: max_positions in {5, 6}; candidates ranked deepest drawdown first.
  - min holding 5 trading days; 10-trading-day re-entry cooldown after exits.
  - Exit: adj_close >= 0.95 * trailing 126d high (recovery).
  - Budget: 90% of equity target, equal split per slot, sized at entry (no rebalancing).

Wheel premium pricing: Black-Scholes r=0 q=0,
  sigma = max(0.15, 63d realized vol of adj_close log returns * 1.1).
Costs both directions incl. buy-backs: 5% premium haircut + $0.65/contract.
Expiry: first trading day >= trade_date + 30 calendar days; European exercise at raw close.

Documented modeling choices / caveats:
  - Universe is today's-survivors backfilled (plus a few acquired names with truncated
    history, e.g. TWX). The sim's point is the MARGINAL wheel-vs-plain comparison on the
    same universe, where the bias mostly cancels.
  - DB volume is split-adjusted (today's share basis) while close is raw, so the 1998
    close*volume ranking further tilts to future splitters (AMZN/AAPL effect). Kept as
    specified.
  - Equity = free cash + reserved CSP cash + shares at raw close - short-option intrinsic.
    Marking shorts at intrinsic understates interim vol drag and overstates wheel Sharpe
    during OTM phases (same convention as wheel_sim.py).
  - Dividends: while holding shares (both variants), daily cash credit per share of
    close[t-1]*adj[t]/adj[t-1] - close[t] (total-return-implied dividend).
  - Splits: detected when (adj-return / raw-return) deviates >10% from 1; share count
    (rounded, cash-in-lieu for fractions) and open option strike/deliverable adjusted
    OCC-style. Post-split odd lots stay uncovered; a call-away sells the odd remainder
    at that day's close.
  - r=0: CSP reserved cash and free cash earn 0 (understates wheel in high-rate eras).
  - Expiry-only (European) assignment; early assignment not modeled.
  - A slot with a pending open[t+1] exit stays occupied until the fill (no same-bar
    rotation). Missed CSP (expired OTM) re-opens the slot with NO cooldown; call-away
    and recovery exits set the 10-day cooldown.
  - If an instrument stops trading >15 straight sessions while engaged, force-liquidate
    at last known close (options settled at intrinsic, no costs); counted.
  - If an instrument doesn't trade on the expiry day, settlement uses last known close.
"""

import json
import math
import os
import time
from bisect import bisect_left, bisect_right
from datetime import date, timedelta

import numpy as np
import psycopg2

SCRATCH = os.path.dirname(os.path.abspath(__file__))
HIST_START = date(1996, 6, 1)
SIM_START = date(1998, 1, 2)
END = date(2026, 7, 31)
INIT_CASH = 250_000.0
COMMISSION = 0.65            # $/contract
SLIPPAGE = 0.05              # fraction of premium, both directions
TARGET_DTE = 30              # calendar days
HIGH_WIN = 126
SMA_WIN = 200
VOL_WIN = 63
EXIT_RECOVERY = 0.05
MIN_HOLD = 5                 # trading days
COOLDOWN = 10                # trading days
INVEST_FRAC = 0.90
PUT_K_RATIO = 0.98
CALL_K_RATIO = 1.08
IV_FACTOR = 1.1
SIGMA_FLOOR = 0.15
SPLIT_TOL = 0.10
STALE_DAYS = 15
TOP_N = 300
ENTRY_DDS = (0.15, 0.20)
MAX_POSITIONS = (5, 6)

ERAS = [
    ("1998-2009", date(1998, 1, 1), date(2009, 12, 31)),
    ("2010-2019", date(2010, 1, 1), date(2019, 12, 31)),
    ("2020-2026", date(2020, 1, 1), date(2026, 12, 31)),
]

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


# ---------------------------------------------------------------- data


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
        (date(1998, 1, 1), date(1998, 12, 31), TOP_N))
    universe = [r[0] for r in cur.fetchall()]
    cur.close()
    print(f"# universe: top {len(universe)} by 1998 ADV  ({time.time()-t0:.1f}s)")

    ids = sorted(set(universe) | {"SPY.US"})
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


class Inst:
    __slots__ = ("name", "midx", "open", "close", "adj", "pos", "lpos",
                 "hi126", "depth", "entry_ok", "exit_trig", "sigma",
                 "is_split", "split_f", "div_ps", "n")


MIN_SEGMENT_DAYS = 300
MAX_GAP_CAL_DAYS = 45


def clean_segment(dts, op, cl, ad):
    """Data hygiene: EODHD backfill contains ticker-reuse tails, wrong-scale price
    segments (e.g. RAL_old jumping $44 -> $21,000) and adjustment-factor breaks where
    adjusted_close jumps while raw close is flat (e.g. KLAC adj /10 on 2026-06-09).
    Untreated, these produce fake 1000x P&L or fake splits in the sim.

    Split the series at break transitions and keep the LONGEST clean segment:
      - calendar gap > 45 days (ticker reuse / long halts)
      - any one-day |ln adj_ret| > ln(3). This also segments away post-bankruptcy
        stubs (LEH's $0.21 tail, BIG 2024): a matched up/down pair of scale breaks
        (GPS 1997) is indistinguishable from a crash+recovery, so no crash exemption.
        An engaged position whose name's data ends is force-liquidated at the last
        pre-break close after 15 stale sessions - favourable, rare, and symmetric
        across variants.
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


def build_instrument(name, buf, mdate_to_idx, M):
    """Returns (Inst | None, hygiene_rows_trimmed, rows_off_master_calendar)."""
    seg, trimmed = clean_segment(*buf)
    if seg is None:
        return None, trimmed, 0
    dts, op, cl, ad = seg
    keep = [(mdate_to_idx[d], o, c, a) for d, o, c, a in zip(dts, op, cl, ad)
            if d in mdate_to_idx]
    offcal = len(dts) - len(keep)
    if len(keep) < MIN_SEGMENT_DAYS:
        return None, trimmed, offcal
    ins = Inst()
    ins.name = name
    ins.midx = np.array([k[0] for k in keep], dtype=np.int32)
    ins.open = np.array([k[1] for k in keep])
    ins.close = np.array([k[2] for k in keep])
    ins.adj = np.array([k[3] for k in keep])
    n = ins.n = len(keep)

    pos = np.full(M, -1, dtype=np.int32)
    pos[ins.midx] = np.arange(n, dtype=np.int32)
    ins.pos = pos
    # last own index at-or-before each master day (-1 before first trade)
    tmp = np.where(pos >= 0, pos, -1)
    ins.lpos = np.maximum.accumulate(tmp)

    adj = ins.adj
    # trailing 126d high (incl. today) + index of the high (first occurrence)
    hi126 = np.full(n, np.nan)
    hi_idx = np.full(n, -1, dtype=np.int64)
    if n >= HIGH_WIN:
        sw = np.lib.stride_tricks.sliding_window_view(adj, HIGH_WIN)
        hi126[HIGH_WIN - 1:] = sw.max(axis=1)
        hi_idx[HIGH_WIN - 1:] = np.arange(n - HIGH_WIN + 1) + sw.argmax(axis=1)
    ins.hi126 = hi126
    with np.errstate(invalid="ignore"):
        ins.depth = 1.0 - adj / hi126

    # 200d SMA
    sma200 = np.full(n, np.nan)
    if n >= SMA_WIN:
        cs = np.concatenate(([0.0], np.cumsum(adj)))
        sma200[SMA_WIN - 1:] = (cs[SMA_WIN:] - cs[:-SMA_WIN]) / SMA_WIN

    # quality gate: at the 126d-high date, adj >= its 200d SMA (needs >=200d history there)
    gate = np.zeros(n, dtype=bool)
    okhi = hi_idx >= SMA_WIN - 1
    hidx = np.where(okhi, hi_idx, 0)
    with np.errstate(invalid="ignore"):
        gate[okhi] = adj[hidx[okhi]] >= sma200[hidx[okhi]]
    ins.entry_ok = gate  # combined later with per-threshold depth condition

    with np.errstate(invalid="ignore"):
        ins.exit_trig = np.where(np.isnan(hi126), False,
                                 adj >= (1.0 - EXIT_RECOVERY) * hi126)

    # 63d realized vol of adj log returns, annualized, *1.1, floored
    sig = np.full(n, SIGMA_FLOOR)
    if n > VOL_WIN + 1:
        r = np.log(adj[1:] / adj[:-1])
        c1 = np.concatenate(([0.0], np.cumsum(r)))
        c2 = np.concatenate(([0.0], np.cumsum(r * r)))
        s1 = c1[VOL_WIN:] - c1[:-VOL_WIN]
        s2 = c2[VOL_WIN:] - c2[:-VOL_WIN]
        var = np.maximum(0.0, (s2 - s1 * s1 / VOL_WIN) / (VOL_WIN - 1))
        vals = np.sqrt(var * 252.0) * IV_FACTOR
        sig[VOL_WIN:] = np.maximum(SIGMA_FLOOR, vals)
    ins.sigma = sig

    # corporate actions (event applies at own day i, i >= 1)
    radj = adj[1:] / adj[:-1]
    rraw = ins.close[1:] / ins.close[:-1]
    f = radj / rraw
    is_split = np.abs(f - 1.0) > SPLIT_TOL
    div = ins.close[:-1] * radj - ins.close[1:]
    ins.is_split = np.concatenate(([False], is_split))
    ins.split_f = np.concatenate(([1.0], f))
    ins.div_ps = np.concatenate(([0.0], np.where(is_split, 0.0, div)))
    return ins, trimmed, offcal


# ---------------------------------------------------------------- simulation


def expiry_idx(mdates, m):
    """First master trading day >= mdates[m] + 30cd, strictly after m. None if past end."""
    j = bisect_left(mdates, mdates[m] + timedelta(days=TARGET_DTE))
    if j >= len(mdates):
        return None
    return max(j, m + 1)


def run_sim(variant, entry_dd, max_pos, insts, mdates, off, cands, trace=None,
            trade_log=None):
    """variant: 'plain' | 'wheel'. Returns dict of sim-space series and event lists.
    trace: optional list; per sim day appends [(name, shares, close, value, div_cash)].
    trade_log: optional list; appends a dict per option write (audit)."""
    M = len(mdates)
    N = M - off                       # sim days
    cash = INIT_CASH
    slots = {}                        # name -> slot dict
    cooldown = {}                     # name -> first master idx allowed again
    equity = np.zeros(N)
    occ = np.zeros(N, dtype=np.int8)
    shr = np.zeros(N, dtype=np.int8)  # slots holding shares
    ev = {k: [] for k in ("prem", "assign", "missed", "missed_reb", "callaway",
                          "rexit", "forced")}
    neg_cash_days = 0
    min_cash = INIT_CASH

    def mark(m):
        e = cash
        for nm, sl in slots.items():
            ins = insts[nm]
            lp = ins.lpos[m]
            S = ins.close[lp] if lp >= 0 else 0.0
            e += sl.get("reserved", 0.0)
            sh = sl.get("shares", 0)
            if sh:
                e += sh * S
            if sl.get("csp"):
                e -= max(sl["csp"]["K"] - S, 0.0) * sl["csp"]["N"]
            if sl.get("call"):
                e -= max(S - sl["call"]["K"], 0.0) * sl["call"]["N"]
        return e

    for m in range(off, M):
        s = m - off
        div_today = {}

        # --- B) corporate actions on engaged names (before open fills)
        for nm, sl in slots.items():
            ins = insts[nm]
            i = ins.pos[m]
            if i <= 0 or not ins.is_split[i]:
                if i > 0 and sl.get("shares", 0) > 0:
                    cash += ins.div_ps[i] * sl["shares"]
                    div_today[nm] = ins.div_ps[i] * sl["shares"]
                continue
            f = ins.split_f[i]
            if sl.get("shares", 0) > 0:
                new_sh = int(round(sl["shares"] * f))
                cash += (sl["shares"] * f - new_sh) * ins.close[i]  # cash in lieu
                sl["shares"] = max(new_sh, 0)
            for leg in ("csp", "call"):
                o = sl.get(leg)
                if o:
                    o["K"] /= f
                    o["N"] = int(round(o["N"] * f))
            if sl.get("csp"):
                new_res = sl["csp"]["K"] * sl["csp"]["N"]
                cash += sl["reserved"] - new_res
                sl["reserved"] = new_res

        # --- A) open fills from yesterday's decisions
        for nm in list(slots):
            sl = slots[nm]
            ins = insts[nm]
            i = ins.pos[m]
            if i < 0:
                continue
            px = ins.open[i] if ins.open[i] > 0 else ins.close[i]
            if sl["state"] == "PENDING_BUY":
                q = int(min(sl["budget"] // px, cash // px))
                if q < 1:
                    del slots[nm]           # cancelled; name back in pool
                else:
                    cash -= q * px
                    slots[nm] = {"state": "SHARES", "shares": q, "entry_m": m}
            elif sl.get("pending_exit"):
                cash += sl["shares"] * px
                del slots[nm]
                cooldown[nm] = m + COOLDOWN

        # --- stale-data force liquidation
        for nm in list(slots):
            sl = slots[nm]
            ins = insts[nm]
            lp = ins.lpos[m]
            if lp >= 0 and m - int(ins.midx[lp]) <= STALE_DAYS:
                continue
            S = ins.close[lp] if lp >= 0 else 0.0
            if sl["state"] == "PENDING_BUY":
                pass
            else:
                cash += sl.get("reserved", 0.0) + sl.get("shares", 0) * S
                if sl.get("csp"):
                    cash -= max(sl["csp"]["K"] - S, 0.0) * sl["csp"]["N"]
                if sl.get("call"):
                    cash -= max(S - sl["call"]["K"], 0.0) * sl["call"]["N"]
                cooldown[nm] = m + COOLDOWN
            del slots[nm]
            ev["forced"].append(s)

        # --- C) settle option expiries at close[m]
        for nm in list(slots):
            sl = slots[nm]
            ins = insts[nm]
            lp = ins.lpos[m]
            S = ins.close[lp]
            csp = sl.get("csp")
            if csp and csp["expiry"] == m:
                K, Nsh = csp["K"], csp["N"]
                if S < K:                       # assigned
                    cash += sl["reserved"] - K * Nsh
                    slots[nm] = {"state": "SHARES", "shares": Nsh, "entry_m": m}
                    ev["assign"].append(s)
                else:                           # missed entry
                    cash += sl["reserved"]
                    del slots[nm]               # no cooldown
                    ev["missed"].append(s)
                    if S > csp["entry_close"]:
                        ev["missed_reb"].append(s)
                continue
            call = sl.get("call")
            if call and call["expiry"] == m:
                K, Nsh = call["K"], call["N"]
                if S > K:                       # called away (the trim)
                    cash += K * Nsh
                    rem = sl["shares"] - Nsh
                    if rem > 0:
                        cash += rem * S         # sell odd remainder
                    del slots[nm]
                    cooldown[nm] = m + COOLDOWN
                    ev["callaway"].append(s)
                else:
                    sl["call"] = None

        # --- D) decisions at close[m]
        eq_now = mark(m)
        slot_budget = INVEST_FRAC * eq_now / max_pos

        # D1) recovery exits
        for nm, sl in slots.items():
            if sl["state"] != "SHARES" or sl.get("pending_exit"):
                continue
            ins = insts[nm]
            i = ins.pos[m]
            if i < 0 or m - sl["entry_m"] < MIN_HOLD or not ins.exit_trig[i]:
                continue
            call = sl.get("call")
            if call:
                intr = max(ins.close[i] - call["K"], 0.0) * call["N"]
                cost = intr * (1.0 + SLIPPAGE) + COMMISSION * math.ceil(call["N"] / 100)
                cash -= cost
                ev["prem"].append((s, -cost))
                sl["call"] = None
            sl["pending_exit"] = True
            ev["rexit"].append(s)

        # D2) entries into free slots
        free = max_pos - len(slots)
        if free > 0:
            for _, nm in cands[s]:
                if free == 0:
                    break
                if nm in slots or cooldown.get(nm, -1) > m:
                    continue
                ins = insts[nm]
                i = ins.pos[m]
                S = ins.close[i]
                if variant == "plain":
                    if slot_budget // S < 1:
                        continue                # too expensive for the slot
                    slots[nm] = {"state": "PENDING_BUY", "budget": slot_budget}
                    free -= 1
                else:
                    K = S * PUT_K_RATIO
                    c = int(min(slot_budget // (K * 100.0), cash // (K * 100.0)))
                    if c < 1:
                        continue                # skip name, take next candidate
                    exp = expiry_idx(mdates, m)
                    if exp is None:
                        continue                # no listable expiry near data end
                    T = (mdates[exp] - mdates[m]).days / 365.0
                    prem = bs_put(S, K, ins.sigma[i], T) * 100 * c
                    net = prem * (1.0 - SLIPPAGE) - COMMISSION * c
                    cash += net - K * 100.0 * c
                    slots[nm] = {"state": "CSP", "reserved": K * 100.0 * c,
                                 "csp": {"K": K, "N": 100 * c, "expiry": exp,
                                         "entry_close": S}}
                    ev["prem"].append((s, net))
                    if trade_log is not None:
                        trade_log.append({"kind": "csp", "s": s, "name": nm, "S": S,
                                          "K": K, "sigma": ins.sigma[i], "c": c,
                                          "net": net, "own_i": int(i),
                                          "expiry_m": exp})
                    free -= 1

        # D3) covered calls on uncovered share slots (wheel)
        if variant == "wheel":
            for nm, sl in slots.items():
                if (sl["state"] != "SHARES" or sl.get("call") or sl.get("pending_exit")
                        or sl["shares"] < 100):
                    continue
                ins = insts[nm]
                i = ins.pos[m]
                if i < 0:
                    continue
                exp = expiry_idx(mdates, m)
                if exp is None:
                    continue
                S = ins.close[i]
                K = S * CALL_K_RATIO
                c = sl["shares"] // 100
                T = (mdates[exp] - mdates[m]).days / 365.0
                prem = bs_call(S, K, ins.sigma[i], T) * 100 * c
                net = prem * (1.0 - SLIPPAGE) - COMMISSION * c
                cash += net
                sl["call"] = {"K": K, "N": 100 * c, "expiry": exp}
                ev["prem"].append((s, net))
                if trade_log is not None:
                    trade_log.append({"kind": "call", "s": s, "name": nm, "S": S,
                                      "K": K, "sigma": ins.sigma[i], "c": c,
                                      "net": net, "own_i": int(i), "expiry_m": exp})

        # --- E) end-of-day mark
        e = mark(m)
        equity[s] = e
        if trace is not None:
            snap = []
            for nm, sl in slots.items():
                sh = sl.get("shares", 0)
                if sh:
                    lp = insts[nm].lpos[m]
                    px = insts[nm].close[lp]
                    snap.append((nm, sh, px, sh * px, div_today.get(nm, 0.0)))
            trace.append(snap)
        occ[s] = len(slots)
        shr[s] = sum(1 for sl in slots.values() if sl.get("shares", 0) > 0)
        if cash < -1e-6:
            neg_cash_days += 1
        min_cash = min(min_cash, cash)
        assert e > 0, f"equity {e:.2f} <= 0 on {mdates[m]} ({variant} dd{entry_dd} p{max_pos})"

    return {"equity": equity, "occ": occ, "shr": shr, "ev": ev,
            "neg_cash_days": neg_cash_days, "min_cash": min_cash, "final_cash": cash,
            "open_slots_at_end": len(slots)}


# ---------------------------------------------------------------- metrics


def slice_metrics(sim_dates, equity, a, b):
    eq = equity[a:b + 1]
    rets = eq[1:] / eq[:-1] - 1.0
    total_ret = float(eq[-1] / eq[0] - 1.0)
    years = (sim_dates[b] - sim_dates[a]).days / 365.25
    cagr = float((eq[-1] / eq[0]) ** (1.0 / years) - 1.0) if years > 0 else 0.0
    if len(rets) > 1:
        sd = float(np.std(rets, ddof=1))
        sharpe = float(np.mean(rets)) / sd * math.sqrt(252) if sd > 0 else 0.0
    else:
        sharpe = 0.0
    peak = np.maximum.accumulate(eq)
    mdd = float(np.min(eq / peak - 1.0))
    return {"total_return": total_ret, "cagr": cagr, "sharpe": sharpe, "max_dd": mdd}


def count_in(lst, a, b):
    return bisect_right(lst, b) - bisect_left(lst, a)


def window_stats(res, a, b, max_pos):
    prem = sum(v for i, v in res["ev"]["prem"] if a <= i <= b)
    occ = res["occ"][a:b + 1]
    return {
        "net_premium": float(prem),
        "put_assignments": count_in(res["ev"]["assign"], a, b),
        "missed_csp": count_in(res["ev"]["missed"], a, b),
        "missed_csp_rebounded": count_in(res["ev"]["missed_reb"], a, b),
        "call_aways": count_in(res["ev"]["callaway"], a, b),
        "recovery_exits": count_in(res["ev"]["rexit"], a, b),
        "forced_liquidations": count_in(res["ev"]["forced"], a, b),
        "avg_positions": float(np.mean(occ)),
        "avg_share_slots": float(np.mean(res["shr"][a:b + 1])),
        "pct_days_fully_invested": float(np.mean(occ == max_pos)),
    }


# ---------------------------------------------------------------- main


def main():
    t0 = time.time()
    universe, raw = load_data()

    # master calendar = SPY trading days
    spy_dates = raw["SPY.US"][0]
    mdates = [d for d in spy_dates if HIST_START <= d <= END]
    mdate_to_idx = {d: i for i, d in enumerate(mdates)}
    M = len(mdates)

    insts = {}
    hygiene_rows = 0
    offcal_rows = 0
    hygiene_names = []
    dropped_names = []
    for nm in universe:
        if nm not in raw:
            continue
        ins, dr, oc = build_instrument(nm, raw[nm], mdate_to_idx, M)
        hygiene_rows += dr
        offcal_rows += oc
        if dr > 0:
            hygiene_names.append(nm)
        if ins is not None:
            insts[nm] = ins
        else:
            dropped_names.append(nm)
    # SPY's pre-1997 rows carry adjusted_close == close (missing adjustment factors)
    # and are trimmed by hygiene; the benchmark only needs 1998+ coverage, which the
    # NaN assert below enforces.
    spy_ins, dr, _ = build_instrument("SPY.US", raw["SPY.US"], mdate_to_idx, M)
    if dr:
        print(f"# note: SPY hygiene trimmed {dr} pre-1997 rows (adj==close segment)")
    del raw
    n_to_end = sum(1 for i in insts.values() if int(i.midx[-1]) >= M - 5)
    print(f"# instruments usable: {len(insts)}  (with data to window end: {n_to_end})")
    print(f"# data hygiene: {hygiene_rows:,} rows trimmed across {len(hygiene_names)} "
          f"instruments ({offcal_rows:,} rows off master calendar dropped); "
          f"dropped entirely: {dropped_names}")

    off = bisect_left(mdates, SIM_START)
    sim_dates = mdates[off:]
    Nd = len(sim_dates)

    # precompute candidate lists per sim day per threshold, sorted deepest-first
    cands = {dd: [[] for _ in range(Nd)] for dd in ENTRY_DDS}
    for nm, ins in insts.items():
        base = ins.entry_ok & (ins.midx >= off)
        for dd in ENTRY_DDS:
            with np.errstate(invalid="ignore"):
                mask = base & (ins.adj <= (1.0 - dd) * ins.hi126)
            for i in np.nonzero(mask)[0]:
                s = int(ins.midx[i]) - off
                cands[dd][s].append((-float(ins.depth[i]), nm))
    for dd in ENTRY_DDS:
        for lst in cands[dd]:
            lst.sort()
    print(f"# candidates precomputed  ({time.time()-t0:.1f}s); sim days {Nd} "
          f"{sim_dates[0]} .. {sim_dates[-1]}")

    # windows (index ranges into sim space)
    windows = [("FULL", 0, Nd - 1)]
    for label, lo, hi in ERAS:
        a = bisect_left(sim_dates, lo)
        b = bisect_right(sim_dates, hi) - 1
        if a <= b:
            windows.append((label, a, b))

    # benchmark: SPY buy-and-hold on adjusted_close
    spy_adj = np.full(Nd, np.nan)
    sel = spy_ins.midx >= off
    spy_adj[spy_ins.midx[sel] - off] = spy_ins.adj[sel]
    assert not np.isnan(spy_adj).any()
    bench_eq = INIT_CASH * spy_adj / spy_adj[0]

    results = {"meta": {
        "window": [str(sim_dates[0]), str(sim_dates[-1])],
        "init_cash": INIT_CASH,
        "universe": f"top {TOP_N} by 1998 avg close*volume (>=100 days), '%.US'",
        "n_instruments_usable": len(insts),
        "n_instruments_data_to_end": n_to_end,
        "data_hygiene": {
            "rows_trimmed": hygiene_rows,
            "instruments_trimmed": hygiene_names,
            "instruments_dropped": dropped_names,
            "rules": "longest clean segment; breaks = gap>45cd, any |ln adj_ret|>ln3, "
                     "or adj-vs-raw return disagreement >1.5x with adj move >1.5x",
        },
        "grid": {"entry_dd": list(ENTRY_DDS), "max_positions": list(MAX_POSITIONS)},
        "core": {"high_window": HIGH_WIN, "sma_window": SMA_WIN,
                 "exit_recovery": EXIT_RECOVERY, "min_hold_days": MIN_HOLD,
                 "cooldown_days": COOLDOWN, "invest_frac": INVEST_FRAC},
        "wheel": {"put_strike": PUT_K_RATIO, "call_strike": CALL_K_RATIO,
                  "dte_calendar": TARGET_DTE, "sigma": "max(0.15, realized63d*1.1)",
                  "rate": 0.0, "commission": COMMISSION, "premium_haircut": SLIPPAGE},
        "notes": [
            "decisions at close[t]; option trades priced at close[t]; share fills at open[t+1]",
            "equity = cash + reserved CSP cash + shares*raw_close - short intrinsic",
            "short options marked at intrinsic only (wheel Sharpe overstated in OTM phases)",
            "dividends credited from adj/raw drift while holding shares (both variants)",
            "splits: adj/raw return-ratio > 10% => share count + option terms adjusted",
            "survivorship-biased universe; volume is split-adjusted so 1998 ADV ranking tilts to future splitters",
            "missed_csp = CSP expired OTM (entry never became shares); no cooldown, name re-enters pool",
            "missed_csp_rebounded = subset where raw close at expiry > close at CSP entry",
            "pending-exit slots occupy until the open fill (no same-bar slot rotation)",
            "no position rebalancing after entry; cash earns 0",
            "MEASURED PREMIUM BIAS (audit, wheel_dd15_p5): sigma_used / realized vol over the "
            "option's own life = median 1.26, mean 1.43 -> premia are sold systematically rich "
            "because trailing 63d vol at dip entries exceeds subsequent vol (mean reversion) "
            "on top of the specified 1.1 factor; at ~4-10% premium per 30d trade this subsidy "
            "is worth very roughly ~1%/month of deployed notional and is the main driver of "
            "the wheel's outperformance. Real post-spike single-name IV is usually BELOW "
            "trailing realized. Wheel results are optimistic.",
            "recovery-exit call buy-backs at intrinsic forfeit remaining time value "
            "(small extra wheel subsidy; ~57 events per run)",
        ],
    }, "benchmark": {}, "runs": []}

    for label, a, b in windows:
        results["benchmark"][label] = slice_metrics(sim_dates, bench_eq, a, b)

    runs = {}
    for dd in ENTRY_DDS:
        for mp in MAX_POSITIONS:
            for variant in ("plain", "wheel"):
                t1 = time.time()
                res = run_sim(variant, dd, mp, insts, mdates, off, cands[dd])
                name = f"{variant}_dd{int(dd*100)}_p{mp}"
                entry = {"name": name, "variant": variant, "entry_dd": dd,
                         "max_positions": mp,
                         "final_equity": float(res["equity"][-1]),
                         "neg_cash_days": res["neg_cash_days"],
                         "min_cash": float(res["min_cash"]),
                         "windows": {}}
                for label, a, b in windows:
                    mtr = slice_metrics(sim_dates, res["equity"], a, b)
                    mtr.update(window_stats(res, a, b, mp))
                    entry["windows"][label] = mtr
                results["runs"].append(entry)
                runs[name] = entry
                print(f"# ran {name:<16} final ${res['equity'][-1]:>14,.0f}  "
                      f"neg-cash days {res['neg_cash_days']:>3}  "
                      f"min-cash {res['min_cash']:>12,.0f}  "
                      f"forced {len(res['ev']['forced']):>2}  ({time.time()-t1:.1f}s)")

    # ------------------------------------------------------------ table
    hdr = (f"{'strategy':<16} {'totret':>10} {'cagr':>7} {'sharpe':>7} {'maxDD':>7} "
           f"{'prem$':>12} {'assign':>6} {'miss':>5} {'callA':>5} {'avgPos':>6} {'%full':>6}")
    for label, a, b in windows:
        print(f"\n== {label} ({sim_dates[a]} .. {sim_dates[b]}) ==")
        print(hdr)
        bm = results["benchmark"][label]
        print(f"{'SPY_BUY_HOLD':<16} {bm['total_return']:>9.1%} {bm['cagr']:>7.2%} "
              f"{bm['sharpe']:>7.2f} {bm['max_dd']:>7.1%} {'-':>12} {'-':>6} {'-':>5} "
              f"{'-':>5} {'-':>6} {'-':>6}")
        for dd in ENTRY_DDS:
            for mp in MAX_POSITIONS:
                for variant in ("plain", "wheel"):
                    e = runs[f"{variant}_dd{int(dd*100)}_p{mp}"]
                    w = e["windows"][label]
                    print(f"{e['name']:<16} {w['total_return']:>9.1%} {w['cagr']:>7.2%} "
                          f"{w['sharpe']:>7.2f} {w['max_dd']:>7.1%} "
                          f"{w['net_premium']:>12,.0f} {w['put_assignments']:>6} "
                          f"{w['missed_csp']:>5} {w['call_aways']:>5} "
                          f"{w['avg_positions']:>6.2f} {w['pct_days_fully_invested']:>6.1%}")

    out_path = os.path.join(SCRATCH, "dip_wheel_results.json")
    with open(out_path, "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"\n# results -> {out_path}")

    # per-era verdict: does the wheel overlay improve CAGR / Sharpe?
    print("\n# Wheel overlay vs dip-plain (same combo), per window:")
    for label, _, _ in windows:
        wins_c = wins_s = 0
        for dd in ENTRY_DDS:
            for mp in MAX_POSITIONS:
                p = runs[f"plain_dd{int(dd*100)}_p{mp}"]["windows"][label]
                w = runs[f"wheel_dd{int(dd*100)}_p{mp}"]["windows"][label]
                wins_c += w["cagr"] > p["cagr"]
                wins_s += w["sharpe"] > p["sharpe"]
        print(f"#   {label}: wheel higher CAGR in {wins_c}/4 combos, "
              f"higher Sharpe in {wins_s}/4")
    print(f"# total {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
