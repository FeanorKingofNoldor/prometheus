"""Compare three market-situation classification approaches.

Approach A: Fixed state machine on existing regime labels + fragility scores.
Approach B: Price-only Market Health Index (MHI) from SPY + universe breadth.
Approach C: Hybrid — MHI primary, fragility as early-warning override.

Outputs per-approach: daily state label, days per state, transition timing
around known events (GFC, COVID, 2022 rate shock, etc.).

Usage:
  PYTHONPATH=cpp/build ./venv/bin/python -m prometheus.scripts.experiments.compare_market_situations
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

logger = get_logger(__name__)

# ── Known events for spot-check ─────────────────────────────────────
EVENTS = {
    "GFC_start": (date(2007, 10, 9), date(2009, 3, 9)),
    "GFC_recovery": (date(2009, 3, 10), date(2010, 4, 23)),
    "EU_debt": (date(2011, 7, 22), date(2011, 10, 3)),
    "China_deval": (date(2015, 8, 18), date(2016, 2, 11)),
    "Volmageddon": (date(2018, 1, 26), date(2018, 2, 8)),
    "Q4_2018": (date(2018, 10, 3), date(2018, 12, 24)),
    "COVID_crash": (date(2020, 2, 19), date(2020, 3, 23)),
    "COVID_recovery": (date(2020, 3, 24), date(2020, 8, 18)),
    "Rate_shock_2022": (date(2022, 1, 3), date(2022, 10, 12)),
}


# ── Data loading ─────────────────────────────────────────────────────

def load_spy_prices(db) -> Dict[date, float]:
    """Return {date: close} for SPY."""
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT trade_date, close
            FROM prices_daily
            WHERE instrument_id = 'SPY.US'
              AND trade_date BETWEEN '2005-01-01' AND '2025-12-31'
            ORDER BY trade_date
        """)
        rows = cur.fetchall()
        cur.close()
    return {d: float(c) for d, c in rows}


def load_universe_closes(db) -> Dict[date, List[float]]:
    """Return {date: [close, close, ...]} for all US_EQ instruments.

    We load in chunks to avoid massive memory; we only need the close
    per instrument per day for breadth computation.
    """
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT instrument_id FROM instruments
            WHERE market_id = 'US_EQ' AND status = 'ACTIVE'
        """)
        inst_ids = [r[0] for r in cur.fetchall()]
        cur.close()

    # Load all closes for these instruments in one query, grouped by date.
    # For breadth we need: per instrument, close on day T and close 21d/50d ago.
    # Strategy: load full close matrix as dict of dicts.
    logger.info("Loading closes for %d instruments...", len(inst_ids))
    inst_closes: Dict[str, Dict[date, float]] = defaultdict(dict)
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT instrument_id, trade_date, close
            FROM prices_daily
            WHERE instrument_id = ANY(%s)
              AND trade_date BETWEEN '2005-01-01' AND '2025-12-31'
              AND close > 0
            ORDER BY trade_date
        """, (inst_ids,))
        for iid, td, c in cur:
            inst_closes[iid][td] = float(c)
        cur.close()
    logger.info("Loaded closes for %d instruments", len(inst_closes))
    return inst_closes


def load_regimes(db) -> Dict[date, str]:
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (as_of_date) as_of_date, regime_label
            FROM regimes
            WHERE region = 'US'
              AND as_of_date BETWEEN '2005-01-01' AND '2025-12-31'
            ORDER BY as_of_date, created_at DESC
        """)
        rows = cur.fetchall()
        cur.close()
    return {d: lbl.upper() for d, lbl in rows}


def load_fragility(db) -> Dict[date, float]:
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (as_of_date) as_of_date, fragility_score
            FROM fragility_measures
            WHERE entity_type = 'MARKET' AND entity_id = 'US_EQ'
              AND as_of_date BETWEEN '2005-01-01' AND '2025-12-31'
            ORDER BY as_of_date, created_at DESC
        """)
        rows = cur.fetchall()
        cur.close()
    return {d: float(f) for d, f in rows}


# ── Signal computation helpers ───────────────────────────────────────

def sma(arr: np.ndarray, window: int) -> np.ndarray:
    """Simple moving average; first (window-1) values are NaN."""
    out = np.full_like(arr, np.nan, dtype=float)
    if len(arr) < window:
        return out
    cs = np.cumsum(arr)
    out[window - 1:] = (cs[window - 1:] - np.concatenate([[0.0], cs[:-window]])) / window
    return out


def rolling_std(arr: np.ndarray, window: int) -> np.ndarray:
    """Rolling standard deviation."""
    out = np.full_like(arr, np.nan, dtype=float)
    for i in range(window - 1, len(arr)):
        out[i] = np.std(arr[i - window + 1: i + 1], ddof=1)
    return out


def rolling_max(arr: np.ndarray, window: int) -> np.ndarray:
    out = np.full_like(arr, np.nan, dtype=float)
    for i in range(window - 1, len(arr)):
        out[i] = np.max(arr[i - window + 1: i + 1])
    return out


def percentile_rank(arr: np.ndarray, window: int) -> np.ndarray:
    """Percentile rank of current value within trailing window."""
    out = np.full_like(arr, np.nan, dtype=float)
    for i in range(window - 1, len(arr)):
        segment = arr[i - window + 1: i + 1]
        valid = segment[~np.isnan(segment)]
        if len(valid) < 2:
            continue
        out[i] = np.sum(valid <= arr[i]) / len(valid)
    return out


# ── Approach A: State machine on regime + fragility ──────────────────

@dataclass
class StateA:
    state: str = "NEUTRAL"
    days_in_state: int = 0
    crisis_exit_counter: int = 0  # consecutive days meeting exit condition

    # Thresholds (calibrated to actual data distribution).
    crisis_frag: float = 0.55
    crisis_regime: str = "CRISIS"
    crisis_exit_frag: float = 0.40
    crisis_exit_days: int = 10
    recovery_min_days: int = 60
    riskoff_frag: float = 0.45
    riskon_frag: float = 0.20


def step_a(st: StateA, regime: Optional[str], frag: Optional[float]) -> str:
    """Advance one day, return new state label."""
    r = (regime or "").upper()
    f = frag if frag is not None else 0.30  # neutral default

    # Crisis entry — instant, from any state
    crisis_trigger = (f >= st.crisis_frag) or (r == st.crisis_regime)
    if crisis_trigger and st.state != "CRISIS":
        st.state = "CRISIS"
        st.days_in_state = 0
        st.crisis_exit_counter = 0
    
    st.days_in_state += 1

    if st.state == "CRISIS":
        # Check exit conditions
        if f < st.crisis_exit_frag:
            st.crisis_exit_counter += 1
        else:
            st.crisis_exit_counter = 0
        if st.crisis_exit_counter >= st.crisis_exit_days:
            st.state = "RECOVERY"
            st.days_in_state = 0
            st.crisis_exit_counter = 0
        return st.state

    if st.state == "RECOVERY":
        # Can snap back to crisis
        if crisis_trigger:
            st.state = "CRISIS"
            st.days_in_state = 0
            st.crisis_exit_counter = 0
            return st.state
        # Minimum duration
        if st.days_in_state >= st.recovery_min_days:
            # Transition out
            if f >= st.riskoff_frag or r == "RISK_OFF":
                st.state = "RISK_OFF"
            elif r == "CARRY" and f < st.riskon_frag:
                st.state = "RISK_ON"
            else:
                st.state = "NEUTRAL"
            st.days_in_state = 0
        return st.state

    # Normal states — can still enter crisis (handled above)
    if f >= st.riskoff_frag or r == "RISK_OFF":
        new = "RISK_OFF"
    elif r == "CARRY" and f < st.riskon_frag:
        new = "RISK_ON"
    else:
        new = "NEUTRAL"

    if new != st.state:
        st.state = new
        st.days_in_state = 0
    return st.state


# ── Approach B: Price-only MHI ───────────────────────────────────────

@dataclass
class StateB:
    state: str = "NEUTRAL"
    days_in_state: int = 0
    crisis_exit_counter: int = 0
    recovery_day: int = 0

    # MHI thresholds for state mapping
    crisis_threshold: float = -0.5
    riskoff_threshold: float = -0.1
    neutral_threshold: float = 0.3  # above this = RISK_ON

    # Instant crisis overrides
    shock_1d_ret: float = -0.04  # SPY 1-day return <= this
    shock_dd: float = -0.08       # drawdown <= this

    # Exit / recovery params
    crisis_exit_mhi: float = -0.2
    crisis_exit_days: int = 10
    recovery_min_days: int = 40


def compute_mhi_series(
    dates: List[date],
    spy_close: np.ndarray,
    breadth_21d: np.ndarray,
    breadth_50d_sma: np.ndarray,
) -> np.ndarray:
    """Compute daily MHI from SPY and breadth arrays (aligned to dates)."""
    n = len(dates)
    mhi = np.full(n, np.nan)

    # SPY-derived signals
    spy_sma200 = sma(spy_close, 200)
    spy_ret_1d = np.zeros(n)
    spy_ret_1d[1:] = spy_close[1:] / spy_close[:-1] - 1.0

    # Momentum: blended 1m/3m/6m returns
    spy_ret_21d = np.full(n, np.nan)
    spy_ret_63d = np.full(n, np.nan)
    spy_ret_126d = np.full(n, np.nan)
    for i in range(21, n):
        if spy_close[i - 21] > 0:
            spy_ret_21d[i] = spy_close[i] / spy_close[i - 21] - 1.0
    for i in range(63, n):
        if spy_close[i - 63] > 0:
            spy_ret_63d[i] = spy_close[i] / spy_close[i - 63] - 1.0
    for i in range(126, n):
        if spy_close[i - 126] > 0:
            spy_ret_126d[i] = spy_close[i] / spy_close[i - 126] - 1.0

    # Realized vol (21d, annualized)
    log_ret = np.zeros(n)
    log_ret[1:] = np.log(spy_close[1:] / spy_close[:-1])
    rvol_21d = rolling_std(log_ret, 21) * math.sqrt(252)
    rvol_pctile = percentile_rank(rvol_21d, 252)

    # Drawdown from 252d rolling high
    rolling_high_252 = rolling_max(spy_close, 252)
    dd = np.where(rolling_high_252 > 0, spy_close / rolling_high_252 - 1.0, 0.0)

    for i in range(252, n):
        # 1) Trend score: SPY / SMA200
        if np.isnan(spy_sma200[i]) or spy_sma200[i] <= 0:
            continue
        ratio = spy_close[i] / spy_sma200[i]
        trend_score = max(-1.0, min(1.0, (ratio - 0.95) / 0.10))

        # 2) Momentum score (blend of 1m/3m/6m)
        m1 = spy_ret_21d[i] if not np.isnan(spy_ret_21d[i]) else 0.0
        m3 = spy_ret_63d[i] if not np.isnan(spy_ret_63d[i]) else 0.0
        m6 = spy_ret_126d[i] if not np.isnan(spy_ret_126d[i]) else 0.0
        mom_blend = 0.4 * m1 + 0.35 * m3 + 0.25 * m6
        momentum_score = max(-1.0, min(1.0, mom_blend / 0.15))

        # 3) Vol score (low vol = positive)
        vp = rvol_pctile[i] if not np.isnan(rvol_pctile[i]) else 0.5
        vol_score = 1.0 - 2.0 * vp

        # 4) Drawdown score
        d = dd[i]
        dd_score = max(-1.0, min(0.0, d / 0.20)) * 2.0 + 1.0
        # Maps: dd=0 → +1, dd=-0.10 → 0, dd=-0.20 → -1

        # 5) Breadth score
        b21 = breadth_21d[i] if not np.isnan(breadth_21d[i]) else 0.5
        b50 = breadth_50d_sma[i] if not np.isnan(breadth_50d_sma[i]) else 0.5
        breadth_blend = 0.5 * b21 + 0.5 * b50
        breadth_score = 2.0 * breadth_blend - 1.0

        # MHI (equal weight for now)
        mhi[i] = 0.20 * trend_score + 0.20 * momentum_score + 0.20 * vol_score + 0.20 * dd_score + 0.20 * breadth_score

    return mhi, spy_ret_1d, dd


def step_b(st: StateB, mhi: float, spy_ret_1d: float, dd: float) -> str:
    """Advance one day with MHI, return state label."""
    if np.isnan(mhi):
        return st.state

    # Instant crisis triggers
    crisis_trigger = (mhi <= st.crisis_threshold) or \
                     (spy_ret_1d <= st.shock_1d_ret) or \
                     (dd <= st.shock_dd)

    if crisis_trigger and st.state != "CRISIS":
        st.state = "CRISIS"
        st.days_in_state = 0
        st.crisis_exit_counter = 0

    st.days_in_state += 1

    if st.state == "CRISIS":
        if mhi > st.crisis_exit_mhi:
            st.crisis_exit_counter += 1
        else:
            st.crisis_exit_counter = 0
        if st.crisis_exit_counter >= st.crisis_exit_days:
            st.state = "RECOVERY"
            st.days_in_state = 0
            st.recovery_day = 0
        return st.state

    if st.state == "RECOVERY":
        st.recovery_day += 1
        if crisis_trigger:
            st.state = "CRISIS"
            st.days_in_state = 0
            st.crisis_exit_counter = 0
            return st.state
        if st.recovery_day >= st.recovery_min_days:
            if mhi >= st.neutral_threshold:
                st.state = "RISK_ON"
            elif mhi >= st.riskoff_threshold:
                st.state = "NEUTRAL"
            else:
                st.state = "STRESS"
            st.days_in_state = 0
        return st.state

    # Normal states
    if mhi <= st.riskoff_threshold:
        new = "STRESS"
    elif mhi >= st.neutral_threshold:
        new = "RISK_ON"
    else:
        new = "NEUTRAL"

    if new != st.state:
        st.state = new
        st.days_in_state = 0
    return st.state


# ── Approach C: Hybrid (MHI primary, fragility early-warning) ────────

@dataclass
class StateC:
    state: str = "NEUTRAL"
    days_in_state: int = 0
    crisis_exit_counter: int = 0
    recovery_day: int = 0

    # Same MHI thresholds as B
    crisis_threshold: float = -0.5
    riskoff_threshold: float = -0.1
    neutral_threshold: float = 0.3

    shock_1d_ret: float = -0.04
    shock_dd: float = -0.08
    crisis_exit_mhi: float = -0.2
    crisis_exit_days: int = 10
    recovery_min_days: int = 40

    # Fragility overrides
    frag_crisis: float = 0.55        # fragility alone can trigger crisis
    frag_accelerate: float = 0.45    # fragility can push MHI thresholds stricter
    frag_exit_block: float = 0.42    # block crisis exit if frag still elevated


def step_c(st: StateC, mhi: float, spy_ret_1d: float, dd: float, frag: Optional[float]) -> str:
    """Hybrid: MHI + fragility early-warning."""
    if np.isnan(mhi):
        return st.state

    f = frag if frag is not None else 0.30

    # Crisis triggers: MHI-based OR fragility spike OR shock
    crisis_trigger = (mhi <= st.crisis_threshold) or \
                     (spy_ret_1d <= st.shock_1d_ret) or \
                     (dd <= st.shock_dd) or \
                     (f >= st.frag_crisis)

    # Fragility can also accelerate: if frag elevated, use stricter MHI threshold
    if f >= st.frag_accelerate:
        crisis_trigger = crisis_trigger or (mhi <= st.crisis_threshold + 0.15)

    if crisis_trigger and st.state != "CRISIS":
        st.state = "CRISIS"
        st.days_in_state = 0
        st.crisis_exit_counter = 0

    st.days_in_state += 1

    if st.state == "CRISIS":
        # Exit requires MHI recovery AND fragility below block threshold
        exit_ok = (mhi > st.crisis_exit_mhi) and (f < st.frag_exit_block)
        if exit_ok:
            st.crisis_exit_counter += 1
        else:
            st.crisis_exit_counter = 0
        if st.crisis_exit_counter >= st.crisis_exit_days:
            st.state = "RECOVERY"
            st.days_in_state = 0
            st.recovery_day = 0
        return st.state

    if st.state == "RECOVERY":
        st.recovery_day += 1
        if crisis_trigger:
            st.state = "CRISIS"
            st.days_in_state = 0
            st.crisis_exit_counter = 0
            return st.state
        if st.recovery_day >= st.recovery_min_days:
            if mhi >= st.neutral_threshold:
                st.state = "RISK_ON"
            elif mhi >= st.riskoff_threshold:
                st.state = "NEUTRAL"
            else:
                st.state = "STRESS"
            st.days_in_state = 0
        return st.state

    # Normal — fragility can push into STRESS earlier
    effective_riskoff = st.riskoff_threshold
    if f >= st.frag_accelerate:
        effective_riskoff = st.riskoff_threshold + 0.15  # easier to trigger stress

    if mhi <= effective_riskoff:
        new = "STRESS"
    elif mhi >= st.neutral_threshold:
        new = "RISK_ON"
    else:
        new = "NEUTRAL"

    if new != st.state:
        st.state = new
        st.days_in_state = 0
    return st.state


# ── Main comparison ──────────────────────────────────────────────────

def main():
    db = get_db_manager()

    logger.info("Loading data...")
    spy_prices = load_spy_prices(db)
    inst_closes = load_universe_closes(db)
    regime_map = load_regimes(db)
    frag_map = load_fragility(db)

    # Build aligned date array from SPY
    all_dates = sorted(spy_prices.keys())
    # Filter to 2006+ for comparison window (need lookback for 200d SMA etc.)
    dates = [d for d in all_dates if d >= date(2006, 1, 3)]
    n = len(dates)
    logger.info("Comparison window: %s to %s (%d trading days)", dates[0], dates[-1], n)

    spy_close = np.array([spy_prices[d] for d in dates])

    # Compute breadth: fraction with positive 21d return, fraction above 50d SMA
    logger.info("Computing breadth...")
    breadth_21d = np.full(n, np.nan)
    breadth_50d_sma = np.full(n, np.nan)

    # Build per-date arrays for breadth
    # For each instrument, get closes on each date, then compute 21d return and 50d SMA
    for i, d in enumerate(dates):
        if i < 50:
            continue  # not enough lookback

        d_21 = dates[i - 21] if i >= 21 else None
        pos_count_21 = 0
        above_sma50_count = 0
        total_count = 0

        for iid, closes_dict in inst_closes.items():
            c_today = closes_dict.get(d)
            if c_today is None or c_today <= 0:
                continue
            total_count += 1

            # 21d return
            if d_21 is not None:
                c_21 = closes_dict.get(d_21)
                if c_21 is not None and c_21 > 0:
                    if c_today / c_21 > 1.0:
                        pos_count_21 += 1

            # 50d SMA: approximate — check if current close > average of last 50 available
            # We'll use a simpler check: just the last 50 calendar-day aligned dates
            sma_vals = []
            for j in range(max(0, i - 49), i + 1):
                c_j = closes_dict.get(dates[j])
                if c_j is not None and c_j > 0:
                    sma_vals.append(c_j)
            if len(sma_vals) >= 30:
                avg = sum(sma_vals) / len(sma_vals)
                if c_today > avg:
                    above_sma50_count += 1

        if total_count > 0:
            breadth_21d[i] = pos_count_21 / total_count
            breadth_50d_sma[i] = above_sma50_count / total_count

    logger.info("Computing MHI series...")
    mhi, spy_ret_1d, dd = compute_mhi_series(dates, spy_close, breadth_21d, breadth_50d_sma)

    # Forward-fill regime and fragility to daily
    def ffill(mapping, dates):
        out = [None] * len(dates)
        last = None
        for i, d in enumerate(dates):
            if d in mapping:
                last = mapping[d]
            out[i] = last
        return out

    regime_daily = ffill(regime_map, dates)
    frag_daily = ffill(frag_map, dates)

    # ── Run all three approaches ─────────────────────────────────────
    logger.info("Running classifiers...")
    st_a = StateA()
    st_b = StateB()
    st_c = StateC()

    labels_a = []
    labels_b = []
    labels_c = []

    for i in range(n):
        r = regime_daily[i]
        f = frag_daily[i]
        m = mhi[i]
        ret1d = spy_ret_1d[i]
        d_dd = dd[i]

        la = step_a(st_a, r, f)
        lb = step_b(st_b, m, ret1d, d_dd)
        lc = step_c(st_c, m, ret1d, d_dd, f)

        labels_a.append(la)
        labels_b.append(lb)
        labels_c.append(lc)

    # ── Summary statistics ───────────────────────────────────────────
    print("\n" + "=" * 80)
    print("MARKET SITUATION CLASSIFIER COMPARISON")
    print(f"Window: {dates[0]} to {dates[-1]} ({n} trading days)")
    print("=" * 80)

    for name, labels in [("A (regime+frag SM)", labels_a),
                          ("B (price-only MHI)", labels_b),
                          ("C (hybrid MHI+frag)", labels_c)]:
        ctr = Counter(labels)
        total = sum(ctr.values())
        print(f"\n── Approach {name} ──")
        for state in ["CRISIS", "RECOVERY", "STRESS", "RISK_OFF", "NEUTRAL", "RISK_ON"]:
            c = ctr.get(state, 0)
            print(f"  {state:12s}: {c:5d} days ({100 * c / total:5.1f}%)")

        # Count transitions
        switches = sum(1 for a, b in zip(labels, labels[1:]) if a != b)
        print(f"  Transitions: {switches}")

    # ── Event-level detail ───────────────────────────────────────────
    print("\n" + "=" * 80)
    print("EVENT-LEVEL ANALYSIS")
    print("=" * 80)

    for event_name, (ev_start, ev_end) in EVENTS.items():
        # Find indices for the event window + some context
        ctx_start = ev_start - timedelta(days=30)
        ctx_end = ev_end + timedelta(days=60)

        indices = [(i, d) for i, d in enumerate(dates) if ctx_start <= d <= ctx_end]
        if not indices:
            print(f"\n  {event_name}: no data in window")
            continue

        print(f"\n── {event_name} ({ev_start} to {ev_end}) ──")

        # Show state at key moments: event start, worst point, event end
        def find_idx(target):
            for i, d in enumerate(dates):
                if d >= target:
                    return i
            return None

        idx_start = find_idx(ev_start)
        idx_end = find_idx(ev_end)

        if idx_start is not None and idx_end is not None:
            # SPY drawdown during event
            spy_peak = max(spy_close[max(0, idx_start - 252):idx_start + 1])
            spy_trough = min(spy_close[idx_start:min(n, idx_end + 1)])
            ev_dd = (spy_trough / spy_peak - 1.0) * 100

            print(f"  SPY peak-to-trough during event: {ev_dd:.1f}%")

            # State distribution during event
            for name, labels in [("A", labels_a), ("B", labels_b), ("C", labels_c)]:
                event_labels = labels[idx_start:idx_end + 1]
                ctr = Counter(event_labels)
                parts = ", ".join(f"{s}:{c}" for s, c in ctr.most_common())
                print(f"  {name}: {parts}")

            # When did each approach first enter CRISIS?
            for name, labels in [("A", labels_a), ("B", labels_b), ("C", labels_c)]:
                first_crisis = None
                # Look from 30 days before event start
                search_start = max(0, idx_start - 30)
                for j in range(search_start, min(n, idx_end + 60)):
                    if labels[j] == "CRISIS":
                        first_crisis = dates[j]
                        break
                if first_crisis:
                    delta = (first_crisis - ev_start).days
                    print(f"    {name} first CRISIS: {first_crisis} (event_start{'+' if delta >= 0 else ''}{delta}d)")
                else:
                    print(f"    {name} first CRISIS: never in window")

    # ── MHI values at key dates ──────────────────────────────────────
    print("\n" + "=" * 80)
    print("MHI VALUES AT KEY DATES")
    print("=" * 80)

    key_dates = [
        date(2007, 10, 9),   # GFC market peak
        date(2008, 9, 15),   # Lehman
        date(2008, 11, 20),  # GFC bottom area
        date(2009, 3, 9),    # GFC trough
        date(2009, 6, 1),    # early recovery
        date(2020, 2, 19),   # COVID peak
        date(2020, 3, 16),   # COVID crash week
        date(2020, 3, 23),   # COVID trough
        date(2020, 5, 1),    # COVID recovery
        date(2022, 1, 3),    # rate shock start
        date(2022, 6, 16),   # rate shock mid
        date(2022, 10, 12),  # rate shock trough
    ]

    for kd in key_dates:
        idx = find_idx(kd)
        if idx is None:
            continue
        m_val = mhi[idx] if not np.isnan(mhi[idx]) else "N/A"
        f_val = frag_daily[idx]
        r_val = regime_daily[idx] or "N/A"
        b21 = breadth_21d[idx] if not np.isnan(breadth_21d[idx]) else "N/A"
        spy_dd = dd[idx] * 100 if not np.isnan(dd[idx]) else "N/A"
        print(f"  {kd}  MHI={m_val:>6.3f}  frag={f_val or 'N/A':>5}  regime={r_val:>10}  "
              f"breadth21={b21:>5.2f}  SPY_DD={spy_dd:>6.1f}%  "
              f"A={labels_a[idx]:>10}  B={labels_b[idx]:>10}  C={labels_c[idx]:>10}") if isinstance(m_val, float) else \
        print(f"  {kd}  MHI={m_val}  frag={f_val or 'N/A'}  regime={r_val}  "
              f"A={labels_a[idx]}  B={labels_b[idx]}  C={labels_c[idx]}")


if __name__ == "__main__":
    main()
