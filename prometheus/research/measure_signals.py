"""Prometheus — DB-backed signal builders for the harness evidence run.

Point-in-time signal constructors used by ``scripts/run/run_signal_evidence.py``
to MEASURE whether any in-house or standard signal carries real IC:

- ``forward_indicator_stress_series`` — reconstructs the apatheon forward-
  indicator macro/regime aggregate stress from FRED history (Bucket 1,
  replayable: each indicator is a trailing function of FRED series), as a single
  number per date for a TIME-SERIES IC vs forward SPY returns.
- ``market_forward_returns`` — forward return + forward realized vol of a market
  index (default SPY.US) indexed by date, for the macro timing test.
- ``soft_target_fragility_signal`` — reads the stored, point-in-time-clean
  soft-target score (trailing vol+drawdown+trend of prices) as a cross-sectional
  signal; tests the claim "fragility predicts negative returns".
- ``short_term_reversal_signal`` — standard 1-week reversal (negative of last
  5d return) from prices_daily.
- ``low_vol_signal`` — standard low-volatility factor (negative of trailing
  realized vol) from prices_daily.

All are computed from data strictly up to ``as_of_date`` => no look-ahead.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
from apatheon.core.logging import get_logger

from prometheus.research.signal_harness import _load_prices

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Macro / regime forward-indicator stress (Bucket 1, time-series signal)
# ---------------------------------------------------------------------------


def _load_fred(db_manager, series_id: str, start: date, end: date) -> pd.Series:
    """Load one FRED series from nation_macro_indicators as a date-indexed Series."""
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT observation_date, value
                FROM nation_macro_indicators
                WHERE series_id = %s AND observation_date BETWEEN %s AND %s
                ORDER BY observation_date
                """,
                (series_id, start, end),
            )
            rows = cur.fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series(
        {pd.Timestamp(d): float(v) for d, v in rows}, dtype=float
    ).sort_index()
    return s


def _rolling_z(s: pd.Series, window: int = 252) -> pd.Series:
    """Trailing z-score (mean/std over trailing window, excluding current point)."""
    mean = s.rolling(window, min_periods=20).mean().shift(1)
    std = s.rolling(window, min_periods=20).std(ddof=1).shift(1)
    return (s - mean) / std.replace(0.0, np.nan)


def forward_indicator_stress_series(
    db_manager,
    start: date,
    end: date,
) -> pd.Series:
    """Reconstruct the forward-indicator aggregate stress as a daily series.

    Faithful to ``apatheon.regime.forward_indicators``: each component is a
    trailing z-score of a FRED stress series (higher = more stress), inverted
    where the source falls under stress (yield curve, real yield direction is
    kept as-is per the module's sign convention). The aggregate is the mean of
    the available component z-scores per day — a single point-in-time stress
    reading. Bucket 1: every value uses only FRED data up to that date.

    Returns a date-indexed Series of the aggregate stress z.
    """
    load_start = start - timedelta(days=400)
    # (series_id, invert) — invert=True means "lower value = more stress"
    components = [
        ("BAMLH0A0HYM2", False),  # HY OAS: higher = stress
        ("T10Y2Y", True),         # 2-10 curve: lower/inverted = stress
        ("DFII10", False),        # real yield: higher = tightening = stress
        ("DGS10", False),         # 10y nominal: higher = stress
        ("VIXCLS", False),        # VIX: higher = stress
    ]
    zs: List[pd.Series] = []
    for series_id, invert in components:
        raw = _load_fred(db_manager, series_id, load_start, end)
        if raw.empty:
            continue
        z = _rolling_z(raw)
        if invert:
            z = -z
        zs.append(z.rename(series_id))
    if not zs:
        return pd.Series(dtype=float)
    # daily index: forward-fill each component (FRED series have gaps/holidays),
    # then average available components per day.
    panel = pd.concat(zs, axis=1).sort_index()
    panel = panel.reindex(pd.date_range(panel.index.min(), panel.index.max(), freq="D"))
    panel = panel.ffill(limit=7)  # carry a stale reading at most a week
    stress = panel.mean(axis=1, skipna=True)
    stress = stress[(stress.index >= pd.Timestamp(start)) & (stress.index <= pd.Timestamp(end))]
    return stress.dropna()


def market_forward_returns(
    db_manager,
    start: date,
    end: date,
    *,
    index_id: str = "SPY.US",
    horizons: Sequence[int] = (5, 21, 63),
) -> pd.DataFrame:
    """Forward return + forward realized vol of a market index, indexed by date.

    For each trading day t and horizon h: fwd_ret_{h}d = px[t+h]/px[t]-1 and
    fwd_vol_{h}d = annualized std of daily returns over (t, t+h]. Strictly
    forward (no look-ahead).
    """
    max_h = max(horizons)
    end_pad = end + timedelta(days=int(max_h * 2 + 21))
    px = _load_prices(db_manager, [index_id], start, end_pad)
    if px.empty:
        return pd.DataFrame()
    px = px[px["instrument_id"] == index_id].sort_values("trade_date").reset_index(drop=True)
    p = px["px"].to_numpy()
    dts = px["trade_date"].to_numpy()
    daily = np.concatenate([[np.nan], p[1:] / p[:-1] - 1.0])
    rows: List[dict] = []
    for i in range(len(p)):
        if dts[i] > np.datetime64(pd.Timestamp(end)):
            continue
        row = {"as_of_date": pd.Timestamp(dts[i])}
        for h in horizons:
            j = i + h
            if j < len(p) and p[i] > 0:
                row[f"fwd_ret_{h}d"] = float(p[j] / p[i] - 1.0)
                w = daily[i + 1 : j + 1]
                w = w[np.isfinite(w)]
                row[f"fwd_vol_{h}d"] = (
                    float(np.std(w, ddof=1) * np.sqrt(252.0)) if w.size >= 2 else np.nan
                )
            else:
                row[f"fwd_ret_{h}d"] = np.nan
                row[f"fwd_vol_{h}d"] = np.nan
        rows.append(row)
    out = pd.DataFrame(rows).set_index("as_of_date").sort_index()
    return out


# ---------------------------------------------------------------------------
# STAB / soft-target fragility (stored, point-in-time clean)
# ---------------------------------------------------------------------------


def soft_target_fragility_signal(
    db_manager,
    start: date,
    end: date,
    *,
    universe: Optional[Sequence[str]] = None,
    sample_every: int = 5,
    min_universe: int = 30,
) -> pd.DataFrame:
    """Cross-sectional soft-target (fragility) score from stored history.

    Reads ``soft_target_classes.soft_target_score`` for INSTRUMENT rows. The
    stored score is a deterministic trailing function of prices (realized vol +
    drawdown + negative trend over a 63d window up to as_of_date — see
    ``apatheon.stability.model_basic``), so it is point-in-time clean and
    replayable despite being backfilled.

    Returns tidy frame instrument_id, as_of_date, score. Score sign convention:
    HIGH = MORE fragile. The claim under test is "high fragility => negative
    forward return".
    """
    sql = """
        SELECT entity_id, as_of_date, soft_target_score
        FROM soft_target_classes
        WHERE entity_type = 'INSTRUMENT'
          AND as_of_date BETWEEN %s AND %s
          AND entity_id LIKE '%%.US'
          AND soft_target_score IS NOT NULL
    """
    params: List = [start, end]
    if universe is not None:
        sql += " AND entity_id = ANY(%s)"
        params.append(list(universe))
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])
    df = pd.DataFrame(rows, columns=["instrument_id", "as_of_date", "score"])
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    df["score"] = df["score"].astype(float)

    all_dates = sorted(df["as_of_date"].unique())
    keep = set(all_dates[::sample_every])
    df = df[df["as_of_date"].isin(keep)]
    counts = df.groupby("as_of_date")["instrument_id"].transform("count")
    df = df[counts >= min_universe].reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Standard benchmark factors (Bucket 1)
# ---------------------------------------------------------------------------


def short_term_reversal_signal(
    db_manager,
    start: date,
    end: date,
    *,
    lookback: int = 5,
    universe: Optional[Sequence[str]] = None,
    sample_every: int = 5,
    min_universe: int = 30,
) -> pd.DataFrame:
    """1-week reversal: score = -(px[t]/px[t-lookback] - 1). High score = recent loser."""
    load_start = start - timedelta(days=int(lookback * 3 + 30))
    prices = _load_prices(db_manager, universe, load_start, end)
    if prices.empty:
        return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])
    rows: List[dict] = []
    for inst, g in prices.groupby("instrument_id"):
        g = g.reset_index(drop=True)
        px = g["px"].to_numpy()
        dts = g["trade_date"].to_numpy()
        for i in range(lookback, len(px)):
            as_of = dts[i]
            if not (np.datetime64(start) <= as_of <= np.datetime64(end)):
                continue
            base = px[i - lookback]
            if base > 0 and px[i] > 0:
                rows.append({"instrument_id": inst, "as_of_date": pd.Timestamp(as_of),
                             "score": float(-(px[i] / base - 1.0))})
    return _finalize(rows, sample_every, min_universe)


def low_vol_signal(
    db_manager,
    start: date,
    end: date,
    *,
    window: int = 63,
    universe: Optional[Sequence[str]] = None,
    sample_every: int = 5,
    min_universe: int = 30,
) -> pd.DataFrame:
    """Low-volatility factor: score = -(trailing realized vol over `window`). High = calm."""
    load_start = start - timedelta(days=int(window * 3 + 30))
    prices = _load_prices(db_manager, universe, load_start, end)
    if prices.empty:
        return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])
    rows: List[dict] = []
    for inst, g in prices.groupby("instrument_id"):
        g = g.reset_index(drop=True)
        px = g["px"].to_numpy()
        dts = g["trade_date"].to_numpy()
        ret = np.concatenate([[np.nan], px[1:] / px[:-1] - 1.0])
        for i in range(window, len(px)):
            as_of = dts[i]
            if not (np.datetime64(start) <= as_of <= np.datetime64(end)):
                continue
            w = ret[i - window + 1 : i + 1]
            w = w[np.isfinite(w)]
            if w.size >= window // 2:
                rows.append({"instrument_id": inst, "as_of_date": pd.Timestamp(as_of),
                             "score": float(-np.std(w, ddof=1))})
    return _finalize(rows, sample_every, min_universe)


def _finalize(rows: List[dict], sample_every: int, min_universe: int) -> pd.DataFrame:
    sig = pd.DataFrame(rows)
    if sig.empty:
        return sig
    all_dates = sorted(sig["as_of_date"].unique())
    keep = set(all_dates[::sample_every])
    sig = sig[sig["as_of_date"].isin(keep)]
    counts = sig.groupby("as_of_date")["instrument_id"].transform("count")
    sig = sig[counts >= min_universe].reset_index(drop=True)
    return sig
