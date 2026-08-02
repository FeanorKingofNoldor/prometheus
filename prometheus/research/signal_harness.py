"""Prometheus — Signal Research Harness (replay-mode evaluator).

A reusable, point-in-time-clean suite for testing whether a signal predicts
anything worth trading. Every future signal/strategy passes through here.

The harness takes a *tidy* signal frame and a *returns source* and answers, with
NO look-ahead (forward returns are strictly after ``as_of_date``):

- **rank-IC** (Spearman) of score vs forward return at multiple horizons, with
  mean / std / t-stat and IC decay across horizons.
- **decile / quintile spread** (top-minus-bottom forward return) + monotonicity.
- **turnover** of the signal (rank stability day-over-day) — a tradeability /
  cost proxy.
- **what-it-predicts classifier**: the IC of score vs forward *volatility* is
  computed alongside the return IC, so a signal that predicts vol-not-return
  (the lambda lesson) is flagged. A verdict hint is emitted:
  ``predicts-return`` / ``predicts-vol`` / ``predicts-timing`` / ``predicts-neither``.

Design:

- The metric layer is **pure** (``evaluate_signal(scores, fwd_returns, ...)`` takes
  frames, returns a dataclass). It has no DB / IO dependency and is the unit the
  tests exercise on synthetic data with known correlation.
- A thin DB layer (``forward_return_panel``, ``momentum_signal``) builds the
  point-in-time forward-return panel and example signals from
  ``prometheus_historical`` (prices_daily) so the harness can be proven
  end-to-end on real data.

The Spearman rank-IC follows the same definition used by
``prometheus.decisions.scorecard.PredictionScorecard`` (cross-sectional rank
correlation of score vs realized forward return), generalized to a panel,
multiple horizons, and a vol target.

Usage::

    from prometheus.research.signal_harness import (
        evaluate_signal, momentum_signal, forward_return_panel,
    )
    scores = momentum_signal(db, start, end, lookback=126)
    rets = forward_return_panel(db, scores, horizons=(1, 5, 21, 63))
    report = evaluate_signal(scores, rets, horizons=(1, 5, 21, 63))
    print(report.headline())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

DEFAULT_HORIZONS: Tuple[int, ...] = (1, 5, 21, 63)

# Minimum cross-section size per date below which an IC observation is dropped
# as small-N noise (mirrors scorecard's DEFAULT_MIN_N intent, per-date).
MIN_CROSS_SECTION = 5

# IC magnitude thresholds for the what-it-predicts verdict.
IC_RETURN_THRESHOLD = 0.02  # |mean IC| above this (and t>2) => meaningful
IC_TSTAT_THRESHOLD = 2.0


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HorizonIC:
    """Per-horizon rank-IC statistics across the evaluation window."""

    horizon: int
    n_dates: int
    mean_ic: float
    std_ic: float
    t_stat: float
    ic_ir: float  # information ratio = mean / std (annualization-agnostic)
    # vs forward volatility (the "what does it predict" probe)
    mean_ic_vol: float
    t_stat_vol: float


@dataclass(frozen=True)
class DecileSpread:
    """Top-minus-bottom forward-return spread at one horizon."""

    horizon: int
    n_buckets: int
    bucket_returns: Tuple[float, ...]  # mean fwd return per bucket, low->high score
    top_minus_bottom: float
    monotonic: bool
    spearman_bucket: float  # rank corr of bucket index vs bucket mean return


@dataclass(frozen=True)
class SignalReport:
    """Complete replay-mode evaluation of one signal."""

    name: str
    n_obs: int
    n_dates: int
    date_range: Tuple[Optional[date], Optional[date]]
    horizons: Tuple[int, ...]
    ic: Dict[int, HorizonIC]
    deciles: Dict[int, DecileSpread]
    turnover: float  # mean 1 - rank_corr(day, prev_day) across consecutive dates
    headline_horizon: int
    what_predicts: str  # predicts-return / predicts-vol / predicts-timing / predicts-neither
    verdict_hint: str  # alpha / risk / timing / shelve
    notes: List[str] = field(default_factory=list)

    @property
    def headline_ic(self) -> float:
        h = self.ic.get(self.headline_horizon)
        return h.mean_ic if h else float("nan")

    def headline(self) -> str:
        """One-paragraph human summary."""
        lines = [
            f"Signal '{self.name}': {self.n_obs} obs over {self.n_dates} dates "
            f"{self.date_range[0]}..{self.date_range[1]}",
        ]
        lines.append(
            f"  what-it-predicts: {self.what_predicts}  |  verdict-hint: {self.verdict_hint}"
        )
        lines.append("  rank-IC (Spearman) vs FORWARD RETURN by horizon:")
        for h in self.horizons:
            ic = self.ic.get(h)
            if ic is None:
                continue
            lines.append(
                f"    {h:>3}d: meanIC={ic.mean_ic:+.4f}  t={ic.t_stat:+.2f}  "
                f"IR={ic.ic_ir:+.3f}  (vs vol: IC={ic.mean_ic_vol:+.4f} t={ic.t_stat_vol:+.2f})  "
                f"n_dates={ic.n_dates}"
            )
        lines.append("  decile spread (top-minus-bottom fwd return), monotonicity:")
        for h in self.horizons:
            d = self.deciles.get(h)
            if d is None:
                continue
            lines.append(
                f"    {h:>3}d: top-bottom={d.top_minus_bottom:+.4%}  "
                f"monotonic={d.monotonic}  bucket-rankcorr={d.spearman_bucket:+.3f}"
            )
        lines.append(f"  turnover (mean daily rank churn): {self.turnover:.3f}")
        for note in self.notes:
            lines.append(f"  note: {note}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Pure metric core
# ---------------------------------------------------------------------------


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation of two aligned 1-D arrays.

    Returns NaN if fewer than 3 finite pairs or no rank variance. Uses average
    ranks (tie-aware) via pandas, then Pearson on ranks — equivalent to the
    Spearman rho and robust to ties (unlike scorecard's d^2 shortcut).
    """
    mask = np.isfinite(a) & np.isfinite(b)
    a, b = a[mask], b[mask]
    if a.size < 3:
        return float("nan")
    ra = pd.Series(a).rank(method="average").to_numpy()
    rb = pd.Series(b).rank(method="average").to_numpy()
    if ra.std() == 0 or rb.std() == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def _cross_sectional_ic(
    merged: pd.DataFrame,
    score_col: str,
    target_col: str,
    *,
    min_cross_section: int = MIN_CROSS_SECTION,
) -> pd.Series:
    """Per-date cross-sectional Spearman IC of score vs target.

    ``merged`` must have an ``as_of_date`` column. Dates with fewer than
    ``min_cross_section`` finite pairs are skipped. Returns a Series indexed by
    as_of_date.
    """
    ics: Dict[date, float] = {}
    for as_of, g in merged.groupby("as_of_date"):
        s = g[score_col].to_numpy(dtype=float)
        t = g[target_col].to_numpy(dtype=float)
        mask = np.isfinite(s) & np.isfinite(t)
        if mask.sum() < min_cross_section:
            continue
        ic = _spearman(s, t)
        if np.isfinite(ic):
            ics[as_of] = ic
    return pd.Series(ics, dtype=float).sort_index()


def _ic_stats(ic_series: pd.Series) -> Tuple[int, float, float, float, float]:
    """(n, mean, std, t_stat, ir) for a series of per-date ICs."""
    vals = ic_series.dropna().to_numpy(dtype=float)
    n = int(vals.size)
    if n == 0:
        return 0, float("nan"), float("nan"), float("nan"), float("nan")
    mean = float(vals.mean())
    std = float(vals.std(ddof=1)) if n > 1 else 0.0
    t_stat = float(mean / (std / np.sqrt(n))) if std > 0 else float("nan")
    ir = float(mean / std) if std > 0 else float("nan")
    return n, mean, std, t_stat, ir


def _decile_spread(
    merged: pd.DataFrame,
    score_col: str,
    ret_col: str,
    *,
    n_buckets: int = 10,
    min_cross_section: int = MIN_CROSS_SECTION,
) -> DecileSpread:
    """Bucket by score each date, average forward return per bucket, pool across dates.

    Buckets run low-score (0) to high-score (n_buckets-1). Monotonic = bucket
    mean returns strictly increase OR strictly decrease across buckets.
    """
    horizon = int(ret_col.split("_")[-1].rstrip("d")) if "_" in ret_col else 0
    per_date_bucket_means: List[np.ndarray] = []
    for _as_of, g in merged.groupby("as_of_date"):
        g = g[[score_col, ret_col]].replace([np.inf, -np.inf], np.nan).dropna()
        if len(g) < max(min_cross_section, n_buckets):
            continue
        try:
            labels = pd.qcut(
                g[score_col].rank(method="first"), n_buckets, labels=False
            )
        except ValueError:
            continue
        means = g[ret_col].groupby(labels).mean()
        row = np.full(n_buckets, np.nan)
        for b, v in means.items():
            row[int(b)] = v
        per_date_bucket_means.append(row)

    if not per_date_bucket_means:
        return DecileSpread(horizon, n_buckets, tuple([float("nan")] * n_buckets),
                            float("nan"), False, float("nan"))

    stacked = np.vstack(per_date_bucket_means)
    bucket_returns = np.nanmean(stacked, axis=0)
    top_minus_bottom = float(bucket_returns[-1] - bucket_returns[0])

    finite = np.isfinite(bucket_returns)
    if finite.sum() >= 3:
        idx = np.arange(n_buckets)[finite]
        vals = bucket_returns[finite]
        diffs = np.diff(vals)
        monotonic = bool(np.all(diffs > 0) or np.all(diffs < 0))
        bucket_rankcorr = _spearman(idx.astype(float), vals)
    else:
        monotonic = False
        bucket_rankcorr = float("nan")

    return DecileSpread(
        horizon=horizon,
        n_buckets=n_buckets,
        bucket_returns=tuple(float(x) for x in bucket_returns),
        top_minus_bottom=top_minus_bottom,
        monotonic=monotonic,
        spearman_bucket=bucket_rankcorr,
    )


def _turnover(scores: pd.DataFrame) -> float:
    """Mean day-over-day rank churn = mean(1 - rankcorr(date, prev_date)).

    Aligns the instrument universe between consecutive dates; uses only the
    intersection. 0 => identical ranking each day (no trading), 1 => fully
    reshuffled (max cost).
    """
    dates = sorted(scores["as_of_date"].unique())
    if len(dates) < 2:
        return float("nan")
    by_date = {
        d: g.set_index("instrument_id")["score"]
        for d, g in scores.groupby("as_of_date")
    }
    churns: List[float] = []
    for prev, cur in zip(dates[:-1], dates[1:]):
        a, b = by_date[prev], by_date[cur]
        common = a.index.intersection(b.index)
        if len(common) < MIN_CROSS_SECTION:
            continue
        rc = _spearman(a.loc[common].to_numpy(float), b.loc[common].to_numpy(float))
        if np.isfinite(rc):
            churns.append(1.0 - rc)
    return float(np.mean(churns)) if churns else float("nan")


def _classify(
    ic: Dict[int, HorizonIC],
    deciles: Dict[int, DecileSpread],
    headline_horizon: int,
) -> Tuple[str, str]:
    """Return (what_predicts, verdict_hint).

    Robust to horizon: a signal counts as a return predictor if its return-IC is
    significant at the HEADLINE horizon OR at any LONGER horizon (real alpha often
    only shows up at the signal's natural horizon — e.g. momentum at 63d). Logic:

    - Return IC significant at headline: predicts-return / alpha.
    - Else return IC significant only at a longer horizon than headline:
      predicts-return / alpha (the headline is just too short for this signal).
    - Else return IC significant only at a shorter horizon (decays out):
      predicts-timing / timing.
    - Else strong vol IC at headline: predicts-vol / risk (the lambda lesson —
      a vol forecaster masquerading as alpha).
    - Else: predicts-neither / shelve.
    """
    head = ic.get(headline_horizon)
    if head is None:
        return "predicts-neither", "shelve"

    def _ret_strong(h: HorizonIC) -> bool:
        return abs(h.mean_ic) >= IC_RETURN_THRESHOLD and abs(h.t_stat) >= IC_TSTAT_THRESHOLD

    head_ret = _ret_strong(head)
    vol_strong = (
        abs(head.mean_ic_vol) >= IC_RETURN_THRESHOLD
        and abs(head.t_stat_vol) >= IC_TSTAT_THRESHOLD
    )
    longer_ret = any(_ret_strong(ic[h]) for h in ic if h > headline_horizon)
    shorter_ret = any(_ret_strong(ic[h]) for h in ic if h < headline_horizon)

    if head_ret or longer_ret:
        return "predicts-return", "alpha"
    if shorter_ret:
        return "predicts-timing", "timing"
    if vol_strong:
        return "predicts-vol", "risk"
    return "predicts-neither", "shelve"


def evaluate_signal(
    scores: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    *,
    name: str = "signal",
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    headline_horizon: Optional[int] = None,
    n_buckets: int = 10,
    min_cross_section: int = MIN_CROSS_SECTION,
) -> SignalReport:
    """Pure replay-mode evaluation of a signal. NO look-ahead.

    Args:
        scores: tidy frame with columns ``instrument_id`` (or any key), ``as_of_date``,
            ``score``. One row per (key, date).
        fwd_returns: tidy frame with ``instrument_id``, ``as_of_date`` and, for each
            horizon ``h``, a column ``fwd_ret_{h}d`` (return from as_of to as_of+h,
            strictly forward) and optionally ``fwd_vol_{h}d`` (realized forward vol).
            Forward columns MUST already be computed point-in-time by the caller
            (see ``forward_return_panel``).
        name: signal name for the report.
        horizons: horizons (days) to evaluate.
        headline_horizon: which horizon drives the verdict (default: middle one).
        n_buckets: decile (10) / quintile (5) bucketing.
        min_cross_section: min names per date for an IC/bucket observation.

    Returns:
        SignalReport.
    """
    horizons = tuple(int(h) for h in horizons)
    if headline_horizon is None:
        headline_horizon = horizons[len(horizons) // 2]

    key = "instrument_id" if "instrument_id" in scores.columns else (
        "cluster_key" if "cluster_key" in scores.columns else scores.columns[0]
    )
    s = scores.rename(columns={key: "instrument_id"})[["instrument_id", "as_of_date", "score"]].copy()
    r = fwd_returns.rename(columns={key: "instrument_id"}).copy()

    merged = s.merge(r, on=["instrument_id", "as_of_date"], how="inner")
    n_obs = int(len(merged))
    dates = sorted(merged["as_of_date"].unique()) if n_obs else []
    date_range = (dates[0], dates[-1]) if dates else (None, None)

    ic_map: Dict[int, HorizonIC] = {}
    decile_map: Dict[int, DecileSpread] = {}
    notes: List[str] = []

    for h in horizons:
        ret_col = f"fwd_ret_{h}d"
        vol_col = f"fwd_vol_{h}d"
        if ret_col not in merged.columns:
            notes.append(f"missing {ret_col}; horizon {h}d skipped")
            continue

        ic_ret = _cross_sectional_ic(merged, "score", ret_col, min_cross_section=min_cross_section)
        n, mean, std, t_stat, ir = _ic_stats(ic_ret)

        if vol_col in merged.columns:
            ic_vol = _cross_sectional_ic(merged, "score", vol_col, min_cross_section=min_cross_section)
            _nv, mean_v, _sv, t_v, _iv = _ic_stats(ic_vol)
        else:
            mean_v, t_v = float("nan"), float("nan")

        ic_map[h] = HorizonIC(
            horizon=h, n_dates=n, mean_ic=mean, std_ic=std, t_stat=t_stat,
            ic_ir=ir, mean_ic_vol=mean_v, t_stat_vol=t_v,
        )
        decile_map[h] = _decile_spread(
            merged, "score", ret_col, n_buckets=n_buckets, min_cross_section=min_cross_section
        )

    if headline_horizon not in ic_map and ic_map:
        headline_horizon = sorted(ic_map.keys())[len(ic_map) // 2]

    turnover = _turnover(s)
    what_predicts, verdict_hint = _classify(ic_map, decile_map, headline_horizon)

    return SignalReport(
        name=name,
        n_obs=n_obs,
        n_dates=len(dates),
        date_range=date_range,
        horizons=horizons,
        ic=ic_map,
        deciles=decile_map,
        turnover=turnover,
        headline_horizon=headline_horizon,
        what_predicts=what_predicts,
        verdict_hint=verdict_hint,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Time-series IC — for single-series macro / regime / timing signals
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TimeSeriesIC:
    """Rank-IC of a one-value-per-date signal vs a forward market return/vol.

    Used for macro / regime layers that emit ONE number per date (not a
    cross-section), so the question is timing — does a high reading predict a
    high/low forward MARKET return or risk — answered with a single time-series
    Spearman rank correlation, not a per-date cross-sectional one.
    """

    name: str
    horizon: int
    n: int
    ic_return: float
    ic_vol: float
    hit_rate: float
    mean_ret_high: float  # mean fwd return when signal in top tercile
    mean_ret_low: float   # mean fwd return when signal in bottom tercile


def time_series_ic(
    signal: pd.Series,
    fwd_return: pd.Series,
    *,
    fwd_vol: Optional[pd.Series] = None,
    name: str = "macro",
    horizon: int = 21,
) -> TimeSeriesIC:
    """Spearman rank-IC of a date-indexed signal vs a date-indexed forward return.

    All series are indexed by date and inner-joined. This is the right tool for
    a macro/regime stress level (one number per day) where the cross-section is
    "dates", not "instruments". Also reports top-minus-bottom-tercile mean
    forward return so the sign/economic size is legible.
    """
    df = pd.DataFrame({"sig": signal, "ret": fwd_return}).dropna()
    if fwd_vol is not None:
        df = df.join(pd.Series(fwd_vol, name="vol"), how="left")
    n = int(len(df))
    if n < 10:
        return TimeSeriesIC(name, horizon, n, float("nan"), float("nan"),
                            float("nan"), float("nan"), float("nan"))
    ic_ret = _spearman(df["sig"].to_numpy(float), df["ret"].to_numpy(float))
    ic_vol = (
        _spearman(df["sig"].to_numpy(float), df["vol"].to_numpy(float))
        if "vol" in df.columns else float("nan")
    )
    try:
        terc = pd.qcut(df["sig"].rank(method="first"), 3, labels=False)
        hi = df["ret"][terc == 2]
        lo = df["ret"][terc == 0]
        mean_hi = float(hi.mean()) if len(hi) else float("nan")
        mean_lo = float(lo.mean()) if len(lo) else float("nan")
    except ValueError:
        mean_hi = mean_lo = float("nan")
    sign_pred = -np.sign(df["sig"] - df["sig"].median())
    hit = float((np.sign(df["ret"]) == sign_pred).mean())
    return TimeSeriesIC(
        name=name, horizon=horizon, n=n,
        ic_return=ic_ret, ic_vol=ic_vol, hit_rate=hit,
        mean_ret_high=mean_hi, mean_ret_low=mean_lo,
    )


# ---------------------------------------------------------------------------
# DB layer — point-in-time forward returns and example signals
# ---------------------------------------------------------------------------


def _load_prices(
    db_manager,
    instrument_ids: Optional[Sequence[str]],
    start: date,
    end: date,
) -> pd.DataFrame:
    """Load (instrument_id, trade_date, close) from prices_daily, adjusted close.

    Uses adjusted_close when present (split/dividend clean) else close.
    """
    sql = """
        SELECT instrument_id, trade_date,
               COALESCE(adjusted_close, close) AS px
        FROM prices_daily
        WHERE trade_date BETWEEN %s AND %s
          AND COALESCE(adjusted_close, close) > 0
          AND instrument_id NOT LIKE 'SYNTH_%%'
    """
    params: List = [start, end]
    if instrument_ids is not None:
        sql += " AND instrument_id = ANY(%s)"
        params.append(list(instrument_ids))
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["instrument_id", "trade_date", "px"])
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["px"] = df["px"].astype(float)
    return df.sort_values(["instrument_id", "trade_date"]).reset_index(drop=True)


def forward_return_panel(
    db_manager,
    scores: pd.DataFrame,
    *,
    horizons: Sequence[int] = DEFAULT_HORIZONS,
    universe: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Build a point-in-time forward-return + forward-vol panel for the signal dates.

    For each (instrument, as_of_date) in ``scores`` and each horizon ``h``:
      - ``fwd_ret_{h}d`` = px[t+h] / px[t] - 1 using the close on/at the as_of_date
        and the close exactly ``h`` trading days LATER (strictly after t).
      - ``fwd_vol_{h}d`` = std of the daily simple returns over (t, t+h], annualized.

    No look-ahead: only trading days strictly after the as_of date feed the
    forward window. Instruments without a price on the as_of date or without a
    full forward window are dropped for that horizon (NaN).
    """
    horizons = tuple(int(h) for h in horizons)
    max_h = max(horizons)
    s = scores.copy()
    s["as_of_date"] = pd.to_datetime(s["as_of_date"])
    key = "instrument_id" if "instrument_id" in s.columns else s.columns[0]
    insts = list(s[key].unique()) if universe is None else list(universe)

    start = s["as_of_date"].min().date()
    # pad the window so the longest forward horizon has price data
    end = (s["as_of_date"].max() + timedelta(days=int(max_h * 2 + 14))).date()
    prices = _load_prices(db_manager, insts, start, end)
    if prices.empty:
        return pd.DataFrame(columns=[key, "as_of_date"])

    out_rows: List[dict] = []
    for inst, g in prices.groupby("instrument_id"):
        g = g.reset_index(drop=True)
        px = g["px"].to_numpy()
        dts = g["trade_date"].to_numpy()
        # daily simple returns for vol
        daily_ret = np.concatenate([[np.nan], px[1:] / px[:-1] - 1.0])
        date_to_idx = {d: i for i, d in enumerate(dts)}
        sub = s[s[key] == inst]
        for as_of in sub["as_of_date"].to_numpy():
            i = date_to_idx.get(as_of)
            if i is None:
                # use the last trading day on/before as_of
                prior = np.searchsorted(dts, as_of, side="right") - 1
                if prior < 0:
                    continue
                i = int(prior)
            row = {key: inst, "as_of_date": pd.Timestamp(as_of)}
            base = px[i]
            for h in horizons:
                j = i + h
                if j < len(px) and base > 0:
                    row[f"fwd_ret_{h}d"] = float(px[j] / base - 1.0)
                    window = daily_ret[i + 1 : j + 1]
                    window = window[np.isfinite(window)]
                    if window.size >= 2:
                        row[f"fwd_vol_{h}d"] = float(np.std(window, ddof=1) * np.sqrt(252.0))
                    else:
                        row[f"fwd_vol_{h}d"] = np.nan
                else:
                    row[f"fwd_ret_{h}d"] = np.nan
                    row[f"fwd_vol_{h}d"] = np.nan
            out_rows.append(row)

    panel = pd.DataFrame(out_rows)
    if not panel.empty:
        panel["as_of_date"] = pd.to_datetime(panel["as_of_date"])
    return panel


def zscore_by_date(scores: pd.DataFrame, *, col: str = "score") -> pd.DataFrame:
    """Cross-sectionally z-score ``col`` within each as_of_date.

    Returns the frame with ``col`` replaced by its per-date z-score (mean 0,
    std 1 each date). Degenerate cross-sections (zero variance) map to 0.
    """
    out = scores.copy()

    def _z(g: pd.Series) -> pd.Series:
        mu = g.mean()
        sigma = g.std(ddof=1)
        if not np.isfinite(sigma) or sigma <= 1e-12:
            return pd.Series(0.0, index=g.index)
        return (g - mu) / sigma

    out[col] = out.groupby("as_of_date")[col].transform(_z)
    return out


def stab_base_signal(
    db_manager,
    score_dates: pd.DataFrame,
    *,
    universe: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """STAB/liquidity 'base' quality component for the combiner, point-in-time.

    Mirrors the universe engine's ranking base:
    ``base = max(0, 100 - soft_target_score) + min(50, avg_volume_63d/1e6)``,
    read as-of each date from the latest STAB instrument state and 63d volume.
    Returns a tidy frame (instrument_id, as_of_date, score) on the same
    (instrument, date) grid as ``score_dates``. Names without a STAB state on a
    date are dropped for that date (the combiner treats them as neutral).
    """
    pairs = score_dates[["instrument_id", "as_of_date"]].drop_duplicates()
    if pairs.empty:
        return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])
    pairs = pairs.copy()
    pairs["as_of_date"] = pd.to_datetime(pairs["as_of_date"])
    insts = list(pairs["instrument_id"].unique()) if universe is None else list(universe)

    start = pairs["as_of_date"].min().date()
    end = pairs["as_of_date"].max().date()

    # avg 63d volume per (instrument, date) from prices_daily.
    vol_sql = """
        SELECT instrument_id, trade_date, volume
        FROM prices_daily
        WHERE trade_date BETWEEN %s AND %s
          AND instrument_id = ANY(%s)
          AND instrument_id NOT LIKE 'SYNTH_%%'
    """
    with db_manager.get_historical_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(vol_sql, (start - timedelta(days=160), end, insts))
            vol_rows = cur.fetchall()

    # latest soft_target_score per (instrument, as_of_date), no look-ahead.
    stab_sql = """
        SELECT entity_id, as_of_date, soft_target_score
        FROM soft_target_classes
        WHERE entity_type = 'INSTRUMENT'
          AND entity_id = ANY(%s)
          AND as_of_date BETWEEN %s AND %s
    """
    stab_rows = []
    try:
        with db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stab_sql, (insts, start - timedelta(days=400), end))
                stab_rows = cur.fetchall()
    except Exception:
        stab_rows = []

    if not vol_rows:
        return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])

    vol = pd.DataFrame(vol_rows, columns=["instrument_id", "trade_date", "volume"])
    vol["trade_date"] = pd.to_datetime(vol["trade_date"])
    vol = vol.sort_values(["instrument_id", "trade_date"])
    vol["avg_vol_63d"] = (
        vol.groupby("instrument_id")["volume"]
        .transform(lambda s: s.rolling(63, min_periods=20).mean())
    )

    stab = pd.DataFrame(
        stab_rows, columns=["instrument_id", "as_of_date", "soft_target_score"]
    )
    if not stab.empty:
        stab["as_of_date"] = pd.to_datetime(stab["as_of_date"])
        stab = stab.sort_values(["instrument_id", "as_of_date"])

    out_rows: List[dict] = []
    vol_by_inst = {i: g for i, g in vol.groupby("instrument_id")}
    stab_by_inst = (
        {i: g for i, g in stab.groupby("instrument_id")} if not stab.empty else {}
    )
    for inst, g in pairs.groupby("instrument_id"):
        vg = vol_by_inst.get(inst)
        if vg is None:
            continue
        sg = stab_by_inst.get(inst)
        for as_of in g["as_of_date"]:
            vrow = vg[vg["trade_date"] <= as_of]
            if vrow.empty or not np.isfinite(vrow["avg_vol_63d"].iloc[-1]):
                continue
            avg_vol = float(vrow["avg_vol_63d"].iloc[-1])
            soft = 0.0
            if sg is not None:
                srow = sg[sg["as_of_date"] <= as_of]
                if not srow.empty:
                    soft = float(srow["soft_target_score"].iloc[-1])
            base = max(0.0, 100.0 - soft) + min(50.0, avg_vol / 1_000_000.0)
            out_rows.append(
                {"instrument_id": inst, "as_of_date": as_of, "score": base}
            )
    return pd.DataFrame(out_rows)


def momentum_signal(
    db_manager,
    start: date,
    end: date,
    *,
    lookback: int = 126,
    skip: int = 21,
    universe: Optional[Sequence[str]] = None,
    min_universe: int = 20,
    sample_every: int = 5,
) -> pd.DataFrame:
    """Cross-sectional momentum signal from prices_daily (Bucket 1: replayable).

    score(i, t) = px[t-skip] / px[t-skip-lookback] - 1

    i.e. ``lookback``-day total return ending ``skip`` days before t (the classic
    12-1 / 6-1 momentum gap that skips the short-term reversal month). Computed
    purely from past prices => point-in-time clean by construction.

    Args:
        start, end: as_of date window (signal observation dates).
        lookback: momentum formation window in trading days (126 ~ 6 months).
        skip: trading days to skip before t (21 ~ 1 month).
        universe: optional instrument whitelist; default = all liquid US equities.
        min_universe: min names on a date to emit that cross-section.
        sample_every: sample every Nth trading day (5 => weekly) to cut compute.

    Returns:
        tidy frame: instrument_id, as_of_date, score.
    """
    need = lookback + skip
    load_start = start - timedelta(days=int(need * 2 + 30))
    prices = _load_prices(db_manager, universe, load_start, end)
    if prices.empty:
        return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])

    rows: List[dict] = []
    for inst, g in prices.groupby("instrument_id"):
        g = g.reset_index(drop=True)
        px = g["px"].to_numpy()
        dts = g["trade_date"].to_numpy()
        for i in range(need, len(px)):
            as_of = dts[i]
            if not (np.datetime64(start) <= as_of <= np.datetime64(end)):
                continue
            base = px[i - skip - lookback]
            recent = px[i - skip]
            if base > 0 and recent > 0:
                rows.append(
                    {"instrument_id": inst, "as_of_date": pd.Timestamp(as_of),
                     "score": float(recent / base - 1.0)}
                )
    sig = pd.DataFrame(rows)
    if sig.empty:
        return sig

    # weekly sampling to reduce overlap/compute
    all_dates = sorted(sig["as_of_date"].unique())
    keep = set(all_dates[::sample_every])
    sig = sig[sig["as_of_date"].isin(keep)]

    # drop thin cross-sections
    counts = sig.groupby("as_of_date")["instrument_id"].transform("count")
    sig = sig[counts >= min_universe].reset_index(drop=True)
    return sig
