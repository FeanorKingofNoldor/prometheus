"""Tests for the signal research harness (replay evaluator) and registry.

The metric core is pure (frames in, dataclass out), so these run with NO DB:
synthetic data with a KNOWN rank correlation must recover ~that correlation;
a monotone synthetic signal must produce a monotonic decile spread; turnover
must be ~0 for a frozen ranking and high for a reshuffled one.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from prometheus.research.signal_harness import (
    DecileSpread,
    HorizonIC,
    _spearman,
    _turnover,
    evaluate_signal,
    time_series_ic,
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_panel(score, fwd, dates, n_per_date, horizons=(1, 5, 21), vol=None):
    """Build aligned scores + fwd_returns frames from arrays keyed by row."""
    rows_s, rows_r = [], []
    idx = 0
    for d in dates:
        for k in range(n_per_date):
            inst = f"I{k}"
            rows_s.append({"instrument_id": inst, "as_of_date": d, "score": score[idx]})
            r = {"instrument_id": inst, "as_of_date": d}
            for h in horizons:
                r[f"fwd_ret_{h}d"] = fwd[idx]
                if vol is not None:
                    r[f"fwd_vol_{h}d"] = vol[idx]
            rows_r.append(r)
            idx += 1
    return pd.DataFrame(rows_s), pd.DataFrame(rows_r)


# ---------------------------------------------------------------------------
# pure spearman
# ---------------------------------------------------------------------------

def test_spearman_perfect_monotone():
    x = np.arange(50, dtype=float)
    y = 3.0 * x + 7.0  # monotone increasing
    assert _spearman(x, y) == pytest.approx(1.0, abs=1e-9)
    assert _spearman(x, -y) == pytest.approx(-1.0, abs=1e-9)


def test_spearman_known_correlation_recovered():
    """Build score and target with a known ~0.6 rank relationship; recover it."""
    rng = np.random.default_rng(42)
    n = 4000
    z = rng.standard_normal(n)
    noise = rng.standard_normal(n)
    target = 0.6 * z + 0.8 * noise  # correlation by construction
    ic = _spearman(z, target)
    # population Spearman should land close to the Pearson of the linear mix
    assert 0.45 < ic < 0.65


# ---------------------------------------------------------------------------
# evaluate_signal: IC recovery
# ---------------------------------------------------------------------------

def test_evaluate_signal_recovers_known_ic():
    """Per-date IC mean should approximate the planted cross-sectional correlation."""
    rng = np.random.default_rng(7)
    dates = pd.to_datetime([f"2024-01-{d:02d}" for d in range(1, 26)])
    n_per_date = 60
    scores, fwds = [], []
    for _ in dates:
        s = rng.standard_normal(n_per_date)
        eps = rng.standard_normal(n_per_date)
        f = 0.5 * s + np.sqrt(1 - 0.25) * eps  # planted ~0.5 corr each date
        scores.append(s)
        fwds.append(f)
    score = np.concatenate(scores)
    fwd = np.concatenate(fwds)
    sdf, rdf = _make_panel(score, fwd, dates, n_per_date, horizons=(1, 5, 21))

    rep = evaluate_signal(sdf, rdf, name="synthetic", horizons=(1, 5, 21), headline_horizon=5)
    ic5 = rep.ic[5]
    assert isinstance(ic5, HorizonIC)
    assert ic5.n_dates == len(dates)
    assert ic5.mean_ic == pytest.approx(0.5, abs=0.12)
    # strong, consistent positive IC => big t-stat and predicts-return verdict
    assert ic5.t_stat > 5.0
    assert rep.what_predicts == "predicts-return"
    assert rep.verdict_hint == "alpha"


def test_evaluate_signal_zero_ic_shelved():
    """Random score vs independent returns => ~0 IC => shelve."""
    rng = np.random.default_rng(11)
    dates = pd.to_datetime([f"2024-02-{d:02d}" for d in range(1, 21)])
    n_per_date = 50
    score = rng.standard_normal(len(dates) * n_per_date)
    fwd = rng.standard_normal(len(dates) * n_per_date)  # independent
    sdf, rdf = _make_panel(score, fwd, dates, n_per_date, horizons=(1, 5, 21))
    rep = evaluate_signal(sdf, rdf, horizons=(1, 5, 21), headline_horizon=5)
    assert abs(rep.ic[5].mean_ic) < 0.1
    assert rep.verdict_hint == "shelve"


def test_evaluate_signal_predicts_vol_not_return():
    """Score correlated with forward VOL but not return => predicts-vol / risk."""
    rng = np.random.default_rng(3)
    dates = pd.to_datetime([f"2024-03-{d:02d}" for d in range(1, 26)])
    n_per_date = 60
    s_all, r_all, v_all = [], [], []
    for _ in dates:
        s = rng.standard_normal(n_per_date)
        ret = rng.standard_normal(n_per_date)  # independent of score
        vol = 0.7 * s + 0.5 * rng.standard_normal(n_per_date)  # correlated with score
        s_all.append(s)
        r_all.append(ret)
        v_all.append(vol)
    sdf, rdf = _make_panel(
        np.concatenate(s_all), np.concatenate(r_all), dates, n_per_date,
        horizons=(1, 5, 21), vol=np.concatenate(v_all),
    )
    rep = evaluate_signal(sdf, rdf, horizons=(1, 5, 21), headline_horizon=5)
    assert rep.what_predicts == "predicts-vol"
    assert rep.verdict_hint == "risk"
    assert rep.ic[5].mean_ic_vol > 0.3


def test_classify_long_horizon_alpha_not_masked_by_vol():
    """Return IC only significant at a LONGER horizon than headline => still alpha.

    Mirrors momentum: washed out at the short headline horizon but a real
    return predictor at the long horizon, alongside a strong vol IC. The
    classifier must not mislabel it as predicts-vol/risk.
    """
    rng = np.random.default_rng(21)
    dates = pd.to_datetime([f"2024-08-{d:02d}" for d in range(1, 26)])
    n_per_date = 60
    s_all = []
    short_ret, long_ret, vol = [], [], []
    for _ in dates:
        s = rng.standard_normal(n_per_date)
        s_all.append(s)
        # short horizon: essentially no relationship to score
        short_ret.append(0.01 * s + 1.0 * rng.standard_normal(n_per_date))
        # long horizon: clear positive relationship to score
        long_ret.append(0.6 * s + np.sqrt(1 - 0.36) * rng.standard_normal(n_per_date))
        # vol: strong negative relationship to score (momentum-like)
        vol.append(-0.7 * s + 0.5 * rng.standard_normal(n_per_date))

    rows_s, rows_r = [], []
    idx = 0
    for d in dates:
        for k in range(n_per_date):
            inst = f"I{k}"
            rows_s.append({"instrument_id": inst, "as_of_date": d,
                           "score": np.concatenate(s_all)[idx]})
            rows_r.append({
                "instrument_id": inst, "as_of_date": d,
                "fwd_ret_5d": np.concatenate(short_ret)[idx],
                "fwd_vol_5d": np.concatenate(vol)[idx],
                "fwd_ret_63d": np.concatenate(long_ret)[idx],
                "fwd_vol_63d": np.concatenate(vol)[idx],
            })
            idx += 1
    sdf, rdf = pd.DataFrame(rows_s), pd.DataFrame(rows_r)
    rep = evaluate_signal(sdf, rdf, horizons=(5, 63), headline_horizon=5)
    assert rep.ic[63].mean_ic > 0.3      # real long-horizon return prediction
    assert rep.ic[5].mean_ic_vol < -0.3  # strong vol relationship at headline
    assert rep.what_predicts == "predicts-return"
    assert rep.verdict_hint == "alpha"


# ---------------------------------------------------------------------------
# time-series IC (macro / regime single-series signals)
# ---------------------------------------------------------------------------

def test_time_series_ic_recovers_negative_relationship():
    """Stress signal that leads negative forward returns => negative IC, hit>0.5."""
    rng = np.random.default_rng(31)
    n = 300
    dates = pd.bdate_range("2020-01-01", periods=n)
    stress = pd.Series(rng.standard_normal(n).cumsum() * 0.0 + rng.standard_normal(n), index=dates)
    # forward return is negatively driven by stress plus noise
    fwd = pd.Series(-0.6 * stress.to_numpy() + 0.8 * rng.standard_normal(n), index=dates)
    res = time_series_ic(stress, fwd, name="stress", horizon=21)
    assert res.n == n
    assert res.ic_return < -0.3            # high stress => low forward return
    assert res.mean_ret_high < res.mean_ret_low
    assert res.hit_rate > 0.55             # directional bet works


def test_time_series_ic_zero_for_noise():
    rng = np.random.default_rng(32)
    n = 200
    dates = pd.bdate_range("2021-01-01", periods=n)
    sig = pd.Series(rng.standard_normal(n), index=dates)
    fwd = pd.Series(rng.standard_normal(n), index=dates)  # independent
    res = time_series_ic(sig, fwd, name="noise", horizon=5)
    assert abs(res.ic_return) < 0.2


def test_time_series_ic_too_few_points_nan():
    dates = pd.bdate_range("2021-01-01", periods=4)
    sig = pd.Series([1.0, 2.0, 3.0, 4.0], index=dates)
    fwd = pd.Series([0.1, -0.1, 0.2, -0.2], index=dates)
    res = time_series_ic(sig, fwd)
    assert res.n == 4
    assert np.isnan(res.ic_return)


# ---------------------------------------------------------------------------
# decile monotonicity
# ---------------------------------------------------------------------------

def test_decile_monotonic_on_monotone_signal():
    """Forward return a strictly increasing function of score => monotonic deciles."""
    rng = np.random.default_rng(5)
    dates = pd.to_datetime([f"2024-04-{d:02d}" for d in range(1, 21)])
    n_per_date = 100
    s_all, f_all = [], []
    for _ in dates:
        s = rng.standard_normal(n_per_date)
        f = 0.01 * s + 1e-4 * rng.standard_normal(n_per_date)  # near-deterministic in score
        s_all.append(s)
        f_all.append(f)
    sdf, rdf = _make_panel(
        np.concatenate(s_all), np.concatenate(f_all), dates, n_per_date, horizons=(21,)
    )
    rep = evaluate_signal(sdf, rdf, horizons=(21,), headline_horizon=21)
    dec = rep.deciles[21]
    assert isinstance(dec, DecileSpread)
    assert dec.monotonic is True
    assert dec.top_minus_bottom > 0          # high score => high return
    assert dec.spearman_bucket > 0.9


# ---------------------------------------------------------------------------
# turnover
# ---------------------------------------------------------------------------

def test_turnover_zero_for_frozen_ranking():
    """Identical scores every day => turnover ~0."""
    dates = pd.to_datetime([f"2024-05-{d:02d}" for d in range(1, 11)])
    rows = []
    base = np.arange(30, dtype=float)
    for d in dates:
        for k in range(30):
            rows.append({"instrument_id": f"I{k}", "as_of_date": d, "score": base[k]})
    sdf = pd.DataFrame(rows)
    assert _turnover(sdf) == pytest.approx(0.0, abs=1e-9)


def test_turnover_high_for_reshuffled_ranking():
    """Independently reshuffled scores each day => turnover near 1."""
    rng = np.random.default_rng(9)
    dates = pd.to_datetime([f"2024-06-{d:02d}" for d in range(1, 21)])
    rows = []
    for d in dates:
        vals = rng.standard_normal(40)
        for k in range(40):
            rows.append({"instrument_id": f"I{k}", "as_of_date": d, "score": vals[k]})
    sdf = pd.DataFrame(rows)
    t = _turnover(sdf)
    assert 0.7 < t < 1.3  # ~1.0 for independent reshuffles


def test_turnover_sane_bounds_partial_drift():
    """A slowly drifting ranking gives turnover strictly between frozen and random."""
    rng = np.random.default_rng(13)
    dates = pd.to_datetime([f"2024-07-{d:02d}" for d in range(1, 21)])
    rows = []
    state = rng.standard_normal(40)
    for d in dates:
        state = 0.95 * state + 0.05 * rng.standard_normal(40)  # high persistence
        for k in range(40):
            rows.append({"instrument_id": f"I{k}", "as_of_date": d, "score": state[k]})
    sdf = pd.DataFrame(rows)
    t = _turnover(sdf)
    assert 0.0 < t < 0.5
