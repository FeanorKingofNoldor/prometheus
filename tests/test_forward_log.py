"""Tests for forward-log store and event-study helpers.

The forward-log DB layer is exercised against an in-memory fake DatabaseManager
(no network). The event-study core is pure (callback in, dataclass out) so it
runs on synthetic prices with a planted abnormal return.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from prometheus.research.forward_log import (
    EventStudyResult,
    append_forward_snapshot,
    event_study,
    make_price_forward_return_fn,
    read_forward_series,
)

# ---------------------------------------------------------------------------
# in-memory fake DB for the forward-log store
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, store: dict):
        self._store = store
        self._result: list = []

    def execute(self, sql, params=None):
        s = " ".join(sql.split())
        if s.startswith("CREATE TABLE"):
            return
        if s.startswith("SELECT"):
            name = params[0]
            rows = [
                (inst, d, score)
                for (sig, d, inst), score in self._store.items()
                if sig == name
            ]
            rows.sort(key=lambda r: (r[1], r[0]))
            self._result = rows

    def executemany(self, sql, rows):
        for sig, d, inst, score in rows:
            self._store[(sig, d, inst)] = score

    def fetchall(self):
        return self._result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, store):
        self._store = store

    def cursor(self):
        return _FakeCursor(self._store)

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeDB:
    def __init__(self):
        self._store: dict = {}

    def get_runtime_connection(self):
        return _FakeConn(self._store)


# ---------------------------------------------------------------------------
# forward-log round-trip
# ---------------------------------------------------------------------------


def test_forward_log_append_and_read_roundtrip():
    db = _FakeDB()
    snap = pd.DataFrame(
        {
            "instrument_id": ["AAA", "BBB", "CCC"],
            "score": [1.0, 2.0, 3.0],
        }
    )
    n = append_forward_snapshot(db, "sig_x", snap, as_of_date=date(2026, 6, 10))
    assert n == 3
    out = read_forward_series(db, "sig_x")
    assert list(out.columns) == ["instrument_id", "as_of_date", "score"]
    assert len(out) == 3
    assert set(out["instrument_id"]) == {"AAA", "BBB", "CCC"}
    assert out["as_of_date"].dt.date.iloc[0] == date(2026, 6, 10)


def test_forward_log_upsert_is_idempotent():
    db = _FakeDB()
    snap = pd.DataFrame({"instrument_id": ["AAA"], "score": [1.0]})
    append_forward_snapshot(db, "sig_y", snap, as_of_date=date(2026, 6, 10))
    snap2 = pd.DataFrame({"instrument_id": ["AAA"], "score": [9.9]})
    append_forward_snapshot(db, "sig_y", snap2, as_of_date=date(2026, 6, 10))
    out = read_forward_series(db, "sig_y")
    assert len(out) == 1
    assert out["score"].iloc[0] == pytest.approx(9.9)


def test_forward_log_accrues_multiple_days():
    db = _FakeDB()
    for i, d in enumerate([date(2026, 6, 8), date(2026, 6, 9), date(2026, 6, 10)]):
        append_forward_snapshot(
            db, "sig_z",
            pd.DataFrame({"instrument_id": ["AAA", "BBB"], "score": [float(i), float(i) + 0.5]}),
            as_of_date=d,
        )
    out = read_forward_series(db, "sig_z")
    assert out["as_of_date"].nunique() == 3
    assert len(out) == 6


def test_forward_log_empty_snapshot_noop():
    db = _FakeDB()
    assert append_forward_snapshot(db, "sig_e", pd.DataFrame(columns=["instrument_id", "score"])) == 0


# ---------------------------------------------------------------------------
# event study
# ---------------------------------------------------------------------------


def _synthetic_prices(instruments, start="2024-01-01", n=80, drift=0.0, seed=0):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, periods=n)
    rows = []
    for inst in instruments:
        px = 100.0
        for d in dates:
            px *= 1.0 + drift + 0.005 * rng.standard_normal()
            rows.append({"instrument_id": inst, "trade_date": d, "px": px})
    return pd.DataFrame(rows)


def test_event_study_recovers_planted_abnormal_return():
    """Exposed assets jump after events; study should see a positive abnormal CAR."""
    exposed_ids = [f"E{i}" for i in range(10)]
    base_ids = [f"B{i}" for i in range(10)]
    # exposed have positive post-event drift, baseline flat
    pe = _synthetic_prices(exposed_ids, drift=0.004, seed=1)
    pb = _synthetic_prices(base_ids, drift=0.0, seed=2)
    prices = pd.concat([pe, pb], ignore_index=True)
    fn = make_price_forward_return_fn(prices)

    event_dates = [date(2024, 1, 15), date(2024, 2, 1), date(2024, 2, 15)]
    events = [(d, exposed_ids) for d in event_dates]
    res = event_study(events, fn, name="planted", horizons=(1, 5, 21), baseline_assets=base_ids)

    assert isinstance(res, EventStudyResult)
    assert res.n_events == 3
    assert res.n_pairs > 0
    # exposed have clear positive forward return at the longer horizon
    assert res.mean_fwd_return[21] > 0.02
    # abnormal vs the flat baseline is positive
    assert res.mean_abnormal[21] > 0.0
    assert res.t_stat[21] > 2.0


def test_event_study_flat_no_signal():
    ids = [f"X{i}" for i in range(8)]
    prices = _synthetic_prices(ids, drift=0.0, seed=5)
    fn = make_price_forward_return_fn(prices)
    events = [(date(2024, 1, 20), ids), (date(2024, 2, 5), ids)]
    res = event_study(events, fn, horizons=(1, 5), baseline_assets=None)
    # no drift => mean forward return small, t-stat not significant
    assert abs(res.mean_fwd_return[5]) < 0.02
    assert abs(res.t_stat[5]) < 2.0


def test_event_study_drops_pairs_without_forward_window():
    ids = ["A"]
    prices = _synthetic_prices(ids, n=30, seed=3)
    fn = make_price_forward_return_fn(prices)
    # event near the end leaves no 21d forward window
    last = prices["trade_date"].max().date()
    res = event_study([(last, ids)], fn, horizons=(1, 21))
    assert np.isnan(res.mean_fwd_return[21])


def test_event_study_empty_events():
    fn = make_price_forward_return_fn(_synthetic_prices(["A"]))
    res = event_study([], fn, horizons=(1, 5))
    assert res.n_events == 0
    assert res.n_pairs == 0
    assert "empty study" in " ".join(res.notes)
