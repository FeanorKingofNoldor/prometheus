"""Tests for prometheus.meta.drift_monitor + the drift alert rule."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Any
from unittest.mock import patch

from prometheus.meta import drift_monitor, notifications

# ── Fake DB ──────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db
        self._result: list[tuple] = []
        self.rowcount: int = 0

    def execute(self, sql: str, args: Any = ()) -> None:
        norm = " ".join(sql.split()).upper()

        if norm.startswith("SELECT DISTINCT STRATEGY_ID"):
            ids = sorted({
                r["strategy_id"] for r in self._db.backtest_runs
                if r["metrics"] is not None
            })
            self._result = [(s,) for s in ids]
            return

        if norm.startswith("INSERT INTO BACKTEST_LIVE_DRIFT"):
            key = (args[0], args[1], args[2])  # date, strategy, horizon
            existing = next(
                (r for r in self._db.drift_rows
                 if (r["as_of_date"], r["strategy_id"], r["horizon_days"]) == key),
                None,
            )
            payload = {
                "as_of_date": args[0], "strategy_id": args[1],
                "horizon_days": args[2], "n_live_outcomes": args[3],
                "backtest_run_id": args[4],
                "live_sharpe": args[5], "backtest_sharpe": args[6],
                "sharpe_delta": args[7],
                "live_return": args[8], "backtest_return": args[9],
                "return_delta": args[10],
                "live_max_drawdown": args[11], "backtest_max_drawdown": args[12],
                "max_drawdown_delta": args[13],
                "severity": args[14], "notes": args[15],
                "drift_id": len(self._db.drift_rows) + 1,
            }
            if existing is not None:
                existing.update(payload)
            else:
                self._db.drift_rows.append(payload)
            self.rowcount = 1
            return

        if "FROM BACKTEST_RUNS" in norm:
            (strategy_id,) = args
            matching = [r for r in self._db.backtest_runs
                        if r["strategy_id"] == strategy_id and r["metrics"] is not None]
            matching.sort(key=lambda r: r["created_at"], reverse=True)
            if matching:
                self._result = [(matching[0]["run_id"], matching[0]["metrics"])]
            else:
                self._result = []
            return

        if "FROM BACKTEST_LIVE_DRIFT" in norm:
            as_of = args[0]
            self._result = [
                (r["drift_id"], r["strategy_id"], r["horizon_days"],
                 r["sharpe_delta"], r["live_sharpe"], r["backtest_sharpe"],
                 r["severity"], r["notes"])
                for r in self._db.drift_rows
                if r["as_of_date"] == as_of
                and r["severity"] in ("warning", "critical")
            ]
            return

        if norm.startswith("INSERT INTO NOTIFICATIONS"):
            (as_of, kind, severity, title, body,
             src_table, src_id, link, md) = args
            key = (as_of, kind, src_id)
            if key in self._db._seen_keys:
                self.rowcount = 0
                return
            self._db._seen_keys.add(key)
            self._db.notifications.append({
                "as_of_date": as_of, "kind": kind, "severity": severity,
                "title": title, "body": body, "source_id": src_id,
            })
            self.rowcount = 1
            return

        raise AssertionError(f"unhandled SQL: {norm[:80]}")

    def fetchone(self) -> tuple | None:
        return self._result[0] if self._result else None

    def fetchall(self) -> list[tuple]:
        return list(self._result)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeConnection:
    def __init__(self, db: "_FakeDb") -> None:
        self._db = db

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._db)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None


class _FakeDb:
    def __init__(self) -> None:
        self.backtest_runs: list[dict[str, Any]] = []
        self.drift_rows: list[dict[str, Any]] = []
        self.notifications: list[dict[str, Any]] = []
        self._seen_keys: set[tuple] = set()

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConnection(self)


# ── Stub LivePerformanceTracker ─────────────────────────────────────


class _LivePerfStub:
    """Returns canned compute_rolling_performance output."""

    def __init__(self, n=50, sharpe=1.2, avg_return=0.012, max_drawdown=0.08):
        self._n = n
        self._sharpe = sharpe
        self._avg_return = avg_return
        self._max_drawdown = max_drawdown

    def __call__(self, db_manager):
        return self

    def compute_rolling_performance(self, **_kw):
        return {
            "n": self._n,
            "sharpe": self._sharpe,
            "avg_return": self._avg_return,
            "max_drawdown": self._max_drawdown,
            "win_rate": 0.55,
            "total_pnl": 1500.0,
            "by_strategy": [],
        }


# ── Severity classification ─────────────────────────────────────────


def test_classify_returns_info_when_sample_size_below_minimum():
    severity, notes = drift_monitor._classify(
        live_n=10, min_live_outcomes=30,
        backtest_present=True, sharpe_delta=-0.8,
    )
    assert severity == drift_monitor.SEVERITY_INFO
    assert "below" in notes


def test_classify_returns_info_when_backtest_missing():
    severity, notes = drift_monitor._classify(
        live_n=100, min_live_outcomes=30,
        backtest_present=False, sharpe_delta=None,
    )
    assert severity == drift_monitor.SEVERITY_INFO
    assert "backtest" in notes


def test_classify_critical_at_or_above_half_sharpe_delta():
    severity, _ = drift_monitor._classify(
        live_n=100, min_live_outcomes=30,
        backtest_present=True, sharpe_delta=-0.7,
    )
    assert severity == drift_monitor.SEVERITY_CRITICAL

    severity, _ = drift_monitor._classify(
        live_n=100, min_live_outcomes=30,
        backtest_present=True, sharpe_delta=0.5,
    )
    assert severity == drift_monitor.SEVERITY_CRITICAL


def test_classify_warning_in_mid_band():
    severity, _ = drift_monitor._classify(
        live_n=100, min_live_outcomes=30,
        backtest_present=True, sharpe_delta=-0.3,
    )
    assert severity == drift_monitor.SEVERITY_WARNING


def test_classify_info_for_small_delta():
    severity, _ = drift_monitor._classify(
        live_n=100, min_live_outcomes=30,
        backtest_present=True, sharpe_delta=0.1,
    )
    assert severity == drift_monitor.SEVERITY_INFO


# ── End-to-end drift run ────────────────────────────────────────────


def test_drift_run_persists_one_row_per_strategy_horizon():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.backtest_runs.append({
        "strategy_id": "US_EQ_CORE_LONG_EQ",
        "run_id": "bt-1", "metrics": {
            "annualised_sharpe": 1.3, "cumulative_return": 0.18,
            "max_drawdown": -0.07,
        },
        "created_at": 1,
    })

    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        result = drift_monitor.run_daily_drift_check(
            db, today, strategies=["US_EQ_CORE_LONG_EQ"], horizons=[21],
        )

    assert len(result.rows) == 1
    row = db.drift_rows[0]
    assert row["strategy_id"] == "US_EQ_CORE_LONG_EQ"
    assert row["live_sharpe"] == 1.2
    assert row["backtest_sharpe"] == 1.3
    assert abs(row["sharpe_delta"] - (1.2 - 1.3)) < 1e-9
    # |delta| = 0.1 → info
    assert row["severity"] == "info"


def test_drift_run_critical_severity_when_large_underperformance():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.backtest_runs.append({
        "strategy_id": "S",
        "run_id": "bt-1", "metrics": {
            "annualised_sharpe": 2.0, "cumulative_return": 0.30,
            "max_drawdown": -0.10,
        },
        "created_at": 1,
    })

    with patch.object(drift_monitor, "LivePerformanceTracker",
                      _LivePerfStub(sharpe=0.5)):
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["S"], horizons=[21],
        )

    assert db.drift_rows[0]["severity"] == "critical"


def test_drift_run_info_severity_when_insufficient_live_data():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.backtest_runs.append({
        "strategy_id": "S",
        "run_id": "bt-1", "metrics": {"annualised_sharpe": 1.0,
                                       "cumulative_return": 0.1,
                                       "max_drawdown": -0.05},
        "created_at": 1,
    })

    with patch.object(drift_monitor, "LivePerformanceTracker",
                      _LivePerfStub(n=5, sharpe=-0.5)):
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["S"], horizons=[21],
            min_live_outcomes=30,
        )

    assert db.drift_rows[0]["severity"] == "info"
    assert "sample size 5 below 30" in db.drift_rows[0]["notes"]


def test_drift_run_handles_strategy_with_no_backtest_gracefully():
    db = _FakeDb()
    today = date(2026, 5, 25)
    # No backtest_runs entry for this strategy

    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["NEW_STRAT"], horizons=[21],
        )

    row = db.drift_rows[0]
    assert row["backtest_sharpe"] is None
    assert row["sharpe_delta"] is None
    assert row["severity"] == "info"
    assert "no recent backtest_runs" in row["notes"]


def test_drift_run_upserts_existing_row_for_same_key():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.backtest_runs.append({
        "strategy_id": "S",
        "run_id": "bt-1", "metrics": {"annualised_sharpe": 1.0,
                                       "cumulative_return": 0.1,
                                       "max_drawdown": -0.05},
        "created_at": 1,
    })

    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["S"], horizons=[21],
        )
    # Run again with different live data → should overwrite
    with patch.object(drift_monitor, "LivePerformanceTracker",
                      _LivePerfStub(sharpe=0.0)):
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["S"], horizons=[21],
        )

    assert len(db.drift_rows) == 1
    assert db.drift_rows[0]["live_sharpe"] == 0.0


def test_drift_run_handles_multiple_horizons():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.backtest_runs.append({
        "strategy_id": "S",
        "run_id": "bt-1", "metrics": {"annualised_sharpe": 1.0,
                                       "cumulative_return": 0.1,
                                       "max_drawdown": -0.05},
        "created_at": 1,
    })

    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        result = drift_monitor.run_daily_drift_check(
            db, today, strategies=["S"], horizons=[5, 21, 63],
        )

    assert len(result.rows) == 3
    horizons = {r.horizon_days for r in result.rows}
    assert horizons == {5, 21, 63}


def test_drift_normalizes_backtest_max_drawdown_to_positive():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.backtest_runs.append({
        "strategy_id": "S",
        "run_id": "bt-1", "metrics": {"annualised_sharpe": 1.0,
                                       "cumulative_return": 0.1,
                                       "max_drawdown": -0.15},  # negative
        "created_at": 1,
    })

    with patch.object(drift_monitor, "LivePerformanceTracker",
                      _LivePerfStub(max_drawdown=0.08)):  # live = positive
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["S"], horizons=[21],
        )

    row = db.drift_rows[0]
    # Backtest was -0.15, should be normalized to +0.15
    assert row["backtest_max_drawdown"] == 0.15
    # Delta: live 0.08 - backtest 0.15 = -0.07 (less drawdown is good)
    assert abs(row["max_drawdown_delta"] - (0.08 - 0.15)) < 1e-9


# ── Strategy filter threading ───────────────────────────────────────


class _RecordingLivePerfStub(_LivePerfStub):
    """Records the kwargs of every compute_rolling_performance call."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.calls: list[dict] = []

    def compute_rolling_performance(self, **kw):
        self.calls.append(kw)
        return super().compute_rolling_performance(**kw)


def test_drift_threads_strategy_id_into_live_performance():
    """Each strategy's live side must be filtered to that strategy —
    not the global portfolio Sharpe diffed against every backtest."""
    db = _FakeDb()
    today = date(2026, 7, 3)
    for s in ("S1", "S2"):
        db.backtest_runs.append({
            "strategy_id": s, "run_id": f"bt-{s}",
            "metrics": {"annualised_sharpe": 1.0, "cumulative_return": 0.1,
                        "max_drawdown": -0.05},
            "created_at": 1,
        })

    stub = _RecordingLivePerfStub()
    with patch.object(drift_monitor, "LivePerformanceTracker", stub):
        drift_monitor.run_daily_drift_check(
            db, today, strategies=["S1", "S2"], horizons=[21],
        )

    assert {c.get("strategy_id") for c in stub.calls} == {"S1", "S2"}


def test_discovery_defaults_to_allowlist_and_excludes_grid_prefixes():
    db = _FakeDb()
    today = date(2026, 7, 3)
    for s in ("US_CORE_LONG_EQ", "BT_GRID_0042", "CPP_SWEEP_7",
              "LAMBDA_FACT_US_EQ", "PERF_TEST_LOAD", "SOME_OTHER"):
        db.backtest_runs.append({
            "strategy_id": s, "run_id": f"bt-{s}",
            "metrics": {"annualised_sharpe": 1.0, "cumulative_return": 0.1,
                        "max_drawdown": -0.05},
            "created_at": 1,
        })

    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        result = drift_monitor.run_daily_drift_check(db, today, horizons=[21])

    # Only the allowlisted live strategy generates drift rows.
    assert {r.strategy_id for r in result.rows} == {"US_CORE_LONG_EQ"}
    assert {r["strategy_id"] for r in db.drift_rows} == {"US_CORE_LONG_EQ"}


def test_discovery_custom_allowlist_still_blocks_grid_prefixes():
    db = _FakeDb()
    today = date(2026, 7, 3)
    for s in ("SOME_OTHER", "BT_GRID_0042", "US_CORE_LONG_EQ"):
        db.backtest_runs.append({
            "strategy_id": s, "run_id": f"bt-{s}",
            "metrics": {"annualised_sharpe": 1.0, "cumulative_return": 0.1,
                        "max_drawdown": -0.05},
            "created_at": 1,
        })

    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        result = drift_monitor.run_daily_drift_check(
            db, today, horizons=[21],
            strategy_allowlist=["SOME_OTHER", "BT_GRID_0042"],
        )

    # BT_ prefix is excluded even when explicitly allowlisted.
    assert {r.strategy_id for r in result.rows} == {"SOME_OTHER"}


def test_explicit_strategies_bypass_allowlist():
    db = _FakeDb()
    today = date(2026, 7, 3)
    with patch.object(drift_monitor, "LivePerformanceTracker", _LivePerfStub()):
        result = drift_monitor.run_daily_drift_check(
            db, today, strategies=["ANYTHING_GOES"], horizons=[21],
        )
    assert {r.strategy_id for r in result.rows} == {"ANYTHING_GOES"}


# ── Drift alert rule ────────────────────────────────────────────────


def test_drift_alert_rule_fires_on_warning_and_critical():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.drift_rows.append({
        "drift_id": 1, "as_of_date": today, "strategy_id": "S1",
        "horizon_days": 21, "sharpe_delta": -0.3,
        "live_sharpe": 0.8, "backtest_sharpe": 1.1,
        "severity": "warning", "notes": "mild drift",
    })
    db.drift_rows.append({
        "drift_id": 2, "as_of_date": today, "strategy_id": "S2",
        "horizon_days": 21, "sharpe_delta": -0.7,
        "live_sharpe": 0.4, "backtest_sharpe": 1.1,
        "severity": "critical", "notes": "investigate",
    })
    db.drift_rows.append({
        "drift_id": 3, "as_of_date": today, "strategy_id": "S3",
        "horizon_days": 21, "sharpe_delta": -0.1,
        "live_sharpe": 1.0, "backtest_sharpe": 1.1,
        "severity": "info", "notes": "no drift",
    })

    notifications.evaluate_daily_alerts(db, today)

    drift_notifs = [n for n in db.notifications if n["kind"] == "drift_alert"]
    assert len(drift_notifs) == 2
    severities = {n["severity"] for n in drift_notifs}
    assert severities == {"warning", "critical"}


def test_drift_alert_does_not_fire_when_only_info_severity():
    db = _FakeDb()
    today = date(2026, 5, 25)
    db.drift_rows.append({
        "drift_id": 1, "as_of_date": today, "strategy_id": "S",
        "horizon_days": 21, "sharpe_delta": -0.1,
        "live_sharpe": 1.0, "backtest_sharpe": 1.1,
        "severity": "info", "notes": "no drift",
    })
    notifications.evaluate_daily_alerts(db, today)
    drift_notifs = [n for n in db.notifications if n["kind"] == "drift_alert"]
    assert drift_notifs == []
