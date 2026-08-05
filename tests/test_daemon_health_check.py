"""Pin _run_health_check fail-vs-warn classification.

Critical data-integrity anomalies fail the run; soft anomalies only warn.
No real DB — connections are faked.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date

from prometheus.orchestration.market_aware_daemon import _run_health_check


class _FakeCursor:
    def __init__(self, answers: dict[str, int]) -> None:
        self._answers = answers
        self._last = 0

    def execute(self, sql: str, params=()) -> None:
        s = sql.lower()
        if "close <= 0" in s:
            self._last = self._answers["nonpos_prices"]
        elif "count(distinct instrument_id) from prices_daily" in s:
            self._last = self._answers["prices"]
        elif "from target_portfolios" in s:
            self._last = self._answers["targets"]
        elif "from orders" in s:
            self._last = self._answers["orders"]
        elif "from sector_health_daily" in s:
            self._last = self._answers["shi"]
        else:
            self._last = 0

    def fetchone(self):
        return (self._last,)

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *a) -> bool:
        return False


class _FakeConn:
    def __init__(self, answers: dict[str, int]) -> None:
        self._answers = answers

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._answers)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *a) -> bool:
        return False


class _FakeDbManager:
    def __init__(self, answers: dict[str, int]) -> None:
        self._answers = answers

    @contextmanager
    def get_historical_connection(self):
        yield _FakeConn(self._answers)

    @contextmanager
    def get_runtime_connection(self):
        yield _FakeConn(self._answers)


class _FakeRun:
    run_id = "run-1"

    class _Phase:
        value = "COMPLETED"

    phase = _Phase()


def _check(answers, monkeypatch, tmp_path):
    monkeypatch.setenv("PROMETHEUS_HEALTH_REPORT_DIR", str(tmp_path))
    # The repo .env may carry PROMETHEUS_EXECUTION_HALT (loaded into the
    # process by apatheon's import-time load_dotenv when the real package
    # is importable) — pin it off so the zero-orders check is under test.
    monkeypatch.delenv("PROMETHEUS_EXECUTION_HALT", raising=False)
    base = {"prices": 660, "nonpos_prices": 0, "targets": 50, "orders": 30, "shi": 11}
    base.update(answers)
    return _run_health_check(
        _FakeDbManager(base), _FakeRun(), date(2026, 6, 10), "US_EQ"
    )


def test_healthy_run_passes(monkeypatch, tmp_path):
    healthy, err = _check({}, monkeypatch, tmp_path)
    assert healthy is True
    assert err is None


def test_orders_query_uses_two_day_window(monkeypatch, tmp_path):
    """Catch-up runs timestamp orders the NEXT morning (or Saturday for a
    Friday run), so the orders check must scan [as_of, as_of+2d), not
    timestamp::date = as_of — the same-date filter FAILED the healthy
    2026-07-31 run whose orders landed on 2026-08-01."""
    captured: list[str] = []

    class _SpyCursor(_FakeCursor):
        def execute(self, sql: str, params=()) -> None:
            captured.append(" ".join(sql.split()).lower())
            super().execute(sql, params)

    class _SpyConn(_FakeConn):
        def cursor(self):
            return _SpyCursor(self._answers)

    class _SpyDb(_FakeDbManager):
        @contextmanager
        def get_historical_connection(self):
            yield _SpyConn(self._answers)

        @contextmanager
        def get_runtime_connection(self):
            yield _SpyConn(self._answers)

    monkeypatch.setenv("PROMETHEUS_HEALTH_REPORT_DIR", str(tmp_path))
    base = {"prices": 660, "nonpos_prices": 0, "targets": 50, "orders": 30, "shi": 11}
    healthy, _ = _run_health_check(_SpyDb(base), _FakeRun(), date(2026, 6, 10), "US_EQ")
    assert healthy is True

    orders_sql = [s for s in captured if "from orders" in s]
    assert orders_sql, "orders check did not run"
    assert "interval '2 days'" in orders_sql[0]
    assert "timestamp::date =" not in orders_sql[0]


def _make_execution(mad, job, as_of, *, attempt):
    from datetime import datetime
    now = datetime(2026, 8, 2, 12, 0)
    return mad.JobExecution(
        execution_id="x-test",
        job_id=job.job_id,
        job_type=job.job_type,
        dag_id=f"US_EQ_{as_of}",
        market_id="US_EQ",
        as_of_date=as_of,
        status=mad.JobStatus.RUNNING,
        started_at=now,
        completed_at=None,
        attempt_number=attempt,
        error_message=None,
        error_details=None,
        created_at=now,
        updated_at=now,
    )


def test_finalize_retry_on_failed_run_does_not_report_success(monkeypatch):
    """A finalize retry on a run the health check already FAILED must fail
    the job attempt too — job_executions and engine_runs must agree."""
    import prometheus.orchestration.market_aware_daemon as mad
    from prometheus.pipeline.state import RunPhase

    class _Run:
        run_id = "run-1"
        phase = RunPhase.FAILED

    monkeypatch.setattr(mad, "_get_or_create_engine_run", lambda *a, **k: _Run())

    job = mad.JobMetadata(
        job_id="us_eq_finalize_2026-07-31",
        job_type="finalize",
        market_id="US_EQ",
    )
    execution = _make_execution(mad, job, date(2026, 7, 31), attempt=2)
    success, error = mad.execute_job(object(), job, execution)
    assert success is False
    assert "FAILED" in (error or "")


def test_finalize_rerun_on_completed_run_is_idempotent_success(monkeypatch):
    import prometheus.orchestration.market_aware_daemon as mad
    from prometheus.pipeline.state import RunPhase

    class _Run:
        run_id = "run-1"
        phase = RunPhase.COMPLETED

    monkeypatch.setattr(mad, "_get_or_create_engine_run", lambda *a, **k: _Run())

    job = mad.JobMetadata(
        job_id="us_eq_finalize_2026-07-30",
        job_type="finalize",
        market_id="US_EQ",
    )
    execution = _make_execution(mad, job, date(2026, 7, 30), attempt=1)
    success, error = mad.execute_job(object(), job, execution)
    assert success is True
    assert error is None


def test_zero_prices_fails(monkeypatch, tmp_path):
    healthy, err = _check({"prices": 0}, monkeypatch, tmp_path)
    assert healthy is False
    assert "ZERO PRICES" in err


def test_zero_targets_fails(monkeypatch, tmp_path):
    healthy, err = _check({"targets": 0, "orders": 0}, monkeypatch, tmp_path)
    assert healthy is False
    assert "NO TARGET PORTFOLIO" in err


def test_zero_orders_with_targets_fails(monkeypatch, tmp_path):
    healthy, err = _check({"targets": 50, "orders": 0}, monkeypatch, tmp_path)
    assert healthy is False
    assert "NO ORDERS" in err


def test_nonpositive_prices_fails(monkeypatch, tmp_path):
    healthy, err = _check({"nonpos_prices": 3}, monkeypatch, tmp_path)
    assert healthy is False
    assert "NON-POSITIVE PRICES" in err


def test_low_price_coverage_only_warns(monkeypatch, tmp_path):
    healthy, err = _check({"prices": 200}, monkeypatch, tmp_path)
    assert healthy is True
    assert err is None


def test_missing_shi_only_warns(monkeypatch, tmp_path):
    healthy, err = _check({"shi": 0}, monkeypatch, tmp_path)
    assert healthy is True
    assert err is None
