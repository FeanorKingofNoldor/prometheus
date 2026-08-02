"""Tests for the daemon's orphan-aware retry guard and active-market resolution.

Timed-out job threads keep running (Python cannot cancel threads); the
scheduler must not start another attempt of the same job while the orphan
is alive — for run_execution that would submit the same order batch twice
concurrently.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from prometheus.orchestration.dag import JobMetadata, JobStatus
from prometheus.orchestration.market_aware_daemon import (
    DEFAULT_ACTIVE_MARKETS,
    MarketAwareDaemon,
    MarketAwareDaemonConfig,
    resolve_active_markets,
    should_retry_job,
)


def _job(job_id: str = "us_eq_run_execution_2026-07-02", max_retries: int = 3) -> JobMetadata:
    return JobMetadata(
        job_id=job_id,
        job_type="run_execution",
        market_id="US_EQ",
        max_retries=max_retries,
    )


def _failed_execution(job_id: str, attempt: int = 1) -> MagicMock:
    execution = MagicMock()
    execution.execution_id = "exec-1"
    execution.job_id = job_id
    execution.status = JobStatus.FAILED
    execution.attempt_number = attempt
    execution.error_message = "boom"
    return execution


def _daemon() -> MarketAwareDaemon:
    config = MarketAwareDaemonConfig(markets=["US_EQ"])
    return MarketAwareDaemon(config, db_manager=MagicMock())


# ---------------------------------------------------------------------------
# should_retry_job
# ---------------------------------------------------------------------------


def test_should_retry_job_allows_normal_retry():
    job = _job()
    assert should_retry_job(job, _failed_execution(job.job_id, attempt=1)) is True


def test_should_retry_job_refuses_while_orphan_alive():
    job = _job()
    execution = _failed_execution(job.job_id, attempt=1)
    assert should_retry_job(job, execution, orphaned_thread_alive=True) is False
    # once the orphan is gone, the same execution is retryable again
    assert should_retry_job(job, execution, orphaned_thread_alive=False) is True


def test_should_retry_job_still_respects_retry_exhaustion():
    job = _job(max_retries=2)
    execution = _failed_execution(job.job_id, attempt=2)
    assert should_retry_job(job, execution) is False


# ---------------------------------------------------------------------------
# Orphan tracking on the daemon
# ---------------------------------------------------------------------------


def test_has_live_orphan_and_reap_lifecycle():
    daemon = _daemon()
    job_id = "us_eq_run_execution_2026-07-02"
    release = threading.Event()
    thread = threading.Thread(target=release.wait, daemon=True)
    thread.start()

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    daemon._orphaned_threads[job_id] = (thread, now)

    try:
        assert daemon._has_live_orphan(job_id) is True
        # reap keeps live orphans tracked
        daemon._reap_orphaned_threads()
        assert job_id in daemon._orphaned_threads
    finally:
        release.set()
        thread.join(timeout=5)

    # thread exited — reap lifts the block
    assert daemon._has_live_orphan(job_id) is False
    daemon._reap_orphaned_threads()
    assert job_id not in daemon._orphaned_threads


def test_dispatch_skips_job_with_live_orphan():
    """A live orphaned thread must block re-dispatch of its job_id.

    (Lane-model successor of the old _process_market test: the dispatcher
    consults the same orphan guard before selecting an execution.)
    """
    daemon = _daemon()
    job = _job()
    release = threading.Event()
    thread = threading.Thread(target=release.wait, daemon=True)
    thread.start()
    daemon._orphaned_threads[job.job_id] = (
        thread, datetime.now(timezone.utc),
    )

    from prometheus.orchestration import market_aware_daemon as mad

    mock_dag = MagicMock()
    mock_dag.get_runnable_jobs.return_value = [job]
    lane = mad.MarketLane(market_id="US_EQ")

    try:
        with patch.object(mad, "get_latest_job_execution") as mock_latest, \
             patch.object(mad, "create_job_execution") as mock_create, \
             patch.object(mad, "update_job_execution_status") as mock_update, \
             patch.object(daemon, "_get_completed_jobs", return_value=set()), \
             patch.object(daemon, "_get_running_job_ids", return_value=set()):
            daemon._dispatch_next(
                lane, mock_dag, "dag-1",
                MagicMock(), date(2026, 7, 2), datetime.now(timezone.utc),
            )

        # The orphaned job must not be (re)started in any form.
        mock_latest.assert_not_called()
        mock_create.assert_not_called()
        mock_update.assert_not_called()
        assert lane.handle is None
    finally:
        release.set()
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Active-market resolution
# ---------------------------------------------------------------------------


def test_resolve_active_markets_cli_wins(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_ACTIVE_MARKETS", "US_EQ,EU_EQ")
    assert resolve_active_markets(["HK_EQ"]) == ["HK_EQ"]


def test_resolve_active_markets_env_var(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_ACTIVE_MARKETS", "us_eq, eu_eq ,IRIS,")
    assert resolve_active_markets(None) == ["US_EQ", "EU_EQ", "IRIS"]


def test_resolve_active_markets_default(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_ACTIVE_MARKETS", raising=False)
    assert resolve_active_markets(None) == list(DEFAULT_ACTIVE_MARKETS)
    assert resolve_active_markets([]) == list(DEFAULT_ACTIVE_MARKETS)
    assert "US_EQ" in DEFAULT_ACTIVE_MARKETS
    assert "EU_EQ" not in DEFAULT_ACTIVE_MARKETS
