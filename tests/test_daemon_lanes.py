"""Tests for the lane-based daemon scheduler (market_aware_daemon).

Covers the lane model introduced by the scheduler refactor:
- per-market strict serialization with cross-market concurrency
- IBKR session-token exclusivity across markets
- deadline timeouts that orphan the worker thread and keep the lane
  occupied until the reaper observes the thread dead
- exactly one job_executions INSERT per fresh attempt (retries reuse
  the row via increment_job_execution_attempt)
- deferred per-lane date rollover (busy lanes keep their old DAG)
- per-lane morning catch-up served through the normal dispatcher
- worker -> scheduler wakeups, shutdown semantics, derived views

Harness conventions (mirrors tests/test_daemon_orphan_guard.py):
- fake clock via the daemon's ``_now()`` seam (instance-attr override)
- ``execute_job`` stubbed at module level with Event-controlled fakes
- MagicMock db_manager; the job_executions persistence functions
  (create/get/update/increment) replaced by an in-memory FakeStore
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from apatheon.core.market_state import MarketState

from prometheus.orchestration import market_aware_daemon as mad
from prometheus.orchestration.dag import DAG, JobMetadata, JobPriority, JobStatus
from prometheus.orchestration.market_aware_daemon import (
    CatchupState,
    JobHandle,
    MarketAwareDaemon,
    MarketAwareDaemonConfig,
)

D = date(2026, 7, 2)
T0 = datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _job(job_id: str, job_type: str = "run_signals", market_id: str = "US_EQ", **kw) -> JobMetadata:
    kw.setdefault("required_state", None)
    return JobMetadata(job_id=job_id, job_type=job_type, market_id=market_id, **kw)


def _dag(market_id: str, jobs: list[JobMetadata], as_of_date: date = D) -> DAG:
    return DAG(
        dag_id=f"{market_id}_{as_of_date.isoformat()}",
        market_id=market_id,
        as_of_date=as_of_date,
        jobs={j.job_id: j for j in jobs},
    )


class FakeClock:
    """Controllable clock plugged into the daemon's ``_now()`` seam."""

    def __init__(self, start: datetime = T0):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _daemon(markets=("US_EQ",), clock: FakeClock | None = None) -> MarketAwareDaemon:
    config = MarketAwareDaemonConfig(markets=list(markets))
    daemon = MarketAwareDaemon(config, db_manager=MagicMock())
    if clock is not None:
        daemon._now = clock  # instance attr shadows the method (fake-clock seam)
    return daemon


class FakeStore:
    """In-memory stand-in for the job_executions persistence functions.

    Stateful across scheduler cycles so completed-set / latest-execution
    reads behave like the real table (unlike single-shot return_value
    mocks, which would let SUCCESS jobs be re-created forever).
    """

    def __init__(self):
        self.rows: list[SimpleNamespace] = []
        self.created: list[tuple[str, str]] = []  # (job_id, dag_id) per INSERT
        self.increments: list[str] = []  # execution_id per retry bump
        self.updates: list[tuple[str, JobStatus, str | None]] = []
        self._seq = 0

    def _new_row(self, job_id: str, dag_id: str, status: JobStatus, attempt: int):
        self._seq += 1
        row = SimpleNamespace(
            execution_id=f"exec-{self._seq}",
            job_id=job_id,
            dag_id=dag_id,
            status=status,
            attempt_number=attempt,
            error_message=None,
        )
        self.rows.append(row)
        return row

    def seed(self, job: JobMetadata, dag_id: str, *, status=JobStatus.PENDING, attempt=1):
        """Pre-populate an execution row without counting it as a fresh INSERT."""
        return self._new_row(job.job_id, dag_id, status, attempt)

    # --- patched module-level functions (db arg ignored) ---

    def create(self, db, job, dag_id, as_of_date):
        self.created.append((job.job_id, dag_id))
        return self._new_row(job.job_id, dag_id, JobStatus.PENDING, 1)

    def get_latest(self, db, job_id, dag_id):
        matches = [r for r in self.rows if r.job_id == job_id and r.dag_id == dag_id]
        return matches[-1] if matches else None

    def get_dag_executions(self, db, dag_id):
        return [r for r in self.rows if r.dag_id == dag_id]

    def update(self, db, execution_id, status, error_message=None, error_details=None):
        self.updates.append((execution_id, status, error_message))
        for r in self.rows:
            if r.execution_id == execution_id:
                r.status = status
                r.error_message = error_message

    def increment(self, db, execution_id):
        self.increments.append(execution_id)
        for r in self.rows:
            if r.execution_id == execution_id:
                r.attempt_number += 1
                r.status = JobStatus.PENDING
                r.error_message = None


@contextmanager
def _patched_store(store: FakeStore):
    with patch.object(mad, "get_dag_executions", side_effect=store.get_dag_executions), \
         patch.object(mad, "get_latest_job_execution", side_effect=store.get_latest), \
         patch.object(mad, "create_job_execution", side_effect=store.create), \
         patch.object(mad, "update_job_execution_status", side_effect=store.update), \
         patch.object(mad, "increment_job_execution_attempt", side_effect=store.increment):
        yield


class BlockingJobs:
    """execute_job stand-in: every call blocks until ``release`` is set."""

    def __init__(self, results: dict | None = None):
        self.release = threading.Event()
        self.calls: list[str] = []
        self.results = dict(results or {})

    def __call__(self, db, job, execution, *, options_mode="paper"):
        self.calls.append(job.job_id)
        assert self.release.wait(timeout=15), "test forgot to release BlockingJobs"
        return self.results.get(job.job_id, (True, None))


def _instant_exec(results: dict | None = None):
    """execute_job stand-in returning immediately (default success)."""
    results = dict(results or {})
    calls: list[str] = []

    def fake(db, job, execution, *, options_mode="paper"):
        calls.append(job.job_id)
        return results.get(job.job_id, (True, None))

    fake.calls = calls  # type: ignore[attr-defined]
    return fake


def _seed_calendars(daemon: MarketAwareDaemon) -> None:
    for market_id in daemon.config.markets:
        daemon._calendars[market_id] = MagicMock()


def _join_all(daemon: MarketAwareDaemon, timeout: float = 10.0) -> None:
    for lane in daemon.lanes.values():
        h = lane.handle
        if h is not None:
            h.thread.join(timeout=timeout)


def _fabricated_handle(job: JobMetadata, execution_id: str, *, orphaned=False, holds_ibkr=False) -> JobHandle:
    return JobHandle(
        job=job,
        execution_id=execution_id,
        dag_id="dag-fab",
        market_id=job.market_id or "US_EQ",
        as_of_date=D,
        thread=threading.Thread(target=lambda: None, daemon=True),
        started_at=T0,
        deadline=T0 + timedelta(seconds=job.timeout_seconds),
        done=threading.Event(),
        result=[],
        attempt_number=1,
        max_retries=job.max_retries,
        orphaned=orphaned,
        holds_ibkr=holds_ibkr,
    )


# ---------------------------------------------------------------------------
# 1. Cross-market concurrency
# ---------------------------------------------------------------------------


def test_two_markets_run_concurrently():
    clock = FakeClock()
    daemon = _daemon(["US_EQ", "EU_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()
    us_dag = _dag("US_EQ", [_job("us_a")])
    eu_dag = _dag("EU_EQ", [_job("eu_a", market_id="EU_EQ")])
    daemon.active_dags["US_EQ"] = (us_dag, us_dag.dag_id)
    daemon.active_dags["EU_EQ"] = (eu_dag, eu_dag.dag_id)
    _seed_calendars(daemon)

    with _patched_store(store), \
         patch.object(mad, "execute_job", new=blocker), \
         patch.object(mad, "get_market_state", return_value=MarketState.POST_CLOSE):
        try:
            daemon._run_cycle(D)

            # Both lanes have a live in-flight handle after ONE cycle.
            assert daemon.lanes["US_EQ"].handle is not None
            assert daemon.lanes["EU_EQ"].handle is not None
            assert daemon.lanes["US_EQ"].handle.job.job_id == "us_a"
            assert daemon.lanes["EU_EQ"].handle.job.job_id == "eu_a"
            assert len(daemon.running_jobs) == 2
        finally:
            blocker.release.set()
            _join_all(daemon)


# ---------------------------------------------------------------------------
# 2. Same-market strict serialization
# ---------------------------------------------------------------------------


def test_same_market_strictly_serialized():
    clock = FakeClock()
    daemon = _daemon(["US_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()
    dag = _dag("US_EQ", [_job("a_first"), _job("b_second")])
    daemon.active_dags["US_EQ"] = (dag, dag.dag_id)
    _seed_calendars(daemon)

    with _patched_store(store), \
         patch.object(mad, "execute_job", new=blocker), \
         patch.object(mad, "get_market_state", return_value=MarketState.POST_CLOSE):
        daemon._run_cycle(D)
        first = daemon.lanes["US_EQ"].handle
        assert first is not None and first.job.job_id == "a_first"
        assert [j for j, _ in store.created] == ["a_first"]

        # Another cycle while the first is still running: nothing new starts.
        daemon._run_cycle(D)
        assert [j for j, _ in store.created] == ["a_first"]
        assert daemon.lanes["US_EQ"].handle is first

        blocker.release.set()
        assert first.done.wait(timeout=10)

        # Next cycle: poll observes completion (SUCCESS write, lane freed),
        # then dispatches the second job.
        daemon._run_cycle(D)
        assert (first.execution_id, JobStatus.SUCCESS, None) in store.updates
        assert [j for j, _ in store.created] == ["a_first", "b_second"]
        second = daemon.lanes["US_EQ"].handle
        assert second is not None and second.job.job_id == "b_second"
        assert second.done.wait(timeout=10)  # release already set
        _join_all(daemon)


# ---------------------------------------------------------------------------
# 3. Backoff does not head-of-line block
# ---------------------------------------------------------------------------


def test_backoff_does_not_head_of_line_block():
    clock = FakeClock()
    daemon = _daemon(["US_EQ"], clock=clock)
    store = FakeStore()
    high = _job("a_high", priority=JobPriority.CRITICAL)
    low = _job("b_low", priority=JobPriority.STANDARD)
    dag = _dag("US_EQ", [high, low])
    lane = daemon.lanes["US_EQ"]

    # Prior FAILED attempt of the higher-priority job, still in backoff.
    failed = store.seed(high, dag.dag_id, status=JobStatus.FAILED, attempt=1)
    daemon.retry_backoff[failed.execution_id] = clock.now + timedelta(minutes=30)

    with _patched_store(store), patch.object(mad, "execute_job", new=_instant_exec()):
        daemon._dispatch_next(lane, dag, dag.dag_id, MarketState.POST_CLOSE, D, clock.now)

        # The lower-priority job dispatched instead of the lane stalling.
        assert lane.handle is not None
        assert lane.handle.job.job_id == "b_low"
        assert [j for j, _ in store.created] == ["b_low"]
        assert lane.handle.done.wait(timeout=10)
        _join_all(daemon)

    # The high-priority job was neither retried nor SKIPPED — it still
    # awaits its backoff window.
    assert failed.execution_id in daemon.retry_backoff
    assert store.increments == []
    assert failed.status == JobStatus.FAILED


# ---------------------------------------------------------------------------
# 4. IBKR exclusivity across markets
# ---------------------------------------------------------------------------


def test_ibkr_exclusive_across_markets():
    clock = FakeClock()
    daemon = _daemon(["EU_EQ", "UK_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()
    eu = _dag("EU_EQ", [_job("eu_run_execution", job_type="run_execution", market_id="EU_EQ")])
    uk = _dag("UK_EQ", [_job("uk_run_execution", job_type="run_execution", market_id="UK_EQ")])
    daemon.active_dags["EU_EQ"] = (eu, eu.dag_id)
    daemon.active_dags["UK_EQ"] = (uk, uk.dag_id)
    _seed_calendars(daemon)

    with _patched_store(store), \
         patch.object(mad, "execute_job", new=blocker), \
         patch.object(mad, "get_market_state", return_value=MarketState.POST_CLOSE):
        daemon._run_cycle(D)

        # Exactly ONE IBKR job in flight; the other market waits.
        assert daemon.lanes["EU_EQ"].handle is not None
        assert daemon.lanes["EU_EQ"].handle.holds_ibkr is True
        assert daemon.lanes["UK_EQ"].handle is None
        assert daemon._ibkr_job_holder == "eu_run_execution"
        assert [j for j, _ in store.created] == ["eu_run_execution"]

        blocker.release.set()
        assert daemon.lanes["EU_EQ"].handle.done.wait(timeout=10)

        # Completion releases the token; the second market dispatches.
        daemon._run_cycle(D)
        assert daemon.lanes["EU_EQ"].handle is None
        uk_handle = daemon.lanes["UK_EQ"].handle
        assert uk_handle is not None
        assert daemon._ibkr_job_holder == "uk_run_execution"
        assert uk_handle.done.wait(timeout=10)
        _join_all(daemon)


# ---------------------------------------------------------------------------
# 5. Timeout orphans the lane
# ---------------------------------------------------------------------------


def test_timeout_orphans_lane():
    clock = FakeClock()
    daemon = _daemon(["US_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()  # never released until cleanup — "never finishes"
    job = _job("us_slow", timeout_seconds=60)
    dag = _dag("US_EQ", [job])
    lane = daemon.lanes["US_EQ"]

    with _patched_store(store), patch.object(mad, "execute_job", new=blocker):
        try:
            daemon._dispatch_next(lane, dag, dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
            handle = lane.handle
            assert handle is not None

            # Advance the fake clock past the deadline; poll detects timeout.
            clock.advance(120)
            daemon._poll_lanes(clock.now)

            assert handle.orphaned is True
            assert lane.handle is handle  # LANE STAYS OCCUPIED
            assert "us_slow" in daemon._orphaned_threads
            failed = [u for u in store.updates
                      if u[0] == handle.execution_id and u[1] == JobStatus.FAILED]
            assert len(failed) == 1
            assert "timed out" in failed[0][2]

            # The dispatcher refuses to start anything on the occupied lane.
            n_created = len(store.created)
            daemon._dispatch_next(lane, dag, dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
            assert len(store.created) == n_created
            assert lane.handle is handle

            # Repeated polls don't double-fail the orphaned execution.
            daemon._poll_lanes(clock.now)
            assert len([u for u in store.updates if u[1] == JobStatus.FAILED]) == 1

            # Reaping while the thread is still alive keeps the lane blocked.
            daemon._reap_orphaned_threads()
            assert lane.handle is handle
        finally:
            blocker.release.set()

        # Once the orphan thread actually exits, the reaper frees the lane.
        assert handle.done.wait(timeout=10)
        handle.thread.join(timeout=10)
        daemon._reap_orphaned_threads()
        assert lane.handle is None
        assert "us_slow" not in daemon._orphaned_threads


# ---------------------------------------------------------------------------
# 6. IBKR token held by orphan until reaped
# ---------------------------------------------------------------------------


def test_ibkr_token_held_by_orphan():
    clock = FakeClock()
    daemon = _daemon(["US_EQ", "EU_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()
    us_job = _job("us_run_execution", job_type="run_execution", timeout_seconds=60)
    us_dag = _dag("US_EQ", [us_job])
    eu_dag = _dag("EU_EQ", [_job("eu_run_execution", job_type="run_execution", market_id="EU_EQ")])
    us_lane, eu_lane = daemon.lanes["US_EQ"], daemon.lanes["EU_EQ"]

    with _patched_store(store), patch.object(mad, "execute_job", new=blocker):
        try:
            daemon._dispatch_next(us_lane, us_dag, us_dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
            handle = us_lane.handle
            assert handle is not None and handle.holds_ibkr

            clock.advance(120)
            daemon._poll_lanes(clock.now)
            assert handle.orphaned is True

            # The zombie may still own the gateway session — the token is
            # NOT released at timeout.
            assert daemon._ibkr_job_holder == "us_run_execution"

            # Another market's IBKR job must not dispatch while the token
            # is held by the orphan.
            daemon._dispatch_next(eu_lane, eu_dag, eu_dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
            assert eu_lane.handle is None
            assert ("eu_run_execution", eu_dag.dag_id) not in store.created
        finally:
            blocker.release.set()

        # Thread exits → reaper releases the token and frees the lane.
        assert handle.done.wait(timeout=10)
        handle.thread.join(timeout=10)
        daemon._reap_orphaned_threads()
        assert daemon._ibkr_job_holder is None
        assert us_lane.handle is None

        # Now the other market's IBKR job can run.
        daemon._dispatch_next(eu_lane, eu_dag, eu_dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
        assert eu_lane.handle is not None
        assert daemon._ibkr_job_holder == "eu_run_execution"
        assert eu_lane.handle.done.wait(timeout=10)
        _join_all(daemon)


# ---------------------------------------------------------------------------
# 7. One create_job_execution per fresh attempt
# ---------------------------------------------------------------------------


def test_one_create_execution_per_attempt():
    clock = FakeClock()
    daemon = _daemon(["US_EQ"], clock=clock)
    store = FakeStore()
    job = _job("us_flaky", max_retries=3)
    dag = _dag("US_EQ", [job])
    lane = daemon.lanes["US_EQ"]
    exec_fn = _instant_exec(results={"us_flaky": (False, "boom")})

    with _patched_store(store), patch.object(mad, "execute_job", new=exec_fn):
        # Fresh attempt: exactly one INSERT, no increments.
        daemon._dispatch_next(lane, dag, dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
        handle = lane.handle
        assert handle is not None
        assert handle.done.wait(timeout=10)
        handle.thread.join(timeout=10)
        assert len(store.created) == 1
        assert store.increments == []
        exec_id = handle.execution_id

        # Poll: FAILED write + retry backoff scheduled.
        daemon._poll_lanes(clock.now)
        assert lane.handle is None
        assert (exec_id, JobStatus.FAILED, "boom") in store.updates
        assert exec_id in daemon.retry_backoff

        # Retry after the backoff window: the SAME row is reused via one
        # increment_job_execution_attempt — no second INSERT.
        clock.advance(3 * 3600)
        daemon._dispatch_next(lane, dag, dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
        retry_handle = lane.handle
        assert retry_handle is not None
        assert retry_handle.execution_id == exec_id
        assert retry_handle.attempt_number == 2
        assert len(store.created) == 1
        assert store.increments == [exec_id]
        assert retry_handle.done.wait(timeout=10)
        _join_all(daemon)


# ---------------------------------------------------------------------------
# 8. Deferred rollover
# ---------------------------------------------------------------------------


def test_rollover_defers_busy_lane():
    clock = FakeClock()
    daemon = _daemon(["US_EQ", "EU_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()
    old_d, new_d = D, D + timedelta(days=1)
    us_dag = _dag("US_EQ", [_job("us_a")], as_of_date=old_d)
    eu_dag = _dag("EU_EQ", [_job("eu_a", market_id="EU_EQ")], as_of_date=old_d)
    daemon.active_dags["US_EQ"] = (us_dag, us_dag.dag_id)
    daemon.active_dags["EU_EQ"] = (eu_dag, eu_dag.dag_id)
    us_lane, eu_lane = daemon.lanes["US_EQ"], daemon.lanes["EU_EQ"]

    with _patched_store(store), \
         patch.object(mad, "execute_job", new=blocker), \
         patch.object(daemon, "_finalize_stale_runs_for_market") as fin:
        try:
            # Make the US lane busy.
            daemon._dispatch_next(us_lane, us_dag, us_dag.dag_id, MarketState.POST_CLOSE, old_d, clock.now)
            handle = us_lane.handle
            assert handle is not None

            # run()'s rollover marks every lane; it does NOT touch handles.
            us_lane.pending_rollover = new_d
            eu_lane.pending_rollover = new_d

            # Idle EU lane swaps immediately (+ stale-run finalization).
            daemon._apply_pending_rollover(eu_lane)
            assert daemon.active_dags["EU_EQ"][0].as_of_date == new_d
            assert daemon.active_dags["EU_EQ"][1] == f"EU_EQ_{new_d.isoformat()}"
            assert eu_lane.pending_rollover is None
            fin.assert_called_once_with("EU_EQ", old_d)

            # Busy US lane keeps the old DAG; the in-flight handle is untouched.
            daemon._apply_pending_rollover(us_lane)
            assert daemon.active_dags["US_EQ"][0] is us_dag
            assert us_lane.pending_rollover == new_d
            assert us_lane.handle is handle
            assert not handle.orphaned
            assert fin.call_count == 1

            # After the job completes, the deferred swap goes through.
            blocker.release.set()
            assert handle.done.wait(timeout=10)
            daemon._poll_lanes(clock.now)
            assert us_lane.handle is None
            daemon._apply_pending_rollover(us_lane)
            assert daemon.active_dags["US_EQ"][0].as_of_date == new_d
            assert us_lane.pending_rollover is None
            assert fin.call_count == 2
            assert fin.call_args_list[-1].args == ("US_EQ", old_d)
        finally:
            blocker.release.set()
            _join_all(daemon)


# ---------------------------------------------------------------------------
# 9. Catch-up occupies a single lane
# ---------------------------------------------------------------------------


def test_catchup_occupies_single_lane():
    clock = FakeClock()
    daemon = _daemon(["US_EQ", "EU_EQ"], clock=clock)
    store = FakeStore()
    past = D - timedelta(days=1)
    cu_job = _job("us_cu", required_state=MarketState.POST_CLOSE)
    cu_dag = _dag("US_EQ", [cu_job], as_of_date=past)
    live_us = _dag("US_EQ", [_job("us_live")], as_of_date=D)
    live_eu = _dag("EU_EQ", [_job("eu_live", market_id="EU_EQ")], as_of_date=D)
    daemon.active_dags["US_EQ"] = (live_us, live_us.dag_id)
    daemon.active_dags["EU_EQ"] = (live_eu, live_eu.dag_id)
    _seed_calendars(daemon)
    us_lane, eu_lane = daemon.lanes["US_EQ"], daemon.lanes["EU_EQ"]

    us_lane.catchup = CatchupState(
        dag=cu_dag,
        dag_id=cu_dag.dag_id,
        catchup_date=past,
        deadline_monotonic=time.monotonic() + 300,
    )

    with _patched_store(store), \
         patch.object(mad, "get_market_state", return_value=MarketState.SESSION):
        # The catch-up lane serves the catch-up DAG with FORCED POST_CLOSE
        # and the fixed past date...
        work = daemon._resolve_lane_work(us_lane, clock.now)
        assert work == (cu_dag, cu_dag.dag_id, MarketState.POST_CLOSE, past)

        # ...while the other market keeps serving live work at the real
        # market state — a US catch-up doesn't stall the EU lane.
        eu_work = daemon._resolve_lane_work(eu_lane, clock.now)
        assert eu_work == (live_eu, live_eu.dag_id, MarketState.SESSION, D)

        # Completion: every catch-up job done → _on_catchup_complete fires
        # and the lane clears back to the live DAG.
        store.seed(cu_job, cu_dag.dag_id, status=JobStatus.SUCCESS)
        with patch.object(daemon, "_on_catchup_complete") as done_hook:
            work = daemon._resolve_lane_work(us_lane, clock.now)
        done_hook.assert_called_once_with(us_lane)
        assert us_lane.catchup is None
        assert work == (live_us, live_us.dag_id, MarketState.SESSION, D)

        # Budget expiry: an unfinished catch-up past its wall-clock budget
        # is abandoned and the live DAG resumes.
        expired_dag = _dag("US_EQ", [_job("us_cu2", required_state=MarketState.POST_CLOSE)], as_of_date=past)
        us_lane.catchup = CatchupState(
            dag=expired_dag,
            dag_id="US_EQ_expired",
            catchup_date=past,
            deadline_monotonic=time.monotonic() - 1,
        )
        with patch.object(daemon, "_on_catchup_complete") as done_hook:
            work = daemon._resolve_lane_work(us_lane, clock.now)
        done_hook.assert_not_called()  # expiry is an abort, not a completion
        assert us_lane.catchup is None
        assert work == (live_us, live_us.dag_id, MarketState.SESSION, D)


# ---------------------------------------------------------------------------
# 10. Worker completion wakes the scheduler
# ---------------------------------------------------------------------------


def test_wake_on_completion():
    clock = FakeClock()
    daemon = _daemon(["US_EQ"], clock=clock)
    store = FakeStore()
    dag = _dag("US_EQ", [_job("us_a")])
    lane = daemon.lanes["US_EQ"]
    daemon._wake_event.clear()

    with _patched_store(store), patch.object(mad, "execute_job", new=_instant_exec()):
        daemon._dispatch_next(lane, dag, dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
        handle = lane.handle
        assert handle is not None
        assert handle.done.wait(timeout=10)
        handle.thread.join(timeout=10)  # _wake_event.set() runs after done.set()

    assert daemon._wake_event.is_set()


# ---------------------------------------------------------------------------
# 11. Shutdown marks in-flight jobs FAILED
# ---------------------------------------------------------------------------


def test_shutdown_marks_inflight_failed():
    clock = FakeClock()
    daemon = _daemon(["US_EQ", "EU_EQ"], clock=clock)
    store = FakeStore()
    blocker = BlockingJobs()
    us_dag = _dag("US_EQ", [_job("us_run_execution", job_type="run_execution")])

    with _patched_store(store), patch.object(mad, "execute_job", new=blocker):
        try:
            # One live in-flight IBKR job...
            us_lane = daemon.lanes["US_EQ"]
            daemon._dispatch_next(us_lane, us_dag, us_dag.dag_id, MarketState.POST_CLOSE, D, clock.now)
            handle = us_lane.handle
            assert handle is not None and handle.holds_ibkr
            assert daemon._ibkr_job_holder == "us_run_execution"

            # ...and one orphaned handle (already FAILED at its timeout).
            orphan = _fabricated_handle(
                _job("eu_orphan", market_id="EU_EQ"), "eu-orphan-exec", orphaned=True,
            )
            daemon.lanes["EU_EQ"].handle = orphan

            daemon._shutdown_lanes()

            # Non-orphaned in-flight execution written FAILED with the
            # shutdown message; orphan NOT re-failed (already FAILED).
            shutdown_writes = [u for u in store.updates if u[1] == JobStatus.FAILED]
            assert shutdown_writes == [
                (handle.execution_id, JobStatus.FAILED, "daemon shutdown while job was running"),
            ]
            assert not any(u[0] == "eu-orphan-exec" for u in store.updates)

            # All lanes cleared and the IBKR token cleared.
            assert all(lane.handle is None for lane in daemon.lanes.values())
            assert daemon._ibkr_job_holder is None
        finally:
            blocker.release.set()
            handle.thread.join(timeout=10)


def test_shutdown_lanes_noop_when_idle():
    daemon = _daemon(["US_EQ"])
    store = FakeStore()
    with _patched_store(store):
        daemon._shutdown_lanes()
    assert store.updates == []


# ---------------------------------------------------------------------------
# 12. running_jobs is a derived view of the lanes
# ---------------------------------------------------------------------------


def test_running_jobs_property_derives_from_lanes():
    daemon = _daemon(["US_EQ", "EU_EQ"])
    assert daemon.running_jobs == {}
    assert daemon._get_running_job_ids() == set()

    job = _job("us_a")
    handle = _fabricated_handle(job, "exec-p1")
    daemon.lanes["US_EQ"].handle = handle

    view = daemon.running_jobs
    assert set(view) == {"exec-p1"}
    assert view["exec-p1"] == (job, handle.started_at)
    assert daemon._get_running_job_ids() == {"us_a"}

    # Derived, not authoritative: mutating the returned dict must not
    # change lane state.
    view.clear()
    assert daemon.running_jobs["exec-p1"][0] is job

    # Orphaned handles still count — their threads may be alive.
    handle.orphaned = True
    assert "exec-p1" in daemon.running_jobs
    assert daemon._get_running_job_ids() == {"us_a"}

    daemon.lanes["US_EQ"].handle = None
    assert daemon.running_jobs == {}
    assert daemon._get_running_job_ids() == set()


# ---------------------------------------------------------------------------
# 13. Global concurrency cap
# ---------------------------------------------------------------------------


def test_concurrency_cap(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_MAX_CONCURRENT_JOBS", "1")
    clock = FakeClock()
    daemon = _daemon(["US_EQ", "EU_EQ"], clock=clock)
    assert daemon._max_concurrent_jobs == 1

    store = FakeStore()
    blocker = BlockingJobs()
    us_dag = _dag("US_EQ", [_job("us_a")])
    eu_dag = _dag("EU_EQ", [_job("eu_a", market_id="EU_EQ")])
    daemon.active_dags["US_EQ"] = (us_dag, us_dag.dag_id)
    daemon.active_dags["EU_EQ"] = (eu_dag, eu_dag.dag_id)
    _seed_calendars(daemon)

    with _patched_store(store), \
         patch.object(mad, "execute_job", new=blocker), \
         patch.object(mad, "get_market_state", return_value=MarketState.POST_CLOSE):
        daemon._run_cycle(D)

        # Cap of 1: only the first market dispatched.
        first = daemon.lanes["US_EQ"].handle
        assert first is not None
        assert daemon.lanes["EU_EQ"].handle is None
        assert [j for j, _ in store.created] == ["us_a"]

        blocker.release.set()
        assert first.done.wait(timeout=10)

        # First completes → second market dispatches on the next cycle.
        daemon._run_cycle(D)
        assert daemon.lanes["EU_EQ"].handle is not None
        assert daemon.lanes["EU_EQ"].handle.job.job_id == "eu_a"
        assert daemon.lanes["EU_EQ"].handle.done.wait(timeout=10)
        _join_all(daemon)
