"""Prometheus v2 – Market-aware DAG orchestration daemon.

This module implements the production market-aware orchestrator that combines:
- Real-time market state detection (trading hours, holidays)
- DAG-based dependency resolution
- Job execution with retry logic and timeout monitoring
- Persistent state tracking in job_executions table

The daemon monitors multiple markets in a follow-the-sun pattern, executing
jobs when:
1. The market is in the required state (e.g., POST_CLOSE for ingestion)
2. All job dependencies have been satisfied
3. Previous attempts have not exceeded retry limits

Design goals:
- **Idempotent**: Jobs can be safely re-run
- **Resilient**: Graceful handling of failures with exponential backoff
- **Observable**: All executions tracked in database for monitoring
- **Non-blocking**: Per-market DAGs execute independently
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from apatheon.core.market_state import MarketState, get_market_state
from apatheon.core.time import TradingCalendar, TradingCalendarConfig
from apatheon.data_ingestion.daily_orchestrator import (
    check_price_data_freshness,
    find_trailing_coverage_gaps,
    is_data_ready_for_market,
    run_daily_ingestion,
)
from psycopg2.extras import Json

from prometheus.env_utils import env_flag
from prometheus.orchestration.clock import now_local
from prometheus.orchestration.dag import (
    DAG,
    JobMetadata,
    JobStatus,
    build_intel_dag,
    build_iris_dag,
    build_market_dag,
)
from prometheus.pipeline.state import EngineRun, RunPhase, get_or_create_run, update_phase
from prometheus.pipeline.tasks import (
    run_books_for_run,
    run_signals_for_run,
    run_universes_for_run,
)

logger = get_logger(__name__)


# ============================================================================
# Job Execution Tracking
# ============================================================================


@dataclass
class JobExecution:
    """Represents a job execution record from the database."""

    execution_id: str
    job_id: str
    job_type: str
    dag_id: str
    market_id: str | None
    as_of_date: date
    status: JobStatus
    started_at: datetime | None
    completed_at: datetime | None
    attempt_number: int
    error_message: str | None
    error_details: dict | None
    created_at: datetime
    updated_at: datetime


def create_job_execution(
    db_manager: DatabaseManager,
    job: JobMetadata,
    dag_id: str,
    as_of_date: date,
) -> JobExecution:
    """Create a new PENDING job execution record."""
    execution_id = generate_uuid()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    job_config = {
        "job_id": job.job_id,
        "job_type": job.job_type,
        "market_id": job.market_id,
        "required_state": job.required_state.value if job.required_state is not None else None,
        "required_states": (
            [s.value for s in job.required_states] if job.required_states is not None else None
        ),
        "dependencies": list(job.dependencies),
        "run_phase": job.run_phase.value if job.run_phase is not None else None,
        "max_retries": int(job.max_retries),
        "retry_delay_seconds": int(job.retry_delay_seconds),
        "priority": int(job.priority.value),
        "timeout_seconds": int(job.timeout_seconds),
        "dispatch_window_local": (
            list(job.dispatch_window_local) if job.dispatch_window_local is not None else None
        ),
    }

    sql = """
        INSERT INTO job_executions (
            execution_id, job_id, job_type, dag_id, market_id, as_of_date,
            status, attempt_number, config_json, log_path, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql,
                (
                    execution_id,
                    job.job_id,
                    job.job_type,
                    dag_id,
                    job.market_id,
                    as_of_date,
                    JobStatus.PENDING.value,
                    1,
                    Json(job_config),
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    return JobExecution(
        execution_id=execution_id,
        job_id=job.job_id,
        job_type=job.job_type,
        dag_id=dag_id,
        market_id=job.market_id,
        as_of_date=as_of_date,
        status=JobStatus.PENDING,
        started_at=None,
        completed_at=None,
        attempt_number=1,
        error_message=None,
        error_details=None,
        created_at=now,
        updated_at=now,
    )


def update_job_execution_status(
    db_manager: DatabaseManager,
    execution_id: str,
    status: JobStatus,
    error_message: str | None = None,
    error_details: dict | None = None,
) -> None:
    """Update the status of a job execution."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Set started_at when transitioning to RUNNING
    # Set completed_at when transitioning to terminal states
    if status == JobStatus.RUNNING:
        sql = """
            UPDATE job_executions
            SET status = %s, started_at = %s, updated_at = %s
            WHERE execution_id = %s
        """
        params = (status.value, now, now, execution_id)
    elif status in {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.SKIPPED}:
        if status == JobStatus.FAILED and (error_message is None or str(error_message).strip() == ""):
            error_message = "Job FAILED (no error_message provided)"
        sql = """
            UPDATE job_executions
            SET status = %s, completed_at = %s, updated_at = %s,
                error_message = %s, error_details = %s
            WHERE execution_id = %s
        """
        import json

        params = (
            status.value,
            now,
            now,
            error_message,
            json.dumps(error_details) if error_details else None,
            execution_id,
        )
    else:
        sql = """
            UPDATE job_executions
            SET status = %s, updated_at = %s,
                error_message = %s, error_details = %s
            WHERE execution_id = %s
        """
        import json

        params = (
            status.value,
            now,
            error_message,
            json.dumps(error_details) if error_details else None,
            execution_id,
        )

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
        finally:
            cursor.close()


def get_dag_executions(
    db_manager: DatabaseManager,
    dag_id: str,
) -> List[JobExecution]:
    """Load all job executions for a DAG ordered by creation time."""
    sql = """
        SELECT execution_id, job_id, job_type, dag_id, market_id, as_of_date,
               status, started_at, completed_at, attempt_number,
               error_message, error_details, created_at, updated_at
        FROM job_executions
        WHERE dag_id = %s
        ORDER BY created_at DESC
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (dag_id,))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    executions = []
    for row in rows:
        import json

        error_details = json.loads(row[11]) if row[11] else None
        executions.append(
            JobExecution(
                execution_id=row[0],
                job_id=row[1],
                job_type=row[2],
                dag_id=row[3],
                market_id=row[4],
                as_of_date=row[5],
                status=JobStatus(row[6]),
                started_at=row[7],
                completed_at=row[8],
                attempt_number=row[9],
                error_message=row[10],
                error_details=error_details,
                created_at=row[12],
                updated_at=row[13],
            )
        )

    return executions


def get_latest_job_execution(
    db_manager: DatabaseManager,
    job_id: str,
    dag_id: str,
) -> JobExecution | None:
    """Get the most recent execution for a specific job in a DAG."""
    sql = """
        SELECT execution_id, job_id, job_type, dag_id, market_id, as_of_date,
               status, started_at, completed_at, attempt_number,
               error_message, error_details, created_at, updated_at
        FROM job_executions
        WHERE job_id = %s AND dag_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (job_id, dag_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return None

    import json

    error_details = json.loads(row[11]) if row[11] else None
    return JobExecution(
        execution_id=row[0],
        job_id=row[1],
        job_type=row[2],
        dag_id=row[3],
        market_id=row[4],
        as_of_date=row[5],
        status=JobStatus(row[6]),
        started_at=row[7],
        completed_at=row[8],
        attempt_number=row[9],
        error_message=row[10],
        error_details=error_details,
        created_at=row[12],
        updated_at=row[13],
    )


def increment_job_execution_attempt(
    db_manager: DatabaseManager,
    execution_id: str,
) -> None:
    """Increment the attempt number for a job execution (for retries)."""
    sql = """
        UPDATE job_executions
        SET attempt_number = attempt_number + 1,
            status = %s,
            started_at = NULL,
            completed_at = NULL,
            error_message = NULL,
            error_details = NULL,
            updated_at = %s
        WHERE execution_id = %s
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (JobStatus.PENDING.value, now, execution_id))
            conn.commit()
        finally:
            cursor.close()


# ============================================================================
# Job Execution Logic
# ============================================================================


def _get_or_create_engine_run(
    db_manager: DatabaseManager,
    market_id: str,
    as_of_date: date,
) -> EngineRun | None:
    """Get or create an EngineRun for the given market and date.

    Returns None if the region cannot be inferred from market_id.

    Note: we delegate creation to the canonical pipeline state-machine helper
    (get_or_create_run) so that timestamps and defaults stay consistent.
    """

    from apatheon.core.markets import infer_region_from_market_id

    region = infer_region_from_market_id(market_id)
    if not region:
        logger.warning(
            "_get_or_create_engine_run: unknown market_id=%s, cannot create EngineRun", market_id
        )
        return None

    return get_or_create_run(db_manager, as_of_date, region)


def execute_job(
    db_manager: DatabaseManager,
    job: JobMetadata,
    execution: JobExecution,
    *,
    options_mode: str = "paper",
) -> Tuple[bool, str | None]:
    """Execute a single job.

    Args:
        options_mode: Passed through to the ``run_options`` handler;
            one of ``"paper"``, ``"live"``, or ``"dry_run"``.

    Returns:
        (success: bool, error_message: str | None)
    """
    logger.info(
        "execute_job: job_type=%s job_id=%s execution_id=%s attempt=%d",
        job.job_type,
        job.job_id,
        execution.execution_id,
        execution.attempt_number,
    )

    try:
        # Intel and Iris jobs have no market_id — execute without an EngineRun.
        if job.market_id is None:
            if job.job_type.startswith("iris_"):
                return _execute_iris_job(job, execution, db_manager=db_manager)
            return _execute_intel_job(job, execution)

        if job.job_type in ("reconcile_fills", "reconcile_fills_eod"):
            # Fill reconciliation.  Two passes with different expiry rights:
            #
            # reconcile_fills (PRE_OPEN/SESSION) — capture-ONLY.  At 08:30 ET
            # yesterday-evening's orders haven't traded yet, and IBKR's
            # reqExecutions can't see prior-day executions anyway (cleared by
            # the overnight gateway restart), so this pass must never expire.
            #
            # reconcile_fills_eod (POST_CLOSE, before run_execution) — runs
            # right after the session whose executions are still visible:
            # captures fills, refreshes order statuses, and expires unfilled
            # orders older than 6h — old enough to have had today's full
            # session (submitted yesterday evening or earlier), while orders
            # submitted THIS evening are minutes old and untouchable even on
            # a retry that lands after run_execution.
            #
            # Needs no EngineRun; real markets only; skipped in dry_run mode.
            if options_mode == "dry_run":
                return True, None
            from prometheus.execution.fill_reconciliation import reconcile_fills

            is_eod = job.job_type == "reconcile_fills_eod"
            if is_eod:
                summary = reconcile_fills(
                    db_manager,
                    mode=options_mode,
                    expire_stale=True,
                    stale_cutoff_utc=datetime.now(timezone.utc) - timedelta(hours=6),
                )
            else:
                summary = reconcile_fills(db_manager, mode=options_mode, expire_stale=False)
            logger.info(
                "%s[%s]: fills_recorded=%d orders_updated=%d "
                "orders_expired=%d errors=%d",
                job.job_type,
                job.market_id,
                summary.get("fills_recorded", 0),
                summary.get("orders_updated", 0),
                summary.get("orders_expired", 0),
                len(summary.get("errors", [])),
            )
            if summary.get("errors"):
                return False, "; ".join(str(e) for e in summary["errors"])[:500]
            return True, None

        if job.job_type == "run_wheel":
            # Core+wheel daily runner (2026-08 spec).  Self-contained:
            # broker truth + wheel config only, no EngineRun / phase
            # machine.  Shadow (plan + decision log, no orders) until
            # PROMETHEUS_WHEEL_ENABLED is set and the halt flag is not.
            if options_mode == "dry_run":
                return True, None
            from prometheus.wheel.runner import run_wheel_daily

            wheel_summary = run_wheel_daily(
                port=4001 if options_mode == "live" else 4002,
            )
            logger.info(
                "run_wheel[%s]: shadow=%s planned=%s submitted=%s filled=%s "
                "warnings=%d",
                job.market_id,
                wheel_summary.get("shadow"),
                wheel_summary.get("orders_planned"),
                wheel_summary.get("orders_submitted", 0),
                wheel_summary.get("orders_filled", 0),
                len(wheel_summary.get("warnings", [])),
            )
            if wheel_summary.get("errors"):
                return False, "; ".join(str(e) for e in wheel_summary["errors"])[:500]
            return True, None

        # Get or create EngineRun
        run = _get_or_create_engine_run(db_manager, job.market_id, execution.as_of_date)
        if not run:
            return False, f"Could not create EngineRun for market_id={job.market_id}"

        # Execute based on job type
        if job.job_type == "ingest_prices":
            # If the same-date run is already terminal (e.g. OPTIONS_DONE from
            # an earlier ad-hoc run), reset it so this post-close cycle can
            # execute the pipeline instead of silently no-oping downstream.
            if run.phase in (
                RunPhase.EXECUTION_DONE,
                RunPhase.OPTIONS_DONE,
                RunPhase.COMPLETED,
                RunPhase.FAILED,
            ):
                from prometheus.pipeline.state import force_reset_run_to_waiting

                run = force_reset_run_to_waiting(
                    db_manager,
                    run.run_id,
                    reason=f"stale terminal phase={run.phase.value} before ingest_prices",
                )
            # Run complete daily ingestion workflow.
            # EODHD publishes EOD data 1-2 hours after market close.
            # If coverage is insufficient, return False so the daemon's
            # retry mechanism re-attempts with exponential backoff.
            result = run_daily_ingestion(
                db_manager,
                job.market_id,
                execution.as_of_date,
            )

            if result.status.value != "COMPLETE":
                return False, result.error_message or "ingestion failed"

            # Vol-complex refresh (VIX/VIX9D/3M/6M/1Y/SKEW → prices_daily).
            # Best-effort: these indices feed the options signal loader; a
            # source hiccup must warn, never block the equity pipeline. US
            # ingest only — the series are US-global. Without this the vol
            # series silently rot (they froze 2026-04-30 → 07-14 unnoticed).
            if job.market_id == "US_EQ":
                try:
                    from prometheus.scripts.backfill.backfill_vol_indices import refresh_vol_indices

                    written = refresh_vol_indices(
                        db_manager, start=execution.as_of_date - timedelta(days=7)
                    )
                    logger.info("ingest_prices: vol-index refresh wrote %d rows", written)
                except Exception:
                    logger.exception(
                        "ingest_prices: vol-index refresh FAILED — VIX series may be stale "
                        "(options signal loader enforces a staleness bound)"
                    )

            # Check if enough instruments got data (>= 95% coverage)
            if is_data_ready_for_market(db_manager, job.market_id, execution.as_of_date):
                # Belt-and-suspenders: ingestion may report COMPLETE even
                # when the upstream feed silently returned stale bars. Verify
                # that the most recent prices_daily.trade_date is within the
                # tolerated lag from the expected as_of_date before letting
                # downstream signal/portfolio jobs run on stale prices.
                # Market-scoped: with several markets ingesting into one
                # prices_daily, a fresh market must not mask another
                # market's staleness.
                fresh, freshness_msg = check_price_data_freshness(
                    db_manager, execution.as_of_date, job.market_id,
                )
                if not fresh:
                    logger.error(
                        "ingest_prices: %s — refusing to advance to DATA_READY",
                        freshness_msg,
                    )
                    return False, freshness_msg
                if run.phase == RunPhase.WAITING_FOR_DATA:
                    update_phase(db_manager, run.run_id, RunPhase.DATA_READY)
                logger.info(
                    "ingest_prices: data ready for %s on %s (%s)",
                    job.market_id, execution.as_of_date, freshness_msg,
                )
                # Non-blocking diagnostic: a past day that fully failed to
                # ingest leaves a permanent hole that the universe's
                # trailing-history filter punishes for weeks (it silently
                # collapsed the universe to a single name). Surface such
                # holes loudly so they can be backfilled. The universe
                # filter itself is now gap-tolerant, so this only warns.
                gaps = find_trailing_coverage_gaps(
                    db_manager, execution.as_of_date, job.market_id,
                )
                if gaps:
                    logger.warning(
                        "ingest_prices: %d under-covered day(s) in the trailing "
                        "price window — backfill recommended: %s",
                        len(gaps),
                        ", ".join(f"{d.isoformat()}({n})" for d, n in gaps[:10]),
                    )
                return True, None
            else:
                # Not enough data yet — EODHD may not have published.
                # Return False to trigger retry (daemon has backoff).
                received = getattr(result, "instruments_received", 0)
                expected = getattr(result, "instruments_expected", 0)
                logger.warning(
                    "ingest_prices: insufficient coverage for %s on %s "
                    "(%d/%d instruments). EODHD data may not be published yet. "
                    "Will retry on next cycle.",
                    job.market_id, execution.as_of_date,
                    received, expected,
                )
                return False, f"insufficient price coverage: {received}/{expected} instruments"

        elif job.job_type == "ingest_factors":
            # Similar to ingest_prices
            if run.phase == RunPhase.WAITING_FOR_DATA:
                update_phase(db_manager, run.run_id, RunPhase.DATA_READY)
            return True, None

        elif job.job_type == "compute_returns":
            # Returns are computed during backfill or on-demand
            # Mark as success if we're past DATA_READY
            if run.phase == RunPhase.WAITING_FOR_DATA:
                return False, f"EngineRun for {execution.as_of_date} still WAITING_FOR_DATA — data not yet ingested"
            return True, None

        elif job.job_type == "compute_volatility":
            # Volatility computed during backfill
            if run.phase == RunPhase.WAITING_FOR_DATA:
                return False, f"EngineRun for {execution.as_of_date} still WAITING_FOR_DATA — data not yet ingested"
            return True, None

        elif job.job_type == "build_numeric_windows":
            # Numeric embeddings backfilled separately
            if run.phase == RunPhase.WAITING_FOR_DATA:
                return False, f"EngineRun for {execution.as_of_date} still WAITING_FOR_DATA — data not yet ingested"
            return True, None

        elif job.job_type == "update_profiles":
            # Profiles are updated as part of run_signals_for_run
            # This is a no-op marker for dependency ordering
            if run.phase == RunPhase.WAITING_FOR_DATA:
                return False, f"EngineRun for {execution.as_of_date} still WAITING_FOR_DATA — data not yet ingested"
            return True, None

        elif job.job_type == "run_signals":
            # Execute signals phase
            if run.phase == RunPhase.DATA_READY:
                run_signals_for_run(db_manager, run)
            return True, None

        elif job.job_type == "run_universes":
            # Execute universes phase
            if run.phase == RunPhase.SIGNALS_DONE:
                run_universes_for_run(db_manager, run)
            return True, None

        elif job.job_type == "run_books":
            # Execute books phase
            if run.phase == RunPhase.UNIVERSES_DONE:
                run_books_for_run(db_manager, run)
            return True, None

        elif job.job_type == "run_execution":
            # Execute target weights against IBKR.
            from prometheus.pipeline.tasks import ExecutionConfig, run_execution_for_run

            if run.phase == RunPhase.BOOKS_DONE:
                # Discover the correct live portfolio for this region.
                # The books phase saves target_portfolios with portfolio_id = book_id
                # from policy.yaml (e.g. US_EQ_LONG_V12). Read it from the policy
                # so we always execute against the CURRENT book, not a stale allocator.
                from prometheus.meta.policy import load_meta_policies
                region = run.region.upper()
                market_id = f"{region}_EQ"
                policies = load_meta_policies()
                market_policy = policies.get(market_id)
                if market_policy is not None:
                    portfolio_id = market_policy.default.book_id
                else:
                    portfolio_id = f"{region}_EQ_LONG_V12"
                exec_cfg = ExecutionConfig(mode=options_mode, portfolio_id=portfolio_id)
                updated = run_execution_for_run(db_manager, run, execution_config=exec_cfg)
                phase_after = getattr(updated, "phase", None)
                if phase_after == RunPhase.BOOKS_DONE:
                    # Pre-submission failure (IBKR connect / broker-state read):
                    # run_execution_for_run swallows those and leaves the phase
                    # untouched. Fail the job so the daemon's retry/backoff
                    # re-attempts tonight, instead of recording a silent
                    # SUCCESS that strands the run at BOOKS_DONE until the
                    # finalize health check flags it (which cannot re-run
                    # execution). Post-submission paths advance the phase or
                    # mark the run FAILED, so they are never retried here.
                    return False, (
                        "run_execution did not advance past BOOKS_DONE "
                        "(pre-submission failure) — eligible for retry"
                    )
            return True, None

        elif job.job_type == "run_options":
            # Evaluate and execute options strategies via the full derivatives pipeline.
            # run_derivatives_daily handles: IBKR connect, position sync, signal loading
            # (IBKR streaming + DB fallback), strategy evaluation, greeks/margin risk
            # checks, futures roll detection, and order submission.
            from prometheus.scripts.run.run_derivatives_daily import run_derivatives_daily

            if run.phase in (RunPhase.EXECUTION_DONE, RunPhase.BOOKS_DONE):
                # Map execution mode → IBKR port and dry_run flag.
                _port = 4001 if options_mode == "live" else 4002
                _dry = options_mode == "dry_run"

                result = run_derivatives_daily(
                    port=_port,
                    client_id=11,  # different from equity execution (client_id=10)
                    dry_run=_dry,
                )
                if result.get("errors"):
                    return False, "; ".join(result["errors"])
                update_phase(db_manager, run.run_id, RunPhase.OPTIONS_DONE)
            return True, None

        elif job.job_type == "fx_sweep":
            # FX settlement sweep — zero negative non-USD cash balances
            # (see prometheus/execution/fx_sweep.py for the policy).
            # OPTIONAL and dependent-free: failure never blocks finalize.
            if options_mode == "dry_run":
                return True, None  # no IBKR in dry_run
            try:
                from prometheus.execution.fx_sweep import run_fx_sweep

                summary = run_fx_sweep(
                    db_manager, mode=options_mode, as_of_date=execution.as_of_date,
                )
                errors = summary.get("errors") or []
                logger.info(
                    "fx_sweep: planned=%s submitted=%s errors=%d",
                    summary.get("planned"), summary.get("submitted"), len(errors),
                )
                if errors:
                    return False, f"fx_sweep errors: {errors[:3]}"
                return True, None
            except Exception as exc:
                logger.exception("fx_sweep failed")
                return False, f"fx_sweep failed: {exc}"

        elif job.job_type == "snapshot_positions":
            # Daily IBKR position snapshot — fills the equity curve chart.
            # Runs after execution/options regardless of whether orders were
            # placed. Connects to IBKR, reads current positions, persists to
            # positions_snapshots. Non-blocking: failure doesn't prevent finalize.
            if options_mode == "dry_run":
                return True, None  # no IBKR in dry_run
            try:
                import asyncio

                from prometheus.execution.ibkr_client_impl import IbkrClientImpl
                from prometheus.execution.ibkr_config import IbkrGatewayType, IbkrMode, create_connection_config
                from prometheus.execution.live_broker import LiveBroker
                from prometheus.execution.storage import record_positions_snapshot

                ibkr_mode = IbkrMode.PAPER if options_mode == "paper" else IbkrMode.LIVE
                conn_config = create_connection_config(
                    mode=ibkr_mode, gateway_type=IbkrGatewayType.GATEWAY, client_id=12,
                )
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                client = IbkrClientImpl(config=conn_config)
                client.connect()
                # From here the client holds a gateway connection and an
                # event loop — disconnect in a finally so a failure in
                # get_positions/persistence can't leak them.
                try:
                    broker = LiveBroker(account_id=conn_config.account_id, client=client)
                    positions = broker.get_positions()
                    portfolio_id = "IBKR_PAPER" if options_mode == "paper" else "IBKR_LIVE"
                    if positions:
                        from datetime import datetime as _dt
                        from datetime import timezone as _tz

                        record_positions_snapshot(
                            db_manager,
                            portfolio_id=portfolio_id,
                            positions=positions,
                            as_of_date=execution.as_of_date,
                            mode=options_mode.upper(),
                            timestamp=_dt.now(_tz.utc),
                        )
                        logger.info(
                            "snapshot_positions: persisted %d positions for %s on %s",
                            len(positions), portfolio_id, execution.as_of_date,
                        )
                    else:
                        logger.warning("snapshot_positions: no positions returned from IBKR")

                    # Equity-history upsert — the RiskCheckingBroker drawdown
                    # breaker reads its trailing peak from
                    # portfolio_equity_history; without this daily write the
                    # breaker silently no-ops (see migration 0106).
                    account_state = broker.get_account_state()
                    equity = account_state.get("NetLiquidation", account_state.get("equity"))
                    cash = account_state.get("TotalCashValue", account_state.get("cash"))
                    try:
                        equity_f = float(equity) if equity is not None else None
                    except (TypeError, ValueError):
                        equity_f = None
                    try:
                        cash_f = float(cash) if cash is not None else None
                    except (TypeError, ValueError):
                        cash_f = None
                    if equity_f is not None:
                        gross = sum(
                            abs(float(p.market_value)) for p in positions.values()
                        ) if positions else 0.0
                        _record_equity_history(
                            db_manager,
                            portfolio_id=portfolio_id,
                            as_of_date=execution.as_of_date,
                            equity=equity_f,
                            cash=cash_f,
                            gross_position_value=gross,
                        )
                        logger.info(
                            "snapshot_positions: equity history %s %s equity=%.2f cash=%s gross=%.2f",
                            portfolio_id, execution.as_of_date, equity_f,
                            f"{cash_f:.2f}" if cash_f is not None else "n/a", gross,
                        )
                    else:
                        logger.warning(
                            "snapshot_positions: no NetLiquidation/equity in account "
                            "state — portfolio_equity_history not written",
                        )
                finally:
                    try:
                        client.disconnect()
                    except Exception:
                        logger.debug("snapshot_positions: disconnect failed", exc_info=True)
            except Exception as exc:
                logger.warning("snapshot_positions: failed (non-blocking): %s", exc)
            return True, None

        elif job.job_type == "geo_exposure_scan":
            # Score the live IBKR portfolio for geopolitical exposure.
            # Non-blocking: failure does not stop finalize.
            try:
                from prometheus.risk.geo_exposure import run_geo_exposure_scan
                portfolio_id = os.environ.get("PROMETHEUS_PRIMARY_PORTFOLIO", "IBKR_PAPER")
                result = run_geo_exposure_scan(
                    portfolio_id=portfolio_id,
                    as_of_date=execution.as_of_date,
                    db_manager=db_manager,
                )
                if result.snapshot is not None:
                    logger.info(
                        "[geo_risk] scan job done portfolio=%s overall=%.1f decision=%s",
                        portfolio_id,
                        result.snapshot.overall_risk_score,
                        result.decision_logged,
                    )
                else:
                    logger.info(
                        "[geo_risk] scan job: no holdings for %s on %s",
                        portfolio_id,
                        execution.as_of_date.isoformat(),
                    )
            except Exception as exc:
                logger.warning("geo_exposure_scan: failed (non-blocking): %s", exc)
            return True, None

        elif job.job_type == "invariants_check":
            # Execution-telemetry cross-check (positions vs fills vs orders
            # vs equity).  Violations alert via the notifications inbox; the
            # job itself succeeds either way — retrying wouldn't change the
            # facts, and the alert IS the escalation path.
            if options_mode == "dry_run":
                return True, None
            try:
                from prometheus.execution.invariants import run_invariants_check

                portfolio_id = os.environ.get("PROMETHEUS_PRIMARY_PORTFOLIO", "IBKR_PAPER")
                inv = run_invariants_check(
                    db_manager,
                    execution.as_of_date,
                    portfolio_id=portfolio_id,
                    mode=options_mode,
                )
                logger.info(
                    "invariants_check[%s]: checks=%d violations=%d (critical=%d) errors=%d",
                    portfolio_id,
                    inv.checks_run,
                    len(inv.violations),
                    inv.critical_count,
                    len(inv.errors),
                )
            except Exception as exc:
                logger.warning("invariants_check: failed (non-blocking): %s", exc)
            return True, None

        elif job.job_type == "finalize":
            # Mark the run COMPLETED.  Handles all terminal predecessor phases:
            # OPTIONS_DONE (normal), EXECUTION_DONE (options skipped/failed),
            # or BOOKS_DONE (execution also skipped — unusual but safe).
            if run.phase == RunPhase.COMPLETED:
                # Idempotent re-run after a completed finalize.
                return True, None
            if run.phase == RunPhase.FAILED:
                # A previous attempt's health check already failed the run.
                # Falling through to SUCCESS here made job_executions read
                # green on a FAILED day (2026-07-31); stay honest instead.
                return False, "run is FAILED (health check); finalize cannot succeed"
            if run.phase in (RunPhase.OPTIONS_DONE, RunPhase.EXECUTION_DONE, RunPhase.BOOKS_DONE):
                # Post-run health check: validate the run produced meaningful
                # output. Critical data-integrity anomalies (zero prices,
                # zero targets, zero orders on a day that produced targets,
                # non-positive prices) fail the run rather than completing it
                # so silent partial-pipeline failures don't propagate.
                healthy, health_error = _run_health_check(
                    db_manager, run, execution.as_of_date, job.market_id
                )
                if not healthy:
                    update_phase(db_manager, run.run_id, RunPhase.FAILED)
                    return False, health_error
                update_phase(db_manager, run.run_id, RunPhase.COMPLETED)
                # Autopilot — fire the daily meta loop once per trading
                # day, gated on US_EQ since it's the primary book and runs
                # last in CET evening. Failure isolated; never blocks
                # the finalize.
                if job.market_id == "US_EQ":
                    try:
                        from prometheus.meta.autopilot import run_daily_autopilot
                        ap = run_daily_autopilot(db_manager, execution.as_of_date)
                        logger.info(
                            "Autopilot[US_EQ finalize]: meta=%d drift=%d "
                            "(warn+=%d) notifs=%d weekly=%s errors=%d",
                            ap.meta_analysis_rows, ap.drift_rows,
                            ap.drift_warning_or_worse,
                            ap.notifications_recorded,
                            ap.weekly_report_persisted, len(ap.errors),
                        )
                    except Exception:
                        logger.exception(
                            "Autopilot failed after US_EQ finalize (non-blocking)",
                        )
            return True, None

        else:
            return False, f"Unknown job_type: {job.job_type}"

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("execute_job: failed job_id=%s: %s", job.job_id, error_msg)
        return False, error_msg


def _run_health_check(
    db_manager: "DatabaseManager",
    run: "EngineRun",
    as_of_date: "date",
    market_id: str,
) -> Tuple[bool, str | None]:
    """Validate a completed run produced meaningful output.

    Returns (healthy, error_message). Critical data-integrity anomalies
    (zero prices, zero targets, zero orders on a day that produced targets,
    non-positive prices) mark the run unhealthy; soft anomalies (low price
    coverage, missing SHI) only warn. Always writes a health report file
    when there are issues.
    """
    from pathlib import Path

    issues: list[str] = []
    critical: list[str] = []

    try:
        with db_manager.get_historical_connection() as conn:
            with conn.cursor() as cur:
                # Check price coverage
                cur.execute(
                    "SELECT COUNT(DISTINCT instrument_id) FROM prices_daily WHERE trade_date = %s",
                    (as_of_date,),
                )
                price_count = cur.fetchone()[0]
                if price_count == 0:
                    critical.append(f"ZERO PRICES: no price data ingested for {as_of_date}")
                elif price_count < 500:
                    issues.append(f"LOW PRICE COVERAGE: only {price_count} instruments (expected ~660)")

                # Non-positive prices are a data-integrity failure.
                cur.execute(
                    "SELECT COUNT(*) FROM prices_daily WHERE trade_date = %s AND close <= 0",
                    (as_of_date,),
                )
                nonpos_price_count = cur.fetchone()[0]
                if nonpos_price_count > 0:
                    critical.append(
                        f"NON-POSITIVE PRICES: {nonpos_price_count} rows with close <= 0"
                    )

        with db_manager.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                # Check target portfolios
                cur.execute(
                    "SELECT COUNT(*) FROM target_portfolios WHERE as_of_date = %s",
                    (as_of_date,),
                )
                target_count = cur.fetchone()[0]
                if target_count == 0:
                    critical.append("NO TARGET PORTFOLIO: books phase produced no targets")

                # Check orders.  Submission time is NOT the as_of_date for
                # catch-up runs: a Friday run caught up Saturday morning
                # timestamps its orders on Saturday, and the same-date
                # filter here failed that healthy run (2026-07-31).  Use a
                # [as_of, as_of+2d) window — wide enough for next-morning
                # and weekend catch-up, narrow enough to exclude the next
                # trading day's own evening submissions.
                cur.execute(
                    "SELECT COUNT(*) FROM orders "
                    "WHERE timestamp >= %s::date AND timestamp < %s::date + INTERVAL '2 days'",
                    (as_of_date, as_of_date),
                )
                order_count = cur.fetchone()[0]
                # Zero orders is only an anomaly on a day that produced targets
                # (a rebalance with targets but no orders means execution
                # silently dropped the book) — and only when execution is
                # actually enabled: under PROMETHEUS_EXECUTION_HALT zero
                # orders is the intended daily outcome.
                from prometheus.env_utils import env_flag

                if order_count == 0 and target_count > 0 and not env_flag("PROMETHEUS_EXECUTION_HALT"):
                    critical.append(
                        "NO ORDERS: targets were produced but execution generated zero orders"
                    )

                # Check sector health
                cur.execute(
                    "SELECT COUNT(*) FROM sector_health_daily WHERE as_of_date = %s",
                    (as_of_date,),
                )
                shi_count = cur.fetchone()[0]
                if shi_count == 0:
                    issues.append("NO SECTOR HEALTH: SHI not computed for this date")

        all_issues = critical + issues
        if all_issues:
            for issue in critical:
                logger.error("HEALTH CHECK [%s %s]: CRITICAL %s", market_id, as_of_date, issue)
            for issue in issues:
                logger.warning("HEALTH CHECK [%s %s]: %s", market_id, as_of_date, issue)

            # Write health report file
            report_dir = Path(
                os.environ.get(
                    "PROMETHEUS_HEALTH_REPORT_DIR",
                    str(Path(__file__).resolve().parents[2] / "data" / "health_reports"),
                )
            )
            report_dir.mkdir(parents=True, exist_ok=True)
            report_path = report_dir / f"health_{as_of_date}_{market_id}.txt"
            report_path.write_text(
                f"Pipeline Health Report: {market_id} {as_of_date}\n"
                f"Run ID: {run.run_id}\n"
                f"Final Phase: {run.phase.value}\n"
                f"Prices: {price_count}\n"
                f"Targets: {target_count}\n"
                f"Orders: {order_count}\n"
                f"Sector Health: {shi_count}\n\n"
                f"ISSUES:\n" + "\n".join(f"  - {i}" for i in all_issues) + "\n",
            )
            logger.warning("Health report written to %s", report_path)
        else:
            logger.info(
                "HEALTH CHECK [%s %s]: OK — prices=%d targets=%d orders=%d shi=%d",
                market_id, as_of_date, price_count, target_count, order_count, shi_count,
            )

        if critical:
            return False, "Run health check failed: " + "; ".join(critical)
        return True, None
    except Exception:
        # A health-check infrastructure failure should not itself fail the run.
        logger.debug("Health check failed (non-critical)", exc_info=True)
        return True, None


def _record_equity_history(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of_date: date,
    equity: float,
    cash: float | None,
    gross_position_value: float | None,
) -> None:
    """Upsert one daily NAV row into ``portfolio_equity_history``.

    The drawdown circuit breaker reads its trailing equity peak from this
    table (migration 0106); the snapshot_positions job is the writer.
    Idempotent per (portfolio_id, as_of_date) — a re-run overwrites with
    the latest broker values.
    """
    sql = """
        INSERT INTO portfolio_equity_history (
            portfolio_id, as_of_date, equity, cash, gross_position_value
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (portfolio_id, as_of_date) DO UPDATE
        SET equity = EXCLUDED.equity,
            cash = EXCLUDED.cash,
            gross_position_value = EXCLUDED.gross_position_value
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql,
                (portfolio_id, as_of_date, equity, cash, gross_position_value),
            )
            conn.commit()
        finally:
            cursor.close()


def _execute_intel_job(
    job: JobMetadata,
    execution: "JobExecution",
) -> Tuple[bool, str | None]:
    """Execute an intel DAG job (no EngineRun required)."""
    try:
        if job.job_type == "intel_flash_check":
            from apatheon.intel.pipeline import run_flash_check
            run_flash_check()
            return True, None

        elif job.job_type == "intel_daily_sitrep":
            from apatheon.intel.pipeline import run_daily_sitrep
            run_daily_sitrep()
            return True, None

        elif job.job_type == "intel_weekly_assessment":
            # run_weekly_assessment() was removed from apatheon (2026-07
            # dead-module cleanup); the maintained API is mode="weekly".
            from apatheon.intel.pipeline import run_situation_report
            run_situation_report("weekly")
            return True, None

        elif job.job_type == "intel_log_health":
            from prometheus.monitoring.report_service import generate_log_report
            generate_log_report("log_daily")
            return True, None

        elif job.job_type == "intel_divergence_scan":
            from prometheus.signals.divergence import run_divergence_scan
            result = run_divergence_scan(as_of_date=execution.as_of_date)
            logger.info(
                "[divergence] scan job done date=%s persisted=%d decisions=%d sig=%d ext=%d",
                execution.as_of_date.isoformat(),
                result.rows_persisted,
                result.decisions_logged,
                len(result.significant),
                len(result.extreme),
            )
            return True, None

        elif job.job_type == "intel_convergence_scan":
            from prometheus.signals.convergence import run_convergence_scan
            result = run_convergence_scan(as_of_date=execution.as_of_date)
            logger.info(
                "[convergence] scan job done date=%s persisted=%d decisions=%d confident=%d",
                execution.as_of_date.isoformat(),
                result.rows_persisted,
                result.decisions_logged,
                len(result.confident),
            )
            return True, None

        elif job.job_type == "intel_compound_pressure_scan":
            from prometheus.signals.compound_pressure import run_compound_pressure_scan
            result = run_compound_pressure_scan(as_of_date=execution.as_of_date)
            logger.info(
                "[compound] scan job done date=%s targets=%d persisted=%d decisions=%d high+=%d",
                execution.as_of_date.isoformat(),
                result.targets_scanned,
                result.rows_persisted,
                result.decisions_logged,
                len(result.high_or_above),
            )
            return True, None

        elif job.job_type == "intel_beneficiary_scan":
            from prometheus.signals.beneficiary import run_beneficiary_scan
            result = run_beneficiary_scan(as_of_date=execution.as_of_date)
            logger.info(
                "[beneficiary] scan job done date=%s victims=%d persisted=%d decisions=%d asym=%d",
                execution.as_of_date.isoformat(),
                result.victims_scanned,
                result.rows_persisted,
                result.decisions_logged,
                len(result.asymmetric),
            )
            return True, None

        else:
            return False, f"Unknown intel job_type: {job.job_type}"

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("_execute_intel_job: failed job_id=%s: %s", job.job_id, error_msg)
        return False, error_msg


# Strategies to run DiagnosticsEngine + ProposalGenerator against.
# These all need backtest_runs data to produce output; missing data is
# handled gracefully (ValueError caught, job still succeeds).
_IRIS_STRATEGY_IDS = [
    "US_CORE_LONG_EQ",
    "US_SMALL_CAP",
    "EU_CORE_LONG_EQ",
]


def _execute_iris_job(
    job: JobMetadata,
    execution: "JobExecution",
    db_manager: DatabaseManager | None = None,
) -> Tuple[bool, str | None]:
    """Execute an Iris meta-intelligence job.

    Runs with no EngineRun.  All jobs are non-fatal — failures are logged
    but never propagate to the trading pipeline.
    """
    # Reuse the daemon's DB manager to avoid connection pool bloat.
    if db_manager is None:
        db_manager = get_db_manager()
    try:
        if job.job_type == "iris_outcome_eval":

            from prometheus.decisions.evaluator import OutcomeEvaluator

            db = db_manager
            evaluator = OutcomeEvaluator(db_manager=db)
            count = evaluator.evaluate_pending_outcomes(
                as_of_date=execution.as_of_date,
                max_decisions=500,
                num_workers=8,
            )
            logger.info("[Iris] outcome_eval: evaluated %d outcomes", count)
            return True, None

        elif job.job_type == "iris_scorecard":

            from prometheus.decisions.scorecard import PredictionScorecard

            db = db_manager
            sc = PredictionScorecard(db_manager=db)
            for horizon in (5, 21, 63):
                try:
                    report = sc.build_scorecard(
                        horizon_days=horizon,
                        max_decisions=500,
                        end_date=execution.as_of_date,
                    )
                    logger.info(
                        "[Iris] scorecard %dd: n=%d hit_rate=%.1f%% spearman_rho=%.3f",
                        horizon,
                        report.total_predictions,
                        report.hit_rate * 100,
                        report.spearman_rho,
                    )
                except Exception:
                    logger.exception("[Iris] scorecard %dd failed", horizon)
            return True, None

        elif job.job_type == "iris_lambda_scorecard":

            from prometheus.decisions.lambda_scorecard import LambdaScorecard

            db = db_manager
            sc = LambdaScorecard(db_manager=db)
            try:
                report = sc.build_scorecard(
                    market_id="US_EQ",
                    end_date=execution.as_of_date,
                )
                logger.info(
                    "[Iris] lambda_scorecard: n=%d mae=%.4f dir_acc=%.1f%% r2=%.3f",
                    report.total_predictions,
                    report.mae,
                    report.direction_accuracy * 100,
                    report.r_squared,
                )
            except Exception:
                logger.exception("[Iris] lambda_scorecard failed (non-fatal)")
            return True, None

        elif job.job_type == "iris_diagnostics":

            from prometheus.meta.diagnostics import DiagnosticsEngine

            db = db_manager
            engine = DiagnosticsEngine(db_manager=db)
            for strategy_id in _IRIS_STRATEGY_IDS:
                try:
                    report = engine.analyze_strategy(strategy_id)
                    logger.info(
                        "[Iris] diagnostics %s: sharpe=%.3f return=%.2f%% drawdown=%.2f%%"
                        " underperforming=%d high_risk=%d",
                        strategy_id,
                        report.overall_performance.sharpe,
                        report.overall_performance.return_ * 100,
                        report.overall_performance.max_drawdown * 100,
                        len(report.underperforming_configs),
                        len(report.high_risk_configs),
                    )
                except ValueError:
                    # Insufficient backtest data — expected early in live operation
                    logger.info("[Iris] diagnostics %s: insufficient data (skipped)", strategy_id)
                except Exception:
                    logger.exception("[Iris] diagnostics %s failed", strategy_id)
            return True, None

        elif job.job_type == "iris_proposals":

            from prometheus.meta.diagnostics import DiagnosticsEngine
            from prometheus.meta.proposal_generator import ProposalGenerator

            db = db_manager
            engine = DiagnosticsEngine(db_manager=db)
            gen = ProposalGenerator(db_manager=db, diagnostics_engine=engine)
            total = 0
            for strategy_id in _IRIS_STRATEGY_IDS:
                try:
                    proposals = gen.generate_proposals(strategy_id, auto_save=True)
                    logger.info(
                        "[Iris] proposals %s: generated %d proposals",
                        strategy_id, len(proposals),
                    )
                    total += len(proposals)
                except ValueError:
                    logger.info("[Iris] proposals %s: insufficient data (skipped)", strategy_id)
                except Exception:
                    logger.exception("[Iris] proposals %s failed", strategy_id)
            logger.info("[Iris] proposals total: %d generated", total)
            return True, None

        elif job.job_type == "iris_log_report":
            from prometheus.monitoring.report_service import generate_log_report
            generate_log_report("log_daily")
            return True, None

        elif job.job_type == "iris_live_perf":

            from prometheus.decisions.live_performance import LivePerformanceTracker

            db = db_manager
            tracker = LivePerformanceTracker(db_manager=db)
            perf = tracker.compute_rolling_performance(execution.as_of_date)
            if "error" not in perf:
                import math
                sharpe_str = f"{perf['sharpe']:.3f}" if not math.isnan(perf.get('sharpe', float('nan'))) else "n/a"
                logger.info(
                    "[Iris] live_perf @21d: n=%d sharpe=%s win=%.0f%% max_dd=%.1f%% pnl=%+.2f",
                    perf["n"], sharpe_str,
                    (perf["win_rate"] or 0) * 100,
                    (perf["max_drawdown"] or 0) * 100,
                    perf["total_pnl"],
                )
                for s in perf.get("by_strategy", []):
                    logger.info(
                        "[Iris] live_perf strategy=%s n=%d avg_ret=%s win=%.0f%%",
                        s["engine"], s["n"],
                        f"{s['avg_return']:+.4f}" if s["avg_return"] is not None else "n/a",
                        (s["win_rate"] or 0) * 100,
                    )
            else:
                logger.warning("[Iris] live_perf error: %s", perf["error"])
            return True, None

        elif job.job_type == "iris_regime_eval":

            from prometheus.decisions.live_performance import LivePerformanceTracker

            db = db_manager
            tracker = LivePerformanceTracker(db_manager=db)
            regimes = tracker.compute_regime_breakdown(execution.as_of_date)
            for r in regimes:
                if "error" in r:
                    logger.warning("[Iris] regime_eval error: %s", r["error"])
                else:
                    import math
                    logger.info(
                        "[Iris] regime_eval %s: n=%d sharpe=%s win=%.0f%%",
                        r["regime_label"], r["n"],
                        f"{r['sharpe']:.3f}" if not math.isnan(r["sharpe"]) else "n/a",
                        r["win_rate"] * 100,
                    )
            return True, None

        elif job.job_type == "iris_fragility_check":

            from prometheus.decisions.live_performance import LivePerformanceTracker

            db = db_manager
            tracker = LivePerformanceTracker(db_manager=db)
            result = tracker.validate_fragility_signal(execution.as_of_date)
            if "error" not in result:
                import math
                rho_str = f"{result['spearman_rho']:.3f}" if not math.isnan(result.get('spearman_rho', float('nan'))) else "n/a"
                icon = "\u2713" if result.get("verdict") == "SIGNAL_VALID" else "\u26a0"
                logger.info(
                    "[Iris] fragility_check: n=%d spearman_rho=%s verdict=%s %s",
                    result["n"], rho_str, result.get("verdict", "?"), icon,
                )
            else:
                logger.warning("[Iris] fragility_check error: %s", result["error"])
            return True, None

        elif job.job_type == "iris_hedge_eval":

            from prometheus.decisions.live_performance import LivePerformanceTracker

            db = db_manager
            tracker = LivePerformanceTracker(db_manager=db)
            result = tracker.compute_hedge_effectiveness(execution.as_of_date)
            if "error" not in result:
                import math
                r_str = f"{result['pearson_r']:.3f}" if not math.isnan(result.get('pearson_r', float('nan'))) else "n/a"
                icon = "\u2713" if result.get("verdict") == "HEDGE_EFFECTIVE" else "\u26a0"
                logger.info(
                    "[Iris] hedge_eval: n=%d pearson_r=%s verdict=%s %s opts_pnl=%+.2f",
                    result["n_dates"], r_str, result.get("verdict", "?"), icon,
                    result.get("options_pnl_total", 0),
                )
            else:
                logger.warning("[Iris] hedge_eval error: %s", result["error"])
            return True, None

        else:
            return False, f"Unknown iris job_type: {job.job_type}"

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("_execute_iris_job: failed job_id=%s: %s", job.job_id, error_msg)
        return False, error_msg


# ============================================================================
# Retry Logic
# ============================================================================


def calculate_retry_delay(
    job: JobMetadata,
    attempt_number: int,
    *,
    error_message: str | None = None,
) -> float:
    """Calculate exponential backoff delay with jitter and a hard cap.

    Pure exponential growth (``base * 2**attempt``) was unbounded — a job
    with a 600s base and a few retries pushed delays past 2 hours, well
    past the daily window for most pipeline stages. We now cap at
    ``PROMETHEUS_RETRY_MAX_DELAY_SECONDS`` (default 1h).

    Detected rate-limit errors (``HTTP 429`` or "Too Many Requests" in
    the message) bump to a longer minimum so we don't immediately
    re-trigger the same throttle.

    Returns delay in seconds.
    """
    base_delay = job.retry_delay_seconds

    # 429-aware: if the upstream is rate-limiting us, exponential
    # back-off from the standard base risks immediately re-triggering
    # the throttle. Lift the floor.
    if error_message:
        msg_lower = error_message.lower()
        if "429" in msg_lower or "too many requests" in msg_lower or "rate limit" in msg_lower:
            base_delay = max(base_delay, 900)  # at least 15 minutes

    # Exponential backoff: base * 2^(attempt - 1)
    delay = base_delay * (2 ** (attempt_number - 1))

    # Hard cap so retries never starve the daily window.
    try:
        max_delay = float(os.environ.get("PROMETHEUS_RETRY_MAX_DELAY_SECONDS", "3600"))
    except ValueError:
        max_delay = 3600.0
    delay = min(delay, max_delay)

    # Add jitter: ±25%
    jitter = delay * 0.25 * (2 * random.random() - 1)
    return max(1.0, delay + jitter)


def should_retry_job(
    job: JobMetadata,
    execution: JobExecution,
    *,
    orphaned_thread_alive: bool = False,
) -> bool:
    """Determine if a failed job should be retried.

    Args:
        orphaned_thread_alive: True when a previous attempt of this job
            timed out but its worker thread is *still running* (Python
            cannot cancel threads).  Retrying while the orphan lives
            would run two instances of the same job concurrently — for
            execution jobs that means the same order batch submitted
            twice.  The caller must re-check once the orphan exits.
    """
    if orphaned_thread_alive:
        logger.warning(
            "should_retry_job: job_id=%s NOT retried — a timed-out previous "
            "attempt's thread is still alive (would run the job twice "
            "concurrently); will re-evaluate after the orphan exits",
            job.job_id,
        )
        return False

    if execution.status != JobStatus.FAILED:
        return False

    if execution.attempt_number >= job.max_retries:
        logger.info(
            "should_retry_job: job_id=%s exhausted retries (%d/%d)",
            job.job_id,
            execution.attempt_number,
            job.max_retries,
        )
        return False

    return True


# ============================================================================
# Market-Aware Daemon
# ============================================================================


#: Default active market set.  US_EQ is the only real market traded today;
#: IRIS and INTEL are pseudo-markets (meta-intelligence DAGs).  Non-US
#: regions fail daily on price coverage and pollute engine_runs, so they
#: are opt-in via PROMETHEUS_ACTIVE_MARKETS rather than on by default.
DEFAULT_ACTIVE_MARKETS: Tuple[str, ...] = ("US_EQ", "IRIS", "INTEL")


def resolve_active_markets(cli_markets: List[str] | None) -> List[str]:
    """Resolve the daemon's active market list.

    Precedence:
        1. Explicit ``--market`` CLI flags (operator override).
        2. ``PROMETHEUS_ACTIVE_MARKETS`` env var (comma-separated, e.g.
           ``"US_EQ,EU_EQ,IRIS,INTEL"``) — re-enabling a region is a
           config change, not a code change.
        3. :data:`DEFAULT_ACTIVE_MARKETS`.
    """
    if cli_markets:
        return list(cli_markets)

    raw = os.environ.get("PROMETHEUS_ACTIVE_MARKETS", "")
    env_markets = [m.strip().upper() for m in raw.split(",") if m.strip()]
    if env_markets:
        logger.info(
            "resolve_active_markets: using PROMETHEUS_ACTIVE_MARKETS=%s",
            ",".join(env_markets),
        )
        return env_markets

    logger.info(
        "resolve_active_markets: no --market flags or PROMETHEUS_ACTIVE_MARKETS "
        "set — defaulting to %s",
        ",".join(DEFAULT_ACTIVE_MARKETS),
    )
    return list(DEFAULT_ACTIVE_MARKETS)


@dataclass
class JobHandle:
    """One in-flight job attempt running on a worker thread.

    All fields are written by the MAIN thread except ``result`` (written
    once by the worker before it sets ``done``) — Event.set() publishes
    the write, so the main thread reads ``result`` only after
    ``done.is_set()`` and no lock is needed.
    """

    job: "JobMetadata"
    execution_id: str
    dag_id: str
    market_id: str
    as_of_date: date
    thread: "threading.Thread"
    started_at: datetime  # tz-aware UTC
    deadline: datetime  # started_at + timeout
    done: "threading.Event"
    result: list  # [(success: bool, error_msg: str | None)]
    attempt_number: int
    max_retries: int
    orphaned: bool = False
    holds_ibkr: bool = False


@dataclass
class CatchupState:
    """A morning catch-up DAG being served through a lane."""

    dag: "DAG"
    dag_id: str
    catchup_date: date
    deadline_monotonic: float


@dataclass
class MarketLane:
    """Per-market execution lane: at most ONE job in flight.

    Strict per-market serialization (pipeline phases are sequential)
    while different markets' lanes run concurrently. An orphaned
    (timed-out but still running) job keeps the lane occupied until its
    thread dies — no job of the market may run while an uncancellable
    thread may still be mutating that market's EngineRun.
    """

    market_id: str
    handle: JobHandle | None = None
    catchup: CatchupState | None = None
    pending_rollover: date | None = None


# Job types that open an IBKR session. Client ids are fixed PER JOB TYPE
# (run_execution=10, run_options=11, snapshot_positions=12,
# reconcile_fills=13, fx_sweep=15, run_wheel=16), so two markets running
# the same job type concurrently would collide on the gateway. The
# dispatcher allows at most one of these in flight globally.
IBKR_EXCLUSIVE_JOB_TYPES = frozenset(
    {
        "run_execution",
        "run_options",
        "snapshot_positions",
        "reconcile_fills",
        "reconcile_fills_eod",
        "fx_sweep",
        "run_wheel",
    }
)


@dataclass(frozen=True)
class MarketAwareDaemonConfig:
    """Configuration for the market-aware orchestrator daemon.

    Attributes:
        markets: List of market IDs to orchestrate (e.g., ["US_EQ", "EU_EQ"])
        poll_interval_seconds: Sleep interval between polling cycles
        as_of_date: Optional fixed date for orchestration (defaults to today)
        options_mode: Execution mode for the run_options job — ``"paper"``,
            ``"live"``, or ``"dry_run"`` (default: ``"paper"``)
    """

    markets: List[str]
    poll_interval_seconds: int = 60
    as_of_date: date | None = None
    options_mode: str = "paper"
    morning_catchup_hour: int = 8  # Local hour (0-23) to trigger catch-up if pipeline missed


class MarketAwareDaemon:
    """Market-aware DAG orchestration daemon.

    Manages execution of market-specific DAGs based on real-time trading
    hours and dependency resolution.
    """

    def __init__(
        self,
        config: MarketAwareDaemonConfig,
        db_manager: DatabaseManager,
    ):
        self.config = config
        self.db_manager = db_manager
        self.shutdown_requested = False
        # Event-based shutdown signal so the main loop can wake from sleep
        # immediately on SIGTERM instead of waiting up to poll_interval_seconds.
        import threading as _threading
        self._shutdown_event = _threading.Event()
        # Separate "wake up, but don't shutdown" event used by the
        # signal listener to short-circuit the poll interval when an
        # EXTREME divergence or CRITICAL compound alert arrives.
        self._wake_event = _threading.Event()

        # Track active DAGs: {market_id: (DAG, dag_id)}
        self.active_dags: Dict[str, Tuple[DAG, str]] = {}

        # Per-market execution lanes — the concurrency unit. Each lane
        # holds at most one in-flight JobHandle; lanes run concurrently.
        # All lane mutation happens on the MAIN thread (workers only set
        # their handle's `done` event), so no locking is required.
        self.lanes: Dict[str, MarketLane] = {
            market_id: MarketLane(market_id=market_id)
            for market_id in config.markets
        }

        # job_id currently holding the global IBKR session token (see
        # IBKR_EXCLUSIVE_JOB_TYPES). Main-thread-only.
        self._ibkr_job_holder: str | None = None

        # Global cap on concurrently dispatched jobs — a misconfigured
        # market list can't exhaust the DB pool.
        try:
            self._max_concurrent_jobs = int(
                os.environ.get("PROMETHEUS_MAX_CONCURRENT_JOBS", "8")
            )
        except ValueError:
            self._max_concurrent_jobs = 8

        # Track retry backoff: {execution_id: retry_after_timestamp}
        self.retry_backoff: Dict[str, datetime] = {}

        # Track threads orphaned by timeout, keyed by job_id. The thread
        # keeps running because Python doesn't support thread cancellation;
        # we reap entries on later cycles to log late completions and to
        # detect connection-pool leaks (the thread holds a DB conn until it
        # exits). While an orphan is alive, its job_id must NOT be retried —
        # that would run two instances of the same job concurrently (e.g.
        # the same order batch submitted twice).
        self._orphaned_threads: Dict[str, Tuple["threading.Thread", datetime]] = {}

        # Cache TradingCalendar per market — loaded once, reused every cycle.
        # Avoids a DB round-trip (full holiday list) on every 60-second poll.
        self._calendars: Dict[str, TradingCalendar] = {}

        # Dispatch-date side channel for _select_next_execution (set by
        # _dispatch_next right before the selection loop; main-thread-only).
        self._lane_dispatch_date: date | None = None

    # ── Clock seam & derived views ────────────────────────────────────

    def _now(self) -> datetime:
        """Current UTC time — overridable seam for fake-clock tests."""
        return datetime.now(timezone.utc)

    @property
    def running_jobs(self) -> Dict[str, Tuple[JobMetadata, datetime]]:
        """Derived view of in-flight jobs, keyed by execution_id.

        Kept for monitoring/tests; lanes are the authoritative state.
        Includes orphaned handles (their threads are still alive).
        """
        return {
            lane.handle.execution_id: (lane.handle.job, lane.handle.started_at)
            for lane in self.lanes.values()
            if lane.handle is not None
        }

    def _live_handle_count(self) -> int:
        return sum(1 for lane in self.lanes.values() if lane.handle is not None)

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers."""

        def _signal_handler(signum, frame):
            logger.info(
                "MarketAwareDaemon: received signal %d, requesting graceful shutdown",
                signum,
            )
            self.shutdown_requested = True
            # Wake up the main loop's interruptible sleep immediately.
            self._shutdown_event.set()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _on_signal_alert(self, alert: Any) -> None:
        """Callback invoked by the LISTEN/NOTIFY listener thread.

        Sets ``_wake_event`` so the main loop's interruptible sleep
        returns early and re-evaluates DAGs immediately instead of
        waiting for the next poll tick.  Heavy work (running the actual
        jobs) still happens on the main thread to avoid contending for
        the DB pool.
        """
        logger.info(
            "MarketAwareDaemon: signal alert source=%s severity=%s entity=%s:%s — waking poll loop",
            getattr(alert, "source", "?"),
            getattr(alert, "severity", "?"),
            getattr(alert, "entity_type", "?"),
            getattr(alert, "entity_id", "?"),
        )
        self._wake_event.set()

    def _interruptible_sleep(self, timeout_seconds: float) -> str:
        """Sleep up to ``timeout_seconds``, but wake early on shutdown or
        signal-alert.  Returns ``"shutdown"`` if the daemon should stop,
        ``"wake"`` if a signal alert arrived, ``"timeout"`` otherwise.
        """
        # Wait on shutdown first — if it's already set, return immediately.
        if self._shutdown_event.is_set():
            return "shutdown"
        if self._wake_event.is_set():
            self._wake_event.clear()
            return "wake"

        # Use a short polling loop so we honour both events without
        # threading.Event.wait() supporting "wait on multiple events".
        # Sleeping in 200ms slices keeps shutdown latency low while
        # avoiding a busy-wait.
        end_time = max(0.0, timeout_seconds)
        slice_s = 0.2
        elapsed = 0.0
        while elapsed < end_time:
            wait_for = min(slice_s, end_time - elapsed)
            if self._shutdown_event.wait(timeout=wait_for):
                return "shutdown"
            if self._wake_event.is_set():
                self._wake_event.clear()
                return "wake"
            elapsed += wait_for
        return "timeout"

    def _start_signal_listener(self) -> "Any | None":
        """Try to start the Postgres LISTEN/NOTIFY listener.

        Returns the listener handle on success; ``None`` (and logs) on
        failure so the daemon falls back to pure polling.
        """
        if env_flag("PROMETHEUS_DISABLE_SIGNAL_LISTENER"):
            logger.info("MarketAwareDaemon: signal listener disabled via env")
            return None
        try:
            from prometheus.orchestration.signal_listener import SignalAlertListener

            listener = SignalAlertListener(
                db_manager=self.db_manager,
                on_alert=self._on_signal_alert,
            )
            listener.start()
            return listener
        except Exception:
            logger.exception("MarketAwareDaemon: failed to start signal listener")
            return None

    def _initialize_dag(self, market_id: str, as_of_date: date) -> None:
        """Initialize or refresh the DAG for one market."""
        if market_id == "INTEL":
            dag = build_intel_dag(as_of_date, is_sunday=as_of_date.weekday() == 6)
            dag_id = dag.dag_id  # e.g. "intel_daily_2026-03-19"
        elif market_id == "IRIS":
            dag = build_iris_dag(as_of_date)
            dag_id = dag.dag_id  # e.g. "iris_daily_2026-03-19"
        else:
            dag = build_market_dag(market_id, as_of_date)
            dag_id = f"{market_id}_{as_of_date.isoformat()}"
        self.active_dags[market_id] = (dag, dag_id)
        logger.info(
            "_initialize_dag: initialized dag_id=%s with %d jobs",
            dag_id,
            len(dag.jobs),
        )

    def _initialize_dags(self, as_of_date: date) -> None:
        """Initialize or refresh DAGs for all configured markets."""
        for market_id in self.config.markets:
            self._initialize_dag(market_id, as_of_date)

    def _get_completed_jobs(self, dag_id: str) -> Set[str]:
        """Get set of job IDs that are done (SUCCESS or SKIPPED) for a DAG.

        SKIPPED jobs are included so their dependents can still run — a job
        permanently skipped after exhausting retries must not block downstream
        work (e.g. finalize should run even when run_options fails repeatedly).
        """
        executions = get_dag_executions(self.db_manager, dag_id)
        return {
            exec.job_id
            for exec in executions
            if exec.status in {JobStatus.SUCCESS, JobStatus.SKIPPED}
        }

    def _get_running_job_ids(self) -> Set[str]:
        """Job ids currently in flight across all lanes.

        Includes orphaned handles: a timed-out thread may still be
        running its job, so the DAG must not re-offer it.
        """
        return {
            lane.handle.job.job_id
            for lane in self.lanes.values()
            if lane.handle is not None
        }

    def _maybe_reap_zombie_runs(self, as_of_date: date) -> None:
        """Daily sweep of stuck (zombie) engine_runs rows.

        Fires from the morning catch-up hour onward (once per day via
        the dedup key) — hour-equality would never fire on a machine
        booted after that hour.
        """
        if now_local().hour < self.config.morning_catchup_hour:
            return
        zombie_key = f"zombie_reap_{as_of_date}"
        if hasattr(self, "_zombie_reap_done") and zombie_key in self._zombie_reap_done:
            return
        try:
            from prometheus.pipeline.state import reap_zombie_runs

            reaped = reap_zombie_runs(self.db_manager, older_than_hours=24)
            if reaped:
                logger.warning(
                    "_maybe_reap_zombie_runs: finalised %d stuck run(s): %s",
                    len(reaped), ", ".join(reaped[:5]),
                )
            else:
                logger.info("_maybe_reap_zombie_runs: no zombies found")
        except Exception:
            logger.exception("_maybe_reap_zombie_runs: sweep failed")
            return
        if not hasattr(self, "_zombie_reap_done"):
            self._zombie_reap_done: set = set()
        self._zombie_reap_done.add(zombie_key)
        # Prune old entries to prevent unbounded growth
        if len(self._zombie_reap_done) > 60:
            self._zombie_reap_done = set(sorted(self._zombie_reap_done)[-30:])

    def _maybe_refresh_holidays(self, as_of_date: date) -> None:
        """Monthly exchange-holiday refresh for all active real markets.

        EODHD serves ~1-2 years of forward holidays; without periodic
        refresh the market_holidays table goes stale and TradingCalendar
        silently degrades to weekends-only for non-US markets. Runs on
        the 1st of the month, any time from the catch-up hour onward.
        """
        if as_of_date.day != 1 or now_local().hour < self.config.morning_catchup_hour:
            return
        key = f"holidays_{as_of_date.isoformat()[:7]}"
        if not hasattr(self, "_holiday_refresh_done"):
            self._holiday_refresh_done: set = set()
        if key in self._holiday_refresh_done:
            return
        self._holiday_refresh_done.add(key)
        try:
            from apatheon.data_ingestion.market_calendar import refresh_market_holidays

            markets = [m for m in self.config.markets if m not in ("IRIS", "INTEL")]
            counts = refresh_market_holidays(self.db_manager, markets)
            logger.info("_maybe_refresh_holidays: %s", counts)
            # Rebuild calendars so new holidays take effect without restart.
            self._calendars.clear()
        except Exception:
            logger.exception("_maybe_refresh_holidays: refresh failed (non-fatal)")

    def _has_live_orphan(self, job_id: str) -> bool:
        """True when a timed-out attempt of ``job_id`` is still running."""
        entry = self._orphaned_threads.get(job_id)
        return entry is not None and entry[0].is_alive()

    def _reap_orphaned_threads(self) -> None:
        """Reap any orphaned (timed-out) threads that have finally exited.

        Logs late completions so operators see when a leak resolves
        itself (and the job becomes retryable again), frees the lane the
        orphan was occupying, releases the IBKR token if the orphan held
        it (the zombie may own the gateway session until it exits), and
        warns when an orphan has been alive long enough to constitute a
        pool-exhaustion risk.
        """
        if not self._orphaned_threads:
            return

        now_dt = self._now()
        still_running: Dict[str, Tuple["threading.Thread", datetime]] = {}
        for job_id, (thread, started_at) in self._orphaned_threads.items():
            # Normalize: tolerate naive timestamps from older entries.
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            age = (now_dt - started_at).total_seconds()
            if not thread.is_alive():
                logger.warning(
                    "_reap_orphaned_threads: orphan job_id=%s finally exited after %.0fs "
                    "— retry guard lifted",
                    job_id, age,
                )
                # Free the lane the dead orphan was occupying + release
                # the IBKR token if held.
                for lane in self.lanes.values():
                    h = lane.handle
                    if h is not None and h.orphaned and h.job.job_id == job_id:
                        if h.holds_ibkr and self._ibkr_job_holder == job_id:
                            self._ibkr_job_holder = None
                            logger.info(
                                "_reap_orphaned_threads: released IBKR token held "
                                "by dead orphan %s", job_id,
                            )
                        lane.handle = None
                continue
            if age > 7200:  # 2h
                logger.error(
                    "_reap_orphaned_threads: job_id=%s STILL alive after %.0fs "
                    "(holding DB connection — pool exhaustion risk)",
                    job_id, age,
                )
            still_running[job_id] = (thread, started_at)

        live_orphans = len(still_running)
        if live_orphans >= 3:
            logger.error(
                "_reap_orphaned_threads: %d live orphaned threads — DB pool "
                "pressure risk (each may hold up to 2 connections)",
                live_orphans,
            )
        self._orphaned_threads = still_running

    # ── Lane dispatch machinery ───────────────────────────────────────
    #
    # The main thread is the sole scheduler: it selects executions,
    # writes ALL job_executions status rows, manages retry backoff and
    # the IBKR token, and starts one worker thread per dispatched job.
    # Workers only run execute_job, publish their result via
    # handle.done, and set _wake_event so the next pipeline phase
    # dispatches within ~200ms instead of a full poll interval.

    def _select_next_execution(self, job: JobMetadata, dag_id: str, now: datetime):
        """Resolve the execution row for a runnable job, or None to skip.

        Handles retry backoff, PENDING reuse, FAILED retry / permanent
        SKIP, and fresh execution creation. Main-thread-only.
        """
        latest_exec = get_latest_job_execution(self.db_manager, job.job_id, dag_id)

        if latest_exec and latest_exec.execution_id in self.retry_backoff:
            retry_after = self.retry_backoff[latest_exec.execution_id]
            if now < retry_after:
                logger.debug(
                    "_select_next_execution: job_id=%s in backoff until %s",
                    job.job_id,
                    retry_after,
                )
                return None
            del self.retry_backoff[latest_exec.execution_id]

        if latest_exec and latest_exec.status == JobStatus.PENDING:
            return latest_exec
        if latest_exec and latest_exec.status == JobStatus.FAILED:
            if not should_retry_job(
                job, latest_exec,
                orphaned_thread_alive=self._has_live_orphan(job.job_id),
            ):
                logger.warning(
                    "_select_next_execution: job_id=%s retries exhausted "
                    "(attempt %d/%d), marking SKIPPED",
                    job.job_id,
                    latest_exec.attempt_number,
                    job.max_retries,
                )
                update_job_execution_status(
                    self.db_manager,
                    latest_exec.execution_id,
                    JobStatus.SKIPPED,
                    error_message=(
                        f"Retries exhausted after {latest_exec.attempt_number} attempts: "
                        f"{latest_exec.error_message}"
                    ),
                )
                self.retry_backoff.pop(latest_exec.execution_id, None)
                return None
            increment_job_execution_attempt(self.db_manager, latest_exec.execution_id)
            return get_latest_job_execution(self.db_manager, job.job_id, dag_id)
        if latest_exec and latest_exec.status == JobStatus.SKIPPED:
            return None
        # No prior execution (or prior was SUCCESS) — start fresh.
        return create_job_execution(
            self.db_manager, job, dag_id, self._lane_dispatch_date
        )

    def _start_job(
        self,
        lane: MarketLane,
        job: JobMetadata,
        execution,
        dag_id: str,
        dispatch_date: date,
        now: datetime,
    ) -> None:
        """Mark RUNNING and start the worker thread for one job."""
        update_job_execution_status(
            self.db_manager, execution.execution_id, JobStatus.RUNNING
        )

        done = threading.Event()
        result: list = []

        def _worker() -> None:
            try:
                r = execute_job(
                    self.db_manager, job, execution,
                    options_mode=self.config.options_mode,
                )
                result.append(r)
            except Exception as exc:
                result.append((False, f"unhandled exception: {exc}"))
            finally:
                done.set()
                # Wake the scheduler so the market's next pipeline phase
                # dispatches immediately instead of on the next poll tick.
                self._wake_event.set()
                logger.debug(
                    "_worker: job_id=%s thread exiting (result=%s)",
                    job.job_id,
                    "ok" if result and result[0][0] else "fail",
                )

        timeout_sec = job.timeout_seconds or 3600
        thread = threading.Thread(
            target=_worker, daemon=True, name=f"job-{job.job_id}"
        )

        handle = JobHandle(
            job=job,
            execution_id=execution.execution_id,
            dag_id=dag_id,
            market_id=lane.market_id,
            as_of_date=dispatch_date,
            thread=thread,
            started_at=now,
            deadline=now + timedelta(seconds=timeout_sec),
            done=done,
            result=result,
            attempt_number=execution.attempt_number,
            max_retries=job.max_retries,
        )

        if job.job_type in IBKR_EXCLUSIVE_JOB_TYPES:
            self._ibkr_job_holder = job.job_id
            handle.holds_ibkr = True

        lane.handle = handle
        thread.start()
        logger.info(
            "_start_job: dispatched job_id=%s market=%s (execution_id=%s, "
            "timeout=%ds, ibkr_token=%s, in_flight=%d)",
            job.job_id,
            lane.market_id,
            execution.execution_id,
            timeout_sec,
            handle.holds_ibkr,
            self._live_handle_count(),
        )

    def _dispatch_next(
        self,
        lane: MarketLane,
        dag: DAG,
        dag_id: str,
        current_state: MarketState,
        dispatch_date: date,
        now: datetime,
    ) -> None:
        """Dispatch the first eligible runnable job of an idle lane."""
        if lane.handle is not None:
            return
        if self._live_handle_count() >= self._max_concurrent_jobs:
            logger.warning(
                "_dispatch_next: concurrency cap %d reached — market %s waits",
                self._max_concurrent_jobs,
                lane.market_id,
            )
            return

        completed = self._get_completed_jobs(dag_id)
        running = self._get_running_job_ids()
        runnable = dag.get_runnable_jobs(
            completed, running, current_state, now_utc=datetime.now(timezone.utc)
        )
        if not runnable:
            return

        logger.debug(
            "_dispatch_next: market=%s state=%s runnable=%d completed=%d",
            lane.market_id,
            current_state.value,
            len(runnable),
            len(completed),
        )

        # dispatch date is threaded to _select_next_execution via an
        # attribute to keep its signature stable for unit tests.
        self._lane_dispatch_date = dispatch_date
        for job in runnable:
            # Orphan guard: a previous attempt of this job timed out but
            # its worker thread is still running. (Lane occupancy already
            # covers the same market; this guards cross-DAG catchup ids.)
            if self._has_live_orphan(job.job_id):
                logger.warning(
                    "_dispatch_next: job_id=%s skipped — timed-out previous "
                    "attempt still running (orphaned thread alive)",
                    job.job_id,
                )
                continue

            # IBKR exclusivity: fixed client ids per job type mean two
            # markets running the same IBKR job type would collide on the
            # gateway session.
            if (
                job.job_type in IBKR_EXCLUSIVE_JOB_TYPES
                and self._ibkr_job_holder is not None
            ):
                logger.info(
                    "_dispatch_next: job_id=%s waits — IBKR token held by %s",
                    job.job_id,
                    self._ibkr_job_holder,
                )
                continue

            execution = self._select_next_execution(job, dag_id, now)
            if execution is None:
                continue

            self._start_job(lane, job, execution, dag_id, dispatch_date, now)
            return

    def _handle_job_result(
        self,
        lane: MarketLane,
        handle: JobHandle,
        success: bool,
        error_msg: str | None,
        now: datetime,
    ) -> None:
        """Persist a completed job's outcome and free the lane."""
        if success:
            update_job_execution_status(
                self.db_manager,
                handle.execution_id,
                JobStatus.SUCCESS,
            )
            logger.info(
                "_handle_job_result: job_id=%s SUCCESS (execution_id=%s)",
                handle.job.job_id,
                handle.execution_id,
            )
        else:
            update_job_execution_status(
                self.db_manager,
                handle.execution_id,
                JobStatus.FAILED,
                error_message=error_msg,
            )
            logger.error(
                "_handle_job_result: job_id=%s FAILED (execution_id=%s): %s",
                handle.job.job_id,
                handle.execution_id,
                error_msg,
            )
            # Schedule retry backoff off the attempt counter captured at
            # dispatch (the DB row now says FAILED; should_retry_job gates
            # on FAILED status at selection time).
            if handle.attempt_number < handle.max_retries:
                delay = calculate_retry_delay(
                    handle.job, handle.attempt_number, error_message=error_msg,
                )
                retry_after = now + timedelta(seconds=delay)
                self.retry_backoff[handle.execution_id] = retry_after
                logger.info(
                    "_handle_job_result: job_id=%s will retry in %.1fs (attempt %d/%d)",
                    handle.job.job_id,
                    delay,
                    handle.attempt_number + 1,
                    handle.max_retries,
                )

        if handle.holds_ibkr and self._ibkr_job_holder == handle.job.job_id:
            self._ibkr_job_holder = None
        lane.handle = None

    def _poll_lanes(self, now: datetime) -> None:
        """Handle completions and deadline timeouts across all lanes."""
        for lane in self.lanes.values():
            handle = lane.handle
            if handle is None or handle.orphaned:
                continue

            if handle.done.is_set():
                if handle.result:
                    success, error_msg = handle.result[0]
                else:
                    success, error_msg = False, "job thread completed without result"
                self._handle_job_result(lane, handle, success, error_msg, now)
                continue

            if now >= handle.deadline:
                # Timed out — Python can't kill threads. Mark FAILED,
                # orphan the thread, and keep the LANE OCCUPIED until the
                # reaper observes the thread dead: no job of this market
                # may run while an uncancellable thread might still be
                # mutating its EngineRun. The IBKR token (if held) is
                # released by the reaper for the same reason.
                timeout_sec = handle.job.timeout_seconds or 3600
                handle.orphaned = True
                self._orphaned_threads[handle.job.job_id] = (
                    handle.thread,
                    handle.started_at,
                )
                update_job_execution_status(
                    self.db_manager,
                    handle.execution_id,
                    JobStatus.FAILED,
                    error_message=f"job timed out after {timeout_sec}s",
                )
                logger.error(
                    "_poll_lanes: job_id=%s TIMED OUT after %ds — thread "
                    "orphaned; lane %s blocked until the orphan exits; "
                    "%d orphan(s) tracked",
                    handle.job.job_id,
                    timeout_sec,
                    lane.market_id,
                    len(self._orphaned_threads),
                )

    def _nearest_deadline_seconds(self, now: datetime) -> float | None:
        """Seconds until the nearest in-flight job deadline, if any."""
        deadlines = [
            (lane.handle.deadline - now).total_seconds()
            for lane in self.lanes.values()
            if lane.handle is not None and not lane.handle.orphaned
        ]
        if not deadlines:
            return None
        return max(0.0, min(deadlines))

    def _resolve_lane_work(
        self,
        lane: MarketLane,
        now: datetime,
    ) -> tuple["DAG", str, MarketState, date] | None:
        """Return (dag, dag_id, state, dispatch_date) for a lane, or None.

        Catch-up work (a fixed past date, forced POST_CLOSE) takes
        precedence over the live DAG. Catch-up completion/expiry is
        detected here, on the main thread, while the lane is idle.
        """
        market_id = lane.market_id

        if lane.catchup is not None:
            cu = lane.catchup
            if time.monotonic() > cu.deadline_monotonic:
                logger.error(
                    "_resolve_lane_work: catch-up for %s %s exceeded its "
                    "wall-clock budget (%d/%d jobs done) — aborting; live "
                    "DAG resumes",
                    market_id,
                    cu.catchup_date,
                    len(self._get_completed_jobs(cu.dag_id)),
                    len(cu.dag.jobs),
                )
                lane.catchup = None
            else:
                completed = self._get_completed_jobs(cu.dag_id)
                runnable = cu.dag.get_runnable_jobs(
                    completed, self._get_running_job_ids(), MarketState.POST_CLOSE
                )
                if not runnable:
                    logger.info(
                        "_resolve_lane_work: catch-up COMPLETE for %s %s "
                        "(%d/%d jobs done)",
                        market_id,
                        cu.catchup_date,
                        len(completed),
                        len(cu.dag.jobs),
                    )
                    self._on_catchup_complete(lane)
                    lane.catchup = None
                else:
                    return cu.dag, cu.dag_id, MarketState.POST_CLOSE, cu.catchup_date

        if market_id not in self.active_dags:
            return None
        dag, dag_id = self.active_dags[market_id]

        # INTEL/IRIS are not real markets — their jobs use
        # required_state=None so any state passes. Use POST_CLOSE as a
        # safe placeholder.
        if market_id in ("INTEL", "IRIS"):
            current_state = MarketState.POST_CLOSE
        else:
            if market_id not in self._calendars:
                self._calendars[market_id] = TradingCalendar(
                    TradingCalendarConfig(market=market_id)
                )
            current_state = get_market_state(
                market_id, now, calendar=self._calendars[market_id]
            )

        return dag, dag_id, current_state, dag.as_of_date

    def _apply_pending_rollover(self, lane: MarketLane) -> None:
        """Swap an idle lane's DAG to the pending as_of_date.

        Deferred per-lane rollover: busy lanes keep their old DAG until
        the in-flight job completes (its status writes go against the
        handle's dag_id), then swap here on the next dispatch.
        """
        if lane.pending_rollover is None or lane.handle is not None:
            return
        new_date = lane.pending_rollover
        old = self.active_dags.get(lane.market_id)
        old_date = old[0].as_of_date if old else None
        try:
            self._finalize_stale_runs_for_market(lane.market_id, old_date)
        except Exception:
            logger.exception(
                "_apply_pending_rollover: stale-run finalization failed for %s",
                lane.market_id,
            )
        self._initialize_dag(lane.market_id, new_date)
        lane.pending_rollover = None
        logger.info(
            "_apply_pending_rollover: %s rolled %s -> %s",
            lane.market_id,
            old_date,
            new_date,
        )

    def _finalize_stale_runs_for_market(
        self, market_id: str, old_date: date | None
    ) -> None:
        """Finalize this market's non-terminal engine_runs at rollover.

        If the old DAG's jobs all succeeded the run was just orphaned
        (mark COMPLETED); with failures, mark FAILED with diagnostics.
        Pseudo-markets have no engine_runs.
        """
        if market_id in ("INTEL", "IRIS") or old_date is None:
            return
        from prometheus.pipeline.state import list_active_runs

        region = market_id.split("_")[0]
        stale_runs = [
            r
            for r in list_active_runs(self.db_manager)
            if r.region.upper() == region
            and r.phase not in (RunPhase.COMPLETED, RunPhase.FAILED)
        ]
        for stale_run in stale_runs:
            dag_id = f"{market_id}_{old_date.isoformat()}"
            dag_execs = get_dag_executions(self.db_manager, dag_id)
            all_succeeded = len(dag_execs) > 0 and all(
                e.status in {JobStatus.SUCCESS, JobStatus.SKIPPED}
                for e in dag_execs
            )
            if all_succeeded:
                logger.info(
                    "_finalize_stale_runs_for_market: stale run %s (phase=%s) "
                    "— DAG %s all succeeded, marking COMPLETED",
                    stale_run.run_id,
                    stale_run.phase.value,
                    dag_id,
                )
                update_phase(self.db_manager, stale_run.run_id, RunPhase.COMPLETED)
            else:
                failed_jobs = [
                    {
                        "job_id": e.job_id,
                        "error": (e.error_message or "")[:500],
                    }
                    for e in dag_execs
                    if e.status == JobStatus.FAILED
                ]
                logger.warning(
                    "_finalize_stale_runs_for_market: finalizing stale run %s "
                    "(phase=%s, n_failed=%d) from %s",
                    stale_run.run_id,
                    stale_run.phase.value,
                    len(failed_jobs),
                    old_date,
                )
                update_phase(
                    self.db_manager,
                    stale_run.run_id,
                    RunPhase.FAILED,
                    error={
                        "reason": "date_rollover_zombie_reap",
                        "stuck_phase": stale_run.phase.value,
                        "dag_id": dag_id,
                        "n_jobs": len(dag_execs),
                        "n_failed_jobs": len(failed_jobs),
                        "failed_jobs": failed_jobs[:10],
                    },
                )

    def _shutdown_lanes(self) -> None:
        """Mark jobs mid-flight at shutdown as FAILED and clear all lanes.

        Prevents executions from appearing orphaned in RUNNING state on the
        next startup. Orphaned handles were already marked FAILED at their
        timeout, so they are not re-failed here. The actual work threads are
        daemon=True and die with the process. Called from ``run()``'s
        shutdown tail; extracted so the semantics are testable in isolation.
        """
        in_flight = [
            lane for lane in self.lanes.values() if lane.handle is not None
        ]
        if not in_flight:
            return
        logger.warning(
            "MarketAwareDaemon: %d job(s) in-flight at shutdown — marking FAILED",
            len(in_flight),
        )
        for lane in in_flight:
            handle = lane.handle
            # Orphaned handles were already marked FAILED at timeout.
            if handle is not None and not handle.orphaned:
                try:
                    update_job_execution_status(
                        self.db_manager,
                        handle.execution_id,
                        JobStatus.FAILED,
                        error_message="daemon shutdown while job was running",
                    )
                except Exception:
                    logger.exception(
                        "MarketAwareDaemon: failed to mark execution %s as FAILED",
                        handle.execution_id,
                    )
            lane.handle = None
        self._ibkr_job_holder = None

    def _on_catchup_complete(self, lane: MarketLane) -> None:
        """Post-catch-up hook: reconcile fills once (paper/live only).

        Orders submitted during the caught-up POST_CLOSE cycle only fill
        at the next open; pull executions from IBKR now. Runs inline on
        the main thread (bounded IBKR call, once per catch-up) and only
        for the home market that owns the account-global jobs.
        """
        if lane.market_id != "US_EQ":
            return
        if self.config.options_mode not in ("paper", "live"):
            return
        try:
            from prometheus.execution.fill_reconciliation import reconcile_fills

            # Capture-only: a catch-up runs AFTER the missed session (often
            # the next morning or a weekend), when reqExecutions can no
            # longer see that session's executions — expiring here would
            # cancel orders whose fills are simply invisible, the exact
            # defect that blinded the 2026-07 run.  Expiry stays with the
            # normally-scheduled reconcile_fills_eod pass.
            summary = reconcile_fills(
                self.db_manager, mode=self.config.options_mode, expire_stale=False,
            )
            logger.info(
                "_on_catchup_complete: reconcile_fills fills=%d updated=%d "
                "expired=%d errors=%d",
                summary.get("fills_recorded", 0),
                summary.get("orders_updated", 0),
                summary.get("orders_expired", 0),
                len(summary.get("errors", [])),
            )
        except Exception:
            logger.exception(
                "_on_catchup_complete: reconcile_fills failed (non-blocking)"
            )


    def _maybe_morning_catchup(self, as_of_date: date) -> None:
        """At the configured morning hour, queue catch-up for missed markets.

        If the machine was off during a market's POST_CLOSE window, that
        market's pipeline never ran. This detects the gap PER MARKET
        (each with its own calendar and engine-run history) and attaches
        a CatchupState to the market's lane; the lane then serves the
        catch-up DAG (forced POST_CLOSE, fixed past date) through the
        normal dispatcher while every other lane keeps running live work
        — a US catch-up no longer stalls Asia's live morning.

        Detection fires any time from the configured local hour onward —
        NOT only in that exact hour: this host is regularly powered on
        after 10:00, and an hour-equality gate meant a boot at 10:05
        never caught up the missed overnight run (US prices froze for
        days). The per-(market, date) dedup below keeps it one-shot.
        """
        now_local_dt = now_local()
        if now_local_dt.hour < self.config.morning_catchup_hour:
            return

        if not hasattr(self, "_catchup_done"):
            self._catchup_done: set = set()

        from prometheus.pipeline.state import load_latest_run

        try:
            catchup_budget_seconds = int(
                os.environ.get("PROMETHEUS_CATCHUP_BUDGET_SECONDS", "1200")
            )
        except ValueError:
            catchup_budget_seconds = 1200
        if catchup_budget_seconds <= 0:
            return

        for market_id in self.config.markets:
            if market_id in ("INTEL", "IRIS"):
                continue
            lane = self.lanes.get(market_id)
            if lane is None or lane.catchup is not None:
                continue

            if market_id not in self._calendars:
                self._calendars[market_id] = TradingCalendar(
                    TradingCalendarConfig(market=market_id)
                )
            cal = self._calendars[market_id]

            candidates = cal.trading_days_between(
                as_of_date - timedelta(days=7), as_of_date - timedelta(days=1),
            )
            if not candidates:
                continue
            last_trading_day = candidates[-1]

            catchup_key = f"catchup_{market_id}_{last_trading_day}"
            if catchup_key in self._catchup_done:
                continue

            latest_run = load_latest_run(
                self.db_manager, market_id=market_id, as_of_date=last_trading_day,
            )
            if latest_run and latest_run.phase == RunPhase.COMPLETED:
                self._catchup_done.add(catchup_key)
                continue

            logger.info(
                "MORNING CATCH-UP: %s last trading day %s has no completed "
                "run — queueing catch-up DAG on its lane (budget %ds)",
                market_id,
                last_trading_day,
                catchup_budget_seconds,
            )
            lane.catchup = CatchupState(
                dag=build_market_dag(market_id, last_trading_day),
                dag_id=f"{market_id}_{last_trading_day.isoformat()}",
                catchup_date=last_trading_day,
                deadline_monotonic=time.monotonic() + catchup_budget_seconds,
            )
            self._catchup_done.add(catchup_key)

        # Prune old entries to prevent unbounded growth
        if len(self._catchup_done) > 120:
            self._catchup_done = set(sorted(self._catchup_done)[-60:])


    def _run_cycle(self, as_of_date: date) -> None:
        """One scheduler cycle: poll lanes, then dispatch idle lanes.

        Non-blocking — a slow job occupies only its own market's lane
        while every other market keeps flowing. ``as_of_date`` is the
        current UTC anchor; each lane's actual dispatch date comes from
        its own DAG (which may lag during a deferred rollover or lead
        during catch-up).
        """
        if self._shutdown_event.is_set():
            return
        now = self._now()

        # 1) Completions + deadline timeouts.
        self._poll_lanes(now)

        # 2) Dispatch one job per idle lane.
        for market_id in self.config.markets:
            lane = self.lanes.get(market_id)
            if lane is None:
                lane = self.lanes[market_id] = MarketLane(market_id=market_id)
            if lane.handle is not None:
                continue

            self._apply_pending_rollover(lane)

            work = self._resolve_lane_work(lane, now)
            if work is None:
                continue
            dag, dag_id, current_state, dispatch_date = work
            self._dispatch_next(lane, dag, dag_id, current_state, dispatch_date, now)

    def run(self) -> None:
        """Run the orchestration daemon until shutdown is requested."""
        self._setup_signal_handlers()

        # Use UTC date — host-local (e.g. CEST) flips at 22:00 UTC and
        # advances as_of_date *during* US POST_CLOSE for the prior session,
        # which makes the daemon ask EODHD for tomorrow's prices.  UTC
        # midnight (02:00 CEST) is past every market's POST_CLOSE end, so
        # trading day D's POST_CLOSE always falls inside UTC date D.
        as_of_date = self.config.as_of_date or datetime.now(timezone.utc).date()
        self._initialize_dags(as_of_date)

        # Start the LISTEN/NOTIFY listener so EXTREME divergence and
        # CRITICAL compound-pressure rows trigger an immediate wake-up
        # instead of waiting up to one poll_interval.  Failure to start
        # is non-fatal — the 60s poll loop still runs.
        signal_listener = self._start_signal_listener()

        logger.info(
            "MarketAwareDaemon: starting markets=%s as_of_date=%s poll_interval=%ds listener=%s",
            ",".join(self.config.markets),
            as_of_date,
            self.config.poll_interval_seconds,
            "on" if signal_listener is not None else "off",
        )

        cycle_count = 0
        while not self.shutdown_requested:
            try:
                cycle_count += 1
                logger.debug("MarketAwareDaemon: cycle %d starting", cycle_count)

                # Detect calendar date rollover (midnight crossings).
                # Only auto-rolls when no explicit as_of_date was configured.
                # DEFERRED PER-LANE SWAP: busy lanes keep their old DAG
                # until the in-flight job completes (status writes go
                # against the handle's dag_id); idle lanes swap on their
                # next dispatch via _apply_pending_rollover, which also
                # finalizes that market's stale engine_runs.
                if self.config.as_of_date is None:
                    today = self._now().date()
                    if today != as_of_date:
                        logger.info(
                            "MarketAwareDaemon: date rolled over %s -> %s — "
                            "queueing per-lane DAG swaps (%d lanes busy)",
                            as_of_date,
                            today,
                            self._live_handle_count(),
                        )
                        for lane in self.lanes.values():
                            lane.pending_rollover = today
                        as_of_date = today

                # Morning catch-up: at the configured local hour, if yesterday's
                # pipeline didn't complete (machine was off overnight), force a
                # POST_CLOSE cycle so the pipeline runs with stale-but-available data.
                self._maybe_morning_catchup(as_of_date)

                # Reap any orphaned (timed-out) threads that have finally exited.
                self._reap_orphaned_threads()

                # Sweep zombie engine_runs once per day (rough — fires whenever
                # we're in the catch-up hour, so it piggybacks on a known
                # low-traffic window). Cheap query; safe to call repeatedly.
                self._maybe_reap_zombie_runs(as_of_date)

                # Monthly holiday-calendar refresh (1st of month).
                self._maybe_refresh_holidays(as_of_date)

                self._run_cycle(as_of_date)

                # Interruptible sleep: SIGTERM and signal alerts both
                # break out, but only "shutdown" exits the loop.  Signal
                # alerts and job completions (workers set _wake_event)
                # cause the next cycle to start immediately. The sleep is
                # clamped to the nearest in-flight job deadline so a
                # timeout is detected within one sleep slice of expiring.
                sleep_s = float(self.config.poll_interval_seconds)
                nearest = self._nearest_deadline_seconds(self._now())
                if nearest is not None:
                    sleep_s = min(sleep_s, max(0.2, nearest))
                if self._interruptible_sleep(sleep_s) == "shutdown":
                    break

            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("MarketAwareDaemon: cycle %d failed: %s", cycle_count, exc)
                if self._interruptible_sleep(self.config.poll_interval_seconds) == "shutdown":
                    break

        # Stop the LISTEN/NOTIFY listener (no-op if it never started).
        if signal_listener is not None:
            try:
                signal_listener.stop(timeout=5.0)
            except Exception:
                logger.exception("MarketAwareDaemon: signal listener stop failed")

        # Mark any jobs that were mid-flight at shutdown as FAILED so they
        # don't appear orphaned in RUNNING state on next startup.
        self._shutdown_lanes()

        logger.info("MarketAwareDaemon: shutdown complete after %d cycles", cycle_count)


# ============================================================================
# CLI Entrypoint
# ============================================================================


def _parse_args(argv: Optional[List[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prometheus v2 market-aware DAG orchestration daemon"
    )

    parser.add_argument(
        "--market",
        action="append",
        required=False,
        default=None,
        help=(
            "Market ID to orchestrate (e.g., US_EQ). Can specify multiple times. "
            "When omitted, PROMETHEUS_ACTIVE_MARKETS (comma-separated) is used, "
            f"falling back to the default set: {','.join(DEFAULT_ACTIVE_MARKETS)}."
        ),
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=60,
        help="Sleep interval between polling cycles (default: 60)",
    )
    parser.add_argument(
        "--as-of-date",
        type=str,
        help="Fixed as-of date for orchestration (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--options-mode",
        type=str,
        default="paper",
        choices=["paper", "live", "dry_run"],
        help="Execution mode for the run_options job (default: paper)",
    )
    parser.add_argument(
        "--morning-catchup-hour",
        type=int,
        default=8,
        help="Local hour (0-23) for morning catch-up pipeline if overnight run missed (default: 8)",
    )

    args = parser.parse_args(argv)

    if args.poll_interval_seconds <= 0:
        parser.error("--poll-interval-seconds must be positive")

    if args.as_of_date:
        try:
            args.as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
        except ValueError:
            parser.error("--as-of-date must be in YYYY-MM-DD format")

    return args


def main(argv: Optional[List[str]] = None) -> None:
    """CLI entrypoint for the market-aware daemon.

    Example::

        python -m prometheus.orchestration.market_aware_daemon \\
            --market US_EQ \\
            --market EU_EQ \\
            --poll-interval-seconds 60
    """
    args = _parse_args(argv)

    config = MarketAwareDaemonConfig(
        markets=resolve_active_markets(args.market),
        poll_interval_seconds=args.poll_interval_seconds,
        as_of_date=args.as_of_date,
        options_mode=args.options_mode,
        morning_catchup_hour=args.morning_catchup_hour,
    )

    # Preflight: surface missing IBKR credentials at boot rather than at
    # 3am during the first POST_CLOSE cycle. Required vars depend on the
    # configured options mode.
    if args.options_mode in ("paper", "live"):
        from prometheus.execution.ibkr_config import validate_credentials_at_startup

        try:
            validate_credentials_at_startup(
                require_paper=args.options_mode == "paper",
                require_live=args.options_mode == "live",
            )
        except ValueError as exc:
            logger.error("IBKR preflight failed: %s", exc)
            raise SystemExit(2)

    db_manager = get_db_manager()
    daemon = MarketAwareDaemon(config, db_manager)
    daemon.run()


if __name__ == "__main__":  # pragma: no cover
    main()
