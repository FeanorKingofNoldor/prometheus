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
import random
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from psycopg2.extras import Json

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from apathis.core.market_state import MarketState, get_market_state, get_next_state_transition
from prometheus.orchestration.dag import (
    DAG,
    JobMetadata,
    JobStatus,
    build_intel_dag,
    build_kronos_dag,
    build_market_dag,
)
from prometheus.pipeline.tasks import (
    run_signals_for_run,
    run_universes_for_run,
    run_books_for_run,
)
from prometheus.pipeline.state import EngineRun, RunPhase, get_or_create_run, update_phase
from apathis.data_ingestion.daily_orchestrator import run_daily_ingestion, is_data_ready_for_market

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
        "dependencies": list(job.dependencies),
        "run_phase": job.run_phase.value if job.run_phase is not None else None,
        "max_retries": int(job.max_retries),
        "retry_delay_seconds": int(job.retry_delay_seconds),
        "priority": int(job.priority.value),
        "timeout_seconds": int(job.timeout_seconds),
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

    from apathis.core.markets import infer_region_from_market_id

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
        # Intel and Kronos jobs have no market_id — execute without an EngineRun.
        if job.market_id is None:
            if job.job_type.startswith("kronos_"):
                return _execute_kronos_job(job, execution)
            return _execute_intel_job(job, execution)

        # Get or create EngineRun
        run = _get_or_create_engine_run(db_manager, job.market_id, execution.as_of_date)
        if not run:
            return False, f"Could not create EngineRun for market_id={job.market_id}"

        # Execute based on job type
        if job.job_type == "ingest_prices":
            # Run complete daily ingestion workflow
            result = run_daily_ingestion(
                db_manager,
                job.market_id,
                execution.as_of_date,
            )
            
            if result.status.value == "COMPLETE":
                # Check if data is ready for processing
                if is_data_ready_for_market(db_manager, job.market_id, execution.as_of_date):
                    # Mark engine run as DATA_READY
                    if run.phase == RunPhase.WAITING_FOR_DATA:
                        update_phase(db_manager, run.run_id, RunPhase.DATA_READY)
                return True, None
            else:
                return False, result.error_message

        elif job.job_type == "ingest_factors":
            # Similar to ingest_prices
            if run.phase == RunPhase.WAITING_FOR_DATA:
                update_phase(db_manager, run.run_id, RunPhase.DATA_READY)
            return True, None

        elif job.job_type == "compute_returns":
            # Returns are computed during backfill or on-demand
            # Mark as success if we're past DATA_READY
            return run.phase != RunPhase.WAITING_FOR_DATA, None

        elif job.job_type == "compute_volatility":
            # Volatility computed during backfill
            return run.phase != RunPhase.WAITING_FOR_DATA, None

        elif job.job_type == "build_numeric_windows":
            # Numeric embeddings backfilled separately
            return run.phase != RunPhase.WAITING_FOR_DATA, None

        elif job.job_type == "update_profiles":
            # Profiles are updated as part of run_signals_for_run
            # This is a no-op marker for dependency ordering
            return run.phase != RunPhase.WAITING_FOR_DATA, None

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
            from prometheus.pipeline.tasks import run_execution_for_run, ExecutionConfig

            if run.phase == RunPhase.BOOKS_DONE:
                exec_cfg = ExecutionConfig(mode="paper")
                run_execution_for_run(db_manager, run, execution_config=exec_cfg)
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
                    client_id=10,
                    dry_run=_dry,
                )
                if result.get("errors"):
                    return False, "; ".join(result["errors"])
                update_phase(db_manager, run.run_id, RunPhase.OPTIONS_DONE)
            return True, None

        else:
            return False, f"Unknown job_type: {job.job_type}"

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("execute_job: failed job_id=%s: %s", job.job_id, error_msg)
        return False, error_msg


def _execute_intel_job(
    job: JobMetadata,
    execution: "JobExecution",
) -> Tuple[bool, str | None]:
    """Execute an intel DAG job (no EngineRun required)."""
    try:
        if job.job_type == "intel_flash_check":
            from apathis.intel.pipeline import run_flash_check
            run_flash_check()
            return True, None

        elif job.job_type == "intel_daily_sitrep":
            from apathis.intel.pipeline import run_daily_sitrep
            run_daily_sitrep()
            return True, None

        elif job.job_type == "intel_weekly_assessment":
            from apathis.intel.pipeline import run_weekly_assessment
            run_weekly_assessment()
            return True, None

        elif job.job_type == "intel_log_health":
            from prometheus.monitoring.report_service import generate_log_report
            generate_log_report("log_daily")
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
_KRONOS_STRATEGY_IDS = [
    "US_CORE_LONG_EQ",
    "US_SMALL_CAP",
    "EU_CORE_LONG_EQ",
]


def _execute_kronos_job(
    job: JobMetadata,
    execution: "JobExecution",
) -> Tuple[bool, str | None]:
    """Execute a Kronos meta-intelligence job.

    Runs with no EngineRun.  All jobs are non-fatal — failures are logged
    but never propagate to the trading pipeline.
    """
    try:
        if job.job_type == "kronos_outcome_eval":
            from apathis.core.database import get_db_manager
            from prometheus.decisions.evaluator import OutcomeEvaluator

            db = get_db_manager()
            evaluator = OutcomeEvaluator(db_manager=db)
            count = evaluator.evaluate_pending_outcomes(
                as_of_date=execution.as_of_date,
                max_decisions=500,
                num_workers=8,
            )
            logger.info("[Kronos] outcome_eval: evaluated %d outcomes", count)
            return True, None

        elif job.job_type == "kronos_scorecard":
            from apathis.core.database import get_db_manager
            from prometheus.decisions.scorecard import PredictionScorecard

            db = get_db_manager()
            sc = PredictionScorecard(db_manager=db)
            for horizon in (5, 21, 63):
                try:
                    report = sc.build_scorecard(
                        horizon_days=horizon,
                        max_decisions=500,
                        end_date=execution.as_of_date,
                    )
                    logger.info(
                        "[Kronos] scorecard %dd: n=%d hit_rate=%.1f%% spearman_rho=%.3f",
                        horizon,
                        report.total_predictions,
                        report.hit_rate * 100,
                        report.spearman_rho,
                    )
                except Exception:
                    logger.exception("[Kronos] scorecard %dd failed", horizon)
            return True, None

        elif job.job_type == "kronos_lambda_scorecard":
            from apathis.core.database import get_db_manager
            from prometheus.decisions.lambda_scorecard import LambdaScorecard

            db = get_db_manager()
            sc = LambdaScorecard(db_manager=db)
            try:
                report = sc.build_scorecard(
                    market_id="US_EQ",
                    end_date=execution.as_of_date,
                )
                logger.info(
                    "[Kronos] lambda_scorecard: n=%d mae=%.4f dir_acc=%.1f%% r2=%.3f",
                    report.total_predictions,
                    report.mae,
                    report.direction_accuracy * 100,
                    report.r_squared,
                )
            except Exception:
                logger.exception("[Kronos] lambda_scorecard failed (non-fatal)")
            return True, None

        elif job.job_type == "kronos_diagnostics":
            from apathis.core.database import get_db_manager
            from prometheus.meta.diagnostics import DiagnosticsEngine

            db = get_db_manager()
            engine = DiagnosticsEngine(db_manager=db)
            for strategy_id in _KRONOS_STRATEGY_IDS:
                try:
                    report = engine.analyze_strategy(strategy_id)
                    logger.info(
                        "[Kronos] diagnostics %s: sharpe=%.3f return=%.2f%% drawdown=%.2f%%"
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
                    logger.info("[Kronos] diagnostics %s: insufficient data (skipped)", strategy_id)
                except Exception:
                    logger.exception("[Kronos] diagnostics %s failed", strategy_id)
            return True, None

        elif job.job_type == "kronos_proposals":
            from apathis.core.database import get_db_manager
            from prometheus.meta.diagnostics import DiagnosticsEngine
            from prometheus.meta.proposal_generator import ProposalGenerator

            db = get_db_manager()
            engine = DiagnosticsEngine(db_manager=db)
            gen = ProposalGenerator(db_manager=db, diagnostics_engine=engine)
            total = 0
            for strategy_id in _KRONOS_STRATEGY_IDS:
                try:
                    proposals = gen.generate_proposals(strategy_id, auto_save=True)
                    logger.info(
                        "[Kronos] proposals %s: generated %d proposals",
                        strategy_id, len(proposals),
                    )
                    total += len(proposals)
                except ValueError:
                    logger.info("[Kronos] proposals %s: insufficient data (skipped)", strategy_id)
                except Exception:
                    logger.exception("[Kronos] proposals %s failed", strategy_id)
            logger.info("[Kronos] proposals total: %d generated", total)
            return True, None

        elif job.job_type == "kronos_log_report":
            from prometheus.monitoring.report_service import generate_log_report
            generate_log_report("log_daily")
            return True, None

        else:
            return False, f"Unknown kronos job_type: {job.job_type}"

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception("_execute_kronos_job: failed job_id=%s: %s", job.job_id, error_msg)
        return False, error_msg


# ============================================================================
# Retry Logic
# ============================================================================


def calculate_retry_delay(
    job: JobMetadata,
    attempt_number: int,
) -> float:
    """Calculate exponential backoff delay with jitter.

    Returns delay in seconds.
    """
    base_delay = job.retry_delay_seconds
    # Exponential backoff: base * 2^(attempt - 1)
    delay = base_delay * (2 ** (attempt_number - 1))
    # Add jitter: ±25%
    jitter = delay * 0.25 * (2 * random.random() - 1)
    return max(1.0, delay + jitter)


def should_retry_job(
    job: JobMetadata,
    execution: JobExecution,
) -> bool:
    """Determine if a failed job should be retried."""
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

        # Track active DAGs: {market_id: (DAG, dag_id)}
        self.active_dags: Dict[str, Tuple[DAG, str]] = {}

        # Track running jobs: {execution_id: (job, start_time)}
        self.running_jobs: Dict[str, Tuple[JobMetadata, datetime]] = {}

        # Track retry backoff: {execution_id: retry_after_timestamp}
        self.retry_backoff: Dict[str, datetime] = {}

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers."""

        def _signal_handler(signum, frame):
            logger.info("MarketAwareDaemon: received signal %d, shutting down", signum)
            self.shutdown_requested = True

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _initialize_dags(self, as_of_date: date) -> None:
        """Initialize or refresh DAGs for all configured markets."""
        for market_id in self.config.markets:
            if market_id == "INTEL":
                dag = build_intel_dag(as_of_date, is_sunday=as_of_date.weekday() == 6)
                dag_id = dag.dag_id  # e.g. "intel_daily_2026-03-19"
            elif market_id == "KRONOS":
                dag = build_kronos_dag(as_of_date)
                dag_id = dag.dag_id  # e.g. "kronos_daily_2026-03-19"
            else:
                dag = build_market_dag(market_id, as_of_date)
                dag_id = f"{market_id}_{as_of_date.isoformat()}"
            self.active_dags[market_id] = (dag, dag_id)
            logger.info(
                "_initialize_dags: initialized dag_id=%s with %d jobs",
                dag_id,
                len(dag.jobs),
            )

    def _get_completed_jobs(self, dag_id: str) -> Set[str]:
        """Get set of successfully completed job IDs for a DAG."""
        executions = get_dag_executions(self.db_manager, dag_id)
        return {
            exec.job_id
            for exec in executions
            if exec.status == JobStatus.SUCCESS
        }

    def _get_running_job_ids(self) -> Set[str]:
        """Get set of currently running job IDs."""
        return {job.job_id for job, _ in self.running_jobs.values()}

    def _check_timeouts(self, now: datetime) -> None:
        """Check for timed-out jobs and mark them as failed."""
        timed_out = []

        for execution_id, (job, start_time) in self.running_jobs.items():
            elapsed = (now - start_time).total_seconds()
            if elapsed > job.timeout_seconds:
                timed_out.append(execution_id)
                logger.warning(
                    "_check_timeouts: job_id=%s timed out after %.1fs (limit: %ds)",
                    job.job_id,
                    elapsed,
                    job.timeout_seconds,
                )

        for execution_id in timed_out:
            update_job_execution_status(
                self.db_manager,
                execution_id,
                JobStatus.FAILED,
                error_message=f"Job timed out after {job.timeout_seconds}s",
            )
            del self.running_jobs[execution_id]

    def _process_market(
        self,
        market_id: str,
        dag: DAG,
        dag_id: str,
        current_state: MarketState,
        as_of_date: date,
        now: datetime,
    ) -> None:
        """Process one market's DAG for the current cycle."""
        # Get DAG state
        completed = self._get_completed_jobs(dag_id)
        running = self._get_running_job_ids()

        # Get runnable jobs
        runnable = dag.get_runnable_jobs(completed, running, current_state)

        if not runnable:
            return

        logger.info(
            "_process_market: market_id=%s state=%s runnable=%d completed=%d running=%d",
            market_id,
            current_state.value,
            len(runnable),
            len(completed),
            len(running),
        )

        # Execute runnable jobs
        for job in runnable:
            # Check if we're in retry backoff
            latest_exec = get_latest_job_execution(self.db_manager, job.job_id, dag_id)

            if latest_exec and latest_exec.execution_id in self.retry_backoff:
                retry_after = self.retry_backoff[latest_exec.execution_id]
                if now < retry_after:
                    logger.debug(
                        "_process_market: job_id=%s in backoff until %s",
                        job.job_id,
                        retry_after,
                    )
                    continue
                else:
                    # Backoff expired, remove from tracking
                    del self.retry_backoff[latest_exec.execution_id]

            # Create or reuse execution record
            if latest_exec and latest_exec.status == JobStatus.PENDING:
                execution = latest_exec
            elif latest_exec and should_retry_job(job, latest_exec):
                # Increment attempt and retry
                increment_job_execution_attempt(self.db_manager, latest_exec.execution_id)
                execution = get_latest_job_execution(self.db_manager, job.job_id, dag_id)
            else:
                # Create new execution
                execution = create_job_execution(self.db_manager, job, dag_id, as_of_date)

            # Mark as running
            update_job_execution_status(self.db_manager, execution.execution_id, JobStatus.RUNNING)
            self.running_jobs[execution.execution_id] = (job, now)

            # Execute job
            success, error_msg = execute_job(
                self.db_manager, job, execution,
                options_mode=self.config.options_mode,
            )

            # Update status
            if success:
                update_job_execution_status(
                    self.db_manager,
                    execution.execution_id,
                    JobStatus.SUCCESS,
                )
                logger.info(
                    "_process_market: job_id=%s SUCCESS (execution_id=%s)",
                    job.job_id,
                    execution.execution_id,
                )
            else:
                update_job_execution_status(
                    self.db_manager,
                    execution.execution_id,
                    JobStatus.FAILED,
                    error_message=error_msg,
                )
                logger.error(
                    "_process_market: job_id=%s FAILED (execution_id=%s): %s",
                    job.job_id,
                    execution.execution_id,
                    error_msg,
                )

                # Schedule retry if applicable
                if should_retry_job(job, execution):
                    delay = calculate_retry_delay(job, execution.attempt_number)
                    retry_after = now + timedelta(seconds=delay)
                    self.retry_backoff[execution.execution_id] = retry_after
                    logger.info(
                        "_process_market: job_id=%s will retry in %.1fs (attempt %d/%d)",
                        job.job_id,
                        delay,
                        execution.attempt_number + 1,
                        job.max_retries,
                    )

            # Remove from running
            if execution.execution_id in self.running_jobs:
                del self.running_jobs[execution.execution_id]

    def _run_cycle(self, as_of_date: date) -> None:
        """Execute one orchestration cycle across all markets."""
        now = datetime.now(timezone.utc)

        # Check for timeouts
        self._check_timeouts(now)

        # Process each market
        for market_id in self.config.markets:
            if market_id not in self.active_dags:
                continue

            dag, dag_id = self.active_dags[market_id]
            # INTEL/KRONOS are not real markets — their jobs use required_state=None
            # so any state passes.  Use POST_CLOSE as a safe placeholder.
            if market_id in ("INTEL", "KRONOS"):
                current_state = MarketState.POST_CLOSE
            else:
                current_state = get_market_state(market_id, now)

            self._process_market(market_id, dag, dag_id, current_state, as_of_date, now)

    def run(self) -> None:
        """Run the orchestration daemon until shutdown is requested."""
        self._setup_signal_handlers()

        as_of_date = self.config.as_of_date or date.today()
        self._initialize_dags(as_of_date)

        logger.info(
            "MarketAwareDaemon: starting markets=%s as_of_date=%s poll_interval=%ds",
            ",".join(self.config.markets),
            as_of_date,
            self.config.poll_interval_seconds,
        )

        cycle_count = 0
        while not self.shutdown_requested:
            try:
                cycle_count += 1
                logger.debug("MarketAwareDaemon: cycle %d starting", cycle_count)

                self._run_cycle(as_of_date)

                # Sleep until next cycle
                time.sleep(self.config.poll_interval_seconds)

            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("MarketAwareDaemon: cycle %d failed: %s", cycle_count, exc)
                time.sleep(self.config.poll_interval_seconds)

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
        required=True,
        help="Market ID to orchestrate (e.g., US_EQ). Can specify multiple times.",
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
        markets=args.market,
        poll_interval_seconds=args.poll_interval_seconds,
        as_of_date=args.as_of_date,
        options_mode=args.options_mode,
    )

    db_manager = get_db_manager()
    daemon = MarketAwareDaemon(config, db_manager)
    daemon.run()


if __name__ == "__main__":  # pragma: no cover
    main()
