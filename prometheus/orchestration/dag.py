"""Prometheus v2 – DAG Definition Framework

This module defines the job dependency and DAG structure for orchestrating
daily market pipelines. Each market has a DAG that describes:
- What jobs need to run (ingestion, features, profiles, engines)
- Dependencies between jobs (features depends on ingestion, etc.)
- Required market states for each job (POST_CLOSE, etc.)
- Retry policies and priority tiers

The DAG framework is independent of the execution layer - it just defines
the logical structure. The market-aware daemon will use these DAGs to
determine which jobs can run at any given time.

Author: Prometheus Team
Created: 2025-12-01
Status: Development
Version: v1.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum

from apathis.core.logging import get_logger
from apathis.core.market_state import MarketState

from prometheus.pipeline.state import RunPhase

logger = get_logger(__name__)


# ============================================================================
# Job Metadata
# ============================================================================


class JobPriority(int, Enum):
    """Priority tiers for jobs (lower number = higher priority).

    Tier 1: Critical jobs that must complete (ingestion, core engines)
    Tier 2: Standard jobs (assessment, universe)
    Tier 3: Optional jobs (analytics, reports)
    """

    CRITICAL = 1
    STANDARD = 2
    OPTIONAL = 3


class JobStatus(str, Enum):
    """Status of a job execution."""

    PENDING = "PENDING"      # Not yet started
    RUNNING = "RUNNING"      # Currently executing
    SUCCESS = "SUCCESS"      # Completed successfully
    FAILED = "FAILED"        # Failed after retries
    SKIPPED = "SKIPPED"      # Skipped due to dependencies


@dataclass(frozen=True)
class JobMetadata:
    """Metadata for a single job in a DAG.

    Attributes:
        job_id: Unique identifier for this job (e.g., "us_eq_ingest_prices_2025-11-21")
        job_type: Logical type (e.g., "ingest_prices", "compute_returns", "run_regime")
        market_id: Market this job belongs to (None for global jobs)
        required_state: Market state required to run (None = any state OK)
        dependencies: List of job_ids that must complete before this job
        run_phase: Optional RunPhase this job maps to (for engine jobs)
        max_retries: Maximum retry attempts on failure
        retry_delay_seconds: Delay between retries
        priority: Priority tier
        timeout_seconds: Maximum execution time before considering failed
    """

    job_id: str
    job_type: str
    market_id: str | None
    required_state: MarketState | None = None
    dependencies: tuple[str, ...] = field(default_factory=tuple)
    run_phase: RunPhase | None = None
    max_retries: int = 3
    retry_delay_seconds: int = 300  # 5 minutes
    priority: JobPriority = JobPriority.STANDARD
    timeout_seconds: int = 3600  # 1 hour default


# ============================================================================
# DAG Structure
# ============================================================================


@dataclass
class DAG:
    """Directed Acyclic Graph of jobs for a market and date.

    Attributes:
        dag_id: Unique identifier (e.g., "us_eq_daily_2025-11-21")
        market_id: Market this DAG is for
        as_of_date: Trading date
        jobs: Dictionary mapping job_id to JobMetadata
    """

    dag_id: str
    market_id: str
    as_of_date: date
    jobs: dict[str, JobMetadata]

    def get_runnable_jobs(
        self,
        completed_jobs: set[str],
        running_jobs: set[str],
        current_market_state: MarketState,
    ) -> list[JobMetadata]:
        """Get jobs that are ready to run.

        A job is runnable if:
        1. Not already completed or running
        2. All dependencies are completed
        3. Market is in required state (or no state requirement)

        Args:
            completed_jobs: Set of job_ids that have completed successfully
            running_jobs: Set of job_ids currently executing
            current_market_state: Current state of the market

        Returns:
            List of JobMetadata for jobs ready to run, sorted by priority
        """
        runnable = []

        for job_id, job in self.jobs.items():
            # Skip if already done or running
            if job_id in completed_jobs or job_id in running_jobs:
                continue

            # Check dependencies
            deps_satisfied = all(dep in completed_jobs for dep in job.dependencies)
            if not deps_satisfied:
                continue

            # Check market state requirement
            if job.required_state is not None and job.required_state != current_market_state:
                continue

            runnable.append(job)

        # Sort by priority (critical first)
        return sorted(runnable, key=lambda j: (j.priority.value, j.job_id))

    def get_dependency_chain(self, job_id: str) -> list[str]:
        """Get all transitive dependencies for a job.

        Args:
            job_id: Job to get dependencies for

        Returns:
            List of job_ids this job depends on (directly or transitively)
        """
        if job_id not in self.jobs:
            return []

        visited = set()
        to_visit = [job_id]

        while to_visit:
            current = to_visit.pop()
            if current in visited:
                continue
            visited.add(current)

            job = self.jobs.get(current)
            if job:
                to_visit.extend(job.dependencies)

        # Remove the job itself
        visited.discard(job_id)
        return sorted(visited)

    def validate(self, skip_missing_deps: bool = False) -> list[str]:
        """Validate DAG structure.

        Args:
            skip_missing_deps: If True, don't error on missing dependencies
                (useful for global DAGs that reference jobs from other DAGs)

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check for cycles by attempting to build dependency chain
        # If a job appears in its own chain, that's a cycle
        for job_id in self.jobs:
            visited = set()
            to_visit = [job_id]
            has_cycle = False

            while to_visit:
                current = to_visit.pop()
                if current == job_id and visited:  # Found ourselves after visiting others
                    has_cycle = True
                    break
                if current in visited:
                    continue
                visited.add(current)

                job = self.jobs.get(current)
                if job:
                    to_visit.extend(job.dependencies)

            if has_cycle:
                errors.append(f"Circular dependency detected for job {job_id}")

        # Check that all dependencies exist (unless skipped)
        if not skip_missing_deps:
            for job_id, job in self.jobs.items():
                for dep in job.dependencies:
                    if dep not in self.jobs:
                        errors.append(f"Job {job_id} depends on non-existent job {dep}")

        return errors


# ============================================================================
# DAG Builder Functions
# ============================================================================


def build_market_dag(market_id: str, as_of_date: date) -> DAG:
    """Build a standard daily DAG for a market.

    Creates a DAG with the standard pipeline:
    1. Ingestion (POST_CLOSE required)
       - ingest_prices
       - ingest_factors
    2. Features (depends on ingestion)
       - compute_returns
       - compute_volatility
       - build_numeric_windows
    3. Profiles (depends on features, POST_CLOSE required)
       - update_profiles
    4. Engines (depends on features + profiles)
       - run_signals (maps to DATA_READY → SIGNALS_DONE)
       - run_universes (maps to SIGNALS_DONE → UNIVERSES_DONE)
       - run_books (maps to UNIVERSES_DONE → BOOKS_DONE)

    Args:
        market_id: Market identifier (e.g., "US_EQ")
        as_of_date: Trading date for this DAG

    Returns:
        DAG instance with all jobs configured
    """
    dag_id = f"{market_id.lower()}_daily_{as_of_date.isoformat()}"
    date_str = as_of_date.isoformat()

    # Job ID helper
    def job_id(job_type: str) -> str:
        return f"{market_id.lower()}_{job_type}_{date_str}"

    jobs: dict[str, JobMetadata] = {}

    # ========================================================================
    # Phase 1: Ingestion (POST_CLOSE required)
    # ========================================================================

    jobs[job_id("ingest_prices")] = JobMetadata(
        job_id=job_id("ingest_prices"),
        job_type="ingest_prices",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(),
        priority=JobPriority.CRITICAL,
        max_retries=12,           # EODHD publishes 1-2h after close
        retry_delay_seconds=600,  # 10 min between retries → covers 2h window
    )

    jobs[job_id("ingest_factors")] = JobMetadata(
        job_id=job_id("ingest_factors"),
        job_type="ingest_factors",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(),
        priority=JobPriority.STANDARD,
    )

    # ========================================================================
    # Phase 2: Feature Computation (depends on ingestion)
    # ========================================================================

    jobs[job_id("compute_returns")] = JobMetadata(
        job_id=job_id("compute_returns"),
        job_type="compute_returns",
        market_id=market_id,
        dependencies=(job_id("ingest_prices"),),
        priority=JobPriority.CRITICAL,
    )

    jobs[job_id("compute_volatility")] = JobMetadata(
        job_id=job_id("compute_volatility"),
        job_type="compute_volatility",
        market_id=market_id,
        dependencies=(job_id("ingest_prices"),),
        priority=JobPriority.CRITICAL,
    )

    jobs[job_id("build_numeric_windows")] = JobMetadata(
        job_id=job_id("build_numeric_windows"),
        job_type="build_numeric_windows",
        market_id=market_id,
        dependencies=(
            job_id("compute_returns"),
            job_id("compute_volatility"),
        ),
        priority=JobPriority.CRITICAL,
        timeout_seconds=7200,  # 2 hours for large datasets
    )

    # ========================================================================
    # Phase 3: Profiles (depends on features, POST_CLOSE preferred)
    # ========================================================================

    jobs[job_id("update_profiles")] = JobMetadata(
        job_id=job_id("update_profiles"),
        job_type="update_profiles",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(job_id("build_numeric_windows"),),
        priority=JobPriority.STANDARD,
    )

    # ========================================================================
    # Phase 4: Engines (depends on features + profiles)
    # ========================================================================

    jobs[job_id("run_signals")] = JobMetadata(
        job_id=job_id("run_signals"),
        job_type="run_signals",
        market_id=market_id,
        dependencies=(
            job_id("build_numeric_windows"),
            job_id("update_profiles"),
        ),
        run_phase=RunPhase.SIGNALS_DONE,  # Advances to this phase
        priority=JobPriority.CRITICAL,
        timeout_seconds=3600,
    )

    jobs[job_id("run_universes")] = JobMetadata(
        job_id=job_id("run_universes"),
        job_type="run_universes",
        market_id=market_id,
        dependencies=(job_id("run_signals"),),
        run_phase=RunPhase.UNIVERSES_DONE,
        priority=JobPriority.CRITICAL,
    )

    jobs[job_id("run_books")] = JobMetadata(
        job_id=job_id("run_books"),
        job_type="run_books",
        market_id=market_id,
        dependencies=(job_id("run_universes"),),
        run_phase=RunPhase.BOOKS_DONE,
        priority=JobPriority.CRITICAL,
    )

    # ========================================================================
    # Phase 5: Execution (depends on books, POST_CLOSE required)
    # ========================================================================

    jobs[job_id("run_execution")] = JobMetadata(
        job_id=job_id("run_execution"),
        job_type="run_execution",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(job_id("run_books"),),
        run_phase=RunPhase.EXECUTION_DONE,
        priority=JobPriority.CRITICAL,
        timeout_seconds=600,  # 10 minutes for order execution
    )

    # ========================================================================
    # Phase 6: Options (depends on execution, POST_CLOSE required)
    # ========================================================================

    jobs[job_id("run_options")] = JobMetadata(
        job_id=job_id("run_options"),
        job_type="run_options",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(job_id("run_execution"),),
        run_phase=RunPhase.OPTIONS_DONE,
        priority=JobPriority.CRITICAL,
        timeout_seconds=600,  # 10 minutes for options evaluation + execution
    )

    # ========================================================================
    # Phase 6b: Snapshot positions — daily IBKR position snapshot for the
    # equity curve chart. Runs after options, before finalize. Non-blocking:
    # if IBKR is down, snapshot fails silently and finalize still runs.
    # ========================================================================

    jobs[job_id("snapshot_positions")] = JobMetadata(
        job_id=job_id("snapshot_positions"),
        job_type="snapshot_positions",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(job_id("run_options"),),
        run_phase=RunPhase.OPTIONS_DONE,
        priority=JobPriority.LOW,
        timeout_seconds=120,
        max_retries=1,
        retry_delay_seconds=60,
    )

    # ========================================================================
    # Phase 7: Finalize — marks run COMPLETED regardless of whether options
    # or snapshot succeeded or were skipped.
    # ========================================================================

    jobs[job_id("finalize")] = JobMetadata(
        job_id=job_id("finalize"),
        job_type="finalize",
        market_id=market_id,
        required_state=MarketState.POST_CLOSE,
        dependencies=(job_id("snapshot_positions"),),
        run_phase=RunPhase.COMPLETED,
        priority=JobPriority.CRITICAL,
        timeout_seconds=60,
    )

    # Create and validate DAG
    dag = DAG(
        dag_id=dag_id,
        market_id=market_id,
        as_of_date=as_of_date,
        jobs=jobs,
    )

    # Validate structure
    errors = dag.validate()
    if errors:
        logger.error("DAG validation failed for %s: %s", dag_id, errors)
        raise ValueError(f"Invalid DAG: {errors}")

    logger.info(
        "Built DAG %s with %d jobs: %s",
        dag_id,
        len(jobs),
        ", ".join(job.job_type for job in jobs.values()),
    )

    return dag


def build_global_dag(as_of_date: date, regional_dags: list[DAG]) -> DAG:
    """Build a global DAG that depends on regional DAGs completing.

    This creates cross-market jobs that run after all regional markets
    have completed their pipelines (e.g., global regime analysis).

    Args:
        as_of_date: Trading date
        regional_dags: List of regional DAGs this depends on

    Returns:
        Global DAG instance
    """
    dag_id = f"global_daily_{as_of_date.isoformat()}"
    date_str = as_of_date.isoformat()

    jobs: dict[str, JobMetadata] = {}

    # Collect all "run_signals" jobs from regional DAGs as dependencies
    regional_signals = []
    for regional_dag in regional_dags:
        for job_id, job in regional_dag.jobs.items():
            if job.job_type == "run_signals":
                regional_signals.append(job_id)

    # Global regime job depends on all regional signals completing
    if regional_signals:
        jobs[f"global_regime_{date_str}"] = JobMetadata(
            job_id=f"global_regime_{date_str}",
            job_type="global_regime",
            market_id=None,  # Global job
            dependencies=tuple(regional_signals),
            priority=JobPriority.STANDARD,
        )

    dag = DAG(
        dag_id=dag_id,
        market_id="GLOBAL",
        as_of_date=as_of_date,
        jobs=jobs,
    )

    # Global DAGs reference jobs from other DAGs, so skip missing dep check
    errors = dag.validate(skip_missing_deps=True)
    if errors:
        logger.error("Global DAG validation failed: %s", errors)
        raise ValueError(f"Invalid global DAG: {errors}")

    return dag


def build_intel_dag(as_of_date: date, is_sunday: bool = False) -> DAG:
    """Build an intelligence-generation DAG.

    Intel jobs are OPTIONAL priority so they never block the main
    trading pipeline. They have no market-state requirements (intel
    is market-agnostic).

    Args:
        as_of_date: Date for this DAG.
        is_sunday: If True, include weekly assessment job.

    Returns:
        DAG with intel generation jobs.
    """
    dag_id = f"intel_daily_{as_of_date.isoformat()}"
    date_str = as_of_date.isoformat()

    jobs: dict[str, JobMetadata] = {}

    # Flash check — lightweight, no LLM, runs frequently
    jobs[f"intel_flash_check_{date_str}"] = JobMetadata(
        job_id=f"intel_flash_check_{date_str}",
        job_type="intel_flash_check",
        market_id=None,
        required_state=None,  # Market-agnostic
        dependencies=(),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=300,  # First run ~180s (cold SQL cache), subsequent <1s
    )

    # Daily SITREP — heavy (4 analysts + synthesis via Ollama)
    jobs[f"intel_daily_sitrep_{date_str}"] = JobMetadata(
        job_id=f"intel_daily_sitrep_{date_str}",
        job_type="intel_daily_sitrep",
        market_id=None,
        required_state=None,
        dependencies=(f"intel_flash_check_{date_str}",),  # Flash first so alerts feed into context
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=300,
        timeout_seconds=5400,  # 90 min — 5 sequential Ollama LLM calls take 45-75 min
    )

    # Weekly assessment — only on Sundays, depends on daily SITREP
    if is_sunday:
        jobs[f"intel_weekly_{date_str}"] = JobMetadata(
            job_id=f"intel_weekly_{date_str}",
            job_type="intel_weekly_assessment",
            market_id=None,
            required_state=None,
            dependencies=(f"intel_daily_sitrep_{date_str}",),
            priority=JobPriority.OPTIONAL,
            max_retries=2,
            retry_delay_seconds=300,
            timeout_seconds=2400,  # 40 min — deeper analysis
        )

    # Log health report — generate after SITREP
    jobs[f"intel_log_health_{date_str}"] = JobMetadata(
        job_id=f"intel_log_health_{date_str}",
        job_type="intel_log_health",
        market_id=None,
        required_state=None,
        dependencies=(f"intel_daily_sitrep_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=600,
    )

    dag = DAG(
        dag_id=dag_id,
        market_id="INTEL",
        as_of_date=as_of_date,
        jobs=jobs,
    )

    errors = dag.validate()
    if errors:
        logger.error("Intel DAG validation failed: %s", errors)
        raise ValueError(f"Invalid intel DAG: {errors}")

    logger.info(
        "Built intel DAG %s with %d jobs: %s",
        dag_id,
        len(jobs),
        ", ".join(job.job_type for job in jobs.values()),
    )

    return dag


def build_iris_dag(as_of_date: date) -> DAG:
    """Build the Iris meta-intelligence DAG.

    Evaluates the quality of all trading decisions at 5/21/63-day
    horizons, builds prediction scorecards, runs diagnostics against
    backtest history, generates config-improvement proposals, and produces
    the daily operational log-health report.

    Jobs (all OPTIONAL, required_state=None — never block the pipeline):

        iris_outcome_eval   — OutcomeEvaluator: evaluate pending decisions
        iris_scorecard      — PredictionScorecard: assessment hit-rate / Sharpe IC
        iris_lambda_sc      — LambdaScorecard: lambda-hat direction accuracy
        iris_diagnostics    — DiagnosticsEngine: backtest performance analysis
        iris_proposals      — ProposalGenerator: generate config proposals
        iris_log_report     — Iris log-health LLM report

    Args:
        as_of_date: Date for this DAG.

    Returns:
        DAG with Iris intelligence jobs.
    """
    dag_id = f"iris_daily_{as_of_date.isoformat()}"
    date_str = as_of_date.isoformat()

    jobs: dict[str, JobMetadata] = {}

# 1. Outcome evaluation — must run first; evaluates all pending decisions
    jobs[f"iris_outcome_eval_{date_str}"] = JobMetadata(
        job_id=f"iris_outcome_eval_{date_str}",
        job_type=f"iris_outcome_eval",
        market_id=None,
        required_state=None,
        dependencies=(),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=1800,  # Can be slow for large backlogs
    )

    # 2. Prediction scorecard — compares assessment scores vs realized returns
    #    63d horizon processes 250K+ evaluations and needs >10 min; no timeout.
    jobs[f"iris_scorecard_{date_str}"] = JobMetadata(
        job_id=f"iris_scorecard_{date_str}",
        job_type=f"iris_scorecard",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_outcome_eval_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=3600,
    )

    # 3. Lambda scorecard — evaluates lambda_hat directional accuracy
    jobs[f"iris_lambda_sc_{date_str}"] = JobMetadata(
        job_id=f"iris_lambda_sc_{date_str}",
        job_type=f"iris_lambda_scorecard",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_outcome_eval_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=300,
    )

    # 4. Diagnostics — analyzes backtest performance (non-fatal if no data)
    jobs[f"iris_diagnostics_{date_str}"] = JobMetadata(
        job_id=f"iris_diagnostics_{date_str}",
        job_type=f"iris_diagnostics",
        market_id=None,
        required_state=None,
        dependencies=(),
        priority=JobPriority.OPTIONAL,
        max_retries=1,
        retry_delay_seconds=300,
        timeout_seconds=600,
    )

    # 5. Proposals — generates config-improvement proposals from diagnostics
    jobs[f"iris_proposals_{date_str}"] = JobMetadata(
        job_id=f"iris_proposals_{date_str}",
        job_type=f"iris_proposals",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_diagnostics_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=1,
        retry_delay_seconds=300,
        timeout_seconds=300,
    )

    # 7. Live performance — rolling Sharpe/drawdown from live outcomes
    jobs[f"iris_live_perf_{date_str}"] = JobMetadata(
        job_id=f"iris_live_perf_{date_str}",
        job_type=f"iris_live_perf",
        market_id=None,
        required_state=None,
        dependencies=(),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=300,
    )

    # 8. Regime-conditioned evaluation — depends on outcome_eval for fresh data
    jobs[f"iris_regime_eval_{date_str}"] = JobMetadata(
        job_id=f"iris_regime_eval_{date_str}",
        job_type=f"iris_regime_eval",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_outcome_eval_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=300,
    )

    # 9. Fragility signal check — depends on outcome_eval
    jobs[f"iris_fragility_check_{date_str}"] = JobMetadata(
        job_id=f"iris_fragility_check_{date_str}",
        job_type=f"iris_fragility_check",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_outcome_eval_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=300,
    )

    # 10. Hedge effectiveness — depends on outcome_eval
    jobs[f"iris_hedge_eval_{date_str}"] = JobMetadata(
        job_id=f"iris_hedge_eval_{date_str}",
        job_type=f"iris_hedge_eval",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_outcome_eval_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=300,
    )

    # 6. Log-health LLM report — runs after outcome eval for fresh data
    jobs[f"iris_log_report_{date_str}"] = JobMetadata(
        job_id=f"iris_log_report_{date_str}",
        job_type=f"iris_log_report",
        market_id=None,
        required_state=None,
        dependencies=(f"iris_outcome_eval_{date_str}",),
        priority=JobPriority.OPTIONAL,
        max_retries=2,
        retry_delay_seconds=120,
        timeout_seconds=600,
    )

    dag = DAG(
        dag_id=dag_id,
        market_id="IRIS",
        as_of_date=as_of_date,
        jobs=jobs,
    )

    errors = dag.validate()
    if errors:
        logger.error("Iris DAG validation failed: %s", errors)
        raise ValueError(f"Invalid Iris DAG: {errors}")

    logger.info(
        "Built Iris DAG %s with %d jobs: %s",
        dag_id,
        len(jobs),
        ", ".join(job.job_type for job in jobs.values()),
    )

    return dag
