"""Prometheus v2 – C2 Backend Application.

FastAPI application that serves all monitoring, visualization, control,
and meta-orchestration APIs for the Prometheus C2 UI.

Run with:
    uvicorn prometheus.monitoring.app:app --reload --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import os
from datetime import date, datetime
from typing import IO, Optional

from apathis.core.logging import get_logger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from prometheus.monitoring.api import router as status_router
from prometheus.monitoring.backtests_api import router as backtests_router
from prometheus.monitoring.control_api import router as control_router
from prometheus.monitoring.intelligence_api import intelligence_router
from prometheus.monitoring.log_buffer import install_buffer
from prometheus.monitoring.logs_api import router as logs_router
from prometheus.monitoring.meta_api import kronos_router, meta_router
from prometheus.monitoring.options_api import router as options_router
from prometheus.monitoring.visualization_api import router as viz_router

logger = get_logger(__name__)


# ============================================================================
# Application Setup
# ============================================================================


app = FastAPI(
    title="Prometheus C2 Backend",
    description="Monitoring, visualization, and control APIs for Prometheus v2",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)


# ============================================================================
# CORS Configuration
# ============================================================================

# Allow Godot client to connect from localhost during development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:*",
        "http://127.0.0.1:*",
        "godot://",  # For Godot client
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Router Registration
# ============================================================================

# Trading dashboard endpoints (overview, pipeline, portfolio, execution)
app.include_router(status_router)

# Visualization endpoints (ANT_HILL, scenes, traces)
app.include_router(viz_router)

# Control endpoints (backtests, configs, DAG scheduling)
app.include_router(control_router)

# Backtest result endpoints (chart-friendly series)
app.include_router(backtests_router)

# Meta endpoints (configs, performance)
app.include_router(meta_router)

# Kronos Chat endpoint
app.include_router(kronos_router)

# Intelligence endpoints (diagnostics, proposals)
app.include_router(intelligence_router)

# Options backtest results endpoints
app.include_router(options_router)

# Logs & Reports endpoints
app.include_router(logs_router)

# NOTE: Info-layer routes (entities, intel, nation, geo) are served
# by the Apathis API on :8100.


# ============================================================================
# Health Check
# ============================================================================


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint with basic system info."""
    return {
        "service": "Prometheus C2 Backend",
        "version": "0.1.0",
        "status": "operational",
        "docs": "/api/docs",
    }


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint for monitoring."""
    return {"status": "healthy"}


# ============================================================================
# Scheduled Intel Generation
# ============================================================================

# Weekly intel: Sunday 04:00 local (ready by morning).
# Daily trading report: Mon–Fri 22:00 local (after US market close).
_WEEKLY_HOUR = 4
_WEEKLY_MINUTE = 0
_TRADING_DAILY_HOUR = 22
_TRADING_DAILY_MINUTE = 0
_intel_scheduler_task: asyncio.Task | None = None
_trading_scheduler_task: asyncio.Task | None = None
_scheduler_lock_file: Optional[IO[str]] = None


def _env_bool(name: str, *, default: bool) -> bool:
    """Parse a boolean environment variable with sane defaults."""
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    logger.warning("Invalid boolean value for %s=%r; using default=%s", name, raw, default)
    return default


def _acquire_scheduler_leader_lock() -> bool:
    """Try to acquire process-level scheduler leadership lock.

    This prevents duplicate scheduled jobs when multiple backend processes
    (different ports and/or worker processes) run on the same host.
    """
    global _scheduler_lock_file
    lock_path = os.getenv("PROMETHEUS_SCHEDULER_LOCK_FILE", "/tmp/prometheus-internal-schedulers.lock")

    try:
        import fcntl
    except Exception:
        logger.warning("fcntl unavailable; scheduler leader lock disabled")
        return True

    lock_file: Optional[IO[str]] = None
    try:
        lock_file = open(lock_path, "a+", encoding="utf-8")
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _scheduler_lock_file = lock_file
        logger.info("Acquired internal scheduler leader lock: %s", lock_path)
        return True
    except BlockingIOError:
        logger.info(
            "Internal schedulers not started: lock already held by another process (%s)",
            lock_path,
        )
        if lock_file is not None:
            try:
                lock_file.close()
            except Exception:
                pass
        return False
    except Exception:
        logger.exception("Failed to acquire internal scheduler leader lock (%s)", lock_path)
        if lock_file is not None:
            try:
                lock_file.close()
            except Exception:
                pass
        return False


def _release_scheduler_leader_lock() -> None:
    """Release scheduler leadership lock if held."""
    global _scheduler_lock_file
    if _scheduler_lock_file is None:
        return

    try:
        import fcntl

        fcntl.flock(_scheduler_lock_file.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        _scheduler_lock_file.close()
    except Exception:
        pass
    _scheduler_lock_file = None


async def _intel_weekly_scheduler() -> None:
    """Background loop: run the weekly intel DAG at Sunday 04:00 local."""
    last_run_date: date | None = None

    while True:
        try:
            await asyncio.sleep(60)  # check every minute
            now = datetime.now()

            # Sunday = weekday 6
            if now.weekday() != 6:
                continue
            if now.hour != _WEEKLY_HOUR or now.minute != _WEEKLY_MINUTE:
                continue
            if last_run_date == now.date():
                continue  # already ran today

            last_run_date = now.date()
            logger.info("[scheduler] Sunday %02d:%02d — launching weekly intel DAG", _WEEKLY_HOUR, _WEEKLY_MINUTE)

            def _run_weekly_dag() -> None:
                from apathis.intel.pipeline import run_daily_sitrep, run_flash_check, run_weekly_assessment

                from prometheus.monitoring.report_service import generate_log_report

                run_flash_check()
                logger.info("[scheduler] Flash check complete")
                run_daily_sitrep()
                logger.info("[scheduler] Daily SITREP complete")
                run_weekly_assessment()
                logger.info("[scheduler] Weekly assessment complete")
                generate_log_report("log_weekly")
                logger.info("[scheduler] Weekly log health report complete")

            await asyncio.to_thread(_run_weekly_dag)
            logger.info("[scheduler] Weekly intel DAG finished")

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("[scheduler] Weekly intel DAG failed")


async def _trading_report_scheduler() -> None:
    """Background loop: generate daily trading report Mon–Fri at 22:00 local.

    Also generates a weekly trading report on Sundays (piggybacking on
    the 04:00 intel window so they run back-to-back).
    """
    last_run_date: date | None = None

    while True:
        try:
            await asyncio.sleep(60)
            now = datetime.now()

            should_run = False

            # Mon–Fri (0–4) at 22:00 → daily trading report
            if now.weekday() <= 4 and now.hour == _TRADING_DAILY_HOUR and now.minute == _TRADING_DAILY_MINUTE:
                should_run = True
                report_type = "trading_daily"
            # Sunday at 04:01 → weekly trading report (1 min after intel DAG starts)
            elif now.weekday() == 6 and now.hour == _WEEKLY_HOUR and now.minute == _WEEKLY_MINUTE + 1:
                should_run = True
                report_type = "trading_weekly"

            if not should_run:
                continue
            if last_run_date == now.date() and report_type == "trading_daily":
                continue

            last_run_date = now.date()
            logger.info("[scheduler] Generating %s trading report", report_type)

            def _gen(rt: str = report_type) -> None:
                from prometheus.monitoring.trading_report_service import generate_trading_report
                generate_trading_report(report_type=rt)

            await asyncio.to_thread(_gen)
            logger.info("[scheduler] %s trading report complete", report_type)

        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("[scheduler] Trading report generation failed")


# ============================================================================
# Startup/Shutdown Events
# ============================================================================


@app.on_event("startup")
async def startup_event() -> None:
    """Initialize connections and resources on startup."""
    global _intel_scheduler_task, _trading_scheduler_task
    install_buffer()
    enable_schedulers = _env_bool("PROMETHEUS_ENABLE_INTERNAL_SCHEDULERS", default=True)

    if not enable_schedulers:
        logger.info("Internal schedulers disabled via PROMETHEUS_ENABLE_INTERNAL_SCHEDULERS")
    elif _acquire_scheduler_leader_lock():
        # Scheduled report generation (leader process only)
        _intel_scheduler_task = asyncio.create_task(_intel_weekly_scheduler())
        _trading_scheduler_task = asyncio.create_task(_trading_report_scheduler())
        logger.info("Internal schedulers started in leader process")
    else:
        logger.info("Internal schedulers skipped in this process (non-leader)")
    print("Prometheus C2 Backend starting up...")
    if enable_schedulers and (_intel_scheduler_task is not None and _trading_scheduler_task is not None):
        print(f"Weekly intel scheduled for Sunday {_WEEKLY_HOUR:02d}:{_WEEKLY_MINUTE:02d} local")
        print(f"Daily trading report scheduled Mon–Fri {_TRADING_DAILY_HOUR:02d}:{_TRADING_DAILY_MINUTE:02d} local")
    else:
        print("Internal schedulers not active in this process")
    print("API docs available at: http://localhost:8000/api/docs")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Clean up resources on shutdown."""
    global _intel_scheduler_task, _trading_scheduler_task
    for task in (_intel_scheduler_task, _trading_scheduler_task):
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    _release_scheduler_leader_lock()
    print("Prometheus C2 Backend shutting down...")
