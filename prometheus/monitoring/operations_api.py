"""Prometheus – Operations Dashboard API.

Provides comprehensive visibility into system health:
- Systemd service status
- 14-day job execution history with per-market breakdown
- Detailed per-day drill-down with every job, engine run, and report
"""

from __future__ import annotations

import subprocess
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from fastapi import APIRouter, Query

logger = get_logger(__name__)
router = APIRouter(prefix="/api/ops", tags=["operations"])

# Services to monitor
SERVICES = [
    {"name": "prometheus-daemon", "description": "Market-Aware Orchestration Daemon"},
    {"name": "prometheus-api", "description": "Prometheus Monitoring API (port 8200)"},
    {"name": "apathis-api", "description": "Apathis Intelligence API (port 8100)"},
    {"name": "postgresql", "description": "PostgreSQL Database"},
]


def _service_status(service_name: str) -> Dict[str, Any]:
    """Query systemd for service status."""
    try:
        result = subprocess.run(
            ["systemctl", "show", service_name,
             "--property=ActiveState,SubState,MainPID,ExecMainStartTimestamp,"
             "NRestarts,Result,MemoryCurrent"],
            capture_output=True, text=True, timeout=5,
        )
        props = {}
        for line in result.stdout.strip().split("\n"):
            if "=" in line:
                k, v = line.split("=", 1)
                props[k] = v

        active = props.get("ActiveState", "unknown")
        return {
            "name": service_name,
            "active": active,
            "sub_state": props.get("SubState", "unknown"),
            "pid": int(props.get("MainPID", 0)) or None,
            "started_at": props.get("ExecMainStartTimestamp") or None,
            "restarts": int(props.get("NRestarts", 0)),
            "result": props.get("Result", "unknown"),
            "memory_mb": round(int(mem_raw) / 1048576, 1) or None
                if (mem_raw := props.get("MemoryCurrent", "0")).isdigit()
                else None,
            "healthy": active == "active",
        }
    except Exception as exc:
        return {
            "name": service_name,
            "active": "error",
            "sub_state": str(exc),
            "pid": None,
            "started_at": None,
            "restarts": 0,
            "result": "error",
            "memory_mb": None,
            "healthy": False,
        }


def _table_exists(table_name: str) -> bool:
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s)",
                (table_name,),
            )
            return bool(cur.fetchone()[0])
        finally:
            cur.close()


def _daily_summaries(days: int = 14) -> List[Dict[str, Any]]:
    """Get per-day job execution summaries for the last N days."""
    if not _table_exists("job_executions"):
        return []

    db = get_db_manager()
    since = date.today() - timedelta(days=days)

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            # Get the latest execution per job per dag (deduped)
            cur.execute("""
                WITH latest AS (
                    SELECT DISTINCT ON (job_id, dag_id)
                        job_id, job_type, dag_id, market_id, as_of_date,
                        status, started_at, completed_at,
                        attempt_number, error_message
                    FROM job_executions
                    WHERE as_of_date >= %s
                    ORDER BY job_id, dag_id, created_at DESC
                )
                SELECT
                    as_of_date,
                    dag_id,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'SUCCESS') AS success,
                    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed,
                    COUNT(*) FILTER (WHERE status = 'SKIPPED') AS skipped,
                    COUNT(*) FILTER (WHERE status = 'RUNNING') AS running,
                    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending
                FROM latest
                GROUP BY as_of_date, dag_id
                ORDER BY as_of_date DESC, dag_id
            """, (since,))
            rows = cur.fetchall()
        finally:
            cur.close()

    # Group by date
    by_date: Dict[str, Dict[str, Any]] = {}
    for as_of, dag_id, total, success, failed, skipped, running, pending in rows:
        d = as_of.isoformat()
        if d not in by_date:
            by_date[d] = {
                "date": d,
                "total": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "running": 0,
                "pending": 0,
                "dags": {},
            }
        entry = by_date[d]
        entry["total"] += total
        entry["success"] += success
        entry["failed"] += failed
        entry["skipped"] += skipped
        entry["running"] += running
        entry["pending"] += pending
        entry["dags"][dag_id] = {
            "dag_id": dag_id,
            "total": total,
            "success": success,
            "failed": failed,
            "skipped": skipped,
            "running": running,
            "pending": pending,
        }

    # Fill in missing dates
    result = []
    for i in range(days):
        d = (date.today() - timedelta(days=i)).isoformat()
        if d in by_date:
            entry = by_date[d]
            entry["status"] = (
                "ok" if entry["failed"] == 0 and entry["success"] > 0
                else "partial" if entry["success"] > 0 and entry["failed"] > 0
                else "failed" if entry["failed"] > 0
                else "idle" if entry["total"] == 0
                else "running" if entry["running"] > 0
                else "pending"
            )
            result.append(entry)
        else:
            result.append({
                "date": d,
                "total": 0, "success": 0, "failed": 0,
                "skipped": 0, "running": 0, "pending": 0,
                "dags": {},
                "status": "idle",
            })

    return result


def _day_detail(target_date: date) -> Dict[str, Any]:
    """Get detailed breakdown for a specific date."""
    db = get_db_manager()
    result: Dict[str, Any] = {
        "date": target_date.isoformat(),
        "jobs": [],
        "engine_runs": [],
        "intel_briefs": [],
    }

    # Job executions
    if _table_exists("job_executions"):
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT
                        execution_id, job_id, job_type, dag_id, market_id,
                        status, started_at, completed_at,
                        attempt_number, error_message, created_at
                    FROM job_executions
                    WHERE as_of_date = %s
                    ORDER BY dag_id, created_at
                """, (target_date,))
                for row in cur.fetchall():
                    (exec_id, job_id, job_type, dag_id, market_id,
                     status, started_at, completed_at,
                     attempt_number, error_message, created_at) = row
                    duration_s = None
                    if started_at and completed_at:
                        duration_s = round((completed_at - started_at).total_seconds(), 1)
                    result["jobs"].append({
                        "execution_id": str(exec_id),
                        "job_id": str(job_id),
                        "job_type": str(job_type),
                        "dag_id": str(dag_id),
                        "market_id": str(market_id) if market_id else None,
                        "status": str(status),
                        "started_at": started_at.isoformat() if started_at else None,
                        "completed_at": completed_at.isoformat() if completed_at else None,
                        "duration_s": duration_s,
                        "attempt": int(attempt_number or 1),
                        "error": str(error_message) if error_message else None,
                        "created_at": created_at.isoformat() if created_at else None,
                    })
            finally:
                cur.close()

    # Engine runs
    if _table_exists("engine_runs"):
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT run_id, region, phase, as_of_date,
                           created_at, updated_at,
                           phase_started_at, phase_completed_at, error
                    FROM engine_runs
                    WHERE as_of_date = %s
                    ORDER BY created_at
                """, (target_date,))
                for row in cur.fetchall():
                    (run_id, region, phase, as_of, created, updated,
                     phase_started, phase_completed, error) = row
                    result["engine_runs"].append({
                        "run_id": str(run_id),
                        "region": str(region),
                        "phase": str(phase),
                        "created_at": created.isoformat() if created else None,
                        "updated_at": updated.isoformat() if updated else None,
                        "phase_started_at": phase_started.isoformat() if phase_started else None,
                        "phase_completed_at": phase_completed.isoformat() if phase_completed else None,
                        "error": error,
                    })
            finally:
                cur.close()

    # Intel briefs
    if _table_exists("intel_briefs"):
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT id, brief_type, severity, domain, title,
                           created_at
                    FROM intel_briefs
                    WHERE created_at::date = %s
                    ORDER BY created_at
                """, (target_date,))
                for row in cur.fetchall():
                    (brief_id, brief_type, severity, domain, title, created) = row
                    result["intel_briefs"].append({
                        "id": str(brief_id),
                        "type": str(brief_type),
                        "severity": str(severity),
                        "domain": str(domain),
                        "title": str(title),
                        "created_at": created.isoformat() if created else None,
                    })
            finally:
                cur.close()

    return result


# ── Endpoints ──────────────────────────────────────────────


@router.get("/overview")
async def get_operations_overview(
    days: int = Query(14, ge=1, le=60, description="Number of days of history"),
):
    """System-wide operations overview: services + daily summaries."""
    services = [
        {**svc, **_service_status(svc["name"])}
        for svc in SERVICES
    ]
    try:
        summaries = _daily_summaries(days)
    except Exception:
        logger.exception("Failed to load daily summaries (DB may be down)")
        summaries = []
    return {
        "services": services,
        "daily": summaries,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/day/{target_date}")
async def get_day_detail(target_date: date):
    """Detailed breakdown of all jobs, engine runs, and reports for a date."""
    try:
        return _day_detail(target_date)
    except Exception:
        logger.exception("Failed to load day detail for %s (DB may be down)", target_date)
        return {
            "date": target_date.isoformat(),
            "jobs": [],
            "engine_runs": [],
            "intel_briefs": [],
            "error": "Database unavailable",
        }
