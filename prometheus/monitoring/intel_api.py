"""Prometheus v2 – Intelligence Briefing API.

REST endpoints for the Geo-Intelligence Briefing Center.
Replaces the old strategy proposal/config change UI.
"""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

from apathis.core.logging import get_logger
from fastapi import APIRouter, HTTPException, Path, Query

logger = get_logger(__name__)

intel_router = APIRouter(prefix="/api/intel", tags=["intel"])


# ── Job Tracker ───────────────────────────────────────────────────────

_jobs: Dict[str, Dict[str, Any]] = {}
_jobs_lock = threading.Lock()

_STEP_LABELS = {
    "collecting": "Collecting snapshots…",
    "nation": "Running Nation Analyst…",
    "conflict": "Running Conflict Analyst…",
    "maritime": "Running Maritime Analyst…",
    "trade": "Running Trade Analyst…",
    "synthesis": "Running Synthesis…",
    "done": "Complete",
}


def _create_job(job_type: str) -> str:
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "type": job_type,
            "status": "running",
            "step": "collecting",
            "step_index": 0,
            "total_steps": 7,
            "step_label": "Starting…",
            "started_at": time.time(),
            "result": None,
            "error": None,
        }
    return job_id


def _update_job(job_id: str, step: str, step_index: int, total_steps: int) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job:
            job["step"] = step
            job["step_index"] = step_index
            job["total_steps"] = total_steps
            job["step_label"] = _STEP_LABELS.get(step, step)


def _finish_job(job_id: str, result: Any = None, error: str | None = None) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job:
            job["status"] = "done" if not error else "error"
            job["step"] = "done"
            job["step_index"] = job["total_steps"]
            job["step_label"] = "Complete" if not error else f"Failed: {error}"
            job["result"] = result
            job["error"] = error
            job["elapsed"] = round(time.time() - job["started_at"], 1)


# ── List Briefs ─────────────────────────────────────────────────────────


@intel_router.get("/briefs")
async def list_briefs(
    brief_type: Optional[str] = Query(None, description="flash_alert | daily_sitrep | weekly_assessment | domain_report"),
    severity: Optional[str] = Query(None, description="critical | high | medium | low | info"),
    domain: Optional[str] = Query(None, description="nation | conflict | maritime | trade | synthesis"),
    unread_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
) -> List[Dict[str, Any]]:
    """List intelligence briefs with optional filters."""
    from apathis.intel.store import get_briefs

    return get_briefs(
        brief_type=brief_type,
        severity=severity,
        domain=domain,
        unread_only=unread_only,
        limit=limit,
    )


# ── Single Brief ──────────────────────────────────────────────────────


@intel_router.get("/briefs/unread-count")
async def unread_count() -> Dict[str, int]:
    """Return unread brief counts by type."""
    from apathis.intel.store import get_unread_count

    return get_unread_count()


@intel_router.get("/briefs/flash-alerts")
async def flash_alerts(limit: int = Query(20, ge=1, le=100)) -> List[Dict[str, Any]]:
    """Convenience endpoint: latest flash alerts only."""
    from apathis.intel.store import get_briefs

    return get_briefs(brief_type="flash_alert", limit=limit)


@intel_router.get("/briefs/{brief_id}")
async def get_brief_detail(brief_id: str = Path(...)) -> Dict[str, Any]:
    """Get a single brief with full content."""
    from apathis.intel.store import get_brief

    brief = get_brief(brief_id)
    if not brief:
        raise HTTPException(status_code=404, detail="Brief not found")
    return brief


# ── Mark Read ─────────────────────────────────────────────────────────


@intel_router.post("/briefs/{brief_id}/read")
async def mark_brief_read(brief_id: str = Path(...)) -> Dict[str, str]:
    """Mark a brief as read."""
    from apathis.intel.store import mark_read

    if mark_read(brief_id):
        return {"status": "ok"}
    raise HTTPException(status_code=404, detail="Brief not found")


# ── Job Status ────────────────────────────────────────────────────────


@intel_router.get("/jobs/{job_id}")
async def get_job_status(job_id: str = Path(...)) -> Dict[str, Any]:
    """Poll generation job progress."""
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    # Return a safe copy without the full result (client can read the brief list)
    return {
        "id": job["id"],
        "type": job["type"],
        "status": job["status"],
        "step": job["step"],
        "step_index": job["step_index"],
        "total_steps": job["total_steps"],
        "step_label": job["step_label"],
        "error": job.get("error"),
        "elapsed": job.get("elapsed"),
    }


# ── Generation Triggers (async — return immediately with job_id) ───


def _run_sitrep_job(job_id: str) -> None:
    """Background thread for SITREP generation."""
    from apathis.intel.pipeline import run_daily_sitrep

    try:
        brief = run_daily_sitrep(
            on_progress=lambda step, idx, total: _update_job(job_id, step, idx, total),
        )
        _finish_job(job_id, result=brief)
    except Exception as exc:
        logger.exception("[intel_api] SITREP generation failed: %s", exc)
        _finish_job(job_id, error=str(exc))


def _run_weekly_job(job_id: str) -> None:
    """Background thread for weekly assessment."""
    from apathis.intel.pipeline import run_weekly_assessment

    try:
        brief = run_weekly_assessment(
            on_progress=lambda step, idx, total: _update_job(job_id, step, idx, total),
        )
        _finish_job(job_id, result=brief)
    except Exception as exc:
        logger.exception("[intel_api] Weekly assessment failed: %s", exc)
        _finish_job(job_id, error=str(exc))


@intel_router.post("/generate/sitrep")
async def generate_sitrep() -> Dict[str, Any]:
    """Trigger SITREP generation. Returns immediately with a job ID.

    Poll ``GET /api/intel/jobs/{job_id}`` for progress.
    """
    job_id = _create_job("sitrep")
    threading.Thread(target=_run_sitrep_job, args=(job_id,), daemon=True).start()
    return {"job_id": job_id, "status": "running", "poll": f"/api/intel/jobs/{job_id}"}


@intel_router.post("/generate/flash-check")
async def generate_flash_check() -> Dict[str, Any]:
    """Manually trigger flash alert evaluation (fast — no LLM)."""
    from apathis.intel.pipeline import run_flash_check

    try:
        alerts = await asyncio.to_thread(run_flash_check)
        return {"alerts_generated": len(alerts), "alerts": alerts}
    except Exception as exc:
        logger.exception("[intel_api] Flash check failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"Flash check failed: {exc}")


@intel_router.post("/generate/weekly")
async def generate_weekly() -> Dict[str, Any]:
    """Trigger weekly assessment. Returns immediately with a job ID."""
    job_id = _create_job("weekly")
    threading.Thread(target=_run_weekly_job, args=(job_id,), daemon=True).start()
    return {"job_id": job_id, "status": "running", "poll": f"/api/intel/jobs/{job_id}"}
