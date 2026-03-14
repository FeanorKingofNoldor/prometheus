"""Prometheus v2 – In-process job runner.

This is a lightweight, in-memory job registry + thread-pool executor for
backend control operations (backtests, etc.).

It is intentionally minimal and is meant to be replaced by a real
orchestrator/queue later.
"""

from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, Optional


logger = logging.getLogger("prometheus.monitoring.job_runner")


@dataclass
class JobRecord:
    job_id: str
    type: str
    status: str
    created_at: datetime

    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress_pct: float = 0.0

    message: str = ""
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


_lock = threading.Lock()
_registry: Dict[str, JobRecord] = {}

# Keep this small by default; CPU-heavy work should be done in C++ where
# possible, and large concurrency should be expressed by the C++ runner.
_executor = ThreadPoolExecutor(max_workers=4)


def create_job(*, prefix: str, job_type: str, status: str = "PENDING", message: str = "") -> JobRecord:
    """Create a job record without scheduling execution."""
    job_id = f"{prefix}_{uuid.uuid4().hex[:8]}"
    rec = JobRecord(
        job_id=job_id,
        type=job_type,
        status=status,
        created_at=datetime.now(),
        message=message,
    )
    with _lock:
        _registry[job_id] = rec
    logger.info("[job] Created job %s type=%s status=%s msg=%s", job_id, job_type, status, message)
    return rec


def get_job(job_id: str) -> Optional[JobRecord]:
    with _lock:
        return _registry.get(job_id)


def update_job(
    job_id: str,
    *,
    status: Optional[str] = None,
    progress_pct: Optional[float] = None,
    message: Optional[str] = None,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
) -> None:
    with _lock:
        rec = _registry.get(job_id)
        if rec is None:
            return

        if status is not None:
            rec.status = status
        if progress_pct is not None:
            rec.progress_pct = float(progress_pct)
        if message is not None:
            rec.message = message
        if result is not None:
            rec.result = result
        if error is not None:
            rec.error = error
        if started_at is not None:
            rec.started_at = started_at
        if completed_at is not None:
            rec.completed_at = completed_at


def submit_job(
    *,
    prefix: str,
    job_type: str,
    message: str,
    fn: Callable[[], Dict[str, Any]],
) -> JobRecord:
    """Create a job and execute it asynchronously in the thread pool."""

    rec = create_job(prefix=prefix, job_type=job_type, status="PENDING", message=message)

    def _run() -> None:
        logger.info("[job] Running job %s type=%s", rec.job_id, rec.type)
        update_job(rec.job_id, status="RUNNING", started_at=datetime.now(), progress_pct=0.0)
        try:
            out = fn()
        except Exception as exc:  # pragma: no cover
            logger.error("[job] Job %s FAILED: %s", rec.job_id, exc, exc_info=True)
            update_job(
                rec.job_id,
                status="FAILED",
                completed_at=datetime.now(),
                progress_pct=100.0,
                error=str(exc),
            )
            return

        logger.info("[job] Job %s COMPLETED", rec.job_id)
        update_job(
            rec.job_id,
            status="COMPLETED",
            completed_at=datetime.now(),
            progress_pct=100.0,
            result=out,
        )

    _executor.submit(_run)
    return rec
