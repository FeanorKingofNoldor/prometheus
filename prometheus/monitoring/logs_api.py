"""Prometheus v2 – Logs & Reports API.

Endpoints for the C2 Logs & Reports tab:
  - System log viewer (from in-memory buffer)
  - Daemon log viewer (from log file)
  - Engine runs (from engine_runs table)
  - LLM-generated reports (from reports table)
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel

from prometheus.monitoring.log_buffer import get_categories, get_logs

logger = get_logger(__name__)

router = APIRouter(prefix="/api/logs", tags=["logs"])

DAEMON_LOG_PATH = os.environ.get("DAEMON_LOG_PATH", "/tmp/prometheus-daemon.log")


# ── Response models ──────────────────────────────────────────────────

class LogEntryResponse(BaseModel):
    timestamp: str
    level: str
    category: str
    source: str
    message: str


class EngineRunResponse(BaseModel):
    run_id: str
    region: str
    phase: str
    as_of_date: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    error: Optional[Dict[str, Any]] = None


class ReportSummary(BaseModel):
    id: str
    report_type: str
    generated_at: str
    as_of_date: str
    title: str
    summary: str


class ReportFull(ReportSummary):
    content: str
    metadata: Dict[str, Any] = {}


class GenerateRequest(BaseModel):
    report_type: str = "log_daily"  # log_daily | log_weekly | log_custom
    start_date: Optional[str] = None  # YYYY-MM-DD, for log_custom
    end_date: Optional[str] = None    # YYYY-MM-DD, for log_custom


class TradingReportRequest(BaseModel):
    report_type: str = "trading_daily"  # trading_daily | trading_weekly | trading_custom
    portfolio_id: str = "IBKR_PAPER"
    start_date: Optional[str] = None  # YYYY-MM-DD, for trading_custom
    end_date: Optional[str] = None    # YYYY-MM-DD, for trading_custom


# ── System Logs ──────────────────────────────────────────────────────

@router.get("/system", response_model=List[LogEntryResponse])
async def get_system_logs(
    level: Optional[str] = Query(None, description="Filter by level: DEBUG|INFO|WARNING|ERROR"),
    category: Optional[str] = Query(None, description="Substring match on category"),
    search: Optional[str] = Query(None, description="Substring match on message"),
    since: Optional[str] = Query(None, description="ISO timestamp cutoff"),
    limit: int = Query(200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    """Return recent system log entries from the in-memory buffer."""
    return get_logs(level=level, category=category, search=search, since=since, limit=limit)


@router.get("/system/categories")
async def get_log_categories() -> List[str]:
    """Return unique log category names."""
    return get_categories()


# ── Daemon Logs (from log file) ─────────────────────────────────────

# Pattern: "2026-03-31 12:37:38 - apatheon.core.database - INFO - message"
_DAEMON_LOG_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+-\s+"  # timestamp
    r"([\w.]+)\s+-\s+"                                     # logger name
    r"(DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+-\s+"           # level
    r"(.*)$"                                                # message
)

_LEVEL_RANK = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}


def _parse_daemon_log(
    *,
    level: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    offset_bytes: int = 0,
) -> tuple[List[Dict[str, Any]], int]:
    """Parse daemon log file, returning (entries_newest_first, file_size)."""
    if not os.path.isfile(DAEMON_LOG_PATH):
        return [], 0

    file_size = os.path.getsize(DAEMON_LOG_PATH)

    # Read from the end for efficiency — grab last 2MB max
    read_size = min(file_size, 2 * 1024 * 1024)
    start_pos = max(0, file_size - read_size) if offset_bytes == 0 else offset_bytes

    with open(DAEMON_LOG_PATH, "r", errors="replace") as f:
        if start_pos > 0:
            f.seek(start_pos)
            f.readline()  # skip partial line
        lines = f.readlines()

    min_rank = _LEVEL_RANK.get(level.upper(), 0) if level else 0
    cat_filter = category.lower() if category else None
    search_filter = search.lower() if search else None

    entries: List[Dict[str, Any]] = []
    # Multiline: accumulate continuation lines into previous entry's message
    current: Optional[Dict[str, Any]] = None

    for line in lines:
        line = line.rstrip("\n")
        m = _DAEMON_LOG_RE.match(line)
        if m:
            # Flush previous entry
            if current is not None:
                entries.append(current)
            ts, name, lvl, msg = m.groups()
            # Shorten category: "apatheon.prometheus.pipeline.tasks" → "pipeline.tasks"
            parts = name.split(".")
            if len(parts) > 2:
                short_cat = ".".join(parts[-2:])
            else:
                short_cat = name
            current = {
                "timestamp": ts,
                "level": lvl,
                "category": short_cat,
                "source": name,
                "message": msg,
            }
        elif current is not None:
            # Continuation line (traceback, etc.)
            current["message"] += "\n" + line

    # Flush last entry
    if current is not None:
        entries.append(current)

    # Filter
    filtered: List[Dict[str, Any]] = []
    for e in reversed(entries):
        if min_rank and _LEVEL_RANK.get(e["level"], 0) < min_rank:
            continue
        if cat_filter and cat_filter not in e["category"].lower() and cat_filter not in e["source"].lower():
            continue
        if search_filter and search_filter not in e["message"].lower():
            continue
        filtered.append(e)
        if len(filtered) >= limit:
            break

    return filtered, file_size


@router.get("/daemon")
async def get_daemon_logs(
    level: Optional[str] = Query(None, description="Min level: DEBUG|INFO|WARNING|ERROR"),
    category: Optional[str] = Query(None, description="Substring match on logger name"),
    search: Optional[str] = Query(None, description="Substring match on message"),
    limit: int = Query(500, ge=1, le=5000),
) -> Dict[str, Any]:
    """Return recent daemon log entries from the log file."""
    entries, file_size = _parse_daemon_log(
        level=level, category=category, search=search, limit=limit,
    )
    return {
        "entries": entries,
        "file_size": file_size,
        "log_path": DAEMON_LOG_PATH,
        "available": os.path.isfile(DAEMON_LOG_PATH),
    }


@router.get("/daemon/categories")
async def get_daemon_categories() -> List[str]:
    """Return unique categories from daemon log file."""
    entries, _ = _parse_daemon_log(limit=5000)
    cats = sorted({e["category"] for e in entries})
    return cats


# ── Engine Runs ──────────────────────────────────────────────────────

@router.get("/runs", response_model=List[EngineRunResponse])
async def get_engine_runs(
    status: Optional[str] = Query(None, description="Filter by phase: COMPLETED|FAILED|etc"),
    region: Optional[str] = Query(None, description="Filter by region"),
    since: Optional[str] = Query(None, description="Only runs created after this date (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=500),
) -> List[Dict[str, Any]]:
    """Return recent engine runs from the database."""
    db = get_db_manager()

    clauses: List[str] = []
    params: List[Any] = []

    if status:
        clauses.append("phase = %s")
        params.append(status.upper())
    if region:
        clauses.append("region = %s")
        params.append(region.upper())
    if since:
        clauses.append("created_at >= %s")
        params.append(since)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    sql = f"""
        SELECT run_id, region, phase, as_of_date, created_at, updated_at, error
        FROM engine_runs
        {where}
        ORDER BY created_at DESC
        LIMIT %s
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        {
            "run_id": r[0],
            "region": r[1],
            "phase": r[2],
            "as_of_date": str(r[3]) if r[3] else None,
            "created_at": r[4].isoformat() if r[4] else None,
            "updated_at": r[5].isoformat() if r[5] else None,
            "error": r[6] if isinstance(r[6], dict) else None,
        }
        for r in rows
    ]


# ── Activity Feed (DB-sourced logs) ───────────────────────────────────

ACTIVITY_SOURCES = ("engine_decisions", "regime_transitions", "risk_actions")


@router.get("/activity")
async def get_activity(
    source: str = Query("engine_decisions", description="engine_decisions|regime_transitions|risk_actions"),
    engine: Optional[str] = Query(None, description="Filter engine_decisions by engine_name"),
    search: Optional[str] = Query(None, description="Substring match"),
    limit: int = Query(200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    """Unified activity feed from various DB tables."""
    db = get_db_manager()

    if source == "engine_decisions":
        clauses: List[str] = []
        params: List[Any] = []
        if engine:
            clauses.append("engine_name = %s")
            params.append(engine.upper())
        if search:
            clauses.append("(engine_name ILIKE %s OR market_id ILIKE %s OR strategy_id ILIKE %s)")
            params.extend([f"%{search}%"] * 3)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        sql = f"""
            SELECT decision_id, engine_name, run_id, strategy_id, market_id,
                   as_of_date, config_id, created_at
            FROM engine_decisions {where}
            ORDER BY created_at DESC LIMIT %s
        """
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                rows = cur.fetchall()
            finally:
                cur.close()
        return [
            {
                "id": r[0], "source": "engine_decisions",
                "engine": r[1], "run_id": r[2], "strategy": r[3],
                "market": r[4], "as_of_date": str(r[5]) if r[5] else None,
                "config_id": r[6],
                "timestamp": r[7].isoformat() if r[7] else None,
                "summary": f"{r[1]} → {r[4] or ''} ({r[3] or ''})",
            }
            for r in rows
        ]

    elif source == "regime_transitions":
        params_rt: List[Any] = []
        search_clause = ""
        if search:
            search_clause = "WHERE from_regime_label ILIKE %s OR to_regime_label ILIKE %s OR region ILIKE %s"
            params_rt.extend([f"%{search}%"] * 3)
        params_rt.append(limit)
        sql = f"""
            SELECT transition_id, region, from_regime_label, to_regime_label,
                   as_of_date, created_at
            FROM regime_transitions {search_clause}
            ORDER BY created_at DESC LIMIT %s
        """
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, params_rt)
                rows = cur.fetchall()
            finally:
                cur.close()
        return [
            {
                "id": r[0], "source": "regime_transitions",
                "region": r[1], "from_regime": r[2], "to_regime": r[3],
                "as_of_date": str(r[4]) if r[4] else None,
                "timestamp": r[5].isoformat() if r[5] else None,
                "summary": f"{r[1]}: {r[2]} → {r[3]}",
            }
            for r in rows
        ]

    elif source == "risk_actions":
        params_ra: List[Any] = []
        search_clause = ""
        if search:
            search_clause = "WHERE action_type ILIKE %s OR instrument_id ILIKE %s OR strategy_id ILIKE %s"
            params_ra.extend([f"%{search}%"] * 3)
        params_ra.append(limit)
        sql = f"""
            SELECT action_id, strategy_id, instrument_id, action_type, created_at
            FROM risk_actions {search_clause}
            ORDER BY created_at DESC LIMIT %s
        """
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, params_ra)
                rows = cur.fetchall()
            finally:
                cur.close()
        return [
            {
                "id": r[0], "source": "risk_actions",
                "strategy": r[1], "instrument": r[2], "action_type": r[3],
                "timestamp": r[4].isoformat() if r[4] else None,
                "summary": f"{r[3]} on {r[2]} ({r[1]})",
            }
            for r in rows
        ]

    return []


@router.get("/activity/engines")
async def get_engine_names() -> List[str]:
    """Return distinct engine names from engine_decisions."""
    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT DISTINCT engine_name FROM engine_decisions ORDER BY engine_name")
            return [r[0] for r in cur.fetchall()]
        finally:
            cur.close()


# ── Run Detail ───────────────────────────────────────────────────────

@router.get("/runs/{run_id}")
async def get_run_detail(run_id: str = Path(...)) -> Dict[str, Any]:
    """Return full detail for a single engine run + its related decisions."""
    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT run_id, region, phase, as_of_date, created_at, updated_at,
                       phase_started_at, phase_completed_at, config_json, live_safe, error
                FROM engine_runs WHERE run_id = %s
            """, (run_id,))
            row = cur.fetchone()
        finally:
            cur.close()

    if row is None:
        raise HTTPException(404, f"Run '{run_id}' not found")

    # Related decisions
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT decision_id, engine_name, strategy_id, market_id,
                       as_of_date, config_id, created_at
                FROM engine_decisions WHERE run_id = %s
                ORDER BY created_at
            """, (run_id,))
            dec_rows = cur.fetchall()
        finally:
            cur.close()

    duration_s = None
    if row[6] and row[7]:
        duration_s = (row[7] - row[6]).total_seconds()

    return {
        "run_id": row[0],
        "region": row[1],
        "phase": row[2],
        "as_of_date": str(row[3]) if row[3] else None,
        "created_at": row[4].isoformat() if row[4] else None,
        "updated_at": row[5].isoformat() if row[5] else None,
        "phase_started_at": row[6].isoformat() if row[6] else None,
        "phase_completed_at": row[7].isoformat() if row[7] else None,
        "duration_seconds": duration_s,
        "config_json": row[8] if isinstance(row[8], dict) else None,
        "live_safe": row[9],
        "error": row[10] if isinstance(row[10], dict) else None,
        "decisions": [
            {
                "decision_id": d[0], "engine_name": d[1],
                "strategy_id": d[2], "market_id": d[3],
                "as_of_date": str(d[4]) if d[4] else None,
                "config_id": d[5],
                "created_at": d[6].isoformat() if d[6] else None,
            }
            for d in dec_rows
        ],
    }


# ── Reports ──────────────────────────────────────────────────────────

@router.get("/reports", response_model=List[ReportSummary])
async def list_reports(
    report_type: Optional[str] = Query(None, description="daily_evening|weekly_sunday"),
    limit: int = Query(20, ge=1, le=100),
) -> List[Dict[str, Any]]:
    """List stored reports (summary only)."""
    db = get_db_manager()

    clauses: List[str] = []
    params: List[Any] = []
    if report_type:
        clauses.append("report_type = %s")
        params.append(report_type)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(limit)

    sql = f"""
        SELECT id, report_type, generated_at, as_of_date, title, summary
        FROM reports
        {where}
        ORDER BY generated_at DESC
        LIMIT %s
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            cur.close()

    return [
        {
            "id": str(r[0]),
            "report_type": r[1],
            "generated_at": r[2].isoformat() if r[2] else "",
            "as_of_date": str(r[3]) if r[3] else "",
            "title": r[4] or "",
            "summary": r[5] or "",
        }
        for r in rows
    ]


@router.get("/reports/{report_id}", response_model=ReportFull)
async def get_report(report_id: str = Path(...)) -> Dict[str, Any]:
    """Return a single report with full content."""
    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT id, report_type, generated_at, as_of_date, title, summary, content, metadata
                FROM reports WHERE id = %s
            """, (report_id,))
            row = cur.fetchone()
        finally:
            cur.close()

    if row is None:
        raise HTTPException(404, f"Report '{report_id}' not found")

    return {
        "id": str(row[0]),
        "report_type": row[1],
        "generated_at": row[2].isoformat() if row[2] else "",
        "as_of_date": str(row[3]) if row[3] else "",
        "title": row[4] or "",
        "summary": row[5] or "",
        "content": row[6] or "",
        "metadata": row[7] or {},
    }


@router.post("/reports/generate", response_model=ReportFull)
async def generate_report(req: GenerateRequest) -> Dict[str, Any]:
    """Generate a log health report via LLM and store it."""
    import asyncio
    from datetime import date as date_cls

    from prometheus.monitoring.report_service import generate_log_report

    valid_types = ("log_daily", "log_weekly", "log_custom")
    if req.report_type not in valid_types:
        raise HTTPException(400, f"Unknown report_type: {req.report_type}. Use: {', '.join(valid_types)}")

    start = None
    end = None
    if req.start_date:
        try:
            start = date_cls.fromisoformat(req.start_date)
        except ValueError:
            raise HTTPException(400, f"Invalid start_date: {req.start_date}")
    if req.end_date:
        try:
            end = date_cls.fromisoformat(req.end_date)
        except ValueError:
            raise HTTPException(400, f"Invalid end_date: {req.end_date}")

    # Run in thread so the sync LLM call doesn't block the event loop
    return await asyncio.to_thread(generate_log_report, req.report_type, start, end)


@router.post("/reports/generate-trading", response_model=ReportFull)
async def generate_trading_report_endpoint(req: TradingReportRequest) -> Dict[str, Any]:
    """Generate a trading performance report via LLM and store it."""
    import asyncio
    from datetime import date as date_cls

    from prometheus.monitoring.trading_report_service import generate_trading_report

    valid_types = ("trading_daily", "trading_weekly", "trading_custom")
    if req.report_type not in valid_types:
        raise HTTPException(400, f"Unknown report_type: {req.report_type}. Use: {', '.join(valid_types)}")

    start = None
    end = None
    if req.start_date:
        try:
            start = date_cls.fromisoformat(req.start_date)
        except ValueError:
            raise HTTPException(400, f"Invalid start_date: {req.start_date}")
    if req.end_date:
        try:
            end = date_cls.fromisoformat(req.end_date)
        except ValueError:
            raise HTTPException(400, f"Invalid end_date: {req.end_date}")

    # Run in thread so the sync LLM call doesn't block the event loop
    return await asyncio.to_thread(
        generate_trading_report,
        req.report_type,
        req.portfolio_id,
        start,
        end,
    )
