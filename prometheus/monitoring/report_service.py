"""Prometheus v2 – Log Health Report Service.

AI-powered operational log analysis agent. Reviews system logs, engine
decisions, regime transitions, and risk actions for a given timeframe,
then produces a structured health report.

Report types:
- ``log_daily``  — last 24 hours
- ``log_weekly`` — last 7 days
- ``log_custom`` — user-specified date range
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

from prometheus.monitoring.log_buffer import get_logs

logger = get_logger(__name__)


# ── System prompt ───────────────────────────────────────────────────

LOG_HEALTH_PROMPT = """\
You are Iris, the operations monitoring agent for Prometheus v2 — a live
quantitative trading system. You review system logs, engine run results,
regime transitions, and risk actions to assess operational health.

Below is operational data collected from the Prometheus system for the
specified timeframe. Produce a health report.

You MUST use this exact template. Fill every section or write "N/A".
Do NOT add sections not listed here. Output markdown.

If nothing notable happened, keep the report very short (just Summary
and Status). If there are issues, be thorough with incident analysis.

---

# System Health Report

## Status: HEALTHY / DEGRADED / ISSUES_DETECTED

## Summary
(One paragraph: overall system health assessment for this period)

## Incidents

### [Incident Title]
- **When:** (timestamp or time range)
- **What:** (description of what happened)
- **Impact:** (what was affected — pipeline, trading, data, etc.)
- **Root Cause:** (analysis based on log patterns)
- **Resolution Status:** resolved / ongoing / unknown
- **Suggested Fix:** (actionable steps to prevent recurrence)

(repeat ### block for each incident, or write "No incidents detected.")

## Engine Performance
- Runs: X total, Y completed, Z failed
- Failures: (list with error messages, or "None")

## Regime Stability
- Transitions: (list transitions, or "No transitions")

## Risk Actions
- Actions: (list actions taken, or "None")

## Recommendations
- (bullet list of suggested operational improvements, or "System operating normally.")

---
"""


# ── DB helper ───────────────────────────────────────────────────────

def _q(sql: str, params: tuple = (), *, db_type: str = "runtime") -> List[tuple]:
    """Run a query and return all rows."""
    db = get_db_manager()
    getter = db.get_runtime_connection if db_type == "runtime" else db.get_historical_connection
    with getter() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()


# ── Context assembly ────────────────────────────────────────────────

def _build_log_context(since: date, until: date) -> str:
    """Assemble operational context for the given date range."""
    sections: List[str] = []
    sections.append(f"TIMEFRAME: {since.isoformat()} to {until.isoformat()}")

    # 1. Backend logs — errors and warnings from the in-memory buffer
    error_logs = get_logs(level="ERROR", limit=100)
    warning_logs = get_logs(level="WARNING", limit=100)

    if error_logs:
        lines = [
            f"  [{e.get('timestamp', '?')[:19]}] {e.get('category', '?')}: {e.get('message', '')[:200]}"
            for e in error_logs[:30]
        ]
        sections.append(f"ERRORS ({len(error_logs)} total, showing first 30):\n" + "\n".join(lines))
    else:
        sections.append("ERRORS: none")

    if warning_logs:
        lines = [
            f"  [{w.get('timestamp', '?')[:19]}] {w.get('category', '?')}: {w.get('message', '')[:200]}"
            for w in warning_logs[:20]
        ]
        sections.append(f"WARNINGS ({len(warning_logs)} total, showing first 20):\n" + "\n".join(lines))
    else:
        sections.append("WARNINGS: none")

    # 2. Engine runs
    rows = _q("""
        SELECT run_id, region, phase, as_of_date, created_at, updated_at, error
        FROM engine_runs
        WHERE created_at::date >= %s AND created_at::date <= %s
        ORDER BY created_at DESC LIMIT 50
    """, (since, until))
    if rows:
        completed = sum(1 for r in rows if r[2] == "COMPLETED")
        failed = sum(1 for r in rows if r[2] == "FAILED")
        failed_details = [
            f"  {r[0][:12]}.. {r[1]} as_of={r[3]} error={r[6]}"
            for r in rows if r[2] == "FAILED" and r[6]
        ]
        summary_line = f"ENGINE RUNS: {len(rows)} total, {completed} completed, {failed} failed"
        if failed_details:
            summary_line += "\nFailed runs:\n" + "\n".join(failed_details[:10])
        sections.append(summary_line)
    else:
        sections.append("ENGINE RUNS: none in this period")

    # 3. Engine decisions
    rows = _q("""
        SELECT engine_name, COUNT(*)
        FROM engine_decisions
        WHERE as_of_date >= %s AND as_of_date <= %s
        GROUP BY engine_name ORDER BY COUNT(*) DESC
    """, (since, until))
    if rows:
        lines = [f"  {r[0]}: {r[1]} decisions" for r in rows]
        total = sum(r[1] for r in rows)
        sections.append(f"ENGINE DECISIONS: {total} total\n" + "\n".join(lines))
    else:
        sections.append("ENGINE DECISIONS: none")

    # 4. Regime transitions
    rows = _q("""
        SELECT region, from_regime_label, to_regime_label, as_of_date, created_at
        FROM regime_transitions
        WHERE as_of_date >= %s AND as_of_date <= %s
        ORDER BY created_at DESC LIMIT 20
    """, (since, until))
    if rows:
        lines = [f"  {r[3]} {r[0]}: {r[1]} → {r[2]}" for r in rows]
        sections.append(f"REGIME TRANSITIONS: {len(rows)}\n" + "\n".join(lines))
    else:
        sections.append("REGIME TRANSITIONS: none")

    # 5. Risk actions
    rows = _q("""
        SELECT action_type, instrument_id, strategy_id, created_at
        FROM risk_actions
        WHERE created_at::date >= %s AND created_at::date <= %s
        ORDER BY created_at DESC LIMIT 30
    """, (since, until))
    if rows:
        lines = [f"  {r[3]}: {r[0]} on {r[1]} (strategy={r[2]})" for r in rows[:15]]
        sections.append(f"RISK ACTIONS: {len(rows)}\n" + "\n".join(lines))
    else:
        sections.append("RISK ACTIONS: none")

    return "\n\n".join(sections)


# ── LLM generation ──────────────────────────────────────────────────

def _call_llm(system_prompt: str, context: str, max_tokens: int = 2048) -> str:
    """Send the assembled context to the LLM and get the report text."""
    try:
        from apathis.llm.gateway import get_llm

        llm = get_llm()
        messages = [
            {"role": "system", "content": system_prompt + context},
            {"role": "user", "content": "Generate the system health report now."},
        ]
        return llm.complete(messages, temperature=0.3, max_tokens=max_tokens)
    except Exception as exc:
        logger.error("LLM report generation failed: %s", exc)
        return f"*Report generation failed: {exc}*\n\n---\n\nRaw data:\n\n{context}"


# ── Store report ────────────────────────────────────────────────────

def _store_report(
    report_type: str,
    title: str,
    content: str,
    summary: str = "",
    as_of: date | None = None,
    metadata: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Persist a report to the reports table. Returns the row dict."""
    report_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    as_of = as_of or date.today()

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO reports (id, report_type, generated_at, as_of_date, title, content, summary, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (report_id, report_type, now, as_of, title, content, summary, json.dumps(metadata or {})))
            conn.commit()
        finally:
            cur.close()

    return {
        "id": report_id,
        "report_type": report_type,
        "generated_at": now.isoformat(),
        "as_of_date": as_of.isoformat(),
        "title": title,
        "content": content,
        "summary": summary,
        "metadata": metadata or {},
    }


# ── Public API ──────────────────────────────────────────────────────

def generate_log_report(
    report_type: str = "log_daily",
    start_date: date | None = None,
    end_date: date | None = None,
) -> Dict[str, Any]:
    """Generate and store a log health report.

    Args:
        report_type: ``log_daily``, ``log_weekly``, or ``log_custom``.
        start_date: Start of analysis period (required for log_custom).
        end_date: End of analysis period (defaults to today).

    Returns the stored report dict.
    """
    today = date.today()
    end = end_date or today

    if report_type == "log_daily":
        since = today - timedelta(days=1)
        label = f"24h Log Health — {today.isoformat()}"
    elif report_type == "log_weekly":
        since = today - timedelta(days=7)
        label = f"Weekly Log Health — w/e {today.isoformat()}"
    elif report_type == "log_custom":
        since = start_date or (today - timedelta(days=1))
        label = f"Log Health — {since.isoformat()} to {end.isoformat()}"
    else:
        raise ValueError(f"Unknown report_type: {report_type}")

    logger.info("[reports] Generating %s report (%s to %s)", report_type, since, end)
    context = _build_log_context(since, end)
    content = _call_llm(LOG_HEALTH_PROMPT, context, max_tokens=2000)

    # Extract status from the report
    summary = "System health report generated"
    for line in content.split("\n"):
        if "Status:" in line:
            summary = line.strip().lstrip("#").strip()
            break

    return _store_report(
        report_type,
        label,
        content,
        summary=summary,
        as_of=today,
        metadata={"since": since.isoformat(), "until": end.isoformat()},
    )


# ── Backward-compatible aliases (used by existing API) ──────────────

def generate_daily_report() -> Dict[str, Any]:
    """Generate daily log health report (backward compat)."""
    return generate_log_report("log_daily")


def generate_weekly_report() -> Dict[str, Any]:
    """Generate weekly log health report (backward compat)."""
    return generate_log_report("log_weekly")
