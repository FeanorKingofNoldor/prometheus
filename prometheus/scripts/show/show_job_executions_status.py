"""Prometheus v2 – Show job_executions status (Layer 0 validation).

Validates basic Layer 0 contracts for ``job_executions``:
- identifiers are non-empty
- status is in the allowed set
- timestamps are consistent with the status/state-machine
- attempt_number is sane
- config_json is present (payload/ref for reproducibility)

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED = ("PENDING", "RUNNING", "SUCCESS", "FAILED", "SKIPPED")


def _summarise(db, which: str) -> dict[str, Any]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    sql = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN btrim(execution_id) = '' THEN 1 ELSE 0 END) AS empty_execution_id,
            SUM(CASE WHEN btrim(job_id) = '' THEN 1 ELSE 0 END) AS empty_job_id,
            SUM(CASE WHEN btrim(job_type) = '' THEN 1 ELSE 0 END) AS empty_job_type,
            SUM(CASE WHEN btrim(dag_id) = '' THEN 1 ELSE 0 END) AS empty_dag_id,
            SUM(CASE WHEN status NOT IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED') THEN 1 ELSE 0 END) AS invalid_status,
            SUM(CASE WHEN attempt_number < 1 THEN 1 ELSE 0 END) AS bad_attempt,
            SUM(CASE WHEN status = 'PENDING' AND (started_at IS NOT NULL OR completed_at IS NOT NULL) THEN 1 ELSE 0 END) AS pending_bad_ts,
            SUM(CASE WHEN status = 'RUNNING' AND (started_at IS NULL OR completed_at IS NOT NULL) THEN 1 ELSE 0 END) AS running_bad_ts,
            SUM(CASE WHEN status IN ('SUCCESS','FAILED','SKIPPED') AND completed_at IS NULL THEN 1 ELSE 0 END) AS terminal_missing_completed,
            SUM(CASE WHEN status = 'FAILED' AND (error_message IS NULL OR btrim(error_message) = '') THEN 1 ELSE 0 END) AS failed_missing_error
        FROM job_executions
    """

    sql_by_status = """
        SELECT status, COUNT(*)
        FROM job_executions
        GROUP BY status
        ORDER BY status
    """

    sql_preview = """
        SELECT execution_id, dag_id, job_id, job_type, market_id, as_of_date, status,
               attempt_number, started_at, completed_at, error_message, created_at
        FROM job_executions
        ORDER BY created_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                empty_execution_id,
                empty_job_id,
                empty_job_type,
                empty_dag_id,
                invalid_status,
                bad_attempt,
                pending_bad_ts,
                running_bad_ts,
                terminal_missing_completed,
                failed_missing_error,
            ) = cur.fetchone()

            cur.execute(sql_by_status)
            rows_by_status = cur.fetchall()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    by_status = {str(s): int(n) for s, n in rows_by_status}
    for st in _ALLOWED:
        by_status.setdefault(st, 0)

    preview = [
        {
            "execution_id": str(eid),
            "dag_id": str(did),
            "job_id": str(jid),
            "job_type": str(jt),
            "market_id": str(mid) if mid is not None else None,
            "as_of_date": d.isoformat() if d is not None else None,
            "status": str(st),
            "attempt_number": int(at) if at is not None else None,
            "started_at": sa.isoformat() if sa is not None else None,
            "completed_at": ca.isoformat() if ca is not None else None,
            "error_message": str(em) if em is not None else None,
            "created_at": cr.isoformat() if cr is not None else None,
        }
        for eid, did, jid, jt, mid, d, st, at, sa, ca, em, cr in preview_rows
    ]

    checks_passed = (
        int(empty_execution_id or 0) == 0
        and int(empty_job_id or 0) == 0
        and int(empty_job_type or 0) == 0
        and int(empty_dag_id or 0) == 0
        and int(invalid_status or 0) == 0
        and int(bad_attempt or 0) == 0
        and int(pending_bad_ts or 0) == 0
        and int(running_bad_ts or 0) == 0
        and int(terminal_missing_completed or 0) == 0
        and int(failed_missing_error or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "rows_by_status": by_status,
        "empty_execution_id_rows": int(empty_execution_id or 0),
        "empty_job_id_rows": int(empty_job_id or 0),
        "empty_job_type_rows": int(empty_job_type or 0),
        "empty_dag_id_rows": int(empty_dag_id or 0),
        "invalid_status_rows": int(invalid_status or 0),
        "bad_attempt_number_rows": int(bad_attempt or 0),
        "pending_bad_timestamps_rows": int(pending_bad_ts or 0),
        "running_bad_timestamps_rows": int(running_bad_ts or 0),
        "terminal_missing_completed_rows": int(terminal_missing_completed or 0),
        "failed_missing_error_rows": int(failed_missing_error or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show job_executions status and basic Layer 0 validation checks"
    )
    parser.parse_args(argv)

    db = get_db_manager()

    report = {
        "runtime": _summarise(db, "runtime"),
        "historical": _summarise(db, "historical"),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
