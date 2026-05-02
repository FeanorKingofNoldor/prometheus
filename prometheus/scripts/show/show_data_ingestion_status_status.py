"""Prometheus v2 – Show data_ingestion_status status (Layer 0 validation).

Validates basic Layer 0 contracts for ``data_ingestion_status``:
- per market/date, at most one row (enforced by unique index)
- status values are in the allowed set
- timestamps are consistent with state-machine semantics

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apatheon.core.database import get_db_manager

_ALLOWED = ("PENDING", "IN_PROGRESS", "COMPLETE", "FAILED")


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
            SUM(CASE WHEN status NOT IN ('PENDING','IN_PROGRESS','COMPLETE','FAILED') THEN 1 ELSE 0 END) AS invalid_status,
            SUM(CASE WHEN status = 'PENDING' AND (started_at IS NOT NULL OR completed_at IS NOT NULL) THEN 1 ELSE 0 END) AS pending_bad_ts,
            SUM(CASE WHEN status = 'IN_PROGRESS' AND (started_at IS NULL OR completed_at IS NOT NULL) THEN 1 ELSE 0 END) AS in_progress_bad_ts,
            SUM(CASE WHEN status = 'COMPLETE' AND (started_at IS NULL OR completed_at IS NULL OR last_price_timestamp IS NULL) THEN 1 ELSE 0 END) AS complete_bad_ts,
            SUM(CASE WHEN status = 'FAILED' AND (started_at IS NULL OR completed_at IS NULL) THEN 1 ELSE 0 END) AS failed_bad_ts,
            SUM(CASE WHEN status = 'FAILED' AND (error_message IS NULL OR btrim(error_message) = '') THEN 1 ELSE 0 END) AS failed_missing_error,
            SUM(CASE WHEN instruments_received < 0 THEN 1 ELSE 0 END) AS negative_received,
            SUM(CASE WHEN instruments_expected IS NOT NULL AND instruments_expected < 0 THEN 1 ELSE 0 END) AS negative_expected,
            SUM(CASE WHEN instruments_expected IS NOT NULL AND instruments_received > instruments_expected THEN 1 ELSE 0 END) AS received_gt_expected
        FROM data_ingestion_status
    """

    sql_by_status = """
        SELECT status, COUNT(*)
        FROM data_ingestion_status
        GROUP BY status
        ORDER BY status
    """

    sql_preview = """
        SELECT market_id, as_of_date, status, instruments_received, instruments_expected,
               started_at, completed_at, last_price_timestamp, error_message
        FROM data_ingestion_status
        ORDER BY as_of_date DESC, market_id
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                invalid_status,
                pending_bad_ts,
                in_progress_bad_ts,
                complete_bad_ts,
                failed_bad_ts,
                failed_missing_error,
                negative_received,
                negative_expected,
                received_gt_expected,
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
            "market_id": str(m),
            "as_of_date": d.isoformat() if d is not None else None,
            "status": str(s),
            "instruments_received": int(r) if r is not None else None,
            "instruments_expected": int(e) if e is not None else None,
            "started_at": sa.isoformat() if sa is not None else None,
            "completed_at": ca.isoformat() if ca is not None else None,
            "last_price_timestamp": lp.isoformat() if lp is not None else None,
            "error_message": str(em) if em is not None else None,
        }
        for m, d, s, r, e, sa, ca, lp, em in preview_rows
    ]

    checks_passed = (
        int(invalid_status or 0) == 0
        and int(pending_bad_ts or 0) == 0
        and int(in_progress_bad_ts or 0) == 0
        and int(complete_bad_ts or 0) == 0
        and int(failed_bad_ts or 0) == 0
        and int(failed_missing_error or 0) == 0
        and int(negative_received or 0) == 0
        and int(negative_expected or 0) == 0
        and int(received_gt_expected or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "rows_by_status": by_status,
        "invalid_status_rows": int(invalid_status or 0),
        "pending_bad_timestamps_rows": int(pending_bad_ts or 0),
        "in_progress_bad_timestamps_rows": int(in_progress_bad_ts or 0),
        "complete_bad_timestamps_rows": int(complete_bad_ts or 0),
        "failed_bad_timestamps_rows": int(failed_bad_ts or 0),
        "failed_missing_error_rows": int(failed_missing_error or 0),
        "negative_instruments_received_rows": int(negative_received or 0),
        "negative_instruments_expected_rows": int(negative_expected or 0),
        "received_greater_than_expected_rows": int(received_gt_expected or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show data_ingestion_status status and basic Layer 0 validation checks"
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
