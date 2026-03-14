"""Prometheus v2 – Show engine_runs status (Layer 0 validation).

Validates basic Layer 0 contracts for ``engine_runs``:
- identifiers are non-empty
- phase is in the allowed set
- live_safe is present
- config_json exists and is an object (payload/ref for reproducibility)
- phase timestamps are consistent (terminal phases have completed_at)

Reports results for both runtime_db and historical_db.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager


_ALLOWED = (
    "WAITING_FOR_DATA",
    "DATA_READY",
    "SIGNALS_DONE",
    "UNIVERSES_DONE",
    "BOOKS_DONE",
    "COMPLETED",
    "FAILED",
)


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
            SUM(CASE WHEN btrim(run_id) = '' THEN 1 ELSE 0 END) AS empty_run_id,
            SUM(CASE WHEN btrim(region) = '' THEN 1 ELSE 0 END) AS empty_region,
            SUM(CASE WHEN phase NOT IN ('WAITING_FOR_DATA','DATA_READY','SIGNALS_DONE','UNIVERSES_DONE','BOOKS_DONE','COMPLETED','FAILED') THEN 1 ELSE 0 END) AS invalid_phase,
            SUM(CASE WHEN phase_started_at IS NULL THEN 1 ELSE 0 END) AS missing_phase_started_at,
            SUM(CASE WHEN phase IN ('COMPLETED','FAILED') AND phase_completed_at IS NULL THEN 1 ELSE 0 END) AS terminal_missing_completed_at,
            SUM(CASE WHEN phase NOT IN ('COMPLETED','FAILED') AND phase_completed_at IS NOT NULL THEN 1 ELSE 0 END) AS nonterminal_has_completed_at,
            SUM(CASE WHEN live_safe IS NULL THEN 1 ELSE 0 END) AS null_live_safe,
            SUM(CASE WHEN config_json IS NULL THEN 1 ELSE 0 END) AS null_config_json,
            SUM(CASE WHEN config_json IS NOT NULL AND jsonb_typeof(config_json) <> 'object' THEN 1 ELSE 0 END) AS bad_config_json_type
        FROM engine_runs
    """

    sql_by_phase = """
        SELECT phase, COUNT(*)
        FROM engine_runs
        GROUP BY phase
        ORDER BY phase
    """

    sql_preview = """
        SELECT run_id, as_of_date, region, phase, live_safe,
               phase_started_at, phase_completed_at, created_at, updated_at
        FROM engine_runs
        ORDER BY updated_at DESC
        LIMIT 25
    """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            (
                total,
                empty_run_id,
                empty_region,
                invalid_phase,
                missing_phase_started_at,
                terminal_missing_completed_at,
                nonterminal_has_completed_at,
                null_live_safe,
                null_config_json,
                bad_config_json_type,
            ) = cur.fetchone()

            cur.execute(sql_by_phase)
            rows_by_phase = cur.fetchall()

            cur.execute(sql_preview)
            preview_rows = cur.fetchall()
        finally:
            cur.close()

    by_phase = {str(p): int(n) for p, n in rows_by_phase}
    for ph in _ALLOWED:
        by_phase.setdefault(ph, 0)

    preview = [
        {
            "run_id": str(run_id),
            "as_of_date": d.isoformat() if d is not None else None,
            "region": str(region),
            "phase": str(phase),
            "live_safe": bool(live_safe) if live_safe is not None else None,
            "phase_started_at": ps.isoformat() if ps is not None else None,
            "phase_completed_at": pc.isoformat() if pc is not None else None,
            "created_at": ca.isoformat() if ca is not None else None,
            "updated_at": ua.isoformat() if ua is not None else None,
        }
        for run_id, d, region, phase, live_safe, ps, pc, ca, ua in preview_rows
    ]

    checks_passed = (
        int(empty_run_id or 0) == 0
        and int(empty_region or 0) == 0
        and int(invalid_phase or 0) == 0
        and int(missing_phase_started_at or 0) == 0
        and int(terminal_missing_completed_at or 0) == 0
        and int(nonterminal_has_completed_at or 0) == 0
        and int(null_live_safe or 0) == 0
        and int(null_config_json or 0) == 0
        and int(bad_config_json_type or 0) == 0
    )

    return {
        "total_rows": int(total or 0),
        "rows_by_phase": by_phase,
        "empty_run_id_rows": int(empty_run_id or 0),
        "empty_region_rows": int(empty_region or 0),
        "invalid_phase_rows": int(invalid_phase or 0),
        "missing_phase_started_at_rows": int(missing_phase_started_at or 0),
        "terminal_missing_completed_at_rows": int(terminal_missing_completed_at or 0),
        "nonterminal_has_completed_at_rows": int(nonterminal_has_completed_at or 0),
        "null_live_safe_rows": int(null_live_safe or 0),
        "null_config_json_rows": int(null_config_json or 0),
        "bad_config_json_type_rows": int(bad_config_json_type or 0),
        "recent_rows_preview": preview,
        "checks_passed": bool(checks_passed),
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Show engine_runs status and basic Layer 0 validation checks"
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
