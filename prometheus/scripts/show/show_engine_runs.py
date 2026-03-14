"""Prometheus v2 – Show engine run state.

This script prints rows from the ``engine_runs`` table for quick
inspection of daily engine run status. It is intended as a lightweight
operational tool alongside ``run_engine_state``.

Typical uses::

    # Show all active (non-completed/non-failed) runs
    python -m prometheus.scripts.show_engine_runs --active

    # Show runs for a specific date and region
    python -m prometheus.scripts.show_engine_runs --as-of 2024-03-04 --region US

Output is a CSV with one row per run containing basic timestamps and
phase information.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from prometheus.pipeline.state import RunPhase


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List engine_runs rows for monitoring engine state",
    )

    parser.add_argument(
        "--active",
        action="store_true",
        help="Show only active runs (phase not in COMPLETED/FAILED)",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="Optional as_of_date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Optional region filter (e.g. US, EU, ASIA)",
    )
    parser.add_argument(
        "--phase",
        type=str,
        choices=[p.value for p in RunPhase],
        default=None,
        help="Optional phase filter (e.g. DATA_READY, SIGNALS_DONE)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of rows to display (default: 100)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive if provided")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    db_manager = get_db_manager()

    where_clauses = []
    params: list[object] = []

    if args.active:
        where_clauses.append("phase NOT IN ('COMPLETED', 'FAILED')")

    if args.as_of is not None:
        where_clauses.append("as_of_date = %s")
        params.append(args.as_of)

    if args.region is not None:
        where_clauses.append("UPPER(region) = UPPER(%s)")
        params.append(args.region)

    if args.phase is not None:
        where_clauses.append("phase = %s")
        params.append(args.phase)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT run_id, as_of_date, region, phase, "
        "phase_started_at, phase_completed_at, created_at, updated_at, error "
        "FROM engine_runs" + where_sql + " ORDER BY as_of_date, region, run_id"
    )

    if args.limit is not None:
        sql += " LIMIT %s"
        params.append(args.limit)

    rows: list[tuple] = []
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        print("run_id,as_of_date,region,phase,phase_started_at,phase_completed_at,created_at,updated_at,error")
        # No data rows; nothing else to print.
        return

    print("run_id,as_of_date,region,phase,phase_started_at,phase_completed_at,created_at,updated_at,error")
    for (
        run_id,
        as_of_date_db,
        region,
        phase,
        phase_started_at,
        phase_completed_at,
        created_at,
        updated_at,
        error,
    ) in rows:
        def _fmt_ts(ts: Optional[datetime]) -> str:
            return ts.isoformat() if ts is not None else ""

        error_str = ""
        if error:
            # Represent error dict compactly on one line.
            error_str = str(error).replace("\n", " ")

        print(
            f"{run_id},{as_of_date_db:%Y-%m-%d},{region},{phase},"
            f"{_fmt_ts(phase_started_at)},{_fmt_ts(phase_completed_at)},"
            f"{_fmt_ts(created_at)},{_fmt_ts(updated_at)},{error_str}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
