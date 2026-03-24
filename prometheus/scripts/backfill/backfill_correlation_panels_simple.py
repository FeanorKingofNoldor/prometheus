"""Backfill simple factor correlation_panels.

This script populates the `correlation_panels` table with a family of
rolling windows derived from `factors_daily` trading dates.

It does **not** compute or store covariance matrices; for Iteration 2 the
Portfolio & Risk Engine only uses `start_date` and `end_date` from
`correlation_panels` to choose the window over which factor volatilities
are measured. The actual factor returns are still read directly from
`factors_daily`.

For a given set of factor_ids and window length `W` it:

- Collects all distinct `trade_date` values where any of the factors
  exist in `factors_daily` within the requested date range.
- For each eligible date D (skipping the first W-1), creates a panel
  with:

    - `panel_id = f"{panel_prefix}_{D.isoformat()}"`
    - `start_date = D - (W-1) days`
    - `end_date = D`
    - `universe_spec = {"factor_ids": [...], "window_days": W}`
    - `matrix_ref` set to a descriptive placeholder string.

These panels enable `_compute_factor_risk` to use rolling windows instead
of a fixed 63-day fallback when present.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import List

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill simple correlation_panels based on factors_daily trading dates."
        ),
    )

    parser.add_argument(
        "--factor-id",
        dest="factor_ids",
        action="append",
        required=True,
        help="Factor id to base panels on (can be specified multiple times)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=63,
        help="Rolling window length in calendar days (default: 63)",
    )
    parser.add_argument(
        "--panel-prefix",
        type=str,
        default="FACTOR_SIMPLE",
        help="Prefix for generated panel_id values (default: FACTOR_SIMPLE)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=False,
        help="Optional inclusive start date (YYYY-MM-DD) for factor history",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=False,
        help="Optional inclusive end date (YYYY-MM-DD) for factor history",
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.window_days <= 1:
        raise SystemExit("--window-days must be > 1")

    factor_ids: List[str] = [str(f) for f in args.factor_ids]

    config = get_config()
    db_manager = DatabaseManager(config)

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            sql_dates = """
                SELECT DISTINCT trade_date
                FROM factors_daily
                WHERE factor_id = ANY(%s)
            """
            params: List[object] = [factor_ids]
            if args.start is not None:
                sql_dates += " AND trade_date >= %s"
                params.append(args.start)
            if args.end is not None:
                sql_dates += " AND trade_date <= %s"
                params.append(args.end)
            sql_dates += " ORDER BY trade_date ASC"

            cursor.execute(sql_dates, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    dates: List[date] = [r[0] for r in rows]
    if len(dates) < args.window_days:
        logger.warning(
            "Not enough factor history to build panels: have %d dates, need at least %d",
            len(dates),
            args.window_days,
        )
        return

    sql_insert = """
        INSERT INTO correlation_panels (
            panel_id,
            start_date,
            end_date,
            universe_spec,
            matrix_ref
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (panel_id) DO NOTHING
    """

    inserted = 0
    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            for idx in range(args.window_days - 1, len(dates)):
                end_date = dates[idx]
                start_date = end_date - timedelta(days=args.window_days - 1)
                panel_id = f"{args.panel_prefix}_{end_date.isoformat()}"

                universe_spec = {
                    "factor_ids": factor_ids,
                    "window_days": args.window_days,
                }
                matrix_ref = (
                    f"FACTOR_WINDOW_{start_date.isoformat()}_{end_date.isoformat()}_"
                    f"{args.window_days}D"
                )

                cursor.execute(
                    sql_insert,
                    (
                        panel_id,
                        start_date,
                        end_date,
                        Json(universe_spec),
                        matrix_ref,
                    ),
                )
                inserted += 1

            conn.commit()
        finally:
            cursor.close()

    logger.info(
        "backfill_correlation_panels_simple: inserted %d panels for factors=%s window_days=%d",
        inserted,
        ",".join(factor_ids),
        args.window_days,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
