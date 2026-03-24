"""Show joint portfolio embeddings (PORTFOLIO_CORE_V0).

This script queries the `joint_embeddings` table for
`joint_type = 'PORTFOLIO_CORE_V0'` and prints a CSV summary including:

- as_of_date
- portfolio_id
- model_id
- dim (embedding dimension)
- l2_norm

Examples
--------

    # List joint portfolio embeddings for a date
    python -m prometheus.scripts.show_joint_portfolios \
        --as-of 2025-01-31 \
        --model-id joint-portfolio-core-v1

    # Filter by portfolio_id and date range
    python -m prometheus.scripts.show_joint_portfolios \
        --portfolio-id PORTFOLIO_CORE_US_EQ_001 \
        --start 2025-01-01 --end 2025-12-31 \
        --model-id joint-portfolio-core-v1
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

import numpy as np
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Show joint portfolio embeddings (PORTFOLIO_CORE_V0) from "
            "joint_embeddings as a CSV summary."
        ),
    )

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="Optional single as-of date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional start date for range (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional end date for range (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--portfolio-id",
        type=str,
        default=None,
        help="Optional portfolio_id filter (entity_scope->>'portfolio_id')",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-portfolio-core-v1",
        help="Filter by joint model_id (default: joint-portfolio-core-v1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of rows to print (default: 1000)",
    )

    args = parser.parse_args(argv)

    if args.as_of and (args.start or args.end):
        parser.error("Use either --as-of or (--start/--end) but not both")

    db_manager = DatabaseManager(get_config())

    where_clauses = ["joint_type = 'PORTFOLIO_CORE_V0'", "model_id = %s"]
    params: list[object] = [args.model_id]

    if args.as_of is not None:
        where_clauses.append("as_of_date = %s")
        params.append(args.as_of)
    else:
        if args.start is not None:
            where_clauses.append("as_of_date >= %s")
            params.append(args.start)
        if args.end is not None:
            where_clauses.append("as_of_date <= %s")
            params.append(args.end)

    if args.portfolio_id is not None:
        where_clauses.append("(entity_scope->>'portfolio_id') = %s")
        params.append(args.portfolio_id)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT as_of_date, entity_scope, model_id, vector "
        "FROM joint_embeddings" + where_sql + " ORDER BY as_of_date ASC, joint_id ASC"
    )

    if args.limit is not None and args.limit > 0:
        sql += " LIMIT %s"
        params.append(args.limit)

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    print("as_of_date,portfolio_id,model_id,dim,l2_norm")

    for as_of_date_db, entity_scope, model_id_db, vector_bytes in rows:
        portfolio_id = None
        if isinstance(entity_scope, dict):
            portfolio_id = entity_scope.get("portfolio_id")

        if vector_bytes is None:
            dim = 0
            l2_norm = 0.0
        else:
            vec = np.frombuffer(vector_bytes, dtype=np.float32)
            dim = vec.shape[0]
            l2_norm = float(np.linalg.norm(vec))

        print(
            f"{as_of_date_db},{portfolio_id or ''},{model_id_db},{dim},{l2_norm:.6f}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
