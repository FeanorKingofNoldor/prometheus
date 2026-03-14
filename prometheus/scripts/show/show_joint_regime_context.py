"""Show joint regime context embeddings for a region and date range.

This script queries the ``joint_embeddings`` table for rows produced by
``backfill_joint_regime_context`` with:

- ``joint_type = 'REGIME_CONTEXT_V0'``
- ``model_id = 'joint-regime-core-v1'``

and prints a CSV-like view including date, region (from entity_scope),
model_id, and simple vector diagnostics (dimension, L2 norm).

It is intended as a dev/debugging helper to inspect how the joint regime
context space is populated over time.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Dict, Optional, Sequence

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


def _row_to_dict(row: tuple[Any, ...]) -> Dict[str, Any]:
    as_of_date, entity_scope_json, model_id, vector_bytes = row
    try:
        scope = json.loads(entity_scope_json)
    except Exception:
        scope = {}

    region = scope.get("region")
    episode_id = scope.get("episode_id")

    vec = np.frombuffer(vector_bytes, dtype=np.float32)
    dim = int(vec.shape[0])
    l2 = float(np.linalg.norm(vec)) if dim > 0 else 0.0

    return {
        "as_of_date": as_of_date.isoformat(),
        "region": region,
        "episode_id": episode_id,
        "model_id": model_id,
        "dim": dim,
        "l2_norm": f"{l2:.6f}",
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect joint regime context embeddings (REGIME_CONTEXT_V0) "
            "from the joint_embeddings table."
        ),
    )

    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help=(
            "Optional region filter; if provided, only rows whose entity_scope "
            "contains this region will be shown."
        ),
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) inclusive",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) inclusive",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-regime-core-v1",
        help="Joint model_id to filter on (default: joint-regime-core-v1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of rows to display (default: 200)",
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    config = get_config()
    db_manager = DatabaseManager(config)

    where_clauses = [
        "joint_type = 'REGIME_CONTEXT_V0'",
        "model_id = %s",
        "as_of_date BETWEEN %s AND %s",
    ]
    params: list[Any] = [args.model_id, args.start, args.end]

    # Region is stored inside entity_scope JSON. We filter in SQL using
    # the JSON operator when a region is provided, otherwise we leave it
    # unconstrained.
    if args.region is not None:
        where_clauses.append("(entity_scope->>'region') = %s")
        params.append(args.region)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT as_of_date, entity_scope::text, model_id, vector "
        "FROM joint_embeddings "
        + where_sql +
        " ORDER BY as_of_date ASC LIMIT %s"
    )
    params.append(args.limit)

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.info("No joint regime context embeddings found for the given filters.")
        return

    # Print CSV header.
    print("as_of_date,region,episode_id,model_id,dim,l2_norm")
    for row in rows:
        rec = _row_to_dict(row)
        print(
            f"{rec['as_of_date']},{rec['region']},{rec['episode_id']},"
            f"{rec['model_id']},{rec['dim']},{rec['l2_norm']}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
