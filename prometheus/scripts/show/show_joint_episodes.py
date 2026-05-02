"""Show joint episode embeddings from the joint_embeddings table.

This script queries ``joint_embeddings`` for rows produced by
``backfill_joint_episode_context`` with:

- ``joint_type = 'EPISODE_V0'``
- ``model_id = 'joint-episode-core-v1'`` (by default)

and prints a CSV-like view including as_of_date, episode_id, label,
region, and simple vector diagnostics (dimension, L2 norm).

It is intended as a dev/debugging helper to inspect how the joint
episode space is populated.
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any, Dict, Optional, Sequence

import numpy as np
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

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

    episode_id = scope.get("episode_id")
    label = scope.get("label")
    region = scope.get("region")
    window = scope.get("window", {}) or {}
    start_date = window.get("start_date")
    end_date = window.get("end_date")

    vec = np.frombuffer(vector_bytes, dtype=np.float32)
    dim = int(vec.shape[0])
    l2 = float(np.linalg.norm(vec)) if dim > 0 else 0.0

    return {
        "as_of_date": as_of_date.isoformat(),
        "episode_id": episode_id,
        "label": label,
        "region": region,
        "start_date": start_date,
        "end_date": end_date,
        "model_id": model_id,
        "dim": dim,
        "l2_norm": f"{l2:.6f}",
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect joint episode embeddings (EPISODE_V0) from the "
            "joint_embeddings table."
        ),
    )

    parser.add_argument(
        "--episode-id",
        type=str,
        default=None,
        help="Optional episode_id filter.",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Optional region filter (from entity_scope.region).",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional start date (YYYY-MM-DD) for as_of_date filter.",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional end date (YYYY-MM-DD) for as_of_date filter.",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-episode-core-v1",
        help="Joint model_id to filter on (default: joint-episode-core-v1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of rows to display (default: 200)",
    )

    args = parser.parse_args(argv)

    if args.start and args.end and args.end < args.start:
        parser.error("--end must be >= --start")

    config = get_config()
    db_manager = DatabaseManager(config)

    where_clauses = [
        "joint_type = 'EPISODE_V0'",
        "model_id = %s",
    ]
    params: list[Any] = [args.model_id]

    if args.start is not None and args.end is not None:
        where_clauses.append("as_of_date BETWEEN %s AND %s")
        params.extend([args.start, args.end])
    elif args.start is not None:
        where_clauses.append("as_of_date >= %s")
        params.append(args.start)
    elif args.end is not None:
        where_clauses.append("as_of_date <= %s")
        params.append(args.end)

    if args.episode_id is not None:
        where_clauses.append("(entity_scope->>'episode_id') = %s")
        params.append(args.episode_id)

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
        logger.info("No joint episode embeddings found for the given filters.")
        return

    print("as_of_date,episode_id,label,region,start_date,end_date,model_id,dim,l2_norm")
    for row in rows:
        rec = _row_to_dict(row)
        print(
            f"{rec['as_of_date']},{rec['episode_id']},{rec['label']},"
            f"{rec['region']},{rec['start_date']},{rec['end_date']},"
            f"{rec['model_id']},{rec['dim']},{rec['l2_norm']}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
