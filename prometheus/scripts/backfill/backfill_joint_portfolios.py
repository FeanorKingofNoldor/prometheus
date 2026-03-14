"""Backfill joint portfolio embeddings (joint-portfolio-core-v1).

This script constructs a v0 joint space for portfolios by reusing numeric
portfolio embeddings (``num-portfolio-core-v1``) and projecting them into
`R^384` via an identity joint model.

It is a thin wrapper over existing numeric portfolio embeddings and
writes into ``historical_db.joint_embeddings`` with:

- `joint_type = 'PORTFOLIO_CORE_V0'`.
- `model_id = 'joint-portfolio-core-v1'` (by default).

Each row represents a portfolio snapshot as a point in the portfolio
joint space with an entity_scope like::

    {
      "entity_type": "PORTFOLIO",
      "portfolio_id": "PORTFOLIO_CORE_US_EQ_001",
      "source": "num-portfolio-core-v1",
      "as_of_date": "2025-01-31"
    }

Examples
--------

    # Backfill joint portfolio embeddings for a date
    python -m prometheus.scripts.backfill_joint_portfolios \
        --as-of 2025-01-31 \
        --numeric-model-id num-portfolio-core-v1 \
        --joint-model-id joint-portfolio-core-v1 \
        --limit 100
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Mapping, Optional, Sequence, Tuple

import numpy as np

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders.joint import JointEmbeddingService, JointEmbeddingStore, JointExample
from apathis.encoders.models_joint_simple import IdentityNumericJointModel


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_numeric_portfolio_embeddings(
    db_manager: DatabaseManager,
    *,
    as_of: date,
    model_id: str,
    portfolio_ids: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, date, np.ndarray]]:
    """Load numeric portfolio embeddings from numeric_window_embeddings.

    Returns (portfolio_id, as_of_date, vector) tuples.
    """

    where_clauses = [
        "entity_type = 'PORTFOLIO'",
        "model_id = %s",
        "as_of_date = %s",
    ]
    params: List[object] = [model_id, as_of]

    if portfolio_ids:
        where_clauses.append("entity_id = ANY(%s)")
        params.append(list(portfolio_ids))

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT entity_id, as_of_date, vector "
        "FROM numeric_window_embeddings" + where_sql + " ORDER BY entity_id ASC"
    )

    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Tuple[str, date, np.ndarray]] = []
    for entity_id, as_of_date_db, vector_bytes in rows:
        if vector_bytes is None:
            continue
        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        results.append((str(entity_id), as_of_date_db, vec))

    return results


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint portfolio embeddings (PORTFOLIO_CORE_V0) "
            "from numeric portfolio embeddings into joint_embeddings."
        ),
    )

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for portfolio snapshots",
    )
    parser.add_argument(
        "--portfolio-id",
        dest="portfolio_ids",
        action="append",
        help="Optional portfolio_id filter (can be repeated)",
    )
    parser.add_argument(
        "--numeric-model-id",
        type=str,
        default="num-portfolio-core-v1",
        help="Model_id for numeric portfolio embeddings (default: num-portfolio-core-v1)",
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-portfolio-core-v1",
        help="Joint model_id to tag embeddings with (default: joint-portfolio-core-v1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of portfolios to process (default: 100)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    portfolio_ids: Optional[Sequence[str]] = args.portfolio_ids if args.portfolio_ids else None

    rows = _load_numeric_portfolio_embeddings(
        db_manager=db_manager,
        as_of=args.as_of,
        model_id=args.numeric_model_id,
        portfolio_ids=portfolio_ids,
        limit=args.limit,
    )

    if not rows:
        logger.warning(
            "No numeric portfolio embeddings found for as_of=%s model_id=%s; nothing to do",
            args.as_of,
            args.numeric_model_id,
        )
        return

    logger.info(
        "Backfilling joint portfolio embeddings: as_of=%s portfolios=%d numeric_model=%s joint_model=%s",
        args.as_of,
        len(rows),
        args.numeric_model_id,
        args.joint_model_id,
    )

    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = IdentityNumericJointModel()
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    examples: List[JointExample] = []

    for portfolio_id, as_of_date_db, vec in rows:
        entity_scope: Mapping[str, object] = {
            "entity_type": "PORTFOLIO",
            "portfolio_id": portfolio_id,
            "source": args.numeric_model_id,
            "as_of_date": as_of_date_db.isoformat(),
        }

        ex = JointExample(
            joint_type="PORTFOLIO_CORE_V0",
            as_of_date=as_of_date_db,
            entity_scope=entity_scope,
            numeric_embedding=vec,
            text_embedding=None,
        )
        examples.append(ex)

    if not examples:
        logger.warning("No joint portfolio examples constructed; nothing to write")
        return

    _ = service.embed_and_store(examples)
    logger.info(
        "Joint portfolio backfill complete: wrote %d embeddings with model_id=%s",
        len(examples),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
