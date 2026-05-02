"""Find portfolios with similar joint portfolio embeddings.

This script queries the PORTFOLIO_CORE_V0 joint space and returns
nearest-neighbour portfolios to a target portfolio based on their joint
portfolio embeddings (``joint-portfolio-core-v1``).

It is analogous to other joint similarity tools such as
``find_similar_profiles`` and ``find_similar_meta_runs`` and is intended
for research and monitoring workflows.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

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


def _load_target_embedding(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of: date,
    model_id: str,
) -> np.ndarray:
    """Load a joint portfolio embedding for a portfolio/date."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'PORTFOLIO_CORE_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'portfolio_id') = %s
        ORDER BY joint_id DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of, portfolio_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise RuntimeError(
            f"No PORTFOLIO_CORE_V0 embedding found for portfolio_id={portfolio_id!r} "
            f"as_of={as_of} model_id={model_id!r}"
        )

    (vector_bytes,) = row
    return np.frombuffer(vector_bytes, dtype=np.float32)


def _load_candidate_embeddings(
    db_manager: DatabaseManager,
    *,
    model_id: str,
    as_of: date,
    exclude_portfolio_id: str,
    limit: Optional[int],
) -> List[Tuple[Dict[str, Any], np.ndarray]]:
    """Load candidate portfolio embeddings for a given date."""

    where_clauses = [
        "joint_type = 'PORTFOLIO_CORE_V0'",
        "model_id = %s",
        "as_of_date = %s",
        "(entity_scope->>'portfolio_id') <> %s",
    ]
    params: List[Any] = [model_id, as_of, exclude_portfolio_id]

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT entity_scope, vector "
        "FROM joint_embeddings "
        + where_sql +
        " ORDER BY joint_id DESC"
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

    results: List[Tuple[Dict[str, Any], np.ndarray]] = []
    for entity_scope, vector_bytes in rows:
        scope: Dict[str, Any]
        if isinstance(entity_scope, dict):
            scope = dict(entity_scope)
        else:
            scope = {}
        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        results.append((scope, vec))

    return results


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    a_norm = np.linalg.norm(a)
    b_norm = np.linalg.norm(b)
    if a_norm == 0.0 or b_norm == 0.0:
        return 0.0
    return float(np.dot(a, b) / (a_norm * b_norm))


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Find portfolios whose joint portfolio embeddings are closest "
            "to a target portfolio on a given date."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        required=True,
        help="Target portfolio_id to compare against",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for the portfolio embeddings",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-portfolio-core-v1",
        help="Joint portfolio model_id to use (default: joint-portfolio-core-v1)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of most similar portfolios to display (default: 10)",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=1000,
        help="Maximum number of candidate portfolios to consider (default: 1000)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    target_vec = _load_target_embedding(
        db_manager=db_manager,
        portfolio_id=args.portfolio_id,
        as_of=args.as_of,
        model_id=args.model_id,
    )

    candidates = _load_candidate_embeddings(
        db_manager=db_manager,
        model_id=args.model_id,
        as_of=args.as_of,
        exclude_portfolio_id=args.portfolio_id,
        limit=args.candidate_limit,
    )

    if not candidates:
        logger.warning("No PORTFOLIO_CORE_V0 candidate embeddings found for the given filters.")
        return

    dim = int(target_vec.shape[0])
    filtered: List[Tuple[Dict[str, Any], np.ndarray]] = []
    for scope, vec in candidates:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping portfolio_id=%s due to dimension mismatch: %s != %s",
                scope.get("portfolio_id"),
                vec.shape[0],
                dim,
            )
            continue
        filtered.append((scope, vec))

    if not filtered:
        logger.warning("No compatible portfolio embeddings after dimension filtering.")
        return

    scored: List[Tuple[float, float, Dict[str, Any]]] = []
    for scope, vec in filtered:
        cos = _cosine_similarity(target_vec, vec)
        dist = float(np.linalg.norm(target_vec - vec))
        scored.append((cos, dist, scope))

    scored.sort(key=lambda t: (-t[0], t[1]))

    top_k = min(args.top_k, len(scored))
    top = scored[:top_k]

    print("cosine,euclidean,portfolio_id,source")
    for cos, dist, scope in top:
        print(
            f"{cos:.6f},{dist:.6f},{scope.get('portfolio_id')},{scope.get('source')}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
