"""Find instruments with similar joint Assessment context embeddings.

This script queries the ASSESSMENT_CTX_V0 joint space and returns
nearest-neighbour instruments to a target instrument based on their
joint Assessment context embeddings (``joint-assessment-context-v1``).

It is analogous to ``find_similar_meta_runs`` and
``find_similar_stab_scenarios`` and is intended for analysis and
diagnostics rather than production trading logic.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

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


def _load_target_embedding(
    db_manager: DatabaseManager,
    *,
    instrument_id: str,
    as_of: date,
    model_id: str,
) -> np.ndarray:
    """Load a joint Assessment context embedding for an instrument/date."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'ASSESSMENT_CTX_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'entity_id') = %s
        ORDER BY joint_id DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of, instrument_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise RuntimeError(
            f"No ASSESSMENT_CTX_V0 embedding found for instrument_id={instrument_id!r} "
            f"as_of={as_of} model_id={model_id!r}"
        )

    (vector_bytes,) = row
    return np.frombuffer(vector_bytes, dtype=np.float32)


def _load_candidate_embeddings(
    db_manager: DatabaseManager,
    *,
    model_id: str,
    as_of: date,
    exclude_instrument_id: str,
    region: Optional[str],
    limit: Optional[int],
) -> List[Tuple[Dict[str, Any], np.ndarray]]:
    """Load candidate Assessment context embeddings for the given date."""

    where_clauses = [
        "joint_type = 'ASSESSMENT_CTX_V0'",
        "model_id = %s",
        "as_of_date = %s",
        "(entity_scope->>'entity_id') <> %s",
    ]
    params: List[Any] = [model_id, as_of, exclude_instrument_id]

    if region is not None:
        where_clauses.append("(entity_scope->>'region') = %s")
        params.append(region)

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
            "Find instruments whose joint Assessment context embeddings are "
            "closest to a target instrument on a given date."
        ),
    )

    parser.add_argument(
        "--instrument-id",
        type=str,
        required=True,
        help="Target instrument_id to compare against",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for the Assessment context embeddings",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-assessment-context-v1",
        help="Joint Assessment context model_id to use (default: joint-assessment-context-v1)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Optional region filter for candidate instruments",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of most similar instruments to display (default: 10)",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=2000,
        help="Maximum number of candidate instruments to consider (default: 2000)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    target_vec = _load_target_embedding(
        db_manager=db_manager,
        instrument_id=args.instrument_id,
        as_of=args.as_of,
        model_id=args.model_id,
    )

    candidates = _load_candidate_embeddings(
        db_manager=db_manager,
        model_id=args.model_id,
        as_of=args.as_of,
        exclude_instrument_id=args.instrument_id,
        region=args.region,
        limit=args.candidate_limit,
    )

    if not candidates:
        logger.warning(
            "No ASSESSMENT_CTX_V0 candidate embeddings found for the given filters.",
        )
        return

    dim = int(target_vec.shape[0])
    filtered: List[Tuple[Dict[str, Any], np.ndarray]] = []
    for scope, vec in candidates:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping instrument_id=%s due to dimension mismatch: %s != %s",
                scope.get("entity_id"),
                vec.shape[0],
                dim,
            )
            continue
        filtered.append((scope, vec))

    if not filtered:
        logger.warning("No compatible Assessment context embeddings after dimension filtering.")
        return

    scored: List[Tuple[float, float, Dict[str, Any]]] = []
    for scope, vec in filtered:
        cos = _cosine_similarity(target_vec, vec)
        dist = float(np.linalg.norm(target_vec - vec))
        scored.append((cos, dist, scope))

    scored.sort(key=lambda t: (-t[0], t[1]))

    top_k = min(args.top_k, len(scored))
    top = scored[:top_k]

    print("cosine,euclidean,instrument_id,issuer_id,region,source")
    for cos, dist, scope in top:
        print(
            f"{cos:.6f},{dist:.6f},{scope.get('entity_id')},"
            f"{scope.get('issuer_id')},{scope.get('region')},{scope.get('source')}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
