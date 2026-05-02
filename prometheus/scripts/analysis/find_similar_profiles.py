"""Find issuers with similar joint profile embeddings.

This script queries the PROFILE_CORE_V0 joint space and returns
nearest-neighbour issuers to a target issuer based on their joint
profile embeddings (`joint-profile-core-v1`).

It:

- Loads the latest joint profile embedding for a target `issuer_id` and
  `as_of_date` from `joint_embeddings` where:
  - `joint_type = 'PROFILE_CORE_V0'`
  - `model_id = 'joint-profile-core-v1'` (by default).
- Loads candidate embeddings for other issuers on the same `as_of_date`
  (or within a date range), optionally filtered by region.
- Computes cosine similarity and Euclidean distance between the target
  profile and each candidate.
- Prints the top-K most similar issuers.

This is a dev/research helper to answer questions like:

    "Which issuers look most similar to ISS_ACME_CORP in profile space
    on 2025-01-31?"
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


def _load_target_profile_embedding(
    db_manager: DatabaseManager,
    *,
    issuer_id: str,
    as_of: date,
    model_id: str,
) -> np.ndarray:
    """Load a joint profile embedding for an issuer/date."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'PROFILE_CORE_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'issuer_id') = %s
        ORDER BY joint_id DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of, issuer_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise RuntimeError(
            f"No PROFILE_CORE_V0 embedding found for issuer_id={issuer_id!r} "
            f"as_of={as_of} model_id={model_id!r}"
        )

    (vector_bytes,) = row
    return np.frombuffer(vector_bytes, dtype=np.float32)


def _load_candidate_profiles(
    db_manager: DatabaseManager,
    *,
    model_id: str,
    as_of: date,
    exclude_issuer_id: str,
    region: Optional[str],
    limit: Optional[int],
) -> List[Tuple[Dict[str, Any], np.ndarray]]:
    """Load candidate joint profile embeddings for a given date."""

    where_clauses = [
        "joint_type = 'PROFILE_CORE_V0'",
        "model_id = %s",
        "as_of_date = %s",
        "(entity_scope->>'issuer_id') <> %s",
    ]
    params: List[Any] = [model_id, as_of, exclude_issuer_id]

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
            "Find issuers whose joint profile embeddings are closest to a "
            "target issuer on a given date."
        ),
    )

    parser.add_argument(
        "--issuer-id",
        type=str,
        required=True,
        help="Target issuer_id to compare against",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for the profile embeddings",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-profile-core-v1",
        help="Joint profile model_id to use (default: joint-profile-core-v1)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Optional region filter for candidate issuers",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of most similar issuers to display (default: 10)",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=1000,
        help="Maximum number of candidate issuers to consider (default: 1000)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    target_vec = _load_target_profile_embedding(
        db_manager=db_manager,
        issuer_id=args.issuer_id,
        as_of=args.as_of,
        model_id=args.model_id,
    )

    candidates = _load_candidate_profiles(
        db_manager=db_manager,
        model_id=args.model_id,
        as_of=args.as_of,
        exclude_issuer_id=args.issuer_id,
        region=args.region,
        limit=args.candidate_limit,
    )

    if not candidates:
        logger.warning("No PROFILE_CORE_V0 candidate embeddings found for the given filters.")
        return

    dim = int(target_vec.shape[0])
    filtered: List[Tuple[Dict[str, Any], np.ndarray]] = []
    for scope, vec in candidates:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping issuer_id=%s due to dimension mismatch: %s != %s",
                scope.get("issuer_id"),
                vec.shape[0],
                dim,
            )
            continue
        filtered.append((scope, vec))

    if not filtered:
        logger.warning("No compatible profile embeddings after dimension filtering.")
        return

    scored: List[Tuple[float, float, Dict[str, Any]]] = []
    for scope, vec in filtered:
        cos = _cosine_similarity(target_vec, vec)
        dist = float(np.linalg.norm(target_vec - vec))
        scored.append((cos, dist, scope))

    scored.sort(key=lambda t: (-t[0], t[1]))

    top_k = min(args.top_k, len(scored))
    top = scored[:top_k]

    print("cosine,euclidean,issuer_id,instrument_id,region,source")
    for cos, dist, scope in top:
        print(
            f"{cos:.6f},{dist:.6f},{scope.get('issuer_id')},"
            f"{scope.get('instrument_id')},{scope.get('region')},{scope.get('source')}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
