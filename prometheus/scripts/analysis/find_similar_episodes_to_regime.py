"""Find episodes similar to a joint regime context embedding.

This script ties together the REGIME_CONTEXT_V0 and EPISODE_V0 joint
spaces by:

- Loading a joint regime context embedding for a given (region, as_of)
  from ``joint_embeddings`` with ``joint_type = 'REGIME_CONTEXT_V0'`` and
  ``model_id = 'joint-regime-core-v1'``, and
- Comparing it to all joint episode embeddings (``joint_type =
  'EPISODE_V0'``) for that region using cosine similarity or
  Euclidean distance.

It then prints the top-K most similar episodes.

This is a dev/research helper to answer "which known episodes look most
like the current joint regime context?".
"""

from __future__ import annotations

import argparse
import json
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


def _load_joint_regime_embedding(
    db_manager: DatabaseManager,
    *,
    region: str,
    as_of: date,
    model_id: str,
) -> np.ndarray:
    """Load a single joint regime context embedding for (region, as_of)."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'REGIME_CONTEXT_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'region') = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of, region))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise RuntimeError(
            f"No joint regime context embedding found for region={region!r} as_of={as_of} "
            f"model_id={model_id!r}"
        )

    return np.frombuffer(row[0], dtype=np.float32)


def _load_joint_episode_embeddings(
    db_manager: DatabaseManager,
    *,
    region: Optional[str],
    model_id: str,
) -> List[Tuple[Dict[str, Any], np.ndarray]]:
    """Load all joint episode embeddings (and scopes) matching filters."""

    where_clauses = [
        "joint_type = 'EPISODE_V0'",
        "model_id = %s",
    ]
    params: List[Any] = [model_id]

    if region is not None:
        where_clauses.append("(entity_scope->>'region') = %s")
        params.append(region)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT as_of_date, entity_scope::text, vector "
        "FROM joint_embeddings "
        + where_sql +
        " ORDER BY as_of_date ASC"
    )

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Tuple[Dict[str, Any], np.ndarray]] = []
    for as_of_date, scope_json, vector_bytes in rows:
        try:
            scope = json.loads(scope_json)
        except Exception:
            scope = {}
        scope["as_of_date"] = as_of_date.isoformat()
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
            "Find episodes whose joint embeddings are closest to the "
            "current joint regime context embedding for a region/date."
        ),
    )

    parser.add_argument(
        "--region",
        type=str,
        required=True,
        help="Region identifier as used in regimes/joint_embeddings (e.g. US)",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for the joint regime context embedding",
    )
    parser.add_argument(
        "--regime-model-id",
        type=str,
        default="joint-regime-core-v1",
        help="Joint regime model_id (default: joint-regime-core-v1)",
    )
    parser.add_argument(
        "--episode-model-id",
        type=str,
        default="joint-episode-core-v1",
        help="Joint episode model_id (default: joint-episode-core-v1)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of most similar episodes to display (default: 10)",
    )
    parser.add_argument(
        "--all-regions",
        action="store_true",
        help=(
            "Search episodes across all regions instead of restricting to "
            "the same region as the regime embedding."
        ),
    )

    args = parser.parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    # Load the joint regime context embedding for (region, as_of).
    regime_vec = _load_joint_regime_embedding(
        db_manager,
        region=args.region,
        as_of=args.as_of,
        model_id=args.regime_model_id,
    )

    # Load joint episode embeddings.
    episode_region = None if args.all_regions else args.region
    episodes = _load_joint_episode_embeddings(
        db_manager,
        region=episode_region,
        model_id=args.episode_model_id,
    )

    if not episodes:
        logger.warning("No joint episode embeddings found for the given filters.")
        return

    # Ensure dimensions match.
    dim = int(regime_vec.shape[0])
    filtered: List[Tuple[Dict[str, Any], np.ndarray]] = []
    for scope, vec in episodes:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping episode_id=%s due to dimension mismatch: %s != %s",
                scope.get("episode_id"),
                vec.shape[0],
                dim,
            )
            continue
        filtered.append((scope, vec))

    if not filtered:
        logger.warning("No compatible episode embeddings after dimension filtering.")
        return

    # Compute similarities.
    scored: List[Tuple[float, float, Dict[str, Any]]] = []
    for scope, vec in filtered:
        cos = _cosine_similarity(regime_vec, vec)
        dist = float(np.linalg.norm(regime_vec - vec))
        scored.append((cos, dist, scope))

    # Sort by cosine similarity descending, then distance ascending.
    scored.sort(key=lambda t: (-t[0], t[1]))

    top_k = max(1, args.top_k)
    top = scored[:top_k]

    print("cosine,euclidean,episode_id,label,region,start_date,end_date,episode_as_of")
    for cos, dist, scope in top:
        print(
            f"{cos:.6f},{dist:.6f},{scope.get('episode_id')},{scope.get('label')},"
            f"{scope.get('region')},{scope.get('window', {}).get('start_date')},"
            f"{scope.get('window', {}).get('end_date')},{scope.get('as_of_date')}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
