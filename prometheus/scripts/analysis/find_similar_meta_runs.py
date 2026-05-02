"""Find runs similar in Meta Config+Env joint space.

This script operates on the META_CONFIG_ENV_V0 joint space populated by
`backfill_joint_meta_config_env` and answers questions like:

    "Given backtest run BT_RUN_X, which other runs are closest in
    config+environment+outcome space?"

It:

- Loads the joint embedding for a target `run_id` from `joint_embeddings`
  (joint_type = 'META_CONFIG_ENV_V0').
- Loads candidate embeddings for other runs with the same joint_type and
  model_id (optionally filtered by strategy_id and/or date range).
- Computes cosine similarity and Euclidean distance between the target
  and each candidate.
- Prints the top-K most similar runs.

This is a dev/research helper for analysing config experiments and
backtests via `joint-meta-config-env-v1`.
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
    run_id: str,
    model_id: str,
) -> np.ndarray:
    """Load joint META_CONFIG_ENV_V0 embedding for a specific run_id."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'META_CONFIG_ENV_V0'
          AND model_id = %s
          AND (entity_scope->>'run_id') = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, run_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise RuntimeError(
            f"No META_CONFIG_ENV_V0 embedding found for run_id={run_id!r} model_id={model_id!r}"
        )

    (vector_bytes,) = row
    return np.frombuffer(vector_bytes, dtype=np.float32)


def _load_candidate_embeddings(
    db_manager: DatabaseManager,
    *,
    model_id: str,
    exclude_run_id: str,
    strategy_id: Optional[str],
    start: Optional[date],
    end: Optional[date],
    limit: Optional[int],
) -> List[Tuple[date, Dict[str, Any], np.ndarray]]:
    """Load candidate META_CONFIG_ENV_V0 embeddings to compare against."""

    where_clauses = [
        "joint_type = 'META_CONFIG_ENV_V0'",
        "model_id = %s",
        "(entity_scope->>'run_id') <> %s",
    ]
    params: List[Any] = [model_id, exclude_run_id]

    if strategy_id is not None:
        where_clauses.append("(entity_scope->>'strategy_id') = %s")
        params.append(strategy_id)

    if start is not None:
        where_clauses.append("as_of_date >= %s")
        params.append(start)
    if end is not None:
        where_clauses.append("as_of_date <= %s")
        params.append(end)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT as_of_date, entity_scope, vector "
        "FROM joint_embeddings "
        + where_sql +
        " ORDER BY as_of_date DESC, joint_id DESC"
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

    results: List[Tuple[date, Dict[str, Any], np.ndarray]] = []
    for as_of_date_db, entity_scope, vector_bytes in rows:
        scope: Dict[str, Any]
        if isinstance(entity_scope, dict):
            scope = dict(entity_scope)
        else:
            scope = {}
        scope["as_of_date"] = as_of_date_db.isoformat()
        vec = np.frombuffer(vector_bytes, dtype=np.float32)
        results.append((as_of_date_db, scope, vec))

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
            "Find backtest runs whose Meta Config+Env joint embeddings are "
            "closest to a target run_id."
        ),
    )

    parser.add_argument(
        "--run-id",
        type=str,
        required=True,
        help="Target backtest run_id to compare against",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-meta-config-env-v1",
        help="Joint model_id to use (default: joint-meta-config-env-v1)",
    )
    parser.add_argument(
        "--strategy-id",
        type=str,
        default=None,
        help="Optional strategy_id filter for candidate runs",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional minimum as_of_date for candidates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional maximum as_of_date for candidates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of most similar runs to display (default: 10)",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=1000,
        help="Maximum number of candidate runs to consider (default: 1000)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    target_vec = _load_target_embedding(
        db_manager=db_manager,
        run_id=args.run_id,
        model_id=args.model_id,
    )

    candidates = _load_candidate_embeddings(
        db_manager=db_manager,
        model_id=args.model_id,
        exclude_run_id=args.run_id,
        strategy_id=args.strategy_id,
        start=args.start,
        end=args.end,
        limit=args.candidate_limit,
    )

    if not candidates:
        logger.warning("No META_CONFIG_ENV_V0 candidate embeddings found for the given filters.")
        return

    dim = int(target_vec.shape[0])
    filtered: List[Tuple[date, Dict[str, Any], np.ndarray]] = []
    for as_of_date_db, scope, vec in candidates:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping run_id=%s due to dimension mismatch: %s != %s",
                scope.get("run_id"),
                vec.shape[0],
                dim,
            )
            continue
        filtered.append((as_of_date_db, scope, vec))

    if not filtered:
        logger.warning("No compatible candidate embeddings after dimension filtering.")
        return

    scored: List[Tuple[float, float, Dict[str, Any]]] = []
    for _as_of_date_db, scope, vec in filtered:
        cos = _cosine_similarity(target_vec, vec)
        dist = float(np.linalg.norm(target_vec - vec))
        scored.append((cos, dist, scope))

    scored.sort(key=lambda t: (-t[0], t[1]))

    top_k = min(args.top_k, len(scored))
    top = scored[:top_k]

    print("cosine,euclidean,run_id,strategy_id,universe_id,as_of_date,source")
    for cos, dist, scope in top:
        print(
            f"{cos:.6f},{dist:.6f},{scope.get('run_id')},{scope.get('strategy_id')},"
            f"{scope.get('universe_id')},{scope.get('as_of_date')},{scope.get('source')}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
