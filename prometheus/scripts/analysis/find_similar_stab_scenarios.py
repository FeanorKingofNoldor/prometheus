"""Find scenarios similar to an instrument's STAB state in joint space.

This script connects entity-level joint STAB states (for instruments)
with scenario-level joint STAB embeddings by:

- Loading a joint STAB embedding for a given (instrument_id, as_of)
  from `joint_embeddings` with:
  - `joint_type = 'STAB_FRAGILITY_V0'`
  - `entity_scope->>'entity_type' = 'INSTRUMENT'`.
- Loading scenario-level STAB embeddings (entity_type = `SCENARIO`) from
  the same joint space, optionally filtered by `scenario_set_id` and
  date range.
- Computing cosine similarity and Euclidean distance between the
  instrument state and each scenario.
- Printing the top-K most similar scenarios.

This is a dev/research helper to answer questions like:

    "Given the current STAB state of instrument AAA.US, which
    scenarios in SET_X produce similar joint STAB patterns?"
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


def _load_instrument_stab_embedding(
    db_manager: DatabaseManager,
    *,
    instrument_id: str,
    as_of: date,
    model_id: str,
) -> np.ndarray:
    """Load a joint STAB embedding for an instrument/date."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'STAB_FRAGILITY_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'entity_type') = 'INSTRUMENT'
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
            f"No STAB_FRAGILITY_V0 embedding found for instrument={instrument_id!r} "
            f"as_of={as_of} model_id={model_id!r}"
        )

    (vector_bytes,) = row
    return np.frombuffer(vector_bytes, dtype=np.float32)


def _load_scenario_embeddings(
    db_manager: DatabaseManager,
    *,
    model_id: str,
    scenario_set_id: Optional[str],
    start: Optional[date],
    end: Optional[date],
    limit: Optional[int],
) -> List[Tuple[date, Dict[str, Any], np.ndarray]]:
    """Load scenario-level joint STAB embeddings as candidates."""

    where_clauses = [
        "joint_type = 'STAB_FRAGILITY_V0'",
        "model_id = %s",
        "(entity_scope->>'entity_type') = 'SCENARIO'",
    ]
    params: List[Any] = [model_id]

    if scenario_set_id is not None:
        where_clauses.append("(entity_scope->>'scenario_set_id') = %s")
        params.append(scenario_set_id)

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
            "Find STAB scenarios in joint space that are closest to an "
            "instrument's joint STAB state."
        ),
    )

    parser.add_argument(
        "--instrument-id",
        type=str,
        required=True,
        help="Instrument_id whose STAB state to use as the query",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for the instrument STAB state",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help="Joint STAB model_id to use (default: joint-stab-fragility-v1)",
    )
    parser.add_argument(
        "--scenario-set-id",
        type=str,
        default=None,
        help="Optional scenario_set_id filter for candidate scenarios",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Optional minimum as_of_date for scenario candidates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="Optional maximum as_of_date for scenario candidates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of most similar scenarios to display (default: 10)",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=500,
        help="Maximum number of scenario candidates to consider (default: 500)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    target_vec = _load_instrument_stab_embedding(
        db_manager=db_manager,
        instrument_id=args.instrument_id,
        as_of=args.as_of,
        model_id=args.model_id,
    )

    candidates = _load_scenario_embeddings(
        db_manager=db_manager,
        model_id=args.model_id,
        scenario_set_id=args.scenario_set_id,
        start=args.start,
        end=args.end,
        limit=args.candidate_limit,
    )

    if not candidates:
        logger.warning("No scenario-level STAB embeddings found for the given filters.")
        return

    dim = int(target_vec.shape[0])
    filtered: List[Tuple[date, Dict[str, Any], np.ndarray]] = []
    for as_of_date_db, scope, vec in candidates:
        if vec.shape[0] != dim:
            logger.warning(
                "Skipping scenario_id=%s due to dimension mismatch: %s != %s",
                scope.get("scenario_id"),
                vec.shape[0],
                dim,
            )
            continue
        filtered.append((as_of_date_db, scope, vec))

    if not filtered:
        logger.warning("No compatible scenario embeddings after dimension filtering.")
        return

    scored: List[Tuple[float, float, Dict[str, Any]]] = []
    for _as_of_date_db, scope, vec in filtered:
        cos = _cosine_similarity(target_vec, vec)
        dist = float(np.linalg.norm(target_vec - vec))
        scored.append((cos, dist, scope))

    scored.sort(key=lambda t: (-t[0], t[1]))

    top_k = min(args.top_k, len(scored))
    top = scored[:top_k]

    print("cosine,euclidean,scenario_set_id,scenario_id,as_of_date,source")
    for cos, dist, scope in top:
        print(
            f"{cos:.6f},{dist:.6f},{scope.get('scenario_set_id')},"
            f"{scope.get('scenario_id')},{scope.get('as_of_date')},{scope.get('source')}"
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
