"""Backfill scenario-level joint stability/fragility embeddings.

This script builds joint stability/fragility embeddings for scenarios by
reusing numeric scenario embeddings (``num-scenario-core-v1``) and
projecting them into the ``STAB_FRAGILITY_V0`` joint space using an
identity joint model.

This is a v0 implementation of the scenario branch described in

- ``docs/joint_spaces/stab_num-stab-core-v1__num-scenario-core-v1__joint-profile-core-v1/README.md``

and is intended primarily for research/analysis use. Entity-level STAB
embeddings are produced separately by
``backfill_joint_stab_fragility_states``.

Embeddings are written to ``historical_db.joint_embeddings`` with:

- ``joint_type = 'STAB_FRAGILITY_V0'``.
- ``model_id = 'joint-stab-fragility-v1'`` (by default).

Each row represents a scenario in the joint space with an entity_scope
like::

    {
      "entity_type": "SCENARIO",
      "scenario_set_id": "SET_ABC123",
      "scenario_id": 42,
      "source": "scenario",
      "as_of_date": "2025-01-31"
    }

Examples
--------

    # Backfill scenario-level STAB embeddings for a scenario set
    python -m prometheus.scripts.backfill_joint_stab_fragility_scenarios \
        --scenario-set-id SET_ABC123 \
        --scenario-model-id num-scenario-core-v1 \
        --joint-model-id joint-stab-fragility-v1 \
        --limit 100
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Mapping, Optional, Sequence, Tuple

import numpy as np
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.encoders.joint import JointEmbeddingService, JointEmbeddingStore, JointExample
from apatheon.encoders.models_joint_simple import IdentityNumericJointModel

logger = get_logger(__name__)


def _load_scenario_set_metadata(
    db_manager: DatabaseManager,
    scenario_set_id: str,
) -> Tuple[Optional[date], Optional[int]]:
    """Return (base_date_end, horizon_days) for a scenario set, if present.

    ``base_date_end`` may be None; in that case we fall back to the
    as_of_date stored with numeric embeddings.
    """

    sql = """
        SELECT base_date_end, horizon_days
        FROM scenario_sets
        WHERE scenario_set_id = %s
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (scenario_set_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        return None, None

    base_date_end, horizon_days = row
    return base_date_end, int(horizon_days) if horizon_days is not None else None


def _load_numeric_scenario_embeddings(
    db_manager: DatabaseManager,
    *,
    scenario_set_id: str,
    scenario_model_id: str,
    limit: Optional[int] = None,
) -> List[Tuple[str, date, np.ndarray]]:
    """Load numeric scenario embeddings for a given scenario_set_id.

    Returns a list of (entity_id, as_of_date, vector), where entity_id has
    the form ``"{scenario_set_id}:{scenario_id}"``.
    """

    like_pattern = f"{scenario_set_id}:%"

    sql = """
        SELECT entity_id, as_of_date, vector
        FROM numeric_window_embeddings
        WHERE entity_type = 'SCENARIO'
          AND model_id = %s
          AND entity_id LIKE %s
        ORDER BY entity_id ASC
    """

    params: List[object] = [scenario_model_id, like_pattern]
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


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill scenario-level joint stability/fragility embeddings "
            "into joint_embeddings using numeric scenario embeddings."
        ),
    )

    parser.add_argument(
        "--scenario-set-id",
        type=str,
        required=True,
        help="Identifier of the scenario set whose scenarios to embed",
    )
    parser.add_argument(
        "--scenario-model-id",
        type=str,
        default="num-scenario-core-v1",
        help="Model_id for numeric scenario embeddings (default: num-scenario-core-v1)",
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help="Joint model_id to tag embeddings with (default: joint-stab-fragility-v1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of scenarios to process (default: 100)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)

    config = get_config()
    db_manager = DatabaseManager(config)

    base_date_end, horizon_days = _load_scenario_set_metadata(
        db_manager=db_manager,
        scenario_set_id=args.scenario_set_id,
    )

    rows = _load_numeric_scenario_embeddings(
        db_manager=db_manager,
        scenario_set_id=args.scenario_set_id,
        scenario_model_id=args.scenario_model_id,
        limit=args.limit,
    )

    if not rows:
        logger.warning(
            "No numeric scenario embeddings found for scenario_set_id=%s model_id=%s; nothing to do",
            args.scenario_set_id,
            args.scenario_model_id,
        )
        return

    logger.info(
        "Backfilling scenario-level joint STAB embeddings: set=%s scenarios=%d scenario_model=%s joint_model=%s",
        args.scenario_set_id,
        len(rows),
        args.scenario_model_id,
        args.joint_model_id,
    )

    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = IdentityNumericJointModel()
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    examples: List[JointExample] = []

    for entity_id, as_of_date_db, vec in rows:
        # entity_id is of the form "{scenario_set_id}:{scenario_id}".
        if ":" in entity_id:
            _, scenario_id_str = entity_id.split(":", 1)
        else:
            scenario_id_str = entity_id

        # Use base_date_end if present; otherwise fall back to the
        # as_of_date stored with the numeric embedding.
        as_of = base_date_end or as_of_date_db

        entity_scope: Mapping[str, object] = {
            "entity_type": "SCENARIO",
            "scenario_set_id": args.scenario_set_id,
            "scenario_id": scenario_id_str,
            "source": "scenario",
            "as_of_date": as_of.isoformat(),
        }

        ex = JointExample(
            joint_type="STAB_FRAGILITY_V0",
            as_of_date=as_of,
            entity_scope=entity_scope,
            numeric_embedding=vec,
            text_embedding=None,
        )
        examples.append(ex)

    if not examples:
        logger.warning("No joint STAB scenario examples constructed; nothing to write")
        return

    _ = service.embed_and_store(examples)
    logger.info(
        "Scenario-level joint STAB backfill complete: wrote %d embeddings with model_id=%s",
        len(examples),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
