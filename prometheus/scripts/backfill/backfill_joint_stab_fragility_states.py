"""Backfill joint stability/fragility embeddings for entity states.

This script builds joint stability/fragility embeddings for instruments
by combining:

- Numeric stability embeddings (``num-stab-core-v1``) from
  ``numeric_window_embeddings``.
- Optional structural profile embeddings (``joint-profile-core-v1``)
  from ``joint_embeddings``.

The numeric branches are combined into a single numeric embedding,
which is then passed through an identity joint model into the
``STAB_FRAGILITY_V0`` joint space.

Embeddings are written to ``historical_db.joint_embeddings`` with:

- ``joint_type = 'STAB_FRAGILITY_V0'``.
- ``model_id = 'joint-stab-fragility-v1'`` (by default).

Examples
--------

    # Backfill joint stability embeddings for instruments in US_EQ
    python -m prometheus.scripts.backfill_joint_stab_fragility_states \
        --as-of 2025-01-31 \
        --market-id US_EQ \
        --stab-model-id num-stab-core-v1 \
        --profile-joint-model-id joint-profile-core-v1 \
        --joint-model-id joint-stab-fragility-v1
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

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


def _load_instruments_for_market(
    db_manager: DatabaseManager,
    market_id: str,
    *,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[str]:
    """Return list of instrument_ids for a given market.

    Mirrors the helper in ``backfill_numeric_embeddings``.
    """

    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE market_id = %s
          AND asset_class = 'EQUITY'
          AND status = 'ACTIVE'
        ORDER BY instrument_id
    """

    params: List[object] = [market_id]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
        if offset is not None:
            sql += " OFFSET %s"
            params.append(offset)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [r[0] for r in rows]


def _load_numeric_embedding(
    db_manager: DatabaseManager,
    *,
    instrument_id: str,
    as_of_date: date,
    model_id: str,
) -> Optional[np.ndarray]:
    """Load a numeric window embedding for (instrument, date, model_id)."""

    sql = """
        SELECT vector
        FROM numeric_window_embeddings
        WHERE entity_type = 'INSTRUMENT'
          AND entity_id = %s
          AND as_of_date = %s
          AND model_id = %s
        ORDER BY embedding_id DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (instrument_id, as_of_date, model_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        return None

    (vector_bytes,) = row
    if vector_bytes is None:
        return None

    vec = np.frombuffer(vector_bytes, dtype=np.float32)
    return vec


def _load_joint_profile_embedding(
    db_manager: DatabaseManager,
    issuer_id: str,
    as_of_date: date,
    model_id: str,
) -> Optional[np.ndarray]:
    """Load joint profile embedding for an issuer/date, if present."""

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
            cursor.execute(sql, (model_id, as_of_date, issuer_id))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        return None

    (vector_bytes,) = row
    if vector_bytes is None:
        return None

    vec = np.frombuffer(vector_bytes, dtype=np.float32)
    return vec


def _load_issuer_for_instrument(
    db_manager: DatabaseManager,
    instrument_id: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """Return issuer_id for an instrument, with simple caching."""

    if instrument_id in cache:
        return cache[instrument_id]

    sql = """
        SELECT issuer_id
        FROM instruments
        WHERE instrument_id = %s
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (instrument_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        cache[instrument_id] = None
        return None

    (issuer_id,) = row
    cache[instrument_id] = str(issuer_id)
    return cache[instrument_id]


def _combine_numeric_branches(
    components: List[np.ndarray],
    weights: List[float],
) -> Optional[np.ndarray]:
    """Combine multiple numeric branches into a single embedding.

    Returns None if there are no valid components.
    """

    valid: List[Tuple[np.ndarray, float]] = []
    for comp, w in zip(components, weights, strict=True):
        if comp is None or w <= 0.0:  # type: ignore[truthy-function]
            continue
        valid.append((comp, w))

    if not valid:
        return None

    base_shape = valid[0][0].shape
    for comp, _ in valid[1:]:
        if comp.shape != base_shape:
            raise ValueError(
                f"Numeric branches have mismatched shapes: {base_shape} vs {comp.shape}"
            )

    stacked = np.stack([c for (c, _) in valid], axis=0)
    w = np.array([w for (_, w) in valid], dtype=np.float32).reshape(-1, 1)
    z = (w * stacked).sum(axis=0) / w.sum()
    return z.astype(np.float32)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint stability/fragility embeddings (STAB_FRAGILITY_V0) "
            "for instruments using num-stab-core-v1 and optional joint-profile-core-v1."
        ),
    )

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for stability state",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        default=None,
        help="Market ID to select instruments from (e.g. US_EQ)",
    )
    parser.add_argument(
        "--instrument-id",
        dest="instrument_ids",
        action="append",
        help="Explicit instrument_id to embed (can be specified multiple times)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of instruments to process when using --market-id (default: 1000)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=None,
        help="Offset into instrument list when using --market-id",
    )
    parser.add_argument(
        "--stab-model-id",
        type=str,
        default="num-stab-core-v1",
        help="Model_id for numeric stability embeddings (default: num-stab-core-v1)",
    )
    parser.add_argument(
        "--profile-joint-model-id",
        type=str,
        default="joint-profile-core-v1",
        help=(
            "Model_id for joint profile embeddings (default: joint-profile-core-v1). "
            "Set to empty string to disable profile branch."
        ),
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help="Model_id to tag joint embeddings with (default: joint-stab-fragility-v1)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=None,
        help="Optional region label to include in entity_scope (e.g. US)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    instrument_ids: List[str] = []
    if args.instrument_ids:
        instrument_ids.extend(args.instrument_ids)

    if args.market_id:
        market_instruments = _load_instruments_for_market(
            db_manager=db_manager,
            market_id=args.market_id,
            limit=args.limit,
            offset=args.offset,
        )
        instrument_ids.extend(market_instruments)

    # Deduplicate while preserving order
    seen = set()
    uniq_instruments: List[str] = []
    for inst in instrument_ids:
        if inst not in seen:
            seen.add(inst)
            uniq_instruments.append(inst)

    if not uniq_instruments:
        logger.warning("No instruments specified or found; nothing to do")
        return

    logger.info(
        "Backfilling joint STAB states: instruments=%d as_of=%s stab_model=%s profile_joint_model=%s joint_model=%s",
        len(uniq_instruments),
        args.as_of,
        args.stab_model_id,
        args.profile_joint_model_id or "<disabled>",
        args.joint_model_id,
    )

    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = IdentityNumericJointModel()
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    issuer_cache: Dict[str, Optional[str]] = {}
    examples: List[JointExample] = []

    for instrument_id in uniq_instruments:
        # Load numeric stability embedding
        z_stab = _load_numeric_embedding(
            db_manager=db_manager,
            instrument_id=instrument_id,
            as_of_date=args.as_of,
            model_id=args.stab_model_id,
        )
        if z_stab is None:
            logger.debug(
                "No num-stab-core-v1 embedding for instrument=%s as_of=%s; skipping",
                instrument_id,
                args.as_of,
            )
            continue

        # Optional profile branch via issuer_id
        z_profile = None
        issuer_id = _load_issuer_for_instrument(db_manager, instrument_id, issuer_cache)
        if issuer_id is not None and args.profile_joint_model_id:
            z_profile = _load_joint_profile_embedding(
                db_manager=db_manager,
                issuer_id=issuer_id,
                as_of_date=args.as_of,
                model_id=args.profile_joint_model_id,
            )

        z_num = _combine_numeric_branches(
            components=[z_stab, z_profile],
            weights=[1.0, 1.0],
        )
        if z_num is None:
            logger.debug(
                "No numeric branches available for instrument=%s as_of=%s; skipping",
                instrument_id,
                args.as_of,
            )
            continue

        entity_scope: Mapping[str, object] = {
            "entity_type": "INSTRUMENT",
            "entity_id": instrument_id,
            "source": "stab+profile" if z_profile is not None else "stab",
            "as_of_date": args.as_of.isoformat(),
        }
        if issuer_id is not None:
            entity_scope = {**entity_scope, "issuer_id": issuer_id}
        if args.region is not None:
            entity_scope = {**entity_scope, "region": args.region}

        ex = JointExample(
            joint_type="STAB_FRAGILITY_V0",
            as_of_date=args.as_of,
            entity_scope=entity_scope,
            numeric_embedding=z_num,
            text_embedding=None,
        )
        examples.append(ex)

    if not examples:
        logger.warning("No joint STAB examples constructed; nothing to write")
        return

    _ = service.embed_and_store(examples)
    logger.info(
        "Joint STAB backfill complete: wrote %d embeddings with model_id=%s",
        len(examples),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
