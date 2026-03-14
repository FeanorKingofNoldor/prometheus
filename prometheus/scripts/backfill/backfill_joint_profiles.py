"""Backfill joint profile embeddings (joint-profile-core-v1).

This script builds joint profile embeddings for issuers by combining:

- Numeric profile embeddings (e.g. `num-profile-core-v1`).
- Behaviour embeddings based on numeric regime windows (e.g.
  `num-regime-core-v1`).
- Profile text embeddings (e.g. `text-profile-v1`).

The numeric branches are combined into a single numeric embedding, which
is then fused with the text branch via ``SimpleAverageJointModel`` into
an `R^384` joint profile space.

Embeddings are written to ``historical_db.joint_embeddings`` with:

- `joint_type = 'PROFILE_CORE_V0'`.
- `model_id = 'joint-profile-core-v1'` (by default).

Examples
--------

    # Backfill joint profiles for all issuers with profiles/text/embeddings
    # on a single date
    python -m prometheus.scripts.backfill_joint_profiles \
        --as-of 2025-01-31 \
        --numeric-profile-model-id num-profile-core-v1 \
        --behaviour-model-id num-regime-core-v1 \
        --text-model-id text-profile-v1 \
        --joint-model-id joint-profile-core-v1
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
from apathis.encoders.models_joint_simple import SimpleAverageJointModel


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _profiles_table_exists(db_manager: DatabaseManager) -> bool:
    """Return True if the `profiles` table exists in the runtime DB."""

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'profiles'
                )
                """
            )
            (exists,) = cursor.fetchone()
        finally:
            cursor.close()

    return bool(exists)


def _load_profile_keys(
    db_manager: DatabaseManager,
    *,
    as_of: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    issuer_filter: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> List[Tuple[str, date]]:
    """Load (issuer_id, as_of_date) pairs from `profiles`.

    The profiles table uses (issuer_id, as_of_date) as its natural key.
    """

    where_clauses: List[str] = []
    params: List[object] = []

    if as_of is not None:
        where_clauses.append("as_of_date = %s")
        params.append(as_of)
    else:
        if start_date is not None:
            where_clauses.append("as_of_date >= %s")
            params.append(start_date)
        if end_date is not None:
            where_clauses.append("as_of_date <= %s")
            params.append(end_date)

    if issuer_filter is not None:
        issuer_list = list(issuer_filter)
        if issuer_list:
            where_clauses.append("issuer_id = ANY(%s)")
            params.append(issuer_list)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT issuer_id, as_of_date "
        "FROM profiles" + where_sql + " ORDER BY as_of_date ASC, issuer_id ASC"
    )

    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    results: List[Tuple[str, date]] = []
    for issuer_id, as_of_date_db in rows:
        results.append((str(issuer_id), as_of_date_db))
    return results


def _load_representative_instrument(
    db_manager: DatabaseManager,
    issuer_id: str,
    cache: Dict[str, Optional[str]],
) -> Optional[str]:
    """Return a representative instrument_id for an issuer.

    This mirrors the simple logic in ProfileFeatureBuilder: choose the
    lexicographically smallest instrument for the issuer.
    """

    if issuer_id in cache:
        return cache[issuer_id]

    sql = """
        SELECT instrument_id
        FROM instruments
        WHERE issuer_id = %s
        ORDER BY instrument_id ASC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (issuer_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        cache[issuer_id] = None
        return None

    (instrument_id,) = row
    cache[issuer_id] = str(instrument_id)
    return cache[issuer_id]


def _load_text_profile_embedding(
    db_manager: DatabaseManager,
    issuer_id: str,
    as_of_date: date,
    model_id: str,
) -> Optional[np.ndarray]:
    """Load text profile embedding from text_embeddings.

    Uses the same source_id convention as `backfill_profile_text_embeddings`:
    "{issuer_id}:{as_of_date}".
    """

    source_id = f"{issuer_id}:{as_of_date.isoformat()}"

    sql = """
        SELECT vector
        FROM text_embeddings
        WHERE source_type = 'PROFILE'
          AND source_id = %s
          AND model_id = %s
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (source_id, model_id))
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
            "Backfill joint profile embeddings (PROFILE_CORE_V0) into joint_embeddings "
            "by combining numeric profile, behaviour, and text branches."
        ),
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--as-of",
        type=_parse_date,
        help="Single as-of date (YYYY-MM-DD) for which to embed profiles",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range [START, END] (YYYY-MM-DD YYYY-MM-DD) to embed",
    )

    parser.add_argument(
        "--issuer-id",
        dest="issuer_ids",
        action="append",
        help="Optional issuer_id to restrict to (can be repeated)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Maximum number of profiles to process (default: 10,000)",
    )
    parser.add_argument(
        "--numeric-profile-model-id",
        type=str,
        default="num-profile-core-v1",
        help="Model_id for numeric profile embeddings (default: num-profile-core-v1)",
    )
    parser.add_argument(
        "--behaviour-model-id",
        type=str,
        default="num-regime-core-v1",
        help=(
            "Model_id for behaviour/Regime numeric embeddings (default: num-regime-core-v1). "
            "Set to empty string to disable this branch."
        ),
    )
    parser.add_argument(
        "--text-model-id",
        type=str,
        default="text-profile-v1",
        help="Model_id for text profile embeddings (default: text-profile-v1)",
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-profile-core-v1",
        help="Model_id to tag joint embeddings with (default: joint-profile-core-v1)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    if args.date_range is not None:
        start = _parse_date(args.date_range[0])
        end = _parse_date(args.date_range[1])
        if end < start:
            parser.error("date-range END must be >= START")
        as_of: Optional[date] = None
        start_date: Optional[date] = start
        end_date: Optional[date] = end
    else:
        as_of = args.as_of
        start_date = None
        end_date = None

    issuer_filter: Optional[Iterable[str]] = args.issuer_ids

    config = get_config()
    db_manager = DatabaseManager(config)

    if not _profiles_table_exists(db_manager):
        logger.warning("profiles table does not exist; run migration 0007 to enable joint profiles backfill")
        return

    profile_keys = _load_profile_keys(
        db_manager=db_manager,
        as_of=as_of,
        start_date=start_date,
        end_date=end_date,
        issuer_filter=issuer_filter,
        limit=args.limit,
    )

    if not profile_keys:
        logger.warning("No profile rows found for the given filters; nothing to do")
        return

    logger.info(
        "Backfilling joint profiles: n_profiles=%d numeric_profile_model=%s behaviour_model=%s text_model=%s joint_model=%s",
        len(profile_keys),
        args.numeric_profile_model_id,
        args.behaviour_model_id or "<disabled>",
        args.text_model_id,
        args.joint_model_id,
    )

    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = SimpleAverageJointModel(numeric_weight=0.5, text_weight=0.5)
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    instrument_cache: Dict[str, Optional[str]] = {}
    examples_batch: List[JointExample] = []

    for issuer_id, as_of_date in profile_keys:
        instrument_id = _load_representative_instrument(db_manager, issuer_id, instrument_cache)
        if instrument_id is None:
            logger.debug("No instrument for issuer %s; skipping", issuer_id)
            continue

        # Text branch
        z_text = _load_text_profile_embedding(
            db_manager=db_manager,
            issuer_id=issuer_id,
            as_of_date=as_of_date,
            model_id=args.text_model_id,
        )
        if z_text is None:
            logger.debug("No text PROFILE embedding for issuer=%s as_of=%s; skipping", issuer_id, as_of_date)
            continue

        # Numeric profile branch
        z_num_profile = _load_numeric_embedding(
            db_manager=db_manager,
            instrument_id=instrument_id,
            as_of_date=as_of_date,
            model_id=args.numeric_profile_model_id,
        )

        # Behaviour branch (optional)
        z_behaviour = None
        if args.behaviour_model_id:
            z_behaviour = _load_numeric_embedding(
                db_manager=db_manager,
                instrument_id=instrument_id,
                as_of_date=as_of_date,
                model_id=args.behaviour_model_id,
            )

        z_num = _combine_numeric_branches(
            components=[z_num_profile, z_behaviour],
            weights=[1.0, 1.0],
        )
        if z_num is None:
            logger.debug(
                "No numeric branches available for issuer=%s as_of=%s; skipping", issuer_id, as_of_date
            )
            continue

        if z_num.shape != z_text.shape:
            logger.warning(
                "Numeric/text profile embeddings have mismatched shapes for issuer=%s as_of=%s: %s vs %s; skipping",
                issuer_id,
                as_of_date,
                z_num.shape,
                z_text.shape,
            )
            continue

        entity_scope: Mapping[str, object] = {
            "entity_type": "ISSUER",
            "issuer_id": issuer_id,
            "instrument_id": instrument_id,
            "source": "profile+regime+text",
            "as_of_date": as_of_date.isoformat(),
        }

        ex = JointExample(
            joint_type="PROFILE_CORE_V0",
            as_of_date=as_of_date,
            entity_scope=entity_scope,
            numeric_embedding=z_num,
            text_embedding=z_text,
        )
        examples_batch.append(ex)

    if not examples_batch:
        logger.warning("No joint examples constructed; nothing to write")
        return

    _ = service.embed_and_store(examples_batch)
    logger.info(
        "Joint profile backfill complete: wrote %d embeddings with model_id=%s",
        len(examples_batch),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
