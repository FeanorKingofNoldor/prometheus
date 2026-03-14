"""Backfill joint Assessment context embeddings (joint-assessment-context-v1).

This script builds joint Assessment context embeddings for instruments by
combining up to four branches:

- Profile joint embeddings (PROFILE_CORE_V0 / joint-profile-core-v1).
- Regime joint context embeddings (REGIME_CONTEXT_V0 / joint-regime-core-v1).
- Stability/fragility joint embeddings (STAB_FRAGILITY_V0 / joint-stab-fragility-v1).
- Recent NEWS text context (text-fin-general-v1) aggregated over a
  configurable look-back window.

All branches are 384-dim vectors. For each instrument and as_of date the
script:

1. Loads available branch vectors.
2. Combines them into a single 384-dim context vector via a weighted
   average (skipping missing/zero-weight branches).
3. Stores the result in historical_db.joint_embeddings with:

   - joint_type = 'ASSESSMENT_CTX_V0'.
   - model_id   = 'joint-assessment-context-v1' (by default).

The combination logic lives in this script; the joint model used by
JointEmbeddingService is an IdentityNumericJointModel that simply
passes the combined numeric embedding through.

Examples
--------

    # Backfill Assessment context for active US_EQ names on a date
    python -m prometheus.scripts.backfill_joint_assessment_context \
        --as-of 2025-01-31 \
        --market-id US_EQ \
        --region US \
        --profile-joint-model-id joint-profile-core-v1 \
        --regime-joint-model-id joint-regime-core-v1 \
        --stab-joint-model-id joint-stab-fragility-v1 \
        --text-model-id text-fin-general-v1 \
        --text-window-days 7 \
        --joint-model-id joint-assessment-context-v1
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
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

    Mirrors the helper in ``backfill_numeric_embeddings`` and
    ``backfill_joint_stab_fragility_states``.
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

    return [str(r[0]) for r in rows]


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


def _load_joint_stab_embedding(
    db_manager: DatabaseManager,
    instrument_id: str,
    as_of_date: date,
    model_id: str,
) -> Optional[np.ndarray]:
    """Load joint stability/fragility embedding for an instrument/date, if present."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'STAB_FRAGILITY_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'entity_id') = %s
        ORDER BY joint_id DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of_date, instrument_id))
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


def _load_regime_joint_embedding(
    db_manager: DatabaseManager,
    region: str,
    as_of_date: date,
    model_id: str,
) -> Optional[np.ndarray]:
    """Load joint regime context embedding for a region/date, if present."""

    sql = """
        SELECT vector
        FROM joint_embeddings
        WHERE joint_type = 'REGIME_CONTEXT_V0'
          AND model_id = %s
          AND as_of_date = %s
          AND (entity_scope->>'region') = %s
        ORDER BY joint_id DESC
        LIMIT 1
    """

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (model_id, as_of_date, region))
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


def _load_recent_text_embedding(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    model_id: str,
    window_days: int,
    language: Optional[str] = None,
) -> Optional[np.ndarray]:
    """Load aggregated recent NEWS text embedding for a look-back window.

    Aggregates all text_embeddings rows of type 'NEWS' with the given
    model_id whose corresponding news_articles.published_at falls in the
    window [as_of_date - window_days + 1, as_of_date]. Returns the mean
    vector or None if no rows match.
    """

    if window_days <= 0:
        return None

    start_date = as_of_date - timedelta(days=window_days - 1)

    where_clauses = [
        "DATE(na.published_at) BETWEEN %s AND %s",
        "te.source_type = 'NEWS'",
        "te.model_id = %s",
    ]
    params: List[object] = [start_date, as_of_date, model_id]

    if language is not None:
        where_clauses.append("na.language = %s")
        params.append(language)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT te.vector "
        "FROM text_embeddings te "
        "JOIN news_articles na "
        "  ON te.source_id = na.article_id::text "
        + where_sql
    )

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        return None

    vectors = [np.frombuffer(row[0], dtype=np.float32) for row in rows]
    first_shape = vectors[0].shape
    for v in vectors[1:]:
        if v.shape != first_shape:
            raise ValueError(
                "Inconsistent text embedding shapes in recent window "
                f"for as_of_date {as_of_date}: {v.shape} vs {first_shape}"
            )

    stacked = np.stack(vectors, axis=0)
    return stacked.mean(axis=0).astype(np.float32)


def _combine_branches(
    components: List[Optional[np.ndarray]],
    weights: List[float],
) -> Optional[np.ndarray]:
    """Combine multiple context branches into a single embedding.

    Components with None value or non-positive weight are skipped. If no
    valid components remain, returns None. Otherwise a weighted average is
    computed and returned as float32.
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
                f"Context branches have mismatched shapes: {base_shape} vs {comp.shape}"
            )

    stacked = np.stack([c for (c, _) in valid], axis=0)
    w = np.array([w for (_, w) in valid], dtype=np.float32).reshape(-1, 1)
    z = (w * stacked).sum(axis=0) / w.sum()
    return z.astype(np.float32)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint Assessment context embeddings (ASSESSMENT_CTX_V0) "
            "for instruments by combining profile, regime, STAB, and text branches."
        ),
    )

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date (YYYY-MM-DD) for Assessment context snapshot",
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
        "--region",
        type=str,
        default=None,
        help="Optional region label for regime branch and entity_scope (e.g. US)",
    )

    parser.add_argument(
        "--profile-joint-model-id",
        type=str,
        default="joint-profile-core-v1",
        help="Model_id for joint profile embeddings (default: joint-profile-core-v1)",
    )
    parser.add_argument(
        "--regime-joint-model-id",
        type=str,
        default="joint-regime-core-v1",
        help="Model_id for joint regime context embeddings (default: joint-regime-core-v1)",
    )
    parser.add_argument(
        "--stab-joint-model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help="Model_id for joint STAB embeddings (default: joint-stab-fragility-v1)",
    )
    parser.add_argument(
        "--text-model-id",
        type=str,
        default="text-fin-general-v1",
        help="Model_id for NEWS text embeddings (default: text-fin-general-v1)",
    )
    parser.add_argument(
        "--text-window-days",
        type=int,
        default=7,
        help="Look-back window in days for recent text context (default: 7)",
    )

    parser.add_argument(
        "--w-profile",
        type=float,
        default=1.0,
        help="Weight for profile branch in numeric combination (default: 1.0)",
    )
    parser.add_argument(
        "--w-regime",
        type=float,
        default=1.0,
        help="Weight for regime branch in numeric combination (default: 1.0)",
    )
    parser.add_argument(
        "--w-stab",
        type=float,
        default=1.0,
        help="Weight for STAB branch in numeric combination (default: 1.0)",
    )
    parser.add_argument(
        "--w-text",
        type=float,
        default=1.0,
        help="Weight for text branch in numeric combination (default: 1.0)",
    )

    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-assessment-context-v1",
        help="Model_id to tag joint embeddings with (default: joint-assessment-context-v1)",
    )
    parser.add_argument(
        "--language",
        type=str,
        default=None,
        help="Optional language filter for NEWS text branch (news_articles.language)",
    )

    args = parser.parse_args(argv)

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    if args.text_window_days <= 0:
        parser.error("--text-window-days must be positive")

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
        "Backfilling joint Assessment context: instruments=%d as_of=%s profile_joint=%s regime_joint=%s stab_joint=%s text_model=%s joint_model=%s",
        len(uniq_instruments),
        args.as_of,
        args.profile_joint_model_id or "<disabled>",
        args.regime_joint_model_id or "<disabled>",
        args.stab_joint_model_id or "<disabled>",
        args.text_model_id or "<disabled>",
        args.joint_model_id,
    )

    # Preload region-level regime and text branches, if enabled.
    z_regime_ctx: Optional[np.ndarray] = None
    if args.w_regime > 0.0 and args.regime_joint_model_id:
        if args.region is None:
            logger.warning("w-regime > 0 but --region not provided; disabling regime branch")
        else:
            z_regime_ctx = _load_regime_joint_embedding(
                db_manager=db_manager,
                region=args.region,
                as_of_date=args.as_of,
                model_id=args.regime_joint_model_id,
            )
            if z_regime_ctx is None:
                logger.warning(
                    "No regime joint embedding found for region=%s as_of=%s model_id=%s; regime branch disabled",
                    args.region,
                    args.as_of,
                    args.regime_joint_model_id,
                )

    z_text_recent: Optional[np.ndarray] = None
    if args.w_text > 0.0 and args.text_model_id:
        z_text_recent = _load_recent_text_embedding(
            db_manager=db_manager,
            as_of_date=args.as_of,
            model_id=args.text_model_id,
            window_days=args.text_window_days,
            language=args.language,
        )
        if z_text_recent is None:
            logger.warning(
                "No recent NEWS text embeddings found for window ending %s; text branch disabled",
                args.as_of,
            )

    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = IdentityNumericJointModel()
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    issuer_cache: Dict[str, Optional[str]] = {}
    examples: List[JointExample] = []

    for instrument_id in uniq_instruments:
        issuer_id = _load_issuer_for_instrument(db_manager, instrument_id, issuer_cache)

        z_profile: Optional[np.ndarray] = None
        if issuer_id is not None and args.profile_joint_model_id and args.w_profile > 0.0:
            z_profile = _load_joint_profile_embedding(
                db_manager=db_manager,
                issuer_id=issuer_id,
                as_of_date=args.as_of,
                model_id=args.profile_joint_model_id,
            )

        z_stab: Optional[np.ndarray] = None
        if args.stab_joint_model_id and args.w_stab > 0.0:
            z_stab = _load_joint_stab_embedding(
                db_manager=db_manager,
                instrument_id=instrument_id,
                as_of_date=args.as_of,
                model_id=args.stab_joint_model_id,
            )

        z_assessment = _combine_branches(
            components=[z_profile, z_regime_ctx, z_stab, z_text_recent],
            weights=[args.w_profile, args.w_regime, args.w_stab, args.w_text],
        )
        if z_assessment is None:
            logger.debug(
                "No context branches available for instrument=%s as_of=%s; skipping",
                instrument_id,
                args.as_of,
            )
            continue

        source_parts: List[str] = []
        if z_profile is not None and args.w_profile > 0.0:
            source_parts.append("profile")
        if z_regime_ctx is not None and args.w_regime > 0.0:
            source_parts.append("regime")
        if z_stab is not None and args.w_stab > 0.0:
            source_parts.append("stab")
        if z_text_recent is not None and args.w_text > 0.0:
            source_parts.append("text")

        entity_scope: Mapping[str, object] = {
            "entity_type": "INSTRUMENT",
            "entity_id": instrument_id,
            "as_of_date": args.as_of.isoformat(),
            "source": "+".join(source_parts) if source_parts else "<none>",
        }
        if issuer_id is not None:
            entity_scope = {**entity_scope, "issuer_id": issuer_id}
        if args.region is not None:
            entity_scope = {**entity_scope, "region": args.region}

        ex = JointExample(
            joint_type="ASSESSMENT_CTX_V0",
            as_of_date=args.as_of,
            entity_scope=entity_scope,
            numeric_embedding=z_assessment,
            text_embedding=None,
        )
        examples.append(ex)

    if not examples:
        logger.warning("No joint Assessment context examples constructed; nothing to write")
        return

    _ = service.embed_and_store(examples)
    logger.info(
        "Joint Assessment context backfill complete: wrote %d embeddings with model_id=%s",
        len(examples),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
