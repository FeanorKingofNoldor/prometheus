"""Backfill joint regime+macro context embeddings (numeric + macro text).

This script constructs a v0 joint space for regime context using macro
text by combining:

- Numeric regime embeddings from the ``regimes`` table, and
- Aggregated macro text embeddings from ``text_embeddings`` / ``macro_events``.

For each (region, as_of_date) where both a regime embedding and at least
one macro text embedding are available, it builds a JointExample and
embeds it using ``SimpleAverageJointModel``, storing the result in
``joint_embeddings`` with:

- ``joint_type = 'REGIME_MACRO_V0'``
- ``model_id = 'joint-regime-core-v1'`` (by default).

This is an offline/research workflow and is not part of the daily live
pipeline.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np
from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders import JointEmbeddingService, JointEmbeddingStore, JointExample
from apathis.encoders.models_joint_simple import SimpleAverageJointModel

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class RegimePoint:
    as_of_date: date
    region: str
    embedding: np.ndarray


def _load_regime_embeddings(
    db_manager: DatabaseManager,
    regions: Iterable[str],
    *,
    start_date: date,
    end_date: date,
) -> List[RegimePoint]:
    """Load regime embeddings for given regions and date range."""

    results: List[RegimePoint] = []

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for region in regions:
                cursor.execute(
                    """
                    SELECT as_of_date, regime_embedding
                    FROM regimes
                    WHERE region = %s
                      AND as_of_date BETWEEN %s AND %s
                    ORDER BY as_of_date ASC
                    """,
                    (region, start_date, end_date),
                )
                rows = cursor.fetchall()
                for as_of_date, embedding_bytes in rows:
                    if embedding_bytes is None:
                        continue
                    vec = np.frombuffer(embedding_bytes, dtype=np.float32)
                    results.append(RegimePoint(as_of_date=as_of_date, region=region, embedding=vec))
        finally:
            cursor.close()

    return results


def _load_macro_embedding_for_date(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    model_id: str,
    country: Optional[str] = None,
    event_type: Optional[str] = None,
) -> Optional[np.ndarray]:
    """Load an aggregated macro text embedding for a single as_of_date.

    This aggregates all ``text_embeddings`` rows of type ``MACRO`` with the
    given ``model_id`` whose corresponding ``macro_events.timestamp`` falls
    on ``as_of_date`` (and optionally matches ``country``/``event_type``),
    and returns their mean vector. If no such rows exist, returns None.
    """

    where_clauses = [
        "DATE(me.timestamp) = %s",
        "te.source_type = 'MACRO'",
        "te.model_id = %s",
    ]
    params: List[object] = [as_of_date, model_id]

    if country is not None:
        where_clauses.append("me.country = %s")
        params.append(country)

    if event_type is not None:
        where_clauses.append("me.event_type = %s")
        params.append(event_type)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT te.vector "
        "FROM text_embeddings te "
        "JOIN macro_events me "
        "  ON te.source_id = me.event_id::text "
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
                "Inconsistent MACRO embedding shapes for date "
                f"{as_of_date}: {v.shape} vs {first_shape}"
            )

    stacked = np.stack(vectors, axis=0)
    return stacked.mean(axis=0).astype(np.float32)


def _build_joint_examples(
    regime_points: List[RegimePoint],
    macro_by_date: Mapping[date, np.ndarray],
    *,
    country: Optional[str],
    event_type: Optional[str],
) -> List[JointExample]:
    """Build JointExample objects for dates where both branches exist."""

    examples: List[JointExample] = []
    for rp in regime_points:
        macro_vec = macro_by_date.get(rp.as_of_date)
        if macro_vec is None:
            continue

        if rp.embedding.shape != macro_vec.shape:
            raise ValueError(
                "Numeric regime embedding and macro text embedding shapes must match; "
                f"got {rp.embedding.shape} and {macro_vec.shape} for date {rp.as_of_date}"
            )

        scope: Dict[str, object] = {
            "region": rp.region,
            "source": "regime+macro",
        }
        if country is not None:
            scope["macro_country"] = country
        if event_type is not None:
            scope["macro_event_type"] = event_type

        examples.append(
            JointExample(
                joint_type="REGIME_MACRO_V0",
                as_of_date=rp.as_of_date,
                entity_scope=scope,
                numeric_embedding=rp.embedding,
                text_embedding=macro_vec,
            )
        )

    return examples


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint regime+macro context embeddings into joint_embeddings "
            "by combining regime embeddings with aggregated MACRO text embeddings."
        ),
    )

    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--as-of",
        type=_parse_date,
        help="Single as-of date (YYYY-MM-DD) to backfill joint embeddings for",
    )
    date_group.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range [START, END] (YYYY-MM-DD YYYY-MM-DD) to backfill",
    )

    parser.add_argument(
        "--region",
        dest="regions",
        action="append",
        required=True,
        help=(
            "Region to include (can be specified multiple times). "
            "Must match regions used in the regimes table."
        ),
    )
    parser.add_argument(
        "--text-model-id",
        type=str,
        default="text-macro-v1",
        help="Text embedding model_id to use (default: text-macro-v1)",
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-regime-core-v1",
        help="Joint embedding model_id to tag outputs with (default: joint-regime-core-v1)",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Optional country filter for macro_events.country",
    )
    parser.add_argument(
        "--event-type",
        type=str,
        default=None,
        help="Optional macro event_type filter (e.g. FOMC)",
    )

    args = parser.parse_args(argv)

    if args.date_range is not None:
        start = _parse_date(args.date_range[0])
        end = _parse_date(args.date_range[1])
        if end < start:
            parser.error("date-range END must be >= START")
        start_date: date = start
        end_date: date = end
    else:
        start_date = end_date = args.as_of

    config = get_config()
    db_manager = DatabaseManager(config)

    regions: List[str] = args.regions

    logger.info(
        "Loading regime embeddings for regions=%s start=%s end=%s",
        regions,
        start_date,
        end_date,
    )

    regime_points = _load_regime_embeddings(
        db_manager=db_manager,
        regions=regions,
        start_date=start_date,
        end_date=end_date,
    )

    if not regime_points:
        logger.warning("No regime embeddings found for the given filters; nothing to do")
        return

    # Collect unique dates to query macro embeddings for.
    dates = sorted({rp.as_of_date for rp in regime_points})

    macro_by_date: Dict[date, np.ndarray] = {}
    for d in dates:
        macro_vec = _load_macro_embedding_for_date(
            db_manager=db_manager,
            as_of_date=d,
            model_id=args.text_model_id,
            country=args.country,
            event_type=args.event_type,
        )
        if macro_vec is not None:
            macro_by_date[d] = macro_vec

    if not macro_by_date:
        logger.warning("No MACRO text embeddings found for the given filters; nothing to do")
        return

    examples = _build_joint_examples(
        regime_points=regime_points,
        macro_by_date=macro_by_date,
        country=args.country,
        event_type=args.event_type,
    )
    if not examples:
        logger.warning(
            "No joint examples constructed (no overlapping regime+MACRO dates); nothing to write",
        )
        return

    store = JointEmbeddingStore(db_manager=db_manager)
    joint_model = SimpleAverageJointModel(numeric_weight=0.5, text_weight=0.5)
    service = JointEmbeddingService(model=joint_model, store=store, model_id=args.joint_model_id)

    _ = service.embed_and_store(examples)

    logger.info(
        "Joint regime+macro backfill complete: wrote %d embeddings with model_id=%s",
        len(examples),
        args.joint_model_id,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
