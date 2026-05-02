"""Prometheus v2: Backfill joint issuer×day context embeddings.

This script constructs a v0 joint space for **issuer×day contexts** by combining:

- Numeric issuer/instrument behaviour embeddings from `numeric_window_embeddings`
  with `model_id = 'num-regime-core-v1'`.
- Issuer×day NEWS embeddings from `text_embeddings` with
  `source_type = 'NEWS_ISSUER_DAY'` and `model_id = 'text-fin-general-v1'`.

For each `(issuer_id, as_of_date)` where both branches are available, it builds
`JointExample`s and embeds them using `SimpleAverageJointModel`, storing the
result in `joint_embeddings` with `model_id = 'joint-issuer-day-core-v1'` and
`joint_type = 'ISSUER_DAY_V0'`.

This is an offline/research workflow and is not part of the daily live
pipeline.

Key responsibilities:
- Discover primary instruments per issuer from `runtime_db.instruments`.
- Load numeric embeddings for those instruments from `numeric_window_embeddings`.
- Load issuer×day NEWS embeddings from `text_embeddings`.
- Build joint embeddings via `SimpleAverageJointModel` and persist them.

External dependencies:
- numpy: numeric array handling.

Database tables accessed:
- runtime_db.instruments (Read): issuer↔instrument mapping.
- historical_db.numeric_window_embeddings (Read): numeric regime embeddings.
- historical_db.text_embeddings (Read): NEWS_ISSUER_DAY embeddings.
- historical_db.joint_embeddings (Write): joint issuer×day embeddings.

Thread safety: Not thread-safe (offline CLI, single process).

Author: Prometheus Team
Created: 2025-12-10
Last Modified: 2025-12-10
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.encoders import (
    JointEmbeddingService,
    JointEmbeddingStore,
    JointExample,
)
from apatheon.encoders.models_joint_simple import SimpleAverageJointModel
from numpy.typing import NDArray

logger = get_logger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass(frozen=True)
class IssuerNumericPoint:
    """Numeric branch state for a single issuer×day.

    Attributes:
        issuer_id: Issuer identifier.
        instrument_id: Primary instrument used for numeric embedding.
        as_of_date: Date for which the numeric embedding is defined.
        embedding: Numeric regime embedding vector (num-regime-core-v1).
    """

    issuer_id: str
    instrument_id: str
    as_of_date: date
    embedding: NDArray[np.float_]


@dataclass(frozen=True)
class IssuerNewsPoint:
    """NEWS branch state for a single issuer×day.

    Attributes:
        issuer_id: Issuer identifier.
        as_of_date: Date for which NEWS_ISSUER_DAY embedding exists.
        embedding: Issuer×day NEWS embedding vector.
    """

    issuer_id: str
    as_of_date: date
    embedding: NDArray[np.float_]


# ============================================================================
# Helpers
# ============================================================================


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string into a :class:`date`."""

    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}, expected YYYY-MM-DD",
        ) from exc


def _load_primary_instruments(
    db_manager: DatabaseManager,
    *,
    market_id: Optional[str] = None,
) -> Dict[str, str]:
    """Load primary instrument per issuer from runtime_db.instruments.

    v0 heuristic:

    - Filter to `asset_class = 'EQUITY'` and `status = 'ACTIVE'`.
    - Optionally restrict to a given `market_id`.
    - For each issuer_id, choose the first instrument ordered by instrument_id
      as the primary.

    Returns:
        Mapping issuer_id → primary instrument_id.

    TODO(prometheus, 2025-12-10): Add a more explicit notion of primary
    instrument in the schema or config instead of this heuristic.
    """

    where_clauses = ["asset_class = 'EQUITY'", "status = 'ACTIVE'"]
    params: List[object] = []

    if market_id is not None:
        where_clauses.append("market_id = %s")
        params.append(market_id)

    where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT issuer_id, instrument_id "
        "FROM instruments "
        + where_sql +
        " ORDER BY issuer_id, instrument_id"
    )

    mapping: Dict[str, str] = {}

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            for issuer_id, instrument_id in cursor.fetchall():
                issuer_id_str = str(issuer_id)
                if issuer_id_str in mapping:
                    continue
                mapping[issuer_id_str] = str(instrument_id)
        finally:
            cursor.close()

    logger.info(
        "Loaded %d primary instruments for market_id=%s",
        len(mapping),
        market_id,
    )
    return mapping


def _load_numeric_embeddings(
    db_manager: DatabaseManager,
    *,
    primary_instruments: Mapping[str, str],
    start_date: date,
    end_date: date,
    numeric_model_id: str,
) -> List[IssuerNumericPoint]:
    """Load numeric issuer×day embeddings from numeric_window_embeddings."""

    sql = """
        SELECT
            entity_id,
            as_of_date,
            vector
        FROM numeric_window_embeddings
        WHERE entity_type = 'INSTRUMENT'
          AND model_id = %s
          AND as_of_date BETWEEN %s AND %s
          AND entity_id = ANY(%s)
        ORDER BY entity_id, as_of_date
    """

    instrument_ids = list({inst for inst in primary_instruments.values()})
    if not instrument_ids:
        logger.warning("No primary instruments supplied; skipping numeric load")
        return []

    params = (numeric_model_id, start_date, end_date, instrument_ids)

    results: List[IssuerNumericPoint] = []
    first_shape: Optional[Tuple[int, ...]] = None

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            for entity_id, as_of_date, vec_bytes in cursor.fetchall():
                if vec_bytes is None:
                    continue
                vec = np.frombuffer(vec_bytes, dtype=np.float32)
                if first_shape is None:
                    first_shape = vec.shape
                elif vec.shape != first_shape:
                    raise ValueError(
                        "Inconsistent numeric embedding shapes in numeric_window_embeddings: "
                        f"got {vec.shape} vs {first_shape}",
                    )
                instrument_id = str(entity_id)
                # Map back to issuer via primary_instruments.
                issuer_ids = [iss for iss, inst in primary_instruments.items() if inst == instrument_id]
                for issuer_id in issuer_ids:
                    results.append(
                        IssuerNumericPoint(
                            issuer_id=issuer_id,
                            instrument_id=instrument_id,
                            as_of_date=as_of_date,
                            embedding=vec,
                        ),
                    )
        finally:
            cursor.close()

    logger.info(
        "Loaded %d numeric issuer×day points for %s→%s (model_id=%s)",
        len(results),
        start_date,
        end_date,
        numeric_model_id,
    )
    return results


def _load_news_embeddings(
    db_manager: DatabaseManager,
    *,
    start_date: date,
    end_date: date,
    news_model_id: str,
    source_type: str,
) -> List[IssuerNewsPoint]:
    """Load issuer×day NEWS embeddings from text_embeddings."""

    sql = """
        SELECT
            source_id,
            vector
        FROM text_embeddings
        WHERE source_type = %s
          AND model_id = %s
    """

    params = (source_type, news_model_id)

    results: List[IssuerNewsPoint] = []
    first_shape: Optional[Tuple[int, ...]] = None

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            for source_id, vec_bytes in cursor.fetchall():
                if vec_bytes is None or source_id is None:
                    continue
                # source_id is expected to be "issuer_id:YYYY-MM-DD" for NEWS_ISSUER_DAY.
                try:
                    issuer_id_str, as_of_str = str(source_id).split(":", 1)
                    as_of_date = date.fromisoformat(as_of_str)
                except Exception:
                    continue
                if as_of_date < start_date or as_of_date > end_date:
                    continue
                vec = np.frombuffer(vec_bytes, dtype=np.float32)
                if first_shape is None:
                    first_shape = vec.shape
                elif vec.shape != first_shape:
                    raise ValueError(
                        "Inconsistent NEWS_ISSUER_DAY embedding shapes: "
                        f"got {vec.shape} vs {first_shape}",
                    )
                results.append(
                    IssuerNewsPoint(
                        issuer_id=issuer_id_str,
                        as_of_date=as_of_date,
                        embedding=vec,
                    ),
                )
        finally:
            cursor.close()

    logger.info(
        "Loaded %d issuer×day NEWS points for %s→%s (model_id=%s, source_type=%s)",
        len(results),
        start_date,
        end_date,
        news_model_id,
        source_type,
    )
    return results


def _build_joint_examples(
    numeric_points: Iterable[IssuerNumericPoint],
    news_by_key: Mapping[Tuple[str, date], IssuerNewsPoint],
    *,
    joint_type: str,
    market_id: Optional[str],
) -> List[JointExample]:
    """Build JointExample objects for issuer×days where both branches exist."""

    examples: List[JointExample] = []

    for np_point in numeric_points:
        key = (np_point.issuer_id, np_point.as_of_date)
        news_point = news_by_key.get(key)
        if news_point is None:
            continue

        if np_point.embedding.shape != news_point.embedding.shape:
            raise ValueError(
                "Numeric and NEWS embeddings must have the same shape; "
                f"got {np_point.embedding.shape} and {news_point.embedding.shape} "
                f"for issuer_id={np_point.issuer_id} date={np_point.as_of_date}",
            )

        examples.append(
            JointExample(
                joint_type=joint_type,
                as_of_date=np_point.as_of_date,
                entity_scope={
                    "issuer_id": np_point.issuer_id,
                    "primary_instrument_id": np_point.instrument_id,
                    "market_id": market_id,
                    "source": "num-regime-core-v1 + NEWS_ISSUER_DAY",
                },
                numeric_embedding=np_point.embedding,
                text_embedding=news_point.embedding,
            ),
        )

    return examples


# ============================================================================
# CLI
# ============================================================================


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Backfill joint issuer×day context embeddings into joint_embeddings."""

    parser = argparse.ArgumentParser(
        description=(
            "Backfill joint issuer×day context embeddings into joint_embeddings "
            "by combining numeric regime embeddings with NEWS_ISSUER_DAY vectors.",
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
        "--market-id",
        type=str,
        default="US_EQ",
        help="Market id to restrict issuers/instruments to (default: US_EQ)",
    )
    parser.add_argument(
        "--numeric-model-id",
        type=str,
        default="num-regime-core-v1",
        help="Numeric embedding model_id to use (default: num-regime-core-v1)",
    )
    parser.add_argument(
        "--news-model-id",
        type=str,
        default="text-fin-general-v1",
        help="Text embedding model_id to use (default: text-fin-general-v1)",
    )
    parser.add_argument(
        "--news-source-type",
        type=str,
        default="NEWS_ISSUER_DAY",
        help="source_type for issuer×day news embeddings (default: NEWS_ISSUER_DAY)",
    )
    parser.add_argument(
        "--joint-model-id",
        type=str,
        default="joint-issuer-day-core-v1",
        help="Joint embedding model_id to tag outputs with (default: joint-issuer-day-core-v1)",
    )
    parser.add_argument(
        "--joint-type",
        type=str,
        default="ISSUER_DAY_V0",
        help="joint_type to use in joint_embeddings (default: ISSUER_DAY_V0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Build joint examples and log counts, but do not write to "
            "joint_embeddings.",
        ),
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

    logger.info(
        "Loading primary instruments for market_id=%s", args.market_id,
    )
    primary_instruments = _load_primary_instruments(
        db_manager=db_manager,
        market_id=args.market_id,
    )
    if not primary_instruments:
        logger.warning("No primary instruments found; nothing to do")
        return

    logger.info(
        "Loading numeric issuer×day embeddings for %s→%s (model_id=%s)",
        start_date,
        end_date,
        args.numeric_model_id,
    )
    numeric_points = _load_numeric_embeddings(
        db_manager=db_manager,
        primary_instruments=primary_instruments,
        start_date=start_date,
        end_date=end_date,
        numeric_model_id=args.numeric_model_id,
    )
    if not numeric_points:
        logger.warning("No numeric issuer×day points loaded; nothing to do")
        return

    logger.info(
        "Loading issuer×day NEWS embeddings for %s→%s (model_id=%s, source_type=%s)",
        start_date,
        end_date,
        args.news_model_id,
        args.news_source_type,
    )
    news_points = _load_news_embeddings(
        db_manager=db_manager,
        start_date=start_date,
        end_date=end_date,
        news_model_id=args.news_model_id,
        source_type=args.news_source_type,
    )
    if not news_points:
        logger.warning("No issuer×day NEWS points loaded; nothing to do")
        return

    news_by_key: Dict[Tuple[str, date], IssuerNewsPoint] = {
        (p.issuer_id, p.as_of_date): p for p in news_points
    }

    examples = _build_joint_examples(
        numeric_points,
        news_by_key=news_by_key,
        joint_type=args.joint_type,
        market_id=args.market_id,
    )

    if not examples:
        logger.warning(
            "No joint issuer×day examples constructed (numeric/news mismatch?); nothing to do",
        )
        return

    logger.info(
        "Prepared %d joint issuer×day examples with joint_type=%s joint_model_id=%s",
        len(examples),
        args.joint_type,
        args.joint_model_id,
    )

    if args.dry_run:
        logger.info("DRY RUN enabled - not writing joint embeddings to joint_embeddings")
        return

    store = JointEmbeddingStore(db_manager=db_manager)
    model = SimpleAverageJointModel()
    service = JointEmbeddingService(model=model, store=store, model_id=args.joint_model_id)

    _ = service.embed_and_store(examples)

    logger.info(
        "Joint issuer×day context backfill complete: wrote %d embeddings to joint_embeddings",
        len(examples),
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
