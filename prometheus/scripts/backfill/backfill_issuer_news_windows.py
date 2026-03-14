"""Prometheus v2: Backfill issuer×day NEWS window embeddings

This module backfills **rolling NEWS window embeddings** for issuers into the
`historical_db.text_embeddings` table.

It takes precomputed issuer×day NEWS embeddings (with
`source_type = 'NEWS_ISSUER_DAY'`) and, for each `(issuer_id, as_of_date)` in a
requested range, aggregates a rolling window of length `window_days` to produce
`NEWS_ISSUER_WINDOW` embeddings. These provide a medium-horizon news context
vector per issuer×day suitable for profiles, risk models, and joint spaces.

Key responsibilities:
- Load issuer×day NEWS embeddings via the `issuer_news_daily` view and
  `text_embeddings` table.
- For each issuer×day, build a rolling window over the previous `window_days`
  calendar days and compute a mean embedding when coverage is sufficient.
- Persist window embeddings back into `text_embeddings` with
  `source_type = 'NEWS_ISSUER_WINDOW'` and a structured `source_id`.

External dependencies:
- numpy: Vector arithmetic for embedding aggregation.

Database tables accessed:
- historical_db.issuer_news_daily (Read): issuer×day news metadata.
- historical_db.text_embeddings (Read/Write): NEWS_ISSUER_DAY inputs and
  NEWS_ISSUER_WINDOW outputs.

Thread safety: Not thread-safe (intended for offline CLI use, single process).

Author: Prometheus Team
Created: 2025-12-10
Last Modified: 2025-12-10
Status: Development
Version: v0.1.0
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
from numpy.typing import NDArray

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.encoders.text import TextDoc, TextEmbeddingStore


logger = get_logger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass(frozen=True)
class IssuerDayEmbedding:
    """Single issuer×day NEWS embedding.

    Attributes:
        issuer_id: Issuer identifier from runtime DB.
        news_date: Date of the issuer×day NEWS embedding.
        vector:   NEWS_ISSUER_DAY embedding decoded as float32 array.
    """

    issuer_id: str
    news_date: date
    vector: NDArray[np.float_]


# ============================================================================
# Helpers
# ============================================================================


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD date string into a :class:`date`.

    Args:
        value: String in YYYY-MM-DD format.

    Returns:
        Parsed :class:`date` instance.

    Raises:
        argparse.ArgumentTypeError: If the value is not a valid date.
    """

    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}, expected YYYY-MM-DD",
        ) from exc


def _load_issuer_day_embeddings(
    db_manager: DatabaseManager,
    *,
    start_date: date,
    end_date: date,
    window_days: int,
    model_id: str,
    source_type: str,
) -> List[IssuerDayEmbedding]:
    """Load issuer×day NEWS embeddings for a window-extended date range.

    This function reads from the logical view ``issuer_news_daily`` to obtain
    issuer×day pairs and uses the ``embedding_source_id`` field to join
    ``text_embeddings`` rows with the requested ``source_type`` and
    ``model_id``.

    The query covers an *extended* range so that, for any as_of_date in
    [start_date, end_date], the preceding ``window_days - 1`` days are
    available for window aggregation.
    """

    # Extend backwards so we can construct full windows at start_date.
    extended_start = start_date - timedelta(days=window_days - 1)

    sql = """
        SELECT
            d.issuer_id,
            d.news_date,
            te.vector
        FROM issuer_news_daily d
        JOIN text_embeddings te
          ON te.source_type = %s
         AND te.model_id = %s
         AND te.source_id = d.embedding_source_id
        WHERE d.news_date BETWEEN %s AND %s
        ORDER BY d.issuer_id, d.news_date
    """

    params = (
        source_type,
        model_id,
        extended_start,
        end_date,
    )

    results: List[IssuerDayEmbedding] = []

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.warning(
            "_load_issuer_day_embeddings: no rows for %s→%s (model_id=%s, source_type=%s)",
            extended_start,
            end_date,
            model_id,
            source_type,
        )
        return results

    first_shape: Optional[Tuple[int, ...]] = None
    for issuer_id, news_date, vec_bytes in rows:
        if vec_bytes is None:
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
            IssuerDayEmbedding(
                issuer_id=str(issuer_id),
                news_date=news_date,
                vector=vec,
            ),
        )

    logger.info(
        "Loaded %d issuer×day NEWS embeddings for %s→%s (extended from %s)",
        len(results),
        start_date,
        end_date,
        extended_start,
    )
    return results


def _build_window_docs_and_vectors(
    rows: Iterable[IssuerDayEmbedding],
    *,
    start_date: date,
    end_date: date,
    window_days: int,
    min_coverage: float,
    window_source_type: str,
) -> Tuple[List[TextDoc], NDArray[np.float_]]:
    """Aggregate issuer×day embeddings into rolling NEWS windows.

    Args:
        rows: Iterable of issuer×day embeddings sorted by (issuer_id, news_date).
        start_date: First as_of_date to emit windows for.
        end_date: Last as_of_date to emit windows for.
        window_days: Calendar days in the lookback window.
        min_coverage: Minimum fraction of window_days that must have at least
            one NEWS_ISSUER_DAY embedding to emit a window.
        window_source_type: ``source_type`` to use for the window embeddings
            (typically ``"NEWS_ISSUER_WINDOW"``).

    Returns:
        A pair ``(docs, vectors)`` ready to be passed to ``TextEmbeddingStore``.
    """

    docs: List[TextDoc] = []
    vectors: List[NDArray[np.float_]] = []

    # Group rows by issuer in-memory. v0 implementation is simple and relies on
    # the upstream query ordering; can be optimised later if needed.
    by_issuer: Dict[str, List[IssuerDayEmbedding]] = {}
    for row in rows:
        by_issuer.setdefault(row.issuer_id, []).append(row)

    window_span = timedelta(days=window_days - 1)

    for issuer_id, series in by_issuer.items():
        # series is already sorted by news_date from the SQL ORDER BY.
        dates = [r.news_date for r in series]
        vecs = [r.vector for r in series]
        n = len(series)

        left = 0
        for as_of in _iter_dates(start_date, end_date):
            window_start = as_of - window_span

            # Advance left index until dates[left] is within the window.
            while left < n and dates[left] < window_start:
                left += 1

            # Collect all entries with news_date in [window_start, as_of].
            right = left
            while right < n and dates[right] <= as_of:
                right += 1

            if right <= left:
                # No news in this window; skip.
                continue

            effective_days = right - left
            coverage = effective_days / float(window_days)
            if coverage < min_coverage:
                continue

            stacked = np.stack(vecs[left:right], axis=0)
            mean_vec = stacked.mean(axis=0).astype(np.float32)

            source_id = f"{issuer_id}:{as_of.isoformat()}:{window_days}d"
            docs.append(
                TextDoc(
                    source_type=window_source_type,
                    source_id=source_id,
                    text="",
                ),
            )
            vectors.append(mean_vec)

    if not docs:
        return [], np.zeros((0, 0), dtype=np.float32)

    return docs, np.stack(vectors, axis=0).astype(np.float32)


def _iter_dates(start: date, end: date) -> Iterable[date]:
    """Yield all calendar dates in [start, end]."""

    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# ============================================================================
# CLI
# ============================================================================


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for NEWS_ISSUER_WINDOW backfill.

    This CLI reads issuer×day NEWS embeddings and writes rolling window
    embeddings back into ``text_embeddings``.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Backfill issuer×day NEWS window embeddings into text_embeddings "
            "by aggregating NEWS_ISSUER_DAY vectors.",
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        help="Inclusive start date (YYYY-MM-DD) for as_of_date windows.",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="Inclusive end date (YYYY-MM-DD) for as_of_date windows.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Calendar days in the NEWS lookback window (default: 30)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="text-fin-general-v1",
        help="Text embedding model_id to use (default: text-fin-general-v1)",
    )
    parser.add_argument(
        "--source-type",
        type=str,
        default="NEWS_ISSUER_DAY",
        help=(
            "Source type of the base issuer×day embeddings in text_embeddings "
            "(default: NEWS_ISSUER_DAY)",
        ),
    )
    parser.add_argument(
        "--window-source-type",
        type=str,
        default="NEWS_ISSUER_WINDOW",
        help=(
            "source_type to use for aggregated window embeddings in "
            "text_embeddings (default: NEWS_ISSUER_WINDOW)",
        ),
    )
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.7,
        help=(
            "Minimum fraction of window_days that must have NEWS_ISSUER_DAY "
            "coverage to emit a window (default: 0.7)",
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Load and aggregate issuer×day NEWS windows but do not write "
            "results to text_embeddings.",
        ),
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")
    if args.window_days <= 0:
        parser.error("--window-days must be positive")
    if not (0.0 < args.min_coverage <= 1.0):
        parser.error("--min-coverage must be in (0, 1]")

    config = get_config()
    db_manager = DatabaseManager(config)

    logger.info(
        "Loading issuer×day NEWS embeddings for %s→%s (window_days=%d, model_id=%s, source_type=%s)",
        args.start,
        args.end,
        args.window_days,
        args.model_id,
        args.source_type,
    )

    rows = _load_issuer_day_embeddings(
        db_manager=db_manager,
        start_date=args.start,
        end_date=args.end,
        window_days=args.window_days,
        model_id=args.model_id,
        source_type=args.source_type,
    )

    if not rows:
        logger.warning("No issuer×day embeddings loaded; nothing to do")
        return

    logger.info(
        "Building NEWS_ISSUER_WINDOW embeddings for %d issuer×day base rows", len(rows),
    )

    docs, vectors = _build_window_docs_and_vectors(
        rows,
        start_date=args.start,
        end_date=args.end,
        window_days=args.window_days,
        min_coverage=args.min_coverage,
        window_source_type=args.window_source_type,
    )

    if not docs:
        logger.warning("No NEWS_ISSUER_WINDOW embeddings constructed; nothing to do")
        return

    logger.info(
        "Prepared %d NEWS_ISSUER_WINDOW embeddings (source_type=%s, model_id=%s)",
        len(docs),
        args.window_source_type,
        args.model_id,
    )

    if args.dry_run:
        logger.info("DRY RUN enabled - not writing window embeddings to text_embeddings")
        return

    store = TextEmbeddingStore(db_manager=db_manager)
    # TODO(prometheus, 2025-12-10): Consider chunking large writes into batches
    # if we hit memory or transaction limits in large backfills.
    store.save_embeddings(docs, args.model_id, vectors)

    logger.info(
        "Issuer×day NEWS window backfill complete: wrote %d rows to text_embeddings",
        len(docs),
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()