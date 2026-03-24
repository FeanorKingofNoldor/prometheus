"""Prometheus v2: Backfill basic issuer×day NEWS factors.

This script computes **scalar NEWS-derived factors** per `(issuer_id, as_of_date)`
and writes them into `historical_db.news_factors_daily`.

v0 factors include:

- `news_intensity_raw`  – number of distinct articles for the issuer/day.
- `news_intensity_log`  – log(1 + n_articles).
- `news_silence_gap`    – days since last news on or before as_of_date.

The script relies on:

- `issuer_news_daily` view for article counts and days-since-last-news.
- Optional issuer×day NEWS embeddings (source_type = 'NEWS_ISSUER_DAY') for
  future factors like novelty; v0 does not compute novelty yet.

External dependencies:
- numpy: vector operations for future factors.

Database tables accessed:
- historical_db.issuer_news_daily (Read).
- historical_db.news_factors_daily (Read/Write).
- historical_db.text_embeddings (Read; reserved for future novelty factors).

Thread safety: Not thread-safe (offline CLI, single process only).

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
from math import log1p
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ============================================================================
# Data structures
# ============================================================================


@dataclass(frozen=True)
class IssuerNewsDailyRow:
    """Logical row from issuer_news_daily.

    Attributes:
        issuer_id: Issuer identifier.
        news_date: Date of the news activity.
        n_articles: Number of distinct news articles on that date.
        days_since_prev_news: Days since the previous news date for this issuer,
            or None if no prior news exists.
    """

    issuer_id: str
    news_date: date
    n_articles: int
    days_since_prev_news: Optional[int]


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


def _load_issuer_news_daily(
    db_manager: DatabaseManager,
    *,
    start_date: date,
    end_date: date,
) -> List[IssuerNewsDailyRow]:
    """Load issuer_news_daily rows for the given date range.

    The underlying view is defined in Alembic migration 0022 and exposes:

    - issuer_id
    - news_date
    - n_articles
    - days_since_prev_news
    - embedding_source_id (not used directly here)
    """

    sql = """
        SELECT issuer_id, news_date, n_articles, days_since_prev_news
        FROM issuer_news_daily
        WHERE news_date BETWEEN %s AND %s
        ORDER BY issuer_id, news_date
    """

    params = (start_date, end_date)

    rows: List[IssuerNewsDailyRow] = []

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            for issuer_id, news_date, n_articles, days_since_prev_news in cursor.fetchall():
                rows.append(
                    IssuerNewsDailyRow(
                        issuer_id=str(issuer_id),
                        news_date=news_date,
                        n_articles=int(n_articles or 0),
                        days_since_prev_news=(
                            int(days_since_prev_news) if days_since_prev_news is not None else None
                        ),
                    ),
                )
        finally:
            cursor.close()

    logger.info(
        "Loaded %d issuer_news_daily rows for %s→%s",
        len(rows),
        start_date,
        end_date,
    )
    return rows


def _compute_coverage_ratio_30d(
    rows: Sequence[IssuerNewsDailyRow],
) -> Dict[Tuple[str, date], float]:
    """Compute 30d NEWS coverage ratio per issuer×day.

    Coverage is defined as the fraction of calendar days in the previous
    30-day window (inclusive of `news_date`) that have at least one news
    article. Since `issuer_news_daily` only has rows for days with news,
    we effectively count how many distinct `news_date` values fall in
    `[news_date-29, news_date]` for the same issuer and divide by 30.
    """

    if not rows:
        return {}

    coverage: Dict[Tuple[str, date], float] = {}
    window_span = timedelta(days=29)

    current_issuer: Optional[str] = None
    window: List[date] = []

    for row in rows:
        if row.issuer_id != current_issuer:
            current_issuer = row.issuer_id
            window.clear()

        cutoff = row.news_date - window_span
        # Evict dates that fall outside the [cutoff, news_date] window.
        i = 0
        while i < len(window) and window[i] < cutoff:
            i += 1
        if i > 0:
            window = window[i:]

        # Append current date (issuer_news_daily guarantees uniqueness per day).
        window.append(row.news_date)

        cov = len(window) / 30.0
        coverage[(row.issuer_id, row.news_date)] = cov

    logger.info(
        "Computed news_coverage_ratio_30d for %d issuer×day rows",
        len(coverage),
    )
    return coverage


def _upsert_news_factors(
    db_manager: DatabaseManager,
    *,
    rows: Iterable[IssuerNewsDailyRow],
    model_id: str,
    dry_run: bool,
    coverage_30d: Optional[Dict[Tuple[str, date], float]] = None,
) -> int:
    """Upsert basic NEWS factors into news_factors_daily.

    v0 computes:

    - news_intensity_raw  (n_articles)
    - news_intensity_log  (log1p(n_articles))
    - news_silence_gap    (days_since_prev_news)

    Returns:
        Number of factor rows written (or that would be written if dry_run=True).
    """

    # Simple per-row UPSERT. For very large backfills this could be batched or
    # rewritten to use COPY; v0 keeps it straightforward.
    insert_sql = """
        INSERT INTO news_factors_daily (
            issuer_id,
            as_of_date,
            model_id,
            factor_name,
            factor_value,
            metadata
        ) VALUES (%(issuer_id)s, %(as_of_date)s, %(model_id)s, %(factor_name)s, %(factor_value)s, %(metadata)s)
        ON CONFLICT (issuer_id, as_of_date, model_id, factor_name)
        DO UPDATE SET
            factor_value = EXCLUDED.factor_value,
            metadata = EXCLUDED.metadata
    """

    count = 0

    if dry_run:
        for row in rows:
            key = (row.issuer_id, row.news_date)
            n = 2  # intensity_raw and intensity_log always exist
            if row.days_since_prev_news is not None:
                n += 1  # news_silence_gap
            if coverage_30d is not None and key in coverage_30d:
                n += 1  # news_coverage_ratio_30d
            # f_news_v0 (derived from intensity & silence gap) always exists
            n += 1
            count += n
        logger.info("DRY RUN: would write %d factor rows to news_factors_daily", count)
        return count

    with db_manager.get_historical_connection() as conn:
        cursor = conn.cursor()
        try:
            for row in rows:
                base_params = {
                    "issuer_id": row.issuer_id,
                    "as_of_date": row.news_date,
                    "model_id": model_id,
                }

                key = (row.issuer_id, row.news_date)

                # news_intensity_raw
                params = dict(base_params)
                params.update(
                    {
                        "factor_name": "news_intensity_raw",
                        "factor_value": float(row.n_articles),
                        "metadata": None,
                    },
                )
                cursor.execute(insert_sql, params)
                count += 1

                # news_intensity_log
                params = dict(base_params)
                params.update(
                    {
                        "factor_name": "news_intensity_log",
                        "factor_value": float(log1p(row.n_articles)),
                        "metadata": None,
                    },
                )
                cursor.execute(insert_sql, params)
                count += 1

                # news_silence_gap (only when defined)
                if row.days_since_prev_news is not None:
                    params = dict(base_params)
                    params.update(
                        {
                            "factor_name": "news_silence_gap",
                            "factor_value": float(row.days_since_prev_news),
                            "metadata": None,
                        },
                    )
                    cursor.execute(insert_sql, params)
                    count += 1

                # news_coverage_ratio_30d (when available)
                if coverage_30d is not None and key in coverage_30d:
                    params = dict(base_params)
                    params.update(
                        {
                            "factor_name": "news_coverage_ratio_30d",
                            "factor_value": float(coverage_30d[key]),
                            "metadata": None,
                        },
                    )
                    cursor.execute(insert_sql, params)
                    count += 1

                # f_news_v0: a simple v0 NEWS activity factor combining
                # log-intensity and silence. We treat it as:
                #   f_news_v0 = news_intensity_log * (1 + min(silence, 10)/10)
                silence = float(row.days_since_prev_news) if row.days_since_prev_news is not None else 0.0
                silence_clamped = min(max(silence, 0.0), 10.0)
                f_news_v0 = float(row.n_articles)
                # work in log space consistent with news_intensity_log
                f_news_v0 = log1p(f_news_v0) * (1.0 + silence_clamped / 10.0)

                params = dict(base_params)
                params.update(
                    {
                        "factor_name": "f_news_v0",
                        "factor_value": float(f_news_v0),
                        "metadata": None,
                    },
                )
                cursor.execute(insert_sql, params)
                count += 1

            conn.commit()
        finally:
            cursor.close()

    logger.info("Wrote %d rows into news_factors_daily", count)
    return count


# ============================================================================
# CLI
# ============================================================================


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for basic issuer×day NEWS factor backfill."""

    parser = argparse.ArgumentParser(
        description=(
            "Backfill basic issuer×day NEWS factors (intensity, silence gap) "
            "into news_factors_daily.",
        ),
    )

    parser.add_argument(
        "--start",
        required=True,
        type=_parse_date,
        help="Inclusive start date (YYYY-MM-DD) for factor as_of_date.",
    )
    parser.add_argument(
        "--end",
        required=True,
        type=_parse_date,
        help="Inclusive end date (YYYY-MM-DD) for factor as_of_date.",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="text-fin-general-v1",
        help=(
            "Logical text model_id the factors are associated with "
            "(default: text-fin-general-v1)",
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Compute factors and log how many rows would be written, "
            "but do not modify news_factors_daily.",
        ),
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    config = get_config()
    db_manager = DatabaseManager(config)

    logger.info(
        "Loading issuer_news_daily rows for %s→%s",
        args.start,
        args.end,
    )

    rows = _load_issuer_news_daily(
        db_manager=db_manager,
        start_date=args.start,
        end_date=args.end,
    )

    if not rows:
        logger.warning("No issuer_news_daily rows loaded; nothing to do")
        return

    logger.info("Computing basic NEWS factors for %d issuer×day rows", len(rows))

    coverage_30d = _compute_coverage_ratio_30d(rows)

    _ = _upsert_news_factors(
        db_manager=db_manager,
        rows=rows,
        model_id=args.model_id,
        dry_run=args.dry_run,
        coverage_30d=coverage_30d,
    )

    # TODO(prometheus, 2025-12-10): Add additional factors such as `news_novelty`
    # and window-based coverage ratios once issuer×day embeddings and
    # NEWS_ISSUER_WINDOW are fully populated and calibrated.


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
