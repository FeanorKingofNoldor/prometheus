"""Backfill numeric window embeddings for instruments.

This script builds numeric windows from `prices_daily` for a set of
instruments and stores embeddings into the
`numeric_window_embeddings` table using a simple numeric encoder.

The encoder is intentionally straightforward for Iteration 1: it
constructs windows of (close, volume, log-return) features and flattens
them into 1D vectors via :class:`FlattenNumericEmbeddingModel`.

Examples
--------

    # Embed a single instrument for a specific as-of date
    python -m prometheus.scripts.backfill_numeric_embeddings \
        --instrument-id AAPL.US --as-of 2025-11-21 --window-days 63

    # Embed all active US_EQ equities for an as-of date
    python -m prometheus.scripts.backfill_numeric_embeddings \
        --market-id US_EQ --as-of 2025-11-21 --window-days 63
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Optional, Sequence

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.logging import get_logger
from apatheon.core.time import US_EQ, TradingCalendar, TradingCalendarConfig
from apatheon.data.reader import DataReader
from apatheon.encoders import (
    FlattenNumericEmbeddingModel,
    NumericEmbeddingStore,
    NumericWindowBuilder,
    NumericWindowEncoder,
    NumericWindowSpec,
)
from apatheon.encoders.models_simple_numeric import PadToDimNumericEmbeddingModel

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

    Instruments are filtered by `asset_class = 'EQUITY'` and
    `status = 'ACTIVE'` to mirror the price backfill scripts.
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
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        finally:
            cur.close()

    return [r[0] for r in rows]


def _build_encoder(
    db_manager: DatabaseManager,
    window_days: int,
    model_id: str,
    market: str,
) -> NumericWindowEncoder:
    """Construct a NumericWindowEncoder for the given ``model_id``.

    For backwards compatibility, the default ``numeric-simple-v1`` uses a
    pure flattening model. For model_ids that correspond to core numeric
    encoders (e.g. ``num-regime-core-v1``), we use a padded model that
    produces fixed-size 384-dim embeddings in line with the global
    encoder spec.
    """

    reader = DataReader(db_manager=db_manager)
    calendar = TradingCalendar(TradingCalendarConfig(market=market))
    builder = NumericWindowBuilder(reader, calendar)
    store = NumericEmbeddingStore(db_manager=db_manager)

    padded_model_ids = {
        "num-regime-core-v1",
        "num-stab-core-v1",
        "num-profile-core-v1",
        "num-scenario-core-v1",
        "num-portfolio-core-v1",
    }

    if model_id in padded_model_ids:
        model = PadToDimNumericEmbeddingModel(target_dim=384)
    else:
        model = FlattenNumericEmbeddingModel()

    return NumericWindowEncoder(builder=builder, model=model, store=store, model_id=model_id)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Backfill numeric window embeddings for instruments",
    )

    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=True,
        help="As-of date for the numeric window (inclusive, YYYY-MM-DD)",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=63,
        help="Number of trading days in the lookback window (default: 63)",
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
        default=None,
        help="Maximum number of instruments to process (used with --market-id)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=None,
        help="Offset into instrument list (used with --market-id)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="numeric-simple-v1",
        help="Model identifier to tag embeddings with (default: numeric-simple-v1)",
    )
    parser.add_argument(
        "--market",
        type=str,
        default=US_EQ,
        help="Trading calendar market code (default: US_EQ)",
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    instrument_ids: List[str] = []
    if args.instrument_ids:
        instrument_ids.extend(args.instrument_ids)

    if args.market_id:
        market_instruments = _load_instruments_for_market(
            db_manager,
            args.market_id,
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
        "Backfilling numeric embeddings: instruments=%d as_of=%s window_days=%d model_id=%s",
        len(uniq_instruments),
        args.as_of,
        args.window_days,
        args.model_id,
    )

    encoder = _build_encoder(
        db_manager=db_manager,
        window_days=args.window_days,
        model_id=args.model_id,
        market=args.market,
    )

    success = 0
    failures = 0

    for instrument_id in uniq_instruments:
        spec = NumericWindowSpec(
            entity_type="INSTRUMENT",
            entity_id=instrument_id,
            window_days=args.window_days,
        )
        try:
            _ = encoder.embed_and_store(spec, args.as_of)
            success += 1
        except Exception as exc:  # pragma: no cover - defensive
            failures += 1
            logger.exception(
                "Failed to embed instrument %s at %s: %s",
                instrument_id,
                args.as_of,
                exc,
            )

    logger.info(
        "Numeric embeddings backfill complete: success=%d failures=%d", success, failures
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
