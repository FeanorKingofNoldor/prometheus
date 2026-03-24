"""Backfill instrument_identifiers (time-versioned instrument identifier history).

This Layer 0 helper seeds `instrument_identifiers` from the current
`instruments` table (e.g. `instruments.symbol`).

Notes
-----
- Many vendors only provide a *current* ticker/symbol. We store it as an
  open-ended interval starting at `--effective-start`.
- The script is idempotent via an upsert on
  (instrument_id, identifier_type, effective_start).
- This is a foundation step for enforcing instrument identity policies
  (ticker changes should not imply instrument identity changes).
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger
from apathis.core.time import US_EQ
from psycopg2.extras import Json

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _load_instruments(
    db: DatabaseManager,
    *,
    market_ids: Sequence[str],
    asset_class: str,
    status: str,
) -> list[tuple[str, str, str | None, str]]:
    """Return (instrument_id, symbol, exchange, market_id) rows."""

    sql = """
        SELECT
            instrument_id,
            symbol,
            exchange,
            market_id
        FROM instruments
        WHERE market_id = ANY(%s)
          AND asset_class = %s
          AND status = %s
        ORDER BY instrument_id
    """

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (list(market_ids), asset_class, status))
            rows = cur.fetchall()
        finally:
            cur.close()

    out: list[tuple[str, str, str | None, str]] = []
    for instrument_id, symbol, exchange, market_id in rows:
        if not instrument_id:
            continue
        out.append(
            (
                str(instrument_id),
                str(symbol or ""),
                str(exchange) if exchange is not None else None,
                str(market_id or ""),
            )
        )
    return out


def _load_existing_instrument_ids(
    db: DatabaseManager,
    *,
    identifier_type: str,
    effective_start: date,
) -> set[str]:
    """Return instrument_ids that already have the (identifier_type, effective_start) row."""

    sql = """
        SELECT instrument_id
        FROM instrument_identifiers
        WHERE identifier_type = %s
          AND effective_start = %s
    """

    out: set[str] = set()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (identifier_type, effective_start))
            for (instrument_id,) in cur.fetchall():
                if instrument_id:
                    out.add(str(instrument_id))
        finally:
            cur.close()

    return out


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill instrument_identifiers from instruments")

    parser.add_argument(
        "--market-id",
        dest="market_ids",
        action="append",
        default=None,
        help=f"Market ID to include (can specify multiple times; default: {US_EQ})",
    )
    parser.add_argument("--asset-class", type=str, default="EQUITY")
    parser.add_argument("--status", type=str, default="ACTIVE")

    parser.add_argument(
        "--identifier-type",
        type=str,
        default="SYMBOL",
        help="Identifier type to write (default: SYMBOL)",
    )
    parser.add_argument(
        "--effective-start",
        type=_parse_date,
        default=date(1997, 1, 1),
        help="Effective start for the seeded interval (default: 1997-01-01)",
    )
    parser.add_argument(
        "--effective-end",
        type=_parse_date,
        default=None,
        help="Optional effective_end (exclusive). Default: NULL (open-ended)",
    )

    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip instruments that already have the (identifier_type, effective_start) row",
    )
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)

    market_ids = args.market_ids if args.market_ids else [US_EQ]
    asset_class = str(args.asset_class)
    status = str(args.status)

    identifier_type = str(args.identifier_type).strip()
    effective_start: date = args.effective_start
    effective_end: date | None = args.effective_end

    if not identifier_type:
        raise SystemExit("--identifier-type must be non-empty")

    db = get_db_manager()

    rows = _load_instruments(db, market_ids=market_ids, asset_class=asset_class, status=status)
    if not rows:
        logger.info("No instruments found for markets=%s asset_class=%s status=%s", market_ids, asset_class, status)
        return

    existing: set[str] = set()
    if args.only_missing:
        existing = _load_existing_instrument_ids(db, identifier_type=identifier_type, effective_start=effective_start)

    sql_upsert = """
        INSERT INTO instrument_identifiers (
            instrument_id,
            identifier_type,
            identifier_value,
            effective_start,
            effective_end,
            source,
            ingested_at,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (instrument_id, identifier_type, effective_start)
        DO UPDATE SET
            identifier_value = EXCLUDED.identifier_value,
            effective_end = EXCLUDED.effective_end,
            source = EXCLUDED.source,
            ingested_at = NOW(),
            metadata = EXCLUDED.metadata
    """

    wrote = 0
    skipped_existing = 0
    skipped_missing_symbol = 0

    if args.dry_run:
        for instrument_id, symbol, exchange, market_id in rows:
            if instrument_id in existing:
                skipped_existing += 1
                continue
            symbol = (symbol or "").strip()
            if not symbol:
                skipped_missing_symbol += 1
                continue
            wrote += 1

        logger.info(
            "DRY RUN: would write %d rows (skipped_existing=%d skipped_missing_symbol=%d)",
            wrote,
            skipped_existing,
            skipped_missing_symbol,
        )
        return

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for instrument_id, symbol, exchange, market_id in rows:
                if instrument_id in existing:
                    skipped_existing += 1
                    continue

                symbol = (symbol or "").strip()
                if not symbol:
                    skipped_missing_symbol += 1
                    continue

                meta = {
                    "seed_source": "instruments",
                    "market_id": market_id,
                    "exchange": exchange,
                    "instrument_symbol": symbol,
                }

                cur.execute(
                    sql_upsert,
                    (
                        instrument_id,
                        identifier_type,
                        symbol,
                        effective_start,
                        effective_end,
                        "seed_from_instruments",
                        Json(meta),
                    ),
                )
                wrote += 1

            conn.commit()
        finally:
            cur.close()

    logger.info(
        "Wrote %d instrument_identifiers rows (skipped_existing=%d skipped_missing_symbol=%d)",
        wrote,
        skipped_existing,
        skipped_missing_symbol,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
