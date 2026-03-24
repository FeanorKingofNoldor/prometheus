"""Prometheus v2 – Ingest Hedge ETF instruments into runtime DB.

This script ensures that hedge ETF instrument_ids referenced by the
HEDGE_ETF book registry exist in the runtime `instruments` table.

Why
---
The hedge ETF backtest runner consumes `instrument_id`s from
`configs/meta/books.yaml`. While the C++ backtester only needs historical
`prices_daily`, many other parts of Prometheus assume referenced
instruments exist in the runtime identity tables.

Design notes
------------
- We intentionally use `asset_class='ETF'` so these instruments are not
  accidentally included in equity universes that filter for `EQUITY`.
- `issuer_id` is left NULL; Layer 0 constraints only require `issuer_id`
  for `asset_class='EQUITY'`.

Usage
-----
  python -m prometheus.scripts.ingest.ingest_hedge_etf_instruments \
    --book-id US_EQ_HEDGE_ETF
"""

from __future__ import annotations

import argparse
import json
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from psycopg2.extras import Json

from prometheus.books.registry import BookKind, HedgeEtfSleeveSpec, load_book_registry


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Upsert runtime instruments for hedge ETF sleeves (from configs/meta/books.yaml)"
    )

    parser.add_argument(
        "--book-id",
        type=str,
        default="US_EQ_HEDGE_ETF",
        help="Book id from configs/meta/books.yaml (default: US_EQ_HEDGE_ETF)",
    )
    parser.add_argument(
        "--asset-class",
        type=str,
        default="ETF",
        help="asset_class value to write for these instruments (default: ETF)",
    )
    parser.add_argument(
        "--status",
        type=str,
        default="ACTIVE",
        help="status value to write for these instruments (default: ACTIVE)",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="US",
        help="exchange value to write for these instruments (default: US)",
    )
    parser.add_argument(
        "--currency",
        type=str,
        default="USD",
        help="currency value to write for these instruments (default: USD)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB; print planned upserts",
    )

    args = parser.parse_args(argv)

    registry = load_book_registry()
    book = registry.get(str(args.book_id))
    if book is None:
        raise SystemExit(f"Unknown book_id={args.book_id!r}; check configs/meta/books.yaml")
    if book.kind != BookKind.HEDGE_ETF:
        raise SystemExit(f"book_id={args.book_id!r} is kind={book.kind}, expected HEDGE_ETF")

    instrument_ids: set[str] = set()
    for _, spec in book.sleeves.items():
        if not isinstance(spec, HedgeEtfSleeveSpec):
            continue
        instrument_ids.update(spec.instrument_ids)

    inst_sorted = sorted(instrument_ids)
    if not inst_sorted:
        raise SystemExit("No hedge ETF instrument_ids found in book registry")

    planned = []
    for instrument_id in inst_sorted:
        symbol = instrument_id.split(".", 1)[0].strip()
        planned.append(
            {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "market_id": book.market_id,
                "asset_class": str(args.asset_class).strip().upper(),
                "exchange": str(args.exchange).strip().upper(),
                "currency": str(args.currency).strip().upper(),
                "status": str(args.status).strip().upper(),
            }
        )

    if args.dry_run:
        print(json.dumps({"planned": planned}, indent=2, sort_keys=True))
        return

    sql = """
        INSERT INTO instruments (
            instrument_id,
            issuer_id,
            market_id,
            asset_class,
            symbol,
            exchange,
            currency,
            status,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            issuer_id = EXCLUDED.issuer_id,
            market_id = EXCLUDED.market_id,
            asset_class = EXCLUDED.asset_class,
            symbol = EXCLUDED.symbol,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            status = EXCLUDED.status,
            metadata = EXCLUDED.metadata
    """

    db = get_db_manager()

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            for p in planned:
                instrument_id = p["instrument_id"]
                meta = {
                    "source": "books_registry",
                    "book_id": str(args.book_id),
                    "eodhd_symbol": str(instrument_id),
                }
                cur.execute(
                    sql,
                    (
                        instrument_id,
                        None,  # issuer_id
                        p["market_id"],
                        p["asset_class"],
                        p["symbol"],
                        p["exchange"],
                        p["currency"],
                        p["status"],
                        Json(meta),
                    ),
                )
            conn.commit()
        finally:
            cur.close()

    print(json.dumps({"upserted": len(planned), "book_id": str(args.book_id)}, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
