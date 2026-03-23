"""Prometheus v2 – Validate Hedge ETF data readiness.

Checks
------
- All hedge ETF instrument_ids referenced by the configured HEDGE_ETF book
  exist in runtime `instruments`.
- Historical `prices_daily` coverage exists for those instrument_ids.

This is a focused data-readiness validator to support fast iteration on
hedge ETF sleeves and the C++ backtester.

Usage
-----
  python -m prometheus.scripts.validate.validate_hedge_etf_data --book-id US_EQ_HEDGE_ETF
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Optional, Sequence

from apathis.core.database import get_db_manager

from prometheus.books.registry import BookKind, HedgeEtfSleeveSpec, load_book_registry


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Validate hedge ETF instrument + price coverage")
    parser.add_argument(
        "--book-id",
        type=str,
        default="US_EQ_HEDGE_ETF",
        help="Book id from configs/meta/books.yaml (default: US_EQ_HEDGE_ETF)",
    )

    args = parser.parse_args(argv)

    registry = load_book_registry()
    book = registry.get(str(args.book_id))
    if book is None:
        raise SystemExit(f"Unknown book_id={args.book_id!r}; check configs/meta/books.yaml")
    if book.kind != BookKind.HEDGE_ETF:
        raise SystemExit(f"book_id={args.book_id!r} is kind={book.kind}, expected HEDGE_ETF")

    instrument_ids: set[str] = set()
    sleeve_to_instruments: dict[str, list[str]] = {}
    for sid, spec in book.sleeves.items():
        if not isinstance(spec, HedgeEtfSleeveSpec):
            continue
        inst = sorted(spec.instrument_ids)
        sleeve_to_instruments[sid] = inst
        instrument_ids.update(inst)

    ids_sorted = sorted(instrument_ids)
    if not ids_sorted:
        raise SystemExit("No hedge ETF instrument_ids found in book registry")

    db = get_db_manager()

    # Runtime instruments coverage
    sql_inst = """
        SELECT instrument_id, symbol, asset_class, status, market_id, currency, exchange
        FROM instruments
        WHERE instrument_id = ANY(%s)
        ORDER BY instrument_id
    """

    runtime_rows = []
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_inst, (ids_sorted,))
            runtime_rows = cur.fetchall()
        finally:
            cur.close()

    runtime_by_id: dict[str, dict[str, Any]] = {}
    for instrument_id, symbol, asset_class, status, market_id, currency, exchange in runtime_rows:
        runtime_by_id[str(instrument_id)] = {
            "instrument_id": str(instrument_id),
            "symbol": str(symbol),
            "asset_class": str(asset_class),
            "status": str(status),
            "market_id": str(market_id),
            "currency": str(currency),
            "exchange": str(exchange) if exchange is not None else None,
        }

    missing_runtime = [iid for iid in ids_sorted if iid not in runtime_by_id]

    # Historical prices coverage
    sql_prices = """
        SELECT instrument_id, COUNT(*) AS n_rows, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
        FROM prices_daily
        WHERE instrument_id = ANY(%s)
        GROUP BY instrument_id
        ORDER BY instrument_id
    """

    price_rows = []
    with db.get_historical_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql_prices, (ids_sorted,))
            price_rows = cur.fetchall()
        finally:
            cur.close()

    prices_by_id: dict[str, dict[str, Any]] = {}
    for instrument_id, n_rows, min_date, max_date in price_rows:
        prices_by_id[str(instrument_id)] = {
            "instrument_id": str(instrument_id),
            "n_rows": int(n_rows or 0),
            "min_trade_date": min_date.isoformat() if min_date is not None else None,
            "max_trade_date": max_date.isoformat() if max_date is not None else None,
        }

    missing_prices = [iid for iid in ids_sorted if iid not in prices_by_id]

    report = {
        "book_id": str(args.book_id),
        "market_id": str(book.market_id),
        "instrument_ids": ids_sorted,
        "sleeves": sleeve_to_instruments,
        "runtime": {
            "found": list(runtime_by_id.values()),
            "missing_instruments": missing_runtime,
        },
        "historical": {
            "prices": list(prices_by_id.values()),
            "missing_prices": missing_prices,
        },
        "checks_passed": (len(missing_runtime) == 0 and len(missing_prices) == 0),
    }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
