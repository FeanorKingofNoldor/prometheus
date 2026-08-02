"""Backfill EODHD prices for any market's instruments.

Generalisation of ``backfill_eodhd_us_eq.py``: non-US instrument ids in
the ``instruments`` table are already EODHD tickers (``0005.HK``,
``AAL.LSE``, ``BMW.XETRA``, ``000660.KO``, ``BHP.AU``), so the
instrument→symbol mapping is the identity and the per-instrument
currency comes from the instruments row.

Non-US markets were onboarded in April 2026 with daily ingestion only —
no history — so the signal chain (126d momentum, 63d vol/liquidity)
could never run for them. This script fills the gap.

Examples
--------

    # Dry-run: show what would be fetched
    python -m prometheus.scripts.backfill.backfill_eodhd_market \
        --market UK_EQ --from 2024-07-01 --to 2026-07-02 --dry-run

    # Backfill UK
    python -m prometheus.scripts.backfill.backfill_eodhd_market \
        --market UK_EQ --from 2024-07-01 --to 2026-07-02
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data.writer import DataWriter
from apatheon.data_ingestion.eodhd_client import EodhdClient
from apatheon.data_ingestion.eodhd_prices import ingest_eodhd_prices_for_instruments

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", required=True, help="market_id, e.g. UK_EQ")
    parser.add_argument("--from", dest="from_date", type=_parse_date, required=True)
    parser.add_argument("--to", dest="to_date", type=_parse_date, required=True)
    parser.add_argument("--status", default="ACTIVE")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT instrument_id, currency FROM instruments
                WHERE market_id = %s AND status = %s
                  AND instrument_id NOT LIKE 'SYNTH%%'
                ORDER BY instrument_id
                """,
                (args.market, args.status),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    if args.limit:
        rows = rows[: args.limit]

    if not rows:
        print(f"No {args.status} instruments for market {args.market}")
        return

    mapping = {iid: iid for iid, _ in rows}
    currencies = {iid: cur_ for iid, cur_ in rows}
    print(f"{args.market}: {len(mapping)} instruments, {args.from_date} → {args.to_date}")

    if args.dry_run:
        for iid in list(mapping)[:10]:
            print(f"  would fetch {iid} ({currencies[iid]})")
        if len(mapping) > 10:
            print(f"  ... and {len(mapping) - 10} more")
        return

    client = EodhdClient()
    writer = DataWriter(db_manager=db)
    results = ingest_eodhd_prices_for_instruments(
        mapping,
        args.from_date,
        args.to_date,
        currency_by_instrument=currencies,
        client=client,
        writer=writer,
    )

    # Failed instruments are logged inside the ingest helper and omitted
    # from results entirely.
    ok = sum(1 for r in results if r.bars_written > 0)
    empty = sum(1 for r in results if r.bars_written == 0)
    failed = sorted(set(mapping) - {r.instrument_id for r in results})
    total_rows = sum(r.bars_written for r in results)
    print(
        f"done: {ok} ok, {empty} empty, {len(failed)} failed, "
        f"{total_rows:,} bars written"
    )
    for iid in failed[:25]:
        print(f"  FAILED {iid}")


if __name__ == "__main__":
    main()
