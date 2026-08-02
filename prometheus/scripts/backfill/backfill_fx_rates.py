"""Backfill daily FX rates (XXXUSD) from EODHD into ``fx_rates_daily``.

The FX layer (``prometheus.execution.fx``) converts local-currency prices
and notionals to USD using ``fx_rates_daily`` in the historical DB. This
script fills the history so backtests and staleness-windowed lookups have
data from day one; the apatheon scheduler job (``fx_rates_refresh``,
22:35 UTC) keeps it current afterwards.

Pairs are stored in ``XXXUSD`` convention (USD per 1 unit of XXX); the
ingest helper transparently fetches and inverts USD-base sources for the
small currencies (KRW/JPY/HKD) where the direct EODHD quote lacks
precision.

Examples
--------

    # Dry-run: show what would be fetched
    python -m prometheus.scripts.backfill.backfill_fx_rates \
        --from 2024-07-01 --to 2026-07-03 --dry-run

    # Backfill everything
    python -m prometheus.scripts.backfill.backfill_fx_rates \
        --from 2024-07-01 --to 2026-07-03
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.data_ingestion.fx_rates import FX_PAIRS, INVERTED_SOURCE, ingest_fx_rates

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    year, month, day = map(int, value.split("-"))
    return date(year, month, day)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="from_date", type=_parse_date, required=True)
    parser.add_argument("--to", dest="to_date", type=_parse_date, required=True)
    parser.add_argument(
        "--pairs",
        default=None,
        help=f"Comma-separated XXXUSD pairs (default: {','.join(FX_PAIRS)})",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    pairs = tuple(p.strip().upper() for p in args.pairs.split(",")) if args.pairs else FX_PAIRS
    print(f"FX backfill: {len(pairs)} pairs, {args.from_date} → {args.to_date}")

    if args.dry_run:
        for pair in pairs:
            source = INVERTED_SOURCE.get(pair, pair)
            note = f" (fetch {source}.FOREX, store inverted)" if source != pair else ""
            print(f"  would fetch {pair}{note}")
        return

    db = get_db_manager()
    results = ingest_fx_rates(db, pairs, args.from_date, args.to_date)

    total = sum(results.values())
    print(f"done: {total:,} rows upserted")
    for pair in pairs:
        print(f"  {pair}: {results.get(pair, 0):,} rows")


if __name__ == "__main__":
    main()
