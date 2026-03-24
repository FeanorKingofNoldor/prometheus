"""Prometheus v2 – Backfill canonical markets.

This script seeds the ``markets`` table with a small set of canonical
market definitions used throughout the codebase.

It is intended as a Layer 0 helper to ensure:
- market rows exist (especially in historical_db, which may not be touched
  by runtime-only ingestion paths)
- market_id / region / timezone are consistently populated

The script is idempotent.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class MarketSeed:
    market_id: str
    name: str
    region: str
    timezone: str


_CANONICAL_MARKETS: list[MarketSeed] = [
    MarketSeed(market_id="US_EQ", name="US Equity", region="US", timezone="America/New_York"),
    MarketSeed(market_id="EU_EQ", name="EU Equity", region="EU", timezone="Europe/London"),
    MarketSeed(market_id="ASIA_EQ", name="ASIA Equity", region="ASIA", timezone="Asia/Tokyo"),
]


def _backfill_one_db(
    *,
    db,
    which: str,
    only_missing: bool,
    dry_run: bool,
) -> dict[str, int]:
    if which == "runtime":
        conn_cm = db.get_runtime_connection()
    elif which == "historical":
        conn_cm = db.get_historical_connection()
    else:  # pragma: no cover
        raise ValueError(f"Unknown db selector: {which!r}")

    inserted = 0
    updated = 0
    skipped_existing = 0

    if only_missing:
        sql = """
            INSERT INTO markets (market_id, name, region, timezone)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (market_id) DO NOTHING
        """
    else:
        # Upsert canonical definitions. We overwrite name/region/timezone;
        # other fields (calendar_spec/metadata) remain untouched.
        sql = """
            INSERT INTO markets (market_id, name, region, timezone)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (market_id) DO UPDATE SET
                name = EXCLUDED.name,
                region = EXCLUDED.region,
                timezone = EXCLUDED.timezone,
                updated_at = NOW()
        """

    with conn_cm as conn:
        cur = conn.cursor()
        try:
            for m in _CANONICAL_MARKETS:
                if dry_run:
                    # Determine whether this would insert/update.
                    cur.execute("SELECT 1 FROM markets WHERE market_id = %s", (m.market_id,))
                    exists = cur.fetchone() is not None
                    if exists:
                        if only_missing:
                            skipped_existing += 1
                        else:
                            updated += 1
                    else:
                        inserted += 1
                    continue

                cur.execute(sql, (m.market_id, m.name, m.region, m.timezone))

                # Rowcount semantics:
                # - INSERT DO NOTHING: 1 if inserted, 0 if skipped
                # - INSERT .. DO UPDATE: 1 for insert or update
                if cur.rowcount == 0:
                    skipped_existing += 1
                else:
                    # Best-effort: classify insert vs update.
                    if only_missing:
                        inserted += 1
                    else:
                        # We cannot reliably distinguish insert vs update without extra queries.
                        updated += 1

            if not dry_run:
                conn.commit()
        finally:
            cur.close()

    return {
        "inserted": inserted,
        "updated": updated,
        "skipped_existing": skipped_existing,
    }


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill canonical markets into the markets table")
    parser.add_argument(
        "--db",
        dest="dbs",
        action="append",
        choices=["runtime", "historical"],
        default=None,
        help="Database to target (can specify multiple times; default: both)",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Only insert missing markets (do not update existing rows)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print what would change without writing")

    args = parser.parse_args(argv)

    targets = args.dbs if args.dbs else ["runtime", "historical"]
    only_missing = bool(args.only_missing)
    dry_run = bool(args.dry_run)

    db = get_db_manager()

    for which in targets:
        logger.info(
            "Backfilling markets into %s_db (only_missing=%s dry_run=%s)",
            which,
            only_missing,
            dry_run,
        )
        stats = _backfill_one_db(db=db, which=which, only_missing=only_missing, dry_run=dry_run)
        print({"db": which, **stats})


if __name__ == "__main__":  # pragma: no cover
    main()
