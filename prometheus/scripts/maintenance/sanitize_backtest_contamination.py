"""One-shot DB sanitation: remove backtest rows written under LIVE ids.

Historical backfills and parity experiments wrote millions of rows under
the same strategy/universe/portfolio ids the live daemon uses
(``US_CORE_LONG_EQ`` instrument scores back to 1997, ``CORE_EQ_US``
universe members back to 2020, ``CPP_UI_PARITY_*`` orders/fills tagged
mode=BACKTEST). Any calibration query that filters only by id silently
averages decades of backtest output into "live" metrics.

This script deletes those rows. Backtest experiments under their OWN ids
(``BT_*``, ``LAMBDA_*``, ``CORE_EQ_US_27Y``, ...) are left untouched.
Going forward the ``BT_`` namespace is enforced at the backtest entry
points (see prometheus/backtest/naming.py).

Usage
-----
    python -m prometheus.scripts.maintenance.sanitize_backtest_contamination           # dry run
    python -m prometheus.scripts.maintenance.sanitize_backtest_contamination --execute # archive + delete

Every targeted table is archived to gzipped CSV under
``data/archive/sanitize_<timestamp>/`` before deletion.
"""

from __future__ import annotations

import argparse
import csv
import gzip
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]

# First trading day of the current clean paper run (post 2026-05-12/13
# resets). Rows under live ids BEFORE this date are backfill/backtest era.
LIVE_RUN_START = "2026-05-14"


@dataclass(frozen=True)
class Target:
    table: str
    where: str
    description: str


TARGETS: tuple[Target, ...] = (
    Target(
        "instrument_scores",
        f"strategy_id = 'US_CORE_LONG_EQ' AND as_of_date < '{LIVE_RUN_START}'",
        "assessment backfill (1997+) written under the live strategy id",
    ),
    Target(
        "universe_members",
        f"universe_id = 'CORE_EQ_US' AND as_of_date < '{LIVE_RUN_START}'",
        "universe backfill (2020+) written under the live universe id",
    ),
    Target(
        "target_portfolios",
        f"portfolio_id = 'US_EQ_LONG_V12' AND as_of_date < '{LIVE_RUN_START}'",
        "pre-reset targets under the live portfolio id",
    ),
    Target(
        "position_convictions",
        "portfolio_id = 'US_EQ_LONG_V12'",
        "conviction states (all — fresh start at the coming account reset; "
        "pre-reset rows carry stale entry prices)",
    ),
    # fills reference orders (fk_fills_order): delete children first.
    Target(
        "fills",
        "mode = 'BACKTEST'",
        "parity-experiment fills",
    ),
    Target(
        "executed_actions",
        "portfolio_id LIKE 'CPP_UI_PARITY%'",
        "parity-experiment executed actions (no mode column; matched by id)",
    ),
    Target(
        "orders",
        "mode = 'BACKTEST'",
        "parity-experiment orders (CPP_UI_PARITY_*, 2024 dates)",
    ),
    Target(
        "positions_snapshots",
        "mode = 'BACKTEST'",
        "parity-experiment position snapshots",
    ),
)


def _archive(cur, target: Target, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    out_path = archive_dir / f"{target.table}.csv.gz"
    cur.execute(f"SELECT * FROM {target.table} WHERE {target.where} LIMIT 0")
    columns = [d[0] for d in cur.description]
    cur.execute(f"SELECT * FROM {target.table} WHERE {target.where}")
    with gzip.open(out_path, "wt", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        while True:
            rows = cur.fetchmany(50_000)
            if not rows:
                break
            writer.writerows(rows)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Archive and DELETE. Without this flag, only counts are printed.",
    )
    args = parser.parse_args()

    db = get_db_manager()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_dir = PROJECT_ROOT / "data" / "archive" / f"sanitize_{stamp}"

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            total = 0
            for target in TARGETS:
                cur.execute(
                    f"SELECT COUNT(*) FROM {target.table} WHERE {target.where}"
                )
                n = int(cur.fetchone()[0])
                total += n
                print(f"{target.table:24s} {n:>10,} rows — {target.description}")
                if not args.execute or n == 0:
                    continue
                path = _archive(cur, target, archive_dir)
                cur.execute(f"DELETE FROM {target.table} WHERE {target.where}")
                deleted = cur.rowcount
                conn.commit()
                print(f"{'':24s} archived -> {path}")
                print(f"{'':24s} DELETED {deleted:,} rows")

            if args.execute:
                for target in TARGETS:
                    cur.execute(f"ANALYZE {target.table}")
                conn.commit()
                print(f"\nDone. Total rows removed: see per-table output. "
                      f"Archives in {archive_dir}")
            else:
                print(f"\nDRY RUN — {total:,} rows would be archived+deleted. "
                      f"Re-run with --execute.")
        finally:
            cur.close()


if __name__ == "__main__":
    main()
