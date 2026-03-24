"""Create a dummy target_portfolios row for IBKR paper testing.

This script writes a single `target_portfolios` row into the runtime
DB for a given portfolio_id and date, with a very simple allocation
across a small set of instruments. It is intended only for local
integration testing of the IBKR execution path.

Usage (dev only)::

    python -m prometheus.scripts.create_dummy_ibkr_targets \
        --portfolio-id US_CORE_LONG_EQ \
        --as-of 2025-12-02
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create a dummy target_portfolios row for IBKR paper testing. "
            "This is for local dev only and should not be used in production."
        ),
    )

    parser.add_argument(
        "--portfolio-id",
        type=str,
        default="US_CORE_LONG_EQ",
        help="Portfolio identifier (default: US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--strategy-id",
        type=str,
        default=None,
        help="Strategy identifier (default: same as --portfolio-id)",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date for the dummy targets (YYYY-MM-DD)",
    )

    args = parser.parse_args(argv)

    portfolio_id = args.portfolio_id
    strategy_id = args.strategy_id or portfolio_id
    as_of = args.as_of

    db_manager = get_db_manager()

    # Very simple equal-weight allocation across a few liquid US names.
    weights = {
        "AAPL.US": 0.4,
        "MSFT.US": 0.3,
        "GOOGL.US": 0.3,
    }

    target_positions = {"weights": weights}
    metadata = {
        "source": "create_dummy_ibkr_targets",
        "note": "Synthetic targets for IBKR paper integration test",
    }

    sql = """
        INSERT INTO target_portfolios (
            target_id,
            strategy_id,
            portfolio_id,
            as_of_date,
            target_positions,
            metadata
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    target_id = generate_uuid()

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                sql,
                (
                    target_id,
                    strategy_id,
                    portfolio_id,
                    as_of,
                    Json(target_positions),
                    Json(metadata),
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    logger.info(
        "Created dummy target_portfolios row: target_id=%s portfolio_id=%s as_of=%s weights=%s",
        target_id,
        portfolio_id,
        as_of,
        weights,
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
