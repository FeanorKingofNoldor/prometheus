"""Prometheus v2 – Run execution for the *meta-selected* book of a market.

This is a convenience wrapper around `run_execution_for_portfolio`.

Instead of requiring you to know the correct `portfolio_id` (book_id)
for the day (e.g. `US_EQ_LONG` vs `US_EQ_HEDGE_ETF`), this script:

1) looks up the latest `META_POLICY_V1` decision (or the one for `--as-of`),
2) extracts the selected `book_id`, and
3) delegates execution to `run_execution_for_portfolio`.

Usage examples
--------------

Dry-run the latest meta-selected book for US_EQ:

    python -m prometheus.scripts.run.run_execution_for_market \
        --market-id US_EQ \
        --mode PAPER \
        --notional 100000 \
        --dry-run

Execute the meta-selected book for a specific date:

    python -m prometheus.scripts.run.run_execution_for_market \
        --market-id US_EQ \
        --as-of 2025-12-30 \
        --mode PAPER \
        --notional 100000
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, Optional, Tuple

from apathis.core.database import DatabaseManager, get_db_manager
from apathis.core.logging import get_logger
from apathis.core.markets import MARKETS_BY_REGION, infer_region_from_market_id
from prometheus.meta.policy import load_meta_policy_artifact
from prometheus.scripts.run.run_execution_for_portfolio import main as run_execution_for_portfolio_main


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _resolve_market_and_region(*, region: str | None, market_id: str | None) -> Tuple[str, str | None]:
    """Resolve market_id and (optional) region from CLI args."""

    if market_id is not None and str(market_id).strip():
        mid = str(market_id).strip().upper()
        inferred_region = infer_region_from_market_id(mid)
        return mid, inferred_region

    if region is None or not str(region).strip():
        raise ValueError("Must provide either --market-id or --region")

    reg = str(region).strip().upper()
    markets = MARKETS_BY_REGION.get(reg)
    if not markets:
        raise ValueError(f"Unknown region {reg!r}; no market mapping found")

    return str(markets[0]).upper(), reg


def _find_meta_policy_selection(
    db_manager: DatabaseManager,
    *,
    market_id: str,
    as_of: date | None,
) -> Tuple[Optional[str], Optional[str], Optional[date]]:
    """Return (selected_book_id, selected_sleeve_id, decision_as_of_date).

    If `as_of` is None, returns the latest decision for the market.
    """

    sql_latest = """
        SELECT as_of_date, strategy_id, config_id, output_refs
        FROM engine_decisions
        WHERE engine_name = 'META_POLICY_V1'
          AND market_id = %s
        ORDER BY as_of_date DESC, created_at DESC
        LIMIT 1
    """

    sql_for_date = """
        SELECT as_of_date, strategy_id, config_id, output_refs
        FROM engine_decisions
        WHERE engine_name = 'META_POLICY_V1'
          AND market_id = %s
          AND as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            if as_of is None:
                cursor.execute(sql_latest, (str(market_id).upper(),))
            else:
                cursor.execute(sql_for_date, (str(market_id).upper(), as_of))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if not row:
        return None, None, None

    as_of_date_db, strategy_id_db, config_id_db, output_refs_db = row

    out = output_refs_db if isinstance(output_refs_db, dict) else {}

    selected_book_id = out.get("selected_book_id")
    if isinstance(selected_book_id, str) and selected_book_id.strip():
        book_id = selected_book_id.strip()
    else:
        book_id = str(strategy_id_db).strip() if strategy_id_db is not None else None

    selected_sleeve_id = out.get("selected_sleeve_id")
    sleeve_id = selected_sleeve_id.strip() if isinstance(selected_sleeve_id, str) and selected_sleeve_id.strip() else None
    if sleeve_id is None and isinstance(config_id_db, str) and config_id_db.strip():
        sleeve_id = config_id_db.strip()

    return book_id, sleeve_id, as_of_date_db


def _fallback_book_from_policy(*, market_id: str) -> Optional[str]:
    artifact = load_meta_policy_artifact()
    pol = artifact.policies.get(str(market_id).upper())
    if pol is None:
        return None
    return str(pol.default.book_id)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Execute the meta-selected book for a market (wrapper around run_execution_for_portfolio)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--market-id",
        type=str,
        required=False,
        help="Market id (e.g. US_EQ). If omitted, --region must be provided.",
    )
    parser.add_argument(
        "--region",
        type=str,
        required=False,
        help="Region (e.g. US). Used only when --market-id is omitted.",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=False,
        help="As-of date for the snapshot (YYYY-MM-DD). If omitted, uses the meta decision's latest as_of_date.",
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["PAPER", "LIVE"],
        default="PAPER",
        help="Execution mode: PAPER (default) or LIVE",
    )
    parser.add_argument(
        "--notional",
        type=float,
        required=True,
        help="Total notional to allocate according to target weights (account currency)",
    )
    parser.add_argument(
        "--readonly",
        action="store_true",
        help=(
            "For LIVE mode: create broker in readonly mode (no order submission). "
            "Has no effect in PAPER mode and defaults to True when mode=LIVE."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan constrained target quantities and print a JSON preview without submitting orders",
    )

    args = parser.parse_args(argv)

    try:
        market_id, region = _resolve_market_and_region(region=args.region, market_id=args.market_id)
    except Exception as exc:
        logger.error("run_execution_for_market: %s", exc)
        return 2

    db_manager = get_db_manager()

    book_id, sleeve_id, decision_as_of = _find_meta_policy_selection(
        db_manager,
        market_id=market_id,
        as_of=args.as_of,
    )

    if book_id is None:
        # Fall back to policy artifact default.
        book_id = _fallback_book_from_policy(market_id=market_id)

    if book_id is None:
        # Final fallback: legacy region-core book id.
        if region is not None:
            book_id = f"{region}_CORE_LONG_EQ"

    if book_id is None:
        logger.error(
            "run_execution_for_market: could not resolve a book_id (market_id=%s region=%s)",
            market_id,
            region,
        )
        return 2

    if str(book_id).upper() == "CASH":
        logger.info(
            "run_execution_for_market: meta-selected book is CASH for market_id=%s as_of=%s; nothing to execute",
            market_id,
            (args.as_of or decision_as_of),
        )
        return 0

    effective_as_of = args.as_of or decision_as_of

    logger.info(
        "run_execution_for_market: market_id=%s as_of=%s selected_book_id=%s selected_sleeve_id=%s",
        market_id,
        effective_as_of,
        book_id,
        sleeve_id,
    )

    # Delegate to the portfolio executor.
    delegated_argv: list[str] = [
        "--portfolio-id",
        str(book_id),
        "--mode",
        str(args.mode).upper(),
        "--notional",
        str(float(args.notional)),
    ]

    if effective_as_of is not None:
        delegated_argv.extend(["--as-of", effective_as_of.isoformat()])

    if args.readonly:
        delegated_argv.append("--readonly")

    if args.dry_run:
        delegated_argv.append("--dry-run")

    run_execution_for_portfolio_main(delegated_argv)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
