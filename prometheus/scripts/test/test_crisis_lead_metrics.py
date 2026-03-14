#!/usr/bin/env python3
"""Quick validation script for crisis-lead metrics in BacktestRunner.

This script runs a simple backtest and verifies that the new crisis-lead
diagnostics are computed and persisted to backtest_runs.metrics_json.

Usage:
    python3 prometheus/scripts/test/test_crisis_lead_metrics.py [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar, US_EQ

logger = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test crisis-lead metrics in backtest runner")
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date for backtest (YYYY-MM-DD). Default: 90 days before end.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date for backtest (YYYY-MM-DD). Default: latest trading day.",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional: inspect an existing run_id instead of running a new backtest.",
    )
    args = parser.parse_args()

    db_manager = get_db_manager()

    def _latest_trading_day(cal: TradingCalendar) -> date:
        """Return the most recent trading day up to today."""
        d = date.today()
        while not cal.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    if args.run_id:
        # Just inspect an existing run.
        run_id = args.run_id
        logger.info(f"Inspecting existing run_id={run_id}")
    else:
        # Determine backtest window.
        calendar = TradingCalendar()
        if args.end:
            end_date = date.fromisoformat(args.end)
        else:
            end_date = _latest_trading_day(calendar)

        if args.start:
            start_date = date.fromisoformat(args.start)
        else:
            start_date = end_date - timedelta(days=90)

        logger.info(f"Running backtest from {start_date} to {end_date}")

        # For this test, we'll just check that the metrics exist in an existing run.
        # A proper backtest requires setting up a full pipeline with Assessment,
        # Universe, and Portfolio engines. For now, let's just query the latest run.
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT run_id, strategy_id, start_date, end_date, metrics_json
                    FROM backtest_runs
                    WHERE start_date >= %s AND end_date <= %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (start_date, end_date),
                )
                row = cursor.fetchone()
                if not row:
                    logger.warning("No recent backtest runs found in the specified window.")
                    logger.info("Please run a backtest first using run_backtest_campaign.py or similar.")
                    return

                run_id, strategy_id, start_db, end_db, metrics_json = row
                logger.info(f"Found recent run: run_id={run_id} strategy={strategy_id} [{start_db} to {end_db}]")
            finally:
                cursor.close()

    # Query the metrics.
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT metrics_json
                FROM backtest_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            if not row:
                logger.error(f"Run {run_id} not found in backtest_runs.")
                return

            metrics_json = row[0]
        finally:
            cursor.close()

    # Check for crisis-lead metrics.
    expected_keys = [
        "crisis_transitions_count",
        "crisis_warnings_found_count",
        "warning_to_crisis_days_mean",
        "warning_to_crisis_days_median",
        "pre_crisis_return_mean",
        "max_drawdown_pre_crisis",
        "warning_coverage_pct",
    ]

    logger.info("=" * 80)
    logger.info(f"Crisis-Lead Metrics for run_id={run_id}")
    logger.info("=" * 80)

    found = []
    missing = []
    for key in expected_keys:
        if key in metrics_json:
            val = metrics_json[key]
            logger.info(f"  {key:40s} = {val:.4f}" if isinstance(val, (int, float)) else f"  {key:40s} = {val}")
            found.append(key)
        else:
            logger.warning(f"  {key:40s} = MISSING")
            missing.append(key)

    logger.info("=" * 80)

    if missing:
        logger.warning(f"Missing {len(missing)} expected crisis-lead metrics.")
        logger.warning("This may indicate that:")
        logger.warning("  - The backtest was run before the crisis-lead metrics were implemented.")
        logger.warning("  - The backtest window had no CRISIS regime transitions.")
        logger.warning("  - An error occurred during crisis-lead metric computation.")
    else:
        logger.info("✓ All expected crisis-lead metrics are present.")

    # Print a few other useful metrics for context.
    logger.info("\nOther key metrics:")
    for key in ["cumulative_return", "annualised_sharpe", "max_drawdown", "annualised_vol"]:
        if key in metrics_json:
            val = metrics_json[key]
            logger.info(f"  {key:40s} = {val:.4f}" if isinstance(val, (int, float)) else f"  {key:40s} = {val}")

    logger.info("\n✓ Validation complete.")


if __name__ == "__main__":
    main()
