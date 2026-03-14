"""Run daily trading pipeline for a specified date.

Usage:
    python -m prometheus.scripts.run.run_daily_pipeline --date 2024-12-15
    python -m prometheus.scripts.run.run_daily_pipeline --date 2024-12-01 --region US
    python -m prometheus.scripts.run.run_daily_pipeline --start 2024-12-01 --end 2024-12-15
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from prometheus.orchestration.daily_orchestrator import (
    DailyOrchestrator,
    DailyPipelineConfig,
)

logger = get_logger(__name__)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return date.fromisoformat(date_str)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_str}. Use YYYY-MM-DD") from e


def main(argv: list[str] | None = None) -> int:
    """Main entrypoint for daily pipeline CLI."""
    parser = argparse.ArgumentParser(
        description="Run Prometheus v2 daily trading pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--date",
        type=parse_date,
        help="Date to run pipeline for (YYYY-MM-DD). Mutually exclusive with --start/--end.",
    )
    
    parser.add_argument(
        "--start",
        type=parse_date,
        help="Start date for date range (YYYY-MM-DD). Requires --end.",
    )
    
    parser.add_argument(
        "--end",
        type=parse_date,
        help="End date for date range (YYYY-MM-DD). Requires --start.",
    )
    
    parser.add_argument(
        "--region",
        type=str,
        default="US",
        help="Region to run (default: US)",
    )
    
    parser.add_argument(
        "--skip-regime",
        action="store_true",
        help="Skip regime detection phase",
    )
    
    parser.add_argument(
        "--skip-profiles",
        action="store_true",
        help="Skip STAB/profiles phase",
    )
    
    parser.add_argument(
        "--skip-universes",
        action="store_true",
        help="Skip universe selection phase",
    )
    
    parser.add_argument(
        "--skip-books",
        action="store_true",
        help="Skip portfolio construction phase",
    )
    
    parser.add_argument(
        "--skip-outcome-eval",
        action="store_true",
        help="Skip outcome evaluation phase",
    )
    
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Enable the EXECUTION phase (target weights → IBKR orders)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execution dry-run: log planned orders without submitting",
    )
    
    parser.add_argument(
        "--paper",
        action="store_true",
        help="Execute against IBKR paper account (port 4002)",
    )
    
    parser.add_argument(
        "--live",
        action="store_true",
        help="Execute against IBKR LIVE account (port 4001) — REAL MONEY",
    )
    
    args = parser.parse_args(argv)
    
    # Validate arguments
    if args.date and (args.start or args.end):
        parser.error("Cannot specify both --date and --start/--end")
    
    if (args.start and not args.end) or (args.end and not args.start):
        parser.error("Both --start and --end must be specified for date range")
    
    if not args.date and not args.start:
        parser.error("Must specify either --date or --start/--end")
    
    # Determine execution mode
    run_execution = args.execute or args.dry_run or args.paper or args.live
    if args.live:
        execution_mode = "live"
    elif args.paper:
        execution_mode = "paper"
    else:
        execution_mode = "dry_run"
    
    # Build config
    config = DailyPipelineConfig(
        region=args.region,
        run_regime=not args.skip_regime,
        run_profiles=not args.skip_profiles,
        run_universes=not args.skip_universes,
        run_books=not args.skip_books,
        run_execution=run_execution,
        execution_mode=execution_mode,
        run_outcome_eval=not args.skip_outcome_eval,
    )
    
    # Initialize
    db_manager = get_db_manager()
    orchestrator = DailyOrchestrator(db_manager=db_manager)
    
    try:
        if args.date:
            # Single date
            logger.info("Running pipeline for date: %s", args.date)
            run = orchestrator.run_pipeline(as_of_date=args.date, config=config)
            logger.info("Pipeline complete: run_id=%s phase=%s", run.run_id, run.phase.name)
            print(f"✓ Pipeline complete for {args.date}: run_id={run.run_id}")
            return 0
        
        else:
            # Date range
            logger.info("Running pipeline for date range: %s to %s", args.start, args.end)
            runs = orchestrator.run_pipeline_for_date_range(
                start_date=args.start,
                end_date=args.end,
                config=config,
            )
            logger.info("Pipeline complete for %d dates", len(runs))
            print(f"✓ Pipeline complete for {len(runs)} dates")
            return 0
    
    except Exception:
        logger.exception("Pipeline failed")
        print("✗ Pipeline failed (see logs for details)", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
