"""Prometheus v2 – Outcome Evaluation Backfill.

Evaluates all pending decision outcomes across history and builds
prediction scorecards. Runs three passes:

1. Fixed-horizon evaluation: 5d and 21d outcomes for PORTFOLIO + ASSESSMENT decisions
2. Exit-triggered evaluation: holding-period returns for dropped instruments
3. Prediction scorecard: assessment score accuracy vs realized returns

Usage::

    python -m prometheus.scripts.run.run_outcome_backfill
    python -m prometheus.scripts.run.run_outcome_backfill --max-decisions 500
    python -m prometheus.scripts.run.run_outcome_backfill --scorecard-only
"""

from __future__ import annotations

import argparse
from datetime import date

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

from prometheus.decisions import OutcomeEvaluator
from prometheus.decisions.scorecard import PredictionScorecard

logger = get_logger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Backfill decision outcomes and build scorecard")
    parser.add_argument("--date", type=str, default=date.today().isoformat(), help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--max-decisions", type=int, default=2000, help="Max decisions per horizon pass")
    parser.add_argument("--max-exit-pairs", type=int, default=1000, help="Max consecutive portfolio pairs for exit eval")
    parser.add_argument("--workers", type=int, default=12, help="Thread pool size for parallel evaluation")
    parser.add_argument("--scorecard-only", action="store_true", help="Skip outcome evaluation, only build scorecard")
    parser.add_argument("--scorecard-decisions", type=int, default=200, help="Max decisions for scorecard")
    args = parser.parse_args(argv)

    as_of = date.fromisoformat(args.date)
    db = get_db_manager()

    if not args.scorecard_only:
        evaluator = OutcomeEvaluator(db_manager=db)

        # Pass 1: Fixed-horizon evaluation (5d, 21d, 63d — handled internally)
        logger.info("=== Pass 1: Fixed-horizon evaluation ===")
        count = evaluator.evaluate_pending_outcomes(
            as_of_date=as_of,
            max_decisions=args.max_decisions,
            num_workers=args.workers,
        )
        logger.info("Evaluated %d fixed-horizon outcomes", count)

        # Pass 2: Exit-triggered evaluation
        logger.info("=== Pass 2: Exit-triggered evaluation ===")
        exit_count = evaluator.evaluate_exit_outcomes(
            as_of_date=as_of,
            max_pairs=args.max_exit_pairs,
            num_workers=args.workers,
        )
        logger.info("Evaluated %d exit-triggered outcomes", exit_count)

    # Pass 3: Prediction scorecard
    logger.info("=== Pass 3: Prediction scorecard ===")
    scorecard = PredictionScorecard(db_manager=db)

    for horizon in [5, 21]:
        report = scorecard.build_scorecard(
            horizon_days=horizon,
            max_decisions=args.scorecard_decisions,
        )

        print(f"\n{'='*60}")
        print(f"PREDICTION SCORECARD ({horizon}d horizon)")
        print(f"{'='*60}")
        print(f"Date range       : {report.date_range[0]} → {report.date_range[1]}")
        print(f"Total predictions: {report.total_predictions:,}")
        print(f"Hit rate         : {report.hit_rate:.1%}")
        print(f"Spearman ρ       : {report.spearman_rho:.4f}")
        print(f"Avg predicted    : {report.avg_predicted_score:.4f}")
        print(f"Avg realized     : {report.avg_realized_return:.4f}")

        if report.sector_breakdown:
            print("\nSector Breakdown (worst → best):")
            print(f"  {'Sector':<25} {'Hit%':>6} {'AvgErr':>8} {'Count':>6}")
            for s in report.sector_breakdown[:15]:
                print(f"  {s.sector:<25} {s.hit_rate:>5.1%} {s.avg_error:>+8.4f} {s.count:>6}")

        if report.top_misses:
            print("\nTop Misses (wrong direction, largest error):")
            print(f"  {'Instrument':<15} {'Date':>12} {'Predicted':>10} {'Realized':>10} {'Sector':<20}")
            for m in report.top_misses[:10]:
                print(
                    f"  {m.instrument_id:<15} {m.as_of_date!s:>12} "
                    f"{m.predicted_score:>+10.4f} {m.realized_return:>+10.4f} {m.sector:<20}"
                )

    print("\nDone.")


if __name__ == "__main__":
    main()
