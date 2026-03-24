"""Prometheus v2  Meta-Orchestrator CLI.

This script runs the Meta-Orchestrator over ``backtest_runs`` for a
single strategy, selects the top-k sleeves by backtest metrics, and
records a decision into the ``engine_decisions`` table.

Example
-------

    python -m prometheus.scripts.run_meta_orchestrator \
        --strategy-id US_CORE_LONG_EQ \
        --as-of 2025-03-31 \
        --top-k 3
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

from prometheus.meta import MetaOrchestrator, MetaStorage
from prometheus.pipeline.tasks import run_meta_for_strategy

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run Meta-Orchestrator over backtest sleeves for a strategy",
    )

    parser.add_argument(
        "--strategy-id",
        type=str,
        required=True,
        help="Strategy identifier whose sleeves should be evaluated (e.g. US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=True,
        help="As-of date for recording the meta decision (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=3,
        help="Number of top sleeves to select (default: 3)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    db_manager = get_db_manager()

    # First compute the top-k sleeves so we can display them in a concise
    # summary after recording the decision.
    storage = MetaStorage(db_manager=db_manager)
    orchestrator = MetaOrchestrator(storage=storage)
    evaluations = orchestrator.select_top_sleeves(args.strategy_id, k=args.top_k)

    if not evaluations:
        logger.info(
            "No candidate sleeves found for strategy_id=%s; no decision recorded",
            args.strategy_id,
        )
        print(f"No backtest runs with metrics found for strategy {args.strategy_id!r}")
        return

    decision_id = run_meta_for_strategy(
        db_manager=db_manager,
        strategy_id=args.strategy_id,
        as_of_date=args.as_of,
        top_k=args.top_k,
    )

    if decision_id is None:
        # This should not normally happen because we already observed
        # non-empty evaluations above, but we guard defensively.
        print(
            f"Meta-Orchestrator did not record a decision for strategy {args.strategy_id!r}",
        )
        return

    logger.info(
        "Meta-Orchestrator decision_id=%s strategy_id=%s top_k=%d",
        decision_id,
        args.strategy_id,
        args.top_k,
    )

    print(f"Recorded Meta-Orchestrator decision {decision_id} for strategy {args.strategy_id!r}")
    print("Selected sleeves (ordered best to worst):")

    for ev in evaluations:
        metrics = ev.metrics or {}
        sharpe = float(metrics.get("annualised_sharpe", 0.0))
        cumret = float(metrics.get("cumulative_return", 0.0))
        maxdd = float(metrics.get("max_drawdown", 0.0))
        print(
            "  sleeve_id={sleeve_id} run_id={run_id} sharpe={sharpe:.4f} "
            "cumret={cumret:.4f} max_drawdown={maxdd:.4f}".format(
                sleeve_id=ev.sleeve_config.sleeve_id,
                run_id=ev.run_id,
                sharpe=sharpe,
                cumret=cumret,
                maxdd=maxdd,
            )
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
