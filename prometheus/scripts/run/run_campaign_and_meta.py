"""Prometheus v2 – Backtest campaign + Meta-Orchestrator CLI.

This script runs a canonical sleeve backtest campaign for a given
strategy/market over a date range and then invokes the Meta-Orchestrator
to record a sleeve-selection decision.

Example
-------

    python -m prometheus.scripts.run_campaign_and_meta \
        --strategy-id US_EQ_CORE_LONG_EQ \
        --market-id US_EQ \
        --start 2024-01-01 \
        --end 2024-03-31 \
        --top-k 2
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

from prometheus.pipeline.tasks import run_backtest_campaign_and_meta_for_strategy

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
            "Run a canonical multi-sleeve backtest campaign for a strategy "
            "and record a Meta-Orchestrator decision over the results."
        ),
    )

    parser.add_argument("--strategy-id", type=str, required=True, help="Logical strategy identifier")
    parser.add_argument("--market-id", type=str, required=True, help="Market identifier (e.g. US_EQ)")
    parser.add_argument("--start", type=_parse_date, required=True, help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, required=True, help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument(
        "--top-k",
        type=int,
        default=3,
        help="Number of top sleeves for Meta-Orchestrator to select (default: 3)",
    )
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=1_000_000.0,
        help="Initial cash per sleeve in the backtest (default: 1,000,000)",
    )
    parser.add_argument(
        "--disable-risk",
        action="store_true",
        help=(
            "Disable Risk Management adjustments inside the sleeve pipeline "
            "when running the backtest campaign (risk-off baseline)"
        ),
    )
    parser.add_argument(
        "--assessment-backend",
        type=str,
        choices=["basic", "context"],
        default="basic",
        help=(
            "Assessment backend used inside the sleeve pipeline: 'basic' "
            "(price/STAB-based) or 'context' (joint Assessment context "
            "embeddings). Applies to all sleeves in this campaign."
        ),
    )
    parser.add_argument(
        "--assessment-use-joint-context",
        action="store_true",
        help=(
            "If set and --assessment-backend=basic, enable joint Assessment "
            "context diagnostics (ASSESSMENT_CTX_V0) inside the basic model."
        ),
    )
    parser.add_argument(
        "--assessment-context-model-id",
        type=str,
        default="joint-assessment-context-v1",
        help=(
            "Joint Assessment context model_id in joint_embeddings "
            "(default: joint-assessment-context-v1)."
        ),
    )
    parser.add_argument(
        "--assessment-model-id",
        type=str,
        default=None,
        help=(
            "Assessment model identifier used for persistence/tracing in "
            "instrument_scores (default: assessment-basic-v1 for basic "
            "backend, assessment-context-v1 for context backend)."
        ),
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    summaries, decision_id = run_backtest_campaign_and_meta_for_strategy(
        db_manager=db_manager,
        strategy_id=args.strategy_id,
        market_id=args.market_id,
        start_date=args.start,
        end_date=args.end,
        top_k=args.top_k,
        initial_cash=args.initial_cash,
        apply_risk=not args.disable_risk,
        assessment_backend=args.assessment_backend,
        assessment_use_joint_context=args.assessment_use_joint_context,
        assessment_context_model_id=args.assessment_context_model_id,
        assessment_model_id=args.assessment_model_id,
    )

    if not summaries:
        print("No sleeves were run; check strategy/market configuration.")
        return

    print("run_id,sleeve_id,strategy_id,cumulative_return,annualised_sharpe,max_drawdown")
    for s in summaries:
        metrics = s.metrics or {}
        cumret = float(metrics.get("cumulative_return", 0.0))
        sharpe = float(metrics.get("annualised_sharpe", 0.0))
        maxdd = float(metrics.get("max_drawdown", 0.0))
        print(
            f"{s.run_id},{s.sleeve_id},{s.strategy_id},{cumret:.6f},{sharpe:.4f},{maxdd:.6f}",
        )

    print()
    if decision_id is None:
        print("Meta-Orchestrator did not record a decision (no eligible sleeves).")
    else:
        print(f"Meta-Orchestrator recorded decision_id={decision_id}")


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
