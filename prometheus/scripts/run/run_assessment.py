"""Prometheus v2 – Assessment Engine CLI.

This script wires together the BasicAssessmentModel, AssessmentEngine,
and InstrumentScoreStorage to score a list of instruments for a given
strategy/market/as-of date and persist results into ``instrument_scores``.

Example
-------

    python -m prometheus.scripts.run_assessment \
        --strategy-id CORE_LONG_EQ \
        --market-id US_EQ \
        --instrument-id AAPL.US MSFT.US \
        --as-of 2025-11-21 \
        --horizon-days 21
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.data.reader import DataReader
from apathis.stability import StabilityStorage
from prometheus.assessment import AssessmentEngine
from prometheus.assessment.model_basic import BasicAssessmentModel
from prometheus.assessment.model_context import ContextAssessmentModel
from prometheus.assessment.storage import InstrumentScoreStorage


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run Basic Assessment Engine for a list of instruments",
    )

    parser.add_argument(
        "--strategy-id",
        type=str,
        required=True,
        help="Strategy identifier for which scores are computed (e.g. CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        required=True,
        help="Market identifier (e.g. US_EQ)",
    )
    parser.add_argument(
        "--instrument-id",
        dest="instrument_ids",
        nargs="+",
        type=str,
        required=True,
        help="One or more instrument identifiers to score",
    )
    parser.add_argument(
        "--as-of",
        dest="as_of",
        type=_parse_date,
        required=True,
        help="As-of date for assessment (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=21,
        help="Prediction horizon in trading days (default: 21)",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default="assessment-basic-v1",
        help=(
            "Assessment model identifier used for persistence/tracing "
            "(default: assessment-basic-v1)."
        ),
    )
    parser.add_argument(
        "--backend",
        type=str,
        choices=["basic", "context"],
        default="basic",
        help=(
            "Assessment model backend: 'basic' (price/STAB-based) or 'context' "
            "(joint Assessment context embeddings)."
        ),
    )
    parser.add_argument(
        "--use-joint-context",
        action="store_true",
        help=(
            "If set with backend=basic, attempt to look up joint Assessment "
            "context embeddings (ASSESSMENT_CTX_V0 / joint-assessment-context-v1) "
            "and record a simple norm diagnostic in score metadata."
        ),
    )
    parser.add_argument(
        "--assessment-context-model-id",
        type=str,
        default="joint-assessment-context-v1",
        help=(
            "Model id in joint_embeddings to use for Assessment context "
            "embeddings (default: joint-assessment-context-v1)."
        ),
    )

    args = parser.parse_args(argv)

    db_manager = get_db_manager()

    reader = DataReader(db_manager=db_manager)
    calendar = TradingCalendar()
    stab_storage = StabilityStorage(db_manager=db_manager)

    if args.backend == "basic":
        model = BasicAssessmentModel(
            data_reader=reader,
            calendar=calendar,
            stability_storage=stab_storage,
            db_manager=db_manager,
            use_assessment_context=args.use_joint_context,
            assessment_context_model_id=args.assessment_context_model_id,
        )
    else:
        model = ContextAssessmentModel(
            db_manager=db_manager,
            assessment_context_model_id=args.assessment_context_model_id,
        )

    storage = InstrumentScoreStorage(db_manager=db_manager)
    engine = AssessmentEngine(model=model, storage=storage, model_id=args.model_id)

    scores = engine.score_universe(
        strategy_id=args.strategy_id,
        market_id=args.market_id,
        instrument_ids=args.instrument_ids,
        as_of_date=args.as_of,
        horizon_days=args.horizon_days,
    )

    logger.info(
        "Assessment run complete: strategy=%s market=%s date=%s horizon=%d instruments=%d",
        args.strategy_id,
        args.market_id,
        args.as_of,
        args.horizon_days,
        len(scores),
    )

    # Print a concise summary of top and bottom names by score.
    if not scores:
        print("No instruments scored")
        return

    sorted_scores = sorted(scores.values(), key=lambda s: s.score, reverse=True)
    top = sorted_scores[:5]
    bottom = sorted_scores[-5:][::-1] if len(sorted_scores) > 5 else []

    print("Top names by assessment score:")
    for s in top:
        print(
            f"  {s.instrument_id}: score={s.score:.3f}, expected_return={s.expected_return:.3f}, "
            f"confidence={s.confidence:.2f}, label={s.signal_label}"
        )

    if bottom:
        print("\nBottom names by assessment score:")
        for s in bottom:
            print(
                f"  {s.instrument_id}: score={s.score:.3f}, expected_return={s.expected_return:.3f}, "
                f"confidence={s.confidence:.2f}, label={s.signal_label}"
            )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
