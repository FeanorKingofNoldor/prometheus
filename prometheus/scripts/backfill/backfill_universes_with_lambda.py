"""Backfill equity universes with lambda-based opportunity scores.

This offline research script builds and persists universes of the form
``CORE_EQ_<REGION>`` for each trading day in a given date range using the
same BasicUniverseModel configuration as ``backfill_universes_basic``, but
optionally augments the ranking score with cluster-level lambda
(opportunity-density) forecasts.

It is intended for **research/backfill** only so that ``universe_members``
contains:

- lambda-based diagnostics per member (``lambda_score``,
  ``lambda_score_weight``), and
- alternative universes that can be compared against the baseline
  (no-lambda) universes in backtests and alignment analysis.

The script does **not** alter the live pipeline; it is purely a helper for
research runs.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.data.reader import DataReader
from apathis.profiles import (
    ProfileService,
    ProfileStorage,
    ProfileFeatureBuilder,
    RoutedProfileFeatureBuilder,
    SovereignProfileFeatureBuilder,
    BasicProfileEmbedder,
)
from apathis.stability import StabilityStorage
from prometheus.universe import UniverseEngine, UniverseStorage, BasicUniverseModel
from prometheus.universe.config import UniverseConfig
from apathis.core.markets import MARKETS_BY_REGION
from prometheus.opportunity.lambda_provider import CsvLambdaClusterScoreProvider


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
            "Backfill basic equity universes CORE_EQ_<REGION> into universe_members "
            "for each trading day in a date range, using BasicUniverseModel "
            "augmented with lambda-based opportunity scores from a predictions CSV."
        ),
    )

    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD) for as_of_date range",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD) for as_of_date range",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="US",
        help="Region code (e.g. US, EU, ASIA). Default: US",
    )
    parser.add_argument(
        "--max-universe-size",
        type=int,
        default=200,
        help="Maximum number of included instruments per day (default: 200)",
    )
    parser.add_argument(
        "--min-liquidity-adv",
        type=float,
        default=100_000.0,
        help="Minimum average daily volume (ADV) for inclusion (default: 100000)",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=1.0,
        help="Minimum last close price for inclusion (default: 1.0)",
    )
    parser.add_argument(
        "--lambda-predictions",
        type=str,
        required=True,
        help=(
            "Path to lambda predictions CSV produced by "
            "run_opportunity_density_experiment.py with --predictions-output."
        ),
    )
    parser.add_argument(
        "--experiment-id",
        type=str,
        required=True,
        help="Experiment ID to filter lambda predictions by",
    )
    parser.add_argument(
        "--lambda-weight",
        type=float,
        default=10.0,
        help=(
            "Weight applied to lambda_score when added to the universe "
            "ranking score (default: 10.0). Effective contribution per "
            "instrument is lambda_weight * lambda_score."
        ),
    )

    args = parser.parse_args(argv)

    start_date: date = args.start
    end_date: date = args.end
    if end_date < start_date:
        parser.error("--end must be >= --start")

    region = args.region.upper()
    markets = MARKETS_BY_REGION.get(region)
    if not markets:
        parser.error(f"No MARKETS_BY_REGION mapping for region {region!r}")

    universe_id = f"CORE_EQ_{region}"

    preds_path = Path(args.lambda_predictions)
    if not preds_path.exists():
        raise SystemExit(f"Lambda predictions CSV not found: {preds_path}")

    logger.info(
        "Backfilling lambda-aware universes for region=%s markets=%s start=%s end=%s universe_id=%s",
        region,
        markets,
        start_date,
        end_date,
        universe_id,
    )

    config = get_config()
    db_manager = DatabaseManager(config)

    calendar = TradingCalendar()
    reader = DataReader(db_manager=db_manager)

    # Profiles and STAB storage reused to configure the universe model.
    profile_storage = ProfileStorage(db_manager=db_manager)

    company_builder = ProfileFeatureBuilder(
        db_manager=db_manager,
        data_reader=reader,
        calendar=calendar,
    )
    sovereign_builder = SovereignProfileFeatureBuilder(db_manager=db_manager)
    feature_builder = RoutedProfileFeatureBuilder(
        db_manager=db_manager,
        builders_by_issuer_type={
            "COMPANY": company_builder,
            "SOVEREIGN": sovereign_builder,
        },
        default_builder=company_builder,
    )

    embedder = BasicProfileEmbedder(embedding_dim=16)
    profile_service = ProfileService(
        storage=profile_storage,
        feature_builder=feature_builder,
        embedder=embedder,
    )

    stab_storage = StabilityStorage(db_manager=db_manager)
    universe_storage = UniverseStorage(db_manager=db_manager)

    universe_config = UniverseConfig(
        strategy_id=f"{region}_CORE_LONG_EQ",
        markets=list(markets),
        max_universe_size=args.max_universe_size,
        min_liquidity_adv=args.min_liquidity_adv,
        min_price=args.min_price,
        sector_max_names=0,
        universe_model_id="basic-equity-v1-lambda-research",
    )

    # Build a lambda cluster score provider once; it will serve all dates.
    lambda_provider = CsvLambdaClusterScoreProvider(
        csv_path=preds_path,
        experiment_id=args.experiment_id,
        score_column="lambda_hat",
    )

    universe_model = BasicUniverseModel(
        db_manager=db_manager,
        calendar=calendar,
        data_reader=reader,
        profile_service=profile_service,
        stability_storage=stab_storage,
        market_ids=tuple(universe_config.markets),
        min_avg_volume=universe_config.min_liquidity_adv,
        max_universe_size=universe_config.max_universe_size,
        sector_max_names=universe_config.sector_max_names,
        min_price=universe_config.min_price,
        hard_exclusion_list=tuple(universe_config.hard_exclusion_list),
        issuer_exclusion_list=tuple(universe_config.issuer_exclusion_list),
        # Keep Assessment integration enabled so that any existing
        # instrument_scores can influence ranking where available.
        use_assessment_scores=True,
        assessment_strategy_id=universe_config.strategy_id,
        assessment_horizon_days=21,
        # Lambda opportunity integration (research-only for now).
        lambda_score_provider=lambda_provider,
        lambda_score_weight=float(args.lambda_weight),
    )

    universe_engine = UniverseEngine(model=universe_model, storage=universe_storage)

    # Enumerate trading days and backfill.
    all_days = calendar.trading_days_between(start_date, end_date)
    if not all_days:
        logger.warning("No trading days between %s and %s; nothing to do", start_date, end_date)
        return

    for as_of in all_days:
        logger.info("Building lambda-aware universe %s for as_of_date=%s", universe_id, as_of)
        members = universe_engine.build_and_save(as_of, universe_id)
        logger.info(
            "Backfill (lambda-aware): as_of=%s universe=%s total_members=%d included=%d",
            as_of,
            universe_id,
            len(members),
            sum(1 for m in members if m.included),
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
