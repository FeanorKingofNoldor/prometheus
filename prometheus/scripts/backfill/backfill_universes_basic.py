"""Backfill basic equity universes over a date range.

This offline script builds and persists universes of the form
``CORE_EQ_<REGION>`` for each trading day in a given date range using the
same BasicUniverseModel configuration as the pipeline's
``run_universes_for_run`` task, but without going through the full
EngineRun state machine.

It is intended for research/backfill purposes so that
``universe_members`` has coverage over a historical window, enabling
joins with lambda forecasts, backtests, etc.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.core.markets import MARKETS_BY_REGION
from apatheon.core.time import TradingCalendar
from apatheon.data.reader import DataReader
from apatheon.profiles import (
    BasicProfileEmbedder,
    ProfileFeatureBuilder,
    ProfileService,
    ProfileStorage,
    RoutedProfileFeatureBuilder,
    SovereignProfileFeatureBuilder,
)
from apatheon.stability import StabilityStorage

from prometheus.universe import BasicUniverseModel, UniverseEngine, UniverseStorage
from prometheus.universe.config import UniverseConfig

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
            "for each trading day in a date range, using BasicUniverseModel."
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

    logger.info(
        "Backfilling universes for region=%s markets=%s start=%s end=%s universe_id=%s",
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
        universe_model_id="basic-equity-v1",
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
        # For backfill we keep Assessment integration enabled so that any
        # existing instrument_scores can influence ranking where available.
        use_assessment_scores=True,
        assessment_strategy_id=universe_config.strategy_id,
        assessment_horizon_days=21,
    )
    universe_engine = UniverseEngine(model=universe_model, storage=universe_storage)

    # Enumerate trading days and backfill.
    all_days = calendar.trading_days_between(start_date, end_date)
    if not all_days:
        logger.warning("No trading days between %s and %s; nothing to do", start_date, end_date)
        return

    for as_of in all_days:
        logger.info("Building universe %s for as_of_date=%s", universe_id, as_of)
        members = universe_engine.build_and_save(as_of, universe_id)
        logger.info(
            "Backfill: as_of=%s universe=%s total_members=%d included=%d",
            as_of,
            universe_id,
            len(members),
            sum(1 for m in members if m.included),
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
