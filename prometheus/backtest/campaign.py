"""Prometheus v2 – Backtest campaign runner.

This module provides a small helper for running *multiple* sleeve-level
backtests for a given strategy over a shared date range, using the
existing :class:`BacktestRunner` and :mod:`prometheus.backtest.sleeve_pipeline`.

The goal is to make it easy to define a catalog of candidate sleeve
configurations, sweep them over a period, and obtain both individual
``backtest_runs`` rows and an in-memory summary suitable for inspection
or Meta-Orchestrator analysis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Sequence

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.data.reader import DataReader

from prometheus.backtest.analyzers import EquityCurveAnalyzer
from prometheus.backtest.config import SleeveConfig
from prometheus.backtest.runner import BacktestRunner
from prometheus.backtest.sleeve_pipeline import (
    build_basic_sleeve_target_and_exposure_fns,
)
from prometheus.execution.backtest_broker import BacktestBroker
from prometheus.execution.market_simulator import FillConfig, MarketSimulator
from prometheus.execution.time_machine import TimeMachine

logger = get_logger(__name__)


@dataclass(frozen=True)
class SleeveRunSummary:
    """Lightweight summary of a single sleeve backtest run."""

    run_id: str
    sleeve_id: str
    strategy_id: str
    start_date: date
    end_date: date
    metrics: Dict[str, float]


def _run_backtest_for_sleeve(
    db_manager: DatabaseManager,
    calendar: TradingCalendar,
    market_id: str,
    start_date: date,
    end_date: date,
    cfg: SleeveConfig,
    initial_cash: float,
    *,
    apply_risk: bool = True,
    lambda_provider: object | None = None,
) -> SleeveRunSummary:
    """Run a single sleeve backtest and return its summary.

    This helper encapsulates the per-sleeve environment construction and
    orchestration used by :func:`run_backtest_campaign` and can also be
    used from parallel workers in CLI code.
    """

    logger.info(
        "run_backtest_campaign: running sleeve_id=%s strategy_id=%s market_id=%s start=%s end=%s",
        cfg.sleeve_id,
        cfg.strategy_id,
        market_id,
        start_date,
        end_date,
    )

    reader = DataReader(db_manager=db_manager)

    time_machine = TimeMachine(
        start_date=start_date,
        end_date=end_date,
        market=market_id,
        data_reader=reader,
        calendar=calendar,
        strict_mode=True,
    )

    simulator = MarketSimulator(
        time_machine=time_machine,
        initial_cash=initial_cash,
        fill_config=FillConfig(market_slippage_bps=0.0),
    )
    broker = BacktestBroker(time_machine=time_machine, simulator=simulator)

    target_fn, exposure_fn = build_basic_sleeve_target_and_exposure_fns(
        db_manager=db_manager,
        calendar=calendar,
        config=cfg,
        broker=broker,
        apply_risk=apply_risk,
        lambda_provider=lambda_provider,
    )

    analyzer = EquityCurveAnalyzer(trading_days_per_year=252)
    runner = BacktestRunner(
        db_manager=db_manager,
        broker=broker,
        equity_analyzer=analyzer,
        target_positions_fn=target_fn,
        exposure_metrics_fn=exposure_fn,
    )

    run_id = runner.run_sleeve(cfg, start_date, end_date)

    # Load the metrics_json we just wrote so callers can inspect results
    # without hitting the DB themselves.
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
            metrics = row[0] if row is not None and row[0] is not None else {}
        finally:
            cursor.close()

    return SleeveRunSummary(
        run_id=run_id,
        sleeve_id=cfg.sleeve_id,
        strategy_id=cfg.strategy_id,
        start_date=start_date,
        end_date=end_date,
        metrics=metrics,
    )


def run_backtest_campaign(
    db_manager: DatabaseManager,
    calendar: TradingCalendar,
    market_id: str,
    start_date: date,
    end_date: date,
    sleeve_configs: Sequence[SleeveConfig],
    initial_cash: float = 1_000_000.0,
    *,
    apply_risk: bool = True,
    lambda_provider: object | None = None,
) -> List[SleeveRunSummary]:
    """Run a set of sleeve backtests over a shared date range.

    For each provided :class:`SleeveConfig`, this helper:

    1. Constructs a :class:`TimeMachine` spanning ``start_date`` to
       ``end_date`` for the given ``market_id``.
    2. Builds a :class:`MarketSimulator` and :class:`BacktestBroker`.
    3. Constructs a :class:`BacktestRunner` whose ``target_positions_fn``
       is driven by :func:`build_basic_sleeve_target_fn`.
    4. Runs the sleeve and records the resulting ``run_id`` and metrics.

    The ``apply_risk`` flag controls whether the Risk Management Service is
    invoked inside the sleeve pipeline. Setting it to ``False`` yields
    "risk-off" baselines using raw portfolio weights.

    Note that each sleeve is currently run in its **own** execution
    environment (TimeMachine + Broker). This keeps interactions between
    sleeves independent and simplifies interpretation of metrics; later
    iterations can add shared book-level constraints if needed.
    """

    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    if not sleeve_configs:
        return []

    summaries: List[SleeveRunSummary] = []

    for cfg in sleeve_configs:
        summary = _run_backtest_for_sleeve(
            db_manager=db_manager,
            calendar=calendar,
            market_id=market_id,
            start_date=start_date,
            end_date=end_date,
            cfg=cfg,
            initial_cash=initial_cash,
            apply_risk=apply_risk,
            lambda_provider=lambda_provider,
        )
        summaries.append(summary)

    return summaries
