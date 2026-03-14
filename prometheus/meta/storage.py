"""Prometheus v2 – Meta-Orchestrator storage helpers.

This module provides a thin abstraction around the runtime database for
recording engine decisions/outcomes and reading backtest metrics used by
:class:`MetaOrchestrator`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from psycopg2.extras import Json

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from prometheus.meta.types import BacktestRunRecord, DecisionOutcome, EngineDecision


logger = get_logger(__name__)


@dataclass
class MetaStorage:
    """Storage façade for Meta-Orchestrator related tables."""

    db_manager: DatabaseManager

    # ------------------------------------------------------------------
    # Engine decisions & outcomes
    # ------------------------------------------------------------------

    def save_engine_decision(self, decision: EngineDecision) -> None:
        """Insert a row into ``engine_decisions``.

        This is not yet used heavily in backtests but provides a concrete
        persistence surface for future orchestration tasks.
        """

        sql = """
            INSERT INTO engine_decisions (
                decision_id,
                engine_name,
                run_id,
                strategy_id,
                market_id,
                as_of_date,
                config_id,
                input_refs,
                output_refs,
                metadata,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        decision.decision_id,
                        decision.engine_name,
                        decision.run_id,
                        decision.strategy_id,
                        decision.market_id,
                        decision.as_of_date,
                        decision.config_id,
                        Json(decision.input_refs or {}),
                        Json(decision.output_refs or {}),
                        Json(decision.metadata or {}),
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

    def save_decision_outcome(self, outcome: DecisionOutcome) -> None:
        """Insert a row into ``decision_outcomes``."""

        sql = """
            INSERT INTO decision_outcomes (
                decision_id,
                horizon_days,
                realized_return,
                realized_pnl,
                realized_drawdown,
                realized_vol,
                metadata,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    sql,
                    (
                        outcome.decision_id,
                        outcome.horizon_days,
                        outcome.realized_return,
                        outcome.realized_pnl,
                        outcome.realized_drawdown,
                        outcome.realized_vol,
                        Json(outcome.metadata or {}),
                    ),
                )
                conn.commit()
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # Backtest metrics
    # ------------------------------------------------------------------

    def load_backtest_runs_for_strategy(self, strategy_id: str) -> List[BacktestRunRecord]:
        """Return backtest runs and metrics for a given strategy_id.

        Only runs with a non-null metrics_json are returned.
        """

        sql = """
            SELECT run_id,
                   strategy_id,
                   universe_id,
                   config_json,
                   metrics_json
            FROM backtest_runs
            WHERE strategy_id = %s
              AND metrics_json IS NOT NULL
        """

        with self.db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (strategy_id,))
                rows = cursor.fetchall()
            finally:
                cursor.close()

        records: List[BacktestRunRecord] = []
        for run_id, strat_id, universe_id, config_json, metrics_json in rows:
            config = config_json or {}
            metrics = metrics_json or {}
            records.append(
                BacktestRunRecord(
                    run_id=str(run_id),
                    strategy_id=str(strat_id),
                    universe_id=str(universe_id) if universe_id is not None else None,
                    config=config,
                    metrics=metrics,
                )
            )
        return records