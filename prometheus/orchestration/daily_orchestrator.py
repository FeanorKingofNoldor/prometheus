"""Prometheus v2 – Daily Pipeline Orchestrator.

This module orchestrates the complete daily trading pipeline:
1. Data ingestion verification
2. Regime detection
3. STAB scoring
4. Universe selection (with decision tracking)
5. Assessment scoring (with decision tracking)
6. Portfolio construction (with decision tracking)
7. Risk management
8. Execution planning (with decision tracking)
9. Outcome evaluation (for past decisions)

The orchestrator is idempotent and uses phase-based checkpointing via
the existing EngineRun state machine.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.logging import get_logger

from prometheus.decisions import DecisionTracker, OutcomeEvaluator
from prometheus.pipeline.state import EngineRun, RunPhase, get_or_create_run
from prometheus.pipeline.tasks import (
    ExecutionConfig,
    _load_sector_health_for_date,
    run_books_for_run,
    run_execution_for_run,
    run_signals_for_run,
    run_universes_for_run,
)

logger = get_logger(__name__)


@dataclass
class DailyPipelineConfig:
    """Configuration for daily pipeline execution.

    Attributes:
        region: Region to run (e.g., "US")
        market_id: Market identifier (e.g., "US_EQ")
        run_regime: Whether to run regime detection
        run_profiles: Whether to run STAB/profiles
        run_universes: Whether to run universe selection
        run_books: Whether to run portfolio construction
        run_execution: Whether to run the IBKR execution phase
        execution_mode: Execution mode ("dry_run", "paper", "live")
        execution_max_orders: Max orders before aborting execution
        run_outcome_eval: Whether to evaluate pending outcomes
        outcome_eval_max_decisions: Max decisions to evaluate
    """

    region: str = "US"
    market_id: str = "US_EQ"
    run_regime: bool = True
    run_profiles: bool = True
    run_universes: bool = True
    run_books: bool = True
    run_execution: bool = True
    execution_mode: str = "paper"
    execution_max_orders: int = 50
    run_options: bool = True
    options_mode: str = "paper"
    options_derivatives_budget_pct: float = 0.15
    run_outcome_eval: bool = True
    outcome_eval_max_decisions: int = 100


@dataclass
class DailyOrchestrator:
    """Orchestrator for complete daily trading pipeline.

    Usage:
        orchestrator = DailyOrchestrator(db_manager=db)
        run = orchestrator.run_pipeline(
            as_of_date=date(2024, 12, 15),
            config=DailyPipelineConfig()
        )
    """

    db_manager: DatabaseManager

    def __post_init__(self) -> None:
        self._tracker = DecisionTracker(db_manager=self.db_manager)
        self._evaluator = OutcomeEvaluator(db_manager=self.db_manager)

    def run_pipeline(
        self,
        as_of_date: date,
        config: DailyPipelineConfig | None = None,
    ) -> EngineRun:
        """Run the complete daily pipeline for a given date.

        This method is idempotent - it can be run multiple times for the
        same date and will resume from the last completed phase.

        Args:
            as_of_date: Date to run the pipeline for
            config: Optional configuration (uses defaults if not provided)

        Returns:
            EngineRun object with final phase status
        """
        if config is None:
            config = DailyPipelineConfig()

        logger.info(
            "Starting daily pipeline: as_of_date=%s region=%s",
            as_of_date,
            config.region,
        )

        # Get or create engine run (idempotent)
        run = get_or_create_run(
            db_manager=self.db_manager,
            as_of_date=as_of_date,
            region=config.region,
        )

        logger.info(
            "Pipeline run_id=%s current_phase=%s",
            run.run_id,
            run.phase.name,
        )

        # Execute phases in order (each phase checks if already complete)

        # SIGNALS phase combines regime + STAB/profiles
        if (config.run_regime or config.run_profiles) and run.phase < RunPhase.SIGNALS_DONE:
            logger.info("Running SIGNALS phase (regime + STAB)...")
            run = run_signals_for_run(self.db_manager, run)
            logger.info("SIGNALS phase complete")

        if config.run_universes and run.phase < RunPhase.UNIVERSES_DONE:
            logger.info("Running UNIVERSES phase...")
            run = run_universes_for_run(self.db_manager, run)
            logger.info("UNIVERSES phase complete")

        if config.run_books and run.phase < RunPhase.BOOKS_DONE:
            logger.info("Running BOOKS phase (portfolio construction)...")
            run = run_books_for_run(self.db_manager, run)
            logger.info("BOOKS phase complete")

        if config.run_execution and run.phase < RunPhase.EXECUTION_DONE:
            logger.info("Running EXECUTION phase (mode=%s)...", config.execution_mode)
            exec_cfg = ExecutionConfig(
                mode=config.execution_mode,
                max_orders=config.execution_max_orders,
            )
            run = run_execution_for_run(
                self.db_manager, run, execution_config=exec_cfg,
            )
            logger.info("EXECUTION phase complete")

        if config.run_options and run.phase < RunPhase.OPTIONS_DONE:
            logger.info("Running OPTIONS phase (mode=%s)...", config.options_mode)
            from prometheus.pipeline.tasks import OptionsExecutionConfig, run_options_for_run
            opts_cfg = OptionsExecutionConfig(
                mode=config.options_mode,
                derivatives_budget_pct=config.options_derivatives_budget_pct,
            )
            try:
                run = run_options_for_run(
                    self.db_manager, run, options_config=opts_cfg,
                )
                logger.info("OPTIONS phase complete")
            except Exception:  # pragma: no cover - non-fatal
                logger.exception("OPTIONS phase failed (non-blocking)")

        # Outcome evaluation (independent phase - doesn't block on errors)
        if config.run_outcome_eval:
            logger.info("Running OUTCOME_EVAL phase...")
            try:
                evaluated_count = self._evaluator.evaluate_pending_outcomes(
                    as_of_date=as_of_date,
                    max_decisions=config.outcome_eval_max_decisions,
                )
                logger.info("OUTCOME_EVAL phase complete: evaluated %d outcomes", evaluated_count)
            except Exception:  # pragma: no cover - defensive
                logger.exception("OUTCOME_EVAL phase failed (non-blocking)")

        # Meta feedback loop — analyze recent decision outcomes
        try:
            from prometheus.meta.feedback import compute_feedback_report
            feedback = compute_feedback_report(self.db_manager, as_of_date)
            for insight in feedback.insights:
                if insight.severity in ("warning", "critical"):
                    logger.warning(
                        "META FEEDBACK [%s] %s: %s",
                        insight.severity.upper(), insight.category, insight.message,
                    )
        except Exception:
            logger.debug("Meta feedback analysis failed (non-critical)", exc_info=True)

        # Trade journal — backfill returns for past entries
        try:
            from prometheus.meta.trade_journal import backfill_journal_returns, ensure_trade_journal_table
            ensure_trade_journal_table(self.db_manager)
            backfill_journal_returns(self.db_manager, as_of_date)
        except Exception:
            logger.debug("Trade journal backfill failed (non-critical)", exc_info=True)

        # ------------------------------------------------------------------
        # Daily summary
        # ------------------------------------------------------------------

        summary = self._build_daily_summary(as_of_date, run, config)
        logger.info(
            "Daily pipeline complete: run_id=%s final_phase=%s",
            run.run_id,
            run.phase.name,
        )
        for line in summary:
            logger.info("  %s", line)

        # Append to daily logfile
        self._write_daily_logfile(as_of_date, run, summary)

        return run

    def _build_daily_summary(
        self,
        as_of_date: date,
        run: EngineRun,
        config: DailyPipelineConfig,
    ) -> list[str]:
        """Build a structured daily summary as a list of log lines."""
        lines: list[str] = []
        lines.append(f"=== Daily Summary {as_of_date} ===")
        lines.append(f"Run ID: {run.run_id}")
        lines.append(f"Final Phase: {run.phase.name}")
        lines.append(f"Region: {config.region}")

        # Sector health snapshot
        try:
            sector_scores = _load_sector_health_for_date(self.db_manager, as_of_date)
            if sector_scores:
                sick = [s for s, sc in sector_scores.items() if sc < 0.25]
                weak = [s for s, sc in sector_scores.items() if 0.25 <= sc < 0.40]
                healthy = [s for s, sc in sector_scores.items() if sc >= 0.40]
                lines.append(f"Sector Health: {len(sector_scores)} sectors | "
                             f"sick={len(sick)} weak={len(weak)} healthy={len(healthy)}")
                if sick:
                    lines.append(f"  Sick sectors: {', '.join(sick)}")
        except Exception:
            pass

        # Portfolio stats from target_portfolios
        try:
            portfolio_id = f"{config.region.upper()}_CORE_LONG_EQ"
            sql = """
                SELECT target_positions
                FROM target_portfolios
                WHERE portfolio_id = %s AND as_of_date = %s
                ORDER BY created_at DESC LIMIT 1
            """
            with self.db_manager.get_runtime_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(sql, (portfolio_id, as_of_date))
                    row = cursor.fetchone()
                finally:
                    cursor.close()

            if row and row[0]:
                raw = row[0]
                if isinstance(raw, str):
                    import json
                    raw = json.loads(raw)
                weights = raw.get("weights", raw) if isinstance(raw, dict) else {}
                n_pos = len([w for w in weights.values() if w])
                net_exp = sum(float(w) for w in weights.values())
                lines.append(f"Portfolio: {n_pos} positions | net_exposure={net_exp:.2%}")
        except Exception:
            pass

        lines.append(f"Execution: mode={config.execution_mode} enabled={config.run_execution}")
        return lines

    def _write_daily_logfile(
        self,
        as_of_date: date,
        run: EngineRun,
        summary: list[str],
    ) -> None:
        """Append daily summary to a persistent logfile."""
        from datetime import datetime, timezone
        from pathlib import Path

        log_dir = Path.home() / "prometheus_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"daily_{as_of_date.isoformat()}.log"

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(log_path, "a") as f:
            f.write(f"\n--- {timestamp} ---\n")
            for line in summary:
                f.write(f"{line}\n")
            f.write("---\n")

    def run_pipeline_for_date_range(
        self,
        start_date: date,
        end_date: date,
        config: DailyPipelineConfig | None = None,
    ) -> list[EngineRun]:
        """Run pipeline for a range of dates.

        This is useful for backfilling historical runs or catching up
        after downtime.

        Args:
            start_date: First date to run (inclusive)
            end_date: Last date to run (inclusive)
            config: Optional configuration

        Returns:
            List of EngineRun objects, one per date
        """
        if config is None:
            config = DailyPipelineConfig()

        logger.info(
            "Running pipeline for date range: %s to %s",
            start_date,
            end_date,
        )

        runs: list[EngineRun] = []
        from datetime import timedelta

        current = start_date

        while current <= end_date:
            try:
                run = self.run_pipeline(as_of_date=current, config=config)
                runs.append(run)
            except Exception:  # pragma: no cover - continue on error
                logger.exception("Pipeline failed for date=%s, continuing...", current)

            current = current + timedelta(days=1)

        logger.info("Completed pipeline for %d dates", len(runs))

        return runs


def run_daily_pipeline(
    as_of_date: date,
    region: str = "US",
    db_manager: DatabaseManager | None = None,
) -> EngineRun:
    """Convenience function to run daily pipeline.

    Args:
        as_of_date: Date to run pipeline for
        region: Region (default "US")
        db_manager: Optional database manager

    Returns:
        EngineRun object with final phase
    """
    if db_manager is None:
        db_manager = get_db_manager()

    config = DailyPipelineConfig(region=region)
    orchestrator = DailyOrchestrator(db_manager=db_manager)

    return orchestrator.run_pipeline(as_of_date=as_of_date, config=config)
