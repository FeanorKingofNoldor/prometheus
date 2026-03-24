"""Prometheus v2 – Full-day core pipeline CLI.

This script provides a convenience wrapper around the existing building
blocks to drive a "full day" core pipeline for a single region and
strategy:

1. Ensure an ``engine_runs`` row exists for (as_of_date, region) and
   mark it ``DATA_READY`` if it was still ``WAITING_FOR_DATA``.
2. Advance the run through the state machine until it reaches
   ``COMPLETED`` or ``FAILED``.
3. Run a canonical sleeve backtest campaign + Meta-Orchestrator for a
   given (strategy_id, market_id) over a date range.

It is intentionally thin: ingestion still happens via the dedicated
ingestion scripts, and this CLI assumes that all required data are
already present in the databases.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

from prometheus.pipeline.state import EngineRun, RunPhase, get_or_create_run, update_phase
from prometheus.pipeline.tasks import advance_run, run_backtest_campaign_and_meta_for_strategy

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _assert_runtime_tables_present(db_manager: DatabaseManager) -> None:
    """Ensure core runtime tables exist before running the pipeline.

    This performs a lightweight check using ``to_regclass`` to verify that
    execution and risk tables have been created by Alembic migrations.
    If any are missing, a RuntimeError is raised with a clear message so
    the CLI can fail fast instead of surfacing raw database errors.
    """

    required_tables = (
        "risk_actions",
        "orders",
        "fills",
        "positions_snapshots",
    )

    missing: list[str] = []
    sql = "SELECT to_regclass(%s)"

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for table in required_tables:
                cursor.execute(sql, (table,))
                row = cursor.fetchone()
                if row is None or row[0] is None:
                    missing.append(table)
        finally:
            cursor.close()

    if missing:
        raise RuntimeError(
            "Missing runtime tables: "
            + ", ".join(sorted(missing))
            + ". Make sure ALEMBIC_DB=runtime alembic upgrade 0020 has been run.",
        )


def _ensure_and_advance_engine_run(
    db_manager: DatabaseManager,
    as_of: date,
    region: str,
    *,
    max_steps: int = 8,
) -> EngineRun:
    """Ensure an engine run exists and advance it until terminal.

    This helper is a thin wrapper over
    :func:`get_or_create_run`, :func:`update_phase`, and
    :func:`advance_run`. It will:

    1. Ensure a run exists for (as_of, region).
    2. If the phase is ``WAITING_FOR_DATA``, bump it to ``DATA_READY``.
    3. Call :func:`advance_run` repeatedly until the run reaches a terminal
       phase or ``max_steps`` transitions have been performed.
    """

    run = get_or_create_run(db_manager, as_of, region)
    logger.info(
        "full_day: initial run state run_id=%s as_of=%s region=%s phase=%s",
        run.run_id,
        run.as_of_date,
        run.region,
        run.phase.value,
    )

    if run.phase == RunPhase.WAITING_FOR_DATA:
        run = update_phase(db_manager, run.run_id, RunPhase.DATA_READY)
        logger.info(
            "full_day: marked run_id=%s DATA_READY (was WAITING_FOR_DATA)",
            run.run_id,
        )

    steps = 0
    while run.phase not in {RunPhase.COMPLETED, RunPhase.FAILED} and steps < max_steps:
        run = advance_run(db_manager, run)
        steps += 1
        logger.info(
            "full_day: step=%d run_id=%s phase=%s",
            steps,
            run.run_id,
            run.phase.value,
        )

    if run.phase not in {RunPhase.COMPLETED, RunPhase.FAILED}:
        logger.warning(
            "full_day: run_id=%s did not reach a terminal phase after %d steps (phase=%s)",
            run.run_id,
            steps,
            run.phase.value,
        )

    return run


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a full-day core pipeline: ensure/advance engine_run for a "
            "region/date and then execute a backtest campaign + Meta-Orchestrator "
            "for a strategy."
        ),
    )

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        required=True,
        help="As-of date for the engine run and default campaign end (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="US",
        help="Region code for the engine run (default: US)",
    )
    parser.add_argument(
        "--strategy-id",
        type=str,
        required=True,
        help="Logical strategy identifier for the backtest campaign (e.g. US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        required=True,
        help="Market identifier traded by the strategy (e.g. US_EQ)",
    )
    parser.add_argument(
        "--campaign-start",
        type=_parse_date,
        help=(
            "Start date for the backtest campaign (YYYY-MM-DD). If omitted, "
            "defaults to 90 calendar days before --as-of."
        ),
    )
    parser.add_argument(
        "--campaign-end",
        type=_parse_date,
        help=(
            "End date for the backtest campaign (YYYY-MM-DD). If omitted, "
            "defaults to --as-of."
        ),
    )
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
    # Optional state-aware knobs for the backtest campaign. By default we
    # leave STAB/regime parameters at their sleeve defaults; callers can
    # override them explicitly if desired.
    parser.add_argument(
        "--stability-risk-alpha",
        type=float,
        default=None,
        help=(
            "Optional override for STAB state-change risk alpha inside backtest "
            "universes. If omitted, the sleeve defaults are used."
        ),
    )
    parser.add_argument(
        "--stability-risk-horizon",
        type=int,
        default=None,
        help=(
            "Optional override for STAB state-change risk horizon (in steps) inside "
            "backtest universes. If omitted, the sleeve defaults are used."
        ),
    )
    parser.add_argument(
        "--regime-risk-alpha",
        type=float,
        default=None,
        help=(
            "Optional override for regime state-change risk alpha inside backtest "
            "universes. If omitted, regime risk remains at the sleeve default "
            "(typically 0.0)."
        ),
    )
    # Lambda-aware universes for the backtest campaign.
    parser.add_argument(
        "--lambda-predictions-csv",
        type=str,
        default=None,
        help=(
            "Optional path to a lambda predictions CSV produced by "
            "run_opportunity_density_experiment.py (--predictions-output). When "
            "provided, backtest universes can consume lambda_hat scores."
        ),
    )
    parser.add_argument(
        "--lambda-experiment-id",
        type=str,
        default=None,
        help=(
            "Optional experiment_id filter for the lambda predictions CSV. If "
            "omitted, all rows in the file are used."
        ),
    )
    parser.add_argument(
        "--lambda-score-weight",
        type=float,
        default=10.0,
        help=(
            "Weight applied to lambda scores when lambda predictions are enabled "
            "(default: 10.0). Effective contribution is weight * lambda_score."
        ),
    )
    # Scenario-based risk for backtest portfolios.
    parser.add_argument(
        "--scenario-risk-set-id",
        type=str,
        default=None,
        help=(
            "Optional scenario_set_id used to compute scenario-based portfolio "
            "risk inside the backtest PortfolioEngine. If omitted, scenario risk "
            "is disabled for the campaign."
        ),
    )
    # STAB-scenario diagnostics for backtests.
    parser.add_argument(
        "--stab-scenario-set-id",
        type=str,
        default=None,
        help=(
            "Optional scenario_set_id whose STAB joint embeddings "
            "(STAB_FRAGILITY_V0) should be used to compute portfolio-level "
            "STAB-scenario diagnostics and summarise them into backtest_runs."
        ),
    )
    parser.add_argument(
        "--stab-joint-model-id",
        type=str,
        default="joint-stab-fragility-v1",
        help=(
            "Joint STAB model_id to use when loading instrument/scenario "
            "embeddings for STAB-scenario diagnostics (default: joint-stab-fragility-v1)."
        ),
    )
    parser.add_argument(
        "--skip-engine",
        action="store_true",
        help="Skip engine_run creation/advancement and only run backtest+meta",
    )
    parser.add_argument(
        "--skip-campaign",
        action="store_true",
        help="Skip backtest campaign + Meta-Orchestrator and only advance engine_run",
    )
    parser.add_argument(
        "--max-engine-steps",
        type=int,
        default=8,
        help="Maximum number of phase transitions to perform for the engine run (default: 8)",
    )

    args = parser.parse_args(argv)

    if args.top_k <= 0:
        parser.error("--top-k must be positive")

    if args.max_engine_steps <= 0:
        parser.error("--max-engine-steps must be positive")

    if args.skip_engine and args.skip_campaign:
        parser.error("--skip-engine and --skip-campaign cannot both be set")

    as_of: date = args.as_of
    campaign_end: date = args.campaign_end or as_of
    campaign_start: date = args.campaign_start or (campaign_end - timedelta(days=90))

    if campaign_end < campaign_start:
        parser.error("--campaign-end must be >= --campaign-start (after defaults are applied)")

    config = get_config()
    db_manager = DatabaseManager(config)

    # Quick preflight: ensure the runtime DB has the core execution and
    # risk tables. If not, fail fast with a clear message rather than
    # surfacing raw psycopg2 UndefinedTable errors downstream.
    try:
        _assert_runtime_tables_present(db_manager)
    except RuntimeError as exc:
        print(str(exc))
        return

    # 1) Ensure and advance the engine run, unless explicitly skipped.
    if not args.skip_engine:
        run = _ensure_and_advance_engine_run(
            db_manager=db_manager,
            as_of=as_of,
            region=args.region.upper(),
            max_steps=args.max_engine_steps,
        )
        logger.info(
            "full_day: final engine_run state run_id=%s phase=%s",
            run.run_id,
            run.phase.value,
        )

    # 2) Run backtest campaign + Meta-Orchestrator, unless explicitly skipped.
    if args.skip_campaign:
        return

    summaries, decision_id = run_backtest_campaign_and_meta_for_strategy(
        db_manager=db_manager,
        strategy_id=args.strategy_id,
        market_id=args.market_id,
        start_date=campaign_start,
        end_date=campaign_end,
        top_k=args.top_k,
        initial_cash=args.initial_cash,
        apply_risk=not args.disable_risk,
        assessment_backend=args.assessment_backend,
        assessment_use_joint_context=args.assessment_use_joint_context,
        assessment_context_model_id=args.assessment_context_model_id,
        assessment_model_id=args.assessment_model_id,
        # Optional state-aware knobs for the backtest campaign.
        stability_risk_alpha=args.stability_risk_alpha,
        stability_risk_horizon_steps=args.stability_risk_horizon,
        regime_risk_alpha=args.regime_risk_alpha,
        # Lambda / scenario settings for lambda-aware, scenario-aware backtests.
        lambda_predictions_csv=args.lambda_predictions_csv,
        lambda_experiment_id=args.lambda_experiment_id,
        lambda_score_weight=args.lambda_score_weight,
        scenario_risk_set_id=args.scenario_risk_set_id,
        stab_scenario_set_id=args.stab_scenario_set_id,
        stab_joint_model_id=args.stab_joint_model_id,
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
