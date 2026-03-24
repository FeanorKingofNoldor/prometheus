"""Run factorial backtests for lambda experiments across horizons.

This research CLI runs a small 2x2 factorial design to isolate how
cluster-level lambda/opportunity scores affect performance:

Modes (per horizon):
- baseline:          no lambda in selection, no lambda in sizing
- universe_only:     lambda affects universe inclusion ranking only
- sizing_only:       lambda affects portfolio sizing score only
- universe_and_size: lambda affects both selection and sizing

Horizon here refers to which *lambda score column* is used from the input
CSV (e.g. lambda_score_h21). These scores must be ex-ante (no lookahead).

Typical usage (short sanity run, C++ backend default):

  PYTHONPATH=cpp/build ./venv/bin/python -m prometheus.scripts.run.run_lambda_factorial_backtests \
    --market-id US_EQ \
    --start 2024-01-01 --end 2024-03-31 \
    --lambda-scores-csv data/lambda_cluster_scores_smoothed_US_EQ_1997_2024.csv \
    --universe-max-size 200 \
    --lambda-weight 10 \
    --horizons 5 21 63

By default the C++ runner uses:
- open execution on the next day (open[t+1])
- adjusted prices (adjusted_close + scaled open)
- example costs: 5 bps slippage + 0.005/share commission with $1 min and 1% max

Override with --cpp-* flags if needed.

This will create 12 sleeves (4 modes x 3 horizons) with distinct universe_id
and portfolio_id values so that runs do not overwrite each other's state.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from apathis.fragility.storage import FragilityStorage

from prometheus.backtest import SleeveConfig, run_backtest_campaign
from prometheus.backtest.campaign import SleeveRunSummary, _run_backtest_for_sleeve
from prometheus.opportunity.lambda_provider import CsvLambdaClusterScoreProvider

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


@dataclass(frozen=True)
class _Job:
    horizon: int
    mode: str
    sleeve_cfg: SleeveConfig


def _build_sleeves_for_horizon(
    *,
    market_id: str,
    assessment_strategy_id: str,
    assessment_horizon_days: int,
    universe_max_size: int | None,
    universe_sector_max_names: int | None,
    portfolio_max_names: int | None,
    portfolio_per_instrument_max_weight: float | None,
    portfolio_hysteresis_buffer: int | None,
    base_prefix: str,
    horizon: int,
    lambda_weight: float,
) -> List[_Job]:
    """Return 4 sleeve jobs for a single lambda horizon."""

    modes = [
        ("baseline", 0.0, 0.0),
        ("universe_only", float(lambda_weight), 0.0),
        ("sizing_only", 0.0, float(lambda_weight)),
        ("universe_and_size", float(lambda_weight), float(lambda_weight)),
    ]

    jobs: List[_Job] = []

    for mode, w_sel, w_port in modes:
        suffix = f"L{horizon}_{mode}".upper()
        sleeve_id = f"{base_prefix}_{suffix}"
        universe_id = f"{base_prefix}_UNIV_{suffix}"
        portfolio_id = f"{base_prefix}_PORT_{suffix}"

        cfg = SleeveConfig(
            sleeve_id=sleeve_id,
            strategy_id=sleeve_id,
            market_id=market_id,
            universe_id=universe_id,
            portfolio_id=portfolio_id,
            assessment_strategy_id=assessment_strategy_id,
            assessment_horizon_days=assessment_horizon_days,
            universe_max_size=universe_max_size,
            universe_sector_max_names=universe_sector_max_names,
            portfolio_max_names=portfolio_max_names,
            portfolio_per_instrument_max_weight=portfolio_per_instrument_max_weight,
            portfolio_hysteresis_buffer=portfolio_hysteresis_buffer,
            # Set the new weights explicitly so we do not depend on the legacy
            # lambda_score_weight behaviour.
            lambda_score_weight_selection=w_sel,
            lambda_score_weight_portfolio=w_port,
            # Keep legacy field at 0.0 to avoid ambiguity.
            lambda_score_weight=0.0,
        )

        jobs.append(_Job(horizon=horizon, mode=mode, sleeve_cfg=cfg))

    return jobs


def _run_one(
    args_tuple: tuple[str, date, date, SleeveConfig, float, bool, str, Optional[str], float | None]
) -> SleeveRunSummary:
    market_id, start_date, end_date, cfg, initial_cash, apply_risk, lambda_csv, score_col, frag_threshold = args_tuple

    local_config = get_config()
    local_db_manager = DatabaseManager(local_config)
    local_calendar = TradingCalendar()

    lambda_provider = None
    if score_col is not None:
        base_provider = CsvLambdaClusterScoreProvider(
            csv_path=Path(lambda_csv),
            experiment_id=None,
            score_column=score_col,
        )

        if frag_threshold is None:
            lambda_provider = base_provider
        else:
            # Build date -> bool mask from fragility (MARKET-level)
            storage = FragilityStorage(db_manager=local_db_manager)
            measures = storage.get_history("MARKET", market_id, start_date, end_date)
            mask = {m.as_of_date: float(m.fragility_score) > frag_threshold for m in measures}

            class _FragilityGatedProvider:
                def __init__(self, base, mask):
                    self.base = base
                    self.mask = mask
                def get_cluster_score(self, *, as_of_date, market_id, sector, soft_target_class):
                    if not self.mask.get(as_of_date, False):
                        return None
                    return self.base.get_cluster_score(
                        as_of_date=as_of_date,
                        market_id=market_id,
                        sector=sector,
                        soft_target_class=soft_target_class,
                    )

            lambda_provider = _FragilityGatedProvider(base_provider, mask)

    return _run_backtest_for_sleeve(
        db_manager=local_db_manager,
        calendar=local_calendar,
        market_id=market_id,
        start_date=start_date,
        end_date=end_date,
        cfg=cfg,
        initial_cash=initial_cash,
        apply_risk=apply_risk,
        lambda_provider=lambda_provider,
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run factorial lambda backtests across horizons")

    parser.add_argument("--market-id", type=str, required=True, help="Market identifier (e.g. US_EQ)")
    parser.add_argument("--start", type=_parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, required=True, help="End date (YYYY-MM-DD)")

    parser.add_argument(
        "--backend",
        type=str,
        choices=["cpp", "python"],
        default="cpp",
        help=(
            "Execution backend: cpp (default; realistic simulation with costs) or python (DB-backed campaign runner)."
        ),
    )
    parser.add_argument(
        "--cpp-threads",
        type=int,
        default=0,
        help="Threads for C++ backend (0 = auto).",
    )
    parser.add_argument(
        "--cpp-verbose",
        action="store_true",
        help="Enable C++ backend progress output to stderr.",
    )
    parser.add_argument(
        "--cpp-instrument-limit",
        type=int,
        default=0,
        help="Limit number of instruments loaded by C++ backend (0 = no limit).",
    )
    parser.add_argument(
        "--cpp-instrument-ids",
        type=str,
        nargs="+",
        default=None,
        help=(
            "Optional allowlist of instrument_id values for C++ backend. If provided, only these instruments "
            "are loaded/used (intended for synthetic integration tests)."
        ),
    )
    parser.add_argument(
        "--cpp-persist",
        action="store_true",
        help="Persist C++ backtest results to runtime DB (backtest_runs + backtest_daily_equity).",
    )
    parser.add_argument(
        "--cpp-persist-execution",
        action="store_true",
        help=(
            "Also persist detailed execution artifacts (orders, fills, positions_snapshots, backtest_trades, "
            "executed_actions). Can be very large for long runs."
        ),
    )
    parser.add_argument(
        "--cpp-persist-meta",
        action="store_true",
        help="Also write engine_decisions + decision_outcomes for each sleeve (Meta-Orchestrator tables).",
    )

    # Execution/realism knobs (C++ backend only).
    #
    # IMPORTANT: Defaults are intentionally set to a realistic + costful configuration.
    # Override to match your broker precisely.
    parser.add_argument(
        "--realistic",
        "--cpp-realistic",
        dest="cpp_realistic",
        action="store_true",
        help=(
            "No-op flag kept for compatibility. The C++ backend defaults are already realistic (open[t+1] execution, "
            "adjusted prices, slippage + commissions)."
        ),
    )
    parser.add_argument(
        "--cpp-execution-price",
        type=str,
        choices=["open", "close"],
        default="open",
        help="C++ execution fill price (open executes at open[t+1], close executes at close[t]).",
    )
    parser.add_argument(
        "--cpp-price-basis",
        type=str,
        choices=["raw", "adjusted"],
        default="adjusted",
        help="C++ price basis: raw or adjusted (adjusted_close + scaled open).",
    )
    parser.add_argument(
        "--cpp-slippage-bps",
        type=float,
        default=5.0,
        help="C++ symmetric slippage in basis points applied to execution price (buys up, sells down).",
    )

    # Example commission schedule: 0.005/share, $1 min, 1% max.
    parser.add_argument("--cpp-commission-per-share", type=float, default=0.005)
    parser.add_argument("--cpp-commission-min-per-order", type=float, default=1.0)
    parser.add_argument("--cpp-commission-max-pct-trade-value", type=float, default=0.01)

    # Regulatory fees (optional; default disabled).
    parser.add_argument("--cpp-finra-taf-per-share", type=float, default=0.0)
    parser.add_argument("--cpp-finra-taf-max-per-order", type=float, default=0.0)
    parser.add_argument("--cpp-sec-fee-rate", type=float, default=0.0)

    parser.add_argument(
        "--lambda-scores-csv",
        type=str,
        required=True,
        help=(
            "CSV with cluster lambda scores. Must include as_of_date, market_id, sector, soft_target_class "
            "and lambda_score_h{H} columns."
        ),
    )

    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[5, 21, 63],
        help="Which score horizons to run (default: 5 21 63)",
    )

    parser.add_argument(
        "--assessment-strategy-id",
        type=str,
        default="US_CORE_LONG_EQ",
        help="Assessment strategy_id used inside the sleeve pipeline (default: US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--assessment-horizon-days",
        type=int,
        default=21,
        help="Assessment horizon_days (default: 21)",
    )

    parser.add_argument(
        "--universe-max-size",
        type=int,
        default=200,
        help="Universe max size cap for all sleeves (default: 200)",
    )
    parser.add_argument(
        "--universe-sector-max-names",
        type=int,
        default=0,
        help="Optional per-sector name cap during universe selection (default: 0 = disabled)",
    )

    parser.add_argument(
        "--portfolio-max-names",
        type=int,
        default=0,
        help=(
            "Optional top-K cap at the portfolio stage (trade at most this many names). "
            "Default: 0 = disabled (use all included universe names)."
        ),
    )
    parser.add_argument(
        "--portfolio-per-instrument-max-weight",
        type=float,
        default=0.0,
        help=(
            "Optional per-name weight cap inside the portfolio model. "
            "Use 0 to keep the sleeve default."
        ),
    )
    parser.add_argument(
        "--portfolio-hysteresis-buffer",
        type=int,
        default=0,
        help=(
            "Optional rank buffer for top-K portfolios to reduce churn. "
            "If set to B>0 and --portfolio-max-names=K, held names are kept until rank > K+B. "
            "Default: 0 = disabled."
        ),
    )

    parser.add_argument(
        "--lambda-weight",
        type=float,
        default=10.0,
        help="Lambda weight used in universe_only / sizing_only / both modes (default: 10.0)",
    )
    parser.add_argument(
        "--fragility-threshold",
        type=float,
        default=None,
        help="If set, apply lambda only on days where market fragility_score > threshold",
    )

    parser.add_argument(
        "--base-prefix",
        type=str,
        default="LAMBDA_FACT",
        help="Prefix used to generate sleeve_id/universe_id/portfolio_id (default: LAMBDA_FACT)",
    )

    parser.add_argument(
        "--initial-cash",
        type=float,
        default=1_000_000.0,
        help="Initial cash per sleeve (default: 1,000,000)",
    )
    parser.add_argument(
        "--disable-risk",
        action="store_true",
        help="Disable Risk Management adjustments inside the sleeve pipeline",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Worker processes for sleeves per horizon (default: 1 = serial)",
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")


    horizons = sorted(set(int(h) for h in args.horizons))
    if any(h <= 0 for h in horizons):
        parser.error("--horizons must be positive")

    universe_max_size = int(args.universe_max_size) if int(args.universe_max_size) > 0 else None
    universe_sector_max_names = (
        int(args.universe_sector_max_names) if int(args.universe_sector_max_names) > 0 else None
    )

    portfolio_max_names = int(args.portfolio_max_names) if int(args.portfolio_max_names) > 0 else None
    portfolio_per_instrument_max_weight = (
        float(args.portfolio_per_instrument_max_weight)
        if float(args.portfolio_per_instrument_max_weight) > 0.0
        else None
    )
    portfolio_hysteresis_buffer = (
        int(args.portfolio_hysteresis_buffer) if int(args.portfolio_hysteresis_buffer) > 0 else None
    )

    # Build sleeve jobs for all horizons.
    jobs: List[_Job] = []
    for h in horizons:
        jobs.extend(
            _build_sleeves_for_horizon(
                market_id=args.market_id,
                assessment_strategy_id=args.assessment_strategy_id,
                assessment_horizon_days=int(args.assessment_horizon_days),
                universe_max_size=universe_max_size,
                universe_sector_max_names=universe_sector_max_names,
                portfolio_max_names=portfolio_max_names,
                portfolio_per_instrument_max_weight=portfolio_per_instrument_max_weight,
                portfolio_hysteresis_buffer=portfolio_hysteresis_buffer,
                base_prefix=str(args.base_prefix),
                horizon=h,
                lambda_weight=float(args.lambda_weight),
            )
        )

    if not jobs:
        print("No sleeves to run")
        return

    logger.info(
        "Running %d sleeves across horizons=%s start=%s end=%s market=%s backend=%s",
        len(jobs),
        horizons,
        args.start,
        args.end,
        args.market_id,
        args.backend,
    )

    lambda_csv = str(args.lambda_scores_csv)
    # For cpp backend, apply fragility gating by pre-filtering the CSV if a threshold is set.
    if args.backend == "cpp" and args.fragility_threshold is not None:
        config = get_config()
        dbm = DatabaseManager(config)
        storage = FragilityStorage(db_manager=dbm)
        measures = storage.get_history("MARKET", args.market_id, args.start, args.end)
        allowed_dates = {m.as_of_date for m in measures if float(m.fragility_score) > args.fragility_threshold}
        if allowed_dates:
            import tempfile

            import pandas as pd
            df = pd.read_csv(lambda_csv)
            df["as_of_date"] = pd.to_datetime(df["as_of_date"]).dt.date
            df = df[df["as_of_date"].isin(allowed_dates)]
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", prefix="lambda_gated_")
            df.to_csv(tmp.name, index=False)
            lambda_csv = tmp.name
            logger.info("CPP fragility gating enabled: %d rows -> %d rows, csv=%s", len(df), len(df), lambda_csv)
        else:
            logger.warning("CPP fragility gating enabled but no dates exceed threshold; lambda effectively off.")

    if args.backend == "cpp":
        # C++ backend: runs in-memory and returns metrics only (no DB persistence).
        try:
            import prom2_cpp  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise SystemExit(
                "prom2_cpp not available. Build it (./cpp/scripts/build.sh) and run with PYTHONPATH=cpp/build"
            ) from exc

        execution_price = str(args.cpp_execution_price)
        price_basis = str(args.cpp_price_basis)
        slippage_bps = float(args.cpp_slippage_bps)

        commission_per_share = float(args.cpp_commission_per_share)
        commission_min_per_order = float(args.cpp_commission_min_per_order)
        commission_max_pct_trade_value = float(args.cpp_commission_max_pct_trade_value)
        finra_taf_per_share = float(args.cpp_finra_taf_per_share)
        finra_taf_max_per_order = float(args.cpp_finra_taf_max_per_order)
        sec_fee_rate = float(args.cpp_sec_fee_rate)

        cfg = {
            "market_id": args.market_id,
            "start": args.start.isoformat(),
            "end": args.end.isoformat(),
            "assessment_strategy_id": str(args.assessment_strategy_id),
            "assessment_horizon_days": int(args.assessment_horizon_days),
            "base_prefix": str(args.base_prefix),
            "lambda_scores_csv": lambda_csv,
            "horizons": horizons,
            "universe_max_size": int(args.universe_max_size),
            "universe_sector_max_names": int(args.universe_sector_max_names),
            "portfolio_max_names": int(args.portfolio_max_names),
            "portfolio_per_instrument_max_weight": float(args.portfolio_per_instrument_max_weight),
            "portfolio_hysteresis_buffer": int(args.portfolio_hysteresis_buffer),
            "lambda_weight": float(args.lambda_weight),
            "initial_cash": float(args.initial_cash),
            "disable_risk": bool(args.disable_risk),
            "num_threads": int(args.cpp_threads),
            "verbose": bool(args.cpp_verbose),
            "persist_to_db": bool(args.cpp_persist),
            "persist_execution_to_db": bool(args.cpp_persist_execution),
            "persist_meta_to_db": bool(args.cpp_persist_meta),
            # Execution/realism knobs.
            "execution_price": str(execution_price),
            "mark_price": "close",
            "price_basis": str(price_basis),
            "slippage_bps": float(slippage_bps),
            "commission_per_share": float(commission_per_share),
            "commission_min_per_order": float(commission_min_per_order),
            "commission_max_pct_trade_value": float(commission_max_pct_trade_value),
            "finra_taf_per_share": float(finra_taf_per_share),
            "finra_taf_max_per_order": float(finra_taf_max_per_order),
            "sec_fee_rate": float(sec_fee_rate),
        }
        if int(args.cpp_instrument_limit) > 0:
            cfg["instrument_limit"] = int(args.cpp_instrument_limit)
        if args.cpp_instrument_ids:
            cfg["instrument_ids"] = list(args.cpp_instrument_ids)

        results = prom2_cpp.run_lambda_factorial_backtests(cfg)
        # Print deterministic order for easy grepping.
        results_sorted = sorted(results, key=lambda r: (int(r.get("horizon", 0)), str(r.get("mode", ""))))
        for r in results_sorted:
            horizon = r.get("horizon")
            mode = r.get("mode")
            run_id = r.get("run_id")
            metrics = r.get("metrics", {})
            if args.cpp_persist or args.cpp_persist_execution or args.cpp_persist_meta:
                print(horizon, mode, run_id, metrics)
            else:
                print(horizon, mode, metrics)
        return

    summaries: List[SleeveRunSummary] = []

    # Run per horizon so we can use a horizon-specific score_column.
    for h in horizons:
        score_col = f"lambda_score_h{h}"
        horizon_jobs = [j for j in jobs if j.horizon == h]
        if not horizon_jobs:
            continue

        logger.info("Running horizon=%d using score_column=%s (%d sleeves)", h, score_col, len(horizon_jobs))

        if args.max_workers == 1 or len(horizon_jobs) == 1:
            # Serial.
            config = get_config()
            db_manager = DatabaseManager(config)
            calendar = TradingCalendar()

            base_provider = CsvLambdaClusterScoreProvider(
                csv_path=Path(lambda_csv),
                experiment_id=None,
                score_column=score_col,
            )
            if args.fragility_threshold is None:
                lambda_provider = base_provider
            else:
                storage = FragilityStorage(db_manager=db_manager)
                measures = storage.get_history("MARKET", args.market_id, args.start, args.end)
                mask = {m.as_of_date: float(m.fragility_score) > args.fragility_threshold for m in measures}

                class _FragilityGatedProvider:
                    def __init__(self, base, mask):
                        self.base = base
                        self.mask = mask
                    def get_cluster_score(self, *, as_of_date, market_id, sector, soft_target_class):
                        if not self.mask.get(as_of_date, False):
                            return None
                        return self.base.get_cluster_score(
                            as_of_date=as_of_date,
                            market_id=market_id,
                            sector=sector,
                            soft_target_class=soft_target_class,
                        )

                lambda_provider = _FragilityGatedProvider(base_provider, mask)

            sleeve_cfgs = [j.sleeve_cfg for j in horizon_jobs]
            summaries.extend(
                run_backtest_campaign(
                    db_manager=db_manager,
                    calendar=calendar,
                    market_id=args.market_id,
                    start_date=args.start,
                    end_date=args.end,
                    sleeve_configs=sleeve_cfgs,
                    initial_cash=float(args.initial_cash),
                    apply_risk=not args.disable_risk,
                    lambda_provider=lambda_provider,
                )
            )
        else:
            # Parallel: each sleeve in its own worker process.
            tasks = []
            for j in horizon_jobs:
                tasks.append(
                    (
                        args.market_id,
                        args.start,
                        args.end,
                        j.sleeve_cfg,
                        float(args.initial_cash),
                        not args.disable_risk,
                        lambda_csv,
                        score_col,
                        args.fragility_threshold,
                    )
                )

            with ProcessPoolExecutor(max_workers=int(args.max_workers)) as executor:
                futures = {executor.submit(_run_one, t): t for t in tasks}
                for fut in as_completed(futures):
                    summaries.append(fut.result())

    if not summaries:
        print("No sleeves were run")
        return

    # Print a compact summary.
    print("run_id,sleeve_id,horizon,mode,cumulative_return,annualised_sharpe,max_drawdown")
    for s in summaries:
        sleeve_id = s.sleeve_id
        # Infer horizon and mode from sleeve_id convention.
        horizon = "?"
        mode = "?"
        parts = sleeve_id.split("_")
        for p in parts:
            if p.startswith("L") and p[1:].isdigit():
                horizon = p[1:]
            if p in {"BASELINE", "UNIVERSE", "ONLY", "SIZING", "AND", "SIZE"}:
                # best-effort; mode parsing below is more robust
                pass
        if "UNIVERSE_ONLY" in sleeve_id:
            mode = "universe_only"
        elif "SIZING_ONLY" in sleeve_id:
            mode = "sizing_only"
        elif "UNIVERSE_AND_SIZE" in sleeve_id:
            mode = "universe_and_size"
        elif "BASELINE" in sleeve_id:
            mode = "baseline"

        m = s.metrics or {}
        cumret = float(m.get("cumulative_return", 0.0))
        sharpe = float(m.get("annualised_sharpe", 0.0))
        maxdd = float(m.get("max_drawdown", 0.0))
        print(f"{s.run_id},{s.sleeve_id},{horizon},{mode},{cumret:.6f},{sharpe:.4f},{maxdd:.6f}")


if __name__ == "__main__":  # pragma: no cover
    main()
