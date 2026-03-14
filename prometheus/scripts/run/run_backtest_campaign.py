"""Prometheus v2 – Backtest campaign CLI.

This script runs a simple backtest campaign over one or more sleeves for
the same strategy and market, using :func:`run_backtest_campaign`.

Example
-------

    python -m prometheus.scripts.run_backtest_campaign \
        --market-id US_EQ \
        --start 2024-01-01 \
        --end 2024-03-31 \
        --sleeve US_CORE_20D:US_CORE_LONG_EQ:US_EQ:US_CORE_UNIVERSE:US_CORE_PORT:US_CORE_ASSESS:21
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import List, Optional, Sequence

from concurrent.futures import ProcessPoolExecutor, as_completed

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar
from prometheus.backtest import SleeveConfig, run_backtest_campaign
from prometheus.backtest.campaign import _run_backtest_for_sleeve
from prometheus.books.registry import BookKind, LongEquitySleeveSpec, load_book_registry


logger = get_logger(__name__)


def _worker(args_tuple: tuple[str, date, date, SleeveConfig, float, bool]) -> "SleeveRunSummary":
    """Worker function to run a single sleeve in a separate process.

    Defined at module top level so it is picklable by multiprocessing.
    """
    market_id, start, end, cfg, initial_cash, apply_risk_flag = args_tuple
    local_config = get_config()
    local_db_manager = DatabaseManager(local_config)
    local_calendar = TradingCalendar()
    return _run_backtest_for_sleeve(
        db_manager=local_db_manager,
        calendar=local_calendar,
        market_id=market_id,
        start_date=start,
        end_date=end,
        cfg=cfg,
        initial_cash=initial_cash,
        apply_risk=apply_risk_flag,
        lambda_provider=None,
    )


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parse_sleeve_arg(raw: str) -> SleeveConfig:
    """Parse a compact sleeve definition into :class:`SleeveConfig`.

    The format is::

        sleeve_id:strategy_id:market_id:universe_id:portfolio_id:assessment_strategy_id:assessment_horizon_days
    """

    parts = raw.split(":")
    if len(parts) != 7:
        raise argparse.ArgumentTypeError(
            "--sleeve must have 7 colon-separated fields: "
            "sleeve_id:strategy_id:market_id:universe_id:portfolio_id:assessment_strategy_id:assessment_horizon_days",
        )

    sleeve_id, strategy_id, market_id, universe_id, portfolio_id, assess_id, horizon_str = parts
    try:
        horizon_days = int(horizon_str)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid assessment_horizon_days {horizon_str!r} in --sleeve argument",
        ) from exc

    return SleeveConfig(
        sleeve_id=sleeve_id,
        strategy_id=strategy_id,
        market_id=market_id,
        universe_id=universe_id,
        portfolio_id=portfolio_id,
        assessment_strategy_id=assess_id,
        assessment_horizon_days=horizon_days,
    )


def _sleeve_configs_from_book(
    *,
    book_id: str,
    id_prefix: str,
    assessment_horizon_days: int,
    assessment_backend: str,
    assessment_model_id: str | None,
    assessment_use_joint_context: bool,
    assessment_context_model_id: str,
    hazard_profile: str | None,
    meta_budget_enabled: bool,
    meta_budget_alpha: float,
    meta_budget_min: float,
    meta_budget_horizon_steps: int,
    meta_budget_region: str | None,
    sleeve_id_filter: str | None,
    conviction_enabled: bool = False,
    conviction_decay_rate: float = 2.0,
    conviction_hard_stop_pct: float = 0.20,
    apply_sector_allocator: bool = False,
    sector_kill_threshold: float = 0.25,
    sector_reduce_threshold: float = 0.40,
) -> tuple[str, List[SleeveConfig]]:
    registry = load_book_registry()
    book = registry.get(str(book_id))
    if book is None:
        raise SystemExit(f"Unknown book_id={book_id!r}; check configs/meta/books.yaml")
    if book.kind != BookKind.LONG_EQUITY:
        raise SystemExit(
            f"book_id={book_id!r} is kind={book.kind}, expected LONG_EQUITY; "
            "use run_hedge_etf_backtests.py for hedge ETF books"
        )

    market_id = str(book.market_id)

    out: List[SleeveConfig] = []
    for sid, spec in book.sleeves.items():
        if not isinstance(spec, LongEquitySleeveSpec):
            continue
        if sleeve_id_filter is not None and str(sid) != str(sleeve_id_filter):
            continue

        cfg = SleeveConfig(
            sleeve_id=str(spec.sleeve_id),
            strategy_id=str(id_prefix) + str(book_id),
            market_id=market_id,
            universe_id=str(id_prefix) + str(spec.sleeve_id) + "_UNIVERSE",
            portfolio_id=str(id_prefix) + str(spec.sleeve_id),
            assessment_strategy_id=str(id_prefix) + str(spec.sleeve_id) + "_ASSESS",
            assessment_horizon_days=int(assessment_horizon_days),
            portfolio_max_names=spec.portfolio_max_names,
            portfolio_hysteresis_buffer=spec.portfolio_hysteresis_buffer,
            portfolio_per_instrument_max_weight=spec.portfolio_per_instrument_max_weight,
            apply_fragility_overlay=bool(getattr(spec, "apply_fragility_overlay", False)),
        )

        # Campaign-wide Assessment settings.
        cfg.assessment_backend = assessment_backend
        cfg.assessment_use_joint_context = assessment_use_joint_context
        cfg.assessment_context_model_id = assessment_context_model_id
        if assessment_model_id is not None:
            cfg.assessment_model_id = assessment_model_id

        # Campaign-wide regime + meta budget config.
        if hazard_profile is not None:
            cfg.hazard_profile = str(hazard_profile)

        if meta_budget_enabled:
            cfg.meta_budget_enabled = True
            cfg.meta_budget_alpha = float(meta_budget_alpha)
            cfg.meta_budget_min = float(meta_budget_min)
            cfg.meta_budget_horizon_steps = int(meta_budget_horizon_steps)
            if meta_budget_region is not None:
                cfg.meta_budget_region = str(meta_budget_region)

        # Conviction + sector allocator.
        if conviction_enabled:
            cfg.conviction_enabled = True
            cfg.conviction_decay_rate = float(conviction_decay_rate)
            cfg.conviction_hard_stop_pct = float(conviction_hard_stop_pct)
        if apply_sector_allocator:
            cfg.apply_sector_allocator = True
            cfg.sector_allocator_kill_threshold = float(sector_kill_threshold)
            cfg.sector_allocator_reduce_threshold = float(sector_reduce_threshold)

        out.append(cfg)

    if not out:
        raise SystemExit("No sleeves selected from book registry")

    out.sort(key=lambda c: c.sleeve_id)
    return market_id, out


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run a multi-sleeve backtest campaign")

    parser.add_argument(
        "--market-id",
        type=str,
        required=False,
        default=None,
        help=(
            "Market identifier (e.g. US_EQ). Required when using --sleeve; "
            "ignored/validated when using --book-id."
        ),
    )
    parser.add_argument("--start", type=_parse_date, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, required=True, help="End date (YYYY-MM-DD)")

    parser.add_argument(
        "--book-id",
        type=str,
        default=None,
        help=(
            "Optional LONG_EQUITY book id from configs/meta/books.yaml. "
            "If set, sleeves are loaded from the registry instead of --sleeve."
        ),
    )
    parser.add_argument(
        "--book-sleeve-id",
        type=str,
        default=None,
        help="Optional sleeve_id filter when using --book-id (run only this sleeve)",
    )
    parser.add_argument(
        "--id-prefix",
        type=str,
        default=None,
        help=(
            "Prefix for generated universe/portfolio/assessment ids when using --book-id. "
            "Default: BT_<BOOK_ID>_."
        ),
    )

    parser.add_argument(
        "--sleeve",
        dest="sleeves",
        action="append",
        required=False,
        help=(
            "Sleeve definition in the form "
            "sleeve_id:strategy_id:market_id:universe_id:portfolio_id:assessment_strategy_id:assessment_horizon_days. "
            "May be specified multiple times. Mutually exclusive with --book-id."
        ),
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
        help=(
            "Disable Risk Management adjustments inside the sleeve pipeline "
            "(use raw portfolio weights for a risk-off baseline)"
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

    # ------------------------------------------------------------------
    # Regime + Meta budget configuration
    # ------------------------------------------------------------------

    parser.add_argument(
        "--hazard-profile",
        type=str,
        default=None,
        help=(
            "Hazard profile name used by the market-proxy regime detector (default: DEFAULT). "
            "Example: US_PROXY_THR0P90_AD1P20_MM0P35"
        ),
    )

    parser.add_argument(
        "--meta-budget-enabled",
        action="store_true",
        help=(
            "Enable Meta budget allocation (global capital scalar) derived from regime state-change risk. "
            "When enabled, portfolio targets are scaled by a budget multiplier in [meta_budget_min, 1]."
        ),
    )
    parser.add_argument("--meta-budget-alpha", type=float, default=1.0)
    parser.add_argument("--meta-budget-min", type=float, default=0.35)
    parser.add_argument("--meta-budget-horizon-steps", type=int, default=21)
    parser.add_argument(
        "--meta-budget-region",
        type=str,
        default=None,
        help=(
            "Optional region label to use for regime state-change risk (default: derived from market_id prefix)."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help=(
            "Maximum number of worker processes to use when running multiple "
            "sleeves (default: 1 = serial)."
        ),
    )

    # ------------------------------------------------------------------
    # Conviction-based position lifecycle
    # ------------------------------------------------------------------

    parser.add_argument(
        "--conviction-enabled",
        action="store_true",
        help=(
            "Enable conviction-based position lifecycle manager. "
            "New entries start at half weight with an entry credit; "
            "positions are only sold when conviction decays below threshold."
        ),
    )
    parser.add_argument("--conviction-decay-rate", type=float, default=2.0)
    parser.add_argument("--conviction-hard-stop-pct", type=float, default=0.20)

    # ------------------------------------------------------------------
    # Sector Allocator overlay
    # ------------------------------------------------------------------

    parser.add_argument(
        "--apply-sector-allocator",
        action="store_true",
        help=(
            "Enable sector health overlay. Computes per-sector SHI scores "
            "and kills/reduces positions in sick/weak sectors."
        ),
    )
    parser.add_argument("--sector-kill-threshold", type=float, default=0.25)
    parser.add_argument("--sector-reduce-threshold", type=float, default=0.40)

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")
    if args.max_workers <= 0:
        parser.error("--max-workers must be positive")

    if args.book_id is not None and args.sleeves:
        parser.error("Use either --book-id or --sleeve (not both)")

    if args.book_id is None and not args.sleeves:
        parser.error("Must specify either --book-id or at least one --sleeve")

    market_id = None
    sleeve_configs: List[SleeveConfig] = []

    if args.book_id is not None:
        prefix = args.id_prefix
        if prefix is None or str(prefix).strip() == "":
            prefix = f"BT_{str(args.book_id).strip()}_"

        market_id, sleeve_configs = _sleeve_configs_from_book(
            book_id=str(args.book_id),
            id_prefix=str(prefix),
            assessment_horizon_days=21,
            assessment_backend=args.assessment_backend,
            assessment_model_id=args.assessment_model_id,
            assessment_use_joint_context=args.assessment_use_joint_context,
            assessment_context_model_id=args.assessment_context_model_id,
            hazard_profile=args.hazard_profile,
            meta_budget_enabled=bool(getattr(args, "meta_budget_enabled", False)),
            meta_budget_alpha=float(args.meta_budget_alpha),
            meta_budget_min=float(args.meta_budget_min),
            meta_budget_horizon_steps=int(args.meta_budget_horizon_steps),
            meta_budget_region=str(args.meta_budget_region) if args.meta_budget_region is not None else None,
            sleeve_id_filter=str(args.book_sleeve_id) if args.book_sleeve_id is not None else None,
            conviction_enabled=bool(getattr(args, "conviction_enabled", False)),
            conviction_decay_rate=float(args.conviction_decay_rate),
            conviction_hard_stop_pct=float(args.conviction_hard_stop_pct),
            apply_sector_allocator=bool(getattr(args, "apply_sector_allocator", False)),
            sector_kill_threshold=float(args.sector_kill_threshold),
            sector_reduce_threshold=float(args.sector_reduce_threshold),
        )

        if args.market_id is not None and str(args.market_id) != str(market_id):
            parser.error(f"--market-id={args.market_id!r} does not match book.market_id={market_id!r}")

    else:
        if args.market_id is None:
            parser.error("--market-id is required when using --sleeve")
        market_id = str(args.market_id)

        for raw in args.sleeves:
            cfg = _parse_sleeve_arg(raw)
            # Apply campaign-wide Assessment configuration to each sleeve.
            cfg.assessment_backend = args.assessment_backend
            cfg.assessment_use_joint_context = args.assessment_use_joint_context
            cfg.assessment_context_model_id = args.assessment_context_model_id
            if args.assessment_model_id is not None:
                cfg.assessment_model_id = args.assessment_model_id
            # Apply campaign-wide regime + meta budget configuration to each sleeve.
            if args.hazard_profile is not None:
                cfg.hazard_profile = str(args.hazard_profile)

            if bool(getattr(args, "meta_budget_enabled", False)):
                cfg.meta_budget_enabled = True
                cfg.meta_budget_alpha = float(args.meta_budget_alpha)
                cfg.meta_budget_min = float(args.meta_budget_min)
                cfg.meta_budget_horizon_steps = int(args.meta_budget_horizon_steps)
                if args.meta_budget_region is not None:
                    cfg.meta_budget_region = str(args.meta_budget_region)

            # Conviction + sector allocator.
            if bool(getattr(args, "conviction_enabled", False)):
                cfg.conviction_enabled = True
                cfg.conviction_decay_rate = float(args.conviction_decay_rate)
                cfg.conviction_hard_stop_pct = float(args.conviction_hard_stop_pct)
            if bool(getattr(args, "apply_sector_allocator", False)):
                cfg.apply_sector_allocator = True
                cfg.sector_allocator_kill_threshold = float(args.sector_kill_threshold)
                cfg.sector_allocator_reduce_threshold = float(args.sector_reduce_threshold)

            sleeve_configs.append(cfg)

    config = get_config()

    # Serial path (existing behaviour) when max_workers == 1 or only one sleeve.
    if args.max_workers == 1 or len(sleeve_configs) == 1:
        db_manager = DatabaseManager(config)
        calendar = TradingCalendar()
        summaries = run_backtest_campaign(
            db_manager=db_manager,
            calendar=calendar,
            market_id=str(market_id),
            start_date=args.start,
            end_date=args.end,
            sleeve_configs=sleeve_configs,
            initial_cash=args.initial_cash,
            apply_risk=not args.disable_risk,
        )
    else:
        # Parallel path: run each sleeve in its own worker process.
        tasks: List[tuple[str, date, date, SleeveConfig, float, bool]] = []
        for cfg in sleeve_configs:
            tasks.append(
                (
                    str(market_id),
                    args.start,
                    args.end,
                    cfg,
                    args.initial_cash,
                    not args.disable_risk,
                )
            )

        summaries = []
        with ProcessPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {executor.submit(_worker, t): t for t in tasks}
            for fut in as_completed(futures):
                summaries.append(fut.result())

        # Preserve original sleeve order by sorting summaries according to
        # the order of sleeve_ids in sleeve_configs.
        order = {cfg.sleeve_id: idx for idx, cfg in enumerate(sleeve_configs)}
        summaries.sort(key=lambda s: order.get(s.sleeve_id, 0))

    if not summaries:
        print("No sleeves were run (empty sleeve list)")
        return

    print("run_id,sleeve_id,strategy_id,cumulative_return,annualised_sharpe,max_drawdown")
    for s in summaries:
        m = s.metrics or {}
        cumret = float(m.get("cumulative_return", 0.0))
        sharpe = float(m.get("annualised_sharpe", 0.0))
        maxdd = float(m.get("max_drawdown", 0.0))
        print(
            f"{s.run_id},{s.sleeve_id},{s.strategy_id},{cumret:.6f},{sharpe:.4f},{maxdd:.6f}",
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
