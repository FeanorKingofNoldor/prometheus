"""Run a single sleeve over a historical period (live-like year replay).

This CLI is a thin convenience wrapper around the existing backtest
infrastructure:

- It constructs a ``SleeveConfig`` for a core long-only US_EQ sleeve.
- It uses the same BasicSleevePipeline (STAB → Assessment → Universe →
  Portfolio) that powers the backtest campaign tools.
- It runs a :class:`BacktestRunner` over a date range, simulating a
  "live-like" daily pipeline via TimeMachine + BacktestBroker.

The goal is to make it easy to say:

    "Run the pilot sleeve over a full historical year and see what happens."

Example
-------

Run the default pilot sleeve over calendar year 2019::

    python -m prometheus.scripts.run_sleeve_year_replay \
      --start 2019-01-01 \
      --end   2019-12-31

You can override sleeve/strategy IDs if you want to experiment with
variants, but the defaults are chosen to match the core long-only US_EQ
sleeve described in the embedding and backtest plans.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger
from apathis.core.time import TradingCalendar, TradingCalendarConfig

from prometheus.backtest.campaign import _run_backtest_for_sleeve
from prometheus.backtest.config import SleeveConfig

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _build_pilot_sleeve(
    *,
    sleeve_id: str,
    strategy_id: str,
    market_id: str,
) -> SleeveConfig:
    """Return the default pilot sleeve configuration.

    This mirrors the Phase 2 design: a core long-only US_EQ sleeve with a
    21-day Assessment horizon, basic Assessment backend, no lambda, and
    scenario risk disabled. Universe/portfolio IDs are derived
    deterministically from the sleeve/strategy IDs.
    """

    base = sleeve_id
    universe_id = f"{base}_UNIVERSE"
    portfolio_id = f"{base}_PORTFOLIO"
    assessment_strategy_id = f"{base}_ASSESS"

    cfg = SleeveConfig(
        sleeve_id=base,
        strategy_id=strategy_id,
        market_id=market_id,
        universe_id=universe_id,
        portfolio_id=portfolio_id,
        assessment_strategy_id=assessment_strategy_id,
        assessment_horizon_days=21,
    )

    # Phase 2 defaults: basic Assessment backend, no context, STAB risk on,
    # regime risk off, no scenario or lambda integration.
    cfg.assessment_backend = "basic"
    cfg.assessment_model_id = None
    cfg.assessment_use_joint_context = False
    cfg.assessment_context_model_id = "joint-assessment-context-v1"

    cfg.stability_risk_alpha = 0.5
    cfg.stability_risk_horizon_steps = 1
    cfg.regime_risk_alpha = 0.0

    cfg.scenario_risk_set_id = None
    cfg.lambda_score_weight = 0.0

    return cfg


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a single sleeve over a historical period using the full "
            "STAB → Assessment → Universe → Portfolio pipeline (live-like year replay)."
        ),
    )

    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Inclusive start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="Inclusive end date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--sleeve-id",
        type=str,
        default="US_CORE_LONG_EQ_H21",
        help="Sleeve id to use (default: US_CORE_LONG_EQ_H21)",
    )
    parser.add_argument(
        "--strategy-id",
        type=str,
        default="US_CORE_LONG_EQ",
        help="Strategy id for the sleeve (default: US_CORE_LONG_EQ)",
    )
    parser.add_argument(
        "--market-id",
        type=str,
        default="US_EQ",
        help="Market id traded by the sleeve (default: US_EQ)",
    )
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=1_000_000.0,
        help="Initial cash for the sleeve (default: 1,000,000)",
    )
    parser.add_argument(
        "--disable-risk",
        action="store_true",
        help=(
            "Disable Risk Management adjustments inside the sleeve pipeline "
            "(use raw portfolio weights for a risk-off baseline).",
        ),
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        parser.error("--end must be >= --start")

    cfg = _build_pilot_sleeve(
        sleeve_id=args.sleeve_id,
        strategy_id=args.strategy_id,
        market_id=args.market_id,
    )

    config = get_config()
    db_manager = DatabaseManager(config)

    # Use a TradingCalendar configured for the requested market_id and
    # share this instance across TimeMachine, MarketSimulator, and all
    # engines so there is a single source of truth for trading days.
    calendar = TradingCalendar(TradingCalendarConfig(market=args.market_id))

    summary = _run_backtest_for_sleeve(
        db_manager=db_manager,
        calendar=calendar,
        market_id=args.market_id,
        start_date=args.start,
        end_date=args.end,
        cfg=cfg,
        initial_cash=args.initial_cash,
        apply_risk=not args.disable_risk,
        lambda_provider=None,
    )

    m = summary.metrics or {}
    cumret = float(m.get("cumulative_return", 0.0))
    sharpe = float(m.get("annualised_sharpe", 0.0))
    maxdd = float(m.get("max_drawdown", 0.0))

    print("run_id,sleeve_id,strategy_id,start,end,cumulative_return,annualised_sharpe,max_drawdown")
    print(
        f"{summary.run_id},{summary.sleeve_id},{summary.strategy_id},"
        f"{args.start},{args.end},{cumret:.6f},{sharpe:.4f},{maxdd:.6f}",
    )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
