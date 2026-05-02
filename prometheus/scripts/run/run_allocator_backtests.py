"""Run fast C++ backtests for ALLOCATOR sleeves.

This script runs the C++ allocator backtest engine over the sleeves
defined in the book registry (configs/meta/books.yaml) for a given
ALLOCATOR book.

Typical usage (C++ backend):

  PYTHONPATH=cpp/build ./venv/bin/python -m prometheus.scripts.run.run_allocator_backtests \
    --book-id US_EQ_ALLOCATOR \
    --start 2015-01-01 --end 2024-12-31 \
    --cpp-threads 16 \
    --cpp-persist

Notes:
- The allocator runner blends a long equity sleeve with hedge ETFs using
  regime+fragility driven allocation.
- Results can optionally be persisted to runtime DB.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger
from apatheon.core.markets import infer_region_from_market_id
from apatheon.regime.storage import RegimeStorage
from psycopg2.extras import Json

from prometheus.backtest.analyzers import EquityCurvePoint
from prometheus.backtest.config import SleeveConfig
from prometheus.backtest.runner import BacktestRunner
from prometheus.books.registry import AllocatorSleeveSpec, BookKind, load_book_registry
from prometheus.meta.market_situation import MarketSituationConfig, classify_market_situation

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Run C++ allocator sleeve backtests")

    parser.add_argument(
        "--book-id",
        type=str,
        default="US_EQ_ALLOCATOR",
        help="Book id from configs/meta/books.yaml (default: US_EQ_ALLOCATOR)",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--sleeve-id",
        type=str,
        default=None,
        help="Optional sleeve_id filter (run only this sleeve)",
    )

    parser.add_argument(
        "--universe-max-size",
        type=int,
        default=0,
        help="Long-universe max size (0=auto; default: max(200, 10*portfolio_max_names)).",
    )

    parser.add_argument(
        "--cpp-instrument-limit",
        type=int,
        default=0,
        help="Optional limit on number of long instruments loaded from DB (0=no limit).",
    )

    # C++ controls.
    parser.add_argument("--cpp-threads", type=int, default=0, help="Threads for C++ backend (0=auto).")
    parser.add_argument("--cpp-verbose", action="store_true", help="Enable progress output from C++ runner.")

    parser.add_argument(
        "--cpp-persist",
        action="store_true",
        help="Persist to runtime DB (backtest_runs + backtest_daily_equity).",
    )
    parser.add_argument(
        "--cpp-persist-meta",
        action="store_true",
        help="Also persist engine_decisions + decision_outcomes.",
    )

    # Crisis-warning tuning for post-processed metrics.
    parser.add_argument(
        "--warning-lookback-days",
        type=int,
        default=30,
        help="Lookback window (days) when computing warning→crisis lead metrics (default: 30).",
    )
    parser.add_argument(
        "--warning-down-threshold",
        type=float,
        default=0.7,
        help="Threshold for down_risk warning signal (default: 0.7).",
    )
    parser.add_argument(
        "--warning-regime-threshold",
        type=float,
        default=0.6,
        help="Threshold for regime_risk_score warning signal (default: 0.6).",
    )
    parser.add_argument(
        "--warning-lambda-threshold",
        type=float,
        default=0.5,
        help="Threshold for lambda_score_mean warning signal (default: 0.5).",
    )

    parser.add_argument(
        "--disable-risk",
        action="store_true",
        help="Disable per-name risk cap on the long leg.",
    )
    parser.add_argument(
        "--gate-csv",
        type=str,
        default=None,
        help="Optional CSV path with date,breadth_flag,credit_flag to gate hedging.",
    )

    args = parser.parse_args(argv)

    try:
        import prom2_cpp  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise SystemExit(
            "prom2_cpp not available. Build it (./cpp/scripts/build.sh) and run with PYTHONPATH=cpp/build"
        ) from exc

    registry = load_book_registry()
    book = registry.get(str(args.book_id))
    if book is None:
        raise SystemExit(f"Unknown book_id={args.book_id!r}; check configs/meta/books.yaml")
    if book.kind != BookKind.ALLOCATOR:
        raise SystemExit(f"book_id={args.book_id!r} is kind={book.kind}, expected ALLOCATOR")

    sleeves = []
    for sid, spec in book.sleeves.items():
        if not isinstance(spec, AllocatorSleeveSpec):
            continue
        if args.sleeve_id is not None and sid != args.sleeve_id:
            continue

        max_names = int(spec.portfolio_max_names or 0)
        universe_max = int(args.universe_max_size)
        if universe_max <= 0:
            universe_max = max(200, 10 * max_names) if max_names > 0 else 200

        sleeves.append(
            {
                "sleeve_id": str(spec.sleeve_id),
                "universe_max_size": int(universe_max),
                "portfolio_max_names": int(max_names),
                "portfolio_hysteresis_buffer": int(spec.portfolio_hysteresis_buffer or 0),
                "portfolio_per_instrument_max_weight": float(spec.portfolio_per_instrument_max_weight or 0.05),
                "hedge_instrument_ids": list(spec.hedge_instrument_ids),
                "hedge_sizing_mode": str(spec.hedge_sizing_mode),
                "fragility_threshold": float(spec.fragility_threshold),
                "max_hedge_allocation": float(spec.max_hedge_allocation),
                "hedge_allocation_overrides": dict(spec.hedge_allocation_overrides or {}),
                "hedge_allocation_floors": dict(spec.hedge_allocation_floors or {}),
                "hedge_allocation_caps": dict(spec.hedge_allocation_caps or {}),
                "non_crisis_hedge_cap": float(spec.non_crisis_hedge_cap) if spec.non_crisis_hedge_cap is not None else None,
                "profitability_weight": float(spec.profitability_weight or 0.0),
            }
        )

    if not sleeves:
        raise SystemExit("No allocator sleeves selected")

    cfg = {
        "market_id": str(book.market_id),
        "regime_region": str(book.region),
        "base_prefix": str(args.book_id),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "sleeves": sleeves,
        "instrument_limit": int(args.cpp_instrument_limit),
        "num_threads": int(args.cpp_threads),
        "verbose": bool(args.cpp_verbose),
        "persist_to_db": bool(args.cpp_persist),
        "persist_meta_to_db": bool(args.cpp_persist_meta),
        "disable_risk": bool(args.disable_risk),
    }

    # Optional allocator switching + risk-rail knobs from book config.
    if getattr(book, "situation_sleeve_map", None):
        cfg["situation_sleeve_map"] = dict(book.situation_sleeve_map)
    if getattr(book, "sleeve_transition_days", None) is not None:
        cfg["sleeve_transition_days"] = int(book.sleeve_transition_days)
    if getattr(book, "max_turnover_one_way", None) is not None:
        cfg["max_turnover_one_way"] = float(book.max_turnover_one_way)
    if getattr(book, "crisis_force_hedge_allocation", None) is not None:
        cfg["crisis_force_hedge_allocation"] = float(book.crisis_force_hedge_allocation)
    if getattr(book, "drawdown_brake_threshold", None) is not None:
        cfg["drawdown_brake_threshold"] = float(book.drawdown_brake_threshold)
    if getattr(book, "drawdown_brake_hedge_allocation", None) is not None:
        cfg["drawdown_brake_hedge_allocation"] = float(book.drawdown_brake_hedge_allocation)
    if getattr(book, "vol_target_annual", None) is not None:
        cfg["vol_target_annual"] = float(book.vol_target_annual)
    if getattr(book, "vol_target_lookback_days", None) is not None:
        cfg["vol_target_lookback_days"] = int(book.vol_target_lookback_days)
    gate_csv = args.gate_csv or getattr(book, "gate_csv_path", None)
    if gate_csv:
        cfg["gate_csv_path"] = gate_csv

    logger.info(
        "Running allocator C++ backtests book_id=%s market_id=%s start=%s end=%s sleeves=%d",
        args.book_id,
        book.market_id,
        args.start,
        args.end,
        len(sleeves),
    )

    results = prom2_cpp.run_allocator_backtests(cfg)
    results_sorted = sorted(results, key=lambda r: str(r.get("sleeve_id", "")))

    def _regime_lookup(db, market_id: str, start_date, end_date):
        region = infer_region_from_market_id(market_id)
        if region is None:
            return {}
        try:
            rs = RegimeStorage(db_manager=db)
            prev = rs.get_latest_regime(region, as_of_date=start_date, inclusive=False)
            history = []
            if prev is not None:
                history.append((prev.as_of_date, prev.regime_label))
            for st in rs.get_history(region, start_date, end_date):
                history.append((st.as_of_date, st.regime_label))
            history.sort(key=lambda x: x[0])
            out = {}
            last = None
            for d, lbl in history:
                last = lbl
                out[d] = lbl
            return {"history": history, "last": last}
        except Exception:
            return {}

    # Optional: enrich metrics_json with crisis-lead diagnostics using Python helper.
    def _postprocess_crisis_metrics(run_id: str, cfg_obj) -> None:
        db = get_db_manager()
        # Load config and existing metrics.
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT config_json, metrics_json, start_date, end_date
                FROM backtest_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                cur.close()
                return
            config_json, metrics_json, start_date, end_date = row

            market_id = (config_json or {}).get("market_id")
            if not market_id:
                cur.close()
                return

            # Fetch equity curve + exposures.
            cur.execute(
                """
                SELECT date, equity_curve_value, exposure_metrics_json
                FROM backtest_daily_equity
                WHERE run_id = %s
                ORDER BY date ASC
                """,
                (run_id,),
            )
            rows = cur.fetchall()
            cur.close()

        if not rows:
            return

        equity_curve = [
            EquityCurvePoint(date=r[0], equity=float(r[1]))
            for r in rows
            if r[1] is not None
        ]
        market_id_val = getattr(cfg_obj, "market_id", None)
        if market_id_val is None and isinstance(cfg_obj, dict):
            market_id_val = cfg_obj.get("market_id")
        regime_cache = _regime_lookup(db, market_id_val, start_date, end_date)
        ms_config = MarketSituationConfig()
        exposure_by_date = {}
        for r in rows:
            d = r[0]
            raw = r[2] or {}
            exp = {str(k): float(v) for k, v in raw.items() if isinstance(v, (int, float))}

            # Fallbacks so warning signals exist even if the C++ exposure feed lacks them.
            # Use fragility_score as a proxy for down_risk, and map regime_label to a coarse regime_risk_score.
            if "down_risk" not in exp:
                fs = raw.get("fragility_score")
                if isinstance(fs, (int, float)):
                    exp["down_risk"] = float(fs)
            if "regime_risk_score" not in exp:
                reg_label = raw.get("regime_label")
                if isinstance(reg_label, str):
                    lbl = reg_label.upper()
                    if lbl == "CRISIS":
                        exp["regime_risk_score"] = 0.9
                    elif lbl == "RISK_OFF":
                        exp["regime_risk_score"] = 0.7
                    else:
                        exp["regime_risk_score"] = 0.0
            if "lambda_score_mean" not in exp:
                # Nothing better available; default to 0.0 so it won't trigger.
                exp["lambda_score_mean"] = 0.0

            if "regime_label" not in raw:
                # Fill from regime history if available.
                if regime_cache and "history" in regime_cache:
                    # find latest regime <= d
                    lbl = None
                    for rd, rlbl in reversed(regime_cache["history"]):
                        if rd <= d:
                            lbl = rlbl
                            break
                    if lbl is not None:
                        exp["regime_label"] = str(lbl)
            # Derive market_situation when possible.
            try:
                regime_lbl = exp.get("regime_label") or raw.get("regime_label")
                prev_regime_lbl = raw.get("prev_regime_label")
                frag_score = exp.get("fragility_score", raw.get("fragility_score"))
                ms = classify_market_situation(
                    regime_label=regime_lbl,
                    prev_regime_label=prev_regime_lbl,
                    fragility_score=frag_score,
                    config=ms_config,
                )
                if ms:
                    exp["market_situation"] = str(ms.value if hasattr(ms, "value") else ms)
            except Exception:
                pass
            exposure_by_date[d] = exp

        cfg = SleeveConfig(
            sleeve_id="CPP_ALLOCATOR",
            strategy_id=str(config_json.get("strategy_id", "")) if isinstance(config_json, dict) else "",
            market_id=str(market_id),
            universe_id=str(config_json.get("universe_id", "")) if isinstance(config_json, dict) else "",
            portfolio_id=str(config_json.get("portfolio_id", "")) if isinstance(config_json, dict) else "",
            assessment_strategy_id=str(config_json.get("assessment_strategy_id", "")) if isinstance(config_json, dict) else "",
            assessment_horizon_days=int(config_json.get("assessment_horizon_days", 21)) if isinstance(config_json, dict) else 21,
        )

        runner = BacktestRunner(
            db_manager=db,
            broker=None,  # not used by _compute_crisis_lead_metrics
            equity_analyzer=None,  # not used
            target_positions_fn=lambda d: {},
        )

        crisis_metrics = runner._compute_crisis_lead_metrics(
            config=cfg,
            equity_curve=equity_curve,
            exposure_by_date=exposure_by_date,
            start_date=start_date,
            end_date=end_date,
            warning_lookback_days=int(args.warning_lookback_days),
            down_risk_threshold=float(args.warning_down_threshold),
            regime_risk_threshold=float(args.warning_regime_threshold),
            lambda_score_threshold=float(args.warning_lambda_threshold),
        )

        if not crisis_metrics:
            return

        merged = {}
        if isinstance(metrics_json, dict):
            merged.update(metrics_json)
        merged.update(crisis_metrics)
        merged["warning_params"] = {
            "lookback_days": int(args.warning_lookback_days),
            "down_risk_threshold": float(args.warning_down_threshold),
            "regime_risk_threshold": float(args.warning_regime_threshold),
            "lambda_score_threshold": float(args.warning_lambda_threshold),
        }

        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE backtest_runs SET metrics_json = %s WHERE run_id = %s",
                (Json(merged), run_id),
            )
            conn.commit()
            cur.close()
    for r in results_sorted:
        sleeve_id = r.get("sleeve_id")
        run_id = r.get("run_id")
        metrics = r.get("metrics", {})

        # Enrich with crisis-lead metrics if the run was persisted to DB.
        if args.cpp_persist:
            try:
                _postprocess_crisis_metrics(str(run_id), cfg)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception("Failed to postprocess crisis metrics for run_id=%s: %s", run_id, exc)

        if args.cpp_persist or args.cpp_persist_meta:
            print(sleeve_id, run_id, metrics)
        else:
            print(sleeve_id, metrics)


if __name__ == "__main__":  # pragma: no cover
    main()
