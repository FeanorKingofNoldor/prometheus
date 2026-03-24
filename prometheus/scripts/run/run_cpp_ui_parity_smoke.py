"""Run a small C++ backtest that populates the UI-facing runtime tables.

This workflow is meant to be a fast, repeatable "UI parity" smoke run:

- Uses the C++ lambda factorial engine (prom2_cpp).
- Persists backtesting tables: backtest_runs + backtest_daily_equity.
- Optionally persists execution artifacts: orders, fills, positions_snapshots,
  backtest_trades, executed_actions.
- Optionally persists Meta tables: engine_decisions + decision_outcomes.

It prints run_ids / portfolio_ids and basic row-count checks so you can
quickly point the UI at fresh data.

Typical usage:
  ./venv/bin/python -m prometheus.scripts.run_cpp_ui_parity_smoke

If you want lambda to affect selection/sizing, pass --lambda-scores-csv.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional, Sequence

from apathis.core.config import get_config
from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid


def _parse_date(value: str) -> date:
    try:
        y, m, d = map(int, value.split("-"))
        return date(y, m, d)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _maybe_add_cpp_build_to_syspath() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    build_dir = repo_root / "cpp" / "build"
    if build_dir.exists():
        sys.path.insert(0, str(build_dir))


def _tables_present(db: DatabaseManager, tables: Sequence[str]) -> bool:
    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            for t in tables:
                cursor.execute("SELECT to_regclass(%s)", (str(t),))
                (name,) = cursor.fetchone()
                if name is None:
                    return False
            return True
        finally:
            cursor.close()


def _cleanup_previous_cpp_runs(db: DatabaseManager, *, base_prefix: str) -> None:
    """Delete prior prom2_cpp rows for a given base_prefix.

    This is intended for *development* UI parity runs so repeated executions
    don't accumulate mixed history for the same portfolio_id values.

    Scope:
      - Only backtest_runs rows with config_json->>'engine' = 'prom2_cpp'
        and config_json->>'base_prefix' = base_prefix are targeted.
      - Related rows in backtest_daily_equity/backtest_trades/executed_actions
        are deleted by run_id.
      - Execution tables are deleted by portfolio_id extracted from config_json.
    """

    base_prefix = str(base_prefix)

    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT run_id, (config_json->>'portfolio_id') AS portfolio_id
                FROM backtest_runs
                WHERE (config_json->>'engine') = 'prom2_cpp'
                  AND (config_json->>'base_prefix') = %s
                """,
                (base_prefix,),
            )
            rows = cursor.fetchall()
            if not rows:
                return

            run_ids = [str(r[0]) for r in rows if r and r[0]]
            portfolio_ids = sorted({str(r[1]) for r in rows if r and r[1]})

            # Meta tables are optional.
            cursor.execute("SELECT to_regclass('engine_decisions')")
            (has_engine_decisions,) = cursor.fetchone()
            cursor.execute("SELECT to_regclass('decision_outcomes')")
            (has_decision_outcomes,) = cursor.fetchone()

            if has_engine_decisions is not None and has_decision_outcomes is not None:
                cursor.execute(
                    """
                    DELETE FROM decision_outcomes
                    WHERE decision_id IN (
                        SELECT decision_id FROM engine_decisions WHERE run_id = ANY(%s)
                    )
                    """,
                    (run_ids,),
                )
                cursor.execute(
                    "DELETE FROM engine_decisions WHERE run_id = ANY(%s)",
                    (run_ids,),
                )

            # executed_actions has no FK; safe to delete early.
            cursor.execute(
                "DELETE FROM executed_actions WHERE run_id = ANY(%s)",
                (run_ids,),
            )

            # Execution tables: delete fills before orders (FK).
            if portfolio_ids:
                cursor.execute(
                    """
                    DELETE FROM fills
                    WHERE order_id IN (
                        SELECT order_id FROM orders
                        WHERE portfolio_id = ANY(%s)
                          AND mode = 'BACKTEST'
                    )
                    """,
                    (portfolio_ids,),
                )
                cursor.execute(
                    "DELETE FROM orders WHERE portfolio_id = ANY(%s) AND mode = 'BACKTEST'",
                    (portfolio_ids,),
                )
                cursor.execute(
                    "DELETE FROM positions_snapshots WHERE portfolio_id = ANY(%s) AND mode = 'BACKTEST'",
                    (portfolio_ids,),
                )

            # Backtest tables.
            cursor.execute(
                "DELETE FROM backtest_daily_equity WHERE run_id = ANY(%s)",
                (run_ids,),
            )
            cursor.execute(
                "DELETE FROM backtest_trades WHERE run_id = ANY(%s)",
                (run_ids,),
            )
            cursor.execute(
                "DELETE FROM backtest_runs WHERE run_id = ANY(%s)",
                (run_ids,),
            )

            conn.commit()
        finally:
            cursor.close()


@dataclass(frozen=True)
class UiParityResult:
    horizon: int
    mode: str
    run_id: str
    universe_id: str
    portfolio_id: str


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run a small C++ backtest that populates UI-facing runtime tables"
    )

    parser.add_argument("--market-id", type=str, default="US_EQ")
    parser.add_argument("--start", type=_parse_date, default=date(2024, 1, 2))
    parser.add_argument("--end", type=_parse_date, default=date(2024, 3, 29))

    parser.add_argument(
        "--base-prefix",
        type=str,
        default="CPP_UI_PARITY",
        help="Prefix used to generate sleeve_id/universe_id/portfolio_id.",
    )
    parser.add_argument(
        "--stable-ids",
        action="store_true",
        help=(
            "Do not append a random suffix to base_prefix. Useful if you want stable portfolio_id values "
            "across runs (note: tables like positions_snapshots will accumulate rows for the same portfolio_id)."
        ),
    )

    parser.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[5],
        help="Lambda horizons to run (default: 5 => 4 sleeves).",
    )
    parser.add_argument(
        "--universe-max-size",
        type=int,
        default=50,
        help="Universe max size (default: 50).",
    )
    parser.add_argument(
        "--instrument-limit",
        type=int,
        default=200,
        help="Max number of instruments loaded from runtime DB (default: 200).",
    )
    parser.add_argument(
        "--instrument-ids",
        type=str,
        nargs="+",
        default=None,
        help="Optional allowlist of instrument_id values (useful for synthetic tests).",
    )

    parser.add_argument(
        "--lambda-scores-csv",
        type=str,
        default="",
        help="Optional CSV path for lambda cluster scores. If omitted/empty, lambda is disabled.",
    )
    parser.add_argument(
        "--lambda-weight",
        type=float,
        default=10.0,
        help="Lambda weight (default: 10.0).",
    )

    parser.add_argument(
        "--assessment-strategy-id",
        type=str,
        default="CPP_UI_PARITY_ASSESS",
        help="Stored into backtest_runs.config_json for MetaOrchestrator compatibility.",
    )
    parser.add_argument(
        "--assessment-horizon-days",
        type=int,
        default=21,
    )

    parser.add_argument("--threads", type=int, default=0, help="C++ threads (0 = auto)")
    parser.add_argument("--verbose", action="store_true", help="Enable C++ progress output")

    parser.add_argument(
        "--persist-execution",
        action="store_true",
        default=True,
        help="Persist execution artifacts (orders/fills/positions/trades/actions).",
    )
    parser.add_argument(
        "--no-persist-execution",
        action="store_false",
        dest="persist_execution",
        help="Do not persist execution artifacts.",
    )

    parser.add_argument(
        "--persist-meta",
        action="store_true",
        default=False,
        help="Persist engine_decisions + decision_outcomes (if tables exist).",
    )

    parser.add_argument(
        "--cleanup",
        action="store_true",
        help=(
            "Before running, delete prior prom2_cpp backtest/execution rows with the same base_prefix ("
            "uses backtest_runs.config_json->>base_prefix/engine to scope deletions)."
        ),
    )

    args = parser.parse_args(argv)

    if args.end < args.start:
        raise SystemExit("--end must be >= --start")

    horizons = sorted(set(int(h) for h in args.horizons))
    if any(h <= 0 for h in horizons):
        raise SystemExit("--horizons must be positive")

    if args.lambda_scores_csv:
        p = Path(str(args.lambda_scores_csv))
        if not p.exists():
            raise SystemExit(f"--lambda-scores-csv path does not exist: {p}")

    _maybe_add_cpp_build_to_syspath()

    try:
        import prom2_cpp  # type: ignore
    except Exception as exc:
        raise SystemExit(
            "prom2_cpp not available. Build it with ./cpp/scripts/build.sh (requires venv + Python headers)."
        ) from exc

    config = get_config()
    db = DatabaseManager(config)

    # Auto-detect Meta tables.
    meta_tables = ["engine_decisions", "decision_outcomes"]
    meta_available = _tables_present(db, meta_tables)

    persist_meta = bool(args.persist_meta and meta_available)
    if bool(args.persist_meta) and not meta_available:
        print("NOTE: engine_decisions/decision_outcomes tables not present; persist_meta disabled")

    if args.stable_ids:
        base_prefix = str(args.base_prefix).upper()
    else:
        # Add a short random suffix so repeated runs don't collide on portfolio_id.
        base_prefix = f"{str(args.base_prefix)}_{generate_uuid()[:8]}".upper()

    if bool(args.cleanup):
        _cleanup_previous_cpp_runs(db, base_prefix=base_prefix)

    cfg = {
        "market_id": str(args.market_id),
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "assessment_strategy_id": str(args.assessment_strategy_id),
        "assessment_horizon_days": int(args.assessment_horizon_days),
        "base_prefix": base_prefix,
        "lambda_scores_csv": str(args.lambda_scores_csv),
        "horizons": horizons,
        "universe_max_size": int(args.universe_max_size),
        "lambda_weight": float(args.lambda_weight),
        "initial_cash": 1_000_000.0,
        "num_threads": int(args.threads),
        "verbose": bool(args.verbose),
        "persist_to_db": True,
        "persist_execution_to_db": bool(args.persist_execution),
        "persist_meta_to_db": bool(persist_meta),
    }

    if args.instrument_ids:
        cfg["instrument_ids"] = list(args.instrument_ids)
    elif int(args.instrument_limit) > 0:
        cfg["instrument_limit"] = int(args.instrument_limit)

    results = prom2_cpp.run_lambda_factorial_backtests(cfg)

    parsed: List[UiParityResult] = []
    for r in results:
        parsed.append(
            UiParityResult(
                horizon=int(r.get("horizon", 0)),
                mode=str(r.get("mode", "")),
                run_id=str(r.get("run_id", "")),
                universe_id=str(r.get("universe_id", "")),
                portfolio_id=str(r.get("portfolio_id", "")),
            )
        )

    parsed = sorted(parsed, key=lambda x: (x.horizon, x.mode))

    run_ids = [p.run_id for p in parsed]
    portfolio_ids = [p.portfolio_id for p in parsed]

    print("CPP UI parity run complete")
    print(f"base_prefix={base_prefix}")
    print(f"date_range={args.start.isoformat()}..{args.end.isoformat()} market_id={args.market_id}")
    print(f"persist_execution={bool(args.persist_execution)} persist_meta={persist_meta}")
    print("")

    for p in parsed:
        print(f"h={p.horizon:>3} mode={p.mode:<16} run_id={p.run_id} portfolio_id={p.portfolio_id}")

    print("")

    # Basic DB sanity checks.
    with db.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM backtest_runs WHERE run_id = ANY(%s)", (run_ids,))
            (n_runs,) = cursor.fetchone()

            cursor.execute("SELECT COUNT(*) FROM backtest_daily_equity WHERE run_id = ANY(%s)", (run_ids,))
            (n_eq,) = cursor.fetchone()

            cursor.execute("SELECT COUNT(*) FROM backtest_trades WHERE run_id = ANY(%s)", (run_ids,))
            (n_trades,) = cursor.fetchone()

            cursor.execute("SELECT COUNT(*) FROM executed_actions WHERE run_id = ANY(%s)", (run_ids,))
            (n_actions,) = cursor.fetchone()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM orders
                WHERE portfolio_id = ANY(%s)
                  AND mode = 'BACKTEST'
                """,
                (portfolio_ids,),
            )
            (n_orders,) = cursor.fetchone()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM fills
                WHERE order_id IN (
                    SELECT order_id FROM orders WHERE portfolio_id = ANY(%s) AND mode = 'BACKTEST'
                )
                  AND mode = 'BACKTEST'
                """,
                (portfolio_ids,),
            )
            (n_fills,) = cursor.fetchone()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM positions_snapshots
                WHERE portfolio_id = ANY(%s)
                  AND as_of_date BETWEEN %s AND %s
                  AND mode = 'BACKTEST'
                """,
                (portfolio_ids, args.start, args.end),
            )
            (n_snaps,) = cursor.fetchone()

            print("DB row counts")
            print(f"backtest_runs:         {n_runs}")
            print(f"backtest_daily_equity: {n_eq}")
            print(f"backtest_trades:       {n_trades}")
            print(f"orders:               {n_orders}")
            print(f"fills:                {n_fills}")
            print(f"positions_snapshots:  {n_snaps}")
            print(f"executed_actions:     {n_actions}")

            if persist_meta:
                cursor.execute("SELECT COUNT(*) FROM engine_decisions WHERE run_id = ANY(%s)", (run_ids,))
                (n_dec,) = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM decision_outcomes
                    WHERE decision_id IN (
                        SELECT decision_id FROM engine_decisions WHERE run_id = ANY(%s)
                    )
                    """,
                    (run_ids,),
                )
                (n_out,) = cursor.fetchone()
                print(f"engine_decisions:      {n_dec}")
                print(f"decision_outcomes:     {n_out}")

        finally:
            cursor.close()


if __name__ == "__main__":  # pragma: no cover
    main()
