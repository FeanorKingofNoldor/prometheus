"""Prometheus v2 – Show backtest run metrics (research convenience).

This CLI prints recent rows from ``backtest_runs`` with key metrics pulled
from ``metrics_json``. It is intended for quick comparisons across sweep
runs (e.g. changing universe_max_size / portfolio_max_names).

Example
-------

  # Recent lambda factorial runs
  python -m prometheus.scripts.show.show_backtest_run_metrics \
    --strategy-prefix LAMBDA_FACT \
    --limit 50
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional, Sequence

from apathis.core.database import get_db_manager


def _parse_date(value: str) -> date:
    try:
        y, m, d = map(int, value.split("-"))
        return date(y, m, d)
    except Exception as exc:  # pragma: no cover
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Show recent backtest run metrics")

    parser.add_argument(
        "--strategy-prefix",
        type=str,
        default=None,
        help="Optional prefix filter applied to backtest_runs.strategy_id (e.g. LAMBDA_FACT)",
    )
    parser.add_argument(
        "--sleeve-prefix",
        type=str,
        default=None,
        help="Optional prefix filter applied to config_json->>'sleeve_id'",
    )
    parser.add_argument(
        "--created-since",
        type=_parse_date,
        default=None,
        help="Optional filter: only runs created on/after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max rows to print (default: 200)",
    )

    args = parser.parse_args(argv)

    if args.limit <= 0:
        parser.error("--limit must be positive")

    where = []
    params: list[object] = []

    if args.strategy_prefix is not None:
        where.append("strategy_id LIKE %s")
        params.append(str(args.strategy_prefix) + "%")

    if args.sleeve_prefix is not None:
        where.append("(config_json->>'sleeve_id') LIKE %s")
        params.append(str(args.sleeve_prefix) + "%")

    if args.created_since is not None:
        where.append("created_at::date >= %s")
        params.append(args.created_since)

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            run_id,
            strategy_id,
            (config_json->>'sleeve_id') AS sleeve_id,
            start_date,
            end_date,
            created_at,
            (metrics_json->>'cumulative_return')::double precision AS cumulative_return,
            (metrics_json->>'annualised_sharpe')::double precision AS annualised_sharpe,
            (metrics_json->>'max_drawdown')::double precision AS max_drawdown,
            (metrics_json->>'universe_size_mean_over_run')::double precision AS universe_size_mean_over_run,
            (metrics_json->>'target_num_positions_mean_over_run')::double precision AS target_num_positions_mean_over_run
        FROM backtest_runs
        {where_sql}
        ORDER BY created_at DESC
        LIMIT %s
    """

    params.append(int(args.limit))

    db_manager = get_db_manager()

    rows: list[tuple] = []
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        finally:
            cur.close()

    print(
        "run_id,strategy_id,sleeve_id,start_date,end_date,created_at,"
        "cumulative_return,annualised_sharpe,max_drawdown,universe_size_mean_over_run,target_num_positions_mean_over_run"
    )

    for (
        run_id,
        strategy_id,
        sleeve_id,
        start_date_db,
        end_date_db,
        created_at,
        cumret,
        sharpe,
        maxdd,
        univ_mean,
        npos_mean,
    ) in rows:
        def _f(x: object) -> str:
            if x is None:
                return ""
            if isinstance(x, float):
                return f"{x:.6f}"
            return str(x)

        print(
            f"{run_id},{strategy_id},{sleeve_id},{start_date_db},{end_date_db},{created_at},"
            f"{_f(cumret)},{_f(sharpe)},{_f(maxdd)},{_f(univ_mean)},{_f(npos_mean)}"
        )


if __name__ == "__main__":  # pragma: no cover
    main()
