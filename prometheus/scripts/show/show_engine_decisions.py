"""Prometheus v2 – Engine decisions inspection CLI.

This script prints rows from the ``engine_decisions`` table (and
optionally aggregates matching ``decision_outcomes``) for quick
inspection of engine/meta decisions.

Examples
--------

    # Show recent decisions for a strategy
    python -m prometheus.scripts.show_engine_decisions \
        --strategy-id US_CORE_LONG_EQ \
        --limit 50

    # Show only BACKTEST_SLEEVE_RUNNER decisions on a given date
    python -m prometheus.scripts.show_engine_decisions \
        --engine-name BACKTEST_SLEEVE_RUNNER \
        --as-of 2025-01-31
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from typing import Optional, Sequence

from psycopg2.extras import Json

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger


logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Show engine_decisions rows (with optional outcome aggregates) "
            "for meta/debugging."
        ),
    )

    parser.add_argument(
        "--engine-name",
        type=str,
        default=None,
        help="Optional engine_name filter (e.g. META_ORCHESTRATOR, BACKTEST_SLEEVE_RUNNER)",
    )
    parser.add_argument(
        "--strategy-id",
        type=str,
        default=None,
        help="Optional strategy_id filter",
    )
    parser.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="Optional as_of_date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of decisions to display (default: 100)",
    )
    parser.add_argument(
        "--include-outcomes",
        action="store_true",
        help=(
            "If set, join aggregated outcome stats (horizons, realized_return, "
            "realized_drawdown, realized_vol) for each decision_id."
        ),
    )

    args = parser.parse_args(argv)

    if args.limit <= 0:
        parser.error("--limit must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    db_manager = get_db_manager()

    where_clauses = []
    params: list[object] = []

    if args.engine_name is not None:
        where_clauses.append("engine_name = %s")
        params.append(args.engine_name)

    if args.strategy_id is not None:
        where_clauses.append("strategy_id = %s")
        params.append(args.strategy_id)

    if args.as_of is not None:
        where_clauses.append("as_of_date = %s")
        params.append(args.as_of)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = (
        "SELECT decision_id, engine_name, strategy_id, market_id, as_of_date, "
        "config_id, input_refs, output_refs, metadata, created_at "
        "FROM engine_decisions" + where_sql + " ORDER BY created_at DESC LIMIT %s"
    )
    params.append(args.limit)

    rows: list[tuple] = []
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    # Optionally pre-load outcome aggregates keyed by decision_id.
    outcomes_by_decision: dict[str, dict[str, float]] = {}
    if args.include_outcomes and rows:
        decision_ids = [r[0] for r in rows]
        placeholders = ",".join(["%s"] * len(decision_ids))
        sql_outcomes = (
            "SELECT decision_id, horizon_days, realized_return, realized_pnl, "
            "realized_drawdown, realized_vol "
            "FROM decision_outcomes "
            f"WHERE decision_id IN ({placeholders})"
        )
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql_outcomes, tuple(decision_ids))
                for (
                    decision_id,
                    horizon_days,
                    realized_return,
                    realized_pnl,
                    realized_drawdown,
                    realized_vol,
                ) in cursor.fetchall():
                    key = str(decision_id)
                    # For now we store the last seen outcome per horizon; this
                    # can be extended to full lists if needed.
                    outcomes_by_decision.setdefault(key, {})[
                        f"h{horizon_days}_return"
                    ] = float(realized_return) if realized_return is not None else 0.0
                    if realized_drawdown is not None:
                        outcomes_by_decision[key][
                            f"h{horizon_days}_maxdd"
                        ] = float(realized_drawdown)
                    if realized_vol is not None:
                        outcomes_by_decision[key][
                            f"h{horizon_days}_vol"
                        ] = float(realized_vol)
            finally:
                cursor.close()

    # CSV header
    header_cols = [
        "decision_id",
        "engine_name",
        "strategy_id",
        "market_id",
        "as_of_date",
        "config_id",
        "created_at",
    ]
    if args.include_outcomes:
        header_cols.append("outcome_summary")

    print(",".join(header_cols))

    for (
        decision_id,
        engine_name,
        strategy_id,
        market_id,
        as_of_date_db,
        config_id,
        _input_refs,
        _output_refs,
        _metadata,
        created_at,
    ) in rows:
        created_str = created_at.isoformat() if isinstance(created_at, datetime) else str(created_at)
        base_fields = [
            str(decision_id),
            str(engine_name),
            str(strategy_id) if strategy_id is not None else "",
            str(market_id) if market_id is not None else "",
            as_of_date_db.isoformat() if isinstance(as_of_date_db, date) else str(as_of_date_db),
            str(config_id) if config_id is not None else "",
            created_str,
        ]

        if args.include_outcomes:
            outcome_summary = outcomes_by_decision.get(str(decision_id), {})
            base_fields.append(Json(outcome_summary).dumps(outcome_summary))

        print(",".join(base_fields))


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
