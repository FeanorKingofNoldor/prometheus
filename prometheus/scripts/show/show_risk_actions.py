"""Prometheus v2 – Risk actions inspection CLI.

This script prints recent entries from the ``risk_actions`` table for a
single strategy. It is intended for debugging how the Risk Management
Service modified proposed weights during backtests or live runs.

Example
-------

    python -m prometheus.scripts.show_risk_actions \
        --strategy-id US_EQ_CORE_LONG_EQ \
        --limit 50
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, Optional, Sequence

from apatheon.core.config import get_config
from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


def _format_details(details: Dict[str, Any] | None) -> tuple[str, str, str]:
    """Extract original, adjusted, reason strings from details_json.

    The Risk Management Service stores ``details_json`` with at least
    ``original_weight``, ``adjusted_weight``, and ``reason`` fields. This
    helper is defensive and tolerates missing keys.
    """

    if not details:
        return "", "", ""

    orig = details.get("original_weight")
    adj = details.get("adjusted_weight")
    reason = details.get("reason")

    def _fmt(x: Any) -> str:
        try:
            return f"{float(x):.6f}"
        except Exception:
            return ""

    return _fmt(orig), _fmt(adj), str(reason) if reason is not None else ""


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Show recent entries from the risk_actions table for a given "
            "strategy, ordered by created_at descending."
        ),
    )

    parser.add_argument(
        "--strategy-id",
        type=str,
        required=True,
        help="Logical strategy identifier used in risk_actions.strategy_id",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of rows to display (default: 100)",
    )

    args = parser.parse_args(argv)

    if args.limit <= 0:
        parser.error("--limit must be positive")

    config = get_config()
    db_manager = DatabaseManager(config)

    sql = """
        SELECT created_at, instrument_id, decision_id, action_type, details_json
        FROM risk_actions
        WHERE strategy_id = %s
        ORDER BY created_at DESC
        LIMIT %s
    """

    try:
        with db_manager.get_runtime_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, (args.strategy_id, args.limit))
                rows = cursor.fetchall()
            finally:
                cursor.close()
    except Exception as exc:  # pragma: no cover - defensive CLI path
        logger.exception("Failed to query risk_actions for strategy_id=%s", args.strategy_id)
        print(f"Error querying risk_actions: {exc}")
        return

    if not rows:
        print(f"No risk_actions rows found for strategy_id={args.strategy_id!r}")
        return

    print(
        "created_at,instrument_id,decision_id,action_type,original_weight,"
        "adjusted_weight,reason",
    )
    for created_at, instrument_id, decision_id, action_type, details in rows:
        orig_str, adj_str, reason_str = _format_details(details)
        print(
            f"{created_at.isoformat()},{instrument_id or ''},{decision_id or ''},"
            f"{action_type},{orig_str},{adj_str},{reason_str}",
        )


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
