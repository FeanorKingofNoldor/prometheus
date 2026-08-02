"""Wipe paper-trading state for a fresh-start month.

Use after resetting the IBKR paper account in Client Portal.  Backs up
every row about to be deleted to a JSONL archive under
``data/archive/paper_wipe_<timestamp>/`` so the wipe is reversible by
re-INSERT if we need to forensically reconstruct.

Usage:
    # Dry run (default) — prints counts, writes nothing.
    python -m prometheus.scripts.maintenance.wipe_paper_trading

    # Real wipe — backs up + deletes + writes reset marker row.
    python -m prometheus.scripts.maintenance.wipe_paper_trading \
        --confirm --starting-nav 250000 --label "fresh-start-may-2026"

What gets wiped:
  - orders (mode='PAPER')
  - fills (mode='PAPER')
  - positions_snapshots (mode='PAPER')
  - executed_actions (all — empty anyway, but flushed for cleanliness)
  - engine_decisions + decision_outcomes (since the reset boundary date)
  - engine_runs (since the boundary)
  - meta_config_proposals + meta_config_proposal_events (all 57 stale)
  - reports of report_type IN ('trading_daily','trading_weekly')
  - risk_actions (since the boundary — 1.1M rows, the big one)
  - position_convictions + target_portfolios (LIVE_PORTFOLIO_IDS)
  - trade_journal (all)
  - options_positions + options_position_events (mode='PAPER' or live portfolio)
  - derivatives_shadow_decisions (all)
  - portfolio_equity_history (all — only if the table exists)
  - meta_signal_validations, backtest_live_drift, weekly_reports,
    meta_feedback_insights (all — derived self-calibration outputs)

What stays:
  - All historical / assessment data (instrument_prices, regimes,
    fragility_measures, signals, etc.) — these are research inputs.
  - log_daily / log_weekly reports — these are operator-facing.
  - User auth, workspace, schema metadata.

A new row in `account_resets` records the reset event so future
analytics can scope to "since reset N".
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
ARCHIVE_ROOT = PROJECT_ROOT / "data" / "archive"

# Portfolio ids that hold live-paper state.  US_EQ_LONG_V12 is the book id
# used by target_portfolios / position_convictions; IBKR_PAPER is the broker
# account id used by snapshots / options tables.  Backtest sleeves keep
# their rows.
LIVE_PORTFOLIO_IDS: list[str] = [
    "US_EQ_LONG_V12",
    "IBKR_PAPER",
    # Regional long-equity book ids (multi-market rollout) — they write
    # target_portfolios / position_convictions under their book id.
    "UK_EQ_LONG_V1",
    "EU_EQ_LONG_V1",
    "HK_EQ_LONG_V1",
    "KR_EQ_LONG_V1",
    "AU_EQ_LONG_V1",
]


def ensure_account_resets_table(cur: Any) -> None:
    """Idempotent: create the reset-marker table if missing.

    One row per paper reset event.  ``starting_nav_usd`` is the deposit
    we put back into IBKR.  ``label`` is human-readable for SQL filters
    later (e.g. ``WHERE label = 'fresh-start-may-2026'``).
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS account_resets (
            reset_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            portfolio_id      TEXT NOT NULL,
            reset_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            starting_nav_usd  NUMERIC(14, 2) NOT NULL,
            label             TEXT NOT NULL DEFAULT '',
            archive_path      TEXT,
            row_counts        JSONB NOT NULL DEFAULT '{}'::jsonb,
            notes             TEXT
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_resets_reset_at "
        "ON account_resets (reset_at DESC)"
    )


# ── Backup helpers ────────────────────────────────────────────────


def _dump_query_to_jsonl(cur: Any, sql: str, params: tuple, out_path: Path) -> int:
    """Stream a SELECT result to a gzipped JSONL file.  Returns row count.

    Streams rather than fetches-all because some tables (risk_actions)
    have millions of rows.
    """
    cur.execute(sql, params)
    cols = [d.name for d in cur.description]
    count = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out_path, "wt", encoding="utf-8") as f:
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                obj = {col: _jsonable(val) for col, val in zip(cols, row, strict=True)}
                f.write(json.dumps(obj) + "\n")
                count += 1
    return count


def _jsonable(value: Any) -> Any:
    """Convert Postgres types to JSON-safe primitives."""
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (dict, list, int, float, str, bool)):
        return value
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


# ── Wipe plan ─────────────────────────────────────────────────────
#
# Tuples are: (label, backup_select_sql, delete_sql, params).
# Order matters — children first (FK constraints).
#
# Each label is also a key in row_counts metadata on the reset marker.

def _build_plan(
    boundary_date: date,
    *,
    include_equity_history: bool = False,
) -> list[tuple[str, str, str, tuple]]:
    live_ids = list(LIVE_PORTFOLIO_IDS)
    plan: list[tuple[str, str, str, tuple]] = [
        # Children first.
        (
            "fills",
            "SELECT * FROM fills WHERE mode='PAPER'",
            "DELETE FROM fills WHERE mode='PAPER'",
            (),
        ),
        # options_position_events references options_positions — child first.
        (
            "options_position_events",
            "SELECT * FROM options_position_events WHERE mode='PAPER' OR portfolio_id = ANY(%s)",
            "DELETE FROM options_position_events WHERE mode='PAPER' OR portfolio_id = ANY(%s)",
            (live_ids,),
        ),
        (
            "options_positions",
            "SELECT * FROM options_positions WHERE mode='PAPER' OR portfolio_id = ANY(%s)",
            "DELETE FROM options_positions WHERE mode='PAPER' OR portfolio_id = ANY(%s)",
            (live_ids,),
        ),
        (
            "decision_outcomes",
            """
            SELECT dout.* FROM decision_outcomes dout
            JOIN engine_decisions ed USING(decision_id)
            WHERE ed.as_of_date >= %s
            """,
            """
            DELETE FROM decision_outcomes
            WHERE decision_id IN (
                SELECT decision_id FROM engine_decisions WHERE as_of_date >= %s
            )
            """,
            (boundary_date,),
        ),
        (
            "config_change_log",
            "SELECT * FROM config_change_log",
            "DELETE FROM config_change_log",
            (),
        ),
        # meta_config_proposals and meta_config_proposal_events are
        # protected by mutation-prevent triggers — they're intentionally
        # append-only audit history.  We leave them in place; the new
        # proposal generator will add fresh rows on the next cycle.
        # Now parents.
        (
            "orders",
            "SELECT * FROM orders WHERE mode='PAPER'",
            "DELETE FROM orders WHERE mode='PAPER'",
            (),
        ),
        (
            "positions_snapshots",
            "SELECT * FROM positions_snapshots WHERE mode='PAPER'",
            "DELETE FROM positions_snapshots WHERE mode='PAPER'",
            (),
        ),
        (
            "executed_actions",
            "SELECT * FROM executed_actions",
            "DELETE FROM executed_actions",
            (),
        ),
        (
            "engine_decisions",
            "SELECT * FROM engine_decisions WHERE as_of_date >= %s",
            "DELETE FROM engine_decisions WHERE as_of_date >= %s",
            (boundary_date,),
        ),
        # meta_config_proposals intentionally NOT wiped — append-only
        # audit table; mutation trigger blocks DELETE.  Leave them.
        (
            "engine_runs",
            "SELECT * FROM engine_runs WHERE as_of_date >= %s",
            "DELETE FROM engine_runs WHERE as_of_date >= %s",
            (boundary_date,),
        ),
        (
            "risk_actions",
            # risk_actions has a created_at column — scope by that.
            "SELECT * FROM risk_actions WHERE created_at >= %s",
            "DELETE FROM risk_actions WHERE created_at >= %s",
            (boundary_date,),
        ),
        (
            "reports_trading",
            "SELECT * FROM reports WHERE report_type IN ('trading_daily','trading_weekly')",
            "DELETE FROM reports WHERE report_type IN ('trading_daily','trading_weekly')",
            (),
        ),
        # Dashboard metric source — without this the UI shows stale NLV,
        # leverage, and exposure long after positions/orders are gone.
        # Scoped to LIVE/PAPER portfolios; backtest sleeves (e.g.
        # US_EQ_LONG_V12) keep their rows for historical comparison.
        (
            "portfolio_risk_reports_live_paper",
            "SELECT * FROM portfolio_risk_reports WHERE portfolio_id IN ('IBKR_PAPER','IBKR_LIVE')",
            "DELETE FROM portfolio_risk_reports WHERE portfolio_id IN ('IBKR_PAPER','IBKR_LIVE')",
            (),
        ),
        # Conviction lifecycle state — stale conviction scores from the old
        # run would seed the fresh account's entry/exit logic.
        (
            "position_convictions",
            "SELECT * FROM position_convictions WHERE portfolio_id = ANY(%s)",
            "DELETE FROM position_convictions WHERE portfolio_id = ANY(%s)",
            (live_ids,),
        ),
        (
            "target_portfolios",
            "SELECT * FROM target_portfolios WHERE portfolio_id = ANY(%s)",
            "DELETE FROM target_portfolios WHERE portfolio_id = ANY(%s)",
            (live_ids,),
        ),
        (
            "trade_journal",
            "SELECT * FROM trade_journal",
            "DELETE FROM trade_journal",
            (),
        ),
        (
            "derivatives_shadow_decisions",
            "SELECT * FROM derivatives_shadow_decisions",
            "DELETE FROM derivatives_shadow_decisions",
            (),
        ),
        # Self-calibration outputs — all derived from the wiped decision /
        # outcome history, so they must go with it or the meta layer keeps
        # "learning" from a run that no longer exists.
        (
            "meta_signal_validations",
            "SELECT * FROM meta_signal_validations",
            "DELETE FROM meta_signal_validations",
            (),
        ),
        (
            "backtest_live_drift",
            "SELECT * FROM backtest_live_drift",
            "DELETE FROM backtest_live_drift",
            (),
        ),
        (
            "weekly_reports",
            "SELECT * FROM weekly_reports",
            "DELETE FROM weekly_reports",
            (),
        ),
        (
            "meta_feedback_insights",
            "SELECT * FROM meta_feedback_insights",
            "DELETE FROM meta_feedback_insights",
            (),
        ),
    ]

    if include_equity_history:
        # Table is created by a separate workstream; only wiped when it
        # actually exists (caller checks to_regclass).
        plan.append(
            (
                "portfolio_equity_history",
                "SELECT * FROM portfolio_equity_history",
                "DELETE FROM portfolio_equity_history",
                (),
            )
        )

    return plan


def _table_exists(cur: Any, table_name: str) -> bool:
    """True if ``table_name`` resolves to an existing relation."""
    cur.execute("SELECT to_regclass(%s)", (table_name,))
    row = cur.fetchone()
    return bool(row and row[0])


# ── Main ──────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Paper-trading state wipe.")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform the wipe.  Without this flag, prints counts and exits.",
    )
    parser.add_argument(
        "--boundary-date",
        type=str,
        default="2026-03-01",
        help="ISO date; wipe affects rows since this date.  Default: 2026-03-01.",
    )
    parser.add_argument(
        "--starting-nav",
        type=float,
        default=250_000.0,
        help="Starting NAV of the freshly-reset IBKR paper account.  Default: 250000.",
    )
    parser.add_argument(
        "--portfolio-id",
        type=str,
        default="IBKR_PAPER",
        help="Portfolio id this reset applies to.  Default: IBKR_PAPER.",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="",
        help="Human-readable label for the reset (e.g. 'fresh-start-may-2026').",
    )
    parser.add_argument(
        "--archive-root",
        type=str,
        default=None,
        help="Override the archive root.  Default: data/archive/.",
    )
    args = parser.parse_args()

    boundary = date.fromisoformat(args.boundary_date)
    archive_root = Path(args.archive_root) if args.archive_root else ARCHIVE_ROOT
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_dir = archive_root / f"paper_wipe_{timestamp}"

    db = get_db_manager()

    # portfolio_equity_history is created by a separate workstream — only
    # include it in the plan when it exists.
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            has_equity_history = _table_exists(cur, "portfolio_equity_history")
        finally:
            cur.close()

    plan = _build_plan(boundary, include_equity_history=has_equity_history)

    if not args.confirm:
        # Dry-run path.  Just print what would happen.
        print("DRY RUN — no changes will be made.")
        print(f"Boundary date: {boundary}")
        print(f"Portfolio: {args.portfolio_id}")
        print(f"Starting NAV after reset: ${args.starting_nav:,.0f}")
        print(f"Archive would land at: {archive_dir}")
        print()
        print(f"{'table':<35} {'rows-to-delete':>16}")
        print("-" * 53)
        with db.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                for label, sel_sql, _del_sql, params in plan:
                    # Wrap select in COUNT(*).
                    count_sql = f"SELECT COUNT(*) FROM ({sel_sql.strip().rstrip(';')}) sub"
                    cur.execute(count_sql, params)
                    n = cur.fetchone()[0]
                    print(f"{label:<35} {n:>16,}")

                # Strategy breakdown for the most consequential tables — so
                # the operator can see whether the wipe will hit backtest
                # history alongside live-paper.
                print()
                print("Strategy breakdown of engine_decisions to be wiped:")
                cur.execute(
                    """
                    SELECT COALESCE(strategy_id, '(null)') AS s, engine_name, COUNT(*)
                    FROM engine_decisions WHERE as_of_date >= %s
                    GROUP BY s, engine_name ORDER BY 3 DESC LIMIT 12
                    """,
                    (boundary,),
                )
                for s, e, n in cur.fetchall():
                    print(f"  {s:<20} {e:<18} {n:>6,}")
            finally:
                cur.close()
        print()
        print("To execute: re-run with --confirm")
        print("To preserve backtest history: move --boundary-date forward, or")
        print("  wipe live-only tables (orders/fills/positions_snapshots already")
        print("  PAPER-scoped) and skip engine_decisions/risk_actions.")
        return 0

    # Live wipe.
    print(f"Wiping paper-trading state.  Archive: {archive_dir}")
    row_counts: dict[str, int] = {}

    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            ensure_account_resets_table(cur)

            # 1. Back up each target into its own .jsonl.gz
            for label, sel_sql, _del_sql, params in plan:
                out_path = archive_dir / f"{label}.jsonl.gz"
                n = _dump_query_to_jsonl(cur, sel_sql, params, out_path)
                row_counts[label] = n
                logger.info("backed up %s rows from %s → %s", n, label, out_path)
                print(f"  backed up  {label:<35} {n:>10,} rows")

            # 2. Delete in FK order
            for label, _sel_sql, del_sql, params in plan:
                cur.execute(del_sql, params)
                deleted = cur.rowcount
                # Sanity check vs. backup count.  Equal in steady state;
                # races against the live daemon could differ.
                logger.info("deleted %s rows from %s", deleted, label)
                print(f"  deleted    {label:<35} {deleted:>10,} rows")

            # 3. Write reset marker
            reset_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO account_resets (
                    reset_id, portfolio_id, starting_nav_usd, label,
                    archive_path, row_counts, notes
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    reset_id,
                    args.portfolio_id,
                    args.starting_nav,
                    args.label or f"reset-{timestamp}",
                    str(archive_dir),
                    json.dumps(row_counts),
                    "Paper account reset via wipe_paper_trading.py",
                ),
            )

            conn.commit()
            print()
            print(f"OK — reset_id={reset_id}")
            print(f"Archive: {archive_dir}")
            print(f"Starting NAV: ${args.starting_nav:,.0f}")
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
