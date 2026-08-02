"""Daily execution-telemetry invariants.

Between 2026-07-13 and 2026-08-01 the account traded at IBKR while the DB
recorded zero fills and expired every order — and nothing noticed for three
weeks.  This module is the cross-check that makes that class of failure a
one-day event: pure-DB assertions over ``positions_snapshots`` / ``fills``
/ ``orders`` / ``portfolio_equity_history``, run daily after
``snapshot_positions``, with violations pushed to the notifications inbox
(idempotent per (as_of_date, kind, source_id), so retries never spam).

All checks are read-only and need no IBKR connection: the position
snapshot taken minutes earlier IS the broker's view, so any disagreement
with the order/fill trail is visible from the DB alone.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:  # pragma: no cover
    from apatheon.core.database import DatabaseManager

logger = logging.getLogger(__name__)

#: Position-quantity mismatch tolerance (shares).
_QTY_EPSILON = 1e-6

#: |day-over-day equity move| beyond this fraction is flagged (marks or
#: NAV-source corruption; a real long-equity book should never print it).
EQUITY_JUMP_THRESHOLD = 0.15

#: SUBMITTED/PENDING orders older than this many days are stuck — the EOD
#: reconcile pass should have filled or expired them.  3 calendar days
#: clears a normal weekend.
STUCK_ORDER_MAX_AGE_DAYS = 3


@dataclass
class Violation:
    check: str
    severity: str  # "critical" | "warning"
    title: str
    detail: str


@dataclass
class InvariantsResult:
    as_of_date: date
    portfolio_id: str
    checks_run: int = 0
    violations: List[Violation] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def critical_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "critical")


def _fetchall(db_manager: "DatabaseManager", sql: str, params: tuple) -> list:
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def _check_snapshot_present(
    db_manager: "DatabaseManager", result: InvariantsResult, mode_db: str,
) -> bool:
    rows = _fetchall(
        db_manager,
        "SELECT COUNT(*) FROM positions_snapshots "
        "WHERE as_of_date = %s AND portfolio_id = %s AND mode = %s",
        (result.as_of_date, result.portfolio_id, mode_db),
    )
    result.checks_run += 1
    if rows[0][0] == 0:
        result.violations.append(
            Violation(
                check="snapshot_missing",
                severity="critical",
                title="No position snapshot for today",
                detail=(
                    f"positions_snapshots has no rows for {result.portfolio_id} "
                    f"on {result.as_of_date}; every downstream invariant and the "
                    "drawdown breaker's equity history depend on this snapshot."
                ),
            )
        )
        return False
    return True


def _check_position_deltas_covered_by_fills(
    db_manager: "DatabaseManager", result: InvariantsResult, mode_db: str,
) -> None:
    """Every equity position change between the last two snapshots must be
    explained by recorded fills in the same window.

    Option contracts are excluded via the inner join to ``instruments``
    (contract ids like ``SPY_260918_742P.US`` have no instruments row);
    their fills follow a separate path.  A mismatch on a split/corporate-
    action day is a known false positive — rare enough to alert anyway.
    """
    prev = _fetchall(
        db_manager,
        "SELECT MAX(as_of_date) FROM positions_snapshots "
        "WHERE as_of_date < %s AND portfolio_id = %s AND mode = %s",
        (result.as_of_date, result.portfolio_id, mode_db),
    )
    result.checks_run += 1
    prev_date = prev[0][0]
    if prev_date is None:
        return  # first snapshot ever — nothing to diff against

    rows = _fetchall(
        db_manager,
        """
        WITH bounds AS (
            SELECT
                (SELECT MAX(timestamp) FROM positions_snapshots
                 WHERE as_of_date = %(prev)s AND portfolio_id = %(pid)s AND mode = %(mode)s) AS t0,
                (SELECT MAX(timestamp) FROM positions_snapshots
                 WHERE as_of_date = %(cur)s AND portfolio_id = %(pid)s AND mode = %(mode)s) AS t1
        ),
        snap AS (
            SELECT s.instrument_id, s.as_of_date, s.quantity
            FROM positions_snapshots s
            JOIN instruments i ON i.instrument_id = s.instrument_id
            WHERE s.as_of_date IN (%(prev)s, %(cur)s)
              AND s.portfolio_id = %(pid)s AND s.mode = %(mode)s
              AND s.timestamp = (
                  SELECT MAX(s2.timestamp) FROM positions_snapshots s2
                  WHERE s2.as_of_date = s.as_of_date
                    AND s2.portfolio_id = s.portfolio_id AND s2.mode = s.mode
              )
        ),
        deltas AS (
            SELECT
                COALESCE(c.instrument_id, p.instrument_id) AS instrument_id,
                COALESCE(c.quantity, 0) - COALESCE(p.quantity, 0) AS dqty
            FROM (SELECT * FROM snap WHERE as_of_date = %(cur)s) c
            FULL OUTER JOIN (SELECT * FROM snap WHERE as_of_date = %(prev)s) p
                USING (instrument_id)
        ),
        fill_net AS (
            SELECT f.instrument_id,
                   SUM(CASE WHEN UPPER(f.side) = 'SELL' THEN -f.quantity
                            ELSE f.quantity END) AS net_qty
            FROM fills f, bounds b
            WHERE f.mode = %(mode)s
              AND f.timestamp > b.t0 AND f.timestamp <= b.t1
            GROUP BY f.instrument_id
        )
        SELECT d.instrument_id, d.dqty, COALESCE(fn.net_qty, 0) AS filled
        FROM deltas d
        LEFT JOIN fill_net fn USING (instrument_id)
        WHERE ABS(d.dqty - COALESCE(fn.net_qty, 0)) > %(eps)s
        ORDER BY ABS(d.dqty - COALESCE(fn.net_qty, 0)) DESC
        LIMIT 20
        """,
        {
            "prev": prev_date,
            "cur": result.as_of_date,
            "pid": result.portfolio_id,
            "mode": mode_db,
            "eps": _QTY_EPSILON,
        },
    )
    result.checks_run += 1
    if rows:
        sample = ", ".join(
            f"{iid}: Δ{dq:+g} vs fills {fq:+g}" for iid, dq, fq in rows[:5]
        )
        result.violations.append(
            Violation(
                check="position_delta_without_fills",
                severity="critical",
                title=f"{len(rows)} position change(s) not covered by recorded fills",
                detail=(
                    f"Between {prev_date} and {result.as_of_date}: {sample}. "
                    "The broker traded but the fill trail disagrees — check "
                    "reconcile_fills_eod before trusting any PnL or order state. "
                    "(Split/corporate-action days can false-positive.)"
                ),
            )
        )


def _check_equity_continuity(
    db_manager: "DatabaseManager", result: InvariantsResult,
) -> None:
    rows = _fetchall(
        db_manager,
        "SELECT as_of_date, equity FROM portfolio_equity_history "
        "WHERE portfolio_id = %s AND as_of_date <= %s "
        "ORDER BY as_of_date DESC LIMIT 2",
        (result.portfolio_id, result.as_of_date),
    )
    result.checks_run += 1
    if not rows or rows[0][0] != result.as_of_date:
        result.violations.append(
            Violation(
                check="equity_missing",
                severity="warning",
                title="No equity history row for today",
                detail=(
                    f"portfolio_equity_history has no {result.as_of_date} row for "
                    f"{result.portfolio_id} — snapshot_positions may have silently "
                    "failed; the drawdown breaker peak is now stale."
                ),
            )
        )
        return
    if len(rows) == 2 and rows[1][1]:
        # equity column is NUMERIC → Decimal; normalize before float math
        cur_eq, prev_eq = float(rows[0][1]), float(rows[1][1])
        change = abs(cur_eq / prev_eq - 1.0)
        if change > EQUITY_JUMP_THRESHOLD:
            result.violations.append(
                Violation(
                    check="equity_jump",
                    severity="warning",
                    title=f"Equity moved {change:.1%} in one day",
                    detail=(
                        f"{rows[1][0]}: {prev_eq:,.0f} → {rows[0][0]}: "
                        f"{cur_eq:,.0f}. Beyond {EQUITY_JUMP_THRESHOLD:.0%} — "
                        "verify marks (option structures / NAV source) before "
                        "trusting the equity curve."
                    ),
                )
            )


def _check_orders_hygiene(
    db_manager: "DatabaseManager", result: InvariantsResult, mode_db: str,
) -> None:
    stuck = _fetchall(
        db_manager,
        "SELECT COUNT(*), MIN(timestamp) FROM orders "
        "WHERE UPPER(mode) = %s AND status IN ('SUBMITTED', 'PENDING') "
        "AND timestamp < %s::date - make_interval(days => %s)",
        (mode_db, result.as_of_date, STUCK_ORDER_MAX_AGE_DAYS),
    )
    result.checks_run += 1
    if stuck[0][0]:
        result.violations.append(
            Violation(
                check="orders_stuck_nonterminal",
                severity="warning",
                title=f"{stuck[0][0]} order(s) stuck SUBMITTED/PENDING",
                detail=(
                    f"Oldest from {stuck[0][1]}; the EOD reconcile pass should "
                    "have filled or expired them within "
                    f"{STUCK_ORDER_MAX_AGE_DAYS} days."
                ),
            )
        )

    contradiction = _fetchall(
        db_manager,
        "SELECT COUNT(DISTINCT o.order_id) FROM orders o "
        "JOIN fills f ON f.order_id = o.order_id "
        "WHERE UPPER(o.mode) = %s AND o.status = 'CANCELLED'",
        (mode_db,),
    )
    result.checks_run += 1
    if contradiction[0][0]:
        result.violations.append(
            Violation(
                check="cancelled_order_has_fills",
                severity="critical",
                title=f"{contradiction[0][0]} CANCELLED order(s) have recorded fills",
                detail=(
                    "An order was expired as unfilled but fills exist for it — "
                    "the expiry policy fired on an order that actually executed. "
                    "Order state and position math cannot both be right."
                ),
            )
        )


def run_invariants_check(
    db_manager: "DatabaseManager",
    as_of_date: date,
    *,
    portfolio_id: str = "IBKR_PAPER",
    mode: str = "paper",
    notify: bool = True,
) -> InvariantsResult:
    """Run all execution-telemetry invariants for one day.

    Never raises: per-check failures land in ``result.errors``.  With
    ``notify=True`` each violation becomes a notifications-inbox row keyed
    ``invariant_<check>`` (idempotent per day+check).
    """
    mode_db = "LIVE" if str(mode).lower() == "live" else "PAPER"
    result = InvariantsResult(as_of_date=as_of_date, portfolio_id=portfolio_id)

    checks = [
        lambda: _check_snapshot_present(db_manager, result, mode_db),
        lambda: _check_position_deltas_covered_by_fills(db_manager, result, mode_db),
        lambda: _check_equity_continuity(db_manager, result),
        lambda: _check_orders_hygiene(db_manager, result, mode_db),
    ]
    # If today's snapshot is missing, the delta check would diff two stale
    # days and report noise — run it only when the snapshot exists.
    snapshot_ok = True
    for i, check in enumerate(checks):
        if i == 1 and not snapshot_ok:
            continue
        try:
            outcome = check()
            if i == 0:
                snapshot_ok = bool(outcome)
        except Exception as exc:  # per-check isolation
            result.errors.append(f"{check.__name__ if hasattr(check, '__name__') else i}: {exc}")
            logger.warning("invariants: check %d failed", i, exc_info=True)

    for v in result.violations:
        log = logger.error if v.severity == "critical" else logger.warning
        log("INVARIANT [%s %s]: %s — %s", portfolio_id, as_of_date, v.title, v.detail)

    if notify and result.violations:
        try:
            from prometheus.meta.notifications import record_notification

            for v in result.violations:
                record_notification(
                    db_manager,
                    as_of_date=as_of_date,
                    kind=f"invariant_{v.check}",
                    severity="critical" if v.severity == "critical" else "warning",
                    title=v.title,
                    body=v.detail,
                    source_table="positions_snapshots",
                    source_id=f"{portfolio_id}:{as_of_date}:{v.check}",
                    link_path="/execution",
                )
        except Exception as exc:
            result.errors.append(f"notify: {exc}")
            logger.warning("invariants: notification write failed", exc_info=True)

    if not result.violations:
        logger.info(
            "INVARIANTS [%s %s]: OK — %d checks clean",
            portfolio_id, as_of_date, result.checks_run,
        )
    return result
