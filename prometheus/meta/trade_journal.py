"""Prometheus -- Trade Journal.

Records every trade decision with full context for post-mortem analysis.
After the 2-3 month validation period, this data answers:
- Which decisions made money and why?
- Which regime/sector combinations work best?
- Is the assessment model predicting correctly?
- Are we losing money to execution (slippage, timing)?

Persists to `trade_journal` table. Each entry captures the full state
at decision time so we can reconstruct what the system "saw" when it
decided to trade.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class JournalEntry:
    """One trade decision with full context."""
    as_of_date: date
    instrument_id: str
    action: str              # BUY / SELL / HOLD
    quantity: float
    target_weight: float
    actual_weight: float

    # Context at decision time
    regime: str
    forward_signal: str
    vix: float
    sector: str
    momentum_score: float
    conviction_score: float
    embedding_outlier_rank: float

    # Execution
    order_type: str
    limit_price: Optional[float]
    fill_price: Optional[float]
    slippage_bps: Optional[float]

    # Outcome (filled later by evaluator)
    return_1d: Optional[float] = None
    return_5d: Optional[float] = None
    return_21d: Optional[float] = None


def ensure_trade_journal_table(db_manager: DatabaseManager) -> None:
    """Create trade_journal table if it doesn't exist.

    NOTE: This table is created at runtime rather than via Alembic migration
    because it's a monitoring/meta table that should self-provision on first use.
    Core schema tables (orders, fills, positions) use Alembic migrations.
    """
    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trade_journal (
                    journal_id SERIAL PRIMARY KEY,
                    as_of_date DATE NOT NULL,
                    instrument_id VARCHAR(50) NOT NULL,
                    action VARCHAR(10) NOT NULL,
                    quantity FLOAT,
                    target_weight FLOAT,
                    actual_weight FLOAT,
                    regime VARCHAR(20),
                    forward_signal VARCHAR(10),
                    vix FLOAT,
                    sector VARCHAR(50),
                    momentum_score FLOAT,
                    conviction_score FLOAT,
                    embedding_outlier_rank FLOAT,
                    order_type VARCHAR(20),
                    limit_price FLOAT,
                    fill_price FLOAT,
                    slippage_bps FLOAT,
                    return_1d FLOAT,
                    return_5d FLOAT,
                    return_21d FLOAT,
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_trade_journal_date
                ON trade_journal (as_of_date)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_trade_journal_instrument
                ON trade_journal (instrument_id, as_of_date)
            """)
        conn.commit()
    logger.info("trade_journal table ensured")


def record_journal_entries(
    db_manager: DatabaseManager,
    entries: List[JournalEntry],
) -> int:
    """Persist journal entries to DB. Returns count inserted."""
    if not entries:
        return 0

    sql = """
        INSERT INTO trade_journal (
            as_of_date, instrument_id, action, quantity, target_weight, actual_weight,
            regime, forward_signal, vix, sector, momentum_score,
            conviction_score, embedding_outlier_rank,
            order_type, limit_price, fill_price, slippage_bps
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            for e in entries:
                cur.execute(sql, (
                    e.as_of_date, e.instrument_id, e.action, e.quantity,
                    e.target_weight, e.actual_weight,
                    e.regime, e.forward_signal, e.vix, e.sector,
                    e.momentum_score, e.conviction_score, e.embedding_outlier_rank,
                    e.order_type, e.limit_price, e.fill_price, e.slippage_bps,
                ))
        conn.commit()

    logger.info("Recorded %d trade journal entries for %s", len(entries), entries[0].as_of_date)
    return len(entries)


def backfill_journal_returns(
    db_manager: DatabaseManager,
    as_of_date: date,
) -> int:
    """Fill in return_1d/5d/21d for journal entries that are old enough.

    Called daily to update past entries with realized outcomes.
    """
    updated = 0

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            # Find entries needing 1d return (entries from yesterday)
            cur.execute("""
                SELECT journal_id, instrument_id, as_of_date, fill_price
                FROM trade_journal
                WHERE return_1d IS NULL
                  AND as_of_date < %s
                  AND fill_price IS NOT NULL
                ORDER BY as_of_date
            """, (as_of_date,))
            pending = cur.fetchall()

    if not pending:
        return 0

    with db_manager.get_historical_connection() as hconn:
        with hconn.cursor() as hcur:
            for (jid, inst_id, entry_date, fill_price) in pending:
                if fill_price <= 0:
                    continue

                # Get prices at horizons
                hcur.execute("""
                    SELECT trade_date, close FROM prices_daily
                    WHERE instrument_id = %s AND trade_date > %s
                    ORDER BY trade_date
                    LIMIT 21
                """, (inst_id, entry_date))
                future_prices = hcur.fetchall()

                if not future_prices:
                    continue

                ret_1d = ret_5d = ret_21d = None
                for i, (td, px) in enumerate(future_prices):
                    ret = (float(px) - fill_price) / fill_price
                    if i == 0:
                        ret_1d = ret
                    if i == 4:
                        ret_5d = ret
                    if i == 20:
                        ret_21d = ret

                with db_manager.get_runtime_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE trade_journal
                            SET return_1d = %s, return_5d = %s, return_21d = %s
                            WHERE journal_id = %s
                        """, (ret_1d, ret_5d, ret_21d, jid))
                    conn.commit()
                    updated += 1

    if updated:
        logger.info("Backfilled returns for %d journal entries", updated)
    return updated


def compute_journal_analysis(
    db_manager: DatabaseManager,
    lookback_days: int = 63,
) -> Dict[str, Any]:
    """Analyze trade journal entries for systematic patterns.

    Returns structured analysis for Iris reporting.
    """
    cutoff = date.today() - timedelta(days=lookback_days)

    with db_manager.get_runtime_connection() as conn:
        with conn.cursor() as cur:
            # Overall stats
            cur.execute("""
                SELECT action, COUNT(*), AVG(return_5d), AVG(slippage_bps)
                FROM trade_journal
                WHERE as_of_date >= %s AND return_5d IS NOT NULL
                GROUP BY action
            """, (cutoff,))
            action_stats = {r[0]: {"count": r[1], "avg_return_5d": float(r[2] or 0), "avg_slippage": float(r[3] or 0)}
                           for r in cur.fetchall()}

            # By regime
            cur.execute("""
                SELECT regime, COUNT(*), AVG(return_5d)
                FROM trade_journal
                WHERE as_of_date >= %s AND return_5d IS NOT NULL
                GROUP BY regime
            """, (cutoff,))
            regime_stats = {r[0]: {"count": r[1], "avg_return_5d": float(r[2] or 0)}
                          for r in cur.fetchall()}

            # By sector
            cur.execute("""
                SELECT sector, COUNT(*), AVG(return_5d)
                FROM trade_journal
                WHERE as_of_date >= %s AND return_5d IS NOT NULL
                GROUP BY sector ORDER BY AVG(return_5d) DESC
            """, (cutoff,))
            sector_stats = {r[0]: {"count": r[1], "avg_return_5d": float(r[2] or 0)}
                          for r in cur.fetchall()}

            # By forward signal
            cur.execute("""
                SELECT forward_signal, COUNT(*), AVG(return_5d)
                FROM trade_journal
                WHERE as_of_date >= %s AND return_5d IS NOT NULL
                GROUP BY forward_signal
            """, (cutoff,))
            signal_stats = {r[0]: {"count": r[1], "avg_return_5d": float(r[2] or 0)}
                          for r in cur.fetchall()}

            # Total slippage cost
            cur.execute("""
                SELECT SUM(ABS(slippage_bps) * ABS(quantity) * fill_price / 10000)
                FROM trade_journal
                WHERE as_of_date >= %s AND slippage_bps IS NOT NULL
            """, (cutoff,))
            total_slippage = float(cur.fetchone()[0] or 0)

    return {
        "lookback_days": lookback_days,
        "by_action": action_stats,
        "by_regime": regime_stats,
        "by_sector": sector_stats,
        "by_forward_signal": signal_stats,
        "total_slippage_cost": total_slippage,
    }
