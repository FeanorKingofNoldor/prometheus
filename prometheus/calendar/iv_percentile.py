"""IV percentile signal — Phase 5.5.

Reads from ``daily_atm_iv`` history and computes, for a given
underlying, where today's IV sits in the distribution of the last N
trading days. Used by trigger guards: long-debit templates skip when
IV percentile is high (overpriced vol); short-premium templates only
fire when IV percentile is rich.

Needs ~30 trading days of Phase 5.4 history before meaningful. Before
that, ``iv_percentile`` returns ``None`` and guards pass through.
"""

from __future__ import annotations

from datetime import date, timedelta


def iv_percentile(
    underlying: str,
    today: date,
    *,
    lookback_days: int = 252,
    min_observations: int = 30,
) -> float | None:
    """Return today's ATM IV's percentile rank within the trailing
    ``lookback_days`` of stored observations.

    Returns ``None`` when:
      * No DB available
      * Fewer than ``min_observations`` historical rows (not enough
        data to make a reliable percentile)
      * No row for ``today`` (we don't infer from yesterday)

    Result is a fraction in [0.0, 1.0]. 0.95 = today's IV is higher
    than 95% of the lookback distribution → "expensive vol".
    """
    try:
        from apatheon.core.database import get_db_manager
        db = get_db_manager()
        start = today - timedelta(days=lookback_days)
        with db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT snapshot_date, atm_iv
                      FROM daily_atm_iv
                     WHERE underlying = %s
                       AND snapshot_date BETWEEN %s AND %s
                  ORDER BY snapshot_date
                    """,
                    (underlying.upper(), start, today),
                )
                rows = cur.fetchall()
    except Exception:
        return None

    if len(rows) < min_observations:
        return None

    by_date = {d: float(iv) for d, iv in rows}
    today_iv = by_date.get(today)
    if today_iv is None:
        return None

    historical = [iv for d, iv in by_date.items() if d < today]
    if not historical:
        return None

    below = sum(1 for v in historical if v < today_iv)
    return below / len(historical)


def is_iv_rich(underlying: str, today: date, *, threshold: float = 0.80) -> bool | None:
    """True when today's IV is at/above the ``threshold`` percentile
    (expensive vol — avoid long-debit). None when no signal."""
    pct = iv_percentile(underlying, today)
    if pct is None:
        return None
    return pct >= threshold


def is_iv_cheap(underlying: str, today: date, *, threshold: float = 0.20) -> bool | None:
    """True when today's IV is at/below the ``threshold`` percentile
    (cheap vol — short premium is bad, long debit is good). None when
    no signal."""
    pct = iv_percentile(underlying, today)
    if pct is None:
        return None
    return pct <= threshold
