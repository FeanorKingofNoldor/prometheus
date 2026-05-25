"""Dynamic risk constraints driven by geo-risk + compound-pressure signals.

Wraps the static :class:`StrategyRiskConfig` with a portfolio-aware
dampener:

* When the portfolio's composite geo-risk score is elevated, per-name
  caps are scaled down (less concentration in any one name during
  geopolitically fragile periods).
* When compound-pressure alerts target sovereigns the portfolio is
  exposed to, the dampener tightens further.

The dampener is multiplicative on top of the static config, so it
**only ever shrinks** position caps — never expands them — and is safe
to enable by default.

Public entry point: :func:`get_dynamic_strategy_risk_config`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.logging import get_logger

from prometheus.risk.constraints import StrategyRiskConfig, get_strategy_risk_config

logger = get_logger(__name__)


# Floor on the dampener so we never disable trading entirely.
_MIN_DAMPENER = 0.40

# Trip points for the geo-risk score (0–100).  The dampener
# interpolates linearly between these.
_GEO_LOW = 30.0   # below this: no dampening (multiplier=1.0)
_GEO_HIGH = 80.0  # at or above this: floor (multiplier=_MIN_DAMPENER)

# Compound-pressure severity → fixed multiplier.  Applied after the
# geo-risk dampener; stays at 1.0 unless one or more alerts hit HIGH or
# CRITICAL targeting an entity the portfolio is exposed to.
_COMPOUND_MULTIPLIER = {
    "LOW": 1.0,
    "MODERATE": 1.0,
    "HIGH": 0.85,
    "CRITICAL": 0.70,
}


# Per-strategy overrides.  When a strategy's dampener is **disabled**,
# the static config flows through unchanged — useful for hedge books
# that *should* concentrate during stress (their whole job is to
# express the hedge thesis).  Per-strategy floors clamp how aggressively
# the dampener can shrink the cap, since some strategies need a minimum
# size to be expressive at all.
_STRATEGY_DAMPENER_DISABLED: frozenset[str] = frozenset(
    {
        # Hedge books are *meant* to lean in during stress
        "US_EQ_HEDGE_ETF",
        # Hedge sleeves expressing structural views — operator runs them
        # at intentional concentration; dampening would defeat the point.
        "US_EQ_TAIL_HEDGE",
    }
)

_STRATEGY_DAMPENER_FLOOR: dict[str, float] = {
    # Allocator needs ≥80% of base capacity even under stress so it can
    # actually rotate exposure (it allocates between sleeves rather than
    # concentrating on names).
    "US_EQ_ALLOCATOR": 0.80,
}


def _strategy_floor(strategy_id: str) -> float:
    """Return the minimum dampener allowed for ``strategy_id``."""
    return _STRATEGY_DAMPENER_FLOOR.get(strategy_id, _MIN_DAMPENER)


def _strategy_dampener_disabled(strategy_id: str) -> bool:
    return strategy_id in _STRATEGY_DAMPENER_DISABLED


@dataclass(frozen=True)
class DampenerInputs:
    """Inputs that drove the dampener — kept for logging / decision metadata."""

    overall_geo_risk: float | None
    compound_severities: tuple[str, ...]
    portfolio_exposed_isos: tuple[str, ...]
    base_max_weight: float
    dampener: float


def _disabled() -> bool:
    raw = os.environ.get("PROMETHEUS_DYNAMIC_RISK_DAMPENER", "").strip().lower()
    return raw in {"0", "false", "no", "off"}


def _interpolate_geo_dampener(score: float | None) -> float:
    if score is None:
        return 1.0
    if score <= _GEO_LOW:
        return 1.0
    if score >= _GEO_HIGH:
        return _MIN_DAMPENER
    # Linear interpolation
    span = _GEO_HIGH - _GEO_LOW
    frac = (score - _GEO_LOW) / span
    return 1.0 - frac * (1.0 - _MIN_DAMPENER)


def _read_latest_geo_risk(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
) -> tuple[float | None, set[str]]:
    """Return (overall_risk_score, set_of_iso3_with_exposure)."""
    sql = """
        SELECT overall_risk_score, exposure
        FROM portfolio_geo_risk_snapshots
        WHERE portfolio_id = %s
        ORDER BY as_of_date DESC
        LIMIT 1
    """
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (portfolio_id,))
            row = cur.fetchone()
        finally:
            cur.close()
    if row is None:
        return None, set()

    score = float(row[0]) if row[0] is not None else None
    exposure = row[1] or {}
    exposed_isos: set[str] = set()
    nation_concentration = exposure.get("nation_concentration") or {}
    if isinstance(nation_concentration, dict):
        exposed_isos = {str(iso).upper() for iso in nation_concentration.keys() if iso}
    return score, exposed_isos


def _read_active_compound_severities(
    db_manager: DatabaseManager,
    *,
    as_of_date: date | None,
    exposed_isos: Iterable[str],
) -> list[str]:
    """Return severities of HIGH+ alerts targeting any sovereign the
    portfolio is exposed to."""
    isos = [iso.upper() for iso in exposed_isos]
    if not isos:
        return []

    placeholders = ", ".join(["%s"] * len(isos))
    if as_of_date is not None:
        sql = f"""
            SELECT severity FROM compound_pressure_alerts
            WHERE as_of_date = %s
              AND target_entity_type = 'SOVEREIGN'
              AND severity IN ('HIGH', 'CRITICAL')
              AND UPPER(target_entity_id) IN ({placeholders})
        """
        params: list = [as_of_date, *isos]
    else:
        sql = f"""
            SELECT severity FROM compound_pressure_alerts
            WHERE as_of_date = (SELECT MAX(as_of_date) FROM compound_pressure_alerts)
              AND target_entity_type = 'SOVEREIGN'
              AND severity IN ('HIGH', 'CRITICAL')
              AND UPPER(target_entity_id) IN ({placeholders})
        """
        params = list(isos)

    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        finally:
            cur.close()

    return [str(r[0]) for r in rows]


def _compound_dampener(severities: Iterable[str]) -> float:
    """Pick the worst (lowest) multiplier across all active alerts."""
    best = 1.0
    for s in severities:
        m = _COMPOUND_MULTIPLIER.get(s, 1.0)
        if m < best:
            best = m
    return best


def compute_dampener(
    *,
    portfolio_id: str,
    strategy_id: str | None = None,
    as_of_date: date | None = None,
    db_manager: DatabaseManager | None = None,
    geo_score: float | None = None,
    compound_severities: Iterable[str] | None = None,
    exposed_isos: Iterable[str] | None = None,
) -> tuple[float, DampenerInputs]:
    """Compute the multiplicative dampener for this portfolio.

    Returns ``(multiplier, inputs)`` where ``multiplier`` is in
    ``[strategy_floor, 1.0]``.  ``strategy_id`` controls per-strategy
    overrides (some strategies opt out entirely; some clamp to a higher
    floor).  Tests can pass ``geo_score`` / ``compound_severities`` /
    ``exposed_isos`` directly to bypass DB I/O.
    """
    if _disabled() or (strategy_id and _strategy_dampener_disabled(strategy_id)):
        return 1.0, DampenerInputs(
            overall_geo_risk=None,
            compound_severities=(),
            portfolio_exposed_isos=(),
            base_max_weight=0.0,
            dampener=1.0,
        )

    if (
        geo_score is None
        or compound_severities is None
        or exposed_isos is None
    ):
        if db_manager is None:
            db_manager = get_db_manager()
        try:
            score_db, exposed_db = _read_latest_geo_risk(
                db_manager, portfolio_id=portfolio_id,
            )
        except Exception:
            logger.exception("[dynamic_risk] failed reading geo-risk snapshot")
            score_db, exposed_db = None, set()
        if geo_score is None:
            geo_score = score_db
        if exposed_isos is None:
            exposed_isos = exposed_db
        if compound_severities is None:
            try:
                compound_severities = _read_active_compound_severities(
                    db_manager,
                    as_of_date=as_of_date,
                    exposed_isos=exposed_isos,
                )
            except Exception:
                logger.exception("[dynamic_risk] failed reading compound severities")
                compound_severities = []

    geo_mult = _interpolate_geo_dampener(geo_score)
    comp_mult = _compound_dampener(compound_severities or [])
    floor = _strategy_floor(strategy_id) if strategy_id else _MIN_DAMPENER
    multiplier = max(floor, geo_mult * comp_mult)

    inputs = DampenerInputs(
        overall_geo_risk=geo_score,
        compound_severities=tuple(compound_severities or []),
        portfolio_exposed_isos=tuple(sorted(exposed_isos or [])),
        base_max_weight=0.0,
        dampener=multiplier,
    )

    logger.info(
        "[dynamic_risk] portfolio=%s geo_score=%s compound=%s dampener=%.2f",
        portfolio_id,
        f"{geo_score:.1f}" if geo_score is not None else "—",
        list(compound_severities or []),
        multiplier,
    )
    return multiplier, inputs


def get_dynamic_strategy_risk_config(
    strategy_id: str,
    *,
    portfolio_id: str | None = None,
    as_of_date: date | None = None,
    db_manager: DatabaseManager | None = None,
) -> tuple[StrategyRiskConfig, DampenerInputs | None]:
    """Return a strategy risk config with the dampener applied.

    When ``portfolio_id`` is None the dampener is skipped and the static
    config is returned unchanged (with ``DampenerInputs`` set to None).
    """
    base = get_strategy_risk_config(strategy_id)
    if (
        portfolio_id is None
        or _disabled()
        or _strategy_dampener_disabled(strategy_id)
    ):
        return base, None

    multiplier, inputs = compute_dampener(
        portfolio_id=portfolio_id,
        strategy_id=strategy_id,
        as_of_date=as_of_date,
        db_manager=db_manager,
    )
    if multiplier >= 0.999:
        return base, inputs

    adjusted = StrategyRiskConfig(
        strategy_id=base.strategy_id,
        max_abs_weight_per_name=base.max_abs_weight_per_name * multiplier,
    )
    inputs = DampenerInputs(
        overall_geo_risk=inputs.overall_geo_risk,
        compound_severities=inputs.compound_severities,
        portfolio_exposed_isos=inputs.portfolio_exposed_isos,
        base_max_weight=base.max_abs_weight_per_name,
        dampener=multiplier,
    )
    return adjusted, inputs
