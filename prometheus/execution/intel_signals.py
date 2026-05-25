"""Intel-derived signals consumed by options strategies.

Pulls the latest divergence / convergence / compound-pressure /
beneficiary / scenario decisions from the runtime DB and shapes them
into a flat dict that gets merged into ``build_options_signals`` output.

Strategies read these fields by name (e.g. ``signals.get("divergence_alerts")``)
and use them to:

* Tighten short-vol positions when narrative-vs-reality is EXTREME.
* Concentrate vega on entities with imminent convergence.
* Shift defensive sizing when compound pressure targets a sovereign the
  portfolio is exposed to.
* Allocate Greeks across scenario-tree branches by probability weight
  via :func:`prometheus.signals.scenarios.weight_greeks_by_scenarios`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class IntelSignals:
    """Structured view of recent intel-derived signals."""

    divergence_alerts: list[dict[str, Any]] = field(default_factory=list)
    convergence_timelines: list[dict[str, Any]] = field(default_factory=list)
    compound_pressure: list[dict[str, Any]] = field(default_factory=list)
    portfolio_geo_risk: dict[str, Any] | None = None

    def as_signals_dict(self) -> dict[str, Any]:
        """Return a flat dict suitable for merging into the options signals."""
        return {
            "divergence_alerts": list(self.divergence_alerts),
            "convergence_timelines": list(self.convergence_timelines),
            "compound_pressure": list(self.compound_pressure),
            "portfolio_geo_risk": self.portfolio_geo_risk,
        }

    def has_extreme_divergence(self) -> bool:
        return any(s.get("severity") == "EXTREME" for s in self.divergence_alerts)

    def has_critical_compound_pressure(self) -> bool:
        return any(s.get("severity") == "CRITICAL" for s in self.compound_pressure)


def _read_divergence(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    min_severity: str = "SIGNIFICANT",
    limit: int = 20,
) -> list[dict[str, Any]]:
    sql = """
        SELECT entity_type, entity_id, severity, direction, trading_signal,
               behavioral_score, narrative_score, divergence, decision_id
        FROM divergence_signals
        WHERE as_of_date <= %s
          AND severity IN ('SIGNIFICANT', 'EXTREME')
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM divergence_signals
              WHERE as_of_date <= %s
          )
        ORDER BY abs_divergence DESC
        LIMIT %s
    """
    if min_severity == "EXTREME":
        sql = sql.replace("'SIGNIFICANT', ", "")
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (as_of_date, as_of_date, limit))
            rows = cur.fetchall()
        finally:
            cur.close()
    return [
        {
            "entity_type": r[0],
            "entity_id": r[1],
            "severity": r[2],
            "direction": r[3],
            "trading_signal": r[4],
            "behavioral_score": r[5],
            "narrative_score": r[6],
            "divergence": r[7],
            "decision_id": r[8],
        }
        for r in rows
    ]


def _read_convergence(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    min_confidence: float = 0.5,
    limit: int = 20,
) -> list[dict[str, Any]]:
    sql = """
        SELECT entity_type, entity_id, estimated_convergence_days,
               convergence_window_min, convergence_window_max,
               confidence, strategy, entry_windows, decision_id
        FROM convergence_signals
        WHERE as_of_date <= %s
          AND confidence >= %s
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM convergence_signals
              WHERE as_of_date <= %s
          )
        ORDER BY estimated_convergence_days NULLS LAST
        LIMIT %s
    """
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (as_of_date, min_confidence, as_of_date, limit))
            rows = cur.fetchall()
        finally:
            cur.close()
    return [
        {
            "entity_type": r[0],
            "entity_id": r[1],
            "estimated_convergence_days": r[2],
            "convergence_window": [r[3], r[4]],
            "confidence": r[5],
            "strategy": r[6],
            "entry_windows": r[7] or [],
            "decision_id": r[8],
        }
        for r in rows
    ]


def _read_compound_pressure(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    min_severity: str = "HIGH",
    limit: int = 20,
) -> list[dict[str, Any]]:
    sql = """
        SELECT target_entity_type, target_entity_id, severity,
               encirclement_score, pressure_points_moved, total_pressure_points,
               cluster_days, decision_id
        FROM compound_pressure_alerts
        WHERE as_of_date <= %s
          AND severity IN ('HIGH', 'CRITICAL')
          AND as_of_date = (
              SELECT MAX(as_of_date) FROM compound_pressure_alerts
              WHERE as_of_date <= %s
          )
        ORDER BY encirclement_score DESC
        LIMIT %s
    """
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, (as_of_date, as_of_date, limit))
            rows = cur.fetchall()
        finally:
            cur.close()
    return [
        {
            "target_entity_type": r[0],
            "target_entity_id": r[1],
            "severity": r[2],
            "encirclement_score": r[3],
            "pressure_points_moved": r[4],
            "total_pressure_points": r[5],
            "cluster_days": r[6],
            "decision_id": r[7],
        }
        for r in rows
    ]


def _read_portfolio_geo_risk(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str | None,
) -> dict[str, Any] | None:
    if not portfolio_id:
        return None
    sql = """
        SELECT overall_risk_score, conflict_risk, chokepoint_risk,
               sovereign_risk, sector_risk, ticker_count, decision_id, as_of_date
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
        return None
    return {
        "overall_risk_score": row[0],
        "conflict_risk": row[1],
        "chokepoint_risk": row[2],
        "sovereign_risk": row[3],
        "sector_risk": row[4],
        "ticker_count": row[5],
        "decision_id": row[6],
        "as_of_date": row[7].isoformat() if row[7] else None,
    }


def load_intel_signals(
    *,
    as_of_date: date,
    db_manager: DatabaseManager | None = None,
    portfolio_id: str | None = None,
) -> IntelSignals:
    """Snapshot the four intel signal streams at ``as_of_date``."""
    if db_manager is None:
        db_manager = get_db_manager()

    divergence: list[dict[str, Any]] = []
    convergence: list[dict[str, Any]] = []
    compound: list[dict[str, Any]] = []
    geo_risk: dict[str, Any] | None = None

    try:
        divergence = _read_divergence(db_manager, as_of_date=as_of_date)
    except Exception:
        logger.exception("[intel_signals] divergence read failed")

    try:
        convergence = _read_convergence(db_manager, as_of_date=as_of_date)
    except Exception:
        logger.exception("[intel_signals] convergence read failed")

    try:
        compound = _read_compound_pressure(db_manager, as_of_date=as_of_date)
    except Exception:
        logger.exception("[intel_signals] compound pressure read failed")

    try:
        geo_risk = _read_portfolio_geo_risk(db_manager, portfolio_id=portfolio_id)
    except Exception:
        logger.exception("[intel_signals] geo risk read failed")

    return IntelSignals(
        divergence_alerts=divergence,
        convergence_timelines=convergence,
        compound_pressure=compound,
        portfolio_geo_risk=geo_risk,
    )


def options_sizing_multiplier(intel: IntelSignals) -> float:
    """Return a [0.5, 1.5] multiplier on options notional.

    * Extreme narrative-vs-reality divergence boosts vega allocation.
    * Critical compound pressure boosts hedge sizing.
    * Elevated portfolio geo risk caps sizing.

    Used by callers that want a single scalar to scale option Greeks; the
    individual signal lists are still available for finer-grained rules.
    """
    multiplier = 1.0
    if intel.has_extreme_divergence():
        multiplier *= 1.20
    if intel.has_critical_compound_pressure():
        multiplier *= 1.15
    geo = intel.portfolio_geo_risk
    if geo and (geo.get("overall_risk_score") or 0.0) >= 70:
        multiplier *= 0.85
    return max(0.5, min(1.5, multiplier))
