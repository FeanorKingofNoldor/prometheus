"""Apatheon intel signals → derivatives template inputs.

The CONVEX sleeve's templates fire on stacked intel — compound
pressure on a sovereign, divergence between narrative and reality,
imminent convergence of an entity's behaviour with its narrative,
elevated portfolio geo risk. Those signals live in their own tables
(``divergence_signals``, ``convergence_signals``,
``compound_pressure_alerts``, ``portfolio_geo_risk``) populated by
Apatheon's intel pipeline.

This module loads them into a single ``IntelSignalsSnapshot`` and
folds them under well-known keys into the signals dict the sleeve
runner already consumes. That way template triggers stay simple — they
read ``signals["divergence"]`` / ``signals["compound_pressure_targets"]``
/ etc. without knowing where the data came from.

The signal-to-action map is also here, as small query methods on the
snapshot, so it's easy to reason about which combinations of signals
fire which template.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger

logger = get_logger(__name__)


# ── Severity ordering (highest → lowest) ─────────────────────────────

SEVERITY_RANK: Mapping[str, int] = {
    "CRITICAL": 4, "EXTREME": 4,
    "HIGH": 3, "SIGNIFICANT": 3,
    "MODERATE": 2, "ELEVATED": 2,
    "LOW": 1, "MILD": 1, "NORMAL": 1,
}


# ── Snapshot ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class IntelSignalsSnapshot:
    """One day's worth of intel signals, queryable by templates."""

    as_of_date: date
    divergence: list[dict] = field(default_factory=list)
    convergence: list[dict] = field(default_factory=list)
    compound_pressure: list[dict] = field(default_factory=list)
    geo_risk: dict | None = None

    # ── Helpers for template triggers ──

    def overall_geo_risk_score(self) -> float:
        if not self.geo_risk:
            return 0.0
        return float(self.geo_risk.get("overall_risk_score", 0.0) or 0.0)

    def critical_compound_pressure(
        self, *, min_severity: str = "HIGH",
    ) -> list[dict]:
        floor = SEVERITY_RANK.get(min_severity.upper(), 3)
        return [
            a for a in self.compound_pressure
            if SEVERITY_RANK.get(str(a.get("severity", "")).upper(), 0) >= floor
        ]

    def extreme_divergences(self, *, min_severity: str = "EXTREME") -> list[dict]:
        floor = SEVERITY_RANK.get(min_severity.upper(), 4)
        return [
            d for d in self.divergence
            if SEVERITY_RANK.get(str(d.get("severity", "")).upper(), 0) >= floor
        ]

    def imminent_convergences(
        self, *, max_days: float = 30.0, min_confidence: float = 0.5,
    ) -> list[dict]:
        out: list[dict] = []
        for c in self.convergence:
            days = c.get("estimated_convergence_days")
            conf = c.get("confidence", 0.0)
            if days is None:
                continue
            try:
                if float(days) <= max_days and float(conf) >= min_confidence:
                    out.append(c)
            except (TypeError, ValueError):
                continue
        return out

    def divergences_for_entity(
        self, entity_type: str, entity_id: str,
    ) -> list[dict]:
        et = entity_type.upper()
        eid = entity_id.upper()
        return [
            d for d in self.divergence
            if str(d.get("entity_type", "")).upper() == et
            and str(d.get("entity_id", "")).upper() == eid
        ]


# ── Loader ──────────────────────────────────────────────────────────


def load_intel_signals(
    db_manager: DatabaseManager,
    *,
    as_of_date: date,
    portfolio_id: str | None = None,
) -> IntelSignalsSnapshot:
    """Load all intel signals for a date into a single snapshot.

    ``portfolio_id`` scopes the ``portfolio_geo_risk`` lookup. The
    other three tables are global per-date.
    """
    divergence = _load_divergence(db_manager, as_of_date)
    convergence = _load_convergence(db_manager, as_of_date)
    compound = _load_compound_pressure(db_manager, as_of_date)
    geo = _load_portfolio_geo_risk(db_manager, as_of_date, portfolio_id)

    return IntelSignalsSnapshot(
        as_of_date=as_of_date,
        divergence=divergence,
        convergence=convergence,
        compound_pressure=compound,
        geo_risk=geo,
    )


def merge_into_signals(
    base_signals: Mapping[str, Any],
    intel: IntelSignalsSnapshot,
) -> dict[str, Any]:
    """Fold intel signals into the runner's signals dict.

    Adds well-known keys:

    * ``intel`` — the full snapshot (for inspection)
    * ``divergence``, ``convergence`` — list of dicts
    * ``compound_pressure_targets`` — list of compound pressure dicts
    * ``geo_risk_score`` — overall portfolio geo risk
    * ``geo_risk`` — full geo risk dict

    Existing ``compound_pressure`` key (single-dict form expected by
    the legacy ``convex.thematic_sector_put`` trigger) is also
    populated from the highest-severity target.
    """
    out: dict[str, Any] = dict(base_signals)
    out["intel"] = intel
    out["divergence"] = intel.divergence
    out["convergence"] = intel.convergence
    out["compound_pressure_targets"] = intel.compound_pressure
    out["geo_risk_score"] = intel.overall_geo_risk_score()
    out["geo_risk"] = intel.geo_risk

    # Back-compat: the existing convex.thematic_sector_put template
    # reads signals["compound_pressure"] as a single-dict. Populate it
    # from the highest-severity target so the legacy template still
    # fires.
    cp_targets = intel.critical_compound_pressure()
    if cp_targets:
        cp_targets.sort(
            key=lambda a: SEVERITY_RANK.get(
                str(a.get("severity", "")).upper(), 0,
            ), reverse=True,
        )
        top = cp_targets[0]
        sector_etf = _target_to_sector_etf(top)
        out["compound_pressure"] = {
            "severity": str(top.get("severity", "")).upper(),
            "target_sector_etf": sector_etf,
            "target_entity_id": top.get("target_entity_id"),
            "target_entity_type": top.get("target_entity_type"),
            "encirclement_score": top.get("encirclement_score"),
        }
    return out


# ── Internal loaders ────────────────────────────────────────────────


def _load_divergence(db: DatabaseManager, as_of: date) -> list[dict]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT signal_id, entity_type, entity_id,
                       behavioral_score, narrative_score, divergence,
                       abs_divergence, direction, severity, trading_signal
                FROM divergence_signals
                WHERE as_of_date = %s
                """,
                (as_of,),
            )
            cols = [
                "signal_id", "entity_type", "entity_id",
                "behavioral_score", "narrative_score", "divergence",
                "abs_divergence", "direction", "severity", "trading_signal",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_convergence(db: DatabaseManager, as_of: date) -> list[dict]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT signal_id, entity_type, entity_id,
                       estimated_convergence_days, confidence,
                       strategy, days_to_hard_deadline,
                       days_to_soft_signal
                FROM convergence_signals
                WHERE as_of_date = %s
                """,
                (as_of,),
            )
            cols = [
                "signal_id", "entity_type", "entity_id",
                "estimated_convergence_days", "confidence",
                "strategy", "days_to_hard_deadline", "days_to_soft_signal",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_compound_pressure(db: DatabaseManager, as_of: date) -> list[dict]:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT alert_id, target_entity_type, target_entity_id,
                       encirclement_score, severity,
                       pressure_points_moved, total_pressure_points
                FROM compound_pressure_alerts
                WHERE as_of_date = %s
                """,
                (as_of,),
            )
            cols = [
                "alert_id", "target_entity_type", "target_entity_id",
                "encirclement_score", "severity",
                "pressure_points_moved", "total_pressure_points",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_portfolio_geo_risk(
    db: DatabaseManager, as_of: date, portfolio_id: str | None,
) -> dict | None:
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            if portfolio_id:
                cur.execute(
                    """
                    SELECT portfolio_id, overall_risk_score,
                           conflict_risk, chokepoint_risk,
                           sovereign_risk, sector_risk, ticker_count
                    FROM portfolio_geo_risk
                    WHERE as_of_date = %s AND portfolio_id = %s
                    ORDER BY snapshot_id DESC LIMIT 1
                    """,
                    (as_of, portfolio_id),
                )
            else:
                cur.execute(
                    """
                    SELECT portfolio_id, overall_risk_score,
                           conflict_risk, chokepoint_risk,
                           sovereign_risk, sector_risk, ticker_count
                    FROM portfolio_geo_risk
                    WHERE as_of_date = %s
                    ORDER BY snapshot_id DESC LIMIT 1
                    """,
                    (as_of,),
                )
            row = cur.fetchone()
            if row is None:
                return None
            cols = [
                "portfolio_id", "overall_risk_score",
                "conflict_risk", "chokepoint_risk",
                "sovereign_risk", "sector_risk", "ticker_count",
            ]
            return dict(zip(cols, row))


# ── Sector mapping (mirrors the one in sleeves.py for now) ──────────


_SOVEREIGN_TO_SECTOR_ETF: Mapping[str, str] = {
    # Imperfect but pragmatic: when a sovereign is encircled, which
    # sector ETF is the most exposed proxy on the US side?
    "IRN": "XLE",   # Iran → energy
    "RUS": "XLE",
    "SAU": "XLE",
    "CHN": "XLK",   # China → tech
    "TWN": "XLK",
    "KOR": "XLK",
    "UKR": "XLE",
    "ISR": "XLE",
    "VEN": "XLE",
    "LBN": "XLE",
}


def _target_to_sector_etf(alert: Mapping[str, Any]) -> str | None:
    target_id = str(alert.get("target_entity_id", "") or "").upper()
    return _SOVEREIGN_TO_SECTOR_ETF.get(target_id)


__all__ = [
    "IntelSignalsSnapshot",
    "load_intel_signals",
    "merge_into_signals",
    "SEVERITY_RANK",
]
