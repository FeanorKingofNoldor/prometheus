"""Compound-pressure / encirclement detection consumer.

Runs Apatheon's ``detect_compound_pressure`` for a watchlist of sovereign
targets, persists the resulting encirclement alerts, and logs HIGH /
CRITICAL alerts to ``engine_decisions`` so they become candidates for
portfolio defensive shifts (cut beta, raise puts, reduce exposure to
proximate sovereigns).

The targets are sovereigns Prometheus has structural exposure to via
holdings, conflicts, or supply chains.  We don't try to scan every
nation in the graph — instead we focus on a curated G20-equivalent list
that the trading layer actually cares about.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable, Sequence

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import EngineDecision

logger = get_logger(__name__)


# Severities at or above this threshold cause an engine_decisions row.
DECISION_MIN_SEVERITY = "HIGH"

_SEVERITY_RANK = {
    "LOW": 0,
    "MODERATE": 1,
    "HIGH": 2,
    "CRITICAL": 3,
}


# Default targets: sovereigns Prometheus tracks through equity / commodity
# / supply-chain exposure.  Override via run_compound_pressure_scan(targets=…).
DEFAULT_TARGETS: tuple[tuple[str, str], ...] = (
    ("SOVEREIGN", "USA"),
    ("SOVEREIGN", "CHN"),
    ("SOVEREIGN", "RUS"),
    ("SOVEREIGN", "IRN"),
    ("SOVEREIGN", "ISR"),
    ("SOVEREIGN", "SAU"),
    ("SOVEREIGN", "TWN"),
    ("SOVEREIGN", "DEU"),
    ("SOVEREIGN", "JPN"),
    ("SOVEREIGN", "KOR"),
    ("SOVEREIGN", "UKR"),
)


@dataclass(frozen=True)
class CompoundPressureAlert:
    alert_id: str
    as_of_date: date
    target_entity_type: str
    target_entity_id: str
    lookback_days: int
    total_pressure_points: int
    pressure_points_moved: int
    cluster_days: float
    encirclement_score: float
    severity: str
    adversarial_movements: list[dict[str, Any]] = field(default_factory=list)
    likely_orchestrators: list[dict[str, Any]] = field(default_factory=list)
    decision_id: str | None = None
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class CompoundPressureScanResult:
    as_of_date: date
    targets_scanned: int
    rows_persisted: int
    decisions_logged: int
    high_or_above: list[CompoundPressureAlert]


def _meets_threshold(severity: str) -> bool:
    return _SEVERITY_RANK.get(severity, 0) >= _SEVERITY_RANK[DECISION_MIN_SEVERITY]


@dataclass
class CompoundPressureStorage:
    db_manager: DatabaseManager

    def upsert(self, alert: CompoundPressureAlert) -> None:
        sql = """
            INSERT INTO compound_pressure_alerts (
                alert_id, as_of_date,
                target_entity_type, target_entity_id,
                lookback_days, total_pressure_points,
                pressure_points_moved, cluster_days,
                encirclement_score, severity,
                adversarial_movements, likely_orchestrators,
                decision_id, computed_at
            ) VALUES (
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
            ON CONFLICT (as_of_date, target_entity_type, target_entity_id)
            DO UPDATE SET
                lookback_days          = EXCLUDED.lookback_days,
                total_pressure_points  = EXCLUDED.total_pressure_points,
                pressure_points_moved  = EXCLUDED.pressure_points_moved,
                cluster_days           = EXCLUDED.cluster_days,
                encirclement_score     = EXCLUDED.encirclement_score,
                severity               = EXCLUDED.severity,
                adversarial_movements  = EXCLUDED.adversarial_movements,
                likely_orchestrators   = EXCLUDED.likely_orchestrators,
                decision_id = COALESCE(compound_pressure_alerts.decision_id, EXCLUDED.decision_id),
                computed_at            = EXCLUDED.computed_at
        """
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        alert.alert_id,
                        alert.as_of_date,
                        alert.target_entity_type,
                        alert.target_entity_id,
                        alert.lookback_days,
                        alert.total_pressure_points,
                        alert.pressure_points_moved,
                        alert.cluster_days,
                        alert.encirclement_score,
                        alert.severity,
                        Json(alert.adversarial_movements),
                        Json(alert.likely_orchestrators),
                        alert.decision_id,
                        alert.computed_at,
                    ),
                )
                conn.commit()
            finally:
                cur.close()

    def existing_decision_id(
        self,
        *,
        as_of_date: date,
        target_entity_type: str,
        target_entity_id: str,
    ) -> str | None:
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT decision_id FROM compound_pressure_alerts
                    WHERE as_of_date=%s AND target_entity_type=%s AND target_entity_id=%s
                    """,
                    (as_of_date, target_entity_type, target_entity_id),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        return row[0] if row else None

    def list_recent(
        self,
        *,
        as_of_date: date,
        min_severity: str = "MODERATE",
        limit: int = 50,
    ) -> list[CompoundPressureAlert]:
        min_rank = _SEVERITY_RANK.get(min_severity, 1)
        eligible = [s for s, r in _SEVERITY_RANK.items() if r >= min_rank]
        if not eligible:
            return []

        placeholders = ", ".join(["%s"] * len(eligible))
        sql = f"""
            SELECT alert_id, as_of_date, target_entity_type, target_entity_id,
                   lookback_days, total_pressure_points, pressure_points_moved,
                   cluster_days, encirclement_score, severity,
                   adversarial_movements, likely_orchestrators,
                   decision_id, computed_at
            FROM compound_pressure_alerts
            WHERE as_of_date = %s AND severity IN ({placeholders})
            ORDER BY encirclement_score DESC
            LIMIT %s
        """
        params: list[Any] = [as_of_date]
        params.extend(eligible)
        params.append(limit)

        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            finally:
                cur.close()

        out: list[CompoundPressureAlert] = []
        for row in rows:
            out.append(
                CompoundPressureAlert(
                    alert_id=row[0],
                    as_of_date=row[1],
                    target_entity_type=row[2],
                    target_entity_id=row[3],
                    lookback_days=row[4],
                    total_pressure_points=row[5],
                    pressure_points_moved=row[6],
                    cluster_days=row[7] or 0.0,
                    encirclement_score=row[8] or 0.0,
                    severity=row[9],
                    adversarial_movements=row[10] or [],
                    likely_orchestrators=row[11] or [],
                    decision_id=row[12],
                    computed_at=row[13],
                )
            )
        return out


def _detect_for_target(graph: Any, target: tuple[str, str]) -> Any | None:
    """Run apatheon's detector for one target, swallowing failures."""
    from apatheon.graph.simulator import detect_compound_pressure

    try:
        return detect_compound_pressure(graph, target, lookback_days=14)
    except Exception:
        logger.debug("[compound] detect failed for %s:%s", *target, exc_info=True)
        return None


def _bootstrap_graph() -> Any:
    """Indirection so tests can monkeypatch the graph load."""
    from apatheon.graph.bootstrap import bootstrap_graph
    return bootstrap_graph()


def _to_alert(upstream: Any, *, as_of_date: date, decision_id: str | None) -> CompoundPressureAlert:
    target = getattr(upstream, "target_entity")
    if isinstance(target, tuple) and len(target) == 2:
        ttype, tid = target
    else:
        # Fall back to splitting "TYPE:KEY" if the upstream returns a string
        as_str = str(target)
        if ":" in as_str:
            ttype, tid = as_str.split(":", 1)
        else:
            ttype, tid = "SOVEREIGN", as_str
    return CompoundPressureAlert(
        alert_id=generate_uuid(),
        as_of_date=as_of_date,
        target_entity_type=str(ttype),
        target_entity_id=str(tid),
        lookback_days=int(getattr(upstream, "lookback_days", 14)),
        total_pressure_points=int(getattr(upstream, "total_pressure_points", 0)),
        pressure_points_moved=int(getattr(upstream, "pressure_points_moved", 0)),
        cluster_days=float(getattr(upstream, "cluster_days", 0.0) or 0.0),
        encirclement_score=float(getattr(upstream, "encirclement_score", 0.0) or 0.0),
        severity=str(getattr(upstream, "severity", "LOW")),
        adversarial_movements=list(getattr(upstream, "adversarial_movements", []) or []),
        likely_orchestrators=list(getattr(upstream, "likely_orchestrators", []) or []),
        decision_id=decision_id,
        computed_at=datetime.now(timezone.utc),
    )


def _record_decision(*, storage: MetaStorage, alert: CompoundPressureAlert) -> str:
    decision_id = generate_uuid()

    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="COMPOUND_PRESSURE",
        run_id=None,
        strategy_id=None,
        market_id="INTEL",
        as_of_date=alert.as_of_date,
        config_id=None,
        input_refs={
            "target_entity_type": alert.target_entity_type,
            "target_entity_id": alert.target_entity_id,
            "lookback_days": alert.lookback_days,
            "total_pressure_points": alert.total_pressure_points,
        },
        output_refs={
            "severity": alert.severity,
            "encirclement_score": alert.encirclement_score,
            "pressure_points_moved": alert.pressure_points_moved,
            "cluster_days": alert.cluster_days,
            "likely_orchestrators": alert.likely_orchestrators,
        },
        metadata={
            "alert_id": alert.alert_id,
            "rationale": (
                f"Encirclement detected against {alert.target_entity_id}: "
                f"{alert.pressure_points_moved}/{alert.total_pressure_points} "
                f"pressure points moved within {alert.cluster_days:.1f} days. "
                "Consider reducing beta and raising tail-hedge sizing on "
                "exposure to this sovereign and its closest dependencies."
            ),
        },
    )
    storage.save_engine_decision(decision)

    logger.info(
        "[compound] decision_id=%s target=%s:%s severity=%s score=%.2f",
        decision_id,
        alert.target_entity_type,
        alert.target_entity_id,
        alert.severity,
        alert.encirclement_score,
    )
    return decision_id


def run_compound_pressure_scan(
    *,
    as_of_date: date,
    db_manager: DatabaseManager | None = None,
    targets: Sequence[tuple[str, str]] | None = None,
    upstream_results: Sequence[Any] | None = None,
) -> CompoundPressureScanResult:
    """Scan watched targets for compound pressure / encirclement."""
    if db_manager is None:
        db_manager = get_db_manager()

    storage = CompoundPressureStorage(db_manager=db_manager)
    meta = MetaStorage(db_manager=db_manager)

    target_list = list(targets) if targets is not None else list(DEFAULT_TARGETS)

    if upstream_results is not None:
        upstream_iter: Iterable[Any] = upstream_results
    else:
        try:
            graph = _bootstrap_graph()
        except Exception:
            logger.exception("[compound] graph bootstrap failed; skipping scan")
            return CompoundPressureScanResult(
                as_of_date=as_of_date,
                targets_scanned=0,
                rows_persisted=0,
                decisions_logged=0,
                high_or_above=[],
            )
        upstream_iter = (
            res for res in (
                _detect_for_target(graph, t) for t in target_list
            ) if res is not None
        )

    rows_persisted = 0
    decisions_logged = 0
    high_or_above: list[CompoundPressureAlert] = []
    targets_scanned = 0

    for upstream in upstream_iter:
        targets_scanned += 1
        try:
            target = getattr(upstream, "target_entity", None)
            if isinstance(target, tuple) and len(target) == 2:
                ttype, tid = str(target[0]), str(target[1])
            else:
                as_str = str(target)
                if ":" in as_str:
                    ttype, tid = as_str.split(":", 1)
                else:
                    ttype, tid = "SOVEREIGN", as_str

            existing = storage.existing_decision_id(
                as_of_date=as_of_date,
                target_entity_type=ttype,
                target_entity_id=tid,
            )
            decision_id: str | None = existing

            alert = _to_alert(upstream, as_of_date=as_of_date, decision_id=decision_id)

            if decision_id is None and _meets_threshold(alert.severity):
                decision_id = _record_decision(storage=meta, alert=alert)
                decisions_logged += 1
                alert = CompoundPressureAlert(
                    **{**alert.__dict__, "decision_id": decision_id}
                )

            storage.upsert(alert)
            rows_persisted += 1

            if _meets_threshold(alert.severity):
                high_or_above.append(alert)
        except Exception:
            logger.exception("[compound] failed to persist alert")

    logger.info(
        "[compound] scan complete date=%s targets=%d persisted=%d decisions=%d high+=%d",
        as_of_date.isoformat(),
        targets_scanned,
        rows_persisted,
        decisions_logged,
        len(high_or_above),
    )

    return CompoundPressureScanResult(
        as_of_date=as_of_date,
        targets_scanned=targets_scanned,
        rows_persisted=rows_persisted,
        decisions_logged=decisions_logged,
        high_or_above=high_or_above,
    )
