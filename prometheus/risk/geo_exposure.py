"""Per-portfolio geopolitical exposure scanner.

Reads the most recent IBKR positions snapshot, calls Apatheon's
``analyze_exposure`` to compute geopolitical risk (conflict, chokepoint,
sovereign, sector breakdown), and persists the result to
``portfolio_geo_risk_snapshots``.  Composite scores ≥ ``DECISION_MIN_SCORE``
are logged to ``engine_decisions`` as a ``GEO_RISK`` row so the
Meta-Orchestrator can track elevated-geo periods against realised
portfolio outcomes.

The persisted JSON ``exposure`` blob keeps the full
``PortfolioExposure.to_dict()`` so downstream consumers (sizing
constraints, the C2 dashboard) can drill into per-conflict / per-ticker
breakdowns without re-running the scan.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import EngineDecision

logger = get_logger(__name__)


# Composite risk ≥ this value triggers a GEO_RISK decision row.
DECISION_MIN_SCORE = 40.0  # 0–100 scale, 40 = early-warning


@dataclass(frozen=True)
class PortfolioGeoRiskSnapshot:
    snapshot_id: str
    portfolio_id: str
    as_of_date: date
    overall_risk_score: float
    conflict_risk: float
    chokepoint_risk: float
    sovereign_risk: float
    sector_risk: float
    ticker_count: int
    exposure: dict[str, Any] = field(default_factory=dict)
    decision_id: str | None = None
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class GeoExposureScanResult:
    portfolio_id: str
    as_of_date: date
    snapshot: PortfolioGeoRiskSnapshot | None
    decision_logged: bool


@dataclass
class PortfolioGeoRiskStorage:
    db_manager: DatabaseManager

    def upsert(self, snap: PortfolioGeoRiskSnapshot) -> None:
        sql = """
            INSERT INTO portfolio_geo_risk_snapshots (
                snapshot_id, portfolio_id, as_of_date,
                overall_risk_score,
                conflict_risk, chokepoint_risk, sovereign_risk, sector_risk,
                ticker_count, exposure, decision_id, computed_at
            ) VALUES (
                %s, %s, %s,
                %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (portfolio_id, as_of_date)
            DO UPDATE SET
                overall_risk_score = EXCLUDED.overall_risk_score,
                conflict_risk      = EXCLUDED.conflict_risk,
                chokepoint_risk    = EXCLUDED.chokepoint_risk,
                sovereign_risk     = EXCLUDED.sovereign_risk,
                sector_risk        = EXCLUDED.sector_risk,
                ticker_count       = EXCLUDED.ticker_count,
                exposure           = EXCLUDED.exposure,
                decision_id        = COALESCE(portfolio_geo_risk_snapshots.decision_id, EXCLUDED.decision_id),
                computed_at        = EXCLUDED.computed_at
        """
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        snap.snapshot_id,
                        snap.portfolio_id,
                        snap.as_of_date,
                        snap.overall_risk_score,
                        snap.conflict_risk,
                        snap.chokepoint_risk,
                        snap.sovereign_risk,
                        snap.sector_risk,
                        snap.ticker_count,
                        Json(snap.exposure),
                        snap.decision_id,
                        snap.computed_at,
                    ),
                )
                conn.commit()
            finally:
                cur.close()

    def existing_decision_id(
        self,
        *,
        portfolio_id: str,
        as_of_date: date,
    ) -> str | None:
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT decision_id FROM portfolio_geo_risk_snapshots
                    WHERE portfolio_id=%s AND as_of_date=%s
                    """,
                    (portfolio_id, as_of_date),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        return row[0] if row else None

    def latest(self, portfolio_id: str) -> PortfolioGeoRiskSnapshot | None:
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT snapshot_id, portfolio_id, as_of_date,
                           overall_risk_score, conflict_risk, chokepoint_risk,
                           sovereign_risk, sector_risk, ticker_count,
                           exposure, decision_id, computed_at
                    FROM portfolio_geo_risk_snapshots
                    WHERE portfolio_id=%s
                    ORDER BY as_of_date DESC
                    LIMIT 1
                    """,
                    (portfolio_id,),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        if row is None:
            return None
        return PortfolioGeoRiskSnapshot(
            snapshot_id=row[0],
            portfolio_id=row[1],
            as_of_date=row[2],
            overall_risk_score=row[3] or 0.0,
            conflict_risk=row[4] or 0.0,
            chokepoint_risk=row[5] or 0.0,
            sovereign_risk=row[6] or 0.0,
            sector_risk=row[7] or 0.0,
            ticker_count=row[8] or 0,
            exposure=row[9] or {},
            decision_id=row[10],
            computed_at=row[11],
        )


def _read_portfolio_holdings(
    db_manager: DatabaseManager,
    *,
    portfolio_id: str,
    as_of_date: date,
) -> dict[str, float]:
    """Return ticker → weight dict from the most recent positions snapshot
    on or before ``as_of_date``.  Weights are absolute(market_value) shares
    of total absolute market value.
    """
    with db_manager.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT instrument_id, market_value
                FROM positions_snapshots
                WHERE portfolio_id = %s
                  AND as_of_date = (
                      SELECT MAX(as_of_date) FROM positions_snapshots
                      WHERE portfolio_id = %s AND as_of_date <= %s
                  )
                """,
                (portfolio_id, portfolio_id, as_of_date),
            )
            rows = cur.fetchall()
        finally:
            cur.close()

    if not rows:
        return {}

    abs_total = sum(abs(float(mv or 0.0)) for _, mv in rows) or 1.0
    return {
        str(iid): abs(float(mv or 0.0)) / abs_total
        for iid, mv in rows
        if iid and mv
    }


def _to_snapshot(
    *,
    portfolio_id: str,
    as_of_date: date,
    upstream: Any,
    decision_id: str | None,
) -> PortfolioGeoRiskSnapshot:
    rt = getattr(upstream, "risk_type_breakdown", None)
    return PortfolioGeoRiskSnapshot(
        snapshot_id=generate_uuid(),
        portfolio_id=portfolio_id,
        as_of_date=as_of_date,
        overall_risk_score=float(getattr(upstream, "overall_risk_score", 0.0) or 0.0),
        conflict_risk=float(getattr(rt, "conflict", 0.0) or 0.0) if rt else 0.0,
        chokepoint_risk=float(getattr(rt, "chokepoint", 0.0) or 0.0) if rt else 0.0,
        sovereign_risk=float(getattr(rt, "sovereign", 0.0) or 0.0) if rt else 0.0,
        sector_risk=float(getattr(rt, "sector", 0.0) or 0.0) if rt else 0.0,
        ticker_count=len(getattr(upstream, "tickers", []) or []),
        exposure=upstream.to_dict() if hasattr(upstream, "to_dict") else {},
        decision_id=decision_id,
        computed_at=datetime.now(timezone.utc),
    )


def _record_decision(
    *,
    storage: MetaStorage,
    snap: PortfolioGeoRiskSnapshot,
) -> str:
    decision_id = generate_uuid()
    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="GEO_RISK",
        run_id=None,
        strategy_id=None,
        market_id="US_EQ",
        as_of_date=snap.as_of_date,
        config_id=None,
        input_refs={
            "portfolio_id": snap.portfolio_id,
            "ticker_count": snap.ticker_count,
        },
        output_refs={
            "overall_risk_score": snap.overall_risk_score,
            "risk_type_breakdown": {
                "conflict": snap.conflict_risk,
                "chokepoint": snap.chokepoint_risk,
                "sovereign": snap.sovereign_risk,
                "sector": snap.sector_risk,
            },
        },
        metadata={
            "snapshot_id": snap.snapshot_id,
            "rationale": (
                f"Portfolio {snap.portfolio_id} composite geopolitical risk "
                f"{snap.overall_risk_score:.0f}/100. Top driver: "
                f"{_top_driver(snap)}. Consider trimming exposure to the "
                "highest-conflict or highest-chokepoint names; raise tail "
                "hedge sizing if this stays elevated."
            ),
        },
    )
    storage.save_engine_decision(decision)

    logger.info(
        "[geo_risk] decision_id=%s portfolio=%s overall=%.1f",
        decision_id,
        snap.portfolio_id,
        snap.overall_risk_score,
    )
    return decision_id


def _top_driver(snap: PortfolioGeoRiskSnapshot) -> str:
    options = {
        "conflict": snap.conflict_risk,
        "chokepoint": snap.chokepoint_risk,
        "sovereign": snap.sovereign_risk,
        "sector": snap.sector_risk,
    }
    return max(options.items(), key=lambda kv: kv[1])[0]


def _analyze_exposure(tickers: list[str], weights: dict[str, float]) -> Any:
    """Indirection so tests can monkeypatch."""
    from apatheon.portfolio.exposure import analyze_exposure
    return analyze_exposure(tickers=tickers, weights=weights)


def run_geo_exposure_scan(
    *,
    portfolio_id: str,
    as_of_date: date,
    db_manager: DatabaseManager | None = None,
    holdings: dict[str, float] | None = None,
    exposure_result: Any | None = None,
) -> GeoExposureScanResult:
    """Scan ``portfolio_id`` for geopolitical risk and persist the snapshot.

    Tests pass ``holdings`` and/or ``exposure_result`` to bypass the live
    Postgres + Apatheon calls.
    """
    if db_manager is None:
        db_manager = get_db_manager()

    storage = PortfolioGeoRiskStorage(db_manager=db_manager)
    meta = MetaStorage(db_manager=db_manager)

    if holdings is None:
        holdings = _read_portfolio_holdings(
            db_manager,
            portfolio_id=portfolio_id,
            as_of_date=as_of_date,
        )

    if not holdings:
        logger.info(
            "[geo_risk] no holdings for portfolio_id=%s as_of=%s — skipping",
            portfolio_id,
            as_of_date.isoformat(),
        )
        return GeoExposureScanResult(
            portfolio_id=portfolio_id,
            as_of_date=as_of_date,
            snapshot=None,
            decision_logged=False,
        )

    if exposure_result is None:
        exposure_result = _analyze_exposure(
            tickers=list(holdings.keys()),
            weights=holdings,
        )

    existing = storage.existing_decision_id(
        portfolio_id=portfolio_id,
        as_of_date=as_of_date,
    )
    decision_id: str | None = existing

    snap = _to_snapshot(
        portfolio_id=portfolio_id,
        as_of_date=as_of_date,
        upstream=exposure_result,
        decision_id=decision_id,
    )

    decision_logged = False
    if decision_id is None and snap.overall_risk_score >= DECISION_MIN_SCORE:
        decision_id = _record_decision(storage=meta, snap=snap)
        decision_logged = True
        snap = PortfolioGeoRiskSnapshot(**{**snap.__dict__, "decision_id": decision_id})

    storage.upsert(snap)

    return GeoExposureScanResult(
        portfolio_id=portfolio_id,
        as_of_date=as_of_date,
        snapshot=snap,
        decision_logged=decision_logged,
    )
