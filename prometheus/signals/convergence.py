"""Convergence-timing signal consumer.

Pulls per-entity convergence timelines from
``apatheon.intel.convergence_timing.scan_convergence_timelines`` and
persists them to ``convergence_signals``, with one engine_decisions row
per entity that has a confident timeline (``confidence >= 0.5``).

A convergence timeline answers *when* narrative will be forced to
reprice — so it pairs with the divergence signal (which says *which way*
narrative will move).  When both signals exist for the same entity on
the same day, the convergence row reuses the divergence ``decision_id``
so the Meta-Orchestrator can score them as a joint hypothesis.
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


# Confidence at or above this threshold causes a decision to be logged.
DECISION_MIN_CONFIDENCE = 0.5


@dataclass(frozen=True)
class ConvergenceSignal:
    signal_id: str
    as_of_date: date
    entity_type: str
    entity_id: str
    days_to_hard_deadline: float | None
    hard_deadline_reason: str | None
    days_to_soft_signal: float | None
    soft_signal_type: str | None
    infrastructure_lag_days: float
    buffer_days: float
    buffer_source: str | None
    estimated_convergence_days: float | None
    convergence_window_min: float | None
    convergence_window_max: float | None
    confidence: float
    strategy: str | None
    entry_windows: list[dict[str, Any]] = field(default_factory=list)
    decision_id: str | None = None
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ConvergenceScanResult:
    as_of_date: date
    rows_persisted: int
    decisions_logged: int
    confident: list[ConvergenceSignal]


@dataclass
class ConvergenceStorage:
    db_manager: DatabaseManager

    def upsert(self, signal: ConvergenceSignal) -> None:
        sql = """
            INSERT INTO convergence_signals (
                signal_id, as_of_date, entity_type, entity_id,
                days_to_hard_deadline, hard_deadline_reason,
                days_to_soft_signal, soft_signal_type,
                infrastructure_lag_days, buffer_days, buffer_source,
                estimated_convergence_days,
                convergence_window_min, convergence_window_max,
                confidence, strategy, entry_windows,
                decision_id, computed_at
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (as_of_date, entity_type, entity_id)
            DO UPDATE SET
                days_to_hard_deadline      = EXCLUDED.days_to_hard_deadline,
                hard_deadline_reason       = EXCLUDED.hard_deadline_reason,
                days_to_soft_signal        = EXCLUDED.days_to_soft_signal,
                soft_signal_type           = EXCLUDED.soft_signal_type,
                infrastructure_lag_days    = EXCLUDED.infrastructure_lag_days,
                buffer_days                = EXCLUDED.buffer_days,
                buffer_source              = EXCLUDED.buffer_source,
                estimated_convergence_days = EXCLUDED.estimated_convergence_days,
                convergence_window_min     = EXCLUDED.convergence_window_min,
                convergence_window_max     = EXCLUDED.convergence_window_max,
                confidence                 = EXCLUDED.confidence,
                strategy                   = EXCLUDED.strategy,
                entry_windows              = EXCLUDED.entry_windows,
                decision_id = COALESCE(convergence_signals.decision_id, EXCLUDED.decision_id),
                computed_at = EXCLUDED.computed_at
        """
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        signal.signal_id,
                        signal.as_of_date,
                        signal.entity_type,
                        signal.entity_id,
                        signal.days_to_hard_deadline,
                        signal.hard_deadline_reason,
                        signal.days_to_soft_signal,
                        signal.soft_signal_type,
                        signal.infrastructure_lag_days,
                        signal.buffer_days,
                        signal.buffer_source,
                        signal.estimated_convergence_days,
                        signal.convergence_window_min,
                        signal.convergence_window_max,
                        signal.confidence,
                        signal.strategy,
                        Json(signal.entry_windows),
                        signal.decision_id,
                        signal.computed_at,
                    ),
                )
                conn.commit()
            finally:
                cur.close()

    def existing_decision_id(
        self,
        *,
        as_of_date: date,
        entity_type: str,
        entity_id: str,
    ) -> str | None:
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT decision_id FROM convergence_signals
                    WHERE as_of_date=%s AND entity_type=%s AND entity_id=%s
                    """,
                    (as_of_date, entity_type, entity_id),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        return row[0] if row else None

    def divergence_decision_id(
        self,
        *,
        as_of_date: date,
        entity_type: str,
        entity_id: str,
    ) -> str | None:
        """Look up the divergence decision_id for the same entity/day so
        we can link convergence + divergence as a joint hypothesis."""
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT decision_id FROM divergence_signals
                    WHERE as_of_date=%s AND entity_type=%s AND entity_id=%s
                    """,
                    (as_of_date, entity_type, entity_id),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        return row[0] if row and row[0] else None

    def list_recent(
        self,
        *,
        as_of_date: date,
        min_confidence: float = 0.0,
        limit: int = 50,
    ) -> list[ConvergenceSignal]:
        sql = """
            SELECT signal_id, as_of_date, entity_type, entity_id,
                   days_to_hard_deadline, hard_deadline_reason,
                   days_to_soft_signal, soft_signal_type,
                   infrastructure_lag_days, buffer_days, buffer_source,
                   estimated_convergence_days,
                   convergence_window_min, convergence_window_max,
                   confidence, strategy, entry_windows,
                   decision_id, computed_at
            FROM convergence_signals
            WHERE as_of_date = %s AND confidence >= %s
            ORDER BY estimated_convergence_days NULLS LAST, confidence DESC
            LIMIT %s
        """
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (as_of_date, min_confidence, limit))
                rows = cur.fetchall()
            finally:
                cur.close()

        out: list[ConvergenceSignal] = []
        for row in rows:
            out.append(
                ConvergenceSignal(
                    signal_id=row[0],
                    as_of_date=row[1],
                    entity_type=row[2],
                    entity_id=row[3],
                    days_to_hard_deadline=row[4],
                    hard_deadline_reason=row[5],
                    days_to_soft_signal=row[6],
                    soft_signal_type=row[7],
                    infrastructure_lag_days=row[8] or 0.0,
                    buffer_days=row[9] or 0.0,
                    buffer_source=row[10],
                    estimated_convergence_days=row[11],
                    convergence_window_min=row[12],
                    convergence_window_max=row[13],
                    confidence=row[14] or 0.0,
                    strategy=row[15],
                    entry_windows=row[16] or [],
                    decision_id=row[17],
                    computed_at=row[18],
                )
            )
        return out


def _scan_apatheon() -> Iterable[Any]:
    from apatheon.intel.convergence_timing import scan_convergence_timelines
    return scan_convergence_timelines()


def _to_signal(upstream: Any, *, as_of_date: date, decision_id: str | None) -> ConvergenceSignal:
    window = getattr(upstream, "convergence_window", (None, None)) or (None, None)
    if isinstance(window, (tuple, list)) and len(window) == 2:
        win_min, win_max = window
    else:
        win_min, win_max = None, None

    return ConvergenceSignal(
        signal_id=generate_uuid(),
        as_of_date=as_of_date,
        entity_type=str(upstream.entity_type),
        entity_id=str(upstream.entity_id),
        days_to_hard_deadline=upstream.days_to_hard_deadline,
        hard_deadline_reason=getattr(upstream, "hard_deadline_reason", None) or None,
        days_to_soft_signal=upstream.days_to_soft_signal,
        soft_signal_type=getattr(upstream, "soft_signal_type", None) or None,
        infrastructure_lag_days=float(getattr(upstream, "infrastructure_lag_days", 0.0) or 0.0),
        buffer_days=float(getattr(upstream, "buffer_days", 0.0) or 0.0),
        buffer_source=getattr(upstream, "buffer_source", None) or None,
        estimated_convergence_days=upstream.estimated_convergence_days,
        convergence_window_min=float(win_min) if win_min is not None else None,
        convergence_window_max=float(win_max) if win_max is not None else None,
        confidence=float(getattr(upstream, "confidence", 0.0) or 0.0),
        strategy=getattr(upstream, "strategy", None) or None,
        entry_windows=list(getattr(upstream, "entry_windows", []) or []),
        decision_id=decision_id,
        computed_at=datetime.now(timezone.utc),
    )


def _record_decision(*, storage: MetaStorage, signal: ConvergenceSignal) -> str:
    decision_id = generate_uuid()

    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="CONVERGENCE",
        run_id=None,
        strategy_id=None,
        market_id="INTEL",
        as_of_date=signal.as_of_date,
        config_id=None,
        input_refs={
            "entity_type": signal.entity_type,
            "entity_id": signal.entity_id,
            "confidence": signal.confidence,
        },
        output_refs={
            "estimated_convergence_days": signal.estimated_convergence_days,
            "convergence_window": [signal.convergence_window_min, signal.convergence_window_max],
            "strategy": signal.strategy,
            "entry_windows": signal.entry_windows,
        },
        metadata={
            "signal_id": signal.signal_id,
            "hard_deadline": {
                "days": signal.days_to_hard_deadline,
                "reason": signal.hard_deadline_reason,
            },
            "soft_signal": {
                "days": signal.days_to_soft_signal,
                "type": signal.soft_signal_type,
            },
            "infrastructure_lag_days": signal.infrastructure_lag_days,
            "buffer": {"days": signal.buffer_days, "source": signal.buffer_source},
        },
    )
    storage.save_engine_decision(decision)

    logger.info(
        "[convergence] decision_id=%s entity=%s:%s est=%.0fd conf=%.2f",
        decision_id,
        signal.entity_type,
        signal.entity_id,
        signal.estimated_convergence_days or -1,
        signal.confidence,
    )
    return decision_id


def run_convergence_scan(
    *,
    as_of_date: date,
    db_manager: DatabaseManager | None = None,
    upstream: Sequence[Any] | None = None,
) -> ConvergenceScanResult:
    """Persist convergence timelines + log decisions for confident ones."""
    if db_manager is None:
        db_manager = get_db_manager()

    storage = ConvergenceStorage(db_manager=db_manager)
    meta = MetaStorage(db_manager=db_manager)

    upstream_list = list(upstream) if upstream is not None else list(_scan_apatheon())

    confident: list[ConvergenceSignal] = []
    rows_persisted = 0
    decisions_logged = 0

    for u in upstream_list:
        try:
            existing = storage.existing_decision_id(
                as_of_date=as_of_date,
                entity_type=u.entity_type,
                entity_id=u.entity_id,
            )
            decision_id: str | None = existing

            # If we already have a divergence decision for this entity/day,
            # link to it so the Meta-Orchestrator scores them jointly.
            if decision_id is None:
                divergence_id = storage.divergence_decision_id(
                    as_of_date=as_of_date,
                    entity_type=u.entity_type,
                    entity_id=u.entity_id,
                )
                if divergence_id is not None:
                    decision_id = divergence_id

            signal = _to_signal(u, as_of_date=as_of_date, decision_id=decision_id)

            if (
                decision_id is None
                and signal.confidence >= DECISION_MIN_CONFIDENCE
            ):
                decision_id = _record_decision(storage=meta, signal=signal)
                decisions_logged += 1
                signal = ConvergenceSignal(
                    **{**signal.__dict__, "decision_id": decision_id}
                )

            storage.upsert(signal)
            rows_persisted += 1

            if signal.confidence >= DECISION_MIN_CONFIDENCE:
                confident.append(signal)
        except Exception:
            logger.exception(
                "[convergence] failed to process %s",
                getattr(u, "entity_id", "?"),
            )

    logger.info(
        "[convergence] scan complete date=%s persisted=%d decisions=%d confident=%d",
        as_of_date.isoformat(),
        rows_persisted,
        decisions_logged,
        len(confident),
    )

    return ConvergenceScanResult(
        as_of_date=as_of_date,
        rows_persisted=rows_persisted,
        decisions_logged=decisions_logged,
        confident=confident,
    )
