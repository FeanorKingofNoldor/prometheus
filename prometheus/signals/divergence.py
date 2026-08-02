"""Narrative-vs-Reality divergence signal consumer.

Pulls divergence scans from Apatheon (``apatheon.intel.signal_classifier``),
persists them to ``divergence_signals``, and logs SIGNIFICANT/EXTREME
results to ``engine_decisions`` so they're tracked by the Meta-Orchestrator.

The Apatheon side computes a per-entity (chokepoint or conflict) gap
between physical reality (transits, deployments, freight) and narrative
belief (news volume, headline sentiment).  When they diverge enough, the
classifier emits a trading signal:

  * ``FADE_NARRATIVE`` — narrative > reality.  Short the news-driven
    instrument; reality should reprice it down.
  * ``FRONT_RUN_REALITY`` — reality > narrative.  Long the
    reality-exposed instrument before media catches up.

Public entrypoint: :func:`run_divergence_scan` is the function the
intel DAG calls.  It is idempotent on (as_of_date, entity_type,
entity_id) — re-running the same day overwrites the row but does not
duplicate ``engine_decisions`` entries (we only log a decision when
signal severity is SIGNIFICANT or EXTREME and no decision exists yet
for that key).
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


# ─────────────────────────────────────────────────────────────────────
# Types
# ─────────────────────────────────────────────────────────────────────


# Severities at or above this threshold cause an engine_decisions row to
# be written.  Anything below is persisted to divergence_signals (so the
# UI can show the full landscape) but does not become a tracked decision.
DECISION_MIN_SEVERITY = "SIGNIFICANT"

_SEVERITY_RANK = {
    "NONE": 0,
    "MILD": 1,
    "SIGNIFICANT": 2,
    "EXTREME": 3,
}


@dataclass(frozen=True)
class DivergenceSignal:
    """One persisted divergence-scan result."""

    signal_id: str
    as_of_date: date
    entity_type: str            # "chokepoint" | "conflict"
    entity_id: str
    behavioral_score: float
    narrative_score: float
    divergence: float
    abs_divergence: float
    direction: str              # ALIGNED | NARRATIVE_OVERSTATES | REALITY_UNDERSTATED
    severity: str               # NONE | MILD | SIGNIFICANT | EXTREME
    trading_signal: str         # NONE | FADE_NARRATIVE | FRONT_RUN_REALITY
    decision_id: str | None
    computed_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScanResult:
    """Outcome of one full scan run."""

    as_of_date: date
    chokepoints_scanned: int
    conflicts_scanned: int
    rows_persisted: int
    decisions_logged: int
    significant: list[DivergenceSignal]
    extreme: list[DivergenceSignal]


# ─────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────


@dataclass
class DivergenceStorage:
    """Postgres persistence for ``divergence_signals``.

    Uses upsert semantics on (as_of_date, entity_type, entity_id) so a
    re-run on the same day refreshes rather than appends.
    """

    db_manager: DatabaseManager

    def upsert(self, signal: DivergenceSignal) -> None:
        sql = """
            INSERT INTO divergence_signals (
                signal_id, as_of_date, entity_type, entity_id,
                behavioral_score, narrative_score,
                divergence, abs_divergence,
                direction, severity, trading_signal,
                decision_id, computed_at, metadata
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (as_of_date, entity_type, entity_id)
            DO UPDATE SET
                behavioral_score = EXCLUDED.behavioral_score,
                narrative_score  = EXCLUDED.narrative_score,
                divergence       = EXCLUDED.divergence,
                abs_divergence   = EXCLUDED.abs_divergence,
                direction        = EXCLUDED.direction,
                severity         = EXCLUDED.severity,
                trading_signal   = EXCLUDED.trading_signal,
                decision_id      = COALESCE(divergence_signals.decision_id, EXCLUDED.decision_id),
                computed_at      = EXCLUDED.computed_at,
                metadata         = EXCLUDED.metadata
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
                        signal.behavioral_score,
                        signal.narrative_score,
                        signal.divergence,
                        signal.abs_divergence,
                        signal.direction,
                        signal.severity,
                        signal.trading_signal,
                        signal.decision_id,
                        signal.computed_at,
                        Json(signal.metadata or {}),
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
        """Return the previously-recorded decision_id for this key, if any."""
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT decision_id FROM divergence_signals
                    WHERE as_of_date = %s
                      AND entity_type = %s
                      AND entity_id = %s
                    """,
                    (as_of_date, entity_type, entity_id),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        if row is None:
            return None
        return row[0]

    def list_recent(
        self,
        *,
        as_of_date: date,
        min_severity: str = "SIGNIFICANT",
        limit: int = 50,
    ) -> list[DivergenceSignal]:
        """Return today's signals at or above the given severity."""
        min_rank = _SEVERITY_RANK.get(min_severity, 2)
        eligible = [s for s, r in _SEVERITY_RANK.items() if r >= min_rank]
        if not eligible:
            return []

        placeholders = ", ".join(["%s"] * len(eligible))
        sql = f"""
            SELECT signal_id, as_of_date, entity_type, entity_id,
                   behavioral_score, narrative_score,
                   divergence, abs_divergence,
                   direction, severity, trading_signal,
                   decision_id, computed_at, metadata
            FROM divergence_signals
            WHERE as_of_date = %s
              AND severity IN ({placeholders})
            ORDER BY abs_divergence DESC
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

        out: list[DivergenceSignal] = []
        for row in rows:
            (
                signal_id, as_of, etype, eid,
                beh, nar, div, abs_div,
                direction, severity, trading_signal,
                decision_id, computed_at, metadata,
            ) = row
            out.append(
                DivergenceSignal(
                    signal_id=signal_id,
                    as_of_date=as_of,
                    entity_type=etype,
                    entity_id=eid,
                    behavioral_score=beh,
                    narrative_score=nar,
                    divergence=div,
                    abs_divergence=abs_div,
                    direction=direction,
                    severity=severity,
                    trading_signal=trading_signal,
                    decision_id=decision_id,
                    computed_at=computed_at,
                    metadata=metadata or {},
                )
            )
        return out


# ─────────────────────────────────────────────────────────────────────
# Scanner
# ─────────────────────────────────────────────────────────────────────


def _meets_decision_threshold(severity: str) -> bool:
    return _SEVERITY_RANK.get(severity, 0) >= _SEVERITY_RANK[DECISION_MIN_SEVERITY]


def _record_decision(
    *,
    storage: MetaStorage,
    signal: DivergenceSignal,
) -> str:
    """Log a SIGNIFICANT/EXTREME divergence as an engine_decisions row.

    The decision encodes the trade hypothesis (FADE_NARRATIVE or
    FRONT_RUN_REALITY) so the Meta-Orchestrator can later evaluate
    realised outcomes — did the narrative actually converge to reality
    over the next 5/21/63 days?
    """
    decision_id = generate_uuid()

    input_refs: dict[str, Any] = {
        "entity_type": signal.entity_type,
        "entity_id": signal.entity_id,
        "behavioral_score": signal.behavioral_score,
        "narrative_score": signal.narrative_score,
    }

    output_refs: dict[str, Any] = {
        "trading_signal": signal.trading_signal,
        "direction": signal.direction,
        "severity": signal.severity,
        "divergence": signal.divergence,
        "abs_divergence": signal.abs_divergence,
    }

    metadata: dict[str, Any] = {
        "signal_id": signal.signal_id,
        "rationale": _rationale(signal),
    }

    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="DIVERGENCE",
        run_id=None,
        strategy_id=None,
        market_id="INTEL",
        as_of_date=signal.as_of_date,
        config_id=None,
        input_refs=input_refs,
        output_refs=output_refs,
        metadata=metadata,
    )
    storage.save_engine_decision(decision)

    logger.info(
        "[divergence] decision_id=%s entity=%s:%s severity=%s signal=%s div=%+.3f",
        decision_id,
        signal.entity_type,
        signal.entity_id,
        signal.severity,
        signal.trading_signal,
        signal.divergence,
    )
    return decision_id


def _rationale(signal: DivergenceSignal) -> str:
    if signal.trading_signal == "FADE_NARRATIVE":
        return (
            f"News overstates reality at {signal.entity_type} {signal.entity_id} "
            f"(narrative={signal.narrative_score:.2f} vs behavioral={signal.behavioral_score:.2f}); "
            f"expect convergence DOWN — short narrative-driven exposure."
        )
    if signal.trading_signal == "FRONT_RUN_REALITY":
        return (
            f"Reality leads narrative at {signal.entity_type} {signal.entity_id} "
            f"(behavioral={signal.behavioral_score:.2f} vs narrative={signal.narrative_score:.2f}); "
            f"expect convergence UP — front-run reality-exposed names."
        )
    return f"Aligned — no trade signal (severity={signal.severity})."


def _scan_apatheon_chokepoints() -> Iterable[Any]:
    """Indirection so tests can monkeypatch the upstream call."""
    from apatheon.intel.signal_classifier import scan_chokepoint_divergences
    return scan_chokepoint_divergences()


def _scan_apatheon_conflicts() -> Iterable[Any]:
    from apatheon.intel.signal_classifier import scan_conflict_divergences
    return scan_conflict_divergences()


def _to_signal(
    upstream: Any,
    *,
    as_of_date: date,
    decision_id: str | None,
) -> DivergenceSignal:
    """Convert apatheon DivergenceResult → our persistence shape."""
    computed_at = _parse_iso(getattr(upstream, "computed_at", "")) or datetime.now(
        timezone.utc
    )
    metadata: dict[str, Any] = {}
    behavioral = getattr(upstream, "behavioral", None)
    narrative = getattr(upstream, "narrative", None)
    if behavioral is not None and hasattr(behavioral, "components"):
        metadata["behavioral_components"] = getattr(behavioral, "components", None)
    if narrative is not None and hasattr(narrative, "components"):
        metadata["narrative_components"] = getattr(narrative, "components", None)
    # Three-channel fields (apatheon 2026-07-24): persisted in metadata so
    # the daily history captures the paper-market view without a schema
    # migration. Absent on pre-upgrade rows.
    if getattr(upstream, "market_score", None) is not None:
        metadata["market_score"] = float(upstream.market_score)
        metadata["paper_physical_gap"] = float(getattr(upstream, "paper_physical_gap", 0.0) or 0.0)
    if getattr(upstream, "regime", None):
        metadata["regime"] = str(upstream.regime)
    if getattr(upstream, "positioning_anomaly", None) is not None:
        metadata["positioning_anomaly"] = float(upstream.positioning_anomaly)
    metadata["data_confidence"] = str(getattr(upstream, "data_confidence", "OK"))
    if behavioral is not None:
        metadata["behavioral_coverage"] = str(getattr(behavioral, "coverage", "OK"))

    return DivergenceSignal(
        signal_id=generate_uuid(),
        as_of_date=as_of_date,
        entity_type=upstream.entity_type,
        entity_id=upstream.entity_id,
        behavioral_score=float(upstream.behavioral_score),
        narrative_score=float(upstream.narrative_score),
        divergence=float(upstream.divergence),
        abs_divergence=float(upstream.abs_divergence),
        direction=str(upstream.direction),
        severity=str(upstream.severity),
        trading_signal=str(upstream.trading_signal),
        decision_id=decision_id,
        computed_at=computed_at,
        metadata=metadata,
    )


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        # datetime.fromisoformat handles "+00:00" but not the trailing "Z"
        # that some upstream callers may use.
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def run_divergence_scan(
    *,
    as_of_date: date,
    db_manager: DatabaseManager | None = None,
    chokepoint_results: Sequence[Any] | None = None,
    conflict_results: Sequence[Any] | None = None,
) -> ScanResult:
    """Execute one divergence scan + persist + log decisions.

    Args:
        as_of_date: Date the scan is logically pinned to.
        db_manager: Optional DB manager (default: package singleton).
        chokepoint_results / conflict_results: Optional pre-computed
            results, used by tests to bypass the live Apatheon calls.

    Returns:
        :class:`ScanResult` summary suitable for daemon logging and the
        monitoring API.
    """
    if db_manager is None:
        db_manager = get_db_manager()

    storage = DivergenceStorage(db_manager=db_manager)
    meta = MetaStorage(db_manager=db_manager)

    chokepoint_results = (
        list(chokepoint_results)
        if chokepoint_results is not None
        else list(_scan_apatheon_chokepoints())
    )
    conflict_results = (
        list(conflict_results)
        if conflict_results is not None
        else list(_scan_apatheon_conflicts())
    )

    significant: list[DivergenceSignal] = []
    extreme: list[DivergenceSignal] = []
    rows_persisted = 0
    decisions_logged = 0

    def _process(upstream_iter: Iterable[Any], entity_type_label: str) -> None:
        nonlocal rows_persisted, decisions_logged
        for upstream in upstream_iter:
            try:
                # Reuse decision_id if we already logged one for this
                # entity earlier today (idempotent re-runs).
                existing = storage.existing_decision_id(
                    as_of_date=as_of_date,
                    entity_type=upstream.entity_type,
                    entity_id=upstream.entity_id,
                )
                decision_id: str | None = existing

                signal = _to_signal(upstream, as_of_date=as_of_date, decision_id=decision_id)

                if (
                    decision_id is None
                    and _meets_decision_threshold(signal.severity)
                ):
                    decision_id = _record_decision(storage=meta, signal=signal)
                    decisions_logged += 1
                    # Replace the placeholder so the persisted row points
                    # at the freshly-written engine_decisions entry.
                    signal = DivergenceSignal(
                        signal_id=signal.signal_id,
                        as_of_date=signal.as_of_date,
                        entity_type=signal.entity_type,
                        entity_id=signal.entity_id,
                        behavioral_score=signal.behavioral_score,
                        narrative_score=signal.narrative_score,
                        divergence=signal.divergence,
                        abs_divergence=signal.abs_divergence,
                        direction=signal.direction,
                        severity=signal.severity,
                        trading_signal=signal.trading_signal,
                        decision_id=decision_id,
                        computed_at=signal.computed_at,
                        metadata=signal.metadata,
                    )

                storage.upsert(signal)
                rows_persisted += 1

                if signal.severity == "EXTREME":
                    extreme.append(signal)
                elif signal.severity == "SIGNIFICANT":
                    significant.append(signal)
            except Exception:
                logger.exception(
                    "[divergence] failed to process %s %s",
                    entity_type_label,
                    getattr(upstream, "entity_id", "?"),
                )

    _process(chokepoint_results, "chokepoint")
    _process(conflict_results, "conflict")

    logger.info(
        "[divergence] scan complete date=%s chokepoints=%d conflicts=%d "
        "persisted=%d decisions=%d significant=%d extreme=%d",
        as_of_date.isoformat(),
        len(chokepoint_results),
        len(conflict_results),
        rows_persisted,
        decisions_logged,
        len(significant),
        len(extreme),
    )

    return ScanResult(
        as_of_date=as_of_date,
        chokepoints_scanned=len(chokepoint_results),
        conflicts_scanned=len(conflict_results),
        rows_persisted=rows_persisted,
        decisions_logged=decisions_logged,
        significant=significant,
        extreme=extreme,
    )
