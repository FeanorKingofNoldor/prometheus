"""Cui Bono / beneficiary scoring consumer.

For each ACTIVE / ESCALATING conflict that Apatheon tracks, runs
``apatheon.graph.beneficiary.analyze_beneficiaries`` to rank sovereign
candidates by motive / means / opportunity / pattern-match and persists
the top-K to ``beneficiary_scores``.

When Apatheon's analysis flags **attribution asymmetry** (claimed
perpetrator scores significantly lower than the top alternative
beneficiary), Prometheus logs a ``BENEFICIARY`` engine_decision so the
Meta-Orchestrator can later score realised market behaviour against
"the official narrative is structurally inconsistent with cui bono"
predictions.
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


# Default number of top candidates persisted per victim.
TOP_K = 5


@dataclass(frozen=True)
class BeneficiaryRow:
    score_id: str
    as_of_date: date
    victim_entity_type: str
    victim_entity_id: str
    rank: int
    candidate_entity_type: str
    candidate_entity_id: str
    composite_score: float
    motive_score: float
    means_score: float
    opportunity_score: float
    pattern_match_score: float
    asymmetry_detected: bool
    decision_id: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class BeneficiaryScanResult:
    as_of_date: date
    victims_scanned: int
    rows_persisted: int
    decisions_logged: int
    asymmetric: list[tuple[str, str]]   # (victim_type, victim_id) with asymmetry


@dataclass
class BeneficiaryStorage:
    db_manager: DatabaseManager

    def upsert(self, row: BeneficiaryRow) -> None:
        sql = """
            INSERT INTO beneficiary_scores (
                score_id, as_of_date,
                victim_entity_type, victim_entity_id,
                rank,
                candidate_entity_type, candidate_entity_id,
                composite_score, motive_score, means_score,
                opportunity_score, pattern_match_score,
                asymmetry_detected, decision_id, metadata, computed_at
            ) VALUES (
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (as_of_date, victim_entity_type, victim_entity_id, rank)
            DO UPDATE SET
                candidate_entity_type = EXCLUDED.candidate_entity_type,
                candidate_entity_id   = EXCLUDED.candidate_entity_id,
                composite_score       = EXCLUDED.composite_score,
                motive_score          = EXCLUDED.motive_score,
                means_score           = EXCLUDED.means_score,
                opportunity_score     = EXCLUDED.opportunity_score,
                pattern_match_score   = EXCLUDED.pattern_match_score,
                asymmetry_detected    = EXCLUDED.asymmetry_detected,
                decision_id           = COALESCE(beneficiary_scores.decision_id, EXCLUDED.decision_id),
                metadata              = EXCLUDED.metadata,
                computed_at           = EXCLUDED.computed_at
        """
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        row.score_id,
                        row.as_of_date,
                        row.victim_entity_type,
                        row.victim_entity_id,
                        row.rank,
                        row.candidate_entity_type,
                        row.candidate_entity_id,
                        row.composite_score,
                        row.motive_score,
                        row.means_score,
                        row.opportunity_score,
                        row.pattern_match_score,
                        row.asymmetry_detected,
                        row.decision_id,
                        Json(row.metadata),
                        row.computed_at,
                    ),
                )
                conn.commit()
            finally:
                cur.close()

    def existing_decision_id(
        self,
        *,
        as_of_date: date,
        victim_entity_type: str,
        victim_entity_id: str,
    ) -> str | None:
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT decision_id FROM beneficiary_scores
                    WHERE as_of_date=%s AND victim_entity_type=%s AND victim_entity_id=%s
                      AND decision_id IS NOT NULL
                    LIMIT 1
                    """,
                    (as_of_date, victim_entity_type, victim_entity_id),
                )
                row = cur.fetchone()
            finally:
                cur.close()
        return row[0] if row else None

    def list_for_victim(
        self,
        *,
        victim_entity_type: str,
        victim_entity_id: str,
        as_of_date: date | None = None,
        top_k: int = TOP_K,
    ) -> list[BeneficiaryRow]:
        if as_of_date is None:
            with self.db_manager.get_runtime_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        """
                        SELECT MAX(as_of_date) FROM beneficiary_scores
                        WHERE victim_entity_type=%s AND victim_entity_id=%s
                        """,
                        (victim_entity_type, victim_entity_id),
                    )
                    row = cur.fetchone()
                finally:
                    cur.close()
            if not row or row[0] is None:
                return []
            as_of_date = row[0]

        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT score_id, as_of_date, victim_entity_type, victim_entity_id, rank,
                           candidate_entity_type, candidate_entity_id,
                           composite_score, motive_score, means_score,
                           opportunity_score, pattern_match_score,
                           asymmetry_detected, decision_id, metadata, computed_at
                    FROM beneficiary_scores
                    WHERE as_of_date=%s
                      AND victim_entity_type=%s
                      AND victim_entity_id=%s
                    ORDER BY rank ASC
                    LIMIT %s
                    """,
                    (as_of_date, victim_entity_type, victim_entity_id, top_k),
                )
                rows = cur.fetchall()
            finally:
                cur.close()

        out: list[BeneficiaryRow] = []
        for r in rows:
            out.append(
                BeneficiaryRow(
                    score_id=r[0],
                    as_of_date=r[1],
                    victim_entity_type=r[2],
                    victim_entity_id=r[3],
                    rank=r[4],
                    candidate_entity_type=r[5],
                    candidate_entity_id=r[6],
                    composite_score=r[7] or 0.0,
                    motive_score=r[8] or 0.0,
                    means_score=r[9] or 0.0,
                    opportunity_score=r[10] or 0.0,
                    pattern_match_score=r[11] or 0.0,
                    asymmetry_detected=bool(r[12]),
                    decision_id=r[13],
                    metadata=r[14] or {},
                    computed_at=r[15],
                )
            )
        return out


def _bootstrap_graph() -> Any:
    from apatheon.graph.bootstrap import bootstrap_graph
    return bootstrap_graph()


def _list_active_conflicts() -> list[dict[str, Any]]:
    """Return ACTIVE/ESCALATING conflicts from Apatheon."""
    try:
        from apatheon.nation.conflicts import get_all_conflicts
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for c in get_all_conflicts():
        if c.get("status") in ("ACTIVE", "ESCALATING"):
            out.append(c)
    return out


def _analyze(graph: Any, victim_id: tuple[str, str], event_desc: str) -> Any | None:
    from apatheon.graph.beneficiary import analyze_beneficiaries

    try:
        return analyze_beneficiaries(
            graph,
            victim_id,
            event_description=event_desc,
            top_k=TOP_K,
        )
    except Exception:
        logger.debug(
            "[beneficiary] analyze failed for %s:%s",
            *victim_id,
            exc_info=True,
        )
        return None


def _record_decision(
    *,
    storage: MetaStorage,
    as_of_date: date,
    analysis: Any,
    victim_type: str,
    victim_id: str,
) -> str:
    decision_id = generate_uuid()

    top = analysis.candidates[0] if getattr(analysis, "candidates", None) else None
    claimed = getattr(analysis, "claimed_perpetrator", None)
    asymmetry = bool(getattr(analysis, "asymmetry_detected", False))

    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="BENEFICIARY",
        run_id=None,
        strategy_id=None,
        market_id="INTEL",
        as_of_date=as_of_date,
        config_id=None,
        input_refs={
            "victim_entity_type": victim_type,
            "victim_entity_id": victim_id,
            "claimed_perpetrator": list(claimed) if isinstance(claimed, tuple) else claimed,
        },
        output_refs={
            "asymmetry_detected": asymmetry,
            "top_candidate": (
                {
                    "entity_type": top.entity_type,
                    "entity_id": top.entity_id,
                    "composite_score": top.composite_score,
                }
                if top is not None
                else None
            ),
        },
        metadata={
            "rationale": (
                "Attribution asymmetry detected: claimed perpetrator scores "
                "structurally lower than the top beneficiary. Treat the "
                "official narrative as a tradeable mispricing — see linked "
                "divergence signals for the same entity."
                if asymmetry
                else "Top beneficiaries logged for outcome tracking."
            ),
        },
    )
    storage.save_engine_decision(decision)

    logger.info(
        "[beneficiary] decision_id=%s victim=%s:%s asymmetry=%s top=%s",
        decision_id,
        victim_type,
        victim_id,
        asymmetry,
        getattr(top, "entity_id", "—"),
    )
    return decision_id


def run_beneficiary_scan(
    *,
    as_of_date: date,
    db_manager: DatabaseManager | None = None,
    conflicts: Sequence[dict[str, Any]] | None = None,
    analyses: Sequence[tuple[tuple[str, str], Any]] | None = None,
) -> BeneficiaryScanResult:
    """Run Cui Bono on each active conflict and persist top-K scores.

    Args:
        analyses: Optional pre-computed analyses for testing —
            sequence of ``((victim_type, victim_id), BeneficiaryAnalysis)``.
    """
    if db_manager is None:
        db_manager = get_db_manager()

    storage = BeneficiaryStorage(db_manager=db_manager)
    meta = MetaStorage(db_manager=db_manager)

    # Build the iterable of (victim_id, analysis) pairs.
    if analyses is not None:
        analysis_iter: Iterable[tuple[tuple[str, str], Any]] = analyses
    else:
        try:
            graph = _bootstrap_graph()
        except Exception:
            logger.exception("[beneficiary] graph bootstrap failed; skipping scan")
            return BeneficiaryScanResult(
                as_of_date=as_of_date,
                victims_scanned=0,
                rows_persisted=0,
                decisions_logged=0,
                asymmetric=[],
            )

        active_conflicts = list(conflicts) if conflicts is not None else _list_active_conflicts()
        analysis_iter = _build_analysis_iter(graph, active_conflicts)

    rows_persisted = 0
    decisions_logged = 0
    victims_scanned = 0
    asymmetric: list[tuple[str, str]] = []

    for victim_id, analysis in analysis_iter:
        victims_scanned += 1
        if analysis is None:
            continue
        try:
            v_type, v_id = victim_id
            existing = storage.existing_decision_id(
                as_of_date=as_of_date,
                victim_entity_type=v_type,
                victim_entity_id=v_id,
            )
            decision_id: str | None = existing

            if decision_id is None:
                decision_id = _record_decision(
                    storage=meta,
                    as_of_date=as_of_date,
                    analysis=analysis,
                    victim_type=v_type,
                    victim_id=v_id,
                )
                decisions_logged += 1

            candidates = list(getattr(analysis, "candidates", []) or [])
            if not candidates:
                continue
            asymmetry = bool(getattr(analysis, "asymmetry_detected", False))
            if asymmetry:
                asymmetric.append((v_type, v_id))

            for rank, cand in enumerate(candidates[:TOP_K], start=1):
                cand_id = getattr(cand, "entity_id", None)
                if isinstance(cand_id, tuple) and len(cand_id) == 2:
                    cand_type, cand_key = str(cand_id[0]), str(cand_id[1])
                else:
                    cand_type = str(getattr(cand, "entity_type", "SOVEREIGN"))
                    cand_key = str(cand_id) if cand_id is not None else ""

                row = BeneficiaryRow(
                    score_id=generate_uuid(),
                    as_of_date=as_of_date,
                    victim_entity_type=v_type,
                    victim_entity_id=v_id,
                    rank=rank,
                    candidate_entity_type=cand_type,
                    candidate_entity_id=cand_key,
                    composite_score=float(cand.composite_score),
                    motive_score=float(cand.motive_score),
                    means_score=float(cand.means_score),
                    opportunity_score=float(cand.opportunity_score),
                    pattern_match_score=float(cand.pattern_match_score),
                    asymmetry_detected=asymmetry,
                    decision_id=decision_id,
                    metadata={
                        "candidate_name": getattr(cand, "entity_name", cand_key),
                        "direct_benefits": list(getattr(cand, "direct_benefits", []) or [])[:5],
                        "indirect_benefits": list(getattr(cand, "indirect_benefits", []) or [])[:5],
                    },
                )
                storage.upsert(row)
                rows_persisted += 1
        except Exception:
            logger.exception(
                "[beneficiary] failed to persist scores for %s",
                victim_id,
            )

    logger.info(
        "[beneficiary] scan complete date=%s victims=%d persisted=%d decisions=%d asym=%d",
        as_of_date.isoformat(),
        victims_scanned,
        rows_persisted,
        decisions_logged,
        len(asymmetric),
    )

    return BeneficiaryScanResult(
        as_of_date=as_of_date,
        victims_scanned=victims_scanned,
        rows_persisted=rows_persisted,
        decisions_logged=decisions_logged,
        asymmetric=asymmetric,
    )


def _build_analysis_iter(
    graph: Any,
    conflicts: Iterable[dict[str, Any]],
) -> Iterable[tuple[tuple[str, str], Any]]:
    """Yield (victim_id_tuple, analysis) for each active conflict."""
    for c in conflicts:
        try:
            cid = str(c["id"])
        except KeyError:
            continue
        # Treat the conflict itself as the "victim" entity since the graph
        # has CONFLICT-typed nodes and analyze_beneficiaries can score
        # sovereigns against any entity type.
        victim = ("CONFLICT", cid)
        event_desc = str(c.get("description") or c.get("name") or cid)
        analysis = _analyze(graph, victim, event_desc)
        yield victim, analysis
