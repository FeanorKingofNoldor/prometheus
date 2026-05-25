"""Scenario-tree probability consumer.

Apatheon's ``apatheon.intel.scenario_tree.build_scenario_tree`` produces
a branching probability tree from a trigger event.  Trees are LLM-driven
(slow, on-demand) so this consumer is **not** a daily DAG job — it's a
programmatic API that prometheus's options-strategy layer calls when
sizing Greeks across plausible futures.

Usage from option strategy code::

    from prometheus.signals.scenarios import (
        run_scenario_tree,
        get_branches_for_entity,
    )

    tree = run_scenario_tree(
        trigger_event="Iran closes Strait of Hormuz",
        trigger_entity=("CHOKEPOINT", "hormuz"),
    )
    # Use tree.branches with probabilities to weight gamma/vega.

Each branch row has a probability — pass them to the options Greek
weighter when allocating across plausible terminal scenarios.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Sequence

from apatheon.core.database import DatabaseManager, get_db_manager
from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger
from psycopg2.extras import Json

from prometheus.meta.storage import MetaStorage
from prometheus.meta.types import EngineDecision

logger = get_logger(__name__)


# Branches with probability ≥ this value cause a SCENARIO decision row to
# be logged so the Meta-Orchestrator can score realised outcomes.
DECISION_MIN_PROBABILITY = 0.30


@dataclass(frozen=True)
class PersistedBranch:
    branch_id: str
    tree_id: str
    as_of_date: date
    trigger_event: str
    trigger_entity_type: str | None
    trigger_entity_id: str | None
    depth: int
    parent_branch_id: str | None
    description: str
    probability: float
    affected_entities: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    decision_id: str | None = None
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ScenarioRunResult:
    tree_id: str
    as_of_date: date
    trigger_event: str
    branches: list[PersistedBranch]
    decisions_logged: int


@dataclass
class ScenarioStorage:
    db_manager: DatabaseManager

    def upsert_branch(self, branch: PersistedBranch) -> None:
        sql = """
            INSERT INTO scenario_branches (
                branch_id, tree_id, as_of_date,
                trigger_event, trigger_entity_type, trigger_entity_id,
                depth, parent_branch_id,
                description, probability,
                affected_entities, metadata,
                decision_id, computed_at
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
            ON CONFLICT (branch_id) DO UPDATE SET
                description       = EXCLUDED.description,
                probability       = EXCLUDED.probability,
                affected_entities = EXCLUDED.affected_entities,
                metadata          = EXCLUDED.metadata,
                decision_id       = COALESCE(scenario_branches.decision_id, EXCLUDED.decision_id),
                computed_at       = EXCLUDED.computed_at
        """
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        branch.branch_id,
                        branch.tree_id,
                        branch.as_of_date,
                        branch.trigger_event,
                        branch.trigger_entity_type,
                        branch.trigger_entity_id,
                        branch.depth,
                        branch.parent_branch_id,
                        branch.description,
                        branch.probability,
                        Json(branch.affected_entities),
                        Json(branch.metadata),
                        branch.decision_id,
                        branch.computed_at,
                    ),
                )
                conn.commit()
            finally:
                cur.close()

    def list_for_entity(
        self,
        *,
        entity_type: str,
        entity_id: str,
        min_probability: float = 0.0,
        limit: int = 50,
    ) -> list[PersistedBranch]:
        with self.db_manager.get_runtime_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT branch_id, tree_id, as_of_date,
                           trigger_event, trigger_entity_type, trigger_entity_id,
                           depth, parent_branch_id,
                           description, probability,
                           affected_entities, metadata,
                           decision_id, computed_at
                    FROM scenario_branches
                    WHERE trigger_entity_type=%s AND trigger_entity_id=%s
                      AND probability >= %s
                    ORDER BY as_of_date DESC, probability DESC
                    LIMIT %s
                    """,
                    (entity_type, entity_id, min_probability, limit),
                )
                rows = cur.fetchall()
            finally:
                cur.close()

        out: list[PersistedBranch] = []
        for r in rows:
            out.append(
                PersistedBranch(
                    branch_id=r[0],
                    tree_id=r[1],
                    as_of_date=r[2],
                    trigger_event=r[3],
                    trigger_entity_type=r[4],
                    trigger_entity_id=r[5],
                    depth=r[6],
                    parent_branch_id=r[7],
                    description=r[8],
                    probability=r[9] or 0.0,
                    affected_entities=r[10] or [],
                    metadata=r[11] or {},
                    decision_id=r[12],
                    computed_at=r[13],
                )
            )
        return out


def _build_tree(
    *,
    trigger_event: str,
    trigger_entity: tuple[str, str] | None,
    max_depth: int,
    branching_factor: int,
) -> Any:
    """Indirection so tests can monkeypatch the LLM call."""
    from apatheon.intel.scenario_tree import build_scenario_tree

    return build_scenario_tree(
        trigger_event=trigger_event,
        trigger_entity=trigger_entity,
        max_depth=max_depth,
        branching_factor=branching_factor,
    )


def _walk_branches(tree: Any) -> list[Any]:
    """Flatten the tree to a list of (branch, depth, parent_id) tuples."""
    branches: list[tuple[Any, int, str | None]] = []

    def _walk(node: Any, depth: int, parent_id: str | None) -> None:
        children = getattr(node, "children", None) or []
        for child in children:
            cid = getattr(child, "branch_id", None) or generate_uuid()
            # Stamp branch_id back onto the upstream object if it was
            # missing — we rely on it for the parent_branch_id chain.
            try:
                if getattr(child, "branch_id", None) is None:
                    object.__setattr__(child, "branch_id", cid)
            except Exception:
                pass
            branches.append((child, depth, parent_id))
            _walk(child, depth + 1, cid)

    root = getattr(tree, "root", None)
    if root is not None:
        root_id = getattr(root, "branch_id", None) or generate_uuid()
        try:
            if getattr(root, "branch_id", None) is None:
                object.__setattr__(root, "branch_id", root_id)
        except Exception:
            pass
        branches.append((root, 0, None))
        _walk(root, 1, root_id)
    return [(b, d, p) for b, d, p in branches]


def _record_decision(
    *,
    storage: MetaStorage,
    branch: PersistedBranch,
) -> str:
    decision_id = generate_uuid()
    decision = EngineDecision(
        decision_id=decision_id,
        engine_name="SCENARIO",
        run_id=None,
        strategy_id=None,
        market_id="INTEL",
        as_of_date=branch.as_of_date,
        config_id=None,
        input_refs={
            "tree_id": branch.tree_id,
            "trigger_event": branch.trigger_event[:200],
            "trigger_entity": (
                f"{branch.trigger_entity_type}:{branch.trigger_entity_id}"
                if branch.trigger_entity_type and branch.trigger_entity_id
                else None
            ),
            "depth": branch.depth,
        },
        output_refs={
            "branch_description": branch.description[:500],
            "probability": branch.probability,
            "affected_entities": branch.affected_entities[:20],
        },
        metadata={
            "branch_id": branch.branch_id,
            "rationale": (
                f"High-probability scenario branch (p={branch.probability:.2f}). "
                "Use to weight options Greek allocation across plausible futures."
            ),
        },
    )
    storage.save_engine_decision(decision)
    return decision_id


def run_scenario_tree(
    *,
    trigger_event: str,
    trigger_entity: tuple[str, str] | None = None,
    as_of_date: date | None = None,
    db_manager: DatabaseManager | None = None,
    max_depth: int = 2,
    branching_factor: int = 3,
    upstream_tree: Any | None = None,
) -> ScenarioRunResult:
    """Run a scenario tree, persist branches, and log high-probability decisions.

    ``upstream_tree`` lets tests pass a pre-computed tree to bypass the
    LLM call.
    """
    if db_manager is None:
        db_manager = get_db_manager()
    if as_of_date is None:
        as_of_date = datetime.now(timezone.utc).date()

    storage = ScenarioStorage(db_manager=db_manager)
    meta = MetaStorage(db_manager=db_manager)

    if upstream_tree is None:
        try:
            upstream_tree = _build_tree(
                trigger_event=trigger_event,
                trigger_entity=trigger_entity,
                max_depth=max_depth,
                branching_factor=branching_factor,
            )
        except Exception:
            logger.exception("[scenarios] build_scenario_tree failed")
            return ScenarioRunResult(
                tree_id="",
                as_of_date=as_of_date,
                trigger_event=trigger_event,
                branches=[],
                decisions_logged=0,
            )

    tree_id = getattr(upstream_tree, "tree_id", None) or generate_uuid()
    branches_raw = _walk_branches(upstream_tree)

    persisted: list[PersistedBranch] = []
    decisions_logged = 0

    trigger_type = trigger_entity[0] if trigger_entity else None
    trigger_id = trigger_entity[1] if trigger_entity else None

    for branch, depth, parent_id in branches_raw:
        try:
            branch_id = getattr(branch, "branch_id", None) or generate_uuid()
            description = str(getattr(branch, "description", "") or "")
            probability = float(getattr(branch, "probability", 0.0) or 0.0)

            decision_id: str | None = None
            placeholder = PersistedBranch(
                branch_id=branch_id,
                tree_id=tree_id,
                as_of_date=as_of_date,
                trigger_event=trigger_event,
                trigger_entity_type=trigger_type,
                trigger_entity_id=trigger_id,
                depth=depth,
                parent_branch_id=parent_id,
                description=description,
                probability=probability,
                affected_entities=list(getattr(branch, "affected_entities", []) or []),
                metadata={
                    "branch_type": getattr(branch, "branch_type", None),
                    "rationale": getattr(branch, "rationale", None),
                },
                decision_id=None,
            )

            if probability >= DECISION_MIN_PROBABILITY:
                decision_id = _record_decision(storage=meta, branch=placeholder)
                decisions_logged += 1
                placeholder = PersistedBranch(
                    **{**placeholder.__dict__, "decision_id": decision_id}
                )

            storage.upsert_branch(placeholder)
            persisted.append(placeholder)
        except Exception:
            logger.exception("[scenarios] failed to persist branch")

    logger.info(
        "[scenarios] tree=%s persisted=%d decisions=%d",
        tree_id,
        len(persisted),
        decisions_logged,
    )

    return ScenarioRunResult(
        tree_id=tree_id,
        as_of_date=as_of_date,
        trigger_event=trigger_event,
        branches=persisted,
        decisions_logged=decisions_logged,
    )


def get_branches_for_entity(
    *,
    entity_type: str,
    entity_id: str,
    min_probability: float = 0.1,
    limit: int = 20,
    db_manager: DatabaseManager | None = None,
) -> list[PersistedBranch]:
    """Lookup helper for the options strategy: "give me persisted scenarios
    triggered by chokepoint=hormuz with p>=0.1"."""
    if db_manager is None:
        db_manager = get_db_manager()
    storage = ScenarioStorage(db_manager=db_manager)
    return storage.list_for_entity(
        entity_type=entity_type,
        entity_id=entity_id,
        min_probability=min_probability,
        limit=limit,
    )


def weight_greeks_by_scenarios(
    branches: Sequence[PersistedBranch],
) -> dict[str, float]:
    """Convert a set of scenario branches into per-affected-entity weights.

    Used by the options strategy to allocate gamma/vega across plausible
    futures.  Returns a dict ``entity_id → probability_weight`` where
    weights sum to 1.0 (or 0.0 if no branches).
    """
    if not branches:
        return {}
    weight: dict[str, float] = {}
    total = 0.0
    for b in branches:
        for ent in b.affected_entities:
            ent_id = ent if isinstance(ent, str) else (
                ent.get("entity_id") if isinstance(ent, dict) else None
            )
            if not ent_id:
                continue
            weight[ent_id] = weight.get(ent_id, 0.0) + b.probability
            total += b.probability
    if total <= 0:
        return {}
    return {k: v / total for k, v in weight.items()}
