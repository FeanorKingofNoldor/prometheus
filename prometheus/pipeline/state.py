"""Prometheus v2 – Engine run state machine.

This module defines the lightweight state machine used to orchestrate
per-date, per-region engine runs. It tracks the current phase of a run
in the ``engine_runs`` table and provides helpers to create, load, and
advance runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Optional

from apathis.core.database import DatabaseManager
from apathis.core.ids import generate_uuid
from apathis.core.logging import get_logger
from psycopg2.extras import Json

logger = get_logger(__name__)


class RunPhase(str, Enum):
    """Discrete phases for an engine run.

    The allowed transitions are linear for now::

        WAITING_FOR_DATA -> DATA_READY -> SIGNALS_DONE
        -> UNIVERSES_DONE -> BOOKS_DONE -> COMPLETED

    Any phase may transition to FAILED on unrecoverable errors.
    """

    WAITING_FOR_DATA = "WAITING_FOR_DATA"
    DATA_READY = "DATA_READY"
    SIGNALS_DONE = "SIGNALS_DONE"
    UNIVERSES_DONE = "UNIVERSES_DONE"
    BOOKS_DONE = "BOOKS_DONE"
    EXECUTION_DONE = "EXECUTION_DONE"
    OPTIONS_DONE = "OPTIONS_DONE"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    def __lt__(self, other: "RunPhase") -> bool:
        """Compare phase ordering for pipeline progression."""
        if not isinstance(other, RunPhase):
            return NotImplemented
        order = [
            RunPhase.WAITING_FOR_DATA,
            RunPhase.DATA_READY,
            RunPhase.SIGNALS_DONE,
            RunPhase.UNIVERSES_DONE,
            RunPhase.BOOKS_DONE,
            RunPhase.EXECUTION_DONE,
            RunPhase.OPTIONS_DONE,
            RunPhase.COMPLETED,
            RunPhase.FAILED,
        ]
        try:
            return order.index(self) < order.index(other)
        except ValueError:
            return NotImplemented

    def __le__(self, other: "RunPhase") -> bool:
        """Compare phase ordering (less than or equal)."""
        if not isinstance(other, RunPhase):
            return NotImplemented
        return self == other or self < other

    def __gt__(self, other: "RunPhase") -> bool:
        """Compare phase ordering (greater than)."""
        if not isinstance(other, RunPhase):
            return NotImplemented
        return not self <= other

    def __ge__(self, other: "RunPhase") -> bool:
        """Compare phase ordering (greater than or equal)."""
        if not isinstance(other, RunPhase):
            return NotImplemented
        return self == other or self > other


@dataclass(frozen=True)
class EngineRun:
    """Snapshot of an engine run row from the database."""

    run_id: str
    as_of_date: date
    region: str
    phase: RunPhase
    error: Optional[dict]
    created_at: datetime
    updated_at: datetime
    phase_started_at: Optional[datetime]
    phase_completed_at: Optional[datetime]


class EngineRunStateError(Exception):
    """Raised when an invalid state transition is attempted."""


def _row_to_engine_run(row: tuple) -> EngineRun:
    (
        run_id,
        as_of_date,
        region,
        phase,
        error,
        created_at,
        updated_at,
        phase_started_at,
        phase_completed_at,
    ) = row

    return EngineRun(
        run_id=run_id,
        as_of_date=as_of_date,
        region=region,
        phase=RunPhase(phase),
        error=error,
        created_at=created_at,
        updated_at=updated_at,
        phase_started_at=phase_started_at,
        phase_completed_at=phase_completed_at,
    )


def load_latest_run(
    db_manager: DatabaseManager,
    market_id: str = "US_EQ",
    as_of_date: date | None = None,
) -> EngineRun | None:
    """Load the most recent EngineRun for a market/date, or None."""
    sql = """
        SELECT run_id, as_of_date, region, phase, error,
               created_at, updated_at, phase_started_at, phase_completed_at
        FROM engine_runs
        WHERE as_of_date = %s
        ORDER BY created_at DESC
        LIMIT 1
    """
    target = as_of_date or date.today()
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (target,))
            row = cursor.fetchone()
        finally:
            cursor.close()
    if row is None:
        return None
    return _row_to_engine_run(row)


def load_run(db_manager: DatabaseManager, run_id: str) -> EngineRun:
    """Load an :class:`EngineRun` by ``run_id``.

    Raises ``EngineRunStateError`` if the run cannot be found.
    """

    sql = """
        SELECT run_id,
               as_of_date,
               region,
               phase,
               error,
               created_at,
               updated_at,
               phase_started_at,
               phase_completed_at
        FROM engine_runs
        WHERE run_id = %s
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (run_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()

    if row is None:
        raise EngineRunStateError(f"Engine run {run_id!r} not found")

    return _row_to_engine_run(row)


def get_or_create_run(
    db_manager: DatabaseManager,
    as_of_date: date,
    region: str,
) -> EngineRun:
    """Return the existing run for (date, region) or create a new one.

    New runs are created in the ``WAITING_FOR_DATA`` phase.
    """

    select_sql = """
        SELECT run_id,
               as_of_date,
               region,
               phase,
               error,
               created_at,
               updated_at,
               phase_started_at,
               phase_completed_at
        FROM engine_runs
        WHERE as_of_date = %s AND region = %s
    """

    insert_sql = """
        INSERT INTO engine_runs (
            run_id,
            as_of_date,
            region,
            phase,
            error,
            created_at,
            updated_at,
            phase_started_at,
            phase_completed_at
        ) VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), NOW(), NULL)
        ON CONFLICT (as_of_date, region) DO NOTHING
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(select_sql, (as_of_date, region))
            row = cursor.fetchone()
            if row is not None:
                return _row_to_engine_run(row)

            run_id = generate_uuid()
            phase = RunPhase.WAITING_FOR_DATA.value
            cursor.execute(
                insert_sql,
                (
                    run_id,
                    as_of_date,
                    region,
                    phase,
                    Json({}),
                ),
            )
            conn.commit()

            cursor.execute(select_sql, (as_of_date, region))
            row = cursor.fetchone()
            if row is None:  # pragma: no cover - defensive
                raise EngineRunStateError("Failed to create engine run row")
            return _row_to_engine_run(row)
        finally:
            cursor.close()


def _validate_transition(current: RunPhase, new: RunPhase) -> None:
    """Validate a phase transition.

    Raises :class:`EngineRunStateError` if the transition is not allowed.
    """

    if current == new:
        return

    if current == RunPhase.FAILED:
        raise EngineRunStateError("Cannot transition from FAILED state")

    allowed_successors: dict[RunPhase, set[RunPhase]] = {
        RunPhase.WAITING_FOR_DATA: {RunPhase.DATA_READY, RunPhase.FAILED},
        RunPhase.DATA_READY: {RunPhase.SIGNALS_DONE, RunPhase.FAILED},
        RunPhase.SIGNALS_DONE: {RunPhase.UNIVERSES_DONE, RunPhase.FAILED},
        RunPhase.UNIVERSES_DONE: {RunPhase.BOOKS_DONE, RunPhase.FAILED},
        RunPhase.BOOKS_DONE: {RunPhase.EXECUTION_DONE, RunPhase.OPTIONS_DONE, RunPhase.COMPLETED, RunPhase.FAILED},
        RunPhase.EXECUTION_DONE: {RunPhase.OPTIONS_DONE, RunPhase.COMPLETED, RunPhase.FAILED},
        RunPhase.OPTIONS_DONE: {RunPhase.COMPLETED, RunPhase.FAILED},
        RunPhase.COMPLETED: set(),
    }

    successors = allowed_successors.get(current, set())
    if new not in successors:
        raise EngineRunStateError(f"Invalid transition {current.value} -> {new.value}")


def update_phase(
    db_manager: DatabaseManager,
    run_id: str,
    new_phase: RunPhase,
    error: Optional[dict] = None,
) -> EngineRun:
    """Atomically update a run's phase.

    This function validates the requested transition and updates the
    ``engine_runs`` row, including ``phase_started_at`` and
    ``phase_completed_at`` timestamps.
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT run_id,
                       as_of_date,
                       region,
                       phase,
                       error,
                       created_at,
                       updated_at,
                       phase_started_at,
                       phase_completed_at
                FROM engine_runs
                WHERE run_id = %s
                FOR UPDATE
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise EngineRunStateError(f"Engine run {run_id!r} not found")

            current = _row_to_engine_run(row)
            _validate_transition(current.phase, new_phase)

            error_payload = Json(error or {})
            # Update timestamps using database NOW() for consistency.
            # For terminal phases (COMPLETED/FAILED), set phase_completed_at.
            if new_phase in {RunPhase.COMPLETED, RunPhase.FAILED}:
                cursor.execute(
                    """
                    UPDATE engine_runs
                    SET phase = %s,
                        error = %s,
                        updated_at = NOW(),
                        phase_started_at = NOW(),
                        phase_completed_at = NOW()
                    WHERE run_id = %s
                    """,
                    (new_phase.value, error_payload, run_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE engine_runs
                    SET phase = %s,
                        error = %s,
                        updated_at = NOW(),
                        phase_started_at = NOW()
                    WHERE run_id = %s
                    """,
                    (new_phase.value, error_payload, run_id),
                )
            conn.commit()

            cursor.execute(
                """
                SELECT run_id,
                       as_of_date,
                       region,
                       phase,
                       error,
                       created_at,
                       updated_at,
                       phase_started_at,
                       phase_completed_at
                FROM engine_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            new_row = cursor.fetchone()
            if new_row is None:  # pragma: no cover - defensive
                raise EngineRunStateError(f"Engine run {run_id!r} disappeared after update")
            return _row_to_engine_run(new_row)
        finally:
            cursor.close()


def list_active_runs(db_manager: DatabaseManager) -> list[EngineRun]:
    """Return all runs that are not in COMPLETED/FAILED phases."""

    sql = """
        SELECT run_id,
               as_of_date,
               region,
               phase,
               error,
               created_at,
               updated_at,
               phase_started_at,
               phase_completed_at
        FROM engine_runs
        WHERE phase NOT IN ('COMPLETED', 'FAILED')
        ORDER BY as_of_date, region
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [_row_to_engine_run(row) for row in rows]


def force_reset_run_to_waiting(
    db_manager: DatabaseManager,
    run_id: str,
    *,
    reason: str = "forced reset to WAITING_FOR_DATA",
) -> EngineRun:
    """Force-reset an existing run back to WAITING_FOR_DATA.

    This bypasses normal forward-only transition validation and is intended
    for operational recovery when a same-date run is stuck in a terminal
    phase (e.g. OPTIONS_DONE) before the intended post-close cycle starts.

    Args:
        db_manager: Runtime database manager.
        run_id: Target run identifier.
        reason: Optional reason recorded in ``error`` payload.

    Returns:
        The updated EngineRun.
    """
    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE engine_runs
                SET phase = %s,
                    error = %s,
                    updated_at = NOW(),
                    phase_started_at = NOW(),
                    phase_completed_at = NULL
                WHERE run_id = %s
                """,
                (
                    RunPhase.WAITING_FOR_DATA.value,
                    Json({"reset_reason": reason}),
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise EngineRunStateError(f"Engine run {run_id!r} not found for force reset")
            conn.commit()

            cursor.execute(
                """
                SELECT run_id,
                       as_of_date,
                       region,
                       phase,
                       error,
                       created_at,
                       updated_at,
                       phase_started_at,
                       phase_completed_at
                FROM engine_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            row = cursor.fetchone()
            if row is None:  # pragma: no cover - defensive
                raise EngineRunStateError(f"Engine run {run_id!r} disappeared after force reset")
            logger.warning(
                "Force-reset engine run %s to WAITING_FOR_DATA (%s)",
                run_id,
                reason,
            )
            return _row_to_engine_run(row)
        finally:
            cursor.close()
