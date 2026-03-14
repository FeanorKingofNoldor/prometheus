"""Add EXECUTION_DONE to engine_runs phase check constraint

Revision ID: 0083_engine_runs_execution_done_phase
Revises: 0082_sector_health_daily
Create Date: 2026-03-01

The live daily orchestrator introduces an EXECUTION_DONE phase between
BOOKS_DONE and COMPLETED.  The existing check constraint from migration
0039 must be widened to accept the new value.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0083_engine_runs_execution_done_phase"
down_revision: Union[str, None] = "0082_sector_health_daily"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_OLD_PHASES = (
    "WAITING_FOR_DATA",
    "DATA_READY",
    "SIGNALS_DONE",
    "UNIVERSES_DONE",
    "BOOKS_DONE",
    "COMPLETED",
    "FAILED",
)

_NEW_PHASES = _OLD_PHASES[:5] + ("EXECUTION_DONE",) + _OLD_PHASES[5:]


def _phase_check_sql(phases: tuple[str, ...]) -> str:
    return "phase IN (" + ",".join(f"'{p}'" for p in phases) + ")"


def upgrade() -> None:
    op.drop_constraint("ck_engine_runs_phase_allowed", "engine_runs", type_="check")
    op.create_check_constraint(
        "ck_engine_runs_phase_allowed",
        "engine_runs",
        _phase_check_sql(_NEW_PHASES),
    )


def downgrade() -> None:
    op.drop_constraint("ck_engine_runs_phase_allowed", "engine_runs", type_="check")
    op.create_check_constraint(
        "ck_engine_runs_phase_allowed",
        "engine_runs",
        _phase_check_sql(_OLD_PHASES),
    )
