"""Layer 0: tighten engine_runs contracts

Revision ID: 0039_engine_runs_l0
Revises: 0038_job_executions_l0
Create Date: 2025-12-16

Layer 0 contract for ``engine_runs``:
- records of engine executions (what ran, when, with what config)
- must reference the config used (via config_json payload/ref)
- must record data cut (as_of_date) and whether the run was live-safe

This table is a mutable state-machine table used by pipeline/orchestration.
This migration adds:
- config_json payload/ref and live_safe flag
- CHECK constraints for basic sanity and timestamp/state consistency
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0039_engine_runs_l0"
down_revision: Union[str, None] = "0038_job_executions_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ALLOWED_PHASES = (
    "WAITING_FOR_DATA",
    "DATA_READY",
    "SIGNALS_DONE",
    "UNIVERSES_DONE",
    "BOOKS_DONE",
    "COMPLETED",
    "FAILED",
)


def upgrade() -> None:
    # Config payload/ref for reproducibility.
    op.add_column(
        "engine_runs",
        sa.Column(
            "config_json",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )

    # Live-safety marker: true means the run was intended to be free of lookahead.
    op.add_column(
        "engine_runs",
        sa.Column(
            "live_safe",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
        ),
    )

    # Backfill legacy rows created before phase_started_at was consistently set.
    op.execute("UPDATE engine_runs SET phase_started_at = created_at WHERE phase_started_at IS NULL")

    # Basic non-empty checks.
    op.create_check_constraint(
        "ck_engine_runs_run_id_nonempty",
        "engine_runs",
        "btrim(run_id) <> ''",
    )
    op.create_check_constraint(
        "ck_engine_runs_region_nonempty",
        "engine_runs",
        "btrim(region) <> ''",
    )

    # Allowed phases.
    allowed_sql = "phase IN (" + ",".join(f"'{p}'" for p in _ALLOWED_PHASES) + ")"
    op.create_check_constraint(
        "ck_engine_runs_phase_allowed",
        "engine_runs",
        allowed_sql,
    )

    # Config json should be an object.
    op.create_check_constraint(
        "ck_engine_runs_config_json_object",
        "engine_runs",
        "jsonb_typeof(config_json) = 'object'",
    )

    # Error json (if present) should be an object.
    op.create_check_constraint(
        "ck_engine_runs_error_object_or_null",
        "engine_runs",
        "error IS NULL OR jsonb_typeof(error) = 'object'",
    )

    # Timestamps monotonic.
    op.create_check_constraint(
        "ck_engine_runs_created_le_updated",
        "engine_runs",
        "created_at <= updated_at",
    )

    # Phase timestamps consistency:
    # - phase_started_at is always set
    # - terminal phases have phase_completed_at; non-terminal phases do not.
    op.create_check_constraint(
        "ck_engine_runs_phase_started_present",
        "engine_runs",
        "phase_started_at IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_engine_runs_phase_completed_consistent",
        "engine_runs",
        "(phase IN ('COMPLETED','FAILED') AND phase_completed_at IS NOT NULL) "
        "OR (phase NOT IN ('COMPLETED','FAILED') AND phase_completed_at IS NULL)",
    )

    op.create_check_constraint(
        "ck_engine_runs_phase_started_le_completed",
        "engine_runs",
        "phase_completed_at IS NULL OR phase_started_at <= phase_completed_at",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_engine_runs_phase_started_le_completed",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_phase_completed_consistent",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_phase_started_present",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_created_le_updated",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_error_object_or_null",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_config_json_object",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_phase_allowed",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_region_nonempty",
        "engine_runs",
        type_="check",
    )
    op.drop_constraint(
        "ck_engine_runs_run_id_nonempty",
        "engine_runs",
        type_="check",
    )

    op.drop_column("engine_runs", "live_safe")
    op.drop_column("engine_runs", "config_json")