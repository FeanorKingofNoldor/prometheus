"""Layer 0: tighten job_executions contracts

Revision ID: 0038_job_executions_l0
Revises: 0037_data_ingestion_status_l0
Create Date: 2025-12-16

Layer 0 contract for ``job_executions``:
- each job run has a stable ID, timestamps, status
- each job run records a config payload/ref and durable log paths

This table is a mutable state-machine table used by the market-aware daemon.
This migration adds:
- FK to markets (when market_id is present)
- config_json + log_path fields
- CHECK constraints for status/state timestamps and basic sanity
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0038_job_executions_l0"
down_revision: Union[str, None] = "0037_data_ingestion_status_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Align market_id length with markets.market_id.
    op.alter_column(
        "job_executions",
        "market_id",
        existing_type=sa.String(length=20),
        type_=sa.String(length=50),
        nullable=True,
    )

    # Add config payload + log reference.
    op.add_column(
        "job_executions",
        sa.Column(
            "config_json",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.add_column(
        "job_executions",
        sa.Column("log_path", sa.Text, nullable=True),
    )

    # FK to markets (nullable).
    op.create_foreign_key(
        "fk_job_executions_market",
        "job_executions",
        "markets",
        ["market_id"],
        ["market_id"],
        ondelete="RESTRICT",
    )

    # Basic non-empty checks.
    op.create_check_constraint(
        "ck_job_executions_execution_id_nonempty",
        "job_executions",
        "btrim(execution_id) <> ''",
    )
    op.create_check_constraint(
        "ck_job_executions_job_id_nonempty",
        "job_executions",
        "btrim(job_id) <> ''",
    )
    op.create_check_constraint(
        "ck_job_executions_job_type_nonempty",
        "job_executions",
        "btrim(job_type) <> ''",
    )
    op.create_check_constraint(
        "ck_job_executions_dag_id_nonempty",
        "job_executions",
        "btrim(dag_id) <> ''",
    )
    op.create_check_constraint(
        "ck_job_executions_market_id_nonempty",
        "job_executions",
        "market_id IS NULL OR btrim(market_id) <> ''",
    )

    # Status allowed.
    op.create_check_constraint(
        "ck_job_executions_status_allowed",
        "job_executions",
        "status IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED')",
    )

    # Attempt number sanity.
    op.create_check_constraint(
        "ck_job_executions_attempt_ge_1",
        "job_executions",
        "attempt_number >= 1",
    )

    # Timestamps monotonic.
    op.create_check_constraint(
        "ck_job_executions_created_le_updated",
        "job_executions",
        "created_at <= updated_at",
    )

    # State/timestamp consistency.
    op.create_check_constraint(
        "ck_job_executions_state_timestamps",
        "job_executions",
        "(status = 'PENDING' AND started_at IS NULL AND completed_at IS NULL) "
        "OR (status = 'RUNNING' AND started_at IS NOT NULL AND completed_at IS NULL) "
        "OR (status IN ('SUCCESS','FAILED','SKIPPED') AND completed_at IS NOT NULL)",
    )

    op.create_check_constraint(
        "ck_job_executions_failed_requires_error",
        "job_executions",
        "status <> 'FAILED' OR (error_message IS NOT NULL AND btrim(error_message) <> '')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_job_executions_failed_requires_error",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_state_timestamps",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_created_le_updated",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_attempt_ge_1",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_status_allowed",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_market_id_nonempty",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_dag_id_nonempty",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_job_type_nonempty",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_job_id_nonempty",
        "job_executions",
        type_="check",
    )
    op.drop_constraint(
        "ck_job_executions_execution_id_nonempty",
        "job_executions",
        type_="check",
    )

    op.drop_constraint(
        "fk_job_executions_market",
        "job_executions",
        type_="foreignkey",
    )

    op.drop_column("job_executions", "log_path")
    op.drop_column("job_executions", "config_json")

    op.alter_column(
        "job_executions",
        "market_id",
        existing_type=sa.String(length=50),
        type_=sa.String(length=20),
        nullable=True,
    )
