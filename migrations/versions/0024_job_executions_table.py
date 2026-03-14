"""Add job_executions table for DAG orchestration tracking.

Revision ID: 0024
Revises: 0023
Create Date: 2025-12-01

This migration creates the job_executions table which tracks the execution
status of individual jobs within DAGs. Used by the market-aware orchestrator
to maintain state across daemon restarts and provide visibility into pipeline
execution.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic
revision = "0024_job_executions_table"
down_revision = "0023_market_holidays_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create job_executions table."""
    op.create_table(
        "job_executions",
        sa.Column("execution_id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("job_id", sa.String(255), nullable=False, index=True),
        sa.Column("job_type", sa.String(100), nullable=False),
        sa.Column("dag_id", sa.String(100), nullable=False, index=True),
        sa.Column("market_id", sa.String(20), nullable=True),
        sa.Column("as_of_date", sa.Date, nullable=False, index=True),
        sa.Column("status", sa.String(20), nullable=False),  # PENDING, RUNNING, SUCCESS, FAILED, SKIPPED
        sa.Column("started_at", sa.TIMESTAMP(timezone=False), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=False), nullable=True),
        sa.Column("attempt_number", sa.Integer, nullable=False, default=1),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("error_details", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=False), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=False), nullable=False, server_default=sa.text("NOW()")),
    )
    
    # Composite index for common queries
    op.create_index(
        "ix_job_executions_dag_status",
        "job_executions",
        ["dag_id", "status"],
    )
    
    # Index for finding latest execution of a job
    op.create_index(
        "ix_job_executions_job_created",
        "job_executions",
        ["job_id", "created_at"],
    )


def downgrade() -> None:
    """Drop job_executions table."""
    op.drop_index("ix_job_executions_job_created", table_name="job_executions")
    op.drop_index("ix_job_executions_dag_status", table_name="job_executions")
    op.drop_table("job_executions")
