"""Add data_ingestion_status table for tracking daily ingestion completion.

Revision ID: 0025
Revises: 0024
Create Date: 2025-12-02

This migration creates the data_ingestion_status table which tracks the
completion status of daily price ingestion for each market. Used by the
data ingestion orchestrator to determine when to mark engine_runs as DATA_READY.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic
revision = "0025_data_ingestion_status"
down_revision = "0024_job_executions_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create data_ingestion_status table."""
    op.create_table(
        "data_ingestion_status",
        sa.Column("status_id", sa.String(36), primary_key=True, nullable=False),
        sa.Column("market_id", sa.String(20), nullable=False, index=True),
        sa.Column("as_of_date", sa.Date, nullable=False, index=True),
        sa.Column("status", sa.String(20), nullable=False),  # PENDING, IN_PROGRESS, COMPLETE, FAILED
        sa.Column("last_price_timestamp", sa.TIMESTAMP(timezone=False), nullable=True),
        sa.Column("instruments_received", sa.Integer, nullable=False, default=0),
        sa.Column("instruments_expected", sa.Integer, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("error_details", postgresql.JSONB, nullable=True),
        sa.Column("started_at", sa.TIMESTAMP(timezone=False), nullable=True),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=False), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=False), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=False), nullable=False, server_default=sa.text("NOW()")),
    )
    
    # Composite index for querying current ingestion status
    op.create_index(
        "ix_data_ingestion_status_market_date",
        "data_ingestion_status",
        ["market_id", "as_of_date"],
        unique=True,
    )
    
    # Index for finding incomplete ingestions
    op.create_index(
        "ix_data_ingestion_status_status_date",
        "data_ingestion_status",
        ["status", "as_of_date"],
    )


def downgrade() -> None:
    """Drop data_ingestion_status table."""
    op.drop_index("ix_data_ingestion_status_status_date", table_name="data_ingestion_status")
    op.drop_index("ix_data_ingestion_status_market_date", table_name="data_ingestion_status")
    op.drop_table("data_ingestion_status")
