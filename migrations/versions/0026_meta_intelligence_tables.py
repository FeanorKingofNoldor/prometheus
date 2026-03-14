"""meta intelligence tables

Revision ID: 0026
Revises: 0025
Create Date: 2025-12-02

This migration introduces tables used by the Meta/Kronos intelligence layer
for analyzing backtest results and generating configuration improvement proposals:

- meta_config_proposals: Stores generated configuration change proposals
- config_change_log: Tracks applied configuration changes and their outcomes
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0026_meta_intelligence_tables"
down_revision = "0025_data_ingestion_status"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create Meta intelligence tables."""

    # Table for storing configuration change proposals
    op.create_table(
        "meta_config_proposals",
        sa.Column("proposal_id", sa.String(length=64), primary_key=True),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("strategy_id", sa.String(length=64), nullable=True),
        sa.Column("market_id", sa.String(length=32), nullable=True),
        sa.Column("proposal_type", sa.String(length=64), nullable=False),
        # What to change (e.g., 'universe_size', 'risk_limit', 'signal_weight')
        sa.Column("target_component", sa.String(length=128), nullable=False),
        # Current and proposed values stored as JSONB for flexibility
        sa.Column("current_value", postgresql.JSONB, nullable=True),
        sa.Column("proposed_value", postgresql.JSONB, nullable=False),
        # Confidence and impact estimates
        sa.Column("confidence_score", sa.Float, nullable=False),
        sa.Column("expected_sharpe_improvement", sa.Float, nullable=True),
        sa.Column("expected_return_improvement", sa.Float, nullable=True),
        sa.Column("expected_risk_reduction", sa.Float, nullable=True),
        # Rationale and supporting data
        sa.Column("rationale", sa.Text, nullable=True),
        sa.Column("supporting_metrics", postgresql.JSONB, nullable=True),
        # Approval workflow
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="PENDING",
        ),  # PENDING, APPROVED, REJECTED, APPLIED, REVERTED
        sa.Column("approved_by", sa.String(length=64), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reverted_at", sa.DateTime(timezone=True), nullable=True),
        # Metadata
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_meta_config_proposals_status",
        "meta_config_proposals",
        ["status"],
    )
    op.create_index(
        "idx_meta_config_proposals_strategy",
        "meta_config_proposals",
        ["strategy_id", "created_at"],
    )
    op.create_index(
        "idx_meta_config_proposals_type",
        "meta_config_proposals",
        ["proposal_type", "target_component"],
    )

    # Table for tracking applied configuration changes
    op.create_table(
        "config_change_log",
        sa.Column("change_id", sa.String(length=64), primary_key=True),
        sa.Column("proposal_id", sa.String(length=64), nullable=True),
        sa.Column("strategy_id", sa.String(length=64), nullable=True),
        sa.Column("market_id", sa.String(length=32), nullable=True),
        sa.Column("change_type", sa.String(length=64), nullable=False),
        sa.Column("target_component", sa.String(length=128), nullable=False),
        # Before and after state
        sa.Column("previous_value", postgresql.JSONB, nullable=True),
        sa.Column("new_value", postgresql.JSONB, nullable=False),
        # Performance tracking
        sa.Column("sharpe_before", sa.Float, nullable=True),
        sa.Column("sharpe_after", sa.Float, nullable=True),
        sa.Column("return_before", sa.Float, nullable=True),
        sa.Column("return_after", sa.Float, nullable=True),
        sa.Column("risk_before", sa.Float, nullable=True),
        sa.Column("risk_after", sa.Float, nullable=True),
        # Evaluation period
        sa.Column("evaluation_start_date", sa.Date, nullable=True),
        sa.Column("evaluation_end_date", sa.Date, nullable=True),
        # Reversion tracking
        sa.Column("is_reverted", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("reverted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reversion_reason", sa.Text, nullable=True),
        # Applied by
        sa.Column("applied_by", sa.String(length=64), nullable=True),
        sa.Column(
            "applied_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # Metadata
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.ForeignKeyConstraint(
            ["proposal_id"],
            ["meta_config_proposals.proposal_id"],
            name="fk_config_change_log_proposal",
        ),
    )

    op.create_index(
        "idx_config_change_log_strategy",
        "config_change_log",
        ["strategy_id", "applied_at"],
    )
    op.create_index(
        "idx_config_change_log_type",
        "config_change_log",
        ["change_type", "target_component"],
    )
    op.create_index(
        "idx_config_change_log_reverted",
        "config_change_log",
        ["is_reverted", "applied_at"],
    )


def downgrade() -> None:
    """Drop Meta intelligence tables."""

    op.drop_index("idx_config_change_log_reverted", table_name="config_change_log")
    op.drop_index("idx_config_change_log_type", table_name="config_change_log")
    op.drop_index("idx_config_change_log_strategy", table_name="config_change_log")
    op.drop_table("config_change_log")

    op.drop_index(
        "idx_meta_config_proposals_type", table_name="meta_config_proposals"
    )
    op.drop_index(
        "idx_meta_config_proposals_strategy", table_name="meta_config_proposals"
    )
    op.drop_index("idx_meta_config_proposals_status", table_name="meta_config_proposals")
    op.drop_table("meta_config_proposals")
