"""Layer 0: tighten data_ingestion_status contracts

Revision ID: 0037_data_ingestion_status_l0
Revises: 0036_meta_config_proposals_l0
Create Date: 2025-12-16

Layer 0 contract for ``data_ingestion_status``:
- per dataset/market, the last successful date and last attempt are consistent
- failures are recorded with enough context to debug

This table is a mutable state-machine table (runtime_db). This migration
adds constraints to enforce basic invariants and a FK to markets.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0037_data_ingestion_status_l0"
down_revision: Union[str, None] = "0036_meta_config_proposals_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Align market_id length with markets.market_id (varchar(50))
    op.alter_column(
        "data_ingestion_status",
        "market_id",
        existing_type=sa.String(length=20),
        type_=sa.String(length=50),
        nullable=False,
    )

    # Add FK to markets.
    op.create_foreign_key(
        "fk_data_ingestion_status_market",
        "data_ingestion_status",
        "markets",
        ["market_id"],
        ["market_id"],
        ondelete="RESTRICT",
    )

    # Basic non-empty checks.
    op.create_check_constraint(
        "ck_data_ingestion_status_status_id_nonempty",
        "data_ingestion_status",
        "btrim(status_id) <> ''",
    )
    op.create_check_constraint(
        "ck_data_ingestion_status_market_id_nonempty",
        "data_ingestion_status",
        "btrim(market_id) <> ''",
    )
    op.create_check_constraint(
        "ck_data_ingestion_status_status_allowed",
        "data_ingestion_status",
        "status IN ('PENDING','IN_PROGRESS','COMPLETE','FAILED')",
    )

    # Counts.
    op.create_check_constraint(
        "ck_data_ingestion_status_counts_nonneg",
        "data_ingestion_status",
        "instruments_received >= 0 AND (instruments_expected IS NULL OR instruments_expected >= 0)",
    )
    op.create_check_constraint(
        "ck_data_ingestion_status_counts_consistent",
        "data_ingestion_status",
        "instruments_expected IS NULL OR instruments_received <= instruments_expected",
    )

    # Timestamps monotonic.
    op.create_check_constraint(
        "ck_data_ingestion_status_created_le_updated",
        "data_ingestion_status",
        "created_at <= updated_at",
    )

    # State/timestamp consistency.
    op.create_check_constraint(
        "ck_data_ingestion_status_state_timestamps",
        "data_ingestion_status",
        "(status = 'PENDING' AND started_at IS NULL AND completed_at IS NULL) "
        "OR (status = 'IN_PROGRESS' AND started_at IS NOT NULL AND completed_at IS NULL) "
        "OR (status = 'COMPLETE' AND started_at IS NOT NULL AND completed_at IS NOT NULL AND last_price_timestamp IS NOT NULL) "
        "OR (status = 'FAILED' AND started_at IS NOT NULL AND completed_at IS NOT NULL)",
    )

    op.create_check_constraint(
        "ck_data_ingestion_status_failed_requires_error",
        "data_ingestion_status",
        "status <> 'FAILED' OR (error_message IS NOT NULL AND btrim(error_message) <> '')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_data_ingestion_status_failed_requires_error",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_state_timestamps",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_created_le_updated",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_counts_consistent",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_counts_nonneg",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_status_allowed",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_market_id_nonempty",
        "data_ingestion_status",
        type_="check",
    )
    op.drop_constraint(
        "ck_data_ingestion_status_status_id_nonempty",
        "data_ingestion_status",
        type_="check",
    )

    op.drop_constraint(
        "fk_data_ingestion_status_market",
        "data_ingestion_status",
        type_="foreignkey",
    )

    op.alter_column(
        "data_ingestion_status",
        "market_id",
        existing_type=sa.String(length=50),
        type_=sa.String(length=20),
        nullable=False,
    )
