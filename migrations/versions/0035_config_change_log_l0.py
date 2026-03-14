"""Layer 0: make config_change_log append-only and explicit reversion linkage

Revision ID: 0035_config_change_log_l0
Revises: 0034_strategy_configs_versioned
Create Date: 2025-12-16

Layer 0 contract for ``config_change_log``:
- append-only (no UPDATE/DELETE)
- entries reference what changed and when

The original schema included mutable columns (is_reverted + performance metrics)
that encouraged UPDATEs. This migration enforces immutability and moves
performance evaluations to a separate append-only table.

Changes
-------
- config_change_log:
  - add reverts_change_id (self-FK) to represent a reversion as its own row
  - require applied_by non-null/non-empty
  - add basic non-empty checks
  - prevent UPDATE/DELETE via trigger

- config_change_evaluations:
  - new append-only table for before/after metrics over an evaluation window
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0035_config_change_log_l0"
down_revision: Union[str, None] = "0034_strategy_configs_versioned"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- config_change_log: linkage + constraints
    op.add_column(
        "config_change_log",
        sa.Column("reverts_change_id", sa.String(length=64), nullable=True),
    )
    op.create_foreign_key(
        "fk_config_change_log_reverts_change",
        "config_change_log",
        "config_change_log",
        ["reverts_change_id"],
        ["change_id"],
    )

    # Enforce strategy_id + applied_by non-null (audit must be attributable).
    op.alter_column(
        "config_change_log",
        "strategy_id",
        existing_type=sa.String(length=64),
        nullable=False,
    )

    # Backfill applied_by for legacy rows (if any), then enforce non-null.
    op.execute(
        """
        UPDATE config_change_log
        SET applied_by = 'system'
        WHERE applied_by IS NULL OR btrim(applied_by) = ''
        """
    )

    op.alter_column(
        "config_change_log",
        "applied_by",
        existing_type=sa.String(length=64),
        nullable=False,
        server_default=sa.text("'system'"),
    )

    op.create_check_constraint(
        "ck_config_change_log_change_id_nonempty",
        "config_change_log",
        "btrim(change_id) <> ''",
    )
    op.create_check_constraint(
        "ck_config_change_log_strategy_id_nonempty",
        "config_change_log",
        "btrim(strategy_id) <> ''",
    )
    op.create_check_constraint(
        "ck_config_change_log_change_type_nonempty",
        "config_change_log",
        "btrim(change_type) <> ''",
    )
    op.create_check_constraint(
        "ck_config_change_log_target_component_nonempty",
        "config_change_log",
        "btrim(target_component) <> ''",
    )
    op.create_check_constraint(
        "ck_config_change_log_applied_by_nonempty",
        "config_change_log",
        "btrim(applied_by) <> ''",
    )
    op.create_check_constraint(
        "ck_config_change_log_reverts_not_self",
        "config_change_log",
        "reverts_change_id IS NULL OR reverts_change_id <> change_id",
    )
    # Only REVERT rows may have reverts_change_id.
    op.create_check_constraint(
        "ck_config_change_log_reverts_implies_type",
        "config_change_log",
        "reverts_change_id IS NULL OR change_type = 'REVERT'",
    )
    # REVERT rows must specify what they revert.
    op.create_check_constraint(
        "ck_config_change_log_revert_requires_pointer",
        "config_change_log",
        "change_type <> 'REVERT' OR reverts_change_id IS NOT NULL",
    )

    # At most one reversion row per original change.
    op.create_index(
        "ux_config_change_log_reverts_once",
        "config_change_log",
        ["reverts_change_id"],
        unique=True,
        postgresql_where=sa.text("reverts_change_id IS NOT NULL"),
    )

    # Prevent UPDATE/DELETE (append-only contract).
    op.execute(
        """
        CREATE OR REPLACE FUNCTION config_change_log_prevent_mutation()
        RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'config_change_log is append-only; UPDATE/DELETE are not allowed';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_config_change_log_prevent_mutation
        BEFORE UPDATE OR DELETE ON config_change_log
        FOR EACH ROW
        EXECUTE FUNCTION config_change_log_prevent_mutation();
        """
    )

    # --- config_change_evaluations: append-only evaluation metrics
    op.create_table(
        "config_change_evaluations",
        sa.Column(
            "evaluation_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "change_id",
            sa.String(length=64),
            sa.ForeignKey("config_change_log.change_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("evaluation_start_date", sa.Date, nullable=False),
        sa.Column("evaluation_end_date", sa.Date, nullable=False),
        sa.Column("sharpe_before", sa.Float, nullable=True),
        sa.Column("sharpe_after", sa.Float, nullable=True),
        sa.Column("return_before", sa.Float, nullable=True),
        sa.Column("return_after", sa.Float, nullable=True),
        sa.Column("risk_before", sa.Float, nullable=True),
        sa.Column("risk_after", sa.Float, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.String(length=64),
            server_default=sa.text("'system'"),
            nullable=False,
        ),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_check_constraint(
        "ck_config_change_evaluations_created_by_nonempty",
        "config_change_evaluations",
        "btrim(created_by) <> ''",
    )
    op.create_check_constraint(
        "ck_config_change_evaluations_date_order",
        "config_change_evaluations",
        "evaluation_start_date <= evaluation_end_date",
    )

    op.create_index(
        "idx_config_change_evaluations_change_created",
        "config_change_evaluations",
        ["change_id", "created_at"],
        unique=False,
    )

    # Prevent UPDATE/DELETE on evaluations as well.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION config_change_evaluations_prevent_mutation()
        RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'config_change_evaluations is append-only; UPDATE/DELETE are not allowed';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_config_change_evaluations_prevent_mutation
        BEFORE UPDATE OR DELETE ON config_change_evaluations
        FOR EACH ROW
        EXECUTE FUNCTION config_change_evaluations_prevent_mutation();
        """
    )


def downgrade() -> None:
    # Drop evaluation triggers/functions/table.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_config_change_evaluations_prevent_mutation ON config_change_evaluations"
    )
    op.execute("DROP FUNCTION IF EXISTS config_change_evaluations_prevent_mutation()")
    op.drop_index(
        "idx_config_change_evaluations_change_created", table_name="config_change_evaluations"
    )
    op.drop_constraint(
        "ck_config_change_evaluations_date_order",
        "config_change_evaluations",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_evaluations_created_by_nonempty",
        "config_change_evaluations",
        type_="check",
    )
    op.drop_table("config_change_evaluations")

    # Drop config_change_log triggers/functions.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_config_change_log_prevent_mutation ON config_change_log"
    )
    op.execute("DROP FUNCTION IF EXISTS config_change_log_prevent_mutation()")

    # Remove indexes/constraints/column.
    op.drop_index(
        "ux_config_change_log_reverts_once", table_name="config_change_log"
    )
    op.drop_constraint(
        "ck_config_change_log_revert_requires_pointer",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_reverts_implies_type",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_reverts_not_self",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_applied_by_nonempty",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_target_component_nonempty",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_change_type_nonempty",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_strategy_id_nonempty",
        "config_change_log",
        type_="check",
    )
    op.drop_constraint(
        "ck_config_change_log_change_id_nonempty",
        "config_change_log",
        type_="check",
    )

    op.drop_constraint(
        "fk_config_change_log_reverts_change",
        "config_change_log",
        type_="foreignkey",
    )
    op.drop_column("config_change_log", "reverts_change_id")

    # Relax applied_by and strategy_id back to nullable.
    op.alter_column(
        "config_change_log",
        "applied_by",
        existing_type=sa.String(length=64),
        nullable=True,
        server_default=None,
    )
    op.alter_column(
        "config_change_log",
        "strategy_id",
        existing_type=sa.String(length=64),
        nullable=True,
    )
