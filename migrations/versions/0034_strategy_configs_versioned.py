"""Make strategy_configs versioned + append-only

Revision ID: 0034_strategy_configs_versioned
Revises: 0033_strategies_portfolios_l0
Create Date: 2025-12-16

The original ``strategy_configs`` table stored a single mutable JSON blob
per strategy_id.

Layer 0 contract requires:
- configs are immutable once written (append-only by version)
- provenance/versioning recorded (config hash)
- active config selection is explicit (no guessing)

This migration replaces ``strategy_configs`` with an append-only design and
adds an explicit pointer on ``strategies``.

New design
----------
- strategy_configs: append-only versions
  - strategy_config_id (bigint PK)
  - strategy_id (FK -> strategies.strategy_id)
  - config_json (jsonb)
  - config_hash (md5 of jsonb text; set by trigger)
  - created_at, created_by, metadata
  - unique (strategy_id, config_hash)
  - UPDATE/DELETE are blocked by trigger (append-only)

- strategies:
  - add active_strategy_config_id FK -> strategy_configs.strategy_config_id
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0034_strategy_configs_versioned"
down_revision: Union[str, None] = "0033_strategies_portfolios_l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Rename legacy table out of the way (it may be empty in early deployments).
    op.rename_table("strategy_configs", "strategy_configs_legacy")

    # Ensure any legacy strategy_id has a corresponding strategies row.
    # (Legacy deployments could have had configs without explicit strategies definitions.)
    op.execute(
        """
        INSERT INTO strategies (strategy_id, name)
        SELECT DISTINCT strategy_id, strategy_id
        FROM strategy_configs_legacy
        ON CONFLICT (strategy_id) DO NOTHING
        """
    )

    # Create new append-only strategy_configs.
    op.create_table(
        "strategy_configs",
        sa.Column(
            "strategy_config_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "strategy_id",
            sa.String(length=50),
            sa.ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "config_json",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "config_hash",
            sa.String(length=32),
            nullable=False,
        ),
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
        "ck_strategy_configs_strategy_id_nonempty",
        "strategy_configs",
        "btrim(strategy_id) <> ''",
    )
    op.create_check_constraint(
        "ck_strategy_configs_created_by_nonempty",
        "strategy_configs",
        "btrim(created_by) <> ''",
    )
    op.create_check_constraint(
        "ck_strategy_configs_config_hash_md5",
        "strategy_configs",
        "config_hash ~ '^[0-9a-f]{32}$'",
    )

    op.create_index(
        "idx_strategy_configs_strategy_created",
        "strategy_configs",
        ["strategy_id", "created_at"],
        unique=False,
    )

    op.create_unique_constraint(
        "ux_strategy_configs_strategy_hash",
        "strategy_configs",
        ["strategy_id", "config_hash"],
    )

    # Prevent updates/deletes (append-only contract).
    op.execute(
        """
        CREATE OR REPLACE FUNCTION strategy_configs_prevent_mutation()
        RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'strategy_configs is append-only; UPDATE/DELETE are not allowed';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_strategy_configs_prevent_mutation
        BEFORE UPDATE OR DELETE ON strategy_configs
        FOR EACH ROW
        EXECUTE FUNCTION strategy_configs_prevent_mutation();
        """
    )

    # Populate config_hash deterministically from config_json.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION strategy_configs_set_hash()
        RETURNS trigger AS $$
        BEGIN
          NEW.config_hash := md5(COALESCE(NEW.config_json, '{}'::jsonb)::text);
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_strategy_configs_set_hash
        BEFORE INSERT ON strategy_configs
        FOR EACH ROW
        EXECUTE FUNCTION strategy_configs_set_hash();
        """
    )

    # Migrate existing rows, if any.
    op.execute(
        """
        INSERT INTO strategy_configs (strategy_id, config_json, config_hash, created_at, created_by)
        SELECT
          strategy_id,
          COALESCE(config_json, '{}'::jsonb) AS config_json,
          md5(COALESCE(config_json, '{}'::jsonb)::text) AS config_hash,
          COALESCE(updated_at, now()) AS created_at,
          'legacy' AS created_by
        FROM strategy_configs_legacy
        """
    )

    # Drop the legacy table.
    op.drop_index("ix_strategy_configs_strategy", table_name="strategy_configs_legacy")
    op.drop_table("strategy_configs_legacy")

    # Add explicit active config pointer on strategies.
    op.add_column(
        "strategies",
        sa.Column("active_strategy_config_id", sa.BigInteger, nullable=True),
    )
    op.create_foreign_key(
        "fk_strategies_active_config",
        "strategies",
        "strategy_configs",
        ["active_strategy_config_id"],
        ["strategy_config_id"],
        ondelete="SET NULL",
    )

    # For migrated legacy rows (single-row-per-strategy), set active pointer to latest.
    op.execute(
        """
        UPDATE strategies s
        SET active_strategy_config_id = latest.strategy_config_id
        FROM (
            SELECT DISTINCT ON (strategy_id)
                strategy_id,
                strategy_config_id
            FROM strategy_configs
            ORDER BY strategy_id, created_at DESC, strategy_config_id DESC
        ) AS latest
        WHERE s.strategy_id = latest.strategy_id
        """
    )


def downgrade() -> None:
    # Remove strategies pointer.
    op.drop_constraint(
        "fk_strategies_active_config",
        "strategies",
        type_="foreignkey",
    )
    op.drop_column("strategies", "active_strategy_config_id")

    # Drop triggers/functions.
    op.execute("DROP TRIGGER IF EXISTS trg_strategy_configs_set_hash ON strategy_configs")
    op.execute("DROP FUNCTION IF EXISTS strategy_configs_set_hash()")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_strategy_configs_prevent_mutation ON strategy_configs"
    )
    op.execute("DROP FUNCTION IF EXISTS strategy_configs_prevent_mutation()")

    # Drop new table.
    op.drop_constraint(
        "ux_strategy_configs_strategy_hash",
        "strategy_configs",
        type_="unique",
    )
    op.drop_index("idx_strategy_configs_strategy_created", table_name="strategy_configs")
    op.drop_table("strategy_configs")

    # Recreate legacy strategy_configs schema.
    op.create_table(
        "strategy_configs",
        sa.Column("strategy_id", sa.String(length=64), primary_key=True, nullable=False),
        sa.Column(
            "config_json",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_strategy_configs_strategy", "strategy_configs", ["strategy_id"], unique=True)
