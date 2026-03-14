"""Add instrument_identifiers (time-versioned instrument ID history)

Revision ID: 0030_instrument_identifiers
Revises: 0029_issuer_classifications
Create Date: 2025-12-16

This migration introduces `instrument_identifiers`, a canonical, time-versioned
identifier history for instruments (tickers, CUSIP/ISIN/FIGI, vendor codes, etc.).

Motivation
----------
Instrument identity must remain stable even when external identifiers change
(e.g. ticker changes) and even when identifiers are reused across time.

`instrument_identifiers` provides:
- Interval validity via (effective_start, effective_end)
- Deterministic as-of joins
- Trigger-based non-overlap enforcement per (instrument_id, identifier_type)
  without requiring Postgres `btree_gist`
- Provenance (source, ingested_at, metadata)

NOTE
----
We only enforce non-overlap per (instrument_id, identifier_type). We do NOT
attempt to enforce non-overlap per (identifier_type, identifier_value) across
instruments because identifier reuse is common (especially tickers) and depends
on how the identifier is scoped (exchange, region, vendor).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0030_instrument_identifiers"
down_revision: Union[str, None] = "0029_issuer_classifications"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "instrument_identifiers",
        sa.Column(
            "instrument_identifier_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column(
            "instrument_id",
            sa.String(length=50),
            sa.ForeignKey("instruments.instrument_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("identifier_type", sa.String(length=32), nullable=False),
        sa.Column("identifier_value", sa.String(length=128), nullable=False),
        sa.Column("effective_start", sa.Date, nullable=False),
        sa.Column("effective_end", sa.Date, nullable=True),
        sa.Column("source", sa.String(length=64), nullable=False, server_default="manual"),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    # Basic sanity checks.
    op.create_check_constraint(
        "ck_instrument_identifiers_effective_range",
        "instrument_identifiers",
        "effective_end IS NULL OR effective_start < effective_end",
    )
    op.create_check_constraint(
        "ck_instrument_identifiers_identifier_type_nonempty",
        "instrument_identifiers",
        "char_length(identifier_type) > 0",
    )
    op.create_check_constraint(
        "ck_instrument_identifiers_identifier_value_nonempty",
        "instrument_identifiers",
        "char_length(identifier_value) > 0",
    )

    # Prevent accidental duplicates.
    op.create_index(
        "ux_instrument_identifiers_inst_type_start",
        "instrument_identifiers",
        ["instrument_id", "identifier_type", "effective_start"],
        unique=True,
    )

    # Common lookup paths.
    op.create_index(
        "idx_instrument_identifiers_inst_type",
        "instrument_identifiers",
        ["instrument_id", "identifier_type"],
        unique=False,
    )
    op.create_index(
        "idx_instrument_identifiers_type_value",
        "instrument_identifiers",
        ["identifier_type", "identifier_value"],
        unique=False,
    )

    # Enforce non-overlapping effective intervals per instrument + identifier_type.
    #
    # Treat NULL effective_end as open-ended until infinity.
    # Using [) bounds means effective_end is exclusive.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION instrument_identifiers_prevent_overlap()
        RETURNS trigger AS $$
        DECLARE
          new_range daterange;
        BEGIN
          new_range := daterange(
            NEW.effective_start,
            COALESCE(NEW.effective_end, 'infinity'::date),
            '[)'
          );

          IF EXISTS (
            SELECT 1
            FROM instrument_identifiers AS ii
            WHERE ii.instrument_id = NEW.instrument_id
              AND ii.identifier_type = NEW.identifier_type
              AND ii.instrument_identifier_id <> COALESCE(NEW.instrument_identifier_id, -1)
              AND daterange(
                    ii.effective_start,
                    COALESCE(ii.effective_end, 'infinity'::date),
                    '[)'
                  ) && new_range
            LIMIT 1
          ) THEN
            RAISE EXCEPTION
              'instrument_identifiers overlap for instrument_id=% identifier_type=% range=%',
              NEW.instrument_id,
              NEW.identifier_type,
              new_range;
          END IF;

          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_instrument_identifiers_prevent_overlap
        BEFORE INSERT OR UPDATE ON instrument_identifiers
        FOR EACH ROW
        EXECUTE FUNCTION instrument_identifiers_prevent_overlap();
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_instrument_identifiers_prevent_overlap ON instrument_identifiers"
    )
    op.execute("DROP FUNCTION IF EXISTS instrument_identifiers_prevent_overlap()")

    op.drop_index(
        "idx_instrument_identifiers_type_value",
        table_name="instrument_identifiers",
    )
    op.drop_index(
        "idx_instrument_identifiers_inst_type",
        table_name="instrument_identifiers",
    )
    op.drop_index(
        "ux_instrument_identifiers_inst_type_start",
        table_name="instrument_identifiers",
    )

    op.drop_constraint(
        "ck_instrument_identifiers_identifier_value_nonempty",
        "instrument_identifiers",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_identifiers_identifier_type_nonempty",
        "instrument_identifiers",
        type_="check",
    )
    op.drop_constraint(
        "ck_instrument_identifiers_effective_range",
        "instrument_identifiers",
        type_="check",
    )

    op.drop_table("instrument_identifiers")
