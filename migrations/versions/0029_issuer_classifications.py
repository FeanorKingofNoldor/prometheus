"""Add issuer_classifications (time-versioned sector/industry)

Revision ID: 0029_issuer_classifications
Revises: 0028_news_factors_daily
Create Date: 2025-12-15

This migration introduces `issuer_classifications`, a canonical, time-versioned
classification history for issuers (sector/industry/sub-industry).

Motivation
----------
Using a single `issuers.sector` value forces a static classification across the
entire history and encourages downstream code to `COALESCE(..., 'UNKNOWN')`,
which can create dominant UNKNOWN buckets and distort clustering.

`issuer_classifications` solves this by allowing classifications to change over
time and by supporting multiple taxonomies/sources.

Key properties
--------------
- Interval validity via (effective_start, effective_end).
- Enforced non-overlap per (issuer_id, taxonomy) using a trigger that checks
  for overlapping dateranges.
- Stores provenance (source, ingested_at, metadata).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0029_issuer_classifications"
down_revision: Union[str, None] = "0028_news_factors_daily"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "issuer_classifications",
        sa.Column("classification_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "issuer_id",
            sa.String(length=64),
            sa.ForeignKey("issuers.issuer_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("taxonomy", sa.String(length=32), nullable=False),
        sa.Column("effective_start", sa.Date, nullable=False),
        sa.Column("effective_end", sa.Date, nullable=True),
        sa.Column("sector", sa.String(length=128), nullable=False),
        sa.Column("industry", sa.String(length=128), nullable=True),
        sa.Column("sub_industry", sa.String(length=128), nullable=True),
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
        "ck_issuer_classifications_effective_range",
        "issuer_classifications",
        "effective_end IS NULL OR effective_start < effective_end",
    )

    # Uniqueness of interval starts per issuer/taxonomy (helps query planning and
    # avoids accidental duplicates).
    op.create_index(
        "ux_issuer_classifications_issuer_tax_start",
        "issuer_classifications",
        ["issuer_id", "taxonomy", "effective_start"],
        unique=True,
    )

    # Common lookup path: (issuer_id, taxonomy) then as_of_date.
    op.create_index(
        "idx_issuer_classifications_issuer_tax",
        "issuer_classifications",
        ["issuer_id", "taxonomy"],
        unique=False,
    )

    # Enforce non-overlapping effective intervals per issuer/taxonomy.
    #
    # We treat NULL effective_end as open-ended until infinity.
    # Using [) bounds means effective_end is exclusive.
    #
    # Note: We intentionally avoid GiST exclusion constraints here to keep
    # this migration portable across Postgres installs that may not have
    # `btree_gist` available.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION issuer_classifications_prevent_overlap()
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
            FROM issuer_classifications AS ic
            WHERE ic.issuer_id = NEW.issuer_id
              AND ic.taxonomy = NEW.taxonomy
              AND ic.classification_id <> COALESCE(NEW.classification_id, -1)
              AND daterange(
                    ic.effective_start,
                    COALESCE(ic.effective_end, 'infinity'::date),
                    '[)'
                  ) && new_range
            LIMIT 1
          ) THEN
            RAISE EXCEPTION
              'issuer_classifications overlap for issuer_id=% taxonomy=% range=%',
              NEW.issuer_id,
              NEW.taxonomy,
              new_range;
          END IF;

          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_issuer_classifications_prevent_overlap
        BEFORE INSERT OR UPDATE ON issuer_classifications
        FOR EACH ROW
        EXECUTE FUNCTION issuer_classifications_prevent_overlap();
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_issuer_classifications_prevent_overlap ON issuer_classifications"
    )
    op.execute("DROP FUNCTION IF EXISTS issuer_classifications_prevent_overlap()")

    op.drop_index(
        "idx_issuer_classifications_issuer_tax",
        table_name="issuer_classifications",
    )
    op.drop_index(
        "ux_issuer_classifications_issuer_tax_start",
        table_name="issuer_classifications",
    )
    op.drop_constraint(
        "ck_issuer_classifications_effective_range",
        "issuer_classifications",
        type_="check",
    )
    op.drop_table("issuer_classifications")
