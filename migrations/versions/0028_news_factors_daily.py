"""news_factors_daily table

Revision ID: 0028_news_factors_daily
Revises: 0027_strategy_configs
Create Date: 2025-12-10

This migration introduces the `news_factors_daily` table in the historical
Postgres database for scalar NEWS-derived features per issuer×day.

The table is designed as a generic key-value store for NEWS factors:

- One row per (issuer_id, as_of_date, model_id, factor_name).
- `factor_value` stores the numeric value.
- `metadata` can hold optional JSON describing the factor construction
  (e.g. window lengths, normalisation method).

Example factors include:

- `news_intensity_raw`      – number of distinct articles for the issuer/day.
- `news_intensity_log`      – log(1 + n_articles).
- `news_silence_gap`        – days since last news on or before as_of_date.
- `news_coverage_ratio_30d` – fraction of days with news in last 30d.
- `news_novelty`            – 1 - cosine_similarity(today, issuer centroid).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0028_news_factors_daily"
down_revision: Union[str, None] = "0027_strategy_configs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create news_factors_daily table."""

    op.create_table(
        "news_factors_daily",
        sa.Column("issuer_id", sa.String(length=64), nullable=False),
        sa.Column("as_of_date", sa.Date, nullable=False),
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("factor_name", sa.String(length=64), nullable=False),
        sa.Column("factor_value", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_primary_key(
        "pk_news_factors_daily",
        "news_factors_daily",
        ["issuer_id", "as_of_date", "model_id", "factor_name"],
    )

    op.create_index(
        "idx_news_factors_issuer_date",
        "news_factors_daily",
        ["issuer_id", "as_of_date"],
        unique=False,
    )


def downgrade() -> None:
    """Drop news_factors_daily table."""

    op.drop_index("idx_news_factors_issuer_date", table_name="news_factors_daily")
    op.drop_constraint("pk_news_factors_daily", "news_factors_daily", type_="primary")
    op.drop_table("news_factors_daily")
