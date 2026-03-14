"""historical text, factor, and event tables

Revision ID: 0011
Revises: 0010
Create Date: 2025-11-24

This migration creates additional tables in the *historical* database
as specified in the data model (020) and architecture schema (30):

- factors_daily
- instrument_factors_daily
- correlation_panels
- news_articles
- news_links
- filings
- earnings_calls
- macro_events
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0011"
down_revision: Union[str, None] = "0010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create historical factor/text/event tables."""

    # ------------------------------------------------------------------
    # Market factors
    # ------------------------------------------------------------------

    op.create_table(
        "factors_daily",
        sa.Column("factor_id", sa.String(length=64), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("value", sa.Float, nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
        sa.PrimaryKeyConstraint("factor_id", "trade_date", name="pk_factors_daily"),
    )

    op.create_table(
        "instrument_factors_daily",
        sa.Column("instrument_id", sa.String(length=50), nullable=False),
        sa.Column("trade_date", sa.Date, nullable=False),
        sa.Column("factor_id", sa.String(length=64), nullable=False),
        sa.Column("exposure", sa.Float, nullable=False),
        sa.PrimaryKeyConstraint(
            "instrument_id",
            "trade_date",
            "factor_id",
            name="pk_instrument_factors_daily",
        ),
    )

    op.create_table(
        "correlation_panels",
        sa.Column("panel_id", sa.String(length=64), primary_key=True),
        sa.Column("start_date", sa.Date, nullable=False),
        sa.Column("end_date", sa.Date, nullable=False),
        sa.Column("universe_spec", postgresql.JSONB, nullable=False),
        sa.Column("matrix_ref", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # ------------------------------------------------------------------
    # Text & events
    # ------------------------------------------------------------------

    op.create_table(
        "news_articles",
        sa.Column("article_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(length=128), nullable=False),
        sa.Column("language", sa.String(length=16), nullable=True),
        sa.Column("headline", sa.Text, nullable=False),
        sa.Column("body", sa.Text, nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_news_articles_timestamp",
        "news_articles",
        ["timestamp"],
    )

    op.create_table(
        "news_links",
        sa.Column("article_id", sa.BigInteger, nullable=False),
        sa.Column("issuer_id", sa.String(length=50), nullable=True),
        sa.Column("instrument_id", sa.String(length=50), nullable=True),
        sa.ForeignKeyConstraint(
            ["article_id"],
            ["news_articles.article_id"],
            name="fk_news_links_article",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("article_id", "issuer_id", "instrument_id", name="pk_news_links"),
    )

    op.create_table(
        "filings",
        sa.Column("filing_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("issuer_id", sa.String(length=50), nullable=False),
        sa.Column("filing_type", sa.String(length=32), nullable=False),
        sa.Column("filing_date", sa.Date, nullable=False),
        sa.Column("text_ref", sa.String(length=255), nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_filings_issuer_date",
        "filings",
        ["issuer_id", "filing_date"],
    )

    op.create_table(
        "earnings_calls",
        sa.Column("call_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("issuer_id", sa.String(length=50), nullable=False),
        sa.Column("call_date", sa.Date, nullable=False),
        sa.Column("transcript_ref", sa.String(length=255), nullable=False),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_earnings_calls_issuer_date",
        "earnings_calls",
        ["issuer_id", "call_date"],
    )

    op.create_table(
        "macro_events",
        sa.Column("event_id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("country", sa.String(length=32), nullable=True),
        sa.Column("description", sa.Text, nullable=False),
        sa.Column("text_ref", sa.String(length=255), nullable=True),
        sa.Column("metadata", postgresql.JSONB, nullable=True),
    )

    op.create_index(
        "idx_macro_events_type_timestamp",
        "macro_events",
        ["event_type", "timestamp"],
    )


def downgrade() -> None:
    """Drop historical factor/text/event tables."""

    op.drop_index("idx_macro_events_type_timestamp", table_name="macro_events")
    op.drop_table("macro_events")

    op.drop_index("idx_earnings_calls_issuer_date", table_name="earnings_calls")
    op.drop_table("earnings_calls")

    op.drop_index("idx_filings_issuer_date", table_name="filings")
    op.drop_table("filings")

    op.drop_table("news_links")
    op.drop_index("idx_news_articles_timestamp", table_name="news_articles")
    op.drop_table("news_articles")

    op.drop_table("correlation_panels")
    op.drop_table("instrument_factors_daily")
    op.drop_table("factors_daily")
