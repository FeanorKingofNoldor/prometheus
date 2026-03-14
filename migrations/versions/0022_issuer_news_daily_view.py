"""issuer×day news features view

Revision ID: 0022
Revises: 0021
Create Date: 2025-12-01

This migration creates a convenience view in the *historical* database
that exposes simple issuer×day news features derived from
`news_articles` / `news_links` and the issuer×day NEWS embeddings.

The view is defined as::

    CREATE VIEW issuer_news_daily AS
    WITH per_day AS (
        SELECT
            nl.issuer_id,
            DATE(na.timestamp) AS news_date,
            COUNT(DISTINCT na.article_id) AS n_articles
        FROM news_links nl
        JOIN news_articles na ON na.article_id = nl.article_id
        WHERE nl.issuer_id IS NOT NULL
        GROUP BY nl.issuer_id, DATE(na.timestamp)
    ),
    with_lag AS (
        SELECT
            issuer_id,
            news_date,
            n_articles,
            LAG(news_date) OVER (
                PARTITION BY issuer_id
                ORDER BY news_date
            ) AS prev_news_date
        FROM per_day
    )
    SELECT
        issuer_id,
        news_date,
        n_articles,
        CASE
            WHEN prev_news_date IS NULL THEN NULL
            ELSE (news_date - prev_news_date)
        END AS days_since_prev_news,
        issuer_id || ':' || news_date::text AS embedding_source_id
    FROM with_lag;

It is a *logical* feature surface only; consumers still fetch the
underlying issuer×day embeddings from `text_embeddings` using
`source_type = 'NEWS_ISSUER_DAY'` and the `embedding_source_id` key.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0022"
down_revision: Union[str, None] = "0021"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create issuer_news_daily view in the historical DB."""

    op.execute(
        sa.text(
            """
            CREATE VIEW issuer_news_daily AS
            WITH per_day AS (
                SELECT
                    nl.issuer_id,
                    DATE(na.timestamp) AS news_date,
                    COUNT(DISTINCT na.article_id) AS n_articles
                FROM news_links nl
                JOIN news_articles na ON na.article_id = nl.article_id
                WHERE nl.issuer_id IS NOT NULL
                GROUP BY nl.issuer_id, DATE(na.timestamp)
            ),
            with_lag AS (
                SELECT
                    issuer_id,
                    news_date,
                    n_articles,
                    LAG(news_date) OVER (
                        PARTITION BY issuer_id
                        ORDER BY news_date
                    ) AS prev_news_date
                FROM per_day
            )
            SELECT
                issuer_id,
                news_date,
                n_articles,
                CASE
                    WHEN prev_news_date IS NULL THEN NULL
                    ELSE (news_date - prev_news_date)
                END AS days_since_prev_news,
                issuer_id || ':' || news_date::text AS embedding_source_id
            FROM with_lag
            """
        )
    )


def downgrade() -> None:
    """Drop issuer_news_daily view."""

    op.execute(sa.text("DROP VIEW IF EXISTS issuer_news_daily"))