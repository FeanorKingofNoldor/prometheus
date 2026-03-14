"""Layer 1: tighten news_articles contracts

Revision ID: 0046_news_articles_l1
Revises: 0045_correlation_panels_l1
Create Date: 2025-12-16

Layer 1 contract for ``news_articles``:
- source is non-empty
- headline is non-empty
- language is either NULL or non-empty
- metadata is either NULL or a JSON object

Note: timezone correctness and content quality are validated via
higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0046_news_articles_l1"
down_revision: Union[str, None] = "0045_correlation_panels_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_news_articles_source_nonempty",
        "news_articles",
        "btrim(source) <> ''",
    )

    op.create_check_constraint(
        "ck_news_articles_headline_nonempty",
        "news_articles",
        "btrim(headline) <> ''",
    )

    op.create_check_constraint(
        "ck_news_articles_language_nonempty_when_present",
        "news_articles",
        "language IS NULL OR btrim(language) <> ''",
    )

    op.create_check_constraint(
        "ck_news_articles_metadata_object_or_null",
        "news_articles",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_news_articles_metadata_object_or_null",
        "news_articles",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_articles_language_nonempty_when_present",
        "news_articles",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_articles_headline_nonempty",
        "news_articles",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_articles_source_nonempty",
        "news_articles",
        type_="check",
    )
