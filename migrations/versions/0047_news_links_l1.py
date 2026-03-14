"""Layer 1: tighten news_links contracts

Revision ID: 0047_news_links_l1
Revises: 0046_news_articles_l1
Create Date: 2025-12-16

Layer 1 contract for ``news_links``:
- issuer_id is non-empty
- instrument_id is non-empty
- article_id refers to an existing news_articles row (already enforced by FK)

Note: correctness of entity linking is validated via higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0047_news_links_l1"
down_revision: Union[str, None] = "0046_news_articles_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_news_links_issuer_id_nonempty",
        "news_links",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_news_links_instrument_id_nonempty",
        "news_links",
        "btrim(instrument_id) <> ''",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_news_links_instrument_id_nonempty",
        "news_links",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_links_issuer_id_nonempty",
        "news_links",
        type_="check",
    )
