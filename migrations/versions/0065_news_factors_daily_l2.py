"""Layer 2: tighten news_factors_daily contracts

Revision ID: 0065_news_factors_daily_l2
Revises: 0064_instrument_scores_l2
Create Date: 2025-12-16

Layer 2 contract for ``news_factors_daily``:
- issuer_id/model_id/factor_name are non-empty
- factor_value is finite (no NaN/Inf)
- metadata is either NULL or a JSON object

Note: lookahead safety and factor semantics are higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0065_news_factors_daily_l2"
down_revision: Union[str, None] = "0064_instrument_scores_l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_news_factors_daily_issuer_id_nonempty",
        "news_factors_daily",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_news_factors_daily_model_id_nonempty",
        "news_factors_daily",
        "btrim(model_id) <> ''",
    )

    op.create_check_constraint(
        "ck_news_factors_daily_factor_name_nonempty",
        "news_factors_daily",
        "btrim(factor_name) <> ''",
    )

    op.create_check_constraint(
        "ck_news_factors_daily_factor_value_finite",
        "news_factors_daily",
        f"factor_value NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_news_factors_daily_metadata_object_or_null",
        "news_factors_daily",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_news_factors_daily_metadata_object_or_null",
        "news_factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_factors_daily_factor_value_finite",
        "news_factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_factors_daily_factor_name_nonempty",
        "news_factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_factors_daily_model_id_nonempty",
        "news_factors_daily",
        type_="check",
    )
    op.drop_constraint(
        "ck_news_factors_daily_issuer_id_nonempty",
        "news_factors_daily",
        type_="check",
    )
