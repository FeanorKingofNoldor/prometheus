"""Layer 1: tighten financial_statements contracts

Revision ID: 0048_financial_statements_l1
Revises: 0047_news_links_l1
Create Date: 2025-12-16

Layer 1 contract for ``financial_statements``:
- issuer_id is non-empty
- fiscal_period is non-empty and matches YYYY[A|Qn]
- statement_type is one of IS/BS/CF
- period_end is present
- currency is either NULL or non-empty
- values is a JSON object
- metadata is either NULL or a JSON object
- report_date is not before period_end

Note: statement content completeness and mapping consistency are
higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0048_financial_statements_l1"
down_revision: Union[str, None] = "0047_news_links_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_financial_statements_issuer_id_nonempty",
        "financial_statements",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_financial_statements_fiscal_period_nonempty",
        "financial_statements",
        "btrim(fiscal_period) <> ''",
    )

    op.create_check_constraint(
        "ck_financial_statements_fiscal_period_format",
        "financial_statements",
        "fiscal_period ~ '^[0-9]{4}(A|Q[1-4])$'",
    )

    op.create_check_constraint(
        "ck_financial_statements_statement_type_allowed",
        "financial_statements",
        "statement_type IN ('IS', 'BS', 'CF')",
    )

    op.create_check_constraint(
        "ck_financial_statements_period_end_present",
        "financial_statements",
        "period_end IS NOT NULL",
    )

    op.create_check_constraint(
        "ck_financial_statements_currency_nonempty_when_present",
        "financial_statements",
        "currency IS NULL OR btrim(currency) <> ''",
    )

    op.create_check_constraint(
        "ck_financial_statements_values_object",
        "financial_statements",
        "jsonb_typeof(values) = 'object'",
    )

    op.create_check_constraint(
        "ck_financial_statements_metadata_object_or_null",
        "financial_statements",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )

    op.create_check_constraint(
        "ck_financial_statements_report_date_not_before_period_end",
        "financial_statements",
        "period_end IS NULL OR report_date >= period_end",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_financial_statements_report_date_not_before_period_end",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_metadata_object_or_null",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_values_object",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_currency_nonempty_when_present",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_period_end_present",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_statement_type_allowed",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_fiscal_period_format",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_fiscal_period_nonempty",
        "financial_statements",
        type_="check",
    )
    op.drop_constraint(
        "ck_financial_statements_issuer_id_nonempty",
        "financial_statements",
        type_="check",
    )
