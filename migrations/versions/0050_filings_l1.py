"""Layer 1: tighten filings contracts

Revision ID: 0050_filings_l1
Revises: 0049_fundamental_ratios_l1
Create Date: 2025-12-16

Layer 1 contract for ``filings``:
- issuer_id is non-empty
- filing_type is non-empty
- text_ref is non-empty
- metadata is either NULL or a JSON object

Note: issuer_id existence and text_ref artifact immutability are
higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0050_filings_l1"
down_revision: Union[str, None] = "0049_fundamental_ratios_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_filings_issuer_id_nonempty",
        "filings",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_filings_filing_type_nonempty",
        "filings",
        "btrim(filing_type) <> ''",
    )

    op.create_check_constraint(
        "ck_filings_text_ref_nonempty",
        "filings",
        "btrim(text_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_filings_metadata_object_or_null",
        "filings",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_filings_metadata_object_or_null",
        "filings",
        type_="check",
    )
    op.drop_constraint(
        "ck_filings_text_ref_nonempty",
        "filings",
        type_="check",
    )
    op.drop_constraint(
        "ck_filings_filing_type_nonempty",
        "filings",
        type_="check",
    )
    op.drop_constraint(
        "ck_filings_issuer_id_nonempty",
        "filings",
        type_="check",
    )
