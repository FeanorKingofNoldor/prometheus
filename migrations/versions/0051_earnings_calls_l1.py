"""Layer 1: tighten earnings_calls contracts

Revision ID: 0051_earnings_calls_l1
Revises: 0050_filings_l1
Create Date: 2025-12-16

Layer 1 contract for ``earnings_calls``:
- issuer_id is non-empty
- transcript_ref is non-empty
- metadata is either NULL or a JSON object

Note: issuer_id existence and transcript_ref artifact immutability are
higher-level audits.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0051_earnings_calls_l1"
down_revision: Union[str, None] = "0050_filings_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_earnings_calls_issuer_id_nonempty",
        "earnings_calls",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_earnings_calls_transcript_ref_nonempty",
        "earnings_calls",
        "btrim(transcript_ref) <> ''",
    )

    op.create_check_constraint(
        "ck_earnings_calls_metadata_object_or_null",
        "earnings_calls",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_earnings_calls_metadata_object_or_null",
        "earnings_calls",
        type_="check",
    )
    op.drop_constraint(
        "ck_earnings_calls_transcript_ref_nonempty",
        "earnings_calls",
        type_="check",
    )
    op.drop_constraint(
        "ck_earnings_calls_issuer_id_nonempty",
        "earnings_calls",
        type_="check",
    )
