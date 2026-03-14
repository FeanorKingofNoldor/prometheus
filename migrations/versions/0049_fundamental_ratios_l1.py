"""Layer 1: tighten fundamental_ratios contracts

Revision ID: 0049_fundamental_ratios_l1
Revises: 0048_financial_statements_l1
Create Date: 2025-12-16

Layer 1 contract for ``fundamental_ratios``:
- issuer_id is non-empty
- frequency is non-empty and one of ANNUAL/QUARTERLY
- period_start < period_end
- numeric ratio fields are finite when present (no NaN/Inf)
- metrics is either NULL or a JSON object
- metadata is either NULL or a JSON object

Note: ratio definition/versioning is a higher-level audit.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0049_fundamental_ratios_l1"
down_revision: Union[str, None] = "0048_financial_statements_l1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_fundamental_ratios_issuer_id_nonempty",
        "fundamental_ratios",
        "btrim(issuer_id) <> ''",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_frequency_nonempty",
        "fundamental_ratios",
        "btrim(frequency) <> ''",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_frequency_allowed",
        "fundamental_ratios",
        "frequency IN ('ANNUAL', 'QUARTERLY')",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_period_window",
        "fundamental_ratios",
        "period_start < period_end",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_roe_finite",
        "fundamental_ratios",
        f"roe IS NULL OR roe NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_roic_finite",
        "fundamental_ratios",
        f"roic IS NULL OR roic NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_gross_margin_finite",
        "fundamental_ratios",
        f"gross_margin IS NULL OR gross_margin NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_op_margin_finite",
        "fundamental_ratios",
        f"op_margin IS NULL OR op_margin NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_net_margin_finite",
        "fundamental_ratios",
        f"net_margin IS NULL OR net_margin NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_leverage_finite",
        "fundamental_ratios",
        f"leverage IS NULL OR leverage NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_interest_coverage_finite",
        "fundamental_ratios",
        f"interest_coverage IS NULL OR interest_coverage NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_revenue_growth_finite",
        "fundamental_ratios",
        f"revenue_growth IS NULL OR revenue_growth NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_eps_growth_finite",
        "fundamental_ratios",
        f"eps_growth IS NULL OR eps_growth NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_metrics_object_or_null",
        "fundamental_ratios",
        "metrics IS NULL OR jsonb_typeof(metrics) = 'object'",
    )

    op.create_check_constraint(
        "ck_fundamental_ratios_metadata_object_or_null",
        "fundamental_ratios",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_fundamental_ratios_metadata_object_or_null",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_metrics_object_or_null",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_eps_growth_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_revenue_growth_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_interest_coverage_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_leverage_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_net_margin_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_op_margin_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_gross_margin_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_roic_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_roe_finite",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_period_window",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_frequency_allowed",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_frequency_nonempty",
        "fundamental_ratios",
        type_="check",
    )
    op.drop_constraint(
        "ck_fundamental_ratios_issuer_id_nonempty",
        "fundamental_ratios",
        type_="check",
    )
