"""Layer 3: tighten portfolio_risk_reports contracts

Revision ID: 0069_portfolio_risk_reports_l3
Revises: 0068_target_portfolios_l3
Create Date: 2025-12-16

Layer 3 contract for ``portfolio_risk_reports``:
- report_id/portfolio_id are non-empty
- portfolio_value > 0 and finite
- cash/net_exposure/gross_exposure/leverage are finite
- gross_exposure >= 0; leverage >= 0
- risk_metrics is a JSON object
- scenario_pnl/exposures_by_sector/exposures_by_factor/metadata are JSON objects when present

Note: risk model correctness is a higher-level audit.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0069_portfolio_risk_reports_l3"
down_revision: Union[str, None] = "0068_target_portfolios_l3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NONFINITE = "('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)"


def upgrade() -> None:
    op.create_check_constraint(
        "ck_portfolio_risk_reports_report_id_nonempty",
        "portfolio_risk_reports",
        "btrim(report_id) <> ''",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_portfolio_id_nonempty",
        "portfolio_risk_reports",
        "btrim(portfolio_id) <> ''",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_portfolio_value_finite",
        "portfolio_risk_reports",
        f"portfolio_value NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_portfolio_value_positive",
        "portfolio_risk_reports",
        "portfolio_value > 0.0",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_cash_finite",
        "portfolio_risk_reports",
        f"cash NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_net_exposure_finite",
        "portfolio_risk_reports",
        f"net_exposure NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_gross_exposure_finite",
        "portfolio_risk_reports",
        f"gross_exposure NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_gross_exposure_nonnegative",
        "portfolio_risk_reports",
        "gross_exposure >= 0.0",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_leverage_finite",
        "portfolio_risk_reports",
        f"leverage NOT IN {_NONFINITE}",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_leverage_nonnegative",
        "portfolio_risk_reports",
        "leverage >= 0.0",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_risk_metrics_object",
        "portfolio_risk_reports",
        "jsonb_typeof(risk_metrics) = 'object'",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_scenario_pnl_object_or_null",
        "portfolio_risk_reports",
        "scenario_pnl IS NULL OR jsonb_typeof(scenario_pnl) = 'object'",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_exposures_by_sector_object_or_null",
        "portfolio_risk_reports",
        "exposures_by_sector IS NULL OR jsonb_typeof(exposures_by_sector) = 'object'",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_exposures_by_factor_object_or_null",
        "portfolio_risk_reports",
        "exposures_by_factor IS NULL OR jsonb_typeof(exposures_by_factor) = 'object'",
    )

    op.create_check_constraint(
        "ck_portfolio_risk_reports_metadata_object_or_null",
        "portfolio_risk_reports",
        "metadata IS NULL OR jsonb_typeof(metadata) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_portfolio_risk_reports_metadata_object_or_null",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_exposures_by_factor_object_or_null",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_exposures_by_sector_object_or_null",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_scenario_pnl_object_or_null",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_risk_metrics_object",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_leverage_nonnegative",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_leverage_finite",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_gross_exposure_nonnegative",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_gross_exposure_finite",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_net_exposure_finite",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_cash_finite",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_portfolio_value_positive",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_portfolio_value_finite",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_portfolio_id_nonempty",
        "portfolio_risk_reports",
        type_="check",
    )
    op.drop_constraint(
        "ck_portfolio_risk_reports_report_id_nonempty",
        "portfolio_risk_reports",
        type_="check",
    )
