"""Prometheus v2 – Scenario-based portfolio risk helpers.

This module provides a small helper for computing **scenario P&L** for a
portfolio given a set of target weights and a ``scenario_set_id`` from
``scenario_paths``.

The intent is to keep scenario-risk calculations explicit and
inspectable, and to make them reusable from both the Portfolio & Risk
Engine and ad-hoc research/CLI workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict

import numpy as np
from apathis.core.database import DatabaseManager
from apathis.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class PortfolioScenarioResult:
    """Result of applying a scenario set to a portfolio.

    Attributes:
        scenario_pnl: Mapping from ``"<scenario_set_id>:<scenario_id>"`` to
            portfolio return under that scenario (in NAV terms).
        summary_metrics: Aggregated statistics over the scenario
            distribution (VaR / ES and basic moments).
    """

    scenario_pnl: Dict[str, float]
    summary_metrics: Dict[str, float]


def compute_portfolio_scenario_pnl(
    db_manager: DatabaseManager,
    *,
    scenario_set_id: str,
    as_of_date: date | None,
    weights: Dict[str, float],
) -> PortfolioScenarioResult:
    """Compute portfolio-level scenario P&L for a given scenario set.

    The implementation uses instrument-level returns stored in
    ``scenario_paths`` for ``scenario_set_id`` and instruments present in
    ``weights``. For each scenario, we:

    1. Aggregate per-instrument simple returns over the full horizon.
    2. Compute a portfolio return assuming weights apply to NAV=1.0.

    The function returns both the raw per-scenario P&L mapping and a set
    of summary statistics (mean, min/max, and simple 95% VaR/ES) that can
    be stored in ``portfolio_risk_reports.risk_metrics``.
    """

    if not weights:
        return PortfolioScenarioResult(scenario_pnl={}, summary_metrics={})

    instrument_ids = [inst_id for inst_id, w in weights.items() if float(w) != 0.0]
    if not instrument_ids:
        return PortfolioScenarioResult(scenario_pnl={}, summary_metrics={})

    sql = """
        SELECT scenario_id, horizon_index, instrument_id, return_value
        FROM scenario_paths
        WHERE scenario_set_id = %s
          AND instrument_id = ANY(%s)
        ORDER BY scenario_id, horizon_index
    """

    with db_manager.get_runtime_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (scenario_set_id, instrument_ids))
            rows = cursor.fetchall()
        finally:
            cursor.close()

    if not rows:
        logger.debug(
            "compute_portfolio_scenario_pnl: no scenario_paths rows for scenario_set_id=%s instruments=%d",
            scenario_set_id,
            len(instrument_ids),
        )
        return PortfolioScenarioResult(scenario_pnl={}, summary_metrics={})

    # Group simple returns by (scenario_id, instrument_id), skipping the
    # optional horizon_index = 0 baseline rows if present.
    by_scenario: Dict[int, Dict[str, list[float]]] = {}
    for scenario_id, horizon_index, instrument_id, ret in rows:
        if horizon_index == 0:
            continue
        inst_id_str = str(instrument_id)
        scen_dict = by_scenario.setdefault(int(scenario_id), {})
        scen_dict.setdefault(inst_id_str, []).append(float(ret))

    if not by_scenario:
        return PortfolioScenarioResult(scenario_pnl={}, summary_metrics={})

    scenario_returns: Dict[int, float] = {}
    for scen_id, inst_map in by_scenario.items():
        pnl = 0.0
        for inst_id, rets in inst_map.items():
            w = float(weights.get(inst_id, 0.0))
            if w == 0.0:
                continue
            rets_arr = np.asarray(rets, dtype=float)
            # Cumulative simple return over the horizon.
            cum_ret = float(np.prod(1.0 + rets_arr) - 1.0)
            pnl += w * cum_ret
        scenario_returns[scen_id] = pnl

    if not scenario_returns:
        return PortfolioScenarioResult(scenario_pnl={}, summary_metrics={})

    # Construct key mapping using "<set>:<id>" convention so multiple
    # scenario sets can co-exist in a single ``scenario_pnl`` dict.
    scenario_pnl: Dict[str, float] = {
        f"{scenario_set_id}:{sid}": float(ret) for sid, ret in scenario_returns.items()
    }

    # Summary statistics over the scenario distribution.
    ret_arr = np.asarray(list(scenario_returns.values()), dtype=float)
    mean_ret = float(ret_arr.mean())
    min_ret = float(ret_arr.min())
    max_ret = float(ret_arr.max())

    # Basic 95% one-sided VaR/ES on the left tail.
    sorted_ret = np.sort(ret_arr)
    n = sorted_ret.size
    var_index = max(int(0.05 * n) - 1, 0)
    var_95 = float(sorted_ret[var_index])
    es_95 = float(sorted_ret[: var_index + 1].mean()) if var_index >= 0 else var_95

    summary: Dict[str, float] = {
        "scenario_pnl_mean": mean_ret,
        "scenario_pnl_min": min_ret,
        "scenario_pnl_max": max_ret,
        "scenario_var_95": var_95,
        "scenario_es_95": es_95,
        "scenario_num_paths": float(n),
    }

    return PortfolioScenarioResult(scenario_pnl=scenario_pnl, summary_metrics=summary)
