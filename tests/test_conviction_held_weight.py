"""Tests for conviction held-position re-sizing (money-path audit fix #1).

A position held alive by conviction but not in today's selection must
keep its own last real target weight rather than being re-sized to the
average weight of the selected names every day.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List

from prometheus.portfolio.conviction import ConvictionConfig
from prometheus.portfolio.model_conviction import ConvictionPortfolioModel
from prometheus.portfolio.types import TargetPortfolio


@dataclass
class _StubInnerModel:
    """Inner model returning a fixed selection per call date."""

    weights_by_date: Dict[date, Dict[str, float]]
    _last_members: List[str] = field(default_factory=list)

    def build_target_portfolio(self, portfolio_id: str, as_of_date: date) -> TargetPortfolio:
        weights = dict(self.weights_by_date.get(as_of_date, {}))
        return TargetPortfolio(
            portfolio_id=portfolio_id,
            as_of_date=as_of_date,
            weights=weights,
            expected_return=0.0,
            expected_volatility=0.0,
            risk_metrics={},
            factor_exposures={},
            constraints_status={},
            metadata={},
        )


def _run_days(model: ConvictionPortfolioModel, start: date, weights_by_date):
    results = {}
    d = start
    for _ in range(len(weights_by_date)):
        tp = model.build_target_portfolio("BOOK1", d)
        results[d] = dict(tp.weights)
        d = d + timedelta(days=1)
    return results


def test_held_position_keeps_stable_weight_across_days():
    """A confirmed position dropped from selection keeps its own weight.

    HELD is selected days 1-4 (so it scales up and locks a real weight),
    then drops out of selection on day 5 while ALPHA/BETA remain. Under
    the old behaviour HELD would be re-sized to the average of ALPHA/BETA
    every day. It must instead keep its own last target weight.
    """
    d0 = date(2026, 1, 1)
    days = [d0 + timedelta(days=i) for i in range(6)]

    # HELD has a distinct, larger weight than the others so avg-of-selected
    # would visibly differ from its own weight.
    sel = {"HELD": 0.50, "ALPHA": 0.10, "BETA": 0.10}
    weights_by_date = {
        days[0]: dict(sel),
        days[1]: dict(sel),
        days[2]: dict(sel),
        days[3]: dict(sel),
        # Day 5+: HELD drops out of selection; ALPHA/BETA remain.
        days[4]: {"ALPHA": 0.10, "BETA": 0.10},
        days[5]: {"ALPHA": 0.10, "BETA": 0.10},
    }

    inner = _StubInnerModel(weights_by_date=weights_by_date)
    model = ConvictionPortfolioModel(
        inner_model=inner,
        conviction_config=ConvictionConfig(),
        conviction_storage=None,
        portfolio_id="BOOK1",
    )

    results = _run_days(model, days[0], weights_by_date)

    # By day 4 HELD is scaled up and holds its full selected weight.
    held_day4 = results[days[3]]["HELD"]
    assert held_day4 > 0

    # Day 5: HELD is no longer selected but kept alive by conviction.
    held_day5 = results[days[4]]["HELD"]
    held_day6 = results[days[5]]["HELD"]

    # It keeps its own last weight, NOT the avg of ALPHA/BETA (0.10).
    # (Day-5 score: entry 5 + 3 builds = 8, one decay -2 → 6, which is
    # outside the default exit_taper_range of 5 → no taper yet.)
    assert abs(held_day5 - held_day4) < 1e-9, (held_day4, held_day5)

    # Day 6: score decays to 4, inside the taper range → the traded
    # weight scales by 4/5 of the (stable) base weight. The BASE stays
    # anchored at held_day4 — no re-averaging drift — the taper is a
    # deliberate glide toward the exit instead of a one-day cliff.
    cfg = ConvictionConfig()
    expected_taper = (8 - 2 * cfg.base_decay_rate - cfg.sell_threshold) / cfg.exit_taper_range
    assert abs(held_day6 - held_day4 * expected_taper) < 1e-9, (held_day6, expected_taper)
    # Explicitly: it is NOT re-sized down to the ~0.10 avg of selected.
    assert held_day5 > 0.20
    assert held_day6 > 0.20
