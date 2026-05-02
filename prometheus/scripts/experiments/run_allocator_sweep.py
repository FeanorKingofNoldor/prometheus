"""Parameter sweep for allocator sleeve settings over key stress windows.

Runs prom2_cpp allocator backtests with in-memory config overrides (no DB
persistence) across a small grid and reports metrics for each window.

Usage (example):
  PYTHONPATH=cpp/build ./venv/bin/python -m prometheus.scripts.experiments.run_allocator_sweep
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Sequence, Tuple

from apatheon.core.logging import get_logger

from prometheus.books.registry import AllocatorSleeveSpec, load_book_registry

try:
    import prom2_cpp  # type: ignore
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "prom2_cpp not available. Build it (./cpp/scripts/build.sh) and run with PYTHONPATH=cpp/build"
    ) from exc


logger = get_logger(__name__)


@dataclass
class Window:
    name: str
    start: date
    end: date


def build_cfg(
    base_sleeve: AllocatorSleeveSpec,
    overrides: Dict[str, float],
    book_id: str,
    window: Window,
) -> dict:
    sleeve = {
        "sleeve_id": str(base_sleeve.sleeve_id),
        "universe_max_size": 200,
        "portfolio_max_names": int(base_sleeve.portfolio_max_names or 20),
        "portfolio_hysteresis_buffer": int(base_sleeve.portfolio_hysteresis_buffer or 5),
        "portfolio_per_instrument_max_weight": float(base_sleeve.portfolio_per_instrument_max_weight or 0.05),
        "hedge_instrument_ids": list(base_sleeve.hedge_instrument_ids),
        "hedge_sizing_mode": str(base_sleeve.hedge_sizing_mode),
        "fragility_threshold": float(overrides["fragility_threshold"]),
        "max_hedge_allocation": float(base_sleeve.max_hedge_allocation or 1.0),
        "hedge_allocation_overrides": dict(base_sleeve.hedge_allocation_overrides or {}),
        "hedge_allocation_floors": {
            "RISK_OFF": overrides["floor_risk_off"],
            "RECOVERY": overrides["floor_recovery"],
        },
        "hedge_allocation_caps": dict(base_sleeve.hedge_allocation_caps or {}),
        "profitability_weight": float(base_sleeve.profitability_weight or 0.0),
    }

    cfg = {
        "market_id": "US_EQ",
        "regime_region": "US",
        "base_prefix": book_id,
        "start": window.start.isoformat(),
        "end": window.end.isoformat(),
        "sleeves": [sleeve],
        "num_threads": 8,
        "persist_to_db": False,
        "persist_meta_to_db": False,
        # switching / rails
        "situation_sleeve_map": {},  # force single sleeve per run to isolate hedge/fragility effects
        "sleeve_transition_days": overrides["transition_days"],
        "max_turnover_one_way": 0.25,
        "crisis_force_hedge_allocation": 1.0,
        "drawdown_brake_threshold": -0.10,
        "drawdown_brake_hedge_allocation": 1.0,
        "vol_target_annual": overrides["vol_target"],
        "vol_target_lookback_days": 21,
    }
    return cfg


def main() -> None:
    registry = load_book_registry()
    book = registry["US_EQ_ALLOCATOR"]
    base_sleeve: AllocatorSleeveSpec = book.sleeves[book.default_sleeve_id]

    windows: Sequence[Window] = [
        Window("GFC", date(2006, 1, 3), date(2010, 12, 31)),
        Window("COVID", date(2020, 1, 2), date(2021, 12, 31)),
        Window("BULL_2013_2019", date(2013, 1, 2), date(2019, 12, 31)),
        Window("RECENT_2022_2025", date(2022, 1, 3), date(2025, 12, 31)),
    ]

    grid: List[Dict[str, float]] = []
    for frag in [0.20, 0.25, 0.30]:
        for floor_ro in [0.50, 0.70]:
            for floor_rec in [0.30, 0.50]:
                for trans_days in [2, 4]:
                    for vol in [0.18, 0.22]:
                        grid.append(
                            {
                                "fragility_threshold": frag,
                                "floor_risk_off": floor_ro,
                                "floor_recovery": floor_rec,
                                "transition_days": trans_days,
                                "vol_target": vol,
                            }
                        )

    rows: List[Tuple] = []
    for w in windows:
        for params in grid:
            cfg = build_cfg(base_sleeve, params, "US_EQ_ALLOCATOR", w)
            res = prom2_cpp.run_allocator_backtests(cfg)
            if not res:
                continue
            r0 = res[0]
            m = r0.get("metrics", {})
            rows.append(
                (
                    w.name,
                    params["fragility_threshold"],
                    params["floor_risk_off"],
                    params["floor_recovery"],
                    params["transition_days"],
                    params["vol_target"],
                    m.get("cumulative_return"),
                    m.get("max_drawdown"),
                    m.get("annualised_sharpe"),
                    m.get("annualised_vol"),
                    m.get("n_trading_days"),
                )
            )
            print(
                w.name,
                params,
                "ret",
                m.get("cumulative_return"),
                "dd",
                m.get("max_drawdown"),
                "sharpe",
                m.get("annualised_sharpe"),
            )

    # Sort by Sharpe within each window and print top 5
    from collections import defaultdict

    by_win = defaultdict(list)
    for r in rows:
        by_win[r[0]].append(r)

    print("\\nTop configs per window (by Sharpe):")
    for w in windows:
        best = sorted(by_win[w.name], key=lambda x: x[8] or -1e9, reverse=True)[:5]
        for b in best:
            print(b)


if __name__ == "__main__":  # pragma: no cover
    main()
