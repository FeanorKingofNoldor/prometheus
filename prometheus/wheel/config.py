"""Loader for the core+wheel strategy configuration.

Single source of truth is ``configs/wheel/strategy.yaml`` — the validated
2026-08 spec (allocation split, VIXCOND wheel parameters, execution
style, PRIIPs live substitutions). This module parses it into frozen
dataclasses so the runner and tests share one config surface.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import yaml

from prometheus.wheel.engine import WheelParams

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "wheel" / "strategy.yaml"


@dataclass(frozen=True)
class BallastLeg:
    """One static ballast sleeve (e.g. TLT.US at 10% of NAV)."""

    instrument_id: str
    weight: float


@dataclass(frozen=True)
class WheelStrategyConfig:
    """Parsed configs/wheel/strategy.yaml."""

    params: WheelParams
    underlying_instrument_id: str            # "SPY.US"
    wheel_allocation: float                  # fraction of NAV for the wheel
    ballast: tuple[BallastLeg, ...]
    rebalance: str                           # "quarterly"
    live_substitutions: Mapping[str, str] = field(default_factory=dict)
    order_style: str = "limit_at_mid"
    limit_walk_bps: float = 25.0             # max concession from mid, bps of spot
    max_contracts_per_day: int = 10
    portfolio_id: str = "US_WHEEL"

    @property
    def underlying_symbol(self) -> str:
        """Bare IB symbol for the wheel underlying ("SPY")."""
        return self.underlying_instrument_id.split(".")[0]

    def ballast_instrument(self, instrument_id: str, *, live: bool) -> str:
        """Resolve a ballast leg to its tradeable instrument.

        Live EU-retail accounts cannot buy US ETFs (PRIIPs); the YAML maps
        each to a UCITS twin. Paper trades the US originals so fills match
        the backtests.
        """
        if live and instrument_id in self.live_substitutions:
            return self.live_substitutions[instrument_id]
        return instrument_id


def load_wheel_config(path: Path | None = None) -> WheelStrategyConfig:
    raw: dict[str, Any] = yaml.safe_load((path or CONFIG_PATH).read_text())

    wheel = raw.get("wheel", {})
    params = WheelParams(
        target_dte_days=int(wheel.get("target_dte_days", 30)),
        put_otm=float(wheel.get("put_otm", 0.02)),
        put_otm_rich=float(wheel.get("put_otm_rich", 0.05)),
        call_otm=float(wheel.get("call_otm", 0.08)),
        vix_rich_threshold=float(wheel.get("vix_rich_threshold", 25.0)),
        vix_dead_threshold=float(wheel.get("vix_dead_threshold", 13.0)),
        profit_take_fraction=float(wheel.get("profit_take_fraction", 0.50)),
    )

    allocation = raw.get("allocation", {})
    ballast_map: Mapping[str, Any] = allocation.get("ballast", {}) or {}
    ballast = tuple(
        BallastLeg(instrument_id=str(iid), weight=float(w))
        for iid, w in ballast_map.items()
    )

    wheel_alloc = float(allocation.get("wheel", 0.80))
    total = wheel_alloc + sum(leg.weight for leg in ballast)
    if not 0.99 <= total <= 1.01:
        raise ValueError(
            f"wheel+ballast allocation must sum to 1.0, got {total:.4f}"
        )

    execution = raw.get("execution", {})
    return WheelStrategyConfig(
        params=params,
        underlying_instrument_id=str(wheel.get("underlying", "SPY.US")),
        wheel_allocation=wheel_alloc,
        ballast=ballast,
        rebalance=str(allocation.get("rebalance", "quarterly")),
        live_substitutions=dict(raw.get("live_substitutions", {}) or {}),
        order_style=str(execution.get("order_style", "limit_at_mid")),
        limit_walk_bps=float(execution.get("limit_walk_bps", 25)),
        max_contracts_per_day=int(execution.get("max_contracts_per_day", 10)),
    )


__all__ = ["BallastLeg", "WheelStrategyConfig", "load_wheel_config", "CONFIG_PATH"]
