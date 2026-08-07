"""Loader for the core+wheel strategy configuration.

Single source of truth is ``configs/wheel/strategy.yaml`` — the validated
2026-08 spec (allocation split, VIXCOND wheel parameters, execution
style, PRIIPs ballast substitutions). This module parses it into frozen
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
class BallastSubstitute:
    """UCITS twin that replaces a PRIIPs-blocked US ETF at the broker.

    The account is EU-retail (IBKR Ireland) in BOTH paper and live —
    confirmed 2026-08-07 when paper rejected TLT/GLD buys with "No
    Trading Permission, Customer Ineligible" (missing KID). Substitution
    therefore applies in every mode, and the exchange/currency must be
    carried explicitly because SMART routing does not resolve these
    tickers (they live on LSEETF).
    """

    instrument_id: str   # canonical Prometheus id, e.g. "DTLA.LSE"
    exchange: str        # IB exchange, e.g. "LSEETF"
    currency: str        # quote currency — only USD lines supported

    @property
    def symbol(self) -> str:
        return self.instrument_id.split(".")[0]


@dataclass(frozen=True)
class WheelStrategyConfig:
    """Parsed configs/wheel/strategy.yaml."""

    params: WheelParams
    underlying_instrument_id: str            # "SPY.US"
    wheel_allocation: float                  # fraction of NAV for the wheel
    ballast: tuple[BallastLeg, ...]
    rebalance: str                           # "quarterly"
    ballast_substitutions: Mapping[str, BallastSubstitute] = field(default_factory=dict)
    order_style: str = "limit_at_mid"
    limit_walk_bps: float = 25.0             # max concession from mid, bps of spot
    max_contracts_per_day: int = 10
    portfolio_id: str = "US_WHEEL"

    @property
    def underlying_symbol(self) -> str:
        """Bare IB symbol for the wheel underlying ("SPY")."""
        return self.underlying_instrument_id.split(".")[0]

    def ballast_substitute(self, instrument_id: str) -> BallastSubstitute | None:
        """UCITS twin for a ballast leg, or None if it trades directly."""
        return self.ballast_substitutions.get(instrument_id)

    @property
    def ballast_symbol_map(self) -> dict[str, str]:
        """IB symbol → canonical ballast leg id, covering originals AND twins.

        Used to fold broker positions back onto the planner's canonical
        legs (a DTLA position counts toward TLT.US's sleeve).
        """
        out: dict[str, str] = {}
        for leg in self.ballast:
            out[leg.instrument_id.split(".")[0]] = leg.instrument_id
            sub = self.ballast_substitutions.get(leg.instrument_id)
            if sub is not None:
                out[sub.symbol] = leg.instrument_id
        return out


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

    substitutions: dict[str, BallastSubstitute] = {}
    for iid, spec in (raw.get("ballast_substitutions", {}) or {}).items():
        sub = BallastSubstitute(
            instrument_id=str(spec["instrument"]),
            exchange=str(spec["exchange"]),
            currency=str(spec["currency"]).upper(),
        )
        if sub.currency != "USD":
            raise ValueError(
                f"ballast substitute {sub.instrument_id} is {sub.currency}-"
                "denominated — only USD lines are supported (planner sizing "
                "and account values are USD; pick the USD listing)"
            )
        substitutions[str(iid)] = sub

    execution = raw.get("execution", {})
    return WheelStrategyConfig(
        params=params,
        underlying_instrument_id=str(wheel.get("underlying", "SPY.US")),
        wheel_allocation=wheel_alloc,
        ballast=ballast,
        rebalance=str(allocation.get("rebalance", "quarterly")),
        ballast_substitutions=substitutions,
        order_style=str(execution.get("order_style", "limit_at_mid")),
        limit_walk_bps=float(execution.get("limit_walk_bps", 25)),
        max_contracts_per_day=int(execution.get("max_contracts_per_day", 10)),
    )


__all__ = [
    "BallastLeg",
    "BallastSubstitute",
    "WheelStrategyConfig",
    "load_wheel_config",
    "CONFIG_PATH",
]
