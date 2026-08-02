"""Core+wheel strategy package.

The validated 2026-08 strategy: cash-secured-put / covered-call wheel on a
liquid index underlying, with VIX-conditional parameters, plus static ETF
ballast sleeves. Evidence: prometheus/scripts/research/wheel_validation/.

`engine` is pure decision logic (state in, order intents out — no I/O), so
the whole rule set is unit-testable without a broker or database.
"""

from prometheus.wheel.engine import (
    BlockPhase,
    BlockState,
    MarketInputs,
    OrderIntent,
    WheelParams,
    decide_block,
)

__all__ = [
    "BlockPhase",
    "BlockState",
    "MarketInputs",
    "OrderIntent",
    "WheelParams",
    "decide_block",
]
