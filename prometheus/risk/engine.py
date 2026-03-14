"""Prometheus v2 – Core Risk Engine.

The Risk Engine applies simple constraints to a list of candidate
decisions. For this iteration it focuses on per-name weight caps;
portfolio- and exposure-level constraints can be added later.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Tuple

from prometheus.risk.constraints import StrategyRiskConfig, apply_per_name_limit


class RiskActionType(str, Enum):
    """Enumerated types of risk actions applied to a decision.

    The core BOOKS pipeline uses ``OK``, ``CAPPED``, and ``REJECTED``
    for per-name risk constraints. Execution-time risk wrappers may
    emit ``EXECUTION_REJECT`` to indicate that an order was blocked at
    the broker layer (e.g. by :class:`RiskCheckingBroker`).
    """

    OK = "OK"
    CAPPED = "CAPPED"
    REJECTED = "REJECTED"
    EXECUTION_REJECT = "EXECUTION_REJECT"


@dataclass(frozen=True)
class RiskResult:
    """Result of applying risk constraints to a single decision."""

    instrument_id: str | None
    original_weight: float
    adjusted_weight: float
    action_type: RiskActionType
    reason: str | None


def apply_risk_to_decision(
    decision: Dict[str, Any],
    config: StrategyRiskConfig,
) -> Tuple[Dict[str, Any], RiskResult]:
    """Apply basic risk constraints to a single decision dict.

    Expected decision fields (soft contract)::

        {
            "instrument_id": "AAPL.US",
            "target_weight": 0.10,
            ...
        }

    Unknown fields are preserved. If ``target_weight`` is missing, a
    default of 0.0 is assumed and the decision is passed through.
    """

    instrument_id = decision.get("instrument_id")
    original_weight = float(decision.get("target_weight", 0.0))

    adjusted_weight, reason = apply_per_name_limit(original_weight, config)

    if reason is None:
        action_type = RiskActionType.OK
    elif reason == "REJECTED_PER_NAME_CAP":
        action_type = RiskActionType.REJECTED
    else:
        action_type = RiskActionType.CAPPED

    updated = dict(decision)
    updated["target_weight"] = adjusted_weight

    # Human-readable summary for downstream inspection/logging.
    if action_type is RiskActionType.OK:
        summary = "OK: within per-name risk limits"
    elif action_type is RiskActionType.REJECTED:
        summary = (
            f"REJECTED_PER_NAME_CAP: proposed weight {original_weight:.6f} "
            f"exceeds absolute cap {config.max_abs_weight_per_name:.6f}"
        )
    else:
        summary = (
            f"CAPPED_PER_NAME: proposed weight {original_weight:.6f} "
            f"capped to {adjusted_weight:.6f} (abs cap {config.max_abs_weight_per_name:.6f})"
        )

    updated["risk_action_type"] = action_type.value
    updated["risk_reasoning_summary"] = summary

    result = RiskResult(
        instrument_id=str(instrument_id) if instrument_id is not None else None,
        original_weight=original_weight,
        adjusted_weight=adjusted_weight,
        action_type=action_type,
        reason=reason,
    )

    return updated, result
