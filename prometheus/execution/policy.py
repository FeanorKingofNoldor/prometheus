"""Prometheus v2 – Execution policy (targets -> trades).

This module provides a small, configuration-driven *policy layer* that
turns a desired target portfolio (weights) into a more production-safe
execution target (quantities) by applying:

- cash buffer (min cash weight)
- turnover cap (daily aggressiveness)
- no-trade band (ignore tiny weight deltas)
- minimum buy notional (skip dust buys; sells may be exempt)

The goal is to avoid churn/fees and cash failures while keeping the
execution bridge intentionally simple for Iteration 1.

The policy is YAML-backed via `configs/execution/policy.yaml`.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import floor
from pathlib import Path
from typing import Any, Mapping

import yaml

from apathis.core.logging import get_logger
from prometheus.execution.broker_interface import Position


logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXEC_POLICY_PATH = PROJECT_ROOT / "configs" / "execution" / "policy.yaml"


class AccountMode(str, Enum):
    CASH = "CASH"
    MARGIN = "MARGIN"


@dataclass(frozen=True)
class TurnoverPolicy:
    # One-way turnover limit as fraction of equity (e.g. 0.05 => 5%).
    one_way_limit: float = 0.05


@dataclass(frozen=True)
class MinTradeNotionalPolicy:
    buy_min_notional: float = 500.0
    sells_exempt: bool = True


@dataclass(frozen=True)
class CrisisOverrides:
    # If True, do not constrain risk-reducing SELLs by turnover cap.
    turnover_override_sells: bool = True

    # v1 defaults: keep other constraints.
    keep_no_trade_band: bool = True
    keep_buy_min_notional: bool = True
    keep_cash_buffer_weight: bool = True


@dataclass(frozen=True)
class OrderStalenessPolicy:
    """Controls best-effort stale order cancellation before submitting new orders."""

    cancel_stale_orders: bool = False

    # Only consider orders older than this TTL as stale.
    order_ttl_seconds: int = 3600

    # Limit DB scan to recent history for performance.
    lookback_days: int = 7


@dataclass(frozen=True)
class ExecutionPolicy:
    account_mode: AccountMode = AccountMode.CASH
    turnover: TurnoverPolicy = TurnoverPolicy()
    no_trade_band_bps: float = 10.0
    min_trade_notional: MinTradeNotionalPolicy = MinTradeNotionalPolicy()
    cash_buffer_weight: float = 0.10
    crisis: CrisisOverrides = CrisisOverrides()
    order_staleness: OrderStalenessPolicy = OrderStalenessPolicy()


@dataclass(frozen=True)
class ExecutionPolicyArtifact:
    policy: ExecutionPolicy
    version: str | None = None
    updated_at: str | None = None
    updated_by: str | None = None


def _default_policy_artifact() -> ExecutionPolicyArtifact:
    return ExecutionPolicyArtifact(policy=ExecutionPolicy())


def load_execution_policy_artifact(path: str | Path | None = None) -> ExecutionPolicyArtifact:
    """Load execution policy artifact from YAML.

    If the file is missing or malformed, a conservative in-code default is used.
    """

    cfg_path = Path(path) if path is not None else DEFAULT_EXEC_POLICY_PATH
    if not cfg_path.exists():
        logger.debug("Execution policy missing at %s; using defaults", cfg_path)
        return _default_policy_artifact()

    raw = yaml.safe_load(cfg_path.read_text())
    if not isinstance(raw, Mapping):
        logger.warning("Execution policy malformed at %s; using defaults", cfg_path)
        return _default_policy_artifact()

    version_raw = raw.get("version")
    updated_at_raw = raw.get("updated_at")
    updated_by_raw = raw.get("updated_by")

    version = str(version_raw) if isinstance(version_raw, str) and version_raw.strip() else None
    updated_at = (
        str(updated_at_raw) if isinstance(updated_at_raw, str) and updated_at_raw.strip() else None
    )
    updated_by = (
        str(updated_by_raw) if isinstance(updated_by_raw, str) and updated_by_raw.strip() else None
    )

    pol_raw = raw.get("policy")
    if not isinstance(pol_raw, Mapping):
        logger.warning("Execution policy missing 'policy' root at %s; using defaults", cfg_path)
        return ExecutionPolicyArtifact(
            policy=ExecutionPolicy(),
            version=version,
            updated_at=updated_at,
            updated_by=updated_by,
        )

    account_mode_raw = pol_raw.get("account_mode")
    try:
        account_mode = AccountMode(str(account_mode_raw))
    except Exception:
        account_mode = AccountMode.CASH

    turnover_raw = pol_raw.get("turnover")
    one_way_limit = 0.05
    if isinstance(turnover_raw, Mapping):
        one_way_limit = float(_coerce_float(turnover_raw.get("one_way_limit"), 0.05))

    no_trade_band_bps = float(_coerce_float(pol_raw.get("no_trade_band_bps"), 10.0))
    cash_buffer_weight = float(_coerce_float(pol_raw.get("cash_buffer_weight"), 0.10))

    mtn_raw = pol_raw.get("min_trade_notional")
    buy_min_notional = 500.0
    sells_exempt = True
    if isinstance(mtn_raw, Mapping):
        buy_min_notional = float(_coerce_float(mtn_raw.get("buy_min_notional"), 500.0))
        sells_exempt = bool(_coerce_bool(mtn_raw.get("sells_exempt"), True))

    crisis_raw = pol_raw.get("crisis")
    turnover_override_sells = True
    keep_no_trade_band = True
    keep_buy_min_notional = True
    keep_cash_buffer_weight = True
    if isinstance(crisis_raw, Mapping):
        turnover_override_sells = bool(_coerce_bool(crisis_raw.get("turnover_override_sells"), True))
        keep_no_trade_band = bool(_coerce_bool(crisis_raw.get("keep_no_trade_band"), True))
        keep_buy_min_notional = bool(_coerce_bool(crisis_raw.get("keep_buy_min_notional"), True))
        keep_cash_buffer_weight = bool(_coerce_bool(crisis_raw.get("keep_cash_buffer_weight"), True))

    staleness_raw = pol_raw.get("order_staleness")
    cancel_stale_orders = False
    order_ttl_seconds = 3600
    lookback_days = 7
    if isinstance(staleness_raw, Mapping):
        cancel_stale_orders = bool(_coerce_bool(staleness_raw.get("cancel_stale_orders"), False))
        order_ttl_seconds = int(_coerce_float(staleness_raw.get("order_ttl_seconds"), 3600))
        lookback_days = int(_coerce_float(staleness_raw.get("lookback_days"), 7))

    if order_ttl_seconds < 0:
        order_ttl_seconds = 0
    if lookback_days <= 0:
        lookback_days = 1

    policy = ExecutionPolicy(
        account_mode=account_mode,
        turnover=TurnoverPolicy(one_way_limit=one_way_limit),
        no_trade_band_bps=no_trade_band_bps,
        min_trade_notional=MinTradeNotionalPolicy(
            buy_min_notional=buy_min_notional,
            sells_exempt=sells_exempt,
        ),
        cash_buffer_weight=cash_buffer_weight,
        crisis=CrisisOverrides(
            turnover_override_sells=turnover_override_sells,
            keep_no_trade_band=keep_no_trade_band,
            keep_buy_min_notional=keep_buy_min_notional,
            keep_cash_buffer_weight=keep_cash_buffer_weight,
        ),
        order_staleness=OrderStalenessPolicy(
            cancel_stale_orders=cancel_stale_orders,
            order_ttl_seconds=order_ttl_seconds,
            lookback_days=lookback_days,
        ),
    )

    return ExecutionPolicyArtifact(policy=policy, version=version, updated_at=updated_at, updated_by=updated_by)


def load_execution_policy(path: str | Path | None = None) -> ExecutionPolicy:
    """Backwards-compatible convenience wrapper."""

    return load_execution_policy_artifact(path).policy


@dataclass(frozen=True)
class ConstrainedExecutionPlan:
    target_positions: dict[str, float]
    summary: dict[str, object]


def build_constrained_execution_plan(
    *,
    current_positions: Mapping[str, Position],
    target_weights: Mapping[str, float],
    prices: Mapping[str, float],
    equity: float,
    policy: ExecutionPolicy,
    market_situation: str | None = None,
) -> ConstrainedExecutionPlan:
    """Build a constrained execution plan.

    Args:
        current_positions: Current broker positions keyed by instrument_id.
        target_weights: Desired target weights keyed by instrument_id.
        prices: Price map used for valuation and quantity sizing.
        equity: Equity basis for sizing (typically the book notional).
        policy: Parsed execution policy.
        market_situation: Optional label (e.g. "CRISIS") used for overrides.

    Returns:
        ConstrainedExecutionPlan containing desired target quantities and a summary.
    """

    if equity <= 0:
        raise ValueError("equity must be > 0")

    inst_ids = set(current_positions.keys()) | set(target_weights.keys())

    is_crisis = str(market_situation).upper() == "CRISIS" if market_situation is not None else False

    # ------------------------------------------------------------------
    # Current state in weight space
    # ------------------------------------------------------------------

    current_qty: dict[str, float] = {}
    current_w: dict[str, float] = {}

    current_value_sum = 0.0
    for inst in inst_ids:
        pos = current_positions.get(inst)
        qty = float(pos.quantity) if pos is not None else 0.0
        current_qty[inst] = qty

        px = _price(prices, inst)
        if px > 0.0:
            val = qty * px
        else:
            # Best-effort: fall back to broker valuation if available.
            val = float(pos.market_value) if pos is not None else 0.0

        if val < 0.0:
            # v1 is long-only; be conservative if a negative value shows up.
            val = abs(val)

        current_value_sum += val
        current_w[inst] = val / float(equity)

    current_invested = float(sum(current_w.values()))
    current_cash_w = max(0.0, 1.0 - current_invested)

    # ------------------------------------------------------------------
    # Target weights with cash buffer
    # ------------------------------------------------------------------

    cash_buffer = float(policy.cash_buffer_weight)
    if is_crisis and not policy.crisis.keep_cash_buffer_weight:
        cash_buffer = 0.0

    if cash_buffer < 0.0:
        cash_buffer = 0.0
    if cash_buffer > 1.0:
        cash_buffer = 1.0

    invested_limit = 1.0 - cash_buffer

    target_w_raw = {inst: float(target_weights.get(inst, 0.0) or 0.0) for inst in inst_ids}
    target_w_raw = {k: (v if v > 0.0 else 0.0) for k, v in target_w_raw.items()}

    sum_target = float(sum(target_w_raw.values()))
    target_scale = 1.0
    if sum_target > 0.0 and sum_target > invested_limit:
        target_scale = invested_limit / sum_target

    target_w = {k: float(v) * target_scale for k, v in target_w_raw.items()}

    # ------------------------------------------------------------------
    # No-trade band
    # ------------------------------------------------------------------

    no_trade_bps = float(policy.no_trade_band_bps)
    if is_crisis and not policy.crisis.keep_no_trade_band:
        no_trade_bps = 0.0

    no_trade_thresh = abs(no_trade_bps) / 10_000.0

    no_trade_skips = 0
    for inst in inst_ids:
        dw = float(target_w.get(inst, 0.0)) - float(current_w.get(inst, 0.0))
        if abs(dw) < no_trade_thresh:
            # Keep current weight; this prevents micro-churn.
            target_w[inst] = float(current_w.get(inst, 0.0))
            no_trade_skips += 1

    # ------------------------------------------------------------------
    # Turnover-limited step towards target
    # ------------------------------------------------------------------

    dw_by_inst = {inst: float(target_w.get(inst, 0.0)) - float(current_w.get(inst, 0.0)) for inst in inst_ids}

    buy_frac_1 = float(sum(max(dw, 0.0) for dw in dw_by_inst.values()))
    sell_frac_1 = float(sum(max(-dw, 0.0) for dw in dw_by_inst.values()))

    turnover_cap = float(policy.turnover.one_way_limit)
    if turnover_cap < 0.0:
        turnover_cap = 0.0

    alpha_buy = 1.0
    alpha_sell = 1.0

    if turnover_cap > 0.0:
        if is_crisis and policy.crisis.turnover_override_sells:
            # Allow sells to fully de-risk, but cap buys.
            if buy_frac_1 > 0.0:
                alpha_buy = min(1.0, turnover_cap / buy_frac_1)
            else:
                alpha_buy = 0.0
            alpha_sell = 1.0
        else:
            denom = max(buy_frac_1, sell_frac_1)
            if denom > 0.0:
                alpha = min(1.0, turnover_cap / denom)
            else:
                alpha = 0.0
            alpha_buy = alpha
            alpha_sell = alpha

    exec_w: dict[str, float] = {}
    for inst, dw in dw_by_inst.items():
        if dw > 0.0:
            w = float(current_w.get(inst, 0.0)) + alpha_buy * dw
        elif dw < 0.0:
            w = float(current_w.get(inst, 0.0)) + alpha_sell * dw
        else:
            w = float(current_w.get(inst, 0.0))

        if w < 0.0:
            w = 0.0
        exec_w[inst] = w

    # ------------------------------------------------------------------
    # Convert to integer quantities
    # ------------------------------------------------------------------

    target_qty: dict[str, float] = {}
    for inst in inst_ids:
        px = _price(prices, inst)
        curr_q = float(current_qty.get(inst, 0.0))

        if px <= 0.0:
            # If we cannot price the instrument, avoid trading it.
            target_qty[inst] = curr_q
            continue

        desired_value = float(exec_w.get(inst, 0.0)) * float(equity)
        desired_shares = floor(desired_value / px)
        if desired_shares < 0:
            desired_shares = 0
        target_qty[inst] = float(desired_shares)

    # ------------------------------------------------------------------
    # Min BUY notional filter (SELLs may be exempt)
    # ------------------------------------------------------------------

    buy_min_notional = float(policy.min_trade_notional.buy_min_notional)
    sells_exempt = bool(policy.min_trade_notional.sells_exempt)

    if is_crisis and not policy.crisis.keep_buy_min_notional:
        buy_min_notional = 0.0

    skipped_small_buys = 0

    for inst in inst_ids:
        px = _price(prices, inst)
        if px <= 0.0:
            continue

        curr_q = float(current_qty.get(inst, 0.0))
        tgt_q = float(target_qty.get(inst, 0.0))
        delta_q = tgt_q - curr_q

        if delta_q > 0.0:
            # BUY
            if buy_min_notional > 0.0 and (delta_q * px) < buy_min_notional:
                target_qty[inst] = curr_q
                skipped_small_buys += 1
        elif delta_q < 0.0:
            # SELL
            if not sells_exempt:
                # If sells are not exempt, apply the same min-notional filter.
                if buy_min_notional > 0.0 and (abs(delta_q) * px) < buy_min_notional:
                    target_qty[inst] = curr_q

    # ------------------------------------------------------------------
    # Summary diagnostics
    # ------------------------------------------------------------------

    buy_notional = 0.0
    sell_notional = 0.0

    for inst in inst_ids:
        px = _price(prices, inst)
        if px <= 0.0:
            continue
        dq = float(target_qty.get(inst, 0.0)) - float(current_qty.get(inst, 0.0))
        if dq > 0.0:
            buy_notional += dq * px
        elif dq < 0.0:
            sell_notional += abs(dq) * px

    summary: dict[str, object] = {
        "market_situation": str(market_situation) if market_situation is not None else None,
        "equity_basis": float(equity),
        "current_value_est": float(current_value_sum),
        "current_invested_weight_est": float(current_invested),
        "current_cash_weight_est": float(current_cash_w),
        "cash_buffer_weight": float(cash_buffer),
        "invested_limit": float(invested_limit),
        "target_weights_raw_sum": float(sum_target),
        "target_weights_scale": float(target_scale),
        "no_trade_band_bps": float(no_trade_bps),
        "no_trade_skips_count": int(no_trade_skips),
        "min_buy_trade_notional": float(buy_min_notional),
        "sells_exempt_min_notional": bool(sells_exempt),
        "skipped_small_buys_count": int(skipped_small_buys),
        "turnover_one_way_limit": float(turnover_cap),
        "alpha_buy": float(alpha_buy),
        "alpha_sell": float(alpha_sell),
        "buy_fraction_raw": float(buy_frac_1),
        "sell_fraction_raw": float(sell_frac_1),
        "buy_notional_est": float(buy_notional),
        "sell_notional_est": float(sell_notional),
        "one_way_turnover_est": float(max(buy_notional, sell_notional) / float(equity)),
    }

    return ConstrainedExecutionPlan(target_positions=target_qty, summary=summary)


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "no", "n", "off"}:
            return False
    return bool(default)


def _price(prices: Mapping[str, float], instrument_id: str) -> float:
    try:
        px = float(prices.get(instrument_id) or 0.0)
    except Exception:
        return 0.0
    return px if px > 0.0 else 0.0
