"""Sleeve budget planner + cutover state.

The legacy ``StrategyAllocator`` slices the 30%-NAV derivatives budget
across five categories (DIRECTIONAL / INCOME / HEDGE / VOLATILITY /
FUTURES) and drives the seventeen strategy classes. The new design
replaces that with three explicit sleeves (HEDGE 10% / INCOME 15% /
CONVEX 5%) — see ``prometheus.derivatives.sleeves``.

Two states are operationally distinct:

* **Shadow** — both pipelines run; legacy submits orders, the new
  sleeve runner only logs to ``derivatives_shadow_decisions``. The
  legacy category budgets are unchanged.
* **Cutover** — for a given sleeve, the legacy strategies it replaces
  are silenced and the new pipeline drives execution. The legacy
  category budgets for the absorbed categories are zeroed so we don't
  double-allocate capital.

The planner reads the cutover state (from env or explicit config),
produces:

* per-sleeve dollar budgets (always — used by the runner and for
  audit), and
* per-legacy-category multipliers (1.0 = legacy still owns this; 0.0
  = a cut-over sleeve has absorbed it).

The submission path multiplies the legacy allocator's per-category
budgets by these multipliers, so a sleeve cutover is one env var.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from apatheon.core.logging import get_logger

from prometheus.derivatives.sleeves import Sleeve, SleeveConfig, default_sleeves

logger = get_logger(__name__)


# ── Sleeve → legacy categories mapping ───────────────────────────────
#
# When a sleeve is cut over, the legacy categories listed here are
# zeroed out — the new sleeve runner is now driving them. CONVEX is
# intentionally empty: it adds Apatheon-signal-driven convex bets
# that didn't exist in the legacy system. (Legacy DIRECTIONAL —
# bull_call_spread / momentum_call / LEAPS — is slated for deletion
# in Phase 5 rather than absorption.)
SLEEVE_REPLACES_LEGACY_CATEGORIES: Mapping[Sleeve, tuple[str, ...]] = {
    Sleeve.HEDGE: ("HEDGE",),
    Sleeve.INCOME: ("INCOME", "VOLATILITY"),
    Sleeve.CONVEX: (),
}


# Per-strategy silencing map — finer-grained than the category map.
# When a sleeve is cut over, every directive emitted by these legacy
# strategy classes is dropped from the submission pipeline (the new
# sleeve runner is driving instead).
SLEEVE_REPLACES_LEGACY_STRATEGIES: Mapping[Sleeve, frozenset[str]] = {
    Sleeve.HEDGE: frozenset({
        "protective_put", "sector_put_spread", "vix_tail_hedge",
        "collar", "crisis_alpha",
    }),
    Sleeve.INCOME: frozenset({
        "covered_call", "iron_condor", "iron_butterfly", "short_put",
        "wheel", "calendar_spread", "straddle_strangle",
    }),
    Sleeve.CONVEX: frozenset(),
}


def silenced_strategies(
    cutover_state: SleeveCutoverState,
    mapping: Mapping[Sleeve, frozenset[str]] | None = None,
) -> frozenset[str]:
    """Union of legacy strategy names silenced by the cutover state."""
    m = mapping or SLEEVE_REPLACES_LEGACY_STRATEGIES
    out: set[str] = set()
    for sleeve, names in m.items():
        if cutover_state.is_active(sleeve):
            out.update(names)
    return frozenset(out)


# ── Cutover state ────────────────────────────────────────────────────


@dataclass(frozen=True)
class SleeveCutoverState:
    """Per-sleeve cutover flag.

    True = the new pipeline drives this sleeve in production; the
    legacy strategies it replaces are silenced.
    False = the new pipeline runs in shadow only; legacy is
    authoritative.
    """

    hedge: bool = False
    income: bool = False
    convex: bool = False

    def is_active(self, sleeve: Sleeve) -> bool:
        if sleeve == Sleeve.HEDGE:
            return self.hedge
        if sleeve == Sleeve.INCOME:
            return self.income
        if sleeve == Sleeve.CONVEX:
            return self.convex
        return False

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SleeveCutoverState:
        """Read cutover state from environment variables.

        Per-sleeve flags:

        * ``PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER``
        * ``PROMETHEUS_DERIVATIVES_INCOME_CUTOVER``
        * ``PROMETHEUS_DERIVATIVES_CONVEX_CUTOVER``

        Truthy values: ``1 / true / yes / on`` (case-insensitive).
        """
        env = env if env is not None else os.environ
        return cls(
            hedge=_truthy(env.get("PROMETHEUS_DERIVATIVES_HEDGE_CUTOVER", "")),
            income=_truthy(env.get("PROMETHEUS_DERIVATIVES_INCOME_CUTOVER", "")),
            convex=_truthy(env.get("PROMETHEUS_DERIVATIVES_CONVEX_CUTOVER", "")),
        )


def _truthy(s: str) -> bool:
    return str(s or "").strip().lower() in ("1", "true", "yes", "on")


# ── Plan output ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class SleeveBudgetPlan:
    """Output of the planner.

    * ``sleeve_budgets`` — per-sleeve dollar budget. Always populated
      regardless of cutover state (the runner uses these in shadow
      mode too).
    * ``legacy_category_multipliers`` — multiplier to apply to each
      legacy category's existing budget. 1.0 = pass through; 0.0 =
      zeroed because a cut-over sleeve owns it.
    * ``active_sleeves`` — sleeves that are *in cutover* (driving real
      orders). For audit/log purposes.
    * ``cutover_state`` — the input state, kept for traceability.
    """

    sleeve_budgets: Mapping[Sleeve, float]
    legacy_category_multipliers: Mapping[str, float]
    active_sleeves: frozenset[Sleeve]
    cutover_state: SleeveCutoverState

    def legacy_multiplier(self, category: str) -> float:
        return self.legacy_category_multipliers.get(category.upper(), 1.0)

    def adjusted_legacy_budget(
        self, category: str, legacy_budget_usd: float,
    ) -> float:
        return legacy_budget_usd * self.legacy_multiplier(category)


# ── Planner ──────────────────────────────────────────────────────────


# Every legacy category the planner knows about. Anything not in this
# set (e.g. FUTURES) keeps a multiplier of 1.0 by default.
_KNOWN_LEGACY_CATEGORIES: tuple[str, ...] = (
    "DIRECTIONAL", "INCOME", "HEDGE", "VOLATILITY", "FUTURES",
)


class SleeveBudgetPlanner:
    """Compute sleeve budgets + legacy multipliers given cutover state."""

    def __init__(
        self,
        sleeves_cfg: Mapping[Sleeve, SleeveConfig] | None = None,
        sleeve_replaces: Mapping[Sleeve, tuple[str, ...]] | None = None,
    ) -> None:
        self._sleeves_cfg = sleeves_cfg or default_sleeves()
        self._replaces = sleeve_replaces or SLEEVE_REPLACES_LEGACY_CATEGORIES

    def plan(
        self,
        *,
        nav: float,
        cutover_state: SleeveCutoverState | None = None,
    ) -> SleeveBudgetPlan:
        nav = max(float(nav), 0.0)
        cutover = cutover_state or SleeveCutoverState()

        # Sleeve budgets — always informational, even in shadow mode.
        sleeve_budgets: dict[Sleeve, float] = {
            sleeve: cfg.budget(nav) for sleeve, cfg in self._sleeves_cfg.items()
        }

        # Legacy multipliers — start permissive, zero out absorbed
        # categories for any sleeve in cutover.
        multipliers: dict[str, float] = {
            cat: 1.0 for cat in _KNOWN_LEGACY_CATEGORIES
        }
        active: set[Sleeve] = set()
        for sleeve in self._sleeves_cfg.keys():
            if not cutover.is_active(sleeve):
                continue
            active.add(sleeve)
            for cat in self._replaces.get(sleeve, ()):
                multipliers[cat.upper()] = 0.0

        return SleeveBudgetPlan(
            sleeve_budgets=sleeve_budgets,
            legacy_category_multipliers=multipliers,
            active_sleeves=frozenset(active),
            cutover_state=cutover,
        )


__all__ = [
    "SleeveCutoverState",
    "SleeveBudgetPlan",
    "SleeveBudgetPlanner",
    "SLEEVE_REPLACES_LEGACY_CATEGORIES",
    "SLEEVE_REPLACES_LEGACY_STRATEGIES",
    "silenced_strategies",
]
