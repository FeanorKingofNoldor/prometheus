"""Prometheus v2 – Regime-Adaptive Strategy Allocator.

The brain that decides WHICH derivative strategies to deploy and HOW MUCH
capital each receives, based on regime, vol environment, conviction signals,
and portfolio-level greeks budget.

Usage::

    from prometheus.execution.strategy_allocator import StrategyAllocator

    allocator = StrategyAllocator()
    allocations = allocator.allocate(
        market_situation="RISK_ON",
        signals=signals,
        portfolio_greeks=greeks,
        existing_positions=positions,
    )

    # allocations is a dict of strategy_name → AllocationDirective
    for name, alloc in allocations.items():
        if alloc.enabled:
            strategy.config.enabled = True
            ...
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Optional, Set

from apathis.core.logging import get_logger

logger = get_logger(__name__)


# ── Strategy categories ──────────────────────────────────────────────

class StrategyCategory(str, Enum):
    """Logical grouping for capital budgeting."""
    DIRECTIONAL = "DIRECTIONAL"     # Bull call spreads, LEAPS
    INCOME = "INCOME"               # Iron condor, butterfly, covered call, short put, wheel
    HEDGE = "HEDGE"                 # Protective put, collar, sector put spread, VIX tail
    VOLATILITY = "VOLATILITY"       # Straddle/strangle, calendar spread
    FUTURES = "FUTURES"             # Futures overlay, futures options


STRATEGY_CATEGORIES: Dict[str, StrategyCategory] = {
    "protective_put": StrategyCategory.HEDGE,
    "covered_call": StrategyCategory.INCOME,
    "sector_put_spread": StrategyCategory.HEDGE,
    "vix_tail_hedge": StrategyCategory.HEDGE,
    "short_put": StrategyCategory.INCOME,
    "futures_overlay": StrategyCategory.FUTURES,
    "futures_option": StrategyCategory.FUTURES,
    "bull_call_spread": StrategyCategory.DIRECTIONAL,
    "momentum_call": StrategyCategory.DIRECTIONAL,
    "leaps": StrategyCategory.DIRECTIONAL,
    "iron_condor": StrategyCategory.INCOME,
    "iron_butterfly": StrategyCategory.INCOME,
    "collar": StrategyCategory.HEDGE,
    "calendar_spread": StrategyCategory.VOLATILITY,
    "straddle_strangle": StrategyCategory.VOLATILITY,
    "wheel": StrategyCategory.INCOME,
}


# ── Allocation output ────────────────────────────────────────────────

@dataclass
class GreeksBudget:
    """Per-strategy greeks budget."""
    max_delta: float = 0.0       # Max net delta contribution (share-equivalents)
    max_gamma: float = 0.0       # Max gamma
    min_theta: float = 0.0       # Min theta (daily, typically negative)
    max_vega: float = 0.0        # Max vega


@dataclass
class AllocationDirective:
    """What the allocator decides for a single strategy."""
    enabled: bool = False
    capital_pct: float = 0.0      # % of total derivatives budget
    priority: int = 0             # Higher = runs first
    greeks_budget: GreeksBudget = field(default_factory=GreeksBudget)
    reason: str = ""


# ── Regime → Strategy mapping ────────────────────────────────────────

# Which strategies are allowed in each market situation.
# Strategies not listed for a situation are disabled.
REGIME_STRATEGY_MAP: Dict[str, Set[str]] = {
    "RISK_ON": {
        "bull_call_spread", "momentum_call", "leaps", "covered_call", "short_put",
        "wheel", "iron_condor", "iron_butterfly", "vix_tail_hedge",
    },
    "NEUTRAL": {
        "covered_call", "short_put", "iron_condor", "iron_butterfly",
        "calendar_spread", "wheel", "vix_tail_hedge",
    },
    "RECOVERY": {
        "collar", "protective_put", "covered_call", "short_put",
        "straddle_strangle", "vix_tail_hedge", "futures_overlay",
    },
    "RISK_OFF": {
        "protective_put", "sector_put_spread", "vix_tail_hedge",
        "collar", "futures_overlay", "futures_option",
    },
    "CRISIS": {
        "protective_put", "vix_tail_hedge", "futures_overlay",
        "futures_option",
    },
}

# Capital allocation templates: category → fraction of total derivatives budget.
# Must sum to ≤ 1.0 for each regime.
REGIME_CAPITAL_TEMPLATES: Dict[str, Dict[str, float]] = {
    "RISK_ON": {
        "DIRECTIONAL": 0.35,
        "INCOME": 0.40,
        "HEDGE": 0.10,
        "VOLATILITY": 0.05,
        "FUTURES": 0.10,
    },
    "NEUTRAL": {
        "DIRECTIONAL": 0.05,
        "INCOME": 0.50,
        "HEDGE": 0.15,
        "VOLATILITY": 0.15,
        "FUTURES": 0.15,
    },
    "RECOVERY": {
        "DIRECTIONAL": 0.00,
        "INCOME": 0.15,
        "HEDGE": 0.45,
        "VOLATILITY": 0.15,
        "FUTURES": 0.25,
    },
    "RISK_OFF": {
        "DIRECTIONAL": 0.00,
        "INCOME": 0.00,
        "HEDGE": 0.50,
        "VOLATILITY": 0.00,
        "FUTURES": 0.50,
    },
    "CRISIS": {
        "DIRECTIONAL": 0.00,
        "INCOME": 0.00,
        "HEDGE": 0.60,
        "VOLATILITY": 0.00,
        "FUTURES": 0.40,
    },
}


# ── Allocator config ─────────────────────────────────────────────────

@dataclass
class StrategyAllocatorConfig:
    """Configuration for the strategy allocator."""
    # Total derivatives capital as fraction of NAV
    total_derivatives_budget_pct: float = 0.15  # 15% of NAV

    # Portfolio-level greeks limits
    max_portfolio_delta_pct: float = 0.20    # Max |delta| as % of NAV
    max_portfolio_gamma: float = 50_000.0
    min_portfolio_theta: float = -10_000.0   # Minimum daily theta (dollars)
    max_portfolio_vega: float = 100_000.0

    # Override: always-on strategies regardless of regime
    always_on_strategies: FrozenSet[str] = frozenset({"vix_tail_hedge"})

    # Straddle/strangle is vol-driven, not regime-driven
    vol_strategy_max_vix: float = 18.0
    vol_strategy_min_frag: float = 0.35


# ── Allocator ────────────────────────────────────────────────────────

class StrategyAllocator:
    """Decide which strategies run and how much capital each gets.

    Parameters
    ----------
    config : StrategyAllocatorConfig, optional
        Override default thresholds and budget percentages.
    regime_map : dict, optional
        Override the default regime → strategy mapping.
    capital_templates : dict, optional
        Override the default regime → category capital allocation.
    """

    def __init__(
        self,
        config: Optional[StrategyAllocatorConfig] = None,
        regime_map: Optional[Dict[str, Set[str]]] = None,
        capital_templates: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> None:
        self._config = config or StrategyAllocatorConfig()
        self._regime_map = regime_map or REGIME_STRATEGY_MAP
        self._capital_templates = capital_templates or REGIME_CAPITAL_TEMPLATES

    def allocate(
        self,
        market_situation: str,
        signals: Dict[str, Any],
        portfolio_greeks: Any = None,
        existing_positions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, AllocationDirective]:
        """Produce allocation directives for all known strategies.

        Parameters
        ----------
        market_situation : str
            One of RISK_ON, NEUTRAL, RECOVERY, RISK_OFF, CRISIS.
        signals : dict
            Current market signals (VIX, FRAG, NAV, etc.).
        portfolio_greeks : PortfolioGreeks, optional
            Current aggregated greeks for budget checking.
        existing_positions : list, optional
            Currently open option positions.

        Returns
        -------
        dict
            strategy_name → AllocationDirective
        """
        nav = signals.get("nav", 0.0)
        vix = signals.get("vix_level", 20.0)
        frag = signals.get("frag", 0.0)

        # Determine which strategies are allowed
        allowed = set(self._regime_map.get(market_situation, set()))
        allowed |= self._config.always_on_strategies

        # Add vol strategies if conditions are met
        if vix <= self._config.vol_strategy_max_vix and frag >= self._config.vol_strategy_min_frag:
            allowed.add("straddle_strangle")

        # Get capital template for this regime
        template = self._capital_templates.get(
            market_situation,
            self._capital_templates.get("NEUTRAL", {}),
        )

        total_budget = nav * self._config.total_derivatives_budget_pct

        # Calculate per-category budgets
        category_budgets: Dict[str, float] = {}
        for cat_name, fraction in template.items():
            category_budgets[cat_name] = total_budget * fraction

        # Count strategies per category to split evenly
        category_strategy_counts: Dict[str, int] = {}
        for strat_name in allowed:
            cat = STRATEGY_CATEGORIES.get(strat_name)
            if cat is not None:
                key = cat.value
                category_strategy_counts[key] = (
                    category_strategy_counts.get(key, 0) + 1
                )

        # Build allocations
        allocations: Dict[str, AllocationDirective] = {}
        all_strategy_names = set(STRATEGY_CATEGORIES.keys())

        # Priority order: hedge > futures > income > directional > volatility
        priority_map = {
            StrategyCategory.HEDGE: 100,
            StrategyCategory.FUTURES: 80,
            StrategyCategory.INCOME: 60,
            StrategyCategory.DIRECTIONAL: 40,
            StrategyCategory.VOLATILITY: 20,
        }

        for strat_name in all_strategy_names:
            if strat_name not in allowed:
                allocations[strat_name] = AllocationDirective(
                    enabled=False,
                    reason=f"Not allowed in {market_situation}",
                )
                continue

            cat = STRATEGY_CATEGORIES.get(strat_name, StrategyCategory.INCOME)
            cat_key = cat.value
            cat_budget = category_budgets.get(cat_key, 0.0)
            n_in_cat = max(category_strategy_counts.get(cat_key, 1), 1)

            # Split category budget evenly among strategies in that category
            strat_budget = cat_budget / n_in_cat
            capital_pct = strat_budget / max(total_budget, 1.0)

            # Greeks sub-budget: proportional to capital share
            greeks_budget = self._compute_greeks_budget(
                capital_pct, nav, portfolio_greeks,
            )

            allocations[strat_name] = AllocationDirective(
                enabled=True,
                capital_pct=round(capital_pct, 4),
                priority=priority_map.get(cat, 50),
                greeks_budget=greeks_budget,
                reason=f"{market_situation}: {cat_key} category "
                       f"({capital_pct:.1%} of derivatives budget)",
            )

        # Log summary
        enabled_count = sum(1 for a in allocations.values() if a.enabled)
        logger.info(
            "StrategyAllocator: %s regime → %d/%d strategies enabled, "
            "total budget $%.0f (%.1f%% of NAV=$%.0f)",
            market_situation, enabled_count, len(allocations),
            total_budget, self._config.total_derivatives_budget_pct * 100, nav,
        )

        return allocations

    def _compute_greeks_budget(
        self,
        capital_pct: float,
        nav: float,
        portfolio_greeks: Any,
    ) -> GreeksBudget:
        """Compute per-strategy greeks budget proportional to capital share."""
        # Each strategy gets a fraction of the portfolio-level greeks budget
        max_delta = nav * self._config.max_portfolio_delta_pct * capital_pct
        max_gamma = self._config.max_portfolio_gamma * capital_pct
        min_theta = self._config.min_portfolio_theta * capital_pct
        max_vega = self._config.max_portfolio_vega * capital_pct

        return GreeksBudget(
            max_delta=round(max_delta, 2),
            max_gamma=round(max_gamma, 2),
            min_theta=round(min_theta, 2),
            max_vega=round(max_vega, 2),
        )

    def get_regime_summary(self, market_situation: str) -> Dict[str, Any]:
        """Return a summary of what would be enabled for a given regime."""
        allowed = set(self._regime_map.get(market_situation, set()))
        allowed |= self._config.always_on_strategies

        template = self._capital_templates.get(
            market_situation,
            self._capital_templates.get("NEUTRAL", {}),
        )

        return {
            "market_situation": market_situation,
            "enabled_strategies": sorted(allowed),
            "disabled_strategies": sorted(
                set(STRATEGY_CATEGORIES.keys()) - allowed
            ),
            "capital_template": template,
        }


__all__ = [
    "StrategyCategory",
    "GreeksBudget",
    "AllocationDirective",
    "StrategyAllocatorConfig",
    "StrategyAllocator",
    "STRATEGY_CATEGORIES",
    "REGIME_STRATEGY_MAP",
    "REGIME_CAPITAL_TEMPLATES",
]
