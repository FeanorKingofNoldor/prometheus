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
    "crisis_alpha": StrategyCategory.HEDGE,
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
        "bull_call_spread", "momentum_call", "leaps", "covered_call",
        "wheel", "iron_condor", "iron_butterfly", "vix_tail_hedge",
        # short_put removed: VIX-as-IV-proxy understates single-stock IV → consistently -EV
    },
    "NEUTRAL": {
        "covered_call", "iron_condor", "iron_butterfly",
        "calendar_spread", "wheel", "vix_tail_hedge",
        # short_put removed (same reason)
    },
    "RECOVERY": {
        "collar", "protective_put", "covered_call",
        "straddle_strangle", "vix_tail_hedge", "futures_overlay",
        # short_put removed (same reason)
    },
    "RISK_OFF": {
        "protective_put", "sector_put_spread", "vix_tail_hedge",
        "collar", "futures_overlay", "futures_option",
        "crisis_alpha",
    },
    "CRISIS": {
        "protective_put", "vix_tail_hedge", "futures_overlay",
        "futures_option", "crisis_alpha",
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
    total_derivatives_budget_pct: float = 0.30  # 30% of NAV (v36 tuned)

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


    def allocate_with_probabilities(
        self,
        regime_probs: RegimeProbabilities,
        signals: Dict[str, Any],
        portfolio_greeks: Any = None,
        existing_positions: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, AllocationDirective]:
        """Allocate using probability-weighted blending across regimes.

        Instead of picking a single regime and going all-or-nothing, this
        method computes allocations for each regime scenario separately,
        then blends them by probability.  A 30% crisis probability enables
        hedge strategies at 30% of their crisis-mode allocation rather than
        all-or-nothing.

        Parameters
        ----------
        regime_probs : RegimeProbabilities
            Probability distribution over market regimes.
        signals : dict
            Current market signals (VIX, FRAG, NAV, etc.).
        portfolio_greeks : PortfolioGreeks, optional
            Current aggregated greeks for budget checking.
        existing_positions : list, optional
            Currently open option positions.

        Returns
        -------
        dict
            strategy_name -> AllocationDirective (probability-blended)
        """
        # Map regime names to strategy allocator market situations.
        regime_situation_map = {
            "crisis": "CRISIS",
            "contraction": "RISK_OFF",
            "expansion": "RISK_ON",
        }

        prob_map = {
            "crisis": regime_probs.crisis,
            "contraction": regime_probs.contraction,
            "expansion": regime_probs.expansion,
        }

        # Compute per-regime allocations.
        regime_allocations: Dict[str, Dict[str, AllocationDirective]] = {}
        for regime_name, probability in prob_map.items():
            if probability < 0.01:
                continue  # skip negligible probabilities
            situation = regime_situation_map[regime_name]
            allocs = self.allocate(
                market_situation=situation,
                signals=signals,
                portfolio_greeks=portfolio_greeks,
                existing_positions=existing_positions,
            )
            regime_allocations[regime_name] = allocs

        # Blend allocations by probability weight.
        all_strategy_names = set(STRATEGY_CATEGORIES.keys())
        blended: Dict[str, AllocationDirective] = {}

        nav = signals.get("nav", 0.0)

        for strat_name in all_strategy_names:
            weighted_capital_pct = 0.0
            weighted_enabled = 0.0
            weighted_priority = 0.0
            weighted_delta = 0.0
            weighted_gamma = 0.0
            weighted_theta = 0.0
            weighted_vega = 0.0
            contributing_regimes: List[str] = []

            for regime_name, probability in prob_map.items():
                if probability < 0.01:
                    continue
                allocs = regime_allocations.get(regime_name, {})
                alloc = allocs.get(strat_name)
                if alloc is None:
                    continue

                if alloc.enabled:
                    weighted_enabled += probability
                    contributing_regimes.append(f"{regime_name}:{probability:.0%}")

                weighted_capital_pct += alloc.capital_pct * probability
                weighted_priority += alloc.priority * probability
                weighted_delta += alloc.greeks_budget.max_delta * probability
                weighted_gamma += alloc.greeks_budget.max_gamma * probability
                weighted_theta += alloc.greeks_budget.min_theta * probability
                weighted_vega += alloc.greeks_budget.max_vega * probability

            # Strategy is enabled if blended probability of being enabled exceeds 10%.
            is_enabled = weighted_enabled >= 0.10

            reason_parts = []
            if contributing_regimes:
                reason_parts.append(f"Blended from {', '.join(contributing_regimes)}")
            else:
                reason_parts.append(
                    f"Not enabled in any regime "
                    f"(dominant={regime_probs.dominant})"
                )

            blended[strat_name] = AllocationDirective(
                enabled=is_enabled,
                capital_pct=round(weighted_capital_pct, 4),
                priority=int(round(weighted_priority)),
                greeks_budget=GreeksBudget(
                    max_delta=round(weighted_delta, 2),
                    max_gamma=round(weighted_gamma, 2),
                    min_theta=round(weighted_theta, 2),
                    max_vega=round(weighted_vega, 2),
                ),
                reason=" | ".join(reason_parts) if reason_parts else "No regime data",
            )

        # Log blended summary.
        enabled_count = sum(1 for a in blended.values() if a.enabled)
        logger.info(
            "StrategyAllocator.allocate_with_probabilities: "
            "dominant=%s (crisis=%.1f%% contraction=%.1f%% expansion=%.1f%%) "
            "→ %d/%d strategies enabled",
            regime_probs.dominant,
            regime_probs.crisis * 100,
            regime_probs.contraction * 100,
            regime_probs.expansion * 100,
            enabled_count,
            len(blended),
        )

        return blended


# ── Regime probabilities ────────────────────────────────────────────


@dataclass
class RegimeProbabilities:
    """Probability distribution over market regimes."""

    crisis: float = 0.0
    contraction: float = 0.0
    expansion: float = 0.0

    def __post_init__(self) -> None:
        total = self.crisis + self.contraction + self.expansion
        # Normalise if values don't sum to ~1.0 (tolerance of 5%).
        if total > 0 and abs(total - 1.0) > 0.05:
            self.crisis = self.crisis / total
            self.contraction = self.contraction / total
            self.expansion = self.expansion / total

    @property
    def dominant(self) -> str:
        """Return the most likely regime."""
        return max(
            [("crisis", self.crisis), ("contraction", self.contraction),
             ("expansion", self.expansion)],
            key=lambda x: x[1],
        )[0]


def estimate_regime_probabilities(signals: dict) -> RegimeProbabilities:
    """Estimate regime probabilities from market signals.

    Uses VIX level, fragility score, and market health index as inputs.
    Simple logistic model -- not ML, just calibrated thresholds.

    Parameters
    ----------
    signals : dict
        Must contain some subset of: ``vix_level``, ``frag``, ``mhi``.

    Returns
    -------
    RegimeProbabilities
    """
    vix = signals.get("vix_level", 20)
    frag = signals.get("frag", 0.5)
    mhi = signals.get("mhi", 0.5)

    # Crisis probability increases with VIX and fragility.
    crisis_raw = (vix - 20) / 30 * 0.5 + frag * 0.3 + (1 - mhi) * 0.2
    crisis_p = max(0.0, min(1.0, crisis_raw))

    # Expansion probability is inverse of crisis signals.
    expansion_raw = (1 - frag) * 0.4 + mhi * 0.4 + max(0, (30 - vix) / 30) * 0.2
    expansion_p = max(0.0, min(1.0, expansion_raw)) * (1 - crisis_p)

    # Contraction is the remainder.
    contraction_p = max(0.0, 1 - crisis_p - expansion_p)

    return RegimeProbabilities(
        crisis=round(crisis_p, 3),
        contraction=round(contraction_p, 3),
        expansion=round(expansion_p, 3),
    )


__all__ = [
    "StrategyCategory",
    "GreeksBudget",
    "AllocationDirective",
    "StrategyAllocatorConfig",
    "StrategyAllocator",
    "RegimeProbabilities",
    "estimate_regime_probabilities",
    "STRATEGY_CATEGORIES",
    "REGIME_STRATEGY_MAP",
    "REGIME_CAPITAL_TEMPLATES",
]
