"""Prometheus v2 – Sector-aware graduated response allocator.

Replaces the binary Growth ↔ Defensive sleeve switch with a graduated
system that monitors per-sector health and responds proportionally:

1. **NORMAL**: all sectors healthy → full growth allocation.
2. **SECTOR_STRESS**: 1-2 sectors sick → kill those sectors, redistribute.
3. **BROAD_STRESS**: 3+ sectors sick OR market MHI below threshold → reduce
   overall equity exposure, increase hedges.
4. **SYSTEMIC_CRISIS**: 6+ sectors sick OR MHI critical → full liquidation
   of equities + maximum hedge allocation.

The allocator operates on *weights* (not orders).  It takes a proposed
portfolio (instrument_id → weight) and adjusts it based on the current
sector health state and market health index.

Usage
-----
    from prometheus.sector.allocator import SectorAllocator, SectorAllocatorConfig

    config = SectorAllocatorConfig()
    allocator = SectorAllocator(config, sector_mapper, sector_health_result)
    adjusted = allocator.adjust_weights(
        weights={"AAPL.US": 0.05, "JPM.US": 0.05, ...},
        market_mhi=0.45,
        as_of_date=date(2007, 11, 1),
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from apathis.core.logging import get_logger
from apathis.sector.health import SectorHealthResult
from apathis.sector.mapper import SectorMapper

logger = get_logger(__name__)


class StressLevel(str, Enum):
    """Market stress classification based on sector health."""

    NORMAL = "NORMAL"
    SECTOR_STRESS = "SECTOR_STRESS"
    BROAD_STRESS = "BROAD_STRESS"
    SYSTEMIC_CRISIS = "SYSTEMIC_CRISIS"


@dataclass
class SectorAllocatorConfig:
    """Configuration for the graduated response allocator.

    Thresholds
    ----------
    SHI thresholds apply to per-sector Sector Health Index scores ∈ [0, 1].
    MHI thresholds apply to the market-wide Market Health Index ∈ [-1, 1].
    """

    # ── Sector-level thresholds ──────────────────────────────────────
    # Below kill_threshold, all positions in that sector are liquidated.
    sector_kill_threshold: float = 0.25
    # Below reduce_threshold (but above kill), exposure is halved.
    sector_reduce_threshold: float = 0.40
    # Maximum portfolio weight per sector (concentration limit).
    sector_max_concentration: float = 0.30

    # ── Stress level escalation ──────────────────────────────────────
    # Number of sick sectors (below kill_threshold) to trigger each level.
    sector_stress_count: int = 1       # ≥ 1 sick sectors → SECTOR_STRESS
    broad_stress_count: int = 3        # ≥ 3 sick sectors → BROAD_STRESS
    systemic_crisis_count: int = 6     # ≥ 6 sick sectors → SYSTEMIC_CRISIS

    # ── Market MHI overrides ─────────────────────────────────────────
    # MHI thresholds that can escalate the stress level regardless of
    # sector count.  MHI ∈ [-1, 1].
    mhi_broad_stress_threshold: float = -0.20
    mhi_systemic_crisis_threshold: float = -0.50

    # ── Response multipliers ─────────────────────────────────────────
    # Overall equity exposure multiplier per stress level.
    # 1.0 = fully invested, 0.0 = all cash/hedges.
    equity_multiplier_normal: float = 1.0
    equity_multiplier_sector_stress: float = 0.85
    equity_multiplier_broad_stress: float = 0.50
    equity_multiplier_systemic_crisis: float = 0.0

    # Hedge allocation per stress level (fraction of NAV).
    hedge_allocation_normal: float = 0.0
    hedge_allocation_sector_stress: float = 0.10
    hedge_allocation_broad_stress: float = 0.40
    hedge_allocation_systemic_crisis: float = 1.0

    # ── Redistribution ───────────────────────────────────────────────
    # When killing a sector, redistribute its weight to healthy sectors?
    redistribute_killed_weight: bool = True
    # Minimum SHI to be considered "healthy" for redistribution target.
    healthy_sector_threshold: float = 0.55


@dataclass
class AllocationDecision:
    """Output of the graduated response allocator for a single day."""

    # Date of the decision.
    as_of_date: date

    # Determined stress level.
    stress_level: StressLevel

    # Sectors classified as sick / weak / healthy.
    sick_sectors: List[str] = field(default_factory=list)
    weak_sectors: List[str] = field(default_factory=list)
    healthy_sectors: List[str] = field(default_factory=list)

    # Adjusted instrument weights (after sector kills/reduces/redistribution).
    adjusted_weights: Dict[str, float] = field(default_factory=dict)

    # Overall equity exposure multiplier applied.
    equity_multiplier: float = 1.0

    # Hedge allocation (fraction of NAV).
    hedge_allocation: float = 0.0

    # Per-sector SHI scores on this date.
    sector_scores: Dict[str, float] = field(default_factory=dict)

    # Market MHI on this date (if provided).
    market_mhi: Optional[float] = None

    # Weight killed (total weight removed from sick sectors).
    weight_killed: float = 0.0

    # Weight reduced (total weight removed from weak sectors).
    weight_reduced: float = 0.0


class SectorAllocator:
    """Graduated response allocator using sector health.

    This class is stateless per-call: each ``adjust_weights()`` call
    produces an independent decision based on the current SHI scores and
    MHI.  State-machine behavior (e.g. minimum days in crisis) is left
    to the higher-level allocator that wraps this.
    """

    def __init__(
        self,
        config: SectorAllocatorConfig,
        sector_mapper: SectorMapper,
        sector_health: SectorHealthResult,
    ) -> None:
        self._cfg = config
        self._mapper = sector_mapper
        self._health = sector_health

    def classify_stress(
        self,
        as_of_date: date,
        market_mhi: Optional[float] = None,
    ) -> Tuple[StressLevel, List[str], List[str], List[str], Dict[str, float]]:
        """Classify the current stress level based on sector health.

        Returns
        -------
        tuple of (stress_level, sick_sectors, weak_sectors, healthy_sectors, sector_scores)
        """
        cfg = self._cfg
        sector_scores: Dict[str, float] = {}

        for sector_name, score_dict in self._health.scores.items():
            score = score_dict.get(as_of_date)
            if score is not None:
                sector_scores[sector_name] = score

        # Classify each sector.
        sick: List[str] = []
        weak: List[str] = []
        healthy: List[str] = []

        for sector, score in sector_scores.items():
            if score < cfg.sector_kill_threshold:
                sick.append(sector)
            elif score < cfg.sector_reduce_threshold:
                weak.append(sector)
            else:
                healthy.append(sector)

        n_sick = len(sick)

        # Determine stress level from sector count.
        if n_sick >= cfg.systemic_crisis_count:
            level = StressLevel.SYSTEMIC_CRISIS
        elif n_sick >= cfg.broad_stress_count:
            level = StressLevel.BROAD_STRESS
        elif n_sick >= cfg.sector_stress_count:
            level = StressLevel.SECTOR_STRESS
        else:
            level = StressLevel.NORMAL

        # MHI can only escalate, never de-escalate.
        if market_mhi is not None:
            if market_mhi <= cfg.mhi_systemic_crisis_threshold:
                level = StressLevel.SYSTEMIC_CRISIS
            elif market_mhi <= cfg.mhi_broad_stress_threshold and level.value < StressLevel.BROAD_STRESS.value:
                # Only escalate if current level is below BROAD_STRESS.
                if level in (StressLevel.NORMAL, StressLevel.SECTOR_STRESS):
                    level = StressLevel.BROAD_STRESS

        return level, sick, weak, healthy, sector_scores

    def adjust_weights(
        self,
        weights: Dict[str, float],
        as_of_date: date,
        market_mhi: Optional[float] = None,
    ) -> AllocationDecision:
        """Adjust portfolio weights based on sector health.

        Parameters
        ----------
        weights : dict
            instrument_id → proposed weight (before sector adjustment).
        as_of_date : date
            Current date for SHI lookup.
        market_mhi : float, optional
            Market-wide MHI score ∈ [-1, 1].  Used for stress level
            escalation.

        Returns
        -------
        AllocationDecision
            The adjusted weights and metadata about the decision.
        """
        cfg = self._cfg

        level, sick, weak, healthy, sector_scores = self.classify_stress(
            as_of_date, market_mhi,
        )

        # Get equity multiplier and hedge allocation for this stress level.
        eq_mult = {
            StressLevel.NORMAL: cfg.equity_multiplier_normal,
            StressLevel.SECTOR_STRESS: cfg.equity_multiplier_sector_stress,
            StressLevel.BROAD_STRESS: cfg.equity_multiplier_broad_stress,
            StressLevel.SYSTEMIC_CRISIS: cfg.equity_multiplier_systemic_crisis,
        }[level]

        hedge_alloc = {
            StressLevel.NORMAL: cfg.hedge_allocation_normal,
            StressLevel.SECTOR_STRESS: cfg.hedge_allocation_sector_stress,
            StressLevel.BROAD_STRESS: cfg.hedge_allocation_broad_stress,
            StressLevel.SYSTEMIC_CRISIS: cfg.hedge_allocation_systemic_crisis,
        }[level]

        sick_set = set(sick)
        weak_set = set(weak)
        healthy_set = set(healthy)

        # ── Step 1: Kill sick sectors, reduce weak sectors ───────────
        adjusted: Dict[str, float] = {}
        weight_killed = 0.0
        weight_reduced = 0.0

        for iid, w in weights.items():
            sector = self._mapper.get_sector(iid) or "UNKNOWN"

            if sector in sick_set:
                # Kill: remove all weight.
                weight_killed += w
                continue

            if sector in weak_set:
                # Reduce: halve the weight.
                reduced = w * 0.5
                weight_reduced += w - reduced
                adjusted[iid] = reduced
            else:
                adjusted[iid] = w

        # ── Step 2: Redistribute killed weight to healthy sectors ────
        if cfg.redistribute_killed_weight and weight_killed > 0 and level != StressLevel.SYSTEMIC_CRISIS:
            # Find instruments in healthy sectors.
            healthy_instruments = {
                iid: w for iid, w in adjusted.items()
                if (self._mapper.get_sector(iid) or "UNKNOWN") in healthy_set
            }
            total_healthy_weight = sum(healthy_instruments.values())

            if total_healthy_weight > 0:
                for iid in healthy_instruments:
                    share = healthy_instruments[iid] / total_healthy_weight
                    adjusted[iid] += share * weight_killed

        # ── Step 3: Apply sector concentration limits ────────────────
        adjusted = self._apply_concentration_limits(adjusted)

        # ── Step 4: Apply overall equity multiplier ──────────────────
        if eq_mult < 1.0:
            for iid in adjusted:
                adjusted[iid] *= eq_mult

        # ── Step 5: Renormalise so total weight ≤ 1.0 ────────────────
        total_w = sum(adjusted.values())
        max_equity = 1.0 - hedge_alloc
        if total_w > max_equity and total_w > 0:
            scale = max_equity / total_w
            for iid in adjusted:
                adjusted[iid] *= scale

        return AllocationDecision(
            as_of_date=as_of_date,
            stress_level=level,
            sick_sectors=sick,
            weak_sectors=weak,
            healthy_sectors=healthy,
            adjusted_weights=adjusted,
            equity_multiplier=eq_mult,
            hedge_allocation=hedge_alloc,
            sector_scores=sector_scores,
            market_mhi=market_mhi,
            weight_killed=weight_killed,
            weight_reduced=weight_reduced,
        )

    def _apply_concentration_limits(
        self,
        weights: Dict[str, float],
    ) -> Dict[str, float]:
        """Enforce per-sector maximum concentration.

        If a sector exceeds ``sector_max_concentration``, scale down all
        instruments in that sector proportionally.  Freed weight is not
        redistributed (becomes cash).
        """
        cap = self._cfg.sector_max_concentration
        if cap >= 1.0:
            return weights

        sector_weights = self._mapper.get_sector_weights(weights)
        sectors_over = {s: w for s, w in sector_weights.items() if w > cap}

        if not sectors_over:
            return weights

        adjusted = dict(weights)
        for sector, total_w in sectors_over.items():
            scale = cap / total_w
            for iid in adjusted:
                if (self._mapper.get_sector(iid) or "UNKNOWN") == sector:
                    adjusted[iid] *= scale

        return adjusted
