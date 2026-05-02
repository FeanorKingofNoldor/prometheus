"""Prometheus v2 – Options Flow Aggregation.

Aggregates options flow signals from IBKR scanner data and live tick
snapshots into a daily options flow score per sector and market-wide.

Data sources
------------
1. **Scanner polling**: ``HIGH_OPT_VOLUME_PUT_CALL_RATIO`` and
   ``HOT_BY_OPT_VOLUME`` scanners identify stocks with unusual options
   activity.  Each hit is mapped to its sector.
2. **Tick snapshots**: streaming ``TickSnapshot`` data provides live
   put/call volume ratios for sector ETFs and the broad market (SPY/QQQ).

Flow score definition
---------------------
For each sector, the options flow score ∈ [-1, +1] is computed as::

    flow_score = clip(
        -0.5 * zscore(put_call_ratio)
        + 0.3 * zscore(unusual_call_volume_fraction)
        - 0.2 * zscore(unusual_put_volume_fraction),
        -1, 1
    )

Where:
- **put_call_ratio**: aggregated put/call volume ratio for the sector
  (from tick snapshots or scanner).  Higher → bearish → negative score.
- **unusual_call_volume_fraction**: fraction of sector scanner hits
  that are call-heavy.  Higher → bullish → positive score.
- **unusual_put_volume_fraction**: fraction of sector scanner hits
  that are put-heavy.  Higher → bearish → negative score.

A market-wide score is computed the same way using SPY/QQQ snapshots
and the full (non-sectored) scanner results.

Usage
-----
    from prometheus.sector.options_flow import OptionsFlowAggregator

    agg = OptionsFlowAggregator(market_data_service, sector_mapper)
    agg.start()           # subscribe scanners
    score = agg.get_sector_flow_score("Technology")
    market = agg.get_market_flow_score()
    agg.stop()
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
from apatheon.core.logging import get_logger
from apatheon.sector.health import SECTOR_ETF_MAP

from prometheus.execution.market_data import (
    MarketDataService,
    ScannerResult,
    ScannerSubscription,
    TickSnapshot,
)

logger = get_logger(__name__)


# ── Reverse lookup: stock symbol → sector (via ETF mapping) ──────────
# Scanner results are individual stocks, not ETFs.  We need a way to map
# them.  This is injected via SectorMapper at runtime, but for ETF
# symbols we have a direct mapping.
_ETF_SYMBOL_TO_SECTOR: Dict[str, str] = {
    etf_id.replace(".US", ""): sector
    for etf_id, sector in SECTOR_ETF_MAP.items()
}


# ── Data classes ─────────────────────────────────────────────────────

@dataclass
class ScannerHit:
    """A single scanner detection for a stock."""
    symbol: str
    scan_code: str
    value: float
    sector: Optional[str]
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )


@dataclass
class SectorFlowSnapshot:
    """Aggregated options flow for a single sector at a point in time."""
    sector: str
    put_call_ratio: float = 1.0   # 1.0 = neutral
    scanner_hits_call: int = 0     # unusual call-heavy activity
    scanner_hits_put: int = 0      # unusual put-heavy activity
    scanner_hits_total: int = 0
    flow_score: float = 0.0        # [-1, +1]
    timestamp: Optional[datetime] = None


@dataclass
class MarketFlowSnapshot:
    """Market-wide options flow snapshot."""
    spy_put_call_ratio: float = 1.0
    qqq_put_call_ratio: float = 1.0
    total_scanner_hits: int = 0
    call_heavy_fraction: float = 0.5
    put_heavy_fraction: float = 0.5
    flow_score: float = 0.0
    timestamp: Optional[datetime] = None


# ── Aggregator ───────────────────────────────────────────────────────

class OptionsFlowAggregator:
    """Aggregate IBKR scanner data and tick snapshots into flow scores.

    Parameters
    ----------
    market_data_service : MarketDataService
        For scanner subscriptions and tick snapshots.
    sector_lookup : callable, optional
        A function ``symbol → sector_name`` to classify scanner hits.
        If None, only ETF symbols are classified directly.
    put_call_neutral : float
        The put/call ratio considered "neutral" (typically 0.7–1.0 for
        equities).
    scanner_poll_rows : int
        Number of rows per scanner subscription.
    """

    # Scanner codes we subscribe to
    SCAN_CODES = [
        "HIGH_OPT_VOLUME_PUT_CALL_RATIO",  # stocks with high P/C ratio
        "HOT_BY_OPT_VOLUME",               # stocks with unusual options vol
    ]

    def __init__(
        self,
        market_data_service: MarketDataService,
        sector_lookup: Optional[Any] = None,
        *,
        put_call_neutral: float = 0.85,
        scanner_poll_rows: int = 50,
    ) -> None:
        self._mds = market_data_service
        self._sector_lookup = sector_lookup  # callable: symbol → sector or None
        self._put_call_neutral = put_call_neutral
        self._scanner_poll_rows = scanner_poll_rows

        # Scanner state: scan_code → list of latest results
        self._scanner_results: Dict[str, List[ScannerResult]] = {}
        self._scanner_lock = threading.Lock()

        # Scanner subscription IDs for cleanup
        self._scanner_req_ids: List[int] = []

        # Computed sector flow snapshots
        self._sector_flows: Dict[str, SectorFlowSnapshot] = {}
        self._market_flow: MarketFlowSnapshot = MarketFlowSnapshot()
        self._flow_lock = threading.Lock()

        self._running = False

    # ── Lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        """Subscribe to scanners and begin aggregation."""
        if self._running:
            logger.warning("OptionsFlowAggregator already running")
            return

        for scan_code in self.SCAN_CODES:
            sub = ScannerSubscription(
                scan_code=scan_code,
                instrument="STK",
                location="STK.US.MAJOR",
                above_price=5.0,
                market_cap_above=1e9,
                number_of_rows=self._scanner_poll_rows,
            )
            req_id = self._mds.subscribe_scanner(
                sub,
                callback=self._on_scanner_update,
            )
            self._scanner_req_ids.append(req_id)

        self._running = True
        logger.info("OptionsFlowAggregator started with %d scanners",
                     len(self.SCAN_CODES))

    def stop(self) -> None:
        """Unsubscribe from all scanners."""
        for req_id in self._scanner_req_ids:
            try:
                self._mds.unsubscribe_scanner(req_id)
            except Exception as exc:
                logger.warning("Error unsubscribing scanner %d: %s", req_id, exc)
        self._scanner_req_ids.clear()
        self._running = False
        logger.info("OptionsFlowAggregator stopped")

    # ── Scanner callback ─────────────────────────────────────────────

    def _on_scanner_update(
        self,
        scan_code: str,
        results: List[ScannerResult],
    ) -> None:
        """Handle scanner data update from MarketDataService."""
        with self._scanner_lock:
            self._scanner_results[scan_code] = list(results)

        logger.debug("Scanner %s updated: %d results", scan_code, len(results))

        # Recompute scores on each update
        self._recompute()

    # ── Score computation ────────────────────────────────────────────

    def _resolve_sector(self, symbol: str) -> Optional[str]:
        """Map a stock symbol to its sector name."""
        # Direct ETF lookup
        if symbol in _ETF_SYMBOL_TO_SECTOR:
            return _ETF_SYMBOL_TO_SECTOR[symbol]

        # Injected sector mapper
        if self._sector_lookup is not None:
            try:
                sector = self._sector_lookup(symbol)
                if sector:
                    return sector
            except Exception:
                pass

        return None

    def _recompute(self) -> None:
        """Recompute sector and market flow scores from current data."""
        now = datetime.now(timezone.utc)

        # ── Classify scanner hits by sector ──────────────────────────
        sector_hits: Dict[str, List[ScannerHit]] = defaultdict(list)
        all_hits: List[ScannerHit] = []

        with self._scanner_lock:
            for scan_code, results in self._scanner_results.items():
                for r in results:
                    sector = self._resolve_sector(r.symbol)
                    hit = ScannerHit(
                        symbol=r.symbol,
                        scan_code=scan_code,
                        value=r.value,
                        sector=sector,
                        timestamp=now,
                    )
                    all_hits.append(hit)
                    if sector:
                        sector_hits[sector].append(hit)

        # ── Get tick snapshots for ETFs ──────────────────────────────
        snapshots = self._mds.get_all_snapshots()

        # ── Per-sector scores ────────────────────────────────────────
        new_sector_flows: Dict[str, SectorFlowSnapshot] = {}

        for etf_sym, sector_name in _ETF_SYMBOL_TO_SECTOR.items():
            snap: Optional[TickSnapshot] = snapshots.get(etf_sym)
            pc_ratio = self._put_call_neutral  # default neutral
            if snap is not None and snap.put_call_ratio > 0:
                pc_ratio = snap.put_call_ratio

            hits = sector_hits.get(sector_name, [])
            n_call = sum(
                1 for h in hits
                if h.scan_code == "HOT_BY_OPT_VOLUME" and h.value < self._put_call_neutral
            )
            n_put = sum(
                1 for h in hits
                if h.scan_code == "HIGH_OPT_VOLUME_PUT_CALL_RATIO"
            )
            n_total = len(hits)

            score = self._compute_flow_score(
                pc_ratio, n_call, n_put, n_total,
            )

            new_sector_flows[sector_name] = SectorFlowSnapshot(
                sector=sector_name,
                put_call_ratio=pc_ratio,
                scanner_hits_call=n_call,
                scanner_hits_put=n_put,
                scanner_hits_total=n_total,
                flow_score=score,
                timestamp=now,
            )

        # ── Market-wide score ────────────────────────────────────────
        spy_snap = snapshots.get("SPY")
        qqq_snap = snapshots.get("QQQ")

        spy_pc = spy_snap.put_call_ratio if spy_snap and spy_snap.put_call_ratio > 0 else self._put_call_neutral
        qqq_pc = qqq_snap.put_call_ratio if qqq_snap and qqq_snap.put_call_ratio > 0 else self._put_call_neutral

        # Market-level P/C: average of SPY and QQQ
        market_pc = (spy_pc + qqq_pc) / 2.0

        total_hits = len(all_hits)
        call_heavy = sum(
            1 for h in all_hits
            if h.scan_code == "HOT_BY_OPT_VOLUME" and h.value < self._put_call_neutral
        )
        put_heavy = sum(
            1 for h in all_hits
            if h.scan_code == "HIGH_OPT_VOLUME_PUT_CALL_RATIO"
        )

        market_score = self._compute_flow_score(
            market_pc, call_heavy, put_heavy, total_hits,
        )

        new_market = MarketFlowSnapshot(
            spy_put_call_ratio=spy_pc,
            qqq_put_call_ratio=qqq_pc,
            total_scanner_hits=total_hits,
            call_heavy_fraction=call_heavy / max(total_hits, 1),
            put_heavy_fraction=put_heavy / max(total_hits, 1),
            flow_score=market_score,
            timestamp=now,
        )

        # ── Atomic update ────────────────────────────────────────────
        with self._flow_lock:
            self._sector_flows = new_sector_flows
            self._market_flow = new_market

    def _compute_flow_score(
        self,
        pc_ratio: float,
        n_call_heavy: int,
        n_put_heavy: int,
        n_total: int,
    ) -> float:
        """Compute the options flow score ∈ [-1, +1].

        Parameters
        ----------
        pc_ratio : float
            Put/call volume ratio (higher = more bearish).
        n_call_heavy : int
            Number of unusual-call-volume scanner hits.
        n_put_heavy : int
            Number of high-put/call-ratio scanner hits.
        n_total : int
            Total scanner hits for this group.
        """
        # Normalise P/C around neutral
        # pc_ratio of 0.85 → z=0, 1.2 → z ≈ +1, 0.5 → z ≈ −1
        pc_z = (pc_ratio - self._put_call_neutral) / max(self._put_call_neutral * 0.3, 0.1)

        # Scanner fractions
        if n_total > 0:
            call_frac = n_call_heavy / n_total
            put_frac = n_put_heavy / n_total
        else:
            call_frac = 0.0
            put_frac = 0.0

        # Normalise fractions: 0.5 → neutral
        call_z = (call_frac - 0.5) / 0.25 if n_total > 3 else 0.0
        put_z = (put_frac - 0.5) / 0.25 if n_total > 3 else 0.0

        # Weighted composite:
        # - High P/C ratio is bearish (negative)
        # - Many call-heavy hits is bullish (positive)
        # - Many put-heavy hits is bearish (negative)
        raw = -0.5 * pc_z + 0.3 * call_z - 0.2 * put_z

        return float(np.clip(raw, -1.0, 1.0))

    # ── Public queries ───────────────────────────────────────────────

    def get_sector_flow_score(self, sector: str) -> float:
        """Return the current flow score for a sector ∈ [-1, +1].

        Returns 0.0 (neutral) if no data available.
        """
        with self._flow_lock:
            snap = self._sector_flows.get(sector)
            return snap.flow_score if snap is not None else 0.0

    def get_sector_flow_snapshot(self, sector: str) -> Optional[SectorFlowSnapshot]:
        """Return full flow snapshot for a sector."""
        with self._flow_lock:
            return self._sector_flows.get(sector)

    def get_all_sector_flow_scores(self) -> Dict[str, float]:
        """Return all sector flow scores."""
        with self._flow_lock:
            return {s: snap.flow_score for s, snap in self._sector_flows.items()}

    def get_market_flow_score(self) -> float:
        """Return the market-wide flow score ∈ [-1, +1]."""
        with self._flow_lock:
            return self._market_flow.flow_score

    def get_market_flow_snapshot(self) -> MarketFlowSnapshot:
        """Return full market-wide flow snapshot."""
        with self._flow_lock:
            return self._market_flow

    def get_all_flow_scores(self) -> Dict[str, float]:
        """Return all scores (sectors + 'MARKET')."""
        with self._flow_lock:
            result = {s: snap.flow_score for s, snap in self._sector_flows.items()}
            result["MARKET"] = self._market_flow.flow_score
            return result

    def get_status(self) -> Dict[str, Any]:
        """Return aggregator status for diagnostics."""
        with self._scanner_lock:
            scanner_counts = {
                code: len(results)
                for code, results in self._scanner_results.items()
            }
        with self._flow_lock:
            sector_count = len(self._sector_flows)

        return {
            "running": self._running,
            "scanner_subscriptions": len(self._scanner_req_ids),
            "scanner_result_counts": scanner_counts,
            "sectors_tracked": sector_count,
            "market_flow_score": self._market_flow.flow_score,
        }
