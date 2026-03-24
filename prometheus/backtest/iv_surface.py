"""Prometheus v2 – Implied Volatility Surface Engine.

Generates realistic implied volatility for synthetic options by combining:
- VIX-to-ATM-IV mapping for index options (SPY, QQQ)
- Realized-vol-to-IV conversion for individual stocks
- Parametric skew (put/call skew steepens in crisis) — overridden by CBOE
  SKEW index when available
- Term structure — interpolated from real VIX9D/VIX/VIX3M/VIX6M/VIX1Y when
  available, falling back to parametric contango/backwardation model

All functions are pure.  The engine is stateless and suitable for
vectorised daily loops.

Usage::

    from prometheus.backtest.iv_surface import IVSurfaceEngine, VolTermStructure

    iv_engine = IVSurfaceEngine()

    # With real vol data (preferred)
    ts = VolTermStructure(vix_30d=18.5, vix_3m=20.1, skew=128.0)
    iv = iv_engine.get_iv(
        strike=440.0, underlying_price=450.0, dte=45,
        vix=18.5, realized_vol_21d=0.16, symbol="SPY",
        term_structure=ts,
    )

    # Without real data (parametric fallback)
    iv = iv_engine.get_iv(
        strike=440.0, underlying_price=450.0, dte=45,
        vix=18.5, realized_vol_21d=0.16, symbol="SPY",
    )
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

# ── Configuration ────────────────────────────────────────────────────

@dataclass(frozen=True)
class IVSurfaceConfig:
    """Tunable parameters for the IV surface model."""

    # ── ATM IV ──────────────────────────────────────────────────────
    # IV/HV premium ratio by regime (VIX bucket)
    # These are calibrated from empirical VIX / SPY-realized-vol ratios.
    iv_hv_ratio_calm: float = 1.15       # VIX < 16
    iv_hv_ratio_normal: float = 1.25     # 16 ≤ VIX < 25
    iv_hv_ratio_elevated: float = 1.35   # 25 ≤ VIX < 35
    iv_hv_ratio_crisis: float = 1.50     # VIX ≥ 35

    # Minimum ATM IV floor (options are never priced below this)
    min_atm_iv: float = 0.08             # 8% annualised

    # Maximum ATM IV cap
    max_atm_iv: float = 1.50             # 150% annualised

    # Fallback realized vol when not available
    fallback_realized_vol: float = 0.20  # 20%

    # ── Skew ────────────────────────────────────────────────────────
    # Base skew slope (negative = OTM puts more expensive)
    # skew_adj = slope * (1 - moneyness) + curvature * (1 - moneyness)^2
    skew_slope_calm: float = -0.15       # VIX < 20
    skew_slope_elevated: float = -0.25   # 20 ≤ VIX < 30
    skew_slope_crisis: float = -0.35     # VIX ≥ 30

    skew_curvature: float = 0.8          # Quadratic term (smile wings)

    # Call-side skew attenuation: calls have flatter skew than puts
    call_skew_attenuation: float = 0.4   # Call skew = put skew × this

    # ── Term structure ──────────────────────────────────────────────
    # Contango coefficient (positive = longer DTE → higher IV)
    term_contango_coeff: float = 0.05    # Applied as sqrt(T/30 - 1) in calm
    # Backwardation coefficient (in crisis, short DTE > long DTE)
    term_backwardation_coeff: float = 0.08

    # ── VIX-specific ────────────────────────────────────────────────
    # VIX options IV is higher than equity IV (vol-of-vol)
    vix_iv_multiplier: float = 1.30      # VIX options IV = 1.3 × VIX-implied

    # ── Risk-free rate assumption ───────────────────────────────────
    # Historical approximations by era
    risk_free_rate_pre_2008: float = 0.045    # 4.5%
    risk_free_rate_2008_2015: float = 0.005   # 0.5% (ZIRP)
    risk_free_rate_2016_2019: float = 0.020   # 2.0%
    risk_free_rate_2020_2021: float = 0.002   # 0.2% (COVID ZIRP)
    risk_free_rate_2022_plus: float = 0.045   # 4.5% (hiking cycle)


# Symbols treated as "index" for IV purposes (IV ≈ VIX directly)
_INDEX_SYMBOLS = frozenset({"SPY", "SPX", "QQQ", "IVV", "VOO"})

# Symbols treated as "VIX" products (vol-of-vol)
_VIX_SYMBOLS = frozenset({"VIX", "VX", "VIXY", "UVXY", "SVXY"})


# ── Term structure data ──────────────────────────────────────────────

# Known DTE anchors for each vol index
_TS_DTE_VIX9D = 9
_TS_DTE_VIX = 30
_TS_DTE_VIX3M = 93
_TS_DTE_VIX6M = 180
_TS_DTE_VIX1Y = 365


@dataclass(frozen=True)
class VolTermStructure:
    """Real volatility term structure for one trading day.

    All values are index levels (e.g. VIX = 18.5 means 18.5%).
    Set a field to ``None`` when no data is available for that tenor.
    """

    vix_9d: Optional[float] = None    # VIX9D index (9-day)
    vix_30d: float = 20.0             # VIX (30-day) — always required
    vix_3m: Optional[float] = None    # VIX3M (93-day)
    vix_6m: Optional[float] = None    # VIX6M (180-day)
    vix_1y: Optional[float] = None    # VIX1Y (365-day)
    skew: Optional[float] = None      # CBOE SKEW index (100-170 typical)

    def to_points(self) -> List[Tuple[int, float]]:
        """Return available (dte, annualised_vol) pairs, sorted by DTE."""
        pts: List[Tuple[int, float]] = []
        if self.vix_9d is not None:
            pts.append((_TS_DTE_VIX9D, self.vix_9d / 100.0))
        pts.append((_TS_DTE_VIX, self.vix_30d / 100.0))
        if self.vix_3m is not None:
            pts.append((_TS_DTE_VIX3M, self.vix_3m / 100.0))
        if self.vix_6m is not None:
            pts.append((_TS_DTE_VIX6M, self.vix_6m / 100.0))
        if self.vix_1y is not None:
            pts.append((_TS_DTE_VIX1Y, self.vix_1y / 100.0))
        pts.sort(key=lambda p: p[0])
        return pts


# ── IV Surface Engine ────────────────────────────────────────────────

class IVSurfaceEngine:
    """Generate implied volatility for any (symbol, strike, dte, date).

    Parameters
    ----------
    config : IVSurfaceConfig, optional
        Override default IV model parameters.
    """

    def __init__(self, config: Optional[IVSurfaceConfig] = None) -> None:
        self._config = config or IVSurfaceConfig()

    # ── Public API ───────────────────────────────────────────────────

    def get_iv(
        self,
        *,
        strike: float,
        underlying_price: float,
        dte: int,
        vix: float,
        realized_vol_21d: float = 0.0,
        symbol: str = "SPY",
        right: str = "P",
        term_structure: Optional[VolTermStructure] = None,
    ) -> float:
        """Get implied volatility for a specific option.

        Parameters
        ----------
        strike : float
            Option strike price.
        underlying_price : float
            Current underlying price.
        dte : int
            Days to expiration.
        vix : float
            Current VIX level (e.g. 18.5).
        realized_vol_21d : float
            21-day realized volatility of the underlying (annualised).
            If 0, a fallback is used.
        symbol : str
            Underlying symbol.
        right : str
            ``"C"`` or ``"P"`` — affects skew direction.
        term_structure : VolTermStructure, optional
            Real vol term structure for the current date.  When provided,
            the ATM IV at the target DTE is interpolated from real data
            and the SKEW index (if available) drives the skew slope.

        Returns
        -------
        float
            Annualised implied volatility (e.g. 0.22 for 22%).
        """
        if underlying_price <= 0 or strike <= 0 or dte <= 0:
            return self._config.fallback_realized_vol

        # ── Step 1: ATM IV at the target DTE ─────────────────────────
        # When real term structure is available and the underlying is an
        # index product, interpolate ATM IV directly from the curve.
        # Otherwise fall back to the parametric model.
        use_real_ts = (
            term_structure is not None
            and symbol.upper() in _INDEX_SYMBOLS
        )

        if use_real_ts:
            atm_iv = self._interpolate_atm_iv(dte, term_structure)
        else:
            # Parametric: ATM at 30-day, then term-structure adjust
            atm_iv = self.get_atm_iv(
                vix=vix,
                realized_vol_21d=realized_vol_21d,
                symbol=symbol,
            )
            term_adj = self._term_structure_adjustment(dte, vix)
            atm_iv = atm_iv * (1.0 + term_adj)

        # ── Step 2: Skew adjustment ──────────────────────────────────
        moneyness = strike / underlying_price
        skew_adj = self._skew_adjustment(
            moneyness, vix, right,
            skew_index=term_structure.skew if term_structure else None,
        )

        # ── Combine ──────────────────────────────────────────────────
        iv = atm_iv * (1.0 + skew_adj)

        # Clamp to reasonable bounds
        iv = max(iv, self._config.min_atm_iv)
        iv = min(iv, self._config.max_atm_iv)

        return iv

    def get_atm_iv(
        self,
        *,
        vix: float,
        realized_vol_21d: float = 0.0,
        symbol: str = "SPY",
    ) -> float:
        """Get at-the-money implied volatility.

        For index products (SPY, QQQ), ATM IV ≈ VIX / 100.
        For VIX products, ATM IV = VIX-based × vix_iv_multiplier.
        For individual stocks, ATM IV = realized_vol × IV/HV ratio.
        """
        cfg = self._config
        symbol_upper = symbol.upper()

        if symbol_upper in _VIX_SYMBOLS:
            # VIX options: vol-of-vol is higher than equity vol
            base = vix / 100.0
            return max(base * cfg.vix_iv_multiplier, cfg.min_atm_iv)

        if symbol_upper in _INDEX_SYMBOLS:
            # Index: VIX is a direct proxy for 30-day ATM IV
            return max(vix / 100.0, cfg.min_atm_iv)

        # Individual stock / sector ETF: use realized vol × premium
        rv = realized_vol_21d if realized_vol_21d > 0 else cfg.fallback_realized_vol
        ratio = self._iv_hv_ratio(vix)
        atm = rv * ratio

        # Floor: at least 80% of VIX-implied (stocks shouldn't be
        # drastically cheaper than the index in practice)
        floor = (vix / 100.0) * 0.80
        atm = max(atm, floor, cfg.min_atm_iv)

        return min(atm, cfg.max_atm_iv)

    def get_risk_free_rate(self, year: int) -> float:
        """Historical risk-free rate approximation by era."""
        cfg = self._config
        if year < 2008:
            return cfg.risk_free_rate_pre_2008
        elif year < 2016:
            return cfg.risk_free_rate_2008_2015
        elif year < 2020:
            return cfg.risk_free_rate_2016_2019
        elif year < 2022:
            return cfg.risk_free_rate_2020_2021
        else:
            return cfg.risk_free_rate_2022_plus

    # ── Private helpers ──────────────────────────────────────────────

    def _iv_hv_ratio(self, vix: float) -> float:
        """IV/HV premium ratio, varying with VIX regime."""
        cfg = self._config
        if vix < 16:
            return cfg.iv_hv_ratio_calm
        elif vix < 25:
            return cfg.iv_hv_ratio_normal
        elif vix < 35:
            return cfg.iv_hv_ratio_elevated
        else:
            return cfg.iv_hv_ratio_crisis

    # ── Real-data helpers ──────────────────────────────────────────────

    def _interpolate_atm_iv(
        self,
        dte: int,
        ts: VolTermStructure,
    ) -> float:
        """Interpolate ATM IV for *dte* from real term structure data.

        Uses linear interpolation in √DTE-space (variance scales with
        time, so √T interpolation is more physically accurate than
        linear-DTE).

        When DTE is outside the available range, the nearest point is
        used (flat extrapolation).
        """
        pts = ts.to_points()
        if not pts:
            return ts.vix_30d / 100.0

        # Flat extrapolation at edges
        if dte <= pts[0][0]:
            return pts[0][1]
        if dte >= pts[-1][0]:
            return pts[-1][1]

        # Find bracketing points
        for i in range(len(pts) - 1):
            d_lo, v_lo = pts[i]
            d_hi, v_hi = pts[i + 1]
            if d_lo <= dte <= d_hi:
                # √DTE interpolation
                sqrt_lo = math.sqrt(d_lo)
                sqrt_hi = math.sqrt(d_hi)
                sqrt_t = math.sqrt(dte)
                frac = (sqrt_t - sqrt_lo) / (sqrt_hi - sqrt_lo) if sqrt_hi != sqrt_lo else 0.0
                return v_lo + frac * (v_hi - v_lo)

        # Shouldn't reach here, but fallback
        return ts.vix_30d / 100.0

    def _skew_adjustment(
        self,
        moneyness: float,
        vix: float,
        right: str,
        skew_index: Optional[float] = None,
    ) -> float:
        """Compute IV skew adjustment as a multiplicative factor.

        OTM puts (moneyness < 1) get positive adjustment (higher IV).
        OTM calls (moneyness > 1) get mild positive adjustment (smile).

        When a CBOE SKEW index value is provided, the skew slope is
        driven by real data instead of VIX-bucket heuristics.  SKEW ≈ 100
        means lognormal (minimal put premium); the long-run average is
        ~115, and values above 130 indicate steep put skew.

        Parameters
        ----------
        moneyness : float
            K / S.  < 1 for OTM puts, > 1 for OTM calls.
        vix : float
            Current VIX level.
        right : str
            ``"C"`` or ``"P"``.
        skew_index : float, optional
            CBOE SKEW index level (100-170 typical).
        """
        cfg = self._config

        if skew_index is not None and skew_index > 0:
            # ── Real-data skew slope ─────────────────────────────────
            # Derive slope from SKEW index.  The factor scales a base
            # slope of -0.20 (moderate skew) by how far SKEW deviates
            # from its long-run mean of ~115.
            _SKEW_MEAN = 115.0
            skew_factor = 1.0 + (skew_index - _SKEW_MEAN) / 50.0
            skew_factor = max(skew_factor, 0.3)   # Floor: don't flip sign
            skew_factor = min(skew_factor, 2.5)   # Cap: extreme skew
            slope = -0.20 * skew_factor
        else:
            # ── Parametric fallback (VIX-bucket) ─────────────────────
            if vix < 20:
                slope = cfg.skew_slope_calm
            elif vix < 30:
                slope = cfg.skew_slope_elevated
            else:
                slope = cfg.skew_slope_crisis

        # Distance from ATM
        x = 1.0 - moneyness  # Positive for OTM puts, negative for OTM calls

        # Linear + quadratic skew
        adj = slope * x + cfg.skew_curvature * x * x

        # Attenuate for calls (the skew is flatter on the call side)
        if right.upper() == "C" and moneyness > 1.0:
            adj *= cfg.call_skew_attenuation

        # Clamp: IV adjustment shouldn't more than double or halve ATM IV
        adj = max(adj, -0.50)
        adj = min(adj, 1.00)

        return adj

    def _term_structure_adjustment(
        self,
        dte: int,
        vix: float,
    ) -> float:
        """Compute term structure adjustment.

        Normal markets (VIX < 20): contango (longer term → higher IV).
        Elevated (20-30): approximately flat.
        Crisis (VIX ≥ 30): backwardation (short term > long term).

        The reference maturity is 30 DTE (where ATM IV ≈ VIX).
        """
        cfg = self._config

        if dte <= 1:
            dte = 1

        # Ratio relative to 30-day reference
        ratio = dte / 30.0

        if ratio <= 0:
            return 0.0

        if vix < 20:
            # Contango: longer = higher IV
            if ratio > 1.0:
                return cfg.term_contango_coeff * math.sqrt(ratio - 1.0)
            else:
                # Short-dated: slightly lower IV
                return -cfg.term_contango_coeff * math.sqrt(1.0 - ratio) * 0.5
        elif vix < 30:
            # Approximately flat (mild contango)
            if ratio > 1.0:
                return cfg.term_contango_coeff * 0.3 * math.sqrt(ratio - 1.0)
            else:
                return 0.0
        else:
            # Backwardation: short-dated IV is higher
            if ratio > 1.0:
                return -cfg.term_backwardation_coeff * math.sqrt(ratio - 1.0)
            else:
                # Short-dated: premium over 30-day
                return cfg.term_backwardation_coeff * math.sqrt(1.0 - ratio) * 0.5


__all__ = [
    "IVSurfaceConfig",
    "IVSurfaceEngine",
    "VolTermStructure",
]
