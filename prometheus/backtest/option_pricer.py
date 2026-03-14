"""Prometheus v2 – Black-Scholes Option Pricer.

Complete European option pricing with full greeks, implied volatility
solver, and bid-ask spread model for synthetic options backtesting.

All functions are pure (no side effects, no I/O) and operate on
scalars for maximum flexibility.  The bid-ask model is parameterized
to approximate realistic execution costs.

Usage::

    from prometheus.backtest.option_pricer import bs_price, bs_greeks

    price = bs_price(S=450.0, K=440.0, T=0.12, r=0.05, sigma=0.20, right="P")
    greeks = bs_greeks(S=450.0, K=440.0, T=0.12, r=0.05, sigma=0.20, right="C")
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

# ── Constants ────────────────────────────────────────────────────────

_SQRT_2PI = math.sqrt(2.0 * math.pi)
_MIN_T = 1e-10          # Minimum time to avoid division by zero
_MIN_SIGMA = 1e-6       # Minimum vol
_IV_MAX_ITER = 50       # Newton-Raphson iterations for IV solver
_IV_TOLERANCE = 1e-8    # IV solver convergence tolerance


# ── Normal distribution helpers ──────────────────────────────────────

def norm_cdf(x: float) -> float:
    """Standard normal CDF (Abramowitz & Stegun, max error ~1.5e-7)."""
    a1 = 0.254829592
    a2 = -0.284496736
    a3 = 1.421413741
    a4 = -1.453152027
    a5 = 1.061405429
    p = 0.3275911
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * math.exp(-x * x / 2.0)
    return 0.5 * (1.0 + sign * y)


def norm_pdf(x: float) -> float:
    """Standard normal PDF."""
    return math.exp(-0.5 * x * x) / _SQRT_2PI


# ── Core d1/d2 ───────────────────────────────────────────────────────

def _d1d2(
    S: float, K: float, T: float, r: float, sigma: float,
) -> tuple[float, float]:
    """Compute Black-Scholes d1 and d2."""
    T = max(T, _MIN_T)
    sigma = max(sigma, _MIN_SIGMA)
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    return d1, d2


# ── Pricing ──────────────────────────────────────────────────────────

def bs_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    right: str,
) -> float:
    """European option price via Black-Scholes.

    Parameters
    ----------
    S : float
        Current underlying price.
    K : float
        Strike price.
    T : float
        Time to expiration in years (e.g. 30/365 for 30 days).
    r : float
        Risk-free rate (annualised, e.g. 0.05 for 5%).
    sigma : float
        Implied volatility (annualised, e.g. 0.20 for 20%).
    right : str
        ``"C"`` for call, ``"P"`` for put.

    Returns
    -------
    float
        Theoretical option price (per share, not per contract).
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        # Intrinsic value only
        if right.upper() == "C":
            return max(S - K, 0.0)
        return max(K - S, 0.0)

    d1, d2 = _d1d2(S, K, T, r, sigma)
    disc = math.exp(-r * T)

    if right.upper() == "C":
        return S * norm_cdf(d1) - K * disc * norm_cdf(d2)
    else:
        return K * disc * norm_cdf(-d2) - S * norm_cdf(-d1)


# ── Greeks ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BSGreeks:
    """Full Black-Scholes greeks for one option."""
    delta: float       # ∂V/∂S
    gamma: float       # ∂²V/∂S²
    theta: float       # ∂V/∂t  (per calendar day, negative for longs)
    vega: float        # ∂V/∂σ  (per 1% vol move, i.e. σ+0.01)
    rho: float         # ∂V/∂r  (per 1% rate move)
    price: float       # Option price


def bs_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    right: str,
) -> BSGreeks:
    """Compute all Black-Scholes greeks.

    Returns
    -------
    BSGreeks
        Frozen dataclass with delta, gamma, theta (per day), vega (per 1%),
        rho (per 1%), and price.
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        price = max(S - K, 0.0) if right.upper() == "C" else max(K - S, 0.0)
        # At-expiry greeks
        itm = (S > K) if right.upper() == "C" else (S < K)
        return BSGreeks(
            delta=1.0 if (right.upper() == "C" and itm) else (-1.0 if (right.upper() == "P" and itm) else 0.0),
            gamma=0.0,
            theta=0.0,
            vega=0.0,
            rho=0.0,
            price=price,
        )

    d1, d2 = _d1d2(S, K, T, r, sigma)
    disc = math.exp(-r * T)
    sqrt_T = math.sqrt(max(T, _MIN_T))
    pdf_d1 = norm_pdf(d1)

    # --- Gamma (same for calls and puts) ---
    gamma = pdf_d1 / (S * sigma * sqrt_T)

    # --- Vega (same for calls and puts, per 1% move = 0.01) ---
    vega = S * pdf_d1 * sqrt_T * 0.01

    is_call = right.upper() == "C"

    if is_call:
        nd1 = norm_cdf(d1)
        nd2 = norm_cdf(d2)
        price = S * nd1 - K * disc * nd2
        delta = nd1
        theta = (
            -S * pdf_d1 * sigma / (2.0 * sqrt_T)
            - r * K * disc * nd2
        ) / 365.0  # Per calendar day
        rho = K * T * disc * nd2 * 0.01
    else:
        n_neg_d1 = norm_cdf(-d1)
        n_neg_d2 = norm_cdf(-d2)
        price = K * disc * n_neg_d2 - S * n_neg_d1
        delta = -n_neg_d1  # = N(d1) - 1
        theta = (
            -S * pdf_d1 * sigma / (2.0 * sqrt_T)
            + r * K * disc * n_neg_d2
        ) / 365.0
        rho = -K * T * disc * n_neg_d2 * 0.01

    return BSGreeks(
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
        rho=rho,
        price=max(price, 0.0),
    )


# ── Implied Volatility Solver ────────────────────────────────────────

def bs_iv_from_price(
    S: float,
    K: float,
    T: float,
    r: float,
    market_price: float,
    right: str,
    *,
    initial_guess: float = 0.25,
    max_iter: int = _IV_MAX_ITER,
    tolerance: float = _IV_TOLERANCE,
) -> Optional[float]:
    """Solve for implied volatility using Newton-Raphson on vega.

    Returns
    -------
    float or None
        Implied volatility, or None if solver fails to converge.
    """
    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None

    # Bounds check: price must be between intrinsic and S (call) or K (put)
    intrinsic = max(S - K, 0.0) if right.upper() == "C" else max(K - S, 0.0)
    if market_price < intrinsic - 0.01:
        return None

    sigma = initial_guess
    for _ in range(max_iter):
        sigma = max(sigma, _MIN_SIGMA)
        price = bs_price(S, K, T, r, sigma, right)
        diff = price - market_price

        if abs(diff) < tolerance:
            return sigma

        # Vega for Newton step (raw, not per-1%)
        d1, _ = _d1d2(S, K, T, r, sigma)
        vega_raw = S * norm_pdf(d1) * math.sqrt(T)

        if vega_raw < 1e-12:
            break

        sigma -= diff / vega_raw

        # Keep sigma in reasonable bounds
        if sigma < 0.001:
            sigma = 0.001
        elif sigma > 5.0:
            sigma = 5.0

    # Final check
    price = bs_price(S, K, T, r, sigma, right)
    if abs(price - market_price) < tolerance * 100:
        return sigma
    return None


# ── Bid-Ask Spread Model ────────────────────────────────────────────

@dataclass(frozen=True)
class BidAskSpreadConfig:
    """Parameters for synthetic bid-ask spread generation."""
    # Base half-spread for liquid ATM options (SPY-like)
    base_half_spread: float = 0.02       # $0.02 → $0.04 total spread

    # OTM penalty: spreads widen as option moves away from ATM
    otm_penalty_coeff: float = 3.0       # Multiplier on |moneyness - 1|

    # Low DTE penalty: spreads tighten slightly near expiry for liquid
    # names but widen for illiquid — net effect is mild widening
    dte_penalty_coeff: float = 0.5       # Added per 1/sqrt(dte)

    # Illiquidity multiplier for non-SPY/QQQ underlyings
    illiquid_multiplier: float = 2.5     # Small-cap options are wider

    # Minimum spread (always at least this)
    min_spread: float = 0.01             # $0.01

    # Maximum spread (cap for very illiquid/OTM)
    max_spread: float = 0.50             # $0.50


# Symbols considered "liquid" for spread purposes
_LIQUID_UNDERLYINGS = frozenset({
    "SPY", "QQQ", "IWM", "DIA", "EEM", "GLD", "TLT", "HYG",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA", "AMD", "NFLX",
})


def compute_bid_ask(
    mid_price: float,
    underlying_price: float,
    strike: float,
    dte: int,
    symbol: str = "SPY",
    config: BidAskSpreadConfig = BidAskSpreadConfig(),
) -> tuple[float, float]:
    """Compute synthetic bid and ask prices.

    Parameters
    ----------
    mid_price : float
        Theoretical (mid) option price.
    underlying_price : float
        Current underlying price.
    strike : float
        Option strike price.
    dte : int
        Days to expiration.
    symbol : str
        Underlying symbol (used for liquidity classification).
    config : BidAskSpreadConfig
        Spread model parameters.

    Returns
    -------
    tuple[float, float]
        (bid, ask) prices.  Both are ≥ 0.
    """
    if mid_price <= 0 or underlying_price <= 0:
        return (0.0, 0.0)

    moneyness = strike / underlying_price
    otm_distance = abs(moneyness - 1.0)

    # Base spread
    half = config.base_half_spread

    # OTM penalty: wider spreads for further OTM
    half *= (1.0 + config.otm_penalty_coeff * otm_distance)

    # DTE effect: slight widening for very short DTE (gamma risk)
    effective_dte = max(dte, 1)
    half *= (1.0 + config.dte_penalty_coeff / math.sqrt(effective_dte))

    # Liquidity: non-liquid underlyings have wider spreads
    if symbol.upper() not in _LIQUID_UNDERLYINGS:
        half *= config.illiquid_multiplier

    # Scale with option price (cheap options have proportionally wider spreads)
    if mid_price < 1.0:
        half *= (1.0 + 0.5 * (1.0 - mid_price))

    # Clamp
    spread = max(2.0 * half, config.min_spread)
    spread = min(spread, config.max_spread)
    half = spread / 2.0

    bid = max(mid_price - half, 0.0)
    ask = mid_price + half

    return (round(bid, 2), round(ask, 2))


def fill_price(
    mid_price: float,
    underlying_price: float,
    strike: float,
    dte: int,
    is_buy: bool,
    symbol: str = "SPY",
    slippage_pct: float = 0.25,
) -> float:
    """Estimate fill price for a synthetic trade.

    Assumes fill between mid and bid/ask, controlled by slippage_pct.
    At 0.0 slippage you fill at mid; at 1.0 you fill at bid/ask.

    Parameters
    ----------
    slippage_pct : float
        Fraction of half-spread added to mid.  0.25 = you capture 75%
        of the theoretical mid (realistic for limit orders with patience).
    """
    bid, ask = compute_bid_ask(mid_price, underlying_price, strike, dte, symbol)
    half_spread = (ask - bid) / 2.0

    if is_buy:
        return mid_price + half_spread * slippage_pct
    else:
        return mid_price - half_spread * slippage_pct


# ── Convenience: price a contract ────────────────────────────────────

def price_contract(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    right: str,
    multiplier: int = 100,
) -> float:
    """Price a single option contract (premium × multiplier)."""
    per_share = bs_price(S, K, T, r, sigma, right)
    return per_share * multiplier


__all__ = [
    "norm_cdf",
    "norm_pdf",
    "bs_price",
    "bs_greeks",
    "bs_iv_from_price",
    "BSGreeks",
    "BidAskSpreadConfig",
    "compute_bid_ask",
    "fill_price",
    "price_contract",
]
