"""Prometheus — Signal Combination Layer.

A regime-conditional, IC-weighted combiner for *standardized* signal
components. This is infrastructure for the day we have several real,
orthogonal signals worth blending; today the equity signals are thin, so the
combiner is built and measured honestly rather than to manufacture alpha.

Design
------
The combiner takes a set of component scores that are ALREADY on a common
z-scale (e.g. the vol-scaled momentum z-score from ``model_basic.py`` and the
STAB/liquidity base z-score from the universe engine) and produces a single
blended score per name:

    combined(i) = sum_c w_c * z_c(i)

The weights ``w_c`` are resolved, in order of precedence:

1. **Regime-conditional** — if a ``regime_label`` is supplied and the config has
   a weight set for that label, use it. Different weights in
   CRISIS / RISK_OFF / CARRY / NEUTRAL.
2. **Default** — the base weight set, used when no regime is supplied or the
   regime has no specific override.

On top of the resolved base weights, an optional **IC tilt** multiplies each
component weight by a function of its trailing rank-IC (so a component that has
been predictive lately gets more weight, one that has been anti-predictive gets
less or is flipped). The IC tilt degrades to a no-op when no IC history is
available.

The combiner is **pure** and dependency-light: it operates on plain dicts /
frames and a regime *label string*, so it has no DB/engine import and is unit
testable on synthetic data. The harness consumes it to score a real panel.

Graceful degradation (all covered by tests):
- A component present in the weights but missing from a name's scores is
  treated as 0 (neutral) for that name.
- A component present in the scores but absent from the weights is ignored.
- Unknown / ``None`` regime → default weights.
- No IC history → IC tilt is the identity (base weights unchanged).
- Weights that don't sum to 1 are normalized (configurable) so the combined
  score's scale is stable across regimes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional

import numpy as np
import pandas as pd

# Canonical regime label strings. We accept either a RegimeLabel enum (its
# ``.value``) or a bare string, so the combiner has no hard apatheon import.
CRISIS = "CRISIS"
RISK_OFF = "RISK_OFF"
CARRY = "CARRY"
NEUTRAL = "NEUTRAL"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def _default_weights() -> Dict[str, float]:
    """Sensible static defaults.

    Momentum-z carries the directional tilt; the STAB/liquidity base is a
    quality screen that we let lean positive but small; fragility is a penalty
    (negative weight) when supplied as a positive-is-worse component.
    """
    return {
        "momentum_z": 1.0,
        "base_z": 0.25,
        "fragility_z": -0.25,
    }


def _default_regime_weights() -> Dict[str, Dict[str, float]]:
    """Per-regime weight overrides.

    The intent (NOT a fitted result — these are priors): in CRISIS lean on the
    quality/fragility screen and de-emphasize momentum (momentum crashes); in
    CARRY lean into momentum. NEUTRAL/expansion ~ defaults. These are the
    *defaults*; a caller can fit/override them and pass them in.
    """
    return {
        CRISIS: {"momentum_z": 0.4, "base_z": 0.5, "fragility_z": -0.6},
        RISK_OFF: {"momentum_z": 0.7, "base_z": 0.4, "fragility_z": -0.4},
        CARRY: {"momentum_z": 1.2, "base_z": 0.2, "fragility_z": -0.15},
        NEUTRAL: {"momentum_z": 1.0, "base_z": 0.25, "fragility_z": -0.25},
    }


@dataclass
class CombinerConfig:
    """Configuration for :class:`SignalCombiner`.

    Attributes:
        weights: default component weights (used when no regime match).
        regime_weights: optional per-regime-label weight overrides.
        use_regime: when True and a label is supplied, prefer regime weights.
        use_ic_tilt: when True, multiply weights by an IC-derived factor.
        ic_tilt_strength: blend between flat (0.0) and fully IC-proportional
            (1.0) tilt. factor_c = (1 - s) + s * f(ic_c).
        ic_floor: IC magnitudes below this are treated as 0 (noise gate).
        normalize: when True, rescale the resolved weights so the sum of their
            absolute values is 1 (keeps the combined score scale stable so IC
            comparisons across regimes are apples-to-apples). Sign is preserved.
    """

    weights: Dict[str, float] = field(default_factory=_default_weights)
    regime_weights: Dict[str, Dict[str, float]] = field(
        default_factory=_default_regime_weights
    )
    use_regime: bool = True
    use_ic_tilt: bool = False
    ic_tilt_strength: float = 1.0
    ic_floor: float = 0.0
    normalize: bool = True


# ---------------------------------------------------------------------------
# Combiner
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedWeights:
    """The weight set actually used for a combine call (for transparency/logging)."""

    regime: Optional[str]
    base: Dict[str, float]
    ic_factors: Dict[str, float]
    effective: Dict[str, float]


def _normalize_l1(weights: Mapping[str, float]) -> Dict[str, float]:
    """Scale weights so sum(|w|) == 1, preserving sign. No-op on all-zero."""
    total = sum(abs(v) for v in weights.values())
    if total <= 1e-12:
        return {k: 0.0 for k in weights}
    return {k: v / total for k, v in weights.items()}


class SignalCombiner:
    """Regime-conditional, optionally IC-weighted combiner of standardized signals."""

    def __init__(self, config: Optional[CombinerConfig] = None) -> None:
        self.config = config or CombinerConfig()

    # -- weight resolution -------------------------------------------------

    def _label_str(self, regime_label: Optional[object]) -> Optional[str]:
        if regime_label is None:
            return None
        # Accept a RegimeLabel enum (has .value) or a bare string.
        val = getattr(regime_label, "value", regime_label)
        return str(val)

    def resolve_weights(
        self,
        *,
        regime_label: Optional[object] = None,
        trailing_ic: Optional[Mapping[str, float]] = None,
    ) -> ResolvedWeights:
        """Resolve the effective per-component weights.

        Args:
            regime_label: regime enum or string; selects a regime weight set
                when ``use_regime`` and an override exists.
            trailing_ic: optional {component: trailing_rank_ic}. Drives the IC
                tilt when ``use_ic_tilt``. Missing/None → identity tilt.
        """
        cfg = self.config
        label = self._label_str(regime_label)

        if cfg.use_regime and label is not None and label in cfg.regime_weights:
            base = dict(cfg.regime_weights[label])
            used_regime: Optional[str] = label
        else:
            base = dict(cfg.weights)
            used_regime = None

        ic_factors: Dict[str, float] = {c: 1.0 for c in base}
        if cfg.use_ic_tilt and trailing_ic:
            s = float(cfg.ic_tilt_strength)
            for c in base:
                ic = trailing_ic.get(c)
                if ic is None or not np.isfinite(ic) or abs(ic) < cfg.ic_floor:
                    ic_factors[c] = 1.0
                    continue
                # Map IC to a sign-and-magnitude factor. A component that is
                # anti-predictive (negative IC) gets its contribution flipped;
                # a strongly predictive one is amplified. tanh keeps it bounded.
                signed = float(np.tanh(ic * 5.0))  # ~[-1, 1] over |ic|~0.4
                ic_factors[c] = (1.0 - s) + s * signed

        effective = {c: base[c] * ic_factors[c] for c in base}
        if cfg.normalize:
            effective = _normalize_l1(effective)

        return ResolvedWeights(
            regime=used_regime, base=base, ic_factors=ic_factors, effective=effective
        )

    # -- single cross-section ---------------------------------------------

    def combine_cross_section(
        self,
        components: Mapping[str, Mapping[str, float]],
        *,
        regime_label: Optional[object] = None,
        trailing_ic: Optional[Mapping[str, float]] = None,
    ) -> Dict[str, float]:
        """Combine one as-of cross-section into a single score per name.

        Args:
            components: {component_name: {instrument_id: z_value}}. Each inner
                dict is one standardized component over the cross-section.
            regime_label: regime for this date (selects weight set).
            trailing_ic: optional per-component trailing IC for the tilt.

        Returns:
            {instrument_id: combined_score}. A name missing a component is
            treated as 0 (neutral) for that component.
        """
        resolved = self.resolve_weights(
            regime_label=regime_label, trailing_ic=trailing_ic
        )
        weights = resolved.effective

        # Union of all instruments seen across the supplied components.
        names: set[str] = set()
        for comp in weights:
            comp_scores = components.get(comp)
            if comp_scores:
                names.update(comp_scores.keys())

        out: Dict[str, float] = {}
        for inst in names:
            total = 0.0
            for comp, w in weights.items():
                comp_scores = components.get(comp)
                if not comp_scores:
                    continue
                v = comp_scores.get(inst)
                if v is None or not np.isfinite(v):
                    continue
                total += w * float(v)
            out[inst] = total
        return out

    # -- panel (multi-date) for the harness -------------------------------

    def combine_panel(
        self,
        components: pd.DataFrame,
        *,
        component_cols: Optional[list[str]] = None,
        regime_by_date: Optional[Mapping[object, object]] = None,
        trailing_ic_by_date: Optional[Mapping[object, Mapping[str, float]]] = None,
    ) -> pd.DataFrame:
        """Combine a tidy multi-date panel into a single ``score`` per row.

        Args:
            components: tidy frame with ``instrument_id``, ``as_of_date`` and one
                column per standardized component (already z-scaled).
            component_cols: which columns to treat as components. Default: every
                column that appears in the configured weight sets and is present
                in the frame.
            regime_by_date: optional {as_of_date: regime_label} for
                regime-conditional weights. Dates absent here use default
                weights (graceful fallback).
            trailing_ic_by_date: optional {as_of_date: {component: ic}} for the
                IC tilt; absent dates use the identity tilt.

        Returns:
            tidy frame: ``instrument_id``, ``as_of_date``, ``score``.
        """
        if components.empty:
            return pd.DataFrame(columns=["instrument_id", "as_of_date", "score"])

        known = set(self.config.weights) | {
            c for w in self.config.regime_weights.values() for c in w
        }
        if component_cols is None:
            component_cols = [c for c in components.columns if c in known]
        if not component_cols:
            raise ValueError(
                "no component columns found in panel matching configured weights "
                f"(have {list(components.columns)}, expected any of {sorted(known)})"
            )

        rows: list[dict] = []
        for as_of, g in components.groupby("as_of_date"):
            comp_dict: Dict[str, Dict[str, float]] = {}
            for col in component_cols:
                comp_dict[col] = dict(
                    zip(g["instrument_id"].astype(str), g[col].astype(float))
                )
            regime = regime_by_date.get(as_of) if regime_by_date else None
            tic = trailing_ic_by_date.get(as_of) if trailing_ic_by_date else None
            combined = self.combine_cross_section(
                comp_dict, regime_label=regime, trailing_ic=tic
            )
            for inst, sc in combined.items():
                rows.append(
                    {"instrument_id": inst, "as_of_date": as_of, "score": sc}
                )

        out = pd.DataFrame(rows)
        if not out.empty:
            out["as_of_date"] = pd.to_datetime(out["as_of_date"])
        return out


# ---------------------------------------------------------------------------
# Trailing-IC estimation (for the IC-informed path)
# ---------------------------------------------------------------------------


def trailing_rank_ic_by_date(
    components: pd.DataFrame,
    fwd_returns: pd.DataFrame,
    *,
    component_cols: list[str],
    horizon: int,
    window: int = 12,
    min_cross_section: int = 5,
) -> Dict[object, Dict[str, float]]:
    """Compute a trailing mean rank-IC per component, as-of each date.

    For each component and date ``t``, the trailing IC is the mean of the
    per-date cross-sectional rank-IC of that component vs the ``horizon``-day
    forward return over the prior ``window`` evaluation dates STRICTLY before
    ``t`` (no look-ahead — the IC used to weight date ``t`` is built only from
    dates whose forward window closed before ``t``).

    Returns {as_of_date: {component: trailing_ic}}; dates without enough history
    are simply absent (caller's combiner falls back to the identity tilt).
    """
    from prometheus.research.signal_harness import _spearman  # local import

    ret_col = f"fwd_ret_{horizon}d"
    if ret_col not in fwd_returns.columns:
        return {}

    merged = components.merge(
        fwd_returns[["instrument_id", "as_of_date", ret_col]],
        on=["instrument_id", "as_of_date"],
        how="inner",
    )
    if merged.empty:
        return {}

    dates = sorted(merged["as_of_date"].unique())

    # Per-date, per-component IC first.
    per_date_ic: Dict[object, Dict[str, float]] = {}
    for as_of, g in merged.groupby("as_of_date"):
        t = g[ret_col].to_numpy(dtype=float)
        ics: Dict[str, float] = {}
        for col in component_cols:
            if col not in g.columns:
                continue
            s = g[col].to_numpy(dtype=float)
            mask = np.isfinite(s) & np.isfinite(t)
            if mask.sum() < min_cross_section:
                continue
            ic = _spearman(s, t)
            if np.isfinite(ic):
                ics[col] = ic
        per_date_ic[as_of] = ics

    # Trailing mean over the prior ``window`` dates (strictly before t).
    out: Dict[object, Dict[str, float]] = {}
    for i, as_of in enumerate(dates):
        prior = dates[max(0, i - window):i]
        if not prior:
            continue
        agg: Dict[str, list[float]] = {}
        for d in prior:
            for col, ic in per_date_ic.get(d, {}).items():
                agg.setdefault(col, []).append(ic)
        trailing = {col: float(np.mean(v)) for col, v in agg.items() if v}
        if trailing:
            out[as_of] = trailing
    return out


# ---------------------------------------------------------------------------
# Harness comparison: combined vs additive vs best-single
# ---------------------------------------------------------------------------


def additive_blend_panel(
    components: pd.DataFrame,
    *,
    weights: Mapping[str, float],
) -> pd.DataFrame:
    """The current production additive z-blend, as a tidy panel.

    score = sum_c weights[c] * component_c, over the components present in the
    frame. This mirrors ``engine.py``'s ``z(base) + alpha_weight_z * z(alpha)``
    so the harness can compare the combiner against the live behaviour.
    """
    cols = [c for c in weights if c in components.columns]
    if not cols:
        raise ValueError("no weight columns present in components frame")
    out = components[["instrument_id", "as_of_date"]].copy()
    score = np.zeros(len(components), dtype=float)
    for c in cols:
        vals = components[c].to_numpy(dtype=float)
        score = score + float(weights[c]) * np.nan_to_num(vals, nan=0.0)
    out["score"] = score
    return out
