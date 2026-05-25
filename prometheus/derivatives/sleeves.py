"""Sleeve + template configuration.

The redesigned options layer is organised into three sleeves with
explicit budgets (rather than seventeen strategies sharing one pot):

* HEDGE — downside protection. Always-on, modulated by stress. Never
  takes profit; held until the reason to hedge is gone. 10% of NAV.
* INCOME — sells premium when vol is rich. Pauses in stress.
  Calibrated so its carry roughly funds the hedge sleeve. 15% of NAV.
* CONVEX — small thematic bets that fire only when Apatheon's intel
  signals stack (compound pressure, divergence, geo escalation). The
  only sleeve with an alpha thesis. 5% of NAV.

Total derivatives budget remains 30% of NAV, matching today's cap.

Each sleeve owns a tuple of ``TemplateConfig`` entries. A template
declares (i) when it fires, (ii) how to build its ``TargetSpec``, and
(iii) its lifecycle rules. The actual selection / sizing pipeline
lives in ``prometheus.derivatives.selection`` and ``sizing``; this
module is the configuration surface.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from prometheus.derivatives.selection import LegSpec, SpreadSpec, TargetSpec

# ── Sleeve identity ──────────────────────────────────────────────────


class Sleeve(str, Enum):
    HEDGE = "HEDGE"
    INCOME = "INCOME"
    CONVEX = "CONVEX"


# ── Trigger contract ─────────────────────────────────────────────────


@dataclass(frozen=True)
class TriggerResult:
    """Outcome of evaluating a template trigger.

    ``fire=True`` enables the template for this run. ``reason`` and
    ``metadata`` are recorded into the decision log; metadata can also
    parameterise the ``target_spec_factory`` (e.g. which sector to
    target for a thematic put).
    """

    fire: bool
    reason: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


TriggerFn = Callable[[Mapping[str, Any]], TriggerResult]
TargetSpecFactory = Callable[[Mapping[str, Any], Mapping[str, Any]], TargetSpec]
SpreadSpecFactory = Callable[[Mapping[str, Any], Mapping[str, Any]], SpreadSpec]


# ── Template ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TemplateConfig:
    """One named entry inside a sleeve.

    Exactly one of ``target_spec_factory`` (single-leg) and
    ``spread_spec_factory`` (multi-leg) must be set. Runner dispatches
    on which is populated.
    """

    name: str                              # e.g. "hedge.spy_protective_put"
    sleeve: Sleeve
    trigger: TriggerFn
    # Fraction of the *sleeve* budget allocated to this template per
    # firing. e.g. 0.30 of a 10%-NAV hedge sleeve = 3% NAV per trade.
    sizing_pct_of_sleeve: float
    target_spec_factory: TargetSpecFactory | None = None
    spread_spec_factory: SpreadSpecFactory | None = None
    # True = buy the option (single-leg hedges, convex bets).
    # False = sell the option (single-leg income).
    # For spreads, the per-leg direction is in ``LegSpec.is_long`` and
    # this field is ignored.
    is_long: bool = True
    max_concurrent: int = 1
    # Regime gate. Empty tuple = "fires in every market state" (the
    # default; back-compat for templates that don't care). Non-empty
    # = only fire when signals["market_state"] is one of these.
    # Aligned with the legacy REGIME_STRATEGY_MAP — e.g. protective
    # puts only fire in RECOVERY/RISK_OFF/CRISIS, not RISK_ON.
    allowed_market_states: tuple[str, ...] = ()
    # Lifecycle hints — picked up by the position manager.
    close_at_dte: int = 7
    profit_target_pct: float | None = None     # None = no take-profit
    stop_loss_multiplier: float | None = None  # None = no stop
    # Default fallback IV when IbkrLive / cache / realized all fail.
    fallback_iv: float = 0.22

    def __post_init__(self) -> None:
        has_single = self.target_spec_factory is not None
        has_spread = self.spread_spec_factory is not None
        if has_single == has_spread:
            raise ValueError(
                f"TemplateConfig {self.name!r}: exactly one of "
                "target_spec_factory / spread_spec_factory must be set"
            )

    @property
    def is_spread(self) -> bool:
        return self.spread_spec_factory is not None


# ── Sleeve ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SleeveConfig:
    sleeve: Sleeve
    nav_pct: float
    templates: tuple[TemplateConfig, ...]

    def budget(self, nav: float) -> float:
        return max(nav, 0.0) * self.nav_pct


# ── Default seed templates ───────────────────────────────────────────
#
# These are intentionally minimal — one template per sleeve — so the
# sleeve runner and shadow harness have something real to drive in
# Phase 1c/1d. The full production set is filled in during Phases 2-4
# (hedge migration, income migration, convex migration).


def _hedge_spy_protective_put_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    mhi = float(signals.get("mhi", 1.0) or 1.0)
    if mhi >= 0.40:
        return TriggerResult(False, f"mhi={mhi:.2f} above threshold 0.40")
    return TriggerResult(True, f"mhi={mhi:.2f} below threshold 0.40", {"mhi": mhi})


def _hedge_spy_protective_put_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> TargetSpec:
    return TargetSpec(
        underlying="SPY", right="P",
        target_delta=0.25, min_dte=45, max_dte=90,
    )


_HEDGE_SPY_PROTECTIVE_PUT = TemplateConfig(
    name="hedge.spy_protective_put",
    sleeve=Sleeve.HEDGE,
    trigger=_hedge_spy_protective_put_trigger,
    target_spec_factory=_hedge_spy_protective_put_target,
    sizing_pct_of_sleeve=0.30,   # 30% of HEDGE sleeve = 3% of NAV per fire
    max_concurrent=1,
    allowed_market_states=("RECOVERY", "RISK_OFF", "CRISIS"),
    close_at_dte=14,
    profit_target_pct=None,      # never take profit on a hedge
    stop_loss_multiplier=None,
    fallback_iv=0.22,
)


# ── hedge.sector_put_spread ──────────────────────────────────────────

# Sector ETF mapping. Aligned with apatheon.sector.health canonical
# names but kept local so this module doesn't take a circular dep on
# apatheon while we're still building. Phase 4 wires the canonical map.
_SECTOR_TO_ETF: Mapping[str, str] = {
    "TECHNOLOGY": "XLK", "FINANCIALS": "XLF", "ENERGY": "XLE",
    "HEALTHCARE": "XLV", "INDUSTRIALS": "XLI", "CONSUMER_STAPLES": "XLP",
    "CONSUMER_DISCRETIONARY": "XLY", "UTILITIES": "XLU",
    "MATERIALS": "XLB", "REAL_ESTATE": "XLRE", "COMMUNICATIONS": "XLC",
}


def _hedge_sector_put_spread_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    sector_shi = signals.get("sector_shi") or {}
    if not sector_shi:
        return TriggerResult(False, "no sector_shi signal")
    threshold = 0.30
    # Filter to *mappable* sectors below the threshold, then pick the
    # weakest among them. Picking the absolute weakest first and then
    # bailing on missing ETF would skip the template even when a less-
    # weak but tradeable sector is also unhealthy.
    candidates: list[tuple[str, float, str]] = []
    for name, score in sector_shi.items():
        try:
            s = float(score)
        except (TypeError, ValueError):
            continue
        if s >= threshold:
            continue
        etf = _SECTOR_TO_ETF.get(str(name).upper())
        if etf is None:
            continue
        candidates.append((str(name), s, etf))

    if not candidates:
        return TriggerResult(
            False,
            f"no mappable sector SHI below threshold {threshold}",
        )

    candidates.sort(key=lambda x: x[1])
    name, score, etf = candidates[0]
    return TriggerResult(
        True,
        f"sector {name} SHI={score:.2f} below {threshold} → {etf} put spread",
        {"sector": name, "sector_etf": etf, "shi": score},
    )


def _hedge_sector_put_spread_target(
    signals: Mapping[str, Any], trigger: Mapping[str, Any],
) -> SpreadSpec:
    return SpreadSpec(
        underlying=trigger["sector_etf"],
        min_dte=30, max_dte=60,
        legs=(
            LegSpec(right="P", target_delta=0.30, is_long=True,  name="long_put"),
            LegSpec(right="P", target_delta=0.10, is_long=False, name="short_put"),
        ),
    )


_HEDGE_SECTOR_PUT_SPREAD = TemplateConfig(
    name="hedge.sector_put_spread",
    sleeve=Sleeve.HEDGE,
    trigger=_hedge_sector_put_spread_trigger,
    spread_spec_factory=_hedge_sector_put_spread_target,
    sizing_pct_of_sleeve=0.30,    # 30% of HEDGE sleeve = 3% NAV per sector
    max_concurrent=3,             # up to 3 concurrent sector spreads
    allowed_market_states=("RISK_OFF", "CRISIS"),
    close_at_dte=10,
    profit_target_pct=None,       # hedge — never take profit
    stop_loss_multiplier=None,
    fallback_iv=0.30,             # sector ETFs are vol-ier than SPY
)


# ── hedge.vix_tail_call ──────────────────────────────────────────────


def _hedge_vix_tail_call_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    # Always-on catastrophe insurance. The size is tiny (one position
    # capped to its sleeve fraction) so the persistent carry is bounded
    # and the convexity payoff is what we're paying for.
    vix = float(signals.get("vix_level", 0.0) or 0.0)
    if vix <= 0:
        return TriggerResult(False, "vix_level missing")
    return TriggerResult(True, f"always-on; vix={vix:.1f}", {"vix": vix})


def _hedge_vix_tail_call_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> TargetSpec:
    return TargetSpec(
        underlying="VIX", right="C",
        target_delta=0.20,    # well OTM — pay less, get more convexity
        min_dte=45, max_dte=90,
        sec_type="IND", exchange="CBOE",
        strike_width_pct=0.60,  # VIX strikes range wider than equity ETFs
    )


_HEDGE_VIX_TAIL_CALL = TemplateConfig(
    name="hedge.vix_tail_call",
    sleeve=Sleeve.HEDGE,
    trigger=_hedge_vix_tail_call_trigger,
    target_spec_factory=_hedge_vix_tail_call_target,
    sizing_pct_of_sleeve=0.20,    # 20% of HEDGE sleeve = 2% NAV
    is_long=True,
    max_concurrent=1,
    # Always-on across every regime (matches legacy vix_tail_hedge).
    allowed_market_states=(),
    close_at_dte=21,              # VIX gamma blows up near expiry — close early
    profit_target_pct=None,
    stop_loss_multiplier=None,
    fallback_iv=0.80,             # VIX IV is very high (vol-of-vol)
)


# ── hedge.collar ─────────────────────────────────────────────────────
#
# Collar = long protective put + short overwrite call on the same
# underlying. Fires in the awkward middle: MHI has recovered enough
# that an outright protective put is too expensive, but fragility is
# still elevated enough that you want downside protection paid for by
# capping upside.


def _hedge_collar_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    mhi = float(signals.get("mhi", 1.0) or 1.0)
    frag = float(signals.get("frag", 0.0) or 0.0)
    in_recovery = 0.40 <= mhi <= 0.60
    fragile = frag > 0.40
    if not (in_recovery and fragile):
        return TriggerResult(
            False,
            f"mhi={mhi:.2f} (need 0.40-0.60) frag={frag:.2f} (need >0.40)",
        )
    return TriggerResult(
        True, f"recovery zone: mhi={mhi:.2f} frag={frag:.2f}",
        {"mhi": mhi, "frag": frag},
    )


def _hedge_collar_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> SpreadSpec:
    return SpreadSpec(
        underlying="SPY",
        min_dte=30, max_dte=60,
        legs=(
            LegSpec(right="P", target_delta=0.25, is_long=True,  name="protective_put"),
            LegSpec(right="C", target_delta=0.20, is_long=False, name="overwrite_call"),
        ),
    )


_HEDGE_COLLAR = TemplateConfig(
    name="hedge.collar",
    sleeve=Sleeve.HEDGE,
    trigger=_hedge_collar_trigger,
    spread_spec_factory=_hedge_collar_target,
    sizing_pct_of_sleeve=0.20,    # 20% of HEDGE sleeve = 2% NAV
    max_concurrent=1,
    allowed_market_states=("RECOVERY", "RISK_OFF"),
    close_at_dte=14,
    profit_target_pct=None,       # hedge structure — managed by lifecycle
    stop_loss_multiplier=None,
    fallback_iv=0.22,
)


def _income_spy_short_put_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    vix = float(signals.get("vix_level", 0.0) or 0.0)
    if vix < 15 or vix > 30:
        return TriggerResult(False, f"vix={vix:.1f} outside 15-30 band")
    return TriggerResult(True, f"vix={vix:.1f} in income band", {"vix": vix})


def _income_spy_short_put_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> TargetSpec:
    return TargetSpec(
        underlying="SPY", right="P",
        target_delta=0.20, min_dte=30, max_dte=45,
    )


_INCOME_SPY_SHORT_PUT = TemplateConfig(
    name="income.spy_short_put",
    sleeve=Sleeve.INCOME,
    trigger=_income_spy_short_put_trigger,
    target_spec_factory=_income_spy_short_put_target,
    sizing_pct_of_sleeve=0.20,   # 20% of INCOME sleeve = 3% of NAV per fire
    is_long=False,               # short premium
    max_concurrent=3,
    allowed_market_states=("RISK_ON", "NEUTRAL"),
    close_at_dte=7,
    profit_target_pct=0.50,      # buy back at 50% of credit
    stop_loss_multiplier=2.0,
    fallback_iv=0.22,
)


# ── income.spy_iron_butterfly ────────────────────────────────────────


def _income_spy_iron_butterfly_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    vix = float(signals.get("vix_level", 0.0) or 0.0)
    frag = float(signals.get("frag", 0.0) or 0.0)
    if vix <= 0:
        return TriggerResult(False, "vix_level missing")
    if vix > 20:
        return TriggerResult(False, f"vix={vix:.1f} above 20 — too noisy for fly")
    if frag > 0.20:
        return TriggerResult(False, f"frag={frag:.2f} above 0.20")
    return TriggerResult(
        True, f"vix={vix:.1f} (≤20) frag={frag:.2f} (≤0.20)",
        {"vix": vix, "frag": frag},
    )


def _income_spy_iron_butterfly_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> SpreadSpec:
    return SpreadSpec(
        underlying="SPY",
        min_dte=21, max_dte=45,
        legs=(
            # Sell ATM straddle, buy wings 5% out for defined-risk
            LegSpec(right="P", target_delta=0.50, is_long=False, name="short_atm_put"),
            LegSpec(right="C", target_delta=0.50, is_long=False, name="short_atm_call"),
            LegSpec(right="P", target_delta=0.15, is_long=True,  name="long_otm_put"),
            LegSpec(right="C", target_delta=0.15, is_long=True,  name="long_otm_call"),
        ),
    )


_INCOME_SPY_IRON_BUTTERFLY = TemplateConfig(
    name="income.spy_iron_butterfly",
    sleeve=Sleeve.INCOME,
    trigger=_income_spy_iron_butterfly_trigger,
    spread_spec_factory=_income_spy_iron_butterfly_target,
    sizing_pct_of_sleeve=0.20,    # 20% of INCOME sleeve = 3% NAV
    max_concurrent=2,
    allowed_market_states=("NEUTRAL",),
    close_at_dte=7,
    profit_target_pct=0.50,       # buy back at 50% of max profit
    stop_loss_multiplier=2.0,
    fallback_iv=0.18,             # SPY ATM IV
)


# ── income.spy_iron_condor ───────────────────────────────────────────


def _income_spy_iron_condor_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    vix = float(signals.get("vix_level", 0.0) or 0.0)
    frag = float(signals.get("frag", 0.0) or 0.0)
    if vix <= 0:
        return TriggerResult(False, "vix_level missing")
    # Condor wants a slightly higher vol band than butterfly so the
    # OTM wings carry useful premium.
    if vix < 14 or vix > 22:
        return TriggerResult(False, f"vix={vix:.1f} outside 14-22 band")
    if frag > 0.30:
        return TriggerResult(False, f"frag={frag:.2f} above 0.30")
    return TriggerResult(
        True, f"vix={vix:.1f} (14-22) frag={frag:.2f} (≤0.30)",
        {"vix": vix, "frag": frag},
    )


def _income_spy_iron_condor_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> SpreadSpec:
    return SpreadSpec(
        underlying="SPY",
        min_dte=30, max_dte=60,
        legs=(
            # Sell 0.20 delta OTM put + call; buy further OTM as wings
            LegSpec(right="P", target_delta=0.20, is_long=False, name="short_put"),
            LegSpec(right="C", target_delta=0.20, is_long=False, name="short_call"),
            LegSpec(right="P", target_delta=0.08, is_long=True,  name="long_put"),
            LegSpec(right="C", target_delta=0.08, is_long=True,  name="long_call"),
        ),
    )


_INCOME_SPY_IRON_CONDOR = TemplateConfig(
    name="income.spy_iron_condor",
    sleeve=Sleeve.INCOME,
    trigger=_income_spy_iron_condor_trigger,
    spread_spec_factory=_income_spy_iron_condor_target,
    sizing_pct_of_sleeve=0.25,    # 25% of INCOME sleeve = ~3.75% NAV
    max_concurrent=2,
    allowed_market_states=("RISK_ON", "NEUTRAL"),
    close_at_dte=10,
    profit_target_pct=0.50,
    stop_loss_multiplier=2.0,
    fallback_iv=0.20,
)


# ── income.covered_call ──────────────────────────────────────────────


def _income_covered_call_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    """Fires when we hold ≥100 shares of any name with non-trivial IV.

    Reads ``signals.equity_positions`` (dict ``symbol → shares``) and
    picks the position with the most coverable contracts.
    """
    vix = float(signals.get("vix_level", 0.0) or 0.0)
    if vix < 15:
        return TriggerResult(False, f"vix={vix:.1f} too low for covered call premium")
    positions = signals.get("equity_positions") or {}
    if not positions:
        return TriggerResult(False, "no equity_positions signal")

    coverable: list[tuple[str, int]] = []
    for sym, shares in positions.items():
        try:
            n = int(shares)
        except (TypeError, ValueError):
            continue
        if n >= 100:
            coverable.append((str(sym).upper(), n // 100))
    if not coverable:
        return TriggerResult(False, "no equity position has ≥100 shares")

    # Pick the position with the most coverable contracts.
    coverable.sort(key=lambda x: -x[1])
    chosen_symbol, max_contracts = coverable[0]
    return TriggerResult(
        True,
        f"covering {chosen_symbol} ({max_contracts} contracts) at vix={vix:.1f}",
        {"symbol": chosen_symbol, "max_contracts": max_contracts, "vix": vix},
    )


def _income_covered_call_target(
    signals: Mapping[str, Any], trigger: Mapping[str, Any],
) -> TargetSpec:
    return TargetSpec(
        underlying=str(trigger["symbol"]),
        right="C",
        target_delta=0.30,          # ~30Δ OTM = balance of premium vs assignment risk
        min_dte=30, max_dte=45,
    )


_INCOME_COVERED_CALL = TemplateConfig(
    name="income.covered_call",
    sleeve=Sleeve.INCOME,
    trigger=_income_covered_call_trigger,
    target_spec_factory=_income_covered_call_target,
    sizing_pct_of_sleeve=0.10,    # capped low; actual cap = shares//100
    is_long=False,                # short call against existing stock
    max_concurrent=5,
    allowed_market_states=("RISK_ON", "NEUTRAL", "RECOVERY"),
    close_at_dte=7,
    profit_target_pct=0.80,       # buy back at 80% of credit
    stop_loss_multiplier=None,    # covered = no stop
    fallback_iv=0.25,             # single-stock IV is higher than SPY
)


def _convex_thematic_sector_put_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    compound = signals.get("compound_pressure") or {}
    severity = str(compound.get("severity", "")).upper()
    sector_etf = compound.get("target_sector_etf")
    if severity not in ("HIGH", "CRITICAL"):
        return TriggerResult(False, f"compound severity={severity!r} below HIGH")
    if not sector_etf:
        return TriggerResult(False, "compound HIGH but no target_sector_etf")
    return TriggerResult(
        True,
        f"compound {severity} on {sector_etf}",
        {"sector_etf": sector_etf, "severity": severity},
    )


def _convex_thematic_sector_put_target(
    signals: Mapping[str, Any], trigger: Mapping[str, Any],
) -> TargetSpec:
    return TargetSpec(
        underlying=trigger["sector_etf"], right="P",
        target_delta=0.25, min_dte=30, max_dte=60,
    )


_CONVEX_THEMATIC_SECTOR_PUT = TemplateConfig(
    name="convex.thematic_sector_put",
    sleeve=Sleeve.CONVEX,
    trigger=_convex_thematic_sector_put_trigger,
    target_spec_factory=_convex_thematic_sector_put_target,
    sizing_pct_of_sleeve=0.40,   # 40% of CONVEX sleeve = 2% of NAV per fire
    max_concurrent=2,
    allowed_market_states=("RISK_OFF", "CRISIS"),
    close_at_dte=14,
    profit_target_pct=2.0,        # 200% — convex bets go big or go home
    stop_loss_multiplier=1.0,     # full debit
    fallback_iv=0.30,             # sector ETFs are vol-ier than SPY
)


# ── convex.vix_escalation_call ───────────────────────────────────────
#
# Fires when Apatheon flags elevated geo risk *but* VIX hasn't reacted
# yet — the bet is that institutional vol pricing lags geopolitical
# escalation by days, and a cheap OTM VIX call captures the move.


def _convex_vix_escalation_call_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    geo_score = float(signals.get("geo_risk_score", 0.0) or 0.0)
    vix = float(signals.get("vix_level", 0.0) or 0.0)
    vix_5d_change_pct = float(signals.get("vix_5d_change_pct", 0.0) or 0.0)

    if geo_score < 50:
        return TriggerResult(
            False, f"geo_risk_score={geo_score:.1f} below 50",
        )
    if vix <= 0:
        return TriggerResult(False, "vix_level missing")
    # Trade is alpha only if VIX hasn't already moved
    if vix_5d_change_pct > 0.10:
        return TriggerResult(
            False,
            f"vix already moved {vix_5d_change_pct:.0%} in 5d — "
            "no asymmetry left",
        )
    return TriggerResult(
        True,
        f"geo_risk={geo_score:.1f} elevated but vix={vix:.1f} "
        f"only {vix_5d_change_pct:+.1%} in 5d",
        {"geo_risk_score": geo_score, "vix": vix,
         "vix_5d_change_pct": vix_5d_change_pct},
    )


def _convex_vix_escalation_call_target(
    signals: Mapping[str, Any], _trigger: Mapping[str, Any],
) -> TargetSpec:
    return TargetSpec(
        underlying="VIX", right="C",
        target_delta=0.25,            # less OTM than always-on tail — more conviction
        min_dte=45, max_dte=90,
        sec_type="IND", exchange="CBOE",
        strike_width_pct=0.60,
    )


_CONVEX_VIX_ESCALATION_CALL = TemplateConfig(
    name="convex.vix_escalation_call",
    sleeve=Sleeve.CONVEX,
    trigger=_convex_vix_escalation_call_trigger,
    target_spec_factory=_convex_vix_escalation_call_target,
    sizing_pct_of_sleeve=0.30,    # 30% of CONVEX sleeve = 1.5% of NAV
    is_long=True,
    max_concurrent=2,
    allowed_market_states=("NEUTRAL", "RECOVERY", "RISK_OFF"),
    close_at_dte=21,
    profit_target_pct=3.0,        # 300% — let convex bets ride if they hit
    stop_loss_multiplier=1.0,     # full debit
    fallback_iv=0.80,             # VIX vol-of-vol
)


# ── convex.convergence_straddle ──────────────────────────────────────
#
# Fires when Apatheon's divergence + convergence both flash for the
# same entity: narrative-reality gap is extreme AND we estimate the
# gap closes within 30 days. Long straddle on the proxy ETF captures
# the move regardless of direction.


# Maps Apatheon entity → proxy underlying for the straddle.
# Conservative; only entities with a clean proxy listed.
_ENTITY_TO_STRADDLE_PROXY: Mapping[str, str] = {
    # Chokepoints
    "HORMUZ": "XLE", "BAB_EL_MANDEB": "XLE", "SUEZ": "XLE",
    "STRAIT_OF_MALACCA": "XLK",
    # Conflicts
    "IRAN_WAR": "XLE", "TAIWAN": "XLK", "RUSSIA_UKRAINE": "XLE",
}


def _convex_convergence_straddle_trigger(signals: Mapping[str, Any]) -> TriggerResult:
    intel = signals.get("intel")
    if intel is None:
        return TriggerResult(False, "no intel snapshot in signals")

    extreme_div = intel.extreme_divergences()
    if not extreme_div:
        return TriggerResult(False, "no extreme divergences")
    imminent_conv = intel.imminent_convergences(max_days=30, min_confidence=0.5)
    if not imminent_conv:
        return TriggerResult(False, "no imminent convergence")

    # Pair: same entity must appear in both
    div_keys = {(d["entity_type"], d["entity_id"]) for d in extreme_div}
    matched = [
        c for c in imminent_conv
        if (c["entity_type"], c["entity_id"]) in div_keys
    ]
    if not matched:
        return TriggerResult(
            False,
            "extreme divergence and imminent convergence present but no "
            "entity in both",
        )

    # Pick the entity with earliest convergence
    matched.sort(key=lambda c: c.get("estimated_convergence_days") or 999)
    chosen = matched[0]
    entity_id = str(chosen["entity_id"]).upper()
    proxy = _ENTITY_TO_STRADDLE_PROXY.get(entity_id)
    if proxy is None:
        return TriggerResult(
            False, f"no straddle proxy mapping for {entity_id}",
        )

    return TriggerResult(
        True,
        f"convergence on {entity_id} in "
        f"{chosen['estimated_convergence_days']:.0f}d → {proxy} straddle",
        {
            "entity_id": entity_id,
            "entity_type": chosen["entity_type"],
            "convergence_days": chosen["estimated_convergence_days"],
            "proxy": proxy,
        },
    )


def _convex_convergence_straddle_target(
    signals: Mapping[str, Any], trigger: Mapping[str, Any],
) -> SpreadSpec:
    return SpreadSpec(
        underlying=str(trigger["proxy"]),
        min_dte=21, max_dte=60,
        legs=(
            LegSpec(right="C", target_delta=0.50, is_long=True, name="long_atm_call"),
            LegSpec(right="P", target_delta=0.50, is_long=True, name="long_atm_put"),
        ),
    )


_CONVEX_CONVERGENCE_STRADDLE = TemplateConfig(
    name="convex.convergence_straddle",
    sleeve=Sleeve.CONVEX,
    trigger=_convex_convergence_straddle_trigger,
    spread_spec_factory=_convex_convergence_straddle_target,
    sizing_pct_of_sleeve=0.30,    # 30% of CONVEX sleeve = 1.5% of NAV
    max_concurrent=2,
    # Convergence bets can fire in any regime — the signals themselves
    # already encode timing. No regime gate.
    allowed_market_states=(),
    close_at_dte=14,
    profit_target_pct=1.5,        # 150% — straddles need bigger move
    stop_loss_multiplier=0.7,     # take loss at 70% of debit
    fallback_iv=0.30,
)


# ── Default sleeve set ───────────────────────────────────────────────


_DEFAULT_HEDGE = SleeveConfig(
    sleeve=Sleeve.HEDGE, nav_pct=0.10,
    templates=(
        _HEDGE_SPY_PROTECTIVE_PUT,
        _HEDGE_SECTOR_PUT_SPREAD,
        _HEDGE_VIX_TAIL_CALL,
        _HEDGE_COLLAR,
    ),
)

_DEFAULT_INCOME = SleeveConfig(
    sleeve=Sleeve.INCOME, nav_pct=0.15,
    templates=(
        _INCOME_SPY_SHORT_PUT,
        _INCOME_SPY_IRON_BUTTERFLY,
        _INCOME_SPY_IRON_CONDOR,
        _INCOME_COVERED_CALL,
    ),
)

_DEFAULT_CONVEX = SleeveConfig(
    sleeve=Sleeve.CONVEX, nav_pct=0.05,
    templates=(
        _CONVEX_THEMATIC_SECTOR_PUT,
        _CONVEX_VIX_ESCALATION_CALL,
        _CONVEX_CONVERGENCE_STRADDLE,
    ),
)


def default_sleeves() -> dict[Sleeve, SleeveConfig]:
    """Return the default seed set of three sleeves.

    Phases 2-4 expand each sleeve's template tuple with the full
    production templates (sector put spreads, iron butterflies, VIX
    escalation calls, etc.). For Phase 1 we ship one template per
    sleeve as a worked example.
    """
    return {
        Sleeve.HEDGE: _DEFAULT_HEDGE,
        Sleeve.INCOME: _DEFAULT_INCOME,
        Sleeve.CONVEX: _DEFAULT_CONVEX,
    }


__all__ = [
    "Sleeve",
    "TriggerResult",
    "TriggerFn",
    "TargetSpecFactory",
    "TemplateConfig",
    "SleeveConfig",
    "default_sleeves",
]
