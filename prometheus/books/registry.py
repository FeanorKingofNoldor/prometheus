"""Prometheus v2 – Book & sleeve registry.

A *book* is a macro objective. A *sleeve* is a concrete implementation
variant (knobs) inside a book.

The daily pipeline uses the registry to:
- interpret meta policy selections (book_id/sleeve_id),
- execute the chosen book+sleeve.

The registry is YAML-backed (configs/meta/books.yaml) with a conservative
in-code default fallback so that the system still runs if the YAML is
missing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

import yaml

from apatheon.core.logging import get_logger

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REGISTRY_PATH = PROJECT_ROOT / "configs" / "meta" / "books.yaml"


# ── Environment variable mapping for allocator risk controls ────────
# These override BookSpec fields on ALLOCATOR-kind books after YAML load.
_ALLOCATOR_RISK_ENV_OVERRIDES: dict[str, tuple[str, type]] = {
    "max_turnover_one_way": ("PROMETHEUS_MAX_TURNOVER", float),
    "drawdown_brake_threshold": ("PROMETHEUS_DRAWDOWN_BRAKE_THRESHOLD", float),
    "vol_target_annual": ("PROMETHEUS_VOL_TARGET_ANNUAL", float),
}


class BookKind(str, Enum):
    LONG_EQUITY = "LONG_EQUITY"
    HEDGE_ETF = "HEDGE_ETF"
    ALLOCATOR = "ALLOCATOR"
    CASH = "CASH"


@dataclass(frozen=True)
class LongEquitySleeveSpec:
    sleeve_id: str

    # Portfolio construction knobs (applied in BOOKS phase).
    portfolio_max_names: int | None = None
    portfolio_hysteresis_buffer: int | None = None
    portfolio_per_instrument_max_weight: float | None = None

    # Conviction-based position lifecycle.
    conviction_enabled: bool = False
    conviction_entry_credit: float | None = None
    conviction_build_rate: float | None = None
    conviction_decay_rate: float | None = None
    conviction_score_cap: float | None = None
    conviction_sell_threshold: float | None = None
    conviction_hard_stop_pct: float | None = None
    conviction_scale_up_days: int | None = None
    conviction_entry_weight_fraction: float | None = None

    # Score concentration: raise raw scores to this power before
    # normalising into weights. 1.0 = linear, 2.5 = strong concentration.
    score_concentration_power: float = 1.0

    # Book-level overlays.
    apply_fragility_overlay: bool = False

    # Market fragility overlay configuration (optional; defaults match the
    # legacy step rule when unset).
    fragility_overlay_mode: str | None = None  # step | ema_hysteresis | circuit_breaker

    # Step mode params.
    fragility_overlay_t1: float | None = None
    fragility_overlay_t2: float | None = None
    fragility_overlay_mid_mult: float | None = None
    fragility_overlay_high_mult: float | None = None

    # EMA+hysteresis params.
    fragility_overlay_ema_span: int | None = None
    fragility_overlay_trim_on: float | None = None
    fragility_overlay_trim_off: float | None = None
    fragility_overlay_off_on: float | None = None
    fragility_overlay_off_off: float | None = None

    # Optional start date used when reconstructing EMA from history.
    # Stored as a string to keep YAML parsing simple; overlay helpers will
    # parse it as YYYY-MM-DD.
    fragility_overlay_ema_history_start_date: str | None = None


@dataclass(frozen=True)
class HedgeEtfSleeveSpec:
    sleeve_id: str

    instrument_ids: tuple[str, ...]

    # Sizing knobs.
    sizing_mode: str = "regime_based"  # regime_based | fragility_linear
    max_hedge_allocation: float = 1.0

    # Used for fragility_linear sizing.
    fragility_threshold: float = 0.30

    # Rebalance schedule.
    rebalance_frequency: str = "daily"  # daily | weekly


@dataclass(frozen=True)
class AllocatorSleeveSpec:
    """Sleeve spec for a blended/allocator book.

    This sleeve produces a *single* master target portfolio by blending:
    - a long-only equity sleeve (constructed from the core universe), and
    - a hedge ETF sleeve (rule-based instruments such as SH.US/SDS.US).

    The hedge allocation is typically driven by fragility so it can act in
    advance, without requiring MarketSituation to flip first.
    """

    sleeve_id: str

    # Long-only portfolio construction knobs.
    portfolio_max_names: int | None = None
    portfolio_hysteresis_buffer: int | None = None
    portfolio_per_instrument_max_weight: float | None = None

    # Optional fragility overlay applied to the long leg.
    apply_fragility_overlay: bool = False

    # Overlay params (optional; same schema as LongEquitySleeveSpec).
    fragility_overlay_mode: str | None = None
    fragility_overlay_t1: float | None = None
    fragility_overlay_t2: float | None = None
    fragility_overlay_mid_mult: float | None = None
    fragility_overlay_high_mult: float | None = None
    fragility_overlay_ema_span: int | None = None
    fragility_overlay_trim_on: float | None = None
    fragility_overlay_trim_off: float | None = None
    fragility_overlay_off_on: float | None = None
    fragility_overlay_off_off: float | None = None
    fragility_overlay_ema_history_start_date: str | None = None

    # Hedge sleeve definition.
    hedge_instrument_ids: tuple[str, ...] = ()
    hedge_sizing_mode: str = "fragility_linear"  # fragility_linear | regime_based

    # Max hedge allocation as a *cap* on NAV (not a multiplier). Inverse ETFs
    # like SH.US can make the portfolio net short if this is too high.
    max_hedge_allocation: float = 0.5

    # Used for fragility_linear sizing.
    fragility_threshold: float = 0.30

    # Optional situation-based adjustments (keys are MarketSituation values,
    # e.g. CRISIS, RISK_OFF, RECOVERY, NEUTRAL, RISK_ON).
    hedge_allocation_overrides: dict[str, float] | None = None
    hedge_allocation_floors: dict[str, float] | None = None
    hedge_allocation_caps: dict[str, float] | None = None
    non_crisis_hedge_cap: float | None = None
    profitability_weight: float | None = None


SleeveSpec = LongEquitySleeveSpec | HedgeEtfSleeveSpec | AllocatorSleeveSpec


@dataclass(frozen=True)
class BookSpec:
    book_id: str
    kind: BookKind
    region: str
    market_id: str

    sleeves: dict[str, SleeveSpec]
    default_sleeve_id: str | None = None
    # Allocator-only extras (optional).
    situation_sleeve_map: dict[str, str] | None = None
    sleeve_transition_days: int | None = None
    max_turnover_one_way: float | None = None
    crisis_force_hedge_allocation: float | None = None
    drawdown_brake_threshold: float | None = None
    drawdown_brake_hedge_allocation: float | None = None
    vol_target_annual: float | None = None
    vol_target_lookback_days: int | None = None
    gate_csv_path: str | None = None

    def resolve_sleeve_id(self, sleeve_id: str | None) -> str | None:
        if sleeve_id and sleeve_id in self.sleeves:
            return sleeve_id
        if self.default_sleeve_id and self.default_sleeve_id in self.sleeves:
            return self.default_sleeve_id
        if self.sleeves:
            # Deterministic fallback: first key.
            return sorted(self.sleeves.keys())[0]
        return None


def _default_registry() -> dict[str, BookSpec]:
    """Return an in-code default registry for US_EQ.

    This is used when configs/meta/books.yaml is missing.
    """

    us_eq_long_sleeves = {
        "US_EQ_LONG_BASE_P10": LongEquitySleeveSpec(
            sleeve_id="US_EQ_LONG_BASE_P10",
            portfolio_max_names=10,
            portfolio_hysteresis_buffer=5,
            portfolio_per_instrument_max_weight=0.05,
            apply_fragility_overlay=False,
        ),
        "US_EQ_LONG_BASE_P15": LongEquitySleeveSpec(
            sleeve_id="US_EQ_LONG_BASE_P15",
            portfolio_max_names=15,
            portfolio_hysteresis_buffer=5,
            portfolio_per_instrument_max_weight=0.05,
            apply_fragility_overlay=False,
        ),
        "US_EQ_LONG_BASE_P20": LongEquitySleeveSpec(
            sleeve_id="US_EQ_LONG_BASE_P20",
            portfolio_max_names=20,
            portfolio_hysteresis_buffer=5,
            portfolio_per_instrument_max_weight=0.05,
            apply_fragility_overlay=False,
        ),
    }

    us_eq_long_def_sleeves = {
        k.replace("US_EQ_LONG_", "US_EQ_LONG_DEF_"): LongEquitySleeveSpec(
            sleeve_id=k.replace("US_EQ_LONG_", "US_EQ_LONG_DEF_"),
            portfolio_max_names=v.portfolio_max_names,
            portfolio_hysteresis_buffer=v.portfolio_hysteresis_buffer,
            portfolio_per_instrument_max_weight=v.portfolio_per_instrument_max_weight,
            apply_fragility_overlay=True,
        )
        for k, v in us_eq_long_sleeves.items()
    }

    us_eq_hedge_sleeves = {
        "US_EQ_HEDGE_SH": HedgeEtfSleeveSpec(
            sleeve_id="US_EQ_HEDGE_SH",
            instrument_ids=("SH.US",),
            sizing_mode="regime_based",
            max_hedge_allocation=1.0,
        ),
        "US_EQ_HEDGE_SDS": HedgeEtfSleeveSpec(
            sleeve_id="US_EQ_HEDGE_SDS",
            instrument_ids=("SDS.US",),
            sizing_mode="regime_based",
            max_hedge_allocation=1.0,
        ),
        "US_EQ_HEDGE_SH_VIXY": HedgeEtfSleeveSpec(
            sleeve_id="US_EQ_HEDGE_SH_VIXY",
            instrument_ids=("SH.US", "VIXY.US"),
            sizing_mode="regime_based",
            max_hedge_allocation=1.0,
        ),
    }

    return {
        "US_EQ_LONG": BookSpec(
            book_id="US_EQ_LONG",
            kind=BookKind.LONG_EQUITY,
            region="US",
            market_id="US_EQ",
            sleeves=us_eq_long_sleeves,
            default_sleeve_id="US_EQ_LONG_BASE_P10",
        ),
        "US_EQ_LONG_DEFENSIVE": BookSpec(
            book_id="US_EQ_LONG_DEFENSIVE",
            kind=BookKind.LONG_EQUITY,
            region="US",
            market_id="US_EQ",
            sleeves=us_eq_long_def_sleeves,
            default_sleeve_id="US_EQ_LONG_DEF_BASE_P10",
        ),
        "US_EQ_HEDGE_ETF": BookSpec(
            book_id="US_EQ_HEDGE_ETF",
            kind=BookKind.HEDGE_ETF,
            region="US",
            market_id="US_EQ",
            sleeves=us_eq_hedge_sleeves,
            default_sleeve_id="US_EQ_HEDGE_SH",
        ),
        "CASH": BookSpec(
            book_id="CASH",
            kind=BookKind.CASH,
            region="US",
            market_id="US_EQ",
            sleeves={},
            default_sleeve_id=None,
        ),
    }


def load_book_registry(path: str | Path | None = None) -> dict[str, BookSpec]:
    """Load the book registry from YAML.

    Args:
        path: Optional path to the registry file. Defaults to
            configs/meta/books.yaml.

    Returns:
        Mapping of book_id -> BookSpec.
    """

    cfg_path = Path(path) if path is not None else DEFAULT_REGISTRY_PATH
    if not cfg_path.exists():
        return _default_registry()

    raw = yaml.safe_load(cfg_path.read_text())
    if not isinstance(raw, Mapping):
        return _default_registry()

    books_raw = raw.get("books")
    if not isinstance(books_raw, Mapping):
        return _default_registry()

    out: dict[str, BookSpec] = {}

    for book_id, b in books_raw.items():
        if not isinstance(book_id, str) or not isinstance(b, Mapping):
            continue

        kind_raw = b.get("kind")
        try:
            kind = BookKind(str(kind_raw))
        except Exception:
            continue

        region = str(b.get("region", "")) or "US"
        market_id = str(b.get("market_id", "")) or "US_EQ"
        default_sleeve_id = b.get("default_sleeve_id")
        default_sleeve_id_s = str(default_sleeve_id) if isinstance(default_sleeve_id, str) else None

        # Allocator-only extras (optional).
        situation_map_raw = b.get("situation_sleeve_map")
        situation_map: dict[str, str] | None = None
        if isinstance(situation_map_raw, Mapping):
            tmp: dict[str, str] = {}
            for k, v in situation_map_raw.items():
                if isinstance(k, str) and isinstance(v, str) and k.strip() and v.strip():
                    tmp[k.strip().upper()] = v.strip()
            situation_map = tmp or None

        sleeve_transition_days = _coerce_int(b.get("sleeve_transition_days"))
        max_turnover_one_way = _coerce_float(b.get("max_turnover_one_way"))
        crisis_force_hedge_allocation = _coerce_float(b.get("crisis_force_hedge_allocation"))
        drawdown_brake_threshold = _coerce_float(b.get("drawdown_brake_threshold"))
        drawdown_brake_hedge_allocation = _coerce_float(b.get("drawdown_brake_hedge_allocation"))
        vol_target_annual = _coerce_float(b.get("vol_target_annual"))
        vol_target_lookback_days = _coerce_int(b.get("vol_target_lookback_days"))
        gate_csv_path = b.get("gate_csv_path")
        gate_csv_path_s = str(gate_csv_path) if isinstance(gate_csv_path, str) and gate_csv_path.strip() else None

        sleeves: dict[str, SleeveSpec] = {}
        sleeves_raw = b.get("sleeves")
        if isinstance(sleeves_raw, Mapping):
            for sid, s in sleeves_raw.items():
                if not isinstance(sid, str) or not isinstance(s, Mapping):
                    continue

                if kind == BookKind.LONG_EQUITY:
                    mode_raw = s.get("fragility_overlay_mode")
                    mode = str(mode_raw).strip() if isinstance(mode_raw, str) and mode_raw.strip() else None

                    start_raw = s.get("fragility_overlay_ema_history_start_date")
                    start_date_s = (
                        str(start_raw).strip()
                        if isinstance(start_raw, str) and str(start_raw).strip()
                        else None
                    )

                    sleeves[sid] = LongEquitySleeveSpec(
                        sleeve_id=sid,
                        portfolio_max_names=_coerce_int(s.get("portfolio_max_names")),
                        portfolio_hysteresis_buffer=_coerce_int(s.get("portfolio_hysteresis_buffer")),
                        portfolio_per_instrument_max_weight=_coerce_float(
                            s.get("portfolio_per_instrument_max_weight")
                        ),
                        conviction_enabled=bool(s.get("conviction_enabled", False)),
                        conviction_entry_credit=_coerce_float(s.get("conviction_entry_credit")),
                        conviction_build_rate=_coerce_float(s.get("conviction_build_rate")),
                        conviction_decay_rate=_coerce_float(s.get("conviction_decay_rate")),
                        conviction_score_cap=_coerce_float(s.get("conviction_score_cap")),
                        conviction_sell_threshold=_coerce_float(s.get("conviction_sell_threshold")),
                        conviction_hard_stop_pct=_coerce_float(s.get("conviction_hard_stop_pct")),
                        conviction_scale_up_days=_coerce_int(s.get("conviction_scale_up_days")),
                        conviction_entry_weight_fraction=_coerce_float(s.get("conviction_entry_weight_fraction")),
                        score_concentration_power=float(
                            _coerce_float(s.get("score_concentration_power")) or 1.0
                        ),
                        apply_fragility_overlay=bool(s.get("apply_fragility_overlay", False)),
                        fragility_overlay_mode=mode,
                        fragility_overlay_t1=_coerce_float(s.get("fragility_overlay_t1")),
                        fragility_overlay_t2=_coerce_float(s.get("fragility_overlay_t2")),
                        fragility_overlay_mid_mult=_coerce_float(s.get("fragility_overlay_mid_mult")),
                        fragility_overlay_high_mult=_coerce_float(s.get("fragility_overlay_high_mult")),
                        fragility_overlay_ema_span=_coerce_int(s.get("fragility_overlay_ema_span")),
                        fragility_overlay_trim_on=_coerce_float(s.get("fragility_overlay_trim_on")),
                        fragility_overlay_trim_off=_coerce_float(s.get("fragility_overlay_trim_off")),
                        fragility_overlay_off_on=_coerce_float(s.get("fragility_overlay_off_on")),
                        fragility_overlay_off_off=_coerce_float(s.get("fragility_overlay_off_off")),
                        fragility_overlay_ema_history_start_date=start_date_s,
                    )
                elif kind == BookKind.HEDGE_ETF:
                    inst_raw = s.get("instrument_ids")
                    inst_ids = _coerce_str_tuple(inst_raw)
                    if not inst_ids:
                        continue
                    sleeves[sid] = HedgeEtfSleeveSpec(
                        sleeve_id=sid,
                        instrument_ids=inst_ids,
                        sizing_mode=str(s.get("sizing_mode", "regime_based")),
                        max_hedge_allocation=float(_coerce_float(s.get("max_hedge_allocation")) or 0.0),
                        fragility_threshold=float(_coerce_float(s.get("fragility_threshold")) or 0.30),
                        rebalance_frequency=str(s.get("rebalance_frequency", "daily")),
                    )
                elif kind == BookKind.ALLOCATOR:
                    hedge_raw = s.get("hedge_instrument_ids") or s.get("instrument_ids")
                    hedge_ids = _coerce_str_tuple(hedge_raw)
                    if not hedge_ids:
                        continue

                    mode_raw = s.get("fragility_overlay_mode")
                    mode = str(mode_raw).strip() if isinstance(mode_raw, str) and mode_raw.strip() else None

                    start_raw = s.get("fragility_overlay_ema_history_start_date")
                    start_date_s = (
                        str(start_raw).strip()
                        if isinstance(start_raw, str) and str(start_raw).strip()
                        else None
                    )

                    sizing_mode = s.get("hedge_sizing_mode") or s.get("sizing_mode")
                    sizing_mode_s = str(sizing_mode) if isinstance(sizing_mode, str) and sizing_mode.strip() else "fragility_linear"

                    sleeves[sid] = AllocatorSleeveSpec(
                        sleeve_id=sid,
                        portfolio_max_names=_coerce_int(s.get("portfolio_max_names")),
                        portfolio_hysteresis_buffer=_coerce_int(s.get("portfolio_hysteresis_buffer")),
                        portfolio_per_instrument_max_weight=_coerce_float(
                            s.get("portfolio_per_instrument_max_weight")
                        ),
                        apply_fragility_overlay=bool(s.get("apply_fragility_overlay", False)),
                        fragility_overlay_mode=mode,
                        fragility_overlay_t1=_coerce_float(s.get("fragility_overlay_t1")),
                        fragility_overlay_t2=_coerce_float(s.get("fragility_overlay_t2")),
                        fragility_overlay_mid_mult=_coerce_float(s.get("fragility_overlay_mid_mult")),
                        fragility_overlay_high_mult=_coerce_float(s.get("fragility_overlay_high_mult")),
                        fragility_overlay_ema_span=_coerce_int(s.get("fragility_overlay_ema_span")),
                        fragility_overlay_trim_on=_coerce_float(s.get("fragility_overlay_trim_on")),
                        fragility_overlay_trim_off=_coerce_float(s.get("fragility_overlay_trim_off")),
                        fragility_overlay_off_on=_coerce_float(s.get("fragility_overlay_off_on")),
                        fragility_overlay_off_off=_coerce_float(s.get("fragility_overlay_off_off")),
                        fragility_overlay_ema_history_start_date=start_date_s,
                        hedge_instrument_ids=hedge_ids,
                        hedge_sizing_mode=sizing_mode_s,
                        max_hedge_allocation=float(_coerce_float(s.get("max_hedge_allocation")) or 0.5),
                        fragility_threshold=float(_coerce_float(s.get("fragility_threshold")) or 0.30),
                        hedge_allocation_overrides=_coerce_str_float_dict(s.get("hedge_allocation_overrides")),
                        hedge_allocation_floors=_coerce_str_float_dict(s.get("hedge_allocation_floors")),
                        hedge_allocation_caps=_coerce_str_float_dict(s.get("hedge_allocation_caps")),
                        non_crisis_hedge_cap=_coerce_float(s.get("non_crisis_hedge_cap")),
                        profitability_weight=_coerce_float(s.get("profitability_weight")),
                    )
                elif kind == BookKind.CASH:
                    sleeves = {}

        out[book_id] = BookSpec(
            book_id=book_id,
            kind=kind,
            region=region,
            market_id=market_id,
            sleeves=sleeves,
            default_sleeve_id=default_sleeve_id_s,
            situation_sleeve_map=situation_map,
            sleeve_transition_days=sleeve_transition_days,
            max_turnover_one_way=max_turnover_one_way,
            crisis_force_hedge_allocation=crisis_force_hedge_allocation,
            drawdown_brake_threshold=drawdown_brake_threshold,
            drawdown_brake_hedge_allocation=drawdown_brake_hedge_allocation,
            vol_target_annual=vol_target_annual,
            vol_target_lookback_days=vol_target_lookback_days,
            gate_csv_path=gate_csv_path_s,
        )

    result = out or _default_registry()

    # ── Apply env var overrides for allocator risk controls ──────────
    result = _apply_allocator_env_overrides(result)

    return result


def _apply_allocator_env_overrides(
    registry: dict[str, BookSpec],
) -> dict[str, BookSpec]:
    """Apply environment variable overrides to ALLOCATOR-kind books.

    Only fields listed in ``_ALLOCATOR_RISK_ENV_OVERRIDES`` are affected.
    Non-ALLOCATOR books are passed through unchanged.
    """
    overrides: dict[str, Any] = {}
    for field_name, (env_var, field_type) in _ALLOCATOR_RISK_ENV_OVERRIDES.items():
        env_val = os.environ.get(env_var)
        if env_val is not None:
            try:
                overrides[field_name] = field_type(env_val)
                logger.info(
                    "Allocator risk control override: %s=%s (from %s)",
                    field_name, overrides[field_name], env_var,
                )
            except (ValueError, TypeError) as exc:
                logger.warning("Invalid env override %s=%r: %s", env_var, env_val, exc)

    if not overrides:
        return registry

    out: dict[str, BookSpec] = {}
    for book_id, spec in registry.items():
        if spec.kind == BookKind.ALLOCATOR:
            out[book_id] = replace(spec, **overrides)
        else:
            out[book_id] = spec
    return out


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        iv = int(value)
    except Exception:
        return None
    return iv


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        fv = float(value)
    except Exception:
        return None
    return fv


def _coerce_str_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for x in value:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return tuple(out)
    return ()


def _coerce_str_float_dict(value: Any) -> dict[str, float] | None:
    """Parse a YAML mapping into a {str: float} dictionary.

    Returns None if the input is empty or not a mapping.
    """

    if value is None:
        return None
    if not isinstance(value, Mapping):
        return None

    out: dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k.strip():
            continue
        fv = _coerce_float(v)
        if fv is None:
            continue
        out[k.strip()] = float(fv)

    return out or None
