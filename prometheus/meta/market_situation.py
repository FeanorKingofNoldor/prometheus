"""Prometheus v2 – Canonical market situation labels.

This module defines a single, canonical MarketSituation label per day,
derived from existing signals:
- RegimeLabel (CARRY/NEUTRAL/RISK_OFF/CRISIS)
- Market fragility score

The intent is to provide a stable abstraction for:
- situation-conditioned evaluation in Meta,
- online (daily) routing of book+sleeve selections.

Decision timing convention:
- situation is computed using signals available as-of close[t]
- allocations are assumed to execute at open[t+1]

The mapping is deliberately simple in v1 and can be refined with richer
signals (breadth, volatility term structure, credit, etc.) later.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Dict

import yaml

from apatheon.core.database import DatabaseManager
from apatheon.core.logging import get_logger
from apatheon.core.markets import infer_region_from_market_id
from apatheon.fragility.storage import FragilityStorage
from apatheon.regime.storage import RegimeStorage
from apatheon.regime.types import RegimeLabel

logger = get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MARKET_SITUATION_CONFIG_PATH = PROJECT_ROOT / "configs" / "meta" / "market_situation.yaml"


class MarketSituation(str, Enum):
    RISK_ON = "RISK_ON"
    NEUTRAL = "NEUTRAL"
    RISK_OFF = "RISK_OFF"
    CRISIS = "CRISIS"
    RECOVERY = "RECOVERY"


@dataclass(frozen=True)
class MarketSituationConfig:
    # When market fragility is above this threshold *and* the regime is not
    # explicitly RISK_OFF/CRISIS, we treat the environment as RECOVERY.
    recovery_fragility_threshold: float = 0.30

    # Optional override: extremely high fragility forces CRISIS even if the
    # regime detector has not flipped yet.
    crisis_fragility_override_threshold: float = 0.75

    # If True, prefer to label RECOVERY only when transitioning out of a
    # stressed regime (RISK_OFF/CRISIS) into NEUTRAL/CARRY, *and* fragility
    # remains elevated.
    recovery_requires_stress_transition: bool = False


# ── Environment variable mapping for the most critical parameters ────
_MARKET_SITUATION_ENV_OVERRIDES: Dict[str, tuple[str, type]] = {
    "recovery_fragility_threshold": ("PROMETHEUS_RECOVERY_FRAGILITY_THRESHOLD", float),
    "crisis_fragility_override_threshold": ("PROMETHEUS_CRISIS_FRAGILITY_OVERRIDE_THRESHOLD", float),
}


def load_market_situation_config(
    path: str | Path | None = None,
) -> MarketSituationConfig:
    """Load a :class:`MarketSituationConfig` from YAML + env overrides.

    Resolution order (last wins):
    1. Dataclass defaults
    2. YAML file at *path* (or ``configs/meta/market_situation.yaml`` if exists)
    3. Environment variable overrides for critical parameters

    If the YAML file does not exist or is malformed, the dataclass defaults
    are used without error.
    """
    cfg_path = Path(path) if path is not None else DEFAULT_MARKET_SITUATION_CONFIG_PATH
    kwargs: Dict[str, Any] = {}

    # ── Step 1: Load from YAML if available ──────────────────────────
    if cfg_path.exists():
        try:
            raw = yaml.safe_load(cfg_path.read_text())
            if isinstance(raw, dict):
                valid_fields = {f.name for f in MarketSituationConfig.__dataclass_fields__.values()}
                for key, value in raw.items():
                    if key in valid_fields and value is not None:
                        kwargs[key] = value
                logger.info("Loaded market situation config from %s (%d fields)", cfg_path, len(kwargs))
        except Exception as exc:
            logger.warning("Failed to load market situation config from %s: %s", cfg_path, exc)

    # ── Step 2: Environment variable overrides ───────────────────────
    for field_name, (env_var, field_type) in _MARKET_SITUATION_ENV_OVERRIDES.items():
        env_val = os.environ.get(env_var)
        if env_val is not None:
            try:
                kwargs[field_name] = field_type(env_val)
                logger.info("Market situation config override: %s=%s (from %s)", field_name, kwargs[field_name], env_var)
            except (ValueError, TypeError) as exc:
                logger.warning("Invalid env override %s=%r: %s", env_var, env_val, exc)

    return MarketSituationConfig(**kwargs)


@dataclass(frozen=True)
class MarketSituationInfo:
    as_of_date: date
    market_id: str
    region: str | None
    situation: MarketSituation

    # Raw inputs (best-effort; may be None if unavailable).
    regime_label: RegimeLabel | None
    regime_confidence: float | None
    prev_regime_label: RegimeLabel | None

    fragility_score: float | None
    fragility_class: str | None
    fragility_as_of: date | None


def classify_market_situation(
    *,
    regime_label: RegimeLabel | None,
    prev_regime_label: RegimeLabel | None,
    fragility_score: float | None,
    config: MarketSituationConfig = MarketSituationConfig(),
) -> MarketSituation:
    """Classify a market situation from regime + fragility.

    This is a pure function intended for unit testing.
    """

    if fragility_score is not None and fragility_score >= config.crisis_fragility_override_threshold:
        return MarketSituation.CRISIS

    if regime_label == RegimeLabel.CRISIS:
        return MarketSituation.CRISIS
    if regime_label == RegimeLabel.RISK_OFF:
        return MarketSituation.RISK_OFF

    elevated = fragility_score is not None and fragility_score >= config.recovery_fragility_threshold

    if config.recovery_requires_stress_transition:
        transitioned = (
            prev_regime_label in {RegimeLabel.CRISIS, RegimeLabel.RISK_OFF}
            and regime_label in {RegimeLabel.NEUTRAL, RegimeLabel.CARRY}
        )
        if transitioned and elevated:
            return MarketSituation.RECOVERY
    else:
        if regime_label in {RegimeLabel.NEUTRAL, RegimeLabel.CARRY} and elevated:
            return MarketSituation.RECOVERY

    if regime_label == RegimeLabel.CARRY:
        return MarketSituation.RISK_ON

    # Default: treat unknown or NEUTRAL regimes as NEUTRAL.
    return MarketSituation.NEUTRAL


@dataclass
class MarketSituationService:
    """DB-backed helper for computing MarketSituationInfo per date."""

    db_manager: DatabaseManager
    config: MarketSituationConfig = MarketSituationConfig()

    def get_situation(
        self,
        *,
        market_id: str,
        as_of_date: date,
        region: str | None = None,
    ) -> MarketSituationInfo:
        region_eff = region.upper() if isinstance(region, str) and region else infer_region_from_market_id(market_id)

        regime_label: RegimeLabel | None = None
        regime_conf: float | None = None
        prev_regime_label: RegimeLabel | None = None

        if region_eff:
            try:
                storage = RegimeStorage(db_manager=self.db_manager)
                state = storage.get_latest_regime(region_eff, as_of_date=as_of_date, inclusive=True)
                if state is not None:
                    regime_label = state.regime_label
                    try:
                        regime_conf = float(state.confidence)
                    except Exception:
                        regime_conf = None

                prev = storage.get_latest_regime(region_eff, as_of_date=as_of_date, inclusive=False)
                if prev is not None:
                    prev_regime_label = prev.regime_label
            except Exception:
                # Best-effort; situations can still be inferred from fragility.
                regime_label = None
                regime_conf = None
                prev_regime_label = None

        fragility_score: float | None = None
        fragility_class: str | None = None
        fragility_as_of: date | None = None

        try:
            frag_storage = FragilityStorage(db_manager=self.db_manager)
            measure = frag_storage.get_latest_measure("MARKET", market_id, as_of_date=as_of_date)
            if measure is not None:
                fragility_score = float(measure.fragility_score)
                fragility_class = str(measure.class_label.value)
                fragility_as_of = measure.as_of_date
        except Exception:
            fragility_score = None
            fragility_class = None
            fragility_as_of = None

        situation = classify_market_situation(
            regime_label=regime_label,
            prev_regime_label=prev_regime_label,
            fragility_score=fragility_score,
            config=self.config,
        )

        return MarketSituationInfo(
            as_of_date=as_of_date,
            market_id=str(market_id),
            region=region_eff,
            situation=situation,
            regime_label=regime_label,
            regime_confidence=regime_conf,
            prev_regime_label=prev_regime_label,
            fragility_score=fragility_score,
            fragility_class=fragility_class,
            fragility_as_of=fragility_as_of,
        )
