"""Prometheus v2 – Assessment Engine configuration models.

This module defines a small Pydantic model describing configuration for
Assessment Engine instances. For Iteration 4 the config is not yet wired
into the main pipeline but provides a typed structure aligned with the
130_assessment_engine spec.
"""

from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel


class AssessmentConfig(BaseModel):
    """Configuration for an Assessment Engine instance.

    Attributes:
        strategy_id: Strategy identifier this config applies to.
        markets: List of market identifiers (e.g. ["US_EQ"]).
        horizons_days: List of horizons in trading days to score.
        base_model_id: Identifier of the primary assessment model.
        alpha_family_models: Mapping from alpha family name to model id
            (e.g. {"value": "value-v1", "momentum": "mom-v1"}).
        use_fragility_penalty: Whether to apply fragility penalties from
            STAB / Fragility Alpha.
        max_soft_target_exposure: Optional cap on exposure to high
            soft-target scores for this strategy.
        feature_spec_id: Identifier of the feature specification used for
            training and inference.
    """

    strategy_id: str
    markets: List[str]
    horizons_days: List[int]
    base_model_id: str
    alpha_family_models: Dict[str, str]
    use_fragility_penalty: bool = True
    max_soft_target_exposure: float = 0.0
    feature_spec_id: str
