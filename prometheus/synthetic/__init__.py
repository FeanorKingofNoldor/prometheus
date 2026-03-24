"""Prometheus v2 – Synthetic Scenario Engine package.

This package exposes types and helpers for generating synthetic
stress scenarios and full market realities used by Portfolio & Risk,
Stability, Meta-Orchestrator, and C++ backtester components.
"""

from .engine import SyntheticScenarioEngine
from .storage import ScenarioPathRow, ScenarioStorage
from .types import RealityConfig, ScenarioRequest, ScenarioSetRef, SyntheticReality
