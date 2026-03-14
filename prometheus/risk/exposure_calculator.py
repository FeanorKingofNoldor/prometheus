"""Prometheus v2 – Exposure calculator helpers.

This module contains small helpers for computing portfolio exposures.
The first iteration provides a minimal gross-exposure calculator that
can be extended with sector/factor breakdowns later.
"""

from __future__ import annotations

from typing import Dict


def compute_gross_exposure(weights: Dict[str, float]) -> float:
    """Return gross exposure given a mapping of instrument weights.

    This assumes that ``weights`` are expressed as portfolio weights
    (i.e. summing approximately to 1.0 for a fully-invested portfolio).
    """

    return float(sum(abs(w) for w in weights.values()))
