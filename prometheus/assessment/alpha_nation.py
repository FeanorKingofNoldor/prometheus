"""Prometheus v2 – Nation Alpha Provider.

Computes a per-instrument alpha component derived from nation-level
scores.  This is registered as ``alpha_components["nation"]`` in the
Assessment Engine's combination layer.

The alpha signal captures:
- Policy direction alignment (easing monetary → positive for equities)
- Economic stability trend (improving → positive)
- Composite risk level (higher stability → positive)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from apathis.core.logging import get_logger
from apathis.nation.storage import NationScoreStorage
from apathis.nation.types import NationScores

logger = get_logger(__name__)


def _clamp(v: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


@dataclass
class NationAlphaProvider:
    """Computes nation-level alpha for instruments.

    For US EQ, all instruments share the same nation alpha (since they're
    all US-domiciled equities).  When multi-nation support is added, this
    would look up the instrument's country_of_risk.

    The output is a score in [-1, +1] where:
    - Positive: nation conditions favour equity longs
    - Negative: nation conditions suggest caution / shorts
    """

    score_storage: NationScoreStorage
    nation: str = "USA"

    # Weight blend between components.
    policy_weight: float = 0.40
    econ_weight: float = 0.30
    composite_weight: float = 0.30

    def compute(self, instrument_id: str, as_of_date: date) -> float:
        """Compute nation alpha for an instrument.

        Currently ignores ``instrument_id`` since all are US EQ. Returns
        0.0 if no nation scores are available.
        """

        scores = self.score_storage.get_latest(self.nation, as_of_date=as_of_date)
        if scores is None:
            return 0.0

        return self._alpha_from_scores(scores)

    def compute_batch(self, as_of_date: date) -> float:
        """Compute a single nation alpha for the batch date.

        Since all US EQ instruments share the same nation alpha, callers
        can use this to avoid repeated DB lookups.
        """

        scores = self.score_storage.get_latest(self.nation, as_of_date=as_of_date)
        if scores is None:
            return 0.0

        return self._alpha_from_scores(scores)

    def _alpha_from_scores(self, scores: NationScores) -> float:
        """Derive alpha from nation scores."""

        # Policy component: easing (negative monetary) → positive for equities.
        # Stimulus (positive fiscal) → positive for equities.
        monetary_signal = -scores.policy_direction.monetary  # easing = positive
        fiscal_signal = scores.policy_direction.fiscal * 0.5
        policy_alpha = _clamp(monetary_signal + fiscal_signal)

        # Economic stability component: above 0.5 → positive.
        econ_alpha = _clamp((scores.economic_stability - 0.5) * 2.0)

        # Composite risk component: above 0.5 → positive.
        composite_alpha = _clamp((scores.composite_risk - 0.5) * 2.0)

        alpha = (
            policy_alpha * self.policy_weight
            + econ_alpha * self.econ_weight
            + composite_alpha * self.composite_weight
        )

        return _clamp(alpha)
