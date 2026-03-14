"""Prometheus v2 – Meta policy artifact.

The meta policy maps a MarketSituation to a chosen {book_id, sleeve_id}.

This module is intentionally simple:
- Offline training can write a YAML artifact.
- Online (daily) pipeline loads it and routes accordingly.

Schema (configs/meta/policy.yaml):

  policies:
    US_EQ:
      default:
        book_id: US_EQ_LONG
        sleeve_id: US_EQ_LONG_BASE_P10
      situations:
        RISK_ON: {book_id: US_EQ_LONG, sleeve_id: US_EQ_LONG_BASE_P10}
        NEUTRAL: {book_id: US_EQ_LONG, sleeve_id: US_EQ_LONG_BASE_P10}
        RISK_OFF: {book_id: US_EQ_LONG_DEFENSIVE, sleeve_id: US_EQ_LONG_DEF_BASE_P10}
        CRISIS: {book_id: US_EQ_HEDGE_ETF, sleeve_id: US_EQ_HEDGE_SH}
        RECOVERY: {book_id: US_EQ_LONG_DEFENSIVE, sleeve_id: US_EQ_LONG_DEF_BASE_P10}

If the file is missing or malformed, a conservative in-code default is
used.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from prometheus.meta.market_situation import MarketSituation


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = PROJECT_ROOT / "configs" / "meta" / "policy.yaml"


@dataclass(frozen=True)
class MetaPolicySelection:
    book_id: str
    sleeve_id: str | None = None


@dataclass(frozen=True)
class MetaPolicy:
    market_id: str
    default: MetaPolicySelection
    by_situation: dict[MarketSituation, MetaPolicySelection]

    def select(self, situation: MarketSituation) -> MetaPolicySelection:
        return self.by_situation.get(situation, self.default)


@dataclass(frozen=True)
class MetaPolicyArtifact:
    """Loaded meta policy artifact.

    The YAML file may include optional top-level metadata fields such as
    ``version`` or ``updated_at``. These are preserved here for
    observability/auditing in engine decisions.
    """

    policies: dict[str, MetaPolicy]
    version: str | None = None
    updated_at: str | None = None
    updated_by: str | None = None


def _default_policy() -> dict[str, MetaPolicy]:
    """Return a conservative default policy mapping for US_EQ."""

    us_default = MetaPolicySelection(book_id="US_EQ_LONG", sleeve_id="US_EQ_LONG_BASE_P10")

    return {
        "US_EQ": MetaPolicy(
            market_id="US_EQ",
            default=us_default,
            by_situation={
                MarketSituation.RISK_ON: MetaPolicySelection(
                    book_id="US_EQ_LONG", sleeve_id="US_EQ_LONG_BASE_P10"
                ),
                MarketSituation.NEUTRAL: MetaPolicySelection(
                    book_id="US_EQ_LONG", sleeve_id="US_EQ_LONG_BASE_P10"
                ),
                MarketSituation.RISK_OFF: MetaPolicySelection(
                    book_id="US_EQ_LONG_DEFENSIVE", sleeve_id="US_EQ_LONG_DEF_BASE_P10"
                ),
                MarketSituation.CRISIS: MetaPolicySelection(
                    book_id="US_EQ_HEDGE_ETF", sleeve_id="US_EQ_HEDGE_SH"
                ),
                MarketSituation.RECOVERY: MetaPolicySelection(
                    book_id="US_EQ_LONG_DEFENSIVE", sleeve_id="US_EQ_LONG_DEF_BASE_P10"
                ),
            },
        )
    }


def load_meta_policy_artifact(path: str | Path | None = None) -> MetaPolicyArtifact:
    """Load the meta policy artifact.

    Returns a :class:`MetaPolicyArtifact` containing both the parsed policy
    mapping and any optional top-level metadata fields.
    """

    cfg_path = Path(path) if path is not None else DEFAULT_POLICY_PATH
    if not cfg_path.exists():
        return MetaPolicyArtifact(policies=_default_policy())

    raw = yaml.safe_load(cfg_path.read_text())
    if not isinstance(raw, Mapping):
        return MetaPolicyArtifact(policies=_default_policy())

    policies_raw = raw.get("policies")
    if not isinstance(policies_raw, Mapping):
        return MetaPolicyArtifact(policies=_default_policy())

    version_raw = raw.get("version")
    updated_at_raw = raw.get("updated_at")
    updated_by_raw = raw.get("updated_by")

    version = str(version_raw) if isinstance(version_raw, str) and version_raw.strip() else None
    updated_at = (
        str(updated_at_raw) if isinstance(updated_at_raw, str) and updated_at_raw.strip() else None
    )
    updated_by = (
        str(updated_by_raw) if isinstance(updated_by_raw, str) and updated_by_raw.strip() else None
    )

    out: dict[str, MetaPolicy] = {}

    for market_id, p in policies_raw.items():
        if not isinstance(market_id, str) or not isinstance(p, Mapping):
            continue

        default_sel = _parse_selection(p.get("default"))
        if default_sel is None:
            continue

        by_situation: dict[MarketSituation, MetaPolicySelection] = {}
        situations_raw = p.get("situations")
        if isinstance(situations_raw, Mapping):
            for sit_key, sel_raw in situations_raw.items():
                try:
                    sit = MarketSituation(str(sit_key))
                except Exception:
                    continue
                sel = _parse_selection(sel_raw)
                if sel is None:
                    continue
                by_situation[sit] = sel

        out[str(market_id).upper()] = MetaPolicy(
            market_id=str(market_id).upper(),
            default=default_sel,
            by_situation=by_situation,
        )

    policies = out or _default_policy()

    return MetaPolicyArtifact(
        policies=policies,
        version=version,
        updated_at=updated_at,
        updated_by=updated_by,
    )


def load_meta_policies(path: str | Path | None = None) -> dict[str, MetaPolicy]:
    """Load meta policies keyed by market_id.

    Backwards-compatible convenience wrapper around
    :func:`load_meta_policy_artifact`.
    """

    return load_meta_policy_artifact(path).policies


def _parse_selection(raw: Any) -> MetaPolicySelection | None:
    if not isinstance(raw, Mapping):
        return None
    book_id = raw.get("book_id")
    if not isinstance(book_id, str) or not book_id.strip():
        return None
    sleeve_id = raw.get("sleeve_id")
    sleeve_id_s = str(sleeve_id) if isinstance(sleeve_id, str) and sleeve_id.strip() else None
    return MetaPolicySelection(book_id=book_id.strip(), sleeve_id=sleeve_id_s)
