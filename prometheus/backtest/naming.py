"""Backtest namespace enforcement.

Live and backtest rows share the same tables (instrument_scores,
universe_members, target_portfolios, ...); the ONLY isolation is the
id. Backtests that write under a live id poison every calibration query
that filters by id — a 2026 audit found 6.8M backtest instrument_scores
rows under the live ``US_CORE_LONG_EQ`` strategy id spanning 1997-2026.

Every backtest entry point must run its strategy/portfolio/universe ids
through :func:`assert_backtest_namespace` before wiring engines.
"""

from __future__ import annotations

import os

# Prefixes that unambiguously mark a row as non-live. Legacy experiment
# families are grandfathered so their historical runs stay addressable.
ALLOWED_BACKTEST_PREFIXES: tuple[str, ...] = (
    "BT_",
    "CPP_",
    "LAMBDA_",
    "LFSWP_",
    "PERF_TEST",
    "TEST_",
    "SYNTH_",
)

# Ids the live daemon writes under — a backtest must never use these.
LIVE_IDS: tuple[str, ...] = (
    "US_CORE_LONG_EQ",
    "US_EQ_LONG_V12",
    "IBKR_PAPER",
    "IBKR_LIVE",
    "CORE_EQ_US",
)

_OVERRIDE_ENV = "PROMETHEUS_ALLOW_LIVE_NAMESPACE_BACKTEST"


class BacktestNamespaceError(ValueError):
    """A backtest tried to write under a live (or unprefixed) id."""


def assert_backtest_namespace(*ids: str | None) -> None:
    """Reject backtest ids that collide with (or shadow) the live namespace.

    Rules:
    - live ids are always rejected (no override);
    - other ids must carry an allowed backtest prefix, unless the
      explicit escape hatch env var is set (for deliberate replay
      experiments — use sparingly, the id still lands in shared tables).
    """
    for id_ in ids:
        if id_ is None or id_ == "":
            continue
        upper = str(id_).upper()
        if upper in LIVE_IDS:
            raise BacktestNamespaceError(
                f"backtest id {id_!r} is a LIVE id — backtests must never "
                f"write under live ids (they share tables with production; "
                f"use a BT_-prefixed id instead)"
            )
        if any(upper.startswith(p) for p in ALLOWED_BACKTEST_PREFIXES):
            continue
        if os.environ.get(_OVERRIDE_ENV, "").strip().lower() in ("1", "true", "yes"):
            continue
        raise BacktestNamespaceError(
            f"backtest id {id_!r} lacks a backtest prefix "
            f"({', '.join(ALLOWED_BACKTEST_PREFIXES)}); prefix it with BT_ "
            f"or set {_OVERRIDE_ENV}=1 for a deliberate exception"
        )
