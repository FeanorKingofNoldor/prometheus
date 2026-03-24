"""Prometheus v2 – Engine run daemon.

This module implements a minimal long-running engine orchestrator process
that wraps the ``engine_runs`` state machine.

Version 0 of the daemon deliberately focuses on a **single concern**:
periodically advancing all active runs using the same helpers as the
``run_engine_state`` CLI. It does **not** attempt to:

- detect when data ingestion has completed, or
- mark runs ``DATA_READY``.

Those responsibilities remain with ingestion jobs and the
``prometheus.scripts.run_engine_state`` entrypoint as described in
``docs/dev_workflows_engine_runs_orchestration.md``.

Over time, this daemon can be extended to:

- coordinate with ingestion and calendars to mark runs ``DATA_READY`` once
  inputs are complete, and
- use market-aware scheduling as per
  ``docs/specs/013_orchestration_and_dags.md``.

The key design constraint is that this daemon must **not** contain business
logic about phases or risk; it should remain a thin orchestration layer atop
``prometheus.pipeline.state`` and ``prometheus.pipeline.tasks``.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence

from apathis.core.database import get_db_manager
from apathis.core.logging import get_logger

from prometheus.pipeline.state import EngineRun, EngineRunStateError, list_active_runs
from prometheus.pipeline.tasks import advance_run

logger = get_logger(__name__)


# ============================================================================
# Configuration
# ============================================================================


@dataclass(frozen=True)
class EngineDaemonConfig:
    """Configuration for the engine run daemon.

    Attributes:
        poll_interval_seconds: Sleep interval between polling cycles.
        regions: Optional list of region codes this daemon is responsible
            for. Currently used only for logging/metrics; the actual
            filtering is delegated to ``list_active_runs``.
    """

    poll_interval_seconds: int = 60
    regions: List[str] | None = None


# ============================================================================
# Core loop
# ============================================================================


def _advance_all_once() -> None:
    """Advance all active engine runs by one phase.

    This mirrors the behaviour of ``run_engine_state --advance-all`` but is
    designed to be called repeatedly from a long-running process.
    """

    db_manager = get_db_manager()
    runs = list_active_runs(db_manager)
    if not runs:
        logger.info("engine_daemon: no active engine runs found")
        return

    logger.info("engine_daemon: advancing %d active engine runs", len(runs))

    for run in runs:
        _log_run_state("before", run)
        try:
            new_run = advance_run(db_manager, run)
        except EngineRunStateError as exc:  # pragma: no cover - defensive
            logger.error("engine_daemon: state error for run_id=%s: %s", run.run_id, exc)
            continue

        _log_run_state("after", new_run)


def _log_run_state(prefix: str, run: EngineRun) -> None:
    """Log a compact summary of an EngineRun state change."""

    logger.info(
        "engine_daemon:%s run_id=%s as_of=%s region=%s phase=%s",
        prefix,
        run.run_id,
        run.as_of_date,
        run.region,
        run.phase.value,
    )


def run_daemon(config: EngineDaemonConfig) -> None:
    """Run the engine daemon loop until interrupted.

    The daemon performs a simple cycle:

    1. Call :func:`_advance_all_once` to advance all active runs.
    2. Sleep for ``config.poll_interval_seconds``.

    This is safe to run alongside cron- or systemd-based calls to
    ``run_engine_state``; both use the same underlying state machine and are
    idempotent.
    """

    poll_interval = max(1, int(config.poll_interval_seconds))
    logger.info(
        "engine_daemon: starting with poll_interval=%ds regions=%s",
        poll_interval,
        ",".join(config.regions) if config.regions else "*",
    )

    try:
        while True:
            _advance_all_once()
            time.sleep(poll_interval)
    except KeyboardInterrupt:  # pragma: no cover - manual shutdown
        logger.info("engine_daemon: received KeyboardInterrupt, shutting down")


# ============================================================================
# CLI entrypoint
# ============================================================================


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Prometheus v2 engine daemon – periodically advance engine_runs "
            "using the pipeline state machine."
        ),
    )

    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=60,
        help="Sleep interval between polling cycles (default: 60)",
    )
    parser.add_argument(
        "--region",
        action="append",
        default=None,
        help=(
            "Optional region code this daemon is responsible for. Can be "
            "specified multiple times. Currently used for logging only; "
            "filtering is delegated to list_active_runs."
        ),
    )

    args = parser.parse_args(argv)

    if args.poll_interval_seconds <= 0:
        parser.error("--poll-interval-seconds must be positive")

    return args


def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entrypoint for the engine daemon.

    Example::

        python -m prometheus.orchestration.engine_daemon \
            --poll-interval-seconds 60 \
            --region US
    """

    args = _parse_args(argv)
    regions: List[str] | None = args.region if args.region is not None else None

    config = EngineDaemonConfig(
        poll_interval_seconds=args.poll_interval_seconds,
        regions=regions,
    )
    run_daemon(config)


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
