"""Prometheus v2 – Engine run state CLI.

This script provides a simple command-line interface for creating and
advancing engine runs using the pipeline state machine defined in
``prometheus.pipeline.state`` and the phase tasks in
``prometheus.pipeline.tasks``.

Short-term (current) usage is to call this script from cron or systemd
*as a one-shot manager*:

- After daily ingestion for a region completes successfully, ensure a run
  exists for that (date, region) and, if appropriate, mark it ``DATA_READY``::

      python -m prometheus.scripts.run_engine_state \
          --as-of 2024-03-04 \
          --region US \
          --ensure \
          --data-ready

- Periodically advance all active runs by one phase (e.g. every few
  minutes)::

      python -m prometheus.scripts.run_engine_state --advance-all

This keeps orchestration logic outside of the script: shell-level timers act
only as heartbeats, while all workflow semantics live in the database and
engine code.

Medium-term, this CLI is expected to be wrapped by a small long-running
engine daemon in ``prometheus.orchestration.engine_daemon`` that uses the
same helpers under the hood.

Longer term, external DAG-based orchestrators can call the same helpers or
this CLI for debugging; the ``engine_runs`` table remains the source of
truth for run state.

See ``docs/dev_workflows_engine_runs_orchestration.md`` for an overview of
short/medium/long-term orchestration patterns.
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Optional

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.pipeline.state import (
    EngineRun,
    EngineRunStateError,
    RunPhase,
    get_or_create_run,
    list_active_runs,
    update_phase,
)
from prometheus.pipeline.tasks import advance_run

logger = get_logger(__name__)


def _parse_date(value: str) -> date:
    try:
        year, month, day = map(int, value.split("-"))
        return date(year, month, day)
    except Exception as exc:  # pragma: no cover - CLI validation
        raise argparse.ArgumentTypeError(f"Invalid date {value!r}, expected YYYY-MM-DD") from exc


def ensure_run(as_of_date: date, region: str, mark_data_ready: bool) -> EngineRun:
    db_manager = get_db_manager()
    run = get_or_create_run(db_manager, as_of_date, region)

    if mark_data_ready and run.phase == RunPhase.WAITING_FOR_DATA:
        run = update_phase(db_manager, run.run_id, RunPhase.DATA_READY)

    logger.info(
        "ensure_run: run_id=%s as_of_date=%s region=%s phase=%s",
        run.run_id,
        run.as_of_date,
        run.region,
        run.phase.value,
    )
    return run


def cmd_advance_all() -> None:
    db_manager = get_db_manager()
    runs = list_active_runs(db_manager)
    if not runs:
        logger.info("No active engine runs found")
        return

    for run in runs:
        try:
            logger.info(
                "advance_all: advancing run_id=%s date=%s region=%s phase=%s",
                run.run_id,
                run.as_of_date,
                run.region,
                run.phase.value,
            )
            new_run = advance_run(db_manager, run)
            logger.info(
                "advance_all: run_id=%s now in phase=%s",
                new_run.run_id,
                new_run.phase.value,
            )
        except EngineRunStateError as exc:
            logger.error("advance_all: state error for run %s: %s", run.run_id, exc)


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Prometheus v2 engine run state manager")

    parser.add_argument(
        "--as-of",
        type=_parse_date,
        help="As-of date for the run (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--region",
        type=str,
        help="Region code for the run (e.g. US, EU, ASIA)",
    )
    parser.add_argument(
        "--ensure",
        action="store_true",
        help="Ensure a run exists for the given date/region",
    )
    parser.add_argument(
        "--data-ready",
        action="store_true",
        help="When used with --ensure, bump phase to DATA_READY if currently WAITING_FOR_DATA",
    )
    parser.add_argument(
        "--advance-all",
        action="store_true",
        help="Advance all active runs by one phase",
    )

    args = parser.parse_args(argv)

    if args.ensure:
        if args.as_of is None or args.region is None:
            parser.error("--ensure requires --as-of and --region")
        ensure_run(args.as_of, args.region.upper(), args.data_ready)

    if args.advance_all:
        cmd_advance_all()

    if not args.ensure and not args.advance_all:
        parser.error("No action specified; use --ensure and/or --advance-all")


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
