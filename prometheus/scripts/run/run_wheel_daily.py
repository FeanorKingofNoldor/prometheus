"""CLI wrapper for the core+wheel daily runner.

Shadow run (plan + decision log, no orders — the default while
PROMETHEUS_WHEEL_ENABLED is unset)::

    python -m prometheus.scripts.run.run_wheel_daily

Force a real submission regardless of env flags (cutover testing)::

    python -m prometheus.scripts.run.run_wheel_daily --submit

Force shadow even if the env flags would allow submission::

    python -m prometheus.scripts.run.run_wheel_daily --no-submit
"""

from __future__ import annotations

import argparse
import json
import sys

from prometheus.wheel.runner import WHEEL_CLIENT_ID, run_wheel_daily


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Core+wheel daily runner")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002,
                        help="4002 paper (default), 4001 live")
    parser.add_argument("--client-id", type=int, default=WHEEL_CLIENT_ID)
    gate = parser.add_mutually_exclusive_group()
    gate.add_argument("--submit", action="store_true",
                      help="Force order submission (overrides env gating)")
    gate.add_argument("--no-submit", action="store_true",
                      help="Force shadow mode (overrides env gating)")
    args = parser.parse_args(argv)

    override = True if args.submit else (False if args.no_submit else None)
    summary = run_wheel_daily(
        host=args.host, port=args.port, client_id=args.client_id,
        submit_override=override,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 1 if summary.get("errors") else 0


if __name__ == "__main__":
    sys.exit(main())
