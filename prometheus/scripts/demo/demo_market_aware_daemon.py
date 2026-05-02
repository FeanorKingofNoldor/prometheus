"""Demo script for the market-aware DAG orchestration daemon.

This script demonstrates the full production orchestrator with:
- Multi-market DAG management
- Real-time market state detection
- Job dependency resolution
- Execution tracking and retry logic

Usage:
    # Demo all markets with 10-second polling
    python -m prometheus.scripts.demo_market_aware_daemon

    # Single market with custom poll interval
    python -m prometheus.scripts.demo_market_aware_daemon \\
        --market US_EQ \\
        --poll-interval-seconds 30

    # Multi-market with fixed date
    python -m prometheus.scripts.demo_market_aware_daemon \\
        --market US_EQ \\
        --market EU_EQ \\
        --as-of-date 2025-12-15
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime

from apatheon.core.database import get_db_manager
from apatheon.core.market_state import get_all_market_states, get_next_state_transition

from prometheus.orchestration.dag import build_market_dag
from prometheus.orchestration.market_aware_daemon import (
    MarketAwareDaemon,
    MarketAwareDaemonConfig,
    get_dag_executions,
)


def print_market_overview():
    """Print current state of all markets."""
    print("\n" + "=" * 80)
    print("MARKET STATE OVERVIEW")
    print("=" * 80)

    from datetime import timezone
    now = datetime.now(timezone.utc)
    states = get_all_market_states(now)

    for market_id, state in sorted(states.items()):
        next_state, transition_time = get_next_state_transition(market_id, now)
        time_until = transition_time - now if transition_time else None

        print(f"\n{market_id}:")
        print(f"  Current State: {state.value}")
        print(f"  Next State: {next_state.value}")
        if transition_time:
            print(f"  Transition Time: {transition_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            if time_until:
                hours = int(time_until.total_seconds() // 3600)
                minutes = int((time_until.total_seconds() % 3600) // 60)
                print(f"  Time Until: {hours}h {minutes}m")


def print_dag_status(db_manager, market_id: str, as_of_date: date):
    """Print status of a market's DAG."""
    dag = build_market_dag(market_id, as_of_date)
    dag_id = f"{market_id}_{as_of_date.isoformat()}"

    executions = get_dag_executions(db_manager, dag_id)
    status_counts = {}
    for exec in executions:
        status = exec.status.value
        status_counts[status] = status_counts.get(status, 0) + 1

    print(f"\n{market_id} DAG Status (dag_id={dag_id}):")
    print(f"  Total Jobs: {len(dag.jobs)}")
    print(f"  Executions: {len(executions)}")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")


def run_demo(markets: list[str], poll_interval: int, as_of_date: date | None, cycles: int = 3):
    """Run the daemon in demo mode for a limited number of cycles."""
    print("\n" + "=" * 80)
    print("MARKET-AWARE DAEMON DEMO")
    print("=" * 80)
    print("\nConfiguration:")
    print(f"  Markets: {', '.join(markets)}")
    print(f"  Poll Interval: {poll_interval}s")
    print(f"  As-of Date: {as_of_date or 'today'}")
    print(f"  Demo Cycles: {cycles}")

    print_market_overview()

    db_manager = get_db_manager()
    as_of_date = as_of_date or date.today()

    # Print initial DAG status
    print("\n" + "=" * 80)
    print("INITIAL DAG STATUS")
    print("=" * 80)
    for market_id in markets:
        print_dag_status(db_manager, market_id, as_of_date)

    # Create daemon
    config = MarketAwareDaemonConfig(
        markets=markets,
        poll_interval_seconds=poll_interval,
        as_of_date=as_of_date,
    )
    daemon = MarketAwareDaemon(config, db_manager)

    # Initialize
    daemon._initialize_dags(as_of_date)

    print("\n" + "=" * 80)
    print("RUNNING DAEMON CYCLES")
    print("=" * 80)

    # Run limited cycles
    for cycle in range(1, cycles + 1):
        print(f"\n--- Cycle {cycle}/{cycles} ---")
        try:
            daemon._run_cycle(as_of_date)
            print(f"Cycle {cycle} completed successfully")
        except Exception as exc:
            print(f"Cycle {cycle} failed: {exc}")

    # Print final DAG status
    print("\n" + "=" * 80)
    print("FINAL DAG STATUS")
    print("=" * 80)
    for market_id in markets:
        print_dag_status(db_manager, market_id, as_of_date)

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)
    print("\nTo run the full daemon in production:")
    print("  python -m prometheus.orchestration.market_aware_daemon \\")
    print("      --market US_EQ \\")
    print("      --market EU_EQ \\")
    print("      --market ASIA_EQ \\")
    print("      --poll-interval-seconds 60")


def main():
    """CLI entrypoint for demo script."""
    parser = argparse.ArgumentParser(
        description="Demo the market-aware DAG orchestration daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--market",
        action="append",
        default=None,
        help="Market ID to orchestrate (default: US_EQ, EU_EQ, ASIA_EQ)",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=10,
        help="Sleep interval between polling cycles (default: 10)",
    )
    parser.add_argument(
        "--as-of-date",
        type=str,
        help="Fixed as-of date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=3,
        help="Number of cycles to run in demo (default: 3)",
    )

    args = parser.parse_args()

    # Default to all markets if none specified
    markets = args.market if args.market else ["US_EQ", "EU_EQ", "ASIA_EQ"]

    # Parse as-of date
    as_of_date = None
    if args.as_of_date:
        try:
            as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Error: Invalid date format '{args.as_of_date}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    try:
        run_demo(markets, args.poll_interval_seconds, as_of_date, args.cycles)
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
        sys.exit(0)
    except Exception as exc:
        print(f"\nDemo failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
