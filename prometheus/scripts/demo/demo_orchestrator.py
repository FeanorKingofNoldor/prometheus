"""Demo of market-aware DAG orchestration.

This script demonstrates how the market state machine and DAG framework work
together to orchestrate jobs intelligently based on market hours and dependencies.

Usage:
    python -m prometheus.scripts.demo_orchestrator
"""

from datetime import date, datetime, timezone

from apatheon.core.logging import get_logger
from apatheon.core.market_state import MarketState, get_market_state

from prometheus.orchestration.dag import build_market_dag

logger = get_logger(__name__)


def demo_orchestration():
    """Demonstrate market-aware DAG orchestration."""

    print("=" * 80)
    print("PROMETHEUS V2 - MARKET-AWARE DAG ORCHESTRATION DEMO")
    print("=" * 80)
    print()

    # Current state
    now = datetime.now(timezone.utc)
    print(f"Current time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Build DAG for US_EQ
    market_id = "US_EQ"
    as_of_date = date.today()

    print(f"📋 Building DAG for {market_id} on {as_of_date}...")
    dag = build_market_dag(market_id, as_of_date)
    print(f"   DAG ID: {dag.dag_id}")
    print(f"   Total jobs: {len(dag.jobs)}")
    print()

    # Get current market state
    current_state = get_market_state(market_id, now)
    print(f"📊 Current market state: {current_state.value}")
    print()

    # Show jobs in dependency order
    print("🔄 DAG Job Structure (dependency order):")
    print("-" * 80)

    # Group jobs by "layer" based on dependencies
    completed = set()
    layer = 1

    while len(completed) < len(dag.jobs):
        runnable = dag.get_runnable_jobs(
            completed_jobs=completed,
            running_jobs=set(),
            current_market_state=MarketState.POST_CLOSE,  # Show all jobs
        )

        if not runnable:
            break

        print(f"\nLayer {layer}:")
        for job in runnable:
            state_req = f" (needs {job.required_state.value})" if job.required_state else ""
            deps = f" [depends on: {', '.join(job.dependencies)}]" if job.dependencies else ""
            print(f"  • {job.job_type}{state_req}{deps}")
            completed.add(job.job_id)

        layer += 1

    print()
    print("-" * 80)
    print()

    # Show which jobs are currently runnable
    print(f"✅ Jobs runnable RIGHT NOW (in {current_state.value} state):")
    print("-" * 80)

    runnable_now = dag.get_runnable_jobs(
        completed_jobs=set(),
        running_jobs=set(),
        current_market_state=current_state,
    )

    if runnable_now:
        for job in runnable_now:
            print(f"  • {job.job_type} (priority: {job.priority.name})")
    else:
        print("  None - market must be in POST_CLOSE to start ingestion")

    print()
    print("=" * 80)
    print()
    print("✨ This demonstrates how the orchestrator will:")
    print("   1. Check market state before running jobs")
    print("   2. Only run jobs with satisfied dependencies")
    print("   3. Execute jobs in priority order")
    print("   4. Wait for appropriate market state transitions")
    print()
    print("📍 **Phase 3 Progress**: Foundation complete!")
    print("   Next: Build full market-aware daemon with job execution tracking")
    print()


if __name__ == "__main__":
    demo_orchestration()
