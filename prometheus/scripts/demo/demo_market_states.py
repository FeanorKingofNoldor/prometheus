"""Demo script for market state machine.

This script demonstrates the market state detection across different markets
and times, showing how the orchestration layer will use these states to
schedule jobs appropriately.

Usage:
    python -m prometheus.scripts.demo_market_states
"""

from datetime import datetime, timezone

from apathis.core.logging import get_logger
from apathis.core.market_state import (
    DEFAULT_CONFIGS,
    get_all_market_states,
    get_market_state,
    get_next_state_transition,
)

logger = get_logger(__name__)


def demo_market_states():
    """Demonstrate market state detection for different times and markets."""

    print("=" * 80)
    print("PROMETHEUS V2 - MARKET STATE MACHINE DEMO")
    print("=" * 80)
    print()

    # Current time
    now = datetime.now(timezone.utc)
    print(f"Current time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()

    # Show all market states right now
    print("📊 CURRENT MARKET STATES:")
    print("-" * 80)
    states = get_all_market_states(now)
    for market_id, state in sorted(states.items()):
        config = DEFAULT_CONFIGS[market_id]
        session_open = config.session_times.session_open_utc
        session_close = config.session_times.session_close_utc

        print(f"  {market_id:10} : {state.value:12} (session: {session_open}-{session_close} UTC)")

        # Show next transition
        next_state, when = get_next_state_transition(market_id, now)
        time_until = when - now
        hours = int(time_until.total_seconds() // 3600)
        minutes = int((time_until.total_seconds() % 3600) // 60)
        print(f"               → {next_state.value} in {hours}h {minutes}m (at {when.strftime('%H:%M UTC')})")
        print()

    print()
    print("🌍 FOLLOW-THE-SUN TRADING DAY:")
    print("-" * 80)
    print("Showing how markets transition throughout a 24-hour cycle...")
    print()

    # Demonstrate a full day cycle for US_EQ
    base_time = datetime(2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc)  # Monday midnight UTC

    print("US_EQ Market States (Monday):")
    print("Time (UTC)  | State        | Description")
    print("-" * 80)

    key_times = [
        (0, 0, "Midnight - Asia trading"),
        (5, 0, "Early morning - overnight"),
        (7, 0, "Asia closing, EU preparing"),
        (8, 30, "EU session ongoing"),
        (13, 0, "EU session, US overnight"),
        (13, 30, "US PRE_OPEN starts"),
        (14, 30, "US SESSION opens"),
        (17, 0, "US SESSION mid-day"),
        (21, 0, "US SESSION closes, POST_CLOSE begins"),
        (22, 0, "US POST_CLOSE ongoing"),
        (23, 0, "US OVERNIGHT begins"),
    ]

    for hour, minute, description in key_times:
        test_time = base_time.replace(hour=hour, minute=minute)
        state = get_market_state("US_EQ", test_time)
        print(f"{hour:02d}:{minute:02d}      | {state.value:12} | {description}")

    print()
    print("=" * 80)
    print()
    print("✨ Market state machine is now operational!")
    print("   Next step: Build DAG orchestration framework on top of this.")
    print()


if __name__ == "__main__":
    demo_market_states()
