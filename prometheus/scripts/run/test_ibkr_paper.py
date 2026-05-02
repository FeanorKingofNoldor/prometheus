#!/usr/bin/env python3
"""Integration test script for IBKR paper trading.

This script tests the IBKR integration with a paper trading account:
1. Connects to IBKR paper account
2. Loads instruments from database
3. Submits a small test order
4. Monitors for fills
5. Retrieves positions and account state

Prerequisites:
- IB Gateway or TWS running with paper account logged in
- Environment variables set (or defaults used):
  - IBKR_PAPER_USERNAME (default: xubtmn245)
  - IBKR_PAPER_ACCOUNT (default: DUN807925)
- Database connection configured
- Instruments table populated

Usage:
    python -m prometheus.scripts.test_ibkr_paper

Safety:
    This script uses paper trading only and submits very small orders (1 share).
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from apatheon.core.ids import generate_uuid
from apatheon.core.logging import get_logger

from prometheus.execution.broker_factory import create_paper_broker
from prometheus.execution.broker_interface import Order, OrderSide, OrderType

logger = get_logger(__name__)


def main() -> int:
    """Run integration test."""

    logger.info("=" * 80)
    logger.info("IBKR Paper Trading Integration Test")
    logger.info("=" * 80)

    try:
        # Create paper broker
        logger.info("\n[1] Creating PaperBroker...")
        broker = create_paper_broker()

        # Connect to IBKR
        logger.info("\n[2] Connecting to IBKR paper account...")
        broker.client.connect()

        if not broker.client.is_connected():
            logger.error("Failed to connect to IBKR")
            return 1

        logger.info("✓ Connected successfully")

        # Check connection health
        logger.info("\n[3] Checking connection health...")
        health = broker.client.get_connection_health()
        logger.info("Connection health: %s", health)

        # Get initial account state
        logger.info("\n[4] Retrieving account state...")
        account_state = broker.get_account_state()

        # Log key metrics
        equity = account_state.get("equity", account_state.get("NetLiquidation", "N/A"))
        cash = account_state.get("cash", account_state.get("TotalCashValue", "N/A"))
        logger.info("Account equity: %s", equity)
        logger.info("Account cash: %s", cash)

        # Get initial positions
        logger.info("\n[5] Retrieving positions...")
        positions = broker.get_positions()
        logger.info("Current positions: %d", len(positions))

        for instrument_id, position in positions.items():
            logger.info(
                "  %s: qty=%.2f, avg_cost=%.2f, market_value=%.2f, pnl=%.2f",
                instrument_id,
                position.quantity,
                position.avg_cost,
                position.market_value,
                position.unrealized_pnl,
            )

        # Test order submission (optional - requires confirmation)
        logger.info("\n[6] Testing order submission...")

        # Use a liquid stock for testing
        test_instrument = "AAPL.US"
        test_quantity = 1  # Very small order for safety

        logger.info(
            "Ready to submit test order: BUY %d share of %s at MARKET",
            test_quantity,
            test_instrument,
        )
        logger.info("This is PAPER TRADING - no real money will be used")

        response = input("\nSubmit test order? [y/N]: ").strip().lower()

        if response == 'y':
            # Create order
            order = Order(
                order_id=generate_uuid(),
                instrument_id=test_instrument,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=test_quantity,
                metadata={
                    "source": "test_ibkr_paper",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )

            logger.info("Submitting order: %s", order.order_id)

            try:
                submitted_order_id = broker.submit_order(order)
                logger.info("✓ Order submitted successfully: %s", submitted_order_id)

                # Wait for fill
                logger.info("Waiting for fill (10 seconds)...")
                time.sleep(10)

                # Check fills
                fills = broker.get_fills()
                logger.info("Fills received: %d", len(fills))

                for fill in fills[-5:]:  # Show last 5 fills
                    logger.info(
                        "  Fill: %s %s %s x %.2f @ %.2f (commission=%.2f)",
                        fill.fill_id[:8],
                        fill.side.value,
                        fill.instrument_id,
                        fill.quantity,
                        fill.price,
                        fill.commission,
                    )

                # Get updated positions
                logger.info("\nUpdated positions:")
                positions = broker.get_positions()

                for instrument_id, position in positions.items():
                    logger.info(
                        "  %s: qty=%.2f, avg_cost=%.2f, market_value=%.2f, pnl=%.2f",
                        instrument_id,
                        position.quantity,
                        position.avg_cost,
                        position.market_value,
                        position.unrealized_pnl,
                    )

            except Exception as e:
                logger.error("Failed to submit order: %s", e, exc_info=True)
                return 1
        else:
            logger.info("Test order skipped by user")

        # Test sync
        logger.info("\n[7] Testing sync...")
        broker.sync()
        logger.info("✓ Sync completed")

        # Final connection health check
        logger.info("\n[8] Final connection health check...")
        health = broker.client.get_connection_health()
        logger.info("Connection health: %s", health)

        # Disconnect
        logger.info("\n[9] Disconnecting...")
        broker.client.disconnect()
        logger.info("✓ Disconnected")

        logger.info("\n" + "=" * 80)
        logger.info("Integration test completed successfully!")
        logger.info("=" * 80)

        return 0

    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        return 130

    except Exception as e:
        logger.error("Integration test failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
