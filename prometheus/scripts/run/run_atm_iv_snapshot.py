"""Daily ATM IV snapshot runner.

Connects to IBKR paper :4002, snapshots ATM IV for the 12 tracked
underlyings, writes to ``daily_atm_iv``. Run once per trading day.

Cron:
    30 16 * * 1-5  /usr/bin/python3.14 -m prometheus.scripts.run.run_atm_iv_snapshot
"""

from __future__ import annotations

import logging
import sys
from datetime import date

from prometheus.calendar.iv_snapshot import (
    DEFAULT_UNDERLYINGS,
    snapshot_all,
)
from prometheus.execution.ib_compat import IB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    ib = IB()
    try:
        ib.connect("127.0.0.1", 4002, clientId=50, timeout=20)
    except Exception as exc:
        logger.error("IBKR paper connect failed: %s", exc)
        return 1

    try:
        today = date.today()
        snaps = snapshot_all(ib, today=today, underlyings=DEFAULT_UNDERLYINGS)
        logger.info(
            "ATM IV snapshot %s: %d/%d underlyings persisted",
            today, len(snaps), len(DEFAULT_UNDERLYINGS),
        )
        for s in snaps:
            logger.info(
                "  %s: IV=%.3f strike=%.2f expiry=%s",
                s.underlying, s.atm_iv, s.atm_strike, s.atm_expiry,
            )
        return 0 if snaps else 2
    finally:
        ib.disconnect()


if __name__ == "__main__":
    sys.exit(main())
