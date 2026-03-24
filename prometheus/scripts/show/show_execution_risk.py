"""Prometheus v2 – Execution risk configuration inspection CLI.

This script prints the current execution risk configuration as seen by
:func:`prometheus.core.config.get_config` and the underlying environment
variables that drive it.

It is intended as a quick, low-risk way to confirm which limits are
active before running live or paper execution via IBKR.

Example
-------

    python -m prometheus.scripts.show_execution_risk

"""

from __future__ import annotations

import argparse
import os
from typing import Optional, Sequence

from apathis.core.config import get_config
from apathis.core.logging import get_logger

logger = get_logger(__name__)


_ENV_KEYS = [
    "EXEC_RISK_ENABLED",
    "EXEC_RISK_MAX_ORDER_NOTIONAL",
    "EXEC_RISK_MAX_POSITION_NOTIONAL",
    "EXEC_RISK_MAX_LEVERAGE",
]


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Show execution risk limits as loaded from environment and "
            "PrometheusConfig. All numeric limits are in account currency."
        ),
    )

    parser.parse_args(argv)

    config = get_config()
    risk = config.execution_risk

    print("# Execution risk config (PrometheusConfig.execution_risk)")
    print(f"enabled={risk.enabled}")
    print(f"max_order_notional={risk.max_order_notional}")
    print(f"max_position_notional={risk.max_position_notional}")
    print(f"max_leverage={risk.max_leverage}")
    print()

    print("# Raw environment variables (empty means not set)")
    for key in _ENV_KEYS:
        value = os.environ.get(key, "")
        print(f"{key}={value}")


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    main()
