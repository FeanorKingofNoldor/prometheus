"""Shared price-series helpers for signal computation.

Signal math (momentum, realised vol, drawdown, trend) must run on
split/dividend-adjusted prices: a 10:1 split reads as a fake -90% return
that corrupts not only the affected name's own features but any
cross-sectional statistic computed over the universe. Raw ``close`` is
reserved for places that need the actual trade price (order limit
prices, notional sizing, price-level filters).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def adjusted_close_series(df_sorted: pd.DataFrame) -> np.ndarray:
    """Return adjusted closes, falling back to raw close per degenerate row.

    ``adjusted_close`` has full coverage in ``prices_daily``; the per-row
    fallback only guards NULL/non-positive rows so a bad ingest row
    degrades to the raw price instead of poisoning the whole series.
    """
    raw = df_sorted["close"].astype(float)
    if "adjusted_close" not in df_sorted.columns:
        # Synthetic/stub frames (tests, ad-hoc tools) may omit the column;
        # production reads from prices_daily always include it.
        return raw.to_numpy()
    adj = df_sorted["adjusted_close"].astype(float)
    return adj.where((adj.notna()) & (adj > 0.0), raw).to_numpy()
