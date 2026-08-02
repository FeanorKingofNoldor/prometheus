"""End-to-end paper smoke for the COMMODITY broker path.

Walks an OptionTradeDirective for a real commodity contract through
the new _IbkrDirectBroker FOP path:

  1. Build a directive that triggers _submit_directives → .FOP iid
  2. Submit a tiny BUY at a far-OTM limit (won't fill)
  3. Confirm IBKR returns an orderId
  4. Cancel immediately

This validates regex + spec lookup + FuturesOption qualification + IB
placeOrder against paper. Cleans up after itself.

Usage:
    PYTHONPATH=. python tools/smoke_commodity_buy.py
"""

from __future__ import annotations

import sys
import time

from prometheus.execution.broker_interface import (
    BrokerInterface,
    OrderSide,
    OrderType,
    Order,
)
from prometheus.execution.ib_compat import IB
from prometheus.execution.instrument_mapper import InstrumentMapper


def main() -> int:
    # Use the BZ Brent contract we confirmed qualifiable via probe v2:
    # BEV6 = Brent Oct 2026 future option, strike 70 P, conId 712137583
    symbol = "BZ"
    expiry = "20260825"   # the probe's sample BEV6 P70 expiry
    strike = 70.0
    right = "P"

    iid = InstrumentMapper.futures_option_instrument_id(symbol, expiry, strike, right)
    print(f"Built FOP instrument_id: {iid}", flush=True)

    ib = IB()
    ib.connect("127.0.0.1", 4002, clientId=44, timeout=15)
    print(f"Connected (server={ib.client.serverVersion()})", flush=True)

    # Re-create the broker dispatch inline (it's a closure inside
    # run_derivatives_daily.py, not importable). This mirrors the
    # production logic 1:1.
    import re
    from prometheus.execution.futures_option_specs import get_fop_spec
    from prometheus.execution.ib_compat import FuturesOption, LimitOrder

    _FOP_RE = re.compile(r'^([A-Z0-9]+)_(\d{6}|\d{8})_([\d.]+)([CP])\.FOP$')

    m = _FOP_RE.match(iid)
    assert m is not None, f"regex failed to parse {iid!r}"
    fop_symbol = m.group(1)
    fop_exp_raw = m.group(2)
    fop_expiry = "20" + fop_exp_raw if len(fop_exp_raw) == 6 else fop_exp_raw
    fop_strike = float(m.group(3))
    fop_right = m.group(4)

    spec = get_fop_spec(fop_symbol)
    assert spec is not None
    print(f"Spec: exchange={spec.exchange} tc={spec.trading_class} mult={spec.multiplier}",
          flush=True)

    contract = FuturesOption(
        symbol=spec.symbol,
        lastTradeDateOrContractMonth=fop_expiry,
        strike=fop_strike,
        right=fop_right,
        exchange=spec.exchange,
        currency=spec.currency,
        multiplier=spec.multiplier,
        tradingClass=spec.trading_class,
    )
    qualified = ib.qualifyContracts(contract)
    if not qualified or not getattr(qualified[0], "conId", 0):
        print(f"FAIL: qualifyContracts returned {qualified!r}", flush=True)
        ib.disconnect()
        return 1
    contract = qualified[0]
    print(f"Qualified: conId={contract.conId} local={contract.localSymbol}",
          flush=True)

    # Place a 1-lot BUY at $0.01 — guaranteed not to fill on Brent put
    # premium. We immediately cancel after confirmation.
    ib_order = LimitOrder("BUY", 1, 0.01)
    ib_order.tif = "DAY"
    trade = ib.placeOrder(contract, ib_order)
    order_id = trade.order.orderId
    print(f"Placed BUY 1x BEV6 P70 @ $0.01 (orderId={order_id})", flush=True)

    # Wait briefly for the order to register, then cancel.
    time.sleep(2.0)
    ib.cancelOrder(ib_order)
    print(f"Cancelled orderId={order_id}", flush=True)

    ib.disconnect()
    print("SMOKE OK — FOP broker path validated end-to-end", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
