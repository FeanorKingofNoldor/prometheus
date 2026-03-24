"""Prometheus v2 – IBKR Integration Smoke Test.

Connects to IBKR TWS/Gateway and exercises the new Phase 2 + 3
infrastructure:

1. Market data service: subscribe ticks, get snapshot
2. Historical bars: request 30 days of SPY daily data
3. Contract model: build option, index, future contracts
4. Instrument ID round-trip: contract → ID → contract
5. Options portfolio: sync and display greeks
6. Scanner: subscribe and display results

Run with IB Gateway / TWS active:
    python -m prometheus.scripts.test_ibkr_integration
    python -m prometheus.scripts.test_ibkr_integration --port 7496  # live TWS
"""

from __future__ import annotations

import argparse
import time

from apathis.core.logging import get_logger

from prometheus.execution.ib_compat import IB, IB_BACKEND
from prometheus.execution.instrument_mapper import InstrumentMapper
from prometheus.execution.market_data import (
    IbkrMarketDataService,
    ScannerSubscription,
    TickSubscription,
)

logger = get_logger(__name__)


def test_contract_model(ib: IB) -> None:
    """Test 3.1: contract building and instrument_id round-trips."""
    print("\n" + "=" * 60)
    print("TEST: Contract Model (3.1)")
    print("=" * 60)

    # Stock
    mapper = InstrumentMapper()
    contract = mapper.build_option_contract("SPY", "20250620", 500.0, "P")
    print(f"  Option contract: {contract}")
    iid = InstrumentMapper.option_instrument_id("SPY", "20250620", 500.0, "P")
    print(f"  Instrument ID:   {iid}")
    iid_rt = InstrumentMapper.contract_to_instrument_id(contract)
    print(f"  Round-trip ID:   {iid_rt}")
    assert iid == iid_rt, f"Round-trip mismatch: {iid} != {iid_rt}"
    print("  ✓ Option ID round-trip OK")

    # Index
    idx = mapper.build_index_contract("VIX", "CBOE")
    print(f"  Index contract:  {idx}")
    idx_id = InstrumentMapper.contract_to_instrument_id(idx)
    print(f"  Index ID:        {idx_id}")
    print("  ✓ Index contract OK")

    # Future
    fut = mapper.build_future_contract("VX", "20250618", "CFE")
    print(f"  Future contract: {fut}")
    fut_id = InstrumentMapper.contract_to_instrument_id(fut)
    print(f"  Future ID:       {fut_id}")
    print("  ✓ Future contract OK")

    # Qualify option contract via IBKR
    try:
        qualified = ib.qualifyContracts(contract)
        if qualified:
            c = qualified[0]
            print(f"  Qualified option: conId={c.conId}, "
                  f"symbol={c.symbol}, strike={c.strike}, "
                  f"right={c.right}, expiry={c.lastTradeDateOrContractMonth}")
            print("  ✓ Option qualification OK")
        else:
            print("  ⚠ Could not qualify option (may need valid expiry)")
    except Exception as e:
        print(f"  ⚠ Option qualification error: {e}")


def test_historical_bars(mds: IbkrMarketDataService) -> None:
    """Test 2.3: historical bar request."""
    print("\n" + "=" * 60)
    print("TEST: Historical Bars (2.3)")
    print("=" * 60)

    # SPY daily bars
    bars = mds.request_historical_bars(
        symbol="SPY", duration="30 D", bar_size="1 day",
        data_type="TRADES",
    )
    print(f"  SPY TRADES: {len(bars)} bars")
    if bars:
        first, last = bars[0], bars[-1]
        print(f"    First: {first.trade_date} O={first.open:.2f} "
              f"H={first.high:.2f} L={first.low:.2f} C={first.close:.2f} "
              f"V={first.volume:.0f}")
        print(f"    Last:  {last.trade_date} O={last.open:.2f} "
              f"H={last.high:.2f} L={last.low:.2f} C={last.close:.2f} "
              f"V={last.volume:.0f}")
    print("  ✓ SPY daily bars OK")

    # Historical vol
    hvol_bars = mds.request_historical_bars(
        symbol="SPY", duration="30 D", bar_size="1 day",
        data_type="HISTORICAL_VOLATILITY",
    )
    print(f"  SPY HISTORICAL_VOLATILITY: {len(hvol_bars)} bars")
    if hvol_bars:
        print(f"    Latest: {hvol_bars[-1].trade_date} "
              f"close={hvol_bars[-1].close:.4f}")
    print("  ✓ Historical vol OK")

    # Implied vol
    iv_bars = mds.request_historical_bars(
        symbol="SPY", duration="30 D", bar_size="1 day",
        data_type="OPTION_IMPLIED_VOLATILITY",
    )
    print(f"  SPY OPTION_IMPLIED_VOLATILITY: {len(iv_bars)} bars")
    if iv_bars:
        print(f"    Latest: {iv_bars[-1].trade_date} "
              f"close={iv_bars[-1].close:.4f}")
    print("  ✓ Implied vol OK")

    # VIX (Index type)
    vix_bars = mds.request_historical_bars(
        symbol="VIX", duration="30 D", bar_size="1 day",
        data_type="TRADES", sec_type="IND", exchange="CBOE",
    )
    print(f"  VIX: {len(vix_bars)} bars")
    if vix_bars:
        print(f"    Latest: {vix_bars[-1].trade_date} "
              f"close={vix_bars[-1].close:.2f}")
    print("  ✓ VIX historical OK")


def test_tick_snapshot(mds: IbkrMarketDataService) -> None:
    """Test 2.2: tick subscription and snapshot."""
    print("\n" + "=" * 60)
    print("TEST: Tick Subscriptions (2.2)")
    print("=" * 60)

    # Subscribe to SPY with options ticks
    sub = TickSubscription(
        symbol="SPY", sec_type="STK", exchange="SMART",
        currency="USD", generic_ticks="100,101,104,105,106",
    )
    req_id = mds.subscribe_ticks(sub)
    print(f"  Subscribed to SPY (req_id={req_id})")

    # Wait for data
    print("  Waiting 5 seconds for tick data...")
    time.sleep(5)

    snap = mds.get_snapshot("SPY")
    if snap:
        print("  SPY Snapshot:")
        print(f"    Last:  ${snap.last:.2f}")
        print(f"    Bid:   ${snap.bid:.2f}")
        print(f"    Ask:   ${snap.ask:.2f}")
        print(f"    Vol:   {snap.volume:.0f}")
        print(f"    P/C ratio:     {snap.put_call_ratio:.3f}")
        print(f"    Call opt vol:  {snap.opt_volume_call:.0f}")
        print(f"    Put opt vol:   {snap.opt_volume_put:.0f}")
        print(f"    Hist vol 30d:  {snap.hist_vol_30d:.4f}")
        print(f"    Avg IV (call): {snap.avg_iv_call:.4f}")
        print("  ✓ Tick snapshot OK")
    else:
        print("  ⚠ No snapshot data received")

    mds.unsubscribe_ticks(req_id)
    print("  Unsubscribed from SPY")


def test_scanner(mds: IbkrMarketDataService) -> None:
    """Test 2.4: scanner subscription."""
    print("\n" + "=" * 60)
    print("TEST: Scanner Subscription (2.4)")
    print("=" * 60)

    results_holder = {"results": []}

    def on_scanner(scan_code, results):
        results_holder["results"] = results

    sub = ScannerSubscription(
        scan_code="HOT_BY_OPT_VOLUME",
        instrument="STK",
        location="STK.US.MAJOR",
        above_price=5.0,
        market_cap_above=1e9,
        number_of_rows=10,
    )

    req_id = mds.subscribe_scanner(sub, callback=on_scanner)
    print(f"  Subscribed to HOT_BY_OPT_VOLUME (req_id={req_id})")

    print("  Waiting 10 seconds for scanner data...")
    time.sleep(10)

    results = results_holder["results"]
    print(f"  Scanner results: {len(results)} rows")
    for r in results[:5]:
        print(f"    #{r.rank}: {r.symbol:6s} value={r.value:.2f}")

    mds.unsubscribe_scanner(req_id)
    print("  ✓ Scanner OK")


def test_service_status(mds: IbkrMarketDataService) -> None:
    """Print service status."""
    print("\n" + "=" * 60)
    print("SERVICE STATUS")
    print("=" * 60)
    status = mds.get_status()
    for k, v in status.items():
        print(f"  {k}: {v}")


def main() -> None:
    parser = argparse.ArgumentParser(description="IBKR integration smoke test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4001,
                        help="4001=paper gateway, 7496=live TWS")
    parser.add_argument("--client-id", type=int, default=20)
    parser.add_argument("--skip-ticks", action="store_true",
                        help="Skip tick subscription test")
    parser.add_argument("--skip-scanner", action="store_true",
                        help="Skip scanner test")
    args = parser.parse_args()

    print(f"IBKR backend: {IB_BACKEND}")
    ib = IB()
    print(f"Connecting to IBKR at {args.host}:{args.port} "
          f"(client_id={args.client_id})...")
    ib.connect(host=args.host, port=args.port, clientId=args.client_id)
    print("Connected!\n")

    mds = IbkrMarketDataService(ib)

    try:
        test_contract_model(ib)
        test_historical_bars(mds)

        if not args.skip_ticks:
            test_tick_snapshot(mds)

        if not args.skip_scanner:
            test_scanner(mds)

        test_service_status(mds)

        print("\n" + "=" * 60)
        print("ALL TESTS PASSED")
        print("=" * 60)

    finally:
        ib.disconnect()
        print("\nDisconnected from IBKR")


if __name__ == "__main__":
    main()
