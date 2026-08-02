"""folio entry point.

  python -m tools.folio --dump [--live] [--wait N]   headless feed check
  python -m tools.folio [--live]                     launch the TUI (P2+)
"""

from __future__ import annotations

import argparse
import sys
import time


def _dump(mode: str, wait: float) -> int:
    from tools.folio.model import PortfolioModel

    m = PortfolioModel(mode=mode)
    m.start()
    print(f"[folio] connecting to {mode} gateway (read-only)...", file=sys.stderr)
    deadline = time.time() + wait
    while time.time() < deadline:
        snap = m.get_snapshot()
        if snap.error:
            print(f"[folio] ERROR: {snap.error}", file=sys.stderr)
            m.stop()
            return 1
        if snap.connected and snap.positions and snap.acct.net_liq:
            break
        time.sleep(0.5)
    snap = m.get_snapshot()
    a = snap.acct
    print(f"\n=== folio · {snap.mode.upper()} · acct {snap.account} · "
          f"{'connected' if snap.connected else 'DISCONNECTED'} · {snap.market_data} data ===")
    print(f"NetLiq ${a.net_liq:,.0f}  DayPnL ${a.day_pnl:,.0f} ({a.day_pct:+.2f}%)  "
          f"Unreal ${a.unrealized_pnl:,.0f}  Lev {a.leverage:.2f}x  "
          f"BuyPwr ${a.buying_power:,.0f}  ExcessLiq ${a.excess_liquidity:,.0f}")
    print(f"Positions: {snap.total} ({snap.streamed} streaming)  NAV samples: {len(snap.nav_hist)}")
    print(f"\n{'SYM':<8}{'TYPE':<6}{'QTY':>10}{'LAST':>10}{'MKTVAL':>14}"
          f"{'UPL$':>12}{'DAY%':>8}{'WT%':>7}  SECTOR/CLASS")
    for r in snap.positions[:40]:
        greeks = f"  Δ{r.delta:.2f}" if r.delta is not None else ""
        print(f"{r.symbol:<8}{r.sec_type:<6}{r.qty:>10.0f}{r.last:>10.2f}{r.market_value:>14,.0f}"
              f"{r.unrealized_pnl:>12,.0f}{r.day_pct:>7.2f}%{r.weight:>6.1f}%  {r.sector}{greeks}")
    if not snap.positions:
        print("(no positions — account may be flat, or data hasn't streamed yet)")
    m.stop()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="folio")
    ap.add_argument("--dump", action="store_true", help="headless feed check, then exit")
    ap.add_argument("--live", action="store_true", help="use the live account (default: paper)")
    ap.add_argument("--wait", type=float, default=8.0, help="seconds to wait for data in --dump")
    args = ap.parse_args()
    mode = "live" if args.live else "paper"
    if args.dump:
        return _dump(mode, args.wait)
    # TUI (built in P2+)
    try:
        from tools.folio.app import run as run_app
    except ImportError:
        print("folio TUI not built yet — use --dump for the headless feed check.", file=sys.stderr)
        return 2
    return run_app(mode)


if __name__ == "__main__":
    raise SystemExit(main())
