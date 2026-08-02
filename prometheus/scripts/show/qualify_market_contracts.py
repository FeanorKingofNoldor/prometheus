"""Prometheus v2 – IBKR contract qualification harness for a market.

Loads all ACTIVE instruments for a given market (``instruments.market_id``),
maps each one through :class:`InstrumentMapper` (EODHD exchange code ->
SMART routing + ``primaryExchange``), asks IBKR to qualify the resulting
contract, and prints an ok/failed table with the resolved conId/exchange.

This is a strictly read-only diagnostic: it connects with ``readonly=True``
(client_id 14 by default), qualifies contracts, and disconnects.  It NEVER
submits, modifies, or cancels anything.

Exit code is nonzero if any instrument fails to qualify.

Example
-------

    python -m prometheus.scripts.show.qualify_market_contracts \
        --market UK_EQ --limit 20 --port 4002
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Sequence, Tuple

from apatheon.core.database import get_db_manager
from apatheon.core.logging import get_logger

from prometheus.execution.ibkr_client import IbkrConnectionConfig
from prometheus.execution.ibkr_client_impl import IbkrClientImpl
from prometheus.execution.instrument_mapper import ContractQualificationError, InstrumentMapper

logger = get_logger(__name__)

# Dedicated diagnostic client id — must not collide with the daemon (1)
# or other operator tools.
DEFAULT_CLIENT_ID = 14


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Qualify IBKR contracts for all ACTIVE instruments of a market "
            "(read-only diagnostic; never submits orders)."
        ),
    )
    parser.add_argument(
        "--market",
        type=str,
        required=True,
        help="Market identifier (instruments.market_id, e.g. UK_EQ, EU_EQ, HK_EQ)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of instruments to qualify (default: all)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="IBKR Gateway/TWS host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=4002,
        help="IBKR Gateway/TWS port (default: 4002, paper gateway)",
    )
    parser.add_argument(
        "--client-id",
        type=int,
        default=DEFAULT_CLIENT_ID,
        help=f"IBKR API client id (default: {DEFAULT_CLIENT_ID})",
    )
    parser.add_argument(
        "--collect-lots",
        action="store_true",
        help=(
            "Also fetch IBKR contract details and persist each instrument's "
            "minimum order size (board lot — required for SEHK/HK) to "
            "instrument_identifiers as IBKR_BOARD_LOT. Sizing reads these; "
            "instruments without a stored lot default to 1 share."
        ),
    )

    args = parser.parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    return args


def _load_market_instruments(market_id: str, limit: Optional[int]) -> List[Tuple[str, str, str, str]]:
    """Return (instrument_id, symbol, exchange, currency) rows for a market."""
    sql = """
        SELECT instrument_id, symbol, exchange, currency
        FROM instruments
        WHERE status = 'ACTIVE'
          AND market_id = %s
          AND asset_class IN ('EQUITY', 'ETF')
          AND instrument_id NOT LIKE 'SYNTH_%%'
        ORDER BY instrument_id
    """
    params: list[object] = [market_id]
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

    db = get_db_manager()
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            return [tuple(row) for row in cur.fetchall()]
        finally:
            cur.close()


def _persist_board_lot(db, instrument_id: str, lot: int) -> None:
    """Upsert the IBKR board lot into instrument_identifiers."""
    with db.get_runtime_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM instrument_identifiers "
                "WHERE instrument_id = %s AND identifier_type = 'IBKR_BOARD_LOT'",
                (instrument_id,),
            )
            cur.execute(
                """
                INSERT INTO instrument_identifiers
                    (instrument_id, identifier_type, identifier_value,
                     effective_start, source)
                VALUES (%s, 'IBKR_BOARD_LOT', %s, CURRENT_DATE, 'ibkr_qualification')
                """,
                (instrument_id, str(lot)),
            )
            conn.commit()
        finally:
            cur.close()


def _fetch_board_lot(client: IbkrClientImpl, contract) -> Optional[int]:
    """Minimum order size from IBKR contract details (board lot on SEHK)."""
    try:
        details = client._ib.reqContractDetails(contract)
    except Exception as e:  # noqa: BLE001 - diagnostic tool
        logger.warning("reqContractDetails failed for %s: %s", contract, e)
        return None
    if not details:
        return None
    min_size = getattr(details[0], "minSize", None)
    try:
        lot = int(float(min_size)) if min_size else 0
    except (TypeError, ValueError):
        return None
    return lot if lot >= 1 else None


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)

    rows = _load_market_instruments(args.market, args.limit)
    if not rows:
        print(f"No ACTIVE EQUITY/ETF instruments found for market_id={args.market!r}")
        return 1

    print(
        f"Qualifying {len(rows)} instruments for market={args.market} "
        f"against IBKR at {args.host}:{args.port} (client_id={args.client_id}, read-only)"
    )
    print("".ljust(100, "="))

    config = IbkrConnectionConfig(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        readonly=True,
    )
    client = IbkrClientImpl(config, mapper=InstrumentMapper())

    results: List[Tuple[str, str, str, str, str, str]] = []
    n_ok = 0
    n_failed = 0

    try:
        client.connect()

        db = get_db_manager() if args.collect_lots else None
        n_lots = 0
        for instrument_id, _symbol, _exchange, _currency in rows:
            try:
                contract = client._create_contract(instrument_id)
                con_id = getattr(contract, "conId", 0) or 0
                if con_id and db is not None:
                    lot = _fetch_board_lot(client, contract)
                    if lot and lot > 1:
                        _persist_board_lot(db, instrument_id, lot)
                        n_lots += 1
                if con_id:
                    status = "ok"
                    n_ok += 1
                else:
                    # US SMART path can return an unqualified contract
                    # without raising — treat that as a failure here.
                    status = "FAILED"
                    n_failed += 1
                results.append((
                    instrument_id,
                    status,
                    str(con_id) if con_id else "-",
                    getattr(contract, "symbol", "") or "-",
                    getattr(contract, "exchange", "") or "-",
                    getattr(contract, "primaryExchange", "") or "-",
                ))
            except ContractQualificationError as e:
                n_failed += 1
                results.append((instrument_id, "FAILED", "-", "-", "-", "-"))
                logger.warning("Qualification failed for %s: %s", instrument_id, e)
            except Exception as e:  # noqa: BLE001 - diagnostic tool, keep going
                n_failed += 1
                results.append((instrument_id, "ERROR", "-", "-", "-", "-"))
                logger.warning("Unexpected error qualifying %s: %s", instrument_id, e)
    finally:
        try:
            client.disconnect()
        except Exception as e:  # noqa: BLE001
            logger.debug("Error during disconnect: %s", e)

    header = (
        f"{'instrument_id':<24} {'status':<8} {'conId':<12} "
        f"{'symbol':<12} {'exchange':<10} {'primaryExchange':<16}"
    )
    print(header)
    print("".ljust(len(header), "-"))
    for instrument_id, status, con_id, symbol, exchange, primary in results:
        print(
            f"{instrument_id:<24} {status:<8} {con_id:<12} "
            f"{symbol:<12} {exchange:<10} {primary:<16}"
        )

    print("".ljust(100, "="))
    lots_note = f" board_lots_stored={n_lots}" if args.collect_lots else ""
    print(f"ok={n_ok} failed={n_failed} total={len(rows)}{lots_note}")

    return 0 if n_failed == 0 else 2


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    sys.exit(main())
