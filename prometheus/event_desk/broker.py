from __future__ import annotations

import math
from types import SimpleNamespace

from .model import Quote


class PaperSession:
    def __init__(self):
        from prometheus.execution.ibkr_client_impl import IbkrClientImpl
        from prometheus.execution.ibkr_config import IbkrMode, create_connection_config

        config = create_connection_config(IbkrMode.PAPER, client_id=18)
        if config.port != 4002 or not str(config.account_id).startswith("DU"):
            raise RuntimeError("Event desk requires an IBKR paper account on port 4002")
        self.client = IbkrClientImpl(config)
        self.client._max_reconnect_attempts = 0

    def __enter__(self):
        try:
            self.client.connect()
            accounts = self.client.ib.managedAccounts()
            if self.client._config.account_id not in accounts or any(not a.startswith("DU") for a in accounts):
                raise RuntimeError("Connected account is not the configured paper account")
            self.client.ib.RequestTimeout = 20
            return self
        except Exception:
            self.client.disconnect()
            raise

    def __exit__(self, *args):
        self.client.disconnect()

    def quotes(self, instruments):
        ib = self.client.ib
        ib.reqMarketDataType(3)
        subscriptions = {}
        try:
            for instrument in instruments:
                try:
                    contract = self.client._create_contract(instrument)
                    qualified = ib.qualifyContracts(contract)
                    if not qualified:
                        continue
                    subscriptions[instrument] = (qualified[0], ib.reqMktData(qualified[0], "236", False, False))
                except Exception:
                    continue
            ib.sleep(3)
            result = {}
            for instrument, (contract, t) in subscriptions.items():
                if t.time is None:
                    continue
                shortable = float(t.shortableShares or 0)
                if not math.isfinite(shortable):
                    shortable = 0
                result[instrument] = Quote(float(t.bid), float(t.ask), t.time, int(t.marketDataType), shortable)
            return result
        finally:
            for contract, t in subscriptions.values():
                ib.cancelMktData(contract)

    def reconcile(self, db):
        from prometheus.execution.fill_reconciliation import reconcile_fills

        proxy = SimpleNamespace(ib=self.client.ib, disconnect=lambda: None)
        result = reconcile_fills(db, mode="paper", expire_stale=False, client_factory=lambda mode: proxy)
        if result["errors"]:
            raise RuntimeError("Paper fill reconciliation failed: " + str(result["errors"]))
        self.client.ib.sleep(0.5)
        with db.get_runtime_connection() as conn:
            with conn.cursor() as cur:
                for fill in self.client.ib.fills():
                    report = fill.commissionReport
                    commission = float(report.commission) if report else float("nan")
                    if math.isfinite(commission) and 0 <= commission <= 100:
                        cur.execute(
                            """UPDATE fills f SET commission=%s FROM orders o
                            WHERE f.order_id=o.order_id AND f.fill_id=%s
                              AND o.portfolio_id='EVENT_PAPER' AND o.mode='PAPER' AND f.mode='PAPER'""",
                            (commission, fill.execution.execId),
                        )
            conn.commit()

    def order_states(self):
        ib = self.client.ib
        trades = list(ib.reqAllOpenOrders()) + list(ib.reqCompletedOrders(apiOnly=False)) + list(ib.trades())
        return {t.order.orderRef: self.client._map_order_status(t).value for t in trades if t.order.orderRef}

    def submit(self, order):
        self.client.submit_order(order)
        self.client.ib.sleep(3)
        return self.client.get_order_status(order.order_id)

    def positions(self):
        positions = self.client.ib.reqPositions()
        return {
            self.client._contract_to_instrument_id(p.contract): SimpleNamespace(quantity=float(p.position))
            for p in positions
            if p.account == self.client._config.account_id
        }
