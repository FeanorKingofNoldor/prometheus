"""Tests for the FX layer.

Covers :mod:`prometheus.execution.fx` (converter semantics: USD identity,
staleness window, missing-rate errors, pence handling, memo cache) and the
inversion logic in :mod:`apatheon.data_ingestion.fx_rates` (small currencies
are fetched USD-base and stored inverted under the XXXUSD key).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta

import pytest

from prometheus.execution.fx import (
    CURRENCY_DECIMALS,
    DEFAULT_CURRENCY_DECIMALS,
    MAX_RATE_STALENESS_DAYS,
    PENCE_QUOTED_SUFFIXES,
    FxConverter,
    FxRateUnavailable,
)

AS_OF = date(2026, 7, 2)


# ---------------------------------------------------------------------------
# Stub DB
# ---------------------------------------------------------------------------


class StubCursor:
    """Cursor that answers fx_rates_daily lookups from a canned rate book."""

    def __init__(self, rates: dict[str, list[tuple[date, float]]], counter: dict[str, int]):
        # rates: pair -> [(trade_date, rate), ...]
        self._rates = rates
        self._counter = counter
        self._result: tuple | None = None

    def execute(self, sql: str, params: tuple) -> None:
        self._counter["queries"] += 1
        pair, as_of, earliest = params
        candidates = [
            (d, r)
            for d, r in self._rates.get(pair, [])
            if earliest <= d <= as_of
        ]
        candidates.sort(reverse=True)
        self._result = (candidates[0][1],) if candidates else None

    def fetchone(self):
        return self._result

    def close(self) -> None:
        pass


class StubDbManager:
    def __init__(self, rates: dict[str, list[tuple[date, float]]]):
        self.rates = rates
        self.counter = {"queries": 0}

    @contextmanager
    def get_historical_connection(self):
        yield self

    def cursor(self) -> StubCursor:
        return StubCursor(self.rates, self.counter)


def _converter(rates: dict[str, list[tuple[date, float]]] | None = None) -> FxConverter:
    return FxConverter(StubDbManager(rates or {}))


# ---------------------------------------------------------------------------
# usd_rate / to_usd
# ---------------------------------------------------------------------------


class TestUsdRate:
    def test_usd_identity_no_db_hit(self):
        fx = _converter()
        assert fx.usd_rate("USD", AS_OF) == 1.0
        assert fx._db_manager.counter["queries"] == 0

    def test_returns_latest_rate_on_or_before_as_of(self):
        fx = _converter({
            "EURUSD": [(AS_OF - timedelta(days=1), 1.14), (AS_OF, 1.15)],
        })
        assert fx.usd_rate("EUR", AS_OF) == 1.15

    def test_falls_back_within_staleness_window(self):
        """A rate up to 5 days old (weekend + holiday) is acceptable."""
        stale = AS_OF - timedelta(days=MAX_RATE_STALENESS_DAYS)
        fx = _converter({"GBP" + "USD": [(stale, 1.33)]})
        assert fx.usd_rate("GBP", AS_OF) == 1.33

    def test_rate_beyond_staleness_window_raises(self):
        too_old = AS_OF - timedelta(days=MAX_RATE_STALENESS_DAYS + 1)
        fx = _converter({"GBPUSD": [(too_old, 1.33)]})
        with pytest.raises(FxRateUnavailable):
            fx.usd_rate("GBP", AS_OF)

    def test_future_rate_not_used(self):
        fx = _converter({"EURUSD": [(AS_OF + timedelta(days=1), 1.15)]})
        with pytest.raises(FxRateUnavailable):
            fx.usd_rate("EUR", AS_OF)

    def test_missing_pair_raises(self):
        fx = _converter({})
        with pytest.raises(FxRateUnavailable):
            fx.usd_rate("KRW", AS_OF)

    def test_inversion_stored_pair_reads_canonical_key(self):
        """Ingestion stores USDKRW inverted under KRWUSD; the converter
        only ever reads the canonical XXXUSD key."""
        fx = _converter({"KRWUSD": [(AS_OF, 1.0 / 1540.95)]})
        rate = fx.usd_rate("KRW", AS_OF)
        assert rate == pytest.approx(1.0 / 1540.95)
        # 1,540,950 KRW ≈ 1,000 USD
        assert fx.to_usd(1_540_950, "KRW", AS_OF) == pytest.approx(1000.0, rel=1e-6)

    def test_memo_cache_single_query_per_currency_date(self):
        fx = _converter({
            "EURUSD": [(AS_OF - timedelta(days=1), 1.13), (AS_OF, 1.14)],
        })
        for _ in range(5):
            assert fx.usd_rate("EUR", AS_OF) == 1.14
        assert fx._db_manager.counter["queries"] == 1
        # A different as_of is a different cache key → one more query.
        assert fx.usd_rate("EUR", AS_OF - timedelta(days=1)) == 1.13
        assert fx._db_manager.counter["queries"] == 2
        # ...which is itself memoised afterwards.
        fx.usd_rate("EUR", AS_OF - timedelta(days=1))
        assert fx._db_manager.counter["queries"] == 2

    def test_to_usd(self):
        fx = _converter({"EURUSD": [(AS_OF, 1.14)]})
        assert fx.to_usd(200.0, "EUR", AS_OF) == pytest.approx(228.0)
        assert fx.to_usd(200.0, "USD", AS_OF) == 200.0


# ---------------------------------------------------------------------------
# price_to_usd — pence (GBX) handling
# ---------------------------------------------------------------------------


class TestPriceToUsd:
    """Verified 2026-07-03: EODHD LSE closes ARE pence (AAL.LSE = 3741
    = £37.41; BP.LSE = 464.4 = £4.64), so LSE is in PENCE_QUOTED_SUFFIXES."""

    def test_lse_price_divided_by_100(self):
        assert "LSE" in PENCE_QUOTED_SUFFIXES
        fx = _converter({"GBPUSD": [(AS_OF, 1.3279)]})
        # AAL.LSE closed at 3741 GBX = £37.41 → USD
        usd = fx.price_to_usd(3741.0, "GBP", "AAL.LSE", AS_OF)
        assert usd == pytest.approx(37.41 * 1.3279)

    def test_non_pence_suffix_unscaled(self):
        fx = _converter({"EURUSD": [(AS_OF, 1.1432)]})
        usd = fx.price_to_usd(88.5, "EUR", "BMW.XETRA", AS_OF)
        assert usd == pytest.approx(88.5 * 1.1432)

    def test_usd_instrument_identity(self):
        fx = _converter({})
        assert fx.price_to_usd(212.5, "USD", "AAPL.US", AS_OF) == 212.5

    def test_no_suffix_instrument_unscaled(self):
        fx = _converter({"GBPUSD": [(AS_OF, 1.3279)]})
        assert fx.price_to_usd(10.0, "GBP", "SOMEID", AS_OF) == pytest.approx(13.279)


# ---------------------------------------------------------------------------
# CURRENCY_DECIMALS
# ---------------------------------------------------------------------------


class TestCurrencyDecimals:
    def test_whole_unit_currencies(self):
        assert CURRENCY_DECIMALS["KRW"] == 0
        assert CURRENCY_DECIMALS["JPY"] == 0

    def test_default_is_two(self):
        assert DEFAULT_CURRENCY_DECIMALS == 2
        assert CURRENCY_DECIMALS.get("EUR", DEFAULT_CURRENCY_DECIMALS) == 2


# ---------------------------------------------------------------------------
# apatheon ingestion — inversion + div-by-zero guard
# ---------------------------------------------------------------------------


class TestIngestInversion:
    def test_inverted_source_pairs(self):
        from apatheon.data_ingestion.fx_rates import FX_PAIRS, INVERTED_SOURCE

        # The low-value currencies are fetched USD-base for precision.
        assert INVERTED_SOURCE == {
            "KRWUSD": "USDKRW",
            "JPYUSD": "USDJPY",
            "HKDUSD": "USDHKD",
        }
        assert set(INVERTED_SOURCE) <= set(FX_PAIRS)

    def test_rows_for_pair_inverts_and_guards_zero(self):
        from apatheon.data_ingestion.eodhd_client import EodhdBar
        from apatheon.data_ingestion.fx_rates import _rows_for_pair

        class FakeClient:
            def __init__(self):
                self.requested: list[str] = []

            def get_eod_prices(self, symbol, start, end):
                self.requested.append(symbol)
                return [
                    EodhdBar(AS_OF, 1540.0, 1550.0, 1530.0, 1540.95, 1540.95, 0.0),
                    EodhdBar(AS_OF + timedelta(days=1), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
                ]

        client = FakeClient()
        rows = _rows_for_pair("KRWUSD", AS_OF, AS_OF + timedelta(days=1), client=client)
        # Fetched the USD-base source symbol...
        assert client.requested == ["USDKRW.FOREX"]
        # ...stored inverted under the canonical key; zero close skipped.
        assert rows == [("KRWUSD", AS_OF, pytest.approx(1.0 / 1540.95))]

    def test_rows_for_pair_direct_pair_not_inverted(self):
        from apatheon.data_ingestion.eodhd_client import EodhdBar
        from apatheon.data_ingestion.fx_rates import _rows_for_pair

        class FakeClient:
            def get_eod_prices(self, symbol, start, end):
                assert symbol == "EURUSD.FOREX"
                return [EodhdBar(AS_OF, 1.14, 1.15, 1.13, 1.1432, 1.1432, 0.0)]

        rows = _rows_for_pair("EURUSD", AS_OF, AS_OF, client=FakeClient())
        assert rows == [("EURUSD", AS_OF, pytest.approx(1.1432))]


# ---------------------------------------------------------------------------
# apply_execution_plan — USD-converted max_single_order_value
# ---------------------------------------------------------------------------


class _MiniBroker:
    """Just enough broker for apply_execution_plan's safety-check path."""

    def __init__(self):
        self.submitted = []

    def sync(self):
        pass

    def get_positions(self):
        return {}

    def get_account_state(self):
        return {"equity": 1_000_000.0}

    def submit_order(self, order):
        self.submitted.append(order)
        return order.order_id

    def get_order_status(self, order_id):
        from prometheus.execution.broker_interface import OrderStatus

        return OrderStatus.FILLED

    def get_fills(self, since=None):
        return []

    def cancel_order(self, order_id):
        return True


class TestApplyExecutionPlanFx:
    def _run(self, *, prices_currency=None, fx=None, max_value=5000.0):
        from unittest.mock import MagicMock

        from prometheus.execution.api import apply_execution_plan
        from prometheus.execution.broker_interface import OrderType

        broker = _MiniBroker()
        summary = apply_execution_plan(
            MagicMock(),  # db_manager — safety check runs before any persistence
            broker=broker,
            portfolio_id="pf-test",
            target_positions={"AAL.LSE": 100.0},
            mode="PAPER",
            as_of_date=AS_OF,
            record_positions=False,
            order_type=OrderType.LIMIT,
            prices={"AAL.LSE": 3741.0},  # pence quote
            prices_currency=prices_currency,
            fx=fx,
            max_single_order_value=max_value,
            status_poll_timeout_sec=0.0,
        )
        return broker, summary

    def test_local_price_treated_as_usd_without_fx_blocks(self):
        """Backward-compatible default: 3741 * 100 read as $374k > $5k → abort."""
        from prometheus.execution.api import ExecutionSafetyError

        with pytest.raises(ExecutionSafetyError, match="max_single_order_value"):
            self._run()

    def test_converted_price_passes_check(self):
        """With fx + prices_currency, 100 shares ≈ $4,968 < $5,000 → submits."""
        fx = _converter({"GBPUSD": [(AS_OF, 1.3279)]})
        broker, summary = self._run(
            prices_currency={"AAL.LSE": "GBP"}, fx=fx,
        )
        assert summary.num_orders == 1
        assert len(broker.submitted) == 1

    def test_fx_rate_unavailable_is_safety_error(self):
        """Missing rate → hard abort BEFORE submission, not a silent pass."""
        from prometheus.execution.api import ExecutionSafetyError

        fx = _converter({})  # no GBPUSD
        with pytest.raises(ExecutionSafetyError, match="FX rate unavailable"):
            broker, _ = self._run(prices_currency={"AAL.LSE": "GBP"}, fx=fx)
