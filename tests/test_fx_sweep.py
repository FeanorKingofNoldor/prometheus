"""Tests for the FX settlement sweep planner (fixed-local-pots policy)."""

from __future__ import annotations

from prometheus.execution.fx_sweep import (
    MIN_TICKET_USD,
    RESTRICTED_CURRENCIES,
    SweepOrder,
    plan_fx_sweep,
)

RATES = {
    "EUR": 1.08,
    "GBP": 1.3357,
    "CHF": 1.10,
    "HKD": 0.1275,
    "AUD": 0.66,
    "KRW": 0.00072,
}


def test_negative_eur_buys_eurusd_in_eur_units():
    orders = plan_fx_sweep({"EUR": -10_000.0}, RATES, threshold_usd=2000)
    assert len(orders) == 1
    o = orders[0]
    assert (o.pair, o.action) == ("EURUSD", "BUY")
    assert o.quantity == 10_000.0  # EUR units
    assert abs(o.usd_value - 10_800.0) < 1e-6


def test_negative_hkd_sells_usdhkd_in_usd_units():
    orders = plan_fx_sweep({"HKD": -100_000.0}, RATES, threshold_usd=2000)
    assert len(orders) == 1
    o = orders[0]
    assert (o.pair, o.action) == ("USDHKD", "SELL")
    # qty in USD = 100,000 HKD × 0.1275 USD/HKD
    assert abs(o.quantity - 12_750.0) < 1e-6


def test_positive_balances_are_the_pot_and_never_swept():
    orders = plan_fx_sweep(
        {"EUR": 50_000.0, "GBP": 30_000.0, "HKD": 400_000.0},
        RATES,
        threshold_usd=2000,
    )
    assert orders == []


def test_below_threshold_ignored():
    # -1000 EUR ≈ -$1080 < $2000 threshold
    assert plan_fx_sweep({"EUR": -1_000.0}, RATES, threshold_usd=2000) == []


def test_min_ticket_floor_applies_even_with_tiny_threshold():
    # threshold 0 still respects MIN_TICKET_USD
    small = (MIN_TICKET_USD / RATES["EUR"]) * 0.5
    assert plan_fx_sweep({"EUR": -small}, RATES, threshold_usd=0) == []


def test_krw_never_swept():
    assert "KRW" in RESTRICTED_CURRENCIES
    orders = plan_fx_sweep({"KRW": -50_000_000.0}, RATES, threshold_usd=2000)
    assert orders == []


def test_base_currency_skipped():
    assert plan_fx_sweep({"USD": -1_000_000.0}, RATES, threshold_usd=2000) == []


def test_missing_rate_skips_loudly_not_crashes():
    orders = plan_fx_sweep({"SEK": -100_000.0}, {}, threshold_usd=2000)
    assert orders == []


def test_multi_currency_first_trading_day():
    """Flip-day shape: several negative balances at once."""
    balances = {
        "EUR": -37_500.0,   # EU book buys
        "GBP": -18_700.0,   # UK book
        "HKD": -196_000.0,  # HK book
        "AUD": -28_400.0,   # AU book
        "KRW": -26_000_000.0,  # trade-linked — must be ignored
        "USD": 120_000.0,
    }
    orders = plan_fx_sweep(balances, RATES, threshold_usd=2000)
    by_ccy = {o.currency: o for o in orders}
    assert set(by_ccy) == {"EUR", "GBP", "HKD", "AUD"}
    assert by_ccy["EUR"].action == "BUY" and by_ccy["EUR"].pair == "EURUSD"
    assert by_ccy["GBP"].action == "BUY" and by_ccy["GBP"].pair == "GBPUSD"
    assert by_ccy["AUD"].action == "BUY" and by_ccy["AUD"].pair == "AUDUSD"
    assert by_ccy["HKD"].action == "SELL" and by_ccy["HKD"].pair == "USDHKD"
    assert all(isinstance(o, SweepOrder) for o in orders)
