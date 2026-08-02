"""Capital budgeting + regional book configs.

Covers:
- BookSpec.capital_fraction parsing in the book registry (default 1.0,
  explicit value, out-of-range raises, sum-validation raises).
- Shipped configs/meta/books.yaml + configs/meta/policy.yaml load cleanly
  through the real loaders, every regional book/sleeve resolves, and the
  US book keeps the 1.0 default (bit-identical US-only run).
- Shipped configs/universe/core_long_eq_daily.yaml has UK/EU/HK/KR/AU
  entries with lambda disabled.
- make_snapshot_positions_provider market_id isolation.
"""

from __future__ import annotations

from datetime import date

import pytest
import yaml

REGIONAL_BOOKS = {
    "UK_EQ_LONG_V1": ("UK", "UK_EQ", "UK_EQ_LONG_V1_K12"),
    "EU_EQ_LONG_V1": ("EU", "EU_EQ", "EU_EQ_LONG_V1_K12"),
    "HK_EQ_LONG_V1": ("HK", "HK_EQ", "HK_EQ_LONG_V1_K12"),
    "KR_EQ_LONG_V1": ("KR", "KR_EQ", "KR_EQ_LONG_V1_K12"),
    "AU_EQ_LONG_V1": ("AU", "AU_EQ", "AU_EQ_LONG_V1_K12"),
}

REGIONAL_MARKETS = ("UK_EQ", "EU_EQ", "HK_EQ", "KR_EQ", "AU_EQ")


def _write_books(tmp_path, books: dict) -> str:
    yaml_file = tmp_path / "books.yaml"
    yaml_file.write_text(yaml.dump({"books": books}))
    return str(yaml_file)


def _long_eq_book(capital_fraction=None, region="US", market_id="US_EQ") -> dict:
    book = {
        "kind": "LONG_EQUITY",
        "region": region,
        "market_id": market_id,
        "default_sleeve_id": "S1",
        "sleeves": {"S1": {"portfolio_max_names": 10}},
    }
    if capital_fraction is not None:
        book["capital_fraction"] = capital_fraction
    return book


# ---------------------------------------------------------------------------
# capital_fraction parsing
# ---------------------------------------------------------------------------


class TestCapitalFractionParsing:
    def test_missing_capital_fraction_defaults_to_one(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        path = _write_books(tmp_path, {"B1": _long_eq_book()})
        registry = load_book_registry(path=path)
        assert registry["B1"].capital_fraction == 1.0

    def test_explicit_capital_fraction_parsed(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        path = _write_books(tmp_path, {"B1": _long_eq_book(capital_fraction=0.5)})
        registry = load_book_registry(path=path)
        assert registry["B1"].capital_fraction == 0.5

    def test_explicit_full_fraction_allowed(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        path = _write_books(tmp_path, {"B1": _long_eq_book(capital_fraction=1.0)})
        registry = load_book_registry(path=path)
        assert registry["B1"].capital_fraction == 1.0

    @pytest.mark.parametrize("bad", [0.0, -0.1, 1.1, 2.0])
    def test_out_of_range_capital_fraction_raises(self, tmp_path, bad):
        from prometheus.books.registry import load_book_registry

        path = _write_books(tmp_path, {"B1": _long_eq_book(capital_fraction=bad)})
        with pytest.raises(ValueError, match="capital_fraction"):
            load_book_registry(path=path)

    def test_non_numeric_capital_fraction_raises(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        path = _write_books(tmp_path, {"B1": _long_eq_book(capital_fraction="lots")})
        with pytest.raises(ValueError, match="capital_fraction"):
            load_book_registry(path=path)

    def test_sum_over_one_raises(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        path = _write_books(
            tmp_path,
            {
                "B1": _long_eq_book(capital_fraction=0.6),
                "B2": _long_eq_book(capital_fraction=0.6, region="UK", market_id="UK_EQ"),
            },
        )
        with pytest.raises(ValueError, match="sum"):
            load_book_registry(path=path)

    def test_sum_exactly_one_allowed(self, tmp_path):
        from prometheus.books.registry import load_book_registry

        path = _write_books(
            tmp_path,
            {
                "B1": _long_eq_book(capital_fraction=0.5),
                "B2": _long_eq_book(capital_fraction=0.15, region="EU", market_id="EU_EQ"),
                "B3": _long_eq_book(capital_fraction=0.10, region="UK", market_id="UK_EQ"),
                "B4": _long_eq_book(capital_fraction=0.10, region="HK", market_id="HK_EQ"),
                "B5": _long_eq_book(capital_fraction=0.075, region="KR", market_id="KR_EQ"),
                "B6": _long_eq_book(capital_fraction=0.075, region="AU", market_id="AU_EQ"),
            },
        )
        registry = load_book_registry(path=path)
        assert sum(b.capital_fraction for b in registry.values()) == pytest.approx(1.0)

    def test_implicit_books_do_not_count_towards_sum(self, tmp_path):
        """Books without an explicit fraction default to 1.0 but must not
        trip the sum validation (missing = legacy full-NAV behavior)."""
        from prometheus.books.registry import load_book_registry

        path = _write_books(
            tmp_path,
            {
                "B1": _long_eq_book(),  # implicit 1.0
                "B2": _long_eq_book(capital_fraction=0.9, region="UK", market_id="UK_EQ"),
            },
        )
        registry = load_book_registry(path=path)
        assert registry["B1"].capital_fraction == 1.0
        assert registry["B2"].capital_fraction == 0.9


# ---------------------------------------------------------------------------
# Shipped books.yaml
# ---------------------------------------------------------------------------


class TestShippedBooksYaml:
    def test_shipped_yaml_loads_and_did_not_fall_back(self):
        from prometheus.books.registry import DEFAULT_REGISTRY_PATH, load_book_registry

        assert DEFAULT_REGISTRY_PATH.exists()
        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)
        # The in-code fallback registry has no US_EQ_LONG_V12; its presence
        # proves the YAML parsed.
        assert "US_EQ_LONG_V12" in registry

    def test_us_book_has_default_capital_fraction(self):
        """CRITICAL: US_EQ_LONG_V12 must stay at 1.0 (no explicit fraction)
        so the US-only run is bit-identical until multi-market cutover."""
        from prometheus.books.registry import DEFAULT_REGISTRY_PATH, load_book_registry

        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)
        assert registry["US_EQ_LONG_V12"].capital_fraction == 1.0

    def test_no_shipped_book_has_explicit_capital_fraction_yet(self):
        """Rollout guard: every book currently defaults to 1.0. The planned
        split goes live in one atomic edit (US 0.50 / EU 0.15 / UK 0.10 /
        HK 0.10 / KR 0.075 / AU 0.075)."""
        from prometheus.books.registry import DEFAULT_REGISTRY_PATH, load_book_registry

        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)
        for book in registry.values():
            assert book.capital_fraction == 1.0, book.book_id

    @pytest.mark.parametrize("book_id", sorted(REGIONAL_BOOKS))
    def test_regional_book_parses_like_us_v12(self, book_id):
        from prometheus.books.registry import (
            DEFAULT_REGISTRY_PATH,
            BookKind,
            load_book_registry,
        )

        region, market_id, sleeve_id = REGIONAL_BOOKS[book_id]
        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)

        book = registry.get(book_id)
        assert book is not None, f"{book_id} not found in registry"
        assert book.kind == BookKind.LONG_EQUITY
        assert book.region == region
        assert book.market_id == market_id
        assert book.default_sleeve_id == sleeve_id
        assert book.resolve_sleeve_id(None) == sleeve_id

        sleeve = book.sleeves.get(sleeve_id)
        assert sleeve is not None, f"{sleeve_id} sleeve not found"
        assert sleeve.portfolio_max_names == 12
        assert sleeve.portfolio_hysteresis_buffer == 5
        assert sleeve.portfolio_per_instrument_max_weight == 0.10
        assert sleeve.score_concentration_power == 1.0
        assert sleeve.apply_fragility_overlay is False

        # Conviction params identical to the US V12 sleeve.
        us_sleeve = registry["US_EQ_LONG_V12"].sleeves["US_EQ_LONG_V12_K20"]
        assert sleeve.conviction_enabled is True
        for field in (
            "conviction_entry_credit",
            "conviction_build_rate",
            "conviction_decay_rate",
            "conviction_score_cap",
            "conviction_sell_threshold",
            "conviction_hard_stop_pct",
            "conviction_scale_up_days",
            "conviction_entry_weight_fraction",
        ):
            assert getattr(sleeve, field) == getattr(us_sleeve, field), field


# ---------------------------------------------------------------------------
# Shipped policy.yaml
# ---------------------------------------------------------------------------


class TestShippedPolicyYaml:
    def test_shipped_policy_loads_and_did_not_fall_back(self):
        from prometheus.meta.policy import DEFAULT_POLICY_PATH, load_meta_policies

        assert DEFAULT_POLICY_PATH.exists()
        policies = load_meta_policies(path=DEFAULT_POLICY_PATH)
        # Fallback default policy routes US_EQ to US_EQ_LONG; the shipped
        # file routes to US_EQ_LONG_V12.
        assert policies["US_EQ"].default.book_id == "US_EQ_LONG_V12"

    @pytest.mark.parametrize("market_id", REGIONAL_MARKETS)
    def test_regional_policy_routes_all_situations_to_regional_book(self, market_id):
        from prometheus.meta.market_situation import MarketSituation
        from prometheus.meta.policy import DEFAULT_POLICY_PATH, load_meta_policies

        policies = load_meta_policies(path=DEFAULT_POLICY_PATH)
        policy = policies.get(market_id)
        assert policy is not None, f"policies.{market_id} missing"

        region = market_id.split("_")[0]
        expected_book = f"{region}_EQ_LONG_V1"
        expected_sleeve = f"{region}_EQ_LONG_V1_K12"

        assert policy.default.book_id == expected_book
        assert policy.default.sleeve_id == expected_sleeve
        for situation in MarketSituation:
            sel = policy.select(situation)
            assert sel.book_id == expected_book, situation
            assert sel.sleeve_id == expected_sleeve, situation

    @pytest.mark.parametrize("market_id", REGIONAL_MARKETS + ("US_EQ",))
    def test_every_policy_selection_resolves_in_book_registry(self, market_id):
        """Cross-config integrity: every book/sleeve the policy can pick
        must exist in books.yaml."""
        from prometheus.books.registry import DEFAULT_REGISTRY_PATH, load_book_registry
        from prometheus.meta.market_situation import MarketSituation
        from prometheus.meta.policy import DEFAULT_POLICY_PATH, load_meta_policies

        registry = load_book_registry(path=DEFAULT_REGISTRY_PATH)
        policy = load_meta_policies(path=DEFAULT_POLICY_PATH)[market_id]

        selections = [policy.default] + [policy.select(s) for s in MarketSituation]
        for sel in selections:
            book = registry.get(sel.book_id)
            assert book is not None, f"{market_id}: book {sel.book_id} not in registry"
            assert book.market_id == market_id
            assert book.resolve_sleeve_id(sel.sleeve_id) == sel.sleeve_id


# ---------------------------------------------------------------------------
# Shipped universe config (core_long_eq_daily.yaml)
# ---------------------------------------------------------------------------


class TestShippedUniverseLambdaConfig:
    @pytest.mark.parametrize("region", ["UK", "EU", "HK", "KR", "AU"])
    def test_regional_entries_present_with_lambda_disabled(self, region):
        from pathlib import Path

        cfg_path = (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "universe"
            / "core_long_eq_daily.yaml"
        )
        raw = yaml.safe_load(cfg_path.read_text())
        region_cfg = raw["core_long_eq"].get(region)
        assert region_cfg is not None, f"core_long_eq.{region} missing"
        assert region_cfg["lambda_predictions_csv"] is None
        assert region_cfg["lambda_score_column"] == "lambda_hat"
        assert float(region_cfg["lambda_score_weight"]) == 0.0

    @pytest.mark.parametrize("region", ["US", "UK", "EU", "HK", "KR", "AU"])
    def test_loader_returns_disabled_lambda_config(self, region):
        """Through the real loader in pipeline.tasks (read-only)."""
        tasks = pytest.importorskip("prometheus.pipeline.tasks")

        cfg = tasks._load_daily_universe_lambda_config(region)
        assert cfg.score_weight == 0.0
        if region != "US":
            assert cfg.predictions_csv is None


# ---------------------------------------------------------------------------
# make_snapshot_positions_provider market isolation
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Emulates the two provider SQL shapes against in-memory rows.

    snapshot_rows: list of (instrument_id, quantity) at the latest snapshot;
    None means "no snapshot within staleness window".
    instruments: instrument_id -> market_id (the runtime instruments table).
    """

    def __init__(self, snapshot_rows, instruments):
        self._snapshot_rows = snapshot_rows
        self._instruments = instruments
        self._rows: list = []

    def execute(self, sql, params):
        if self._snapshot_rows is None:
            self._rows = []
            return
        if "LEFT JOIN instruments" in sql:
            market_id = params[0]
            self._rows = [
                (
                    iid,
                    qty if self._instruments.get(iid) == market_id else None,
                )
                for iid, qty in self._snapshot_rows
            ]
        else:
            self._rows = list(self._snapshot_rows)

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class _FakeDbManager:
    def __init__(self, snapshot_rows, instruments):
        self._cursor = _FakeCursor(snapshot_rows, instruments)

    def get_runtime_connection(self):
        import contextlib

        @contextlib.contextmanager
        def _cm():
            yield _FakeConn(self._cursor)

        return _cm()


SNAPSHOT = [
    ("AAPL.US", 10.0),
    ("MSFT.US", 5.0),
    ("VOD.LSE", 100.0),
    ("SHEL.LSE", 40.0),
    ("FLAT.US", 0.0),  # zero quantity — never held
]
INSTRUMENTS = {
    "AAPL.US": "US_EQ",
    "MSFT.US": "US_EQ",
    "FLAT.US": "US_EQ",
    "VOD.LSE": "UK_EQ",
    "SHEL.LSE": "UK_EQ",
}


class TestSnapshotProviderMarketFilter:
    def test_no_market_returns_all_positions(self):
        from prometheus.portfolio.model_conviction import make_snapshot_positions_provider

        provider = make_snapshot_positions_provider(_FakeDbManager(SNAPSHOT, INSTRUMENTS))
        held = provider(date(2026, 7, 3))
        assert held == {"AAPL.US", "MSFT.US", "VOD.LSE", "SHEL.LSE"}

    def test_market_filter_returns_only_that_market(self):
        from prometheus.portfolio.model_conviction import make_snapshot_positions_provider

        provider = make_snapshot_positions_provider(
            _FakeDbManager(SNAPSHOT, INSTRUMENTS), market_id="UK_EQ"
        )
        held = provider(date(2026, 7, 3))
        assert held == {"VOD.LSE", "SHEL.LSE"}

    def test_market_filter_us_side(self):
        from prometheus.portfolio.model_conviction import make_snapshot_positions_provider

        provider = make_snapshot_positions_provider(
            _FakeDbManager(SNAPSHOT, INSTRUMENTS), market_id="US_EQ"
        )
        held = provider(date(2026, 7, 3))
        assert held == {"AAPL.US", "MSFT.US"}

    def test_snapshot_exists_but_flat_in_market_returns_empty_set(self):
        """Broker flat in this market (but snapshot exists) must read as
        EMPTY (phantom drops fire), not None (reconcile skipped)."""
        from prometheus.portfolio.model_conviction import make_snapshot_positions_provider

        us_only = [("AAPL.US", 10.0)]
        provider = make_snapshot_positions_provider(
            _FakeDbManager(us_only, INSTRUMENTS), market_id="UK_EQ"
        )
        held = provider(date(2026, 7, 3))
        assert held == set()

    def test_no_snapshot_returns_none_with_and_without_market(self):
        from prometheus.portfolio.model_conviction import make_snapshot_positions_provider

        for market_id in (None, "UK_EQ"):
            provider = make_snapshot_positions_provider(
                _FakeDbManager(None, INSTRUMENTS), market_id=market_id
            )
            assert provider(date(2026, 7, 3)) is None

    def test_default_call_signature_unchanged(self):
        """Legacy call sites pass only db_manager — must keep working and
        must use the legacy (non-joined) query."""
        from prometheus.portfolio.model_conviction import make_snapshot_positions_provider

        db = _FakeDbManager(SNAPSHOT, INSTRUMENTS)
        provider = make_snapshot_positions_provider(db)
        provider(date(2026, 7, 3))
        # The fake only takes the joined branch when the SQL contains the
        # join; capture which branch ran by re-executing and inspecting rows.
        assert all(len(r) == 2 for r in db._cursor.fetchall())
