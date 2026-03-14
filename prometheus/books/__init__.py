"""Prometheus v2 – Books and sleeves.

A *book* represents a macro objective (e.g. long equities, hedge via ETFs).
A *sleeve* is a concrete implementation variant within a book.

This package provides:
- configuration/registry structures for books and sleeves,
- helpers used by the daily pipeline and by backtest runners.
"""

from __future__ import annotations

from prometheus.books.registry import (
    AllocatorSleeveSpec,
    BookKind,
    BookSpec,
    HedgeEtfSleeveSpec,
    LongEquitySleeveSpec,
    load_book_registry,
)

__all__ = [
    "AllocatorSleeveSpec",
    "BookKind",
    "BookSpec",
    "HedgeEtfSleeveSpec",
    "LongEquitySleeveSpec",
    "load_book_registry",
]
