"""Prometheus v2 – one-shot multi-market activation (the "last plug").

Applies the capital-fraction split to configs/meta/books.yaml (US 0.50 /
EU 0.15 / UK 0.10 / HK 0.10 / KR 0.075 / AU 0.075), validates the result
through the real book registry, and prints the remaining operator steps
(env var + daemon restart), which need root and are therefore NOT done
here.

The edit is text-surgical (uncomment the staged ``# capital_fraction:``
lines, insert the US line) so YAML comments survive. Idempotent: running
it twice is a no-op. ``--check`` validates preconditions without writing.

Run AFTER per-market IBKR readiness (see the enable-a-market runbook in
CLAUDE.md): qualification harness green + exchange trading permissions
enabled in IBKR Client Portal.

    python -m prometheus.scripts.maintenance.enable_multimarket [--check]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

BOOKS_YAML = Path(__file__).resolve().parents[3] / "configs" / "meta" / "books.yaml"

US_FRACTION_LINE = "    capital_fraction: 0.50  # multi-market NAV split (enable_multimarket.py)\n"
US_ANCHOR = re.compile(r"^  US_EQ_LONG_V12:\n", re.MULTILINE)
COMMENTED_FRACTION = re.compile(
    r"^(\s*)# (capital_fraction: 0\.\d+)(\s*#.*)?$", re.MULTILINE
)

EXPECTED_FRACTIONS = {
    "US_EQ_LONG_V12": 0.50,
    "EU_EQ_LONG_V1": 0.15,
    "UK_EQ_LONG_V1": 0.10,
    "HK_EQ_LONG_V1": 0.10,
    "KR_EQ_LONG_V1": 0.075,
    "AU_EQ_LONG_V1": 0.075,
}

ENV_LINE = (
    "PROMETHEUS_ACTIVE_MARKETS=US_EQ,UK_EQ,EU_EQ,HK_EQ,KR_EQ,AU_EQ,IRIS,INTEL"
)


def _apply(text: str) -> str:
    # 1) Uncomment the staged regional fraction lines.
    text = COMMENTED_FRACTION.sub(r"\1\2\3", text)
    # 2) Insert the US fraction right under the book id (if absent).
    if "capital_fraction: 0.50" not in text:
        m = US_ANCHOR.search(text)
        if not m:
            raise SystemExit("US_EQ_LONG_V12 block not found in books.yaml")
        text = text[: m.end()] + US_FRACTION_LINE + text[m.end() :]
    return text


def _validate() -> None:
    from prometheus.books.registry import load_book_registry

    registry = load_book_registry()
    problems = []
    for book_id, expected in EXPECTED_FRACTIONS.items():
        spec = registry.get(book_id)
        if spec is None:
            problems.append(f"{book_id}: missing from registry")
        elif abs(float(spec.capital_fraction) - expected) > 1e-9:
            problems.append(
                f"{book_id}: capital_fraction={spec.capital_fraction} != {expected}"
            )
    if problems:
        raise SystemExit("validation FAILED:\n  " + "\n  ".join(problems))
    total = sum(EXPECTED_FRACTIONS.values())
    print(f"registry validation OK — 6 books, Σ capital_fraction = {total:.3f}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate current state only; write nothing.",
    )
    args = parser.parse_args(argv)

    text = BOOKS_YAML.read_text()
    new_text = _apply(text)

    if args.check:
        state = "ALREADY APPLIED" if new_text == text else "READY (not applied)"
        print(f"books.yaml: {state}")
        if new_text == text:
            _validate()
        return 0

    if new_text == text:
        print("books.yaml already has the multi-market split — nothing to do.")
    else:
        BOOKS_YAML.write_text(new_text)
        print(f"books.yaml updated: capital fractions applied ({BOOKS_YAML})")

    _validate()

    print(
        "\nRemaining operator steps (root):\n"
        f"  1. sudo edit /etc/sysconfig/prometheus-daemon — set:\n"
        f"       {ENV_LINE}\n"
        "  2. sudo systemctl restart prometheus-daemon apatheon-api\n"
        "  3. Watch the first cycle: journalctl -u prometheus-daemon -f\n"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - manual CLI entry
    sys.exit(main())
