"""Guard: the 2026-09-03 stability rename is permanent.

nation_scores' composite/currency fields are stability scores (higher =
more stable) and were renamed composite_stability/currency_stability
across all repos plus the shared DB. Any word-boundary reappearance of
the old names is a regression.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PATTERN = re.compile(r"\b(composite_risk|currency_risk)\b")
SCAN_DIRS = ("prometheus", "prometheus_web/src", "tests")
EXTENSIONS = {".py", ".ts", ".tsx", ".js", ".sql"}
ALLOWLIST = {Path(__file__).resolve()}


def test_no_legacy_stability_field_names():
    offenders = []
    for d in SCAN_DIRS:
        base = REPO / d
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            if path.suffix not in EXTENSIONS or not path.is_file():
                continue
            if path.resolve() in ALLOWLIST or "node_modules" in path.parts:
                continue
            text = path.read_text(errors="ignore")
            for i, line in enumerate(text.splitlines(), 1):
                if PATTERN.search(line):
                    offenders.append(f"{path.relative_to(REPO)}:{i}: {line.strip()}")
    assert not offenders, "legacy stability field names found:\n" + "\n".join(offenders)
