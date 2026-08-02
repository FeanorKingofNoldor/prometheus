#!/usr/bin/env bash
# Launch `folio` — the btop-style live IBKR portfolio monitor.
# Invoked as `folio` (symlink in ~/.local/bin). Read-only IBKR connection.
set -euo pipefail

PROM=/home/feanor/coding/prometheus
APATHEON=/home/feanor/coding/apatheon

# Prefer the prometheus venv (textual + plotext + ib live there).
PY="$PROM/.venv/bin/python"
if [[ ! -x "$PY" ]] || ! "$PY" -c "import textual, plotext" >/dev/null 2>&1; then
    echo "folio needs textual + plotext in $PY" >&2
    echo "Install via: $PROM/.venv/bin/pip install textual plotext" >&2
    exit 2
fi

# IBKR creds for the read-only connection.
if [[ -f "$PROM/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$PROM/.env"
    set +a
fi

export PYTHONPATH="$PROM:$APATHEON:${PYTHONPATH:-}"
exec "$PY" -m tools.folio "$@"
