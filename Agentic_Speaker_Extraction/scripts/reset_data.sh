#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../backend"

PYTHON_BIN="python"
if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
  PYTHON_BIN="${VIRTUAL_ENV}/bin/python"
elif [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

"$PYTHON_BIN" - <<'PY'
from sqlalchemy import text

from app.db import engine

tables = [
    "run_events",
    "run_conference_years",
    "scrape_runs",
    "extractions",
    "sources",
    "appearances",
    "physician_aliases",
    "physicians",
    "conference_years",
    "conferences",
]

with engine.connect() as conn:
    conn.execute(text(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE"))
    conn.commit()

print("Reset complete.")
PY
