#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../backend"

PYTHON_BIN="python"
if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
  PYTHON_BIN="${VIRTUAL_ENV}/bin/python"
elif [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

# Repair stale alembic state where alembic_version=head exists but core tables are missing.
if ! "$PYTHON_BIN" - <<'PY'
from sqlalchemy import inspect
from app.db import engine
inspector = inspect(engine)
tables = set(inspector.get_table_names())
if "conferences" not in tables and "alembic_version" in tables:
    raise SystemExit(12)
PY
then
  rc=$?
  if [[ $rc -eq 12 ]]; then
    echo "[run_backend] Detected stale migration state (alembic_version without tables). Repairing..."
    "$PYTHON_BIN" -m alembic stamp base
  else
    exit $rc
  fi
fi

"$PYTHON_BIN" -m alembic upgrade head

"$PYTHON_BIN" -m uvicorn app.main:app \
  --reload \
  --reload-dir app \
  --reload-exclude ".venv/*" \
  --port 8000
