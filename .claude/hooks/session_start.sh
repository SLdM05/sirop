#!/usr/bin/env bash
# SessionStart hook: ensure Poetry venv is present and dependencies are installed.
# Cheap: a present venv with a resolvable `sirop` import means nothing to do.

set -u
cd "$CLAUDE_PROJECT_DIR" || exit 0

if ! command -v poetry >/dev/null 2>&1; then
  echo "poetry not found on PATH — skipping auto-install." >&2
  exit 0
fi

if poetry run python -c "import sirop" >/dev/null 2>&1; then
  exit 0
fi

echo "sirop import failed; running 'poetry install' (one-time per fresh venv)..." >&2
poetry install --quiet
