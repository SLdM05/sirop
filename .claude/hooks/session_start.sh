#!/usr/bin/env bash
# SessionStart hook: ensure the uv venv is present and dependencies are installed.
# Cheap: a present venv with a resolvable `sirop` import means nothing to do.

set -u
cd "$CLAUDE_PROJECT_DIR" || exit 0

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found on PATH — skipping auto-install." >&2
  exit 0
fi

if uv run --no-sync python -c "import sirop" >/dev/null 2>&1; then
  exit 0
fi

echo "sirop import failed; running 'uv sync' (one-time per fresh venv)..." >&2
uv sync --quiet
