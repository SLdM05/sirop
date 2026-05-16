"""Lint-style guard: no bare ``input()`` under ``src/sirop/cli/``.

The CLI handlers must use ``sirop.ui.ask`` / ``sirop.ui.confirm`` for user
input. Rich's ``Console.input(...)`` (e.g. ``out.input(...)``) is allowed —
it's the Rich-styled equivalent and the seam the future Textual TUI will
swap.

If you legitimately need ``input()`` somewhere new, prefer adding a helper
to ``sirop.ui.prompts`` instead so the prompt stays consistent.
"""

from __future__ import annotations

import re
from pathlib import Path

_CLI_DIR = Path(__file__).resolve().parent.parent / "src" / "sirop" / "cli"

# Match a bare call to input(...) — possibly with leading whitespace — but not
# qualified attribute access like ``out.input(...)`` (Rich Console.input).
_BARE_INPUT_RE = re.compile(r"(?<![.\w])input\s*\(")


def test_no_bare_input_in_cli_handlers() -> None:
    offenders: list[str] = []
    for path in sorted(_CLI_DIR.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if _BARE_INPUT_RE.search(line):
                rel = path.relative_to(_CLI_DIR.parent.parent.parent)
                offenders.append(f"{rel}:{i}: {stripped}")
    assert not offenders, (
        "Found bare input() in CLI handlers — replace with sirop.ui.ask / "
        "sirop.ui.confirm:\n  " + "\n  ".join(offenders)
    )
