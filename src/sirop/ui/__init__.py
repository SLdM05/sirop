"""Rich-based UI primitives for sirop CLI handlers.

Every prompt, table, and progress bar rendered by the CLI must go through
this package so the Textual TUI can later swap the rendering layer without
touching command logic.

All output ultimately flows through the two Console singletons in
``sirop.utils.console`` (``out``, ``err``); modules here never instantiate
their own Console.
"""

from sirop.ui.prompts import NonInteractiveError, ask, confirm, is_tty
from sirop.ui.tables import make_table

__all__ = [
    "NonInteractiveError",
    "ask",
    "confirm",
    "is_tty",
    "make_table",
]
