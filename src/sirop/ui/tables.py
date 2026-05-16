"""Standard Rich Table factory for sirop CLI output.

The house style is ``box=None, header_style="bold dim", pad_edge=False`` —
the same combination already used by the two existing hand-built tables
in ``cli/stir.py`` and ``cli/boil.py``. Centralising it here means future
tables stay visually consistent and the Textual TUI can later remap the
factory to a Textual DataTable without touching call sites.
"""

from __future__ import annotations

from rich.table import Table


def make_table(*, title: str | None = None) -> Table:
    """Return a Rich ``Table`` configured with sirop's house style.

    Callers add their own columns via ``table.add_column(...)`` and rows via
    ``table.add_row(...)`` exactly as they would with a vanilla ``Table``.
    """
    return Table(
        title=title,
        box=None,
        show_header=True,
        header_style="bold dim",
        pad_edge=False,
    )
