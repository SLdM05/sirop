"""User-facing message emitter for sirop.

All user-visible output (other than logger.* calls) goes through emit().
Message text lives in config/messages.yaml so wording can be updated
without touching Python source.

Usage
-----
from sirop.utils.messages import emit, spinner
from sirop.models.messages import MessageCode

emit(MessageCode.TAP_SUCCESS, count=42, filename="export.csv", fmt="Shakepay",
     batch="my2025tax", skip_note="")

with spinner("Reading export.csv…"):
    rows = importer.parse(path)

Output format by category
-------------------------
  error   → "error [E001]: text"   to stderr  (code always shown, bold red)
  warning → "warning [W001]: text" to stderr  (code always shown, bold yellow)
  output  → "text"                 to stdout  (no prefix, no code)
  fluff   → "text"                 to stdout  (no prefix, no code, dimmed)
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

import yaml
from rich.text import Text

from sirop.utils.console import err as _err
from sirop.utils.console import out as _out

if TYPE_CHECKING:
    from collections.abc import Generator

    from rich.status import Status

    from sirop.models.messages import MessageCode

_CATALOG_PATH = Path("config/messages.yaml")


class _MessageEntry(TypedDict):
    category: str
    text: str
    code: NotRequired[str]


@lru_cache(maxsize=1)
def _load_catalog() -> dict[str, _MessageEntry]:
    """Load and cache the message catalog from config/messages.yaml."""
    if not _CATALOG_PATH.exists():
        raise FileNotFoundError(
            f"sirop message catalog not found at {_CATALOG_PATH}. "
            "Is the working directory the project root?"
        )
    with _CATALOG_PATH.open(encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh)
    return raw["messages"]  # type: ignore[no-any-return]


def emit(code: MessageCode, **kwargs: object) -> None:
    """Look up *code* in the message catalog, format with *kwargs*, and print.

    Routing by category:
      error / warning → stderr, prefixed with level and code (bold coloured)
      output          → stdout, message text only
      fluff           → stdout, message text dimmed

    Parameters
    ----------
    code:
        A ``MessageCode`` enum value identifying the message.
    **kwargs:
        Named substitutions for the message template's ``{placeholder}``
        fields.
    """
    catalog = _load_catalog()
    key = str(code)
    try:
        entry = catalog[key]
    except KeyError:
        raise RuntimeError(
            f"sirop bug: no catalog entry for message code {key!r}. "
            "Check that config/messages.yaml and MessageCode are in sync."
        ) from None

    try:
        text = entry["text"].format(**kwargs)
    except KeyError as exc:
        raise RuntimeError(
            f"sirop bug: message {key!r} is missing required kwarg {exc}. "
            "Check the emit() call site."
        ) from None

    category = entry["category"]
    ref = entry.get("code", "")

    match category:
        case "error":
            t = Text()
            t.append(f"error [{ref}]: " if ref else "error: ", style="bold red")
            t.append(text)
            _err.print(t)
        case "warning":
            t = Text()
            t.append(f"warning [{ref}]: " if ref else "warning: ", style="bold yellow")
            t.append(text)
            _err.print(t)
        case "output":
            _out.print(text, markup=False)
        case "fluff":
            _out.print(text, style="dim", markup=False)
        case _:
            raise RuntimeError(
                f"sirop bug: unknown message category {category!r} for code {key!r}. "
                "Valid categories: error, warning, output, fluff."
            )


@contextmanager
def spinner(text: str) -> Generator[Status, None, None]:
    """Context manager showing a transient spinner for long-running operations.

    Yields the Rich ``Status`` object so callers can update the displayed text
    mid-operation (e.g. to show a progress counter).  The spinner clears itself
    on exit. Any ``emit()`` calls made inside the context are printed above the
    spinner line and remain visible.

    Parameters
    ----------
    text:
        Short description displayed next to the spinner dots.

    Example
    -------
    Basic use — no update needed::

        with spinner("Normalizing transactions…"):
            result = do_work()

    With live counter::

        with spinner(f"Fetching prices… [0/{total}]") as status:
            for i, item in enumerate(items, 1):
                fetch(item)
                status.update(f"Fetching prices… [{i}/{total}]")
    """
    with _err.status(text, spinner="dots") as status:
        yield status
