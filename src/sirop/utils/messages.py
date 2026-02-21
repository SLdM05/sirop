"""User-facing message emitter for sirop.

All user-visible output (other than logger.* calls) goes through emit().
Message text lives in config/messages.yaml so wording can be updated
without touching Python source.

Usage
-----
from sirop.utils.messages import emit
from sirop.models.messages import MessageCode

emit(MessageCode.TAP_SUCCESS, count=42, filename="export.csv", fmt="Shakepay",
     batch="my2025tax", skip_note="")

Output format by category
-------------------------
  error   → "error [E001]: text"   to stderr  (code always shown)
  warning → "warning [W001]: text" to stderr  (code always shown)
  output  → "text"                 to stdout  (no prefix, no code)
  fluff   → "text"                 to stdout  (no prefix, no code)
"""

from __future__ import annotations

import sys
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

import yaml

if TYPE_CHECKING:
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
      error / warning → stderr, prefixed with level and code identifier
      output / fluff  → stdout, message text only

    Parameters
    ----------
    code:
        A ``MessageCode`` enum value identifying the message.
    **kwargs:
        Named substitutions for the message template's ``{placeholder}``
        fields.
    """
    catalog = _load_catalog()
    entry = catalog[str(code)]
    text = entry["text"].format(**kwargs)
    category = entry["category"]
    ref = entry.get("code", "")

    match category:
        case "error":
            code_part = f" [{ref}]" if ref else ""
            print(f"error{code_part}: {text}", file=sys.stderr)
        case "warning":
            code_part = f" [{ref}]" if ref else ""
            print(f"warning{code_part}: {text}", file=sys.stderr)
        case "output" | "fluff":
            print(text)
        case _:
            print(text)
