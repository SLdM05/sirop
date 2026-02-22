"""Wallet model.

A ``Wallet`` represents a named source of transactions — an exchange account,
a hardware wallet, or any other bucket where crypto is held.

Wallets are assigned at ``tap`` time and persisted in the ``wallets`` table.
Two kinds exist:

- **Auto-created** (``auto_created=True``): created implicitly when tapping a
  file, named after the detected source format (e.g. ``"shakepay"``).  If the
  user taps multiple Shakepay exports, they all share the same auto-created
  wallet.
- **User-named** (``auto_created=False``): created when the user passes
  ``--wallet NAME`` to ``tap``, or calls ``sirop wallet create``.  Useful for
  distinguishing two accounts at the same exchange, or for labelling hardware
  wallets before their CSV is available.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(frozen=True)
class Wallet:
    """A named source of transactions.

    Attributes
    ----------
    id:
        SQLite row ID (0 until persisted).
    name:
        Unique display name, e.g. ``"shakepay"`` or ``"ledger-cold-storage"``.
    source:
        Format key of the importer that created this wallet, e.g. ``"shakepay"``,
        ``"ndax"``, ``"sparrow"``.  Empty string for manually created wallets.
    auto_created:
        ``True`` if the wallet was created automatically when tapping a file.
        ``False`` if the user explicitly named it.
    created_at:
        UTC timestamp of when the wallet was first recorded.
    note:
        Optional free-text annotation.
    """

    id: int
    name: str
    source: str
    auto_created: bool
    created_at: datetime
    note: str
