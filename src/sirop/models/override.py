"""Transfer override model.

A ``TransferOverride`` records a user decision to force two transactions to be
treated as a wallet-to-wallet transfer pair (``action='link'``) or to prevent
them from being auto-matched as a transfer pair (``action='unlink'``).

Overrides are stored in the ``transfer_overrides`` table and consumed by the
transfer matcher before it runs its automatic matching logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(frozen=True)
class TransferOverride:
    """A user-specified link or unlink between two transactions.

    Attributes
    ----------
    id:
        SQLite row ID (0 until persisted).
    tx_id_a:
        ``transactions.id`` of the first transaction (typically the outgoing leg).
    tx_id_b:
        ``transactions.id`` of the second transaction (typically the incoming leg).
    action:
        ``'link'``  — force the pair to be treated as a wallet-to-wallet transfer.
        ``'unlink'`` — prevent the pair from being auto-matched as a transfer.
    created_at:
        UTC timestamp of when the override was recorded.
    note:
        Optional free-text annotation from the user.
    """

    id: int
    tx_id_a: int
    tx_id_b: int
    action: Literal["link", "unlink"]
    created_at: datetime
    note: str
