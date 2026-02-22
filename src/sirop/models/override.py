"""Transfer override model.

A ``TransferOverride`` records a user decision about how a transaction should
be classified by the transfer matcher.  Four actions are supported:

``'link'``
    Force two transactions to be treated as a wallet-to-wallet transfer pair
    (non-taxable).  ``implied_fee_crypto`` carries the exact difference between
    the sent and received amounts; the boil stage converts it into a
    fee micro-disposition.

``'unlink'``
    Prevent two transactions from being auto-matched as a transfer pair.  Both
    legs will be classified as independent taxable events (sell/buy).

``'external-out'``
    Mark a withdrawal as crypto sent to an external / untracked wallet.  The
    transaction is a non-taxable transfer out of the tracked universe.
    ``external_wallet`` names the destination.  ``tx_id_b`` is ``None``.

``'external-in'``
    Mark a deposit as crypto received from an external / untracked wallet.
    The transaction is a non-taxable transfer into the tracked universe;
    ACB is established at FMV on receipt.  ``external_wallet`` names the source.
    ``tx_id_b`` is ``None``.

Overrides are stored in the ``transfer_overrides`` table and consumed by the
transfer matcher (via ``match_transfers``) before it runs auto-matching.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal


@dataclass(frozen=True)
class TransferOverride:
    """A user-specified classification decision for one or two transactions.

    Attributes
    ----------
    id:
        SQLite row ID (0 until persisted).
    tx_id_a:
        ``transactions.id`` of the primary transaction (typically the outgoing
        leg for ``'link'``/``'external-out'``, or the incoming leg for
        ``'external-in'``).
    tx_id_b:
        ``transactions.id`` of the counterpart transaction.  ``None`` for
        ``'external-out'`` and ``'external-in'`` overrides (no matched leg).
    action:
        Classification decision — see module docstring.
    implied_fee_crypto:
        For ``'link'`` overrides: the exact difference between sent and received
        amounts (``sent - received``).  The boil stage emits a fee
        micro-disposition for this amount.  ``Decimal("0")`` when the amounts
        balance exactly.  Always zero for other action types.
    external_wallet:
        For ``'external-out'`` and ``'external-in'`` overrides: the name of the
        external / untracked wallet.  Empty string for ``'link'``/``'unlink'``.
    created_at:
        UTC timestamp of when the override was recorded.
    note:
        Optional free-text annotation from the user.
    """

    id: int
    tx_id_a: int
    tx_id_b: int | None
    action: Literal["link", "unlink", "external-out", "external-in"]
    implied_fee_crypto: Decimal
    external_wallet: str
    created_at: datetime
    note: str
