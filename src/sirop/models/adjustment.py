"""Manual adjustment model.

A ``ManualAdjustment`` is a user-injected synthetic acquisition or disposition
recorded to reconcile the ACB pool with the user's actual wallet/exchange
balance when the imported transaction history is incomplete (defunct exchange,
lost CSV, hardware-wallet history the user never exported, etc.).

Each row produces a single ``ClassifiedEvent`` during the ``transfer_match``
stage, typed ``buy`` (acquire) or ``sell`` (dispose), with ``source='manual'``
and ``is_provisional=True``.  The ACB engine processes these events identically
to imported ones — the engine has no awareness of manual provenance.

The mandatory ``reason`` field plus a permanent ``audit_log`` entry per row
make these defensible under CRA "reasonable basis" record-keeping rules; the
report formatter flags rows derived from manual adjustments so an accountant
or auditor can identify them at a glance.

See ``docs/ref/reconciliation-and-missing-data.md`` for the user-facing
workflow and CRA framing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal


@dataclass(frozen=True)
class ManualAdjustment:
    """A user-recorded synthetic acquisition or disposition.

    Attributes
    ----------
    id:
        SQLite row ID (0 until persisted).
    kind:
        ``'acquire'`` (adds units to the pool with cost ``cad_value``) or
        ``'dispose'`` (removes units from the pool with proceeds ``cad_value``).
    asset:
        Asset symbol (e.g. ``"BTC"``).
    units:
        Number of units involved.  Always positive.
    cad_value:
        For ``'acquire'``: the CAD cost basis to add to the pool.
        For ``'dispose'``: the CAD proceeds.
    timestamp:
        UTC datetime the user attributes the event to (chronological position
        in the ACB event stream).
    reason:
        Free-text justification — required.  Empty strings are rejected at
        the CLI boundary.  This is what the user would show to a CRA auditor.
    created_at:
        UTC datetime the user entered the adjustment (write time).
    note:
        Optional additional annotation.
    """

    id: int
    kind: Literal["acquire", "dispose"]
    asset: str
    units: Decimal
    cad_value: Decimal
    timestamp: datetime
    reason: str
    created_at: datetime
    note: str = ""
