from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sirop.models.enums import TransactionType


@dataclass(frozen=True)
class Transaction:
    """Fully normalized transaction. All values CAD, timestamps UTC.

    `id` is the SQLite row ID assigned by the repository after insert.
    `counterpart_id` links the matching deposit/withdrawal for a
    wallet-to-wallet transfer pair (set by the transfer_match stage).
    """

    id: int  # matches transactions.id (INTEGER PRIMARY KEY)
    source: str
    timestamp: datetime
    tx_type: TransactionType
    asset: str
    amount: Decimal
    cad_value: Decimal
    fee_cad: Decimal
    fee_crypto: Decimal
    txid: str | None
    is_transfer: bool  # True once transfer_match confirms a wallet-to-wallet pair
    counterpart_id: int | None  # FK → transactions.id of the matching leg
    notes: str
