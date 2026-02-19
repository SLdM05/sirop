from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sirop.models.enums import TransactionType


@dataclass(frozen=True)
class Transaction:
    """Fully normalized transaction. All values CAD, timestamps UTC."""

    id: str
    source: str
    timestamp: datetime
    tx_type: TransactionType
    asset: str
    amount: Decimal
    cad_value: Decimal
    fee_cad: Decimal
    fee_crypto: Decimal
    txid: str | None
    is_transfer: bool
    counterpart_id: str | None
    notes: str
