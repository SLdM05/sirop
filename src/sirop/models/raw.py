from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class RawTransaction:
    """Direct output of an importer. Not yet normalized to CAD/UTC.

    ``wallet_id`` is ``None`` when produced by an importer (importers are
    wallet-unaware).  The repository populates it from the ``wallets`` table
    when reading rows back from the database after ``tap`` has assigned a
    wallet to each raw transaction.
    """

    source: str
    timestamp: datetime
    transaction_type: str
    asset: str
    amount: Decimal
    amount_currency: str
    fiat_value: Decimal | None
    fiat_currency: str | None
    fee_amount: Decimal | None
    fee_currency: str | None
    rate: Decimal | None
    spot_rate: Decimal | None
    txid: str | None
    raw_type: str
    raw_row: dict[str, str]
    wallet_id: int | None = None  # assigned at tap time; None until persisted
