from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class RawTransaction:
    """Direct output of an importer. Not yet normalized to CAD/UTC."""

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
