from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ClassifiedEvent:
    """Output of the transfer_match stage for a single economic event.

    Only taxable events (buys, sells, fee micro-dispositions, and income) reach
    the ACB engine. Confirmed wallet-to-wallet transfers are excluded
    (``is_taxable=False``) and are not written to this table at all.

    ``id`` is 0 before repository insertion; the repo replaces it with the
    SQLite-assigned row ID after writing.
    """

    id: int  # matches classified_events.id (0 pre-insert)
    vtx_id: int  # FK → verified_transactions.id
    timestamp: datetime
    event_type: str  # 'buy' | 'sell' | 'fee_disposal' | 'income' | 'other'
    asset: str  # e.g. "BTC"
    amount: Decimal  # crypto units involved
    cad_proceeds: Decimal | None  # FMV in CAD for disposals; None for acquisitions
    cad_cost: Decimal | None  # cost in CAD for acquisitions; None for disposals
    cad_fee: Decimal | None  # fee in CAD (selling fees for disposals, buying fees for acquisitions)
    txid: str | None
    source: str  # origin exchange / wallet
    is_taxable: bool  # False for transfer legs excluded from ACB engine
    wallet_id: int | None = None  # FK → wallets.id; None for pre-v7 rows
    is_provisional: bool = False  # True for synthetic events emitted for graph-pair deltas
