"""On-chain data models for the Bitcoin node / graph traversal module.

All dataclasses are frozen (value objects). Monetary values are kept as
raw satoshi integers here — conversion to Decimal BTC happens at the
boundary where these models are consumed by sirop's transfer matcher.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from datetime import datetime
    from decimal import Decimal


@dataclass(frozen=True)
class OnChainTx:
    """Minimal representation of a Bitcoin transaction fetched from the node.

    ``vin_txids`` contains the txid of each input's previous output (the
    parent transactions).  Used by backward traversal to climb the UTXO
    ancestry tree.

    ``vout_count`` is the number of outputs on this transaction.  The
    actual output spending information is fetched separately via
    ``fetch_outspends`` to avoid over-fetching on deep traversals.
    """

    txid: str
    fee_sat: int | None  # miner fee in satoshis; None when unconfirmed
    confirmed: bool
    block_time: datetime | None  # UTC; None when unconfirmed
    vin_txids: tuple[str, ...] = field(default_factory=tuple)
    vout_count: int = 0


@dataclass(frozen=True)
class TxOutspend:
    """Spend status of one output of a Bitcoin transaction.

    Returned by ``fetch_outspends`` as a list whose index matches the
    output index (vout) of the parent transaction.
    """

    spent: bool
    txid: str | None = None  # txid of the spending transaction; None if unspent
    vin: int | None = None  # vin index in the spending transaction


@dataclass(frozen=True)
class AddressTransaction:
    """One transaction that sends value to a queried Bitcoin address.

    Returned by ``fetch_address_txs`` for Pass 1.25 (address-based txid
    resolution).  Used to resolve a Shakepay withdrawal's on-chain txid
    from the known recipient address.
    """

    txid: str
    received_sats: int  # satoshis sent to the queried address in this tx
    block_time: int | None  # Unix timestamp of block confirmation; None if unconfirmed


@dataclass(frozen=True)
class GraphMatch:
    """A transfer pair discovered via on-chain UTXO graph traversal.

    ``direction`` is ``"backward"`` when the match was found by following
    the deposit's input ancestry (unmatched deposit → search backward for
    a known withdrawal) or ``"forward"`` when found by following the
    withdrawal's output spend-chain (unmatched withdrawal → search
    forward for a known deposit).

    ``hops`` is the number of intermediate on-chain transactions between
    the withdrawal and the deposit (0 = direct parent/child, same txid
    would have been caught by Pass 1 txid match, so in practice hops ≥ 1).

    ``fee_crypto`` is the amount difference (withdrawal amount minus
    deposit amount) expressed in the asset's native unit.  It may include
    multiple intermediate miner fees when ``hops > 1``.
    """

    deposit_db_id: int  # Transaction.id of the deposit leg
    withdrawal_db_id: int  # Transaction.id of the withdrawal leg
    direction: Literal["backward", "forward"]
    hops: int
    fee_crypto: Decimal
    deposit_vout_count: int = 0  # number of outputs on the deposit tx (2 = payment+change)
    deposit_vin_count: int = 0  # number of inputs on the deposit tx (5+ = possible CoinJoin)
