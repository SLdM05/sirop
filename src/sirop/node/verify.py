"""Pure on-chain fee/timestamp validation for the verify stage.

Given a list of normalized :class:`Transaction` rows, fetch each one's on-chain
data from a Mempool-compatible node (via injected ``fetch_tx``) and produce
enriched :class:`VerifiedRow` results.

Authoritative override rule
---------------------------
The on-chain fee is paid by whoever signed the inputs.  We only override the
imported ``fee_crypto`` when the user owns those inputs — i.e. the row comes
from a user-controlled wallet (``wallet_id IS NOT NULL``) AND its transaction
type is a send (``transfer_out`` / ``withdrawal``).  Exchange-side rows still
receive ``node_verified=1`` and ``block_height`` so downstream stages have
authoritative confirmation metadata, but their ``fee_crypto`` is preserved.

Pure module: no I/O, no DB access.  ``fetch_tx`` is injected by the caller.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.models.enums import TransactionType
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from sirop.models.transaction import Transaction
    from sirop.node.models import OnChainTx

logger = get_logger(__name__)


_SATS_PER_BTC: Decimal = Decimal("100000000")

# Send-side transaction types whose on-chain fee is genuinely user-paid when
# the spending wallet is user-controlled.
_USER_SEND_TYPES: frozenset[TransactionType] = frozenset(
    {TransactionType.TRANSFER_OUT, TransactionType.WITHDRAWAL}
)


@dataclass(frozen=True)
class AuditEntry:
    """One authoritative override recorded for the audit trail.

    Written into ``audit_log`` by the repository layer.  ``old_value`` and
    ``new_value`` are the pre/post canonical Decimal strings (or ISO 8601
    timestamps for the ``timestamp`` field).
    """

    txid: str
    field: str  # "fee_crypto" | "timestamp"
    old_value: str
    new_value: str
    reason: str


@dataclass(frozen=True)
class VerifiedRow:
    """Enriched transaction ready for ``verified_transactions``.

    ``tx`` is the Transaction with possibly-corrected ``fee_crypto`` and
    ``timestamp``.  ``node_verified`` is True when on-chain data was applied.
    ``block_height`` is populated whenever a confirmed on-chain record was
    found, even if no field was overridden (exchange-side rows).
    """

    tx: Transaction
    node_verified: bool
    block_height: int | None
    confirmations: int | None


def is_user_paid_send(tx: Transaction) -> bool:
    """Return True if *tx* represents a send from a user-controlled wallet.

    Only such rows are eligible for fee override — the on-chain fee was paid
    out of UTXOs owned by the user, so it is the authoritative tax-relevant
    fee.  Exchange-side withdrawals (``wallet_id IS NULL``) are excluded:
    the exchange paid the on-chain fee out of its own UTXOs and charged the
    user a separate (often higher) withdrawal fee in the exchange ledger.
    """
    return tx.wallet_id is not None and tx.tx_type in _USER_SEND_TYPES


def validate_fees(
    rows: list[Transaction],
    fetch_tx: Callable[[str, str], OnChainTx | None],
    mempool_url: str,
    *,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[list[VerifiedRow], list[AuditEntry]]:
    """Enrich *rows* with on-chain data; return verified rows + audit trail.

    Behaviour per row:

    - No ``txid``                       → pass-through, ``node_verified=False``.
    - ``fetch_tx`` returns ``None``     → pass-through, ``node_verified=False``.
    - Tx unconfirmed (no ``fee_sat``)   → pass-through, ``node_verified=False``.
    - Confirmed, user-paid send         → override ``fee_crypto`` if different.
                                          Override ``timestamp`` to block time.
                                          ``node_verified=True``.
    - Confirmed, not user-paid          → ``node_verified=True`` and populate
                                          ``block_height`` only; ``fee_crypto``
                                          and ``timestamp`` preserved.

    Each override is recorded as an :class:`AuditEntry`.  All fee values use
    canonical Decimal string serialization (``format(d, 'f')``).

    Parameters
    ----------
    rows:
        Normalized transactions to verify.
    fetch_tx:
        Injected ``mempool_client.fetch_tx`` (or a fake in tests).
    mempool_url:
        Base URL passed to ``fetch_tx`` as its first argument.
    on_progress:
        Optional callback ``(done, total)`` invoked after each fetch.
    """
    verified: list[VerifiedRow] = []
    audit: list[AuditEntry] = []
    total = len(rows)

    for idx, tx in enumerate(rows, start=1):
        if tx.txid is None:
            verified.append(
                VerifiedRow(
                    tx=tx,
                    node_verified=False,
                    block_height=None,
                    confirmations=None,
                )
            )
            if on_progress is not None:
                on_progress(idx, total)
            continue

        onchain = fetch_tx(mempool_url, tx.txid)
        if on_progress is not None:
            on_progress(idx, total)

        if onchain is None or not onchain.confirmed or onchain.fee_sat is None:
            verified.append(
                VerifiedRow(
                    tx=tx,
                    node_verified=False,
                    block_height=None,
                    confirmations=None,
                )
            )
            continue

        block_height = onchain.block_height

        if not is_user_paid_send(tx):
            verified.append(
                VerifiedRow(
                    tx=tx,
                    node_verified=True,
                    block_height=block_height,
                    confirmations=None,
                )
            )
            continue

        # User-paid send: authoritative override.
        on_chain_fee = (Decimal(onchain.fee_sat) / _SATS_PER_BTC).quantize(Decimal("0.00000001"))
        new_tx = tx
        if on_chain_fee != tx.fee_crypto:
            audit.append(
                AuditEntry(
                    txid=tx.txid,
                    field="fee_crypto",
                    old_value=format(tx.fee_crypto, "f"),
                    new_value=format(on_chain_fee, "f"),
                    reason="node override: imported fee replaced with on-chain fee",
                )
            )
            new_tx = replace(new_tx, fee_crypto=on_chain_fee)

        if onchain.block_time is not None and onchain.block_time != tx.timestamp:
            audit.append(
                AuditEntry(
                    txid=tx.txid,
                    field="timestamp",
                    old_value=tx.timestamp.isoformat(),
                    new_value=onchain.block_time.isoformat(),
                    reason="node override: imported timestamp replaced with block time",
                )
            )
            new_tx = replace(new_tx, timestamp=onchain.block_time)

        verified.append(
            VerifiedRow(
                tx=new_tx,
                node_verified=True,
                block_height=block_height,
                confirmations=None,
            )
        )

    return verified, audit
