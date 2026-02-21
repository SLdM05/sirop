"""Transfer matcher — pipeline stage 4.

Consumes the full list of verified transactions and produces two outputs:

1. ``classified_events`` — taxable events (buys, sells, fee micro-dispositions,
   income) that the ACB engine will process.
2. ``income_events`` — income sub-records (staking, airdrops) for TP-21.4.39-V.

Wallet-to-wallet transfers are identified and excluded entirely.  When an
on-chain transfer has an associated network fee, that fee is emitted as a
``fee_disposal`` event so the ACB engine can record the tiny capital gain/loss.

Design notes
------------
- **txid matching** is the primary matching signal — if both legs (withdrawal
  and deposit) carry the same on-chain txid, they are definitively paired.
- **Amount proximity** is the secondary signal: the deposit amount must be
  within ``FEE_TOLERANCE_RELATIVE`` of the withdrawal amount (accounting for
  the network fee deducted from the transferred amount).
- The search window is ``MATCH_WINDOW_HOURS`` hours around the withdrawal
  timestamp.  Cross-blockchain transfers that settle slowly are handled by the
  wider default window.
- A withdrawal that is not matched becomes a regular sell event (a disposition
  of crypto at the withdrawal CAD value).  This is the conservative choice —
  the user or a future node-verification stage can correct it.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.models.disposition import IncomeEvent
from sirop.models.enums import TransactionType
from sirop.models.event import ClassifiedEvent
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    from sirop.models.transaction import Transaction

logger = get_logger(__name__)

# Maximum time difference between a withdrawal and its matching deposit.
MATCH_WINDOW_HOURS: int = 4

# Relative tolerance for amount matching (e.g. 0.01 = 1% difference allowed).
# Covers typical Bitcoin network fees relative to the transfer amount.
FEE_TOLERANCE_RELATIVE: Decimal = Decimal("0.01")

# Absolute tolerance floor so tiny transfers aren't rejected on rounding.
FEE_TOLERANCE_ABSOLUTE: Decimal = Decimal("0.00001")

# TransactionType values that represent outgoing crypto (potential withdrawal leg).
_OUTGOING = frozenset({TransactionType.WITHDRAWAL, TransactionType.TRANSFER_OUT})

# TransactionType values that represent incoming crypto (potential deposit leg).
_INCOMING = frozenset({TransactionType.DEPOSIT, TransactionType.TRANSFER_IN})

# TransactionType values that are non-taxable fiat movements — always excluded.
_FIAT = frozenset({TransactionType.FIAT_DEPOSIT, TransactionType.FIAT_WITHDRAWAL})

# Income types that generate both a ClassifiedEvent (acquisition) and an IncomeEvent.
_INCOME_TYPES = frozenset({TransactionType.INCOME})


def match_transfers(
    txs: list[Transaction],
) -> tuple[list[ClassifiedEvent], list[IncomeEvent]]:
    """Classify *txs* into taxable events and income sub-records.

    Parameters
    ----------
    txs:
        Output of ``repositories.read_transactions()`` — sorted chronologically.

    Returns
    -------
    tuple[list[ClassifiedEvent], list[IncomeEvent]]
        - First element: classified events for the ACB engine (is_taxable=True)
          plus non-taxable transfer records (is_taxable=False).
        - Second element: income sub-records for TP-21.4.39-V Part 6.
    """
    sorted_txs = sorted(txs, key=lambda t: t.timestamp)

    # Track which transaction IDs have been paired as transfer legs.
    paired_ids: set[int] = set()

    # --- Pass 1: match outgoing ↔ incoming transfers ---
    fee_disposals: list[ClassifiedEvent] = []

    for tx in sorted_txs:
        if tx.tx_type not in _OUTGOING:
            continue
        if tx.id in paired_ids:
            continue

        match = _find_match(tx, sorted_txs, paired_ids)
        if match is None:
            continue

        # Mark both legs as paired.
        paired_ids.add(tx.id)
        paired_ids.add(match.id)

        # Emit a fee micro-disposition if the withdrawal carried a network fee.
        if tx.fee_crypto and tx.fee_crypto > Decimal("0") and tx.cad_value > Decimal("0"):
            cad_rate = tx.cad_value / tx.amount if tx.amount else Decimal("0")
            fee_proceeds = tx.fee_crypto * cad_rate
            fee_disposals.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=tx.id,
                    timestamp=tx.timestamp,
                    event_type="fee_disposal",
                    asset=tx.asset,
                    amount=tx.fee_crypto,
                    cad_proceeds=fee_proceeds,
                    cad_cost=None,
                    cad_fee=None,
                    txid=tx.txid,
                    source=tx.source,
                    is_taxable=True,
                )
            )

    # --- Pass 2: convert remaining transactions to ClassifiedEvents ---
    events: list[ClassifiedEvent] = []
    income_events: list[IncomeEvent] = []

    for tx in sorted_txs:
        if tx.id in paired_ids:
            # Confirmed transfer leg — emit a non-taxable marker so the DB has
            # a record, but it won't reach the ACB engine.
            events.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=tx.id,
                    timestamp=tx.timestamp,
                    event_type="transfer",
                    asset=tx.asset,
                    amount=tx.amount,
                    cad_proceeds=None,
                    cad_cost=None,
                    cad_fee=tx.fee_cad if tx.fee_cad else None,
                    txid=tx.txid,
                    source=tx.source,
                    is_taxable=False,
                )
            )
            continue

        evt, income = _classify(tx)
        if evt is not None:
            events.append(evt)
        if income is not None:
            income_events.append(income)

    # Insert fee micro-disposals in chronological order.
    events.extend(fee_disposals)
    events.sort(key=lambda e: e.timestamp)

    # Log summary.
    taxable = sum(1 for e in events if e.is_taxable)
    transfers = sum(1 for e in events if not e.is_taxable)
    logger.info(
        "transfer_match: %d events (%d taxable, %d transfers), %d income events",
        len(events),
        taxable,
        transfers,
        len(income_events),
    )
    if paired_ids:
        logger.info("transfer_match: %d transfer legs paired", len(paired_ids))

    return events, income_events


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_match(
    outgoing: Transaction,
    all_txs: list[Transaction],
    already_paired: set[int],
) -> Transaction | None:
    """Search *all_txs* for a deposit that matches *outgoing*.

    Match criteria (in priority order):
    1. Same non-null txid.
    2. Same asset, amount within tolerance, timestamp within window.
    """
    window = timedelta(hours=MATCH_WINDOW_HOURS)
    window_start = outgoing.timestamp - window
    window_end = outgoing.timestamp + window

    for candidate in all_txs:
        if candidate.id == outgoing.id:
            continue
        if candidate.id in already_paired:
            continue
        if candidate.tx_type not in _INCOMING:
            continue
        if candidate.asset != outgoing.asset:
            continue

        # Txid match — definitive.
        if outgoing.txid and candidate.txid and outgoing.txid == candidate.txid:
            logger.debug(
                "transfer_match: txid match for %s %s",
                outgoing.asset,
                outgoing.txid,
            )
            return candidate

        # Amount + timestamp proximity match.
        if not (window_start <= candidate.timestamp <= window_end):
            continue

        # The deposit amount should ≈ withdrawal amount minus fee.
        expected_deposit = outgoing.amount - (outgoing.fee_crypto or Decimal("0"))
        tolerance = max(
            expected_deposit * FEE_TOLERANCE_RELATIVE,
            FEE_TOLERANCE_ABSOLUTE,
        )
        if abs(candidate.amount - expected_deposit) <= tolerance:
            logger.debug(
                "transfer_match: amount+time match for %s on %s",
                outgoing.asset,
                outgoing.timestamp.date(),
            )
            return candidate

    return None


def _classify(tx: Transaction) -> tuple[ClassifiedEvent | None, IncomeEvent | None]:
    """Convert a single ``Transaction`` to a ``ClassifiedEvent`` (and optionally
    an ``IncomeEvent`` for income transactions).

    Returns a non-taxable placeholder for fiat movements and unknown types.
    """
    tx_type = tx.tx_type

    # Income (staking / airdrop / mining) — returns both event and income sub-record.
    if tx_type in _INCOME_TYPES:
        return _classify_income(tx)

    # Acquisition types: buy, trade, and unmatched deposit.
    if tx_type in {TransactionType.BUY, TransactionType.TRADE, TransactionType.DEPOSIT}:
        return _classify_acquisition(tx), None

    # Disposal types: sell, spend, and unmatched withdrawal.
    if tx_type in {TransactionType.SELL, TransactionType.SPEND, TransactionType.WITHDRAWAL}:
        return _classify_disposal(tx), None

    # Fiat and everything else — non-taxable placeholder.
    if tx_type not in _FIAT:
        logger.debug(
            "transfer_match: no classification rule for tx_type=%s — marking non-taxable",
            tx_type.value,
        )
    return (
        ClassifiedEvent(
            id=0,
            vtx_id=tx.id,
            timestamp=tx.timestamp,
            event_type="fiat" if tx_type in _FIAT else "other",
            asset=tx.asset,
            amount=tx.amount,
            cad_proceeds=None,
            cad_cost=None,
            cad_fee=tx.fee_cad if tx.fee_cad else None,
            txid=tx.txid,
            source=tx.source,
            is_taxable=False,
        ),
        None,
    )


def _classify_acquisition(tx: Transaction) -> ClassifiedEvent:
    """Build a buy ClassifiedEvent for acquisition transactions.

    Emits a warning for unmatched deposits so the user knows to check
    whether the deposit is actually a wallet-to-wallet transfer.
    """
    if tx.tx_type == TransactionType.DEPOSIT:
        logger.warning(
            "transfer_match: unmatched deposit of %s %s on %s — treating as buy. "
            "If this is a wallet-to-wallet transfer, tag it manually.",
            tx.amount,
            tx.asset,
            tx.timestamp.date(),
        )
    return ClassifiedEvent(
        id=0,
        vtx_id=tx.id,
        timestamp=tx.timestamp,
        event_type="buy",
        asset=tx.asset,
        amount=tx.amount,
        cad_proceeds=None,
        cad_cost=tx.cad_value,
        cad_fee=tx.fee_cad if tx.fee_cad else None,
        txid=tx.txid,
        source=tx.source,
        is_taxable=tx.cad_value > Decimal("0"),
    )


def _classify_disposal(tx: Transaction) -> ClassifiedEvent:
    """Build a sell ClassifiedEvent for disposal transactions.

    Emits a warning for unmatched withdrawals so the user knows to tap
    the receiving wallet if this is actually a transfer.
    """
    if tx.tx_type == TransactionType.WITHDRAWAL:
        logger.warning(
            "transfer_match: unmatched withdrawal of %s %s on %s — treating as sell. "
            "If this is a wallet-to-wallet transfer, tap the receiving wallet.",
            tx.amount,
            tx.asset,
            tx.timestamp.date(),
        )
    return ClassifiedEvent(
        id=0,
        vtx_id=tx.id,
        timestamp=tx.timestamp,
        event_type="sell",
        asset=tx.asset,
        amount=tx.amount,
        cad_proceeds=tx.cad_value,
        cad_cost=None,
        cad_fee=tx.fee_cad if tx.fee_cad else None,
        txid=tx.txid,
        source=tx.source,
        is_taxable=tx.cad_value > Decimal("0"),
    )


def _classify_income(tx: Transaction) -> tuple[ClassifiedEvent, IncomeEvent]:
    """Build a (ClassifiedEvent, IncomeEvent) pair for income transactions."""
    income_type = _infer_income_type(tx)
    evt = ClassifiedEvent(
        id=0,
        vtx_id=tx.id,
        timestamp=tx.timestamp,
        event_type="income",
        asset=tx.asset,
        amount=tx.amount,
        cad_proceeds=None,
        cad_cost=tx.cad_value,  # FMV at receipt establishes ACB
        cad_fee=None,
        txid=tx.txid,
        source=tx.source,
        is_taxable=True,
    )
    income = IncomeEvent(
        id=0,
        vtx_id=tx.id,
        timestamp=tx.timestamp,
        asset=tx.asset,
        units=tx.amount,
        income_type=income_type,
        fmv_cad=tx.cad_value,
        source=tx.source,
    )
    return evt, income


def _infer_income_type(tx: Transaction) -> str:
    """Return the income type string for an income transaction.

    The source field may carry hints (e.g. "staking" in the source name).
    Defaults to "other".
    """
    source_lower = tx.source.lower()
    if "stak" in source_lower:
        return "staking"
    if "airdrop" in source_lower:
        return "airdrop"
    if "mining" in source_lower or "mine" in source_lower:
        return "mining"
    # Try to infer from notes.
    notes_lower = tx.notes.lower()
    if "stak" in notes_lower:
        return "staking"
    if "airdrop" in notes_lower:
        return "airdrop"
    return "other"
