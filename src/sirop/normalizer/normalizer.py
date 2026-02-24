"""Normalizer — pipeline stage 2.

Converts ``RawTransaction`` records (as-imported from exchange CSVs) into
``Transaction`` records where all values are expressed in CAD and timestamps
are UTC.

The normalizer is the only module that calls the BoC rate client.  All
subsequent stages work exclusively with CAD values.

Design notes
------------
- For exchanges that already provide CAD values (Shakepay, NDAX trade legs),
  the normalizer accepts those values directly.
- For amounts denominated in USD, the BoC USDCAD daily average rate for the
  transaction date is applied.
- For crypto deposits / withdrawals that carry no fiat value at all (e.g. an
  on-chain transfer with no associated CAD amount from the exchange), the
  normalizer sets ``cad_value = 0``.  These rows become transfer legs in stage 4
  and are excluded from ACB calculations.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.models.enums import TransactionType
from sirop.models.transaction import Transaction
from sirop.utils.boc import get_rate
from sirop.utils.crypto_prices import CryptoPriceError, get_crypto_price_cad
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    import sqlite3

    from sirop.models.raw import RawTransaction

logger = get_logger(__name__)

# Fiat currencies that require BoC conversion (others are assumed to be CAD).
_USD_CODES = frozenset({"USD", "USDT", "USDC"})

# TransactionType values that represent transfers between own wallets.
# These are excluded from the "missing CAD value" warning.
_TRANSFER_TYPES = frozenset(
    {
        TransactionType.DEPOSIT.value,
        TransactionType.WITHDRAWAL.value,
        TransactionType.TRANSFER_IN.value,
        TransactionType.TRANSFER_OUT.value,
        TransactionType.FIAT_DEPOSIT.value,
        TransactionType.FIAT_WITHDRAWAL.value,
    }
)


class NormalizationError(Exception):
    """Raised when a raw transaction cannot be normalized."""


def normalize(
    raw_txs: list[RawTransaction],
    conn: sqlite3.Connection,
) -> list[Transaction]:
    """Normalize *raw_txs* into CAD-denominated ``Transaction`` records.

    Parameters
    ----------
    raw_txs:
        Output of ``repositories.read_raw_transactions()``.
    conn:
        Open connection to the active ``.sirop`` batch (used for BoC rate
        caching via ``utils.boc.get_rate``).

    Returns
    -------
    list[Transaction]
        Normalized records sorted chronologically.  IDs are 0 — the
        repository assigns real IDs after writing.
    """
    result: list[Transaction] = []

    for raw in raw_txs:
        try:
            tx = _normalize_one(raw, conn)
        except NormalizationError as exc:
            logger.warning("normalize: skipping row — %s", exc)
            continue
        result.append(tx)

    result.sort(key=lambda t: t.timestamp)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_one(raw: RawTransaction, conn: sqlite3.Connection) -> Transaction:
    """Normalize a single ``RawTransaction`` into a ``Transaction``."""
    tx_type = _map_tx_type(raw.transaction_type)
    cad_value, cad_rate = _resolve_cad_value(raw, conn, tx_type)
    fee_cad, fee_crypto = _resolve_fees(raw, cad_rate)

    return Transaction(
        id=0,  # assigned by repository after INSERT
        source=raw.source,
        timestamp=raw.timestamp,
        tx_type=tx_type,
        asset=raw.asset,
        amount=raw.amount,
        cad_value=cad_value,
        fee_cad=fee_cad,
        fee_crypto=fee_crypto,
        txid=raw.txid,
        is_transfer=False,  # transfer_match stage sets this
        counterpart_id=None,
        notes="",
        wallet_id=raw.wallet_id,  # propagated from tap time
    )


def _map_tx_type(raw_type: str) -> TransactionType:
    """Map a raw transaction type string to a ``TransactionType`` enum value.

    Falls back to ``OTHER`` for unrecognised strings so that the pipeline
    keeps running.  The transfer_match stage handles the classification.
    """
    try:
        return TransactionType(raw_type)
    except ValueError:
        logger.warning("normalize: unknown transaction type %r → OTHER", raw_type)
        return TransactionType.OTHER


def _resolve_cad_value(
    raw: RawTransaction,
    conn: sqlite3.Connection,
    tx_type: TransactionType,
) -> tuple[Decimal, Decimal]:
    """Return ``(cad_value, cad_rate)`` for *raw*.

    CAD value resolution order:
    1. ``fiat_currency == "CAD"`` → use ``fiat_value`` directly.
    2. ``fiat_currency`` in USD-like codes → apply BoC USDCAD rate.
    3. ``fiat_value is None`` and explicit ``rate`` available → compute from
       ``amount x rate`` (exchange's own rate in CAD).
    4. No CAD value available → use 0 and emit a warning for taxable types.
    """
    fiat_val = raw.fiat_value
    fiat_cur = (raw.fiat_currency or "").upper().strip()

    if fiat_val is not None and fiat_val >= Decimal("0"):
        if fiat_cur in {"CAD", ""}:
            rate = (
                raw.rate
                if raw.rate is not None
                else (fiat_val / raw.amount if raw.amount else Decimal("0"))
            )
            return fiat_val, rate

        if fiat_cur in _USD_CODES:
            tx_date = raw.timestamp.date()
            usd_cad = get_rate(conn, "USDCAD", tx_date)
            cad_val = fiat_val * usd_cad
            cad_rate = usd_cad * (fiat_val / raw.amount) if raw.amount else usd_cad
            return cad_val, cad_rate

        # Unsupported fiat currency — fall through to 0.
        logger.warning(
            "normalize: unsupported fiat currency %r for %s %s on %s — using 0",
            fiat_cur,
            raw.transaction_type,
            raw.asset,
            raw.timestamp.date(),
        )

    # No fiat value — try to use the exchange's own rate (already in CAD).
    if raw.rate is not None and raw.rate > Decimal("0"):
        cad_val = raw.amount * raw.rate
        return cad_val, raw.rate

    # No exchange rate either — look up the crypto price for the transaction date.
    tx_date = raw.timestamp.date()
    try:
        price_cad = get_crypto_price_cad(conn, raw.asset, tx_date)
        cad_val = raw.amount * price_cad
        return cad_val, price_cad
    except CryptoPriceError:
        logger.warning(
            "normalize: crypto price lookup failed for %s on %s — continuing without CAD value",
            raw.asset,
            tx_date,
        )

    # Last resort — use 0 and warn for non-transfer types.
    if tx_type.value not in _TRANSFER_TYPES:
        logger.warning(
            "normalize: no CAD value for %s %s %s on %s — using 0. "
            "ACB may be incorrect for this event.",
            tx_type.value,
            raw.amount,
            raw.asset,
            raw.timestamp.date(),
        )
    return Decimal("0"), Decimal("0")


def _resolve_fees(
    raw: RawTransaction,
    cad_rate: Decimal,
) -> tuple[Decimal, Decimal]:
    """Return ``(fee_cad, fee_crypto)`` for *raw*.

    For an explicit fee:
    - If the fee currency matches the asset (e.g. BTC fee on a BTC withdrawal):
      ``fee_crypto = fee_amount``, ``fee_cad = fee_amount x cad_rate``.
    - If the fee currency is CAD: ``fee_cad = fee_amount``, ``fee_crypto = 0``.
    - Otherwise: treat as crypto fee and apply ``cad_rate``.
    """
    if raw.fee_amount is None or raw.fee_amount == Decimal("0"):
        return Decimal("0"), Decimal("0")

    fee_cur = (raw.fee_currency or "").upper().strip()

    if fee_cur == "CAD":
        return raw.fee_amount, Decimal("0")

    # Crypto fee — convert to CAD using the transaction's own rate.
    fee_crypto = raw.fee_amount
    fee_cad = fee_crypto * cad_rate if cad_rate else Decimal("0")
    return fee_cad, fee_crypto
