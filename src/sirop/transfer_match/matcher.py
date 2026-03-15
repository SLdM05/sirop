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
  Note: Shakepay's 2025 export format removed the "Blockchain Transaction ID"
  column, so Shakepay withdrawals no longer carry a real txid.  Their
  Description column contains the recipient Bitcoin address instead, which
  the Shakepay importer stores in ``notes`` (not ``txid``) for use by Pass 1.25.

- **Amount + timestamp proximity** is the secondary (and only available)
  signal for sources that do not export the blockchain txid.  NDAX is the
  primary example: its ``TX_ID`` column contains an internal order identifier
  (a short integer such as ``10008``), NOT an on-chain txid.  The NDAX
  importer therefore always sets ``txid = None``; NDAX withdrawals are matched
  solely by amount proximity within ``MATCH_WINDOW_HOURS``.

- **Amount+timestamp matches are probabilistic**, not cryptographically
  verified.  Two independent transfers of the same amount on the same day
  would be incorrectly merged into a single transfer pair.
  TODO: surface amount+timestamp-matched pairs to the user for confirmation
  before they are treated as definitive self-transfers.  The confirmation flag
  should be stored in the ``classified_events`` table (e.g. a
  ``match_confidence`` column: ``"txid"`` | ``"amount_time"`` | ``"manual"``).
  TODO: the Bitcoin node integration module (see docs/ref/bitcoin-node-
  validation-module.md) will be able to verify self-transfers independently
  by confirming that the deposit address belongs to the user's own xpub — use
  that as the definitive confirmation path for amount+timestamp matches.

- A withdrawal that is not matched becomes a regular sell event (a disposition
  of crypto at the withdrawal CAD value).  This is the conservative choice —
  the user or a future node-verification stage can correct it.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.config.settings import get_settings
from sirop.models.disposition import IncomeEvent
from sirop.models.enums import TransactionType
from sirop.models.event import ClassifiedEvent
from sirop.models.messages import MessageCode
from sirop.transfer_match.graph_analysis import find_graph_matches, resolve_withdrawal_txids
from sirop.utils.logging import get_logger
from sirop.utils.messages import emit

if TYPE_CHECKING:
    from collections.abc import Callable

    from sirop.models.override import TransferOverride
    from sirop.models.transaction import Transaction
    from sirop.node.models import GraphMatch

logger = get_logger(__name__)

# Maximum time difference between a withdrawal and its matching deposit.
MATCH_WINDOW_HOURS: int = 8

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


def match_transfers(  # noqa: PLR0912 PLR0915
    txs: list[Transaction],
    overrides: list[TransferOverride] | None = None,
    tax_year: int | None = None,
    graph_traversal_allowed: bool = True,
    on_graph_progress: Callable[[int, int, int, int], None] | None = None,
) -> tuple[list[ClassifiedEvent], list[IncomeEvent], list[GraphMatch]]:
    """Classify *txs* into taxable events and income sub-records.

    Parameters
    ----------
    txs:
        Output of ``repositories.read_transactions()`` — sorted chronologically.
    overrides:
        Optional user-specified link/unlink decisions from the ``stir`` command.
        Forced links are applied before auto-matching; forced unlinks prevent
        specific pairs from being auto-matched.
    graph_traversal_allowed:
        When ``False``, Pass 1b (BTC UTXO graph traversal) is skipped even if
        ``BTC_TRAVERSAL_MAX_HOPS > 0``.  Set to ``False`` when the configured
        Mempool URL is a public host and the user has not confirmed the privacy
        risk (interactively or via ``BTC_TRAVERSAL_ALLOW_PUBLIC=true``).

    Returns
    -------
    tuple[list[ClassifiedEvent], list[IncomeEvent]]
        - First element: classified events for the ACB engine (is_taxable=True)
          plus non-taxable transfer records (is_taxable=False).
        - Second element: income sub-records for TP-21.4.39-V Part 6.
    """
    sorted_txs = sorted(txs, key=lambda t: t.timestamp)

    # Build override lookup structures.
    # forced_link_pairs: pairs that must always be treated as transfers.
    # forced_unlink_pairs: pairs that must never be auto-matched.
    # external_ids: single transactions marked as going to/from an external wallet.
    forced_link_pairs: set[tuple[int, int]] = set()
    forced_unlink_pairs: set[tuple[int, int]] = set()
    external_ids: set[int] = set()
    if overrides:
        for ov in overrides:
            if ov.action == "link" and ov.tx_id_b is not None:
                pair = (ov.tx_id_a, ov.tx_id_b)
                pair_rev = (ov.tx_id_b, ov.tx_id_a)
                forced_link_pairs.add(pair)
                forced_link_pairs.add(pair_rev)
            elif ov.action == "unlink" and ov.tx_id_b is not None:
                pair = (ov.tx_id_a, ov.tx_id_b)
                pair_rev = (ov.tx_id_b, ov.tx_id_a)
                forced_unlink_pairs.add(pair)
                forced_unlink_pairs.add(pair_rev)
            elif ov.action in ("external-out", "external-in"):
                external_ids.add(ov.tx_id_a)

    # Track which transaction IDs have been paired as transfer legs.
    paired_ids: set[int] = set()

    # Build a map of tx.id → Transaction for fast override lookups.
    tx_by_id: dict[int, Transaction] = {t.id: t for t in sorted_txs}

    # --- Pass 0a: apply external-out / external-in overrides ---
    # These are single-transaction overrides with no counterpart.
    # Mark the tx as a non-taxable transfer out/into the tracked universe.
    fee_disposals: list[ClassifiedEvent] = []

    for ov in overrides or []:
        if ov.action not in ("external-out", "external-in"):
            continue
        tx = tx_by_id.get(ov.tx_id_a)
        if tx is None:
            logger.warning(
                "transfer_match: external override references unknown tx id %d — skipping",
                ov.tx_id_a,
            )
            continue
        if tx.id in paired_ids:
            logger.warning(
                "transfer_match: external override for tx %d skipped — already paired",
                tx.id,
            )
            continue
        paired_ids.add(tx.id)
        wallet_label = ov.external_wallet or "external"
        logger.debug(
            "transfer_match: %s applied for tx %d → wallet '%s'",
            ov.action,
            tx.id,
            wallet_label,
        )

        # Emit a fee micro-disposal.  Prefer the user-recorded implied fee; fall
        # back to the exchange-reported fee_crypto (same pattern as Pass 0b).
        _ext_fee = (
            ov.implied_fee_crypto
            if ov.implied_fee_crypto > Decimal("0")
            else (tx.fee_crypto if tx.fee_crypto and tx.fee_crypto > Decimal("0") else None)
        )
        if _ext_fee and tx.cad_value > Decimal("0"):
            cad_rate = tx.cad_value / tx.amount if tx.amount else Decimal("0")
            fee_proceeds = _ext_fee * cad_rate
            fee_disposals.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=tx.id,
                    timestamp=tx.timestamp,
                    event_type="fee_disposal",
                    asset=tx.asset,
                    amount=_ext_fee,
                    cad_proceeds=fee_proceeds,
                    cad_cost=None,
                    cad_fee=None,
                    txid=tx.txid,
                    source=tx.source,
                    is_taxable=True,
                    wallet_id=tx.wallet_id,
                )
            )

    # --- Pass 0b: apply forced links ---
    for ov in overrides or []:
        if ov.action != "link":
            continue
        if ov.tx_id_b is None:
            continue
        if ov.tx_id_a in paired_ids or ov.tx_id_b in paired_ids:
            logger.warning(
                "transfer_match: forced link (%d ↔ %d) skipped — one leg already paired",
                ov.tx_id_a,
                ov.tx_id_b,
            )
            continue
        tx_a = tx_by_id.get(ov.tx_id_a)
        tx_b = tx_by_id.get(ov.tx_id_b)
        if tx_a is None or tx_b is None:
            logger.warning(
                "transfer_match: forced link references unknown tx id(s) (%d, %d) — skipping",
                ov.tx_id_a,
                ov.tx_id_b,
            )
            continue

        paired_ids.add(ov.tx_id_a)
        paired_ids.add(ov.tx_id_b)
        logger.debug("transfer_match: forced link applied (%d ↔ %d)", ov.tx_id_a, ov.tx_id_b)

        # Determine the outgoing leg.
        outgoing = tx_a if tx_a.tx_type in _OUTGOING else tx_b

        # Prefer the user-recorded implied fee over the exchange-reported fee.
        # implied_fee_crypto is the exact difference between sent and received.
        if ov.implied_fee_crypto > Decimal("0"):
            implied_fee = ov.implied_fee_crypto
            cad_rate = outgoing.cad_value / outgoing.amount if outgoing.amount else Decimal("0")
            fee_proceeds = implied_fee * cad_rate
            fee_disposals.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=outgoing.id,
                    timestamp=outgoing.timestamp,
                    event_type="fee_disposal",
                    asset=outgoing.asset,
                    amount=implied_fee,
                    cad_proceeds=fee_proceeds,
                    cad_cost=None,
                    cad_fee=None,
                    txid=outgoing.txid,
                    source=outgoing.source,
                    is_taxable=True,
                    wallet_id=outgoing.wallet_id,
                )
            )
        else:
            # Fall back to the exchange-reported fee on the outgoing leg.
            has_fee = outgoing.fee_crypto and outgoing.fee_crypto > Decimal("0")
            if has_fee and outgoing.cad_value > Decimal("0"):
                cad_rate = outgoing.cad_value / outgoing.amount if outgoing.amount else Decimal("0")
                fee_proceeds = outgoing.fee_crypto * cad_rate
                fee_disposals.append(
                    ClassifiedEvent(
                        id=0,
                        vtx_id=outgoing.id,
                        timestamp=outgoing.timestamp,
                        event_type="fee_disposal",
                        asset=outgoing.asset,
                        amount=outgoing.fee_crypto,
                        cad_proceeds=fee_proceeds,
                        cad_cost=None,
                        cad_fee=None,
                        txid=outgoing.txid,
                        source=outgoing.source,
                        is_taxable=True,
                        wallet_id=outgoing.wallet_id,
                    )
                )

    # --- Pass 1: match outgoing ↔ incoming transfers (auto) ---
    for tx in sorted_txs:
        if tx.tx_type not in _OUTGOING:
            continue
        if tx.id in paired_ids:
            continue

        match = _find_match(tx, sorted_txs, paired_ids, forced_unlink_pairs)
        if match is None:
            continue

        # Warn if both legs share the same wallet — likely coin consolidation,
        # not a true inter-wallet transfer. Still treated as a transfer.
        if (
            tx.wallet_id is not None
            and match.wallet_id is not None
            and tx.wallet_id == match.wallet_id
        ):
            logger.warning(
                "transfer_match: same-wallet transfer in '%s' (wallet_id=%d) —"
                " possible coin consolidation; treating as transfer",
                tx.source,
                tx.wallet_id,
            )

        # Mark both legs as paired.
        paired_ids.add(tx.id)
        paired_ids.add(match.id)

        # Emit a fee micro-disposition for the amount that left the outgoing wallet
        # but did not arrive at the incoming wallet (network fee, miner fee, or
        # multi-payee output where only one leg is visible).
        # Prefer the observed on-chain difference; fall back to exchange-reported fee.
        _observed_diff = tx.amount - match.amount
        _fee_amount = (
            _observed_diff if _observed_diff > Decimal("0") else (tx.fee_crypto or Decimal("0"))
        )
        if _fee_amount > Decimal("0") and tx.cad_value > Decimal("0"):
            cad_rate = tx.cad_value / tx.amount if tx.amount else Decimal("0")
            fee_proceeds = _fee_amount * cad_rate
            fee_disposals.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=tx.id,
                    timestamp=tx.timestamp,
                    event_type="fee_disposal",
                    asset=tx.asset,
                    amount=_fee_amount,
                    cad_proceeds=fee_proceeds,
                    cad_cost=None,
                    cad_fee=None,
                    txid=tx.txid,
                    source=tx.source,
                    is_taxable=True,
                    wallet_id=tx.wallet_id,
                )
            )

    # --- Passes 1.25 and 1b: mempool-based transfer resolution ---
    # Both contact the Mempool REST API and share the same privacy gate
    # (BTC_TRAVERSAL_MAX_HOPS > 0 and caller-confirmed graph_traversal_allowed).
    # Errors are caught so the pipeline degrades gracefully when the node is
    # unreachable or BTC_TRAVERSAL_MAX_HOPS is not configured.
    graph_matches: list[GraphMatch] = []
    _settings = get_settings()
    if _settings.btc_traversal_max_hops > 0 and graph_traversal_allowed:
        # Pass 1.25 — address-based txid resolution.
        # For Shakepay withdrawals where the Description held a Bitcoin address
        # (e.g. "Bitcoin address bc1q…"), the importer stores that address in
        # notes as "Sent to: bc1q…" and sets txid=None.  Here we query mempool
        # for that address's transactions to discover the real on-chain txid,
        # then match it against unmatched deposits by txid.
        _addr_withdrawals = [
            t
            for t in sorted_txs
            if t.tx_type in _OUTGOING and t.id not in paired_ids and "Sent to: " in (t.notes or "")
        ]
        if _addr_withdrawals:
            try:
                _resolved = resolve_withdrawal_txids(
                    _addr_withdrawals,
                    _settings.btc_mempool_url,
                    _settings.btc_traversal_request_delay,
                )
            except Exception:
                logger.warning("transfer_match: address-based txid resolution failed — skipping")
                _resolved = {}
            for _w in _addr_withdrawals:
                _rtxid = _resolved.get(_w.id)
                if not _rtxid or _w.id in paired_ids:
                    continue
                for _dep in sorted_txs:
                    if _dep.id in paired_ids:
                        continue
                    if _dep.tx_type not in _INCOMING:
                        continue
                    if _dep.asset != _w.asset:
                        continue
                    if _dep.txid == _rtxid:
                        paired_ids.add(_w.id)
                        paired_ids.add(_dep.id)
                        logger.info(
                            "transfer_match: Pass 1.25 address-lookup match"
                            " — withdrawal %d → deposit %d",
                            _w.id,
                            _dep.id,
                        )
                        _obs_diff = _w.amount - _dep.amount
                        _fee_amt = (
                            _obs_diff
                            if _obs_diff > Decimal("0")
                            else (_w.fee_crypto or Decimal("0"))
                        )
                        if _fee_amt > Decimal("0") and _w.cad_value > Decimal("0"):
                            _cad_rate = _w.cad_value / _w.amount if _w.amount else Decimal("0")
                            fee_disposals.append(
                                ClassifiedEvent(
                                    id=0,
                                    vtx_id=_w.id,
                                    timestamp=_w.timestamp,
                                    event_type="fee_disposal",
                                    asset=_w.asset,
                                    amount=_fee_amt,
                                    cad_proceeds=_fee_amt * _cad_rate,
                                    cad_cost=None,
                                    cad_fee=None,
                                    txid=_w.txid,
                                    source=_w.source,
                                    is_taxable=True,
                                    wallet_id=_w.wallet_id,
                                )
                            )
                        break

        # Pass 1b — BTC graph traversal for remaining unmatched txid transactions.
        _unmatched_withdrawals = [
            t for t in sorted_txs if t.tx_type in _OUTGOING and t.id not in paired_ids and t.txid
        ]
        _unmatched_deposits = [
            t for t in sorted_txs if t.tx_type in _INCOMING and t.id not in paired_ids and t.txid
        ]
        if _unmatched_withdrawals or _unmatched_deposits:
            try:
                graph_matches = find_graph_matches(
                    unmatched_withdrawals=_unmatched_withdrawals,
                    unmatched_deposits=_unmatched_deposits,
                    mempool_url=_settings.btc_mempool_url,
                    max_hops=_settings.btc_traversal_max_hops,
                    request_delay=_settings.btc_traversal_request_delay,
                    on_progress=on_graph_progress,
                )
            except Exception:
                logger.warning(
                    "transfer_match: graph traversal raised an unexpected error — skipping"
                )
                emit(MessageCode.BOIL_GRAPH_TRAVERSAL_UNAVAILABLE)
                graph_matches = []

            for gm in graph_matches:
                paired_ids.add(gm.deposit_db_id)
                paired_ids.add(gm.withdrawal_db_id)
                emit(
                    MessageCode.BOIL_GRAPH_MATCH_FOUND,
                    hops=gm.hops,
                    direction=gm.direction,
                    fee=format(gm.fee_crypto, "f"),
                )
                # Emit fee micro-disposal — same pattern as Pass 1.
                withdrawal_tx = tx_by_id[gm.withdrawal_db_id]
                if gm.fee_crypto > Decimal("0") and withdrawal_tx.cad_value > Decimal("0"):
                    cad_rate = (
                        withdrawal_tx.cad_value / withdrawal_tx.amount
                        if withdrawal_tx.amount
                        else Decimal("0")
                    )
                    fee_disposals.append(
                        ClassifiedEvent(
                            id=0,
                            vtx_id=withdrawal_tx.id,
                            timestamp=withdrawal_tx.timestamp,
                            event_type="fee_disposal",
                            asset=withdrawal_tx.asset,
                            amount=gm.fee_crypto,
                            cad_proceeds=gm.fee_crypto * cad_rate,
                            cad_cost=None,
                            cad_fee=None,
                            txid=withdrawal_tx.txid,
                            source=withdrawal_tx.source,
                            is_taxable=True,
                            wallet_id=withdrawal_tx.wallet_id,
                        )
                    )

    # --- Pass 2: convert remaining transactions to ClassifiedEvents ---
    events: list[ClassifiedEvent] = []
    income_events: list[IncomeEvent] = []

    for tx in sorted_txs:
        if tx.id in paired_ids:
            # Confirmed transfer leg (matched pair or external override) — emit a
            # non-taxable marker so the DB has a record, but it won't reach the ACB
            # engine.
            event_type = "external" if tx.id in external_ids else "transfer"
            events.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=tx.id,
                    timestamp=tx.timestamp,
                    event_type=event_type,
                    asset=tx.asset,
                    amount=tx.amount,
                    cad_proceeds=None,
                    cad_cost=None,
                    cad_fee=tx.fee_cad if tx.fee_cad else None,
                    txid=tx.txid,
                    source=tx.source,
                    is_taxable=False,
                    wallet_id=tx.wallet_id,
                )
            )
            continue

        evt, income = _classify(tx, tax_year)
        if evt is not None:
            events.append(evt)
        if income is not None:
            income_events.append(income)

        # Emit a fee micro-disposal for any crypto fee paid on this transaction.
        # Matched transfer legs already get their fee_disposal in Pass 1.
        # For buys, sells, and unmatched withdrawals/deposits the fee_crypto
        # represents units of the asset that were spent (e.g. SOL paid as an
        # NDAX TRADE/FEE or WITHDRAW/FEE row) and must be removed from the pool.
        if tx.fee_crypto and tx.fee_crypto > Decimal("0") and tx.cad_value > Decimal("0"):
            _cad_rate = tx.cad_value / tx.amount if tx.amount else Decimal("0")
            fee_disposals.append(
                ClassifiedEvent(
                    id=0,
                    vtx_id=tx.id,
                    timestamp=tx.timestamp,
                    event_type="fee_disposal",
                    asset=tx.asset,
                    amount=tx.fee_crypto,
                    cad_proceeds=tx.fee_crypto * _cad_rate,
                    cad_cost=None,
                    cad_fee=None,
                    txid=tx.txid,
                    source=tx.source,
                    is_taxable=True,
                    wallet_id=tx.wallet_id,
                )
            )

    # Insert fee micro-disposals in chronological order.
    events.extend(fee_disposals)
    events.sort(key=lambda e: e.timestamp)

    # Log summary.
    taxable = sum(1 for e in events if e.is_taxable)
    transfers = sum(1 for e in events if not e.is_taxable)
    logger.info(
        "%d events (%d taxable, %d transfers), %d income events",
        len(events),
        taxable,
        transfers,
        len(income_events),
    )
    if paired_ids:
        logger.info("%d transfer legs paired", len(paired_ids))

    return events, income_events, graph_matches


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_match(
    outgoing: Transaction,
    all_txs: list[Transaction],
    already_paired: set[int],
    forced_unlink_pairs: set[tuple[int, int]] | None = None,
) -> Transaction | None:
    """Search *all_txs* for a deposit that matches *outgoing*.

    Match criteria (in priority order):
    1. Same non-null txid.
    2. Same asset, amount within tolerance, timestamp within window.

    Pairs in *forced_unlink_pairs* are never returned regardless of match signals.
    """
    window = timedelta(hours=MATCH_WINDOW_HOURS)
    window_start = outgoing.timestamp - window
    window_end = outgoing.timestamp + window
    _unlinks = forced_unlink_pairs or set()

    for candidate in all_txs:
        if candidate.id == outgoing.id:
            continue
        if candidate.id in already_paired:
            continue
        if candidate.tx_type not in _INCOMING:
            continue
        if candidate.asset != outgoing.asset:
            continue

        # Respect forced unlinks.
        if (outgoing.id, candidate.id) in _unlinks:
            logger.debug(
                "transfer_match: skipping forced-unlinked pair (%d ↔ %d)",
                outgoing.id,
                candidate.id,
            )
            continue

        # Txid match — definitive.
        if outgoing.txid and candidate.txid and outgoing.txid == candidate.txid:
            logger.debug(
                "transfer_match: txid match for %s %s",
                outgoing.asset,
                outgoing.txid,
            )
            return candidate

        # Amount + timestamp proximity match — probabilistic.
        # Used for sources with no blockchain txid (e.g. NDAX withdrawals).
        # TODO: tag these matches with match_confidence="amount_time" and
        # surface them to the user for confirmation (see module docstring).
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
                "transfer_match: amount+time match for %s on %s from %s"
                " (probabilistic — no txid available from %s)",
                outgoing.asset,
                outgoing.timestamp.date(),
                outgoing.source,
                outgoing.source,
            )
            return candidate

    return None


def _classify(
    tx: Transaction, tax_year: int | None = None
) -> tuple[ClassifiedEvent | None, IncomeEvent | None]:
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
        return _classify_acquisition(tx, tax_year), None

    # Disposal types: sell, spend, and unmatched withdrawal.
    if tx_type in {TransactionType.SELL, TransactionType.SPEND, TransactionType.WITHDRAWAL}:
        return _classify_disposal(tx, tax_year), None

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
            wallet_id=tx.wallet_id,
        ),
        None,
    )


def _classify_acquisition(tx: Transaction, tax_year: int | None = None) -> ClassifiedEvent:
    """Build a buy ClassifiedEvent for acquisition transactions.

    Emits a warning for unmatched deposits so the user knows to check
    whether the deposit is actually a wallet-to-wallet transfer.
    Future-year deposits are silently classified — they won't generate a
    disposition but may be needed for superficial-loss detection.
    """
    if tx.tx_type == TransactionType.DEPOSIT and (
        tax_year is None or tx.timestamp.year <= tax_year
    ):
        emit(
            MessageCode.BOIL_TRANSFER_MATCH_UNMATCHED_DEPOSIT,
            amount=tx.amount,
            asset=tx.asset,
            date=tx.timestamp.date(),
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
        wallet_id=tx.wallet_id,
    )


def _classify_disposal(tx: Transaction, tax_year: int | None = None) -> ClassifiedEvent:
    """Build a sell ClassifiedEvent for disposal transactions.

    Emits a warning for unmatched withdrawals so the user knows to tap
    the receiving wallet if this is actually a transfer.
    Future-year withdrawals are silently classified — a consolidated W004
    is emitted by the caller instead.
    """
    if tx.tx_type == TransactionType.WITHDRAWAL and (
        tax_year is None or tx.timestamp.year <= tax_year
    ):
        emit(
            MessageCode.BOIL_TRANSFER_MATCH_UNMATCHED_WITHDRAWAL,
            amount=tx.amount,
            asset=tx.asset,
            date=tx.timestamp.date(),
        )
    # Use only the exchange-reported CAD fee.  Crypto fees (fee_crypto > 0) are
    # emitted as separate fee_disposal events in the Pass 2 loop so that the ACB
    # pool is correctly reduced by the fee units rather than just deriving a CAD
    # deduction that leaves those units phantom in the pool.
    fee_cad = tx.fee_cad or None

    return ClassifiedEvent(
        id=0,
        vtx_id=tx.id,
        timestamp=tx.timestamp,
        event_type="sell",
        asset=tx.asset,
        amount=tx.amount,
        cad_proceeds=tx.cad_value,
        cad_cost=None,
        cad_fee=fee_cad,
        txid=tx.txid,
        source=tx.source,
        is_taxable=tx.cad_value > Decimal("0"),
        wallet_id=tx.wallet_id,
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
        wallet_id=tx.wallet_id,
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
