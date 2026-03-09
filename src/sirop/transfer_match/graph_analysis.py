"""BTC on-chain graph traversal for transfer matching.

Orchestrates the pure BFS functions in ``sirop.node.graph`` over sirop
``Transaction`` objects, using the Mempool REST client to fetch on-chain
data on demand.

Public API
----------
``find_graph_matches(unmatched_withdrawals, unmatched_deposits,
                     mempool_url, max_hops)``

    Takes the lists of currently unmatched withdrawal and deposit
    ``Transaction`` objects produced by the transfer matcher after Pass 1,
    and returns a list of ``GraphMatch`` objects describing pairs that
    were discovered via on-chain UTXO graph traversal.

Algorithm
---------
1. Build index maps: ``withdrawal_txid_map`` and ``deposit_txid_map``
   (only transactions that carry a non-null blockchain txid are eligible).

2. **Backward pass** — for each unmatched deposit with a txid, call
   ``backward_traverse`` with the set of known withdrawal txids as
   targets.  If a match is found, record a ``GraphMatch(direction="backward")``.

3. **Forward pass** — for each unmatched withdrawal with a txid that was
   *not* already paired by the backward pass, call ``forward_traverse``
   with the set of remaining unmatched deposit txids as targets.

4. Deduplicate: the backward and forward passes may independently find
   the same pair (one searching from each end).  The first discovery
   (backward pass results take priority) is kept.

The fee is computed as ``withdrawal.amount - deposit.amount``.  If the
result is negative (can happen with rounding on amount+time-matched
sources), the fee is clamped to zero.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.node.graph import backward_traverse, forward_traverse
from sirop.node.mempool_client import fetch_outspends, fetch_tx
from sirop.node.models import GraphMatch, OnChainTx, TxOutspend
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    from sirop.models.transaction import Transaction

logger = get_logger(__name__)


def find_graph_matches(  # noqa: PLR0912 PLR0915
    unmatched_withdrawals: list[Transaction],
    unmatched_deposits: list[Transaction],
    mempool_url: str,
    max_hops: int,
) -> list[GraphMatch]:
    """Discover transfer pairs via on-chain UTXO graph traversal.

    Parameters
    ----------
    unmatched_withdrawals:
        Withdrawal ``Transaction`` objects that were not paired by Pass 1.
        Only those with a non-null ``txid`` are traversed.
    unmatched_deposits:
        Deposit ``Transaction`` objects that were not paired by Pass 1.
        Only those with a non-null ``txid`` are traversed.
    mempool_url:
        Base URL for the Mempool REST API (e.g. ``"https://mempool.space/api"``).
    max_hops:
        Maximum number of on-chain hops to search.  Must be ≥ 1.

    Returns
    -------
    List of ``GraphMatch`` objects, one per discovered transfer pair.
    Each pair appears exactly once regardless of which direction found it.
    """
    if max_hops <= 0:
        return []

    # Build txid-indexed maps (skip transactions without a blockchain txid).
    withdrawal_txid_map: dict[str, Transaction] = {
        t.txid: t for t in unmatched_withdrawals if t.txid
    }
    deposit_txid_map: dict[str, Transaction] = {t.txid: t for t in unmatched_deposits if t.txid}

    if not withdrawal_txid_map and not deposit_txid_map:
        return []

    logger.debug(
        "graph_analysis: starting traversal — %d withdrawals, %d deposits, max_hops=%d",
        len(withdrawal_txid_map),
        len(deposit_txid_map),
        max_hops,
    )

    # Bind the mempool URL into the fetch callables (partial application).
    def _fetch_tx(txid: str) -> OnChainTx | None:
        return fetch_tx(mempool_url, txid)

    def _fetch_outspends(txid: str) -> list[TxOutspend]:
        return fetch_outspends(mempool_url, txid)

    matches: list[GraphMatch] = []

    # Track which DB ids have already been paired so we don't emit duplicates.
    paired_deposit_ids: set[int] = set()
    paired_withdrawal_ids: set[int] = set()

    # ── Backward pass: unmatched deposits → search ancestors for withdrawals ──

    withdrawal_target_txids: set[str] = set(withdrawal_txid_map)

    for deposit_tx in unmatched_deposits:
        if not deposit_tx.txid:
            continue
        if deposit_tx.id in paired_deposit_ids:
            continue

        logger.debug(
            "graph_analysis: backward traverse from deposit %d (txid=[txid redacted])",
            deposit_tx.id,
        )

        result = backward_traverse(
            start_txid=deposit_tx.txid,
            target_txids=withdrawal_target_txids,
            fetch_tx=_fetch_tx,
            max_hops=max_hops,
        )
        if result is None:
            continue

        matched_withdrawal_txid, hops = result
        withdrawal_tx = withdrawal_txid_map[matched_withdrawal_txid]

        if withdrawal_tx.id in paired_withdrawal_ids:
            logger.debug(
                "graph_analysis: backward match found but withdrawal %d already paired — skipping",
                withdrawal_tx.id,
            )
            continue

        fee_crypto = _compute_fee(withdrawal_tx.amount, deposit_tx.amount)
        matches.append(
            GraphMatch(
                deposit_db_id=deposit_tx.id,
                withdrawal_db_id=withdrawal_tx.id,
                direction="backward",
                hops=hops,
                fee_crypto=fee_crypto,
            )
        )
        paired_deposit_ids.add(deposit_tx.id)
        paired_withdrawal_ids.add(withdrawal_tx.id)
        logger.info(
            "graph_analysis: backward match — withdrawal %d → deposit %d via %d hop(s)",
            withdrawal_tx.id,
            deposit_tx.id,
            hops,
        )

    # ── Forward pass: remaining unmatched withdrawals → search spend-chain ───

    # Re-build the deposit target set excluding deposits already paired.
    remaining_deposit_txid_map = {
        txid: tx for txid, tx in deposit_txid_map.items() if tx.id not in paired_deposit_ids
    }
    deposit_target_txids: set[str] = set(remaining_deposit_txid_map)

    for withdrawal_tx in unmatched_withdrawals:
        if not withdrawal_tx.txid:
            continue
        if withdrawal_tx.id in paired_withdrawal_ids:
            continue
        if not deposit_target_txids:
            break  # no more deposit targets to find

        logger.debug(
            "graph_analysis: forward traverse from withdrawal %d (txid=[txid redacted])",
            withdrawal_tx.id,
        )

        result = forward_traverse(
            start_txid=withdrawal_tx.txid,
            target_txids=deposit_target_txids,
            fetch_outspends=_fetch_outspends,
            max_hops=max_hops,
        )
        if result is None:
            continue

        matched_deposit_txid, hops = result
        deposit_tx = remaining_deposit_txid_map[matched_deposit_txid]

        if deposit_tx.id in paired_deposit_ids:
            continue

        fee_crypto = _compute_fee(withdrawal_tx.amount, deposit_tx.amount)
        matches.append(
            GraphMatch(
                deposit_db_id=deposit_tx.id,
                withdrawal_db_id=withdrawal_tx.id,
                direction="forward",
                hops=hops,
                fee_crypto=fee_crypto,
            )
        )
        paired_deposit_ids.add(deposit_tx.id)
        paired_withdrawal_ids.add(withdrawal_tx.id)
        # Remove from target set so subsequent iterations don't try to match it again.
        deposit_target_txids.discard(matched_deposit_txid)
        logger.info(
            "graph_analysis: forward match — withdrawal %d → deposit %d via %d hop(s)",
            withdrawal_tx.id,
            deposit_tx.id,
            hops,
        )

    logger.debug(
        "graph_analysis: traversal complete — %d new pair(s) found",
        len(matches),
    )
    return matches


def _compute_fee(withdrawal_amount: Decimal, deposit_amount: Decimal) -> Decimal:
    """Return the non-negative fee implied by the amount difference."""
    diff = withdrawal_amount - deposit_amount
    return diff if diff > Decimal("0") else Decimal("0")
