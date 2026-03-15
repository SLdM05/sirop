"""Pure BFS traversal of the Bitcoin UTXO graph.

No HTTP, no database, no sirop domain objects.  Both functions accept
dependency-injected fetch callables so they are fully unit-testable
without a live node.

Traversal semantics
-------------------
**Backward traversal** (unmatched deposits → find originating withdrawal):

  Start at a deposit's txid and follow each input's previous-output txid
  (``vin_txids``) up the UTXO ancestry tree.  At each level, check
  whether the parent txid is in ``target_txids`` (the set of known
  withdrawal txids).  If found, return ``(matched_txid, hops)`` where
  ``hops`` is the number of edges traversed (1 = the deposit's direct
  input is the withdrawal, 2 = one intermediate hop, etc.).

**Forward traversal** (unmatched withdrawals → find destination deposit):

  Start at a withdrawal's txid and follow each output's spending
  transaction via ``fetch_outspends``.  At each level, check whether the
  spending txid is in ``target_txids`` (the set of known deposit txids).

Both functions:
- Use BFS (breadth-first) so the shortest path is returned first.
- Maintain a ``visited`` set to handle diamond-shaped UTXO graphs and
  prevent infinite loops.
- Respect ``max_hops``: the search is abandoned after this many layers of
  parent/child transactions are inspected.
- Return ``None`` when no match is found within ``max_hops``.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from sirop.node.models import OnChainTx, TxOutspend


def backward_traverse(
    start_txid: str,
    target_txids: set[str],
    fetch_tx: Callable[[str], OnChainTx | None],
    max_hops: int,
) -> tuple[str, int] | None:
    """BFS backward from *start_txid* through UTXO inputs.

    Parameters
    ----------
    start_txid:
        The txid of the deposit transaction to start from.
    target_txids:
        Set of withdrawal txids to search for in the ancestry tree.
    fetch_tx:
        Callable that returns an ``OnChainTx`` for a given txid, or
        ``None`` if the transaction cannot be fetched.
    max_hops:
        Maximum number of parent layers to explore.  ``max_hops=1``
        checks only the direct inputs of *start_txid*.

    Returns
    -------
    ``(matched_txid, hops)`` on success, ``None`` when no match is found
    within the hop limit.
    """
    if max_hops <= 0:
        return None

    # Queue entries: (txid_to_fetch_parents_of, depth_already_at_this_node)
    # depth=0 means we are at the start node; we fetch its parents at depth=1.
    queue: deque[tuple[str, int]] = deque([(start_txid, 0)])
    visited: set[str] = {start_txid}

    while queue:
        current_txid, depth = queue.popleft()

        if depth >= max_hops:
            continue

        tx = fetch_tx(current_txid)
        if tx is None:
            continue

        for parent_txid in tx.vin_txids:
            if parent_txid in visited:
                continue
            visited.add(parent_txid)

            hop_distance = depth + 1
            if parent_txid in target_txids:
                return parent_txid, hop_distance

            if hop_distance < max_hops:
                queue.append((parent_txid, hop_distance))

    return None


def backward_traverse_all(
    start_txid: str,
    target_txids: set[str],
    fetch_tx: Callable[[str], OnChainTx | None],
    max_hops: int,
) -> list[tuple[str, int]]:
    """BFS backward from *start_txid*, returning ALL matching ancestor txids.

    Identical to :func:`backward_traverse` except it does not stop at the
    first match — it collects every target txid reachable within *max_hops*
    and returns them all.  Used for consolidation transactions where a single
    deposit is funded by multiple withdrawals.

    Parameters
    ----------
    start_txid:
        The txid of the deposit transaction to start from.
    target_txids:
        Set of withdrawal txids to search for in the ancestry tree.
    fetch_tx:
        Callable that returns an ``OnChainTx`` for a given txid, or
        ``None`` if the transaction cannot be fetched.
    max_hops:
        Maximum number of parent layers to explore.

    Returns
    -------
    List of ``(matched_txid, hops)`` tuples, one per reachable target.
    Empty list when no match is found within the hop limit.
    """
    if max_hops <= 0:
        return []

    results: list[tuple[str, int]] = []
    queue: deque[tuple[str, int]] = deque([(start_txid, 0)])
    visited: set[str] = {start_txid}

    while queue:
        current_txid, depth = queue.popleft()

        if depth >= max_hops:
            continue

        tx = fetch_tx(current_txid)
        if tx is None:
            continue

        for parent_txid in tx.vin_txids:
            if parent_txid in visited:
                continue
            visited.add(parent_txid)

            hop_distance = depth + 1
            if parent_txid in target_txids:
                results.append((parent_txid, hop_distance))
                # Do not enqueue matched ancestors — we don't need to traverse
                # deeper from a confirmed match.
            elif hop_distance < max_hops:
                queue.append((parent_txid, hop_distance))

    return results


def forward_traverse(
    start_txid: str,
    target_txids: set[str],
    fetch_outspends: Callable[[str], list[TxOutspend]],
    max_hops: int,
) -> tuple[str, int] | None:
    """BFS forward from *start_txid* through output spending transactions.

    Parameters
    ----------
    start_txid:
        The txid of the withdrawal transaction to start from.
    target_txids:
        Set of deposit txids to search for in the spend-chain.
    fetch_outspends:
        Callable that returns the list of ``TxOutspend`` for a given
        txid (one entry per output).
    max_hops:
        Maximum number of child layers to explore.

    Returns
    -------
    ``(matched_txid, hops)`` on success, ``None`` when no match is found
    within the hop limit.
    """
    if max_hops <= 0:
        return None

    queue: deque[tuple[str, int]] = deque([(start_txid, 0)])
    visited: set[str] = {start_txid}

    while queue:
        current_txid, depth = queue.popleft()

        if depth >= max_hops:
            continue

        outspends = fetch_outspends(current_txid)

        for outspend in outspends:
            if not outspend.spent or outspend.txid is None:
                continue

            child_txid = outspend.txid
            if child_txid in visited:
                continue
            visited.add(child_txid)

            hop_distance = depth + 1
            if child_txid in target_txids:
                return child_txid, hop_distance

            if hop_distance < max_hops:
                queue.append((child_txid, hop_distance))

    return None
