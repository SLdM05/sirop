"""Unit tests for the pure BFS graph traversal functions.

All tests use dict-based stubs — no HTTP, no network, no sirop DB.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sirop.node.graph import backward_traverse, backward_traverse_all, forward_traverse

if TYPE_CHECKING:
    from collections.abc import Callable
from sirop.node.models import OnChainTx, TxOutspend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tx(txid: str, vin_txids: list[str], vout_count: int = 1) -> OnChainTx:
    return OnChainTx(
        txid=txid,
        fee_sat=None,
        confirmed=True,
        block_time=None,
        vin_txids=tuple(vin_txids),
        vout_count=vout_count,
    )


def _make_fetch_tx(tx_map: dict[str, OnChainTx]) -> Callable[[str], OnChainTx | None]:
    """Return a fetch_tx callable backed by a static dict."""

    def _fetch(txid: str) -> OnChainTx | None:
        return tx_map.get(txid)

    return _fetch


def _make_fetch_outspends(
    spend_map: dict[str, list[TxOutspend]],
) -> Callable[[str], list[TxOutspend]]:
    """Return a fetch_outspends callable backed by a static dict."""

    def _fetch(txid: str) -> list[TxOutspend]:
        return spend_map.get(txid, [])

    return _fetch


# ---------------------------------------------------------------------------
# backward_traverse
# ---------------------------------------------------------------------------


class TestBackwardTraverse:
    def test_finds_direct_parent(self) -> None:
        """1-hop backward: deposit's direct input is the withdrawal."""
        # withdrawal_tx ──→ deposit_tx
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["withdrawal_tx"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result == ("withdrawal_tx", 1)

    def test_finds_grandparent(self) -> None:
        """2-hop backward: withdrawal → intermediate → deposit."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["intermediate_tx"]),
            "intermediate_tx": _make_tx("intermediate_tx", vin_txids=["withdrawal_tx"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result == ("withdrawal_tx", 2)

    def test_max_hops_respected(self) -> None:
        """3-hop match blocked when max_hops=2."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["hop1"]),
            "hop1": _make_tx("hop1", vin_txids=["hop2"]),
            "hop2": _make_tx("hop2", vin_txids=["withdrawal_tx"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=2,
        )
        assert result is None

    def test_max_hops_exactly_reached(self) -> None:
        """3-hop match succeeds when max_hops=3."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["hop1"]),
            "hop1": _make_tx("hop1", vin_txids=["hop2"]),
            "hop2": _make_tx("hop2", vin_txids=["withdrawal_tx"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result == ("withdrawal_tx", 3)

    def test_no_match_returns_none(self) -> None:
        """Returns None when target not found within hop limit."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["unrelated_tx"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result is None

    def test_cycle_safe(self) -> None:
        """Graph with a cycle does not loop forever."""
        # cyclic_a ↔ cyclic_b (both point at each other)
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["cyclic_a"]),
            "cyclic_a": _make_tx("cyclic_a", vin_txids=["cyclic_b"]),
            "cyclic_b": _make_tx("cyclic_b", vin_txids=["cyclic_a"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=10,
        )
        assert result is None

    def test_zero_max_hops_disabled(self) -> None:
        """max_hops=0 disables traversal entirely."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["withdrawal_tx"]),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=0,
        )
        assert result is None

    def test_fetch_returns_none_gracefully(self) -> None:
        """A 404 (fetch returning None) does not crash the BFS."""
        tx_map: dict[str, OnChainTx] = {}  # all lookups return None
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result is None

    def test_multiple_inputs_finds_correct_one(self) -> None:
        """BFS finds the target among multiple inputs at the same depth."""
        tx_map = {
            "deposit_tx": _make_tx(
                "deposit_tx",
                vin_txids=["unrelated_a", "withdrawal_tx", "unrelated_b"],
            ),
        }
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result == ("withdrawal_tx", 1)


# ---------------------------------------------------------------------------
# backward_traverse_all
# ---------------------------------------------------------------------------


class TestBackwardTraverseAll:
    def test_returns_empty_when_no_match(self) -> None:
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["unrelated_tx"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result == []

    def test_returns_single_match(self) -> None:
        """Behaves like backward_traverse when there is exactly one ancestor."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["w1"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=1,
        )
        assert result == [("w1", 1)]

    def test_returns_all_matching_direct_inputs(self) -> None:
        """Consolidation: deposit has three withdrawal inputs — all returned."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["w1", "w2", "w3"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1", "w2", "w3"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=1,
        )
        assert set(result) == {("w1", 1), ("w2", 1), ("w3", 1)}

    def test_partial_match_among_inputs(self) -> None:
        """Only inputs that are in target_txids are returned."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["w1", "noise", "w2"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1", "w2"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=1,
        )
        assert set(result) == {("w1", 1), ("w2", 1)}

    def test_matches_at_different_depths(self) -> None:
        """Returns matches at multiple hop depths in one traversal."""
        # w1 is a direct input (hop=1); w2 is one hop further (hop=2).
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["w1", "intermediate"]),
            "intermediate": _make_tx("intermediate", vin_txids=["w2"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1", "w2"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=2,
        )
        assert set(result) == {("w1", 1), ("w2", 2)}

    def test_max_hops_zero_returns_empty(self) -> None:
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["w1"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=0,
        )
        assert result == []

    def test_does_not_traverse_past_matched_ancestor(self) -> None:
        """Matched nodes are not enqueued, so deeper ancestors are not found."""
        # w1's own parent is also a withdrawal — but we should not traverse past w1.
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["w1"]),
            "w1": _make_tx("w1", vin_txids=["deeper_w"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1", "deeper_w"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        # Only w1 (hop=1) should be found; deeper_w is behind w1 and we stop there.
        assert set(result) == {("w1", 1)}

    def test_cycle_safe(self) -> None:
        """Cycles do not cause infinite loops."""
        tx_map = {
            "deposit_tx": _make_tx("deposit_tx", vin_txids=["cycle_a"]),
            "cycle_a": _make_tx("cycle_a", vin_txids=["cycle_b"]),
            "cycle_b": _make_tx("cycle_b", vin_txids=["cycle_a"]),
        }
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=10,
        )
        assert result == []

    def test_fetch_returns_none_gracefully(self) -> None:
        tx_map: dict[str, OnChainTx] = {}
        result = backward_traverse_all(
            start_txid="deposit_tx",
            target_txids={"w1"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result == []


# ---------------------------------------------------------------------------
# forward_traverse
# ---------------------------------------------------------------------------


class TestForwardTraverse:
    def _outspend(self, spending_txid: str) -> TxOutspend:
        return TxOutspend(spent=True, txid=spending_txid, vin=0)

    def _unspent(self) -> TxOutspend:
        return TxOutspend(spent=False, txid=None, vin=None)

    def test_finds_direct_child(self) -> None:
        """1-hop forward: withdrawal's output is directly spent by deposit."""
        spend_map = {
            "withdrawal_tx": [self._outspend("deposit_tx")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=3,
        )
        assert result == ("deposit_tx", 1)

    def test_finds_grandchild(self) -> None:
        """2-hop forward: withdrawal → intermediate → deposit."""
        spend_map = {
            "withdrawal_tx": [self._outspend("intermediate_tx")],
            "intermediate_tx": [self._outspend("deposit_tx")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=3,
        )
        assert result == ("deposit_tx", 2)

    def test_max_hops_respected(self) -> None:
        """3-hop match blocked when max_hops=2."""
        spend_map = {
            "withdrawal_tx": [self._outspend("hop1")],
            "hop1": [self._outspend("hop2")],
            "hop2": [self._outspend("deposit_tx")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=2,
        )
        assert result is None

    def test_unspent_output_skipped(self) -> None:
        """Unspent outputs (TxOutspend.spent=False) are skipped gracefully."""
        spend_map = {
            "withdrawal_tx": [self._unspent(), self._outspend("deposit_tx")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=3,
        )
        assert result == ("deposit_tx", 1)

    def test_no_match_returns_none(self) -> None:
        """Returns None when deposit not reachable within hop limit."""
        spend_map = {
            "withdrawal_tx": [self._outspend("unrelated_tx")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=3,
        )
        assert result is None

    def test_zero_max_hops_disabled(self) -> None:
        """max_hops=0 disables traversal."""
        spend_map = {
            "withdrawal_tx": [self._outspend("deposit_tx")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=0,
        )
        assert result is None

    def test_cycle_safe(self) -> None:
        """Cycle in spend-chain does not loop forever."""
        spend_map = {
            "withdrawal_tx": [self._outspend("cycle_a")],
            "cycle_a": [self._outspend("cycle_b")],
            "cycle_b": [self._outspend("cycle_a")],
        }
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=10,
        )
        assert result is None

    def test_empty_outspends_returns_none(self) -> None:
        """Empty outspend list (no outputs) does not crash BFS."""
        spend_map: dict[str, list[TxOutspend]] = {"withdrawal_tx": []}
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=3,
        )
        assert result is None
