"""Unit tests for the pure BFS graph traversal functions.

All tests use dict-based stubs — no HTTP, no network, no sirop DB.
"""

from __future__ import annotations

from sirop.node.graph import backward_traverse, forward_traverse
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


def _make_fetch_tx(tx_map: dict[str, OnChainTx]):
    """Return a fetch_tx callable backed by a static dict."""

    def _fetch(txid: str) -> OnChainTx | None:
        return tx_map.get(txid)

    return _fetch


def _make_fetch_outspends(spend_map: dict[str, list[TxOutspend]]):
    """Return a fetch_outspends callable backed by a static dict."""

    def _fetch(txid: str) -> list[TxOutspend]:
        return spend_map.get(txid, [])

    return _fetch


# ---------------------------------------------------------------------------
# backward_traverse
# ---------------------------------------------------------------------------


class TestBackwardTraverse:
    def test_finds_direct_parent(self):
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

    def test_finds_grandparent(self):
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

    def test_max_hops_respected(self):
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

    def test_max_hops_exactly_reached(self):
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

    def test_no_match_returns_none(self):
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

    def test_cycle_safe(self):
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

    def test_zero_max_hops_disabled(self):
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

    def test_fetch_returns_none_gracefully(self):
        """A 404 (fetch returning None) does not crash the BFS."""
        tx_map: dict[str, OnChainTx] = {}  # all lookups return None
        result = backward_traverse(
            start_txid="deposit_tx",
            target_txids={"withdrawal_tx"},
            fetch_tx=_make_fetch_tx(tx_map),
            max_hops=3,
        )
        assert result is None

    def test_multiple_inputs_finds_correct_one(self):
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
# forward_traverse
# ---------------------------------------------------------------------------


class TestForwardTraverse:
    def _outspend(self, spending_txid: str) -> TxOutspend:
        return TxOutspend(spent=True, txid=spending_txid, vin=0)

    def _unspent(self) -> TxOutspend:
        return TxOutspend(spent=False, txid=None, vin=None)

    def test_finds_direct_child(self):
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

    def test_finds_grandchild(self):
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

    def test_max_hops_respected(self):
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

    def test_unspent_output_skipped(self):
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

    def test_no_match_returns_none(self):
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

    def test_zero_max_hops_disabled(self):
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

    def test_cycle_safe(self):
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

    def test_empty_outspends_returns_none(self):
        """Empty outspend list (no outputs) does not crash BFS."""
        spend_map: dict[str, list[TxOutspend]] = {"withdrawal_tx": []}
        result = forward_traverse(
            start_txid="withdrawal_tx",
            target_txids={"deposit_tx"},
            fetch_outspends=_make_fetch_outspends(spend_map),
            max_hops=3,
        )
        assert result is None
