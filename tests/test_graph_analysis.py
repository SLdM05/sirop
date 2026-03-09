"""Tests for the graph analysis orchestrator (find_graph_matches).

Uses mocked fetch functions — no live Mempool API, no sirop DB.
Transaction objects are built with obviously fake data.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

from sirop.models.enums import TransactionType
from sirop.models.transaction import Transaction
from sirop.node.models import OnChainTx, TxOutspend
from sirop.transfer_match.graph_analysis import find_graph_matches

_BASE_TS = datetime(2025, 4, 1, 12, 0, 0, tzinfo=UTC)

MEMPOOL_URL = "https://mempool.space/api"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _withdrawal(
    db_id: int,
    txid: str | None,
    amount: str = "0.10100000",
) -> Transaction:
    return Transaction(
        id=db_id,
        source="shakepay",
        timestamp=_BASE_TS,
        tx_type=TransactionType.WITHDRAWAL,
        asset="BTC",
        amount=Decimal(amount),
        cad_value=Decimal("5000"),
        fee_cad=Decimal("0"),
        fee_crypto=Decimal("0"),
        txid=txid,
        is_transfer=False,
        counterpart_id=None,
        notes="",
        wallet_id=1,
    )


def _deposit(
    db_id: int,
    txid: str | None,
    amount: str = "0.10000000",
) -> Transaction:
    return Transaction(
        id=db_id,
        source="sparrow",
        timestamp=_BASE_TS,
        tx_type=TransactionType.DEPOSIT,
        asset="BTC",
        amount=Decimal(amount),
        cad_value=Decimal("4950"),
        fee_cad=Decimal("0"),
        fee_crypto=Decimal("0"),
        txid=txid,
        is_transfer=False,
        counterpart_id=None,
        notes="",
        wallet_id=2,
    )


def _on_chain_tx(txid: str, vin_txids: list[str]) -> OnChainTx:
    return OnChainTx(
        txid=txid,
        fee_sat=1000,
        confirmed=True,
        block_time=_BASE_TS,
        vin_txids=tuple(vin_txids),
        vout_count=1,
    )


def _outspend(spending_txid: str) -> TxOutspend:
    return TxOutspend(spent=True, txid=spending_txid, vin=0)


def _unspent() -> TxOutspend:
    return TxOutspend(spent=False, txid=None, vin=None)


# ---------------------------------------------------------------------------
# Backward pass tests
# ---------------------------------------------------------------------------


class TestBackwardMatch:
    def test_backward_match_emitted(self):
        """Deposit whose direct input is a known withdrawal txid is matched."""
        withdrawal_txid = "aaa" + "0" * 61
        deposit_txid = "bbb" + "0" * 61

        w = _withdrawal(db_id=10, txid=withdrawal_txid)
        d = _deposit(db_id=20, txid=deposit_txid)

        tx_map = {deposit_txid: _on_chain_tx(deposit_txid, [withdrawal_txid])}

        with (
            patch(
                "sirop.transfer_match.graph_analysis.fetch_tx",
                side_effect=lambda url, txid: tx_map.get(txid),
            ),
            patch(
                "sirop.transfer_match.graph_analysis.fetch_outspends",
                return_value=[],
            ),
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        assert len(matches) == 1
        m = matches[0]
        assert m.withdrawal_db_id == 10  # noqa: PLR2004
        assert m.deposit_db_id == 20  # noqa: PLR2004
        assert m.direction == "backward"
        assert m.hops == 1
        assert m.fee_crypto == Decimal("0.00100000")

    def test_backward_two_hop_match(self):
        """2-hop backward: withdrawal → intermediate → deposit."""
        withdrawal_txid = "aaa" + "0" * 61
        intermediate_txid = "ccc" + "0" * 61
        deposit_txid = "bbb" + "0" * 61

        w = _withdrawal(db_id=10, txid=withdrawal_txid)
        d = _deposit(db_id=20, txid=deposit_txid)

        tx_map = {
            deposit_txid: _on_chain_tx(deposit_txid, [intermediate_txid]),
            intermediate_txid: _on_chain_tx(intermediate_txid, [withdrawal_txid]),
        }

        with (
            patch(
                "sirop.transfer_match.graph_analysis.fetch_tx",
                side_effect=lambda url, txid: tx_map.get(txid),
            ),
            patch(
                "sirop.transfer_match.graph_analysis.fetch_outspends",
                return_value=[],
            ),
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        assert len(matches) == 1
        assert matches[0].hops == 2  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Forward pass tests
# ---------------------------------------------------------------------------


class TestForwardMatch:
    def test_forward_match_emitted(self):
        """Withdrawal whose output is spent by a known deposit txid is matched."""
        withdrawal_txid = "aaa" + "0" * 61
        deposit_txid = "bbb" + "0" * 61

        w = _withdrawal(db_id=10, txid=withdrawal_txid)
        d = _deposit(db_id=20, txid=deposit_txid)

        spend_map = {withdrawal_txid: [_outspend(deposit_txid)]}

        with (
            patch(
                "sirop.transfer_match.graph_analysis.fetch_tx",
                return_value=None,  # backward pass finds nothing
            ),
            patch(
                "sirop.transfer_match.graph_analysis.fetch_outspends",
                side_effect=lambda url, txid: spend_map.get(txid, []),
            ),
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        assert len(matches) == 1
        m = matches[0]
        assert m.withdrawal_db_id == 10  # noqa: PLR2004
        assert m.deposit_db_id == 20  # noqa: PLR2004
        assert m.direction == "forward"
        assert m.hops == 1
        assert m.fee_crypto == Decimal("0.00100000")


# ---------------------------------------------------------------------------
# Edge case tests
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_txid_skipped(self):
        """Transactions without a blockchain txid are not traversed."""
        w = _withdrawal(db_id=10, txid=None)
        d = _deposit(db_id=20, txid=None)

        with (
            patch("sirop.transfer_match.graph_analysis.fetch_tx") as mock_fetch,
            patch("sirop.transfer_match.graph_analysis.fetch_outspends") as mock_outspends,
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        assert matches == []
        mock_fetch.assert_not_called()
        mock_outspends.assert_not_called()

    def test_empty_lists_return_empty(self):
        """Both empty lists — no traversal, empty result."""
        matches = find_graph_matches([], [], MEMPOOL_URL, max_hops=3)
        assert matches == []

    def test_max_hops_zero_skips_traversal(self):
        """max_hops=0 skips all traversal without any API calls."""
        w = _withdrawal(db_id=10, txid="aaa" + "0" * 61)
        d = _deposit(db_id=20, txid="bbb" + "0" * 61)

        with (
            patch("sirop.transfer_match.graph_analysis.fetch_tx") as mock_fetch,
            patch("sirop.transfer_match.graph_analysis.fetch_outspends") as mock_outspends,
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=0)

        assert matches == []
        mock_fetch.assert_not_called()
        mock_outspends.assert_not_called()

    def test_deduplication_backward_takes_priority(self):
        """Same pair found by both backward and forward appears only once."""
        withdrawal_txid = "aaa" + "0" * 61
        deposit_txid = "bbb" + "0" * 61

        w = _withdrawal(db_id=10, txid=withdrawal_txid)
        d = _deposit(db_id=20, txid=deposit_txid)

        # Backward: deposit's input is the withdrawal
        tx_map = {deposit_txid: _on_chain_tx(deposit_txid, [withdrawal_txid])}
        # Forward: withdrawal's output is spent by deposit
        spend_map = {withdrawal_txid: [_outspend(deposit_txid)]}

        with (
            patch(
                "sirop.transfer_match.graph_analysis.fetch_tx",
                side_effect=lambda url, txid: tx_map.get(txid),
            ),
            patch(
                "sirop.transfer_match.graph_analysis.fetch_outspends",
                side_effect=lambda url, txid: spend_map.get(txid, []),
            ),
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        # Backward pass finds it first; forward pass skips because already paired.
        assert len(matches) == 1
        assert matches[0].direction == "backward"

    def test_fee_clamped_to_zero_when_negative(self):
        """Fee is clamped to zero if deposit amount exceeds withdrawal amount."""
        withdrawal_txid = "aaa" + "0" * 61
        deposit_txid = "bbb" + "0" * 61

        # Deposit amount > withdrawal amount (unusual but guard against it)
        w = _withdrawal(db_id=10, txid=withdrawal_txid, amount="0.05000000")
        d = _deposit(db_id=20, txid=deposit_txid, amount="0.06000000")

        tx_map = {deposit_txid: _on_chain_tx(deposit_txid, [withdrawal_txid])}

        with (
            patch(
                "sirop.transfer_match.graph_analysis.fetch_tx",
                side_effect=lambda url, txid: tx_map.get(txid),
            ),
            patch(
                "sirop.transfer_match.graph_analysis.fetch_outspends",
                return_value=[],
            ),
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        assert len(matches) == 1
        assert matches[0].fee_crypto == Decimal("0")

    def test_no_match_found_returns_empty(self):
        """No matching pair in graph — empty result without exceptions."""
        withdrawal_txid = "aaa" + "0" * 61
        deposit_txid = "bbb" + "0" * 61

        w = _withdrawal(db_id=10, txid=withdrawal_txid)
        d = _deposit(db_id=20, txid=deposit_txid)

        with (
            patch(
                "sirop.transfer_match.graph_analysis.fetch_tx",
                return_value=None,
            ),
            patch(
                "sirop.transfer_match.graph_analysis.fetch_outspends",
                return_value=[],
            ),
        ):
            matches = find_graph_matches([w], [d], MEMPOOL_URL, max_hops=3)

        assert matches == []
