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
    def test_backward_match_emitted(self) -> None:
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

    def test_backward_two_hop_match(self) -> None:
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
    def test_forward_match_emitted(self) -> None:
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
    def test_no_txid_skipped(self) -> None:
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

    def test_empty_lists_return_empty(self) -> None:
        """Both empty lists — no traversal, empty result."""
        matches = find_graph_matches([], [], MEMPOOL_URL, max_hops=3)
        assert matches == []

    def test_max_hops_zero_skips_traversal(self) -> None:
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

    def test_deduplication_backward_takes_priority(self) -> None:
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

    def test_fee_clamped_to_zero_when_negative(self) -> None:
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

    def test_no_match_found_returns_empty(self) -> None:
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


# ---------------------------------------------------------------------------
# Consolidation (many-to-one) tests
# ---------------------------------------------------------------------------


class TestConsolidationMatch:
    """A single Sparrow deposit funded by multiple NDAX withdrawal UTXOs.

    Real-world scenario confirmed in data/test2025.sirop: three NDAX
    withdrawals (amounts 0.0003992, 0.0002994, 0.0003992 BTC) were all
    inputs to one Sparrow consolidation transaction (0.00109513 BTC received).
    The previous 1-to-1 backward pass only matched one withdrawal; the other
    two fell to Pass 2 and were incorrectly classified as ``sell``.
    """

    def test_three_withdrawals_one_deposit_all_matched(self) -> None:
        """All three withdrawal inputs to a consolidation deposit are matched."""
        w1_txid = "aaa" + "0" * 61
        w2_txid = "bbb" + "0" * 61
        w3_txid = "ccc" + "0" * 61
        deposit_txid = "ddd" + "0" * 61

        w1 = _withdrawal(db_id=1, txid=w1_txid, amount="0.0003992")
        w2 = _withdrawal(db_id=2, txid=w2_txid, amount="0.0002994")
        w3 = _withdrawal(db_id=3, txid=w3_txid, amount="0.0003992")
        # Deposit receives slightly less than sum of inputs (miner fee deducted).
        d = _deposit(db_id=10, txid=deposit_txid, amount="0.00109513")

        tx_map = {
            deposit_txid: _on_chain_tx(deposit_txid, [w1_txid, w2_txid, w3_txid]),
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
            matches = find_graph_matches([w1, w2, w3], [d], MEMPOOL_URL, max_hops=1)

        assert len(matches) == 3  # noqa: PLR2004
        withdrawal_ids = {m.withdrawal_db_id for m in matches}
        assert withdrawal_ids == {1, 2, 3}
        deposit_ids = {m.deposit_db_id for m in matches}
        assert deposit_ids == {10}
        assert all(m.direction == "backward" for m in matches)
        assert all(m.hops == 1 for m in matches)

    def test_consolidation_fee_proportional(self) -> None:
        """Total fee is split proportionally by each withdrawal's input share."""
        w1_txid = "aaa" + "0" * 61
        w2_txid = "bbb" + "0" * 61
        deposit_txid = "ddd" + "0" * 61

        # w1 contributes 0.6 BTC, w2 contributes 0.4 BTC; total = 1.0 BTC.
        # Deposit receives 0.999 BTC → total fee = 0.001 BTC.
        # w1 fee share = 0.001 * 0.6 = 0.0006 BTC
        # w2 fee share = 0.001 * 0.4 = 0.0004 BTC
        w1 = _withdrawal(db_id=1, txid=w1_txid, amount="0.60000000")
        w2 = _withdrawal(db_id=2, txid=w2_txid, amount="0.40000000")
        d = _deposit(db_id=10, txid=deposit_txid, amount="0.99900000")

        tx_map = {deposit_txid: _on_chain_tx(deposit_txid, [w1_txid, w2_txid])}

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
            matches = find_graph_matches([w1, w2], [d], MEMPOOL_URL, max_hops=1)

        assert len(matches) == 2  # noqa: PLR2004
        fee_by_withdrawal = {m.withdrawal_db_id: m.fee_crypto for m in matches}
        assert fee_by_withdrawal[1] == Decimal("0.00060000")
        assert fee_by_withdrawal[2] == Decimal("0.00040000")

    def test_consolidation_fee_zero_when_deposit_exceeds_sum(self) -> None:
        """Fee clamped to zero for each withdrawal if deposit > sum of inputs."""
        w1_txid = "aaa" + "0" * 61
        w2_txid = "bbb" + "0" * 61
        deposit_txid = "ddd" + "0" * 61

        w1 = _withdrawal(db_id=1, txid=w1_txid, amount="0.05000000")
        w2 = _withdrawal(db_id=2, txid=w2_txid, amount="0.05000000")
        # Deposit larger than total inputs — impossible on-chain but we guard against it.
        d = _deposit(db_id=10, txid=deposit_txid, amount="0.20000000")

        tx_map = {deposit_txid: _on_chain_tx(deposit_txid, [w1_txid, w2_txid])}

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
            matches = find_graph_matches([w1, w2], [d], MEMPOOL_URL, max_hops=1)

        assert len(matches) == 2  # noqa: PLR2004
        assert all(m.fee_crypto == Decimal("0") for m in matches)

    def test_unrelated_deposit_not_matched_to_consolidation_withdrawals(self) -> None:
        """Withdrawals not in target set are not incorrectly claimed."""
        w1_txid = "aaa" + "0" * 61
        w2_txid = "bbb" + "0" * 61
        unrelated_txid = "eee" + "0" * 61
        deposit_txid = "ddd" + "0" * 61

        w1 = _withdrawal(db_id=1, txid=w1_txid, amount="0.10000000")
        w2 = _withdrawal(db_id=2, txid=w2_txid, amount="0.10000000")
        d = _deposit(db_id=10, txid=deposit_txid, amount="0.19900000")

        # Deposit inputs include w1, an unrelated tx, but NOT w2.
        tx_map = {deposit_txid: _on_chain_tx(deposit_txid, [w1_txid, unrelated_txid])}

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
            matches = find_graph_matches([w1, w2], [d], MEMPOOL_URL, max_hops=1)

        assert len(matches) == 1
        assert matches[0].withdrawal_db_id == 1

    def test_hop1_only_when_mixed_hops(self) -> None:
        """Hop=1 inputs take priority; hop>1 ancestors are left unmatched."""
        w1_txid = "aaa" + "0" * 61
        w2_txid = "bbb" + "0" * 61
        w3_txid = "ccc" + "0" * 61
        intermediate_txid = "iii" + "0" * 61
        deposit_txid = "ddd" + "0" * 61

        # deposit has two direct inputs (w1, w2) and one hop=3 indirect ancestor (w3).
        # w3 is reachable via: deposit → intermediate → w3 (hop=2)
        w1 = _withdrawal(db_id=1, txid=w1_txid, amount="0.10000000")
        w2 = _withdrawal(db_id=2, txid=w2_txid, amount="0.10000000")
        w3 = _withdrawal(db_id=3, txid=w3_txid, amount="0.05000000")
        d = _deposit(db_id=10, txid=deposit_txid, amount="0.19900000")

        tx_map = {
            deposit_txid: _on_chain_tx(deposit_txid, [w1_txid, w2_txid, intermediate_txid]),
            intermediate_txid: _on_chain_tx(intermediate_txid, [w3_txid]),
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
            matches = find_graph_matches([w1, w2, w3], [d], MEMPOOL_URL, max_hops=3)

        # Only hop=1 inputs (w1, w2) should be matched; w3 stays free.
        assert len(matches) == 2  # noqa: PLR2004
        withdrawal_ids = {m.withdrawal_db_id for m in matches}
        assert withdrawal_ids == {1, 2}
        assert all(m.hops == 1 for m in matches)

    def test_multihop_1to1_when_no_direct_inputs(self) -> None:
        """When no hop=1 inputs exist, only the first multi-hop result is taken (1-to-1)."""
        w1_txid = "aaa" + "0" * 61
        w2_txid = "bbb" + "0" * 61
        intermediate1_txid = "iii" + "0" * 61
        intermediate2_txid = "jjj" + "0" * 61
        deposit_txid = "ddd" + "0" * 61

        # deposit has two intermediate inputs, each leading to a different withdrawal.
        w1 = _withdrawal(db_id=1, txid=w1_txid, amount="0.10000000")
        w2 = _withdrawal(db_id=2, txid=w2_txid, amount="0.10000000")
        d = _deposit(db_id=10, txid=deposit_txid, amount="0.19900000")

        tx_map = {
            deposit_txid: _on_chain_tx(deposit_txid, [intermediate1_txid, intermediate2_txid]),
            intermediate1_txid: _on_chain_tx(intermediate1_txid, [w1_txid]),
            intermediate2_txid: _on_chain_tx(intermediate2_txid, [w2_txid]),
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
            matches = find_graph_matches([w1, w2], [d], MEMPOOL_URL, max_hops=2)

        # Only 1 match — first multi-hop result wins; second is left free.
        assert len(matches) == 1
        assert matches[0].hops == 2  # noqa: PLR2004
