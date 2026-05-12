"""Tests for the pure on-chain fee/timestamp validation module.

Covers the override rule (user-paid sends only), audit-entry production,
and the various pass-through cases (no txid, unconfirmed, fetch failure).
``fetch_tx`` is injected as a fake — no network, no DB.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from sirop.models.enums import TransactionType
from sirop.models.transaction import Transaction
from sirop.node.models import OnChainTx
from sirop.node.verify import AuditEntry, is_user_paid_send, validate_fees

_MEMPOOL_URL = "http://localhost:3006/api"
_TXID_A = "aaa" + "0" * 61
_TXID_B = "bbb" + "0" * 61
_BLOCK_TIME = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)


def _tx(  # noqa: PLR0913 — keyword-only helper, all defaulted
    *,
    id_: int = 1,
    tx_type: TransactionType = TransactionType.TRANSFER_OUT,
    txid: str | None = _TXID_A,
    wallet_id: int | None = 7,
    fee_crypto: Decimal = Decimal("0.0001"),
    timestamp: datetime | None = None,
) -> Transaction:
    return Transaction(
        id=id_,
        source="sparrow",
        timestamp=timestamp or datetime(2025, 6, 1, 11, 50, 0, tzinfo=UTC),
        tx_type=tx_type,
        asset="BTC",
        amount=Decimal("0.01"),
        cad_value=Decimal("1000"),
        fee_cad=Decimal("10"),
        fee_crypto=fee_crypto,
        txid=txid,
        is_transfer=False,
        counterpart_id=None,
        notes="",
        wallet_id=wallet_id,
    )


def _onchain(
    *,
    fee_sat: int | None = 15_000,
    confirmed: bool = True,
    block_time: datetime | None = _BLOCK_TIME,
    block_height: int | None = 850_000,
) -> OnChainTx:
    return OnChainTx(
        txid=_TXID_A,
        fee_sat=fee_sat,
        confirmed=confirmed,
        block_time=block_time,
        vin_txids=(),
        vout_count=2,
        block_height=block_height,
    )


# ---------------------------------------------------------------------------
# is_user_paid_send
# ---------------------------------------------------------------------------


class TestIsUserPaidSend:
    def test_wallet_send_qualifies(self) -> None:
        assert is_user_paid_send(_tx(tx_type=TransactionType.TRANSFER_OUT, wallet_id=1))

    def test_wallet_withdrawal_qualifies(self) -> None:
        assert is_user_paid_send(_tx(tx_type=TransactionType.WITHDRAWAL, wallet_id=1))

    def test_no_wallet_does_not_qualify(self) -> None:
        assert not is_user_paid_send(_tx(tx_type=TransactionType.WITHDRAWAL, wallet_id=None))

    def test_receive_does_not_qualify(self) -> None:
        assert not is_user_paid_send(_tx(tx_type=TransactionType.TRANSFER_IN, wallet_id=1))

    def test_buy_does_not_qualify(self) -> None:
        assert not is_user_paid_send(_tx(tx_type=TransactionType.BUY, wallet_id=1))


# ---------------------------------------------------------------------------
# validate_fees
# ---------------------------------------------------------------------------


class TestValidateFees:
    def test_user_send_overrides_fee_and_timestamp(self) -> None:
        tx = _tx(fee_crypto=Decimal("0.0002"))  # imported fee differs from on-chain
        calls: list[tuple[str, str]] = []

        def fake_fetch(url: str, txid: str) -> OnChainTx | None:
            calls.append((url, txid))
            return _onchain(fee_sat=15_000)  # 0.00015 BTC

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)

        assert calls == [(_MEMPOOL_URL, _TXID_A)]
        assert len(verified) == 1
        v = verified[0]
        assert v.node_verified is True
        assert v.block_height == 850_000  # noqa: PLR2004
        assert v.tx.fee_crypto == Decimal("0.00015")
        assert v.tx.timestamp == _BLOCK_TIME

        assert len(audit) == 2  # noqa: PLR2004 — fee + timestamp overrides
        fields = {entry.field for entry in audit}
        assert fields == {"fee_crypto", "timestamp"}
        fee_entry = next(e for e in audit if e.field == "fee_crypto")
        assert fee_entry.old_value == "0.0002"
        assert fee_entry.new_value == "0.00015000"  # quantized to 8 dp
        assert fee_entry.txid == _TXID_A

    def test_user_send_matching_fee_no_audit(self) -> None:
        tx = _tx(fee_crypto=Decimal("0.00015"), timestamp=_BLOCK_TIME)

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(fee_sat=15_000)

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)

        assert verified[0].node_verified is True
        assert verified[0].tx.fee_crypto == Decimal("0.00015")
        assert audit == []

    def test_exchange_side_no_override_but_node_verified(self) -> None:
        """Exchange-paid withdrawal: fee preserved, block_height populated."""
        tx = _tx(
            tx_type=TransactionType.WITHDRAWAL,
            wallet_id=None,  # exchange-side row
            fee_crypto=Decimal("0.0005"),  # exchange-charged fee
        )

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(fee_sat=15_000)

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)

        v = verified[0]
        assert v.node_verified is True
        assert v.block_height == 850_000  # noqa: PLR2004
        assert v.tx.fee_crypto == Decimal("0.0005")  # unchanged
        assert v.tx.timestamp == tx.timestamp  # unchanged
        assert audit == []

    def test_receive_no_override(self) -> None:
        tx = _tx(tx_type=TransactionType.TRANSFER_IN, wallet_id=1)

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain()

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        assert verified[0].node_verified is True
        assert verified[0].tx.fee_crypto == tx.fee_crypto  # unchanged
        assert audit == []

    def test_no_txid_pass_through(self) -> None:
        tx = _tx(txid=None)
        calls: list[tuple[str, str]] = []

        def fake_fetch(url: str, txid: str) -> OnChainTx | None:
            calls.append((url, txid))
            return _onchain()

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        assert calls == []  # never fetched
        assert verified[0].node_verified is False
        assert verified[0].block_height is None
        assert audit == []

    def test_fetch_returns_none_pass_through(self) -> None:
        tx = _tx()

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return None

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        assert verified[0].node_verified is False
        assert verified[0].tx.fee_crypto == tx.fee_crypto
        assert audit == []

    def test_unconfirmed_tx_pass_through(self) -> None:
        tx = _tx()

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(confirmed=False, fee_sat=None, block_time=None)

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        assert verified[0].node_verified is False
        assert audit == []

    def test_progress_callback_invoked(self) -> None:
        txs = [_tx(id_=1, txid=_TXID_A), _tx(id_=2, txid=_TXID_B)]

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain()

        seen: list[tuple[int, int]] = []
        validate_fees(txs, fake_fetch, _MEMPOOL_URL, on_progress=lambda d, t: seen.append((d, t)))
        assert seen == [(1, 2), (2, 2)]


# ---------------------------------------------------------------------------
# AuditEntry serialization
# ---------------------------------------------------------------------------


class TestAuditEntrySerialization:
    def test_canonical_decimal_string_no_scientific_notation(self) -> None:
        """Tiny fees must serialize as fixed-point, not '1.5E-7' etc."""
        tx = _tx(fee_crypto=Decimal("0.00000150"))

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(fee_sat=10)  # 0.00000010 BTC

        _verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        fee_entry = next(e for e in audit if e.field == "fee_crypto")
        assert "E" not in fee_entry.new_value
        assert "E" not in fee_entry.old_value
        assert isinstance(fee_entry, AuditEntry)
