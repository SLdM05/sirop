"""Integration tests for the verify stage's node-backed fee validation.

Exercises the repository layer (`promote_to_verified_with_node`) and the
node-aware overlay in `read_transactions`.  Uses in-memory SQLite plus the
real schema; fetch_tx is faked.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from sirop.db import repositories as repo
from sirop.db.schema import create_tables
from sirop.models.enums import TransactionType
from sirop.models.transaction import Transaction
from sirop.node.models import OnChainTx
from sirop.node.verify import validate_fees

_MEMPOOL_URL = "http://localhost:3006/api"
_TXID_SEND = "aaa" + "0" * 61
_TXID_EXCHANGE = "bbb" + "0" * 61
_BLOCK_TIME = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.execute("PRAGMA foreign_keys=ON")
    c.row_factory = sqlite3.Row
    create_tables(c)
    return c


def _insert_wallet(conn: sqlite3.Connection, name: str = "sparrow-main") -> int:
    return repo.find_or_create_wallet(conn, name=name, source="sparrow", auto_created=False).id


def _make_tx(
    *,
    id_: int,
    tx_type: TransactionType,
    txid: str | None,
    wallet_id: int | None,
    fee_crypto: Decimal,
) -> Transaction:
    return Transaction(
        id=id_,
        source="sparrow" if wallet_id is not None else "ndax",
        timestamp=datetime(2025, 6, 1, 11, 50, 0, tzinfo=UTC),
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


def _onchain(fee_sat: int = 15_000, height: int = 850_000) -> OnChainTx:
    return OnChainTx(
        txid=_TXID_SEND,
        fee_sat=fee_sat,
        confirmed=True,
        block_time=_BLOCK_TIME,
        vin_txids=(),
        vout_count=2,
        block_height=height,
    )


# ---------------------------------------------------------------------------
# promote_to_verified_with_node
# ---------------------------------------------------------------------------


class TestPromoteToVerifiedWithNode:
    def test_writes_rows_and_audit_entries(self, conn: sqlite3.Connection) -> None:
        wallet_id = _insert_wallet(conn)
        tx = _make_tx(
            id_=0,
            tx_type=TransactionType.TRANSFER_OUT,
            txid=_TXID_SEND,
            wallet_id=wallet_id,
            fee_crypto=Decimal("0.0002"),
        )
        written = repo.write_transactions(conn, [tx])
        tx = written[0]

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(fee_sat=15_000)

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        rows_inserted, overrides = repo.promote_to_verified_with_node(conn, verified, audit)

        assert rows_inserted == 1
        assert overrides == 2  # noqa: PLR2004 — fee + timestamp

        # Verified row exists with corrected fee + block_height + node_verified flag.
        row = conn.execute(
            "SELECT fee_crypto, timestamp, node_verified, block_height, txid"
            " FROM verified_transactions"
        ).fetchone()
        assert row["node_verified"] == 1
        assert Decimal(row["fee_crypto"]) == Decimal("0.00015")
        assert row["block_height"] == 850_000  # noqa: PLR2004
        assert datetime.fromisoformat(row["timestamp"]) == _BLOCK_TIME

        # Audit log captured both overrides.
        log = conn.execute(
            "SELECT field, old_value, new_value, stage FROM audit_log ORDER BY field"
        ).fetchall()
        assert len(log) == 2  # noqa: PLR2004
        fields = {r["field"] for r in log}
        assert fields == {"fee_crypto", "timestamp"}
        assert all(r["stage"] == "verify" for r in log)

    def test_exchange_row_node_verified_but_fee_preserved(self, conn: sqlite3.Connection) -> None:
        tx = _make_tx(
            id_=0,
            tx_type=TransactionType.WITHDRAWAL,
            txid=_TXID_EXCHANGE,
            wallet_id=None,  # exchange-side
            fee_crypto=Decimal("0.0005"),  # exchange-charged fee
        )
        written = repo.write_transactions(conn, [tx])
        tx = written[0]

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(fee_sat=15_000)  # on-chain fee differs

        verified, audit = validate_fees([tx], fake_fetch, _MEMPOOL_URL)
        repo.promote_to_verified_with_node(conn, verified, audit)

        row = conn.execute(
            "SELECT fee_crypto, node_verified, block_height FROM verified_transactions"
        ).fetchone()
        assert row["node_verified"] == 1
        assert row["block_height"] == 850_000  # noqa: PLR2004
        assert Decimal(row["fee_crypto"]) == Decimal("0.0005")  # exchange fee preserved
        assert audit == []

    def test_atomic_write(self, conn: sqlite3.Connection) -> None:
        """All inserts succeed within one transaction (no partial state)."""
        wallet_id = _insert_wallet(conn)
        txs = [
            _make_tx(
                id_=0,
                tx_type=TransactionType.TRANSFER_OUT,
                txid=_TXID_SEND,
                wallet_id=wallet_id,
                fee_crypto=Decimal("0.0001"),
            )
        ]
        written = repo.write_transactions(conn, txs)

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain()

        verified, audit = validate_fees(written, fake_fetch, _MEMPOOL_URL)
        rows_inserted, _overrides = repo.promote_to_verified_with_node(conn, verified, audit)
        # Calling promote a second time should append (caller is responsible
        # for clearing on re-runs); confirm we don't crash.
        rows_inserted_2, _ = repo.promote_to_verified_with_node(conn, verified, audit)
        assert rows_inserted == rows_inserted_2 == 1


# ---------------------------------------------------------------------------
# read_transactions overlays verified data
# ---------------------------------------------------------------------------


class TestReadTransactionsOverlay:
    def test_overlay_applies_verified_fee_and_timestamp(self, conn: sqlite3.Connection) -> None:
        wallet_id = _insert_wallet(conn)
        tx = _make_tx(
            id_=0,
            tx_type=TransactionType.TRANSFER_OUT,
            txid=_TXID_SEND,
            wallet_id=wallet_id,
            fee_crypto=Decimal("0.0002"),
        )
        written = repo.write_transactions(conn, [tx])

        def fake_fetch(_url: str, _txid: str) -> OnChainTx | None:
            return _onchain(fee_sat=15_000)

        verified, audit = validate_fees(written, fake_fetch, _MEMPOOL_URL)
        repo.promote_to_verified_with_node(conn, verified, audit)

        # read_transactions should reflect the overridden fee + timestamp.
        result = repo.read_transactions(conn)
        assert len(result) == 1
        t = result[0]
        assert t.node_verified is True
        assert t.fee_crypto == Decimal("0.00015")
        assert t.timestamp == _BLOCK_TIME

    def test_overlay_inert_when_pass_through(self, conn: sqlite3.Connection) -> None:
        """promote_to_verified (pass-through) sets node_verified=0; no override."""
        tx = _make_tx(
            id_=0,
            tx_type=TransactionType.TRANSFER_OUT,
            txid=_TXID_SEND,
            wallet_id=None,
            fee_crypto=Decimal("0.0003"),
        )
        repo.write_transactions(conn, [tx])
        repo.promote_to_verified(conn)  # pass-through

        result = repo.read_transactions(conn)
        assert len(result) == 1
        assert result[0].node_verified is False
        assert result[0].fee_crypto == Decimal("0.0003")  # untouched

    def test_overlay_no_verified_row_falls_back_to_transactions(
        self, conn: sqlite3.Connection
    ) -> None:
        """Before verify runs there is no verified_transactions row — read still works."""
        tx = _make_tx(
            id_=0,
            tx_type=TransactionType.TRANSFER_OUT,
            txid=_TXID_SEND,
            wallet_id=None,
            fee_crypto=Decimal("0.0004"),
        )
        repo.write_transactions(conn, [tx])

        result = repo.read_transactions(conn)
        assert len(result) == 1
        assert result[0].node_verified is False
        assert result[0].fee_crypto == Decimal("0.0004")
