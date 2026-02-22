"""Tests for the stir command — transfer override model, repository CRUD, and
matcher integration.

All test data uses obviously fake identifiers (txids like "aaa...111",
amounts like 0.01 BTC) to ensure nothing real is committed to the repo.
"""

from __future__ import annotations

import dataclasses
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from sirop.db import repositories as repo
from sirop.db.schema import create_tables
from sirop.models.enums import TransactionType
from sirop.models.override import TransferOverride
from sirop.models.transaction import Transaction
from sirop.transfer_match.matcher import match_transfers

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TS = datetime(2025, 3, 1, 12, 0, 0, tzinfo=UTC)


def _tx(  # noqa: PLR0913
    tx_id: int,
    tx_type: TransactionType,
    amount: str = "0.01",
    asset: str = "BTC",
    txid: str | None = None,
    offset_hours: int = 0,
    source: str = "test",
    cad_value: str = "500",
) -> Transaction:
    return Transaction(
        id=tx_id,
        source=source,
        timestamp=_BASE_TS + timedelta(hours=offset_hours),
        tx_type=tx_type,
        asset=asset,
        amount=Decimal(amount),
        cad_value=Decimal(cad_value),
        fee_cad=Decimal("0"),
        fee_crypto=Decimal("0"),
        txid=txid,
        is_transfer=False,
        counterpart_id=None,
        notes="",
    )


def _make_conn() -> sqlite3.Connection:
    """Create an in-memory SQLite connection with the full sirop schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    return conn


def _seed_transactions(conn: sqlite3.Connection, txs: list[Transaction]) -> None:
    """Insert fake Transaction rows directly — bypasses the normalizer."""
    with conn:
        for tx in txs:
            conn.execute(
                """
                INSERT INTO transactions
                    (id, raw_id, timestamp, transaction_type, asset, amount,
                     fee_crypto, fee_currency, cad_amount, cad_fee, cad_rate,
                     txid, source, is_transfer, counterpart_id, notes)
                VALUES (?, NULL, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    tx.id,
                    tx.timestamp.isoformat(),
                    tx.tx_type.value,
                    tx.asset,
                    format(tx.amount, "f"),
                    format(tx.fee_crypto, "f") if tx.fee_crypto else None,
                    format(tx.cad_value, "f"),
                    format(tx.fee_cad, "f"),
                    format(tx.cad_value / tx.amount, "f") if tx.amount else "0",
                    tx.txid,
                    tx.source,
                    1 if tx.is_transfer else 0,
                    tx.notes,
                ),
            )


# ---------------------------------------------------------------------------
# TransferOverride model
# ---------------------------------------------------------------------------


class TestTransferOverrideModel:
    def test_fields(self) -> None:
        now = datetime.now(UTC)
        ov = TransferOverride(
            id=1,
            tx_id_a=10,
            tx_id_b=20,
            action="link",
            created_at=now,
            note="test note",
        )
        assert ov.tx_id_a == 10  # noqa: PLR2004
        assert ov.tx_id_b == 20  # noqa: PLR2004
        assert ov.action == "link"
        assert ov.note == "test note"

    def test_frozen(self) -> None:
        now = datetime.now(UTC)
        ov = TransferOverride(id=1, tx_id_a=1, tx_id_b=2, action="unlink", created_at=now, note="")
        with pytest.raises(dataclasses.FrozenInstanceError):
            ov.action = "link"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Repository CRUD
# ---------------------------------------------------------------------------


class TestTransferOverrideRepository:
    def test_write_and_read_link(self) -> None:
        conn = _make_conn()
        txs = [
            _tx(1, TransactionType.WITHDRAWAL),
            _tx(2, TransactionType.DEPOSIT),
        ]
        _seed_transactions(conn, txs)

        ov = repo.write_transfer_override(conn, 1, 2, "link", "manual link")
        assert ov.id > 0
        assert ov.tx_id_a == 1
        assert ov.tx_id_b == 2  # noqa: PLR2004
        assert ov.action == "link"
        assert ov.note == "manual link"

        overrides = repo.read_transfer_overrides(conn)
        assert len(overrides) == 1
        assert overrides[0].id == ov.id

    def test_write_and_read_unlink(self) -> None:
        conn = _make_conn()
        txs = [
            _tx(1, TransactionType.WITHDRAWAL),
            _tx(2, TransactionType.DEPOSIT),
        ]
        _seed_transactions(conn, txs)

        repo.write_transfer_override(conn, 1, 2, "unlink")
        overrides = repo.read_transfer_overrides(conn)
        assert overrides[0].action == "unlink"
        assert overrides[0].note == ""

    def test_delete_override(self) -> None:
        conn = _make_conn()
        txs = [_tx(1, TransactionType.WITHDRAWAL), _tx(2, TransactionType.DEPOSIT)]
        _seed_transactions(conn, txs)

        ov = repo.write_transfer_override(conn, 1, 2, "link")
        repo.delete_transfer_override(conn, ov.id)
        assert repo.read_transfer_overrides(conn) == []

    def test_clear_overrides_for_tx(self) -> None:
        conn = _make_conn()
        txs = [
            _tx(1, TransactionType.WITHDRAWAL),
            _tx(2, TransactionType.DEPOSIT),
            _tx(3, TransactionType.WITHDRAWAL),
            _tx(4, TransactionType.DEPOSIT),
        ]
        _seed_transactions(conn, txs)

        repo.write_transfer_override(conn, 1, 2, "link")
        repo.write_transfer_override(conn, 3, 4, "unlink")

        # Clearing tx 1 should remove the first override only.
        count = repo.clear_transfer_overrides_for_tx(conn, 1)
        assert count == 1
        remaining = repo.read_transfer_overrides(conn)
        assert len(remaining) == 1
        assert remaining[0].tx_id_a == 3  # noqa: PLR2004

    def test_clear_affects_both_legs(self) -> None:
        conn = _make_conn()
        txs = [_tx(5, TransactionType.WITHDRAWAL), _tx(6, TransactionType.DEPOSIT)]
        _seed_transactions(conn, txs)

        repo.write_transfer_override(conn, 5, 6, "link")
        # Clearing by the second leg (tx_id_b) should also work.
        count = repo.clear_transfer_overrides_for_tx(conn, 6)
        assert count == 1
        assert repo.read_transfer_overrides(conn) == []

    def test_multiple_overrides_ordered(self) -> None:
        conn = _make_conn()
        txs = [
            _tx(1, TransactionType.WITHDRAWAL),
            _tx(2, TransactionType.DEPOSIT),
            _tx(3, TransactionType.WITHDRAWAL),
            _tx(4, TransactionType.DEPOSIT),
        ]
        _seed_transactions(conn, txs)

        repo.write_transfer_override(conn, 1, 2, "link")
        repo.write_transfer_override(conn, 3, 4, "unlink")
        overrides = repo.read_transfer_overrides(conn)
        assert len(overrides) == 2  # noqa: PLR2004
        assert overrides[0].tx_id_a == 1
        assert overrides[1].tx_id_a == 3  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Matcher integration — forced links
# ---------------------------------------------------------------------------


class TestMatcherForcedLink:
    def test_forced_link_pairs_non_matching_transactions(self) -> None:
        """Two transactions that would not auto-match are force-linked."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.10", cad_value="6000")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.05", offset_hours=48, cad_value="3000")
        # These won't auto-match (amount mismatch + too far apart).
        now = datetime.now(UTC)
        override = TransferOverride(
            id=1, tx_id_a=1, tx_id_b=2, action="link", created_at=now, note=""
        )
        events, income = match_transfers([out_tx, in_tx], overrides=[override])

        transfer_events = [e for e in events if e.event_type == "transfer"]
        assert len(transfer_events) == 2  # noqa: PLR2004
        assert not any(e.is_taxable for e in transfer_events)

    def test_forced_link_overrides_amount_mismatch(self) -> None:
        """Force-linking ignores the 1% tolerance check."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="1.00", cad_value="60000")
        in_tx = _tx(
            2,
            TransactionType.DEPOSIT,
            amount="0.50",  # 50% mismatch
            cad_value="30000",
        )
        now = datetime.now(UTC)
        override = TransferOverride(
            id=1, tx_id_a=1, tx_id_b=2, action="link", created_at=now, note=""
        )
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        transfer_events = [e for e in events if e.event_type == "transfer"]
        assert len(transfer_events) == 2  # noqa: PLR2004

    def test_forced_link_skipped_when_leg_already_claimed(self) -> None:
        """A forced link is skipped when one of its legs is already paired by an earlier link."""
        out1 = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        out2 = _tx(2, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500", offset_hours=1)
        in1 = _tx(3, TransactionType.DEPOSIT, amount="0.01", cad_value="500")

        now = datetime.now(UTC)
        # Two forced links compete for in1 (id=3). The first (out1 ↔ in1) wins.
        # The second (out2 ↔ in1) is skipped because in1 is already paired.
        overrides = [
            TransferOverride(id=1, tx_id_a=1, tx_id_b=3, action="link", created_at=now, note=""),
            TransferOverride(id=2, tx_id_a=2, tx_id_b=3, action="link", created_at=now, note=""),
        ]
        events, _ = match_transfers([out1, out2, in1], overrides=overrides)

        # out1 and in1 are a forced-link transfer pair.
        transfers = [e for e in events if e.event_type == "transfer"]
        assert len(transfers) == 2  # noqa: PLR2004
        transfer_vtx_ids = {e.vtx_id for e in transfers}
        assert 1 in transfer_vtx_ids
        assert 3 in transfer_vtx_ids  # noqa: PLR2004

        # out2 (id=2) is unmatched and becomes a sell.
        sells = [e for e in events if e.event_type == "sell"]
        assert any(e.vtx_id == 2 for e in sells)  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Matcher integration — forced unlinks
# ---------------------------------------------------------------------------


class TestMatcherForcedUnlink:
    def test_forced_unlink_breaks_auto_match(self) -> None:
        """A pair that would auto-match by txid is prevented by forced unlink."""
        out_tx = _tx(
            1, TransactionType.WITHDRAWAL, txid="abc123fake", amount="0.01", cad_value="500"
        )
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="abc123fake", amount="0.01", cad_value="500")

        now = datetime.now(UTC)
        override = TransferOverride(
            id=1, tx_id_a=1, tx_id_b=2, action="unlink", created_at=now, note=""
        )
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        # Both should be taxable (sell + buy), not transfers.
        taxable = [e for e in events if e.is_taxable]
        assert len(taxable) == 2  # noqa: PLR2004
        types = {e.event_type for e in taxable}
        assert "sell" in types
        assert "buy" in types

    def test_forced_unlink_amount_match_pair(self) -> None:
        """Amount+time match is broken by forced unlink."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01", offset_hours=1, cad_value="500")

        now = datetime.now(UTC)
        override = TransferOverride(
            id=1, tx_id_a=1, tx_id_b=2, action="unlink", created_at=now, note=""
        )
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        taxable = [e for e in events if e.is_taxable]
        assert len(taxable) == 2  # noqa: PLR2004

    def test_unlink_symmetric(self) -> None:
        """Forced unlink blocks the pair regardless of which id is tx_id_a."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, txid="deadbeef", amount="0.01", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="deadbeef", amount="0.01", cad_value="500")

        now = datetime.now(UTC)
        # Reversed order compared to the default (out, in) convention.
        override = TransferOverride(
            id=1, tx_id_a=2, tx_id_b=1, action="unlink", created_at=now, note=""
        )
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        taxable = [e for e in events if e.is_taxable]
        assert len(taxable) == 2  # noqa: PLR2004


# ---------------------------------------------------------------------------
# Matcher integration — no overrides (regression)
# ---------------------------------------------------------------------------


class TestMatcherNoOverrides:
    def test_txid_match_still_works(self) -> None:
        """Auto-matching by txid continues to work when no overrides are passed."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, txid="feedcafe", amount="0.01", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="feedcafe", amount="0.01", cad_value="500")

        events, _ = match_transfers([out_tx, in_tx])
        transfers = [e for e in events if e.event_type == "transfer"]
        assert len(transfers) == 2  # noqa: PLR2004

    def test_amount_time_match_still_works(self) -> None:
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01", offset_hours=1, cad_value="500")

        events, _ = match_transfers([out_tx, in_tx])
        transfers = [e for e in events if e.event_type == "transfer"]
        assert len(transfers) == 2  # noqa: PLR2004

    def test_empty_overrides_list(self) -> None:
        """Passing an empty list is equivalent to passing None."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, txid="cafe0000", amount="0.01", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="cafe0000", amount="0.01", cad_value="500")

        events_none, _ = match_transfers([out_tx, in_tx], overrides=None)
        events_empty, _ = match_transfers([out_tx, in_tx], overrides=[])
        assert len(events_none) == len(events_empty)
