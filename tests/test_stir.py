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

from sirop.cli.stir import _compute_implied_fee, _parse_fee_amount, _validate_wallet_name
from sirop.db import repositories as repo
from sirop.db.schema import create_tables, migrate_to_v5
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
    fee_crypto: str = "0",
    fee_cad: str = "0",
) -> Transaction:
    return Transaction(
        id=tx_id,
        source=source,
        timestamp=_BASE_TS + timedelta(hours=offset_hours),
        tx_type=tx_type,
        asset=asset,
        amount=Decimal(amount),
        cad_value=Decimal(cad_value),
        fee_cad=Decimal(fee_cad),
        fee_crypto=Decimal(fee_crypto),
        txid=txid,
        is_transfer=False,
        counterpart_id=None,
        notes="",
        wallet_id=None,
    )


def _make_conn() -> sqlite3.Connection:
    """Create an in-memory SQLite connection with the full sirop schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    migrate_to_v5(conn)
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
                     txid, source, is_transfer, counterpart_id, notes, wallet_id)
                VALUES (?, NULL, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
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
                    tx.wallet_id,
                ),
            )


def _make_override(  # noqa: PLR0913
    tx_id_a: int,
    tx_id_b: int | None,
    action: str = "link",
    implied_fee: str = "0",
    external_wallet: str = "",
    note: str = "",
) -> TransferOverride:
    """Build a TransferOverride with all v5 fields for use in tests."""
    return TransferOverride(
        id=0,
        tx_id_a=tx_id_a,
        tx_id_b=tx_id_b,
        action=action,  # type: ignore[arg-type]
        implied_fee_crypto=Decimal(implied_fee),
        external_wallet=external_wallet,
        created_at=datetime.now(UTC),
        note=note,
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
            implied_fee_crypto=Decimal("0.00015"),
            external_wallet="",
            created_at=now,
            note="test note",
        )
        assert ov.tx_id_a == 10  # noqa: PLR2004
        assert ov.tx_id_b == 20  # noqa: PLR2004
        assert ov.action == "link"
        assert ov.implied_fee_crypto == Decimal("0.00015")
        assert ov.note == "test note"

    def test_fields_external(self) -> None:
        now = datetime.now(UTC)
        ov = TransferOverride(
            id=2,
            tx_id_a=5,
            tx_id_b=None,
            action="external-out",
            implied_fee_crypto=Decimal("0"),
            external_wallet="ledger-cold",
            created_at=now,
            note="",
        )
        assert ov.tx_id_b is None
        assert ov.action == "external-out"
        assert ov.external_wallet == "ledger-cold"

    def test_frozen(self) -> None:
        now = datetime.now(UTC)
        ov = TransferOverride(
            id=1,
            tx_id_a=1,
            tx_id_b=2,
            action="unlink",
            implied_fee_crypto=Decimal("0"),
            external_wallet="",
            created_at=now,
            note="",
        )
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

        ov = repo.write_transfer_override(conn, 1, 2, "link", note="manual link")
        assert ov.id > 0
        assert ov.tx_id_a == 1
        assert ov.tx_id_b == 2  # noqa: PLR2004
        assert ov.action == "link"
        assert ov.note == "manual link"
        assert ov.implied_fee_crypto == Decimal("0")

        overrides = repo.read_transfer_overrides(conn)
        assert len(overrides) == 1
        assert overrides[0].id == ov.id

    def test_write_and_read_link_with_implied_fee(self) -> None:
        conn = _make_conn()
        txs = [
            _tx(1, TransactionType.WITHDRAWAL, amount="0.01005"),
            _tx(2, TransactionType.DEPOSIT, amount="0.01"),
        ]
        _seed_transactions(conn, txs)

        fee = Decimal("0.00005")
        ov = repo.write_transfer_override(conn, 1, 2, "link", implied_fee_crypto=fee)
        assert ov.implied_fee_crypto == fee

        read_back = repo.read_transfer_overrides(conn)
        assert read_back[0].implied_fee_crypto == fee

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
        assert overrides[0].implied_fee_crypto == Decimal("0")

    def test_write_external_out(self) -> None:
        conn = _make_conn()
        txs = [_tx(1, TransactionType.WITHDRAWAL)]
        _seed_transactions(conn, txs)

        ov = repo.write_transfer_override(
            conn, 1, None, "external-out", external_wallet="ledger-cold"
        )
        assert ov.tx_id_b is None
        assert ov.action == "external-out"
        assert ov.external_wallet == "ledger-cold"

        read_back = repo.read_transfer_overrides(conn)
        assert read_back[0].action == "external-out"
        assert read_back[0].tx_id_b is None
        assert read_back[0].external_wallet == "ledger-cold"

    def test_write_external_in(self) -> None:
        conn = _make_conn()
        txs = [_tx(1, TransactionType.DEPOSIT)]
        _seed_transactions(conn, txs)

        ov = repo.write_transfer_override(
            conn, 1, None, "external-in", external_wallet="binance-old"
        )
        assert ov.action == "external-in"
        assert ov.external_wallet == "binance-old"

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

    def test_clear_affects_external_override(self) -> None:
        """clear_transfer_overrides_for_tx removes external-out/in (tx_id_b=NULL) by tx_id_a."""
        conn = _make_conn()
        txs = [_tx(7, TransactionType.WITHDRAWAL)]
        _seed_transactions(conn, txs)

        repo.write_transfer_override(conn, 7, None, "external-out", external_wallet="trezor")
        count = repo.clear_transfer_overrides_for_tx(conn, 7)
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
        override = _make_override(1, 2, "link")
        events, _income = match_transfers([out_tx, in_tx], overrides=[override])

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
        override = _make_override(1, 2, "link")
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        transfer_events = [e for e in events if e.event_type == "transfer"]
        assert len(transfer_events) == 2  # noqa: PLR2004

    def test_forced_link_implied_fee_creates_fee_disposal(self) -> None:
        """When implied_fee_crypto > 0, a fee_disposal event is emitted."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01005", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01", cad_value="498")
        # User has set implied_fee = 0.00005 (sent - received)
        override = _make_override(1, 2, "link", implied_fee="0.00005")
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.00005")
        assert fee_events[0].asset == "BTC"

    def test_forced_link_skipped_when_leg_already_claimed(self) -> None:
        """A forced link is skipped when one of its legs is already paired by an earlier link."""
        out1 = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        out2 = _tx(2, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500", offset_hours=1)
        in1 = _tx(3, TransactionType.DEPOSIT, amount="0.01", cad_value="500")

        # Two forced links compete for in1 (id=3). The first (out1 ↔ in1) wins.
        # The second (out2 ↔ in1) is skipped because in1 is already paired.
        overrides = [
            _make_override(1, 3, "link"),
            _make_override(2, 3, "link"),
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
# Matcher integration — external overrides
# ---------------------------------------------------------------------------


class TestMatcherExternalOverrides:
    def test_external_out_marks_as_non_taxable(self) -> None:
        """A withdrawal marked external-out becomes a non-taxable 'external' event."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        override = _make_override(1, None, "external-out", external_wallet="ledger")
        events, _ = match_transfers([out_tx], overrides=[override])

        ext_events = [e for e in events if e.event_type == "external"]
        assert len(ext_events) == 1
        assert not ext_events[0].is_taxable
        assert ext_events[0].vtx_id == 1

    def test_external_in_marks_as_non_taxable(self) -> None:
        """A deposit marked external-in becomes a non-taxable 'external' event."""
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.05", cad_value="2500")
        override = _make_override(2, None, "external-in", external_wallet="old-exchange")
        events, _ = match_transfers([in_tx], overrides=[override])

        ext_events = [e for e in events if e.event_type == "external"]
        assert len(ext_events) == 1
        assert not ext_events[0].is_taxable

    def test_external_out_does_not_auto_match(self) -> None:
        """A tx marked external-out is not auto-matched even if a matching deposit exists."""
        out_tx = _tx(
            1, TransactionType.WITHDRAWAL, txid="abc123fake", amount="0.01", cad_value="500"
        )
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="abc123fake", amount="0.01", cad_value="500")
        override = _make_override(1, None, "external-out")
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        # out_tx (id:1) is external; in_tx (id:2) is unmatched → buy
        ext_events = [e for e in events if e.event_type == "external"]
        assert any(e.vtx_id == 1 for e in ext_events)
        buy_events = [e for e in events if e.event_type == "buy"]
        assert any(e.vtx_id == 2 for e in buy_events)  # noqa: PLR2004


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

        override = _make_override(1, 2, "unlink")
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

        override = _make_override(1, 2, "unlink")
        events, _ = match_transfers([out_tx, in_tx], overrides=[override])

        taxable = [e for e in events if e.is_taxable]
        assert len(taxable) == 2  # noqa: PLR2004

    def test_unlink_symmetric(self) -> None:
        """Forced unlink blocks the pair regardless of which id is tx_id_a."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, txid="deadbeef", amount="0.01", cad_value="500")
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="deadbeef", amount="0.01", cad_value="500")

        # Reversed order compared to the default (out, in) convention.
        override = _make_override(2, 1, "unlink")
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

    def test_txid_match_amount_diff_no_fee_crypto_creates_fee_disposal(self) -> None:
        """txid match with amount diff and no fee_crypto → fee_disposal from diff."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, txid="abc123", amount="0.01", cad_value="600")
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="abc123", amount="0.0099")

        events, _ = match_transfers([out_tx, in_tx])
        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.0001")
        assert fee_events[0].is_taxable is True

    def test_txid_match_equal_amounts_fee_crypto_creates_fee_disposal(self) -> None:
        """txid match with equal amounts and fee_crypto set → fee_disposal from fee_crypto."""
        out_tx = _tx(
            1,
            TransactionType.WITHDRAWAL,
            txid="abc123",
            amount="0.01",
            cad_value="600",
            fee_crypto="0.0001",
        )
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="abc123", amount="0.01")

        events, _ = match_transfers([out_tx, in_tx])
        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.0001")

    def test_txid_match_observed_diff_preferred_over_fee_crypto(self) -> None:
        """txid match: observed amount diff is used instead of fee_crypto when diff > 0."""
        # fee_crypto says 0.0001 but on-chain diff is 0.00015
        out_tx = _tx(
            1,
            TransactionType.WITHDRAWAL,
            txid="abc123",
            amount="0.01",
            cad_value="600",
            fee_crypto="0.0001",
        )
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="abc123", amount="0.00985")

        events, _ = match_transfers([out_tx, in_tx])
        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.00015")

    def test_txid_match_equal_amounts_no_fee_no_disposal(self) -> None:
        """txid match with equal amounts and no fee_crypto → no fee_disposal emitted."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, txid="abc123", amount="0.01", cad_value="600")
        in_tx = _tx(2, TransactionType.DEPOSIT, txid="abc123", amount="0.01")

        events, _ = match_transfers([out_tx, in_tx])
        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 0


# ---------------------------------------------------------------------------
# Amount validation (_compute_implied_fee)
# ---------------------------------------------------------------------------


class TestComputeImpliedFee:
    def test_clean_transfer_no_fee(self) -> None:
        """Sent == received → fee is zero, no error."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01")
        fee, err = _compute_implied_fee(out_tx, in_tx)
        assert fee == Decimal("0")
        assert err == ""

    def test_fee_equals_difference(self) -> None:
        """Sent > received → fee = sent - received."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01005")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01")
        fee, err = _compute_implied_fee(out_tx, in_tx)
        assert fee == Decimal("0.00005")
        assert err == ""

    def test_impossible_link_rejected(self) -> None:
        """Received > sent → non-empty error string, fee=0."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.02")
        fee, err = _compute_implied_fee(out_tx, in_tx)
        assert fee == Decimal("0")
        assert err != ""
        assert "impossible" in err.lower() or "greater" in err.lower() or "exceeds" in err.lower()

    def test_asset_mismatch_rejected(self) -> None:
        """Different assets → non-empty error string."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", asset="BTC")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01", asset="ETH")
        fee, err = _compute_implied_fee(out_tx, in_tx)
        assert fee == Decimal("0")
        assert "mismatch" in err.lower() or "btc" in err.lower()

    def test_eth_asset_agnostic(self) -> None:
        """Works for non-BTC assets."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="1.005", asset="ETH")
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="1.000", asset="ETH")
        fee, err = _compute_implied_fee(out_tx, in_tx)
        assert fee == Decimal("0.005")
        assert err == ""


# ---------------------------------------------------------------------------
# Wallet name validation
# ---------------------------------------------------------------------------


class TestValidateWalletName:
    def test_empty_string_accepted(self) -> None:
        assert _validate_wallet_name("") is None

    def test_valid_alphanumeric(self) -> None:
        assert _validate_wallet_name("ledger1") is None

    def test_valid_with_hyphens_and_underscores(self) -> None:
        assert _validate_wallet_name("cold-wallet_2025") is None

    def test_starts_with_digit(self) -> None:
        assert _validate_wallet_name("2025wallet") is None

    def test_space_rejected(self) -> None:
        assert _validate_wallet_name("cold wallet") is not None

    def test_special_char_rejected(self) -> None:
        assert _validate_wallet_name("wallet!") is not None

    def test_slash_rejected(self) -> None:
        assert _validate_wallet_name("cold/vault") is not None

    def test_max_length_accepted(self) -> None:
        # 64-char name: 1 leading + 63 trailing alnum chars
        assert _validate_wallet_name("a" * 64) is None

    def test_over_max_length_rejected(self) -> None:
        assert _validate_wallet_name("a" * 65) is not None


# ---------------------------------------------------------------------------
# _parse_fee_amount — validation for the untracked-wallet fee prompt
# ---------------------------------------------------------------------------


class TestParseFeeAmount:
    def test_valid_fee(self) -> None:
        fee, err = _parse_fee_amount("0.00005", "BTC", Decimal("0.01"))
        assert fee == Decimal("0.00005")
        assert err == ""

    def test_non_numeric_rejected(self) -> None:
        fee, err = _parse_fee_amount("abc", "BTC", Decimal("0.01"))
        assert err != ""
        assert fee == Decimal("0")

    def test_zero_rejected(self) -> None:
        fee, err = _parse_fee_amount("0", "BTC", Decimal("0.01"))
        assert err != ""

    def test_negative_rejected(self) -> None:
        fee, err = _parse_fee_amount("-0.001", "BTC", Decimal("0.01"))
        assert err != ""

    def test_fee_equal_to_amount_rejected(self) -> None:
        fee, err = _parse_fee_amount("0.01", "BTC", Decimal("0.01"))
        assert err != ""

    def test_fee_greater_than_amount_rejected(self) -> None:
        fee, err = _parse_fee_amount("0.02", "BTC", Decimal("0.01"))
        assert err != ""

    def test_fee_just_below_amount_accepted(self) -> None:
        fee, err = _parse_fee_amount("0.00999999", "BTC", Decimal("0.01"))
        assert err == ""
        assert fee == Decimal("0.00999999")


# ---------------------------------------------------------------------------
# Matcher — fee micro-disposal from external overrides
# ---------------------------------------------------------------------------


class TestMatcherExternalFeeDisposal:
    def test_external_out_with_fee_emits_fee_disposal(self) -> None:
        """external-out with implied_fee_crypto > 0 emits a taxable fee_disposal."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        ov = _make_override(1, None, action="external-out", implied_fee="0.00005")
        events, _ = match_transfers([out_tx], overrides=[ov])

        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.00005")
        assert fee_events[0].is_taxable is True

    def test_external_in_with_fee_emits_fee_disposal(self) -> None:
        """external-in with implied_fee_crypto > 0 also emits a fee_disposal."""
        in_tx = _tx(2, TransactionType.DEPOSIT, amount="0.01", cad_value="500")
        ov = _make_override(2, None, action="external-in", implied_fee="0.0001")
        events, _ = match_transfers([in_tx], overrides=[ov])

        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.0001")

    def test_external_out_without_fee_no_fee_disposal(self) -> None:
        """external-out with zero implied_fee_crypto emits no fee_disposal."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        ov = _make_override(1, None, action="external-out", implied_fee="0")
        events, _ = match_transfers([out_tx], overrides=[ov])

        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert fee_events == []

    def test_external_fee_disposal_cad_proceeds_computed(self) -> None:
        """CAD proceeds on the fee disposal = fee_amount * (cad_value / amount)."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        # rate = 500 / 0.01 = 50000 CAD/BTC; fee = 0.0001; proceeds = 5 CAD
        ov = _make_override(1, None, action="external-out", implied_fee="0.0001")
        events, _ = match_transfers([out_tx], overrides=[ov])

        fee_evt = next(e for e in events if e.event_type == "fee_disposal")
        assert fee_evt.cad_proceeds == Decimal("5")

    def test_external_event_itself_is_non_taxable(self) -> None:
        """The external marker event remains non-taxable even when a fee is present."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        ov = _make_override(1, None, action="external-out", implied_fee="0.00005")
        events, _ = match_transfers([out_tx], overrides=[ov])

        ext_events = [e for e in events if e.event_type == "external"]
        assert len(ext_events) == 1
        assert ext_events[0].is_taxable is False

    def test_external_out_no_implied_fee_falls_back_to_fee_crypto(self) -> None:
        """When implied_fee_crypto=0 but tx.fee_crypto > 0, fee_crypto is used."""
        # Simulate a Sparrow withdrawal marked external-out where user chose [N]
        # in the transfer wizard but the transaction already carries a fee.
        out_tx = _tx(
            1,
            TransactionType.WITHDRAWAL,
            amount="0.5005",
            cad_value="30030",
            fee_crypto="0.0005",
        )
        ov = _make_override(1, None, action="external-out", implied_fee="0")
        events, _ = match_transfers([out_tx], overrides=[ov])

        fee_events = [e for e in events if e.event_type == "fee_disposal"]
        assert len(fee_events) == 1
        assert fee_events[0].amount == Decimal("0.0005")
        assert fee_events[0].is_taxable is True
        # cad_proceeds = 0.0005 * (30030 / 0.5005) ≈ 30
        assert fee_events[0].cad_proceeds == pytest.approx(Decimal("30"), rel=Decimal("0.01"))

    def test_external_out_no_implied_fee_no_fee_crypto_no_disposal(self) -> None:
        """When both implied_fee_crypto and tx.fee_crypto are zero, no fee_disposal emitted."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        ov = _make_override(1, None, action="external-out", implied_fee="0")
        events, _ = match_transfers([out_tx], overrides=[ov])

        assert [e for e in events if e.event_type == "fee_disposal"] == []


class TestClassifyDisposalFeeCadDerivation:
    """Unmatched withdrawal: cad_fee derived from fee_crypto when fee_cad is absent."""

    def test_unmatched_withdrawal_fee_crypto_sets_cad_fee(self) -> None:
        """Sparrow-style: fee_cad=0 but fee_crypto > 0 → cad_fee derived on sell event."""
        # rate = 30030 / 0.5005 ≈ 60000 CAD/BTC; fee = 0.0005 BTC → cad_fee ≈ 30
        out_tx = _tx(
            1,
            TransactionType.WITHDRAWAL,
            amount="0.5005",
            cad_value="30030",
            fee_crypto="0.0005",
            fee_cad="0",
        )
        events, _ = match_transfers([out_tx])

        sell_events = [e for e in events if e.event_type == "sell"]
        assert len(sell_events) == 1
        assert sell_events[0].cad_fee is not None
        assert sell_events[0].cad_fee > Decimal("0")

    def test_unmatched_withdrawal_fee_cad_takes_precedence(self) -> None:
        """When fee_cad is provided it is used verbatim; fee_crypto is ignored."""
        out_tx = _tx(
            1,
            TransactionType.WITHDRAWAL,
            amount="0.5005",
            cad_value="30030",
            fee_crypto="0.0005",
            fee_cad="25",
        )
        events, _ = match_transfers([out_tx])

        sell_events = [e for e in events if e.event_type == "sell"]
        assert len(sell_events) == 1
        assert sell_events[0].cad_fee == Decimal("25")

    def test_unmatched_withdrawal_no_fee_no_cad_fee(self) -> None:
        """When both fee_crypto and fee_cad are zero, cad_fee on sell is None."""
        out_tx = _tx(1, TransactionType.WITHDRAWAL, amount="0.01", cad_value="500")
        events, _ = match_transfers([out_tx])

        sell_events = [e for e in events if e.event_type == "sell"]
        assert len(sell_events) == 1
        assert sell_events[0].cad_fee is None
