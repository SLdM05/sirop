"""Tests for the Sparrow Wallet importer.

Fixture summary
---------------
sparrow_btc.csv   -- BTC-unit export; 1 unconfirmed row + 3 confirmed rows
                     (1 receive, 1 send with fee, 1 receive with no fee)
sparrow_sats.csv  -- Satoshi-unit export; same 3 confirmed transactions
                     (no unconfirmed row; values are integer satoshis)
sparrow_fiat.csv  -- BTC-unit export with optional "Value (CAD)" column
                     and Sparrow's trailing footer comment line

All txids are 64-char synthetic hex strings (no real transaction data).
"""

from decimal import Decimal
from pathlib import Path

import pytest

from sirop.importers.base import MissingColumnError
from sirop.importers.sparrow import SparrowImporter
from sirop.models.raw import RawTransaction

SPARROW_YAML = Path("config/importers/sparrow.yaml")
SPARROW_BTC_CSV = Path("tests/fixtures/sparrow_btc.csv")
SPARROW_SATS_CSV = Path("tests/fixtures/sparrow_sats.csv")
SPARROW_FIAT_CSV = Path("tests/fixtures/sparrow_fiat.csv")

# Synthetic txids used in fixtures — exactly 64 hex characters each.
TXID_A = "a" * 64  # unconfirmed row (must NOT appear in results)
TXID_B = "b" * 64  # deposit row 1
TXID_C = "c" * 64  # withdrawal row
TXID_D = "d" * 64  # deposit row 2

# Named counts matching the synthetic fixture content.
_BTC_CONFIRMED_COUNT = 3
_BTC_DEPOSIT_COUNT = 2
_BTC_WITHDRAWAL_COUNT = 1
_FIAT_ROW_COUNT = 2
_TXID_LENGTH = 64


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def importer() -> SparrowImporter:
    return SparrowImporter.from_yaml(SPARROW_YAML)


@pytest.fixture(scope="module")
def btc_txs(importer: SparrowImporter) -> list[RawTransaction]:
    return importer.parse(SPARROW_BTC_CSV)


@pytest.fixture(scope="module")
def sats_txs(importer: SparrowImporter) -> list[RawTransaction]:
    return importer.parse(SPARROW_SATS_CSV)


@pytest.fixture(scope="module")
def fiat_txs(importer: SparrowImporter) -> list[RawTransaction]:
    return importer.parse(SPARROW_FIAT_CSV)


# ---------------------------------------------------------------------------
# Basic sanity
# ---------------------------------------------------------------------------


def test_parse_returns_list(btc_txs: list[RawTransaction]) -> None:
    assert isinstance(btc_txs, list)


def test_source_is_sparrow(btc_txs: list[RawTransaction]) -> None:
    for tx in btc_txs:
        assert tx.source == "sparrow"


def test_asset_is_btc(btc_txs: list[RawTransaction], sats_txs: list[RawTransaction]) -> None:
    for tx in [*btc_txs, *sats_txs]:
        assert tx.asset == "BTC"
        assert tx.amount_currency == "BTC"


def test_rate_and_spot_rate_are_none(btc_txs: list[RawTransaction]) -> None:
    """Sparrow carries no exchange-rate data."""
    for tx in btc_txs:
        assert tx.rate is None
        assert tx.spot_rate is None


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


def test_all_timestamps_utc(btc_txs: list[RawTransaction], sats_txs: list[RawTransaction]) -> None:
    for tx in [*btc_txs, *sats_txs]:
        assert tx.timestamp.tzinfo is not None
        offset = tx.timestamp.utcoffset()
        assert offset is not None
        assert offset.total_seconds() == 0


def test_sorted_chronologically(
    btc_txs: list[RawTransaction], sats_txs: list[RawTransaction]
) -> None:
    for txs in (btc_txs, sats_txs):
        timestamps = [tx.timestamp for tx in txs]
        assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# Unconfirmed handling
# ---------------------------------------------------------------------------


def test_unconfirmed_row_excluded(btc_txs: list[RawTransaction]) -> None:
    """BTC fixture has 1 unconfirmed row; result must contain only the confirmed rows."""
    assert len(btc_txs) == _BTC_CONFIRMED_COUNT


def test_unconfirmed_txid_absent(btc_txs: list[RawTransaction]) -> None:
    """The unconfirmed row's txid (64 'a's) must not appear in results."""
    txids = {tx.txid for tx in btc_txs}
    assert TXID_A not in txids


# ---------------------------------------------------------------------------
# Unit detection — BTC mode
# ---------------------------------------------------------------------------


def test_btc_deposit_amount(btc_txs: list[RawTransaction]) -> None:
    """BTC-mode CSV value 0.00150000 must be stored as BTC decimal."""
    deposit = next(tx for tx in btc_txs if tx.txid == TXID_B)
    assert deposit.amount == Decimal("0.00150000")


def test_btc_withdrawal_amount(btc_txs: list[RawTransaction]) -> None:
    """BTC-mode CSV value 0.00050000 must be stored as BTC decimal."""
    withdrawal = next(tx for tx in btc_txs if tx.txid == TXID_C)
    assert withdrawal.amount == Decimal("0.00050000")


def test_btc_fee(btc_txs: list[RawTransaction]) -> None:
    """BTC-mode fee 0.00001000 must be stored as BTC decimal."""
    withdrawal = next(tx for tx in btc_txs if tx.txid == TXID_C)
    assert withdrawal.fee_amount == Decimal("0.00001000")


def test_all_amounts_positive(
    btc_txs: list[RawTransaction], sats_txs: list[RawTransaction]
) -> None:
    """Importer must strip the sign from Value; amounts are always positive."""
    for tx in [*btc_txs, *sats_txs]:
        assert tx.amount >= Decimal("0")


# ---------------------------------------------------------------------------
# Unit detection — satoshi mode
# ---------------------------------------------------------------------------


def test_sats_deposit_amount(sats_txs: list[RawTransaction]) -> None:
    """Sats-mode CSV value 150000 must be divided by 10^8 to give BTC decimal."""
    deposit = next(tx for tx in sats_txs if tx.txid == TXID_B)
    assert deposit.amount == Decimal("0.00150000")


def test_sats_withdrawal_amount(sats_txs: list[RawTransaction]) -> None:
    """Sats-mode CSV value 50000 must be divided by 10^8 to give BTC decimal."""
    withdrawal = next(tx for tx in sats_txs if tx.txid == TXID_C)
    assert withdrawal.amount == Decimal("0.00050000")


def test_sats_fee(sats_txs: list[RawTransaction]) -> None:
    """Sats-mode fee 1000 must be divided by 10^8 to give BTC decimal."""
    withdrawal = next(tx for tx in sats_txs if tx.txid == TXID_C)
    assert withdrawal.fee_amount == Decimal("0.00001000")


def test_unit_detection_produces_identical_amounts(
    btc_txs: list[RawTransaction], sats_txs: list[RawTransaction]
) -> None:
    """BTC and sats fixtures represent the same transactions — amounts must match."""
    btc_amounts = sorted(tx.amount for tx in btc_txs)
    sats_amounts = sorted(tx.amount for tx in sats_txs)
    assert btc_amounts == sats_amounts


# ---------------------------------------------------------------------------
# Transaction type inference
# ---------------------------------------------------------------------------


def test_positive_value_becomes_deposit(btc_txs: list[RawTransaction]) -> None:
    deposits = [tx for tx in btc_txs if tx.transaction_type == "deposit"]
    assert len(deposits) == _BTC_DEPOSIT_COUNT


def test_negative_value_becomes_withdrawal(btc_txs: list[RawTransaction]) -> None:
    withdrawals = [tx for tx in btc_txs if tx.transaction_type == "withdrawal"]
    assert len(withdrawals) == _BTC_WITHDRAWAL_COUNT


def test_raw_type_matches_transaction_type(btc_txs: list[RawTransaction]) -> None:
    for tx in btc_txs:
        assert tx.raw_type == tx.transaction_type


# ---------------------------------------------------------------------------
# Fee handling
# ---------------------------------------------------------------------------


def test_fee_none_on_receives(btc_txs: list[RawTransaction]) -> None:
    """Receives have no fee column — must be None, not zero."""
    deposits = [tx for tx in btc_txs if tx.transaction_type == "deposit"]
    assert len(deposits) == _BTC_DEPOSIT_COUNT
    for tx in deposits:
        assert tx.fee_amount is None
        assert tx.fee_currency is None


def test_fee_present_on_send(btc_txs: list[RawTransaction]) -> None:
    withdrawal = next(tx for tx in btc_txs if tx.transaction_type == "withdrawal")
    assert withdrawal.fee_amount is not None
    assert withdrawal.fee_amount > Decimal("0")
    assert withdrawal.fee_currency == "BTC"


# ---------------------------------------------------------------------------
# Txid handling
# ---------------------------------------------------------------------------


def test_txid_preserved(btc_txs: list[RawTransaction]) -> None:
    for tx in btc_txs:
        assert tx.txid is not None
        assert len(tx.txid) == _TXID_LENGTH


def test_invalid_txid_cleared(importer: SparrowImporter, tmp_path: Path) -> None:
    """A non-hex txid must be cleared to None rather than discarding the row."""
    bad_csv = tmp_path / "bad_txid.csv"
    bad_csv.write_text(
        "Date (UTC),Label,Value,Balance,Fee,Txid\n"
        "2024-01-01 00:00:00,,150000,150000,,not-a-real-txid\n"
    )
    result = importer.parse(bad_csv)
    assert len(result) == 1
    assert result[0].txid is None
    assert result[0].amount == Decimal("0.00150000")  # row still parsed


# ---------------------------------------------------------------------------
# Fiat column handling
# ---------------------------------------------------------------------------


def test_fiat_value_always_none(fiat_txs: list[RawTransaction]) -> None:
    """CoinGecko rates must never reach the ACB engine."""
    for tx in fiat_txs:
        assert tx.fiat_value is None


def test_fiat_currency_always_none(fiat_txs: list[RawTransaction]) -> None:
    for tx in fiat_txs:
        assert tx.fiat_currency is None


def test_fiat_value_preserved_in_raw_row(fiat_txs: list[RawTransaction]) -> None:
    """Audit trail: raw CoinGecko value must be accessible in raw_row."""
    deposit = next(tx for tx in fiat_txs if tx.transaction_type == "deposit")
    assert "Value (CAD)" in deposit.raw_row
    assert deposit.raw_row["Value (CAD)"] == "8250.00"


def test_fiat_withdrawal_raw_row(fiat_txs: list[RawTransaction]) -> None:
    withdrawal = next(tx for tx in fiat_txs if tx.transaction_type == "withdrawal")
    assert withdrawal.raw_row.get("Value (CAD)") == "-2750.00"


def test_footer_comment_tolerated(fiat_txs: list[RawTransaction]) -> None:
    """Sparrow's trailing comment line must not cause an error or a spurious row."""
    assert len(fiat_txs) == _FIAT_ROW_COUNT


def test_fiat_amounts_match_btc_fixture(
    btc_txs: list[RawTransaction], fiat_txs: list[RawTransaction]
) -> None:
    """Fiat fixture uses same BTC values — satoshi amounts must match."""
    btc_amounts = sorted(tx.amount for tx in btc_txs if tx.txid in {TXID_B, TXID_C})
    fiat_amounts = sorted(tx.amount for tx in fiat_txs)
    assert btc_amounts == fiat_amounts


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_missing_column_raises(importer: SparrowImporter, tmp_path: Path) -> None:
    """CSV missing required columns must raise MissingColumnError."""
    bad_csv = tmp_path / "missing_cols.csv"
    bad_csv.write_text("Date (UTC),Label,Value\n2024-01-01 00:00:00,,150000\n")
    with pytest.raises(MissingColumnError):
        importer.parse(bad_csv)


def test_empty_file_returns_empty_list(importer: SparrowImporter, tmp_path: Path) -> None:
    """A header-only CSV (no data rows) must return an empty list."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("Date (UTC),Label,Value,Balance,Fee,Txid\n")
    assert importer.parse(empty_csv) == []


def test_unparseable_value_skipped(importer: SparrowImporter, tmp_path: Path) -> None:
    """A row with a non-numeric Value must be skipped; other rows must still parse."""
    csv_path = tmp_path / "bad_value.csv"
    csv_path.write_text(
        "Date (UTC),Label,Value,Balance,Fee,Txid\n"
        f"2024-01-01 00:00:00,,not-a-number,150000,,{TXID_B}\n"
        f"2024-01-02 00:00:00,,50000,200000,,{TXID_C}\n"
    )
    result = importer.parse(csv_path)
    assert len(result) == 1
    assert result[0].amount == Decimal("0.00050000")
