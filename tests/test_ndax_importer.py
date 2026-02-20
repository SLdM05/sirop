"""Tests for the NDAX ledger CSV importer.

All assertions are derived from ``tests/fixtures/ndax_synthetic_ledger_v1.csv``.
No real personal data; amounts and IDs are synthetic.
"""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from sirop.importers.base import InvalidCSVFormatError, MissingColumnError
from sirop.importers.ndax import NDAXImporter
from sirop.models.raw import RawTransaction

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
NDAX_CSV = FIXTURES_DIR / "ndax_synthetic_ledger_v1.csv"
NDAX_YAML = Path(__file__).parent.parent / "config" / "importers" / "ndax.yaml"


@pytest.fixture(scope="module")
def importer() -> NDAXImporter:
    return NDAXImporter.from_yaml(NDAX_YAML)


@pytest.fixture(scope="module")
def transactions(importer: NDAXImporter) -> list[RawTransaction]:
    return importer.parse(NDAX_CSV)


# ---------------------------------------------------------------------------
# Basic sanity
# ---------------------------------------------------------------------------


def test_parse_returns_list(transactions: list[RawTransaction]) -> None:
    assert isinstance(transactions, list)


def test_results_non_empty(transactions: list[RawTransaction]) -> None:
    assert len(transactions) > 0


def test_results_sorted_chronologically(transactions: list[RawTransaction]) -> None:
    timestamps = [t.timestamp for t in transactions]
    assert timestamps == sorted(timestamps)


def test_source_is_ndax(transactions: list[RawTransaction]) -> None:
    assert all(t.source == "ndax" for t in transactions)


def test_all_timestamps_are_utc(transactions: list[RawTransaction]) -> None:
    for tx in transactions:
        assert tx.timestamp.tzinfo is not None
        assert tx.timestamp.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_all_amounts_are_positive(transactions: list[RawTransaction]) -> None:
    """Importer must strip the sign from AMOUNT; amounts are always positive."""
    for tx in transactions:
        assert tx.amount >= Decimal("0"), f"Negative amount on {tx}"


# ---------------------------------------------------------------------------
# CAD deposits
# ---------------------------------------------------------------------------


def _find(
    txs: list[RawTransaction], tx_type: str, asset: str | None = None
) -> list[RawTransaction]:
    return [t for t in txs if t.transaction_type == tx_type and (asset is None or t.asset == asset)]


def test_cad_deposits_present(transactions: list[RawTransaction]) -> None:
    deposits = _find(transactions, "fiat_deposit", "CAD")
    assert len(deposits) > 0


def test_cad_deposit_fields(transactions: list[RawTransaction]) -> None:
    deposits = _find(transactions, "fiat_deposit", "CAD")
    # Spot-check the earliest deposit: 2025-02-27 15:46:17 UTC — 50 CAD
    earliest = min(deposits, key=lambda t: t.timestamp)
    assert earliest.amount == Decimal("50")
    assert earliest.fiat_value == Decimal("50")
    assert earliest.fiat_currency == "CAD"
    assert earliest.fee_amount is None


# ---------------------------------------------------------------------------
# BTC trades (buy / sell)
# ---------------------------------------------------------------------------


def test_btc_trades_present(transactions: list[RawTransaction]) -> None:
    trades = _find(transactions, "trade", "BTC")
    assert len(trades) > 0


def test_btc_buy_has_positive_amount(transactions: list[RawTransaction]) -> None:
    """After sign-stripping, every BTC trade amount is positive."""
    trades = _find(transactions, "trade", "BTC")
    for tx in trades:
        assert tx.amount > Decimal("0")


def test_btc_buy_has_fiat_value(transactions: list[RawTransaction]) -> None:
    trades = _find(transactions, "trade", "BTC")
    for tx in trades:
        assert tx.fiat_value is not None
        assert tx.fiat_value > Decimal("0")
        assert tx.fiat_currency == "CAD"


def test_btc_buy_rate_is_consistent(transactions: list[RawTransaction]) -> None:
    """rate ≈ fiat_value / amount for every BTC trade."""
    trades = _find(transactions, "trade", "BTC")
    for tx in trades:
        assert tx.rate is not None
        assert tx.fiat_value is not None
        expected_rate = tx.fiat_value / tx.amount
        assert abs(tx.rate - expected_rate) < Decimal("0.01"), (
            f"Rate mismatch on {tx.timestamp}: {tx.rate} vs {expected_rate}"
        )


def test_earliest_btc_buy(transactions: list[RawTransaction]) -> None:
    """
    Earliest BTC trade in the fixture:
      2025-02-27 15:48:04 UTC
      BTC TRADE  +0.0004   (received)
      CAD TRADE  -49.676   (spent)
      BTC TRADE/FEE  0.0   (fee = 0)
    """
    trades = _find(transactions, "trade", "BTC")
    earliest = min(trades, key=lambda t: t.timestamp)
    # Timestamps preserve milliseconds from the CSV; compare at second precision.
    assert earliest.timestamp.replace(microsecond=0) == datetime(2025, 2, 27, 15, 48, 4, tzinfo=UTC)
    assert earliest.amount == Decimal("0.0004")
    assert earliest.fiat_value == Decimal("49.676")
    # Fee row amount is 0, so fee_amount should be None
    assert earliest.fee_amount is None


# ---------------------------------------------------------------------------
# BTC withdrawals
# ---------------------------------------------------------------------------


def test_btc_withdrawals_present(transactions: list[RawTransaction]) -> None:
    withdrawals = _find(transactions, "withdrawal", "BTC")
    assert len(withdrawals) > 0


def test_btc_withdrawal_fields(transactions: list[RawTransaction]) -> None:
    withdrawals = _find(transactions, "withdrawal", "BTC")
    for tx in withdrawals:
        assert tx.asset == "BTC"
        assert tx.amount > Decimal("0")
        assert tx.fiat_value is None  # no fiat value at raw stage


# ---------------------------------------------------------------------------
# ETH buy
# ---------------------------------------------------------------------------


def test_eth_buy_present(transactions: list[RawTransaction]) -> None:
    trades = _find(transactions, "trade", "ETH")
    assert len(trades) > 0


def test_eth_buy_fee_captured(transactions: list[RawTransaction]) -> None:
    """The 2025-03-06 ETH buy has a TRADE/FEE row with -0.000007 ETH."""
    trades = _find(transactions, "trade", "ETH")
    # Find the ETH buy (positive ETH from a TRADE, not a staking row)
    eth_buys = [t for t in trades if t.fiat_value is not None]
    assert len(eth_buys) > 0
    # At least one ETH trade should carry a fee
    fees = [t.fee_amount for t in eth_buys if t.fee_amount is not None]
    assert len(fees) > 0


# ---------------------------------------------------------------------------
# SOL trade (crypto sold for CAD)
# ---------------------------------------------------------------------------


def test_sol_sell_present(transactions: list[RawTransaction]) -> None:
    trades = _find(transactions, "trade", "SOL")
    assert len(trades) > 0


def test_sol_sell_fiat_fee(transactions: list[RawTransaction]) -> None:
    """
    The SOL→CAD trade at 2025-04-01 00:25:07 has a CAD TRADE/FEE row.
    fee_amount should be Decimal("0.1184436"), fee_currency "CAD".
    """
    trades = _find(transactions, "trade", "SOL")
    # The sell has a CAD fee; the buy has a SOL fee.  Find the one with CAD fee.
    sol_sell = [t for t in trades if t.fee_currency == "CAD"]
    assert len(sol_sell) > 0, "Expected at least one SOL trade with a CAD fee"
    tx = sol_sell[0]
    assert tx.fee_amount == Decimal("0.1184436")
    assert tx.fee_currency == "CAD"


# ---------------------------------------------------------------------------
# Staking events
# ---------------------------------------------------------------------------


def test_staking_reward_present(transactions: list[RawTransaction]) -> None:
    rewards = _find(transactions, "income")
    assert len(rewards) > 0


def test_staking_reward_fields(transactions: list[RawTransaction]) -> None:
    rewards = _find(transactions, "income")
    for tx in rewards:
        assert tx.amount > Decimal("0")
        assert tx.fiat_value is None  # normalizer handles fiat conversion


# ---------------------------------------------------------------------------
# Dust conversions
# ---------------------------------------------------------------------------


def test_dust_conversions_present(transactions: list[RawTransaction]) -> None:
    dust = _find(transactions, "other")
    assert len(dust) > 0


def test_dust_received_asset_is_positive(transactions: list[RawTransaction]) -> None:
    dust = _find(transactions, "other")
    for tx in dust:
        assert tx.amount > Decimal("0")


# ---------------------------------------------------------------------------
# ETH / SOL withdrawals
# ---------------------------------------------------------------------------


def test_eth_withdrawal_present(transactions: list[RawTransaction]) -> None:
    withdrawals = _find(transactions, "withdrawal", "ETH")
    assert len(withdrawals) > 0


def test_eth_withdrawal_has_fee(transactions: list[RawTransaction]) -> None:
    """ETH withdrawals have a WITHDRAW/FEE row; fee_amount should be set."""
    withdrawals = _find(transactions, "withdrawal", "ETH")
    # At least one should carry a fee
    with_fee = [t for t in withdrawals if t.fee_amount is not None]
    assert len(with_fee) > 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_file_not_found_raises(importer: NDAXImporter) -> None:
    with pytest.raises(InvalidCSVFormatError, match="not found"):
        importer.parse(Path("/nonexistent/path/ledger.csv"))


def test_missing_column_raises(importer: NDAXImporter, tmp_path: Path) -> None:
    """A CSV missing the ASSET column should raise MissingColumnError."""
    bad_csv = tmp_path / "bad.csv"
    # Include a data row so the importer reaches the column-validation step.
    bad_csv.write_text(
        "ASSET_CLASS,AMOUNT,BALANCE,TYPE,TX_ID,DATE\n"
        "FIAT,50,50,DEPOSIT,99999,2025-03-01T10:00:00.000Z\n"
    )
    with pytest.raises(MissingColumnError):
        importer.parse(bad_csv)


def test_empty_csv_returns_empty_list(importer: NDAXImporter, tmp_path: Path) -> None:
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("ASSET,ASSET_CLASS,AMOUNT,BALANCE,TYPE,TX_ID,DATE\n")
    result = importer.parse(empty_csv)
    assert result == []


def test_blank_rows_are_skipped(importer: NDAXImporter, tmp_path: Path) -> None:
    """The NDAX export pads to a fixed size with blank rows; they must be dropped."""
    csv_content = (
        "ASSET,ASSET_CLASS,AMOUNT,BALANCE,TYPE,TX_ID,DATE\n"
        "CAD,FIAT,50,50,DEPOSIT,99999,2025-03-01T10:00:00.000Z\n"
        ",,,,,,\n"
        ",,,,,,\n"
    )
    blank_csv = tmp_path / "blanks.csv"
    blank_csv.write_text(csv_content)
    result = importer.parse(blank_csv)
    assert len(result) == 1
    assert result[0].transaction_type == "fiat_deposit"
