"""Tests for the NDAX ledger CSV importer.

Primary fixture: ``tests/fixtures/ndax_synthetic_ledger_v1.csv``
Direction fixture: ``tests/fixtures/ndax_buy_sell_direction.csv``

No real personal data; all amounts and IDs are synthetic.

The "direction" fixture was added after a production bug where the importer
stripped the sign from TRADE AMOUNT rows and always emitted ``"trade"`` as
the transaction_type — causing every sell to be treated as a buy.  The tests
in the "Trade direction" section are the direct regression guard for that bug.

The synthetic ledger contains ETH and SOL rows.  sirop is BTC-only, so all
non-BTC crypto rows are filtered at parse time and must not appear in the
output (covered in the "BTC-only filter" section below).
"""

import logging
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
DIRECTION_CSV = FIXTURES_DIR / "ndax_buy_sell_direction.csv"
NDAX_YAML = Path(__file__).parent.parent / "config" / "importers" / "ndax.yaml"


@pytest.fixture(scope="module")
def importer() -> NDAXImporter:
    return NDAXImporter.from_yaml(NDAX_YAML)


@pytest.fixture(scope="module")
def transactions(importer: NDAXImporter) -> list[RawTransaction]:
    return importer.parse(NDAX_CSV)


@pytest.fixture(scope="module")
def direction_txs(importer: NDAXImporter) -> list[RawTransaction]:
    """Minimal fixture: one buy trade, one sell trade, one fiat deposit."""
    return importer.parse(DIRECTION_CSV)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find(
    txs: list[RawTransaction], tx_type: str, asset: str | None = None
) -> list[RawTransaction]:
    return [t for t in txs if t.transaction_type == tx_type and (asset is None or t.asset == asset)]


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
# BTC-only filter
# ---------------------------------------------------------------------------


def test_no_eth_in_output(transactions: list[RawTransaction]) -> None:
    """All ETH rows in the fixture must be filtered — sirop is BTC-only."""
    eth_txs = [t for t in transactions if t.asset == "ETH"]
    assert eth_txs == [], f"ETH rows leaked through filter: {eth_txs}"


def test_no_sol_in_output(transactions: list[RawTransaction]) -> None:
    """All SOL rows in the fixture must be filtered — sirop is BTC-only."""
    sol_txs = [t for t in transactions if t.asset == "SOL"]
    assert sol_txs == [], f"SOL rows leaked through filter: {sol_txs}"


def test_non_btc_filter_emits_warning(
    importer: NDAXImporter, caplog: pytest.LogCaptureFixture
) -> None:
    """Parsing a CSV with non-BTC rows must emit one warning per skipped asset."""
    with caplog.at_level(logging.WARNING, logger="sirop.importers.ndax"):
        importer.parse(NDAX_CSV)

    warned_assets = {
        record.message for record in caplog.records if "sirop is BTC-only" in record.message
    }
    # The fixture contains both ETH and SOL rows — expect a warning for each.
    assert any("ETH" in msg for msg in warned_assets), "No warning emitted for ETH rows"
    assert any("SOL" in msg for msg in warned_assets), "No warning emitted for SOL rows"


# ---------------------------------------------------------------------------
# Trade direction — regression guard for buy/sell confusion bug
#
# NDAX uses a signed AMOUNT column: positive = received (buy), negative =
# sent (sell).  The importer must emit "buy" or "sell" based on that sign,
# not a generic "trade" type.  These tests pin that behaviour directly so
# that reverting _parse_trade() would immediately fail here.
# ---------------------------------------------------------------------------


def test_no_trade_type_in_output(transactions: list[RawTransaction]) -> None:
    """Regression: the importer must never emit transaction_type=="trade".

    TRADE rows are resolved to "buy" or "sell" based on the AMOUNT sign.
    A "trade" output means direction detection has been lost.
    """
    trade_typed = [t for t in transactions if t.transaction_type == "trade"]
    assert trade_typed == [], (
        f"Found {len(trade_typed)} transaction(s) still typed as 'trade' — "
        "buy/sell direction was not resolved."
    )


def test_positive_amount_trade_becomes_buy(direction_txs: list[RawTransaction]) -> None:
    """A TRADE row where the crypto AMOUNT is positive must become type 'buy'.

    Fixture TX 10002: BTC AMOUNT=+0.000999 → buying BTC with CAD.
    """
    btc_buys = _find(direction_txs, "buy", "BTC")
    assert len(btc_buys) == 1, f"Expected 1 BTC buy, got {len(btc_buys)}"
    tx = btc_buys[0]
    assert tx.amount == Decimal("0.000999")
    assert tx.fiat_value == Decimal("99.9")
    assert tx.fiat_currency == "CAD"


def test_negative_amount_trade_becomes_sell(direction_txs: list[RawTransaction]) -> None:
    """A TRADE row where the crypto AMOUNT is negative must become type 'sell'.

    Fixture TX 10003: BTC AMOUNT=-0.000999 → selling BTC for CAD.
    This is the exact scenario that triggered the production bug where both
    legs were classified as buys and no disposition was produced.
    """
    btc_sells = _find(direction_txs, "sell", "BTC")
    assert len(btc_sells) == 1, f"Expected 1 BTC sell, got {len(btc_sells)}"
    tx = btc_sells[0]
    assert tx.amount == Decimal("0.000999")  # sign stripped, magnitude preserved
    assert tx.fiat_value == Decimal("149.85")  # CAD received
    assert tx.fiat_currency == "CAD"


def test_sell_trade_fiat_value_is_proceeds(direction_txs: list[RawTransaction]) -> None:
    """fiat_value on a sell trade is the CAD amount received (proceeds)."""
    tx = _find(direction_txs, "sell", "BTC")[0]
    assert tx.fiat_value == Decimal("149.85")


def test_buy_trade_fiat_value_is_cost(direction_txs: list[RawTransaction]) -> None:
    """fiat_value on a buy trade is the CAD amount spent (cost)."""
    tx = _find(direction_txs, "buy", "BTC")[0]
    assert tx.fiat_value == Decimal("99.9")


def test_buy_fee_captured(direction_txs: list[RawTransaction]) -> None:
    """The TRADE/FEE row on the buy is captured as fee_amount."""
    tx = _find(direction_txs, "buy", "BTC")[0]
    assert tx.fee_amount == Decimal("0.1")
    assert tx.fee_currency == "CAD"


def test_sell_no_fee(direction_txs: list[RawTransaction]) -> None:
    """The sell in the direction fixture has no FEE row."""
    tx = _find(direction_txs, "sell", "BTC")[0]
    assert tx.fee_amount is None


def test_direction_rate_is_consistent(direction_txs: list[RawTransaction]) -> None:
    """rate == fiat_value / amount for both the buy and the sell."""
    for tx in direction_txs:
        if tx.transaction_type not in {"buy", "sell"} or tx.fiat_value is None:
            continue
        assert tx.rate is not None
        expected = tx.fiat_value / tx.amount
        assert abs(tx.rate - expected) < Decimal(
            "0.01"
        ), f"{tx.transaction_type} rate mismatch: {tx.rate} vs {expected}"


def test_no_trade_type_in_direction_fixture(direction_txs: list[RawTransaction]) -> None:
    """None of the TRADE rows in the direction fixture should remain as 'trade'."""
    trade_typed = [t for t in direction_txs if t.transaction_type == "trade"]
    assert trade_typed == []


# ---------------------------------------------------------------------------
# BTC buys (from main synthetic fixture)
# ---------------------------------------------------------------------------


def test_btc_buys_present(transactions: list[RawTransaction]) -> None:
    buys = _find(transactions, "buy", "BTC")
    assert len(buys) > 0


def test_btc_buy_has_positive_amount(transactions: list[RawTransaction]) -> None:
    """Buy amounts are always positive."""
    for tx in _find(transactions, "buy", "BTC"):
        assert tx.amount > Decimal("0")


def test_btc_buy_has_fiat_value(transactions: list[RawTransaction]) -> None:
    """TRADE-originated buys always carry a fiat value from the matching CAD row.
    Dust conversion buys have no fiat_value at import time — BoC rate is applied
    by the normalizer."""
    for tx in _find(transactions, "buy", "BTC"):
        if tx.raw_type == "dust / in":
            assert tx.fiat_value is None
        else:
            assert tx.fiat_value is not None
            assert tx.fiat_value > Decimal("0")
            assert tx.fiat_currency == "CAD"


def test_btc_buy_rate_is_consistent(transactions: list[RawTransaction]) -> None:
    """rate == fiat_value / amount for every TRADE-originated BTC buy.
    Dust conversion buys have no embedded rate; the normalizer applies the BoC rate."""
    for tx in _find(transactions, "buy", "BTC"):
        if tx.raw_type == "dust / in":
            assert tx.rate is None
            continue
        assert tx.rate is not None
        assert tx.fiat_value is not None
        expected_rate = tx.fiat_value / tx.amount
        assert abs(tx.rate - expected_rate) < Decimal(
            "0.01"
        ), f"Rate mismatch on {tx.timestamp}: {tx.rate} vs {expected_rate}"


def test_earliest_btc_buy(transactions: list[RawTransaction]) -> None:
    """
    Earliest BTC buy in the fixture:
      2025-02-27 15:48:04 UTC
      BTC TRADE  +0.0004  (positive = buying BTC)
      CAD TRADE  -49.676  (CAD spent)
      BTC TRADE/FEE  0.0  (zero fee)
    """
    buys = _find(transactions, "buy", "BTC")
    earliest = min(buys, key=lambda t: t.timestamp)
    assert earliest.timestamp.replace(microsecond=0) == datetime(2025, 2, 27, 15, 48, 4, tzinfo=UTC)
    assert earliest.amount == Decimal("0.0004")
    assert earliest.fiat_value == Decimal("49.676")
    assert earliest.fee_amount is None  # zero-amount fee row → None


# ---------------------------------------------------------------------------
# BTC withdrawals
# ---------------------------------------------------------------------------


def test_btc_withdrawals_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "withdrawal", "BTC")) > 0


def test_btc_withdrawal_fields(transactions: list[RawTransaction]) -> None:
    for tx in _find(transactions, "withdrawal", "BTC"):
        assert tx.asset == "BTC"
        assert tx.amount > Decimal("0")
        assert tx.fiat_value is None  # no fiat value at raw stage


# ---------------------------------------------------------------------------
# Dust conversions
#
# With BTC-only filtering, only the BTC (received) leg of a DUST group
# survives.  The non-BTC sent leg is filtered out.
# ---------------------------------------------------------------------------


def test_dust_conversions_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "buy")) > 0


def test_dust_received_asset_is_positive(transactions: list[RawTransaction]) -> None:
    for tx in _find(transactions, "buy"):
        assert tx.amount > Decimal("0")


def test_dust_group_btc_only_emits_one_transaction(
    transactions: list[RawTransaction],
) -> None:
    """BTC/ETH DUST group (TX 10010) emits only the BTC acquisition after filtering.

    Fixture TX 10010:
      BTC  DUST / IN   +0.00000010  (buying BTC) ← kept
      ETH  DUST / OUT  -0.00000043  (disposing ETH) ← filtered (non-BTC)

    Only the BTC buy survives — the ETH disposal is silently dropped.
    """
    dust_ts = datetime(2025, 4, 15, 16, 45, 0, tzinfo=UTC)
    dust_txs = [t for t in transactions if t.timestamp.replace(microsecond=0) == dust_ts]
    assert (
        len(dust_txs) == 1
    ), f"Expected 1 transaction for BTC/ETH DUST group (ETH filtered), got {len(dust_txs)}"
    assert dust_txs[0].asset == "BTC"
    assert dust_txs[0].amount == Decimal("0.00000010")


def test_dust_group_with_fee_leg_emits_one_transaction(
    transactions: list[RawTransaction],
) -> None:
    """BTC/SOL DUST group (TX 10011) emits only the BTC acquisition after filtering.

    Fixture TX 10011:
      BTC  DUST / IN   +0.00001691  (BTC received) ← kept
      SOL  DUST / OUT  -0.011182    (SOL disposed) ← filtered (non-BTC)
      BTC  DUST / FEE  -0.00000034  (fee on received leg)
    """
    dust_ts = datetime(2025, 5, 1, 10, 0, 0, tzinfo=UTC)
    dust_txs = [t for t in transactions if t.timestamp.replace(microsecond=0) == dust_ts]
    assert (
        len(dust_txs) == 1
    ), f"Expected 1 transaction for BTC/SOL DUST group (SOL filtered), got {len(dust_txs)}"


def test_dust_group_with_fee_leg_fee_on_received_tx(
    transactions: list[RawTransaction],
) -> None:
    """DUST/FEE row must be captured on the received-asset (BTC) transaction."""
    dust_ts = datetime(2025, 5, 1, 10, 0, 0, tzinfo=UTC)
    btc_tx = next(
        (
            t
            for t in transactions
            if t.timestamp.replace(microsecond=0) == dust_ts and t.asset == "BTC"
        ),
        None,
    )
    assert btc_tx is not None
    assert btc_tx.fee_amount == Decimal("0.00000034")
    assert btc_tx.fee_currency == "BTC"


# ---------------------------------------------------------------------------
# Zero-amount rows (skipped by importer)
# ---------------------------------------------------------------------------


def test_zero_amount_rows_not_emitted(transactions: list[RawTransaction]) -> None:
    """Fixture TX 10016 is a SOL DEPOSIT with AMOUNT=0 — must be silently skipped.

    The importer returns None for zero-amount rows in _parse_single().
    Even if it weren't zero-amount, it would also be filtered by the BTC-only filter.
    """
    zero_amount = [t for t in transactions if t.amount == Decimal("0")]
    assert (
        zero_amount == []
    ), f"Found {len(zero_amount)} zero-amount transaction(s) — should be skipped"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_file_not_found_raises(importer: NDAXImporter) -> None:
    with pytest.raises(InvalidCSVFormatError, match="not found"):
        importer.parse(Path("/nonexistent/path/ledger.csv"))


def test_missing_column_raises(importer: NDAXImporter, tmp_path: Path) -> None:
    """A CSV missing the ASSET column should raise MissingColumnError."""
    bad_csv = tmp_path / "bad.csv"
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
