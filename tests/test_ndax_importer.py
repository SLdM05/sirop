"""Tests for the NDAX ledger CSV importer.

Primary fixture: ``tests/fixtures/ndax_synthetic_ledger_v1.csv``
Direction fixture: ``tests/fixtures/ndax_buy_sell_direction.csv``

No real personal data; all amounts and IDs are synthetic.

The "direction" fixture was added after a production bug where the importer
stripped the sign from TRADE AMOUNT rows and always emitted ``"trade"`` as
the transaction_type — causing every sell to be treated as a buy.  The tests
in the "Trade direction" section are the direct regression guard for that bug.
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
    for tx in _find(transactions, "buy", "BTC"):
        assert tx.fiat_value is not None
        assert tx.fiat_value > Decimal("0")
        assert tx.fiat_currency == "CAD"


def test_btc_buy_rate_is_consistent(transactions: list[RawTransaction]) -> None:
    """rate == fiat_value / amount for every BTC buy."""
    for tx in _find(transactions, "buy", "BTC"):
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
# ETH buy
# ---------------------------------------------------------------------------


def test_eth_buy_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "buy", "ETH")) > 0


def test_eth_buy_fee_captured(transactions: list[RawTransaction]) -> None:
    """The 2025-03-06 ETH buy has a TRADE/FEE row with -0.000007 ETH."""
    eth_buys = _find(transactions, "buy", "ETH")
    fees = [t.fee_amount for t in eth_buys if t.fee_amount is not None]
    assert len(fees) > 0


# ---------------------------------------------------------------------------
# ETH sell (negative AMOUNT TRADE → "sell")
# ---------------------------------------------------------------------------


def test_eth_sell_present(transactions: list[RawTransaction]) -> None:
    """The fixture contains at least one ETH→CAD sell (negative AMOUNT TRADE)."""
    assert len(_find(transactions, "sell", "ETH")) > 0


def test_eth_sell_known_values(transactions: list[RawTransaction]) -> None:
    """
    ETH sell at 2025-04-01 09:15:00 UTC:
      ETH TRADE  -0.3   (selling 0.3 ETH)
      CAD TRADE  +690   (receiving 690 CAD)
      CAD TRADE/FEE  -0.1184436
    """
    sells = _find(transactions, "sell", "ETH")
    target = datetime(2025, 4, 1, 9, 15, 0, tzinfo=UTC)
    tx = next(
        (t for t in sells if t.timestamp.replace(microsecond=0) == target),
        None,
    )
    assert tx is not None, "ETH sell on 2025-04-01 not found"
    assert tx.amount == Decimal("0.3")
    assert tx.fiat_value == Decimal("690.00")
    assert tx.fiat_currency == "CAD"
    assert tx.fee_amount == Decimal("0.1184436")
    assert tx.fee_currency == "CAD"


def test_eth_sell_type_is_sell_not_buy(transactions: list[RawTransaction]) -> None:
    """The ETH→CAD trade must be classified as 'sell', not 'buy'.

    Before the direction-detection fix this test would have failed because
    the ETH TRADE row had AMOUNT=-0.3 (negative = sending) but the importer
    emitted 'trade' (later routed to 'buy') for every TRADE regardless.
    """
    sells = _find(transactions, "sell", "ETH")
    assert len(sells) >= 1

    # Double-check the Apr 1 sell is not in the buy list.
    buys = _find(transactions, "buy", "ETH")
    sell_timestamps = {t.timestamp for t in sells}
    buy_timestamps = {t.timestamp for t in buys}
    assert sell_timestamps.isdisjoint(
        buy_timestamps
    ), "Same timestamp appears in both buy and sell ETH lists — direction detection broken"


# ---------------------------------------------------------------------------
# Staking events
# ---------------------------------------------------------------------------


def test_staking_reward_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "income")) > 0


def test_staking_reward_fields(transactions: list[RawTransaction]) -> None:
    for tx in _find(transactions, "income"):
        assert tx.amount > Decimal("0")
        assert tx.fiat_value is None  # normalizer handles fiat conversion


# ---------------------------------------------------------------------------
# Dust conversions
# ---------------------------------------------------------------------------


def test_dust_conversions_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "other")) > 0


def test_dust_received_asset_is_positive(transactions: list[RawTransaction]) -> None:
    for tx in _find(transactions, "other"):
        assert tx.amount > Decimal("0")


# ---------------------------------------------------------------------------
# ETH withdrawals
# ---------------------------------------------------------------------------


def test_eth_withdrawal_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "withdrawal", "ETH")) > 0


def test_eth_withdrawal_has_fee(transactions: list[RawTransaction]) -> None:
    """ETH withdrawals have a WITHDRAW/FEE row; fee_amount should be set."""
    withdrawals = _find(transactions, "withdrawal", "ETH")
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
