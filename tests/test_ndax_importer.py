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
NON_FIAT_CSV = FIXTURES_DIR / "ndax_non_fiat_trade.csv"
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


@pytest.fixture(scope="module")
def non_fiat_txs(importer: NDAXImporter) -> list[RawTransaction]:
    """Minimal fixture: one SOL→ETH non-fiat-to-non-fiat trade."""
    return importer.parse(NON_FIAT_CSV)


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
    assert len(_find(transactions, "buy")) > 0


def test_dust_received_asset_is_positive(transactions: list[RawTransaction]) -> None:
    for tx in _find(transactions, "buy"):
        assert tx.amount > Decimal("0")


def test_dust_group_emits_two_transactions(transactions: list[RawTransaction]) -> None:
    """DUST conversion must emit both the received-asset acquisition and the
    sent-asset disposal.

    Fixture TX 10010:
      BTC  DUST / IN   +0.00000010  (buying BTC)
      ETH  DUST / OUT  -0.00000043  (disposing ETH)

    Previously only the BTC buy was emitted; the ETH disposal was silently
    discarded, leaving ETH in the ACB pool forever.
    """
    dust_ts = datetime(2025, 4, 15, 16, 45, 0, tzinfo=UTC)
    dust_txs = [t for t in transactions if t.timestamp.replace(microsecond=0) == dust_ts]
    expected_count = 2
    assert (
        len(dust_txs) == expected_count
    ), f"Expected {expected_count} transactions for DUST group at {dust_ts}, got {len(dust_txs)}"

    btc_buy = next((t for t in dust_txs if t.asset == "BTC"), None)
    eth_sell = next((t for t in dust_txs if t.asset == "ETH"), None)

    assert btc_buy is not None, "BTC acquisition not emitted from DUST group"
    assert eth_sell is not None, "ETH disposal not emitted from DUST group"

    assert btc_buy.amount == Decimal("0.00000010")
    assert eth_sell.transaction_type == "sell"
    assert eth_sell.amount == Decimal("0.00000043")  # sign stripped


def test_dust_group_with_fee_leg_emits_two_transactions(
    transactions: list[RawTransaction],
) -> None:
    """3-leg DUST group (DUST/IN + DUST/OUT + DUST/FEE) must emit two transactions.

    Fixture TX 10011:
      BTC  DUST / IN   +0.00001691  (BTC received)
      SOL  DUST / OUT  -0.011182    (SOL disposed)
      BTC  DUST / FEE  -0.00000034  (fee on received leg)
    """
    dust_ts = datetime(2025, 5, 1, 10, 0, 0, tzinfo=UTC)
    dust_txs = [t for t in transactions if t.timestamp.replace(microsecond=0) == dust_ts]
    expected_count = 2
    assert (
        len(dust_txs) == expected_count
    ), f"Expected {expected_count} transactions for 3-leg DUST group, got {len(dust_txs)}"


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


def test_dust_group_with_fee_leg_sent_tx_no_fee(transactions: list[RawTransaction]) -> None:
    """The sent-asset (SOL) disposal from a DUST group carries no fee."""
    dust_ts = datetime(2025, 5, 1, 10, 0, 0, tzinfo=UTC)
    sol_tx = next(
        (
            t
            for t in transactions
            if t.timestamp.replace(microsecond=0) == dust_ts and t.asset == "SOL"
        ),
        None,
    )
    assert sol_tx is not None
    assert sol_tx.transaction_type == "sell"
    assert sol_tx.amount == Decimal("0.011182")
    assert sol_tx.fee_amount is None


# ---------------------------------------------------------------------------
# SOL deposit
# ---------------------------------------------------------------------------


def test_sol_deposit_present(transactions: list[RawTransaction]) -> None:
    """Fixture TX 10012: standalone SOL DEPOSIT row."""
    assert len(_find(transactions, "deposit", "SOL")) > 0


def test_sol_deposit_amount(transactions: list[RawTransaction]) -> None:
    sol_deposits = _find(transactions, "deposit", "SOL")
    assert len(sol_deposits) == 1
    tx = sol_deposits[0]
    assert tx.amount == Decimal("0.25")
    assert tx.fiat_value is None  # non-fiat deposit, no CAD value at raw stage


# ---------------------------------------------------------------------------
# SOL withdrawal
# ---------------------------------------------------------------------------


def test_sol_withdrawal_present(transactions: list[RawTransaction]) -> None:
    """Fixture TX 10013: SOL WITHDRAW with WITHDRAW/FEE."""
    assert len(_find(transactions, "withdrawal", "SOL")) > 0


def test_sol_withdrawal_has_fee(transactions: list[RawTransaction]) -> None:
    """SOL WITHDRAW/FEE row is captured as fee_amount on the withdrawal."""
    sol_withdrawals = _find(transactions, "withdrawal", "SOL")
    assert len(sol_withdrawals) == 1
    tx = sol_withdrawals[0]
    assert tx.fee_amount == Decimal("0.001")
    assert tx.fee_currency == "SOL"


# ---------------------------------------------------------------------------
# Staking subtypes
# ---------------------------------------------------------------------------


def test_staking_deposit_maps_to_transfer_out(transactions: list[RawTransaction]) -> None:
    """STAKING / DEPOSIT → transaction_type == 'transfer_out' (ETH sent to staking contract)."""
    transfer_outs = _find(transactions, "transfer_out", "ETH")
    assert len(transfer_outs) > 0


def test_staking_refund_maps_to_transfer_in(transactions: list[RawTransaction]) -> None:
    """STAKING / REFUND → transaction_type == 'transfer_in' (ETH returned from staking)."""
    transfer_ins = _find(transactions, "transfer_in", "ETH")
    assert len(transfer_ins) > 0


# ---------------------------------------------------------------------------
# Zero-amount rows (skipped by importer)
# ---------------------------------------------------------------------------


def test_zero_amount_rows_not_emitted(transactions: list[RawTransaction]) -> None:
    """Fixture TX 10016 is a SOL DEPOSIT with AMOUNT=0 — must be silently skipped.

    The importer returns None for zero-amount rows in _parse_single().
    """
    zero_amount = [t for t in transactions if t.amount == Decimal("0")]
    assert (
        zero_amount == []
    ), f"Found {len(zero_amount)} zero-amount transaction(s) — should be skipped"


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


# ---------------------------------------------------------------------------
# Non-fiat-to-non-fiat trade (SOL → ETH direct swap)
#
# Fixture: ``tests/fixtures/ndax_non_fiat_trade.csv``
# TX 20001: SOL -0.33 (sent) + ETH +0.11 (received) + ETH TRADE/FEE -0.00001
#
# This exercises _parse_non_fiat_trade(), which was previously completely
# untested.  Both legs are needed so the ACB pool for the sent asset is
# correctly reduced.
# ---------------------------------------------------------------------------


def test_non_fiat_trade_emits_two_transactions(non_fiat_txs: list[RawTransaction]) -> None:
    """A non-fiat trade group must emit exactly two transactions."""
    expected_count = 2
    assert (
        len(non_fiat_txs) == expected_count
    ), f"Expected {expected_count} transactions, got {len(non_fiat_txs)}"


def test_non_fiat_trade_received_is_acquisition(non_fiat_txs: list[RawTransaction]) -> None:
    """The received leg (ETH) is an acquisition with no embedded fiat value."""
    eth_tx = next((t for t in non_fiat_txs if t.asset == "ETH"), None)
    assert eth_tx is not None, "ETH acquisition not found"
    assert eth_tx.amount == Decimal("0.11")
    assert eth_tx.fiat_value is None  # normalizer derives CAD value from BoC rate


def test_non_fiat_trade_sent_is_disposal(non_fiat_txs: list[RawTransaction]) -> None:
    """The sent leg (SOL) is a disposal with transaction_type == 'sell'."""
    sol_tx = next((t for t in non_fiat_txs if t.asset == "SOL"), None)
    assert sol_tx is not None, "SOL disposal not found"
    assert sol_tx.transaction_type == "sell"
    assert sol_tx.amount == Decimal("0.33")  # sign stripped
    assert sol_tx.fiat_value is None


def test_non_fiat_trade_fee_on_received_leg(non_fiat_txs: list[RawTransaction]) -> None:
    """TRADE/FEE row is attached to the received-asset (ETH) transaction."""
    eth_tx = next((t for t in non_fiat_txs if t.asset == "ETH"), None)
    assert eth_tx is not None
    assert eth_tx.fee_amount == Decimal("0.00001")
    assert eth_tx.fee_currency == "ETH"


def test_non_fiat_trade_sent_leg_no_fee(non_fiat_txs: list[RawTransaction]) -> None:
    """The sent-asset (SOL) leg carries no fee — fee belongs to the received leg."""
    sol_tx = next((t for t in non_fiat_txs if t.asset == "SOL"), None)
    assert sol_tx is not None
    assert sol_tx.fee_amount is None
