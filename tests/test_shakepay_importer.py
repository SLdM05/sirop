"""Tests for the Shakepay CSV importer.

Fixture: ``tests/fixtures/shakepay_synthetic.csv``

No real personal data; all amounts, addresses, and txids are synthetic.

Transaction types covered by the fixture
-----------------------------------------
fiat funding     → fiat_deposit
purchase/sale    → buy  (CAD→BTC, debit=fiat, credit=crypto)
purchase/sale    → sell (BTC→CAD, debit=crypto, credit=fiat)
crypto cashout   → withdrawal
crypto purchase  → deposit
shakingsats      → reward_shake
Reward           → reward_shake  (ShakingSats description)
Reward           → reward_cashback  (description contains "cashback")
Reward           → reward_shakesquad  (description contains "ShakeSquad")
Reward           → interest  (description contains "Interest payout")
peer transfer    → other
other            → other
fiat cashout     → fiat_withdrawal
mystery_reward   → unknown (stored for manual review, not dropped)

Direction regression guard
--------------------------
The ``purchase/sale`` type covers both buy and sell trades.  Direction is
resolved from the Debit/Credit Currency columns, not from a "Direction"
field or a generic "trade" type.  Tests in the "Direction" section pin this
behaviour so that any regression in _parse_purchase_sale() fails immediately.
"""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from sirop.importers.base import InvalidCSVFormatError, MissingColumnError
from sirop.importers.shakepay import ShakepayImporter
from sirop.models.raw import RawTransaction

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SHAKEPAY_CSV = FIXTURES_DIR / "shakepay_synthetic.csv"
SHAKEPAY_YAML = Path(__file__).parent.parent / "config" / "importers" / "shakepay.yaml"


@pytest.fixture(scope="module")
def importer() -> ShakepayImporter:
    return ShakepayImporter.from_yaml(SHAKEPAY_YAML)


@pytest.fixture(scope="module")
def transactions(importer: ShakepayImporter) -> list[RawTransaction]:
    return importer.parse(SHAKEPAY_CSV)


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


def test_source_is_shakepay(transactions: list[RawTransaction]) -> None:
    assert all(t.source == "shakepay" for t in transactions)


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


def test_all_timestamps_are_utc(transactions: list[RawTransaction]) -> None:
    for tx in transactions:
        assert tx.timestamp.tzinfo is not None
        assert tx.timestamp.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_fiat_deposit_timestamp(transactions: list[RawTransaction]) -> None:
    deposits = _find(transactions, "fiat_deposit")
    assert len(deposits) == 1
    assert deposits[0].timestamp == datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)


def test_timestamps_parsed_from_space_format(importer: ShakepayImporter, tmp_path: Path) -> None:
    """Shakepay 2025 exports use 'YYYY-MM-DD HH:MM:SS' (no T, no TZ offset).
    The base importer must treat naive timestamps as UTC."""
    csv_text = (
        "Date,Amount Debited,Asset Debited,Amount Credited,Asset Credited,"
        "Market Value,Market Value Currency,Book Cost,Book Cost Currency,"
        "Type,Spot Rate,Buy / Sell Rate,Description\n"
        "2025-06-15 09:30:00,,,0.00100000,BTC,,,,,shakingsats,,,\n"
    )
    f = tmp_path / "sp.csv"
    f.write_text(csv_text)
    txs = importer.parse(f)
    assert len(txs) == 1
    assert txs[0].timestamp == datetime(2025, 6, 15, 9, 30, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Amounts
# ---------------------------------------------------------------------------


def test_all_amounts_are_non_negative(transactions: list[RawTransaction]) -> None:
    """Importer must always emit non-negative amounts."""
    for tx in transactions:
        assert tx.amount >= Decimal("0"), f"Negative amount on {tx}"


def test_all_amounts_are_decimal(transactions: list[RawTransaction]) -> None:
    for tx in transactions:
        assert isinstance(tx.amount, Decimal)


# ---------------------------------------------------------------------------
# Direction regression — purchase/sale must resolve to "buy" or "sell"
# ---------------------------------------------------------------------------


def test_no_trade_type_in_output(transactions: list[RawTransaction]) -> None:
    """Regression: the importer must never emit transaction_type=='trade'.

    'purchase/sale' rows are resolved to 'buy' or 'sell' based on the
    Debit/Credit Currency columns.  A 'trade' output means direction
    detection has been lost.
    """
    trade_typed = [t for t in transactions if t.transaction_type == "trade"]
    assert trade_typed == [], (
        f"Found {len(trade_typed)} transaction(s) still typed as 'trade' — "
        "buy/sell direction was not resolved."
    )


def test_cad_debit_btc_credit_becomes_buy(transactions: list[RawTransaction]) -> None:
    """Debit=CAD, Credit=BTC → transaction_type must be 'buy'.

    Fixture row: 250 CAD debited, 0.00405049 BTC credited.
    """
    buys = [t for t in _find(transactions, "buy", "BTC") if t.raw_type == "purchase/sale"]
    assert len(buys) == 1, f"Expected 1 purchase/sale BTC buy, got {len(buys)}"


def test_btc_debit_cad_credit_becomes_sell(transactions: list[RawTransaction]) -> None:
    """Debit=BTC, Credit=CAD → transaction_type must be 'sell'.

    Fixture row: 0.00100000 BTC debited, 145.00 CAD credited.
    """
    sells = _find(transactions, "sell", "BTC")
    assert len(sells) == 1, f"Expected 1 BTC sell, got {len(sells)}"


def test_buy_and_sell_timestamps_disjoint(transactions: list[RawTransaction]) -> None:
    """No timestamp should appear in both the buy and sell lists."""
    buy_ts = {t.timestamp for t in _find(transactions, "buy", "BTC")}
    sell_ts = {t.timestamp for t in _find(transactions, "sell", "BTC")}
    assert buy_ts.isdisjoint(
        sell_ts
    ), "Same timestamp appears in both buy and sell lists — direction detection broken"


# ---------------------------------------------------------------------------
# Buy (purchase/sale, CAD→BTC)
# ---------------------------------------------------------------------------


def test_buy_asset_is_btc(transactions: list[RawTransaction]) -> None:
    tx = _find(transactions, "buy", "BTC")[0]
    assert tx.asset == "BTC"
    assert tx.amount_currency == "BTC"


def test_buy_amount(transactions: list[RawTransaction]) -> None:
    """Buy amount is the BTC credited (0.00405049)."""
    tx = _find(transactions, "buy", "BTC")[0]
    assert tx.amount == Decimal("0.00405049")


def test_buy_fiat_value(transactions: list[RawTransaction]) -> None:
    """Buy fiat_value is the CAD debited (250.00)."""
    tx = _find(transactions, "buy", "BTC")[0]
    assert tx.fiat_value == Decimal("250.00")
    assert tx.fiat_currency == "CAD"


def test_buy_rate_is_consistent(transactions: list[RawTransaction]) -> None:
    """rate == fiat_value / amount for the buy trade."""
    tx = _find(transactions, "buy", "BTC")[0]
    assert tx.rate is not None
    assert tx.fiat_value is not None
    expected = tx.fiat_value / tx.amount
    assert abs(tx.rate - expected) < Decimal("0.01"), f"rate mismatch: {tx.rate} vs {expected}"


def test_buy_fee_is_none(transactions: list[RawTransaction]) -> None:
    """Shakepay uses a spread model — no explicit fee column."""
    tx = _find(transactions, "buy", "BTC")[0]
    assert tx.fee_amount is None
    assert tx.fee_currency is None


def test_buy_spot_rate_is_none(transactions: list[RawTransaction]) -> None:
    """spot_rate is always None — Shakepay's rate ≠ BoC rate."""
    tx = _find(transactions, "buy", "BTC")[0]
    assert tx.spot_rate is None


# ---------------------------------------------------------------------------
# Sell (purchase/sale, BTC→CAD)
# ---------------------------------------------------------------------------


def test_sell_asset_is_btc(transactions: list[RawTransaction]) -> None:
    tx = _find(transactions, "sell", "BTC")[0]
    assert tx.asset == "BTC"
    assert tx.amount_currency == "BTC"


def test_sell_amount(transactions: list[RawTransaction]) -> None:
    """Sell amount is the BTC debited (0.00100000)."""
    tx = _find(transactions, "sell", "BTC")[0]
    assert tx.amount == Decimal("0.00100000")


def test_sell_fiat_value_is_proceeds(transactions: list[RawTransaction]) -> None:
    """Sell fiat_value is the CAD credited (145.00)."""
    tx = _find(transactions, "sell", "BTC")[0]
    assert tx.fiat_value == Decimal("145.00")
    assert tx.fiat_currency == "CAD"


def test_sell_rate_is_consistent(transactions: list[RawTransaction]) -> None:
    tx = _find(transactions, "sell", "BTC")[0]
    assert tx.rate is not None
    assert tx.fiat_value is not None
    expected = tx.fiat_value / tx.amount
    assert abs(tx.rate - expected) < Decimal("0.01")


# ---------------------------------------------------------------------------
# Fiat deposit (fiat funding)
# ---------------------------------------------------------------------------


def test_fiat_deposit_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "fiat_deposit")) == 1


def test_fiat_deposit_fields(transactions: list[RawTransaction]) -> None:
    tx = _find(transactions, "fiat_deposit")[0]
    assert tx.asset == "CAD"
    assert tx.amount == Decimal("500.00")
    assert tx.fiat_value == Decimal("500.00")
    assert tx.fiat_currency == "CAD"
    assert tx.fee_amount is None


# ---------------------------------------------------------------------------
# Fiat withdrawal (fiat cashout)
# ---------------------------------------------------------------------------


def test_fiat_withdrawal_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "fiat_withdrawal")) == 1


def test_fiat_withdrawal_fields(transactions: list[RawTransaction]) -> None:
    tx = _find(transactions, "fiat_withdrawal")[0]
    assert tx.asset == "CAD"
    assert tx.amount == Decimal("200.00")
    assert tx.fiat_value == Decimal("200.00")
    assert tx.fiat_currency == "CAD"
    assert tx.txid is None


# ---------------------------------------------------------------------------
# Withdrawal (crypto cashout)
# ---------------------------------------------------------------------------


def test_withdrawal_present(transactions: list[RawTransaction]) -> None:
    # crypto cashout + Send (2025 type)
    assert len(_find(transactions, "withdrawal", "BTC")) == 2  # noqa: PLR2004


def test_withdrawal_fields(transactions: list[RawTransaction]) -> None:
    txs = _find(transactions, "withdrawal", "BTC")
    tx = next(t for t in txs if t.raw_type == "crypto cashout")
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.00405049")
    assert tx.fiat_value is None
    assert tx.fiat_currency is None


def test_withdrawal_txid(transactions: list[RawTransaction]) -> None:
    """Crypto cashout rows carry the on-chain txid for node verification."""
    txs = _find(transactions, "withdrawal", "BTC")
    tx = next(t for t in txs if t.raw_type == "crypto cashout")
    assert tx.txid == "aabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"


def test_send_type_maps_to_withdrawal(transactions: list[RawTransaction]) -> None:
    """'Send' is the 2025 Shakepay type for crypto sent to an external address."""
    txs = _find(transactions, "withdrawal", "BTC")
    tx = next(t for t in txs if t.raw_type == "send")
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.00103685")
    assert tx.fiat_value is None


# ---------------------------------------------------------------------------
# Deposit (crypto purchase)
# ---------------------------------------------------------------------------


def test_deposit_present(transactions: list[RawTransaction]) -> None:
    assert len(_find(transactions, "deposit", "BTC")) == 1


def test_deposit_fields(transactions: list[RawTransaction]) -> None:
    tx = _find(transactions, "deposit", "BTC")[0]
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.01000000")
    assert tx.fiat_value is None
    assert tx.txid == "bbbbccccbbbbccccbbbbccccbbbbccccbbbbccccbbbbccccbbbbccccbbbbcccc"


# ---------------------------------------------------------------------------
# Buy (2025 credit-only purchase type)
# ---------------------------------------------------------------------------


def test_buy_2025_present(transactions: list[RawTransaction]) -> None:
    """'Buy' rows emit a 'buy' transaction; normalizer will supply CAD value via BoC/CoinGecko."""
    txs = [t for t in transactions if t.transaction_type == "buy" and t.raw_type == "buy"]
    assert len(txs) == 1


def test_buy_2025_fields(transactions: list[RawTransaction]) -> None:
    tx = next(t for t in transactions if t.transaction_type == "buy" and t.raw_type == "buy")
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.00000619")
    assert tx.fiat_value is None  # normalizer fetches BoC/CoinGecko rate for ACB


# ---------------------------------------------------------------------------
# Rewards — shakingsats and Reward (2025 type name) → reward_shake
# Note: tax treatment (discount = ACB $0, not income) is determined by
# tax_rules.yaml reward_treatment config, not by the importer type.
# ---------------------------------------------------------------------------


def test_reward_shake_present(transactions: list[RawTransaction]) -> None:
    """'shakingsats' and plain 'Reward' (ShakingSats description) map to 'reward_shake'."""
    assert len(_find(transactions, "reward_shake", "BTC")) == 2  # noqa: PLR2004


def test_reward_shake_fields(transactions: list[RawTransaction]) -> None:
    """shakingsats: BTC credited, no fiat_value (normalizer handles FMV), no fee, no txid."""
    txs = _find(transactions, "reward_shake", "BTC")
    tx = next(t for t in txs if t.raw_type == "shakingsats")
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.00001500")
    assert tx.fiat_value is None  # normalizer fetches BoC rate
    assert tx.fiat_currency is None
    assert tx.fee_amount is None
    assert tx.txid is None


def test_reward_type_maps_to_reward_shake(transactions: list[RawTransaction]) -> None:
    """'Reward' rows with ShakingSats description map to 'reward_shake'."""
    txs = _find(transactions, "reward_shake", "BTC")
    tx = next(t for t in txs if t.raw_type == "reward")
    assert tx.asset == "BTC"
    assert tx.amount == Decimal("0.00000021")
    assert tx.fiat_value is None
    assert tx.txid is None


# ---------------------------------------------------------------------------
# Reward sub-types via description override (all have raw_type == "reward")
# ---------------------------------------------------------------------------


def test_reward_cashback_from_description(transactions: list[RawTransaction]) -> None:
    """'Reward' with 'Bitcoin cashback' description → reward_cashback."""
    txs = _find(transactions, "reward_cashback", "BTC")
    assert len(txs) == 1
    tx = txs[0]
    assert tx.raw_type == "reward"
    assert tx.amount == Decimal("0.00000275")
    assert tx.fiat_value is None


def test_reward_shakesquad_from_description(transactions: list[RawTransaction]) -> None:
    """'Reward' with 'ShakeSquad' description → reward_shakesquad."""
    txs = _find(transactions, "reward_shakesquad", "BTC")
    assert len(txs) == 1
    tx = txs[0]
    assert tx.raw_type == "reward"
    assert tx.amount == Decimal("0.00000588")
    assert tx.fiat_value is None


def test_interest_from_description(transactions: list[RawTransaction]) -> None:
    """'Reward' with 'Interest payout' description → interest."""
    txs = _find(transactions, "interest", "BTC")
    assert len(txs) == 1
    tx = txs[0]
    assert tx.raw_type == "reward"
    assert tx.amount == Decimal("0.0000001")
    assert tx.fiat_value is None


# ---------------------------------------------------------------------------
# Other (peer transfer and referral bonus)
# ---------------------------------------------------------------------------


_EXPECTED_OTHER_COUNT = 2


def test_other_transactions_present(transactions: list[RawTransaction]) -> None:
    """Both 'peer transfer' and 'other' map to canonical type 'other'."""
    others = _find(transactions, "other")
    assert len(others) == _EXPECTED_OTHER_COUNT


def test_other_amounts_are_positive(transactions: list[RawTransaction]) -> None:
    for tx in _find(transactions, "other"):
        assert tx.amount > Decimal("0")


def test_peer_transfer_asset_is_btc(transactions: list[RawTransaction]) -> None:
    """The peer transfer row credits BTC — asset should be BTC."""
    btc_others = _find(transactions, "other", "BTC")
    assert len(btc_others) == 1
    assert btc_others[0].amount == Decimal("0.00010000")


def test_referral_other_asset_is_cad(transactions: list[RawTransaction]) -> None:
    """The referral 'other' row credits CAD."""
    cad_others = _find(transactions, "other", "CAD")
    assert len(cad_others) == 1
    assert cad_others[0].amount == Decimal("10.00")


# ---------------------------------------------------------------------------
# Unknown transaction types
# ---------------------------------------------------------------------------


def test_unknown_type_not_dropped(transactions: list[RawTransaction]) -> None:
    """Unknown types must be stored for manual review, not silently discarded.

    Fixture row: 'mystery_reward' — not in transaction_type_map.
    The importer should store it with transaction_type == 'mystery_reward'.
    """
    unknown = [t for t in transactions if t.raw_type == "mystery_reward"]
    assert len(unknown) == 1, (
        "Unknown transaction type 'mystery_reward' was dropped — "
        "importer must store unknown types for manual review."
    )


def test_unknown_type_has_amount(transactions: list[RawTransaction]) -> None:
    unknown = next(t for t in transactions if t.raw_type == "mystery_reward")
    assert unknown.amount == Decimal("5.00")
    assert unknown.asset == "CAD"


# ---------------------------------------------------------------------------
# Raw row preservation
# ---------------------------------------------------------------------------


def test_raw_row_preserved(transactions: list[RawTransaction]) -> None:
    """Every transaction must carry the original CSV row for the audit trail."""
    for tx in transactions:
        assert isinstance(tx.raw_row, dict)
        assert len(tx.raw_row) > 0


def test_buy_sell_rate_in_raw_row(transactions: list[RawTransaction]) -> None:
    """'Buy / Sell Rate' must be preserved in raw_row even though it's not used."""
    tx = _find(transactions, "buy", "BTC")[0]
    assert "Buy / Sell Rate" in tx.raw_row
    assert tx.raw_row["Buy / Sell Rate"] == "61720.90"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_file_not_found_raises(importer: ShakepayImporter) -> None:
    with pytest.raises(InvalidCSVFormatError, match="not found"):
        importer.parse(Path("/nonexistent/path/shakepay.csv"))


def test_missing_column_raises(importer: ShakepayImporter, tmp_path: Path) -> None:
    """A CSV missing 'Amount Debited' should raise MissingColumnError."""
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text(
        # All required columns present except 'Amount Debited'.
        "Date,Asset Debited,Amount Credited,Asset Credited,"
        "Market Value,Market Value Currency,Book Cost,Book Cost Currency,"
        "Type,Spot Rate,Buy / Sell Rate,Description\n"
        "2025-01-01T10:00:00+00,,500.00,CAD,500.00,CAD,500.00,CAD,fiat funding,,,\n"
    )
    with pytest.raises(MissingColumnError):
        importer.parse(bad_csv)


def test_empty_csv_returns_empty_list(importer: ShakepayImporter, tmp_path: Path) -> None:
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text(
        "Date,Amount Debited,Asset Debited,Amount Credited,Asset Credited,"
        "Market Value,Market Value Currency,Book Cost,Book Cost Currency,"
        "Type,Spot Rate,Buy / Sell Rate,Description\n"
    )
    result = importer.parse(empty_csv)
    assert result == []
