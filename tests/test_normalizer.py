"""Tests for the normalize pipeline stage (sirop.normalizer.normalizer).

All BoC and crypto price API calls are avoided by pre-seeding the SQLite
cache or mocking the price functions.  Tests use an in-memory database
initialised with the full sirop schema.

Coverage
--------
- CAD fiat value used directly (no conversion needed).
- USD fiat value converted via BoC USDCAD rate.
- USDT / USDC treated as USD (same BoC conversion path).
- Unsupported fiat currency falls through to crypto API.
- No fiat value, exchange rate available -> amount x rate (step 3 fallback).
- No fiat value, no rate → get_crypto_price_cad called (step 4 fallback).
- All sources fail → cad_value=0 with a warning logged.
- CAD fee passed through directly; crypto fee converted via transaction rate.
- Output is sorted chronologically regardless of input order.
- Unknown transaction type maps to TransactionType.OTHER.
- Transfer-type rows with cad_value=0 do not produce a warning.
- Empty input list returns empty output.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import patch

if TYPE_CHECKING:
    import pytest

from sirop.db.schema import create_tables, migrate_to_v5, migrate_to_v6
from sirop.models.enums import TransactionType
from sirop.models.raw import RawTransaction
from sirop.normalizer.normalizer import normalize
from sirop.utils.boc import _write_cache as boc_seed
from sirop.utils.crypto_prices import CryptoPriceError
from sirop.utils.crypto_prices import _write_cache as price_seed

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATE = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)
_DATE2 = datetime(2025, 3, 16, 12, 0, 0, tzinfo=UTC)
_DATE_EARLY = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory SQLite connection with the full sirop schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    migrate_to_v5(conn)
    migrate_to_v6(conn)
    return conn


def _raw(  # noqa: PLR0913
    *,
    transaction_type: str = "buy",
    asset: str = "BTC",
    amount: str = "0.1",
    fiat_value: str | None = "5000",
    fiat_currency: str | None = "CAD",
    fee_amount: str | None = None,
    fee_currency: str | None = None,
    rate: str | None = None,
    timestamp: datetime = _DATE,
) -> RawTransaction:
    """Build a RawTransaction with sensible defaults."""
    return RawTransaction(
        source="test",
        timestamp=timestamp,
        transaction_type=transaction_type,
        asset=asset,
        amount=Decimal(amount),
        amount_currency=asset,
        fiat_value=Decimal(fiat_value) if fiat_value is not None else None,
        fiat_currency=fiat_currency,
        fee_amount=Decimal(fee_amount) if fee_amount is not None else None,
        fee_currency=fee_currency,
        rate=Decimal(rate) if rate is not None else None,
        spot_rate=None,
        txid=None,
        raw_type=transaction_type,
        raw_row={},
        wallet_id=None,
    )


# ---------------------------------------------------------------------------
# CAD fiat used directly
# ---------------------------------------------------------------------------


class TestCadFiatUsedDirectly:
    def test_cad_fiat_value_is_passed_through(self) -> None:
        conn = _make_conn()
        raw = _raw(fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert len(txs) == 1
        assert txs[0].cad_value == Decimal("5000")

    def test_empty_fiat_currency_treated_as_cad(self) -> None:
        """Empty/blank fiat_currency string falls into the CAD branch."""
        conn = _make_conn()
        raw = _raw(fiat_value="3000", fiat_currency="")
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("3000")

    def test_no_api_call_for_cad_transaction(self) -> None:
        conn = _make_conn()
        raw = _raw(fiat_value="5000", fiat_currency="CAD")
        with (
            patch("sirop.normalizer.normalizer.get_rate") as mock_boc,
            patch("sirop.normalizer.normalizer.get_crypto_price_cad") as mock_price,
        ):
            normalize([raw], conn)
        mock_boc.assert_not_called()
        mock_price.assert_not_called()


# ---------------------------------------------------------------------------
# USD fiat converted via BoC rate
# ---------------------------------------------------------------------------


class TestUsdFiatConvertedViaBoC:
    def test_usd_fiat_multiplied_by_boc_rate(self) -> None:
        conn = _make_conn()
        boc_seed(conn, "USDCAD", _DATE.date(), Decimal("1.40"))
        raw = _raw(fiat_value="200", fiat_currency="USD")
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("200") * Decimal("1.40")

    def test_usdt_treated_as_usd(self) -> None:
        conn = _make_conn()
        boc_seed(conn, "USDCAD", _DATE.date(), Decimal("1.35"))
        raw = _raw(fiat_value="100", fiat_currency="USDT")
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("100") * Decimal("1.35")

    def test_usdc_treated_as_usd(self) -> None:
        conn = _make_conn()
        boc_seed(conn, "USDCAD", _DATE.date(), Decimal("1.38"))
        raw = _raw(fiat_value="50", fiat_currency="USDC")
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("50") * Decimal("1.38")

    def test_usd_uses_transaction_date_not_today(self) -> None:
        """BoC lookup uses the transaction date, not the current date."""
        conn = _make_conn()
        boc_seed(conn, "USDCAD", _DATE.date(), Decimal("1.40"))
        boc_seed(conn, "USDCAD", _DATE2.date(), Decimal("1.50"))
        raw = _raw(fiat_value="100", fiat_currency="USD", timestamp=_DATE)
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("100") * Decimal("1.40")


# ---------------------------------------------------------------------------
# Unsupported fiat currency → crypto API fallback
# ---------------------------------------------------------------------------


class TestUnsupportedFiatCurrency:
    def test_unsupported_fiat_falls_through_to_api(self) -> None:
        """A known non-zero fiat_value in an unsupported currency (e.g. GBP)
        is ignored; the normalizer falls through to the crypto price lookup."""
        conn = _make_conn()
        price_seed(conn, "BTC", _DATE.date(), Decimal("80000"), Decimal("112000"), "mempool")
        # fiat_value set but currency unsupported — should use crypto price API
        raw = _raw(fiat_value="500", fiat_currency="GBP", amount="0.1")
        txs = normalize([raw], conn)
        # Fallback: try raw.rate (None) then crypto price (0.1 BTC * 112000 CAD/BTC)
        assert txs[0].cad_value == Decimal("0.1") * Decimal("112000")


# ---------------------------------------------------------------------------
# Exchange-rate fallback (step 3)
# ---------------------------------------------------------------------------


class TestExchangeRateFallback:
    def test_no_fiat_with_rate_uses_amount_times_rate(self) -> None:
        """When fiat_value=None but raw.rate is set, cad_value = amount * rate."""
        conn = _make_conn()
        raw = _raw(fiat_value=None, fiat_currency=None, amount="0.5", rate="95000")
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("0.5") * Decimal("95000")

    def test_zero_rate_not_used(self) -> None:
        """A rate of 0 should not be used (would produce cad_value=0 incorrectly)."""
        conn = _make_conn()
        price_seed(conn, "BTC", _DATE.date(), Decimal("80000"), Decimal("112000"), "mempool")
        raw = _raw(fiat_value=None, fiat_currency=None, amount="0.1", rate="0")
        txs = normalize([raw], conn)
        # rate=0 is skipped; falls through to crypto API (0.1 * 112000)
        assert txs[0].cad_value == Decimal("0.1") * Decimal("112000")


# ---------------------------------------------------------------------------
# Crypto price API fallback (step 4)
# ---------------------------------------------------------------------------


class TestCryptoPriceApiFallback:
    def test_no_fiat_no_rate_uses_crypto_price(self) -> None:
        conn = _make_conn()
        price_seed(conn, "BTC", _DATE.date(), Decimal("80000"), Decimal("112000"), "mempool")
        raw = _raw(fiat_value=None, fiat_currency=None, amount="0.25")
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("0.25") * Decimal("112000")

    def test_crypto_price_uses_transaction_date(self) -> None:
        conn = _make_conn()
        price_seed(conn, "BTC", _DATE.date(), Decimal("80000"), Decimal("112000"), "mempool")
        price_seed(conn, "BTC", _DATE2.date(), Decimal("90000"), Decimal("126000"), "mempool")
        raw = _raw(fiat_value=None, fiat_currency=None, amount="0.1", timestamp=_DATE)
        txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("0.1") * Decimal("112000")


# ---------------------------------------------------------------------------
# All sources fail → cad_value = 0
# ---------------------------------------------------------------------------


class TestAllSourcesFail:
    def test_cad_value_is_zero_when_all_sources_fail(self) -> None:
        conn = _make_conn()
        raw = _raw(fiat_value=None, fiat_currency=None, rate=None, transaction_type="sell")
        with patch(
            "sirop.normalizer.normalizer.get_crypto_price_cad",
            side_effect=CryptoPriceError("no price"),
        ):
            txs = normalize([raw], conn)
        assert txs[0].cad_value == Decimal("0")

    def test_row_is_still_included_in_output(self) -> None:
        """A zero-CAD row is kept — it should not be silently dropped."""
        conn = _make_conn()
        raw = _raw(fiat_value=None, fiat_currency=None, rate=None)
        with patch(
            "sirop.normalizer.normalizer.get_crypto_price_cad",
            side_effect=CryptoPriceError("no price"),
        ):
            txs = normalize([raw], conn)
        assert len(txs) == 1

    def test_transfer_type_zero_cad_no_acb_warning_logged(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Transfer-type rows with cad_value=0 should NOT produce the 'ACB may be
        incorrect' warning — transfers are excluded from ACB calculations."""
        conn = _make_conn()
        raw = _raw(
            fiat_value=None,
            fiat_currency=None,
            rate=None,
            transaction_type="deposit",
        )
        with (
            patch(
                "sirop.normalizer.normalizer.get_crypto_price_cad",
                side_effect=CryptoPriceError("no price"),
            ),
            caplog.at_level("WARNING", logger="sirop.normalizer.normalizer"),
        ):
            normalize([raw], conn)
        acb_warnings = [r for r in caplog.records if "ACB may be incorrect" in r.message]
        assert not acb_warnings


# ---------------------------------------------------------------------------
# Fee resolution
# ---------------------------------------------------------------------------


class TestFeeResolution:
    def test_cad_fee_passed_through(self) -> None:
        conn = _make_conn()
        raw = _raw(
            fiat_value="5000",
            fiat_currency="CAD",
            fee_amount="10",
            fee_currency="CAD",
        )
        txs = normalize([raw], conn)
        assert txs[0].fee_cad == Decimal("10")
        assert txs[0].fee_crypto == Decimal("0")

    def test_crypto_fee_converted_via_rate(self) -> None:
        """BTC fee is converted to CAD using the same rate as the main transaction."""
        conn = _make_conn()
        # CAD fiat value: 5000 CAD for 0.1 BTC → rate = 50000 CAD/BTC
        raw = _raw(
            amount="0.1",
            fiat_value="5000",
            fiat_currency="CAD",
            fee_amount="0.0001",
            fee_currency="BTC",
        )
        txs = normalize([raw], conn)
        expected_fee_cad = Decimal("0.0001") * (Decimal("5000") / Decimal("0.1"))
        assert txs[0].fee_crypto == Decimal("0.0001")
        assert txs[0].fee_cad == expected_fee_cad

    def test_no_fee_produces_zeros(self) -> None:
        conn = _make_conn()
        raw = _raw(fiat_value="5000", fiat_currency="CAD", fee_amount=None)
        txs = normalize([raw], conn)
        assert txs[0].fee_cad == Decimal("0")
        assert txs[0].fee_crypto == Decimal("0")


# ---------------------------------------------------------------------------
# Output ordering
# ---------------------------------------------------------------------------


class TestChronologicalSort:
    def test_output_sorted_by_timestamp(self) -> None:
        conn = _make_conn()
        raw_late = _raw(fiat_value="3000", fiat_currency="CAD", timestamp=_DATE2)
        raw_early = _raw(fiat_value="1000", fiat_currency="CAD", timestamp=_DATE_EARLY)
        raw_mid = _raw(fiat_value="2000", fiat_currency="CAD", timestamp=_DATE)
        txs = normalize([raw_late, raw_early, raw_mid], conn)
        assert txs[0].timestamp == _DATE_EARLY
        assert txs[1].timestamp == _DATE
        assert txs[2].timestamp == _DATE2

    def test_empty_input_returns_empty_list(self) -> None:
        conn = _make_conn()
        assert normalize([], conn) == []


# ---------------------------------------------------------------------------
# Transaction type mapping
# ---------------------------------------------------------------------------


class TestTransactionTypeMapping:
    def test_known_type_mapped_correctly(self) -> None:
        conn = _make_conn()
        raw = _raw(transaction_type="buy", fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].tx_type == TransactionType.BUY

    def test_unknown_type_maps_to_other(self) -> None:
        conn = _make_conn()
        raw = _raw(transaction_type="totally_unknown", fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].tx_type == TransactionType.OTHER

    def test_sell_type_mapped(self) -> None:
        conn = _make_conn()
        raw = _raw(transaction_type="sell", fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].tx_type == TransactionType.SELL


# ---------------------------------------------------------------------------
# Field propagation
# ---------------------------------------------------------------------------


class TestFieldPropagation:
    def test_source_propagated(self) -> None:
        conn = _make_conn()
        raw = _raw(fiat_value="1000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].source == "test"

    def test_asset_propagated(self) -> None:
        conn = _make_conn()
        raw = _raw(asset="ETH", fiat_value="1000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].asset == "ETH"

    def test_amount_propagated(self) -> None:
        conn = _make_conn()
        raw = _raw(amount="2.5", fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].amount == Decimal("2.5")

    def test_is_transfer_initially_false(self) -> None:
        """transfer_match stage sets is_transfer; normalizer always emits False."""
        conn = _make_conn()
        raw = _raw(fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].is_transfer is False

    def test_id_zero_before_db_insert(self) -> None:
        """IDs are assigned by the repository; normalizer always emits 0."""
        conn = _make_conn()
        raw = _raw(fiat_value="5000", fiat_currency="CAD")
        txs = normalize([raw], conn)
        assert txs[0].id == 0
