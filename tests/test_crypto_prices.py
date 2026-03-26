"""Tests for the crypto price client (sirop.utils.crypto_prices).

All HTTP calls are mocked — no network access required.  Tests use an
in-memory SQLite database initialised with the full sirop schema.

sirop is BTC-only.  Only Mempool.space is used for price lookups.

Coverage
--------
- Cache hit returns cached CAD price without any HTTP call.
- BTC: Mempool.space fetch succeeds -> returns correct CAD price.
- BTC: Mempool.space fails -> CryptoPriceError raised.
- Non-BTC asset -> CryptoPriceError raised immediately (sirop is BTC-only).
- _write_cache / _read_cached round-trip (price_cad stored as fixed-point text).
- Price stored as fixed-point Decimal text, never scientific notation.
- BoC rate failure propagates as CryptoPriceError.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Generator

import pytest

from sirop.db.schema import create_tables, migrate_to_v5, migrate_to_v6
from sirop.utils.boc import BoCRateError
from sirop.utils.boc import _write_cache as boc_write_cache
from sirop.utils.crypto_prices import (
    CryptoPriceError,
    _fetch_btc_usd_mempool,
    _read_cached,
    _write_cache,
    get_crypto_price_cad,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_DATE = date(2025, 3, 15)


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    migrate_to_v5(conn)
    migrate_to_v6(conn)
    return conn


def _mempool_response(usd_price: float) -> bytes:
    return json.dumps({"prices": [{"time": 1741953600, "USD": usd_price}]}).encode()


def _make_mock_resp(data: bytes) -> MagicMock:
    mock = MagicMock()
    mock.__enter__ = lambda s: s
    mock.__exit__ = MagicMock(return_value=False)
    mock.read.return_value = data
    return mock


@contextmanager
def _mock_boc_rate(conn: sqlite3.Connection, rate: str = "1.4000") -> Generator[None, None, None]:
    """Pre-seed the BoC USDCAD rate for _TEST_DATE so crypto_prices tests pass."""
    boc_write_cache(conn, "USDCAD", _TEST_DATE, Decimal(rate))
    yield


# ---------------------------------------------------------------------------
# _read_cached / _write_cache
# ---------------------------------------------------------------------------


class TestCache:
    def test_returns_none_when_absent(self) -> None:
        conn = _make_conn()
        assert _read_cached(conn, "BTC", _TEST_DATE) is None

    def test_round_trip(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("80000"), Decimal("112000"), "mempool")
        result = _read_cached(conn, "BTC", _TEST_DATE)
        assert result == Decimal("112000")

    def test_idempotent(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("80000"), Decimal("112000"), "mempool")
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("85000"), Decimal("119000"), "mempool")
        result = _read_cached(conn, "BTC", _TEST_DATE)
        assert result == Decimal("119000")

    def test_price_stored_as_fixed_point(self) -> None:
        conn = _make_conn()
        price_cad = Decimal("112000.123456789")
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("80000"), price_cad, "mempool")
        row = conn.execute(
            "SELECT price_cad FROM crypto_prices WHERE asset = 'BTC' AND date = '2025-03-15'"
        ).fetchone()
        assert row is not None
        stored = row[0]
        assert "E" not in stored and "e" not in stored, f"Scientific notation: {stored!r}"


# ---------------------------------------------------------------------------
# _fetch_btc_usd_mempool
# ---------------------------------------------------------------------------


class TestFetchBtcUsdMempool:
    def test_happy_path(self) -> None:
        mock_resp = _make_mock_resp(_mempool_response(84000.0))
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen", return_value=mock_resp):
            result = _fetch_btc_usd_mempool(_TEST_DATE)
        assert result == Decimal("84000.0")

    def test_returns_none_on_empty_prices(self) -> None:
        data = json.dumps({"prices": []}).encode()
        mock_resp = _make_mock_resp(data)
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen", return_value=mock_resp):
            result = _fetch_btc_usd_mempool(_TEST_DATE)
        assert result is None

    def test_returns_none_on_missing_usd_field(self) -> None:
        data = json.dumps({"prices": [{"time": 123}]}).encode()
        mock_resp = _make_mock_resp(data)
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen", return_value=mock_resp):
            result = _fetch_btc_usd_mempool(_TEST_DATE)
        assert result is None

    def test_returns_none_on_network_error(self) -> None:
        with patch(
            "sirop.utils.crypto_prices.urllib.request.urlopen",
            side_effect=OSError("network down"),
        ):
            result = _fetch_btc_usd_mempool(_TEST_DATE)
        assert result is None


# ---------------------------------------------------------------------------
# get_crypto_price_cad -- main public function
# ---------------------------------------------------------------------------


class TestGetCryptoPriceCad:
    def test_cache_hit_no_http_call(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("80000"), Decimal("112000"), "mempool")

        with patch("sirop.utils.crypto_prices.urllib.request.urlopen") as mock_no_call:
            result = get_crypto_price_cad(conn, "BTC", _TEST_DATE)
            mock_no_call.assert_not_called()

        assert result == Decimal("112000")

    def test_btc_mempool_success(self) -> None:
        conn = _make_conn()
        with _mock_boc_rate(conn, "1.40000"):
            mempool_resp = _make_mock_resp(_mempool_response(80000.0))
            with patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                return_value=mempool_resp,
            ):
                result = get_crypto_price_cad(conn, "BTC", _TEST_DATE)

        # 80000 USD * 1.40 USDCAD = 112000 CAD
        assert result == Decimal("80000.0") * Decimal("1.40000")

    def test_btc_mempool_fails_raises(self) -> None:
        conn = _make_conn()
        with (
            _mock_boc_rate(conn),
            patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                side_effect=OSError("timeout"),
            ),
            pytest.raises(CryptoPriceError),
        ):
            get_crypto_price_cad(conn, "BTC", _TEST_DATE)

    def test_non_btc_asset_raises_immediately(self) -> None:
        """Non-BTC assets must raise CryptoPriceError immediately — sirop is BTC-only."""
        conn = _make_conn()
        with (
            _mock_boc_rate(conn),
            pytest.raises(CryptoPriceError, match="BTC-only"),
        ):
            get_crypto_price_cad(conn, "ETH", _TEST_DATE)

    def test_unknown_asset_raises(self) -> None:
        conn = _make_conn()
        with (
            _mock_boc_rate(conn),
            pytest.raises(CryptoPriceError, match="BTC-only"),
        ):
            get_crypto_price_cad(conn, "UNKNOWNXYZ", _TEST_DATE)

    def test_boc_rate_failure_raises_crypto_price_error(self) -> None:
        conn = _make_conn()
        # No BoC rate seeded -- get_rate will try network and fail.
        mempool_resp = _make_mock_resp(_mempool_response(80000.0))
        with (
            patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                return_value=mempool_resp,
            ),
            patch(
                "sirop.utils.crypto_prices.get_rate",
                side_effect=BoCRateError("BoC unavailable"),
            ),
            pytest.raises(CryptoPriceError),
        ):
            get_crypto_price_cad(conn, "BTC", _TEST_DATE)

    def test_result_is_cached_after_fetch(self) -> None:
        conn = _make_conn()
        with _mock_boc_rate(conn, "1.40000"):
            mempool_resp = _make_mock_resp(_mempool_response(80000.0))
            with patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                return_value=mempool_resp,
            ):
                first_result = get_crypto_price_cad(conn, "BTC", _TEST_DATE)

        # Second call must hit cache, not network.
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen") as mock_no_call:
            second_result = get_crypto_price_cad(conn, "BTC", _TEST_DATE)
            mock_no_call.assert_not_called()

        assert first_result == second_result

    def test_asset_is_uppercased(self) -> None:
        """Lowercase "btc" must be treated identically to "BTC"."""
        conn = _make_conn()
        with _mock_boc_rate(conn, "1.40000"):
            mempool_resp = _make_mock_resp(_mempool_response(80000.0))
            with patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                return_value=mempool_resp,
            ):
                result = get_crypto_price_cad(conn, "btc", _TEST_DATE)

        assert result == Decimal("80000.0") * Decimal("1.40000")
        # Should be cached under uppercase key.
        assert _read_cached(conn, "BTC", _TEST_DATE) is not None
