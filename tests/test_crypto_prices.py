"""Tests for the crypto price client (sirop.utils.crypto_prices).

All HTTP calls are mocked — no network access required.  Tests use an
in-memory SQLite database initialised with the full sirop schema.

Coverage
--------
- Cache hit returns cached CAD price without any HTTP call.
- BTC: Mempool.space fetch succeeds -> returns correct CAD price.
- BTC: Mempool.space fails -> CoinGecko fallback succeeds.
- BTC: Both APIs fail -> CryptoPriceError raised.
- Non-BTC (ETH): CoinGecko path -> returns correct CAD price.
- Unknown asset (not in currencies.yaml) -> CryptoPriceError.
- HTTP 429 on CoinGecko -> retries with back-off, succeeds on 3rd attempt.
- HTTP 429 exhausted (all retries fail) -> CryptoPriceError.
- _write_cache / _read_cached round-trip (price_cad stored as fixed-point text).
- Price stored as fixed-point Decimal text, never scientific notation.
- BoC rate failure propagates as CryptoPriceError.
"""

from __future__ import annotations

import json
import sqlite3
import urllib.error
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
    _fetch_usd_coingecko,
    _fetch_with_backoff,
    _load_asset_config,
    _read_cached,
    _write_cache,
    get_crypto_price_cad,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_DATE = date(2025, 3, 15)
_TEST_DATE_CG_FMT = "15-03-2025"  # CoinGecko date format


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


def _coingecko_response(usd_price: float) -> bytes:
    return json.dumps({"market_data": {"current_price": {"usd": usd_price}}}).encode()


def _make_mock_resp(data: bytes) -> MagicMock:
    mock = MagicMock()
    mock.__enter__ = lambda s: s
    mock.__exit__ = MagicMock(return_value=False)
    mock.read.return_value = data
    return mock


def _make_http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url="http://x", code=code, msg="", hdrs=MagicMock(), fp=None)


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
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("85000"), Decimal("119000"), "coingecko")
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

    def test_different_assets_isolated(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "BTC", _TEST_DATE, Decimal("80000"), Decimal("112000"), "mempool")
        _write_cache(conn, "ETH", _TEST_DATE, Decimal("3000"), Decimal("4200"), "coingecko")
        assert _read_cached(conn, "BTC", _TEST_DATE) == Decimal("112000")
        assert _read_cached(conn, "ETH", _TEST_DATE) == Decimal("4200")


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
# _fetch_usd_coingecko
# ---------------------------------------------------------------------------


class TestFetchUsdCoingecko:
    def test_happy_path(self) -> None:
        mock_resp = _make_mock_resp(_coingecko_response(84000.0))
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen", return_value=mock_resp):
            result = _fetch_usd_coingecko("bitcoin", _TEST_DATE)
        assert result == Decimal("84000.0")

    def test_returns_none_when_no_market_data(self) -> None:
        data = json.dumps({"symbol": "btc"}).encode()  # no market_data key
        mock_resp = _make_mock_resp(data)
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen", return_value=mock_resp):
            result = _fetch_usd_coingecko("bitcoin", _TEST_DATE)
        assert result is None

    def test_returns_none_on_fetch_error(self) -> None:
        with patch(
            "sirop.utils.crypto_prices.urllib.request.urlopen",
            side_effect=OSError("timeout"),
        ):
            result = _fetch_usd_coingecko("bitcoin", _TEST_DATE)
        assert result is None


# ---------------------------------------------------------------------------
# _fetch_with_backoff
# ---------------------------------------------------------------------------


class TestFetchWithBackoff:
    def test_succeeds_on_first_try(self) -> None:
        mock_resp = _make_mock_resp(b"hello")
        with patch("sirop.utils.crypto_prices.urllib.request.urlopen", return_value=mock_resp):
            result = _fetch_with_backoff("http://example.com")
        assert result == b"hello"

    def test_retries_on_429_then_succeeds(self) -> None:
        """Two 429s followed by a success: should succeed after back-off."""
        http_429 = _make_http_error(429)
        success_resp = _make_mock_resp(b"ok")
        side_effects = [http_429, http_429, success_resp]

        with (
            patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                side_effect=side_effects,
            ),
            patch("sirop.utils.crypto_prices.time.sleep") as mock_sleep,
        ):
            result = _fetch_with_backoff("http://example.com")

        assert result == b"ok"
        assert mock_sleep.call_count == 2  # noqa: PLR2004
        mock_sleep.assert_any_call(2)
        mock_sleep.assert_any_call(4)

    def test_raises_after_all_retries_exhausted(self) -> None:
        http_429 = _make_http_error(429)

        with (
            patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                side_effect=[http_429, http_429, http_429, http_429],
            ),
            patch("sirop.utils.crypto_prices.time.sleep"),
            pytest.raises(CryptoPriceError, match="rate limit exceeded"),
        ):
            _fetch_with_backoff("http://example.com")

    def test_raises_immediately_on_non_429_http_error(self) -> None:
        http_500 = _make_http_error(500)
        with (
            patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                side_effect=http_500,
            ),
            pytest.raises(CryptoPriceError, match="HTTP 500"),
        ):
            _fetch_with_backoff("http://example.com")


# ---------------------------------------------------------------------------
# _load_asset_config
# ---------------------------------------------------------------------------


class TestLoadAssetConfig:
    def test_known_asset_returns_config(self) -> None:
        cfg = _load_asset_config("BTC")
        assert cfg["coingecko_id"] == "bitcoin"
        assert cfg["mempool_supported"] is True

    def test_known_eth_returns_config(self) -> None:
        cfg = _load_asset_config("ETH")
        assert cfg["coingecko_id"] == "ethereum"
        assert cfg["mempool_supported"] is False

    def test_unknown_asset_raises(self) -> None:
        with pytest.raises(CryptoPriceError, match="not found in currencies.yaml"):
            _load_asset_config("UNKNOWNXYZ")


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

    def test_btc_mempool_fails_coingecko_fallback(self) -> None:
        conn = _make_conn()
        with _mock_boc_rate(conn, "1.40000"):
            mempool_fail = OSError("timeout")
            coingecko_resp = _make_mock_resp(_coingecko_response(80000.0))
            side_effects = [mempool_fail, coingecko_resp]
            with patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                side_effect=side_effects,
            ):
                result = get_crypto_price_cad(conn, "BTC", _TEST_DATE)

        assert result == Decimal("80000.0") * Decimal("1.40000")

    def test_btc_both_sources_fail_raises(self) -> None:
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

    def test_eth_coingecko_path(self) -> None:
        conn = _make_conn()
        with _mock_boc_rate(conn, "1.40000"):
            coingecko_resp = _make_mock_resp(_coingecko_response(3000.0))
            with patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                return_value=coingecko_resp,
            ):
                result = get_crypto_price_cad(conn, "ETH", _TEST_DATE)

        # 3000 USD * 1.40 USDCAD = 4200 CAD
        assert result == Decimal("3000.0") * Decimal("1.40000")

    def test_unknown_asset_raises(self) -> None:
        conn = _make_conn()
        with (
            _mock_boc_rate(conn),
            pytest.raises(CryptoPriceError, match="not found in currencies.yaml"),
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
        conn = _make_conn()
        with _mock_boc_rate(conn, "1.40000"):
            coingecko_resp = _make_mock_resp(_coingecko_response(3000.0))
            with patch(
                "sirop.utils.crypto_prices.urllib.request.urlopen",
                return_value=coingecko_resp,
            ):
                result = get_crypto_price_cad(conn, "eth", _TEST_DATE)

        # ETH = 3000 USD * 1.40 = 4200 CAD
        assert result == Decimal("3000.0") * Decimal("1.40000")
        # Should be cached under uppercase key.
        assert _read_cached(conn, "ETH", _TEST_DATE) is not None
