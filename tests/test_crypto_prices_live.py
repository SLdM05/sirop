"""Live integration tests for the crypto price client.

These tests make real HTTP requests to Mempool.space and/or CoinGecko — no mocking.
They are marked ``slow`` and excluded from the default test run.

Run with:
    poetry run pytest -m slow -v
"""

from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest

from sirop.db.schema import create_tables, migrate_to_v5, migrate_to_v6
from sirop.utils.crypto_prices import _fetch_btc_usd_mempool, get_crypto_price_cad

# Monday 6 January 2025 — a confirmed BoC business day; BTC was actively trading.
_KNOWN_DATE = date(2025, 1, 6)


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    migrate_to_v5(conn)
    migrate_to_v6(conn)
    return conn


@pytest.mark.slow
class TestCryptoPricesLive:
    def test_btc_usd_mempool_returns_positive_price(self) -> None:
        """Mempool.space returns a positive BTC/USD price for a known date."""
        price = _fetch_btc_usd_mempool(_KNOWN_DATE)
        assert price is not None, "Mempool.space returned no price — may be unavailable"
        assert isinstance(price, Decimal)
        assert price > Decimal("0")

    def test_btc_price_cad_is_positive_decimal(self) -> None:
        """get_crypto_price_cad returns a positive CAD price for BTC on a known date."""
        conn = _make_conn()
        price_cad = get_crypto_price_cad(conn, "BTC", _KNOWN_DATE)
        assert isinstance(price_cad, Decimal)
        assert price_cad > Decimal("0"), f"BTC/CAD price should be positive, got: {price_cad}"

    def test_price_is_cached_after_live_fetch(self) -> None:
        """A price fetched live is written to cache; subsequent call skips the network."""
        conn = _make_conn()
        first = get_crypto_price_cad(conn, "BTC", _KNOWN_DATE)

        with patch("sirop.utils.crypto_prices.urllib.request.urlopen") as mock_no_call:
            second = get_crypto_price_cad(conn, "BTC", _KNOWN_DATE)
            mock_no_call.assert_not_called()

        assert first == second
