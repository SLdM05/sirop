"""Live integration tests for the Bank of Canada Valet API client.

These tests make real HTTP requests to the BoC Valet API — no mocking.
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
from sirop.utils.boc import _fetch_from_api, _fetch_range_from_api, get_rate, prefetch_rates

# Monday 6 January 2025 — confirmed BoC business day.
_KNOWN_DATE = date(2025, 1, 6)

# Monday-Friday 6-10 January 2025 - five consecutive BoC business days.
_KNOWN_WEEK_START = date(2025, 1, 6)
_KNOWN_WEEK_END = date(2025, 1, 10)


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    migrate_to_v5(conn)
    migrate_to_v6(conn)
    return conn


@pytest.mark.slow
class TestBoCLive:
    def test_usdcad_rate_is_positive_decimal(self) -> None:
        """BoC Valet API returns a plausible USDCAD rate for a known business day."""
        conn = _make_conn()
        rate = get_rate(conn, "USDCAD", _KNOWN_DATE)
        assert isinstance(rate, Decimal)
        assert Decimal("1.0") < rate < Decimal("2.0"), f"Unexpected USDCAD rate: {rate}"

    def test_rate_is_cached_after_live_fetch(self) -> None:
        """A rate fetched live is written to cache; subsequent call skips the network."""
        conn = _make_conn()
        first = get_rate(conn, "USDCAD", _KNOWN_DATE)

        with patch("sirop.utils.boc.urllib.request.urlopen") as mock_no_call:
            second = get_rate(conn, "USDCAD", _KNOWN_DATE)
            mock_no_call.assert_not_called()

        assert first == second

    def test_fetch_from_api_directly(self) -> None:
        """_fetch_from_api returns a Decimal, bypassing the cache layer."""
        rate = _fetch_from_api("USDCAD", _KNOWN_DATE)
        assert rate is not None, "BoC API returned no rate — may be a connectivity issue"
        assert isinstance(rate, Decimal)
        assert rate > Decimal("0")

    def test_fetch_range_from_api_returns_full_week(self) -> None:
        """_fetch_range_from_api returns all 5 business-day rates in a single call."""
        rates = _fetch_range_from_api("USDCAD", _KNOWN_WEEK_START, _KNOWN_WEEK_END)
        assert len(rates) == 5, f"Expected 5 rates for Mon-Fri, got {len(rates)}: {list(rates)}"  # noqa: PLR2004
        for d, rate in rates.items():
            assert isinstance(rate, Decimal)
            assert Decimal("1.0") < rate < Decimal("2.0"), f"Implausible rate {rate} on {d}"

    def test_prefetch_rates_for_week(self) -> None:
        """prefetch_rates caches 5 rates with a single HTTP call."""
        conn = _make_conn()
        count = prefetch_rates(conn, "USDCAD", _KNOWN_WEEK_START, _KNOWN_WEEK_END)
        assert count == 5, f"Expected 5 rates cached, got {count}"  # noqa: PLR2004

    def test_prefetch_then_get_rate_no_network(self) -> None:
        """After prefetch_rates, every get_rate in the range is served from cache."""
        conn = _make_conn()
        prefetch_rates(conn, "USDCAD", _KNOWN_WEEK_START, _KNOWN_WEEK_END)

        with patch("sirop.utils.boc.urllib.request.urlopen") as mock_no_call:
            for offset in range(5):
                from datetime import timedelta

                d = _KNOWN_WEEK_START + timedelta(days=offset)
                rate = get_rate(conn, "USDCAD", d)
                assert rate > Decimal("0")
            mock_no_call.assert_not_called()
