"""Tests for the Bank of Canada Valet API client (sirop.utils.boc).

All HTTP calls are mocked — no network access required.  Tests use an
in-memory SQLite database initialised with the full sirop schema so that
the ``boc_rates`` cache table is available.

Coverage
--------
- Cache hit returns cached value without making an HTTP call.
- Cache miss triggers a live fetch, caches the result, and returns the rate.
- Weekend / holiday fallback: if the requested date has no rate, the client
  retries the previous day and eventually the first business day with a rate.
- All fallback days exhausted → BoCRateError.
- Network error in urlopen → BoCRateError.
- Empty observations list → _fetch_from_api returns None (no rate published).
- Unparseable rate value → BoCRateError.
- _write_cache is idempotent (INSERT OR REPLACE — calling twice is safe).
- currency_pair is uppercased before cache lookup and storage.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from sirop.db.schema import create_tables, migrate_to_v5, migrate_to_v6
from sirop.utils.boc import (
    BoCRateError,
    _fetch_from_api,
    _fetch_range_from_api,
    _read_cached,
    _write_cache,
    get_rate,
    prefetch_rates,
)

if TYPE_CHECKING:
    from collections.abc import Generator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory SQLite connection with the full sirop schema."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    create_tables(conn)
    migrate_to_v5(conn)
    migrate_to_v6(conn)
    return conn


def _boc_range_response(series: str, observations: list[tuple[str, str]]) -> bytes:
    """Build a BoC Valet API JSON response with multiple observations."""
    payload = {"observations": [{"d": d, series: {"v": v}} for d, v in observations]}
    return json.dumps(payload).encode("utf-8")


def _boc_response(series: str, date_str: str, value: str) -> bytes:
    """Build a minimal BoC Valet API JSON response payload."""
    payload = {
        "observations": [
            {"d": date_str, series: {"v": value}},
        ]
    }
    return json.dumps(payload).encode("utf-8")


def _empty_response() -> bytes:
    """Build a BoC Valet API response with no observations (weekend/holiday)."""
    return json.dumps({"observations": []}).encode("utf-8")


@contextmanager
def _mock_urlopen(responses: list[bytes]) -> Generator[MagicMock, None, None]:
    """Patch urllib.request.urlopen to return *responses* in sequence."""
    call_count = 0

    def fake_urlopen(url: str, timeout: int = 10) -> MagicMock:
        nonlocal call_count
        data = responses[call_count % len(responses)]
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = data
        return mock_resp

    with patch("sirop.utils.boc.urllib.request.urlopen", side_effect=fake_urlopen) as m:
        yield m


# ---------------------------------------------------------------------------
# _read_cached
# ---------------------------------------------------------------------------


class TestReadCached:
    def test_returns_none_when_not_present(self) -> None:
        conn = _make_conn()
        result = _read_cached(conn, "USDCAD", date(2025, 3, 15))
        assert result is None

    def test_returns_decimal_when_present(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 15), Decimal("1.4321"))
        result = _read_cached(conn, "USDCAD", date(2025, 3, 15))
        assert result == Decimal("1.4321")

    def test_different_date_returns_none(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 14), Decimal("1.4300"))
        result = _read_cached(conn, "USDCAD", date(2025, 3, 15))
        assert result is None


# ---------------------------------------------------------------------------
# _write_cache
# ---------------------------------------------------------------------------


class TestWriteCache:
    def test_stores_rate(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 15), Decimal("1.4321"))
        result = _read_cached(conn, "USDCAD", date(2025, 3, 15))
        assert result == Decimal("1.4321")

    def test_idempotent_insert_or_replace(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 15), Decimal("1.4300"))
        _write_cache(conn, "USDCAD", date(2025, 3, 15), Decimal("1.4321"))
        result = _read_cached(conn, "USDCAD", date(2025, 3, 15))
        assert result == Decimal("1.4321")

    def test_rate_stored_as_fixed_point(self) -> None:
        """Rate must be stored as fixed-point text, never scientific notation."""
        conn = _make_conn()
        # A computed Decimal can have internal scientific-notation repr.
        rate = Decimal("1.23456789012345") * Decimal("1.0000001")
        _write_cache(conn, "USDCAD", date(2025, 3, 15), rate)
        row = conn.execute(
            "SELECT rate FROM boc_rates WHERE currency_pair = 'USDCAD' AND date = '2025-03-15'"
        ).fetchone()
        assert row is not None
        stored = row[0]
        assert (
            "E" not in stored and "e" not in stored
        ), f"Scientific notation in stored rate: {stored!r}"


# ---------------------------------------------------------------------------
# _fetch_from_api
# ---------------------------------------------------------------------------


class TestFetchFromApi:
    def test_returns_decimal_on_valid_response(self) -> None:
        with _mock_urlopen([_boc_response("FXUSDCAD", "2025-03-15", "1.4321")]):
            result = _fetch_from_api("USDCAD", date(2025, 3, 15))
        assert result == Decimal("1.4321")

    def test_returns_none_on_empty_observations(self) -> None:
        with _mock_urlopen([_empty_response()]):
            result = _fetch_from_api("USDCAD", date(2025, 3, 15))
        assert result is None

    def test_raises_on_network_error(self) -> None:
        with (
            patch(
                "sirop.utils.boc.urllib.request.urlopen",
                side_effect=OSError("connection refused"),
            ),
            pytest.raises(BoCRateError, match="Failed to fetch"),
        ):
            _fetch_from_api("USDCAD", date(2025, 3, 15))

    def test_raises_on_unparseable_rate(self) -> None:
        payload = json.dumps(
            {"observations": [{"d": "2025-03-15", "FXUSDCAD": {"v": "not-a-number"}}]}
        ).encode("utf-8")
        with _mock_urlopen([payload]), pytest.raises(BoCRateError, match="Cannot parse"):
            _fetch_from_api("USDCAD", date(2025, 3, 15))

    def test_returns_none_when_value_key_missing(self) -> None:
        payload = json.dumps({"observations": [{"d": "2025-03-15", "FXUSDCAD": {}}]}).encode(
            "utf-8"
        )
        with _mock_urlopen([payload]):
            result = _fetch_from_api("USDCAD", date(2025, 3, 15))
        assert result is None

    def test_returns_none_when_series_key_missing(self) -> None:
        payload = json.dumps({"observations": [{"d": "2025-03-15"}]}).encode("utf-8")
        with _mock_urlopen([payload]):
            result = _fetch_from_api("USDCAD", date(2025, 3, 15))
        assert result is None


# ---------------------------------------------------------------------------
# get_rate — main public function
# ---------------------------------------------------------------------------


class TestGetRate:
    def test_cache_hit_no_http_call(self) -> None:
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 15), Decimal("1.4321"))

        with patch("sirop.utils.boc.urllib.request.urlopen") as mock_urlopen:
            result = get_rate(conn, "USDCAD", date(2025, 3, 15))
            mock_urlopen.assert_not_called()

        assert result == Decimal("1.4321")

    def test_cache_miss_fetches_api_and_caches(self) -> None:
        conn = _make_conn()
        response = _boc_response("FXUSDCAD", "2025-03-15", "1.4321")

        with _mock_urlopen([response]):
            result = get_rate(conn, "USDCAD", date(2025, 3, 15))

        assert result == Decimal("1.4321")
        # Second call should use cache, not the network.
        with patch("sirop.utils.boc.urllib.request.urlopen") as mock_no_call:
            cached = get_rate(conn, "USDCAD", date(2025, 3, 15))
            mock_no_call.assert_not_called()
        assert cached == Decimal("1.4321")

    def test_weekend_fallback_cached_prior_day(self) -> None:
        """Saturday API returns empty → Friday cached rate used."""
        conn = _make_conn()
        # Pre-seed Friday's rate in the cache.
        _write_cache(conn, "USDCAD", date(2025, 3, 14), Decimal("1.4300"))  # Friday

        # Saturday: not cached, API returns empty (no data published on weekends).
        # Then Friday: cached → no second API call needed.
        with _mock_urlopen([_empty_response()]) as mock_urlopen:
            result = get_rate(conn, "USDCAD", date(2025, 3, 15))  # Saturday
            assert mock_urlopen.call_count == 1  # Only Saturday was tried via API.

        assert result == Decimal("1.4300")

    def test_weekend_fallback_api_hit_prior_day(self) -> None:
        """Saturday with no cached/published rate → API returns Friday's rate."""
        conn = _make_conn()
        # Saturday API returns empty; Friday API returns a rate.
        responses = [
            _empty_response(),  # Saturday (offset=0) — no data
            _boc_response("FXUSDCAD", "2025-03-14", "1.4300"),  # Friday (offset=1)
        ]

        with _mock_urlopen(responses):
            result = get_rate(conn, "USDCAD", date(2025, 3, 15))  # Saturday

        assert result == Decimal("1.4300")

    def test_pair_is_uppercased(self) -> None:
        conn = _make_conn()
        response = _boc_response("FXUSDCAD", "2025-03-17", "1.4350")

        with _mock_urlopen([response]):
            result = get_rate(conn, "usdcad", date(2025, 3, 17))

        assert result == Decimal("1.4350")
        # Check it was stored in upper case.
        cached = _read_cached(conn, "USDCAD", date(2025, 3, 17))
        assert cached == Decimal("1.4350")

    def test_all_fallbacks_exhausted_raises(self) -> None:
        conn = _make_conn()
        # All attempts (offset 0..3) return empty — simulates 4 consecutive holidays.
        with (
            _mock_urlopen([_empty_response()]),
            pytest.raises(BoCRateError, match="No Bank of Canada rate found"),
        ):
            get_rate(conn, "USDCAD", date(2025, 3, 15))

    def test_network_error_raises_boc_rate_error(self) -> None:
        conn = _make_conn()
        with (
            patch(
                "sirop.utils.boc.urllib.request.urlopen",
                side_effect=OSError("timeout"),
            ),
            pytest.raises(BoCRateError, match="Failed to fetch"),
        ):
            get_rate(conn, "USDCAD", date(2025, 3, 15))

    def test_multiple_currency_pairs_isolated(self) -> None:
        """Different currency pairs don't interfere with each other."""
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 15), Decimal("1.4321"))
        _write_cache(conn, "EURCAD", date(2025, 3, 15), Decimal("1.5500"))

        assert get_rate(conn, "USDCAD", date(2025, 3, 15)) == Decimal("1.4321")
        assert get_rate(conn, "EURCAD", date(2025, 3, 15)) == Decimal("1.5500")


# ---------------------------------------------------------------------------
# _fetch_range_from_api
# ---------------------------------------------------------------------------


class TestFetchRangeFromApi:
    def test_returns_dict_of_decimals(self) -> None:
        obs = [("2025-03-10", "1.4300"), ("2025-03-11", "1.4310"), ("2025-03-12", "1.4320")]
        with _mock_urlopen([_boc_range_response("FXUSDCAD", obs)]):
            result = _fetch_range_from_api("USDCAD", date(2025, 3, 10), date(2025, 3, 12))
        assert len(result) == 3  # noqa: PLR2004
        assert result[date(2025, 3, 10)] == Decimal("1.4300")
        assert result[date(2025, 3, 11)] == Decimal("1.4310")
        assert result[date(2025, 3, 12)] == Decimal("1.4320")

    def test_skips_observation_missing_value_key(self) -> None:
        """An observation without a 'v' key is silently skipped."""
        payload = json.dumps(
            {
                "observations": [
                    {"d": "2025-03-10", "FXUSDCAD": {}},  # missing 'v'
                    {"d": "2025-03-11", "FXUSDCAD": {"v": "1.4310"}},
                ]
            }
        ).encode("utf-8")
        with _mock_urlopen([payload]):
            result = _fetch_range_from_api("USDCAD", date(2025, 3, 10), date(2025, 3, 11))
        assert date(2025, 3, 10) not in result
        assert result[date(2025, 3, 11)] == Decimal("1.4310")

    def test_empty_observations_returns_empty_dict(self) -> None:
        with _mock_urlopen([_empty_response()]):
            result = _fetch_range_from_api("USDCAD", date(2025, 3, 15), date(2025, 3, 15))
        assert result == {}

    def test_raises_on_network_error(self) -> None:
        with (
            patch(
                "sirop.utils.boc.urllib.request.urlopen",
                side_effect=OSError("connection refused"),
            ),
            pytest.raises(BoCRateError, match="Failed to fetch"),
        ):
            _fetch_range_from_api("USDCAD", date(2025, 3, 10), date(2025, 3, 14))

    def test_raises_on_unparseable_rate(self) -> None:
        obs = [("2025-03-10", "not-a-number")]
        with (
            _mock_urlopen([_boc_range_response("FXUSDCAD", obs)]),
            pytest.raises(BoCRateError, match="Cannot parse"),
        ):
            _fetch_range_from_api("USDCAD", date(2025, 3, 10), date(2025, 3, 10))


# ---------------------------------------------------------------------------
# prefetch_rates
# ---------------------------------------------------------------------------


class TestPrefetchRates:
    def test_writes_all_rates_to_cache(self) -> None:
        conn = _make_conn()
        obs = [("2025-03-10", "1.4300"), ("2025-03-11", "1.4310"), ("2025-03-12", "1.4320")]
        with _mock_urlopen([_boc_range_response("FXUSDCAD", obs)]):
            prefetch_rates(conn, "USDCAD", date(2025, 3, 10), date(2025, 3, 12))
        assert _read_cached(conn, "USDCAD", date(2025, 3, 10)) == Decimal("1.4300")
        assert _read_cached(conn, "USDCAD", date(2025, 3, 11)) == Decimal("1.4310")
        assert _read_cached(conn, "USDCAD", date(2025, 3, 12)) == Decimal("1.4320")

    def test_returns_count(self) -> None:
        conn = _make_conn()
        obs = [("2025-03-10", "1.4300"), ("2025-03-11", "1.4310"), ("2025-03-12", "1.4320")]
        with _mock_urlopen([_boc_range_response("FXUSDCAD", obs)]):
            count = prefetch_rates(conn, "USDCAD", date(2025, 3, 10), date(2025, 3, 12))
        assert count == 3  # noqa: PLR2004

    def test_empty_response_returns_zero(self) -> None:
        conn = _make_conn()
        with _mock_urlopen([_empty_response()]):
            count = prefetch_rates(conn, "USDCAD", date(2025, 3, 15), date(2025, 3, 15))
        assert count == 0

    def test_pair_is_uppercased(self) -> None:
        conn = _make_conn()
        obs = [("2025-03-10", "1.4300")]
        with _mock_urlopen([_boc_range_response("FXUSDCAD", obs)]):
            prefetch_rates(conn, "usdcad", date(2025, 3, 10), date(2025, 3, 10))
        assert _read_cached(conn, "USDCAD", date(2025, 3, 10)) == Decimal("1.4300")

    def test_overwrites_stale_cached_rate(self) -> None:
        """INSERT OR REPLACE updates a previously cached rate."""
        conn = _make_conn()
        _write_cache(conn, "USDCAD", date(2025, 3, 10), Decimal("1.0000"))
        obs = [("2025-03-10", "1.4300")]
        with _mock_urlopen([_boc_range_response("FXUSDCAD", obs)]):
            prefetch_rates(conn, "USDCAD", date(2025, 3, 10), date(2025, 3, 10))
        assert _read_cached(conn, "USDCAD", date(2025, 3, 10)) == Decimal("1.4300")

    def test_network_error_raises_boc_rate_error(self) -> None:
        conn = _make_conn()
        with (
            patch(
                "sirop.utils.boc.urllib.request.urlopen",
                side_effect=OSError("timeout"),
            ),
            pytest.raises(BoCRateError, match="Failed to fetch"),
        ):
            prefetch_rates(conn, "USDCAD", date(2025, 3, 10), date(2025, 3, 14))
