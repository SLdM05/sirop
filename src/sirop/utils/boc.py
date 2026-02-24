"""Bank of Canada Valet API client with SQLite caching.

Fetches daily average exchange rates for fiat currency pairs
(e.g. USDCAD, EURCAD). Does NOT fetch crypto prices — the BoC
Valet API only covers fiat foreign exchange.

Usage
-----
from sirop.utils.boc import get_rate, BoCRateError
import sqlite3

conn = sqlite3.connect("my2025tax.sirop")
try:
    rate = get_rate(conn, "USDCAD", date(2025, 3, 15))
except BoCRateError as exc:
    ...  # handle missing rate
"""

from __future__ import annotations

import json
import urllib.request
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3

from sirop.utils.logging import get_logger

logger = get_logger(__name__)

# BoC Valet REST endpoint — no API key required.
_BOC_VALET_URL = "https://www.bankofcanada.ca/valet/observations/{series}/json"

# Maximum number of prior calendar days to look back for weekends/holidays.
_MAX_FALLBACK_DAYS = 3


class BoCRateError(Exception):
    """Raised when no BoC rate can be found for a currency pair and date."""


def get_rate(conn: sqlite3.Connection, currency_pair: str, rate_date: date) -> Decimal:
    """Return the Bank of Canada daily rate for *currency_pair* on *rate_date*.

    Reads from the ``boc_rates`` cache in the active ``.sirop`` database first.
    Falls back to a live HTTP request when the rate is not cached, then writes
    the result to the cache.

    Weekend / holiday fallback: retries up to ``_MAX_FALLBACK_DAYS`` prior
    calendar days when no observation exists for the requested date (the BoC
    publishes rates only on business days).

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    currency_pair:
        BoC series suffix, e.g. ``"USDCAD"`` for the USD→CAD rate.
        The function prepends ``"FX"`` to form the series code ``FXUSDCAD``.
    rate_date:
        The transaction date for which the rate is needed.

    Returns
    -------
    Decimal
        The daily average rate (how many CAD per 1 unit of the foreign currency).

    Raises
    ------
    BoCRateError
        When no rate is available for the requested date after all fallbacks.
    """
    pair_upper = currency_pair.upper()

    # Try each date starting from rate_date, working backwards.
    for offset in range(_MAX_FALLBACK_DAYS + 1):
        query_date = rate_date - timedelta(days=offset)

        # 1. Check cache.
        cached = _read_cached(conn, pair_upper, query_date)
        if cached is not None:
            if offset > 0:
                logger.debug(
                    "boc: used cached rate for %s on %s (fallback from %s, offset=%d)",
                    pair_upper,
                    query_date,
                    rate_date,
                    offset,
                )
            return cached

        # 2. Fetch from API.
        fetched = _fetch_from_api(pair_upper, query_date)
        if fetched is not None:
            _write_cache(conn, pair_upper, query_date, fetched)
            if offset > 0:
                logger.debug(
                    "boc: fetched rate for %s on %s (fallback from %s, offset=%d)",
                    pair_upper,
                    query_date,
                    rate_date,
                    offset,
                )
            else:
                logger.debug("boc: fetched rate for %s on %s", pair_upper, query_date)
            return fetched

    raise BoCRateError(
        f"No Bank of Canada rate found for {pair_upper!r} near {rate_date} "
        f"(checked {_MAX_FALLBACK_DAYS + 1} days back). "
        "The BoC Valet API may be unavailable, or this currency pair is not supported."
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_cached(conn: sqlite3.Connection, pair_upper: str, query_date: date) -> Decimal | None:
    """Return a cached rate from ``boc_rates``, or None if not present."""
    row = conn.execute(
        "SELECT rate FROM boc_rates WHERE currency_pair = ? AND date = ?",
        (pair_upper, str(query_date)),
    ).fetchone()
    if row is None:
        return None
    return Decimal(row[0])


def _write_cache(
    conn: sqlite3.Connection, pair_upper: str, query_date: date, rate: Decimal
) -> None:
    """Insert or replace a rate in the ``boc_rates`` cache."""
    fetched_at = datetime.now(tz=UTC).isoformat()
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO boc_rates (date, currency_pair, rate, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            (str(query_date), pair_upper, format(rate, "f"), fetched_at),
        )


def _fetch_from_api(pair_upper: str, query_date: date) -> Decimal | None:
    """Fetch a single-day rate from the BoC Valet API.

    Returns None when the API returns no observation for the given date
    (weekend, holiday, or unsupported pair).  Raises BoCRateError on
    network / parse errors.
    """
    series = f"FX{pair_upper}"
    url = _BOC_VALET_URL.format(series=series)
    date_str = str(query_date)
    full_url = f"{url}?start_date={date_str}&end_date={date_str}"

    try:
        with urllib.request.urlopen(full_url, timeout=10) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise BoCRateError(f"Failed to fetch BoC rate for {series} on {query_date}: {exc}") from exc

    observations = payload.get("observations", [])
    if not observations:
        return None

    obs = observations[0]
    value_entry = obs.get(series, {})
    raw_value = value_entry.get("v")
    if raw_value is None:
        return None

    try:
        return Decimal(str(raw_value))
    except Exception as exc:
        raise BoCRateError(
            f"Cannot parse BoC rate value {raw_value!r} for {series} on {query_date}"
        ) from exc
