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


def prefetch_rates(
    conn: sqlite3.Connection,
    currency_pair: str,
    start_date: date,
    end_date: date,
) -> int:
    """Fetch and cache all BoC rates for *currency_pair* in a single HTTP call.

    Makes one request to the BoC Valet API for the full date range and writes
    every returned observation to the ``boc_rates`` cache in a single SQLite
    transaction.  Subsequent ``get_rate`` calls for any date in the range will
    be served from the cache without touching the network.

    Weekend and holiday dates are absent from the BoC response and are silently
    skipped here; the ``get_rate`` fallback handles them at lookup time.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    currency_pair:
        BoC series suffix, e.g. ``"USDCAD"``.
    start_date:
        First date of the range (inclusive).
    end_date:
        Last date of the range (inclusive).

    Returns
    -------
    int
        Number of rates written to the cache.

    Raises
    ------
    BoCRateError
        On network or parse errors.
    """
    pair_upper = currency_pair.upper()
    rates = _fetch_range_from_api(pair_upper, start_date, end_date)
    if not rates:
        return 0
    fetched_at = datetime.now(tz=UTC).isoformat()
    with conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO boc_rates (date, currency_pair, rate, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            [(str(d), pair_upper, format(r, "f"), fetched_at) for d, r in rates.items()],
        )
    logger.debug(
        "boc: prefetched %d rates for %s (%s to %s)",
        len(rates),
        pair_upper,
        start_date,
        end_date,
    )
    return len(rates)


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
    rates = _fetch_range_from_api(pair_upper, query_date, query_date)
    return rates.get(query_date)


def _fetch_range_from_api(pair_upper: str, start_date: date, end_date: date) -> dict[date, Decimal]:
    """Fetch all BoC observations for *pair_upper* between *start_date* and *end_date*.

    Returns a dict mapping each observed date to its rate.  Weekend and holiday
    dates will be absent — the BoC publishes no observation for those days.
    Raises BoCRateError on network or parse errors.
    """
    series = f"FX{pair_upper}"
    url = f"{_BOC_VALET_URL.format(series=series)}" f"?start_date={start_date}&end_date={end_date}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise BoCRateError(
            f"Failed to fetch BoC rates for {series} from {start_date} to {end_date}: {exc}"
        ) from exc

    result: dict[date, Decimal] = {}
    for obs in payload.get("observations", []):
        date_str = obs.get("d")
        raw_value = obs.get(series, {}).get("v")
        if date_str is None or raw_value is None:
            continue
        try:
            result[date.fromisoformat(date_str)] = Decimal(str(raw_value))
        except Exception as exc:
            raise BoCRateError(f"Cannot parse BoC observation {obs!r} for {series}: {exc}") from exc
    return result
