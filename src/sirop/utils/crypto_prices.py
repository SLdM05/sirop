"""Historical BTC price client with SQLite caching.

Fetches daily BTC/USD prices from Mempool.space, then converts to CAD using
the Bank of Canada USDCAD rate.

sirop is BTC-only.  Only ``"BTC"`` is a valid *asset* argument — any other
value raises ``CryptoPriceError`` immediately.

Source: Mempool.space public API (no authentication required, no documented
rate limit).
Endpoint: https://mempool.space/api/v1/historical-price

Usage
-----
from sirop.utils.crypto_prices import get_crypto_price_cad, CryptoPriceError
import sqlite3

conn = sqlite3.connect("my2025tax.sirop")
try:
    price_cad = get_crypto_price_cad(conn, "BTC", date(2025, 3, 15))
except CryptoPriceError as exc:
    ...  # handle missing price
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.utils.boc import BoCRateError, get_rate
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Public API endpoint constants
# ---------------------------------------------------------------------------

_MEMPOOL_URL = "https://mempool.space/api/v1/historical-price"

# HTTP timeout for all requests.
_TIMEOUT = 15


class CryptoPriceError(Exception):
    """Raised when no crypto price can be retrieved for an asset and date."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def get_crypto_price_cad(
    conn: sqlite3.Connection,
    asset: str,
    price_date: date,
) -> Decimal:
    """Return the daily CAD price of *asset* on *price_date*.

    Only ``"BTC"`` is supported — sirop is BTC-only.

    Reads from the ``crypto_prices`` cache in the active ``.sirop`` database
    first.  Falls back to a live Mempool.space request on cache miss, converts
    the fetched USD price to CAD using the BoC USDCAD daily rate, then writes
    both prices to the cache.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    asset:
        Must be ``"BTC"`` (case-insensitive).
    price_date:
        The transaction date for which the price is needed.

    Returns
    -------
    Decimal
        The daily USD price converted to CAD (USD price * BoC USDCAD rate).

    Raises
    ------
    CryptoPriceError
        When *asset* is not BTC, when Mempool.space returns no data, or when
        the BoC USDCAD rate for that date is unavailable.
    """
    asset_upper = asset.upper()
    if asset_upper != "BTC":
        raise CryptoPriceError(f"sirop is BTC-only — price lookup for {asset!r} is not supported.")

    # 1. Cache hit.
    cached = _read_cached(conn, asset_upper, price_date)
    if cached is not None:
        logger.debug("crypto_prices: cache hit for %s on %s", asset_upper, price_date)
        return cached

    # 2. Fetch USD price from Mempool.space.
    usd_price = _fetch_btc_usd_mempool(price_date)
    if usd_price is None:
        raise CryptoPriceError(
            f"No BTC price available for {price_date}. Mempool.space returned no data."
        )

    # 3. Convert to CAD via BoC USDCAD rate.
    try:
        usd_cad = get_rate(conn, "USDCAD", price_date)
    except BoCRateError as exc:
        raise CryptoPriceError(
            f"Cannot convert BTC price to CAD on {price_date}: "
            f"BoC USDCAD rate unavailable — {exc}"
        ) from exc

    price_cad = usd_price * usd_cad

    # 4. Cache.
    _write_cache(conn, asset_upper, price_date, usd_price, price_cad, "mempool")

    logger.debug(
        "crypto_prices: fetched BTC on %s — USD %s → CAD %s (source=mempool)",
        price_date,
        format(usd_price, "f"),
        format(price_cad, "f"),
    )
    return price_cad


# ---------------------------------------------------------------------------
# Mempool.space (BTC only)
# ---------------------------------------------------------------------------


def _fetch_btc_usd_mempool(price_date: date) -> Decimal | None:
    """Fetch BTC/USD price from Mempool.space for *price_date*.

    Uses noon UTC on the requested date as the representative daily price.
    Returns None on any error.
    """
    # Noon UTC gives the most representative mid-day price.
    noon_dt = datetime(price_date.year, price_date.month, price_date.day, 12, 0, 0, tzinfo=UTC)
    noon_utc = int(noon_dt.timestamp())
    url = f"{_MEMPOOL_URL}?currency=USD&timestamp={noon_utc}"

    try:
        with urllib.request.urlopen(url, timeout=_TIMEOUT) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("crypto_prices: mempool.space request failed: %s", exc)
        return None

    # Response format: {"prices": [{"time": <unix>, "USD": <float>, ...}]}
    prices = payload.get("prices", [])
    if not prices:
        logger.debug("crypto_prices: mempool.space returned empty prices for %s", price_date)
        return None

    raw_usd = prices[0].get("USD")
    if raw_usd is None:
        logger.debug("crypto_prices: mempool.space response missing USD field for %s", price_date)
        return None

    try:
        return Decimal(str(raw_usd))
    except Exception as exc:
        logger.debug("crypto_prices: cannot parse mempool.space price %r: %s", raw_usd, exc)
        return None


# ---------------------------------------------------------------------------
# Prefetch helpers
# ---------------------------------------------------------------------------


def prefetch_crypto_prices(
    conn: sqlite3.Connection,
    requests: list[tuple[str, date]],
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int]:
    """Fetch and cache CAD prices for all *(asset, date)* pairs in *requests*.

    Cache hits are skipped — only missing prices trigger network calls.
    Non-BTC assets are skipped with a warning.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    requests:
        List of ``(asset_upper, price_date)`` pairs to warm.
    progress_cb:
        Optional callable invoked after each unique pair is processed.
        Receives ``(processed, total)`` where ``total = len(requests)``.

    Returns
    -------
    tuple[int, int]
        ``(total_fetched, mempool_fetched)`` — total new prices fetched and
        how many came from Mempool.space (cache hits not counted).
    """
    fetched = 0
    mempool_fetched = 0
    total = len(requests)
    processed = 0
    seen: set[tuple[str, date]] = set()
    for asset, price_date in requests:
        asset_upper = asset.upper()
        key = (asset_upper, price_date)
        if key in seen:
            continue
        seen.add(key)
        processed += 1
        if _read_cached(conn, asset_upper, price_date) is not None:
            if progress_cb is not None:
                progress_cb(processed, total)
            continue
        try:
            get_crypto_price_cad(conn, asset_upper, price_date)
            fetched += 1
            row = conn.execute(
                "SELECT source FROM crypto_prices WHERE asset = ? AND date = ?",
                (asset_upper, str(price_date)),
            ).fetchone()
            if row and row[0] == "mempool":
                mempool_fetched += 1
        except CryptoPriceError as exc:
            logger.warning(
                "crypto_prices: prefetch failed for %s on %s — %s",
                asset_upper,
                price_date,
                exc,
            )
        if progress_cb is not None:
            progress_cb(processed, total)
    return fetched, mempool_fetched


# ---------------------------------------------------------------------------
# Cache read / write
# ---------------------------------------------------------------------------


def _read_cached(
    conn: sqlite3.Connection,
    asset: str,
    price_date: date,
) -> Decimal | None:
    """Return a cached CAD price from ``crypto_prices``, or None if absent."""
    row = conn.execute(
        "SELECT price_cad FROM crypto_prices WHERE asset = ? AND date = ?",
        (asset, str(price_date)),
    ).fetchone()
    if row is None:
        return None
    return Decimal(row[0])


def _write_cache(  # noqa: PLR0913
    conn: sqlite3.Connection,
    asset: str,
    price_date: date,
    price_usd: Decimal,
    price_cad: Decimal,
    source: str,
) -> None:
    """Insert or replace a price entry in ``crypto_prices``."""
    fetched_at = datetime.now(tz=UTC).isoformat()
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO crypto_prices
                (date, asset, price_usd, price_cad, source, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(price_date),
                asset,
                format(price_usd, "f"),
                format(price_cad, "f"),
                source,
                fetched_at,
            ),
        )
