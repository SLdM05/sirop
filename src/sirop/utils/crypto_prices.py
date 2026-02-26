"""Historical cryptocurrency price client with SQLite caching.

Fetches daily USD prices for crypto assets from public, no-auth APIs,
then converts to CAD using the Bank of Canada USDCAD rate.

Two sources are supported:
- **Mempool.space** (BTC only) — BTC-native, already used by the node
  verification module.  No documented rate limit.
  Endpoint: https://mempool.space/api/v1/historical-price
- **CoinGecko** (any supported asset) — free public tier, no API key.
  Rate limit: ~30 req/min.  Handled by exponential backoff on HTTP 429.
  Endpoint: https://api.coingecko.com/api/v3/coins/{id}/history

Fetch strategy for BTC:
  1. Try Mempool.space.
  2. On failure, fall back to CoinGecko.

Fetch strategy for other assets:
  1. CoinGecko only.

Asset → CoinGecko ID mapping is read from ``config/currencies.yaml``
(``crypto.<ASSET>.coingecko_id``). Assets not in that file raise
``CryptoPriceError``.

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
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from sirop.utils.boc import BoCRateError, get_rate
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    import sqlite3

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Public API endpoint constants — no auth required for either.
# ---------------------------------------------------------------------------

_MEMPOOL_URL = "https://mempool.space/api/v1/historical-price"
_COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/history"

# HTTP timeout for all requests.
_TIMEOUT = 15

# HTTP status code for rate limiting.
_HTTP_TOO_MANY_REQUESTS = 429

# Exponential back-off delays (seconds) for HTTP 429 / transient errors.
_BACKOFF_DELAYS = (2, 4, 8)

# Path to the currencies config relative to the project root.
# Resolved at module level so it fails fast if missing.
_CURRENCIES_YAML = Path(__file__).parent.parent.parent.parent / "config" / "currencies.yaml"


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

    Reads from the ``crypto_prices`` cache in the active ``.sirop`` database
    first.  Falls back to a live HTTP request on cache miss, converts the
    fetched USD price to CAD using the BoC USDCAD daily rate, then writes
    both prices to the cache.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    asset:
        Uppercase asset symbol, e.g. ``"BTC"`` or ``"ETH"``.
    price_date:
        The transaction date for which the price is needed.

    Returns
    -------
    Decimal
        The daily USD price converted to CAD (USD price * BoC USDCAD rate).

    Raises
    ------
    CryptoPriceError
        When no price can be fetched from any available source, or when the
        BoC USDCAD rate for that date is unavailable.
    """
    asset_upper = asset.upper()

    # 1. Cache hit.
    cached = _read_cached(conn, asset_upper, price_date)
    if cached is not None:
        logger.debug("crypto_prices: cache hit for %s on %s", asset_upper, price_date)
        return cached

    # 2. Fetch USD price.
    usd_price, source = _fetch_usd_price(asset_upper, price_date)

    # 3. Convert to CAD via BoC USDCAD rate.
    try:
        usd_cad = get_rate(conn, "USDCAD", price_date)
    except BoCRateError as exc:
        raise CryptoPriceError(
            f"Cannot convert {asset_upper} price to CAD on {price_date}: "
            f"BoC USDCAD rate unavailable — {exc}"
        ) from exc

    price_cad = usd_price * usd_cad

    # 4. Cache.
    _write_cache(conn, asset_upper, price_date, usd_price, price_cad, source)

    logger.debug(
        "crypto_prices: fetched %s on %s — USD %s → CAD %s (source=%s)",
        asset_upper,
        price_date,
        format(usd_price, "f"),
        format(price_cad, "f"),
        source,
    )
    return price_cad


# ---------------------------------------------------------------------------
# Fetch dispatch
# ---------------------------------------------------------------------------


def _fetch_usd_price(asset: str, price_date: date) -> tuple[Decimal, str]:
    """Return ``(usd_price, source_name)`` for *asset* on *price_date*.

    For BTC: tries Mempool.space first, then CoinGecko.
    For other assets: CoinGecko only.

    Raises
    ------
    CryptoPriceError
        When all available sources fail.
    """
    cfg = _load_asset_config(asset)
    mempool_ok = cfg.get("mempool_supported", False)

    if mempool_ok:
        usd = _fetch_btc_usd_mempool(price_date)
        if usd is not None:
            return usd, "mempool"
        logger.debug(
            "crypto_prices: mempool.space failed for %s on %s — trying CoinGecko",
            asset,
            price_date,
        )

    coin_id = str(cfg["coingecko_id"])
    usd = _fetch_usd_coingecko(coin_id, price_date)
    if usd is not None:
        return usd, "coingecko"

    raise CryptoPriceError(
        f"No price available for {asset} on {price_date}. "
        "Both Mempool.space and CoinGecko returned no data."
        if mempool_ok
        else f"No price available for {asset} on {price_date}. CoinGecko returned no data."
    )


# ---------------------------------------------------------------------------
# Mempool.space (BTC only)
# ---------------------------------------------------------------------------


def _fetch_btc_usd_mempool(price_date: date) -> Decimal | None:
    """Fetch BTC/USD price from Mempool.space for *price_date*.

    Uses noon UTC on the requested date as the representative daily price.
    Returns None on any error so the caller can fall back to CoinGecko.
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
# CoinGecko (any supported asset)
# ---------------------------------------------------------------------------


def _fetch_usd_coingecko(coin_id: str, price_date: date) -> Decimal | None:
    """Fetch the USD price of *coin_id* on *price_date* from CoinGecko.

    Date must be passed in CoinGecko's required format: DD-MM-YYYY.
    Returns None when no market data is available; raises CryptoPriceError
    on unrecoverable errors (network, parse failures after all retries).
    """
    from sirop.config.settings import get_settings  # noqa: PLC0415 - avoid circular import

    date_str = price_date.strftime("%d-%m-%Y")
    url = f"{_COINGECKO_URL.format(coin_id=coin_id)}?date={date_str}&localization=false"
    api_key = get_settings().coingecko_api_key

    try:
        raw = _fetch_with_backoff(url, api_key=api_key)
    except CryptoPriceError as exc:
        logger.debug(
            "crypto_prices: CoinGecko request failed for %s on %s: %s",
            coin_id,
            price_date,
            exc,
        )
        return None

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        logger.debug(
            "crypto_prices: CoinGecko JSON parse error for %s on %s: %s",
            coin_id,
            price_date,
            exc,
        )
        return None

    try:
        usd_price = payload["market_data"]["current_price"]["usd"]
        return Decimal(str(usd_price))
    except (KeyError, TypeError):
        # CoinGecko returns no market_data for very old or unsupported dates.
        logger.debug(
            "crypto_prices: CoinGecko has no market data for %s on %s", coin_id, price_date
        )
        return None
    except Exception as exc:
        logger.debug(
            "crypto_prices: cannot parse CoinGecko price for %s on %s: %s",
            coin_id,
            price_date,
            exc,
        )
        return None


def _fetch_with_backoff(url: str, api_key: str = "") -> bytes:
    """Fetch *url*, retrying on HTTP 429 with exponential back-off.

    Returns raw response bytes on success.
    Raises ``CryptoPriceError`` after all retries are exhausted or on
    non-retryable HTTP errors.

    Parameters
    ----------
    url:
        Fully-formed URL to fetch.
    api_key:
        Optional CoinGecko Demo API key.  When non-empty, sent as the
        ``x-cg-demo-api-key`` request header, which gives a higher rate
        limit and more reliable responses than the unauthenticated public
        API.
    """
    headers: dict[str, str] = {}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    for attempt, wait in enumerate((*_BACKOFF_DELAYS, None), start=1):
        req = urllib.request.Request(url, headers=headers)  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:  # noqa: S310
                return resp.read()  # type: ignore[no-any-return]
        except urllib.error.HTTPError as exc:
            if exc.code == _HTTP_TOO_MANY_REQUESTS:
                if wait is None:
                    raise CryptoPriceError(
                        f"CoinGecko rate limit exceeded after {attempt} attempts for {url}"
                    ) from exc
                logger.debug(
                    "crypto_prices: CoinGecko 429 on attempt %d — waiting %ds", attempt, wait
                )
                time.sleep(wait)
            else:
                raise CryptoPriceError(
                    f"CoinGecko HTTP {exc.code} for {url}: {exc.reason}"
                ) from exc
        except Exception as exc:
            raise CryptoPriceError(f"CoinGecko request failed for {url}: {exc}") from exc

    # Unreachable — the loop always raises before exhausting without returning.
    raise CryptoPriceError(f"CoinGecko request failed for {url}")  # pragma: no cover


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_asset_config(asset: str) -> dict[str, object]:
    """Load config for *asset* from currencies.yaml.

    Returns the dict under ``crypto.<ASSET>``.

    Raises
    ------
    CryptoPriceError
        When *asset* is not listed or currencies.yaml is missing.
    """
    try:
        with _CURRENCIES_YAML.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except FileNotFoundError as exc:
        raise CryptoPriceError(
            f"currencies.yaml not found at {_CURRENCIES_YAML}. "
            "Cannot look up CoinGecko ID for asset prices."
        ) from exc
    except Exception as exc:
        raise CryptoPriceError(f"Failed to load currencies.yaml: {exc}") from exc

    crypto_section = (data or {}).get("crypto", {})
    cfg = crypto_section.get(asset)
    if cfg is None:
        raise CryptoPriceError(
            f"Asset {asset!r} not found in currencies.yaml crypto section. "
            "Add it with a coingecko_id to enable price lookups."
        )
    if not isinstance(cfg, dict):
        raise CryptoPriceError(
            f"currencies.yaml: crypto.{asset} must be a mapping, got {type(cfg).__name__!r}"
        )
    return cfg


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


def prefetch_crypto_prices(
    conn: sqlite3.Connection,
    requests: list[tuple[str, date]],
) -> tuple[int, int]:
    """Fetch and cache CAD prices for all *(asset, date)* pairs in *requests*.

    Cache hits are skipped — only missing prices trigger network calls.
    CoinGecko calls are serialised (one per unique date for non-BTC assets)
    and use the existing back-off on HTTP 429.  Mempool.space calls for BTC
    have no documented rate limit and are interleaved naturally.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    requests:
        List of ``(asset_upper, price_date)`` pairs to warm.

    Returns
    -------
    tuple[int, int]
        ``(total_fetched, coingecko_fetched)`` — total new prices fetched and
        how many of those came from CoinGecko (cache hits not counted).
    """
    fetched = 0
    coingecko_fetched = 0
    seen: set[tuple[str, date]] = set()
    for asset, price_date in requests:
        asset_upper = asset.upper()
        key = (asset_upper, price_date)
        if key in seen:
            continue
        seen.add(key)
        if _read_cached(conn, asset_upper, price_date) is not None:
            continue
        try:
            get_crypto_price_cad(conn, asset_upper, price_date)
            fetched += 1
            row = conn.execute(
                "SELECT source FROM crypto_prices WHERE asset = ? AND date = ?",
                (asset_upper, str(price_date)),
            ).fetchone()
            if row and row[0] == "coingecko":
                coingecko_fetched += 1
        except CryptoPriceError as exc:
            logger.warning(
                "crypto_prices: prefetch failed for %s on %s — %s",
                asset_upper,
                price_date,
                exc,
            )
    return fetched, coingecko_fetched


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
