"""Historical cryptocurrency price client with SQLite caching.

Fetches daily USD prices for crypto assets from public, no-auth APIs,
then converts to CAD using the Bank of Canada USDCAD rate.

Three sources are supported:
- **Mempool.space** (BTC only) — BTC-native, already used by the node
  verification module.  No documented rate limit.
  Endpoint: https://mempool.space/api/v1/historical-price
- **CoinGecko** (any supported asset) — requires a free Demo API key set via
  COINGECKO_API_KEY in .env.  Rate limit: ~30 req/min; handled by exponential
  backoff on HTTP 429.  Demo plan covers the last 365 days of history.
  Endpoint: https://api.coingecko.com/api/v3/coins/{id}/history
  Attribution: CoinGecko (https://www.coingecko.com)
- **Kraken** (any asset with a ``kraken_pair`` in currencies.yaml) — free
  public OHLC endpoint, no authentication required.  Returns up to 720 daily
  candles per call; VWAP of the matching candle is used as the daily price.
  Used as a fallback when CoinGecko is unavailable or lacks data (e.g. dates
  older than 365 days on the Demo plan).
  Endpoint: https://api.kraken.com/0/public/OHLC
  Attribution: Kraken Exchange public REST API
  (https://docs.kraken.com/api/docs/rest-api/get-ohlc-data)

Fetch strategy for BTC:
  1. Try Mempool.space.
  2. On failure, fall back to CoinGecko.
  3. On CoinGecko failure, fall back to Kraken.

Fetch strategy for other assets:
  1. Try CoinGecko.
  2. On failure, fall back to Kraken.

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

from sirop.config.settings import get_settings
from sirop.utils.boc import BoCRateError, get_rate
from sirop.utils.logging import get_logger

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Public API endpoint constants — no auth required for either.
# ---------------------------------------------------------------------------

_MEMPOOL_URL = "https://mempool.space/api/v1/historical-price"
_COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/history"
_COINGECKO_CHART_URL = "https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
# Kraken public OHLC endpoint — no authentication required.
# Attribution: Kraken Exchange REST API
# https://docs.kraken.com/api/docs/rest-api/get-ohlc-data
_KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"

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

    kraken_pair = str(cfg.get("kraken_pair", ""))
    if kraken_pair:
        logger.debug(
            "crypto_prices: CoinGecko unavailable for %s on %s — trying Kraken (%s)",
            asset,
            price_date,
            kraken_pair,
        )
        usd = _fetch_usd_kraken(kraken_pair, price_date)
        if usd is not None:
            return usd, "kraken"

    sources = "Mempool.space, CoinGecko, and Kraken" if mempool_ok else "CoinGecko and Kraken"
    if not kraken_pair:
        sources = "Mempool.space and CoinGecko" if mempool_ok else "CoinGecko"
    raise CryptoPriceError(
        f"No price available for {asset} on {price_date}. {sources} returned no data."
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


def _fetch_coingecko_range(
    coin_id: str,
    start_date: date,
    end_date: date,
) -> dict[date, Decimal]:
    """Fetch daily USD prices for *coin_id* over a date range using CoinGecko.

    Uses the ``/market_chart/range`` endpoint which returns all prices in the
    range in a single HTTP call — far more efficient than one call per date.

    For ranges > 90 days CoinGecko returns daily granularity (one price per
    day).  For shorter ranges it returns hourly data; in that case this
    function takes the last price observed on each calendar day (UTC).

    Parameters
    ----------
    coin_id:
        CoinGecko coin identifier, e.g. ``"bitcoin"``.
    start_date:
        First date of the range (inclusive, UTC).
    end_date:
        Last date of the range (inclusive, UTC).

    Returns
    -------
    dict[date, Decimal]
        Mapping of UTC date → USD price.  Dates with no data are absent.

    Raises
    ------
    CryptoPriceError
        On network errors or unrecoverable HTTP failures after retries.
    """
    start_ts = int(
        datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC).timestamp()
    )
    # Use end of day so the end_date itself is included in the response.
    end_ts = int(
        datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=UTC).timestamp()
    )

    url = (
        f"{_COINGECKO_CHART_URL.format(coin_id=coin_id)}"
        f"?vs_currency=usd&from={start_ts}&to={end_ts}"
    )
    api_key = get_settings().coingecko_api_key

    try:
        raw = _fetch_with_backoff(url, api_key=api_key)
    except CryptoPriceError as exc:
        raise CryptoPriceError(
            f"CoinGecko range fetch failed for {coin_id} " f"({start_date} to {end_date}): {exc}"
        ) from exc

    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise CryptoPriceError(
            f"CoinGecko range response parse error for {coin_id}: {exc}"
        ) from exc

    # For each calendar day (UTC) take the last price point observed that day.
    daily: dict[date, Decimal] = {}
    for ts_ms, price_float in payload.get("prices", []):
        d = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date()
        daily[d] = Decimal(str(price_float))

    return daily


def _fetch_usd_kraken(kraken_pair: str, price_date: date) -> Decimal | None:
    """Fetch the USD VWAP of *kraken_pair* on *price_date* from Kraken's OHLC API.

    Uses daily candles (interval=1440).  The VWAP field (index 5) of the
    matching candle is returned as the representative daily price.

    Kraken's public OHLC endpoint requires no authentication and returns up to
    720 candles per call.

    Attribution: Kraken Exchange public REST API
    https://docs.kraken.com/api/docs/rest-api/get-ohlc-data

    Returns None on any error so the caller can propagate the failure.
    """
    midnight_ts = int(
        datetime(price_date.year, price_date.month, price_date.day, tzinfo=UTC).timestamp()
    )
    # Kraken's ``since`` is exclusive — subtract 1 so the first candle returned
    # is the one that opens exactly at midnight on price_date.
    url = f"{_KRAKEN_OHLC_URL}?pair={kraken_pair}&interval=1440&since={midnight_ts - 1}"

    try:
        with urllib.request.urlopen(url, timeout=_TIMEOUT) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug(
            "crypto_prices: Kraken request failed for %s on %s: %s", kraken_pair, price_date, exc
        )
        return None

    if payload.get("error"):
        logger.debug("crypto_prices: Kraken API error for %s: %s", kraken_pair, payload["error"])
        return None

    # The result key is Kraken's canonical pair name (may differ from input).
    result = {k: v for k, v in payload.get("result", {}).items() if k != "last"}
    if not result:
        logger.debug(
            "crypto_prices: Kraken returned no candles for %s on %s", kraken_pair, price_date
        )
        return None

    candles = next(iter(result.values()))
    for candle in candles:
        if candle[0] == midnight_ts:
            try:
                return Decimal(str(candle[5]))  # VWAP
            except Exception as exc:
                logger.debug(
                    "crypto_prices: cannot parse Kraken VWAP for %s on %s: %s",
                    kraken_pair,
                    price_date,
                    exc,
                )
                return None

    logger.debug("crypto_prices: Kraken has no candle matching %s for %s", price_date, kraken_pair)
    return None


def _fetch_kraken_range(
    kraken_pair: str,
    start_date: date,
    end_date: date,
) -> dict[date, Decimal]:
    """Fetch daily USD VWAP prices for *kraken_pair* over a date range from Kraken.

    Uses daily candles (interval=1440).  Kraken returns up to 720 candles per
    call (~2 years), so a single request covers the full range in almost all
    practical cases.  The VWAP field (index 5) is used as the daily price.

    Attribution: Kraken Exchange public REST API
    https://docs.kraken.com/api/docs/rest-api/get-ohlc-data

    Returns
    -------
    dict[date, Decimal]
        UTC date → USD VWAP price.  Dates with no candle are absent.

    Raises
    ------
    CryptoPriceError
        On network errors or unparseable responses.
    """
    since_ts = (
        int(datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC).timestamp()) - 1
    )  # exclusive lower bound

    url = f"{_KRAKEN_OHLC_URL}?pair={kraken_pair}&interval=1440&since={since_ts}"

    try:
        with urllib.request.urlopen(url, timeout=_TIMEOUT) as resp:  # noqa: S310
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise CryptoPriceError(
            f"Kraken request failed for {kraken_pair} ({start_date} to {end_date}): {exc}"
        ) from exc

    if payload.get("error"):
        raise CryptoPriceError(f"Kraken API error for {kraken_pair}: {payload['error']}")

    result = {k: v for k, v in payload.get("result", {}).items() if k != "last"}
    if not result:
        raise CryptoPriceError(
            f"Kraken returned no candles for {kraken_pair} ({start_date} to {end_date})"
        )

    candles = next(iter(result.values()))
    end_ts = int(
        datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=UTC).timestamp()
    )
    daily: dict[date, Decimal] = {}
    for candle in candles:
        ts = candle[0]
        if ts > end_ts:
            break
        d = datetime.fromtimestamp(ts, tz=UTC).date()
        if d < start_date:
            continue
        try:
            daily[d] = Decimal(str(candle[5]))  # VWAP
        except Exception as exc:
            logger.debug("crypto_prices: cannot parse Kraken VWAP in range for %s: %s", d, exc)
            continue

    return daily


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


def prefetch_crypto_prices_by_range(
    conn: sqlite3.Connection,
    asset_dates: dict[str, list[date]],
) -> tuple[int, int]:
    """Prefetch crypto CAD prices using one CoinGecko range call per asset.

    More efficient than :func:`prefetch_crypto_prices` for large date sets —
    O(N_assets) HTTP calls instead of O(N_dates x N_assets).

    Assets with ``mempool_supported: true`` in ``currencies.yaml`` (currently
    only BTC) are **skipped** here.  Mempool.space is preferred for those assets
    because it has no documented rate limit.  They are handled by the per-date
    fallback in :func:`prefetch_crypto_prices` where mempool is tried first.

    For each eligible asset, one ``/market_chart/range`` call covers the full
    span of requested dates.  Results are written to the ``crypto_prices`` cache
    with ``source="coingecko"``; subsequent ``prefetch_crypto_prices`` or
    ``get_crypto_price_cad`` calls will hit the cache for those dates.

    Requires the BoC USDCAD rates for the date range to already be cached
    (call ``prefetch_rates`` first).  Dates where the BoC rate is still missing
    are skipped silently — the per-date fallback handles them.

    On failure for any asset, logs a warning and continues; the per-date
    fallback in :func:`prefetch_crypto_prices` will fill remaining gaps.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    asset_dates:
        Mapping of uppercase asset symbol to the list of required dates.

    Returns
    -------
    tuple[int, int]
        ``(total_written, coingecko_count)`` — total prices written to cache
        and how many came from CoinGecko (always equal; mempool not used here).
    """
    total_written = 0
    coingecko_count = 0

    for asset, dates in asset_dates.items():
        if not dates:
            continue
        try:
            cfg = _load_asset_config(asset)
        except CryptoPriceError:
            logger.debug("crypto_prices: no config for %s — skipping range prefetch", asset)
            continue
        if cfg.get("mempool_supported", False):
            logger.debug(
                "crypto_prices: skipping range prefetch for %s — mempool preferred",
                asset,
            )
            continue

        coin_id = str(cfg["coingecko_id"])
        start_date = min(dates)
        end_date = max(dates)
        date_set = frozenset(dates)

        try:
            usd_prices = _fetch_coingecko_range(coin_id, start_date, end_date)
        except CryptoPriceError as exc:
            logger.warning(
                "crypto_prices: range prefetch failed for %s (%s to %s) — %s",
                asset,
                start_date,
                end_date,
                exc,
            )
            kraken_pair = str(cfg.get("kraken_pair", ""))
            if kraken_pair:
                logger.warning(
                    "crypto_prices: trying Kraken range fallback for %s (%s)",
                    asset,
                    kraken_pair,
                )
                try:
                    usd_prices = _fetch_kraken_range(kraken_pair, start_date, end_date)
                except CryptoPriceError as exc2:
                    logger.warning(
                        "crypto_prices: Kraken range fallback also failed for %s (%s to %s) — %s. "
                        "Per-date fallback will handle remaining gaps.",
                        asset,
                        start_date,
                        end_date,
                        exc2,
                    )
                    continue
            else:
                logger.warning(
                    "crypto_prices: no Kraken pair configured for %s — "
                    "per-date fallback will handle remaining gaps.",
                    asset,
                )
                continue

        for d, usd_price in usd_prices.items():
            if d not in date_set:
                continue
            if _read_cached(conn, asset, d) is not None:
                continue
            try:
                usd_cad = get_rate(conn, "USDCAD", d)
            except BoCRateError:
                logger.debug(
                    "crypto_prices: no BoC USDCAD rate for %s — skipping range cache write",
                    d,
                )
                continue
            _write_cache(conn, asset, d, usd_price, usd_price * usd_cad, "coingecko")
            total_written += 1
            coingecko_count += 1

        logger.debug(
            "crypto_prices: range prefetch wrote %d price(s) for %s (%s to %s)",
            coingecko_count,
            asset,
            start_date,
            end_date,
        )

    return total_written, coingecko_count


def prefetch_crypto_prices(
    conn: sqlite3.Connection,
    requests: list[tuple[str, date]],
    progress_cb: Callable[[int, int], None] | None = None,
) -> tuple[int, int, int]:
    """Fetch and cache CAD prices for all *(asset, date)* pairs in *requests*.

    Cache hits are skipped — only missing prices trigger network calls.
    CoinGecko calls are serialised (one per unique date for non-BTC assets)
    and use the existing back-off on HTTP 429.  Mempool.space calls for BTC
    have no documented rate limit and are interleaved naturally.

    Typically called after :func:`prefetch_crypto_prices_by_range` to fill
    any gaps the range call did not cover.

    Parameters
    ----------
    conn:
        Open SQLite connection to the active ``.sirop`` batch file.
    requests:
        List of ``(asset_upper, price_date)`` pairs to warm.
    progress_cb:
        Optional callable invoked after each unique pair is processed
        (whether fetched or a cache hit).  Receives ``(processed, total)``
        where ``total = len(requests)``.  Used for live progress display.

    Returns
    -------
    tuple[int, int, int]
        ``(total_fetched, coingecko_fetched, mempool_fetched)`` — total new
        prices fetched and how many came from each source (cache hits not
        counted).
    """
    fetched = 0
    coingecko_fetched = 0
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
            if row and row[0] == "coingecko":
                coingecko_fetched += 1
            elif row and row[0] == "mempool":
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
    return fetched, coingecko_fetched, mempool_fetched


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
