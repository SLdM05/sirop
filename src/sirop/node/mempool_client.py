"""Mempool.space REST API client for on-chain transaction lookup.

Provides two public functions:

- ``fetch_tx(base_url, txid)``  → ``OnChainTx | None``
- ``fetch_outspends(base_url, txid)``  → ``list[TxOutspend]``

Both use only the Python standard library (``urllib.request``) — no
third-party HTTP dependencies are added.

Error handling contract:
- HTTP 404 / unknown txid   → return ``None`` / ``[]`` (not an exception)
- HTTP 5xx                  → one retry after 2 s; then return ``None`` / ``[]``
- Network / parse errors    → return ``None`` / ``[]`` (caller should log)

The public Mempool.space instance at https://mempool.space/api is used
when ``BTC_MEMPOOL_URL`` is not set.  A self-hosted instance is
recommended for production use to avoid rate limiting.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Any

from sirop.node.models import OnChainTx, TxOutspend
from sirop.utils.logging import get_logger

logger = get_logger(__name__)

# Seconds to wait before retrying a 5xx response.
_RETRY_DELAY_SECONDS: float = 2.0

# Timeout in seconds for each HTTP request.
_REQUEST_TIMEOUT_SECONDS: float = 10.0

_HTTP_NOT_FOUND: int = 404
_HTTP_SERVER_ERROR: int = 500


def fetch_tx(base_url: str, txid: str) -> OnChainTx | None:
    """Fetch a transaction by txid from the Mempool REST API.

    ``GET {base_url}/tx/{txid}``

    Returns ``None`` on any error (404, network failure, parse error).
    The caller is responsible for logging the context when ``None`` is
    returned.
    """
    url = f"{base_url.rstrip('/')}/tx/{txid}"
    data = _get_json(url)
    if data is None:
        return None
    return _parse_tx(data)


def fetch_outspends(base_url: str, txid: str) -> list[TxOutspend]:
    """Fetch the outspend status of all outputs for a transaction.

    ``GET {base_url}/tx/{txid}/outspends``

    Returns an empty list on any error.  Each element of the returned
    list corresponds to the output at that index (vout).
    """
    url = f"{base_url.rstrip('/')}/tx/{txid}/outspends"
    data = _get_json(url)
    if not isinstance(data, list):
        return []
    return [_parse_outspend(item) for item in data if isinstance(item, dict)]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_json(url: str, *, _retry: bool = True) -> Any:
    """Perform a GET request and return the parsed JSON body.

    Returns ``None`` on 404, network failure, or JSON parse error.
    Retries once on 5xx after ``_RETRY_DELAY_SECONDS``.
    """
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})  # noqa: S310
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_SECONDS) as resp:  # noqa: S310
            raw = resp.read()
        return json.loads(raw)
    except urllib.error.HTTPError as exc:
        if exc.code == _HTTP_NOT_FOUND:
            logger.debug("mempool_client: 404 for %s — txid not found", url)
            return None
        if exc.code >= _HTTP_SERVER_ERROR and _retry:
            logger.debug(
                "mempool_client: %d for %s — retrying in %.0fs",
                exc.code,
                url,
                _RETRY_DELAY_SECONDS,
            )
            time.sleep(_RETRY_DELAY_SECONDS)
            return _get_json(url, _retry=False)
        logger.debug("mempool_client: HTTP %d for %s", exc.code, url)
        return None
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        logger.debug("mempool_client: request failed for %s — %s", url, exc)
        return None


def _parse_tx(data: dict[str, Any]) -> OnChainTx | None:
    """Parse a Mempool transaction JSON blob into an ``OnChainTx``."""
    try:
        txid: str = data["txid"]
        fee_sat: int | None = data.get("fee")
        status: dict[str, Any] = data.get("status", {})
        confirmed: bool = bool(status.get("confirmed", False))
        block_time_raw: int | None = status.get("block_time")
        block_time: datetime | None = (
            datetime.fromtimestamp(block_time_raw, tz=UTC) if block_time_raw else None
        )
        vin: list[dict[str, Any]] = data.get("vin", [])
        vin_txids: tuple[str, ...] = tuple(
            entry["txid"] for entry in vin if isinstance(entry.get("txid"), str)
        )
        vout: list[Any] = data.get("vout", [])
        return OnChainTx(
            txid=txid,
            fee_sat=fee_sat,
            confirmed=confirmed,
            block_time=block_time,
            vin_txids=vin_txids,
            vout_count=len(vout),
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.debug("mempool_client: failed to parse tx JSON — %s", exc)
        return None


def _parse_outspend(data: dict[str, Any]) -> TxOutspend:
    """Parse one element from the outspends array."""
    spent: bool = bool(data.get("spent", False))
    txid: str | None = data.get("txid") or None
    vin: int | None = data.get("vin")
    return TxOutspend(spent=spent, txid=txid, vin=vin)
