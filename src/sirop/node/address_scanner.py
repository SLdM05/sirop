"""BIP32/49/84 address derivation and xpub wallet scanning via Mempool API."""

from __future__ import annotations

import json
import logging
import ssl
import time
import urllib.request
from dataclasses import dataclass
from typing import Any

from bip_utils import (  # type: ignore[import-untyped]
    Bip44,
    Bip44Changes,
    Bip44Coins,
    Bip49,
    Bip49Coins,
    Bip84,
    Bip84Coins,
)

from sirop.node.privacy import is_private_node_url

logger = logging.getLogger(__name__)

_MEMPOOL_PAGE_SIZE = 25
_HTTP_NOT_FOUND = 404
_HTTP_SERVER_ERROR = 500


@dataclass(frozen=True)
class ScannedTx:
    """One transaction observed on a derived address."""

    txid: str
    net_sats: int  # satoshis — integers by protocol definition (no float risk)
    fee_sats: int  # satoshis — converted to Decimal in XpubImporter._to_raw_transaction
    block_time: int  # Unix timestamp; 0 for unconfirmed
    confirmed: bool


def derive_address(xpub: str, branch: int, index: int) -> str:
    """Return the Bitcoin address at path m/.../branch/index from an account xpub.

    Supports:
      ``zpub`` -> P2WPKH native SegWit (BIP84)
      ``ypub`` -> P2SH-P2WPKH wrapped SegWit (BIP49)
      ``xpub`` -> P2PKH legacy (BIP44)

    Raises:
        ValueError: Unrecognised xpub prefix.
    """
    prefix = xpub[:4]
    if prefix == "zpub":
        change = Bip44Changes.CHAIN_EXT if branch == 0 else Bip44Changes.CHAIN_INT
        ctx = Bip84.FromExtendedKey(xpub, Bip84Coins.BITCOIN)
        return str(ctx.Change(change).AddressIndex(index).PublicKey().ToAddress())
    if prefix == "ypub":
        change = Bip44Changes.CHAIN_EXT if branch == 0 else Bip44Changes.CHAIN_INT
        ctx = Bip49.FromExtendedKey(xpub, Bip49Coins.BITCOIN)
        return str(ctx.Change(change).AddressIndex(index).PublicKey().ToAddress())
    if prefix == "xpub":
        change = Bip44Changes.CHAIN_EXT if branch == 0 else Bip44Changes.CHAIN_INT
        ctx = Bip44.FromExtendedKey(xpub, Bip44Coins.BITCOIN)
        return str(ctx.Change(change).AddressIndex(index).PublicKey().ToAddress())
    raise ValueError(f"Unsupported xpub prefix: {prefix!r} — expected zpub, ypub, or xpub")


def scan_wallet(
    mempool_url: str,
    xpub: str,
    branches: list[int],
    gap_limit: int,
    request_delay: float = 0.0,
) -> list[ScannedTx]:
    """Derive addresses and scan tx history with gap-limit logic via Mempool API.

    Parameters
    ----------
    request_delay:
        Seconds to sleep between each address HTTP request. Sourced from
        ``BTC_TRAVERSAL_REQUEST_DELAY`` in settings. Use 0.05-0.1 for public
        endpoints to avoid rate limiting; 0.0 is fine for a local node.
    """
    for b in branches:
        if b not in (0, 1):
            raise ValueError(f"Invalid branch {b!r} — must be 0 (external) or 1 (internal)")

    private = is_private_node_url(mempool_url)

    all_addresses: set[str] = set()
    raw_txs: dict[str, Any] = {}

    for branch in branches:
        gap = 0
        idx = 0
        while gap < gap_limit:
            addr = derive_address(xpub, branch, idx)
            all_addresses.add(addr)
            addr_txs = _fetch_address_txs(mempool_url, addr, private, request_delay)
            if addr_txs:
                for raw_tx in addr_txs:
                    raw_txs[raw_tx["txid"]] = raw_tx
                gap = 0
            else:
                gap += 1
            idx += 1

    result: list[ScannedTx] = []
    for txid, raw_tx in raw_txs.items():
        received = sum(
            vout.get("value", 0)
            for vout in raw_tx.get("vout", [])
            if vout.get("scriptpubkey_address") in all_addresses
        )
        spent = sum(
            vin.get("prevout", {}).get("value", 0)
            for vin in raw_tx.get("vin", [])
            if vin.get("prevout", {}).get("scriptpubkey_address") in all_addresses
        )
        net_sats = received - spent
        if net_sats == 0:
            continue

        fee_sats = raw_tx.get("fee", 0) if net_sats < 0 else 0
        status = raw_tx.get("status", {})
        confirmed = bool(status.get("confirmed", False))
        block_time = int(status.get("block_time", 0)) if confirmed else 0

        result.append(
            ScannedTx(
                txid=txid,
                net_sats=net_sats,
                fee_sats=fee_sats,
                block_time=block_time,
                confirmed=confirmed,
            )
        )
        logger.debug("scanned tx %s net_sats=%d", txid, net_sats)

    # Confirmed txs first (sorted by block_time asc), then unconfirmed.
    confirmed_txs = sorted((t for t in result if t.confirmed), key=lambda t: t.block_time)
    unconfirmed_txs = [t for t in result if not t.confirmed]
    return confirmed_txs + unconfirmed_txs


_RETRY_DELAY = 2


def _fetch_address_txs(
    base_url: str, address: str, private: bool, request_delay: float = 0.0
) -> list[Any]:
    """Fetch raw tx objects from GET /address/{address}/txs with pagination."""
    url = f"{base_url.rstrip('/')}/address/{address}/txs"
    all_txs: list[Any] = []
    while url:
        if request_delay > 0:
            time.sleep(request_delay)
        page = _get_json(url, private)
        if not isinstance(page, list):
            break
        all_txs.extend(page)
        if len(page) < _MEMPOOL_PAGE_SIZE:
            break
        last_txid = page[-1].get("txid", "")
        if not last_txid:
            break
        url = f"{base_url.rstrip('/')}/address/{address}/txs/chain/{last_txid}"
    return all_txs


def _get_json(url: str, private: bool) -> object:
    """HTTP GET with one retry on 5xx. Disables SSL for private URLs."""
    ctx = ssl.create_default_context()
    if private:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        logger.debug("address_scanner: SSL verification disabled for private URL %s", url)

    for attempt in range(2):
        try:
            with urllib.request.urlopen(url, context=ctx, timeout=15) as resp:  # noqa: S310
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code == _HTTP_NOT_FOUND:
                return []
            if exc.code >= _HTTP_SERVER_ERROR and attempt == 0:
                logger.debug("HTTP 5xx from %s, retrying in %ds", url, _RETRY_DELAY)
                time.sleep(_RETRY_DELAY)
                continue
            logger.debug("HTTP %d for %s", exc.code, url)
            return []
        except Exception as exc:
            logger.debug("request failed for %s: %s", url, exc)
            return []
    return []
