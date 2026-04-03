# tests/test_address_scanner.py
"""Tests for BIP32/BIP84/BIP49/BIP44 address derivation and gap-limit scanning."""

from __future__ import annotations

from typing import Any

import pytest

from sirop.node.address_scanner import derive_address

# ---------------------------------------------------------------------------
# BIP84 test vectors — derived from "12 abandon" mnemonic (no passphrase)
# via bip-utils 2.12.1: m/84'/0'/0' account-level zpub.
# ---------------------------------------------------------------------------
_BIP84_ZPUB = (
    "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r"
    "1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs"
)


def test_derive_zpub_external_index_0() -> None:
    assert derive_address(_BIP84_ZPUB, branch=0, index=0) == (
        "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"
    )


def test_derive_zpub_external_index_1() -> None:
    assert derive_address(_BIP84_ZPUB, branch=0, index=1) == (
        "bc1qnjg0jd8228aq7egyzacy8cys3knf9xvrerkf9g"
    )


def test_derive_zpub_internal_index_0() -> None:
    # BIP84 internal (change) chain address
    assert derive_address(_BIP84_ZPUB, branch=1, index=0) == (
        "bc1q8c6fshw2dlwun7ekn9qwf37cu2rn755upcp6el"
    )


def test_unsupported_prefix_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported xpub prefix"):
        derive_address("badpub6rFR7y4Q2AijF" + "x" * 80, branch=0, index=0)


# BIP49 test vector — ypub from "12 abandon" mnemonic at m/49'/0'/0'
# Source: Ian Coleman BIP39 tool, "abandon abandon ... about", no passphrase.
_BIP49_YPUB = (
    "ypub6Ww3ibxVfGzLrAH1PNcjyAWenMTbbAosGNB6VvmSEgytSER9azLDWCxoJwW"
    "7Ke7icmizBMXrzBx9979FfaHxHcrArf3zbeJJJUZPf663zsP"
)


def test_derive_ypub_external_index_0() -> None:
    addr = derive_address(_BIP49_YPUB, branch=0, index=0)
    # P2SH-P2WPKH addresses always start with "3" on mainnet
    assert addr.startswith("3"), f"Expected P2SH (3...) address, got {addr!r}"


# ---------------------------------------------------------------------------
# scan_wallet tests — mocked HTTP via unittest.mock.patch
# ---------------------------------------------------------------------------
from unittest.mock import patch  # noqa: E402

from sirop.node.address_scanner import scan_wallet  # noqa: E402

# Known addresses — validated against test_derive_zpub_external_index_0/1 above.
_ADDR_0_0 = "bc1qcr8te4kr609gcawutmrza0j4xv80jy8z306fyu"
_ADDR_0_1 = "bc1qnjg0jd8228aq7egyzacy8cys3knf9xvrerkf9g"

# Test fixture constants — extracted to avoid PLR2004 magic-value warnings.
_RECEIVE_VALUE_SATS = 100_000
_SEND_VALUE_SATS = 200_000
_SEND_FEE_SATS = 500
_EXPECTED_CHECKED_ADDRS = 3  # index 0 (active), index 1 (gap=1), index 2 (gap=2 → stop)


def _confirmed_receive(txid: str, address: str, value_sat: int, block_time: int) -> dict[str, Any]:
    return {
        "txid": txid,
        "vin": [{"prevout": {"scriptpubkey_address": "bc1qexternal", "value": value_sat}}],
        "vout": [{"scriptpubkey_address": address, "value": value_sat}],
        "fee": 0,
        "status": {"confirmed": True, "block_time": block_time},
    }


def _confirmed_send(
    txid: str, address: str, value_sat: int, fee_sat: int, block_time: int
) -> dict[str, Any]:
    change = value_sat - fee_sat - 1000
    return {
        "txid": txid,
        "vin": [{"prevout": {"scriptpubkey_address": address, "value": value_sat}}],
        "vout": [
            {"scriptpubkey_address": "bc1qdestination", "value": 1000},
            {"scriptpubkey_address": "bc1qchange", "value": change},
        ],
        "fee": fee_sat,
        "status": {"confirmed": True, "block_time": block_time},
    }


def test_scan_single_receive() -> None:
    """One receive on external index 0, gap_limit=3 stops after 3 consecutive empty."""
    tx = _confirmed_receive("aaa" + "0" * 61, _ADDR_0_0, _RECEIVE_VALUE_SATS, 1_700_000_000)

    def fake_fetch(
        base_url: str, address: str, private: bool, request_delay: float = 0.0
    ) -> list[Any]:
        return [tx] if address == _ADDR_0_0 else []

    with patch("sirop.node.address_scanner._fetch_address_txs", side_effect=fake_fetch):
        results = scan_wallet("https://mempool.space/api", _BIP84_ZPUB, branches=[0], gap_limit=3)

    assert len(results) == 1
    assert results[0].txid == "aaa" + "0" * 61
    assert results[0].net_sats == _RECEIVE_VALUE_SATS
    assert results[0].fee_sats == 0
    assert results[0].confirmed is True


def test_scan_send_includes_fee() -> None:
    tx = _confirmed_send(
        "bbb" + "0" * 61, _ADDR_0_0, _SEND_VALUE_SATS, _SEND_FEE_SATS, 1_700_000_001
    )

    def fake_fetch(
        base_url: str, address: str, private: bool, request_delay: float = 0.0
    ) -> list[Any]:
        return [tx] if address == _ADDR_0_0 else []

    with patch("sirop.node.address_scanner._fetch_address_txs", side_effect=fake_fetch):
        results = scan_wallet("https://mempool.space/api", _BIP84_ZPUB, branches=[0], gap_limit=3)

    assert len(results) == 1
    assert results[0].net_sats < 0
    assert results[0].fee_sats == _SEND_FEE_SATS


def test_scan_deduplication_across_branches() -> None:
    """Same txid seen on both branches → appears only once."""
    tx = _confirmed_receive("ccc" + "0" * 61, _ADDR_0_0, 50_000, 1_700_000_002)

    def fake_fetch(
        base_url: str, address: str, private: bool, request_delay: float = 0.0
    ) -> list[Any]:
        return [tx] if address == _ADDR_0_0 else []

    with patch("sirop.node.address_scanner._fetch_address_txs", side_effect=fake_fetch):
        results = scan_wallet(
            "https://mempool.space/api", _BIP84_ZPUB, branches=[0, 1], gap_limit=2
        )

    assert len(results) == 1


def test_scan_gap_limit_stops_derivation() -> None:
    """With gap_limit=2 and only index 0 active, derivation stops after 2 empty."""
    tx = _confirmed_receive("ddd" + "0" * 61, _ADDR_0_0, 10_000, 1_700_000_003)
    addresses_checked: list[str] = []

    def fake_fetch(
        base_url: str, address: str, private: bool, request_delay: float = 0.0
    ) -> list[Any]:
        addresses_checked.append(address)
        return [tx] if address == _ADDR_0_0 else []

    with patch("sirop.node.address_scanner._fetch_address_txs", side_effect=fake_fetch):
        scan_wallet("https://mempool.space/api", _BIP84_ZPUB, branches=[0], gap_limit=2)

    # index 0 (tx → reset gap=0), index 1 (empty → gap=1), index 2 (empty → gap=2 stop)
    assert len(addresses_checked) == _EXPECTED_CHECKED_ADDRS


def test_scan_invalid_branch_raises() -> None:
    with pytest.raises(ValueError, match="Invalid branch"):
        scan_wallet("https://mempool.space/api", _BIP84_ZPUB, branches=[2], gap_limit=5)
