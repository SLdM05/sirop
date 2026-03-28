# tests/test_address_scanner.py
"""Tests for BIP32/BIP84/BIP49/BIP44 address derivation and gap-limit scanning."""

from __future__ import annotations

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
def test_derive_ypub_external_index_0() -> None:
    # Derive the ypub from the "12 abandon" mnemonic first, then test derivation
    from bip_utils import Bip39SeedGenerator, Bip49, Bip49Coins

    mnemonic = (
        "abandon abandon abandon abandon abandon abandon "
        "abandon abandon abandon abandon abandon about"
    )
    seed = Bip39SeedGenerator(mnemonic).Generate()
    ctx = Bip49.FromSeed(seed, Bip49Coins.BITCOIN)
    ypub = ctx.Purpose().Coin().Account(0).PublicKey().ToExtended()
    addr = derive_address(ypub, branch=0, index=0)
    # verify it looks like a P2SH address
    assert addr.startswith("3"), f"Expected P2SH (3...) address, got {addr!r}"
