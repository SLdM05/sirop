"""Tests for XpubImporter — mocks scan_wallet so no real HTTP or key derivation."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from sirop.importers.xpub import XpubImporter
from sirop.node.address_scanner import ScannedTx

_IMPORTER_CONFIG_PATH = Path("config/importers/xpub.yaml")

_BIP84_ZPUB = (
    "zpub6rFR7y4Q2AijBEqTUquhVz398htDFrtymD9xYYfG1m4wAcvPhXNfE3EfH1r"
    "1ADqtfSdVCToUG868RvUUkgDKf31mGDtKsAYz2oz2AGutZYs"
)

_WALLET_DEF = {
    "source": "xpub",
    "wallets": [
        {
            "name": "test-wallet",
            "xpub": _BIP84_ZPUB,
            "gap_limit": 5,
        }
    ],
}

_FAKE_SCANNED = [
    ScannedTx(
        txid="aaa" + "0" * 61,
        net_sats=100_000,
        fee_sats=0,
        block_time=1_700_000_000,
        confirmed=True,
    ),
    ScannedTx(
        txid="bbb" + "0" * 61,
        net_sats=-50_000,
        fee_sats=200,
        block_time=1_700_100_000,
        confirmed=True,
    ),
]


@pytest.fixture()
def tmp_wallet_def(tmp_path: Path) -> Path:
    p = tmp_path / "wallets.yaml"
    p.write_text(yaml.dump(_WALLET_DEF))
    return p


@pytest.fixture()
def importer() -> XpubImporter:
    return XpubImporter.from_yaml(_IMPORTER_CONFIG_PATH)


@pytest.fixture()
def fake_settings() -> MagicMock:
    s = MagicMock()
    s.btc_mempool_url = "https://mempool.space/api"
    s.btc_traversal_allow_public = True  # skip privacy block in tests
    s.btc_traversal_request_delay = 0.0
    return s


def test_parse_multi_returns_wallet_name(
    importer: XpubImporter, tmp_wallet_def: Path, fake_settings: MagicMock
) -> None:
    with patch("sirop.importers.xpub.scan_wallet", return_value=_FAKE_SCANNED):
        result = importer.parse_multi(tmp_wallet_def, fake_settings)
    assert "test-wallet" in result


def test_parse_multi_receive_is_deposit(
    importer: XpubImporter, tmp_wallet_def: Path, fake_settings: MagicMock
) -> None:
    with patch("sirop.importers.xpub.scan_wallet", return_value=_FAKE_SCANNED):
        result = importer.parse_multi(tmp_wallet_def, fake_settings)
    deposits = [t for t in result["test-wallet"] if t.transaction_type == "deposit"]
    assert len(deposits) == 1
    assert deposits[0].amount == Decimal("0.001")  # 100_000 sats
    assert deposits[0].fee_amount is None


def test_parse_multi_spend_is_withdrawal_with_fee(
    importer: XpubImporter, tmp_wallet_def: Path, fake_settings: MagicMock
) -> None:
    with patch("sirop.importers.xpub.scan_wallet", return_value=_FAKE_SCANNED):
        result = importer.parse_multi(tmp_wallet_def, fake_settings)
    withdrawals = [t for t in result["test-wallet"] if t.transaction_type == "withdrawal"]
    assert len(withdrawals) == 1
    assert withdrawals[0].amount == Decimal("0.0005")  # 50_000 sats
    assert withdrawals[0].fee_amount == Decimal("0.000002")  # 200 sats


def test_parse_multi_source_is_xpub(
    importer: XpubImporter, tmp_wallet_def: Path, fake_settings: MagicMock
) -> None:
    with patch("sirop.importers.xpub.scan_wallet", return_value=_FAKE_SCANNED):
        result = importer.parse_multi(tmp_wallet_def, fake_settings)
    for tx in result["test-wallet"]:
        assert tx.source == "xpub"


def test_parse_multi_txid_preserved(
    importer: XpubImporter, tmp_wallet_def: Path, fake_settings: MagicMock
) -> None:
    with patch("sirop.importers.xpub.scan_wallet", return_value=_FAKE_SCANNED):
        result = importer.parse_multi(tmp_wallet_def, fake_settings)
    txids = {t.txid for t in result["test-wallet"]}
    assert "aaa" + "0" * 61 in txids
    assert "bbb" + "0" * 61 in txids


def test_public_endpoint_blocked_by_default(importer: XpubImporter, tmp_wallet_def: Path) -> None:
    """Public Mempool URL must be rejected unless btc_traversal_allow_public=True."""
    s = MagicMock()
    s.btc_mempool_url = "https://mempool.space/api"
    s.btc_traversal_allow_public = False
    s.btc_traversal_request_delay = 0.0
    with pytest.raises(ValueError, match="private Mempool node"):
        importer.parse_multi(tmp_wallet_def, s)


def test_private_endpoint_allowed(importer: XpubImporter, tmp_wallet_def: Path) -> None:
    """Local Mempool URL must be allowed without any flag."""
    s = MagicMock()
    s.btc_mempool_url = "http://localhost:3006/api"
    s.btc_traversal_allow_public = False
    s.btc_traversal_request_delay = 0.0
    with patch("sirop.importers.xpub.scan_wallet", return_value=_FAKE_SCANNED):
        result = importer.parse_multi(tmp_wallet_def, s)
    assert "test-wallet" in result


def test_missing_wallets_key_raises(
    importer: XpubImporter, tmp_path: Path, fake_settings: MagicMock
) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text(yaml.dump({"source": "xpub"}))
    with pytest.raises(ValueError, match="wallets"):
        importer.parse_multi(bad, fake_settings)


def test_missing_xpub_field_raises(
    importer: XpubImporter, tmp_path: Path, fake_settings: MagicMock
) -> None:
    bad = tmp_path / "bad2.yaml"
    bad.write_text(yaml.dump({"source": "xpub", "wallets": [{"name": "x"}]}))
    with pytest.raises(ValueError, match="xpub"):
        importer.parse_multi(bad, fake_settings)
