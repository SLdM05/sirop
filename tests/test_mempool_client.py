"""Tests for the Mempool REST API client.

All HTTP calls are mocked via ``unittest.mock.patch`` — no live network.
"""

from __future__ import annotations

import json
import urllib.error
from unittest.mock import MagicMock, patch

from sirop.node.mempool_client import fetch_outspends, fetch_tx
from sirop.node.models import OnChainTx, TxOutspend

BASE_URL = "https://mempool.space/api"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TX_JSON = {
    "txid": "aaa" + "0" * 61,
    "fee": 1500,
    "status": {
        "confirmed": True,
        "block_time": 1_700_000_000,
    },
    "vin": [
        {"txid": "bbb" + "0" * 61},
        {"txid": "ccc" + "0" * 61},
    ],
    "vout": [
        {"value": 50_000, "scriptpubkey_address": "bc1qfakeaddressfortesting"},
    ],
}

_OUTSPENDS_JSON = [
    {"spent": True, "txid": "ddd" + "0" * 61, "vin": 0},
    {"spent": False, "txid": None, "vin": None},
]


def _mock_response(data: object, status: int = 200) -> MagicMock:
    """Build a mock urlopen context manager that returns JSON bytes."""
    body = json.dumps(data).encode()
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url="", code=code, msg="", hdrs=None, fp=None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# fetch_tx tests
# ---------------------------------------------------------------------------


class TestFetchTx:
    def test_ok_parses_on_chain_tx(self):
        with patch("urllib.request.urlopen", return_value=_mock_response(_TX_JSON)):
            result = fetch_tx(BASE_URL, _TX_JSON["txid"])

        assert isinstance(result, OnChainTx)
        assert result.txid == _TX_JSON["txid"]
        assert result.fee_sat == 1500  # noqa: PLR2004
        assert result.confirmed is True
        assert result.block_time is not None
        assert "bbb" + "0" * 61 in result.vin_txids
        assert "ccc" + "0" * 61 in result.vin_txids
        assert result.vout_count == 1

    def test_404_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=_http_error(404)):
            result = fetch_tx(BASE_URL, "nonexistent" + "0" * 53)
        assert result is None

    def test_500_retries_then_returns_none(self):
        """On 500, client retries once (sleep mocked) then returns None."""
        side_effects = [_http_error(500), _http_error(500)]
        with (
            patch("urllib.request.urlopen", side_effect=side_effects),
            patch("sirop.node.mempool_client.time.sleep"),
        ):
            result = fetch_tx(BASE_URL, "aaa" + "0" * 61)
        assert result is None

    def test_500_succeeds_on_retry(self):
        """On 500, client retries and returns the result from the retry."""
        side_effects = [_http_error(500), _mock_response(_TX_JSON)]
        with (
            patch("urllib.request.urlopen", side_effect=side_effects),
            patch("sirop.node.mempool_client.time.sleep"),
        ):
            result = fetch_tx(BASE_URL, _TX_JSON["txid"])
        assert isinstance(result, OnChainTx)

    def test_network_error_returns_none(self):
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("connection refused"),
        ):
            result = fetch_tx(BASE_URL, "aaa" + "0" * 61)
        assert result is None

    def test_malformed_json_returns_none(self):
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"not-json{"
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = fetch_tx(BASE_URL, "aaa" + "0" * 61)
        assert result is None

    def test_unconfirmed_tx_no_block_time(self):
        unconfirmed = {**_TX_JSON, "status": {"confirmed": False}}
        with patch("urllib.request.urlopen", return_value=_mock_response(unconfirmed)):
            result = fetch_tx(BASE_URL, _TX_JSON["txid"])
        assert isinstance(result, OnChainTx)
        assert result.confirmed is False
        assert result.block_time is None
        assert result.fee_sat == 1500  # noqa: PLR2004


# ---------------------------------------------------------------------------
# fetch_outspends tests
# ---------------------------------------------------------------------------


class TestFetchOutspends:
    def test_ok_parses_outspend_list(self):
        with patch("urllib.request.urlopen", return_value=_mock_response(_OUTSPENDS_JSON)):
            result = fetch_outspends(BASE_URL, _TX_JSON["txid"])

        assert len(result) == 2  # noqa: PLR2004
        assert isinstance(result[0], TxOutspend)
        assert result[0].spent is True
        assert result[0].txid == "ddd" + "0" * 61
        assert result[1].spent is False
        assert result[1].txid is None

    def test_404_returns_empty_list(self):
        with patch("urllib.request.urlopen", side_effect=_http_error(404)):
            result = fetch_outspends(BASE_URL, "nonexistent" + "0" * 53)
        assert result == []

    def test_network_error_returns_empty_list(self):
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("timeout"),
        ):
            result = fetch_outspends(BASE_URL, "aaa" + "0" * 61)
        assert result == []

    def test_non_list_response_returns_empty(self):
        with patch("urllib.request.urlopen", return_value=_mock_response({"error": 1})):
            result = fetch_outspends(BASE_URL, "aaa" + "0" * 61)
        assert result == []
