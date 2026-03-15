"""Tests for the Mempool privacy guard — is_private_node_url().

All tests use only stdlib so no live network or external packages are needed.
"""

from __future__ import annotations

import pytest

from sirop.node.privacy import is_private_node_url


class TestPrivateAddresses:
    """URLs that should be classified as private / local."""

    @pytest.mark.parametrize(
        "url",
        [
            # localhost variants
            "http://localhost:3006/api",
            "http://localhost/api",
            "http://LOCALHOST:8332",
            # IPv4 loopback
            "http://127.0.0.1:3006/api",
            "http://127.0.0.1:8332",
            "http://127.1.2.3/api",
            # RFC 1918 private ranges
            "http://10.0.0.1:3006/api",
            "http://10.255.255.255/api",
            "http://172.16.0.1/api",
            "http://172.31.255.255/api",
            "http://192.168.1.100:3006/api",
            "http://192.168.0.1/api",
            # Link-local
            "http://169.254.1.1/api",
            # IPv6 loopback
            "http://[::1]:3006/api",
            # IPv6 ULA
            "http://[fd00::1]/api",
            "http://[fd12:3456:789a::1]:3006/api",
            # Local DNS suffixes
            "http://node.local:3006/api",
            "http://mynode.local/api",
            "http://raspi.lan:3006/api",
            "http://mempool.internal/api",
            "http://server.intranet/api",
            "http://device.home.arpa/api",
            "http://btc.corp/api",
            "http://node.private/api",
        ],
    )
    def test_private_url(self, url: str) -> None:
        assert is_private_node_url(url) is True, f"Expected private: {url}"


class TestPublicAddresses:
    """URLs that should be classified as public."""

    @pytest.mark.parametrize(
        "url",
        [
            "https://mempool.space/api",
            "https://mempool.space",
            "http://mempool.space/api",
            "https://blockstream.info/api",
            "https://btc.example.com/api",
            "http://1.2.3.4/api",
            "http://8.8.8.8/api",
            "https://1.1.1.1/api",
        ],
    )
    def test_public_url(self, url: str) -> None:
        assert is_private_node_url(url) is False, f"Expected public: {url}"


class TestEdgeCases:
    def test_empty_url_returns_false(self) -> None:
        assert is_private_node_url("") is False

    def test_url_with_no_host_returns_false(self) -> None:
        assert is_private_node_url("not-a-url") is False

    def test_url_with_path_only(self) -> None:
        assert is_private_node_url("/api/v1") is False

    def test_trailing_dot_localhost(self) -> None:
        # urllib.parse strips trailing dots from hostnames
        assert is_private_node_url("http://localhost./api") is True

    def test_uppercase_ip(self) -> None:
        # IP addresses with uppercase are not valid but parsed lowercase
        assert is_private_node_url("http://127.0.0.1/api") is True
