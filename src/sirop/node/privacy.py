"""Privacy guard for Bitcoin node API calls.

Graph traversal sends on-chain txids to the configured Mempool REST API.
When that endpoint is a public service (e.g. mempool.space), the operator
can log those txids and correlate them with IP addresses, linking the user's
wallet activity to their network identity.

``is_private_node_url`` detects whether ``BTC_MEMPOOL_URL`` points to a
private/local address so the caller can skip the prompt for local nodes
and warn (or require confirmation) for public ones.

Detection rules
---------------
An address is considered private when the hostname or IP resolves to:

- ``localhost`` or ``::1`` (loopback names)
- ``127.x.x.x`` — IPv4 loopback (RFC 5735)
- ``10.x.x.x`` — RFC 1918 class A private
- ``172.16.x.x``-``172.31.x.x`` — RFC 1918 class B private
- ``192.168.x.x`` — RFC 1918 class C private
- ``169.254.x.x`` — link-local (RFC 3927)
- ``fd00::/8`` or ``fe80::/10`` — IPv6 ULA / link-local
- Hostnames ending in ``.local``, ``.internal``, ``.lan``, ``.intranet``,
  ``.home.arpa`` — common mDNS / split-horizon DNS suffixes for local nets

Everything else (including bare hostnames such as ``mempool.space``) is
treated as public and triggers the privacy prompt.
"""

from __future__ import annotations

import ipaddress
import urllib.parse

# Hostnames that always map to loopback.
_LOOPBACK_NAMES: frozenset[str] = frozenset({"localhost", "ip6-localhost", "ip6-loopback"})

# Common local-network DNS suffixes used by mDNS, Avahi, and split-horizon DNS.
_LOCAL_SUFFIXES: tuple[str, ...] = (
    ".local",
    ".internal",
    ".lan",
    ".intranet",
    ".home.arpa",
    ".corp",
    ".private",
)


def is_private_node_url(url: str) -> bool:
    """Return ``True`` when *url* points to a private or local-network host.

    Returns ``False`` for any hostname that cannot be confirmed as local
    (i.e. it is treated as potentially public, erring on the side of caution).

    Parameters
    ----------
    url:
        The full URL from ``BTC_MEMPOOL_URL``, e.g.
        ``"http://localhost:3006/api"`` or ``"https://mempool.space/api"``.
    """
    parsed = urllib.parse.urlparse(url)
    hostname: str = (parsed.hostname or "").lower().strip(".")

    if not hostname:
        return False

    # Fast path: known loopback names.
    if hostname in _LOOPBACK_NAMES:
        return True

    # Local-network DNS suffix check (handles Raspberry Pi node on .local, etc.)
    for suffix in _LOCAL_SUFFIXES:
        if hostname.endswith(suffix):
            return True

    # Attempt IP address parsing.
    try:
        addr = ipaddress.ip_address(hostname)
    except ValueError:
        # Not a bare IP address — unrecognised hostname, treat as public.
        return False

    return addr.is_private or addr.is_loopback or addr.is_link_local
