"""xpub wallet-definition importer for sirop.

Accepts a user-created YAML file listing xpub/ypub/zpub keys with wallet names.
Derives all child addresses, scans transaction history via the Mempool API, and
returns a mapping of {wallet_name: list[RawTransaction]}.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

import yaml

from sirop.models.messages import MessageCode
from sirop.models.raw import RawTransaction
from sirop.node.address_scanner import ScannedTx, scan_wallet
from sirop.node.privacy import is_private_node_url
from sirop.utils.messages import emit

logger = logging.getLogger(__name__)

_SATS_PER_BTC = Decimal("100000000")
_DEFAULT_GAP_LIMIT = 20
_MAX_GAP_LIMIT = 200
_DEFAULT_BRANCHES = [0, 1]


@dataclass(frozen=True)
class XpubWalletEntry:
    name: str
    xpub: str
    gap_limit: int = _DEFAULT_GAP_LIMIT
    branches: list[int] = field(default_factory=lambda: list(_DEFAULT_BRANCHES))
    label: str = ""


class XpubImporter:
    """Imports BTC transaction history from one or more xpub/ypub/zpub keys."""

    def __init__(self, default_gap_limit: int, max_gap_limit: int) -> None:
        self._default_gap_limit = default_gap_limit
        self._max_gap_limit = max_gap_limit

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> XpubImporter:
        """Load importer defaults from ``config/importers/xpub.yaml``."""
        raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())
        defaults = raw.get("defaults", {})
        return cls(
            default_gap_limit=int(defaults.get("gap_limit", _DEFAULT_GAP_LIMIT)),
            max_gap_limit=int(defaults.get("max_gap_limit", _MAX_GAP_LIMIT)),
        )

    def parse_multi(
        self,
        path: Path,
        settings: Any,
    ) -> dict[str, list[RawTransaction]]:
        """Parse *path* (user wallet-definition YAML) → ``{wallet_name: [RawTransaction]}``.

        Raises:
            ValueError: YAML is malformed, missing required keys, or gap_limit exceeds max.
        """
        entries = self._load_wallet_entries(path)
        mempool_url: str = settings.btc_mempool_url
        allow_public: bool = getattr(settings, "btc_traversal_allow_public", False)
        request_delay: float = float(getattr(settings, "btc_traversal_request_delay", 0.0))

        if not is_private_node_url(mempool_url):
            if not allow_public:
                # Hard block — address scanning leaks wallet structure to a public host.
                # Unlike price lookups (which send only dates/assets), address scanning
                # sends derived Bitcoin addresses that directly identify the user's wallet.
                # Require a private node unless the user explicitly opts in.
                raise ValueError(
                    f"xpub address scanning requires a private Mempool node. "
                    f"BTC_MEMPOOL_URL={mempool_url!r} is a public endpoint. "
                    f"Set BTC_MEMPOOL_URL to a local node, or set "
                    f"BTC_TRAVERSAL_ALLOW_PUBLIC=true to override."
                )
            # allow_public=True — warn once then proceed.
            total_addresses = sum(len(e.branches) * (e.gap_limit + 1) for e in entries)
            emit(
                MessageCode.TAP_XPUB_PRIVACY_WARNING,
                count=total_addresses,
                url=mempool_url,
            )

        result: dict[str, list[RawTransaction]] = {}
        for entry in entries:
            emit(
                MessageCode.TAP_XPUB_SCANNING,
                wallet_name=entry.name,
                xpub_prefix=entry.xpub[:12],
                branch_count=len(entry.branches),
                gap_limit=entry.gap_limit,
            )
            try:
                scanned = scan_wallet(
                    mempool_url,
                    entry.xpub,
                    entry.branches,
                    entry.gap_limit,
                    request_delay,
                )
            except Exception as exc:
                emit(
                    MessageCode.TAP_XPUB_ERROR_SCAN_FAILED,
                    name=entry.name,
                    xpub_prefix=entry.xpub[:12],
                    detail=str(exc),
                )
                raise

            txs = [self._to_raw_transaction(s) for s in scanned]
            result[entry.name] = txs

            if txs:
                emit(
                    MessageCode.TAP_XPUB_SCAN_COMPLETE,
                    wallet_name=entry.name,
                    tx_count=len(txs),
                )
            else:
                emit(MessageCode.TAP_XPUB_NO_TRANSACTIONS, wallet_name=entry.name)

        return result

    def parse(self, path: Path) -> list[RawTransaction]:  # pragma: no cover
        raise NotImplementedError(
            "XpubImporter requires parse_multi(path, settings); "
            "call via `sirop tap <file> --source xpub`"
        )

    def _load_wallet_entries(self, path: Path) -> list[XpubWalletEntry]:
        try:
            raw: dict[str, Any] = yaml.safe_load(path.read_text())
        except Exception as exc:
            raise ValueError(
                f"Failed to parse xpub wallet-definition file {str(path)!r}: {exc}"
            ) from exc

        wallets_raw = raw.get("wallets")
        if not isinstance(wallets_raw, list):
            raise ValueError(
                f"xpub wallet-definition file {str(path)!r} must contain a top-level 'wallets' list"
            )

        entries: list[XpubWalletEntry] = []
        for idx, item in enumerate(wallets_raw):
            if not isinstance(item, dict):
                raise ValueError(
                    f"Wallet entry #{idx} in {str(path)!r} is invalid: must be a mapping"
                )
            name = item.get("name")
            xpub = item.get("xpub")
            if not name:
                raise ValueError(f"Wallet entry #{idx} in {str(path)!r} is invalid: missing 'name'")
            if not xpub:
                raise ValueError(f"Wallet entry #{idx} in {str(path)!r} is invalid: missing 'xpub'")
            gap_limit = int(item.get("gap_limit", self._default_gap_limit))
            if gap_limit > self._max_gap_limit:
                raise ValueError(
                    f"Wallet entry #{idx} gap_limit={gap_limit} exceeds "
                    f"max_gap_limit={self._max_gap_limit}"
                )
            branches = [int(b) for b in item.get("branches", _DEFAULT_BRANCHES)]
            entries.append(
                XpubWalletEntry(
                    name=str(name),
                    xpub=str(xpub),
                    gap_limit=gap_limit,
                    branches=branches,
                    label=str(item.get("label", "")),
                )
            )
        return entries

    @staticmethod
    def _to_raw_transaction(scanned: ScannedTx) -> RawTransaction:
        amount_btc = Decimal(abs(scanned.net_sats)) / _SATS_PER_BTC
        fee_btc = Decimal(scanned.fee_sats) / _SATS_PER_BTC if scanned.fee_sats else None
        ts = (
            datetime.fromtimestamp(scanned.block_time, tz=UTC)
            if scanned.block_time
            else datetime.now(tz=UTC)
        )
        tx_type = "deposit" if scanned.net_sats > 0 else "withdrawal"

        return RawTransaction(
            source="xpub",
            timestamp=ts,
            transaction_type=tx_type,
            asset="BTC",
            amount=amount_btc,
            amount_currency="BTC",
            fiat_value=None,
            fiat_currency=None,
            fee_amount=fee_btc,
            fee_currency="BTC" if fee_btc is not None else None,
            rate=None,
            spot_rate=None,
            txid=scanned.txid,
            raw_type=tx_type,
            raw_row={
                "txid": scanned.txid,
                "net_sats": str(scanned.net_sats),
                "fee_sats": str(scanned.fee_sats),
                "block_time": str(scanned.block_time),
                "confirmed": str(scanned.confirmed),
            },
        )
