"""NDAX importer — AlphaPoint APEX ledger CSV format.

NDAX exports one ledger *row* per asset movement.  A single economic event
(e.g. buying BTC with CAD) produces multiple rows that share the same
timestamp (to-the-second precision).  This importer groups rows by that
truncated timestamp and collapses each group into one ``RawTransaction``.

Column layout (7 columns, no trailing empties in the raw export):

    ASSET, ASSET_CLASS, AMOUNT, BALANCE, TYPE, TX_ID, DATE

TYPE uses a "PRIMARY / SECONDARY" notation, e.g.:

    TRADE          — main trade leg
    TRADE / FEE    — fee associated with a trade
    WITHDRAW       — crypto or fiat withdrawal
    WITHDRAW / FEE — on-chain withdrawal fee
    DEPOSIT        — crypto or fiat deposit
    STAKING / REWARD   — staking income
    STAKING / DEPOSIT  — crypto locked for staking
    STAKING / REFUND   — crypto returned from staking
    DUST / IN / OUT / FEE — dust conversion legs

Rows with a secondary type of "FEE" are extracted from the group and used
to populate ``fee_amount`` / ``fee_currency`` on the resulting transaction.

Which primary TYPE values route to which parsing strategy is driven by the
``group_handlers`` key in ``config/importers/ndax.yaml``.  No primary TYPE
strings are hardcoded in this module.
"""

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from sirop.importers.base import BaseImporter, InvalidCSVFormatError, load_importer_config
from sirop.models.importer import ImporterConfig
from sirop.models.raw import RawTransaction

logger = logging.getLogger(__name__)

# Secondary TYPE suffix that marks a fee row.
_FEE_SUFFIX = "fee"

# Handler strategy names — internal Python concepts, not exchange-specific strings.
# Must match the keys used in the ``group_handlers`` section of ndax.yaml.
_HANDLER_SINGLE = "single"
_HANDLER_TRADE = "trade"
_HANDLER_DUST = "dust"

# A non-fiat-to-non-fiat trade produces exactly two main rows (one per side).
_NON_FIAT_TRADE_LEG_COUNT = 2


class NDAXImporter(BaseImporter):
    """Imports NDAX ledger CSV exports (AlphaPoint APEX format)."""

    def __init__(self, config: ImporterConfig) -> None:
        super().__init__(config)
        # ASSET_CLASS value that identifies fiat rows — overridden by from_yaml().
        self._fiat_asset_class: str = "FIAT"
        # Maps lowercased primary TYPE string → handler strategy name.
        # Populated from the ``group_handlers`` section of the YAML by from_yaml().
        self._type_to_handler: dict[str, str] = {}
        # Crypto assets that are allowed through the filter — all others are
        # skipped with a warning.  Populated from allowed_crypto_assets in YAML.
        self._allowed_crypto_assets: frozenset[str] = frozenset({"BTC"})

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "NDAXImporter":
        """Construct an ``NDAXImporter`` from a YAML config file."""
        raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())
        config = load_importer_config(yaml_path, source_name=yaml_path.stem)
        importer = cls(config)
        importer._fiat_asset_class = str(raw.get("fiat_asset_class", "FIAT"))

        for handler_name, primary_types in raw.get("group_handlers", {}).items():
            for pt in primary_types:
                importer._type_to_handler[str(pt)] = str(handler_name)

        allowed = raw.get("allowed_crypto_assets")
        if allowed:
            importer._allowed_crypto_assets = frozenset(str(a).upper() for a in allowed)

        return importer

    def parse(self, csv_path: Path) -> list[RawTransaction]:
        """Parse *csv_path* and return one ``RawTransaction`` per logical event."""
        rows = self._read_csv(csv_path)
        if not rows:
            return []

        cols = self._config.columns
        self._validate_columns(
            rows[0],
            required_keys={
                cols["asset"],
                cols["asset_class"],
                cols["amount"],
                cols["type"],
                cols["tx_id"],
                cols["date"],
            },
        )

        groups = self._group_by_timestamp(rows)
        results: list[RawTransaction] = []
        for ts_key, group_rows in groups.items():
            results.extend(self._parse_group(ts_key, group_rows))

        results.sort(key=lambda t: t.timestamp)
        return self._filter_btc_only(results)

    # ------------------------------------------------------------------
    # Grouping
    # ------------------------------------------------------------------

    def _group_by_timestamp(self, rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
        """Group rows by their timestamp truncated to the second.

        NDAX rows belonging to the same economic event share the same
        timestamp (sometimes differing by a few milliseconds but always
        within the same second).
        """
        date_col = self._config.columns["date"]
        groups: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            raw_ts = row.get(date_col, "").strip()
            if not raw_ts:
                continue
            # Truncate to second: "2025-05-30T14:38:02.586Z" → "2025-05-30T14:38:02Z"
            if "." in raw_ts:
                second_key = raw_ts[: raw_ts.index(".")] + "Z"
            else:
                second_key = raw_ts.rstrip("Z") + "Z"
            groups.setdefault(second_key, []).append(row)
        return groups

    # ------------------------------------------------------------------
    # Group parsing
    # ------------------------------------------------------------------

    def _parse_group(self, ts_key: str, rows: list[dict[str, str]]) -> list[RawTransaction]:
        cols = self._config.columns

        # Separate fee rows from main transaction rows.
        fee_rows: list[dict[str, str]] = []
        main_rows: list[dict[str, str]] = []
        for row in rows:
            _, secondary = self._split_type(row[cols["type"]])
            if secondary == _FEE_SUFFIX:
                fee_rows.append(row)
            else:
                main_rows.append(row)

        if not main_rows:
            return []

        timestamp = self._parse_timestamp(main_rows[0][cols["date"]])
        primary, _ = self._split_type(main_rows[0][cols["type"]])
        raw_type_full = main_rows[0][cols["type"]].strip().lower()
        fee_amount, fee_currency = self._extract_fee(fee_rows)

        # Dispatch to the handler strategy configured in ndax.yaml.
        handler = self._type_to_handler.get(primary)
        if handler == _HANDLER_SINGLE:
            result = self._parse_single(
                main_rows[0], timestamp, fee_amount, fee_currency, raw_type_full
            )
            return [result] if result is not None else []
        elif handler == _HANDLER_TRADE:
            return self._parse_trade(main_rows, timestamp, fee_amount, fee_currency)
        elif handler == _HANDLER_DUST:
            return self._parse_dust(main_rows, timestamp, fee_amount, fee_currency)
        else:
            logger.warning(
                "ndax: unrecognised TYPE %r in group %s — skipping",
                main_rows[0][cols["type"]],
                ts_key,
            )
            return []

    # ------------------------------------------------------------------
    # Transaction-type handlers
    # ------------------------------------------------------------------

    def _parse_trade(
        self,
        rows: list[dict[str, str]],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
    ) -> list[RawTransaction]:
        cols = self._config.columns
        fiat_class = self._fiat_asset_class

        fiat_rows = [r for r in rows if r[cols["asset_class"]].strip() == fiat_class]
        non_fiat_rows = [r for r in rows if r[cols["asset_class"]].strip() != fiat_class]

        if not non_fiat_rows:
            logger.warning("ndax: TRADE group has no non-fiat rows — skipping")
            return []

        if len(non_fiat_rows) == _NON_FIAT_TRADE_LEG_COUNT and not fiat_rows:
            # Both legs are non-fiat (e.g. SOL → ETH direct trade).
            return self._parse_non_fiat_trade(non_fiat_rows, timestamp, fee_amount, fee_currency)

        if len(non_fiat_rows) != 1 or not fiat_rows:
            logger.warning(
                "ndax: unexpected TRADE row count (non_fiat=%d fiat=%d) — skipping",
                len(non_fiat_rows),
                len(fiat_rows),
            )
            return []

        non_fiat_row = non_fiat_rows[0]
        fiat_row = fiat_rows[0]

        asset = non_fiat_row[cols["asset"]].strip()
        fiat_currency_code = fiat_row[cols["asset"]].strip()
        non_fiat_amount = self._parse_amount(non_fiat_row[cols["amount"]], asset)
        fiat_amount = self._parse_amount(fiat_row[cols["amount"]], fiat_currency_code)

        # Positive non-fiat amount = buy (spending CAD, receiving crypto).
        # Negative non-fiat amount = sell (spending crypto, receiving CAD).
        # fiat_value is always stored as a positive (what was spent or received).
        fiat_value = abs(fiat_amount)
        is_sell = non_fiat_amount < Decimal("0")
        if is_sell:
            non_fiat_amount = abs(non_fiat_amount)

        rate = (fiat_value / non_fiat_amount) if non_fiat_amount else None

        tx_type = "sell" if is_sell else "buy"
        return [
            RawTransaction(
                source=self._config.source_name,
                timestamp=timestamp,
                transaction_type=tx_type,
                asset=asset,
                amount=non_fiat_amount,
                amount_currency=asset,
                fiat_value=fiat_value,
                fiat_currency=fiat_currency_code,
                fee_amount=fee_amount,
                fee_currency=fee_currency,
                rate=rate,
                spot_rate=None,
                txid=None,
                raw_type=non_fiat_row[cols["type"]].strip().lower(),
                raw_row=non_fiat_row,
            )
        ]

    def _parse_non_fiat_trade(
        self,
        rows: list[dict[str, str]],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
    ) -> list[RawTransaction]:
        """Handle a trade where both sides are non-fiat assets (e.g. SOL → ETH).

        Emits two ``RawTransaction`` objects: one acquisition for the received
        asset and one disposal for the sent asset.  Both legs are needed so that
        the ACB pool for the sent asset is correctly reduced.
        """
        cols = self._config.columns

        # Received asset has a positive AMOUNT; sent asset has a negative AMOUNT.
        received = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") > 0), None)
        sent = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") < 0), None)

        if not received or not sent:
            logger.warning("ndax: non-fiat TRADE group missing a leg — skipping")
            return []

        received_asset = received[cols["asset"]].strip()
        sent_asset = sent[cols["asset"]].strip()
        received_amount = self._parse_amount(received[cols["amount"]], received_asset)
        sent_amount = abs(self._parse_amount(sent[cols["amount"]], sent_asset))

        rate = (received_amount / sent_amount) if sent_amount else None

        received_tx = RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type=self._lookup_type("trade"),
            asset=received_asset,
            amount=received_amount,
            amount_currency=received_asset,
            fiat_value=None,  # normalizer derives from BoC rates
            fiat_currency=None,
            fee_amount=fee_amount,
            fee_currency=fee_currency,
            rate=rate,
            spot_rate=None,
            txid=None,
            raw_type=received[cols["type"]].strip().lower(),
            raw_row=dict(received),
        )
        # Disposal of the sent asset at its own BoC-derived rate.
        sent_tx = RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type="sell",
            asset=sent_asset,
            amount=sent_amount,
            amount_currency=sent_asset,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=None,  # fee is recorded on the received leg
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=None,
            raw_type=sent[cols["type"]].strip().lower(),
            raw_row=dict(sent),
        )
        return [received_tx, sent_tx]

    def _parse_single(
        self,
        row: dict[str, str],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
        raw_type_full: str,
    ) -> RawTransaction | None:
        """Handle single-row groups: deposits, withdrawals, and staking events."""
        cols = self._config.columns

        asset = row[cols["asset"]].strip()
        asset_class = row[cols["asset_class"]].strip()
        amount = abs(self._parse_amount(row[cols["amount"]], asset))

        if amount == Decimal("0"):
            return None  # skip zero-amount rows

        is_fiat = asset_class == self._fiat_asset_class
        primary, _ = self._split_type(row[cols["type"]])

        if is_fiat:
            tx_type = self._lookup_type(
                "fiat_deposit" if primary == "deposit" else "fiat_withdrawal"
            )
            return RawTransaction(
                source=self._config.source_name,
                timestamp=timestamp,
                transaction_type=tx_type,
                asset=asset,
                amount=amount,
                amount_currency=asset,
                fiat_value=amount,
                fiat_currency=asset,
                fee_amount=fee_amount,
                fee_currency=fee_currency,
                rate=None,
                spot_rate=None,
                txid=None,
                raw_type=raw_type_full,
                raw_row=row,
            )
        else:
            # Try the full type string first (e.g. "staking / reward" → "income"),
            # then fall back to the primary part alone (e.g. "deposit" → "deposit").
            tx_type = self._config.transaction_type_map.get(raw_type_full) or self._lookup_type(
                primary
            )
            # TX_ID is NDAX's internal order identifier (a short integer such as
            # "10008"), NOT a blockchain transaction ID.  It cannot be used to
            # match this withdrawal against a Sparrow or Shakepay deposit.
            # The transfer_match stage will pair NDAX withdrawals by
            # amount + timestamp proximity instead.
            txid_value: str | None = None
            return RawTransaction(
                source=self._config.source_name,
                timestamp=timestamp,
                transaction_type=tx_type,
                asset=asset,
                amount=amount,
                amount_currency=asset,
                fiat_value=None,
                fiat_currency=None,
                fee_amount=fee_amount,
                fee_currency=fee_currency,
                rate=None,
                spot_rate=None,
                txid=txid_value,
                raw_type=raw_type_full,
                raw_row=row,
            )

    def _parse_dust(
        self,
        rows: list[dict[str, str]],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
    ) -> list[RawTransaction]:
        """Handle DUST conversion groups (DUST / IN, DUST / OUT).

        Emits two ``RawTransaction`` objects when both legs are present: one
        acquisition for the received asset and one disposal for the sent asset.
        Emitting only the acquisition (the previous behaviour) left the sent
        asset's ACB pool unreduced, producing phantom ending balances.
        """
        cols = self._config.columns

        received = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") > 0), None)
        sent = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") < 0), None)

        if not received:
            return []

        received_asset = received[cols["asset"]].strip()
        received_amount = self._parse_amount(received[cols["amount"]], received_asset)

        received_tx = RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type=self._lookup_type("dust") or "other",
            asset=received_asset,
            amount=received_amount,
            amount_currency=received_asset,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=fee_amount,
            fee_currency=fee_currency,
            rate=None,
            spot_rate=None,
            txid=None,
            raw_type=received[cols["type"]].strip().lower(),
            raw_row=dict(received),
        )

        if not sent:
            return [received_tx]

        sent_asset = sent[cols["asset"]].strip()
        sent_amount = abs(self._parse_amount(sent[cols["amount"]], sent_asset))

        # Disposal of the sent asset.  fiat_value is None — the normalizer will
        # derive the CAD value from the BoC spot rate at the transaction timestamp.
        sent_tx = RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type="sell",
            asset=sent_asset,
            amount=sent_amount,
            amount_currency=sent_asset,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=None,  # fee is recorded on the received leg
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=None,
            raw_type=sent[cols["type"]].strip().lower(),
            raw_row=dict(sent),
        )
        return [received_tx, sent_tx]

    # ------------------------------------------------------------------
    # BTC-only filter
    # ------------------------------------------------------------------

    # Fiat currencies that are always allowed regardless of allowed_crypto_assets.
    _FIAT_CURRENCIES: frozenset[str] = frozenset({"CAD", "USD"})

    def _filter_btc_only(self, results: list[RawTransaction]) -> list[RawTransaction]:
        """Drop any RawTransaction whose asset is not in allowed_crypto_assets or fiat.

        Emits one warning per unique skipped asset symbol (not per row).
        """
        allowed = self._allowed_crypto_assets | self._FIAT_CURRENCIES
        skipped: dict[str, int] = {}
        kept: list[RawTransaction] = []
        for tx in results:
            asset = tx.asset.upper()
            if asset in allowed:
                kept.append(tx)
            else:
                skipped[asset] = skipped.get(asset, 0) + 1
        for asset, count in sorted(skipped.items()):
            logger.warning(
                "ndax: skipping %d non-BTC row(s) for asset %s — sirop is BTC-only",
                count,
                asset,
            )
        return kept

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _split_type(self, raw: str) -> tuple[str, str]:
        """Split ``"TRADE / FEE"`` into ``("trade", "fee")``.

        Returns ``("primary", "")`` when there is no secondary part.
        """
        parts = raw.strip().lower().split(" / ", maxsplit=1)
        primary = parts[0].strip()
        secondary = parts[1].strip() if len(parts) > 1 else ""
        return primary, secondary

    def _lookup_type(self, key: str) -> str:
        """Look up *key* in the transaction_type_map; return *key* if absent."""
        return self._config.transaction_type_map.get(key, key)

    def _extract_fee(self, fee_rows: list[dict[str, str]]) -> tuple[Decimal | None, str | None]:
        """Sum all fee rows and return ``(total_fee, currency)``.

        If there are multiple fee currencies (unusual), only the last one is
        returned — a warning is emitted.  Returns ``(None, None)`` when there
        are no fee rows or the total is zero.
        """
        if not fee_rows:
            return None, None

        cols = self._config.columns
        total = Decimal("0")
        currency: str | None = None
        seen_currencies: set[str] = set()

        for row in fee_rows:
            raw_amt = row[cols["amount"]].strip()
            if not raw_amt:
                continue
            asset = row[cols["asset"]].strip()
            seen_currencies.add(asset)
            amount = abs(self._parse_amount(raw_amt, asset))
            total += amount
            currency = asset

        if len(seen_currencies) > 1:
            logger.warning(
                "ndax: fee rows have multiple currencies %s — using last seen", seen_currencies
            )

        if total == Decimal("0"):
            return None, None
        return total, currency

    def _parse_amount(self, value: str, asset: str) -> Decimal:
        """Parse *value* as a ``Decimal``.

        Raises ``InvalidCSVFormatError`` on an unparseable value.
        """
        value = value.strip()
        if not value:
            return Decimal("0")
        try:
            return Decimal(value)
        except InvalidOperation as exc:
            raise InvalidCSVFormatError(
                f"Cannot parse AMOUNT {value!r} for asset {asset!r} as Decimal."
            ) from exc
