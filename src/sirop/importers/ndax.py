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
"""

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from sirop.importers.base import BaseImporter, InvalidCSVFormatError
from sirop.models.importer import ImporterConfig
from sirop.models.raw import RawTransaction

logger = logging.getLogger(__name__)

# Secondary TYPE suffix that marks a fee row.
_FEE_SUFFIX = "fee"

# Number of crypto legs that indicates a crypto-to-crypto trade.
_CRYPTO_CRYPTO_LEG_COUNT = 2


class NDAXImporter(BaseImporter):
    """Imports NDAX ledger CSV exports (AlphaPoint APEX format)."""

    def __init__(self, config: ImporterConfig) -> None:
        super().__init__(config)
        # ASSET_CLASS value that identifies fiat rows — comes from the YAML
        # via a non-standard key.  We read it from the raw YAML through the
        # extra data attached to the config name comment; since ImporterConfig
        # doesn't carry arbitrary extras we store it separately.
        # The YAML loader sets this before constructing the importer.
        self._fiat_asset_class: str = "FIAT"  # overridden by from_yaml()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "NDAXImporter":
        """Construct an ``NDAXImporter`` from a YAML config file."""
        import yaml

        raw: dict[str, object] = yaml.safe_load(yaml_path.read_text())
        from sirop.importers.base import load_importer_config

        config = load_importer_config(yaml_path, source_name=yaml_path.stem)
        importer = cls(config)
        importer._fiat_asset_class = str(raw.get("fiat_asset_class", "FIAT"))
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
            tx = self._parse_group(ts_key, group_rows)
            if tx is not None:
                results.append(tx)

        results.sort(key=lambda t: t.timestamp)
        return results

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
            # Works for ISO 8601 with or without fractional seconds.
            if "." in raw_ts:
                second_key = raw_ts[: raw_ts.index(".")] + "Z"
            else:
                second_key = raw_ts.rstrip("Z") + "Z"
            groups.setdefault(second_key, []).append(row)
        return groups

    # ------------------------------------------------------------------
    # Group parsing
    # ------------------------------------------------------------------

    def _parse_group(self, ts_key: str, rows: list[dict[str, str]]) -> RawTransaction | None:
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
            return None

        # Parse the timestamp from the first main row.
        timestamp = self._parse_timestamp(main_rows[0][cols["date"]])

        # Determine the primary type of this group.
        primary, _ = self._split_type(main_rows[0][cols["type"]])
        raw_type_full = main_rows[0][cols["type"]].strip().lower()

        # Extract fee info from any fee rows.
        fee_amount, fee_currency = self._extract_fee(fee_rows)

        # Route to the appropriate handler.
        result: RawTransaction | None
        if primary in {"deposit", "withdraw"}:
            result = self._parse_single(
                main_rows[0], timestamp, fee_amount, fee_currency, raw_type_full
            )
        elif primary == "trade":
            result = self._parse_trade(main_rows, timestamp, fee_amount, fee_currency)
        elif primary == "dust":
            result = self._parse_dust(main_rows, timestamp, fee_amount, fee_currency)
        elif primary == "staking":
            result = self._parse_staking(
                main_rows[0], timestamp, fee_amount, fee_currency, raw_type_full
            )
        else:
            logger.warning(
                "ndax: unrecognised TYPE %r in group %s — skipping",
                main_rows[0][cols["type"]],
                ts_key,
            )
            result = None
        return result

    # ------------------------------------------------------------------
    # Transaction-type handlers
    # ------------------------------------------------------------------

    def _parse_trade(
        self,
        rows: list[dict[str, str]],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
    ) -> RawTransaction | None:
        cols = self._config.columns
        fiat_class = self._fiat_asset_class

        fiat_rows = [r for r in rows if r[cols["asset_class"]].strip() == fiat_class]
        crypto_rows = [r for r in rows if r[cols["asset_class"]].strip() != fiat_class]

        if not crypto_rows:
            logger.warning("ndax: TRADE group has no crypto rows — skipping")
            return None

        if len(crypto_rows) == _CRYPTO_CRYPTO_LEG_COUNT and not fiat_rows:
            # Crypto-to-crypto (e.g. SOL → BTC via DUST or direct trade).
            return self._parse_crypto_crypto_trade(crypto_rows, timestamp, fee_amount, fee_currency)

        if len(crypto_rows) != 1 or not fiat_rows:
            logger.warning(
                "ndax: unexpected TRADE row count (crypto=%d fiat=%d) — skipping",
                len(crypto_rows),
                len(fiat_rows),
            )
            return None

        crypto_row = crypto_rows[0]
        fiat_row = fiat_rows[0]

        asset = crypto_row[cols["asset"]].strip()
        fiat_currency_code = fiat_row[cols["asset"]].strip()
        crypto_amount = self._parse_amount(crypto_row[cols["amount"]], asset)
        fiat_amount = self._parse_amount(fiat_row[cols["amount"]], fiat_currency_code)

        # Determine direction: positive crypto amount = buy; negative = sell.
        # The fiat amount has the opposite sign (negative on buy, positive on sell).
        if crypto_amount > 0:
            # Buy: fiat_value is what was spent (we store it as a positive).
            fiat_value = abs(fiat_amount)
        else:
            # Sell: fiat_value is what was received.
            fiat_value = abs(fiat_amount)
            crypto_amount = abs(crypto_amount)

        rate = (fiat_value / crypto_amount) if crypto_amount else None

        tx_type = self._lookup_type("trade")
        return RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type=tx_type,
            asset=asset,
            amount=crypto_amount,
            amount_currency=asset,
            fiat_value=fiat_value,
            fiat_currency=fiat_currency_code,
            fee_amount=fee_amount,
            fee_currency=fee_currency,
            rate=rate,
            spot_rate=None,
            txid=None,
            raw_type=crypto_row[cols["type"]].strip().lower(),
            raw_row=crypto_row,
        )

    def _parse_crypto_crypto_trade(
        self,
        rows: list[dict[str, str]],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
    ) -> RawTransaction | None:
        """Handle a trade where both sides are crypto (e.g. SOL → BTC)."""
        cols = self._config.columns

        # Received asset has a positive AMOUNT; sent asset has a negative AMOUNT.
        received = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") > 0), None)
        sent = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") < 0), None)

        if not received or not sent:
            logger.warning("ndax: crypto-to-crypto TRADE group missing a leg — skipping")
            return None

        received_asset = received[cols["asset"]].strip()
        sent_asset = sent[cols["asset"]].strip()
        received_amount = self._parse_amount(received[cols["amount"]], received_asset)
        sent_amount = abs(self._parse_amount(sent[cols["amount"]], sent_asset))

        rate = (received_amount / sent_amount) if sent_amount else None

        # raw_row carries the received leg as primary; sent info is embedded
        # so downstream stages can reconstruct the full picture.
        primary_row = dict(received)
        primary_row["_ndax_sent_asset"] = sent_asset
        primary_row["_ndax_sent_amount"] = str(sent_amount)

        tx_type = self._lookup_type("trade")
        return RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type=tx_type,
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
            raw_row=primary_row,
        )

    def _parse_single(
        self,
        row: dict[str, str],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
        raw_type_full: str,
    ) -> RawTransaction | None:
        """Handle DEPOSIT and WITHDRAW rows (single-row groups)."""
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
            ) or ("fiat_deposit" if primary == "deposit" else "fiat_withdrawal")
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
            tx_type = self._lookup_type(primary)
            # TX_ID may be an on-chain txid for crypto withdrawals; leave it
            # as-is and let the normalizer validate it.
            txid_value = row[cols["tx_id"]].strip() or None
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
    ) -> RawTransaction | None:
        """Handle DUST conversion groups (DUST / IN, DUST / OUT)."""
        cols = self._config.columns

        received = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") > 0), None)
        sent = next((r for r in rows if self._parse_amount(r[cols["amount"]], "") < 0), None)

        if not received:
            return None

        received_asset = received[cols["asset"]].strip()
        received_amount = self._parse_amount(received[cols["amount"]], received_asset)

        primary_row = dict(received)
        if sent:
            sent_asset = sent[cols["asset"]].strip()
            sent_amount = abs(self._parse_amount(sent[cols["amount"]], sent_asset))
            primary_row["_ndax_sent_asset"] = sent_asset
            primary_row["_ndax_sent_amount"] = str(sent_amount)

        tx_type = self._lookup_type("dust") or "other"
        return RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type=tx_type,
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
            raw_row=primary_row,
        )

    def _parse_staking(
        self,
        row: dict[str, str],
        timestamp: datetime,
        fee_amount: Decimal | None,
        fee_currency: str | None,
        raw_type_full: str,
    ) -> RawTransaction | None:
        """Handle STAKING rows (REWARD, DEPOSIT, REFUND)."""
        cols = self._config.columns

        asset = row[cols["asset"]].strip()
        amount = abs(self._parse_amount(row[cols["amount"]], asset))

        if amount == Decimal("0"):
            return None

        tx_type = self._lookup_type(raw_type_full) or self._lookup_type("staking") or "income"
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
            txid=None,
            raw_type=raw_type_full,
            raw_row=row,
        )

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
