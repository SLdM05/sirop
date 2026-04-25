"""Shakepay importer — debit/credit CSV format.

Each row in a Shakepay export represents one transaction.  Shakepay splits
exports into multiple files by currency type (BTC/ETH activity, CAD, USD).
This importer handles a single file at a time; the caller is responsible for
merging results from multiple files by calling ``parse()`` once per file.

Direction resolution for ``purchase/sale`` rows
------------------------------------------------
Shakepay uses a debit/credit model rather than a signed amount column.
The ``purchase/sale`` transaction type covers both buy and sell trades.
Direction is determined by comparing Debit Currency and Credit Currency
against the configured ``fiat_currencies`` list:

    Debit=fiat + Credit=crypto  →  "buy"   (spending CAD, acquiring BTC)
    Debit=crypto + Credit=fiat  →  "sell"  (disposing BTC, receiving CAD)

The importer never emits ``"trade"`` — only ``"buy"`` or ``"sell"``.

Rates
-----
``Buy / Sell Rate`` and ``Spot Rate`` are Shakepay's proprietary rates,
not Bank of Canada rates.  They are preserved in ``raw_row`` for the audit
trail but are never exposed to the calculation engine.

``RawTransaction`` fields:
- ``rate``:      computed as ``fiat_value / amount`` for buy/sell trades only.
- ``spot_rate``: always ``None`` — Shakepay's spot rate ≠ BoC rate; discard.

Unknown transaction types
--------------------------
If Shakepay introduces a new type not in the ``transaction_type_map``, the
importer logs a warning and stores the row for manual review rather than
discarding it silently.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from sirop.importers.base import BaseImporter, InvalidCSVFormatError, load_importer_config
from sirop.models.importer import ImporterConfig
from sirop.models.raw import RawTransaction

logger = logging.getLogger(__name__)

# Internal sentinel: the type-map returns this for "purchase/sale" rows.
# The importer resolves it to "buy" or "sell" via _parse_purchase_sale().
_PURCHASE_SALE: str = "purchase_sale"

# Matches Shakepay Description values that contain a recipient crypto address
# rather than a blockchain txid.  Captures the address itself as group 1.
# Examples: "Bitcoin address bc1q...", "Ethereum address 0x..."
_ADDR_DESCRIPTION_RE = re.compile(r"^(?:Bitcoin|Ethereum) address (\S+)")


@dataclass(frozen=True)
class _DescOverride:
    """One entry from ``description_type_overrides`` in the YAML config.

    When a row's canonical type matches ``applies_to`` and the Description
    column contains ``pattern`` (case-insensitive), the canonical type is
    replaced with ``transaction_type``.
    """

    applies_to: str  # canonical type that triggers the check
    pattern: str  # case-insensitive substring to search for in Description
    transaction_type: str  # replacement canonical type


@dataclass
class _RowCtx:
    """Parsed state for one CSV row, threaded through the handler methods.

    Bundling these into a single object keeps every handler's argument count
    within the project's PLR0913 limit while avoiding a proliferation of
    positional parameters.
    """

    row: dict[str, str]
    timestamp: datetime
    debited: Decimal | None
    credited: Decimal | None
    debit_currency: str
    credit_currency: str
    txid_raw: str | None
    notes: str  # "Sent to: <addr>" when Description contains a crypto address
    raw_type: str
    row_num: int
    description: str  # raw Description value for override matching


class ShakepayImporter(BaseImporter):
    """Imports Shakepay transaction CSV exports.

    One CSV row → one ``RawTransaction``.  No row grouping is needed
    (unlike NDAX which groups multiple rows per economic event).
    """

    def __init__(self, config: ImporterConfig) -> None:
        super().__init__(config)
        # Frozenset for O(1) membership checks: "is this currency fiat?"
        self._fiat_currencies: frozenset[str] = frozenset(config.fiat_currencies)
        # Crypto assets that are allowed through the filter — all others are
        # skipped with a warning.  Populated from allowed_crypto_assets in YAML.
        self._allowed_crypto_assets: frozenset[str] = frozenset({"BTC"})
        # Description-based sub-classification overrides.  Loaded from
        # description_type_overrides in the YAML; applied after canonical type lookup.
        self._desc_overrides: list[_DescOverride] = []

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "ShakepayImporter":
        """Construct a ``ShakepayImporter`` from a YAML config file."""
        raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())
        config = load_importer_config(yaml_path, source_name=yaml_path.stem)
        importer = cls(config)
        allowed = raw.get("allowed_crypto_assets")
        if allowed:
            importer._allowed_crypto_assets = frozenset(str(a).upper() for a in allowed)
        overrides = raw.get("description_type_overrides") or []
        importer._desc_overrides = [
            _DescOverride(
                applies_to=str(o["applies_to"]),
                pattern=str(o["pattern"]),
                transaction_type=str(o["transaction_type"]),
            )
            for o in overrides
        ]
        return importer

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def parse(self, csv_path: Path) -> list[RawTransaction]:
        """Parse *csv_path* and return one ``RawTransaction`` per row.

        Rows that cannot be parsed are skipped with a warning.  Unknown
        transaction types are stored for manual review rather than dropped.
        Results are sorted chronologically (ascending timestamp).
        """
        rows = self._read_csv(csv_path)
        if not rows:
            return []

        cols = self._config.columns
        self._validate_columns(
            rows[0],
            required_keys={
                cols["transaction_type"],
                self._config.date_column,
                cols["amount_debited"],
                cols["debit_currency"],
                cols["amount_credited"],
                cols["credit_currency"],
                cols["txid"],
            },
        )

        results: list[RawTransaction] = []
        # Data starts at row 2 (row 1 is the header).
        for row_num, row in enumerate(rows, start=2):
            tx = self._parse_row(row, row_num)
            if tx is not None:
                results.append(tx)

        results.sort(key=lambda t: t.timestamp)
        return self._filter_btc_only(results)

    # ------------------------------------------------------------------
    # BTC-only filter
    # ------------------------------------------------------------------

    def _filter_btc_only(self, results: list[RawTransaction]) -> list[RawTransaction]:
        """Drop any RawTransaction whose asset is not in allowed_crypto_assets or fiat.

        Emits one warning per unique skipped asset symbol (not per row).
        """
        allowed = self._allowed_crypto_assets | self._fiat_currencies
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
                "shakepay: skipping %d non-BTC row(s) for asset %s — sirop is BTC-only",
                count,
                asset,
            )
        return kept

    # ------------------------------------------------------------------
    # Row-level dispatch
    # ------------------------------------------------------------------

    def _parse_row(self, row: dict[str, str], row_num: int) -> RawTransaction | None:
        cols = self._config.columns

        raw_type = row[cols["transaction_type"]].strip()
        raw_date = row[self._config.date_column].strip()

        if not raw_type or not raw_date:
            logger.debug("shakepay: skipping row %d — empty type or date", row_num)
            return None

        try:
            timestamp = self._parse_timestamp(raw_date)
        except InvalidCSVFormatError:
            logger.warning(
                "shakepay: cannot parse timestamp %r at row %d — skipping",
                raw_date,
                row_num,
            )
            return None

        # Detect whether the Description/txid column holds a real blockchain txid
        # or a recipient crypto address (e.g. "Bitcoin address bc1q...").
        # Shakepay's 2025 export dropped the dedicated txid column; withdrawals
        # now show the recipient address in Description, not the on-chain txid.
        _raw_desc = row[cols["txid"]].strip()
        _addr_m = _ADDR_DESCRIPTION_RE.match(_raw_desc)
        if _addr_m:
            _txid_raw: str | None = None
            _notes: str = f"Sent to: {_addr_m.group(1)}"
        else:
            _txid_raw = _raw_desc or None
            _notes = ""

        # Raw Description value — captured before address-extraction strips it from _txid_raw.
        _description = row[cols["txid"]].strip()

        ctx = _RowCtx(
            row=row,
            timestamp=timestamp,
            debited=self._parse_optional_decimal(
                row[cols["amount_debited"]].strip(), "Amount Debited", row_num
            ),
            credited=self._parse_optional_decimal(
                row[cols["amount_credited"]].strip(), "Amount Credited", row_num
            ),
            debit_currency=row[cols["debit_currency"]].strip(),
            credit_currency=row[cols["credit_currency"]].strip(),
            txid_raw=_txid_raw,
            notes=_notes,
            raw_type=raw_type,
            row_num=row_num,
            description=_description,
        )

        # Look up the canonical type; log a warning and fall through for unknowns.
        canonical_type = self._config.transaction_type_map.get(raw_type.lower())
        if canonical_type is None:
            logger.warning(
                "shakepay: unknown transaction type %r at row %d — storing for manual review",
                raw_type,
                row_num,
            )
            canonical_type = raw_type.lower()

        # Apply description-based overrides (e.g. "Reward" rows with "interest" description).
        canonical_type = self._apply_desc_override(canonical_type, ctx.description)

        if canonical_type == _PURCHASE_SALE:
            result = self._parse_purchase_sale(ctx)
        elif canonical_type == "buy":
            # 2025 Shakepay format: debit columns are empty; only BTC credit is
            # present.  fiat_value is left None; the normalizer fetches the BoC
            # rate to determine the CAD cost for ACB.
            result = self._parse_buy_credit_only(ctx)
        elif canonical_type == "withdrawal":
            result = self._parse_withdrawal(ctx)
        elif canonical_type == "deposit":
            result = self._parse_deposit(ctx)
        elif canonical_type in ("fiat_deposit", "fiat_withdrawal"):
            result = self._parse_fiat(ctx, canonical_type)
        elif canonical_type in (
            "income",
            "interest",
            "reward_shake",
            "reward_shakesquad",
            "reward_cashback",
        ):
            result = self._parse_income(ctx, canonical_type)
        else:
            # "other" (covers "peer transfer" and literal "other") and unknowns.
            result = self._parse_generic(ctx, canonical_type)

        return result

    # ------------------------------------------------------------------
    # Transaction-type handlers
    # ------------------------------------------------------------------

    def _parse_purchase_sale(self, ctx: _RowCtx) -> RawTransaction | None:
        """Resolve buy/sell direction from Debit/Credit Currency columns.

        Debit = fiat + Credit = crypto  →  "buy"
        Debit = crypto + Credit = fiat  →  "sell"
        """
        debit_is_fiat = ctx.debit_currency in self._fiat_currencies
        credit_is_fiat = ctx.credit_currency in self._fiat_currencies

        if debit_is_fiat and not credit_is_fiat and ctx.credit_currency:
            # Spending fiat to acquire crypto → buy.
            if ctx.debited is None or ctx.credited is None:
                logger.warning(
                    "shakepay: buy row at %d is missing debit or credit amount — skipping",
                    ctx.row_num,
                )
                return None
            tx_type = "buy"
            asset = ctx.credit_currency
            amount = ctx.credited
            fiat_value = ctx.debited
            fiat_currency = ctx.debit_currency

        elif not debit_is_fiat and ctx.debit_currency and credit_is_fiat:
            # Disposing crypto to receive fiat → sell.
            if ctx.debited is None or ctx.credited is None:
                logger.warning(
                    "shakepay: sell row at %d is missing debit or credit amount — skipping",
                    ctx.row_num,
                )
                return None
            tx_type = "sell"
            asset = ctx.debit_currency
            amount = ctx.debited
            fiat_value = ctx.credited
            fiat_currency = ctx.credit_currency

        else:
            logger.warning(
                "shakepay: purchase/sale at row %d has ambiguous currencies "
                "(debit=%r credit=%r) — skipping",
                ctx.row_num,
                ctx.debit_currency,
                ctx.credit_currency,
            )
            return None

        rate = fiat_value / amount if amount else None
        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type=tx_type,
            asset=asset,
            amount=amount,
            amount_currency=asset,
            fiat_value=fiat_value,
            fiat_currency=fiat_currency,
            fee_amount=None,  # spread model — fees embedded in rate, no explicit fee column
            fee_currency=None,
            rate=rate,
            spot_rate=None,  # Shakepay spot rate ≠ BoC rate; preserved in raw_row only
            txid=None,  # Description for buy/sell rows contains the rate string
            # ("Bought @ CA$X"), not a blockchain txid. Using it as a
            # txid causes false deduplication when the same rounded rate
            # appears in multiple rows. Rate is preserved in raw_row.
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
            notes=ctx.notes,
        )

    def _parse_buy_credit_only(self, ctx: _RowCtx) -> RawTransaction | None:
        """Handle 2025-format ``Buy`` rows — credit-only; debit columns are empty.

        Shakepay's 2025 CSV omits the fiat debit for BTC purchases.  The CAD
        cost is available in ``Book Cost`` but the YAML marks that column as
        ignored (it is Shakepay's proprietary spread-inclusive rate, not a BoC
        rate).  ``fiat_value`` is left ``None``; the normalizer fetches the BoC
        daily rate to determine the CAD cost for ACB, consistent with the
        treatment of income/reward rows.
        """
        if ctx.credited is None or not ctx.credit_currency:
            logger.warning(
                "shakepay: Buy row at %d has no credited amount — skipping",
                ctx.row_num,
            )
            return None
        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type="buy",
            asset=ctx.credit_currency,
            amount=ctx.credited,
            amount_currency=ctx.credit_currency,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=None,
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=None,
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
        )

    def _parse_withdrawal(self, ctx: _RowCtx) -> RawTransaction | None:
        """Handle ``crypto cashout`` rows (BTC sent to an external wallet)."""
        if ctx.debited is None:
            logger.warning(
                "shakepay: crypto cashout at row %d has no debit amount — skipping",
                ctx.row_num,
            )
            return None
        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type="withdrawal",
            asset=ctx.debit_currency,
            amount=ctx.debited,
            amount_currency=ctx.debit_currency,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=None,
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=ctx.txid_raw,
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
            notes=ctx.notes,
        )

    def _parse_deposit(self, ctx: _RowCtx) -> RawTransaction | None:
        """Handle ``crypto purchase`` rows (BTC received from an external wallet)."""
        if ctx.credited is None:
            logger.warning(
                "shakepay: crypto purchase at row %d has no credit amount — skipping",
                ctx.row_num,
            )
            return None
        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type="deposit",
            asset=ctx.credit_currency,
            amount=ctx.credited,
            amount_currency=ctx.credit_currency,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=None,
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=ctx.txid_raw,
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
            notes=ctx.notes,
        )

    def _parse_fiat(self, ctx: _RowCtx, canonical_type: str) -> RawTransaction | None:
        """Handle ``fiat funding`` (deposit) and ``fiat cashout`` (withdrawal) rows."""
        if canonical_type == "fiat_deposit":
            if ctx.credited is None:
                logger.warning(
                    "shakepay: fiat funding at row %d has no credit amount — skipping",
                    ctx.row_num,
                )
                return None
            amount = ctx.credited
            currency = ctx.credit_currency
        else:
            if ctx.debited is None:
                logger.warning(
                    "shakepay: fiat cashout at row %d has no debit amount — skipping",
                    ctx.row_num,
                )
                return None
            amount = ctx.debited
            currency = ctx.debit_currency

        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type=canonical_type,
            asset=currency,
            amount=amount,
            amount_currency=currency,
            fiat_value=amount,
            fiat_currency=currency,
            fee_amount=None,
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=None,
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
        )

    def _parse_income(self, ctx: _RowCtx, canonical_type: str = "income") -> RawTransaction | None:
        """Handle BTC reward/income rows (shakingsats, reward_shake, reward_cashback).

        ``fiat_value`` is left ``None``; the normalizer fetches the BoC rate
        to determine the CAD fair market value at receipt.
        ``canonical_type`` is passed through so reward subtypes are preserved.
        """
        if ctx.credited is None:
            logger.warning(
                "shakepay: income row at %d has no credit amount — skipping", ctx.row_num
            )
            return None
        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type=canonical_type,
            asset=ctx.credit_currency,
            amount=ctx.credited,
            amount_currency=ctx.credit_currency,
            fiat_value=None,  # normalizer derives FMV from BoC rate at receipt
            fiat_currency=None,
            fee_amount=None,
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=None,
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
        )

    def _parse_generic(self, ctx: _RowCtx, canonical_type: str) -> RawTransaction | None:
        """Handle ``peer transfer``, ``other``, and unknown types.

        Prefers the credit leg; falls back to the debit leg.
        """
        if ctx.credited is not None and ctx.credit_currency:
            asset = ctx.credit_currency
            amount = ctx.credited
        elif ctx.debited is not None and ctx.debit_currency:
            asset = ctx.debit_currency
            amount = ctx.debited
        else:
            logger.warning(
                "shakepay: %r row at %d has no amount — skipping",
                canonical_type,
                ctx.row_num,
            )
            return None

        return RawTransaction(
            source=self._config.source_name,
            timestamp=ctx.timestamp,
            transaction_type=canonical_type,
            asset=asset,
            amount=amount,
            amount_currency=asset,
            fiat_value=None,
            fiat_currency=None,
            fee_amount=None,
            fee_currency=None,
            rate=None,
            spot_rate=None,
            txid=ctx.txid_raw,
            raw_type=ctx.raw_type.lower(),
            raw_row=ctx.row,
            notes=ctx.notes,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _apply_desc_override(self, canonical_type: str, description: str) -> str:
        """Return a replacement canonical type if a description override matches.

        Iterates ``self._desc_overrides`` in order; returns the first match.
        Returns *canonical_type* unchanged when no override applies.
        """
        desc_lower = description.lower()
        for override in self._desc_overrides:
            if canonical_type == override.applies_to and override.pattern in desc_lower:
                logger.debug(
                    "shakepay: description override %r → %r (matched %r)",
                    canonical_type,
                    override.transaction_type,
                    override.pattern,
                )
                return override.transaction_type
        return canonical_type

    def _parse_optional_decimal(self, value: str, field_name: str, row_num: int) -> Decimal | None:
        """Return ``None`` for empty strings; parse non-empty values as ``Decimal``.

        Logs a warning (not an exception) on parse failure so a single bad
        field does not abort the entire import.
        """
        if not value:
            return None
        try:
            return Decimal(value)
        except InvalidOperation:
            logger.warning(
                "shakepay: cannot parse %r as Decimal in field %r at row %d",
                value,
                field_name,
                row_num,
            )
            return None
