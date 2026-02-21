"""Sparrow Wallet importer — signed BTC/satoshi CSV format.

Each row in a Sparrow export represents one on-chain transaction.
Sparrow exports a single file per wallet; callers must call ``parse()``
once per file and merge results.

Unit detection
--------------
Sparrow writes ``Value``, ``Balance``, and ``Fee`` in either BTC (decimal)
or satoshis (integer) depending on the user's Preferences > Bitcoin Unit
setting.  The unit is detected **per file** by scanning the ``Value``
column across all rows before parsing any of them:

    any "." in Value column  BTC mode  multiply x 100_000_000  sats
    no  "." anywhere         sats mode  parse as integer Decimal

Scanning all rows (not just the first) is essential: a receive of exactly
one BTC in BTC mode is written as ``1.00000000``, which would look like
sats if we only checked the first row.

Because detection is per-file, files exported at different unit settings
are handled correctly by calling ``parse()`` independently for each file.

Transaction type inference
--------------------------
Sparrow has no explicit transaction type column.  Type is inferred from
the sign of the ``Value`` column:

    Value > 0  ->  ``"deposit"``     (received BTC)
    Value < 0  ->  ``"withdrawal"``  (sent BTC)

The transfer-match pipeline stage later determines whether a withdrawal
is a self-transfer (no taxable event) by cross-referencing ``Txid`` with
other sources (e.g. Shakepay).

Unconfirmed transactions
------------------------
When ``Date (UTC)`` equals the literal string ``"Unconfirmed"``, the
transaction is in the mempool: broadcast to the Bitcoin network but not
yet confirmed in a block.  No block timestamp exists yet.

These rows are **skipped** with a WARNING for now.  A future pipeline
stage will surface them in the TUI so the user can decide whether to
include them manually or wait for confirmation.

# TODO: future — expose unconfirmed rows in TUI for user review/edit before
# they confirm on-chain.  Will require an ``unconfirmed_transactions`` table
# in the .sirop schema and a dedicated review step before the boil stage.

Fiat value column
-----------------
When an exchange rate source is configured in Sparrow, it appends a
``Value (CAD)`` column (or similar) populated from CoinGecko daily rates.
These rates are **not** Bank of Canada rates and must never be used for
ACB.  The importer detects the column by matching ``Value \\(.+\\)`` in
the header row, stores the raw value in ``raw_row`` for audit purposes,
and always sets ``fiat_value=None`` on the emitted ``RawTransaction``.

The fiat file also has a trailing comment line:
    # Historical CAD values are taken from daily rates and ...
The importer skips any row whose date field starts with ``"#"``.
"""

import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

from sirop.importers.base import BaseImporter, InvalidCSVFormatError, load_importer_config
from sirop.models.importer import ImporterConfig
from sirop.models.raw import RawTransaction

logger = logging.getLogger(__name__)

_SATS_PER_BTC: Decimal = Decimal("100000000")

# Bitcoin txids are exactly 64 lowercase hex characters.
# We accept uppercase too (some tools emit mixed case).
_TXID_RE: re.Pattern[str] = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass(frozen=True)
class _SparrowOptions:
    """Sparrow-specific config fields extracted from the YAML.

    Bundled into a dataclass so ``SparrowImporter.__init__`` stays within
    the five-argument limit (PLR0913).
    """

    unconfirmed_sentinel: str
    amount_unit: str
    fee_nullable: bool
    fiat_value_pattern: str | None


class SparrowImporter(BaseImporter):
    """Imports Sparrow Wallet transaction CSV exports.

    One CSV row -> one ``RawTransaction``.  No row grouping is needed
    (unlike NDAX which groups multiple rows per economic event).
    """

    def __init__(self, config: ImporterConfig, options: _SparrowOptions) -> None:
        super().__init__(config)
        self._unconfirmed_sentinel = options.unconfirmed_sentinel
        # "auto_detect", "BTC", or "sats" — auto_detect is the normal mode.
        self._amount_unit = options.amount_unit
        self._fee_nullable = options.fee_nullable
        self._fiat_value_re: re.Pattern[str] | None = (
            re.compile(options.fiat_value_pattern) if options.fiat_value_pattern else None
        )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "SparrowImporter":
        """Construct a ``SparrowImporter`` from a YAML config file."""
        raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())
        config = load_importer_config(yaml_path, source_name=yaml_path.stem)

        optional_cols: dict[str, str] = {
            str(k): str(v) for k, v in raw.get("optional_columns", {}).items()
        }
        options = _SparrowOptions(
            unconfirmed_sentinel=str(raw.get("unconfirmed_sentinel", "Unconfirmed")),
            amount_unit=str(raw.get("amount_unit", "auto_detect")),
            fee_nullable=bool(raw.get("fee_nullable", True)),
            fiat_value_pattern=optional_cols.get("fiat_value_pattern"),
        )
        return cls(config, options)

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def parse(self, csv_path: Path) -> list[RawTransaction]:
        """Parse *csv_path* and return one ``RawTransaction`` per confirmed row.

        Unconfirmed rows (mempool transactions with no block timestamp) are
        skipped with a WARNING.  Rows that cannot be parsed are also skipped
        with a WARNING.  Results are sorted chronologically (ascending).
        """
        rows = self._read_csv(csv_path)
        if not rows:
            return []

        cols = self._config.columns
        self._validate_columns(
            rows[0],
            required_keys={
                self._config.date_column,
                cols["value"],
                cols["balance"],
                cols["fee"],
                cols["txid"],
            },
        )

        # Per-file unit detection: scan Value column before parsing any row.
        unit = self._detect_unit(rows)
        logger.debug("sparrow: detected unit %r for %s", unit, csv_path.name)

        # Detect optional fiat column (e.g. "Value (CAD)").
        fiat_col = self._detect_fiat_column(rows[0])
        if fiat_col:
            logger.debug(
                "sparrow: found fiat column %r — values stored in raw_row, "
                "discarded from ACB (CoinGecko rates != BoC rates)",
                fiat_col,
            )

        results: list[RawTransaction] = []
        unconfirmed_count = 0

        # Data rows start at row 2 (row 1 is the header).
        for row_num, row in enumerate(rows, start=2):
            date_raw = row.get(self._config.date_column, "").strip()

            if date_raw.startswith("#"):
                # Sparrow appends a comment line when a fiat column is enabled:
                # "# Historical CAD values are taken from daily rates..."
                continue

            if date_raw == self._unconfirmed_sentinel:
                # Mempool transaction: broadcast but not yet mined.
                # TODO: future — surface in TUI for user review/include decision
                #       before the boil stage; requires schema support.
                unconfirmed_count += 1
                logger.warning(
                    "sparrow: row %d is unconfirmed (mempool) — excluded; "
                    "re-tap this file after the transaction confirms on-chain",
                    row_num,
                )
                continue

            tx = self._parse_row(row, row_num, unit, fiat_col)
            if tx is not None:
                results.append(tx)

        if unconfirmed_count:
            logger.warning(
                "sparrow: %d unconfirmed transaction(s) skipped — "
                "re-tap %s after they confirm on-chain",
                unconfirmed_count,
                csv_path.name,
            )

        results.sort(key=lambda t: t.timestamp)
        return results

    # ------------------------------------------------------------------
    # Row parsing
    # ------------------------------------------------------------------

    def _parse_row(
        self,
        row: dict[str, str],
        row_num: int,
        unit: str,
        fiat_col: str | None,
    ) -> RawTransaction | None:
        cols = self._config.columns
        date_raw = row.get(self._config.date_column, "").strip()

        try:
            timestamp = self._parse_timestamp(date_raw)
        except InvalidCSVFormatError:
            logger.warning(
                "sparrow: cannot parse timestamp %r at row %d — skipping",
                date_raw,
                row_num,
            )
            return None

        # Parse signed value and infer transaction type from its sign.
        value_str = row.get(cols["value"], "").strip()
        if not value_str:
            logger.warning("sparrow: empty Value at row %d — skipping", row_num)
            return None

        try:
            signed_value = Decimal(value_str)
        except InvalidOperation:
            logger.warning(
                "sparrow: cannot parse Value %r at row %d — skipping",
                value_str,
                row_num,
            )
            return None

        if signed_value > 0:
            tx_type = "deposit"
        elif signed_value < 0:
            tx_type = "withdrawal"
        else:
            logger.warning("sparrow: zero Value at row %d — skipping", row_num)
            return None

        amount_sats = self._to_sats(abs(signed_value), unit)

        # Fee: empty string -> None (not zero).  Only present on sends.
        fee_str = row.get(cols["fee"], "").strip()
        fee_sats: Decimal | None = None
        if fee_str:
            try:
                fee_sats = self._to_sats(Decimal(fee_str), unit)
            except InvalidOperation:
                logger.warning(
                    "sparrow: cannot parse Fee %r at row %d — treating as None",
                    fee_str,
                    row_num,
                )

        # Txid: validate format; clear if malformed rather than dropping the row.
        txid_raw: str | None = row.get(cols["txid"], "").strip() or None
        if txid_raw and not _TXID_RE.fullmatch(txid_raw):
            logger.warning(
                "sparrow: txid at row %d is not a 64-char hex string — clearing",
                row_num,
            )
            txid_raw = None

        return RawTransaction(
            source=self._config.source_name,
            timestamp=timestamp,
            transaction_type=tx_type,
            asset="BTC",
            amount=amount_sats,
            amount_currency="BTC",
            # CoinGecko rates (fiat_col) are never used for ACB — always None here.
            fiat_value=None,
            fiat_currency=None,
            fee_amount=fee_sats,
            fee_currency="BTC" if fee_sats is not None else None,
            rate=None,
            spot_rate=None,
            txid=txid_raw,
            raw_type=tx_type,
            raw_row=row,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _detect_unit(self, rows: list[dict[str, str]]) -> str:
        """Return ``"BTC"`` if any Value contains a decimal point, else ``"sats"``.

        Scans all rows before parsing any of them so that receives with
        round BTC amounts (e.g. ``"1.00000000"``) are not misclassified
        as satoshis when they happen to appear before the first non-round row.

        A decimal point can only appear in BTC-mode exports (Sparrow writes
        exactly 8 decimal places in BTC mode; satoshi mode is always integer).
        """
        if self._amount_unit != "auto_detect":
            return self._amount_unit

        value_col = self._config.columns["value"]
        for row in rows:
            if "." in row.get(value_col, ""):
                return "BTC"
        return "sats"

    def _detect_fiat_column(self, first_row: dict[str, str]) -> str | None:
        """Return the fiat column header name if present, else ``None``.

        Matches against the configured ``fiat_value_pattern`` regex
        (e.g. ``Value \\(.+\\)`` matches ``"Value (CAD)"``).
        """
        if self._fiat_value_re is None:
            return None
        for header in first_row:
            if isinstance(header, str) and self._fiat_value_re.fullmatch(header):
                return header
        return None

    def _to_sats(self, value: Decimal, unit: str) -> Decimal:
        """Convert *value* (positive) from *unit* to satoshis.

        BTC mode: multiply by 100_000_000 and drop any fractional remainder
        (BTC amounts have at most 8 decimal places; the conversion is exact).
        Sats mode: return as-is.
        """
        if unit == "BTC":
            return (value * _SATS_PER_BTC).to_integral_value()
        return value
