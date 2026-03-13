# Exchange & Wallet Export Format Specifications

**Document purpose:** Defines the CSV schemas for each data source sirop must parse. Claude Code must implement importers that conform exactly to these specifications. Do not infer column semantics from column names alone — read the notes for each field carefully.

**Sources covered:** Shakepay, Sparrow Wallet, NDAX
**Sources pending:** Koinly (separate document)

> **NDAX** is implemented (`NDAXImporter`, `config/importers/ndax.yaml`).
> The AlphaPoint Ledgers format spec is documented at the end of this file.

---

## Authoritative Sources

| Source | Schema Authority |
|--------|-----------------|
| Shakepay | `github.com/StevenBlack/Shakepay-export-fix` (sample data) + `help.shakepay.com` (official export docs) + Koinly import guide (cross-reference) |
| Sparrow | `WalletTransactions.java` from `github.com/sparrowwallet/sparrow` (exact source of CSV generation) |

---

## 1. Shakepay

### Export Mechanics

Shakepay splits transaction history into **multiple CSV files** by currency type:
- Crypto transactions (BTC/ETH activity)
- CAD transactions
- USD transactions (if applicable)

The importer must accept and merge all files from a single Shakepay export. Files are identified by content (header row), not filename.

### Header Row

2025 export format (current):

```
Date,Amount Debited,Asset Debited,Amount Credited,Asset Credited,Market Value,Market Value Currency,Book Cost,Book Cost Currency,Type,Spot Rate,Buy / Sell Rate,Description
```

> **Column renames from the pre-2025 format:** `Transaction Type` → `Type`, `Debit Currency` → `Asset Debited`, `Credit Currency` → `Asset Credited`. Columns removed: `Direction`, `Source / Destination`, `Blockchain Transaction ID`. Columns added: `Market Value`, `Market Value Currency`, `Book Cost`, `Book Cost Currency`, `Description`.

### Field Definitions

| Column | Type | Notes |
|--------|------|-------|
| `Date` | ISO 8601 datetime | Format: `2022-09-10T00:36:12+00`. The timezone offset is always `+00` (UTC). Parse with timezone awareness. |
| `Amount Debited` | decimal or empty | Positive number. Empty if this transaction has no debit leg. |
| `Asset Debited` | string or empty | ISO currency code (`CAD`, `BTC`, `ETH`) or empty. |
| `Amount Credited` | decimal or empty | Positive number. Empty if this transaction has no credit leg. |
| `Asset Credited` | string or empty | ISO currency code or empty. |
| `Market Value` | decimal or empty | Shakepay's computed market value at trade time. **Do not use for ACB.** Not a Bank of Canada rate. |
| `Market Value Currency` | string or empty | Currency of `Market Value` (e.g. `CAD`). |
| `Book Cost` | decimal or empty | Shakepay's computed book cost. **Do not use for ACB.** |
| `Book Cost Currency` | string or empty | Currency of `Book Cost`. |
| `Type` | string | See transaction type taxonomy below. |
| `Buy / Sell Rate` | decimal or empty | Shakepay's internal rate at time of trade. **Do not use for ACB.** Not a Bank of Canada rate. |
| `Spot Rate` | decimal or empty | Shakepay's spot rate in CAD. **Do not use for ACB.** |
| `Description` | string or empty | Free-text description. May contain a counterparty identifier (e.g. email for peer transfers) or a transaction reference. Not a structured blockchain txid — do not rely on for transfer matching. |

### Transaction Type Taxonomy

| `Type` | Description | Tax Relevance |
|--------------------|-------------|---------------|
| `purchase/sale` | CAD ↔ BTC or ETH trade | **Disposition event.** Buying BTC increases ACB. Selling BTC triggers capital gain/loss calculation. |
| `crypto cashout` | BTC withdrawal to external wallet | **Potential disposition.** Treat as a transfer unless the destination is your own wallet (verify via node/Sparrow match). Does not trigger gain/loss if self-transfer. |
| `fiat funding` | CAD deposit from bank | No tax event. |
| `crypto purchase` | BTC deposit from external wallet | Acquire at ACB from originating transaction. |
| `shakingsats` | Shakepay rewards (BTC earned) | **Income event.** Fair market value in CAD at receipt is income; becomes ACB of acquired BTC. |
| `other` | Miscellaneous credit (e.g. referral bonus) | **Income event** if BTC or CAD credited. Confirm case by case. |
| `peer transfer` | Shakepay-to-Shakepay transfers | Treat as internal transfer; no disposition. |

> **Note:** The transaction type list above is derived from empirical data. Additional types may exist. The importer must not crash on unknown types — log a warning and flag for manual review.

### BTC Quantity Precision

BTC amounts are expressed as decimal values (e.g. `0.00405049`). Parse as Python `Decimal`, never `float`.

### Rates to Discard

`Buy / Sell Rate` and `Spot Rate` are Shakepay's proprietary rates, not the Bank of Canada USD/CAD rate required by CRA. **Never use these for ACB calculation.** The importer must store them in the raw record for audit trail but must not expose them to the calculation engine. The calculation engine must fetch BoC rates independently.

### Sample Records

```csv
Date,Amount Debited,Asset Debited,Amount Credited,Asset Credited,Market Value,Market Value Currency,Book Cost,Book Cost Currency,Type,Spot Rate,Buy / Sell Rate,Description
2022-09-10T00:36:12+00,,,500,CAD,500,CAD,500,CAD,fiat funding,,,myemail@example.com
2022-09-10T00:56:41+00,250,CAD,0.00405049,BTC,250,CAD,250,CAD,purchase/sale,,61720.8993,
2022-09-10T00:58:55+00,0.00405049,BTC,,,,,,,crypto cashout,61027.7612,,
2022-09-11T14:00:00+00,,,0.00001500,BTC,,,,,shakingsats,,,
```

---

## 2. Sparrow Wallet

### Export Mechanics

Sparrow exports a **single CSV file** per wallet via `File > Export Wallet > Transactions` or the Export CSV button on the Transactions tab.

If you manage multiple wallets in Sparrow (e.g. a hot wallet and a cold storage wallet), each wallet produces a separate export file. The importer must process each file independently and associate records with the correct wallet identifier.

### Header Row

```
Date (UTC),Label,Value,Balance,Fee,Txid
```

Or, if an exchange rate source is configured in Sparrow preferences and the app is online at export time:

```
Date (UTC),Label,Value,Balance,Fee,Value (CAD),Txid
```

The fiat column header varies by configured currency (e.g. `Value (CAD)`, `Value (USD)`). The `Txid` column is always last.

### Field Definitions

| Column | Type | Notes |
|--------|------|-------|
| `Date (UTC)` | datetime string or literal | Format: `yyyy-MM-dd HH:mm:ss` in UTC (e.g. `2024-03-15 14:22:10`). Unconfirmed mempool transactions write the literal string `Unconfirmed`. Treat `Unconfirmed` rows as pending — do not include in ACB calculations until confirmed. |
| `Label` | string | User-assigned label from Sparrow's label system. May be empty. Use as the transaction memo/description in the internal record. |
| `Value` | decimal or integer | **See unit detection rules below.** Signed: positive = received, negative = sent. |
| `Balance` | decimal or integer | Running wallet balance after this transaction. Same unit as `Value`. |
| `Fee` | decimal, integer, or empty | Transaction miner fee. **Empty for received transactions** where inputs belong to third parties (Sparrow cannot calculate the fee without access to input UTXOs). Same unit as `Value`. Never negative. |
| `Value (XXX)` | decimal or empty | **Conditionally present.** Fiat value at daily exchange rate from CoinGecko. **Do not use for ACB.** Not a Bank of Canada rate. Discard for tax calculations; retain for audit trail only. |
| `Txid` | hex string | 64-character lowercase hex transaction ID. Use for on-chain verification and cross-referencing with Shakepay `Blockchain Transaction ID` to identify self-transfers. |

### Critical: Unit Detection

Sparrow writes `Value`, `Balance`, and `Fee` in either BTC (decimal) or satoshis (integer) depending on the user's `Preferences > Bitcoin Unit` setting. The setting may be `BTC`, `Satoshis`, or `Auto` (auto selects sats if balance < 0.01 BTC, BTC otherwise).

**The importer must detect the unit from the data, not assume it:**

1. Inspect the `Value` column across all rows in the file.
2. If any row contains a decimal point (`.`) **or a locale decimal comma (`,`)** in the
   `Value` field → unit is BTC. Parse all `Value`, `Balance`, and `Fee` fields as BTC
   decimals. (French/European Sparrow installations use `,` as the decimal separator —
   e.g. `"0,00005123"` instead of `"0.00005123"`. Satoshi-mode values are always plain
   integers and never contain either separator.)
3. If no row contains either separator → unit is satoshis. Parse as integer.
4. Use Python `Decimal` for all BTC parsing. Never use `float`. When comma-decimal
   format is detected, normalise each field by replacing `,` with `.` before passing
   to `Decimal()`.
5. After normalization, all internal values must be stored as integer satoshis.

### Fiat Column Detection

The importer must handle files with and without the fiat column:

1. Parse the header row.
2. Check whether a column matching the pattern `Value \(.+\)` is present.
3. If present, record the currency code from the header (e.g. `CAD`) and store the value in the raw record.
4. Pass `fiat_value = None` to the calculation engine regardless — this column is never used for ACB.

### Unconfirmed Transactions

Rows where `Date (UTC)` equals the literal string `Unconfirmed` must be:
- Stored in the raw import table with `confirmed = False` and `date = NULL`
- Excluded from all ACB and gain/loss calculations
- Surfaced to the user as a warning in the TUI

### Fee Availability

`Fee` is empty (`""`) for:
- All received transactions (inputs are third-party UTXOs not in the wallet)
- Any transaction where Sparrow could not resolve all input transactions

The importer must treat an empty `Fee` as `None`, not zero. Do not impute a fee value. For tax purposes, only fees on **disposition transactions** (sends/sales) are deductible, and those fees will typically be populated since Sparrow can resolve its own outgoing UTXOs.

### File Footer

When a fiat currency is configured, Sparrow appends a blank line followed by a comment line:

```
# Historical CAD values are taken from daily rates and should only be considered as approximate.
```

The CSV parser must tolerate this trailing comment without error.

### Sample Records (BTC unit, with fiat column)

```csv
Date (UTC),Label,Value,Balance,Fee,Value (CAD),Txid
2024-01-15 09:14:32,received from shakepay,0.00450000,0.00450000,,36.23,abcd1234...
2024-03-02 17:45:01,cold storage consolidation,-0.00450000,0.00000000,0.00002100,-37.15,efgh5678...
Unconfirmed,pending incoming,0.01000000,0.01000000,,801.50,ijkl9012...

# Historical CAD values are taken from daily rates and should only be considered as approximate.
```

### Sample Records (Satoshi unit, no fiat column)

```csv
Date (UTC),Label,Value,Balance,Fee,Txid
2024-01-15 09:14:32,received from shakepay,450000,450000,,abcd1234...
2024-03-02 17:45:01,cold storage consolidation,-450000,0,21000,efgh5678...
```

---

## 3. Cross-Source Transfer Matching

A withdrawal from Shakepay or NDAX that moves BTC to a Sparrow wallet is a
**self-transfer** — not a disposition. The transfer_match stage detects these
and marks both legs non-taxable so they are excluded from ACB calculations.

### Matching strategies

Four passes are used, in priority order:

#### Pass 1 — txid match (definitive)

If both legs carry the same non-null 64-character hex blockchain txid, they are
paired immediately as a definitive self-transfer. Works for Sparrow ↔ Sparrow
transfers and any future exchange that exports real txids.

#### Pass 1.25 — address-based txid resolution (Shakepay → Sparrow)

Shakepay's **2025 export format removed the `Blockchain Transaction ID`
column**. For BTC withdrawals (`crypto cashout` / `send`) the `Description`
field contains the recipient Bitcoin address (e.g. `"Bitcoin address bc1q…"`),
not a txid.

The Shakepay importer detects this prefix and:
- Sets `txid = None` on the `RawTransaction`
- Stores `notes = "Sent to: bc1q…"` (the recipient address)

Pass 1.25 in the transfer matcher then:
1. Collects unmatched withdrawals whose `notes` starts with `"Sent to: "`
2. Queries the Mempool API `GET /api/address/{addr}/txs` to find transactions
   that sent value to that address
3. Matches by satoshi amount (±1000 sat tolerance) and block time (±24h window)
4. If a match is found, re-runs Pass 1 txid matching using the resolved txid
   against unmatched Sparrow deposits

This bridges the asymmetry: one side has a Bitcoin address, the other has the
real on-chain txid (from Sparrow's `Txid` column). Pass 1.25 uses the same
privacy gate as graph traversal (`BTC_TRAVERSAL_MAX_HOPS > 0` and user-confirmed
mempool URL).

#### Pass 1b — BTC UTXO graph traversal (multi-hop)

For transfers routed through intermediate transactions (e.g. UTXO consolidation,
CoinJoin inputs). Both sides must carry a real blockchain txid. Shakepay
withdrawals with `txid = None` are correctly excluded from this pass.
See `docs/ref/bitcoin-node-validation-module.md` for details.

#### Pass 2 — amount + timestamp proximity (probabilistic)

Fallback for sources that export no blockchain txid. Used for:
- **NDAX withdrawals**: `TX_ID` is an internal order integer, not a txid.
  The NDAX importer always sets `txid = None`.
- **Shakepay withdrawals** that were not resolved by Pass 1.25 (e.g. mempool
  unavailable, or address lookup returned no matching transaction).

Match criteria:
1. Same asset (e.g. BTC)
2. Deposit amount ≈ withdrawal amount (within 1% or 0.00001 BTC absolute floor)
3. Deposit timestamp within ±8 hours of the withdrawal timestamp

**This match is probabilistic.** Two independent transfers of the same amount
on the same day would be incorrectly merged into a single self-transfer pair.
The pipeline currently treats every amount+timestamp match as a self-transfer.

> **TODO (user confirmation):** Amount+timestamp matched transfers must
> eventually be surfaced to the user for explicit confirmation. A
> `match_confidence` field (`"txid"` | `"amount_time"` | `"manual"`) should
> be added to `classified_events` so the TUI can flag probabilistic pairs and
> require a user acknowledgement before they are finalised.

> **TODO (node integration):** Once the Bitcoin node module is integrated
> (see `docs/ref/bitcoin-node-validation-module.md`), it will be able to
> verify self-transfers by checking whether the deposit address derives from
> the user's own xpub. This provides cryptographic confirmation for
> amount+timestamp matches and eliminates the ambiguity.

### Fallback behaviour

A withdrawal with no matching deposit is treated as an **unmatched withdrawal**
— a taxable sell event — and a warning is logged. This is the conservative
choice: over-reporting a gain is less harmful than silently dropping a taxable
event. The user can correct it by tapping the receiving wallet.

### Implementation note

Store transfer matching results in the database (`classified_events.is_taxable`
and a future `match_confidence` column), not as derived calculations. This
allows the user to override a match via the TUI without re-running the pipeline.

---

## 4. NDAX (AlphaPoint Ledgers format)

> **Status:** Implemented (`NDAXImporter`, `config/importers/ndax.yaml`).

### Export Mechanics

Export via NDAX web → **Reports → Create Report → CSV → Ledgers** report type.
NDAX produces a single flat file containing one ledger row per asset movement.
A single economic event (e.g. buying BTC with CAD) produces **multiple rows**
sharing the same `TX_ID` and `DATE` (to-the-second precision). The importer
groups rows by truncated timestamp and collapses each group into one record.

### Header Row

```
ASSET,ASSET_CLASS,AMOUNT,BALANCE,TYPE,TX_ID,DATE
```

### Field Definitions

| Column | Type | Notes |
|--------|------|-------|
| `ASSET` | string | Asset ticker: `BTC`, `ETH`, `CAD`, etc. |
| `ASSET_CLASS` | string | `FIAT` or `CRYPTO`. Used to distinguish fiat legs from crypto legs in a trade group. |
| `AMOUNT` | decimal | **Signed.** Positive = credit (received). Negative = debit (sent). Parse as `Decimal`. |
| `BALANCE` | decimal | Running balance of this asset after this row. Informational — not used for calculations. |
| `TYPE` | string | Primary type, optionally followed by ` / SECONDARY` (e.g. `TRADE / FEE`). See type taxonomy below. |
| `TX_ID` | integer string | **NDAX internal order identifier only.** Not a blockchain txid. Never store as txid. See critical note below. |
| `DATE` | ISO 8601 datetime | Format: `2024-01-15T14:00:00.000Z`. Parse with `datetime.fromisoformat()` (handles trailing Z and fractional seconds). All timestamps are UTC. |

### CRITICAL: TX_ID Is Not a Blockchain Txid

`TX_ID` is a short integer that uniquely identifies an order within NDAX's
internal ledger system (e.g. `10008`, `90002`). It is **not** an on-chain
Bitcoin transaction ID and **must never be stored as `txid`** on the resulting
`RawTransaction`.

Consequences:
- NDAX withdrawals have `txid = None` in the pipeline.
- Transfer matching for NDAX → Sparrow uses **amount + timestamp proximity
  only** (Strategy B in §3 above).
- Do not attempt to validate or use TX_ID for cross-source matching.

### TYPE Taxonomy

`TYPE` uses a `PRIMARY / SECONDARY` format. Rows where the secondary part is
`FEE` are extracted as the fee for the group before type routing.

| TYPE (primary) | Meaning | Maps to |
|---|---|---|
| `TRADE` | Trade leg (BTC or CAD side of a buy/sell) | `"buy"` or `"sell"` (resolved from signed AMOUNT) |
| `TRADE / FEE` | Trading fee for the same TX_ID group | `fee_amount` / `fee_currency` on the trade record |
| `DEPOSIT` | Fiat or crypto deposit | `"fiat_deposit"` (fiat) or `"deposit"` (crypto) |
| `WITHDRAW` | Fiat or crypto withdrawal | `"fiat_withdrawal"` (fiat) or `"withdrawal"` (crypto) |
| `WITHDRAW / FEE` | On-chain fee for the same TX_ID group | `fee_amount` / `fee_currency` on the withdrawal record |
| `STAKING / REWARD` | Staking income | `"income"` |
| `STAKING / DEPOSIT` | Crypto locked for staking | `"transfer_out"` |
| `STAKING / REFUND` | Crypto returned from staking | `"transfer_in"` |
| `DUST / IN`, `DUST / OUT` | Dust conversion legs | `"other"` |

### Sample Records

```csv
ASSET,ASSET_CLASS,AMOUNT,BALANCE,TYPE,TX_ID,DATE
CAD,FIAT,5005.00,5005.00,DEPOSIT,90001,2024-01-14T10:00:00.000Z
BTC,CRYPTO,0.10000000,0.10000000,TRADE,90002,2024-01-15T14:00:00.000Z
CAD,FIAT,-5000.00,5.00,TRADE,90002,2024-01-15T14:00:00.000Z
CAD,FIAT,-5.00,0.00,TRADE / FEE,90002,2024-01-15T14:00:00.000Z
BTC,CRYPTO,-0.10000000,0.00000000,WITHDRAW,90003,2024-01-16T10:00:00.000Z
```

Note that TX_ID `90003` on the WITHDRAW row is an internal NDAX order number.
The corresponding Sparrow deposit will carry the actual on-chain txid — these
two records are linked only by amount + timestamp in the transfer_match stage.

---

## 5. YAML Configuration Reference

The exchange format configurations live in `config/exchanges/`. Each file defines how to parse a specific source. Below is the expected structure for the two sources covered here.

```yaml
# config/exchanges/shakepay.yaml
source_id: shakepay
display_name: Shakepay
file_pattern: "*.csv"
delimiter: ","
encoding: utf-8
has_header: true

columns:
  transaction_type: "Type"
  timestamp: "Date"
  amount_debited: "Amount Debited"
  debit_currency: "Asset Debited"
  amount_credited: "Amount Credited"
  credit_currency: "Asset Credited"
  txid: "Description"

ignored_columns:
  - "Buy / Sell Rate"        # Shakepay internal rate — not BoC, never use for ACB
  - "Spot Rate"              # Same
  - "Market Value"           # Shakepay computed value — not BoC, never use for ACB
  - "Market Value Currency"  # Currency of Market Value
  - "Book Cost"              # Shakepay computed book cost — never use for ACB
  - "Book Cost Currency"     # Currency of Book Cost

timestamp_format: "%Y-%m-%dT%H:%M:%S%z"
timestamp_timezone: UTC

amount_type: decimal_btc   # Parse BTC fields as Decimal; convert to sats internally
```

```yaml
# config/exchanges/sparrow.yaml
source_id: sparrow
display_name: Sparrow Wallet
file_pattern: "*.csv"
delimiter: ","
encoding: utf-8
has_header: true

columns:
  timestamp: "Date (UTC)"
  label: "Label"
  value: "Value"
  balance: "Balance"
  fee: "Fee"
  txid: "Txid"

optional_columns:
  fiat_value_pattern: "Value \\(.+\\)"   # Regex — capture currency code from header

ignored_columns:
  - fiat_value_pattern   # CoinGecko daily rate — not BoC, never use for ACB

# Regex patterns for optional headers that may appear in some Sparrow exports.
# The format detector matches incoming CSV headers against these patterns and
# suppresses the "unknown column" drift-detection warning for any match.
# This prevents false-positive warnings for the optional fiat column
# (e.g. "Value (CAD)", "Value (EUR)") which Sparrow appends when an exchange
# rate source is configured.
known_column_patterns:
  - "Value \\(.+\\)"   # optional fiat column — currency code varies by locale

timestamp_format: "%Y-%m-%d %H:%M:%S"
timestamp_timezone: UTC
unconfirmed_sentinel: "Unconfirmed"

amount_unit: auto_detect   # Detect BTC vs sats from presence of '.' or ',' separator
amount_type: signed        # Positive = receive, negative = send
fee_nullable: true         # Empty fee field = None, not zero
```

---

## 6. Parser Implementation Requirements

The following requirements apply to all importers regardless of source.

1. **Use `Decimal` for all monetary values.** Never use `float` for BTC amounts or CAD values.
2. **Normalize to satoshis** at the boundary of the import layer. The calculation engine only handles integer satoshis.
3. **Preserve raw values.** Store the original string values from the CSV in the raw import table before any conversion. This is the audit trail.
4. **Reject malformed rows gracefully.** A row that fails parsing must be logged with row number and reason, and must not abort the entire import.
5. **Detect duplicate imports.** Use `(source_id, txid, timestamp)` as the deduplication key. If a row already exists in the database, skip it and log a notice.
6. **Never discard unknown transaction types.** If Shakepay adds a new type, store the record and flag it for manual review rather than silently dropping it.
7. **Validate txid format.** Bitcoin txids must be 64-character lowercase hex strings. If a column mapped to `txid` in the YAML config contains anything other than 64 lowercase hex characters (e.g. a Bitcoin address, a URL, an internal order ID), set `txid = None` — do not store the non-txid value in the `txid` field.

8. **Recipient address convention.** If an exchange or wallet exports the **recipient Bitcoin address** (not the on-chain txid) for an outgoing transfer, store it in `notes` as `"Sent to: <addr>"` and set `txid = None`. This enables Pass 1.25 address-based txid resolution in the transfer matcher, which queries `GET /api/address/{addr}/txs` to find the real txid and match it against a Sparrow deposit. The Shakepay importer (`shakepay.py`) is the reference implementation: it detects `"Bitcoin address bc1q…"` in the Description column and applies this convention automatically.

9. **Label / annotation field.** Any wallet importer (Sparrow, Electrum, Blue Wallet, etc.) that exports a user-editable label column **must** read it via the `label` key in its YAML column mapping and pass the value to `RawTransaction.notes`. This enables `sirop stir` to display the label as context for each transaction. If no `label` key exists in the config, `notes` defaults to `""`. See `config/importers/sparrow.yaml` for the reference YAML declaration and `src/sirop/importers/sparrow.py` for the implementation.