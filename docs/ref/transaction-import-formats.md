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

```
Transaction Type,Date,Amount Debited,Debit Currency,Amount Credited,Credit Currency,Buy / Sell Rate,Direction,Spot Rate,Source / Destination,Blockchain Transaction ID
```

### Field Definitions

| Column | Type | Notes |
|--------|------|-------|
| `Transaction Type` | string | See transaction type taxonomy below |
| `Date` | ISO 8601 datetime | Format: `2022-09-10T00:36:12+00`. The timezone offset is always `+00` (UTC). Parse with timezone awareness. |
| `Amount Debited` | decimal or empty | Positive number. Empty if this transaction has no debit leg. |
| `Debit Currency` | string or empty | ISO currency code (`CAD`, `BTC`, `ETH`) or empty. |
| `Amount Credited` | decimal or empty | Positive number. Empty if this transaction has no credit leg. |
| `Credit Currency` | string or empty | ISO currency code or empty. |
| `Buy / Sell Rate` | decimal or empty | Shakepay's internal rate at time of trade. **Do not use for ACB.** Not a Bank of Canada rate. |
| `Direction` | string | `"purchase"`, `"debit"`, `"credit"` |
| `Spot Rate` | decimal or empty | Shakepay's spot rate in CAD. **Do not use for ACB.** |
| `Source / Destination` | string or empty | For crypto withdrawals: the destination Bitcoin address. For fiat deposits: the source identifier (e.g. email). |
| `Blockchain Transaction ID` | string or empty | On-chain txid for crypto cashouts and deposits. Empty for fiat and internal transactions. Use for node verification cross-reference. |

### Transaction Type Taxonomy

| `Transaction Type` | Description | Tax Relevance |
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
"Transaction Type","Date","Amount Debited","Debit Currency","Amount Credited","Credit Currency","Buy / Sell Rate","Direction","Spot Rate","Source / Destination","Blockchain Transaction ID"
"fiat funding","2022-09-10T00:36:12+00",,,500,"CAD",,"credit",,"myemail@example.com",
"purchase/sale","2022-09-10T00:56:41+00",250,"CAD",0.00405049,"BTC","61720.8993","purchase",,,
"crypto cashout","2022-09-10T00:58:55+00",0.00405049,"BTC",,,,"debit","61027.7612","bc1q6f5b95fe8cc165adad7bb399dd7416f25f08348dc0f7cdbdbca6b01ca9","887534e0dbe0af1c77ea5b7e45876dd40b5e9664f1bce7384071023406e2729d"
"shakingsats","2022-09-11T14:00:00+00",,,0.00001500,"BTC",,"credit",,,
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
2. If any row contains a decimal point in the `Value` field → unit is BTC. Parse all `Value`, `Balance`, and `Fee` fields as BTC and convert to satoshis by multiplying by `100_000_000`.
3. If no row contains a decimal point → unit is satoshis. Parse as integer.
4. Use Python `Decimal` for all BTC parsing. Never use `float`.
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

A `crypto cashout` from Shakepay that moves BTC to a Sparrow wallet is a **self-transfer** — not a disposition. The importer pipeline must detect these and tag them appropriately so they are excluded from ACB calculations.

**Matching logic:**

1. For each Shakepay `crypto cashout` row with a non-empty `Blockchain Transaction ID`:
   - Look for a Sparrow row with the same `Txid`
   - If found: tag both records as `transfer_pair` with a shared transfer ID
   - If not found: flag as `unmatched_withdrawal` for manual review

2. For each Sparrow received transaction (positive `Value`):
   - If not matched to a Shakepay cashout: tag as `external_receive`
   - External receives require the user to provide ACB (e.g. from another exchange)

3. The calculation engine must never compute a capital gain/loss on a `transfer_pair` transaction.

**Implementation note:** Store the transfer matching result in the database, not as a derived calculation. This allows the user to override a match manually via the TUI.

---

## 4. YAML Configuration Reference

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
  transaction_type: "Transaction Type"
  timestamp: "Date"
  amount_debited: "Amount Debited"
  debit_currency: "Debit Currency"
  amount_credited: "Amount Credited"
  credit_currency: "Credit Currency"
  direction: "Direction"
  source_destination: "Source / Destination"
  txid: "Blockchain Transaction ID"

ignored_columns:
  - "Buy / Sell Rate"   # Shakepay internal rate — not BoC, never use for ACB
  - "Spot Rate"          # Same

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

timestamp_format: "%Y-%m-%d %H:%M:%S"
timestamp_timezone: UTC
unconfirmed_sentinel: "Unconfirmed"

amount_unit: auto_detect   # Detect BTC vs sats from presence of decimal point
amount_type: signed        # Positive = receive, negative = send
fee_nullable: true         # Empty fee field = None, not zero
```

---

## 5. Parser Implementation Requirements

The following requirements apply to all importers regardless of source.

1. **Use `Decimal` for all monetary values.** Never use `float` for BTC amounts or CAD values.
2. **Normalize to satoshis** at the boundary of the import layer. The calculation engine only handles integer satoshis.
3. **Preserve raw values.** Store the original string values from the CSV in the raw import table before any conversion. This is the audit trail.
4. **Reject malformed rows gracefully.** A row that fails parsing must be logged with row number and reason, and must not abort the entire import.
5. **Detect duplicate imports.** Use `(source_id, txid, timestamp)` as the deduplication key. If a row already exists in the database, skip it and log a notice.
6. **Never discard unknown transaction types.** If Shakepay adds a new type, store the record and flag it for manual review rather than silently dropping it.
7. **Validate txid format.** Bitcoin txids must be 64-character lowercase hex strings. Reject and flag rows where txid is present but malformed.