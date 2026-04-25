---
verified-at: b5e6b66
tracks:
  - src/sirop/importers
  - config/importers
---

# Exchange & Wallet Export Format Specifications

**Document purpose:** Defines the schemas for each data source sirop can import. Claude Code must implement importers that conform exactly to these specifications. Do not infer column semantics from column names alone — read the notes for each field carefully.

**Sources covered:** Shakepay, Sparrow Wallet, NDAX, xpub/ypub/zpub wallet definitions
**Sources pending:** Koinly (separate document)

---

## Quick Reference — `sirop tap` Usage

```bash
# CSV imports (auto-detected by file content)
sirop tap shakepay_btc.csv
sirop tap sparrow_export.csv
sirop tap ndax_ledger.csv

# xpub wallet-definition YAML — imports multiple named wallets at once
sirop tap my_wallets.yaml

# Same, explicit source flag (required if file is not .yaml/.yml)
sirop tap my_wallets.yaml --source xpub

# JoinMarket — one YAML with all 10 branch xpubs, each as a distinct wallet
sirop tap joinmarket_wallets.yaml
```

The xpub importer **requires a private Mempool node** (`BTC_MEMPOOL_URL` in `.env`).
Address scanning sends derived Bitcoin addresses to the configured endpoint — use a
local node to avoid disclosing your wallet to a public service. Set
`BTC_TRAVERSAL_ALLOW_PUBLIC=true` only if you have accepted the privacy implications.

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
- Bitcoin transactions (BTC activity)
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
| `Date` | datetime | 2025 format: `2025-03-17 09:47:31` (space-separated, no timezone offset — treat as UTC). Pre-2025 format: `2022-09-10T00:36:12+00` (ISO 8601 with bare `+HH` offset). Both are accepted by `datetime.fromisoformat()` (Python 3.11+). |
| `Amount Debited` | decimal or empty | Positive number. Empty if this transaction has no debit leg. |
| `Asset Debited` | string or empty | ISO currency code (`CAD`, `BTC`) or empty. Non-BTC crypto rows are skipped by the importer. |
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
| `Type` value | Era | Description | Tax Relevance |
|---|---|---|---|
| `purchase/sale` | pre-2025 | CAD ↔ BTC trade (both buy and sell in one type; direction derived from debit/credit currency columns) | **Disposition event.** Buying BTC increases ACB. Selling BTC triggers capital gain/loss. |
| `Buy` | 2025 | BTC purchase — credit-only row; debit columns are empty; CAD cost is not recorded in the CSV | **Acquisition.** `fiat_value=None`; normalizer fetches BoC rate for ACB. |
| `crypto cashout` | pre-2025 | BTC withdrawal to external wallet | **Potential disposition.** Treat as a transfer unless the destination is your own wallet. |
| `Send` | 2025 | BTC or ETH withdrawal to external address (replaces `crypto cashout`) | Same treatment as `crypto cashout`. |
| `fiat funding` | both | CAD deposit from bank | No tax event. |
| `fiat cashout` | both | CAD withdrawal to bank | No tax event. |
| `crypto purchase` | pre-2025 | BTC deposit from external wallet | Acquire at ACB from originating transaction. |
| `shakingsats` | pre-2025 | ShakingSats daily BTC loyalty reward | **Reward event** → canonical type `reward_shake`. Tax treatment is configurable: default is **discount** (ACB=$0, not income); can be set to **income** (FMV as ACB). See `config/tax_rules.yaml` `reward_treatment`. |
| `Reward` | 2025 | ShakingSats daily BTC loyalty reward (renamed from `shakingsats`) | Same as `shakingsats` → `reward_shake`. |
| `shakesquads` | both | Shakesquads loyalty program BTC reward | **Reward event** → `reward_shake`. Same configurable treatment as `shakingsats`. |
| `card_cashback` | both | Shakepay Card BTC cashback on purchases | **Reward event** → `reward_cashback`. Default: **discount** (ACB=$0, not income). |
| `other` | both | Miscellaneous credit (e.g. referral bonus in CAD) | **Out of scope** for BTC-only rows. Unknown BTC `other` rows are flagged for manual review. |
| `peer transfer` | both | Shakepay-to-Shakepay transfers | Treat as internal transfer; no disposition. |

> **Note:** The transaction type list above is derived from empirical data. Additional types may exist. The importer must not crash on unknown types — log a warning and flag for manual review.
>
> **Reward tax treatment:** `reward_shake` and `reward_cashback` are distinct canonical types introduced in sirop. Their treatment (discount vs income) is controlled by the `reward_treatment` section of `config/tax_rules.yaml`. Under **discount** treatment, the BTC enters the ACB pool at $0 cost (no income recognition, full proceeds become capital gain on disposal). Under **income** treatment, FMV at receipt is recognised as income and becomes the ACB. Shakepay's own help centre states rewards are "not taxable at receipt"; CRA has issued no specific ruling on crypto reward cashback.

### BTC Quantity Precision

BTC amounts are expressed as decimal values (e.g. `0.00405049`). Parse as Python `Decimal`, never `float`.

### Rates to Discard

`Buy / Sell Rate` and `Spot Rate` are Shakepay's proprietary rates, not the Bank of Canada USD/CAD rate required by CRA. **Never use these for ACB calculation.** The importer must store them in the raw record for audit trail but must not expose them to the calculation engine. The calculation engine must fetch BoC rates independently.

### Sample Records

2025 format:

```csv
Date,Amount Debited,Asset Debited,Amount Credited,Asset Credited,Market Value,Market Value Currency,Book Cost,Book Cost Currency,Type,Spot Rate,Buy / Sell Rate,Description
2025-01-01 10:00:00,,,500,CAD,500,CAD,500,CAD,fiat funding,,,
2025-01-02 11:00:00,,,0.00405049,BTC,250,CAD,250,CAD,Buy,61027.76,61720.90,Bought @ CA$61720.90
2025-01-03 12:00:00,0.00103685,BTC,,,,,,,Send,,,Bitcoin address bc1qexampleaddress000000000000000000000000
2025-01-04 09:00:00,,,0.00001500,BTC,0.90,CAD,0.90,CAD,Reward,60000.00,,ShakingSats
```

Pre-2025 format (for reference — still accepted by the importer):

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
3. If present, record the full column header name (e.g. `Value (CAD)`) and store the fiat value in the raw record keyed by that header.
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
2024-01-15 09:14:32,received from shakepay,0.00450000,0.00450000,,36.23,aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111
2024-03-02 17:45:01,cold storage consolidation,-0.00450000,0.00000000,0.00002100,-37.15,bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222
Unconfirmed,pending incoming,0.01000000,0.01000000,,801.50,cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333cccc3333

# Historical CAD values are taken from daily rates and should only be considered as approximate.
```

### Sample Records (Satoshi unit, no fiat column)

```csv
Date (UTC),Label,Value,Balance,Fee,Txid
2024-01-15 09:14:32,received from shakepay,450000,450000,,aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111aaaa1111
2024-03-02 17:45:01,cold storage consolidation,-450000,0,21000,bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222
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
| `ASSET` | string | Asset ticker: `BTC`, `CAD`, etc. Non-BTC crypto rows (e.g. ETH, SOL) are skipped by the importer with a warning. |
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
| `DUST / IN` | Received asset leg of a dust conversion | `"buy"` — normalizer fetches BoC rate for ACB |
| `DUST / OUT` | Sent asset leg of a dust conversion | Not emitted as a standalone transaction; sent asset recorded in `raw_row` of the `DUST / IN` record |
| `DUST / FEE` | Fee leg of a dust conversion group | `fee_amount` / `fee_currency` on the `DUST / IN` record |
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

## 5. Configuration Files

Exchange-specific column mappings, type maps, fee models, and parsing rules live in
`config/importers/`. Each file is a YAML document whose structure is documented inline
with comments. The canonical references are:

- [`config/importers/shakepay.yaml`](../../config/importers/shakepay.yaml)
- [`config/importers/ndax.yaml`](../../config/importers/ndax.yaml)
- [`config/importers/sparrow.yaml`](../../config/importers/sparrow.yaml)

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

---

## xpub Wallet Definition (source: `xpub`)

**File type:** YAML (`.yaml` or `.yml`)
**Auto-detected:** yes — `sirop tap` detects `.yaml` suffix and routes to the xpub importer
**Explicit flag:** `sirop tap wallets.yaml --source xpub`

### File schema

```yaml
source: xpub       # required; identifies this as an xpub wallet definition

wallets:
  - name: <str>           # required — wallet name in the sirop batch database
    xpub: <str>           # required — account-level zpub / ypub / xpub
    gap_limit: <int>      # optional — consecutive empty addresses before stop (default: 20)
    branches: [0, 1]      # optional — HD branches to scan (default: both)
    label: <str>          # optional — free-text annotation stored in wallet record
    script_type: <str>    # optional — override prefix-based address encoding (see below)
```

### Supported key types

| Prefix | Default address type | HD standard | Notes |
|--------|----------------------|-------------|-------|
| `zpub` | P2WPKH (`bc1q…`)    | BIP84       | |
| `ypub` | P2SH-P2WPKH (`3…`)  | BIP49       | |
| `xpub` | P2PKH (`1…`)         | BIP44       | See `script_type` for exceptions |

### `script_type` field

Overrides prefix-based address derivation. Required when a wallet exports `xpub` prefix
but uses a different address type (most common: JoinMarket BIP84 wallets).

| Value | Address type |
|-------|-------------|
| `p2wpkh` | native SegWit `bc1q…` |
| `p2sh-p2wpkh` | wrapped SegWit `3…` |
| `p2pkh` | legacy `1…` |

When absent, address type is inferred from the key prefix as shown in the table above.
Validated at parse time — unknown values raise `ValueError` before any network request.

### Derivation

Given account xpub at path `m/purpose'/coin'/account'`, child addresses are derived at
`m/.../branch/index` where `branch=0` is external (receive) and `branch=1` is internal (change).

The key passed as `xpub` must be the **account-level key** at `m/purpose'/coin'/account'`.
Passing a branch-level key (e.g. at `m/84'/0'/0'/0`) would add an extra derivation step
and produce wrong addresses.

### Gap limit

Scanning stops when `gap_limit` consecutive address indices have no transaction history on
the Mempool API. Default is 20 (BIP44 standard). Increase to 50–100 for JoinMarket wallets
where coinjoin rounds create wide gaps in address usage.

### JoinMarket note

JoinMarket's wallet display shows two keys per mixdepth:

```
mixdepth        0       xpub6Bff...   ← account-level key (m/84'/0'/0') — USE THIS
external addresses  m/84'/0'/0'/0   xpub6Epx...   ← branch-level key — DO NOT USE
```

Use the **account-level key** (on the `mixdepth N` line) with `branches: [0, 1]`. The
branch-level key is already one derivation step below the account — passing it would
cause sirop to scan `m/84'/0'/0'/0/branch/index` instead of `m/84'/0'/0'/branch/index`.

JoinMarket exports `xpub` prefix for all keys regardless of address type. Set
`script_type: p2wpkh` to derive the correct `bc1q…` native SegWit addresses.

There are 5 mixing depths (0–4) by default. Import each as a separate entry with a
distinct wallet name. Example:

```yaml
source: xpub

wallets:
  - name: jm-depth0
    xpub: xpub6...       # account-level key from "mixdepth 0" line
    script_type: p2wpkh  # JoinMarket BIP84 wallet
    gap_limit: 50
    branches: [0, 1]

  - name: jm-depth1
    xpub: xpub6...       # account-level key from "mixdepth 1" line
    script_type: p2wpkh
    gap_limit: 50
    branches: [0, 1]
```

### Transaction mapping

| Mempool data        | sirop field             |
|---------------------|-------------------------|
| `net_sats > 0`      | `transaction_type = deposit` |
| `net_sats < 0`      | `transaction_type = withdrawal` |
| `fee` (spending tx) | `fee_amount` in BTC     |
| `status.block_time` | `timestamp` (UTC)       |
| `txid`              | `txid`                  |

### Unconfirmed transactions and RBF

**Unlike the Sparrow CSV importer**, the xpub importer includes unconfirmed
transactions in its output. The Mempool API returns any transaction currently in the
mempool alongside confirmed ones.

**Unconfirmed rows** are assigned a sentinel timestamp of `1970-01-01T00:00:00Z` (Unix
epoch) so that repeated taps produce a stable dedup key — using wall-clock time would
re-insert the same unconfirmed transaction on every run. Unconfirmed rows should be
treated as provisional: they have no confirmed block time, so any FMV or ACB lookup
against them will use the wrong date.

**RBF (Replace-By-Fee):** Bitcoin allows a sender to replace an unconfirmed transaction
with a new one that pays a higher fee (BIP 125). When this happens:

- The **original txid** disappears from the Mempool API once the replacement confirms.
- The **replacement txid** is a different hash and appears as a new confirmed transaction.

The xpub importer does not currently check the `rbf` or `replaced_by` fields that the
Mempool API exposes on unconfirmed transactions. The practical consequence depends on
**when** `sirop tap` is run:

| Tap timing | What ends up in the batch |
|---|---|
| After replacement confirms | Only the replacement txid — correct |
| While original is still unconfirmed (before replacement) | Original txid with epoch timestamp |
| On next tap after original is replaced | Replacement txid added as a new row; original stays with epoch timestamp — **potential duplicate** |

**Recommended practice:** run `sirop tap` after all pending transactions have confirmed
(i.e. at tax-filing time, not in real time). If you do tap with unconfirmed transactions
present, re-tap after they confirm to ensure the final txids are captured. The dedup logic
will not remove the stale unconfirmed row automatically — you would need to re-create the
batch to start clean.

**Comparison with Sparrow CSV:**
Sparrow labels RBF-related rows explicitly (the replaced transaction carries a label like
`(Replaced By Fee)` in the `Label` column). Because Sparrow CSV is a point-in-time
snapshot exported by the user, it reflects whatever Sparrow knew at export time —
including labels applied to replaced transactions. The xpub importer, by contrast,
queries the Mempool API live and has no equivalent labelling mechanism.

### Privacy

All derived Bitcoin addresses are sent to the configured `BTC_MEMPOOL_URL`. Use a private
node (e.g. `BTC_MEMPOOL_URL=http://localhost:8332`) to avoid disclosing your wallet to a
public endpoint. If a public URL is configured, sirop emits warning `[W009]` before scanning.
Set `BTC_TRAVERSAL_ALLOW_PUBLIC=true` to suppress the warning in scripted/CI runs.