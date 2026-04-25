---
verified-at: b5e6b66
tracks:
  - src/sirop/cli/tap.py
  - src/sirop/__main__.py
  - src/sirop/importers
---

# sirop tap — Import Exchange Transactions

## What `tap` does

`tap` reads a CSV export from a supported exchange or wallet, identifies its
format, runs the matching importer, and writes the parsed transactions into the
`raw_transactions` table of the active `.sirop` batch.

This is always the first pipeline stage. Every downstream stage (`normalize`,
`verify`, `boil`, `pour`) depends on data written by `tap`.

---

## Syntax

```
sirop tap <file>   [--source NAME] [--wallet NAME]
sirop tap <folder> [--source NAME] [--wallet NAME]
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `file` or `folder` | yes | Path to a single CSV export, or a directory containing one or more CSV exports. |
| `--source NAME` | no | Importer name (e.g. `ndax`, `shakepay`). Auto-detected from CSV headers when omitted. |
| `--wallet NAME` | no | Wallet label to assign these transactions to. Defaults to the detected source name. Use this to distinguish two accounts at the same exchange (e.g. `shakepay-savings` vs `shakepay-trading`). |

---

## Single-file import

Pass a path to one CSV file. sirop reads the header row, detects the format,
and writes the parsed transactions into the active batch.

```
$ sirop tap ~/Downloads/ndax_2025_ledger.csv
Detected format: NDAX
Tapped 47 transaction(s) from ndax_2025_ledger.csv [NDAX] into 'my2025tax'.
```

Re-tapping the same batch is explicitly supported. Each additional `tap` call
appends new rows and deduplicates against what is already in the batch, then
marks all downstream stages as `invalidated` so they re-run with the full
combined dataset. Duplicate rows (same source + timestamp + asset + amount) are
silently skipped:

```
$ sirop tap ~/Downloads/ndax_2025_ledger.csv   # second run, nothing new
Nothing new to tap from ndax_2025_ledger.csv [NDAX] — all 47 row(s) already exist in 'my2025tax'.
```

---

## Folder import

Pass a directory path instead of a file. sirop scans for `*.csv` files,
detects the format of each one, shows you the full listing, and asks for
confirmation before tapping.

```
$ sirop tap ~/Downloads/exports/
Found 3 CSV file(s) in exports/:
  ndax_2025_ledger.csv  →  NDAX
  shakepay_2025_btc.csv  →  Shakepay
  random_notes.csv  →  unknown (will be skipped)

Tap 2 file(s)? [y/N] y
Tapped 47 transaction(s) from ndax_2025_ledger.csv [NDAX] into 'my2025tax'.
Tapped 23 transaction(s) from shakepay_2025_btc.csv [Shakepay] into 'my2025tax'.
```

**Behaviour details:**

- Files are scanned and listed in alphabetical order.
- Unrecognised files (no fingerprint match, or multiple matches) appear as
  `unknown (will be skipped)` and are excluded from the tap.
- If **all** files are unrecognised, the command exits with an error and nothing
  is tapped.
- Answering `n` (or pressing Enter, or EOF) cancels without tapping anything.
- `--source NAME` applies the declared format to every file in the folder.
  Files whose headers do not satisfy that format's fingerprint are shown as
  unknown and skipped.
- `--wallet NAME` is applied to all tapped files.

---

## Format detection

When `--source` is omitted, sirop reads the CSV header row and matches it
against the `fingerprint_columns` defined in each importer's YAML config
(`config/importers/*.yaml`).

**Auto-detect example:**

```
$ sirop tap exports/ndax_2025.csv
Detected format: NDAX
Tapped 47 transaction(s) from ndax_2025.csv [NDAX] into 'my2025tax'.
```

**Multiple matches** — if headers satisfy more than one fingerprint (unlikely
with the built-in configs but possible with custom importers), sirop asks you
to disambiguate:

```
error: CSV headers match multiple formats: ndax, custom_ndax. Pass --source to pick one.
```

**No match** — if no fingerprint matches, sirop reports the closest partial
match as a hint:

```
error: cannot identify CSV format — headers don't match any known exchange.
  headers found: ['Amount', 'Currency', 'Date', 'Type']
hint: closest match is 'NDAX' (71% of expected columns found).
      Pass --source ndax to override, or check you exported the correct report type.
```

### Explicit `--source`

Pass `--source` when auto-detection fails or when you want to skip the scan:

```
$ sirop tap exports/ledger.csv --source ndax
```

If the file's headers are missing columns required by the declared source,
sirop lists exactly what is absent and suggests an alternative if the headers
fit another known format better:

```
error: --source 'ndax' declared but CSV is missing expected columns:
  missing: 'ASSET_CLASS'
  missing: 'TX_ID'
hint: headers look more like 'shakepay'. Try --source shakepay instead.
```

**Unknown headers** (columns present in the CSV but not in any known
fingerprint) produce a warning, not an error. They are preserved verbatim in
`extra_json` for the next stage:

```
WARNING  CSV has 2 column(s) not seen in any known NDAX format: ['Notes', 'Reference']
         — possible format change. Run with --debug for details.
```

---

## Supported importers

| `--source` name | Exchange / Wallet | Export type | Fee model |
|-----------------|-------------------|-------------|-----------|
| `ndax` | NDAX (AlphaPoint APEX) | Reports → CSV → Ledgers | Explicit `TRADE/FEE` rows |
| `shakepay` | Shakepay | Account → Export Transactions | Embedded in spread (no explicit fee column) |
| `sparrow` | Sparrow Wallet | File → Export Transactions | Explicit fee column (sends only; NULL on receives) |
| `xpub` | Any HD Bitcoin wallet (Sparrow, JoinMarket, etc.) | User-created YAML | Fee from Mempool (sends only) |

Full column specs, unit-detection rules (BTC vs satoshis for Sparrow), and
cross-source transfer matching logic: `docs/ref/transaction-import-formats.md`.

---

## xpub wallet-definition YAML

Instead of a CSV, pass a YAML file listing one or more extended public keys. sirop
derives all child addresses, scans transaction history via your Mempool node, and
writes each key as a named wallet into the active batch.

**Requires a private Mempool node** — address scanning sends derived Bitcoin addresses
to the configured endpoint. Set `BTC_MEMPOOL_URL` in `.env` to a local node.

```
$ sirop tap my_wallets.yaml
$ sirop tap my_wallets.yaml --source xpub   # explicit, if file is not .yaml/.yml
```

### YAML schema

```yaml
source: xpub

wallets:
  - name: my-sparrow-wallet        # wallet name in the sirop batch
    xpub: zpub6rFR7y4Q2Aij...      # account-level zpub / ypub / xpub
    gap_limit: 20                  # optional, default 20
    branches: [0, 1]               # optional, default both (0=receive, 1=change)
    label: ""                      # optional free-text annotation
    script_type: p2wpkh            # optional — overrides prefix-based address encoding
```

### Key prefix → address type

| Prefix | Default address type | HD standard |
|--------|----------------------|-------------|
| `zpub` | P2WPKH (`bc1q…`)    | BIP84       |
| `ypub` | P2SH-P2WPKH (`3…`)  | BIP49       |
| `xpub` | P2PKH (`1…`)         | BIP44       |

### `script_type` override

Some wallets export `xpub` prefix even when the wallet uses a different address
type. Use `script_type` to override the prefix-based encoding:

| Value | Address type |
|-------|-------------|
| `p2wpkh` | native SegWit `bc1q…` |
| `p2sh-p2wpkh` | wrapped SegWit `3…` |
| `p2pkh` | legacy `1…` |

**JoinMarket** is the most common case: it exports `xpub` prefix for all keys
regardless of derivation path, but derives native SegWit addresses (`bc1q…`). Set
`script_type: p2wpkh` for every JoinMarket entry.

### JoinMarket

JoinMarket's wallet display shows two kinds of keys per mixdepth. Use only the
**account-level key** (the one on the `mixdepth N` line):

```
mixdepth        0       xpub6Bff...   ← use this one (account level, m/84'/0'/0')
external addresses  m/84'/0'/0'/0   xpub6Epx...   ← do NOT use (branch level, one step too deep)
```

Using the branch-level key would cause sirop to derive addresses at the wrong
path (`m/84'/0'/0'/0/branch/index` instead of `m/84'/0'/0'/branch/index`).

Each mixdepth covers both branches (external + change). Set `branches: [0, 1]`
and use a larger `gap_limit` (50–100) because coinjoin rounds create wide gaps:

```yaml
source: xpub

wallets:
  - name: jm-depth0
    xpub: xpub6Bff...    # account-level key from "mixdepth 0" line
    script_type: p2wpkh  # JoinMarket BIP84 wallet — override xpub prefix
    gap_limit: 50
    branches: [0, 1]

  - name: jm-depth1
    xpub: xpub6...       # account-level key from "mixdepth 1" line
    script_type: p2wpkh
    gap_limit: 50
    branches: [0, 1]
```

See `config/importers/xpub_example.yaml` for the full annotated template.

---

## What gets written

`tap` writes one row to `raw_transactions` per logical transaction — not per
CSV line. The NDAX importer, for example, groups multiple CSV rows that share
the same timestamp into a single transaction (one buy + one fee row = one
`raw_transactions` row).

### Column mapping

| `raw_transactions` column | Source value |
|---------------------------|-------------|
| `source` | Importer name, e.g. `"ndax"` |
| `raw_timestamp` | Timestamp as-is from CSV, ISO 8601 UTC |
| `transaction_type` | Mapped from CSV type string via `transaction_type_map` in the importer YAML |
| `asset` | Ticker of the primary asset, e.g. `"BTC"`, `"CAD"` |
| `amount` | Absolute amount in `asset` units, fixed-point string |
| `amount_currency` | Same as `asset` |
| `fee` | Fee amount or `NULL` (zero-fee rows are stored as `NULL`, not `"0"`) |
| `fee_currency` | Fee asset ticker or `NULL` |
| `cad_amount` | Fiat value reported by the exchange, or `NULL` for crypto-only rows |
| `fiat_currency` | Currency of `cad_amount` (e.g. `"CAD"`) or `NULL` |
| `cad_rate` | Implicit rate derived from `cad_amount / amount`, or `NULL` |
| `spot_rate` | Spot rate from exchange (Shakepay only — spread fee calculation) |
| `txid` | On-chain transaction ID for withdrawals/deposits, or `NULL` |
| `extra_json` | Full verbatim CSV row as JSON, plus any importer-specific fields |

### Decimal serialization

All numeric values are stored as fixed-point decimal strings using
`format(d, 'f')`. Scientific notation (`"1.2419E+5"`, `"1.0E-7"`) is never
written. This matches the storage convention in
[`docs/ref/database-schema.md`](../ref/database-schema.md).

### `extra_json`

The full original CSV row (all columns as strings) is always preserved in
`extra_json`. For example, a BTC dust conversion row from NDAX:

```json
{
  "ASSET": "BTC",
  "ASSET_CLASS": "CRYPTO",
  "AMOUNT": "0.00000010",
  "BALANCE": "0.010345",
  "TYPE": "DUST / IN",
  "TX_ID": "10010",
  "DATE": "2025-04-15T16:45:00.000Z"
}
```

---

## Stage state

After a successful `tap`, `stage_status` is updated:

```
tap  →  done
normalize, verify, transfer_match, boil, superficial_loss, pour  →  pending
```

Re-tapping an already-`done` batch appends new rows and sets all downstream
stages to `invalidated` so the next `boil` re-runs the full pipeline with the
combined data. Duplicate rows are silently skipped, so re-tapping the same
file twice is safe.

---

## Example sessions

**Tap files one at a time:**

```
$ sirop create my2025tax
Created batch: my2025tax (2025) → data/my2025tax.sirop

$ sirop tap ~/Downloads/ndax_2025_ledger.csv
Detected format: NDAX
Tapped 47 transaction(s) from ndax_2025_ledger.csv [NDAX] into 'my2025tax'.

$ sirop tap ~/Downloads/shakepay_2025.csv
Detected format: Shakepay
Tapped 23 transaction(s) from shakepay_2025.csv [Shakepay] into 'my2025tax'.

$ sirop tap ~/Downloads/sparrow_wallet.csv
Detected format: Sparrow Wallet
Tapped 8 transaction(s) from sparrow_wallet.csv [Sparrow Wallet] into 'my2025tax'.
```

**Tap a whole folder at once:**

```
$ sirop create my2025tax
Created batch: my2025tax (2025) → data/my2025tax.sirop

$ sirop tap ~/Downloads/exports/
Found 3 CSV file(s) in exports/:
  ndax_2025_ledger.csv   →  NDAX
  shakepay_2025.csv      →  Shakepay
  sparrow_wallet.csv     →  Sparrow Wallet

Tap 3 file(s)? [y/N] y
Tapped 47 transaction(s) from ndax_2025_ledger.csv [NDAX] into 'my2025tax'.
Tapped 23 transaction(s) from shakepay_2025.csv [Shakepay] into 'my2025tax'.
Tapped 8 transaction(s) from sparrow_wallet.csv [Sparrow Wallet] into 'my2025tax'.
```

**Two wallets at the same exchange — use `--wallet` to distinguish them:**

```
$ sirop tap ~/Downloads/shakepay_savings.csv --wallet shakepay-savings
Detected format: Shakepay
Tapped 12 transaction(s) from shakepay_savings.csv [Shakepay] into 'my2025tax'.

$ sirop tap ~/Downloads/shakepay_trading.csv --wallet shakepay-trading
Detected format: Shakepay
Tapped 31 transaction(s) from shakepay_trading.csv [Shakepay] into 'my2025tax'.
```

**Two hardware wallets (both Sparrow) — conflict prompt on second tap:**

When `--wallet` is omitted and the auto-derived wallet name already exists, sirop
asks before appending. This prevents silently merging two different physical wallets
into one, which would break transfer matching.

```
$ sirop tap ~/Downloads/ledger-cold.csv
Detected format: Sparrow Wallet
Tapped 18 transaction(s) from ledger-cold.csv [Sparrow Wallet] into 'my2025tax'.

$ sirop tap ~/Downloads/trezor-hot.csv
Detected format: Sparrow Wallet

Wallet "sparrow" already exists. Append to it? [Y/n] (use --wallet NAME to create a new wallet) n
Tap cancelled. Use --wallet NAME to import into a new wallet.

$ sirop tap ~/Downloads/trezor-hot.csv --wallet trezor-hot
Detected format: Sparrow Wallet
Tapped 9 transaction(s) from trezor-hot.csv [Sparrow Wallet] into 'my2025tax'.
```

Answering `Y` or pressing Enter appends to the existing wallet (the default — correct
when tapping multiple date-range exports from the same physical wallet). Answering `n`
aborts with a hint to re-run with `--wallet`. Non-interactive stdin (EOFError) is
treated as `Y` so scripted pipelines are unaffected.

---

## Error cases

| Situation | Message |
|-----------|---------|
| File not found | `error: file not found: exports/missing.csv` |
| No active batch | `error: no active batch. Run 'sirop create <name>' first.` |
| Format unknown | `error: cannot identify CSV format — headers don't match any known exchange.` |
| Declared source has missing columns | `error: --source 'ndax' declared but CSV is missing expected columns: …` |
| CSV parse error (bad value) | `error: failed to parse <file>: Cannot parse AMOUNT '???' …` |
| Folder with no CSV files | *(output)* `No CSV files found in <path>.` |
| Folder where all files are unrecognised | *(output)* `No files could be identified — nothing to tap.` |
| Wallet conflict declined | *(output)* `Tap cancelled. Use --wallet NAME to import into a new wallet.` |

---

## Inspecting imported data

```python
import sqlite3, json

conn = sqlite3.connect("data/my2025tax.sirop")
conn.row_factory = sqlite3.Row

# Count and type breakdown
for r in conn.execute(
    "SELECT transaction_type, COUNT(*) AS n FROM raw_transactions GROUP BY transaction_type"
):
    print(r["transaction_type"], r["n"])

# Inspect a specific row including the original CSV fields
row = conn.execute("SELECT * FROM raw_transactions WHERE id = 1").fetchone()
print(dict(row))
print(json.loads(row["extra_json"]))
```
