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
sirop tap <file> [--source NAME]
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `file` | yes | Path to the exchange or wallet CSV export. |
| `--source NAME` | no | Importer name (e.g. `ndax`, `shakepay`). Auto-detected from CSV headers when omitted. |

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

Further importers (Shakepay, Sparrow, Koinly) are planned.

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
| `asset` | Ticker of the primary asset, e.g. `"BTC"`, `"ETH"`, `"CAD"` |
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
`extra_json`. Importer-specific derived values are added under keys prefixed
with `_`. For example, the NDAX importer embeds the sent side of a
non-fiat-to-non-fiat trade or a dust conversion:

```json
{
  "ASSET": "BTC",
  "ASSET_CLASS": "CRYPTO",
  "AMOUNT": "0.00000010",
  "BALANCE": "0.010345",
  "TYPE": "DUST / IN",
  "TX_ID": "10010",
  "DATE": "2025-04-15T16:45:00.000Z",
  "_ndax_sent_asset": "ETH",
  "_ndax_sent_amount": "4.3E-7"
}
```

---

## Stage state

After a successful `tap`, `stage_status` is updated:

```
tap  →  done
normalize, verify, transfer_match, boil, superficial_loss, pour  →  pending
```

Re-tapping the same batch is not yet supported. If `tap` is already `done`,
the command fails cleanly:

```
error: batch 'my2025tax' already has tap data.
       Re-tap is not yet supported — create a new batch or switch to one.
```

---

## Example session

```
$ sirop create my2025tax
Created batch: my2025tax (2025) → data/my2025tax.sirop

$ sirop tap ~/Downloads/ndax_2025_ledger.csv
Detected format: NDAX
Tapped 47 transaction(s) from ndax_2025_ledger.csv [NDAX] into 'my2025tax'.

$ sirop tap ~/Downloads/shakepay_2025.csv
error: format detected as 'Shakepay' but no importer is implemented for 'shakepay' yet.
```

---

## Error cases

| Situation | Message |
|-----------|---------|
| File not found | `error: file not found: exports/missing.csv` |
| No active batch | `error: no active batch. Run 'sirop create <name>' first.` |
| Tap already done | `error: batch 'X' already has tap data. Re-tap is not yet supported …` |
| Format unknown | `error: cannot identify CSV format — headers don't match any known exchange.` |
| Declared source has missing columns | `error: --source 'ndax' declared but CSV is missing expected columns: …` |
| CSV parse error (bad value) | `error: failed to parse <file>: Cannot parse AMOUNT '???' …` |

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
