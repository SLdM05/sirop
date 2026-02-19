# sirop Database Schema

Each `.sirop` file is a SQLite database representing **one batch** — one
person's one tax year (e.g. `my2025tax.sirop`). Batches are isolated by
file, not by a `batch_id` column. All tables in this document exist in
every `.sirop` file.

---

## Connection settings

Every connection is opened with:

```sql
PRAGMA journal_mode=WAL;    -- better concurrent read performance
PRAGMA foreign_keys=ON;     -- enforce referential integrity
```

`row_factory = sqlite3.Row` is set so rows are accessible by column name.

---

## Storage conventions

| Type | SQLite affinity | Reason |
|---|---|---|
| Money (`Decimal`) | `TEXT` | Avoids float precision loss — stored as exact decimal string e.g. `"1234.56"` |
| Timestamps | `TEXT` | ISO 8601 UTC, e.g. `"2025-03-15T12:00:00+00:00"` |
| Booleans | `INTEGER` | `0` = false, `1` = true |
| Optional fields | nullable column | `NULL` when not applicable |

---

## Schema version

```sql
schema_version (
    version     INTEGER NOT NULL,
    applied_at  TEXT    NOT NULL    -- ISO 8601 UTC
)
```

Single row. The migration guard in `db/connection.py` reads this value on
open and refuses to process a file whose version does not match the current
`SCHEMA_VERSION` constant in `db/schema.py`. Bump `SCHEMA_VERSION` whenever
the schema changes.

Current version: **1**

---

## Metadata tables

### `batch_meta`

```sql
batch_meta (
    name            TEXT    NOT NULL,
    tax_year        INTEGER NOT NULL,
    created_at      TEXT    NOT NULL,   -- ISO 8601 UTC
    sirop_version   TEXT    NOT NULL
)
```

Single row. Written once by `sirop create`. `tax_year` is inferred from the
batch name (e.g. `my2025tax` → `2025`) or supplied via `--year`.

### `stage_status`

```sql
stage_status (
    stage           TEXT    PRIMARY KEY,
    status          TEXT    NOT NULL
                            CHECK(status IN ('pending','running','done','invalidated')),
    completed_at    TEXT,               -- ISO 8601 UTC, NULL until done
    error           TEXT                -- NULL unless status='invalidated' with error
)
```

One row per pipeline stage, seeded to `pending` on batch creation. The
pipeline coordinator reads this table before each stage to decide whether
to run, skip, or abort.

**State transitions:**

```
pending ──► running ──► done
                   └──► invalidated   (re-running an upstream stage cascades downstream)
```

Pipeline stages in execution order:

| Stage | CLI verb |
|---|---|
| `tap` | `sirop tap` |
| `normalize` | `sirop boil` |
| `verify` | `sirop verify` |
| `transfer_match` | `sirop boil` |
| `boil` | `sirop boil` |
| `superficial_loss` | `sirop boil` |
| `pour` | `sirop pour` |

---

## Cached external data

### `boc_rates`

```sql
boc_rates (
    date            TEXT    NOT NULL,   -- YYYY-MM-DD
    currency_pair   TEXT    NOT NULL,   -- e.g. "CADUSD"
    rate            TEXT    NOT NULL,   -- Decimal string
    fetched_at      TEXT    NOT NULL,   -- ISO 8601 UTC
    PRIMARY KEY (date, currency_pair)
)
```

Bank of Canada daily exchange rates. Fetched once per `(date, currency_pair)`
pair and never re-fetched. The normalizer reads from this cache before
calling the BoC Valet API, so repeated runs do not make network calls for
already-seen dates.

---

## Pipeline tables

These are written by pipeline stages in order. Each table references the
previous stage's output via a foreign key.

### `raw_transactions`

Written by: **`tap`**

```sql
raw_transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT    NOT NULL,   -- e.g. "shakepay", "ndax"
    raw_timestamp       TEXT    NOT NULL,   -- as-is from the CSV
    transaction_type    TEXT    NOT NULL,   -- raw type string from the CSV
    asset               TEXT    NOT NULL,   -- e.g. "BTC", "ETH"
    amount              TEXT    NOT NULL,   -- Decimal string
    fee                 TEXT,               -- Decimal string or NULL
    fee_currency        TEXT,               -- e.g. "BTC" or NULL
    cad_amount          TEXT,               -- Decimal string or NULL
    cad_rate            TEXT,               -- Decimal string or NULL
    txid                TEXT,               -- blockchain txid or NULL
    extra_json          TEXT                -- JSON blob for source-specific fields
)
```

One row per line in the source CSV. Timestamps and types are stored
as-is — no normalisation at this stage. `extra_json` captures any
source-specific fields that don't map to the standard columns.

### `transactions`

Written by: **`normalize`**

```sql
transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER REFERENCES raw_transactions(id),
    timestamp           TEXT    NOT NULL,   -- ISO 8601 UTC (normalised)
    transaction_type    TEXT    NOT NULL,   -- normalised TransactionType enum value
    asset               TEXT    NOT NULL,
    amount              TEXT    NOT NULL,   -- Decimal string
    fee                 TEXT,               -- Decimal string or NULL
    fee_currency        TEXT,
    cad_amount          TEXT    NOT NULL,   -- Decimal string (BoC rate applied)
    cad_fee             TEXT,               -- Decimal string or NULL
    cad_rate            TEXT    NOT NULL,   -- Decimal string (BoC rate used)
    txid                TEXT,
    source              TEXT    NOT NULL
)
```

Timestamps are converted to UTC, transaction types are mapped to the
canonical `TransactionType` enum, and all amounts are expressed in CAD
using the Bank of Canada daily rate for the transaction date.

### `verified_transactions`

Written by: **`verify`**

```sql
verified_transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id               INTEGER REFERENCES transactions(id),
    timestamp           TEXT    NOT NULL,   -- ISO 8601 UTC (possibly node-corrected)
    transaction_type    TEXT    NOT NULL,
    asset               TEXT    NOT NULL,
    amount              TEXT    NOT NULL,   -- Decimal string
    fee                 TEXT,               -- Decimal string or NULL (enriched by node)
    fee_currency        TEXT,
    cad_amount          TEXT    NOT NULL,   -- Decimal string
    cad_fee             TEXT,               -- Decimal string or NULL
    cad_rate            TEXT    NOT NULL,   -- Decimal string
    txid                TEXT,
    source              TEXT    NOT NULL,
    node_verified       INTEGER NOT NULL DEFAULT 0  -- 0=false, 1=true
)
```

When the Bitcoin node is available, timestamps and fees may be corrected to
match on-chain data. Every override is recorded in `audit_log`. When the
node is unavailable, rows are promoted from `transactions` as-is with
`node_verified=0`.

### `classified_events`

Written by: **`transfer_match`**

```sql
classified_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vtx_id          INTEGER REFERENCES verified_transactions(id),
    timestamp       TEXT    NOT NULL,   -- ISO 8601 UTC
    event_type      TEXT    NOT NULL,   -- 'buy', 'sell', 'fee_disposal', etc.
    asset           TEXT    NOT NULL,
    amount          TEXT    NOT NULL,   -- Decimal string (units)
    cad_proceeds    TEXT,               -- Decimal string or NULL (disposals only)
    cad_cost        TEXT,               -- Decimal string or NULL (acquisitions only)
    cad_fee         TEXT,               -- Decimal string or NULL
    txid            TEXT,
    is_taxable      INTEGER NOT NULL DEFAULT 1  -- 0=transfer excluded from ACB
)
```

This is the **sole input to the ACB engine**. It is not a copy of
`verified_transactions` — the transfer matcher transforms the data:

- Buys and sells are kept as-is with `is_taxable=1`
- Wallet-to-wallet transfers are excluded (`is_taxable=0`) and never reach
  the ACB engine
- Each on-chain network fee becomes a separate `fee_disposal` row — a
  micro-disposition of BTC at block confirmation time

### `dispositions`

Written by: **`boil`** (ACB engine)

```sql
dispositions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            INTEGER REFERENCES classified_events(id),
    timestamp           TEXT    NOT NULL,   -- ISO 8601 UTC
    asset               TEXT    NOT NULL,
    units               TEXT    NOT NULL,   -- Decimal string
    proceeds            TEXT    NOT NULL,   -- Decimal string (CAD)
    acb_of_disposed     TEXT    NOT NULL,   -- Decimal string (CAD)
    gain_loss           TEXT    NOT NULL,   -- Decimal string (CAD, negative = loss)
    acb_per_unit_after  TEXT    NOT NULL,   -- Decimal string (CAD)
    pool_units_after    TEXT    NOT NULL,   -- Decimal string
    pool_cost_after     TEXT    NOT NULL    -- Decimal string (CAD)
)
```

One row per disposal event. Gain/loss is calculated using the weighted-
average ACB method as required by the CRA. The `*_after` columns snapshot
the pool state immediately after this disposal.

### `acb_state`

Written by: **`boil`** (ACB engine)

```sql
acb_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    disposition_id  INTEGER REFERENCES dispositions(id),
    asset           TEXT    NOT NULL,
    pool_cost       TEXT    NOT NULL,   -- Decimal string (CAD)
    units           TEXT    NOT NULL,   -- Decimal string
    snapshot_date   TEXT    NOT NULL    -- YYYY-MM-DD
)
```

Snapshots of the ACB pool after each disposal. Used by the TUI's ACB state
viewer and for audit purposes. One row per disposal per asset.

### `dispositions_adjusted`

Written by: **`superficial_loss`** (superficial loss detector)

```sql
dispositions_adjusted (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    disposition_id              INTEGER REFERENCES dispositions(id),
    timestamp                   TEXT    NOT NULL,   -- ISO 8601 UTC
    asset                       TEXT    NOT NULL,
    units                       TEXT    NOT NULL,   -- Decimal string
    proceeds                    TEXT    NOT NULL,   -- Decimal string (CAD)
    acb_of_disposed             TEXT    NOT NULL,   -- Decimal string (CAD)
    gain_loss                   TEXT    NOT NULL,   -- Decimal string (CAD)
    superficial_loss_denied     TEXT    NOT NULL DEFAULT '0',  -- Decimal string (CAD)
    adjusted_acb_of_repurchase  TEXT                -- Decimal string or NULL
)
```

Applies the superficial loss rule (61-day window: 30 days before + day of
sale + 30 days after). Denied losses are added back to the ACB of the
repurchased position via `adjusted_acb_of_repurchase`. This table is what
Schedule 3 and TP-21.4.39-V are generated from, not `dispositions`.

---

## Audit table

### `audit_log`

```sql
audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT    NOT NULL,   -- ISO 8601 UTC
    stage       TEXT    NOT NULL,
    txid        TEXT,
    field       TEXT    NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    reason      TEXT    NOT NULL
)
```

Every field-level override is recorded here — node-corrected timestamps,
node-enriched fees, manual corrections. One row per changed field per
transaction. CRA requires records to be retained for **6 years**; this
table is the audit trail that satisfies that requirement. It is never
modified or deleted by the pipeline — only appended to.

---

## Foreign key chain

Data flows through the pipeline tables in a single chain:

```
raw_transactions
    └─► transactions          (raw_id)
            └─► verified_transactions    (tx_id)
                    └─► classified_events        (vtx_id)
                            └─► dispositions             (event_id)
                                    ├─► acb_state               (disposition_id)
                                    └─► dispositions_adjusted    (disposition_id)
```

Re-running a stage deletes that stage's rows and cascades
`stage_status` downstream to `invalidated`, but does not delete downstream
rows immediately — they become stale until the downstream stages re-run.

---

## Active batch marker

The currently active batch is tracked outside SQLite in a plain text file:

```
DATA_DIR/.active    ← contains the batch name, e.g. "my2025tax"
```

This file is written by `sirop create` and `sirop switch`. All commands
that need a batch (and weren't given one explicitly) read this file first.
