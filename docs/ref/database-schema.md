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

| Type | SQLite affinity | Rule |
|---|---|---|
| Money (`Decimal`) | `TEXT` | Serialize with `format(d, 'f')` — **never** `str(d)`. `str()` on computed Decimals produces scientific notation (`"1.2419E+5"`, `"1.0E-7"`) which is unreadable and non-canonical. `format(d, 'f')` always gives fixed-point: `"124190"`, `"0.00000010"`. |
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

Current version: **5**

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

## Support tables

These tables store user configuration and override data that persists across
pipeline runs. They are not pipeline stage outputs — they are read inputs that
influence stage behaviour.

### `custom_importers`

```sql
custom_importers (
    name         TEXT    PRIMARY KEY,     -- importer key, e.g. "my_exchange"
    source_name  TEXT    NOT NULL,        -- display name
    yaml_config  TEXT    NOT NULL,        -- full YAML config stored verbatim
    embedded_at  TEXT    NOT NULL         -- ISO 8601 UTC
)
```

Stores user-supplied importer YAML configs inside the batch file so a `.sirop`
file is self-contained and portable. Written by `sirop tap --embed`. Consulted
by `tap` before scanning the built-in `config/importers/` directory.

### `wallets`

```sql
wallets (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL UNIQUE,     -- display name, e.g. "shakepay" or "cold-storage"
    source       TEXT    NOT NULL DEFAULT '', -- format key, e.g. "shakepay", "ndax", "sparrow"
    auto_created INTEGER NOT NULL DEFAULT 1,  -- 1=auto from tap format, 0=user-named
    created_at   TEXT    NOT NULL,            -- ISO 8601 UTC
    note         TEXT    NOT NULL DEFAULT ''
)
```

Each tap source is associated with a named wallet. Auto-created wallets
(`auto_created=1`) are generated from the format key at tap time. User-named
wallets (`auto_created=0`) can be created with the `--wallet` flag or manually.
The distinction is surfaced in `stir` to help audit cross-source coverage.

`raw_transactions.wallet_id` and `transactions.wallet_id` both reference this
table so the originating wallet can be traced through the pipeline.

### `transfer_overrides`

Written by: **`stir`**; read by: **`transfer_match`**

```sql
transfer_overrides (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id_a            INTEGER NOT NULL REFERENCES transactions(id),
    tx_id_b            INTEGER REFERENCES transactions(id),  -- NULL for external-out / external-in
    action             TEXT    NOT NULL
                               CHECK(action IN ('link','unlink','external-out','external-in')),
    implied_fee_crypto TEXT    NOT NULL DEFAULT '',  -- Decimal string; '' = zero
    external_wallet    TEXT    NOT NULL DEFAULT '',  -- wallet label for external legs
    created_at         TEXT    NOT NULL,             -- ISO 8601 UTC
    note               TEXT    NOT NULL DEFAULT ''
)
```

User-specified overrides that modify transfer matching behaviour. The
`transfer_match` stage loads all overrides from this table before running its
automatic matching algorithm.

| `action` value | Effect |
|----------------|--------|
| `link` | Force `tx_id_a` and `tx_id_b` to be treated as a matched transfer pair, regardless of what the auto-matcher would decide. If `implied_fee_crypto` is non-zero, the difference between the sent and received amounts is emitted as a `fee_disposal` event. |
| `unlink` | Prevent the auto-matcher from pairing `tx_id_a` with `tx_id_b`. Both legs are treated as separate taxable events. |
| `external-out` | Mark `tx_id_a` (a withdrawal) as going to an untracked external wallet. Non-taxable. `tx_id_b` is NULL. |
| `external-in` | Mark `tx_id_a` (a deposit) as arriving from an untracked external wallet. Establishes ACB at FMV. `tx_id_b` is NULL. |

Overrides survive `sirop boil --from transfer_match` — they are never deleted
by re-running stages. To remove an override, use `sirop stir --clear <id>`.

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
    amount_currency     TEXT    NOT NULL,   -- currency of amount (e.g. "BTC", "CAD", "USD")
    fee                 TEXT,               -- Decimal string or NULL
    fee_currency        TEXT,               -- e.g. "BTC" or NULL
    cad_amount          TEXT,               -- Decimal string or NULL (may be USD pre-normalise)
    fiat_currency       TEXT,               -- currency of cad_amount before normalisation
    cad_rate            TEXT,               -- Decimal string or NULL
    spot_rate           TEXT,               -- Decimal string or NULL (Shakepay spread calc)
    txid                TEXT,               -- blockchain txid or NULL
    extra_json          TEXT,               -- JSON blob for source-specific fields
    wallet_id           INTEGER REFERENCES wallets(id)  -- source wallet (auto-created from tap)
)
```

One row per line in the source CSV. Timestamps and types are stored
as-is — no normalisation at this stage.

`amount_currency` tells the normaliser what unit `amount` is in (BTC, CAD,
USD, etc.). `fiat_currency` records what currency `cad_amount` is actually
denominated in before conversion — needed when a source reports USD values.
`spot_rate` is used alongside `cad_rate` to calculate the implicit spread
fee on Shakepay, where no explicit fee column exists.

`extra_json` captures any source-specific fields that don't map to the
standard columns.

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
    fee_crypto          TEXT,               -- Decimal string or NULL (fee in asset units)
    fee_currency        TEXT,               -- e.g. "BTC" or NULL
    cad_amount          TEXT    NOT NULL,   -- Decimal string (BoC rate applied)
    cad_fee             TEXT,               -- Decimal string or NULL (fee converted to CAD)
    cad_rate            TEXT    NOT NULL,   -- Decimal string (BoC rate used)
    txid                TEXT,
    source              TEXT    NOT NULL,
    is_transfer         INTEGER NOT NULL DEFAULT 0,  -- 1 once transfer_match confirms pair
    counterpart_id      INTEGER REFERENCES transactions(id),  -- matched withdrawal/deposit leg
    notes               TEXT    NOT NULL DEFAULT '',
    wallet_id           INTEGER REFERENCES wallets(id)        -- propagated from raw_transactions
)
```

Timestamps are converted to UTC, transaction types are mapped to the
canonical `TransactionType` enum, and all amounts are expressed in CAD
using the Bank of Canada daily rate for the transaction date.

`fee_crypto` stores the fee in its native asset (e.g. satoshis of BTC) so
the ACB engine can calculate the micro-disposition correctly. `cad_fee` is
the CAD value of that fee for Schedule 3 reporting.

`is_transfer` and `counterpart_id` are set by the `transfer_match` stage
(not `normalize`), but stored here so the verified stage can carry them
forward. `counterpart_id` is a self-referential FK linking the two legs of
a wallet-to-wallet transfer.

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
    fee_crypto          TEXT,               -- Decimal string or NULL (enriched by node)
    fee_currency        TEXT,
    cad_amount          TEXT    NOT NULL,   -- Decimal string
    cad_fee             TEXT,               -- Decimal string or NULL
    cad_rate            TEXT    NOT NULL,   -- Decimal string
    txid                TEXT,
    source              TEXT    NOT NULL,
    is_transfer         INTEGER NOT NULL DEFAULT 0,
    counterpart_id      INTEGER REFERENCES verified_transactions(id),
    node_verified       INTEGER NOT NULL DEFAULT 0,  -- 0=false, 1=true
    block_height        INTEGER,            -- NULL unless node_verified=1
    confirmations       INTEGER             -- NULL unless node_verified=1
)
```

When the Bitcoin node is available, timestamps and fees may be corrected to
match on-chain data. Every override is recorded in `audit_log`. When the
node is unavailable, rows are promoted from `transactions` as-is with
`node_verified=0`.

`block_height` is the canonical block number reported by the node. It
provides provenance for any timestamp correction — the `audit_log` records
the change but this column records the authoritative source. `confirmations`
gives the confidence level at the time of verification.

### `classified_events`

Written by: **`transfer_match`**

```sql
classified_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vtx_id          INTEGER REFERENCES verified_transactions(id),
    timestamp       TEXT    NOT NULL,   -- ISO 8601 UTC
    event_type      TEXT    NOT NULL,   -- 'buy', 'sell', 'fee_disposal', 'staking', etc.
    asset           TEXT    NOT NULL,
    amount          TEXT    NOT NULL,   -- Decimal string (units)
    cad_proceeds    TEXT,               -- Decimal string or NULL (disposals only)
    cad_cost        TEXT,               -- Decimal string or NULL (acquisitions only)
    cad_fee         TEXT,               -- Decimal string or NULL
    txid            TEXT,
    source          TEXT    NOT NULL,   -- origin exchange/wallet for traceability
    is_taxable      INTEGER NOT NULL DEFAULT 1  -- 0=transfer excluded from ACB engine
)
```

This is the **sole input to the ACB engine**. It is not a copy of
`verified_transactions` — the transfer matcher transforms the data:

- Buys and sells are kept as-is with `is_taxable=1`
- Wallet-to-wallet transfers are excluded (`is_taxable=0`) and never reach
  the ACB engine
- Each on-chain network fee becomes a separate `fee_disposal` row — a
  micro-disposition of BTC at block confirmation time
- Income events (staking, airdrops) are included as acquisitions so the
  ACB engine records the cost basis established at FMV

`source` is carried forward for audit queries (e.g. "show all Shakepay
disposals") without requiring a join back through the full chain.

`acb_state` also references `classified_events(id)` so pool snapshots can
be recorded after both acquisitions (buys, income) and disposals.

### `income_events`

Written by: **`transfer_match`**

```sql
income_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vtx_id          INTEGER REFERENCES verified_transactions(id),
    timestamp       TEXT    NOT NULL,   -- ISO 8601 UTC
    asset           TEXT    NOT NULL,   -- e.g. "BTC"
    units           TEXT    NOT NULL,   -- Decimal string (amount received)
    income_type     TEXT    NOT NULL,   -- 'staking', 'airdrop', 'mining', 'other'
    fmv_cad         TEXT    NOT NULL,   -- Decimal string — taxable income AND ACB established
    source          TEXT    NOT NULL    -- origin platform/wallet
)
```

Tracks income events separately from capital dispositions. An income event
(staking reward, airdrop, mining) is taxable as **income** at FMV when
received, and simultaneously establishes an ACB equal to that FMV for the
received units.

This table is the source for:

- **TP-21.4.39-V Part 6** — property income from cryptoassets
- **Schedule G** — income rows (not capital gains rows)

The same event also appears in `classified_events` as an acquisition
(with `cad_cost = fmv_cad`) so the ACB engine picks up the cost basis.

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
    selling_fees        TEXT    NOT NULL,   -- Decimal string (CAD) — Schedule 3 Column 4
    gain_loss           TEXT    NOT NULL,   -- Decimal string (CAD, negative = loss)
    disposition_type    TEXT    NOT NULL,   -- 'sell' | 'trade' | 'spend' | 'fee_disposal'
    year_acquired       TEXT    NOT NULL,   -- e.g. "2024" or "Various" — Schedule 3 Column 1
    acb_per_unit_before TEXT    NOT NULL,   -- Decimal string (CAD) — pool state before disposal
    pool_units_before   TEXT    NOT NULL,   -- Decimal string — pool state before disposal
    pool_cost_before    TEXT    NOT NULL,   -- Decimal string (CAD) — pool state before disposal
    acb_per_unit_after  TEXT    NOT NULL,   -- Decimal string (CAD) — pool state after disposal
    pool_units_after    TEXT    NOT NULL,   -- Decimal string — pool state after disposal
    pool_cost_after     TEXT    NOT NULL    -- Decimal string (CAD) — pool state after disposal
)
```

One row per disposal event. Gain/loss is calculated using the weighted-
average ACB method as required by the CRA:

```
gain_loss = proceeds - acb_of_disposed - selling_fees
```

Both the before-state and after-state of the ACB pool are stored for a
complete audit trail. `selling_fees` populates Schedule 3 Column 4
("Outlays and expenses"). `year_acquired` populates Column 1 and may be
"Various" when the pool spans multiple acquisition years.

`disposition_type` distinguishes report categories: 'sell' and 'trade' go
on Schedule 3 Line 10; 'fee_disposal' are micro-dispositions from network
fees.

### `acb_state`

Written by: **`boil`** (ACB engine)

```sql
acb_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER REFERENCES classified_events(id),
    asset           TEXT    NOT NULL,
    pool_cost       TEXT    NOT NULL,   -- Decimal string (CAD)
    units           TEXT    NOT NULL,   -- Decimal string
    snapshot_date   TEXT    NOT NULL    -- YYYY-MM-DD
)
```

Snapshots of the ACB pool after **every** classified event — acquisitions
(buys, income) and disposals alike. The FK references `classified_events`
rather than `dispositions` so that buy-triggered pool changes are also
recorded (a buy updates the pool without creating a disposition row).

Used by the TUI's ACB state viewer and for audit queries such as "what was
my BTC ACB on date X?".

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
    selling_fees                TEXT    NOT NULL,   -- Decimal string (CAD) — Schedule 3 col 4
    gain_loss                   TEXT    NOT NULL,   -- Decimal string (CAD, pre-adjustment)
    is_superficial_loss         INTEGER NOT NULL DEFAULT 0,  -- 1 if rule triggered
    superficial_loss_denied     TEXT    NOT NULL DEFAULT '0',  -- Decimal string (CAD)
    allowable_loss              TEXT    NOT NULL DEFAULT '0',  -- Decimal string (CAD)
    adjusted_gain_loss          TEXT    NOT NULL,   -- Decimal string (CAD) — Schedule 3 col 5
    adjusted_acb_of_repurchase  TEXT,               -- Decimal string or NULL
    disposition_type            TEXT    NOT NULL,   -- carried from dispositions
    year_acquired               TEXT    NOT NULL    -- carried from dispositions
)
```

Applies the superficial loss rule (61-day window: 30 days before + day of
sale + 30 days after). Denied losses are added back to the ACB of the
repurchased position via `adjusted_acb_of_repurchase`.

**This table is the authoritative source for all report generation.**
Schedule 3, Schedule G, and TP-21.4.39-V Part 4 are generated from
`dispositions_adjusted`, not from `dispositions`.

Column mapping to Schedule 3:

| Schedule 3 column | `dispositions_adjusted` column |
|---|---|
| Col 1 — Year of acquisition | `year_acquired` |
| Col 2 — Proceeds of disposition | `proceeds` |
| Col 3 — Adjusted cost base | `acb_of_disposed` |
| Col 4 — Outlays and expenses | `selling_fees` |
| Col 5 — Gain (or loss) | `adjusted_gain_loss` |

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
wallets ◄───────────────────────────────────────────────────────┐
raw_transactions (wallet_id → wallets)                          │
    └─► transactions (wallet_id → wallets)     (raw_id)         │
            │                                                    │
            ├─► transfer_overrides              (tx_id_a/tx_id_b)
            │       (read by transfer_match; written by stir)
            │
            └─► verified_transactions           (tx_id)
                    ├─► income_events               (vtx_id)
                    └─► classified_events           (vtx_id)
                            ├─► acb_state               (event_id)
                            └─► dispositions             (event_id)
                                    └─► dispositions_adjusted  (disposition_id)
```

Re-running a stage deletes that stage's rows and cascades
`stage_status` downstream to `invalidated`, but does not delete downstream
rows immediately — they become stale until the downstream stages re-run.

`transfer_overrides` is the exception: it is never deleted by any pipeline
re-run. Overrides are user intent — they survive `--from transfer_match` and
must be explicitly removed with `sirop stir --clear`.

---

## Active batch marker

The currently active batch is tracked outside SQLite in a plain text file:

```
DATA_DIR/.active    ← contains the batch name, e.g. "my2025tax"
```

This file is written by `sirop create` and `sirop switch`. All commands
that need a batch (and weren't given one explicitly) read this file first.

---

## Schema change log

| Version | Changes |
|---|---|
| 1 | Initial schema |
| 2 | `raw_transactions`: added `amount_currency`, `fiat_currency`, `spot_rate`. `transactions`/`verified_transactions`: renamed `fee` → `fee_crypto`, added `is_transfer`, `counterpart_id`, `notes`. `verified_transactions`: added `block_height`, `confirmations`. `classified_events`: added `source`. `dispositions`: added `selling_fees`, `disposition_type`, `year_acquired`, full before/after ACB pool state. `acb_state`: FK target changed from `dispositions` to `classified_events`, column renamed `disposition_id` → `event_id`. `dispositions_adjusted`: added `selling_fees`, `is_superficial_loss`, `allowable_loss`, `adjusted_gain_loss`, `disposition_type`, `year_acquired`. New table: `income_events`. |
| 3 | New table: `custom_importers` — stores user-supplied importer YAML configs inside the batch file so a `.sirop` file is self-contained and portable. |
| 4 | New table: `transfer_overrides` (initial version — `link` and `unlink` actions only, `tx_id_b` NOT NULL). Written by `sirop stir`; consumed by the `transfer_match` stage. |
| 5 | New table: `wallets`. `wallet_id` FK added to `raw_transactions` and `transactions` — traces each transaction to its originating wallet. `transfer_overrides` recreated with nullable `tx_id_b` (supports external-leg overrides), new columns `implied_fee_crypto` and `external_wallet`, and `external-out` / `external-in` added to `action` CHECK constraint. |
