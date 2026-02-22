"""
SQLite DDL for sirop batch files (.sirop).

Each .sirop file is one person's one tax year. There are no batch_id
columns — isolation is achieved by having a separate file per batch.

Call create_tables(conn) on a fresh connection to initialise the full
schema. The caller is responsible for inserting seed rows into
batch_meta, schema_version, and stage_status.
"""

import sqlite3
from typing import Final

# Bump this when the schema changes. The migration guard reads this value
# and refuses to open a file whose schema_version doesn't match.
SCHEMA_VERSION: Final[int] = 4

# All pipeline stage names in execution order.
PIPELINE_STAGES: Final[tuple[str, ...]] = (
    "tap",
    "normalize",
    "verify",
    "transfer_match",
    "boil",
    "superficial_loss",
    "pour",
)

# ── DDL strings ───────────────────────────────────────────────────────────────
# All monetary values are stored as TEXT (Decimal string) to avoid float
# precision loss. Timestamps are ISO 8601 TEXT in UTC. Booleans are INTEGER.

_BATCH_META_DDL = """
CREATE TABLE IF NOT EXISTS batch_meta (
    name            TEXT    NOT NULL,
    tax_year        INTEGER NOT NULL,
    created_at      TEXT    NOT NULL,   -- ISO 8601 UTC
    sirop_version   TEXT    NOT NULL
)
"""

_SCHEMA_VERSION_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER NOT NULL,
    applied_at  TEXT    NOT NULL        -- ISO 8601 UTC
)
"""

_STAGE_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS stage_status (
    stage           TEXT    PRIMARY KEY,
    status          TEXT    NOT NULL
                            CHECK(status IN ('pending', 'running', 'done', 'invalidated')),
    completed_at    TEXT,               -- ISO 8601 UTC, NULL until done
    error           TEXT                -- NULL unless an error was recorded
)
"""

_BOC_RATES_DDL = """
CREATE TABLE IF NOT EXISTS boc_rates (
    date            TEXT    NOT NULL,   -- YYYY-MM-DD
    currency_pair   TEXT    NOT NULL,   -- e.g. "CADUSD"
    rate            TEXT    NOT NULL,   -- Decimal string
    fetched_at      TEXT    NOT NULL,   -- ISO 8601 UTC
    PRIMARY KEY (date, currency_pair)
)
"""

_CUSTOM_IMPORTERS_DDL = """
CREATE TABLE IF NOT EXISTS custom_importers (
    name         TEXT    PRIMARY KEY,
    source_name  TEXT    NOT NULL,
    yaml_config  TEXT    NOT NULL,      -- full YAML stored verbatim
    embedded_at  TEXT    NOT NULL       -- ISO 8601 UTC
)
"""

# ── Gap 1 fix ─────────────────────────────────────────────────────────────────
# Added: amount_currency (what currency `amount` is in — BTC, CAD, USD, etc.)
#        fiat_currency   (what currency `cad_amount` is actually in at raw stage)
#        spot_rate       (Shakepay: spot rate alongside buy/sell rate for spread fee)
_RAW_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS raw_transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT    NOT NULL,   -- e.g. "shakepay", "ndax"
    raw_timestamp       TEXT    NOT NULL,   -- as-is from CSV
    transaction_type    TEXT    NOT NULL,   -- raw type string from CSV
    asset               TEXT    NOT NULL,   -- e.g. "BTC", "ETH"
    amount              TEXT    NOT NULL,   -- Decimal string
    amount_currency     TEXT    NOT NULL,   -- currency of amount (e.g. "BTC", "CAD")
    fee                 TEXT,               -- Decimal string or NULL
    fee_currency        TEXT,               -- e.g. "BTC" or NULL
    cad_amount          TEXT,               -- Decimal string or NULL
    fiat_currency       TEXT,               -- currency of cad_amount before normalisation
    cad_rate            TEXT,               -- Decimal string or NULL
    spot_rate           TEXT,               -- Decimal string or NULL (Shakepay spread calc)
    txid                TEXT,               -- blockchain txid or NULL
    extra_json          TEXT                -- JSON blob for source-specific fields
)
"""

# ── Gap 2 fix ─────────────────────────────────────────────────────────────────
# Renamed: fee → fee_crypto (clearer: fee in crypto units, not CAD)
# Added:   is_transfer     (TRUE when transfer-matcher confirms wallet-to-wallet)
#          counterpart_id  (FK to the matching deposit/withdrawal row)
#          notes           (free-text override or annotation)
_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER REFERENCES raw_transactions(id),
    timestamp           TEXT    NOT NULL,   -- ISO 8601 UTC
    transaction_type    TEXT    NOT NULL,   -- normalised TransactionType
    asset               TEXT    NOT NULL,
    amount              TEXT    NOT NULL,   -- Decimal string
    fee_crypto          TEXT,               -- Decimal string or NULL (fee in asset units)
    fee_currency        TEXT,               -- e.g. "BTC" or NULL
    cad_amount          TEXT    NOT NULL,   -- Decimal string
    cad_fee             TEXT,               -- Decimal string or NULL (fee in CAD)
    cad_rate            TEXT    NOT NULL,   -- Decimal string (BoC rate used)
    txid                TEXT,
    source              TEXT    NOT NULL,
    is_transfer         INTEGER NOT NULL DEFAULT 0,  -- 1 after transfer_match confirms pair
    counterpart_id      INTEGER REFERENCES transactions(id),  -- matched withdrawal/deposit
    notes               TEXT    NOT NULL DEFAULT ''
)
"""

# ── Gap 7 fix ─────────────────────────────────────────────────────────────────
# Renamed: fee → fee_crypto (consistency with transactions)
# Added:   block_height   (canonical block number from node — used for timestamp provenance)
#          confirmations  (confirmation count at verification time)
#          is_transfer    (carried forward from transactions)
#          counterpart_id (carried forward from transactions)
_VERIFIED_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS verified_transactions (
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
"""

# Bonus fix: added source column for traceability back to origin exchange/wallet.
_CLASSIFIED_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS classified_events (
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
    is_taxable      INTEGER NOT NULL DEFAULT 1  -- 0=transfer, excluded from ACB engine
)
"""

# ── Gap 3 + 4 fix ─────────────────────────────────────────────────────────────
# Added: selling_fees      (Schedule 3 Column 4 — outlays and expenses)
#        disposition_type  (sell/trade/spend/fee_disposal — for report categorisation)
#        year_acquired     (Schedule 3 Column 1 — int or "Various")
#        acb_per_unit_before / pool_units_before / pool_cost_before
#            (full before-state snapshot for audit trail; after-state was already present)
_DISPOSITIONS_DDL = """
CREATE TABLE IF NOT EXISTS dispositions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id            INTEGER REFERENCES classified_events(id),
    timestamp           TEXT    NOT NULL,   -- ISO 8601 UTC
    asset               TEXT    NOT NULL,
    units               TEXT    NOT NULL,   -- Decimal string
    proceeds            TEXT    NOT NULL,   -- Decimal string (CAD)
    acb_of_disposed     TEXT    NOT NULL,   -- Decimal string (CAD)
    selling_fees        TEXT    NOT NULL,   -- Decimal string (CAD) — Schedule 3 col 4
    gain_loss           TEXT    NOT NULL,   -- Decimal string (CAD, negative = loss)
    disposition_type    TEXT    NOT NULL,   -- 'sell','trade','spend','fee_disposal'
    year_acquired       TEXT    NOT NULL,   -- e.g. "2024" or "Various"
    acb_per_unit_before TEXT    NOT NULL,   -- Decimal string (CAD) — before this disposal
    pool_units_before   TEXT    NOT NULL,   -- Decimal string — before this disposal
    pool_cost_before    TEXT    NOT NULL,   -- Decimal string (CAD) — before this disposal
    acb_per_unit_after  TEXT    NOT NULL,   -- Decimal string (CAD) — after this disposal
    pool_units_after    TEXT    NOT NULL,   -- Decimal string — after this disposal
    pool_cost_after     TEXT    NOT NULL    -- Decimal string (CAD) — after this disposal
)
"""

# ── Gap 6 fix ─────────────────────────────────────────────────────────────────
# Changed FK target from dispositions(id) to classified_events(id) so that
# pool snapshots can be recorded after acquisitions (buys) as well as disposals.
# Renamed disposition_id → event_id to match the new reference.
_ACB_STATE_DDL = """
CREATE TABLE IF NOT EXISTS acb_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER REFERENCES classified_events(id),
    asset           TEXT    NOT NULL,
    pool_cost       TEXT    NOT NULL,   -- Decimal string (CAD)
    units           TEXT    NOT NULL,   -- Decimal string
    snapshot_date   TEXT    NOT NULL    -- YYYY-MM-DD
)
"""

# ── Gap 5 fix ─────────────────────────────────────────────────────────────────
# Added: is_superficial_loss  (explicit boolean flag — don't derive from denied != 0)
#        allowable_loss        (the portion of loss that IS allowed after denial)
#        adjusted_gain_loss    (final reportable gain/loss — what goes on Schedule 3)
# Carried forward from dispositions (report stage reads this table, not dispositions):
#        selling_fees, disposition_type, year_acquired
_DISPOSITIONS_ADJUSTED_DDL = """
CREATE TABLE IF NOT EXISTS dispositions_adjusted (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    disposition_id              INTEGER REFERENCES dispositions(id),
    timestamp                   TEXT    NOT NULL,   -- ISO 8601 UTC
    asset                       TEXT    NOT NULL,
    units                       TEXT    NOT NULL,   -- Decimal string
    proceeds                    TEXT    NOT NULL,   -- Decimal string (CAD)
    acb_of_disposed             TEXT    NOT NULL,   -- Decimal string (CAD)
    selling_fees                TEXT    NOT NULL,   -- Decimal string (CAD)
    gain_loss                   TEXT    NOT NULL,   -- Decimal string (CAD, pre-adjustment)
    is_superficial_loss         INTEGER NOT NULL DEFAULT 0,  -- 1 if rule applies
    superficial_loss_denied     TEXT    NOT NULL DEFAULT '0',  -- Decimal string (CAD)
    allowable_loss              TEXT    NOT NULL DEFAULT '0',  -- Decimal string (CAD)
    adjusted_gain_loss          TEXT    NOT NULL,   -- Decimal string (CAD) — Schedule 3 col 5
    adjusted_acb_of_repurchase  TEXT,               -- Decimal string or NULL
    disposition_type            TEXT    NOT NULL,   -- carried from dispositions
    year_acquired               TEXT    NOT NULL    -- carried from dispositions
)
"""

_TRANSFER_OVERRIDES_DDL = """
CREATE TABLE IF NOT EXISTS transfer_overrides (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id_a     INTEGER NOT NULL REFERENCES transactions(id),
    tx_id_b     INTEGER NOT NULL REFERENCES transactions(id),
    action      TEXT    NOT NULL CHECK(action IN ('link', 'unlink')),
    created_at  TEXT    NOT NULL,   -- ISO 8601 UTC
    note        TEXT    NOT NULL DEFAULT ''
)
"""

_AUDIT_LOG_DDL = """
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT    NOT NULL,   -- ISO 8601 UTC
    stage       TEXT    NOT NULL,
    txid        TEXT,
    field       TEXT    NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    reason      TEXT    NOT NULL
)
"""

# ── Gap 8 fix ─────────────────────────────────────────────────────────────────
# New table: income events (staking rewards, airdrops, mining).
# These establish ACB (at FMV when received) and are reported separately on
# TP-21.4.39-V Part 6 and Schedule G. They also appear in classified_events
# as acquisitions so the ACB engine picks them up.
_INCOME_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS income_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vtx_id          INTEGER REFERENCES verified_transactions(id),
    timestamp       TEXT    NOT NULL,   -- ISO 8601 UTC
    asset           TEXT    NOT NULL,   -- e.g. "BTC"
    units           TEXT    NOT NULL,   -- Decimal string (amount received)
    income_type     TEXT    NOT NULL,   -- 'staking', 'airdrop', 'mining', 'other'
    fmv_cad         TEXT    NOT NULL,   -- Decimal string — taxable income AND ACB established
    source          TEXT    NOT NULL    -- origin platform/wallet
)
"""

_ALL_DDL: Final[tuple[str, ...]] = (
    _BATCH_META_DDL,
    _SCHEMA_VERSION_DDL,
    _STAGE_STATUS_DDL,
    _BOC_RATES_DDL,
    _CUSTOM_IMPORTERS_DDL,
    _RAW_TRANSACTIONS_DDL,
    _TRANSACTIONS_DDL,
    _VERIFIED_TRANSACTIONS_DDL,
    _CLASSIFIED_EVENTS_DDL,
    _DISPOSITIONS_DDL,
    _ACB_STATE_DDL,
    _DISPOSITIONS_ADJUSTED_DDL,
    _INCOME_EVENTS_DDL,
    _TRANSFER_OVERRIDES_DDL,
    _AUDIT_LOG_DDL,
)


def create_tables(conn: sqlite3.Connection) -> None:
    """Create all sirop tables in the given connection.

    Safe to call on an existing database — all statements use
    CREATE TABLE IF NOT EXISTS. Does not insert any seed rows.
    Executes as a single transaction so the schema is either fully
    created or not at all.
    """
    with conn:
        for ddl in _ALL_DDL:
            conn.execute(ddl)
