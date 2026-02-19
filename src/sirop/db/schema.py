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
SCHEMA_VERSION: Final[int] = 1

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

_RAW_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS raw_transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT    NOT NULL,   -- e.g. "shakepay", "ndax"
    raw_timestamp       TEXT    NOT NULL,   -- as-is from CSV
    transaction_type    TEXT    NOT NULL,   -- raw type string from CSV
    asset               TEXT    NOT NULL,   -- e.g. "BTC", "ETH"
    amount              TEXT    NOT NULL,   -- Decimal string
    fee                 TEXT,               -- Decimal string or NULL
    fee_currency        TEXT,               -- e.g. "BTC" or NULL
    cad_amount          TEXT,               -- Decimal string or NULL
    cad_rate            TEXT,               -- Decimal string or NULL
    txid                TEXT,               -- blockchain txid or NULL
    extra_json          TEXT                -- JSON blob for source-specific fields
)
"""

_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS transactions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_id              INTEGER REFERENCES raw_transactions(id),
    timestamp           TEXT    NOT NULL,   -- ISO 8601 UTC
    transaction_type    TEXT    NOT NULL,   -- normalised TransactionType
    asset               TEXT    NOT NULL,
    amount              TEXT    NOT NULL,   -- Decimal string
    fee                 TEXT,               -- Decimal string or NULL
    fee_currency        TEXT,
    cad_amount          TEXT    NOT NULL,   -- Decimal string
    cad_fee             TEXT,               -- Decimal string or NULL
    cad_rate            TEXT    NOT NULL,   -- Decimal string (BoC rate used)
    txid                TEXT,
    source              TEXT    NOT NULL
)
"""

_VERIFIED_TRANSACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS verified_transactions (
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
"""

_CLASSIFIED_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS classified_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    vtx_id          INTEGER REFERENCES verified_transactions(id),
    timestamp       TEXT    NOT NULL,   -- ISO 8601 UTC
    event_type      TEXT    NOT NULL,   -- 'buy', 'sell', 'fee_disposal', etc.
    asset           TEXT    NOT NULL,
    amount          TEXT    NOT NULL,   -- Decimal string (units)
    cad_proceeds    TEXT,               -- Decimal string or NULL (disposals)
    cad_cost        TEXT,               -- Decimal string or NULL (acquisitions)
    cad_fee         TEXT,               -- Decimal string or NULL
    txid            TEXT,
    is_taxable      INTEGER NOT NULL DEFAULT 1  -- 0=transfer, excluded from ACB
)
"""

_DISPOSITIONS_DDL = """
CREATE TABLE IF NOT EXISTS dispositions (
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
"""

_ACB_STATE_DDL = """
CREATE TABLE IF NOT EXISTS acb_state (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    disposition_id  INTEGER REFERENCES dispositions(id),
    asset           TEXT    NOT NULL,
    pool_cost       TEXT    NOT NULL,   -- Decimal string (CAD)
    units           TEXT    NOT NULL,   -- Decimal string
    snapshot_date   TEXT    NOT NULL    -- YYYY-MM-DD
)
"""

_DISPOSITIONS_ADJUSTED_DDL = """
CREATE TABLE IF NOT EXISTS dispositions_adjusted (
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

_ALL_DDL: Final[tuple[str, ...]] = (
    _BATCH_META_DDL,
    _SCHEMA_VERSION_DDL,
    _STAGE_STATUS_DDL,
    _BOC_RATES_DDL,
    _RAW_TRANSACTIONS_DDL,
    _TRANSACTIONS_DDL,
    _VERIFIED_TRANSACTIONS_DDL,
    _CLASSIFIED_EVENTS_DDL,
    _DISPOSITIONS_DDL,
    _ACB_STATE_DDL,
    _DISPOSITIONS_ADJUSTED_DDL,
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
