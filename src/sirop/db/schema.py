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
SCHEMA_VERSION: Final[int] = 9

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

_CRYPTO_PRICES_DDL = """
CREATE TABLE IF NOT EXISTS crypto_prices (
    date        TEXT NOT NULL,   -- YYYY-MM-DD
    asset       TEXT NOT NULL,   -- e.g. "BTC"
    price_usd   TEXT NOT NULL,   -- Decimal string
    price_cad   TEXT NOT NULL,   -- Decimal string (price_usd * BoC USDCAD rate)
    source      TEXT NOT NULL,   -- "mempool" | "coingecko"
    fetched_at  TEXT NOT NULL,   -- ISO 8601 UTC
    PRIMARY KEY (date, asset)
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

# ── v5: wallets ───────────────────────────────────────────────────────────────
# Each tap source is associated with a named wallet.  Wallets auto-created from
# a tap carry auto_created=1; user-named wallets (--wallet flag or manual) carry
# auto_created=0.  The distinction is shown in `stir` to help users audit coverage.
_WALLETS_DDL = """
CREATE TABLE IF NOT EXISTS wallets (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL UNIQUE,        -- display name, e.g. "shakepay" or "cold-storage"
    source       TEXT    NOT NULL DEFAULT '',    -- format key, e.g. "shakepay", "ndax", "sparrow"
    auto_created INTEGER NOT NULL DEFAULT 1,     -- 1=auto from tap format, 0=user-named
    created_at   TEXT    NOT NULL,               -- ISO 8601 UTC
    note         TEXT    NOT NULL DEFAULT ''
)
"""

# ── Gap 1 fix ─────────────────────────────────────────────────────────────────
# Added: amount_currency (what currency `amount` is in — BTC, CAD, USD, etc.)
#        fiat_currency   (what currency `cad_amount` is actually in at raw stage)
#        spot_rate       (Shakepay: spot rate alongside buy/sell rate for spread fee)
# v5: wallet_id — FK to wallets, assigned at tap time
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
    extra_json          TEXT,               -- JSON blob for source-specific fields
    wallet_id           INTEGER REFERENCES wallets(id),  -- v5: source wallet
    notes               TEXT    NOT NULL DEFAULT ''  -- v9: label/annotation (e.g. Sparrow "Label")
)
"""

# ── Gap 2 fix ─────────────────────────────────────────────────────────────────
# Renamed: fee → fee_crypto (clearer: fee in crypto units, not CAD)
# Added:   is_transfer     (TRUE when transfer-matcher confirms wallet-to-wallet)
#          counterpart_id  (FK to the matching deposit/withdrawal row)
#          notes           (free-text override or annotation)
# v5:      wallet_id       (FK to wallets, propagated from raw_transactions)
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
    notes               TEXT    NOT NULL DEFAULT '',
    wallet_id           INTEGER REFERENCES wallets(id)  -- v5: source wallet
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
    confirmations       INTEGER,            -- NULL unless node_verified=1
    wallet_id           INTEGER REFERENCES wallets(id)  -- v7: source wallet
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
    is_taxable      INTEGER NOT NULL DEFAULT 1,  -- 0=transfer, excluded from ACB engine
    wallet_id       INTEGER REFERENCES wallets(id)  -- v7: source wallet FK
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
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    tx_id_a            INTEGER NOT NULL REFERENCES transactions(id),
    tx_id_b            INTEGER REFERENCES transactions(id),  -- NULL for external-out / external-in
    action             TEXT    NOT NULL
                               CHECK(action IN ('link','unlink','external-out','external-in')),
    implied_fee_crypto TEXT    NOT NULL DEFAULT '',  -- Decimal string; '' = zero
    external_wallet    TEXT    NOT NULL DEFAULT '',  -- wallet name for external legs
    created_at         TEXT    NOT NULL,             -- ISO 8601 UTC
    note               TEXT    NOT NULL DEFAULT ''
)
"""

_GRAPH_TRANSFER_PAIRS_DDL = """
CREATE TABLE IF NOT EXISTS graph_transfer_pairs (
    withdrawal_id       INTEGER NOT NULL REFERENCES transactions(id),
    deposit_id          INTEGER NOT NULL REFERENCES transactions(id),
    hops                INTEGER NOT NULL,
    direction           TEXT    NOT NULL,   -- 'backward' | 'forward'
    fee_crypto          TEXT    NOT NULL,   -- Decimal string
    deposit_vout_count  INTEGER NOT NULL DEFAULT 0,   -- v9: outputs on deposit tx
    deposit_vin_count   INTEGER NOT NULL DEFAULT 0,   -- v9: inputs on deposit tx
    PRIMARY KEY (withdrawal_id, deposit_id)
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
    _CRYPTO_PRICES_DDL,
    _CUSTOM_IMPORTERS_DDL,
    _WALLETS_DDL,
    _RAW_TRANSACTIONS_DDL,
    _TRANSACTIONS_DDL,
    _VERIFIED_TRANSACTIONS_DDL,
    _CLASSIFIED_EVENTS_DDL,
    _DISPOSITIONS_DDL,
    _ACB_STATE_DDL,
    _DISPOSITIONS_ADJUSTED_DDL,
    _INCOME_EVENTS_DDL,
    _TRANSFER_OVERRIDES_DDL,
    _GRAPH_TRANSFER_PAIRS_DDL,
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


def migrate_to_v5(conn: sqlite3.Connection) -> None:
    """Apply v5 schema migrations to an existing batch file.

    Idempotent — safe to call on fresh databases and already-migrated ones.
    Handles three changes that SQLite cannot express via CREATE TABLE IF NOT
    EXISTS alone:

    1. ``wallets.id`` column added to ``raw_transactions``.
    2. ``wallet_id`` column added to ``transactions``.
    3. ``transfer_overrides`` recreated with nullable ``tx_id_b``,
       ``implied_fee_crypto``, ``external_wallet``, and the expanded
       ``action`` CHECK constraint.
    """
    with conn:
        # -- raw_transactions: add wallet_id --------------------------------
        raw_cols = {r[1] for r in conn.execute("PRAGMA table_info(raw_transactions)")}
        if "wallet_id" not in raw_cols:
            conn.execute(
                "ALTER TABLE raw_transactions ADD COLUMN wallet_id INTEGER REFERENCES wallets(id)"
            )

        # -- transactions: add wallet_id ------------------------------------
        tx_cols = {r[1] for r in conn.execute("PRAGMA table_info(transactions)")}
        if "wallet_id" not in tx_cols:
            conn.execute(
                "ALTER TABLE transactions ADD COLUMN wallet_id INTEGER REFERENCES wallets(id)"
            )

        # -- transfer_overrides: recreate with new structure ----------------
        # Check for the new column; if absent the table pre-dates v5.
        ov_cols = {r[1] for r in conn.execute("PRAGMA table_info(transfer_overrides)")}
        if "implied_fee_crypto" not in ov_cols:
            conn.execute(
                """
                CREATE TABLE transfer_overrides_v5 (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    tx_id_a            INTEGER NOT NULL REFERENCES transactions(id),
                    tx_id_b            INTEGER REFERENCES transactions(id),
                    action             TEXT    NOT NULL
                                               CHECK(action IN
                                                ('link','unlink','external-out','external-in')),
                    implied_fee_crypto TEXT    NOT NULL DEFAULT '',
                    external_wallet    TEXT    NOT NULL DEFAULT '',
                    created_at         TEXT    NOT NULL,
                    note               TEXT    NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                INSERT INTO transfer_overrides_v5
                    (id, tx_id_a, tx_id_b, action, created_at, note)
                SELECT id, tx_id_a, tx_id_b, action, created_at, note
                FROM transfer_overrides
                """
            )
            conn.execute("DROP TABLE transfer_overrides")
            conn.execute("ALTER TABLE transfer_overrides_v5 RENAME TO transfer_overrides")


def migrate_to_v6(conn: sqlite3.Connection) -> None:
    """Apply v6 schema migrations to an existing batch file.

    Idempotent — safe to call on fresh databases and already-migrated ones.
    v6 adds the ``crypto_prices`` table for caching historical cryptocurrency
    prices (USD and CAD) fetched from public APIs.  Because it is a brand-new
    table, ``CREATE TABLE IF NOT EXISTS`` inside ``create_tables`` is sufficient
    and no column-level ALTER TABLE statements are required.
    """
    create_tables(conn)


def migrate_to_v7(conn: sqlite3.Connection) -> None:
    """Apply v7 schema migrations to an existing batch file.

    Idempotent — safe to call on fresh databases and already-migrated ones.
    v7 adds ``wallet_id`` (FK → wallets) to ``verified_transactions`` and
    ``classified_events`` so wallet identity is preserved all the way to the
    ACB-engine inputs instead of being dropped at the verify stage.
    """
    with conn:
        vtx_cols = {r[1] for r in conn.execute("PRAGMA table_info(verified_transactions)")}
        if "wallet_id" not in vtx_cols:
            conn.execute(
                "ALTER TABLE verified_transactions"
                " ADD COLUMN wallet_id INTEGER REFERENCES wallets(id)"
            )

        ce_cols = {r[1] for r in conn.execute("PRAGMA table_info(classified_events)")}
        if "wallet_id" not in ce_cols:
            conn.execute(
                "ALTER TABLE classified_events ADD COLUMN wallet_id INTEGER REFERENCES wallets(id)"
            )


def migrate_to_v8(conn: sqlite3.Connection) -> None:
    """Apply v8 schema migrations to an existing batch file.

    Idempotent — safe to call on fresh databases and already-migrated ones.
    v8 adds ``graph_transfer_pairs`` to persist the matched-pair identity and
    hop metadata from graph-traversal matching so ``sirop stir`` can display
    them.  Because it is a brand-new table, CREATE TABLE IF NOT EXISTS inside
    ``create_tables`` is sufficient and no ALTER TABLE statements are required.
    """
    create_tables(conn)


def migrate_to_v9(conn: sqlite3.Connection) -> None:
    """Apply v9 schema migrations to an existing batch file.

    Idempotent — safe to call on fresh databases and already-migrated ones.
    v9 adds:
    - ``raw_transactions.notes`` — label/annotation from wallet CSV (e.g. Sparrow "Label")
    - ``graph_transfer_pairs.deposit_vout_count`` — output count of deposit tx
    - ``graph_transfer_pairs.deposit_vin_count`` — input count of deposit tx
    """
    create_tables(conn)
    with conn:
        raw_cols = {r[1] for r in conn.execute("PRAGMA table_info(raw_transactions)")}
        if "notes" not in raw_cols:
            conn.execute("ALTER TABLE raw_transactions ADD COLUMN notes TEXT NOT NULL DEFAULT ''")
        gtp_cols = {r[1] for r in conn.execute("PRAGMA table_info(graph_transfer_pairs)")}
        if "deposit_vout_count" not in gtp_cols:
            conn.execute(
                "ALTER TABLE graph_transfer_pairs"
                " ADD COLUMN deposit_vout_count INTEGER NOT NULL DEFAULT 0"
            )
        if "deposit_vin_count" not in gtp_cols:
            conn.execute(
                "ALTER TABLE graph_transfer_pairs"
                " ADD COLUMN deposit_vin_count INTEGER NOT NULL DEFAULT 0"
            )
