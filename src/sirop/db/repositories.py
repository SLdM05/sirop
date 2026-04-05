"""Repository layer — all SQLite reads and writes for sirop pipeline tables.

Every pipeline module reads its inputs and writes its outputs through this
module.  No other module touches the ``sqlite3.Connection`` directly for
pipeline data.  Each write function wraps its inserts in a single atomic
SQLite transaction so that a write failure rolls back completely.

Conventions
-----------
- Monetary values stored as ``TEXT`` (fixed-point Decimal string via
  ``format(d, 'f')``).  Read back with ``Decimal(row["col"])``.
- Timestamps stored as ISO 8601 UTC strings. Read back with
  ``datetime.fromisoformat()``.
- Booleans stored as ``INTEGER`` (0 / 1).
- ``id`` columns are assigned by SQLite after INSERT; callers receive
  updated dataclass instances with the real IDs.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Literal, cast

from sirop.models.disposition import ACBState, AdjustedDisposition, Disposition, IncomeEvent
from sirop.models.enums import TransactionType
from sirop.models.event import ClassifiedEvent
from sirop.models.override import TransferOverride
from sirop.models.raw import RawTransaction
from sirop.models.transaction import Transaction
from sirop.models.wallet import Wallet
from sirop.node.models import GraphMatch

if TYPE_CHECKING:
    import sqlite3
from sirop.utils.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Stage status
# ---------------------------------------------------------------------------


def get_stage_status(conn: sqlite3.Connection, stage: str) -> str | None:
    """Return the current status string for *stage*, or None if not found."""
    row = conn.execute("SELECT status FROM stage_status WHERE stage = ?", (stage,)).fetchone()
    return row["status"] if row else None


def set_stage_running(conn: sqlite3.Connection, stage: str) -> None:
    """Mark *stage* as running (non-transactional — called before the write)."""
    conn.execute(
        "UPDATE stage_status SET status = 'running', completed_at = NULL, error = NULL "
        "WHERE stage = ?",
        (stage,),
    )
    conn.commit()


def set_stage_done(conn: sqlite3.Connection, stage: str) -> None:
    """Mark *stage* as done."""
    now = datetime.now(tz=UTC).isoformat()
    conn.execute(
        "UPDATE stage_status SET status = 'done', completed_at = ? WHERE stage = ?",
        (now, stage),
    )
    conn.commit()


def set_stages_invalidated(conn: sqlite3.Connection, stages: list[str]) -> None:
    """Mark all *stages* as invalidated (downstream re-run required)."""
    with conn:
        for stage in stages:
            conn.execute(
                "UPDATE stage_status SET status = 'invalidated', completed_at = NULL "
                "WHERE stage = ?",
                (stage,),
            )


# Reverse-dependency order for safe deletion during stage re-runs.
# Most-downstream stage first so FK constraints are never violated.
_BOIL_CLEAR_ORDER: tuple[str, ...] = (
    "superficial_loss",
    "boil",
    "transfer_match",
    "verify",
    "normalize",
)
_CLEAR_TABLES_BY_STAGE: dict[str, tuple[str, ...]] = {
    "superficial_loss": ("dispositions_adjusted",),
    "boil": ("acb_state", "dispositions"),
    "transfer_match": ("income_events", "classified_events"),
    "verify": ("verified_transactions",),
    "normalize": ("transactions",),
}


def clear_stages_output(conn: sqlite3.Connection, stages: tuple[str, ...]) -> None:
    """Delete all output rows for the given pipeline *stages*.

    Deletion is performed in reverse-dependency order (most downstream first)
    to satisfy FK constraints.  Call this before a ``--from`` re-run to
    prevent rows from the previous run stacking on top of fresh output.

    Foreign key enforcement is suspended for the duration of the delete so
    that ``transfer_overrides`` (which references ``transactions``) does not
    block clearing the ``transactions`` table.  The overrides survive and
    remain valid because normalize assigns the same IDs after the
    ``sqlite_sequence`` high-water mark is reset.

    Parameters
    ----------
    stages:
        Tuple of stage names to clear, e.g. ``("normalize", "verify",
        "transfer_match", "boil", "superficial_loss")``.
    """
    stages_set = set(stages)
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        with conn:
            for stage in _BOIL_CLEAR_ORDER:
                if stage not in stages_set:
                    continue
                for table in _CLEAR_TABLES_BY_STAGE[stage]:
                    conn.execute(f"DELETE FROM {table}")  # noqa: S608
                    conn.execute("DELETE FROM sqlite_sequence WHERE name = ?", (table,))
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


# ---------------------------------------------------------------------------
# Batch metadata
# ---------------------------------------------------------------------------


def read_tax_year(conn: sqlite3.Connection) -> int:
    """Return the tax year stored in batch_meta."""
    row = conn.execute("SELECT tax_year FROM batch_meta").fetchone()
    if row is None:
        raise RuntimeError("batch_meta is missing — was this batch created with 'sirop create'?")
    return int(row["tax_year"])


# ---------------------------------------------------------------------------
# Stage 1 → Stage 2: raw_transactions
# ---------------------------------------------------------------------------


def read_raw_transactions(conn: sqlite3.Connection) -> list[RawTransaction]:
    """Deserialize all rows from ``raw_transactions`` into ``RawTransaction`` dataclasses."""
    rows = conn.execute(
        """
        SELECT source, raw_timestamp, transaction_type, asset, amount,
               amount_currency, fee, fee_currency, cad_amount, fiat_currency,
               cad_rate, spot_rate, txid, extra_json, wallet_id, notes
        FROM raw_transactions
        ORDER BY raw_timestamp
        """
    ).fetchall()

    result: list[RawTransaction] = []
    for row in rows:
        raw_row: dict[str, str] = json.loads(row["extra_json"]) if row["extra_json"] else {}
        ts_str: str = row["raw_timestamp"]
        # Handle both ISO 8601 with offset and plain UTC strings.
        try:
            ts = datetime.fromisoformat(ts_str)
        except ValueError:
            # Fallback: strip trailing 'Z' and parse as UTC.
            ts = datetime.fromisoformat(ts_str.rstrip("Z")).replace(tzinfo=UTC)

        result.append(
            RawTransaction(
                source=row["source"],
                timestamp=ts,
                transaction_type=row["transaction_type"],
                asset=row["asset"],
                amount=Decimal(row["amount"]),
                amount_currency=row["amount_currency"],
                fiat_value=Decimal(row["cad_amount"]) if row["cad_amount"] else None,
                fiat_currency=row["fiat_currency"],
                fee_amount=Decimal(row["fee"]) if row["fee"] else None,
                fee_currency=row["fee_currency"],
                rate=Decimal(row["cad_rate"]) if row["cad_rate"] else None,
                spot_rate=Decimal(row["spot_rate"]) if row["spot_rate"] else None,
                txid=row["txid"],
                raw_type=row["transaction_type"],
                raw_row=raw_row,
                wallet_id=row["wallet_id"],
                notes=row["notes"] or "",
            )
        )
    return result


# ---------------------------------------------------------------------------
# Stage 2 output: transactions
# ---------------------------------------------------------------------------


def write_transactions(conn: sqlite3.Connection, txs: list[Transaction]) -> list[Transaction]:
    """Write *txs* to ``transactions`` and return copies with DB-assigned IDs.

    Executes as a single atomic transaction.
    """
    updated: list[Transaction] = []
    with conn:
        for tx in txs:
            cursor = conn.execute(
                """
                INSERT INTO transactions
                    (raw_id, timestamp, transaction_type, asset, amount,
                     fee_crypto, fee_currency, cad_amount, cad_fee, cad_rate,
                     txid, source, is_transfer, counterpart_id, notes, wallet_id)
                VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tx.timestamp.isoformat(),
                    tx.tx_type.value,
                    tx.asset,
                    format(tx.amount, "f"),
                    format(tx.fee_crypto, "f") if tx.fee_crypto else None,
                    None,  # fee_currency — not tracked on Transaction currently
                    format(tx.cad_value, "f"),
                    format(tx.fee_cad, "f"),
                    format(tx.cad_value / tx.amount, "f")
                    if tx.amount
                    else format(Decimal("0"), "f"),
                    tx.txid,
                    tx.source,
                    1 if tx.is_transfer else 0,
                    tx.counterpart_id,
                    tx.notes,
                    tx.wallet_id,
                ),
            )
            updated.append(
                Transaction(
                    id=cursor.lastrowid or 0,
                    source=tx.source,
                    timestamp=tx.timestamp,
                    tx_type=tx.tx_type,
                    asset=tx.asset,
                    amount=tx.amount,
                    cad_value=tx.cad_value,
                    fee_cad=tx.fee_cad,
                    fee_crypto=tx.fee_crypto,
                    txid=tx.txid,
                    is_transfer=tx.is_transfer,
                    counterpart_id=tx.counterpart_id,
                    notes=tx.notes,
                    wallet_id=tx.wallet_id,
                )
            )
    return updated


def read_transactions(conn: sqlite3.Connection) -> list[Transaction]:
    """Deserialize all rows from ``transactions`` into ``Transaction`` dataclasses."""
    rows = conn.execute(
        """
        SELECT id, source, timestamp, transaction_type, asset, amount,
               cad_amount, cad_fee, fee_crypto, txid, is_transfer, counterpart_id, notes,
               wallet_id
        FROM transactions
        ORDER BY timestamp
        """
    ).fetchall()

    result: list[Transaction] = []
    for row in rows:
        result.append(
            Transaction(
                id=row["id"],
                source=row["source"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                tx_type=TransactionType(row["transaction_type"]),
                asset=row["asset"],
                amount=Decimal(row["amount"]),
                cad_value=Decimal(row["cad_amount"]),
                fee_cad=Decimal(row["cad_fee"]) if row["cad_fee"] else Decimal("0"),
                fee_crypto=Decimal(row["fee_crypto"]) if row["fee_crypto"] else Decimal("0"),
                txid=row["txid"],
                is_transfer=bool(row["is_transfer"]),
                counterpart_id=row["counterpart_id"],
                notes=row["notes"] or "",
                wallet_id=row["wallet_id"],
            )
        )
    return result


# ---------------------------------------------------------------------------
# Stage 3: pass-through verify (promote transactions → verified_transactions)
# ---------------------------------------------------------------------------


def promote_to_verified(conn: sqlite3.Connection) -> int:
    """Copy all rows from ``transactions`` into ``verified_transactions`` as-is.

    This is the pass-through verify path used when no Bitcoin node is available.
    Row IDs are preserved (tx_id = transactions.id) so downstream stages can
    join back to the source.

    Returns the number of rows promoted.
    """
    with conn:
        conn.execute(
            """
            INSERT INTO verified_transactions
                (tx_id, timestamp, transaction_type, asset, amount,
                 fee_crypto, fee_currency, cad_amount, cad_fee, cad_rate,
                 txid, source, is_transfer, counterpart_id,
                 node_verified, block_height, confirmations, wallet_id)
            SELECT
                id, timestamp, transaction_type, asset, amount,
                fee_crypto, fee_currency, cad_amount, cad_fee, cad_rate,
                txid, source, is_transfer, counterpart_id,
                0, NULL, NULL, wallet_id
            FROM transactions
            """
        )
    count = conn.execute("SELECT COUNT(*) FROM verified_transactions").fetchone()[0]
    return int(count)


# ---------------------------------------------------------------------------
# Stage 4 output: classified_events, income_events
# ---------------------------------------------------------------------------


def read_verified_tx_id_map(conn: sqlite3.Connection) -> dict[int, int]:
    """Return a mapping from ``transactions.id`` to ``verified_transactions.id``.

    The transfer matcher receives ``Transaction`` objects whose ``.id`` equals
    ``transactions.id`` and uses it as ``vtx_id`` in ``ClassifiedEvent`` and
    ``IncomeEvent``.  But ``classified_events.vtx_id`` and
    ``income_events.vtx_id`` FK-reference ``verified_transactions.id`` — a
    separate autoincrement sequence.  On an untouched batch these happen to
    match; after a ``--from`` re-run the sequences diverge.  Use this map to
    patch ``vtx_id`` before writing to the DB.
    """
    rows = conn.execute("SELECT id, tx_id FROM verified_transactions").fetchall()
    return {int(row["tx_id"]): int(row["id"]) for row in rows}


def write_classified_events(
    conn: sqlite3.Connection, events: list[ClassifiedEvent]
) -> list[ClassifiedEvent]:
    """Write *events* to ``classified_events`` and return copies with DB IDs."""
    updated: list[ClassifiedEvent] = []
    with conn:
        for evt in events:
            cursor = conn.execute(
                """
                INSERT INTO classified_events
                    (vtx_id, timestamp, event_type, asset, amount,
                     cad_proceeds, cad_cost, cad_fee, txid, source, is_taxable,
                     wallet_id, is_provisional)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evt.vtx_id,
                    evt.timestamp.isoformat(),
                    evt.event_type,
                    evt.asset,
                    format(evt.amount, "f"),
                    format(evt.cad_proceeds, "f") if evt.cad_proceeds is not None else None,
                    format(evt.cad_cost, "f") if evt.cad_cost is not None else None,
                    format(evt.cad_fee, "f") if evt.cad_fee is not None else None,
                    evt.txid,
                    evt.source,
                    1 if evt.is_taxable else 0,
                    evt.wallet_id,
                    1 if evt.is_provisional else 0,
                ),
            )
            updated.append(
                ClassifiedEvent(
                    id=cursor.lastrowid or 0,
                    vtx_id=evt.vtx_id,
                    timestamp=evt.timestamp,
                    event_type=evt.event_type,
                    asset=evt.asset,
                    amount=evt.amount,
                    cad_proceeds=evt.cad_proceeds,
                    cad_cost=evt.cad_cost,
                    cad_fee=evt.cad_fee,
                    txid=evt.txid,
                    source=evt.source,
                    is_taxable=evt.is_taxable,
                    wallet_id=evt.wallet_id,
                    is_provisional=evt.is_provisional,
                )
            )
    return updated


def write_income_events(conn: sqlite3.Connection, events: list[IncomeEvent]) -> list[IncomeEvent]:
    """Write *events* to ``income_events`` and return copies with DB IDs."""
    updated: list[IncomeEvent] = []
    with conn:
        for evt in events:
            cursor = conn.execute(
                """
                INSERT INTO income_events
                    (vtx_id, timestamp, asset, units, income_type, fmv_cad, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evt.vtx_id,
                    evt.timestamp.isoformat(),
                    evt.asset,
                    format(evt.units, "f"),
                    evt.income_type,
                    format(evt.fmv_cad, "f"),
                    evt.source,
                ),
            )
            updated.append(
                IncomeEvent(
                    id=cursor.lastrowid or 0,
                    vtx_id=evt.vtx_id,
                    timestamp=evt.timestamp,
                    asset=evt.asset,
                    units=evt.units,
                    income_type=evt.income_type,
                    fmv_cad=evt.fmv_cad,
                    source=evt.source,
                )
            )
    return updated


def read_income_events(conn: sqlite3.Connection) -> list[IncomeEvent]:
    """Deserialize all rows from ``income_events`` into ``IncomeEvent`` dataclasses."""
    rows = conn.execute(
        """
        SELECT id, vtx_id, timestamp, asset, units, income_type, fmv_cad, source
        FROM income_events
        ORDER BY timestamp
        """
    ).fetchall()

    result: list[IncomeEvent] = []
    for row in rows:
        result.append(
            IncomeEvent(
                id=row["id"],
                vtx_id=row["vtx_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                asset=row["asset"],
                units=Decimal(row["units"]),
                income_type=row["income_type"],
                fmv_cad=Decimal(row["fmv_cad"]),
                source=row["source"],
            )
        )
    return result


def read_classified_events(conn: sqlite3.Connection) -> list[ClassifiedEvent]:
    """Deserialize taxable rows from ``classified_events`` into dataclasses.

    Only ``is_taxable=1`` events are returned — these are the inputs to the
    ACB engine.  Transfer legs are excluded.
    """
    rows = conn.execute(
        """
        SELECT id, vtx_id, timestamp, event_type, asset, amount,
               cad_proceeds, cad_cost, cad_fee, txid, source, is_taxable, wallet_id,
               is_provisional
        FROM classified_events
        WHERE is_taxable = 1
        ORDER BY timestamp
        """
    ).fetchall()

    result: list[ClassifiedEvent] = []
    for row in rows:
        result.append(
            ClassifiedEvent(
                id=row["id"],
                vtx_id=row["vtx_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                event_type=row["event_type"],
                asset=row["asset"],
                amount=Decimal(row["amount"]),
                cad_proceeds=Decimal(row["cad_proceeds"]) if row["cad_proceeds"] else None,
                cad_cost=Decimal(row["cad_cost"]) if row["cad_cost"] else None,
                cad_fee=Decimal(row["cad_fee"]) if row["cad_fee"] else None,
                txid=row["txid"],
                source=row["source"],
                is_taxable=bool(row["is_taxable"]),
                wallet_id=row["wallet_id"],
                is_provisional=bool(row["is_provisional"]),
            )
        )
    return result


def read_all_classified_events(conn: sqlite3.Connection) -> list[ClassifiedEvent]:
    """Deserialize ALL rows from ``classified_events`` (including transfers).

    Used by the superficial loss detector which needs the full acquisition
    history to compute running balances.
    """
    rows = conn.execute(
        """
        SELECT id, vtx_id, timestamp, event_type, asset, amount,
               cad_proceeds, cad_cost, cad_fee, txid, source, is_taxable, wallet_id,
               is_provisional
        FROM classified_events
        ORDER BY timestamp
        """
    ).fetchall()

    result: list[ClassifiedEvent] = []
    for row in rows:
        result.append(
            ClassifiedEvent(
                id=row["id"],
                vtx_id=row["vtx_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                event_type=row["event_type"],
                asset=row["asset"],
                amount=Decimal(row["amount"]),
                cad_proceeds=Decimal(row["cad_proceeds"]) if row["cad_proceeds"] else None,
                cad_cost=Decimal(row["cad_cost"]) if row["cad_cost"] else None,
                cad_fee=Decimal(row["cad_fee"]) if row["cad_fee"] else None,
                txid=row["txid"],
                source=row["source"],
                is_taxable=bool(row["is_taxable"]),
                wallet_id=row["wallet_id"],
                is_provisional=bool(row["is_provisional"]),
            )
        )
    return result


# ---------------------------------------------------------------------------
# Stage 5 output: dispositions + acb_state
# ---------------------------------------------------------------------------


def write_dispositions(
    conn: sqlite3.Connection,
    disps: list[Disposition],
    states: list[ACBState],
) -> list[Disposition]:
    """Write dispositions and ACB state snapshots atomically.

    ACB state snapshots are associated with the classified_event that triggered
    them.  Since ACBState has no ``event_id`` field in the dataclass, we pair
    states with dispositions positionally (one state per disposition, for
    disposal events).

    Returns copies of *disps* with DB-assigned IDs.
    """
    # Build a map: classified_event id → acb_state for disposal events.
    # states list length must match disps list length.
    updated: list[Disposition] = []

    with conn:
        for disp, state_after in zip(disps, states, strict=True):
            # Write disposition row.
            cursor = conn.execute(
                """
                INSERT INTO dispositions
                    (event_id, timestamp, asset, units, proceeds, acb_of_disposed,
                     selling_fees, gain_loss, disposition_type, year_acquired,
                     acb_per_unit_before, pool_units_before, pool_cost_before,
                     acb_per_unit_after, pool_units_after, pool_cost_after)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    disp.event_id,
                    disp.timestamp.isoformat(),
                    disp.asset,
                    format(disp.units, "f"),
                    format(disp.proceeds_cad, "f"),
                    format(disp.acb_of_disposed_cad, "f"),
                    format(disp.selling_fees_cad, "f"),
                    format(disp.gain_loss_cad, "f"),
                    disp.disposition_type,
                    disp.year_acquired,
                    format(disp.acb_state_before.acb_per_unit_cad, "f"),
                    format(disp.acb_state_before.total_units, "f"),
                    format(disp.acb_state_before.total_acb_cad, "f"),
                    format(state_after.acb_per_unit_cad, "f"),
                    format(state_after.total_units, "f"),
                    format(state_after.total_acb_cad, "f"),
                ),
            )
            disp_id = cursor.lastrowid or 0

            # Write ACB state snapshot linked to this disposition's event.
            conn.execute(
                """
                INSERT INTO acb_state
                    (event_id, asset, pool_cost, units, snapshot_date)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    disp.event_id,
                    state_after.asset,
                    format(state_after.total_acb_cad, "f"),
                    format(state_after.total_units, "f"),
                    disp.timestamp.date().isoformat(),
                ),
            )

            updated.append(
                Disposition(
                    id=disp_id,
                    event_id=disp.event_id,
                    timestamp=disp.timestamp,
                    asset=disp.asset,
                    units=disp.units,
                    proceeds_cad=disp.proceeds_cad,
                    acb_of_disposed_cad=disp.acb_of_disposed_cad,
                    selling_fees_cad=disp.selling_fees_cad,
                    gain_loss_cad=disp.gain_loss_cad,
                    disposition_type=disp.disposition_type,
                    year_acquired=disp.year_acquired,
                    acb_state_before=disp.acb_state_before,
                    acb_state_after=state_after,
                )
            )
    return updated


def write_holdover_acb_states(
    conn: sqlite3.Connection,
    holdovers: list[tuple[ACBState, int]],
) -> None:
    """Write year-end acb_state snapshots for assets with no disposals.

    Called after write_dispositions() to record the final pool balance for
    assets that were only acquired (never sold) in the tax year.  Without
    these rows the year-end holdings query in _print_summary finds nothing
    for those assets.

    Parameters
    ----------
    holdovers:
        List of (ACBState, classified_event_id) pairs — one per acquisition-
        only asset.  The event_id is the last classified event processed for
        that asset.
    """
    with conn:
        for pool, event_id in holdovers:
            row = conn.execute(
                "SELECT timestamp FROM classified_events WHERE id = ?", (event_id,)
            ).fetchone()
            snapshot_date = row["timestamp"][:10] if row else "unknown"
            conn.execute(
                """
                INSERT INTO acb_state (event_id, asset, pool_cost, units, snapshot_date)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    pool.asset,
                    format(pool.total_acb_cad, "f"),
                    format(pool.total_units, "f"),
                    snapshot_date,
                ),
            )


def read_acb_state_final(conn: sqlite3.Connection) -> list[ACBState]:
    """Return the latest ``acb_state`` snapshot per asset with remaining units > 0.

    Selects the row with the highest ``id`` for each asset, then filters to
    assets where ``CAST(units AS REAL) > 0``.
    """
    rows = conn.execute(
        """
        SELECT asset, pool_cost, units
        FROM acb_state
        WHERE id IN (
            SELECT MAX(id) FROM acb_state GROUP BY asset
        )
        AND CAST(units AS REAL) > 0
        ORDER BY asset
        """
    ).fetchall()

    result: list[ACBState] = []
    for row in rows:
        total_units = Decimal(row["units"])
        total_acb_cad = Decimal(row["pool_cost"])
        acb_per_unit_cad = total_acb_cad / total_units if total_units else Decimal("0")
        result.append(
            ACBState(
                asset=row["asset"],
                total_units=total_units,
                total_acb_cad=total_acb_cad,
                acb_per_unit_cad=acb_per_unit_cad,
            )
        )
    return result


@dataclasses.dataclass(frozen=True)
class WalletHolding:
    """Year-end net units held per wallet/asset, derived from the transactions table.

    Used by ``_print_wallet_holdings`` in boil.py and by tests to assert that
    the per-wallet breakdown sums to the same total as the global ACB pool.
    """

    wallet_name: str
    asset: str
    net_units: Decimal


def read_per_wallet_holdings(conn: sqlite3.Connection, tax_year: int) -> list[WalletHolding]:
    """Return year-end net units held per wallet/asset through year-end.

    Sums deposits (positive) and withdrawals (negative) from the ``transactions``
    table, then subtracts fee_disposal amounts so the per-wallet total matches
    the global ACB pool unit count.

    Transfer-linked transactions (vtx_id in classified_events where
    event_type='transfer') are excluded — they are inter-wallet movements that
    do not affect the ACB pool, so including them would cause the per-wallet
    sum to diverge from the global block whenever a matched transfer pair has
    mismatched amounts.
    """
    year_end = f"{tax_year}-12-31"
    rows = conn.execute(
        """
        WITH transfer_vtx_ids AS (
            -- vtx_ids of matched transfer legs (is_taxable=0).
            -- Exclude NULL vtx_ids to avoid NOT IN returning UNKNOWN for all rows.
            SELECT DISTINCT vtx_id
            FROM classified_events
            WHERE event_type = 'transfer'
              AND vtx_id IS NOT NULL
        ),
        fee_disposal_adj AS (
            -- Sum fee_disposal amounts per wallet/asset for non-transfer txs
            -- within the tax year.  These reduce the global ACB pool but are
            -- not reflected in transactions.amount, so per-wallet holdings
            -- would otherwise overstate.
            SELECT
                ce.wallet_id,
                ce.asset,
                SUM(CAST(ce.amount AS REAL)) AS adj
            FROM classified_events ce
            WHERE ce.event_type = 'fee_disposal'
              AND ce.is_taxable = 1
              AND ce.timestamp <= ?
              AND (ce.vtx_id IS NULL OR ce.vtx_id NOT IN (SELECT vtx_id FROM transfer_vtx_ids))
            GROUP BY ce.wallet_id, ce.asset
        ),
        provisional_adj AS (
            -- Sum provisional buy amounts per wallet/asset within the tax year.
            -- Provisional events represent surplus BTC in graph-matched transfer pairs
            -- (deposit.amount > withdrawal.amount).  The deposit tx is excluded from
            -- the main sum (it is transfer-linked), so we add the surplus back here.
            SELECT
                ce.wallet_id,
                ce.asset,
                SUM(CAST(ce.amount AS REAL)) AS adj
            FROM classified_events ce
            WHERE ce.is_provisional = 1
              AND ce.is_taxable = 1
              AND ce.timestamp <= ?
            GROUP BY ce.wallet_id, ce.asset
        )
        SELECT
            COALESCE(w.name, t.source) AS wallet_name,
            t.asset,
            SUM(CASE
                WHEN t.transaction_type IN ('buy','transfer_in','deposit','income')
                    THEN CAST(t.amount AS REAL)
                WHEN t.transaction_type IN ('sell','transfer_out','withdrawal','spend','fee')
                    THEN -CAST(t.amount AS REAL)
                ELSE 0
            END) - COALESCE(MAX(fda.adj), 0) + COALESCE(MAX(pa.adj), 0) AS net_units
        FROM transactions t
        LEFT JOIN wallets w ON t.wallet_id = w.id
        LEFT JOIN fee_disposal_adj fda ON fda.wallet_id = t.wallet_id AND fda.asset = t.asset
        LEFT JOIN provisional_adj pa ON pa.wallet_id = t.wallet_id AND pa.asset = t.asset
        WHERE t.timestamp <= ?
          AND t.id NOT IN (SELECT vtx_id FROM transfer_vtx_ids)
          AND t.asset IN (
            SELECT asset FROM acb_state
            WHERE id IN (SELECT MAX(id) FROM acb_state GROUP BY asset)
              AND CAST(units AS REAL) > 0
        )
        GROUP BY wallet_name, t.asset
        HAVING net_units > 0.00000001
        ORDER BY wallet_name, t.asset
        """,
        (year_end, year_end, year_end),
    ).fetchall()

    return [
        WalletHolding(
            wallet_name=str(r["wallet_name"]),
            asset=str(r["asset"]),
            net_units=Decimal(str(r["net_units"])),
        )
        for r in rows
    ]


@dataclasses.dataclass(frozen=True)
class ProvisionalEvent:
    """A provisional classified_event paired with its triggering graph-transfer context."""

    ce_id: int
    timestamp: str  # ISO 8601
    asset: str
    amount: Decimal
    cad_cost: Decimal | None
    wallet_name: str
    withdrawal_id: int  # first (or only) withdrawal id — MIN(gtp.withdrawal_id)
    deposit_id: int  # transactions.id of the deposit leg (vtx_id of this event)
    withdrawal_count: int  # number of matched withdrawals (>1 = consolidation)
    withdrawal_amount: Decimal  # sum of all matched withdrawal amounts
    deposit_amount: Decimal


def read_provisional_events(conn: sqlite3.Connection) -> list[ProvisionalEvent]:
    """Return all provisional classified events joined with their graph-pair context.

    Provisional events are surplus acquisitions emitted when a graph-matched
    transfer pair has a larger deposit than withdrawal.  They need user review
    (via ``sirop stir view``) and will be reclassified as transfers if the user
    adds the intermediate wallet via ``sirop tap``.
    """
    rows = conn.execute(
        """
        SELECT
            ce.id,
            ce.timestamp,
            ce.asset,
            ce.amount,
            ce.cad_cost,
            COALESCE(w.name, ce.source)       AS wallet_name,
            COUNT(DISTINCT gtp.withdrawal_id)  AS withdrawal_count,
            MIN(gtp.withdrawal_id)             AS withdrawal_id,
            ce.vtx_id                          AS deposit_id,
            SUM(t_out.amount)                  AS withdrawal_amount,
            t_in.amount                        AS deposit_amount
        FROM classified_events ce
        LEFT JOIN wallets w    ON w.id         = ce.wallet_id
        LEFT JOIN graph_transfer_pairs gtp
               ON gtp.deposit_id              = ce.vtx_id
        LEFT JOIN transactions t_out ON t_out.id = gtp.withdrawal_id
        LEFT JOIN transactions t_in  ON t_in.id  = ce.vtx_id
        WHERE ce.is_provisional = 1
          AND ce.is_taxable = 1
        GROUP BY ce.id
        ORDER BY ce.timestamp
        """
    ).fetchall()

    return [
        ProvisionalEvent(
            ce_id=row["id"],
            timestamp=row["timestamp"],
            asset=row["asset"],
            amount=Decimal(row["amount"]),
            cad_cost=Decimal(row["cad_cost"]) if row["cad_cost"] else None,
            wallet_name=str(row["wallet_name"]),
            withdrawal_id=row["withdrawal_id"],
            deposit_id=row["deposit_id"],
            withdrawal_count=int(row["withdrawal_count"]),
            withdrawal_amount=Decimal(row["withdrawal_amount"]),
            deposit_amount=Decimal(row["deposit_amount"]),
        )
        for row in rows
    ]


def read_dispositions(conn: sqlite3.Connection) -> list[Disposition]:
    """Deserialize all rows from ``dispositions`` into ``Disposition`` dataclasses."""
    rows = conn.execute(
        """
        SELECT id, event_id, timestamp, asset, units, proceeds, acb_of_disposed,
               selling_fees, gain_loss, disposition_type, year_acquired,
               acb_per_unit_before, pool_units_before, pool_cost_before,
               acb_per_unit_after, pool_units_after, pool_cost_after
        FROM dispositions
        ORDER BY timestamp
        """
    ).fetchall()

    result: list[Disposition] = []
    for row in rows:
        before = ACBState(
            asset=row["asset"],
            total_units=Decimal(row["pool_units_before"]),
            total_acb_cad=Decimal(row["pool_cost_before"]),
            acb_per_unit_cad=Decimal(row["acb_per_unit_before"]),
        )
        after = ACBState(
            asset=row["asset"],
            total_units=Decimal(row["pool_units_after"]),
            total_acb_cad=Decimal(row["pool_cost_after"]),
            acb_per_unit_cad=Decimal(row["acb_per_unit_after"]),
        )
        result.append(
            Disposition(
                id=row["id"],
                event_id=row["event_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                asset=row["asset"],
                units=Decimal(row["units"]),
                proceeds_cad=Decimal(row["proceeds"]),
                acb_of_disposed_cad=Decimal(row["acb_of_disposed"]),
                selling_fees_cad=Decimal(row["selling_fees"]),
                gain_loss_cad=Decimal(row["gain_loss"]),
                disposition_type=row["disposition_type"],
                year_acquired=row["year_acquired"],
                acb_state_before=before,
                acb_state_after=after,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Stage 6 output: dispositions_adjusted
# ---------------------------------------------------------------------------


def write_adjusted_dispositions(
    conn: sqlite3.Connection, adjs: list[AdjustedDisposition]
) -> list[AdjustedDisposition]:
    """Write adjusted dispositions and return copies with DB IDs."""
    updated: list[AdjustedDisposition] = []
    with conn:
        for adj in adjs:
            cursor = conn.execute(
                """
                INSERT INTO dispositions_adjusted
                    (disposition_id, timestamp, asset, units, proceeds, acb_of_disposed,
                     selling_fees, gain_loss, is_superficial_loss,
                     superficial_loss_denied, allowable_loss,
                     adjusted_gain_loss, adjusted_acb_of_repurchase,
                     disposition_type, year_acquired)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    adj.disposition_id,
                    adj.timestamp.isoformat(),
                    adj.asset,
                    format(adj.units, "f"),
                    format(adj.proceeds_cad, "f"),
                    format(adj.acb_of_disposed_cad, "f"),
                    format(adj.selling_fees_cad, "f"),
                    format(adj.gain_loss_cad, "f"),
                    1 if adj.is_superficial_loss else 0,
                    format(adj.superficial_loss_denied_cad, "f"),
                    format(adj.allowable_loss_cad, "f"),
                    format(adj.adjusted_gain_loss_cad, "f"),
                    format(adj.adjusted_acb_of_repurchase_cad, "f")
                    if adj.adjusted_acb_of_repurchase_cad is not None
                    else None,
                    adj.disposition_type,
                    adj.year_acquired,
                ),
            )
            updated.append(
                AdjustedDisposition(
                    id=cursor.lastrowid or 0,
                    disposition_id=adj.disposition_id,
                    timestamp=adj.timestamp,
                    asset=adj.asset,
                    units=adj.units,
                    proceeds_cad=adj.proceeds_cad,
                    acb_of_disposed_cad=adj.acb_of_disposed_cad,
                    selling_fees_cad=adj.selling_fees_cad,
                    gain_loss_cad=adj.gain_loss_cad,
                    is_superficial_loss=adj.is_superficial_loss,
                    superficial_loss_denied_cad=adj.superficial_loss_denied_cad,
                    allowable_loss_cad=adj.allowable_loss_cad,
                    adjusted_gain_loss_cad=adj.adjusted_gain_loss_cad,
                    adjusted_acb_of_repurchase_cad=adj.adjusted_acb_of_repurchase_cad,
                    disposition_type=adj.disposition_type,
                    year_acquired=adj.year_acquired,
                )
            )
    return updated


def read_adjusted_dispositions(conn: sqlite3.Connection) -> list[AdjustedDisposition]:
    """Deserialize all rows from ``dispositions_adjusted`` into dataclasses."""
    rows = conn.execute(
        """
        SELECT id, disposition_id, timestamp, asset, units, proceeds, acb_of_disposed,
               selling_fees, gain_loss, is_superficial_loss, superficial_loss_denied,
               allowable_loss, adjusted_gain_loss, adjusted_acb_of_repurchase,
               disposition_type, year_acquired
        FROM dispositions_adjusted
        ORDER BY timestamp
        """
    ).fetchall()

    result: list[AdjustedDisposition] = []
    for row in rows:
        result.append(
            AdjustedDisposition(
                id=row["id"],
                disposition_id=row["disposition_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                asset=row["asset"],
                units=Decimal(row["units"]),
                proceeds_cad=Decimal(row["proceeds"]),
                acb_of_disposed_cad=Decimal(row["acb_of_disposed"]),
                selling_fees_cad=Decimal(row["selling_fees"]),
                gain_loss_cad=Decimal(row["gain_loss"]),
                is_superficial_loss=bool(row["is_superficial_loss"]),
                superficial_loss_denied_cad=Decimal(row["superficial_loss_denied"]),
                allowable_loss_cad=Decimal(row["allowable_loss"]),
                adjusted_gain_loss_cad=Decimal(row["adjusted_gain_loss"]),
                adjusted_acb_of_repurchase_cad=Decimal(row["adjusted_acb_of_repurchase"])
                if row["adjusted_acb_of_repurchase"] is not None
                else None,
                disposition_type=row["disposition_type"],
                year_acquired=row["year_acquired"],
            )
        )
    return result


# ---------------------------------------------------------------------------
# BoC rate cache helpers (called by normalizer via utils/boc.py)
# ---------------------------------------------------------------------------


def read_boc_rate(
    conn: sqlite3.Connection, currency_pair: str, rate_date: object
) -> Decimal | None:
    """Return a cached BoC rate or None. Used by ``utils/boc.py``."""
    row = conn.execute(
        "SELECT rate FROM boc_rates WHERE currency_pair = ? AND date = ?",
        (currency_pair, str(rate_date)),
    ).fetchone()
    return Decimal(row["rate"]) if row else None


def write_boc_rate(
    conn: sqlite3.Connection,
    currency_pair: str,
    rate_date: object,
    rate: Decimal,
) -> None:
    """Insert or replace a BoC rate in the cache. Used by ``utils/boc.py``."""
    now = datetime.now(tz=UTC).isoformat()
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO boc_rates (date, currency_pair, rate, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            (str(rate_date), currency_pair, format(rate, "f"), now),
        )


# ---------------------------------------------------------------------------
# Wallets (assigned at tap time)
# ---------------------------------------------------------------------------


def find_or_create_wallet(
    conn: sqlite3.Connection,
    name: str,
    source: str,
    auto_created: bool,
) -> Wallet:
    """Return the wallet with *name*, creating it if it does not exist.

    If the wallet already exists its ``source`` and ``auto_created`` fields
    are left unchanged — a later ``tap --wallet NAME`` for the same wallet
    name is treated as tapping into the same wallet.
    """
    now = datetime.now(tz=UTC)
    with conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO wallets (name, source, auto_created, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (name, source, 1 if auto_created else 0, now.isoformat()),
        )
    row = conn.execute(
        "SELECT id, name, source, auto_created, created_at, note FROM wallets WHERE name = ?",
        (name,),
    ).fetchone()
    return Wallet(
        id=row["id"],
        name=row["name"],
        source=row["source"],
        auto_created=bool(row["auto_created"]),
        created_at=datetime.fromisoformat(row["created_at"]),
        note=row["note"] or "",
    )


def wallet_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Return True if a wallet row with *name* already exists."""
    row = conn.execute("SELECT 1 FROM wallets WHERE name = ?", (name,)).fetchone()
    return row is not None


def read_wallets(conn: sqlite3.Connection) -> list[Wallet]:
    """Return all wallets ordered by name."""
    rows = conn.execute(
        "SELECT id, name, source, auto_created, created_at, note FROM wallets ORDER BY name"
    ).fetchall()
    return [
        Wallet(
            id=row["id"],
            name=row["name"],
            source=row["source"],
            auto_created=bool(row["auto_created"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            note=row["note"] or "",
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Transfer overrides (stir command)
# ---------------------------------------------------------------------------


def write_transfer_override(  # noqa: PLR0913
    conn: sqlite3.Connection,
    tx_id_a: int,
    tx_id_b: int | None,
    action: str,
    implied_fee_crypto: Decimal = Decimal("0"),
    external_wallet: str = "",
    note: str = "",
) -> TransferOverride:
    """Insert a transfer override and return it with its DB-assigned ID.

    Parameters
    ----------
    tx_id_a:
        ``transactions.id`` of the primary transaction.
    tx_id_b:
        ``transactions.id`` of the counterpart transaction.  ``None`` for
        ``'external-out'`` and ``'external-in'`` actions.
    action:
        ``'link'``, ``'unlink'``, ``'external-out'``, or ``'external-in'``.
    implied_fee_crypto:
        For ``'link'``: exact difference between sent and received amounts.
        The boil stage converts this into a fee micro-disposition.
    external_wallet:
        For ``'external-out'``/``'external-in'``: name of the external wallet.
    note:
        Optional free-text annotation.
    """
    now = datetime.now(tz=UTC)
    fee_str = format(implied_fee_crypto, "f") if implied_fee_crypto else ""
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO transfer_overrides
                (tx_id_a, tx_id_b, action, implied_fee_crypto, external_wallet,
                 created_at, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (tx_id_a, tx_id_b, action, fee_str, external_wallet, now.isoformat(), note),
        )
    return TransferOverride(
        id=cursor.lastrowid or 0,
        tx_id_a=tx_id_a,
        tx_id_b=tx_id_b,
        action=action,  # type: ignore[arg-type]
        implied_fee_crypto=implied_fee_crypto,
        external_wallet=external_wallet,
        created_at=now,
        note=note,
    )


def read_transfer_overrides(conn: sqlite3.Connection) -> list[TransferOverride]:
    """Return all transfer overrides ordered by creation time."""
    rows = conn.execute(
        """
        SELECT id, tx_id_a, tx_id_b, action, implied_fee_crypto,
               external_wallet, created_at, note
        FROM transfer_overrides
        ORDER BY created_at
        """
    ).fetchall()

    return [
        TransferOverride(
            id=row["id"],
            tx_id_a=row["tx_id_a"],
            tx_id_b=row["tx_id_b"],
            action=row["action"],
            implied_fee_crypto=Decimal(row["implied_fee_crypto"])
            if row["implied_fee_crypto"]
            else Decimal("0"),
            external_wallet=row["external_wallet"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
            note=row["note"] or "",
        )
        for row in rows
    ]


def delete_transfer_override(conn: sqlite3.Connection, override_id: int) -> None:
    """Delete a single override row by its primary key."""
    with conn:
        conn.execute("DELETE FROM transfer_overrides WHERE id = ?", (override_id,))


def clear_transfer_overrides_for_tx(conn: sqlite3.Connection, tx_id: int) -> int:
    """Remove all overrides that involve *tx_id* (as either leg).

    Returns the number of rows deleted.
    """
    with conn:
        cursor = conn.execute(
            "DELETE FROM transfer_overrides WHERE tx_id_a = ? OR tx_id_b = ?",
            (tx_id, tx_id),
        )
    return cursor.rowcount


def write_graph_transfer_pairs(conn: sqlite3.Connection, pairs: list[GraphMatch]) -> None:
    """Replace the graph_transfer_pairs table contents atomically.

    Clears all existing rows before inserting *pairs* so the table always
    reflects the most recent ``boil`` run.  An empty *pairs* list results in
    a cleared table (graph traversal found no matches or was disabled).
    """
    with conn:
        conn.execute("DELETE FROM graph_transfer_pairs")
        conn.executemany(
            "INSERT INTO graph_transfer_pairs"
            " (withdrawal_id, deposit_id, hops, direction, fee_crypto,"
            "  deposit_vout_count, deposit_vin_count)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    gm.withdrawal_db_id,
                    gm.deposit_db_id,
                    gm.hops,
                    gm.direction,
                    format(gm.fee_crypto, "f"),
                    gm.deposit_vout_count,
                    gm.deposit_vin_count,
                )
                for gm in pairs
            ],
        )


def read_graph_transfer_pairs(conn: sqlite3.Connection) -> list[GraphMatch]:
    """Return all graph-traversal matched pairs from the last boil run.

    Returns an empty list if the table does not exist (boil not yet run on a
    pre-v8 batch file) or contains no rows (traversal disabled or no matches).
    """
    try:
        rows = conn.execute(
            "SELECT withdrawal_id, deposit_id, hops, direction, fee_crypto,"
            "       deposit_vout_count, deposit_vin_count"
            " FROM graph_transfer_pairs"
        ).fetchall()
    except Exception:  # table absent on pre-v8 schema
        return []
    return [
        GraphMatch(
            deposit_db_id=int(row[1]),
            withdrawal_db_id=int(row[0]),
            direction=cast("Literal['backward', 'forward']", str(row[3])),
            hops=int(row[2]),
            fee_crypto=Decimal(row[4]),
            deposit_vout_count=int(row[5]) if row[5] is not None else 0,
            deposit_vin_count=int(row[6]) if row[6] is not None else 0,
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Transaction txid overrides  (v10)
# ---------------------------------------------------------------------------


def write_transaction_txid_override(
    conn: sqlite3.Connection, transaction_id: int, txid: str
) -> None:
    """Persist a user-supplied blockchain txid for a transaction that had none.

    Uses INSERT OR REPLACE so calling this twice for the same *transaction_id*
    simply overwrites the previous value.  The txid must be a valid 64-char
    lowercase hex string — validation is the caller's responsibility.
    """
    now = datetime.now(tz=UTC).isoformat()
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO transaction_txid_overrides"
            " (transaction_id, txid, created_at) VALUES (?, ?, ?)",
            (transaction_id, txid, now),
        )


def read_transaction_txid_overrides(conn: sqlite3.Connection) -> dict[int, str]:
    """Return all user-supplied txid overrides as a ``{transaction_id: txid}`` map.

    Returns an empty dict when no overrides exist or the table is absent
    (pre-v10 batch file opened before migration ran).
    """
    try:
        rows = conn.execute(
            "SELECT transaction_id, txid FROM transaction_txid_overrides"
        ).fetchall()
    except Exception:
        return {}
    return {int(row[0]): str(row[1]) for row in rows}


def delete_transaction_txid_override(conn: sqlite3.Connection, transaction_id: int) -> None:
    """Remove the txid override for *transaction_id*, if one exists."""
    with conn:
        conn.execute(
            "DELETE FROM transaction_txid_overrides WHERE transaction_id = ?",
            (transaction_id,),
        )
