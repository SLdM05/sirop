"""
Read-only audit helper for .sirop SQLite batch files.

Bundled with the /sirop-query skill so Claude (or any developer) can
inspect a batch without re-deriving the schema or writing throwaway SQL.
The file is opened in read-only mode and `PRAGMA query_only=1` is set
defensively — this script can never modify a .sirop file.

Run `poetry run python .claude/scripts/sirop_query.py --help` for usage.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

# Pipeline tables in execution order — used by `status` for rowcount summary.
PIPELINE_TABLES = (
    "raw_transactions",
    "transactions",
    "verified_transactions",
    "classified_events",
    "income_events",
    "dispositions",
    "acb_state",
    "dispositions_adjusted",
)

# Side tables surfaced by `status`.
SIDE_TABLES = (
    "wallets",
    "transfer_overrides",
    "graph_transfer_pairs",
    "transaction_txid_overrides",
    "audit_log",
    "boc_rates",
    "crypto_prices",
    "custom_importers",
)

console = Console()
err_console = Console(stderr=True, style="bold red")


# ── batch resolution ─────────────────────────────────────────────────────────


def resolve_batch_path(args: argparse.Namespace) -> Path:
    """Pick the .sirop file to open from --file / --batch / .active.

    Resolution order:
      1. --file <path>        — explicit path wins
      2. --batch <name>       — combined with --data-dir or $DATA_DIR
      3. {data_dir}/.active   — fall back to the active batch
    """
    if args.file:
        path = Path(args.file).expanduser()
        if not path.exists():
            sys.exit(f"error: --file path does not exist: {path}")
        return path

    data_dir = Path(args.data_dir or os.environ.get("DATA_DIR", "./data")).expanduser()

    if args.batch:
        path = data_dir / f"{args.batch}.sirop"
    else:
        active_file = data_dir / ".active"
        if not active_file.exists():
            sys.exit(
                f"error: no batch specified and {active_file} not found. "
                "Use --batch <name>, --file <path>, or run `sirop switch <name>`."
            )
        name = active_file.read_text(encoding="utf-8").strip()
        if not name:
            sys.exit(f"error: {active_file} is empty.")
        path = data_dir / f"{name}.sirop"

    if not path.exists():
        sys.exit(f"error: batch file does not exist: {path}")
    return path


def open_readonly(path: Path) -> sqlite3.Connection:
    """Open a .sirop file with belt-and-braces read-only guarantees."""
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = 1")
    return conn


# ── output helpers ───────────────────────────────────────────────────────────


def render_rows(
    title: str,
    cursor: sqlite3.Cursor,
    *,
    raw: bool,
    limit: int,
) -> None:
    """Render a result set as a rich Table or as TSV when --raw."""
    columns = [c[0] for c in cursor.description] if cursor.description else []
    rows = cursor.fetchall() if limit == 0 else cursor.fetchmany(limit)

    if raw:
        if columns:
            print("\t".join(columns))
        for row in rows:
            print("\t".join("" if v is None else str(v) for v in row))
        return

    if not columns:
        console.print(f"[dim]{title}: (no columns returned)[/dim]")
        return

    table = Table(title=title, header_style="bold cyan", show_lines=False)
    for col in columns:
        table.add_column(col, overflow="fold")
    for row in rows:
        table.add_row(*("" if v is None else str(v) for v in row))

    if not rows:
        console.print(f"[dim]{title}: no rows[/dim]")
    else:
        console.print(table)
        if limit and cursor.fetchone() is not None:
            console.print(f"[dim](truncated at --limit {limit}; use --limit 0 for all)[/dim]")


def section(text: str) -> None:
    console.rule(f"[bold]{text}[/bold]")


# ── subcommands ──────────────────────────────────────────────────────────────


def cmd_status(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Batch overview: meta, schema version, stage status, rowcounts."""
    section("batch_meta")
    cur = conn.execute("SELECT name, tax_year, created_at, sirop_version FROM batch_meta")
    render_rows("batch_meta", cur, raw=args.raw, limit=args.limit)

    section("schema_version")
    cur = conn.execute("SELECT version, applied_at FROM schema_version")
    render_rows("schema_version", cur, raw=args.raw, limit=args.limit)

    section("stage_status")
    cur = conn.execute("SELECT stage, status, completed_at, error FROM stage_status ORDER BY rowid")
    render_rows("stage_status", cur, raw=args.raw, limit=args.limit)

    section("rowcounts")
    counts: list[tuple[str, int, str]] = []
    for name in PIPELINE_TABLES:
        count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]  # noqa: S608
        counts.append((name, count, "pipeline"))
    for name in SIDE_TABLES:
        count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]  # noqa: S608
        counts.append((name, count, "support"))
    if args.raw:
        print("table\trowcount\tnote")
        for name, count, note in counts:
            print(f"{name}\t{count}\t{note}")
    else:
        table = Table(header_style="bold cyan")
        table.add_column("table")
        table.add_column("rowcount", justify="right")
        table.add_column("note", style="dim")
        for name, count, note in counts:
            table.add_row(name, str(count), note)
        console.print(table)
    return 0


# Tables queried by `trace` when given a txid.  txid is denormalized through
# every pipeline stage, so a single `WHERE txid = ?` per table reconstructs
# the chain reliably even when intermediate FK columns are NULL.
_TRACE_BY_TXID: tuple[tuple[str, str], ...] = (
    (
        "raw_transactions",
        "id, source, raw_timestamp, transaction_type, asset, amount, "
        "amount_currency, fee, fee_currency, cad_amount, txid, wallet_id, notes",
    ),
    (
        "transactions",
        "id, raw_id, timestamp, transaction_type, asset, amount, fee_crypto, "
        "cad_amount, cad_fee, txid, source, is_transfer, counterpart_id, "
        "wallet_id, notes",
    ),
    (
        "verified_transactions",
        "id, tx_id, timestamp, transaction_type, asset, amount, fee_crypto, "
        "cad_amount, cad_fee, txid, source, is_transfer, counterpart_id, "
        "node_verified, block_height, confirmations, wallet_id",
    ),
    (
        "classified_events",
        "id, vtx_id, timestamp, event_type, asset, amount, cad_proceeds, "
        "cad_cost, cad_fee, txid, source, is_taxable, wallet_id, is_provisional",
    ),
)


def _trace_by_txid(conn: sqlite3.Connection, txid: str, raw: bool) -> int:
    for table, columns in _TRACE_BY_TXID:
        section(f"{table} (txid = {txid})")
        rows = conn.execute(
            f"SELECT {columns} FROM {table} WHERE txid = ? ORDER BY id",  # noqa: S608
            (txid,),
        ).fetchall()
        render_table_from_rows(table, rows, raw=raw)
    # dispositions / dispositions_adjusted have no txid column — reach them
    # via classified_events.id we just printed.
    ce_ids = [
        r["id"]
        for r in conn.execute("SELECT id FROM classified_events WHERE txid = ?", (txid,)).fetchall()
    ]
    _trace_dispositions_for(conn, ce_ids, raw=raw)
    return 0


def _trace_dispositions_for(
    conn: sqlite3.Connection, classified_ids: list[int], *, raw: bool
) -> None:
    if not classified_ids:
        section("dispositions / dispositions_adjusted")
        render_table_from_rows("dispositions", [], raw=raw)
        return
    placeholders = ",".join(["?"] * len(classified_ids))
    section("dispositions")
    rows = conn.execute(
        f"SELECT id, event_id, timestamp, asset, units, proceeds, "  # noqa: S608
        f"acb_of_disposed, selling_fees, gain_loss, disposition_type, "
        f"year_acquired, acb_per_unit_before, pool_units_before, "
        f"pool_cost_before, acb_per_unit_after, pool_units_after, pool_cost_after "
        f"FROM dispositions WHERE event_id IN ({placeholders}) ORDER BY id",
        classified_ids,
    ).fetchall()
    render_table_from_rows("dispositions", rows, raw=raw)
    disp_ids = [r["id"] for r in rows]
    section("dispositions_adjusted")
    if not disp_ids:
        render_table_from_rows("dispositions_adjusted", [], raw=raw)
        return
    placeholders = ",".join(["?"] * len(disp_ids))
    rows = conn.execute(
        f"SELECT id, disposition_id, timestamp, asset, units, proceeds, "  # noqa: S608
        f"acb_of_disposed, selling_fees, gain_loss, is_superficial_loss, "
        f"superficial_loss_denied, allowable_loss, adjusted_gain_loss, "
        f"adjusted_acb_of_repurchase, disposition_type, year_acquired "
        f"FROM dispositions_adjusted WHERE disposition_id IN ({placeholders}) ORDER BY id",
        disp_ids,
    ).fetchall()
    render_table_from_rows("dispositions_adjusted", rows, raw=raw)


def _trace_by_transaction_id(conn: sqlite3.Connection, tx_id: int, raw: bool) -> int:
    """Numeric trace: ident is `transactions.id` (the ID surfaced by `stir`)."""
    section(f"transactions (id = {tx_id})")
    tx_rows = conn.execute(
        "SELECT id, raw_id, timestamp, transaction_type, asset, amount, "
        "fee_crypto, cad_amount, cad_fee, txid, source, is_transfer, "
        "counterpart_id, wallet_id, notes "
        "FROM transactions WHERE id = ?",
        (tx_id,),
    ).fetchall()
    render_table_from_rows("transactions", tx_rows, raw=raw)
    if not tx_rows:
        return 0

    section("verified_transactions")
    vtx_rows = conn.execute(
        "SELECT id, tx_id, timestamp, transaction_type, asset, amount, "
        "fee_crypto, cad_amount, cad_fee, txid, source, is_transfer, "
        "counterpart_id, node_verified, block_height, confirmations, wallet_id "
        "FROM verified_transactions WHERE tx_id = ?",
        (tx_id,),
    ).fetchall()
    render_table_from_rows("verified_transactions", vtx_rows, raw=raw)
    vtx_ids = [r["id"] for r in vtx_rows]
    if not vtx_ids:
        return 0

    placeholders = ",".join(["?"] * len(vtx_ids))
    section("classified_events")
    ce_rows = conn.execute(
        f"SELECT id, vtx_id, timestamp, event_type, asset, amount, "  # noqa: S608
        f"cad_proceeds, cad_cost, cad_fee, txid, source, is_taxable, "
        f"wallet_id, is_provisional "
        f"FROM classified_events WHERE vtx_id IN ({placeholders}) ORDER BY id",
        vtx_ids,
    ).fetchall()
    render_table_from_rows("classified_events", ce_rows, raw=raw)
    _trace_dispositions_for(conn, [r["id"] for r in ce_rows], raw=raw)
    return 0


def cmd_trace(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Walk pipeline tables for a single txid or transactions.id."""
    ident: str = args.ident
    if ident.isdigit():
        return _trace_by_transaction_id(conn, int(ident), raw=args.raw)
    return _trace_by_txid(conn, ident, raw=args.raw)


def render_table_from_rows(
    title: str,
    rows: list[sqlite3.Row],
    *,
    raw: bool,
) -> None:
    """Render an already-fetched list[Row] (no truncation — caller owns it)."""
    if not rows:
        if not raw:
            console.print(f"[dim]{title}: no rows[/dim]")
        return
    columns = list(rows[0].keys())
    if raw:
        print("\t".join(columns))
        for row in rows:
            print("\t".join("" if v is None else str(v) for v in row))
        return
    table = Table(title=title, header_style="bold cyan", show_lines=False)
    for col in columns:
        table.add_column(col, overflow="fold")
    for row in rows:
        table.add_row(*("" if v is None else str(v) for v in row))
    console.print(table)


def cmd_acb(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Chronological pool snapshots for an asset, joined to triggering event."""
    cur = conn.execute(
        """
        SELECT s.id           AS snap_id,
               s.snapshot_date,
               s.units        AS pool_units,
               s.pool_cost,
               ce.id          AS event_id,
               ce.event_type,
               ce.amount      AS event_amount,
               ce.cad_proceeds,
               ce.cad_cost,
               ce.cad_fee,
               ce.txid,
               ce.source,
               ce.wallet_id
        FROM acb_state s
        LEFT JOIN classified_events ce ON ce.id = s.event_id
        WHERE s.asset = ?
        ORDER BY s.snapshot_date, s.id
        """,
        (args.asset,),
    )
    render_rows(f"acb_state for {args.asset}", cur, raw=args.raw, limit=args.limit)
    return 0


def cmd_dispositions(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Side-by-side raw disposition vs superficial-loss-adjusted row."""
    cur = conn.execute(
        """
        SELECT d.id              AS disp_id,
               d.timestamp,
               d.asset,
               d.units,
               d.proceeds,
               d.acb_of_disposed,
               d.selling_fees,
               d.gain_loss       AS raw_gain_loss,
               a.is_superficial_loss,
               a.superficial_loss_denied,
               a.allowable_loss,
               a.adjusted_gain_loss,
               a.adjusted_acb_of_repurchase,
               d.disposition_type,
               d.year_acquired
        FROM dispositions d
        LEFT JOIN dispositions_adjusted a ON a.disposition_id = d.id
        WHERE d.asset = ?
        ORDER BY d.timestamp, d.id
        """,
        (args.asset,),
    )
    render_rows(f"dispositions for {args.asset}", cur, raw=args.raw, limit=args.limit)
    return 0


def cmd_wallets(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Wallet registry plus per-wallet event counts grouped by event_type."""
    section("wallets")
    cur = conn.execute(
        "SELECT id, name, source, auto_created, created_at, note " "FROM wallets ORDER BY id"
    )
    render_rows("wallets", cur, raw=args.raw, limit=args.limit)

    section("event counts per wallet x event_type (taxable only)")
    cur = conn.execute(
        """
        SELECT COALESCE(w.name, '(none)') AS wallet,
               ce.event_type,
               ce.asset,
               COUNT(*) AS n,
               SUM(CAST(ce.amount AS REAL)) AS sum_amount_lossy
        FROM classified_events ce
        LEFT JOIN wallets w ON w.id = ce.wallet_id
        WHERE ce.is_taxable = 1
        GROUP BY w.name, ce.event_type, ce.asset
        ORDER BY wallet, ce.asset, ce.event_type
        """
    )
    render_rows("wallet x event_type counts", cur, raw=args.raw, limit=args.limit)
    return 0


def cmd_transfers(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """User-set transfer overrides plus auto-detected on-chain graph pairs."""
    section("transfer_overrides (user)")
    cur = conn.execute(
        "SELECT id, tx_id_a, tx_id_b, action, implied_fee_crypto, "
        "external_wallet, created_at, note "
        "FROM transfer_overrides ORDER BY created_at, id"
    )
    render_rows("transfer_overrides", cur, raw=args.raw, limit=args.limit)

    section("graph_transfer_pairs (auto, BTC)")
    cur = conn.execute(
        "SELECT withdrawal_id, deposit_id, hops, direction, fee_crypto, "
        "deposit_vout_count, deposit_vin_count "
        "FROM graph_transfer_pairs ORDER BY withdrawal_id"
    )
    render_rows("graph_transfer_pairs", cur, raw=args.raw, limit=args.limit)
    return 0


def cmd_events(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Filtered classified_events listing."""
    where: list[str] = []
    params: list[object] = []
    if args.txid:
        where.append("txid = ?")
        params.append(args.txid)
    if args.asset:
        where.append("asset = ?")
        params.append(args.asset)
    if args.type:
        where.append("event_type = ?")
        params.append(args.type)
    if args.wallet is not None:
        where.append("wallet_id = ?")
        params.append(args.wallet)
    if args.taxable_only:
        where.append("is_taxable = 1")
    sql = (
        "SELECT id, vtx_id, timestamp, event_type, asset, amount, "
        "cad_proceeds, cad_cost, cad_fee, txid, source, is_taxable, "
        "wallet_id, is_provisional FROM classified_events"
    )
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY timestamp, id"
    cur = conn.execute(sql, params)
    render_rows("classified_events", cur, raw=args.raw, limit=args.limit)
    return 0


def cmd_sql(conn: sqlite3.Connection, args: argparse.Namespace) -> int:
    """Free-form read-only query. PRAGMA query_only=1 blocks any writes."""
    try:
        cur = conn.execute(args.query)
    except sqlite3.OperationalError as exc:
        err_console.print(f"sqlite error: {exc}")
        return 2
    render_rows("query result", cur, raw=args.raw, limit=args.limit)
    return 0


# ── argparse wiring ──────────────────────────────────────────────────────────


def add_common_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument("--batch", help="batch name (resolves to <data-dir>/<name>.sirop)")
    p.add_argument("--file", help="explicit path to a .sirop file")
    p.add_argument("--data-dir", help="override DATA_DIR (env var default: ./data)")
    p.add_argument(
        "--limit",
        type=int,
        default=50,
        help="max rows to display (default 50; use 0 for unlimited)",
    )
    p.add_argument("--raw", action="store_true", help="emit TSV instead of a rich table")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sirop_query",
        description="Read-only audit helper for .sirop SQLite batch files.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("status", help="batch metadata, stage status, rowcounts")
    add_common_flags(p)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("trace", help="walk raw → adjusted for a txid or raw_id")
    p.add_argument("ident", help="txid (hex) or numeric raw_transactions.id")
    add_common_flags(p)
    p.set_defaults(func=cmd_trace)

    p = sub.add_parser("acb", help="chronological ACB pool snapshots for an asset")
    p.add_argument("asset", help="asset symbol, e.g. BTC")
    add_common_flags(p)
    p.set_defaults(func=cmd_acb)

    p = sub.add_parser(
        "dispositions",
        help="raw vs adjusted disposition rows (superficial-loss diff)",
    )
    p.add_argument("asset", help="asset symbol, e.g. BTC")
    add_common_flags(p)
    p.set_defaults(func=cmd_dispositions)

    p = sub.add_parser("wallets", help="wallet registry plus per-wallet event counts")
    add_common_flags(p)
    p.set_defaults(func=cmd_wallets)

    p = sub.add_parser("transfers", help="transfer_overrides + graph_transfer_pairs")
    add_common_flags(p)
    p.set_defaults(func=cmd_transfers)

    p = sub.add_parser("events", help="filtered classified_events listing")
    p.add_argument("--txid")
    p.add_argument("--asset")
    p.add_argument("--type", help="event_type (buy/sell/fee_disposal/...)")
    p.add_argument("--wallet", type=int, help="wallet_id")
    p.add_argument("--taxable-only", action="store_true")
    add_common_flags(p)
    p.set_defaults(func=cmd_events)

    p = sub.add_parser("sql", help="free-form read-only SELECT")
    p.add_argument("query", help="a single SQL statement (read-only)")
    add_common_flags(p)
    p.set_defaults(func=cmd_sql)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    path = resolve_batch_path(args)
    if not args.raw:
        console.print(f"[dim]batch: {path}[/dim]")
    conn = open_readonly(path)
    try:
        rc: int = args.func(conn, args)
        return rc
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
