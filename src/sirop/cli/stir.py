"""Handler for ``sirop stir``.

Lets the user review imported transactions, inspect auto-detected transfer
pairs, and manually link or unlink transaction pairs before running ``boil``.

Design
------
``stir`` reads the ``transactions`` table (requires normalize to have been run)
and runs the transfer matcher in-memory to preview what would be auto-matched.
It reads existing overrides from ``transfer_overrides`` and displays them
alongside auto-matches.

The user can then:

- **transfer <id>** — guided wizard: pick the counterpart transaction (tracked
  wallet with CSV) or mark the movement as going to/from an untracked wallet
  (no CSV available).  For untracked wallets the user can optionally record
  a network fee so boil emits a fee micro-disposal.  Validates that no asset
  units are created from nothing.
- **link <id1> <id2>** — force-link two transactions as a transfer pair.
  Validates amounts: received must not exceed sent.  The exact difference is
  stored as ``implied_fee_crypto`` and becomes a fee micro-disposition at
  ``boil`` time.
- **unlink <id1> <id2>** — prevent two auto-matched transactions from being
  paired. Both legs will be treated as taxable events (sell/buy).
- **clear <id>** — remove all overrides for a transaction.

Asset-agnostic
--------------
All validation is performed on the ``asset`` field of each transaction.  The
wizard works for BTC, ETH, or any other crypto asset — sirop does not hard-code
any asset name.

Invariant
---------
When two transactions are linked, ``received ≤ sent`` must hold (for the same
asset).  The difference is the on-chain network fee.  The tool never accepts a
link where the receiving side shows more units than were sent — that would create
units from nothing.

Modes
-----
Interactive (default)
    Running ``sirop stir`` with no flags launches an interactive prompt.

Non-interactive flags
    ``--list``             Show current state and exit (no prompt).
    ``--link <id1> <id2>``  Force link two transactions by ``transactions.id``.
    ``--unlink <id1> <id2>`` Force unlink two transactions by ``transactions.id``.
    ``--clear <id>``       Clear all overrides for a transaction.
"""

from __future__ import annotations

import dataclasses
import re
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from rich.table import Table
from rich.text import Text

from sirop.config.settings import Settings, get_settings
from sirop.db import repositories as repo
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.models.enums import TransactionType
from sirop.models.messages import MessageCode
from sirop.transfer_match.matcher import (
    _INCOMING,
    _OUTGOING,
    FEE_TOLERANCE_ABSOLUTE,
    FEE_TOLERANCE_RELATIVE,
    MATCH_WINDOW_HOURS,
)
from sirop.utils.console import out
from sirop.utils.logging import get_logger
from sirop.utils.messages import emit

if TYPE_CHECKING:
    import sqlite3

    from sirop.models.override import TransferOverride
    from sirop.models.transaction import Transaction
    from sirop.models.wallet import Wallet

logger = get_logger(__name__)

# Rich colour per transaction direction — used by _fmt_tx() and _tx_table().
# Minimum input or output count to flag a graph match as a possible CoinJoin.
_COINJOIN_TX_THRESHOLD = 5
# Minimum output count on the deposit tx to suspect a purchase+change scenario.
_PURCHASE_CHANGE_VOUT_MIN = 2

_TYPE_STYLE: dict[TransactionType, str] = {
    TransactionType.WITHDRAWAL: "red",
    TransactionType.SELL: "red",
    TransactionType.SPEND: "red",
    TransactionType.DEPOSIT: "green",
    TransactionType.BUY: "green",
    TransactionType.TRADE: "cyan",
}

# Transaction types that need transfer-match resolution when unmatched.
# Only WITHDRAWAL and DEPOSIT are auto-match candidates; everything else (BUY, SELL, TRADE,
# INCOME, SPEND, …) is already definitively classified by the importer and goes to "other".
_NEEDS_MATCH_OUT = frozenset({TransactionType.WITHDRAWAL})
_NEEDS_MATCH_IN = frozenset({TransactionType.DEPOSIT})
# Expected part count for two-argument commands (cmd + id1 + id2).
_TWO_ARG_PARTS = 3
# Expected part count for one-argument commands (cmd + id).
_ONE_ARG_PARTS = 2

# Wallet names: alphanumeric, hyphens, underscores; 1-64 chars; starts with alnum.
# Same character-set rules as batch names (security-input-hardening.md §2).
_WALLET_NAME_RE: re.Pattern[str] = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


def _validate_wallet_name(name: str) -> str | None:
    """Return an error message if *name* is not a valid wallet identifier, else ``None``.

    An empty string is accepted (wallet name is optional -- the user may leave it blank).
    Non-empty names must be 1-64 characters, start with a letter or digit, and contain
    only letters, digits, hyphens, and underscores.  Spaces and special characters are
    rejected to prevent display corruption and to satisfy the identifier-hardening policy.
    """
    if not name:
        return None
    if not _WALLET_NAME_RE.fullmatch(name):
        return (
            "Wallet name must be 1-64 characters, start with a letter or digit, "
            "and contain only letters, digits, hyphens (-), and underscores (_). "
            "No spaces or special characters."
        )
    return None


class _StirError(Exception):
    """Sentinel — raised to exit early; caught by handle_stir."""

    def __init__(self, code: MessageCode, **kwargs: object) -> None:
        self.msg_code = code
        self.msg_kwargs = kwargs
        super().__init__(str(code))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def handle_stir(
    list_only: bool = False,
    link: tuple[int, int] | None = None,
    unlink: tuple[int, int] | None = None,
    clear: int | None = None,
    settings: Settings | None = None,
) -> int:
    """Review and edit transfer pair assignments for the active batch.

    Parameters
    ----------
    list_only:
        Print the current state and exit without entering the interactive loop.
    link:
        If given, force-link the two transaction IDs and exit.
    unlink:
        If given, force-unlink the two transaction IDs and exit.
    clear:
        If given, clear all overrides for this transaction ID and exit.
    settings:
        Application settings; resolved from the environment if omitted.
    """
    if settings is None:
        settings = get_settings()
    try:
        return _run_stir(
            list_only=list_only, link=link, unlink=unlink, clear=clear, settings=settings
        )
    except _StirError as exc:
        emit(exc.msg_code, **exc.msg_kwargs)
        return 1


# ---------------------------------------------------------------------------
# Core implementation
# ---------------------------------------------------------------------------


def _run_stir(
    *,
    list_only: bool,
    link: tuple[int, int] | None,
    unlink: tuple[int, int] | None,
    clear: int | None,
    settings: Settings,
) -> int:
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _StirError(MessageCode.BATCH_ERROR_NO_ACTIVE)

    conn = open_batch(batch_name, settings)
    try:
        normalize_status = repo.get_stage_status(conn, "normalize")
        if normalize_status != "done":
            raise _StirError(MessageCode.STIR_ERROR_NOT_NORMALIZED, name=batch_name)

        # Non-interactive flag-based modes.
        if link is not None:
            return _apply_link(conn, link[0], link[1])
        if unlink is not None:
            return _apply_unlink(conn, unlink[0], unlink[1])
        if clear is not None:
            return _apply_clear(conn, clear)

        # Display mode — shared by --list and interactive.
        tax_year = repo.read_tax_year(conn)
        txs = repo.read_transactions(conn)
        # Apply user-supplied destination txid overrides so Pass 1 exact matching
        # in _build_state finds the same pairs that boil's match_transfers would find.
        txs = _apply_txid_overrides(conn, txs)
        wallets = repo.read_wallets(conn)
        overrides = repo.read_transfer_overrides(conn)

        # Load graph-traversal pairs from the last boil run (empty if not yet run).
        graph_pairs = _load_graph_pairs(conn, txs)
        state = _build_state(txs, overrides, graph_pairs=graph_pairs)
        if list_only:
            _print_state(batch_name, txs, state, overrides, wallets)
            return 0

        _print_unmatched(batch_name, txs, state, wallets, tax_year)
        return _interactive_loop(conn, txs, wallets, overrides, state, batch_name, tax_year)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Amount validation helpers
# ---------------------------------------------------------------------------


def _parse_fee_amount(raw: str, asset: str, tx_amount: Decimal) -> tuple[Decimal, str]:
    """Parse a user-supplied fee string.  Return ``(fee, error)``.

    *error* is an empty string on success, a human-readable message on failure.
    The fee must be a positive number strictly less than *tx_amount*.
    """
    try:
        fee = Decimal(raw)
    except Exception:
        return Decimal("0"), "Fee must be a number."
    if fee <= Decimal("0"):
        return Decimal("0"), "Fee must be greater than zero."
    if fee >= tx_amount:
        return Decimal("0"), (
            f"Fee ({fee:.8f} {asset}) cannot equal or exceed "
            f"the transaction amount ({tx_amount:.8f} {asset})."
        )
    return fee, ""


def _compute_implied_fee(
    tx_a: Transaction,
    tx_b: Transaction,
) -> tuple[Decimal, str]:
    """Return (implied_fee, error_message) for a forced link.

    The implied fee is ``sent_amount - received_amount`` for the asset.
    An error message (non-empty string) is returned when the link is impossible.

    Rules (asset-agnostic):
    - Both transactions must involve the same asset.
    - If one is outgoing and the other incoming: fee = sent - received.
      - received > sent → impossible (creates units from nothing).
      - received ≤ sent → fee is the network cost; may be zero.
    - If both are the same direction (unusual forced link): fee = 0, no check.
    """
    if tx_a.asset != tx_b.asset:
        return Decimal("0"), (
            f"Asset mismatch: id:{tx_a.id} is {tx_a.asset} but id:{tx_b.id} is {tx_b.asset}. "
            "Cannot link transactions of different assets."
        )

    a_out = tx_a.tx_type in _OUTGOING
    b_in = tx_b.tx_type in _INCOMING
    a_in = tx_a.tx_type in _INCOMING
    b_out = tx_b.tx_type in _OUTGOING

    if a_out and b_in:
        sent, received = tx_a.amount, tx_b.amount
    elif a_in and b_out:
        sent, received = tx_b.amount, tx_a.amount
    else:
        # Same direction (e.g. two buys) — allow the link, no fee logic.
        return Decimal("0"), ""

    if received > sent:
        excess = received - sent
        return Decimal("0"), (
            f"Received {received:.8f} {tx_a.asset} (id:{tx_b.id if a_out else tx_a.id}) "
            f"exceeds sent {sent:.8f} {tx_a.asset} (id:{tx_a.id if a_out else tx_b.id}) "
            f"by {excess:.8f} {tx_a.asset}.\n"
            f"  This would create {excess:.8f} {tx_a.asset} from nothing — impossible.\n"
            f"  Check that you have selected the correct transactions."
        )

    return sent - received, ""


# ---------------------------------------------------------------------------
# Non-interactive flag handlers
# ---------------------------------------------------------------------------


def _apply_link(conn: sqlite3.Connection, id_a: int, id_b: int) -> int:
    """Force-link two transactions.  Validates amounts, computes implied fee."""
    _assert_tx_exists(conn, id_a)
    _assert_tx_exists(conn, id_b)

    tx_a = _load_tx(conn, id_a)
    tx_b = _load_tx(conn, id_b)
    implied_fee, err = _compute_implied_fee(tx_a, tx_b)
    if err:
        out.print(f"  Error: {err}")
        return 1

    _remove_conflicting_override(conn, id_a, id_b, "unlink")
    override = repo.write_transfer_override(
        conn, id_a, id_b, "link", implied_fee_crypto=implied_fee
    )
    if implied_fee > Decimal("0"):
        out.print(
            f"  Implied fee: {implied_fee:.8f} {tx_a.asset} "
            f"(will become a fee micro-disposal at boil time)"
        )
    emit(MessageCode.STIR_LINK_APPLIED, id_a=id_a, id_b=id_b, ov_id=override.id)
    return 0


def _apply_unlink(conn: sqlite3.Connection, id_a: int, id_b: int) -> int:
    """Force-unlink two transactions. Removes any conflicting link override."""
    _assert_tx_exists(conn, id_a)
    _assert_tx_exists(conn, id_b)
    _remove_conflicting_override(conn, id_a, id_b, "link")
    override = repo.write_transfer_override(conn, id_a, id_b, "unlink")
    emit(MessageCode.STIR_UNLINK_APPLIED, id_a=id_a, id_b=id_b, ov_id=override.id)
    return 0


def _apply_clear(conn: sqlite3.Connection, tx_id: int) -> int:
    """Remove all overrides involving a transaction."""
    _assert_tx_exists(conn, tx_id)
    count = repo.clear_transfer_overrides_for_tx(conn, tx_id)
    emit(MessageCode.STIR_CLEAR_APPLIED, tx_id=tx_id, count=count)
    return 0


def _assert_tx_exists(conn: sqlite3.Connection, tx_id: int) -> None:
    """Raise _StirError if the transaction ID does not exist."""
    row = conn.execute("SELECT id FROM transactions WHERE id = ?", (tx_id,)).fetchone()
    if row is None:
        raise _StirError(MessageCode.STIR_ERROR_TX_NOT_FOUND, tx_id=tx_id)


def _load_tx(conn: sqlite3.Connection, tx_id: int) -> Transaction:
    """Load a single Transaction by id (must exist)."""
    txs = repo.read_transactions(conn)
    for tx in txs:
        if tx.id == tx_id:
            return tx
    raise _StirError(MessageCode.STIR_ERROR_TX_NOT_FOUND, tx_id=tx_id)


def _remove_conflicting_override(
    conn: sqlite3.Connection,
    id_a: int,
    id_b: int,
    conflicting_action: str,
) -> None:
    """Delete any existing override for this pair with *conflicting_action*."""
    rows = conn.execute(
        """
        SELECT id FROM transfer_overrides
        WHERE action = ?
          AND ((tx_id_a = ? AND tx_id_b = ?) OR (tx_id_a = ? AND tx_id_b = ?))
        """,
        (conflicting_action, id_a, id_b, id_b, id_a),
    ).fetchall()
    for row in rows:
        repo.delete_transfer_override(conn, row["id"])


# ---------------------------------------------------------------------------
# State computation — dry-run preview of auto-matching
# ---------------------------------------------------------------------------


class _MatchState:
    """Result of a dry-run transfer-match preview."""

    def __init__(self) -> None:
        # auto_pairs: list of (outgoing_tx, incoming_tx, reason_str)
        self.auto_pairs: list[tuple[Transaction, Transaction, str]] = []
        # graph_pairs: pairs found by on-chain graph traversal (from last boil run).
        # Each entry holds withdrawal_tx, deposit_tx, hops, direction, fee_crypto,
        # deposit_vout_count, and deposit_vin_count.
        self.graph_pairs: list[tuple[Transaction, Transaction, int, str, Decimal, int, int]] = []
        # forced_link_pairs: list of (tx_a, tx_b) from overrides
        self.forced_link_pairs: list[tuple[Transaction, Transaction, Decimal]] = []
        # forced_unlink_pairs: pairs blocked by 'unlink' overrides
        self.forced_unlink_pairs: list[tuple[Transaction, Transaction]] = []
        # external_markers: txs marked as external-out or external-in
        self.external_markers: list[
            tuple[Transaction, str, str, Decimal]
        ] = []  # (tx, action, wallet, fee)
        # unmatched_out: outgoing txs that had no match
        self.unmatched_out: list[Transaction] = []
        # unmatched_in: incoming txs that had no match
        self.unmatched_in: list[Transaction] = []
        # other: remaining txs (buys, sells, income, fiat, …)
        self.other: list[Transaction] = []


def _build_override_sets(
    overrides: list[TransferOverride],
) -> tuple[set[tuple[int, int]], set[tuple[int, int]], set[int]]:
    """Return (forced_link_ids, forced_unlink_ids, external_ids) as sets."""
    forced_link_ids: set[tuple[int, int]] = set()
    forced_unlink_ids: set[tuple[int, int]] = set()
    external_ids: set[int] = set()
    for ov in overrides:
        if ov.action == "link" and ov.tx_id_b is not None:
            pair = (ov.tx_id_a, ov.tx_id_b)
            pair_rev = (ov.tx_id_b, ov.tx_id_a)
            forced_link_ids.add(pair)
            forced_link_ids.add(pair_rev)
        elif ov.action == "unlink" and ov.tx_id_b is not None:
            pair = (ov.tx_id_a, ov.tx_id_b)
            pair_rev = (ov.tx_id_b, ov.tx_id_a)
            forced_unlink_ids.add(pair)
            forced_unlink_ids.add(pair_rev)
        elif ov.action in ("external-out", "external-in"):
            external_ids.add(ov.tx_id_a)
    return forced_link_ids, forced_unlink_ids, external_ids


def _apply_forced_links(
    overrides: list[TransferOverride],
    tx_by_id: dict[int, Transaction],
    paired_ids: set[int],
    state: _MatchState,
) -> None:
    """Populate *state.forced_link_pairs* from ``action='link'`` overrides."""
    for ov in overrides:
        if ov.action != "link" or ov.tx_id_b is None:
            continue
        tx_a = tx_by_id.get(ov.tx_id_a)
        tx_b = tx_by_id.get(ov.tx_id_b)
        if tx_a is None or tx_b is None:
            continue
        if tx_a.id in paired_ids or tx_b.id in paired_ids:
            continue
        paired_ids.add(tx_a.id)
        paired_ids.add(tx_b.id)
        state.forced_link_pairs.append((tx_a, tx_b, ov.implied_fee_crypto))


def _apply_txid_overrides(
    conn: sqlite3.Connection,
    txs: list[Transaction],
) -> list[Transaction]:
    """Patch in-memory txids from user-supplied destination overrides.

    Mirrors the same logic in ``boil._run_transfer_match`` so stir's
    ``_build_state`` Pass 1 can find the same txid-based matches that
    boil's ``match_transfers`` would find.  Only patches transactions whose
    ``txid`` is currently ``None`` — existing CSV txids are never overwritten.
    """
    overrides = repo.read_transaction_txid_overrides(conn)
    if not overrides:
        return txs
    return [
        dataclasses.replace(t, txid=overrides[t.id]) if t.id in overrides and t.txid is None else t
        for t in txs
    ]


def _load_graph_pairs(
    conn: sqlite3.Connection,
    txs: list[Transaction],
) -> list[tuple[Transaction, Transaction, int, str, Decimal, int, int]]:
    """Read graph-traversal matched pairs from DB and resolve to Transaction objects."""
    tx_by_id = {t.id: t for t in txs}
    return [
        (
            tx_by_id[gm.withdrawal_db_id],
            tx_by_id[gm.deposit_db_id],
            gm.hops,
            gm.direction,
            gm.fee_crypto,
            gm.deposit_vout_count,
            gm.deposit_vin_count,
        )
        for gm in repo.read_graph_transfer_pairs(conn)
        if gm.withdrawal_db_id in tx_by_id and gm.deposit_db_id in tx_by_id
    ]


def _build_state(  # noqa: PLR0912 PLR0915
    txs: list[Transaction],
    overrides: list[TransferOverride],
    graph_pairs: list[tuple[Transaction, Transaction, int, str, Decimal, int, int]] | None = None,
) -> _MatchState:
    """Compute a preview of how the transfer matcher will classify *txs*.

    *graph_pairs* are pairs already matched by on-chain graph traversal during
    the last ``boil`` run.  They are applied before Pass 1 so that these
    transactions are not listed as unmatched withdrawals/deposits.
    """
    state = _MatchState()
    tx_by_id: dict[int, Transaction] = {t.id: t for t in txs}
    sorted_txs = sorted(txs, key=lambda t: t.timestamp)

    forced_link_ids, forced_unlink_ids, external_ids = _build_override_sets(overrides)
    paired_ids: set[int] = set()

    # Pre-pass: mark graph-traversal-matched pairs as paired so Pass 1 skips them.
    for w_tx, d_tx, hops, direction, fee, vout_count, vin_count in graph_pairs or []:
        if w_tx.id in paired_ids or d_tx.id in paired_ids:
            continue
        paired_ids.add(w_tx.id)
        paired_ids.add(d_tx.id)
        state.graph_pairs.append((w_tx, d_tx, hops, direction, fee, vout_count, vin_count))

    # Pass 0a: apply external markers.
    ov_by_tx: dict[int, TransferOverride] = {ov.tx_id_a: ov for ov in overrides}
    for ov in overrides:
        if ov.action not in ("external-out", "external-in"):
            continue
        tx = tx_by_id.get(ov.tx_id_a)
        if tx is None or tx.id in paired_ids:
            continue
        paired_ids.add(tx.id)
        state.external_markers.append((tx, ov.action, ov.external_wallet, ov.implied_fee_crypto))

    # Pass 0b: apply forced links.
    _apply_forced_links(overrides, tx_by_id, paired_ids, state)

    # Pass 1: auto-matching (respecting forced unlinks).
    window = timedelta(hours=MATCH_WINDOW_HOURS)
    for tx in sorted_txs:
        if tx.tx_type not in _OUTGOING or tx.id in paired_ids:
            continue

        match: Transaction | None = None
        reason = ""
        for candidate in sorted_txs:
            if candidate.id == tx.id or candidate.id in paired_ids:
                continue
            if candidate.tx_type not in _INCOMING or candidate.asset != tx.asset:
                continue

            pair_key = (tx.id, candidate.id)
            if pair_key in forced_unlink_ids:
                if _would_match(tx, candidate, window):
                    state.forced_unlink_pairs.append((tx, candidate))
                continue

            if tx.txid and candidate.txid and tx.txid == candidate.txid:
                match = candidate
                reason = f"same txid ({tx.txid[:12]}...)"
                break

            if not (tx.timestamp - window <= candidate.timestamp <= tx.timestamp + window):
                continue
            expected = tx.amount - (tx.fee_crypto or Decimal("0"))
            tol = max(expected * FEE_TOLERANCE_RELATIVE, FEE_TOLERANCE_ABSOLUTE)
            if abs(candidate.amount - expected) <= tol:
                delta_h = abs((candidate.timestamp - tx.timestamp).total_seconds()) / 3600
                reason = f"amount match + {delta_h:.1f}h apart"
                match = candidate
                break

        if match is not None:
            paired_ids.add(tx.id)
            paired_ids.add(match.id)
            state.auto_pairs.append((tx, match, reason))

    # Remaining unmatched outgoing / incoming / other.
    for tx in sorted_txs:
        if tx.id in paired_ids:
            continue
        if tx.tx_type in _NEEDS_MATCH_OUT:
            state.unmatched_out.append(tx)
        elif tx.tx_type in _NEEDS_MATCH_IN:
            state.unmatched_in.append(tx)
        else:
            state.other.append(tx)

    _ = (forced_link_ids, external_ids, ov_by_tx)  # used above; satisfy static analysis
    return state


def _would_match(tx: Transaction, candidate: Transaction, window: object) -> bool:
    """Quick check: would these two auto-match if not blocked?"""
    w = window if isinstance(window, timedelta) else timedelta(hours=MATCH_WINDOW_HOURS)
    if tx.txid and candidate.txid and tx.txid == candidate.txid:
        return True
    if not (tx.timestamp - w <= candidate.timestamp <= tx.timestamp + w):
        return False
    expected = tx.amount - (tx.fee_crypto or Decimal("0"))
    tol = max(expected * FEE_TOLERANCE_RELATIVE, FEE_TOLERANCE_ABSOLUTE)
    return bool(abs(candidate.amount - expected) <= tol)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def _wallet_label(tx: Transaction, wallets: list[Wallet]) -> str:
    """Return a short wallet label for display."""
    if tx.wallet_id is not None:
        for w in wallets:
            if w.id == tx.wallet_id:
                tag = "" if w.auto_created else "*"
                return f"{w.name}{tag}"
    return tx.source


def _fmt_tx(tx: Transaction, wallets: list[Wallet] | None = None) -> Text:
    """One-line Rich Text summary of a transaction."""
    t = Text()
    t.append(f"id:{tx.id:<5}", style="bold cyan")
    t.append(f"  {tx.timestamp.strftime('%Y-%m-%d %H:%M:%S')}  ")
    wallet_str = _wallet_label(tx, wallets or [])
    t.append(f"{wallet_str:<14}  ", style="dim")
    t.append(f"{tx.tx_type.value:<14}", style=_TYPE_STYLE.get(tx.tx_type, ""))
    t.append(f"  {tx.amount:>14.8f} {tx.asset}", style="bold")
    t.append(f"  CAD {tx.cad_value:>10,.2f}", style="yellow")
    if tx.notes:
        t.append(f"  [{tx.notes}]", style="dim italic")
    return t


def _print_pair(
    tx_a: Transaction,
    tx_b: Transaction,
    wallets: list[Wallet],
    connector: str = "↔",
) -> None:
    line_a = Text("  ")
    line_a.append_text(_fmt_tx(tx_a, wallets))
    out.print(line_a)
    line_b = Text(f"    {connector}  ")
    line_b.append_text(_fmt_tx(tx_b, wallets))
    out.print(line_b)
    out.print()


def _tx_table(txs: list[Transaction], wallets: list[Wallet]) -> Table:
    """Build a Rich Table for a flat list of transactions."""
    table = Table(box=None, show_header=True, header_style="bold dim", pad_edge=False)
    table.add_column("ID", style="bold cyan", no_wrap=True)
    table.add_column("Date", no_wrap=True)
    table.add_column("Wallet", style="dim")
    table.add_column("Type", no_wrap=True)
    table.add_column("Amount", justify="right", style="bold", no_wrap=True)
    table.add_column("Asset")
    table.add_column("CAD Value", justify="right", style="yellow", no_wrap=True)
    table.add_column("Label", style="dim italic")
    for tx in txs:
        wallet_str = _wallet_label(tx, wallets)
        table.add_row(
            f"id:{tx.id}",
            tx.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            wallet_str,
            Text(tx.tx_type.value, style=_TYPE_STYLE.get(tx.tx_type, "")),
            f"{tx.amount:.8f}",
            tx.asset,
            f"CAD {tx.cad_value:>10,.2f}",
            tx.notes,
        )
    return table


def _print_state(  # noqa: PLR0912 PLR0915
    batch_name: str,
    txs: list[Transaction],
    state: _MatchState,
    overrides: list[TransferOverride],
    wallets: list[Wallet] | None = None,
) -> None:
    _wallets = wallets or []

    with out.pager(styles=True):
        out.print("[dim]  ↑↓ arrows · PgUp/PgDn page · /search · q exit[/dim]")
        out.print()
        out.print(
            f"Stirring batch: [bold]{batch_name}[/bold]  "
            f"([dim]{len(txs)} transactions loaded[/dim])"
        )
        out.print()

        # Auto-matched pairs.
        out.rule(f"Auto-matched transfer pairs ({len(state.auto_pairs)})", style="dim")
        if state.auto_pairs:
            for out_tx, in_tx, reason in state.auto_pairs:
                _print_pair(out_tx, in_tx, _wallets)
                out.print(f"       [dim]Reason: {reason}[/dim]")
                _fee, _ = _compute_implied_fee(out_tx, in_tx)
                if _fee > Decimal("0"):
                    out.print(f"       [dim]Fee disposal: {_fee:.8f} {out_tx.asset}[/dim]")
                out.print()
        else:
            out.print("  [dim](none)[/dim]")
        out.print()

        # Graph-traversal-matched pairs (populated after boil has run).
        out.rule(f"Graph-matched transfer pairs ({len(state.graph_pairs)})", style="dim")
        if state.graph_pairs:
            for w_tx, d_tx, hops, direction, fee, vout_count, vin_count in state.graph_pairs:
                _print_pair(w_tx, d_tx, _wallets)
                hop_word = "hop" if hops == 1 else "hops"
                out.print(
                    f"       [dim]Reason: graph traversal: {hops} {hop_word} ({direction})[/dim]"
                )
                if fee > Decimal("0"):
                    out.print(f"       [dim]Fee disposal: {fee:.8f} {w_tx.asset}[/dim]")

                # Classify match confidence using deposit tx structure.
                ratio = fee / w_tx.amount if w_tx.amount > Decimal("0") else Decimal("0")
                thr = _COINJOIN_TX_THRESHOLD
                is_coinjoin = vin_count >= thr or vout_count >= thr
                min_vout = _PURCHASE_CHANGE_VOUT_MIN
                is_purchase_change = ratio > Decimal("0.30") and vout_count >= min_vout
                if is_coinjoin:
                    out.print(
                        f"       [yellow]⚠ High input/output count"
                        f" ({vin_count} in, {vout_count} out)"
                        f" — possible CoinJoin; verify manually.[/yellow]"
                    )
                elif is_purchase_change:
                    pct = f"{ratio * 100:.0f}%"
                    out.print(
                        f"       [yellow]⚠ Implied fee is {pct} of withdrawal"
                        f" ({vout_count} outputs on deposit tx)"
                        f" — likely a purchase with change UTXO, not a self-transfer."
                        f" Review and unlink if incorrect.[/yellow]"
                    )
                out.print()
        else:
            out.print("  [dim](none — run sirop boil to populate)[/dim]")
        out.print()

        # Forced links.
        out.rule(f"Forced links — stir overrides ({len(state.forced_link_pairs)})", style="dim")
        if state.forced_link_pairs:
            for tx_a, tx_b, implied_fee in state.forced_link_pairs:
                _print_pair(tx_a, tx_b, _wallets)
                if implied_fee > Decimal("0"):
                    out.print(f"       [dim]Implied fee: {implied_fee:.8f} {tx_a.asset}[/dim]")
                    out.print()
        else:
            out.print("  [dim](none)[/dim]")
        out.print()

        # External markers.
        if state.external_markers:
            out.rule(f"External transfers ({len(state.external_markers)})", style="dim")
            for tx, action, ext_wallet, fee in state.external_markers:
                direction = "→ external" if action == "external-out" else "← external"
                wallet_label = f"  [{ext_wallet}]" if ext_wallet else ""
                fee_label = f"  fee: {fee:.8f} {tx.asset}" if fee > Decimal("0") else ""
                row = Text("  ")
                row.append_text(_fmt_tx(tx, _wallets))
                row.append(f"  {direction}{wallet_label}{fee_label}", style="dim")
                out.print(row)
            out.print()

        # Forced unlinks (would-have-matched but blocked).
        if state.forced_unlink_pairs:
            out.rule(
                f"Forced unlinks — blocked auto-matches ({len(state.forced_unlink_pairs)})",
                style="dim",
            )
            for tx_a, tx_b in state.forced_unlink_pairs:
                _print_pair(tx_a, tx_b, _wallets, connector="✗")
            out.print()

        # Unmatched outgoing → sells.
        out.rule(
            f"Unmatched withdrawals — will be treated as sells ({len(state.unmatched_out)})",
            style="red dim",
        )
        if state.unmatched_out:
            out.print(_tx_table(state.unmatched_out, _wallets))
        else:
            out.print("  [dim](none)[/dim]")
        out.print()

        # Unmatched incoming → buys.
        out.rule(
            f"Unmatched deposits — will be treated as buys ({len(state.unmatched_in)})",
            style="green dim",
        )
        if state.unmatched_in:
            out.print(_tx_table(state.unmatched_in, _wallets))
        else:
            out.print("  [dim](none)[/dim]")
        out.print()

        # Other taxable events.
        out.rule(
            f"Other events (buys, sells, income, fiat, …) ({len(state.other)})",
            style="dim",
        )
        if state.other:
            out.print(_tx_table(state.other, _wallets))
        else:
            out.print("  [dim](none)[/dim]")
        out.print()

        # Active overrides summary.
        if overrides:
            out.rule(f"Active overrides ({len(overrides)} total)", style="dim")
            for ov in overrides:
                ts = ov.created_at.strftime("%Y-%m-%d %H:%M:%S")
                note_part = f"  note: {ov.note}" if ov.note else ""
                b_part = f" ↔ {ov.tx_id_b}" if ov.tx_id_b is not None else ""
                fee_part = (
                    f"  fee:{ov.implied_fee_crypto:.8f}"
                    if ov.implied_fee_crypto > Decimal("0")
                    else ""
                )
                ext_part = f"  → {ov.external_wallet}" if ov.external_wallet else ""
                out.print(
                    f"  override id:{ov.id}  action:{ov.action}  "
                    f"tx:{ov.tx_id_a}{b_part}  {ts}{fee_part}{ext_part}{note_part}"
                )
            out.print()


def _print_unmatched(
    batch_name: str,
    txs: list[Transaction],
    state: _MatchState,
    wallets: list[Wallet] | None = None,
    tax_year: int | None = None,
) -> None:
    """Print only unmatched withdrawals and deposits inline (no pager).

    When *tax_year* is supplied, future-year unmatched items are hidden from
    the display — they are silently handled by the boil pipeline.
    """
    _wallets = wallets or []
    resolved = (
        len(state.auto_pairs) * 2
        + len(state.graph_pairs) * 2
        + len(state.forced_link_pairs) * 2
        + len(state.external_markers)
    )
    unmatched_out = [
        t for t in state.unmatched_out if tax_year is None or t.timestamp.year <= tax_year
    ]
    unmatched_in = [
        t for t in state.unmatched_in if tax_year is None or t.timestamp.year <= tax_year
    ]
    out.print()
    out.print(
        f"Batch: [bold]{batch_name}[/bold]  "
        f"[dim]{len(txs)} transactions · "
        f"{resolved} resolved · "
        f"{len(unmatched_out)} unmatched withdrawals · "
        f"{len(unmatched_in)} unmatched deposits[/dim]"
    )
    out.print()
    out.rule(
        f"Unmatched withdrawals — will be treated as sells ({len(unmatched_out)})",
        style="red dim",
    )
    if unmatched_out:
        out.print(_tx_table(unmatched_out, _wallets))
    else:
        out.print("  [dim](none — all withdrawals resolved)[/dim]")
    out.print()
    out.rule(
        f"Unmatched deposits — will be treated as buys ({len(unmatched_in)})",
        style="green dim",
    )
    if unmatched_in:
        out.print(_tx_table(unmatched_in, _wallets))
    else:
        out.print("  [dim](none — all deposits resolved)[/dim]")
    out.print()

    # User overrides — forced links, external markers, forced unlinks.
    has_overrides = state.forced_link_pairs or state.external_markers or state.forced_unlink_pairs
    if has_overrides:
        out.rule("Your overrides", style="dim")

        if state.external_markers:
            out.print(f"  [dim]External transfers ({len(state.external_markers)})[/dim]")
            for tx, action, ext_wallet, fee in state.external_markers:
                direction = "-> external" if action == "external-out" else "<- external"
                wallet_label = f"  [{ext_wallet}]" if ext_wallet else ""
                fee_label = f"  fee: {fee:.8f} {tx.asset}" if fee > Decimal("0") else ""
                row = Text("  ")
                row.append_text(_fmt_tx(tx, _wallets))
                row.append(f"  {direction}{wallet_label}{fee_label}", style="dim")
                out.print(row)
            out.print()

        if state.forced_link_pairs:
            out.print(f"  [dim]Forced links ({len(state.forced_link_pairs)})[/dim]")
            for tx_a, tx_b, implied_fee in state.forced_link_pairs:
                _print_pair(tx_a, tx_b, _wallets)
                if implied_fee > Decimal("0"):
                    out.print(f"       [dim]Implied fee: {implied_fee:.8f} {tx_a.asset}[/dim]")
            out.print()

        if state.forced_unlink_pairs:
            out.print(f"  [dim]Forced unlinks ({len(state.forced_unlink_pairs)})[/dim]")
            for tx_a, tx_b in state.forced_unlink_pairs:
                _print_pair(tx_a, tx_b, _wallets, connector="✗")
            out.print()


def _print_help() -> None:
    out.print()
    out.print("Commands:")
    out.print("  transfer <id>       Link or mark a withdrawal/deposit as a transfer.")
    out.print("                      Tracked wallet (CSV available): select the counterpart tx.")
    out.print("                      Untracked wallet (no CSV): records an optional network fee.")
    out.print(
        "  link <id1> <id2>    Force two transactions to be a transfer pair (validates amounts)."
    )
    out.print("  unlink <id1> <id2>  Prevent two transactions from being auto-paired.")
    out.print("  clear <id>          Remove all stir overrides for a transaction.")
    out.print("  destination <id>    Add the on-chain txid for a BTC transaction whose CSV")
    out.print("                      did not include one (e.g. NDAX withdrawals).")
    out.print("                      After setting, run: sirop boil --from transfer_match")
    out.print("  list                Refresh unmatched transactions.")
    out.print("  view                Open full state in pager (all sections).")
    out.print("  help                Show this help.")
    out.print("  quit                Exit stir (overrides are already saved).")
    out.print()
    out.print("Transaction ids are shown as  [bold cyan]id:NNN[/bold cyan]  in the listing.")
    out.print("* after a wallet name = user-named wallet (vs auto-created from tap format).")
    out.print()


# ---------------------------------------------------------------------------
# Interactive loop — sub-command handlers
# ---------------------------------------------------------------------------


def _cmd_link(  # noqa: PLR0911 PLR0913
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
    wallets: list[Wallet],
    tax_year: int | None = None,
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Handle the ``link`` sub-command. Returns updated (overrides, state) or None on error."""
    if len(parts) != _TWO_ARG_PARTS:
        out.print("Usage: link <id1> <id2>")
        return None
    try:
        id_a, id_b = int(parts[1]), int(parts[2])
    except ValueError:
        out.print("Transaction ids must be integers.")
        return None
    if id_a not in tx_ids or id_b not in tx_ids:
        out.print("One or both transaction ids not found. Check the listing for valid ids.")
        return None
    if id_a == id_b:
        out.print("Cannot link a transaction to itself.")
        return None

    tx_by_id = {t.id: t for t in txs}
    tx_a, tx_b = tx_by_id[id_a], tx_by_id[id_b]
    implied_fee, err = _compute_implied_fee(tx_a, tx_b)
    if err:
        out.print(f"  Cannot link: {err}")
        return None

    if implied_fee > Decimal("0"):
        out.print(
            f"  Difference: {implied_fee:.8f} {tx_a.asset} — "
            f"will be recorded as a network fee (fee micro-disposal at boil time)."
        )
        confirm = input("  Proceed? [y/N] ").strip().lower()
        if confirm not in ("y", "yes"):
            out.print("  Cancelled.")
            return None

    _remove_conflicting_override(conn, id_a, id_b, "unlink")
    ov = repo.write_transfer_override(conn, id_a, id_b, "link", implied_fee_crypto=implied_fee)
    emit(MessageCode.STIR_LINK_APPLIED, id_a=id_a, id_b=id_b, ov_id=ov.id)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
    _print_unmatched(batch_name, txs, state, wallets, tax_year)
    return overrides, state


def _cmd_unlink(  # noqa: PLR0913
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
    wallets: list[Wallet],
    tax_year: int | None = None,
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Handle the ``unlink`` sub-command. Returns updated (overrides, state) or None on error."""
    if len(parts) != _TWO_ARG_PARTS:
        out.print("Usage: unlink <id1> <id2>")
        return None
    try:
        id_a, id_b = int(parts[1]), int(parts[2])
    except ValueError:
        out.print("Transaction ids must be integers.")
        return None
    if id_a not in tx_ids or id_b not in tx_ids:
        out.print("One or both transaction ids not found.")
        return None
    if id_a == id_b:
        out.print("Cannot unlink a transaction from itself.")
        return None
    _remove_conflicting_override(conn, id_a, id_b, "link")
    ov = repo.write_transfer_override(conn, id_a, id_b, "unlink")
    emit(MessageCode.STIR_UNLINK_APPLIED, id_a=id_a, id_b=id_b, ov_id=ov.id)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
    _print_unmatched(batch_name, txs, state, wallets, tax_year)
    return overrides, state


def _cmd_clear(  # noqa: PLR0913
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
    wallets: list[Wallet],
    tax_year: int | None = None,
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Handle the ``clear`` sub-command. Returns updated (overrides, state) or None on error."""
    if len(parts) != _ONE_ARG_PARTS:
        out.print("Usage: clear <id>")
        return None
    try:
        tx_id = int(parts[1])
    except ValueError:
        out.print("Transaction id must be an integer.")
        return None
    if tx_id not in tx_ids:
        out.print(f"Transaction id {tx_id} not found.")
        return None
    count = repo.clear_transfer_overrides_for_tx(conn, tx_id)
    emit(MessageCode.STIR_CLEAR_APPLIED, tx_id=tx_id, count=count)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
    _print_unmatched(batch_name, txs, state, wallets, tax_year)
    return overrides, state


_TXID_RE = re.compile(r"^[0-9a-f]{64}$")


def _cmd_destination(  # noqa: PLR0911
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    txs: list[Transaction],
) -> None:
    """Handle the ``destination <id>`` sub-command.

    Prompts the user for the on-chain txid and saves it to
    ``transaction_txid_overrides``.  Only BTC transactions whose CSV did not
    include a txid are accepted.
    """
    if len(parts) != _ONE_ARG_PARTS:
        out.print("Usage: destination <id>")
        return
    try:
        tx_id = int(parts[1])
    except ValueError:
        out.print("Transaction id must be an integer.")
        return
    if tx_id not in tx_ids:
        out.print(f"Transaction id {tx_id} not found.")
        return

    tx_by_id = {t.id: t for t in txs}
    tx = tx_by_id[tx_id]

    if tx.asset != "BTC":
        emit(MessageCode.STIR_ERROR_DESTINATION_NOT_BTC, tx_id=tx_id, asset=tx.asset)
        return
    # Refuse only if the txid came from the CSV import (not from a prior override).
    # Checking the DB avoids false positives when txs has already been patched
    # by _apply_txid_overrides (where the txid is ours and can be replaced).
    existing_overrides = repo.read_transaction_txid_overrides(conn)
    has_csv_txid = tx.txid is not None and tx_id not in existing_overrides
    if has_csv_txid:
        emit(MessageCode.STIR_ERROR_DESTINATION_HAS_TXID, tx_id=tx_id)
        return

    ts = tx.timestamp.strftime("%Y-%m-%d %H:%M UTC")
    out.print(
        f"Transaction {tx_id} — {tx.source} {tx.tx_type.value}" f"  BTC {tx.amount:.8f}  {ts}"
    )
    out.print("Paste the on-chain txid (64 lowercase hex characters):")
    try:
        raw = input("> ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        out.print()
        out.print("Cancelled.")
        return

    if not _TXID_RE.fullmatch(raw):
        emit(MessageCode.STIR_ERROR_DESTINATION_INVALID_TXID)
        return

    repo.write_transaction_txid_override(conn, tx_id, raw)
    emit(MessageCode.STIR_DESTINATION_SAVED, tx_id=tx_id)


def _cmd_transfer(  # noqa: PLR0911 PLR0912 PLR0915 PLR0913
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
    wallets: list[Wallet],
    tax_year: int | None = None,
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Guided transfer wizard.

    Walks the user through pairing a withdrawal or deposit with its counterpart,
    validating that no asset units are created from nothing, and recording the
    exact implied fee when sent > received.
    """
    if len(parts) != _ONE_ARG_PARTS:
        out.print("Usage: transfer <id> [<id> ...]")
        return None
    try:
        tx_id = int(parts[1])
    except ValueError:
        out.print("Transaction id must be an integer.")
        return None
    if tx_id not in tx_ids:
        out.print(f"Transaction id {tx_id} not found.")
        return None

    tx_by_id = {t.id: t for t in txs}
    tx = tx_by_id[tx_id]

    if tx.tx_type not in _OUTGOING and tx.tx_type not in _INCOMING:
        out.print(
            f"  Transaction {tx_id} is type '{tx.tx_type.value}' — "
            "only withdrawals and deposits can be classified as transfers."
        )
        return None

    is_out = tx.tx_type in _OUTGOING
    arrow = "sent to" if is_out else "received from"

    out.print()
    out.print(f"  Transfer wizard — transaction {tx_id}:")
    _tx_header = Text("  ")
    _tx_header.append_text(_fmt_tx(tx, wallets))
    out.print(_tx_header)
    out.print()

    # Step 1: pick a wallet.
    out.print(f"  Where was this {tx.asset} {arrow}?")
    out.print()
    options: list[tuple[str, str]] = []  # (label, wallet_name_or_special)
    for w in wallets:
        tag = "" if w.auto_created else "*"
        options.append((f"{w.name}{tag}", w.name))
    options.append(("(enter a different wallet name)", "__new__"))
    options.append(("(untracked wallet — no CSV available)", "__external__"))

    for i, (label, _) in enumerate(options, 1):
        out.print(f"    [{i}] {label}")
    out.print()

    choice_raw = input("  Choice (number or wallet name): ").strip()
    wallet_name: str | None = None
    is_external = False

    try:
        choice_idx = int(choice_raw) - 1
        if 0 <= choice_idx < len(options):
            _, special = options[choice_idx]
            if special == "__external__":
                is_external = True
            elif special == "__new__":
                wallet_name = input("  Wallet name: ").strip() or None
            else:
                wallet_name = special
    except ValueError:
        # User typed a wallet name directly.
        wallet_name = choice_raw if choice_raw else None

    # Step 2: untracked wallet path.
    if is_external:
        ext_label = input("  Label for this external wallet (optional): ").strip()
        wallet_err = _validate_wallet_name(ext_label)
        if wallet_err:
            out.print(f"  {wallet_err}")
            return None

        # Ask whether a network fee should be recorded.
        out.print()
        out.print("  Was there a network fee for this transfer?")
        out.print("    [N] No fee to record (exchange covered it, or already embedded)")
        out.print("    [F] Enter fee amount manually")
        fee_choice = input("  Choice [N/F]: ").strip().upper()
        implied_fee = Decimal("0")
        if fee_choice == "F":
            fee_raw = input(f"  Fee amount (in {tx.asset}): ").strip()
            implied_fee, fee_err = _parse_fee_amount(fee_raw, tx.asset, tx.amount)
            if fee_err:
                out.print(f"  {fee_err}")
                return None

        action = "external-out" if is_out else "external-in"
        repo.clear_transfer_overrides_for_tx(conn, tx_id)
        repo.write_transfer_override(
            conn, tx_id, None, action, external_wallet=ext_label, implied_fee_crypto=implied_fee
        )
        verb = "out to" if action == "external-out" else "in from"
        ext_display = f" '{ext_label}'" if ext_label else ""
        fee_display = f" (fee: {implied_fee:.8f} {tx.asset})" if implied_fee > Decimal("0") else ""
        out.print(f"  Marked id:{tx_id} as external transfer {verb}{ext_display}{fee_display}.")
        out.print("  Run 'boil --from transfer_match' to apply.")
        overrides = repo.read_transfer_overrides(conn)
        state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
        _print_unmatched(batch_name, txs, state, wallets, tax_year)
        return overrides, state

    if not wallet_name:
        out.print("  No wallet selected — cancelled.")
        return None

    # Step 3: find candidate transactions from the chosen wallet.
    # Group by wallet name (matching on wallet.name or tx.source as fallback).
    wallet_obj = next((w for w in wallets if w.name == wallet_name), None)
    candidates: list[Transaction] = []
    for t in txs:
        if t.id == tx_id:
            continue
        # Match by wallet_id if available, otherwise by source == wallet_name.
        by_id = wallet_obj is not None and t.wallet_id == wallet_obj.id
        by_source = wallet_obj is None and t.source == wallet_name
        if by_id or by_source:
            candidates.append(t)

    # Filter to opposite direction and same asset.
    expected_direction = _INCOMING if is_out else _OUTGOING
    candidates = [c for c in candidates if c.tx_type in expected_direction and c.asset == tx.asset]

    if not candidates:
        noun = "deposits" if is_out else "withdrawals"
        out.print(f"  No {tx.asset} {noun} found in '{wallet_name}'.")
        out.print(
            f"  If the wallet isn't tapped yet, run 'sirop tap <file> --wallet {wallet_name}'."
        )
        return None

    out.print()
    out.print(f"  {tx.asset} {'deposits' if is_out else 'withdrawals'} from '{wallet_name}':")
    for i, c in enumerate(candidates, 1):
        _cand_line = Text(f"    [{i}] ")
        _cand_line.append_text(_fmt_tx(c, wallets))
        out.print(_cand_line)
    out.print()

    sel_raw = input("  Select the counterpart transaction (number or 'none'): ").strip().lower()
    if sel_raw in ("none", "n", ""):
        out.print("  Cancelled.")
        return None

    try:
        sel_idx = int(sel_raw) - 1
        if not (0 <= sel_idx < len(candidates)):
            out.print("  Invalid selection.")
            return None
        counterpart = candidates[sel_idx]
    except ValueError:
        out.print("  Invalid selection.")
        return None

    # Step 4: validate amounts.
    implied_fee, err = _compute_implied_fee(tx, counterpart)
    if err:
        out.print()
        out.print(f"  Cannot link: {err}")
        return None

    # Step 5: confirm and handle fee difference.
    out.print()
    sent_tx = tx if is_out else counterpart
    recv_tx = counterpart if is_out else tx
    out.print(f"  Sent:     {sent_tx.amount:.8f} {tx.asset}  (id:{sent_tx.id})")
    out.print(f"  Received: {recv_tx.amount:.8f} {tx.asset}  (id:{recv_tx.id})")

    if implied_fee > Decimal("0"):
        out.print(f"  Difference: {implied_fee:.8f} {tx.asset}")
        out.print()
        out.print("  How should this difference be classified?")
        out.print("    [F] Network fee   (fee micro-disposal at boil time — most common)")
        out.print("    [C] Cancel        (go back, do not save)")
        out.print()
        fee_choice = input("  Choice [F/C]: ").strip().upper()
        if fee_choice != "F":
            out.print("  Cancelled.")
            return None
    else:
        confirm = (
            input("  Clean transfer — no fee difference. Confirm link? [y/N] ").strip().lower()
        )
        if confirm not in ("y", "yes"):
            out.print("  Cancelled.")
            return None

    # Write the override.
    _remove_conflicting_override(conn, tx.id, counterpart.id, "unlink")
    ov = repo.write_transfer_override(
        conn,
        tx.id,
        counterpart.id,
        "link",
        implied_fee_crypto=implied_fee,
    )
    if implied_fee > Decimal("0"):
        out.print(
            f"  Linked id:{tx.id} ↔ id:{counterpart.id}  "
            f"(implied fee: {implied_fee:.8f} {tx.asset})"
        )
    else:
        out.print(f"  Linked id:{tx.id} ↔ id:{counterpart.id}  (clean transfer, no fee)")
    out.print("  Run 'boil --from transfer_match' to apply.")
    emit(MessageCode.STIR_LINK_APPLIED, id_a=tx.id, id_b=counterpart.id, ov_id=ov.id)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
    _print_unmatched(batch_name, txs, state, wallets, tax_year)
    return overrides, state


def _interactive_loop(  # noqa: PLR0912 PLR0913 PLR0915
    conn: sqlite3.Connection,
    txs: list[Transaction],
    wallets: list[Wallet],
    overrides: list[TransferOverride],
    state: _MatchState,
    batch_name: str,
    tax_year: int | None = None,
) -> int:
    """Prompt-based REPL for reviewing and editing transfer assignments."""
    tx_ids: set[int] = {t.id for t in txs}

    while True:
        try:
            raw = input("stir> ").strip()
        except (EOFError, KeyboardInterrupt):
            out.print()
            emit(MessageCode.STIR_EXIT)
            return 0

        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0].lower()

        if cmd in {"quit", "q", "exit"}:
            emit(MessageCode.STIR_EXIT)
            return 0

        if cmd in {"help", "h", "?"}:
            _print_help()
            continue

        if cmd in {"list", "l", "ls"}:
            wallets = repo.read_wallets(conn)
            overrides = repo.read_transfer_overrides(conn)
            state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
            _print_unmatched(batch_name, txs, state, wallets, tax_year)
            continue

        if cmd in {"view", "v"}:
            wallets = repo.read_wallets(conn)
            overrides = repo.read_transfer_overrides(conn)
            state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
            _print_state(batch_name, txs, state, overrides, wallets)
            continue

        if cmd == "transfer":
            ids = parts[1:]
            if not ids:
                out.print("Usage: transfer <id> [<id> ...]")
            else:
                total = len(ids)
                for idx, raw_id in enumerate(ids, 1):
                    if total > 1:
                        out.print(f"\n  — Transfer {idx} of {total} (id: {raw_id}) —")
                    result = _cmd_transfer(
                        conn, ["transfer", raw_id], tx_ids, batch_name, txs, wallets, tax_year
                    )
                    if result is not None:
                        overrides, state = result
            continue

        if cmd == "link":
            result = _cmd_link(conn, parts, tx_ids, batch_name, txs, wallets, tax_year)
            if result is not None:
                overrides, state = result
            continue

        if cmd == "unlink":
            result = _cmd_unlink(conn, parts, tx_ids, batch_name, txs, wallets, tax_year)
            if result is not None:
                overrides, state = result
            continue

        if cmd == "clear":
            result = _cmd_clear(conn, parts, tx_ids, batch_name, txs, wallets, tax_year)
            if result is not None:
                overrides, state = result
            continue

        if cmd == "destination":
            _cmd_destination(conn, parts, tx_ids, txs)
            # Re-patch txs with any newly-saved override so the next state
            # rebuild (list, view, or the next command) sees the updated txid.
            txs = _apply_txid_overrides(conn, txs)
            overrides = repo.read_transfer_overrides(conn)
            state = _build_state(txs, overrides, graph_pairs=_load_graph_pairs(conn, txs))
            continue

        out.print(f"Unknown command: {cmd!r}. Type 'help' for available commands.")
