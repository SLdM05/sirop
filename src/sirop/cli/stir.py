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

- **link** two transactions — force them to be treated as a wallet-to-wallet
  transfer (non-taxable). This survives ``boil --from transfer_match`` re-runs.
- **unlink** two transactions — prevent two auto-matched transactions from
  being paired. Both legs will be treated as taxable events (sell/buy).
- **clear** all overrides for a transaction — restore auto-matching behaviour.

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

from decimal import Decimal
from typing import TYPE_CHECKING

from sirop.config.settings import Settings, get_settings
from sirop.db import repositories as repo
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.models.messages import MessageCode
from sirop.transfer_match.matcher import (
    _INCOMING,
    _OUTGOING,
    FEE_TOLERANCE_ABSOLUTE,
    FEE_TOLERANCE_RELATIVE,
    MATCH_WINDOW_HOURS,
)
from sirop.utils.logging import get_logger
from sirop.utils.messages import emit

if TYPE_CHECKING:
    import sqlite3

    from sirop.models.override import TransferOverride
    from sirop.models.transaction import Transaction

logger = get_logger(__name__)

# Width of the separator line.
_COL_WIDTH = 76
# Expected part count for two-argument commands (cmd + id1 + id2).
_TWO_ARG_PARTS = 3
# Expected part count for one-argument commands (cmd + id).
_ONE_ARG_PARTS = 2


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
        txs = repo.read_transactions(conn)
        overrides = repo.read_transfer_overrides(conn)
        state = _build_state(txs, overrides)
        _print_state(batch_name, txs, state, overrides)

        if list_only:
            return 0

        return _interactive_loop(conn, txs, overrides, state, batch_name)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Non-interactive flag handlers
# ---------------------------------------------------------------------------


def _apply_link(conn: sqlite3.Connection, id_a: int, id_b: int) -> int:
    """Force-link two transactions. Removes any conflicting unlink override."""
    _assert_tx_exists(conn, id_a)
    _assert_tx_exists(conn, id_b)
    _remove_conflicting_override(conn, id_a, id_b, "unlink")
    override = repo.write_transfer_override(conn, id_a, id_b, "link")
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
        # forced_link_pairs: list of (tx_a, tx_b) from overrides
        self.forced_link_pairs: list[tuple[Transaction, Transaction]] = []
        # forced_unlink_pairs: pairs blocked by 'unlink' overrides
        self.forced_unlink_pairs: list[tuple[Transaction, Transaction]] = []
        # unmatched_out: outgoing txs that had no match
        self.unmatched_out: list[Transaction] = []
        # unmatched_in: incoming txs that had no match
        self.unmatched_in: list[Transaction] = []
        # other: remaining txs (buys, sells, income, fiat, …)
        self.other: list[Transaction] = []


def _build_override_sets(
    overrides: list[TransferOverride],
) -> tuple[set[tuple[int, int]], set[tuple[int, int]]]:
    """Return (forced_link_ids, forced_unlink_ids) as symmetric pair sets."""
    forced_link_ids: set[tuple[int, int]] = set()
    forced_unlink_ids: set[tuple[int, int]] = set()
    for ov in overrides:
        pair = (ov.tx_id_a, ov.tx_id_b)
        pair_rev = (ov.tx_id_b, ov.tx_id_a)
        if ov.action == "link":
            forced_link_ids.add(pair)
            forced_link_ids.add(pair_rev)
        else:
            forced_unlink_ids.add(pair)
            forced_unlink_ids.add(pair_rev)
    return forced_link_ids, forced_unlink_ids


def _apply_forced_links(
    overrides: list[TransferOverride],
    tx_by_id: dict[int, Transaction],
    paired_ids: set[int],
    state: _MatchState,
) -> None:
    """Populate *state.forced_link_pairs* from ``action='link'`` overrides."""
    for ov in overrides:
        if ov.action != "link":
            continue
        tx_a = tx_by_id.get(ov.tx_id_a)
        tx_b = tx_by_id.get(ov.tx_id_b)
        if tx_a is None or tx_b is None:
            continue
        if tx_a.id in paired_ids or tx_b.id in paired_ids:
            continue
        paired_ids.add(tx_a.id)
        paired_ids.add(tx_b.id)
        state.forced_link_pairs.append((tx_a, tx_b))


def _build_state(  # noqa: PLR0912
    txs: list[Transaction],
    overrides: list[TransferOverride],
) -> _MatchState:
    """Compute a preview of how the transfer matcher will classify *txs*."""
    from datetime import timedelta

    state = _MatchState()
    tx_by_id: dict[int, Transaction] = {t.id: t for t in txs}
    sorted_txs = sorted(txs, key=lambda t: t.timestamp)

    forced_link_ids, forced_unlink_ids = _build_override_sets(overrides)
    paired_ids: set[int] = set()

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
        if tx.tx_type in _OUTGOING:
            state.unmatched_out.append(tx)
        elif tx.tx_type in _INCOMING:
            state.unmatched_in.append(tx)
        else:
            state.other.append(tx)

    _ = forced_link_ids  # used in _build_override_sets; satisfy static analysis
    return state


def _would_match(tx: Transaction, candidate: Transaction, window: object) -> bool:
    """Quick check: would these two auto-match if not blocked?"""
    from datetime import timedelta

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

_HR = "─" * _COL_WIDTH


def _fmt_tx(tx: Transaction) -> str:
    """One-line summary of a transaction."""
    date_str = tx.timestamp.strftime("%Y-%m-%d")
    amount_str = f"{tx.amount:>14.8f} {tx.asset}"
    cad_str = f"CAD {tx.cad_value:>10,.2f}"
    return (
        f"id:{tx.id:<5}  {date_str}  {tx.source:<12}  "
        f"{tx.tx_type.value:<14}  {amount_str}  {cad_str}"
    )


def _print_pair(tx_a: Transaction, tx_b: Transaction, connector: str = "↔") -> None:
    print(f"  {_fmt_tx(tx_a)}")
    print(f"    {connector}  {_fmt_tx(tx_b)}")
    print()


def _print_state(  # noqa: PLR0912 PLR0915
    batch_name: str,
    txs: list[Transaction],
    state: _MatchState,
    overrides: list[TransferOverride],
) -> None:
    print(f"\nStirring batch: {batch_name}  ({len(txs)} transactions loaded)")
    print()

    # Auto-matched pairs.
    print(f"Auto-matched transfer pairs ({len(state.auto_pairs)}):")
    print(_HR)
    if state.auto_pairs:
        for out_tx, in_tx, reason in state.auto_pairs:
            _print_pair(out_tx, in_tx)
            print(f"       Reason: {reason}")
            print()
    else:
        print("  (none)")
    print()

    # Forced links.
    print(f"Forced links — stir overrides ({len(state.forced_link_pairs)}):")
    print(_HR)
    if state.forced_link_pairs:
        for tx_a, tx_b in state.forced_link_pairs:
            _print_pair(tx_a, tx_b)
    else:
        print("  (none)")
    print()

    # Forced unlinks (would-have-matched but blocked).
    if state.forced_unlink_pairs:
        print(f"Forced unlinks — blocked auto-matches ({len(state.forced_unlink_pairs)}):")
        print(_HR)
        for tx_a, tx_b in state.forced_unlink_pairs:
            _print_pair(tx_a, tx_b, connector="✗")
        print()

    # Unmatched outgoing → sells.
    print(f"Unmatched withdrawals — will be treated as sells ({len(state.unmatched_out)}):")
    print(_HR)
    if state.unmatched_out:
        for tx in state.unmatched_out:
            print(f"  {_fmt_tx(tx)}")
    else:
        print("  (none)")
    print()

    # Unmatched incoming → buys.
    print(f"Unmatched deposits — will be treated as buys ({len(state.unmatched_in)}):")
    print(_HR)
    if state.unmatched_in:
        for tx in state.unmatched_in:
            print(f"  {_fmt_tx(tx)}")
    else:
        print("  (none)")
    print()

    # Other taxable events.
    print(f"Other events (buys, sells, income, fiat, …) ({len(state.other)}):")
    print(_HR)
    if state.other:
        for tx in state.other:
            print(f"  {_fmt_tx(tx)}")
    else:
        print("  (none)")
    print()

    if overrides:
        print(f"Active overrides ({len(overrides)} total):")
        print(_HR)
        for ov in overrides:
            ts = ov.created_at.strftime("%Y-%m-%d %H:%M")
            note_part = f"  note: {ov.note}" if ov.note else ""
            print(
                f"  override id:{ov.id}  action:{ov.action}  "
                f"tx:{ov.tx_id_a} ↔ {ov.tx_id_b}  {ts}{note_part}"
            )
        print()


def _print_help() -> None:
    print()
    print("Commands:")
    print("  link <id1> <id2>    Force two transactions to be a transfer pair (non-taxable).")
    print("  unlink <id1> <id2>  Prevent two transactions from being auto-paired.")
    print("  clear <id>          Remove all stir overrides for a transaction.")
    print("  list                Refresh the display.")
    print("  help                Show this help.")
    print("  quit                Exit stir (overrides are already saved).")
    print()
    print("Transaction ids are shown as  id:NNN  in the listing above.")
    print()


# ---------------------------------------------------------------------------
# Interactive loop — sub-command handlers
# ---------------------------------------------------------------------------


def _cmd_link(
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Handle the ``link`` sub-command. Returns updated (overrides, state) or None on error."""
    if len(parts) != _TWO_ARG_PARTS:
        print("Usage: link <id1> <id2>")
        return None
    try:
        id_a, id_b = int(parts[1]), int(parts[2])
    except ValueError:
        print("Transaction ids must be integers.")
        return None
    if id_a not in tx_ids or id_b not in tx_ids:
        print("One or both transaction ids not found. Check the listing for valid ids.")
        return None
    if id_a == id_b:
        print("Cannot link a transaction to itself.")
        return None
    _remove_conflicting_override(conn, id_a, id_b, "unlink")
    ov = repo.write_transfer_override(conn, id_a, id_b, "link")
    emit(MessageCode.STIR_LINK_APPLIED, id_a=id_a, id_b=id_b, ov_id=ov.id)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides)
    _print_state(batch_name, txs, state, overrides)
    return overrides, state


def _cmd_unlink(
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Handle the ``unlink`` sub-command. Returns updated (overrides, state) or None on error."""
    if len(parts) != _TWO_ARG_PARTS:
        print("Usage: unlink <id1> <id2>")
        return None
    try:
        id_a, id_b = int(parts[1]), int(parts[2])
    except ValueError:
        print("Transaction ids must be integers.")
        return None
    if id_a not in tx_ids or id_b not in tx_ids:
        print("One or both transaction ids not found.")
        return None
    if id_a == id_b:
        print("Cannot unlink a transaction from itself.")
        return None
    _remove_conflicting_override(conn, id_a, id_b, "link")
    ov = repo.write_transfer_override(conn, id_a, id_b, "unlink")
    emit(MessageCode.STIR_UNLINK_APPLIED, id_a=id_a, id_b=id_b, ov_id=ov.id)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides)
    _print_state(batch_name, txs, state, overrides)
    return overrides, state


def _cmd_clear(
    conn: sqlite3.Connection,
    parts: list[str],
    tx_ids: set[int],
    batch_name: str,
    txs: list[Transaction],
) -> tuple[list[TransferOverride], _MatchState] | None:
    """Handle the ``clear`` sub-command. Returns updated (overrides, state) or None on error."""
    if len(parts) != _ONE_ARG_PARTS:
        print("Usage: clear <id>")
        return None
    try:
        tx_id = int(parts[1])
    except ValueError:
        print("Transaction id must be an integer.")
        return None
    if tx_id not in tx_ids:
        print(f"Transaction id {tx_id} not found.")
        return None
    count = repo.clear_transfer_overrides_for_tx(conn, tx_id)
    emit(MessageCode.STIR_CLEAR_APPLIED, tx_id=tx_id, count=count)
    overrides = repo.read_transfer_overrides(conn)
    state = _build_state(txs, overrides)
    _print_state(batch_name, txs, state, overrides)
    return overrides, state


def _interactive_loop(
    conn: sqlite3.Connection,
    txs: list[Transaction],
    overrides: list[TransferOverride],
    state: _MatchState,
    batch_name: str,
) -> int:
    """Prompt-based REPL for reviewing and editing transfer assignments."""
    _print_help()

    tx_ids: set[int] = {t.id for t in txs}

    while True:
        try:
            raw = input("stir> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
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
            overrides = repo.read_transfer_overrides(conn)
            state = _build_state(txs, overrides)
            _print_state(batch_name, txs, state, overrides)
            continue

        if cmd == "link":
            result = _cmd_link(conn, parts, tx_ids, batch_name, txs)
            if result is not None:
                overrides, state = result
            continue

        if cmd == "unlink":
            result = _cmd_unlink(conn, parts, tx_ids, batch_name, txs)
            if result is not None:
                overrides, state = result
            continue

        if cmd == "clear":
            result = _cmd_clear(conn, parts, tx_ids, batch_name, txs)
            if result is not None:
                overrides, state = result
            continue

        print(f"Unknown command: {cmd!r}. Type 'help' for available commands.")
