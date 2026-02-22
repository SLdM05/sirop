"""Handler for ``sirop tap <file> [--source <name>]``.

Reads a CSV file, detects or validates its exchange format, runs the
matching importer, and writes the resulting ``RawTransaction`` rows into
the ``raw_transactions`` table of the active ``.sirop`` batch.

Flow
----
1. Resolve the active batch; abort if none is set.
2. Read only the CSV header row (no data rows yet).
3. Detect or validate the format via :class:`~sirop.importers.detector.FormatDetector`.
4. Load the matching importer and parse the full CSV.
5. Write rows to ``raw_transactions`` in a single SQLite transaction.
6. Mark ``stage_status[tap] = done``.

Error behaviour
---------------
* No ``--source``: auto-detect.  Zero matches → error with partial-match
  hint.  Multiple matches → ask user to disambiguate with ``--source``.
* ``--source <name>``: validate that the fingerprint columns are present.
  Missing columns → error naming each one, plus a suggestion if another
  format fits better.
* Extra columns (present in CSV but unknown to any fingerprint) → WARNING
  only, not a hard stop.  The importer stores them in ``extra_json``.
* Batch ``tap`` stage already ``done`` → allowed; rows are appended and all
  downstream stages are marked ``invalidated`` so they re-run with the
  full combined dataset.  Useful when a batch spans multiple exchange files.
"""

import csv
import json
import sqlite3
from collections.abc import Callable
from pathlib import Path

from sirop.config.settings import Settings, get_settings
from sirop.db.connection import get_active_batch_name, open_batch
from sirop.db.schema import PIPELINE_STAGES
from sirop.importers.base import BaseImporter, ImporterError
from sirop.importers.detector import FormatDetector
from sirop.importers.ndax import NDAXImporter
from sirop.importers.shakepay import ShakepayImporter
from sirop.importers.sparrow import SparrowImporter
from sirop.models.messages import MessageCode
from sirop.models.raw import RawTransaction
from sirop.utils.logging import get_logger
from sirop.utils.messages import emit

logger = get_logger(__name__)

# Directory that contains the built-in importer YAML configs.
_BUILTIN_CONFIG_DIR = Path("config/importers")

# Registry: source_name → factory callable (classmethod bound to the class).
# Adding a new importer: implement the class, add its from_yaml here.
_IMPORTER_REGISTRY: dict[str, Callable[[Path], BaseImporter]] = {
    "ndax": NDAXImporter.from_yaml,
    "shakepay": ShakepayImporter.from_yaml,
    "sparrow": SparrowImporter.from_yaml,
}


class _TapError(Exception):
    """Internal sentinel — raised to exit early; caught by handle_tap."""

    def __init__(self, code: MessageCode, **kwargs: object) -> None:
        self.msg_code = code
        self.msg_kwargs = kwargs
        super().__init__(str(code))


def handle_tap(
    file_path: Path,
    source: str | None,
    settings: Settings | None = None,
) -> int:
    """Parse *file_path* and write raw transactions into the active batch.

    Parameters
    ----------
    file_path:
        Path to the exchange CSV export.
    source:
        Importer name (e.g. ``"ndax"``).  When ``None``, auto-detection
        is attempted.
    settings:
        Application settings; resolved from the environment if omitted.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on any error.
    """
    if settings is None:
        settings = get_settings()
    try:
        return _run_tap(file_path, source, settings)
    except _TapError as exc:
        emit(exc.msg_code, **exc.msg_kwargs)
        return 1


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _run_tap(file_path: Path, source: str | None, settings: Settings) -> int:
    """Core tap logic; raises ``_TapError`` on any user-facing error."""
    # ── 1. Active batch ───────────────────────────────────────────────────────
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _TapError(MessageCode.BATCH_ERROR_NO_ACTIVE)

    if not file_path.exists():
        raise _TapError(MessageCode.TAP_ERROR_FILE_NOT_FOUND, path=file_path)

    # ── 2. Read CSV headers (header row only — no data rows yet) ─────────────
    headers = _read_headers(file_path)
    if not headers:
        raise _TapError(MessageCode.TAP_ERROR_NO_HEADER, path=file_path)

    # ── 3. Detect / validate format ───────────────────────────────────────────
    config_dirs = [_BUILTIN_CONFIG_DIR, settings.data_dir / "importers"]
    detector = FormatDetector(config_dirs)

    detected_source = (
        _validate_source(detector, headers, source)
        if source is not None
        else _auto_detect(detector, headers)
    )
    if detected_source is None:
        return 1  # helpers already emitted the error

    # ── 4. Load importer and parse ────────────────────────────────────────────
    factory = _IMPORTER_REGISTRY.get(detected_source)
    if factory is None:
        disp = detector.display_name(detected_source)
        raise _TapError(
            MessageCode.TAP_ERROR_NO_IMPORTER,
            fmt=disp,
            source=detected_source,
        )

    yaml_path = _BUILTIN_CONFIG_DIR / f"{detected_source}.yaml"
    try:
        importer = factory(yaml_path)
        txs = importer.parse(file_path)
    except ImporterError as exc:
        raise _TapError(
            MessageCode.TAP_ERROR_PARSE_FAILED,
            filename=file_path.name,
            detail=exc,
        ) from exc

    # ── 5. Write to DB ────────────────────────────────────────────────────────
    inserted, skipped = _write_to_batch(batch_name, settings, txs)

    disp = detector.display_name(detected_source)
    if inserted == 0:
        emit(
            MessageCode.TAP_NOTHING_NEW,
            filename=file_path.name,
            fmt=disp,
            count=skipped,
            batch=batch_name,
        )
        logger.debug(
            "tap: 0 new rows from %s (%s) — %d duplicate(s) skipped",
            file_path.name,
            detected_source,
            skipped,
        )
        return 0

    skip_note = f" ({skipped} duplicate(s) skipped)" if skipped else ""
    emit(
        MessageCode.TAP_SUCCESS,
        count=inserted,
        filename=file_path.name,
        fmt=disp,
        batch=batch_name,
        skip_note=skip_note,
    )
    logger.debug(
        "tap: %d rows written from %s (%s) into batch %s (%d duplicate(s) skipped)",
        inserted,
        file_path.name,
        detected_source,
        batch_name,
        skipped,
    )
    return 0


def _write_to_batch(
    batch_name: str, settings: Settings, txs: list[RawTransaction]
) -> tuple[int, int]:
    """Write *txs* to the active batch DB in a single atomic transaction.

    Duplicate rows (same source + timestamp + asset + amount already present in
    ``raw_transactions``) are silently skipped.  This handles the common case of
    overlapping date-range exports from the same exchange without blocking or
    requiring the user to manually deduplicate files.

    Returns
    -------
    tuple[int, int]
        ``(inserted, skipped)`` — number of new rows written and number of
        duplicate rows that were omitted.

    Raises
    ------
    _TapError
        When the tap stage is running (concurrent process guard), or a DB
        write fails.
    """
    conn = open_batch(batch_name, settings)
    try:
        stage_row = conn.execute("SELECT status FROM stage_status WHERE stage = 'tap'").fetchone()
        if stage_row and stage_row["status"] == "running":
            raise _TapError(MessageCode.TAP_ERROR_STAGE_RUNNING, name=batch_name)

        is_append = stage_row and stage_row["status"] == "done"

        # Deduplication — single pass, covers two sources of duplicates:
        #
        # 1. Within-file: some exchange exports contain repeated rows (e.g. a
        #    CSV exported across overlapping date ranges that was concatenated,
        #    or a platform bug).  Caught by the `seen` set.
        # 2. Cross-file: the same row already exists in raw_transactions from a
        #    previous tap of a different (or the same) file.  Caught by
        #    `excluded_keys`, which is populated from the DB on append taps.
        #
        # (source, raw_timestamp, asset, amount) is a natural unique key —
        # no exchange produces two distinct transactions with identical values
        # at the exact same second.
        excluded_keys: set[tuple[str, str, str, str]] = set()
        if is_append:
            excluded_keys = {
                (row[0], row[1], row[2], row[3])
                for row in conn.execute(
                    "SELECT source, raw_timestamp, asset, amount FROM raw_transactions"
                ).fetchall()
            }
        seen: set[tuple[str, str, str, str]] = set()
        new_txs: list[RawTransaction] = []
        for tx in txs:
            key = (tx.source, tx.timestamp.isoformat(), tx.asset, format(tx.amount, "f"))
            if key not in excluded_keys and key not in seen:
                seen.add(key)
                new_txs.append(tx)
        skipped = len(txs) - len(new_txs)
        txs = new_txs

        # Nothing new to insert — leave the batch state exactly as-is.
        if not txs:
            return 0, skipped

        # format(d, 'f') forces fixed-point notation for every Decimal before
        # it reaches the DB.  str(Decimal) is non-deterministic: division
        # results and very small values produce scientific notation
        # (e.g. "1.2419E+5", "1.0E-7") which is unreadable in DB Browser and
        # breaks any downstream code that expects plain numeric strings.
        db_rows = [
            (
                tx.source,
                tx.timestamp.isoformat(),
                tx.transaction_type,
                tx.asset,
                format(tx.amount, "f"),
                tx.amount_currency,
                format(tx.fee_amount, "f") if tx.fee_amount is not None else None,
                tx.fee_currency,
                format(tx.fiat_value, "f") if tx.fiat_value is not None else None,
                tx.fiat_currency,
                format(tx.rate, "f") if tx.rate is not None else None,
                format(tx.spot_rate, "f") if tx.spot_rate is not None else None,
                tx.txid,
                json.dumps(tx.raw_row),
            )
            for tx in txs
        ]

        with conn:
            conn.execute("UPDATE stage_status SET status = 'running' WHERE stage = 'tap'")
            if is_append:
                # Invalidate all downstream stages within the same transaction so
                # they re-run with the full combined dataset.  Inline the SQL here
                # rather than calling set_stages_invalidated() to keep the whole
                # write (status update + invalidations + inserts) in one atomic block.
                downstream = PIPELINE_STAGES[PIPELINE_STAGES.index("tap") + 1 :]
                for _stage in downstream:
                    conn.execute(
                        "UPDATE stage_status"
                        " SET status='invalidated', completed_at=NULL, error=NULL"
                        " WHERE stage = ?",
                        (_stage,),
                    )
            conn.executemany(
                """
                INSERT INTO raw_transactions
                    (source, raw_timestamp, transaction_type, asset, amount,
                     amount_currency, fee, fee_currency, cad_amount, fiat_currency,
                     cad_rate, spot_rate, txid, extra_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                db_rows,
            )
            conn.execute("UPDATE stage_status SET status = 'done' WHERE stage = 'tap'")

        return len(txs), skipped

    except sqlite3.Error as exc:
        raise _TapError(MessageCode.TAP_ERROR_WRITE_FAILED, name=batch_name, detail=exc) from exc
    finally:
        conn.close()


def _auto_detect(detector: FormatDetector, headers: set[str]) -> str | None:
    """Attempt auto-detection; emit errors and return the source name or None."""
    result = detector.detect(headers)

    if not result.matched:
        emit(MessageCode.TAP_ERROR_NO_FORMAT_MATCH)
        emit(MessageCode.TAP_HEADERS_FOUND, headers=", ".join(sorted(headers)))
        if result.partial:
            best_name, best_ratio = result.partial[0]
            disp = detector.display_name(best_name)
            emit(
                MessageCode.TAP_HINT_CLOSEST_MATCH,
                fmt=disp,
                ratio=best_ratio,
                source=best_name,
            )
        else:
            emit(MessageCode.TAP_KNOWN_FORMATS, formats=", ".join(detector.known_sources))
        return None

    if len(result.matched) > 1:
        emit(MessageCode.TAP_ERROR_MULTIPLE_FORMATS, formats=", ".join(result.matched))
        return None

    detected = result.matched[0]
    disp = detector.display_name(detected)
    emit(MessageCode.TAP_FORMAT_DETECTED, fmt=disp)

    if result.unknown_headers:
        logger.warning(
            "CSV has %d column(s) not in any known %s layout: %s — "
            "possible format change in export. Storing in extra_json.",
            len(result.unknown_headers),
            disp,
            sorted(result.unknown_headers),
        )

    return detected


def _validate_source(detector: FormatDetector, headers: set[str], source: str) -> str | None:
    """Validate the user-declared source; emit errors and return source or None."""
    result = detector.validate(headers, source)

    if not result.ok:
        emit(MessageCode.TAP_ERROR_MISSING_COLUMNS, source=source)
        for col in sorted(result.missing):
            emit(MessageCode.TAP_MISSING_COLUMN, column=col)
        if result.suggested:
            suggested_disp = detector.display_name(result.suggested)
            emit(
                MessageCode.TAP_HINT_SUGGESTED_SOURCE,
                fmt=suggested_disp,
                source=result.suggested,
            )
        else:
            fp = detector.fingerprint(source)
            if fp is not None:
                emit(MessageCode.TAP_EXPECTED_COLUMNS, source=source, columns=sorted(fp))
        return None

    if result.unknown_headers:
        disp = detector.display_name(source)
        logger.warning(
            "CSV has %d column(s) not seen in any known %s format: %s — "
            "possible format change. Run with --debug for details.",
            len(result.unknown_headers),
            disp,
            sorted(result.unknown_headers),
        )

    return source


def _read_headers(csv_path: Path) -> set[str]:
    """Return the set of non-empty header names from the first row of *csv_path*."""
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        try:
            header_row = next(reader)
        except StopIteration:
            return set()
    return {h.strip() for h in header_row if h.strip()}
