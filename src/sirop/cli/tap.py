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
from sirop.models.raw import RawTransaction
from sirop.utils.logging import get_logger

logger = get_logger(__name__)

# Directory that contains the built-in importer YAML configs.
_BUILTIN_CONFIG_DIR = Path("config/importers")

# Registry: source_name → factory callable (classmethod bound to the class).
# Adding a new importer: implement the class, add its from_yaml here.
_IMPORTER_REGISTRY: dict[str, Callable[[Path], BaseImporter]] = {
    "ndax": NDAXImporter.from_yaml,
    "shakepay": ShakepayImporter.from_yaml,
}


class _TapError(Exception):
    """Internal sentinel — raised to exit early with a printed error message."""


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
        print(str(exc))
        return 1


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _run_tap(file_path: Path, source: str | None, settings: Settings) -> int:
    """Core tap logic; raises ``_TapError`` on any user-facing error."""
    # ── 1. Active batch ───────────────────────────────────────────────────────
    batch_name = get_active_batch_name(settings)
    if batch_name is None:
        raise _TapError("error: no active batch. Run `sirop create <name>` first.")

    if not file_path.exists():
        raise _TapError(f"error: file not found: {file_path}")

    # ── 2. Read CSV headers (header row only — no data rows yet) ─────────────
    headers = _read_headers(file_path)
    if not headers:
        raise _TapError(f"error: CSV has no header row: {file_path}")

    # ── 3. Detect / validate format ───────────────────────────────────────────
    config_dirs = [_BUILTIN_CONFIG_DIR, settings.data_dir / "importers"]
    detector = FormatDetector(config_dirs)

    detected_source = (
        _validate_source(detector, headers, source)
        if source is not None
        else _auto_detect(detector, headers)
    )
    if detected_source is None:
        return 1  # helpers already printed the error

    # ── 4. Load importer and parse ────────────────────────────────────────────
    factory = _IMPORTER_REGISTRY.get(detected_source)
    if factory is None:
        disp = detector.display_name(detected_source)
        raise _TapError(
            f"error: format detected as {disp!r} "
            f"but no importer is implemented for {detected_source!r} yet."
        )

    yaml_path = _BUILTIN_CONFIG_DIR / f"{detected_source}.yaml"
    try:
        importer = factory(yaml_path)
        txs = importer.parse(file_path)
    except ImporterError as exc:
        raise _TapError(f"error: failed to parse {file_path.name}: {exc}") from exc

    # ── 5. Write to DB ────────────────────────────────────────────────────────
    _write_to_batch(batch_name, settings, txs)

    disp = detector.display_name(detected_source)
    print(f"Tapped {len(txs)} transaction(s) from {file_path.name} [{disp}] into '{batch_name}'.")
    logger.info(
        "tap: %d rows written from %s (%s) into batch %s",
        len(txs),
        file_path.name,
        detected_source,
        batch_name,
    )
    return 0


def _write_to_batch(batch_name: str, settings: Settings, txs: list[RawTransaction]) -> None:
    """Write *txs* to the active batch DB in a single atomic transaction.

    Raises
    ------
    _TapError
        When the tap stage is already done/running, or a DB write fails.
    """
    conn = open_batch(batch_name, settings)
    try:
        stage_row = conn.execute("SELECT status FROM stage_status WHERE stage = 'tap'").fetchone()
        if stage_row and stage_row["status"] == "running":
            raise _TapError(
                f"error: batch '{batch_name}' tap stage is currently running "
                "(another process?). Aborting."
            )

        # Appending a second (or later) file to an existing tap stage is fine —
        # we just invalidate all downstream stages so they re-run with the full
        # combined dataset.
        is_append = stage_row and stage_row["status"] == "done"

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

    except sqlite3.Error as exc:
        raise _TapError(f"error: failed to write to batch '{batch_name}': {exc}") from exc
    finally:
        conn.close()


def _auto_detect(detector: FormatDetector, headers: set[str]) -> str | None:
    """Attempt auto-detection; print errors and return the source name or None."""
    result = detector.detect(headers)

    if not result.matched:
        print("error: cannot identify CSV format — headers don't match any known exchange.")
        print(f"  headers found: {sorted(headers)}")
        if result.partial:
            best_name, best_ratio = result.partial[0]
            disp = detector.display_name(best_name)
            print(
                f"hint: closest match is {disp!r} "
                f"({best_ratio:.0%} of expected columns found). "
                f"Pass --source {best_name} to override, or check you exported "
                f"the correct report type."
            )
        else:
            print(f"  known formats: {', '.join(detector.known_sources)}")
        return None

    if len(result.matched) > 1:
        names = ", ".join(result.matched)
        print(f"error: CSV headers match multiple formats: {names}. Pass --source to pick one.")
        return None

    detected = result.matched[0]
    disp = detector.display_name(detected)
    print(f"Detected format: {disp}")

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
    """Validate the user-declared source; print errors and return source or None."""
    result = detector.validate(headers, source)

    if not result.ok:
        print(f"error: --source {source!r} declared but CSV is missing expected columns:")
        for col in sorted(result.missing):
            print(f"  missing: {col!r}")
        if result.suggested:
            suggested_disp = detector.display_name(result.suggested)
            print(
                f"hint: headers look more like {suggested_disp!r}. "
                f"Try --source {result.suggested} instead."
            )
        else:
            fp = detector.fingerprint(source)
            if fp is not None:
                print(f"  expected columns for {source!r}: {sorted(fp)}")
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
