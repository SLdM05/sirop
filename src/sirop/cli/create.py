"""
Handler for `sirop create <name>`.

Creates a new .sirop batch file, initialises the schema, and sets
the new batch as the active batch.
"""

import re
import sqlite3
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version
from pathlib import Path

from sirop.config.settings import Settings, get_settings
from sirop.db.connection import get_batch_path, open_batch, set_active_batch
from sirop.db.schema import PIPELINE_STAGES, SCHEMA_VERSION, create_tables
from sirop.utils.logging import get_logger

logger = get_logger(__name__)

# Matches a four-digit year in the 2000s anywhere in the batch name.
# Negative lookaround ensures it isn't part of a longer digit sequence.
_YEAR_RE: re.Pattern[str] = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def _infer_year(name: str) -> int | None:
    """Extract a tax year from the batch name, e.g. 'my2025tax' → 2025."""
    match = _YEAR_RE.search(name)
    return int(match.group(1)) if match else None


def _sirop_version() -> str:
    try:
        return pkg_version("sirop")
    except PackageNotFoundError:
        return "0.1.0"


def handle_create(
    name: str,
    year: int | None,
    settings: Settings | None = None,
) -> int:
    """Create a new .sirop batch file and set it as the active batch.

    Parameters
    ----------
    name:
        Batch name, e.g. ``"my2025tax"``. Used as the filename stem.
    year:
        Tax year. Inferred from the name if not supplied.
    settings:
        Application settings. Resolved from the environment if not supplied.

    Returns
    -------
    int
        Exit code: 0 on success, 1 on failure.
    """
    if settings is None:
        settings = get_settings()

    resolved_year = year if year is not None else _infer_year(name)
    if resolved_year is None:
        print(f"error: cannot infer tax year from '{name}'. Pass --year YYYY.")
        return 1

    batch_path = get_batch_path(name, settings)

    if batch_path.exists():
        print(f"error: batch '{name}' already exists at {batch_path}")
        return 1

    settings.data_dir.mkdir(parents=True, exist_ok=True)

    conn = open_batch(name, settings)
    try:
        create_tables(conn)
        _seed_metadata(conn, name=name, tax_year=resolved_year)
    except sqlite3.Error as exc:
        conn.close()
        # Remove the partially-created file so a retry starts clean.
        batch_path.unlink(missing_ok=True)
        print(f"error: failed to initialise batch '{name}': {exc}")
        return 1
    finally:
        conn.close()

    set_active_batch(name, settings)

    rel = _relative(batch_path)
    print(f"Created batch: {name} ({resolved_year}) → {rel}")
    logger.info(f"Created batch: {name} ({resolved_year}) → {rel}")
    return 0


def _seed_metadata(conn: sqlite3.Connection, *, name: str, tax_year: int) -> None:
    """Insert the one-time seed rows into a freshly created batch."""
    now = datetime.now(UTC).isoformat()
    sirop_ver = _sirop_version()

    with conn:
        conn.execute(
            "INSERT INTO batch_meta (name, tax_year, created_at, sirop_version)"
            " VALUES (?, ?, ?, ?)",
            (name, tax_year, now, sirop_ver),
        )
        conn.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, now),
        )
        conn.executemany(
            "INSERT INTO stage_status (stage, status) VALUES (?, 'pending')",
            [(stage,) for stage in PIPELINE_STAGES],
        )


def _relative(path: Path) -> Path:
    """Return path relative to cwd if possible, else return as-is."""
    try:
        return path.relative_to(Path.cwd())
    except ValueError:
        return path
