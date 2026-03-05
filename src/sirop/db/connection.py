"""
.sirop file connection helpers.

A .sirop file is a SQLite database representing one batch (one person's
one tax year). This module handles opening existing files and resolving
their paths from the configured DATA_DIR.

Usage
-----
from sirop.db.connection import get_batch_path, open_batch
from sirop.config.settings import get_settings

settings = get_settings()
conn = open_batch("my2025tax", settings)
try:
    ...
finally:
    conn.close()
"""

import re
import sqlite3
from pathlib import Path
from typing import Final

from sirop.config.settings import Settings
from sirop.db.schema import create_tables, migrate_to_v5, migrate_to_v6, migrate_to_v7

# Batch names must start with a letter or digit and contain only alphanumerics,
# underscores, and hyphens. Max 64 chars. This prevents path traversal
# (e.g. "../../../tmp/attack") and shell-special characters.
_BATCH_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$")


def _validate_batch_name(name: str) -> None:
    """Raise ValueError if name contains path-traversal or invalid characters."""
    if not _BATCH_NAME_RE.fullmatch(name):
        raise ValueError(
            f"Invalid batch name {name!r}. "
            "Use only letters, digits, underscores, and hyphens (1-64 chars)."
        )


def get_batch_path(name: str, settings: Settings) -> Path:
    """Return the filesystem path for a named batch file.

    Does not check whether the file exists.

    Raises
    ------
    ValueError
        If *name* contains path-traversal sequences or invalid characters.
    """
    _validate_batch_name(name)
    return settings.data_dir / f"{name}.sirop"


def open_batch(name: str, settings: Settings) -> sqlite3.Connection:
    """Open (or create) the SQLite database for a named batch.

    Enables WAL mode for better concurrent read performance and
    enforces foreign key constraints. The caller owns the connection
    and must close it.

    Parameters
    ----------
    name:
        Batch name without extension, e.g. ``"my2025tax"``.
    settings:
        Resolved application settings (provides data_dir).

    Returns
    -------
    sqlite3.Connection
        Open connection with WAL mode and foreign keys enabled.
    """
    path = get_batch_path(name, settings)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    # Ensure all tables exist — safe for existing files (IF NOT EXISTS).
    # This auto-migrates older .sirop files when new tables are added.
    create_tables(conn)
    # Apply column-level migrations that CREATE TABLE IF NOT EXISTS cannot handle.
    migrate_to_v5(conn)
    migrate_to_v6(conn)
    migrate_to_v7(conn)
    return conn


def get_active_batch_name(settings: Settings) -> str | None:
    """Return the name of the currently active batch, or None if unset."""
    active_file = settings.data_dir / ".active"
    if not active_file.exists():
        return None
    name = active_file.read_text(encoding="utf-8").strip()
    return name if name else None


def set_active_batch(name: str, settings: Settings) -> None:
    """Write the active batch name to DATA_DIR/.active.

    Raises
    ------
    ValueError
        If *name* contains path-traversal sequences or invalid characters.
    """
    _validate_batch_name(name)
    active_file = settings.data_dir / ".active"
    active_file.write_text(name, encoding="utf-8")


def get_user_importers_dir(settings: Settings) -> Path:
    """Return the shared custom importer registry directory.

    This directory (DATA_DIR/importers/) holds user-defined YAML configs that
    survive across batches. Callers must create it if absent before writing.
    """
    return settings.data_dir / "importers"
