"""Abstract base importer, config loader, and importer-specific exceptions."""

import csv
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import yaml

from sirop.models.importer import ImporterConfig
from sirop.models.raw import RawTransaction

# ---------------------------------------------------------------------------
# Importer-specific exceptions
# ---------------------------------------------------------------------------


class ImporterError(Exception):
    """Base class for all importer errors."""


class MissingColumnError(ImporterError):
    """A column expected by the config is absent from the CSV."""


class InvalidCSVFormatError(ImporterError):
    """The CSV structure or a field value does not match the expected format."""


# ---------------------------------------------------------------------------
# strptime format allowlist  (security-input-hardening.md §4)
# ---------------------------------------------------------------------------

# Only tokens actually used by the four supported importers are permitted.
# Any other token would either silently mis-parse dates or accept garbage input.
_ALLOWED_STRPTIME_TOKENS: Final[frozenset[str]] = frozenset(
    {
        "%Y",
        "%m",
        "%d",  # date parts
        "%H",
        "%M",
        "%S",  # time parts
        "%f",  # microseconds
        "%z",  # UTC offset  (+0000 / +00:00 / Z)
        "%Z",  # timezone name
        "-",
        "T",
        ":",
        ".",
        " ",
        "+",  # separators / offset sign
    }
)


def _validate_date_format(fmt: str) -> None:
    """Raise ``InvalidCSVFormatError`` if *fmt* contains disallowed strptime tokens.

    The sentinel value ``"iso8601"`` is always accepted (handled separately by
    ``_parse_timestamp`` via ``datetime.fromisoformat``).
    """
    if fmt == "iso8601":
        return
    remaining = fmt
    while remaining:
        if remaining.startswith("%"):
            token = remaining[:2]
            if token not in _ALLOWED_STRPTIME_TOKENS:
                raise InvalidCSVFormatError(
                    f"date_format contains disallowed strptime token {token!r}. "
                    f"Allowed tokens: {sorted(_ALLOWED_STRPTIME_TOKENS)}"
                )
            remaining = remaining[2:]
        else:
            char = remaining[0]
            if char not in _ALLOWED_STRPTIME_TOKENS:
                raise InvalidCSVFormatError(
                    f"date_format contains disallowed character {char!r}. "
                    f"Allowed separators: "
                    f"{sorted(c for c in _ALLOWED_STRPTIME_TOKENS if not c.startswith('%'))}"
                )
            remaining = remaining[1:]


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------


def load_importer_config(yaml_path: Path, source_name: str | None = None) -> ImporterConfig:
    """Load and return an ``ImporterConfig`` from a YAML file.

    Args:
        yaml_path: Path to the importer YAML (e.g. ``config/importers/ndax.yaml``).
        source_name: Machine-readable identifier; defaults to the YAML stem
            (``"ndax"`` from ``ndax.yaml``).
    """
    # yaml.safe_load returns an untyped structure; Any is intentional here.
    raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text())
    if source_name is None:
        source_name = yaml_path.stem

    fiat_currencies: tuple[str, ...] = tuple(str(c) for c in raw.get("fiat_currencies", []))

    date_format = str(raw["date_format"])
    _validate_date_format(date_format)  # fail fast on disallowed tokens

    return ImporterConfig(
        name=str(raw["name"]),
        source_name=source_name,
        date_column=str(raw["date_column"]),
        date_format=date_format,
        columns={str(k): str(v) for k, v in raw["columns"].items()},
        transaction_type_map={
            str(k): str(v) for k, v in raw.get("transaction_type_map", {}).items()
        },
        fee_model=str(raw.get("fee_model", "explicit")),
        group_by_column=str(raw["group_by_column"]) if raw.get("group_by_column") else None,
        fiat_currencies=fiat_currencies,
    )


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class BaseImporter(ABC):
    """Abstract base for all CSV importers.

    Subclasses implement ``parse()`` and may use ``_read_csv()`` and
    ``_parse_timestamp()`` from this base.
    """

    def __init__(self, config: ImporterConfig) -> None:
        self._config = config

    @abstractmethod
    def parse(self, csv_path: Path) -> list[RawTransaction]:
        """Parse *csv_path* into a list of ``RawTransaction`` objects.

        Results must be sorted chronologically (ascending timestamp).
        """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _read_csv(self, csv_path: Path) -> list[dict[str, str]]:
        """Read a CSV file and return rows as plain string dicts.

        Skips rows that are entirely empty (common in NDAX exports that pad
        the file to a fixed number of rows with blank lines).
        """
        if not csv_path.exists():
            raise InvalidCSVFormatError(f"CSV file not found: {csv_path}")

        with csv_path.open(newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            rows = []
            for row in reader:
                # Skip blank rows (all string values are empty).
                # csv.DictReader places overflow columns under a None key with a
                # list value — guard with isinstance so we never call .strip() on
                # a list and crash on malformed/wide CSV files.
                if all(isinstance(v, str) and not v.strip() for v in row.values()):
                    continue
                rows.append(dict(row))
        return rows

    def _validate_columns(self, row: dict[str, str], required_keys: set[str]) -> None:
        """Raise ``MissingColumnError`` if any required CSV header is absent."""
        present = set(row.keys())
        missing = required_keys - present
        if missing:
            raise MissingColumnError(
                f"{self._config.name} CSV is missing required columns: {sorted(missing)}"
            )

    def _parse_timestamp(self, value: str) -> datetime:
        """Parse a timestamp string according to the configured date_format.

        When ``date_format`` is ``"iso8601"``, uses ``datetime.fromisoformat()``
        which handles the trailing ``Z`` and millisecond/microsecond precision
        natively (Python 3.11+).  Otherwise falls back to ``strptime``.

        Naïve timestamps (no timezone) are assumed to be UTC.
        """
        value = value.strip()
        try:
            if self._config.date_format == "iso8601":
                # Python 3.11+ accepts the Z suffix directly.
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            else:
                dt = datetime.strptime(value, self._config.date_format)
        except ValueError as exc:
            raise InvalidCSVFormatError(
                f"Cannot parse timestamp {value!r} (format: {self._config.date_format!r}): {exc}"
            ) from exc

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
