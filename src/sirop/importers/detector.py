"""CSV format detector — used by ``sirop tap`` before any parsing.

Reads ``fingerprint_columns`` from every importer YAML in the config
directories and matches them against the headers of an incoming CSV.

Three use-cases
---------------
1. **Auto-detect** (no ``--source`` given): scan all known fingerprints,
   return an exact match.  Surface partial matches as hints when nothing
   matches fully.
2. **Validate** (``--source ndax`` given): confirm the file's headers
   contain all fingerprint columns for that source.  If not, report
   what's missing and suggest the format that actually fits.
3. **Format-drift detection**: even on a valid match, columns present in
   the CSV but absent from *all* known fingerprints are surfaced as
   ``unknown_headers`` so callers can emit a warning.

This module is pure data-in / data-out — it does no I/O beyond reading
YAML config files, and no parsing of CSV data rows.
"""

from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

import yaml

if TYPE_CHECKING:
    from pathlib import Path

# Minimum ratio of fingerprint columns that must be present for a config to
# be considered a "partial match" — surfaced as a hint, not a hard match.
_PARTIAL_MATCH_THRESHOLD: Final[float] = 0.5


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FormatCandidate:
    """One importer config loaded for detection."""

    source_name: str  # yaml stem, e.g. "ndax"
    display_name: str  # human label from config, e.g. "NDAX"
    fingerprint: frozenset[str]  # required CSV column names (subset of all_columns)
    all_columns: frozenset[str]  # every column name declared in the YAML (for drift detection)
    # Compiled regex patterns from ``known_column_patterns`` in the YAML.
    # Headers matching any pattern are excluded from unknown_headers even when
    # they are absent from all_columns (e.g. Sparrow's optional "Value (CAD)").
    column_patterns: tuple[re.Pattern[str], ...]


@dataclass(frozen=True)
class DetectionResult:
    """Result of auto-detecting the format of a CSV file.

    Attributes
    ----------
    matched:
        Source names whose *full* fingerprint is present in the CSV
        headers.  Ideally this has exactly one entry.
    partial:
        ``(source_name, ratio)`` pairs for configs where at least 50 %
        of the fingerprint columns are present, sorted by ratio
        descending.  Used to build helpful error hints.
    unknown_headers:
        CSV headers that appear in no known fingerprint.  A non-empty
        set may indicate format drift in an exchange export.
    """

    matched: list[str]
    partial: list[tuple[str, float]]
    unknown_headers: frozenset[str]


@dataclass(frozen=True)
class ValidationResult:
    """Result of validating a CSV against a *declared* source.

    Attributes
    ----------
    source_name:
        The source that was validated against.
    ok:
        True when all fingerprint columns are present.
    missing:
        Fingerprint columns absent from the CSV.  Empty when ``ok``.
    unknown_headers:
        CSV headers absent from *all* known fingerprints — possible
        sign of format evolution, even when ``ok`` is True.
    suggested:
        Another source name whose fingerprint fits the CSV better,
        populated only when ``ok`` is False.
    """

    source_name: str
    ok: bool
    missing: frozenset[str]
    unknown_headers: frozenset[str]
    suggested: str | None


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class FormatDetector:
    """Detects or validates the format of an incoming CSV file.

    Parameters
    ----------
    config_dirs:
        Directories to scan for ``*.yaml`` importer configs.  Each
        YAML must have a ``fingerprint_columns`` list to participate in
        detection; files without it are silently skipped.
        Dirs are scanned in order; a later dir can override an earlier
        one with the same stem name.

    Usage
    -----
    detector = FormatDetector([Path("config/importers")])
    result = detector.detect(headers)            # auto-detect
    result = detector.validate(headers, "ndax")  # validate declared source
    """

    def __init__(self, config_dirs: list[Path]) -> None:
        # source_name → FormatCandidate, in load order (later dirs win on clash)
        self._candidates: dict[str, FormatCandidate] = {}
        for config_dir in config_dirs:
            if config_dir.is_dir():
                self._load_dir(config_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def known_sources(self) -> list[str]:
        """Source names for all loaded configs with fingerprints."""
        return sorted(self._candidates)

    def display_name(self, source_name: str) -> str:
        """Human-readable label for *source_name*, or *source_name* itself."""
        candidate = self._candidates.get(source_name)
        return candidate.display_name if candidate else source_name

    def fingerprint(self, source_name: str) -> frozenset[str] | None:
        """Return the fingerprint column set for *source_name*, or None."""
        candidate = self._candidates.get(source_name)
        return candidate.fingerprint if candidate else None

    def detect(self, headers: set[str]) -> DetectionResult:
        """Return which importer config(s) match *headers*.

        A config matches when every column in its fingerprint appears in
        *headers*.  Partial matches (≥ 50 % of fingerprint present) are
        returned as suggestions.
        """
        matched: list[str] = []
        partial: list[tuple[str, float]] = []
        all_known: set[str] = set()

        for source_name, candidate in self._candidates.items():
            all_known |= candidate.all_columns
            present = candidate.fingerprint & headers
            ratio = len(present) / len(candidate.fingerprint) if candidate.fingerprint else 0.0
            if ratio == 1.0:
                matched.append(source_name)
            elif ratio >= _PARTIAL_MATCH_THRESHOLD:
                partial.append((source_name, ratio))

        partial.sort(key=lambda x: x[1], reverse=True)
        all_patterns = [p for c in self._candidates.values() for p in c.column_patterns]
        unknown = frozenset(
            h for h in (headers - all_known) if not any(p.fullmatch(h) for p in all_patterns)
        )
        return DetectionResult(matched=matched, partial=partial, unknown_headers=unknown)

    def validate(self, headers: set[str], source_name: str) -> ValidationResult:
        """Validate *headers* against the fingerprint of *source_name*.

        If the source has no fingerprint (unknown YAML stem), returns
        ``ok=True`` with empty missing set — the importer will surface
        any column errors during parsing.
        """
        candidate = self._candidates.get(source_name)
        if candidate is None:
            # No fingerprint available — can't disprove, let the importer try.
            return ValidationResult(
                source_name=source_name,
                ok=True,
                missing=frozenset(),
                unknown_headers=frozenset(),
                suggested=None,
            )

        all_known: set[str] = set()
        all_patterns: list[re.Pattern[str]] = []
        for c in self._candidates.values():
            all_known |= c.all_columns
            all_patterns.extend(c.column_patterns)

        missing = candidate.fingerprint - headers
        unknown = frozenset(
            h for h in (headers - all_known) if not any(p.fullmatch(h) for p in all_patterns)
        )

        # When validation fails, find a better-fitting source to suggest.
        suggested: str | None = None
        if missing:
            best = self.detect(headers)
            for candidate_name in best.matched:
                if candidate_name != source_name:
                    suggested = candidate_name
                    break
            if suggested is None and best.partial:
                top_name, _ = best.partial[0]
                if top_name != source_name:
                    suggested = top_name

        return ValidationResult(
            source_name=source_name,
            ok=len(missing) == 0,
            missing=frozenset(missing),
            unknown_headers=unknown,
            suggested=suggested,
        )

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _load_dir(self, config_dir: Path) -> None:
        for yaml_path in sorted(config_dir.glob("*.yaml")):
            raw: dict[str, Any] = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
            fingerprint_list: list[Any] = raw.get("fingerprint_columns", [])
            if not fingerprint_list:
                continue

            # Build the full set of declared column names for drift detection.
            # "Unknown" means unknown to the entire YAML, not just the fingerprint.
            all_cols: set[str] = set(str(c) for c in fingerprint_list)
            # columns: {logical_name: csv_column_name} — collect the CSV names.
            for csv_col in raw.get("columns", {}).values():
                all_cols.add(str(csv_col))
            if date_col := raw.get("date_column"):
                all_cols.add(str(date_col))
            for col in raw.get("ignored_columns", []):
                all_cols.add(str(col))

            patterns: list[re.Pattern[str]] = []
            for pattern_str in raw.get("known_column_patterns", []):
                with contextlib.suppress(re.error):
                    patterns.append(re.compile(str(pattern_str)))

            source_name = yaml_path.stem
            self._candidates[source_name] = FormatCandidate(
                source_name=source_name,
                display_name=str(raw.get("name", source_name)),
                fingerprint=frozenset(str(c) for c in fingerprint_list),
                all_columns=frozenset(all_cols),
                column_patterns=tuple(patterns),
            )
