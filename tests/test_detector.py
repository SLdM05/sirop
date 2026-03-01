"""Unit tests for the CSV format detector.

All tests use synthetic header sets — no real CSV files are read.
The detector is initialised against the real built-in config dir so
that fingerprint changes in YAML are caught automatically.
"""

from pathlib import Path

import pytest

from sirop.importers.detector import FormatDetector

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONFIG_DIR = Path(__file__).parent.parent / "config" / "importers"

# Real headers from each exchange's CSV export (synthetic values, real structure).
NDAX_HEADERS: set[str] = {"ASSET", "ASSET_CLASS", "AMOUNT", "BALANCE", "TYPE", "TX_ID", "DATE"}
SHAKEPAY_HEADERS: set[str] = {
    "Date",
    "Amount Debited",
    "Asset Debited",
    "Amount Credited",
    "Asset Credited",
    "Market Value",
    "Market Value Currency",
    "Book Cost",
    "Book Cost Currency",
    "Type",
    "Spot Rate",
    "Buy / Sell Rate",
    "Description",
}
UNKNOWN_HEADERS: set[str] = {"OrderID", "Quantity", "Symbol", "Side", "Price", "Timestamp"}


@pytest.fixture(scope="module")
def detector() -> FormatDetector:
    return FormatDetector([CONFIG_DIR])


# ---------------------------------------------------------------------------
# Loader sanity
# ---------------------------------------------------------------------------


def test_known_sources_includes_ndax(detector: FormatDetector) -> None:
    assert "ndax" in detector.known_sources


def test_known_sources_includes_shakepay(detector: FormatDetector) -> None:
    assert "shakepay" in detector.known_sources


def test_display_name_ndax(detector: FormatDetector) -> None:
    assert detector.display_name("ndax") == "NDAX"


def test_display_name_shakepay(detector: FormatDetector) -> None:
    assert detector.display_name("shakepay") == "Shakepay"


def test_display_name_unknown_falls_back(detector: FormatDetector) -> None:
    assert detector.display_name("unknown_exchange") == "unknown_exchange"


# ---------------------------------------------------------------------------
# Auto-detection — exact matches
# ---------------------------------------------------------------------------


def test_detect_ndax_exact(detector: FormatDetector) -> None:
    result = detector.detect(NDAX_HEADERS)
    assert result.matched == ["ndax"]
    assert result.partial == []


def test_detect_shakepay_exact(detector: FormatDetector) -> None:
    result = detector.detect(SHAKEPAY_HEADERS)
    assert result.matched == ["shakepay"]
    assert result.partial == []


def test_detect_unknown_headers_no_match(detector: FormatDetector) -> None:
    result = detector.detect(UNKNOWN_HEADERS)
    assert result.matched == []


def test_detect_empty_headers_no_match(detector: FormatDetector) -> None:
    result = detector.detect(set())
    assert result.matched == []


# ---------------------------------------------------------------------------
# Auto-detection — unknown_headers field
# ---------------------------------------------------------------------------


def test_detect_ndax_no_unknown_headers(detector: FormatDetector) -> None:
    """NDAX headers are all known — no unexpected columns."""
    result = detector.detect(NDAX_HEADERS)
    assert result.unknown_headers == frozenset()


def test_detect_extra_column_flagged(detector: FormatDetector) -> None:
    """An extra column not in any fingerprint must appear in unknown_headers."""
    headers_with_extra = NDAX_HEADERS | {"MYSTERY_COLUMN"}
    result = detector.detect(headers_with_extra)
    assert "MYSTERY_COLUMN" in result.unknown_headers
    # Should still match NDAX (all fingerprint cols present)
    assert "ndax" in result.matched


# ---------------------------------------------------------------------------
# Auto-detection — partial matches (format drift / wrong report type)
# ---------------------------------------------------------------------------


def test_detect_partial_ndax_gives_suggestion(detector: FormatDetector) -> None:
    """Five of seven NDAX columns present → partial match, not exact."""
    partial_headers = {"ASSET", "ASSET_CLASS", "AMOUNT", "TYPE", "DATE"}  # missing BALANCE, TX_ID
    result = detector.detect(partial_headers)
    assert "ndax" not in result.matched
    partial_names = [name for name, _ in result.partial]
    assert "ndax" in partial_names


def test_detect_partial_ratio_between_zero_and_one(detector: FormatDetector) -> None:
    partial_headers = {"ASSET", "ASSET_CLASS", "AMOUNT"}  # 3/7 NDAX cols
    result = detector.detect(partial_headers)
    # 3/7 ≈ 43 % → below the 50 % threshold, should NOT appear in partial
    partial_names = [name for name, _ in result.partial]
    assert "ndax" not in partial_names


def test_detect_partial_sorted_descending(detector: FormatDetector) -> None:
    """Partial matches must be sorted by ratio, best first."""
    # Mix enough of one fingerprint to create a partial
    partial_headers = {"ASSET", "ASSET_CLASS", "AMOUNT", "TYPE", "TX_ID"}  # 5/7 NDAX
    result = detector.detect(partial_headers)
    ratios = [r for _, r in result.partial]
    assert ratios == sorted(ratios, reverse=True)


# ---------------------------------------------------------------------------
# Validation — correct source declared
# ---------------------------------------------------------------------------


def test_validate_ndax_correct(detector: FormatDetector) -> None:
    result = detector.validate(NDAX_HEADERS, "ndax")
    assert result.ok is True
    assert result.missing == frozenset()
    assert result.suggested is None


def test_validate_shakepay_correct(detector: FormatDetector) -> None:
    result = detector.validate(SHAKEPAY_HEADERS, "shakepay")
    assert result.ok is True
    assert result.missing == frozenset()


# ---------------------------------------------------------------------------
# Validation — wrong source declared (user error)
# ---------------------------------------------------------------------------


def test_validate_ndax_file_declared_as_shakepay(detector: FormatDetector) -> None:
    """User passes --source shakepay but gives an NDAX file."""
    result = detector.validate(NDAX_HEADERS, "shakepay")
    assert result.ok is False
    assert len(result.missing) > 0


def test_validate_wrong_source_suggests_correct(detector: FormatDetector) -> None:
    """When validation fails, suggested should point to the actual format."""
    result = detector.validate(NDAX_HEADERS, "shakepay")
    assert result.suggested == "ndax"


def test_validate_shakepay_file_declared_as_ndax(detector: FormatDetector) -> None:
    result = detector.validate(SHAKEPAY_HEADERS, "ndax")
    assert result.ok is False
    assert result.suggested == "shakepay"


# ---------------------------------------------------------------------------
# Validation — extra/unknown columns (format drift warning scenario)
# ---------------------------------------------------------------------------


def test_validate_unknown_columns_still_ok(detector: FormatDetector) -> None:
    """Extra columns not in any fingerprint → ok=True, but unknown_headers non-empty."""
    headers_with_extra = NDAX_HEADERS | {"NEW_NDAX_COLUMN_2026"}
    result = detector.validate(headers_with_extra, "ndax")
    assert result.ok is True
    assert "NEW_NDAX_COLUMN_2026" in result.unknown_headers


# ---------------------------------------------------------------------------
# Validation — unknown source name
# ---------------------------------------------------------------------------


def test_validate_unknown_source_returns_ok(detector: FormatDetector) -> None:
    """An unregistered source has no fingerprint; we can't disprove it."""
    result = detector.validate(UNKNOWN_HEADERS, "koinly")
    assert result.ok is True
    assert result.missing == frozenset()


# ---------------------------------------------------------------------------
# Multiple config dirs (custom importer overlay)
# ---------------------------------------------------------------------------


def test_empty_config_dir_ignored(tmp_path: Path) -> None:
    """A config dir with no YAML files is silently skipped."""
    detector = FormatDetector([tmp_path, CONFIG_DIR])
    assert "ndax" in detector.known_sources


def test_nonexistent_config_dir_ignored(tmp_path: Path) -> None:
    """A config dir that doesn't exist on disk is silently skipped."""
    missing = tmp_path / "does_not_exist"
    detector = FormatDetector([missing, CONFIG_DIR])
    assert "ndax" in detector.known_sources


def test_yaml_without_fingerprint_is_skipped(tmp_path: Path) -> None:
    """A YAML file with no fingerprint_columns does not add a candidate."""
    (tmp_path / "myexchange.yaml").write_text(
        "name: MyExchange\nfee_model: explicit\n", encoding="utf-8"
    )
    detector = FormatDetector([tmp_path])
    assert "myexchange" not in detector.known_sources


def test_custom_dir_overrides_builtin(tmp_path: Path) -> None:
    """Later config dir can replace a built-in fingerprint."""
    (tmp_path / "ndax.yaml").write_text(
        "name: NDAX Custom\nfingerprint_columns:\n  - CUSTOM_COL\n", encoding="utf-8"
    )
    # Custom dir is listed second — it wins on name collision
    detector = FormatDetector([CONFIG_DIR, tmp_path])
    result = detector.detect({"CUSTOM_COL"})
    assert "ndax" in result.matched
    # Original NDAX headers should no longer match the overridden fingerprint
    original = detector.detect(NDAX_HEADERS)
    assert "ndax" not in original.matched
