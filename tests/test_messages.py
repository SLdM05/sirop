"""Tests for the sirop messaging system.

Covers:
- enum/YAML sync: every MessageCode has a YAML entry and vice versa
- emit() error paths: unknown code, missing kwarg, unknown category
- category routing: error/warning → stderr with prefix; output/fluff → stdout, no prefix
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

import pytest
import yaml

from sirop.models.messages import MessageCode
from sirop.utils.messages import _CATALOG_PATH, _load_catalog, emit

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _yaml_keys() -> frozenset[str]:
    with _CATALOG_PATH.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return frozenset(raw["messages"].keys())


# ---------------------------------------------------------------------------
# Sync tests
# ---------------------------------------------------------------------------


def test_every_message_code_has_yaml_entry() -> None:
    """Each MessageCode value must have a matching key in messages.yaml."""
    yaml_keys = _yaml_keys()
    missing = [code for code in MessageCode if str(code) not in yaml_keys]
    assert not missing, (
        f"MessageCode values with no YAML entry: {missing}. "
        "Add the missing entries to config/messages.yaml."
    )


def test_no_orphaned_yaml_entries() -> None:
    """Every key in messages.yaml must have a matching MessageCode value."""
    enum_values = frozenset(str(code) for code in MessageCode)
    orphans = [key for key in _yaml_keys() if key not in enum_values]
    assert not orphans, (
        f"YAML keys with no MessageCode: {orphans}. "
        "Add the missing entries to src/sirop/models/messages.py or remove from YAML."
    )


# ---------------------------------------------------------------------------
# emit() error paths
# ---------------------------------------------------------------------------


def test_emit_unknown_code_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """emit() with a code not in the catalog must raise RuntimeError."""
    monkeypatch.setattr("sirop.utils.messages._CATALOG_PATH", tmp_path / "messages.yaml")
    (tmp_path / "messages.yaml").write_text(
        "messages:\n  output.valid:\n    category: output\n    text: ok\n",
        encoding="utf-8",
    )
    # Clear the lru_cache so the patched path is used.
    _load_catalog.cache_clear()

    class _FakeCode(str):
        """A fake code that is not in the catalog."""

        def __str__(self) -> str:
            return "nonexistent.code"

    with pytest.raises(RuntimeError, match="no catalog entry for message code"):
        emit(_FakeCode())  # type: ignore[arg-type]

    # Restore the real catalog for subsequent tests.
    _load_catalog.cache_clear()


def test_emit_missing_kwarg_raises() -> None:
    """emit() called without a required kwarg must raise RuntimeError."""
    # TAP_ERROR_FILE_NOT_FOUND requires {path}
    with pytest.raises(RuntimeError, match="missing required kwarg"):
        emit(MessageCode.TAP_ERROR_FILE_NOT_FOUND)  # missing path=


def test_emit_unknown_category_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """emit() when the catalog entry has an unknown category must raise RuntimeError."""
    (tmp_path / "messages.yaml").write_text(
        "messages:\n  test.bad_category:\n    category: bogus\n    text: hello\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("sirop.utils.messages._CATALOG_PATH", tmp_path / "messages.yaml")
    _load_catalog.cache_clear()

    class _FakeCode(str):
        def __str__(self) -> str:
            return "test.bad_category"

    with pytest.raises(RuntimeError, match="unknown message category"):
        emit(_FakeCode())  # type: ignore[arg-type]

    _load_catalog.cache_clear()


# ---------------------------------------------------------------------------
# Category routing
# ---------------------------------------------------------------------------


def test_error_routes_to_stderr_with_prefix(capsys: pytest.CaptureFixture[str]) -> None:
    """Error messages go to stderr with 'error [Exxx]:' prefix."""
    emit(MessageCode.BATCH_ERROR_NO_ACTIVE)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error [E001]:")
    assert "No active batch" in captured.err


def test_warning_routes_to_stderr_with_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Warning messages go to stderr with 'warning [Wxxx]:' prefix."""
    (tmp_path / "messages.yaml").write_text(
        "messages:\n  test.warn:\n    category: warning\n    code: W001\n    text: test warning\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("sirop.utils.messages._CATALOG_PATH", tmp_path / "messages.yaml")
    _load_catalog.cache_clear()

    class _FakeCode(str):
        def __str__(self) -> str:
            return "test.warn"

    emit(_FakeCode())  # type: ignore[arg-type]
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("warning [W001]:")
    assert "test warning" in captured.err

    _load_catalog.cache_clear()


def test_output_routes_to_stdout_no_prefix(capsys: pytest.CaptureFixture[str]) -> None:
    """Output messages go to stdout with no prefix or code."""
    emit(MessageCode.SWITCH_ACTIVATED, name="my2025tax")
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "my2025tax" in captured.out
    # No prefix markers
    assert "error" not in captured.out
    assert "warning" not in captured.out
    assert "[" not in captured.out


def test_fluff_routes_to_stdout_no_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Fluff messages go to stdout with no prefix or code."""
    (tmp_path / "messages.yaml").write_text(
        "messages:\n  test.fluff:\n    category: fluff\n    text: Checking sap levels...\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("sirop.utils.messages._CATALOG_PATH", tmp_path / "messages.yaml")
    _load_catalog.cache_clear()

    class _FakeCode(str):
        def __str__(self) -> str:
            return "test.fluff"

    emit(_FakeCode())  # type: ignore[arg-type]
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "Checking sap levels..." in captured.out

    _load_catalog.cache_clear()


def test_error_without_code_has_no_bracket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Error entries without a 'code' field emit 'error: text' (no brackets)."""
    (tmp_path / "messages.yaml").write_text(
        "messages:\n  test.nocode:\n    category: error\n    text: bare error\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("sirop.utils.messages._CATALOG_PATH", tmp_path / "messages.yaml")
    _load_catalog.cache_clear()

    class _FakeCode(str):
        def __str__(self) -> str:
            return "test.nocode"

    emit(_FakeCode())  # type: ignore[arg-type]
    captured = capsys.readouterr()
    assert captured.err == "error: bare error\n"

    _load_catalog.cache_clear()


def test_kwarg_substitution_in_output(capsys: pytest.CaptureFixture[str]) -> None:
    """Kwargs are correctly substituted in the output text."""
    emit(MessageCode.LIST_BATCH_ITEM, name="my2025tax", marker=" *")
    captured = capsys.readouterr()
    assert "my2025tax *" in captured.out
