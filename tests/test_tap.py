"""Tests for the tap command's importer registry and folder-tap feature.

The bug this file guards against
---------------------------------
After adding a new importer (YAML + class), a developer can forget to wire
it into ``_IMPORTER_REGISTRY`` in ``sirop.cli.tap``.  The symptom is a
successful format-detection followed by:

    error: format detected as 'X' but no importer is implemented for 'x' yet.

These tests catch that in two complementary ways:

1. **Per-source regression pins** — one assertion per implemented importer.
   Adding a new importer requires adding a corresponding pin here; removing
   an importer from the registry immediately fails the matching test.

2. **Registry-to-YAML consistency** — every entry in ``_IMPORTER_REGISTRY``
   must have a matching ``config/importers/<source>.yaml`` and the factory
   must construct a working importer from it.  This catches the reverse
   mistake: registering a source whose YAML was deleted or renamed.
"""

from pathlib import Path

import pytest

from sirop.cli.tap import _IMPORTER_REGISTRY, _handle_tap_folder
from sirop.config.settings import Settings

CONFIG_DIR = Path(__file__).parent.parent / "config" / "importers"

# ---------------------------------------------------------------------------
# Per-source regression pins
# ---------------------------------------------------------------------------


def test_ndax_is_registered() -> None:
    """Regression: ndax must always appear in _IMPORTER_REGISTRY."""
    assert "ndax" in _IMPORTER_REGISTRY


def test_shakepay_is_registered() -> None:
    """Regression: shakepay must always appear in _IMPORTER_REGISTRY.

    Was missing after the initial Shakepay importer implementation, causing
    ``sirop tap`` to print "no importer is implemented for 'shakepay' yet"
    even though the class and YAML both existed.
    """
    assert "shakepay" in _IMPORTER_REGISTRY


# ---------------------------------------------------------------------------
# Registry ↔ YAML consistency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source", list(_IMPORTER_REGISTRY))
def test_registry_entry_has_yaml(source: str) -> None:
    """Every registered source must have a matching config YAML."""
    yaml_path = CONFIG_DIR / f"{source}.yaml"
    assert yaml_path.exists(), (
        f"Registered source {source!r} has no YAML at {yaml_path}. "
        "Either add the YAML or remove the registry entry."
    )


@pytest.mark.parametrize("source,factory", list(_IMPORTER_REGISTRY.items()))
def test_registry_factory_loads(source: str, factory: object) -> None:
    """Every registered factory must load its YAML and return an importer."""
    yaml_path = CONFIG_DIR / f"{source}.yaml"
    importer = factory(yaml_path)  # type: ignore[operator]
    assert importer is not None, f"Factory for {source!r} returned None"


# ---------------------------------------------------------------------------
# Folder-tap: _handle_tap_folder
# ---------------------------------------------------------------------------

# Synthetic header rows containing the fingerprint columns for each format.
_SHAKEPAY_HEADER = "Transaction Type,Amount Debited,Debit Currency,Buy / Sell Rate,Spot Rate"
_SPARROW_HEADER = "Date (UTC),Value,Balance,Fee,Txid"


def _make_settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path)


class TestHandleTapFolderEmpty:
    def test_empty_folder_returns_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Empty directory: emit no-files message and return 0."""
        folder = tmp_path / "imports"
        folder.mkdir()
        rc = _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        assert rc == 0
        out = capsys.readouterr().out
        assert "No CSV files found" in out


class TestHandleTapFolderDetection:
    def test_detected_files_shown_in_listing(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Identified files appear in the listing with their display name."""
        folder = tmp_path / "imports"
        folder.mkdir()
        (folder / "shk.csv").write_text(_SHAKEPAY_HEADER + "\n")
        (folder / "spw.csv").write_text(_SPARROW_HEADER + "\n")
        monkeypatch.setattr("builtins.input", lambda _: "n")
        _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        out = capsys.readouterr().out
        assert "Shakepay" in out
        assert "Sparrow Wallet" in out

    def test_unknown_files_shown_as_unknown(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Unrecognised CSVs appear with 'unknown (will be skipped)'."""
        folder = tmp_path / "imports"
        folder.mkdir()
        (folder / "shk.csv").write_text(_SHAKEPAY_HEADER + "\n")
        (folder / "random.csv").write_text("ColA,ColB,ColC\n")
        monkeypatch.setattr("builtins.input", lambda _: "n")
        _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        out = capsys.readouterr().out
        assert "unknown (will be skipped)" in out

    def test_all_unknown_returns_one(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """All-unknown folder: emit error message and return 1."""
        folder = tmp_path / "imports"
        folder.mkdir()
        (folder / "random.csv").write_text("ColA,ColB,ColC\n")
        rc = _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        assert rc == 1
        out = capsys.readouterr().out
        assert "No files could be identified" in out


class TestHandleTapFolderConfirmation:
    def test_decline_returns_zero_and_emits_aborted(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Answering 'n' cancels without tapping; returns 0."""
        folder = tmp_path / "imports"
        folder.mkdir()
        (folder / "shk.csv").write_text(_SHAKEPAY_HEADER + "\n")
        monkeypatch.setattr("builtins.input", lambda _: "n")
        rc = _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        assert rc == 0
        out = capsys.readouterr().out
        assert "cancelled" in out.lower()

    def test_eof_on_prompt_treated_as_no(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Non-interactive stdin (EOFError) is treated as 'no', returns 0."""
        folder = tmp_path / "imports"
        folder.mkdir()
        (folder / "shk.csv").write_text(_SHAKEPAY_HEADER + "\n")

        def _raise_eof(prompt: str) -> str:
            raise EOFError

        monkeypatch.setattr("builtins.input", _raise_eof)
        rc = _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        assert rc == 0

    def test_yes_taps_only_identified_files(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Answering 'y' calls handle_tap for each identified file only."""
        folder = tmp_path / "imports"
        folder.mkdir()
        (folder / "shk.csv").write_text(_SHAKEPAY_HEADER + "\n")
        (folder / "spw.csv").write_text(_SPARROW_HEADER + "\n")
        (folder / "random.csv").write_text("ColA,ColB\n")

        monkeypatch.setattr("builtins.input", lambda _: "y")

        tapped: list[Path] = []

        def _fake_handle_tap(
            file_path: Path,
            source: str | None,
            wallet: str | None,
            settings: Settings,
        ) -> int:
            tapped.append(file_path)
            return 0

        monkeypatch.setattr("sirop.cli.tap.handle_tap", _fake_handle_tap)
        rc = _handle_tap_folder(folder, None, None, _make_settings(tmp_path))
        assert rc == 0
        tapped_names = {p.name for p in tapped}
        assert tapped_names == {"shk.csv", "spw.csv"}
        assert "random.csv" not in tapped_names
