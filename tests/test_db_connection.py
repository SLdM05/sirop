"""Tests for batch name validation in db/connection.py.

These tests verify that path-traversal sequences, empty strings, and
shell-special characters are rejected, while normal batch names pass.
"""

import pytest

from sirop.db.connection import _validate_batch_name


class TestValidateBatchName:
    """_validate_batch_name raises ValueError for invalid names, passes for valid ones."""

    # ── Valid names ────────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "name",
        [
            "my2025tax",
            "tax-2025",
            "tax_2025",
            "a",
            "A",
            "0abc",
            "ABC123",
            "a" * 64,  # max length
            "my-tax_2025-batch",
        ],
    )
    def test_valid_names_pass(self, name: str) -> None:
        _validate_batch_name(name)  # must not raise

    # ── Path traversal ─────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "name",
        [
            "../../../tmp/attack",
            "../sibling",
            "foo/bar",
            "foo\\bar",
            "./local",
            "/absolute",
        ],
    )
    def test_path_traversal_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid batch name"):
            _validate_batch_name(name)

    # ── Empty / whitespace ─────────────────────────────────────────────────────

    @pytest.mark.parametrize("name", ["", " ", "\t", "\n"])
    def test_empty_or_whitespace_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid batch name"):
            _validate_batch_name(name)

    # ── Shell-special and SQL characters ──────────────────────────────────────

    @pytest.mark.parametrize(
        "name",
        [
            "'; DROP TABLE dispositions;--",
            "name;rm -rf /",
            "foo$bar",
            "foo`bar`",
            "foo bar",  # space
            "foo.bar",  # dot
            "foo@bar",
            "foo!bar",
            "foo(bar)",
        ],
    )
    def test_special_characters_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid batch name"):
            _validate_batch_name(name)

    # ── Length limit ──────────────────────────────────────────────────────────

    def test_too_long_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid batch name"):
            _validate_batch_name("a" * 65)  # one over the 64-char max

    def test_exactly_64_chars_passes(self) -> None:
        _validate_batch_name("a" * 64)  # must not raise
