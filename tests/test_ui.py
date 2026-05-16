"""Tests for the sirop.ui package.

Covers:
- TTY detection (non-TTY under pytest by default)
- ``ask`` / ``confirm`` raise ``NonInteractiveError`` off a TTY
- ``ask`` with a validator re-prompts on failure
- ``ask`` passes choices/default through to Rich Prompt
- ``make_table`` produces the expected house style
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from rich.prompt import Confirm, InvalidResponse, Prompt

from sirop.ui import NonInteractiveError, ask, confirm, is_tty, make_table

if TYPE_CHECKING:
    from collections.abc import Iterator


# ---------------------------------------------------------------------------
# TTY detection
# ---------------------------------------------------------------------------


def test_is_tty_false_under_pytest() -> None:
    """Pytest captures stdin/stdout so isatty() is False — confirms the helper
    reflects that, which is the basis for non-interactive error handling."""
    assert is_tty() is False


def test_ask_raises_non_interactive_off_tty() -> None:
    with pytest.raises(NonInteractiveError):
        ask("Wallet name?")


def test_confirm_raises_non_interactive_off_tty() -> None:
    with pytest.raises(NonInteractiveError):
        confirm("Proceed?")


# ---------------------------------------------------------------------------
# Force-TTY fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def force_tty(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Patch ``is_tty`` to return True for prompt-behaviour tests."""
    monkeypatch.setattr("sirop.ui.prompts.is_tty", lambda: True)
    yield


# ---------------------------------------------------------------------------
# ask — happy and re-prompt paths
# ---------------------------------------------------------------------------


def test_ask_returns_answer_when_validator_passes(
    monkeypatch: pytest.MonkeyPatch, force_tty: None
) -> None:
    monkeypatch.setattr(Prompt, "ask", classmethod(lambda cls, *a, **kw: "  Acme  "))
    assert ask("Wallet?", validator=lambda s: s.strip()) == "Acme"


def test_ask_re_prompts_until_validator_passes(
    monkeypatch: pytest.MonkeyPatch, force_tty: None
) -> None:
    answers = iter(["", "", "Ledger"])

    def fake_ask(cls: type[Prompt], /, *_a: object, **_kw: object) -> str:
        return next(answers)

    monkeypatch.setattr(Prompt, "ask", classmethod(fake_ask))

    def validate(s: str) -> str:
        if not s.strip():
            raise InvalidResponse("Wallet name cannot be empty.")
        return s

    assert ask("Wallet?", validator=validate) == "Ledger"


def test_ask_without_validator_returns_first_answer(
    monkeypatch: pytest.MonkeyPatch, force_tty: None
) -> None:
    monkeypatch.setattr(Prompt, "ask", classmethod(lambda cls, *a, **kw: "answer"))
    assert ask("Q?") == "answer"


def test_ask_forwards_choices_and_default(monkeypatch: pytest.MonkeyPatch, force_tty: None) -> None:
    seen: dict[str, object] = {}

    def fake_ask(cls: type[Prompt], /, _question: str, **kw: object) -> str:
        seen.update(kw)
        return "a"

    monkeypatch.setattr(Prompt, "ask", classmethod(fake_ask))
    ask("Pick", default="a", choices=["a", "b"])
    assert seen["default"] == "a"
    assert seen["choices"] == ["a", "b"]
    assert seen["show_default"] is True


# ---------------------------------------------------------------------------
# confirm — argument forwarding
# ---------------------------------------------------------------------------


def test_confirm_passes_default_through(monkeypatch: pytest.MonkeyPatch, force_tty: None) -> None:
    seen: dict[str, object] = {}

    def fake_ask(cls: type[Confirm], /, _question: str, **kw: object) -> bool:
        seen.update(kw)
        return True

    monkeypatch.setattr(Confirm, "ask", classmethod(fake_ask))
    assert confirm("Proceed?", default=True) is True
    assert seen["default"] is True


# ---------------------------------------------------------------------------
# make_table — house style
# ---------------------------------------------------------------------------


def test_make_table_house_style() -> None:
    table = make_table()
    assert table.box is None
    assert table.show_header is True
    assert table.header_style == "bold dim"
    assert table.pad_edge is False


def test_make_table_accepts_title() -> None:
    table = make_table(title="Holdings")
    assert table.title == "Holdings"
