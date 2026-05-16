"""Interactive prompt helpers wrapping Rich Prompt/Confirm.

These replace bare ``input(...)`` calls in CLI handlers so prompts have
consistent styling, behave predictably off a TTY, and become the single
seam the future Textual TUI will swap.

Pattern: flags-first, prompt-on-missing. CLI handlers accept all required
inputs as flags; when a required value is missing *and* stdin/stdout are a
TTY, the handler calls ``ask`` / ``confirm`` to obtain it interactively.
Off a TTY (CI, piped), the call raises :class:`NonInteractiveError`, which
the handler maps to a clear "missing required flag" error.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from rich.prompt import Confirm, InvalidResponse, Prompt

from sirop.utils.console import err as _err
from sirop.utils.console import out as _out

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


class NonInteractiveError(RuntimeError):
    """Raised when an interactive prompt is requested off a TTY.

    Handlers should catch this at the CLI boundary and emit a structured
    "missing required flag" error instead of letting it propagate.
    """


def is_tty() -> bool:
    """Return True when both stdin and stdout are connected to a terminal."""
    return sys.stdin.isatty() and sys.stdout.isatty()


def ask(
    question: str,
    *,
    default: str | None = None,
    choices: Sequence[str] | None = None,
    validator: Callable[[str], str] | None = None,
    password: bool = False,
) -> str:
    """Prompt the user for a string value with consistent styling.

    Re-prompts on validator failure: the validator receives the raw answer
    and must either return the (possibly normalised) value or raise
    :class:`rich.prompt.InvalidResponse` with a user-facing message.

    Raises
    ------
    NonInteractiveError
        If stdin or stdout is not a TTY.
    """
    if not is_tty():
        raise NonInteractiveError(question)

    choice_list = list(choices) if choices else None
    while True:
        if default is None:
            answer = Prompt.ask(question, console=_out, choices=choice_list, password=password)
        else:
            answer = Prompt.ask(
                question,
                console=_out,
                choices=choice_list,
                password=password,
                default=default,
                show_default=True,
            )
        if validator is None:
            return answer
        try:
            return validator(answer)
        except InvalidResponse as exc:
            _err.print(f"[bold red]{exc}[/bold red]")


def confirm(question: str, *, default: bool = False) -> bool:
    """Yes/no prompt with consistent styling.

    Raises
    ------
    NonInteractiveError
        If stdin or stdout is not a TTY.
    """
    if not is_tty():
        raise NonInteractiveError(question)

    return Confirm.ask(question, console=_out, default=default)
