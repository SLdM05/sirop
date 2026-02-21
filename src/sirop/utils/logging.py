"""
Logging configuration for sirop.

Call configure_logging() once at CLI entry. Every other module calls
get_logger(__name__) to obtain a named logger. Do not call
configure_logging() from library code (importers, engine, db, etc.).

Usage
-----
# At CLI entry point:
from sirop.utils.logging import configure_logging
configure_logging(verbose=args.verbose, debug=args.debug)

# In every module:
from sirop.utils.logging import get_logger
logger = get_logger(__name__)

# Around each pipeline stage:
from sirop.utils.logging import StageContext
with StageContext(batch_id="my2025tax", stage="normalize"):
    logger.info("Checking sap levels...")
"""

from __future__ import annotations

import logging
import re
import sys
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import TracebackType

# ──────────────────────────────────────────────────────────────
# Internal state
# ──────────────────────────────────────────────────────────────

_verbose: bool = False
_configured: bool = False

_ctx_batch_id: ContextVar[str] = ContextVar("batch_id", default="")
_ctx_stage: ContextVar[str] = ContextVar("stage", default="")

_ROOT = "sirop"

# ──────────────────────────────────────────────────────────────
# Redaction patterns
# ──────────────────────────────────────────────────────────────

# Bitcoin txid: exactly 64 lowercase hex characters
_RE_TXID = re.compile(r"\b[0-9a-f]{64}\b", re.IGNORECASE)

# Bitcoin addresses: bech32 (bc1...), legacy P2PKH (1...), P2SH (3...)
_RE_ADDRESS = re.compile(r"\b(bc1[a-z0-9]{6,87}|[13][a-zA-Z0-9]{25,34})\b")

# BTC amounts: up to 8 decimal places, optionally preceded/followed by BTC/btc
_RE_BTC_AMOUNT = re.compile(
    r"\b\d{1,10}\.\d{1,8}\s*(BTC|btc)\b" r"|\b(BTC|btc)\s*\d{1,10}\.\d{1,8}\b"
)

# CAD amounts near tax keywords (proceeds, acb, gain, loss, fee, value, rate).
# Requires either a leading $ sign or a decimal point so plain integer counts
# (e.g. "ACB engine on 2 event(s)") are not redacted as monetary amounts.
_RE_CAD_AMOUNT = re.compile(
    r"(?i)(proceeds|acb|gain|loss|fee|value|rate|cad|amount)[^\d$]{0,10}"
    r"(?:\$\s*\d[\d,]*\.?\d*|\d[\d,]*\.\d+)"
)


def _redact_cad(m: re.Match[str]) -> str:
    """Keep the tax keyword, replace the amount."""
    return f"{m.group(1)} [amount redacted]"


def _redact(message: str) -> str:
    """Replace sensitive values in a log message with placeholder tokens."""
    message = _RE_TXID.sub("[txid redacted]", message)
    message = _RE_ADDRESS.sub("[address redacted]", message)
    message = _RE_BTC_AMOUNT.sub("[amount redacted]", message)
    message = _RE_CAD_AMOUNT.sub(_redact_cad, message)
    return message


# ──────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────


class SensitiveDataFilter(logging.Filter):
    """Redacts PII from log messages when not in verbose mode.

    Replaces txids, Bitcoin addresses, BTC amounts, and CAD amounts near
    tax keywords with placeholder tokens. Attach to handlers, not loggers,
    so the original LogRecord is untouched for any other handler.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # getMessage() interpolates % args into the format string first,
        # so the full message is available for redaction. Clearing args
        # afterwards prevents double-interpolation by the formatter.
        record.msg = _redact(record.getMessage())
        record.args = ()
        return True


class _ContextInjectFilter(logging.Filter):
    """Injects batch_id and stage from ContextVars into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.batch_id = _ctx_batch_id.get()
        record.stage = _ctx_stage.get()
        return True


# ──────────────────────────────────────────────────────────────
# Formatters
# ──────────────────────────────────────────────────────────────


class _SiropFormatter(logging.Formatter):
    """Human-readable formatter with optional stage context prefix."""

    def __init__(self, *, debug: bool = False) -> None:
        super().__init__()
        self._debug = debug

    def format(self, record: logging.LogRecord) -> str:
        stage: str = getattr(record, "stage", "")
        stage_part = f"  {stage} " if stage else " "

        levelname_part = ""
        if record.levelno >= logging.WARNING:
            levelname_part = f"{record.levelname}  "

        if self._debug:
            # Show module name and full level for debug output
            name_short = record.name.removeprefix("sirop.")
            return f"[sirop][{record.levelname}] {name_short}  {record.getMessage()}"

        return f"[sirop]{stage_part}{levelname_part}{record.getMessage()}"


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────


def configure_logging(*, verbose: bool = False, debug: bool = False) -> None:
    """Configure sirop logging. Call exactly once at CLI entry.

    Parameters
    ----------
    verbose:
        Show sensitive values (txids, amounts, addresses) in log output.
        Prints a warning banner. Implied by debug=True.
    debug:
        Set log level to DEBUG and imply verbose=True.
    """
    global _verbose, _configured  # noqa: PLW0603
    if _configured:
        return

    _verbose = verbose or debug
    _configured = True

    root = logging.getLogger(_ROOT)
    root.setLevel(logging.DEBUG if debug else logging.INFO)
    root.propagate = False

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG if debug else logging.INFO)
    handler.setFormatter(_SiropFormatter(debug=debug))
    handler.addFilter(_ContextInjectFilter())

    if not _verbose:
        handler.addFilter(SensitiveDataFilter())

    root.addHandler(handler)

    if _verbose:
        _print_verbose_banner()


def _print_verbose_banner() -> None:
    banner = (
        "\n"
        "  ⚠  VERBOSE MODE: sensitive data (txids, amounts, addresses) is being logged.\n"
        "     Do not share this output. Run without --verbose for redacted logs.\n"
    )
    print(banner, file=sys.stderr)


def get_logger(name: str) -> logging.Logger:
    """Return a logger named sirop.<name>.

    Pass __name__ from the calling module. The sirop. prefix is added
    automatically so that configure_logging() controls the root handler.

    Example
    -------
    logger = get_logger(__name__)
    logger.info("Tapping Shakepay...")
    """
    # Strip the package prefix if already present to avoid sirop.sirop.x
    clean = name.removeprefix("sirop.")
    return logging.getLogger(f"{_ROOT}.{clean}")


# ──────────────────────────────────────────────────────────────
# Stage context manager
# ──────────────────────────────────────────────────────────────


class StageContext:
    """Context manager that injects batch_id and stage into all log records.

    Use around every pipeline stage invocation in the coordinator so that
    every log line emitted by importers, the normalizer, engines, and the
    repository automatically carries the current stage name and batch id.

    Example
    -------
    with StageContext(batch_id="my2025tax", stage="normalize"):
        normalizer.run(...)   # all logs inside carry batch_id and stage
    """

    def __init__(self, *, batch_id: str, stage: str) -> None:
        self._batch_id = batch_id
        self._stage = stage
        self._tokens: list[Any] = []

    def __enter__(self) -> StageContext:
        self._tokens = [
            _ctx_batch_id.set(self._batch_id),
            _ctx_stage.set(self._stage),
        ]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        for token in self._tokens:
            token.var.reset(token)
