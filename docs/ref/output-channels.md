---
verified-at: 872b414
tracks:
  - src/sirop/utils/messages.py
  - src/sirop/utils/logging.py
  - src/sirop/models/messages.py
  - config/messages.yaml
anchors:
  - path: src/sirop/utils/messages.py
    lines: 68-110
    hash: 3002b2a4abef
---
# Output channels

sirop has two distinct output channels. Never mix them.

## Channel 1 — `emit()` (user-facing results)

All structured user-visible output goes through `emit()`:

```python
from sirop.utils.messages import emit
from sirop.models.messages import MessageCode

emit(MessageCode.TAP_SUCCESS, count=42, filename="export.csv", fmt="Shakepay",
     batch="my2025tax", skip_note="")
```

Message text lives in `config/messages.yaml`, keyed by the dotted code that
matches the `MessageCode` StrEnum value. Categories control routing:

| Category | Output | Code shown? | Use for |
|----------|--------|-------------|---------|
| `error`   | stderr | yes (`[E001]`) | Unrecoverable failures |
| `warning` | stderr | yes (`[W001]`) | User-visible data quality issues |
| `output`  | stdout | no | Command results, confirmations, hints |
| `fluff`   | stdout | no | Maple-themed personality messages |

### Rules

- `emit()` is the sole source of structured user output. Never `print()`
  directly in CLI handlers.
- `error` and `warning` display their `[Exxx]`/`[Wxxx]` code so users can
  quote them in support requests.
- `output` and `fluff` display clean text only — no prefix, no code.
- Tax output (Schedule 3, TP-21.4.39-V, etc.) never goes through `emit()` —
  it is written directly to files by the reports module, personality-free.

### Adding a new message

1. Add an entry to `config/messages.yaml` with `category`, `text`, and (for
   errors/warnings) a `code`.
2. Add a matching constant to `MessageCode` in
   `src/sirop/models/messages.py`.
3. Call `emit(MessageCode.YOUR_CODE, **kwargs)` at the call site.

### Sentinel exceptions in CLI handlers

`_TapError` and `_BoilError` carry a `MessageCode` + kwargs instead of a
pre-formatted string. The handler boundary catches them and calls `emit()`:

```python
# raise site — no formatting here
raise _TapError(MessageCode.TAP_ERROR_FILE_NOT_FOUND, path=file_path)

# handler boundary — emit() does the formatting and routing
except _TapError as exc:
    emit(exc.msg_code, **exc.msg_kwargs)
    return 1
```

## Channel 2 — `logger` (diagnostic traces)

All diagnostic and audit output goes through `src/sirop/utils/logging.py`.
No module imports Python's `logging` directly or calls `logging.basicConfig()`.

### Setup

- Call `configure_logging(debug)` **once** at the CLI entry point only.
- Every other module: `from sirop.utils.logging import get_logger; logger = get_logger(__name__)`.
- Wrap each pipeline stage: `with StageContext(batch_id, stage): ...` — all
  logs inside carry context.
- Pure engine modules use `logging.getLogger(__name__)` directly.

### Log levels

| Level | Visible by default | Content |
|-------|--------------------|---------|
| `WARNING` | always | Data quality issues, node fallback, superficial losses. Never themed. |
| `INFO`    | always | Stage progress milestones with no `emit()` pair (e.g. `"Checking sap levels..."`). Maple theme OK. |
| `DEBUG`   | `--debug` only | Field transforms, SQL queries, ACB step-by-step, raw values. |

**No double-printing.** When `emit()` already covers a milestone, the
adjacent `logger` call must be `logger.debug()`, not `logger.info()`.
`logger.info()` is only for pipeline progress that has no `emit()` pair.

### Privacy and redaction

Default `SensitiveDataFilter` replaces in every log message:

- 64-char hex strings (txids) → `[txid redacted]`
- Bitcoin addresses (`bc1…`, `1…`, `3…`) → `[address redacted]`
- BTC amounts near `BTC` keyword → `[amount redacted]`
- CAD amounts near tax keywords → `[amount redacted]`

`--debug`: redaction bypassed; warning banner printed once.

Per-module log message guidelines: `docs/ref/logging-spec.md`.
