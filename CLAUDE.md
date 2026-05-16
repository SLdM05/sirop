# CLAUDE.md

Guidance for Claude Code working in this repository. **Invariants only** —
detail lives in `docs/ref/` and is loaded on demand via the index below.

## sirop — Quebec 2025

**sirop** (lowercase, always) is a Quebec maple syrup-themed crypto tax tool.
It calculates Canadian/Quebec tax obligations from exchange and wallet
transaction exports. BTC-focused today; ETH/SOL are deferred.

Supported import sources are whatever sits under `config/importers/` (today:
shakepay, ndax, sparrow, xpub). Source-of-truth column specs and edge cases
are in [`docs/ref/transaction-import-formats.md`](docs/ref/transaction-import-formats.md).

## Key rules (CRA / Quebec)

- Weighted-average ACB method (not FIFO/LIFO)
- 50% capital gains inclusion rate for 2025 (no threshold)
- Wallet-to-wallet transfers are NOT dispositions
- Superficial loss rule: 61-day window (30 before + day of + 30 after)
- All values in CAD via Bank of Canada daily rates
- Quebec mandates the TP-21.4.39-V form ($10/day penalty)

## Two-register rule

Maple theme is allowed in CLI chrome (output, logs, TUI). Tax output
(Schedule 3, Schedule G, TP-21.4.39-V, audit logs, any file leaving the
tool) is personality-free. Never mix registers in the same sentence. Numbers
are never flavoured. Vocabulary: [`docs/ref/sirop-language-guide.md`](docs/ref/sirop-language-guide.md).

## Module boundaries (invariant)

- `importers/` — CSV → `RawTransaction`. No tax rules, no I/O beyond CSV.
- `engine/` — pure calculations. No file/API/DB access; deps injected.
- `db/` — sole interface to `.sirop` files. Knows no business logic.
- `reports/` — format only. Knows form line numbers, not how numbers were computed.
- `node/` — Mempool client + BFS traversal + verify stage. Pipeline must
  function without it (`BTC_TRAVERSAL_MAX_HOPS=0`).
- `normalizer/`, `models/`, `utils/`, `config/` — stateless helpers.

Rationale, examples, full rules: [`docs/ref/architecture.md`](docs/ref/architecture.md).

## Persistence invariant

One `.sirop` SQLite file per batch (one person × one tax year). All DB
access goes through `src/db/repositories.py`; engine modules never import
from `src/db/`. Stage outputs are atomic transactions; re-running a stage
cascades `invalidated` downstream. Full schema and per-table notes:
[`docs/ref/database-schema.md`](docs/ref/database-schema.md).

## Output channels (invariant)

Two channels, never mixed:

- `emit()` (`src/sirop/utils/messages.py`) — sole source of structured
  user-visible output. Never `print()` in CLI handlers.
- `logger` (`src/sirop/utils/logging.py`) — diagnostic traces. Never import
  Python's `logging` directly outside engine internals. No double-printing:
  if `emit()` covers a milestone, the adjacent log is `debug`, not `info`.

Tax output never goes through `emit()` — written directly by `reports/`,
personality-free. Full spec, redaction rules, sentinel exception pattern:
[`docs/ref/output-channels.md`](docs/ref/output-channels.md).

## Privacy invariant

`.env` for secrets, gitignored. No addresses, xpubs, txids, amounts, or
PII in code, tests, config, comments, or commit messages. Test fixtures
use obviously fake data. Logs redact by default; `--debug` shows raw
values and prints a warning banner. Public Mempool endpoints require user
confirmation before any txid is sent. Full rules:
[`docs/ref/security-input-hardening.md`](docs/ref/security-input-hardening.md)
and the privacy section of [`docs/ref/architecture.md`](docs/ref/architecture.md#3-privacy-first--public-repo-safe).

## Reference index (load on demand)

Bolded **⚠ Read before X** lines are hard gates — obey them.

- [`docs/ref/architecture.md`](docs/ref/architecture.md) — module boundary rationale, config-driven design, privacy rules
- [`docs/ref/output-channels.md`](docs/ref/output-channels.md) — emit/logger spec, redaction, sentinel exceptions
  - **⚠ Read before adding `print()`, `logger.*`, or `emit()` calls.**
- [`docs/ref/branch-model.md`](docs/ref/branch-model.md) — feat/claude sub-branch policy, squash merge rules
  - **⚠ Read before opening a PR.**
- [`docs/ref/database-schema.md`](docs/ref/database-schema.md) — full schema, per-wallet vs global ACB reconciliation, ad-hoc queries
  - **⚠ Read before debugging `boil` discrepancies or touching `src/db/`.**
- [`docs/ref/transaction-import-formats.md`](docs/ref/transaction-import-formats.md) — column specs, unit detection, fee availability, transfer matching, xpub YAML schema
  - **⚠ Read before building or modifying any importer.**
- [`docs/ref/security-input-hardening.md`](docs/ref/security-input-hardening.md) — SQL parameterization, batch name validation, YAML loading, log redaction
  - **⚠ Read before adding SQL, YAML loaders, or log lines that print user data.**
- [`docs/ref/sirop-language-guide.md`](docs/ref/sirop-language-guide.md) — tool name, CLI verbs, message vocabulary, two-register rule
  - **⚠ Read before writing any user-facing message string.**
- [`docs/ref/reconciliation-and-missing-data.md`](docs/ref/reconciliation-and-missing-data.md) — `manual_adjustments`, `audit_log`, CRA framing
  - **⚠ Read before touching `manual_adjustments`, `audit_log`, or "balance does not match" UX.**
- [`docs/ref/bitcoin-node-validation-module.md`](docs/ref/bitcoin-node-validation-module.md) — node verification module (implemented + planned scope)
- [`docs/ref/tui-design-guidelines.md`](docs/ref/tui-design-guidelines.md) — TUI layout, widgets, keyboard bindings, phase roadmap
- [`docs/ref/logging-spec.md`](docs/ref/logging-spec.md) — per-module log message guidelines
- [`docs/ref/data-pipeline.mermaid`](docs/ref/data-pipeline.mermaid) — visual diagram of all 7 pipeline stages

CLI usage manuals: [`docs/usage/sirop-<verb>.md`](docs/usage/). Update the
matching manual when changing a command's flags, prompts, or output.

## CLI verbs (today)

- `tap` — import; `boil` — calculate (normalize → verify → transfer_match →
  boil → superficial_loss); `pour` — export; `stir` — review/override
  transfer matching and record manual reconciliation entries.
- Batch commands (no theme): `create`, `list`, `switch`.
- Planned but not implemented: `grade` (status summary) and a standalone
  `verify` (node verification currently runs as a stage inside `boil`).

## Commands and hooks

- `/check` — runs ruff (check + format), mypy, pytest. Caches passing
  steps within a session.
- `/commit` — gather diff, draft conventional message, commit with HEREDOC.
  Never commits without `/check` passing first.
- `/new-importer` — checklist for adding an exchange or wallet importer.
- `/sirop-query` — read-only inspection of `.sirop` SQLite files.

Skills (auto-invoked by description match):

- `docs-audit` — anchor-based drift detection across `docs/ref/`. Auto-loads
  when about to edit a reference doc or after code changes under an anchored
  path. Also user-invokable via `/docs-audit`.

Hooks (configured in `.claude/settings.json`):

- **SessionStart**: ensures the Poetry venv has `sirop` importable; runs
  `poetry install` once when needed. No manual preamble required.
- **PreToolUse**: blocks `git commit` on `main` or `dev`. Use a
  `claude/<feature>-<id>` branch — see [`docs/ref/branch-model.md`](docs/ref/branch-model.md).

## Tech stack

- Python 3.12+, Poetry (`pyproject.toml`)
- `decimal.Decimal` for all money (never `float`)
- Bank of Canada Valet API (FX rates, no key)
- Mempool.space REST (historical BTC prices and node verification, no key)
- SQLite via `sqlite3` stdlib (one `.sirop` per batch)
- `ruff`, `mypy`, `pytest`

## Conventions

- Run `/check` before every commit. Run `/commit` to execute the commit workflow.
- `Decimal` for money. `float` in a financial calculation is a bug.
- Serialize Decimals with `format(d, 'f')` — `str(Decimal)` is banned for
  storage (produces scientific notation like `"1.2419E+5"`).
- UTC internally; convert to `America/Toronto` only in final report display.
- `pathlib` for paths; f-strings for formatting (no `%` or `.format()`).
- Explicit kwargs over `*args/**kwargs` unless wrapping an external API.
- Errors are specific (`InvalidCSVFormatError`, `MissingColumnError`,
  `NodeConnectionError`), not bare `Exception` or `ValueError`.
- Config-loaded values are validated at startup, not at first use.
- Full annotations (params + return); `Decimal` for money; timezone-aware
  `datetime`; no bare `Any`; `Protocol` over ABCs.
- Engine: known-answer unit tests. Importers: synthetic CSV fixtures with
  obviously fake data. Mark live-API / live-node tests `slow`/`integration`.
