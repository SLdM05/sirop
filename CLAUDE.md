# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# sirop — Quebec 2025

**sirop** (lowercase, always) is a Quebec maple syrup-themed crypto tax tool.
It calculates Canadian/Quebec tax obligations from exchange and wallet
transaction exports.

## Session startup

The system Python may be 3.11 (incompatible). Poetry will auto-select
Python 3.13 and create a new virtualenv the first time it runs, but that
virtualenv starts empty. **Always run this before anything else in a session:**

```bash
poetry install
```

This ensures the virtualenv exists, all dependencies are present, and
`poetry run` commands work correctly. Skipping it causes spurious
`ModuleNotFoundError: No module named 'sirop'` failures in tests and
`Library stubs not installed` noise in mypy output.

## Commands

```bash
# Install dependencies (also the session-startup command — run first)
poetry install

# Run any command inside the Poetry virtualenv
poetry run <command>

# Lint and format (run before every commit)
poetry run ruff check --fix .
poetry run ruff format .

# Type checking
poetry run mypy .

# Run all tests
poetry run pytest

# Run tests excluding slow/integration tests (no live node required)
poetry run pytest -m "not slow and not integration"

# Run a single test file
poetry run pytest tests/path/to/test_file.py -v

# Textual dev console (hot-reload CSS during TUI development)
poetry run textual run --dev src/main.py

# Add a dependency
poetry add <package>

# Add a dev-only dependency
poetry add --group dev <package>
```

## Branch Model

```
main   ← releases only (tagged v0.1.0, v0.2.0, …)
 └── dev   ← integration branch; squash merges in from feat/*
      └── feat/<name>
            └── claude/<name>-<id>   ← Claude session sub-branches
```

### Starting a session

Always branch from the relevant `feat/<name>` branch (or create one from `dev`), then
create a `claude/<name>-<id>` sub-branch for the session:

```bash
git checkout dev && git pull sirop dev
git checkout -b feat/<name>                 # if the feature branch doesn't exist yet
git checkout -b claude/<name>-<random-id>   # session branch
```

All commits go on the `claude/` branch. When the session is done, open a PR targeting
the parent `feat/<name>` branch. Never open a `claude/` branch directly from `dev`.

### Squash merge policy

| Merge direction        | GitHub method         | Result on target           |
|------------------------|-----------------------|----------------------------|
| `claude/*` → `feat/*` | Squash and merge      | 1 commit per session       |
| `feat/*` → `dev`       | Squash and merge      | 1 commit per feature       |
| `dev` → `sirop-0.x`   | Create a merge commit | Feature list preserved     |
| `sirop-0.x` → `main`  | Create a merge commit | Release boundary preserved |

Never squash `dev → sirop-0.x` or `sirop-0.x → main` — those merge commits are
the project's release record. After merging to `main`, tag immediately:
`git tag v0.x.0 main && git push sirop v0.x.0`.

### Branch naming

- Feature branches: `feat/<short-noun>` — e.g., `feat/koinly-importer`, `feat/pour-command`
- Session branches: `claude/<feature>-<id>` — e.g., `claude/koinly-importer-x7k2q`
- RC branches: `sirop-<major>.<minor>` — e.g., `sirop-0.1`, `sirop-0.2`

### What NOT to do

- Never commit directly to `main` or `dev` — both are branch-protected
- Never squash `dev → sirop-0.x` or `sirop-0.x → main`
- Never rebase a branch after it has been pushed and shared
- Never open a `claude/` branch directly from `dev`
- Never use the old `feature-` prefix — always use `feat/`

---

## Project Context

Read these before making changes:

- `docs/ref/crypto-tax-reference-quebec-2025.md` — Complete tax rules, ACB formulas,
  form references, and data format specs
- `docs/ref/bitcoin-node-validation-module.md` — Node verification module spec
- `docs/ref/tui-design-guidelines.md` — TUI layout, widget design, keyboard bindings, and phase roadmap
- `docs/ref/data-pipeline.mermaid` — Visual diagram of all 7 pipeline stages and data models
- `docs/ref/sirop-language-guide.md` — Tool name, CLI verbs, message vocabulary, and the two-register rule
- `docs/ref/security-input-hardening.md` — Security rules for every pipeline boundary: SQL parameterization, batch name validation, YAML loading, node API responses, logging redaction
- `docs/ref/transaction-import-formats.md` — Authoritative CSV schemas for Shakepay and Sparrow Wallet: column definitions, unit detection rules (BTC vs sats), fee availability, unconfirmed-row handling, cross-source transfer matching, and YAML config reference. **Read this before building or modifying the Shakepay or Sparrow importers.**
- `docs/ref/database-schema.md` — Schema changelog, per-wallet vs global ACB pool reconciliation pattern, and ad-hoc SQLite investigation queries. **Read this before debugging `boil` output discrepancies or querying `.sirop` files directly.**

## Key Rules (quick reference)

- CRA mandates weighted-average ACB method (not FIFO/LIFO)
- 50% capital gains inclusion rate for 2025 (no threshold)
- Wallet-to-wallet transfers are NOT dispositions
- Superficial loss rule: 61-day window (30 before + day of + 30 after)
- All values must be in CAD using Bank of Canada daily rates
- Quebec requires mandatory TP-21.4.39-V form ($10/day penalty)

## Data Sources

- Shakepay CSV — debit/credit model; fees embedded in spread; exports multiple files per
  currency type (BTC, CAD, USD); buy/sell direction determined from Debit/Credit Currency columns
- NDAX CSV (AlphaPoint Ledgers format) — grouped rows by TX_ID, signed AMOUNT column
  (positive = credit/buy, negative = debit/sell), explicit FEE rows
- Sparrow Wallet CSV — signed BTC or satoshi amounts (auto-detect unit); fee present on
  sends, absent on receives; optional fiat column (CoinGecko rate — never use for ACB)
- Koinly Capital Gains Report CSV (primary/validation source)

See `docs/ref/transaction-import-formats.md` for full column specs, unit detection rules,
and cross-source transfer matching logic for Shakepay ↔ Sparrow.

## Tech Stack

- Python 3.12+
- **Poetry** for dependency management and virtualenv (`pyproject.toml`)
- `decimal.Decimal` for all money (never float)
- Bank of Canada Valet API for FX rates (no key required)
- Mempool.space + CoinGecko public APIs for historical crypto prices (no key required)
- SQLite via `sqlite3` (stdlib) for pipeline persistence (one `.sirop` file per batch)
- Bitcoin Core RPC or Mempool REST API for node verification
- `ruff` for linting and formatting
- `mypy` for static type checking
- `pytest` for testing

---

## Language and Naming

The tool is `sirop` (lowercase always). CLI verbs: `tap` (import), `boil` (calculate),
`pour` (export), `grade` (status), `verify`. Batch commands (no theme): `create`, `list`, `switch`.

**Strict two-register rule:** maple theme in CLI chrome (output, logs, TUI); zero personality
in tax output (Schedule 3, Schedule G, TP-21.4.39-V, audit logs, any file leaving the tool).
Never mix registers in the same sentence. Numbers are never flavoured.

Full vocabulary, verb definitions, and examples: `docs/ref/sirop-language-guide.md`

---

## Architecture Principles

### 1. Separation of Concerns

Every module has a single responsibility. No module should mix concerns.

```
src/
├── config/          # YAML loading, env resolution, validation
├── importers/       # CSV parsing ONLY — one importer per source
├── normalizer/      # Currency conversion, timestamp normalization
├── engine/          # Pure tax calculation logic (ACB, superficial loss)
├── node/            # Bitcoin node integration: graph traversal (implemented) + full verification (planned)
├── db/              # Repository layer — SQLite read/write ONLY
├── reports/         # Output formatting ONLY (Schedule 3, Schedule G, TP-21.4.39-V)
├── models/          # Dataclasses and type definitions shared across modules
└── utils/           # Stateless helpers (BoC API client, crypto price client, caching, date math)
```

Rules for module boundaries:

- **Importers** parse raw CSV bytes into `RawTransaction` dataclasses. They know
  nothing about tax rules, ACB, or reports. Each importer is driven by a YAML
  column mapping — the importer code itself contains zero hardcoded column
  names, date formats, or transaction type strings.
- **Engine** performs pure calculations. It receives typed dataclasses and
  returns typed results. It never reads files, calls APIs, accesses config,
  or imports from `src/db/` — all dependencies are injected.
- **db/** is the exclusive interface to `.sirop` batch files. No other module
  reads from or writes to SQLite directly. `db/` knows nothing about tax rules
  or business logic — it only serializes and deserializes dataclasses to rows,
  and wraps every stage write in a single SQLite transaction.
- **Reports** take calculated results and format them for output. They know
  about form field names and line numbers but never about how the numbers
  were computed.
- **Node module** (`src/node/`) has two distinct responsibilities:
  - *Graph traversal (implemented)*: `mempool_client.py` fetches UTXO data;
    `graph.py` runs pure BFS (backward through `vin[]`, forward through
    outspends) to link BTC transfers across mismatched txids; `graph_analysis.py`
    orchestrates Pass 1b in the transfer matcher. `privacy.py` gates all
    traversal calls — public Mempool endpoints require user confirmation.
  - *Full node verification (planned)*: on-chain timestamp override, exact fee
    capture, amount cross-validation, audit trail. Currently a pass-through.
  - The pipeline must function identically (minus traversal/verification) when
    the node is unavailable or `BTC_TRAVERSAL_MAX_HOPS=0`.

### 2. Config-Driven Design

**No magic constants or hardcoded values in source code.** All of the following
must live in YAML configuration files under `config/`:

```
config/
├── tax_rules.yaml         # Inclusion rates, loss carry rules, form line numbers
├── importers/
│   ├── shakepay.yaml      # Column mappings, date format, transaction type map
│   ├── ndax.yaml          # Column mappings, date format, transaction type map
│   ├── sparrow.yaml       # Column mappings, date format
│   └── koinly.yaml        # Column mappings, date format, transaction type map
├── currencies.yaml        # Supported currencies: BoC API series codes (fiat) and CoinGecko IDs + Mempool flags (crypto)
└── reports.yaml           # Output form field mappings, line numbers
```

See `config/importers/shakepay.yaml` for a fully annotated example. See
`config/importers/ndax.yaml` for `group_handlers` and `fiat_asset_class` patterns.

When adding a new exchange importer, run `/new-importer` for the full checklist.

Rules for config:

- Source code references config keys, never literal column names or values
- Adding a new exchange means adding a YAML file, not writing new Python
- Tax rule changes (e.g., inclusion rate update) require editing `tax_rules.yaml`
  only — zero code changes
- Config files are validated at startup against a schema; fail fast on
  missing or invalid keys
- `date_format` values are validated against an allowlist of permitted strptime
  tokens before use (see `security-input-hardening.md §4`). Only tokens
  actually needed by the four supported importers are permitted — anything else
  raises `InvalidCSVFormatError` at load time, not at first parse attempt.

### 3. Privacy-First / Public-Repo Safe

**This repo may be made public. No private data may ever be committed.**

All secrets and user-specific values go in `.env` (gitignored). See `.env.example`
in the repo root for the full variable list and comments.

Rules for privacy:

- Use `pydantic-settings` (or `python-dotenv`) to load `.env` into typed
  config objects. Never use `os.getenv()` scattered throughout code.
- **No addresses, xpubs, txids, amounts, or personal identifiers** in code,
  tests, config, comments, or commit messages
- Test fixtures use obviously fake data (e.g., `txid: "aaa...111"`,
  address: `bc1qfakeaddressfortesting`)
- `.gitignore` must include: `.env`, `data/`, `output/`, `.node_cache/`,
  `*.csv`, any file that could contain real transaction data
- Log output should redact sensitive values by default. A `--verbose` flag
  can reveal txids/amounts for local debugging but this mode should print
  a clear warning that sensitive data is being logged.
- **BTC graph traversal privacy**: `is_private_node_url()` in `src/node/privacy.py`
  determines whether `BTC_MEMPOOL_URL` is a private address. Public endpoints
  trigger an interactive Y/n prompt (or `[W008]` skip) before any txid is sent.
  `BTC_TRAVERSAL_ALLOW_PUBLIC=true` / `--allow-public-mempool` bypass the prompt
  for CI and scripted runs — only use after reviewing the privacy implications.

---

## Database and Persistence Layer

Each **batch** is a self-contained SQLite file with a `.sirop` extension, stored
in `DATA_DIR` (configured in `.env`, default `./data`). The active batch is
tracked in `DATA_DIR/.active`. The file is the single source of truth between
pipeline stages — no in-memory lists are passed between stage boundaries.

A `.sirop` file is plain SQLite — rename it `.db` to open it in any SQLite
browser.

### Batch model

A **batch** is one person's one tax year (e.g. `my2025tax`). Each batch is an
isolated `.sirop` file. There are no `batch_id` columns — isolation is achieved
by having a separate database per batch. This means files are portable: you can
hand `my2025tax.sirop` to your accountant or archive it independently.

### .sirop file schema

Full table definitions and column specs: `docs/ref/database-schema.md`

Key tables: `batch_meta`, `schema_version`, `stage_status` (values: `pending` | `running` | `done` | `invalidated`),
`boc_rates`, `crypto_prices`, `raw_transactions`, `transactions`, `verified_transactions`, `classified_events`,
`dispositions`, `acb_state`, `dispositions_adjusted`, `audit_log`.

### `classified_events` — what it contains

`classified_events` is **not** a copy of `transactions`. The transfer matcher
produces it by:

1. Keeping taxable events (buys, sells) as-is
2. Creating **fee micro-dispositions** — each on-chain network fee is a small
   taxable disposal of BTC at the block confirmation time
3. **Excluding** wallet-to-wallet transfer records entirely

`classified_events` is the sole input to the ACB engine. If an event is not
in this table, it does not affect ACB or capital gains.

### Repository layer (`src/db/repositories.py`)

All DB reads and writes are mediated by this module. The contract:

- **Before a stage runs**: repository reads rows from the active `.sirop` file,
  deserializes them to typed dataclasses, and passes them to the stage
- **After a stage runs**: repository serializes the returned dataclasses to
  rows and writes them in a **single SQLite transaction** — if the write fails,
  nothing is committed
- **Pure engine modules** (`engine/`, and the pure parts of `node/`) never
  import from `src/db/`. Storage is injected by the pipeline coordinator.

### Cascading invalidation

Re-running stage N for a batch:

1. Delete that stage's output rows from the active `.sirop` file
2. Mark all downstream stages as `invalidated` in `stage_status`

The pipeline coordinator checks `stage_status` before each stage:
- `done` → skip
- `running` → abort (another process is running)
- `pending` or `invalidated` → execute

This means `sirop boil --from normalize` re-runs normalize through report
generation without re-tapping source files.

### TUI data access

The TUI reads the active `.sirop` file directly for live display — it never
re-runs pipeline stages. It queries: `stage_status` (progress view),
`classified_events` (transaction browser), `dispositions_adjusted`
(gains/losses view), `acb_state` (ACB state viewer), and `batch_meta`
(batch info header). The batch switcher uses `sirop switch` to change
`DATA_DIR/.active`, then reloads.

---

## User Output — Two Channels

sirop has two distinct output channels. Never mix them.

### Channel 1 — `emit()` (user-facing results)

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
| `error` | stderr | yes (`[E001]`) | Unrecoverable failures |
| `warning` | stderr | yes (`[W001]`) | User-visible data quality issues |
| `output` | stdout | no | Command results, confirmations, hints |
| `fluff` | stdout | no | Maple-themed personality messages |

**Rules:**
- `emit()` is the sole source of structured user output. Never `print()` directly
  in CLI handlers.
- `error` and `warning` categories display their `[Exxx]`/`[Wxxx]` code so users
  can quote them in support requests.
- `output` and `fluff` display clean text only — no prefix, no code.
- Tax output (Schedule 3, TP-21.4.39-V, etc.) never goes through `emit()` —
  it is written directly to files by the reports module, personality-free.

**Adding a new message:**
1. Add an entry to `config/messages.yaml` with `category`, `text`, and (for
   errors/warnings) a `code`.
2. Add a matching constant to `MessageCode` in `src/sirop/models/messages.py`.
3. Call `emit(MessageCode.YOUR_CODE, **kwargs)` at the call site.

**Sentinel exceptions in CLI handlers:**

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

### Channel 2 — `logger` (diagnostic traces)

All diagnostic and audit output goes through `src/sirop/utils/logging.py`.
No module imports Python's `logging` directly or calls `logging.basicConfig()`.

**Setup:**
- Call `configure_logging(verbose, debug)` **once** at CLI entry point only
- Every other module: `from sirop.utils.logging import get_logger; logger = get_logger(__name__)`
- Wrap each pipeline stage: `with StageContext(batch_id, stage): ...` — all logs inside carry context
- Pure engine modules use `logging.getLogger(__name__)` directly

**Log levels:**

| Level | Visible by default | Content |
|-------|--------------------|---------|
| `WARNING` | always | Data quality issues, node fallback, superficial losses. Never themed. |
| `INFO` | always | Stage progress milestones with no `emit()` pair (e.g. `"Checking sap levels..."`). Maple theme OK. |
| `DEBUG` | `--debug` only | Field transforms, SQL queries, ACB step-by-step, raw values. |

`--debug` implies `--verbose`.

**Key rule — no double-printing:** When `emit()` already covers a milestone,
the adjacent `logger` call must be `logger.debug()`, not `logger.info()`.
`logger.info()` is only for pipeline progress that has *no* `emit()` pair.

**Privacy and redaction:**

Default: `SensitiveDataFilter` replaces in every log message:
- 64-char hex strings (txids) → `[txid redacted]`
- Bitcoin addresses (`bc1…`, `1…`, `3…`) → `[address redacted]`
- BTC amounts near `BTC` keyword → `[amount redacted]`
- CAD amounts near tax keywords → `[amount redacted]`

`--verbose`: redaction bypassed; warning banner printed once.

Per-module log message guidelines: `docs/ref/logging-spec.md`

---

## Code Quality Standards

### Ruff, Mypy, Pytest

Full tool config lives in `pyproject.toml` — do not duplicate it here.

Run `/check` before every commit (runs all four steps in order). Or manually:

```bash
poetry run ruff check --fix . && poetry run ruff format .
poetry run mypy .
poetry run pytest -m "not slow and not integration" -v
```

Key typing rules:
- Every function has complete annotations (params + return); `Decimal` for money, never `float`
- `datetime` must be timezone-aware; use `dataclasses` or `pydantic.BaseModel` for structured data
- No bare `Any`; prefer `Protocol` over abstract base classes

Key testing rules:
- Engine functions need known-answer unit tests; importers test against synthetic fixture CSVs
- Fake data only in fixtures (no real txids, amounts, or addresses)
- `slow`/`integration` markers for tests requiring external APIs or a live node

---

## Data Flow Convention

Data flows through typed dataclasses between stages. Each stage boundary is
persisted to the active `.sirop` file via the repository layer. No raw dicts,
no in-memory lists passed across stage boundaries.

7-stage pipeline diagram: `docs/ref/data-pipeline.mermaid`

**PURE** means the function receives typed dataclasses and returns typed
dataclasses only. It has no knowledge of files, databases, APIs, or config.
The repository layer handles all serialization around it.

Each `[repo write]` is a single atomic SQLite transaction inside the `.sirop`
file. If it fails, the stage output is rolled back and `stage_status` is not
updated to `done`.

---

## Conventions Summary

- Run `/check` before every commit; run `/commit` to execute the commit workflow.
- `Decimal` for money, always. `float` in a financial calculation is a bug.
- `format(d, 'f')` to serialize any `Decimal` to a DB TEXT column. `str(Decimal)`
  is banned for storage — computed values produce scientific notation (`"1.2419E+5"`,
  `"1.0E-7"`) that is unreadable and non-canonical.
- UTC internally. Convert to `America/Toronto` only in final report display.
- Pathlib for all file paths. No string concatenation for paths.
- f-strings for formatting. No `%` or `.format()`.
- Explicit is better than implicit: no `*args/**kwargs` unless wrapping
  an external API.
- Functions should be small and pure where possible. If a function needs
  more than 3 dependencies, it probably needs decomposition.
- Errors should be specific: `InvalidCSVFormatError`, `MissingColumnError`,
  `NodeConnectionError` — not bare `Exception` or `ValueError`.
- All config-loaded values are validated at startup, not at first use.
  Fail fast, fail loud.