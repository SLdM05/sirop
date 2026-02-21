# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# sirop — Quebec 2025

**sirop** (lowercase, always) is a Quebec maple syrup-themed crypto tax tool.
It calculates Canadian/Quebec tax obligations from exchange and wallet
transaction exports.

## Commands

```bash
# Install dependencies
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
- SQLite via `sqlite3` (stdlib) for pipeline persistence (one `.sirop` file per batch)
- Bitcoin Core RPC or Mempool REST API for node verification
- `ruff` for linting and formatting
- `mypy` for static type checking
- `pytest` for testing

---

## Language and Naming

The tool is `sirop`. Lowercase everywhere — package name, CLI binary, module
name, prose. Never `Sirop`.

### CLI verbs (top-level commands)

| Verb | Replaces | Purpose |
|------|----------|---------|
| `tap` | import | Read raw transaction files |
| `boil` | calculate / process | Run the full pipeline |
| `pour` | export / generate | Produce tax report outputs |
| `grade` | summarize / status | Show gains, losses, ACB state |
| `verify` | verify | Node verification (plain name is clear enough) |

Sub-commands and flags use plain descriptive names. The theme lives at the
top level only.

### Batch management commands

These are utility commands, not pipeline verbs — no maple theme.

| Command | Purpose |
|---------|---------|
| `create <name>` | Create a new `.sirop` batch file and set it as active |
| `list` | List all batches in `DATA_DIR`, mark the active one |
| `switch <name>` | Change the active batch |

Batch names should embed the tax year so it can be inferred automatically
(`my2025tax` → 2025). Pass `--year YYYY` to override inference.

### Two registers — strict separation

**In-app chrome** (personality on): CLI output, progress messages, status
bars, log lines, TUI headers/footers. Use the maple metaphor where it fits
naturally. Keep it dry — one well-placed word beats forced whimsy.

**Tax output** (personality off): anything in Schedule 3, Schedule G,
TP-21.4.39-V, audit logs, or any file leaving the tool. Zero personality.
Standard accounting vocabulary only. If the form says "Proceeds of
disposition," the tool says "Proceeds of disposition."

**Never mix registers in the same sentence.**

### Key rules

- Errors: clear and actionable first, flavour second. Never let the metaphor
  obscure what went wrong.
- Numbers are never flavoured. `$4,209.23` is `$4,209.23`.
- When inventing new language, ask: does the sugarbush give a better word than
  the generic one? If yes, use it. If not, use plain language.

See `docs/ref/sirop-language-guide.md` for the full vocabulary and examples.

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
├── node/            # Bitcoin node verification (optional layer)
├── db/              # Repository layer — SQLite read/write ONLY
├── reports/         # Output formatting ONLY (Schedule 3, Schedule G, TP-21.4.39-V)
├── models/          # Dataclasses and type definitions shared across modules
└── utils/           # Stateless helpers (BoC API client, caching, date math)
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
- **Node verification** is an optional enrichment layer. The pipeline must
  function identically (minus verification) when the node is unavailable.

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
├── currencies.yaml        # Supported currencies, BoC API series codes
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

**Metadata** (written once at `sirop create` time)

| Table | Key columns | Purpose |
|-------|-------------|---------|
| `batch_meta` | name, tax_year, created_at, sirop_version | Single-row batch identity |
| `schema_version` | version, applied_at | Single-row migration guard — bump on schema changes |
| `stage_status` | stage, status, completed_at, error | Pipeline state machine |

`stage_status.status` values: `pending` | `running` | `done` | `invalidated`

**Cached external data**

| Table | Key columns | Purpose |
|-------|-------------|---------|
| `boc_rates` | date, currency_pair, rate, fetched_at | BoC daily rates — fetched once per date, never re-fetched |

**Pipeline tables**

| Table | Written by | Maps to |
|-------|------------|---------|
| `raw_transactions` | tap (import) | `models/raw.py` |
| `transactions` | normalize | `models/transaction.py` |
| `verified_transactions` | verify | `models/verified.py` |
| `classified_events` | transfer matching | See note below |
| `dispositions` | boil (ACB engine) | `models/disposition.py` |
| `acb_state` | boil (ACB engine) | asset, pool_cost, units, snapshot_date |
| `dispositions_adjusted` | superficial loss detection | adjusted dispositions |

**Audit**

| Table | Key columns | Purpose |
|-------|-------------|---------|
| `audit_log` | stage, txid, field, old_value, new_value, reason | Every field-level override (node verification, manual corrections). CRA 6-year retention. |

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

## Logging and Debug Mode

All logging goes through `src/utils/logging.py`. No module imports Python's
`logging` directly or calls `logging.basicConfig()`.

### Setup

See `src/sirop/utils/logging.py` for the full API. Key contract:
- Call `configure_logging(verbose, debug)` **once** at CLI entry point only
- Every other module: `from sirop.utils.logging import get_logger; logger = get_logger(__name__)`
- Wrap each pipeline stage: `with StageContext(batch_id, stage): ...` — all logs inside carry context
- Pure engine modules use `logging.getLogger(__name__)` directly

### Log levels

| Level | Visible by default | Content |
|-------|--------------------|---------|
| `ERROR` | always | Stage failures, unrecoverable errors. Always clear, never themed. |
| `WARNING` | always | Discrepancies, data quality issues, node fallback, superficial losses detected. |
| `INFO` | always | Stage start/completion, row counts, key milestones. Maple theme OK here. |
| `DEBUG` | `--debug` only | Field transforms, SQL queries, ACB step-by-step, raw values. |

`--debug` implies `--verbose`.

### Privacy and redaction

**Default (no flags):** `SensitiveDataFilter` is attached to the log handler
and replaces the following in every message:

- 64-char hex strings (txids) → `[txid redacted]`
- Bitcoin addresses (`bc1…`, `1…`, `3…`) → `[address redacted]`
- BTC amounts near the keyword `BTC` → `[amount redacted]`
- CAD amounts near tax keywords (proceeds, acb, gain, loss, fee) → `[amount redacted]`

**`--verbose` mode:** Redaction filter is bypassed. A banner is printed once:
```
⚠  VERBOSE MODE: sensitive data (txids, amounts, addresses) is being logged.
   Do not share this output. Run without --verbose for redacted logs.
```

### What each layer logs

| Module | INFO | WARNING | DEBUG |
|--------|------|---------|-------|
| Importers | `"Tapping {source}..."`, row count on done | Missing fields, unknown tx types | Each parsed row (redacted by default) |
| Normalizer | `"Checking sap levels..."` (BoC fetch/cache) | Missing rates, unusual types | Each BoC rate used, each field converted |
| Node verifier | `"Verifying on-chain..."`, `"Block confirmed."` | `"Node unreachable — running without on-chain verification."`, each discrepancy | Raw API response fields, before/after overrides |
| Transfer matcher | `"Tracing the flow..."`, each confirmed match | `"Loose tap — withdrawal on {date} has no matching deposit."` | Window checks |
| ACB engine (PURE) | — (caller logs milestones) | — (caller logs, engine returns flags) | ACB state before/after each event, gain/loss steps |
| SLD engine (PURE) | — | — | Each 61-day window check, each denied loss |
| Repository | — | Schema version mismatch | Every SQL query (parameter count only unless `--debug --verbose`) |

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

```
CSV file(s)
  → tap: Importer(s)          → [repo write] → raw_transactions
  → normalize: Normalizer     → [repo write] → transactions
      (BoC rates read from boc_rates cache or fetched and cached)
  → verify: NodeVerifier      → [repo write + audit_log] → verified_transactions
      (if node unavailable: transactions rows promoted to verified_transactions as-is)
  → transfer matching:
      TransferMatcher         → [repo write] → classified_events
      (buys/sells kept, fee micro-dispositions created, transfers excluded)
  → boil: ACBEngine  PURE     → [repo write] → dispositions + acb_state
  → SuperficialLossDetector  PURE  → [repo write] → dispositions_adjusted
  → pour: ReportGenerator     → Schedule 3 / Schedule G / TP-21.4.39-V / Summary
```

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