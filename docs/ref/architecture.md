---
verified-at: 9f411cb
tracks:
  - src/sirop/config/
  - src/sirop/importers/
  - src/sirop/normalizer/
  - src/sirop/engine/
  - src/sirop/node/
  - src/sirop/db/
  - src/sirop/reports/
  - src/sirop/models/
  - src/sirop/utils/
  - config/
notes: Module boundary contract. The invariant summary lives in CLAUDE.md; the rationale, examples, and rules live here.
---

# Architecture

## 1. Separation of concerns

Every module has a single responsibility. No module mixes concerns.

```
src/
├── config/          # YAML loading, env resolution, validation
├── importers/       # CSV parsing ONLY — one importer per source
├── normalizer/      # Currency conversion, timestamp normalization
├── engine/          # Pure tax calculation logic (ACB, superficial loss)
├── node/            # Bitcoin node integration: graph traversal + verify
├── db/              # Repository layer — SQLite read/write ONLY
├── reports/         # Output formatting ONLY (Schedule 3, Schedule G, TP-21.4.39-V)
├── models/          # Dataclasses and type definitions shared across modules
└── utils/           # Stateless helpers (BoC API client, crypto price client, caching, date math)
```

### Module boundary rules

- **Importers** parse raw CSV bytes into `RawTransaction` dataclasses. They
  know nothing about tax rules, ACB, or reports. Each importer is driven by a
  YAML column mapping — the importer code contains zero hardcoded column
  names, date formats, or transaction type strings.
- **Engine** performs pure calculations. It receives typed dataclasses and
  returns typed results. It never reads files, calls APIs, accesses config,
  or imports from `src/db/` — all dependencies are injected.
- **db/** is the exclusive interface to `.sirop` batch files. No other module
  reads or writes SQLite directly. `db/` knows nothing about tax rules or
  business logic — it only serializes and deserializes dataclasses to rows,
  and wraps every stage write in a single SQLite transaction.
- **Reports** take calculated results and format them for output. They know
  about form field names and line numbers but never how the numbers were
  computed.
- **Node module** (`src/node/`) has two responsibilities:
  - *Graph traversal*: `mempool_client.py` fetches UTXO data; `graph.py` runs
    pure BFS (backward through `vin[]`, forward through outspends) to link
    BTC transfers across mismatched txids; `graph_analysis.py` orchestrates
    Pass 1b in the transfer matcher. `privacy.py` gates all traversal calls —
    public Mempool endpoints require user confirmation.
  - *Verify stage* (`verify.py`): on-chain fee and timestamp validation. The
    on-chain fee overrides the imported `fee_crypto` only when the user owns
    the inputs (wallet-side `transfer_out` / `withdrawal`); exchange-side rows
    keep their imported fee but still receive `node_verified=1` and
    `block_height`. Future scope: amount cross-validation and audit-trail
    surfaces.
  - The pipeline functions identically (minus traversal/verification) when
    the node is unavailable or `BTC_TRAVERSAL_MAX_HOPS=0`.

## 2. Config-driven design

**No magic constants or hardcoded values in source code.** All of the
following live in YAML configuration files under `config/`:

```
config/
├── tax_rules.yaml         # Inclusion rates, loss carry rules, form line numbers
├── importers/             # One YAML per importer; see config/importers/
├── currencies.yaml        # Supported currencies: BoC API series codes (fiat) and Mempool flag (BTC only)
└── reports.yaml           # Output form field mappings, line numbers
```

See `config/importers/shakepay.yaml` for a fully annotated example and
`config/importers/ndax.yaml` for `group_handlers` and `fiat_asset_class`
patterns. When adding a new exchange importer, run `/new-importer` for the
full checklist.

Rules:

- Source code references config keys, never literal column names or values
- Adding a new exchange means adding a YAML file, not writing new Python
- Tax rule changes (e.g. inclusion rate update) edit `tax_rules.yaml` only
- Config files are validated at startup against a schema; fail fast on
  missing or invalid keys
- `date_format` values are validated against an allowlist of permitted
  strptime tokens before use (see `security-input-hardening.md §4`). Only
  tokens actually needed by the currently supported importers are permitted —
  anything else raises `InvalidCSVFormatError` at load time.

## 3. Privacy-first / public-repo safe

**This repo may be made public. No private data may ever be committed.** All
secrets and user-specific values go in `.env` (gitignored); see `.env.example`
for the full variable list.

Rules:

- Use `pydantic-settings` (or `python-dotenv`) to load `.env` into typed
  config objects. Never use `os.getenv()` scattered throughout code.
- **No addresses, xpubs, txids, amounts, or personal identifiers** in code,
  tests, config, comments, or commit messages.
- Test fixtures use obviously fake data (e.g. `txid: "aaa...111"`,
  address: `bc1qfakeaddressfortesting`).
- `.gitignore` must include: `.env`, `data/`, `output/`, `.node_cache/`,
  `*.csv`, any file that could contain real transaction data.
- Log output redacts sensitive values by default. `--debug` bypasses
  redaction (shows txids/amounts/addresses) and prints a warning banner.
- **BTC graph traversal privacy**: `is_private_node_url()` in
  `src/node/privacy.py` determines whether `BTC_MEMPOOL_URL` is private.
  Public endpoints trigger an interactive Y/n prompt (or `[W008]` skip)
  before any txid is sent. `BTC_TRAVERSAL_ALLOW_PUBLIC=true` /
  `--allow-public-mempool` bypass the prompt for CI and scripted runs.

## Data flow

Data flows through typed dataclasses between stages. Each stage boundary is
persisted to the active `.sirop` file via the repository layer. No raw dicts;
no in-memory lists passed across stage boundaries.

The 7-stage pipeline diagram lives in `docs/ref/data-pipeline.mermaid`.

**PURE** means the function receives typed dataclasses and returns typed
dataclasses only. It has no knowledge of files, databases, APIs, or config.
The repository layer handles all serialization around it.

Each `[repo write]` is a single atomic SQLite transaction inside the `.sirop`
file. If it fails, the stage output is rolled back and `stage_status` is not
updated to `done`.
