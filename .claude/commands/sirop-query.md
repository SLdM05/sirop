Query and audit `.sirop` SQLite batch files when debugging ACB, dispositions, transfer matching, or any display/summary bug. Use the bundled read-only helper instead of writing ad-hoc SQL.

## When to use

- An ACB number disagrees with a hand calculation
- A disposition is missing, doubled, or has the wrong sign
- The superficial loss flag looks wrong (or is missing on a loss)
- A wallet-to-wallet transfer was not matched (shows as a sale)
- A `fee_disposal` row is unexpected, missing, or has the wrong amount
- A summary number doesn't match what `boil` or `pour` printed
- You need to compare two `.sirop` files (same query, different `--file`)

If the question is "what does this code do," read the source. If the question is "what's actually in the batch right now," use this skill.

## Quick start

```bash
poetry run python .claude/scripts/sirop_query.py --help
poetry run python .claude/scripts/sirop_query.py status
poetry run python .claude/scripts/sirop_query.py acb BTC
poetry run python .claude/scripts/sirop_query.py trace <txid-or-raw_id>
```

Defaults to the active batch (`$DATA_DIR/.active`, default `./data`). Override with `--batch <name>` or `--file <path>`. Add `--limit 0` to see every row, `--raw` for TSV output (handy for piping to `grep`/`awk`).

## Schema cheatsheet (current schema_version: 11)

Pipeline foreign-key chain — read top-to-bottom; each table is the input to the next stage:

```
raw_transactions          (tap)              CSV in, no normalisation
        ↓ raw_id
transactions              (normalize)        ISO timestamps, CAD via BoC, canonical types
        ↓ tx_id
verified_transactions     (verify)           node-corrected timestamps/fees, or pass-through
        ↓ vtx_id
classified_events         (transfer_match)   buys/sells/fee_disposals; transfers excluded (is_taxable=0)
        ↓ event_id
dispositions              (boil)             ACB engine output; full before/after pool snapshot
        ↓ disposition_id
dispositions_adjusted     (superficial_loss) AUTHORITATIVE for tax forms
```

Side tables:

| Table | Written by | Notes |
|---|---|---|
| `batch_meta`, `schema_version`, `stage_status` | `sirop create` / pipeline | Always check `stage_status` first if results look stale |
| `wallets` | every importer | `auto_created=1` if synthesised by `tap`; `0` if user-named |
| `acb_state` | `boil` | One row per acquisition AND disposal — chronological pool log |
| `income_events` | `transfer_match` | Schedule G source (staking/airdrop/mining); separate from dispositions |
| `transfer_overrides` | `sirop stir` | User link/unlink/external — survives pipeline re-runs |
| `graph_transfer_pairs` | `boil` (BTC only) | On-chain BFS matches when txids don't line up |
| `transaction_txid_overrides` | `sirop stir` | User-supplied txids for CSV records that lacked one |
| `audit_log` | every override | Immutable; one row per field change |
| `boc_rates`, `crypto_prices` | normalize / lookups | API caches — never refetched if present |
| `custom_importers` | `sirop tap --importer` | YAML stored verbatim for batch portability |

Repository readers in `src/sirop/db/repositories.py` return typed dataclasses for the same data: `read_dispositions`, `read_adjusted_dispositions`, `read_classified_events`, `read_acb_state_final`, `read_per_wallet_holdings`, etc. Use those when writing Python rather than SQL.

## Storage gotchas — these eat the most cycles when forgotten

- **Decimals are TEXT** serialised via `format(d, 'f')`. To compare numerically in SQL, use `CAST(col AS REAL)` (lossy — fine for inspection, never for tax math). For exact arithmetic, post-process in Python with `Decimal(...)`.
- **Timestamps are ISO 8601 UTC strings** — comparable lexicographically (`WHERE timestamp >= '2025-01-01'` works).
- **`classified_events.is_taxable = 0`** rows are excluded transfers; the ACB engine never sees them. Filter them in/out explicitly.
- **`dispositions_adjusted` is the authoritative source** for Schedule 3, Schedule G, and TP-21.4.39-V — not `dispositions`. Only the adjusted table has `adjusted_gain_loss` and the superficial-loss flags.
- **Booleans are INTEGER** (`0`/`1`). Don't compare to Python `True`/`False` in SQL parameters.
- **Active batch** lives at `<DATA_DIR>/.active` (plain text, batch name, no extension). Default `DATA_DIR` is `./data`.
- The script enforces `mode=ro` URI plus `PRAGMA query_only = 1`. Any `INSERT/UPDATE/DELETE` in `sql` subcommand will raise — that is intentional.

## Recipes

### 1. Trace one transaction end-to-end

```bash
poetry run python .claude/scripts/sirop_query.py trace <txid>
poetry run python .claude/scripts/sirop_query.py trace 42      # by transactions.id (the ID stir shows)
```

- **txid mode** (any non-numeric ident) — queries every pipeline table with `WHERE txid = ?` independently. Reliable because txid is denormalized through every stage, so it works even when `raw_id`/`tx_id` FK columns are NULL. Dispositions are reached via the matching `classified_events.id`.
- **numeric mode** — treats the ident as `transactions.id` (the ID surfaced by `sirop stir` as `id:42`). Walks downstream by FK: `transactions → verified_transactions → classified_events → dispositions → dispositions_adjusted`.

If a stage shows no rows, that's the diagnostic: usually a transfer was matched away at `classified_events` (`is_taxable=0`), or a buy/income event has no disposition (correct — only sales create disposition rows).

### 2. "Why is my BTC ACB wrong?"

```bash
poetry run python .claude/scripts/sirop_query.py acb BTC --limit 0
```

Chronological `acb_state` rows joined to the triggering `classified_events` (event_type, amount, cad_proceeds, cad_cost, txid, source, wallet). Walk top-to-bottom; the snapshot after each event is the new pool state. If a fee_disposal is missing, the deltas won't match.

### 3. Disposition vs adjusted, side-by-side

```bash
poetry run python .claude/scripts/sirop_query.py dispositions BTC
```

Joins `dispositions` to `dispositions_adjusted`: `raw_gain_loss`, `is_superficial_loss`, `superficial_loss_denied`, `allowable_loss`, `adjusted_gain_loss` next to each other. Any row where `raw_gain_loss != adjusted_gain_loss` is one the superficial-loss rule touched.

### 4. Pipeline state and rowcounts

```bash
poetry run python .claude/scripts/sirop_query.py status
```

Shows `batch_meta`, `schema_version`, every stage's `status` + `completed_at` + `error`, and rowcounts for every pipeline and support table. The fastest way to confirm a re-run actually happened (look for `invalidated` rows or unexpectedly small counts).

### 5. Compare two batches

Run the same subcommand twice with `--file` and diff the `--raw` output:

```bash
poetry run python .claude/scripts/sirop_query.py acb BTC --file data/before.sirop --raw --limit 0 > /tmp/before.tsv
poetry run python .claude/scripts/sirop_query.py acb BTC --file data/after.sirop  --raw --limit 0 > /tmp/after.tsv
diff /tmp/before.tsv /tmp/after.tsv
```

Useful after a refactor to confirm "no change to the calculation" or to localise where a regression first appears.

### 6. Filtered events

```bash
poetry run python .claude/scripts/sirop_query.py events --asset BTC --type fee_disposal
poetry run python .claude/scripts/sirop_query.py events --txid <txid>
poetry run python .claude/scripts/sirop_query.py events --wallet 3 --taxable-only
```

### 7. Free-form SQL (read-only)

```bash
poetry run python .claude/scripts/sirop_query.py sql \
  "SELECT asset, SUM(CAST(amount AS REAL)) FROM classified_events WHERE event_type='fee_disposal' GROUP BY asset"
```

Use `CAST(... AS REAL)` only for inspection. For exact arithmetic, fetch and aggregate in Python with `Decimal`.

## Rules

- **Read-only.** The helper enforces this; do not work around it. If you need to mutate a batch, use `sirop stir` (interactive overrides) or re-run the relevant pipeline stage with `boil --from <stage>`.
- **Privacy when reporting findings.** Real txids, addresses, and amounts must not appear in PR descriptions, commit messages, or chat output unless the user has explicitly asked for them. When summarising for the user, redact the same things `SensitiveDataFilter` redacts (txids, BTC addresses, BTC and CAD amounts near tax keywords). See `CLAUDE.md` §"Privacy and redaction" and `docs/ref/security-input-hardening.md`.
- **Use `repositories.py` typed readers** when you need the data inside Python code (a one-off script, a test). Reach for raw SQL only when you're investigating, not when writing product code.
- For deep schema questions beyond this cheatsheet (column-level history, migration log, per-wallet vs global ACB reconciliation), open `docs/ref/database-schema.md`.
