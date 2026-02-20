# sirop create — Batch Management

## What is a batch?

A **batch** is one person's one tax year. It is stored as a single file with
a `.sirop` extension, for example `my2025tax.sirop`. The file is a plain SQLite
database — rename it `.db` to open it in any SQLite browser.

Each batch contains the full pipeline state for that tax year: raw imported
transactions, normalised data, ACB calculations, superficial loss adjustments,
and the final report output. Nothing spills between files.

```
data/
  my2025tax.sirop    ← Alice's 2025 taxes
  alice2024.sirop    ← Alice's 2024 taxes  (archived)
  bob2025.sirop      ← Bob's 2025 taxes    (separate, isolated)
  .active            ← one-liner: "my2025tax"
```

---

## `sirop create`

Create a new batch file and set it as the active batch.

```
sirop create <name> [--year YYYY]
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `name` | yes | Batch name used as the filename stem. No spaces. |
| `--year YYYY` | no | Tax year (four digits). Inferred from the name if omitted. |

### Year inference

If `--year` is not supplied, sirop scans the name for a four-digit year
in the 2000s (pattern `20xx`). The scan ignores surrounding digits, so it
works anywhere in the name:

| Name | Inferred year |
|------|---------------|
| `my2025tax` | 2025 |
| `2025` | 2025 |
| `alice-2024-btc` | 2024 |
| `taxes` | — (error, must pass `--year`) |

### What it does

1. Resolves `DATA_DIR` from the environment (default `./data`).
2. Constructs the path: `DATA_DIR/<name>.sirop`.
3. Fails immediately if the file already exists.
4. Creates `DATA_DIR/` if it does not exist.
5. Creates the SQLite file and initialises the full schema in one transaction.
6. Writes a single row to `batch_meta`:
   `name`, `tax_year`, `created_at` (UTC ISO 8601), `sirop_version`.
7. Inserts one row per pipeline stage into `stage_status`, all `pending`:
   `tap`, `normalize`, `verify`, `transfer_match`, `boil`, `superficial_loss`, `pour`.
8. Writes `DATA_DIR/.active` with the batch name.
9. Prints a confirmation line.

### Example output

```
$ sirop create my2025tax
Created batch: my2025tax (2025) → data/my2025tax.sirop
```

### Error cases

```
$ sirop create my2025tax
error: batch 'my2025tax' already exists at data/my2025tax.sirop

$ sirop create mytaxes
error: cannot infer tax year from 'mytaxes'. Pass --year YYYY.

$ sirop create mytaxes --year 2025
Created batch: mytaxes (2025) → data/mytaxes.sirop
```

---

## `sirop list`

List all batches in `DATA_DIR`. The active batch is marked with `*`.

```
sirop list
```

### Example output

```
$ sirop list
  alice2024
  my2025tax *
```

No arguments. Reads `DATA_DIR` from the environment.

---

## `sirop switch`

Change the active batch. All subsequent commands (`tap`, `boil`, `pour`,
`grade`, `verify`) will operate on this batch unless `--batch` is passed
explicitly.

```
sirop switch <name>
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `name` | yes | Name of an existing batch (without `.sirop`). |

### Example

```
$ sirop switch alice2024
Active batch: alice2024

$ sirop list
  alice2024 *
  my2025tax
```

### Error case

```
$ sirop switch nonexistent
error: batch 'nonexistent' not found at data/nonexistent.sirop
```

---

## Storage on disk

### During development

`DATA_DIR` defaults to `./data` relative to the working directory. The
`data/` folder is gitignored. Your `.sirop` files stay out of version control
automatically.

```
# .env (or defaults if .env is absent)
DATA_DIR=./data
```

### On your own machine

Set `DATA_DIR` to any path you prefer:

```env
DATA_DIR=/home/alice/taxes/sirop
```

### Future: platform-standard paths

When multi-user or GUI support is added, sirop will default to
platform-standard locations via `platformdirs`:

| Platform | Default path |
|----------|-------------|
| Linux | `$XDG_DATA_HOME/sirop/` (usually `~/.local/share/sirop/`) |
| macOS | `~/Library/Application Support/sirop/` |
| Windows | `%APPDATA%\sirop\` |

`DATA_DIR` in `.env` overrides platform defaults.

---

## The `.active` file

`DATA_DIR/.active` is a plain-text file containing the name of the active
batch (no extension, no newline). All pipeline commands read it to determine
which `.sirop` file to open.

```
$ cat data/.active
my2025tax
```

You can edit it directly, but `sirop switch` is safer. Pointing `.active` at
a name with no corresponding `.sirop` file will cause subsequent commands to
fail with a clear error.

---

## Inspecting a batch manually

Because `.sirop` files are plain SQLite, you can inspect them without sirop:

```bash
# Python
python3 -c "
import sqlite3
conn = sqlite3.connect('data/my2025tax.sirop')
print(conn.execute('SELECT * FROM batch_meta').fetchall())
print(conn.execute('SELECT stage, status FROM stage_status').fetchall())
"

# sqlite3 CLI
sqlite3 data/my2025tax.sirop '.tables'
sqlite3 data/my2025tax.sirop 'SELECT * FROM batch_meta'
sqlite3 data/my2025tax.sirop 'SELECT stage, status FROM stage_status'
```
