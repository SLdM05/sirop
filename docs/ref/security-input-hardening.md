# sirop — Security Input Hardening Rules

This document records the security invariants established during the 2025-02
pipeline security review. Every new module, command, or pipeline stage must
comply with these rules before being merged.

The golden rule: **trust nothing that crosses a boundary into the process.**
User CLI arguments, CSV file contents, YAML config files, node API responses,
and the `.active` pointer file are all untrusted inputs. The SQLite `.sirop`
file itself is treated as trusted only after the schema version check passes.

---

## 1. SQL — Parameterized Queries Only

**Rule:** All SQL executed against a `.sirop` file must use `?` placeholder
syntax. No exceptions.

**Banned patterns — any of these is a bug:**

```python
# WRONG — f-string in SQL
conn.execute(f"SELECT * FROM {table_name} WHERE asset = '{asset}'")

# WRONG — .format() in SQL
conn.execute("SELECT * FROM {} WHERE id = {}".format(table, row_id))

# WRONG — % formatting in SQL
conn.execute("INSERT INTO audit_log VALUES ('%s', '%s')" % (stage, msg))
```

**Correct pattern:**

```python
# RIGHT — bound parameters
conn.execute(
    "SELECT * FROM transactions WHERE asset = ? AND timestamp >= ?",
    (asset, since),
)
conn.executemany(
    "INSERT INTO stage_status (stage, status) VALUES (?, 'pending')",
    [(stage,) for stage in PIPELINE_STAGES],
)
```

**Why this matters here:** CSV files fed to `tap` can contain arbitrary text in
every field, including transaction descriptions, counterparty notes, and txids.
Any of those values that flows into an unparameterized query is a SQL injection
vector. The ruff `S` (Bandit) ruleset catches most violations, but it does not
catch all dynamic SQL patterns — code review is the second line of defence.

**DDL (schema definitions):** DDL strings are module-level `Final` constants,
never constructed at runtime. User input must never appear in a DDL string.

---

## 2. Batch Names — Validated Before Filesystem Use

**Rule:** Batch names are validated by `_validate_batch_name()` in
`src/sirop/db/connection.py` before any filesystem operation. Do not bypass
this call or duplicate the logic elsewhere.

The allowed regex is: `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`

- Starts with a letter or digit
- Contains only letters, digits, underscores, hyphens
- 1 to 64 characters

**What this prevents:**

| Input | Attack |
|-------|--------|
| `../../../tmp/attack` | Path traversal outside `DATA_DIR` |
| `/etc/passwd` | Absolute path injection |
| `'; DROP TABLE dispositions;--` | SQL injection via batch name column |
| `foo bar`, `foo$bar`, `foo\`cmd\`` | Shell injection if name is ever passed to a subprocess |
| `` (empty) | Degenerate path `DATA_DIR/.sirop` |

**Implementation note:** Both `get_batch_path()` and `set_active_batch()` call
`_validate_batch_name()`. Any new function that constructs a path from a batch
name must do the same. The validation lives in one place — do not copy the
regex; import the function.

---

## 3. YAML Config Loading — `safe_load` Only

**Status:** Config loader not yet implemented. This rule applies when it is.

**Rule:** All YAML files (importer configs, custom importers stored in the DB,
user-provided configs) must be loaded exclusively with `yaml.safe_load()`.
`yaml.load()` without an explicit `Loader` is banned.

```python
# WRONG — executes arbitrary Python via !!python/object tags
data = yaml.load(stream)
data = yaml.load(stream, Loader=yaml.Loader)
data = yaml.load(stream, Loader=yaml.UnsafeLoader)

# RIGHT
data = yaml.safe_load(stream)
```

**Why this matters:** `yaml.load()` can deserialize arbitrary Python objects.
A crafted YAML file with `!!python/object/apply:os.system ["rm -rf /"]` would
execute that command in the sirop process. `yaml.safe_load()` restricts
deserialization to basic Python types only.

The ruff rule `S506` (`unsafe-yaml-load`) flags `yaml.load()` calls — ensure
it remains enabled. After loading, the raw dict must be validated against a
pydantic schema before any field is used. Do not access raw dict keys directly
from untrusted YAML.

---

## 4. Importer `date_format` — Validate Before Use in `strptime`

**Status:** Implemented. `load_importer_config()` in
`src/sirop/importers/base.py` calls `_validate_date_format()` before returning
the config. Any disallowed token raises ``InvalidCSVFormatError`` at load time.

**Rule:** The `date_format` field in importer YAML configs must be validated
against an allowlist of strptime tokens before being passed to
`datetime.strptime()`.

`datetime.strptime()` is not an RCE vector, but a malformed or adversarial
format string can silently accept garbage date values, producing wrong UTC
timestamps that corrupt ACB calculations in ways that are hard to detect.

**Allowlist approach:** accept only format strings composed of the tokens
explicitly used in the four supported importers:

```python
# Allowed strptime tokens
_ALLOWED_STRPTIME_TOKENS = frozenset({
    "%Y", "%m", "%d",          # date parts
    "%H", "%M", "%S",          # time parts
    "%f",                      # microseconds
    "%z",                      # UTC offset
    "%Z",                      # timezone name
    "-", "T", ":", ".", " ",   # separators
    "+",                       # offset sign
})
```

Validate by tokenizing the format string and rejecting any token not in the
allowlist. Fail fast at config load time, not at first parse attempt.

---

## 5. Node API Responses — Validate Shape Before Use

**Status:** Node verification module not yet implemented. This rule applies
when it is.

**Rule:** Treat responses from Bitcoin Core RPC and Mempool REST as untrusted.
Validate the structure and types of every field before using values in
`verified_transactions` or `audit_log`.

Key checks:

- `confirmations` must be a non-negative integer, not a string or float
- `block_time` must parse to a valid UTC timestamp within a plausible range
  (not a negative number, not a date in 2140)
- `txid` in the response must match the txid that was queried — reject responses
  where these differ (prevents response-swapping attacks if the backend is
  compromised or misconfigured)
- BTC amounts from the node are in satoshis (integer) — convert with
  `Decimal(satoshis) / Decimal(100_000_000)`, never with float division

Do not propagate raw API field values into the DB without this validation.

---

## 6. `btc_node_backend` — Constrained to Known Values

`btc_node_backend` in `src/sirop/config/settings.py` is typed as
`Literal["rpc", "mempool"]`. pydantic rejects any other value at startup.

When the node module is implemented, add a `match` statement on this value with
an explicit `case _: raise AssertionError` branch so mypy exhaustiveness
checking catches any future literal addition that isn't handled.

---

## 7. CSV Parsing — Guard Against Malformed Row Width

**Status:** Implemented in `src/sirop/importers/base.py` — `_read_csv()`.

**Rule:** Never call `.strip()` (or any string method) on a value from
`csv.DictReader` without first confirming it is a `str`.

`csv.DictReader` places overflow columns (more values than headers) into the
row dict under a `None` key as a `list[str]`. Calling `.strip()` on that list
raises `AttributeError` and crashes the import on any malformed or
wide-format CSV file.

**Banned pattern:**

```python
# WRONG — crashes with AttributeError when row has more columns than headers
if all(v.strip() == "" for v in row.values()):
    ...
```

**Correct pattern:**

```python
# RIGHT — isinstance guard prevents .strip() on list values
if all(isinstance(v, str) and not v.strip() for v in row.values()):
    ...
```

The `isinstance` check short-circuits for non-`str` values (the overflow
list), so they are treated as non-empty and the row is never incorrectly
skipped.

---

## 8. Logging — Never Log Unredacted Sensitive Values by Default

The `SensitiveDataFilter` in `src/sirop/utils/logging.py` redacts txids,
addresses, and amounts in all log output by default. New log calls must not
circumvent this:

```python
# WRONG — bypasses the filter by pre-formatting the string
logger.info("Processing txid " + txid)
logger.info("Processing txid %s" % txid)

# RIGHT — the filter sees the args and can redact
logger.info("Processing txid %s", txid)
```

Always pass values as `logger.*()` arguments, never pre-formatted into the
message string. The filter operates on `record.args`, not on pre-built strings.

---

## 9. Test Fixtures — Fake Data Only

No real txids, addresses, xpubs, amounts, or exchange account identifiers may
appear in test fixtures, test parametrize lists, or comments.

**Conventions for fake values:**

| Type | Fake value pattern |
|------|--------------------|
| txid | `"aaa" + "0" * 61` (64 chars, obviously synthetic) |
| Bitcoin address | `"bc1qfakeaddressfortesting"` |
| xpub | `"xpubFAKE..."` |
| CAD amount | Round numbers like `Decimal("1000.00")` |
| Exchange name | `"TestExchange"` |

---

## 10. Wallet Names — Same Rules as Batch Names

**Rule:** External wallet names (from `stir external` and the `transfer`
wizard) are validated by `_validate_wallet_name()` in
`src/sirop/cli/stir.py` before any override is written.

Allowed pattern: `^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`

| Input | Status |
|-------|--------|
| `cold-vault` | allowed |
| `ledger_2025` | allowed |
| `cold storage` | rejected (space) |
| `vault!` | rejected (special char) |
| `` (empty) | allowed (name is optional) |

Empty string is accepted — the user is not required to name an external wallet.
Non-empty names follow the same character set as batch names so that wallet
labels are safe for display, filenames, and future CLI use without shell
quoting.

The same `_validate_wallet_name()` function is called by:
- `_cmd_external()` (interactive `external` command)
- `_cmd_transfer()` wizard (external path)

---

## Summary Checklist

Use this when reviewing any new pipeline module:

- [ ] All SQL uses `?` placeholders — no f-strings, `.format()`, or `%` in queries
- [ ] Batch names pass through `_validate_batch_name()` before filesystem use
- [ ] Wallet names pass through `_validate_wallet_name()` before any override is written
- [ ] YAML loaded with `yaml.safe_load()` and parsed through a pydantic schema
- [x] `date_format` validated against the strptime allowlist before use
- [ ] Node API responses validated for shape, types, and txid match
- [x] CSV row values guarded with `isinstance(v, str)` before calling string methods
- [ ] Log calls pass values as arguments, not pre-formatted strings
- [ ] Test fixtures use fake data only
