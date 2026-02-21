Implement a new exchange or wallet importer for sirop. Follow this checklist in order.

## Before you start

Read **`docs/ref/transaction-import-formats.md`** for the exchange's column spec, field
definitions, and any edge cases (unit detection, unconfirmed rows, fee availability, etc.).
Do not infer column semantics from column names alone.

---

## Checklist

### 1. YAML config — `config/importers/<exchange>.yaml`

Use `config/importers/ndax.yaml` as the structural template. Every value the importer
needs must come from this file — zero hardcoded column names in Python code.

Required fields:
- `name` — display name (e.g. `"Shakepay"`)
- `date_column` — CSV header for the timestamp column
- `date_format` — strptime format string or the sentinel `"iso8601"`
- `columns` — map of internal key → CSV header name
- `transaction_type_map` — map of raw CSV type string → sirop canonical type
- `fee_model` — `"explicit"` or `"spread"`
- `fingerprint_columns` — list of CSV headers unique to this exchange (used by FormatDetector)

Optional fields: `ignored_columns`, `fiat_asset_class`, `group_handlers`,
`unconfirmed_sentinel`, `amount_unit`, `fee_nullable`.

Annotate the YAML with comments explaining any non-obvious fields.

### 2. Importer class — `src/sirop/importers/<exchange>.py`

Extend `BaseImporter` from `src/sirop/importers/base.py`.

Rules:
- Use `self._config.columns["key"]` for every column access — never a literal string
- Use `self._lookup_type(raw_type)` for transaction type mapping
- Use `self._parse_timestamp(value)` for all date parsing
- Use `self._read_csv(path)` to load rows
- Parse all monetary values as `Decimal`, never `float`
- Emit `"buy"` or `"sell"` based on data (signed amounts, debit/credit currency) —
  never emit `"trade"` from an importer
- `parse()` must return results sorted chronologically

For exchanges with complex grouping (like NDAX), add private `_parse_*()` methods per
group type. Keep each method focused on one transaction shape.

### 3. Format detector — `src/sirop/importers/detector.py`

Register the new importer's `fingerprint_columns` so `sirop tap` can auto-detect the format.

### 4. Synthetic fixture — `tests/fixtures/<exchange>_synthetic.csv`

Create a minimal CSV covering every transaction type the importer handles. Use obviously
fake data: synthetic amounts, fake txids (`aaa...111`), fake addresses
(`bc1qfakeaddressfortesting`). No real personal data.

Include at least:
- One of each transaction type in `transaction_type_map`
- Both sides of a trade (buy + sell) if the format supports it
- A fee row if the format has explicit fees
- An edge-case row (zero amount, missing optional field, unknown type)

### 5. Tests — `tests/test_<exchange>_importer.py`

Required test groups:
- **Basic sanity**: list returned, non-empty, sorted chronologically, correct `source`
- **Timestamps**: all UTC, correct format parsed
- **Amounts**: all positive (sign stripped), `Decimal` type
- **Per type**: at least one test per transaction type checking `transaction_type`, `amount`,
  `fiat_value`, `fiat_currency`, `fee_amount`
- **Direction**: if the exchange has buy/sell trades, add regression tests that
  `transaction_type` is `"buy"` or `"sell"` — never `"trade"`
- **Error handling**: file not found → `InvalidCSVFormatError`; missing column →
  `MissingColumnError`; empty CSV → empty list

### 6. Run /check

All checks must pass before the importer is considered done.

---

## Definition of done

- [ ] YAML config with fingerprint columns
- [ ] Importer class with zero hardcoded column names
- [ ] Registered in FormatDetector
- [ ] Synthetic fixture with all transaction types
- [ ] Test file with direction regression guard
- [ ] `/check` passes (ruff, mypy, pytest)
