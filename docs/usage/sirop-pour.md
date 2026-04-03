# sirop pour — Tax Report Generator

`sirop pour` reads the computed results from the active batch and writes two
Markdown (`.md`) files to `OUTPUT_DIR`:

| File | Purpose |
|------|---------|
| `{batch}-{year}-tax-report.md` | **Filing summary** — the numbers you enter on your forms |
| `{batch}-{year}-tax-detail.md` | **Audit backup** — full transaction log, keep for your records |

Both files are plain text Markdown. They are not PDFs. Open them in any
Markdown viewer, text editor, or paste the tables directly into a spreadsheet.

---

## Usage

```
sirop pour [--output-dir PATH]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--output-dir PATH` | `OUTPUT_DIR` from `.env` (default `./output`) | Override the output directory for this run only. |

---

## Prerequisites

The full pipeline must be complete before `pour` will run:

```
sirop create → tap → boil (includes normalize, verify, transfer_match, superficial_loss)
```

If any upstream stage has not completed, `pour` exits with an error and tells
you which stage to run.

---

## What the filing summary contains

`{batch}-{year}-tax-report.md` contains only what you need to fill in your
tax forms. Nothing more.

### Part A — Federal · CRA Schedule 3

- Dispositions table (one row per sale/trade)
- Summary: total proceeds, ACB, expenses, net gain/loss, taxable capital gains → **T1 Line 12700**

### Part B — Quebec · TP-21.4.39-V and Schedule G

- **Part 3** — acquisitions aggregated by asset type
- **Part 4** — dispositions aggregated by asset type
- **Part 5** — year-end holdings (units held + ACB)
- **Part 6** — cryptoasset income aggregated by asset and income type
  (e.g. one row for "BTC / staking" with the full-year total)
  → T1 Line 13000 and Schedule G / TP-1-V Line 154
- Schedule G capital gains summary → **TP-1-V Line 139**
- Mandatory TP-21.4.39-V filing reminder and penalty notice

### What is not in the filing summary

The per-transaction income event log (e.g. 300+ individual daily Shakepay
rewards) is **not** in the filing summary. Neither CRA nor Revenu Québec
require individual line items for staking/reward income — only the annual
total per asset type is entered on the return. The full log is in the detail
backup file.

---

## What the detail backup contains

`{batch}-{year}-tax-detail.md` is your audit trail. Keep it with your tax
records but do not file it.

- **Full income event log** — every individual reward, interest, or airdrop
  event with date, asset, units, FMV, and source
- **Superficial loss detail** — each denied loss with gross loss, denied
  amount, and allowable loss (only present if superficial losses were triggered)
- **Year-end ACB pool** — carry-forward cost basis for next year's filing

---

## Example output

```
$ sirop pour
Tax report written to output/my2025tax-2025-tax-report.md
Detail backup written to output/my2025tax-2025-tax-detail.md
```

Override the output directory:

```
$ sirop pour --output-dir /tmp/review
Tax report written to /tmp/review/my2025tax-2025-tax-report.md
Detail backup written to /tmp/review/my2025tax-2025-tax-detail.md
```

---

## Error cases

```
$ sirop pour
# No active batch set:
[E024] No active batch. Run `sirop create` or `sirop switch` first.

# Pipeline not complete:
[E026] The superficial loss stage has not completed for batch 'my2025tax'. Run `sirop boil` first.
```

---

## How to use the filing summary

### TurboTax Canada / UFile (federal)

Enter each row from **Part A → Schedule 3 → "Other properties"** section.
Enter the taxable capital gains total on **T1 Line 12700**.
Enter the income total on **T1 Line 13000**.

### TurboTax Quebec / ImpôtRapide (Quebec)

Enter capital gains on **Schedule G** using the same columns as Part A.
Enter **TP-1-V Line 139** from the Schedule G summary.

### TP-21.4.39-V (mandatory Quebec form)

Complete the form using the grouped summaries from Part B sections 3–6.
Check **TP-1-V Line 24** to indicate the form was filed.
Penalty for late or missing filing: **$10/day, maximum $2,500**.
