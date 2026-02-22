# sirop boil — Run the Tax Calculation Pipeline

## What `boil` does

`boil` runs all tax calculation stages on the active batch, picking up from
where `tap` left off. It processes raw imported transactions through five
sequential stages and leaves fully computed, adjusted dispositions in the
`.sirop` file, ready for `pour` (report generation).

The five stages `boil` runs, in order:

| Stage | Internal name | What it does |
|-------|---------------|-------------|
| 1 | `normalize` | Converts raw transactions to CAD, standardises timestamps |
| 2 | `verify` | Promotes transactions to verified (pass-through until node verification is available) |
| 3 | `transfer_match` | Detects wallet-to-wallet transfers; classifies everything else as taxable events |
| 4 | `boil` | Computes weighted-average ACB and capital gain/loss for every disposal |
| 5 | `superficial_loss` | Applies the 61-day superficial loss rule; adjusts denied losses |

Each stage reads from and writes to the `.sirop` file atomically. If a stage
is already marked `done`, it is skipped — rerunning `boil` on a complete batch
is a no-op unless you pass `--from`.

---

## Syntax

```
sirop boil [--from STAGE]
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--from STAGE` | no | Re-run the pipeline starting from this stage. Earlier stages are skipped; this stage and all later ones are re-run even if `done`. |

Valid `--from` values: `normalize`, `verify`, `transfer_match`, `boil`, `superficial_loss`.

---

## Stage details

### normalize

Reads every row from `raw_transactions` and produces a row in `transactions` with:

- **Transaction type**: maps exchange-specific type strings (e.g. NDAX's `"trade"`) to
  canonical types (`buy`, `sell`, `withdrawal`, `income`, …).
- **CAD conversion**: if the exchange reported the value in CAD, it is used directly.
  For USD-denominated values, the Bank of Canada daily rate is fetched (or read from
  the `boc_rates` cache) and applied. Missing rates produce a warning and fall back
  to `0`, which must be corrected before `pour`.
- **Timestamps**: normalised to UTC ISO 8601.

BoC rates are cached in the `boc_rates` table after the first fetch — they are
never re-fetched for the same date.

### verify

Currently a pass-through stage: copies `transactions` → `verified_transactions` unchanged.
When Bitcoin node verification is implemented, this stage will cross-check on-chain
timestamps, amounts, and fees, writing any overrides to `audit_log`.

### transfer_match

Identifies wallet-to-wallet transfers so they are not treated as dispositions.
Overrides written by `sirop stir` are applied first; auto-matching runs after.

**Pass 0 — apply stir overrides:**

- `link` overrides: force two transactions into a transfer pair. If the sent
  amount exceeds the received amount, the difference is emitted as a
  `fee_disposal` event using `implied_fee_crypto`.
- `external-out` / `external-in` overrides: mark a withdrawal or deposit as
  going to / coming from an untracked external wallet. Excluded from taxable
  events.
- `unlink` overrides: prevent the auto-matcher from pairing those two
  transactions.

**Pass 1 — auto-matching (respects unlinks):**

- Matches withdrawals to deposits of the same asset by shared `txid` (primary
  signal) or by amount proximity (±1%) + timestamp proximity (±4 hours).

**Classification:**

- Matched transfer legs → `classified_events` with `is_taxable=0` — stored for
  auditability but never enter the ACB engine.
- On-chain network fees on matched transfers → **fee micro-dispositions**
  (`event_type = fee_disposal`) — small taxable disposals at the prevailing rate.
- Income events (staking rewards, airdrops) → both `classified_events` and
  `income_events`. The income establishes ACB at fair market value.
- Unmatched withdrawals → treated conservatively as sells. A warning is emitted
  so you can decide whether to `stir link` or tap the receiving wallet.

> **Tip:** Run `sirop stir` between `tap` and `boil` to review auto-detected
> pairs, fix mismatches, and mark external transfers before tax calculations run.
> Overrides survive `sirop boil --from transfer_match` so you only set them once.

### boil (ACB engine)

Implements the CRA-mandated **weighted-average ACB method** for each asset:

**On acquisition (buy / income):**
```
new_total_acb  = prior_acb + cost_cad + fees_cad
new_units      = prior_units + acquired_units
new_acb_per_unit = new_total_acb / new_units
```

**On disposition (sell / fee_disposal / spend):**
```
acb_of_disposed = acb_per_unit × units_sold
gain_loss       = proceeds_cad − acb_of_disposed − selling_fees_cad
```

Results are written to `dispositions` (one row per taxable disposal) and
`acb_state` (a pool snapshot after every event).

### superficial_loss

Applies **Section 54 of the Income Tax Act** to each loss disposition.

A loss is superficial when all three conditions hold:
1. The disposal is at a loss (`gain_loss < 0`).
2. The same asset was acquired in the **61-day window** (30 days before through
   30 days after the sale date).
3. The asset is still held at the end of the 30th day after the sale.

When triggered:
```
superficial_portion = min(units_sold, units_reacquired, units_held_at_day_30)
denied              = abs(loss) × (superficial_portion / units_sold)
allowable           = abs(loss) − denied
```

The denied amount is added to the ACB of the repurchased units (loss deferral,
not destruction). `adjusted_gain_loss` in `dispositions_adjusted` is what
flows to `pour` and the tax forms.

---

## Re-running from a specific stage

Use `--from` when you need to re-process from a particular stage without
starting over:

```bash
# Re-run everything from transfer matching onward
sirop boil --from transfer_match

# Only re-run the ACB and superficial loss stages
sirop boil --from boil
```

When `--from STAGE` is given:
1. All stages before `STAGE` are left untouched.
2. `STAGE` and all stages after it are marked `invalidated` and their output
   rows are deleted from the `.sirop` file.
3. The pipeline then runs `STAGE` through `superficial_loss`.

This is useful when you manually correct a classified event (e.g., override
an unmatched withdrawal to a transfer) and need to recompute ACB without
re-importing.

---

## What gets written

| Table | Written by | Content |
|-------|------------|---------|
| `transactions` | normalize | Normalised transactions with CAD values |
| `verified_transactions` | verify | Promoted copy of `transactions` |
| `classified_events` | transfer_match | Taxable events + non-taxable transfer markers |
| `income_events` | transfer_match | Income sub-records for TP-21.4.39-V |
| `dispositions` | boil | One row per disposal with gain/loss and ACB pool state |
| `acb_state` | boil | Pool snapshot after every taxable event |
| `dispositions_adjusted` | superficial_loss | Final adjusted dispositions for `pour` |

All writes are atomic: if a stage fails, nothing from that stage is committed.

---

## Stage state after a successful `boil`

```
tap               → done   (unchanged)
normalize         → done
verify            → done
transfer_match    → done
boil              → done
superficial_loss  → done
pour              → pending
```

---

## Example sessions

### Fresh batch — run all stages

```
$ sirop create my2025tax --year 2025
Created batch: my2025tax (2025) → data/my2025tax.sirop

$ sirop tap ~/Downloads/ndax_2025_ledger.csv
Detected format: NDAX
Tapped 47 transaction(s) from ndax_2025_ledger.csv [NDAX] into 'my2025tax'.

$ sirop tap ~/Downloads/shakepay_2025.csv
Detected format: Shakepay
Tapped 23 transaction(s) from shakepay_2025.csv [Shakepay] into 'my2025tax'.

$ sirop tap ~/Downloads/sparrow_wallet.csv
Detected format: Sparrow Wallet
Tapped 8 transaction(s) from sparrow_wallet.csv [Sparrow Wallet] into 'my2025tax'.

# Optional: review and fix transfer pairs before calculating
$ sirop stir --list
# (inspect auto-detected pairs, link/unlink as needed)

$ sirop boil
 normalize    Checking sap levels...
 verify       verify: 47 row(s) promoted to verified_transactions
 transfer_match  Tracing the flow...
 boil         Boiling the sap...
 superficial_loss  superficial_loss: 0 superficial loss(es) found

Batch 'my2025tax' pipeline complete.
  transactions:               47
  classified_events:          38
  income_events:               3
  dispositions:               12
  dispositions_adjusted:      12

  Net capital gain/loss: +4,209.35 CAD (before inclusion rate)

  [       done]  tap
  [       done]  normalize    2025-04-15
  [       done]  verify       2025-04-15
  [       done]  transfer_match  2025-04-15
  [       done]  boil         2025-04-15
  [       done]  superficial_loss  2025-04-15
  [    pending]  pour
```

### Re-running a completed batch (no-op)

```
$ sirop boil
 normalize    skipping (status=done)
 verify       skipping (status=done)
 transfer_match  skipping (status=done)
 boil         skipping (status=done)
 superficial_loss  skipping (status=done)

Batch 'my2025tax' pipeline complete.
  ...
```

### Re-running from a specific stage

```
$ sirop boil --from boil
 normalize    skipping (status=done)
 verify       skipping (status=done)
 transfer_match  skipping (status=done)
 boil         Boiling the sap...
 superficial_loss  superficial_loss: scanning for 61-day window violations

Batch 'my2025tax' pipeline complete.
  ...
```

---

## Error cases

| Situation | Message |
|-----------|---------|
| No active batch | `error: no active batch. Run 'sirop create <name>' first.` |
| `tap` not run yet | `error: batch 'X' has not been tapped yet. Run 'sirop tap <file>' first.` |
| Stage already running | `error: batch 'X' stage 'normalize' is currently running (another process?). Aborting.` |
| Invalid `--from` value | `error: unknown stage 'foo'. Valid stages: normalize, verify, transfer_match, boil, superficial_loss` |
| Missing `config/tax_rules.yaml` | `error: tax rules config not found at config/tax_rules.yaml. Is the working directory the project root?` |
| Disposal with no prior acquisition | `error: Cannot dispose of X BTC: pool only holds Y units. Check that all buy transactions have been imported.` |

---

## Inspecting results

```python
import sqlite3
from decimal import Decimal

conn = sqlite3.connect("data/my2025tax.sirop")
conn.row_factory = sqlite3.Row

# All dispositions with their adjusted gain/loss
for row in conn.execute("""
    SELECT d.timestamp, d.asset, d.units, d.proceeds,
           da.adjusted_gain_loss, da.is_superficial_loss
    FROM dispositions d
    JOIN dispositions_adjusted da ON da.disposition_id = d.id
    ORDER BY d.timestamp
"""):
    print(dict(row))

# Net capital gain/loss
row = conn.execute(
    "SELECT SUM(CAST(adjusted_gain_loss AS REAL)) FROM dispositions_adjusted"
).fetchone()
print(f"Net: {row[0]:,.2f} CAD")

# ACB pool state at end of year
for row in conn.execute(
    "SELECT asset, units, pool_cost FROM acb_state "
    "WHERE id IN (SELECT MAX(id) FROM acb_state GROUP BY asset)"
):
    print(dict(row))

# Stage status
for row in conn.execute("SELECT stage, status, completed_at FROM stage_status"):
    print(dict(row))
```

---

## Tax rules configuration

`boil` reads `config/tax_rules.yaml` before running. Current values for 2025:

```yaml
# config/tax_rules.yaml
capital_gains_inclusion_rate: 0.50     # 50% — unchanged for 2025 (no threshold)
superficial_loss_window_days: 30       # 30 days before + day of + 30 days after = 61-day window
```

To change the inclusion rate or window (e.g. for a different tax year), edit this
file. No code changes are required.
