# Reconciling incomplete records — manual adjustments

When sirop's calculated ACB pool does not match your actual wallet or exchange
balance, you have a record-keeping gap.  This is common: defunct exchanges
(QuadrigaCX, FTX, Mt. Gox), pre-2018 records, hardware wallets whose history
you never exported, exchanges that delete history older than N years.  This
document explains the workflow sirop offers, what the CRA expects, and how
to keep the resulting figures defensible.

> **Disclaimer.** This is a description of how the tool works and a
> high-level summary of well-established CRA expectations.  It is not legal
> or tax advice.  If a real audit is on the table or your figures are
> material, talk to a qualified Canadian tax professional.

---

## When you need this

Symptoms:

- `sirop boil` prints a `[W005] ACB pool underrun` warning — sirop tried to
  dispose more units than it knows you held.
- The "Year-end holdings" table at the end of `boil` does not match the real
  balance shown in your wallet/exchange.
- A `pour` report shows zero ACB for a disposition that came from coins you
  bought before any of the CSVs you tapped.

Before reaching for a manual adjustment, exhaust the cheaper fixes:

1. Re-export and re-tap CSVs you may have missed (`sirop tap`).
2. Run `sirop stir --list` and walk through unmatched withdrawals/deposits —
   most "missing units" turn out to be unmatched transfers.
3. If you own a wallet sirop does not know about, tap it
   (`sirop tap <file> --wallet <name>`).

If none of those close the gap, the records are genuinely incomplete and a
manual adjustment is appropriate.

---

## CRA framing

Three load-bearing sources sit behind this:

- **Income Tax Act s.230** — every taxpayer must keep books and records
  sufficient to determine tax payable.  Records must be retained for six
  years from the end of the last tax year they relate to.
- **CRA Folio S3-F10-C1** — guidance on cryptocurrency: ACB is calculated
  using the weighted-average method, valued in CAD at the time of each
  transaction, on a "reasonable basis" when records are incomplete.
- **IT-479R** — capital property: the same reasonable-basis principle
  applies to securities and other capital assets.  Crypto is not unique
  here — the same rule has applied to lost paper share certificates,
  inherited assets without basis, and demutualisation events for decades.

Two postures are defensible:

| Posture | Effect on tax | When to use |
|--|--|--|
| **$0 ACB** for the unrecoverable portion | over-states the gain (more tax) | When you cannot reconstruct cost at all and prefer the conservative position. CRA does not object — this disadvantages the taxpayer. |
| **Best-estimate ACB** with documented methodology | accurate gain | When you have *some* evidence — bank statements showing CAD outflows, exchange screenshots, email confirmations — and a defensible date. |

The number itself is rarely what gets challenged in an audit.  What gets
challenged is whether you can explain *how* you arrived at it.  That is why
the next section — keeping a paper trail — matters more than the precise
figure.

---

## What sirop persists

Two tables back this feature:

- `manual_adjustments` — one row per adjustment.  Stores `kind`
  (`acquire`/`dispose`), `asset`, `units`, `cad_value`, `timestamp`,
  `reason`, `created_at`, `note`.  The `reason` field is mandatory at
  the CLI boundary.
- `audit_log` — append-only log of every adjustment add and remove.
  Stores `occurred_at`, `stage`, `field`, `old_value`, `new_value`,
  `reason`.  **Rows are never updated or deleted.**  Removing an
  adjustment with `clear-adjustment` writes a *second* `audit_log` row
  recording the removal — the original create entry stays intact.

You can inspect both with any SQLite browser by renaming the `.sirop` file
to `.db`, or with the `sirop_query.py` helper script.  This is the paper
trail you would point a CRA auditor at.

---

## Workflow

```bash
# 1. Confirm the gap.
sirop boil
# look for [W005] underruns and the year-end holdings table

# 2. Add an acquisition (or disposition) to close the gap.
#    This example reconstructs an early BTC purchase from a 2017 bank statement.
sirop stir --adjust-acquire BTC 0.5 12500.00 2017-11-15 \
  --reason "Reconstructed from RBC chequing statement showing CAD 12,500 transfer
            to QuadrigaCX on 2017-11-15. Statement saved at ~/Records/2017/rbc.pdf.
            BTC quantity from QCX dashboard screenshot, archived 2018-12-04."

# 3. Re-run the pipeline so the new event flows through ACB.
sirop boil --from transfer_match

# 4. Confirm the underrun is gone, and review the result.
sirop pour
```

The dispositions table in the resulting tax report will mark any row that
came from a manual adjustment with `⚠ Manual reconciliation entry`, and the
TP-21.4.39-V Part 3 acquisitions table will flag the asset.  The Markdown
report has a dedicated **Manual Reconciliation Entries** section listing
each adjustment and its reason.  These are visible to anyone reading the
output — there is no hiding them, by design.

To remove an adjustment:

```bash
sirop stir --list-adjustments              # see the IDs
sirop stir --clear-adjustment 3            # remove adj_id 3 (writes audit_log)
sirop boil --from transfer_match           # re-derive
```

---

## Keeping the paper trail

Sirop stores the *figure* and the *reason text*.  It does not store the
underlying evidence.  Keep that evidence outside sirop, organised so a
future you (or your accountant) can find it.  Suggested layout:

```
~/Records/
  2017/
    rbc-2017-11-statement.pdf
    quadriga-2017-12-04-screenshot.png
    quadriga-deposit-confirmation.eml
  2018/
    ...
  manual-adjustments-log.md       # one entry per adjustment, summarising the evidence
```

The `reason` you type into sirop should reference these files concretely —
"see ~/Records/2017/rbc-2017-11-statement.pdf line 3".  A reason like
"approximately 0.5 BTC, I think" is not defensible.

---

## What sirop will not do

- **Auto-reconcile** — sirop never invents adjustments on your behalf.
  Every entry is your decision and carries your reason.
- **Hide manual entries** — they are flagged in every report and listed in
  a dedicated section.  An accountant or auditor reading the output will
  see them.
- **Substitute for your evidence** — the `audit_log` records *that* you
  made the adjustment and *why you said you did*; it is not a proof of
  the underlying facts.

---

## Related

- `docs/usage/sirop-stir.md` — full CLI reference for the `stir` command.
- `docs/ref/database-schema.md` — table definitions for `manual_adjustments`
  and `audit_log`.
- `docs/ref/crypto-tax-reference-quebec-2025.md` — the broader ACB context.
