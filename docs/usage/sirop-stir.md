# sirop stir — Review and Override Transfer Matching

## What `stir` does

`stir` lets you inspect how sirop has auto-detected wallet-to-wallet transfer
pairs across your imported sources, and manually correct any mismatches before
running `boil`.

The transfer matcher automatically pairs withdrawals to deposits using shared
`txid` or amount+timestamp proximity. `stir` shows you those auto-detected
pairs and lets you:

- **Force-link** two transactions that the auto-matcher missed or got wrong.
- **Force-unlink** a pair that was incorrectly matched.
- **Mark external transfers** — withdrawals to, or deposits from, wallets you
  haven't imported (cold storage, a gift, etc.) — so they aren't treated as
  taxable sells or buys.

Overrides are written to the `transfer_overrides` table in the active `.sirop`
file. They survive `sirop boil --from transfer_match` — you set them once and
they persist through every subsequent re-run.

**Requires:** `tap` complete. Does not require `boil`.

---

## Syntax

```
sirop stir [--list]
sirop stir --link <id1> <id2> [--fee AMOUNT]
sirop stir --unlink <id1> <id2>
sirop stir --external-out <id> [--wallet NAME]
sirop stir --external-in <id> [--wallet NAME]
sirop stir --clear <id>
```

### Arguments

| Argument | Description |
|----------|-------------|
| *(no flags)* | Launch the interactive REPL |
| `--list` | Print current transfer state and active overrides, then exit |
| `--link <id1> <id2>` | Force-link two transactions as a transfer pair |
| `--fee AMOUNT` | With `--link`: implied on-chain fee (sent minus received, in asset units). Emitted as a `fee_disposal` event at boil time. |
| `--unlink <id1> <id2>` | Prevent the auto-matcher from pairing these two transactions |
| `--external-out <id>` | Mark a withdrawal as going to an untracked external wallet |
| `--external-in <id>` | Mark a deposit as arriving from an untracked external wallet |
| `--wallet NAME` | With `--external-*`: label for the external wallet (e.g. `"cold-storage"`) |
| `--clear <id>` | Remove all overrides that involve this transaction ID |

Transaction IDs (`id1`, `id2`, `id`) are the `id` values from the `transactions`
table — shown in `stir --list` output and in the interactive display.

---

## Interactive mode

Running `sirop stir` with no flags launches an interactive REPL that shows the
full transfer state and accepts commands.

```
$ sirop stir

Active batch: my2025tax  (2025)
Transactions: 78   Transfer events: 6   Active overrides: 0

AUTO-MATCHED PAIRS (4)
  tx:3  [ndax]     2024-01-16 10:00  withdrawal  BTC  0.10000000
  tx:4  [sparrow]  2024-01-16 10:30  deposit     BTC  0.10000000
  — reason: amount + timestamp proximity (30 min)

  tx:5  [sparrow]  2024-04-15 10:00  withdrawal  BTC  0.10000000
  tx:6  [shakepay] 2024-04-15 12:00  deposit     BTC  0.10000000
  — reason: shared txid 2222...

UNMATCHED WITHDRAWALS (1)  ← will be treated as SELLS unless linked
  tx:41 [ndax]     2024-09-03 14:22  withdrawal  ETH  2.5

ACTIVE OVERRIDES (0)

stir> _
```

### Interactive commands

| Command | Effect |
|---------|--------|
| `link <id1> <id2>` | Force-link two transactions as a transfer pair. Prompts for implied fee. |
| `unlink <id1> <id2>` | Block the auto-matcher from pairing these two. |
| `external <id>` | Guided wizard: mark as external-out or external-in with wallet label. |
| `clear <id>` | Remove all overrides that involve this transaction. |
| `help` | Show command reference. |
| `quit` | Exit. Overrides already written are saved. |

---

## Transfer state display

`stir --list` (and the REPL on startup) prints five sections:

### Auto-matched pairs

Pairs found by automatic matching. Shown with the matching reason (`txid` or
`amount + timestamp proximity`). These become non-taxable transfers at boil time
unless overridden.

### Forced links

Pairs you manually linked with `stir --link`. Shows the implied fee if
non-zero — that fee will be emitted as a `fee_disposal` at boil time.

### External transfers

Withdrawals and deposits you marked as going to / coming from an untracked
wallet. These are excluded from taxable events.

### Forced unlinks

Auto-matched pairs you've blocked. Both legs will be treated as separate
taxable events (sell or buy) when `boil` runs.

### Unmatched withdrawals

Withdrawals with no matching deposit — either not yet imported, or genuinely
external. These will be treated as **sells** at boil time unless you:
- `stir link` them to a matching deposit in another source, or
- `stir --external-out` them if they went to a wallet you don't intend to import.

---

## Override actions in detail

### `link` — force a transfer pair

Use when sirop missed a cross-source transfer (e.g., no shared txid and
timestamps are too far apart for auto-matching).

```bash
sirop stir --link 41 87
```

If the sent amount differs from the received amount (on-chain fee deducted),
pass `--fee` with the difference in asset units:

```bash
# Sent 0.10 BTC from NDAX, received 0.09997000 BTC in Sparrow
# On-chain fee = 0.00003000 BTC
sirop stir --link 41 87 --fee 0.00003000
```

The implied fee becomes a `fee_disposal` event at `boil` time — a micro-
disposition of BTC at its CAD value on the transfer date, treated as a
taxable disposal per CRA guidance on network fees.

### `unlink` — block an incorrect auto-match

Use when sirop incorrectly matched two transactions (e.g., coincidentally
similar amounts near the same time from different exchanges).

```bash
sirop stir --unlink 12 34
```

Both transactions will re-enter the auto-matcher pass but the specific pair
(12, 34) will be blocked. If neither matches anything else, both become
unmatched events (sell or buy).

### `external-out` — withdrawal to an untracked wallet

Use for withdrawals to cold storage, a hardware wallet you haven't imported,
or any wallet outside this batch.

```bash
sirop stir --external-out 41 --wallet "cold-storage"
```

The transaction is excluded from taxable events. **No ACB is deducted.** The
BTC is still in your pool — you'll account for it when you eventually sell from
that wallet (which should be a separate batch or a future tap of that wallet's
export).

### `external-in` — deposit from an untracked wallet

Use for deposits from a wallet you haven't imported (gift received, transfer
from prior-year holdings, etc.).

```bash
sirop stir --external-in 87 --wallet "prior-year-cold-storage"
```

The deposit is excluded from taxable events but its CAD value at receipt
establishes ACB for those units.

### `clear` — remove all overrides for a transaction

```bash
sirop stir --clear 41
```

Removes every override where `tx_id_a = 41` or `tx_id_b = 41`. Run
`sirop boil --from transfer_match` afterward to recompute.

---

## Workflow

```
sirop tap <files>           # import all sources
sirop stir --list           # review auto-detected pairs
sirop stir --link ...       # fix mismatches (if any)
sirop stir --external-out ..# mark external withdrawals (if any)
sirop boil                  # run tax calculations with overrides applied
```

After correcting overrides and re-running `boil`:

```bash
sirop boil --from transfer_match
```

This re-runs transfer matching onward, picking up all stir overrides, without
re-importing or re-normalising.

---

## Stage state

`stir` does not update `stage_status`. It is not a pipeline stage — it is
an interactive correction tool that writes to `transfer_overrides`, which is
consumed when `transfer_match` runs.

Running `stir` before `normalize` completes will fail:

```
error [E020]: Batch 'my2025tax' has not been normalized yet.
              Run 'sirop boil' first (or at least up to normalize).
```

---

## Error cases

| Situation | Message |
|-----------|---------|
| No active batch | `error: no active batch. Run 'sirop create <name>' first.` |
| Batch not normalized | `error [E020]: Batch '…' has not been normalized yet. Run 'sirop boil' first.` |
| Transaction ID not found | `error [E021]: Transaction id 99 not found in the active batch.` |
| Link: different assets | `error: cannot link tx:3 (BTC) with tx:41 (ETH) — asset mismatch.` |
| Link: receive > send | `error: implied fee is negative — received amount exceeds sent amount.` |

---

## Inspecting overrides directly

```python
import sqlite3

conn = sqlite3.connect("data/my2025tax.sirop")
conn.row_factory = sqlite3.Row

for r in conn.execute(
    "SELECT * FROM transfer_overrides ORDER BY created_at"
):
    print(dict(r))
```
