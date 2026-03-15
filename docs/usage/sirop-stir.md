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
  taxable sells or buys. An optional network fee can be recorded so `boil`
  emits a `fee_disposal` event for the transferred amount.

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
| `--clear <id>` | Remove all overrides that involve this transaction ID |

External wallet marking is done through the interactive `transfer` wizard (REPL only —
no non-interactive flag equivalent). See [Interactive mode](#interactive-mode) below.

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
| `transfer <id> [<id> ...]` | Guided wizard: link a withdrawal to its matching deposit (or vice versa), or mark it as going to/coming from an untracked wallet (no CSV available). Accepts one or more IDs — runs the wizard for each in sequence. |
| `link <id1> <id2>` | Force-link two transactions as a transfer pair. Prompts for implied fee. |
| `unlink <id1> <id2>` | Block the auto-matcher from pairing these two. |
| `clear <id>` | Remove all overrides that involve this transaction. |
| `list` | Refresh the unmatched transactions display. |
| `view` | Open the full transfer state in a pager (all sections). |
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
- Use `transfer <id>` → tracked wallet to link them to a matching deposit in another source, or
- Use `transfer <id>` → untracked wallet if they went to a wallet you don't intend to import.

---

## Override actions in detail

### `transfer` — guided wizard for unmatched transactions

Use in the REPL to interactively resolve an unmatched withdrawal or deposit.
The wizard presents two paths:

**Tracked wallet (CSV available)**
Picks the counterpart transaction from an imported source. The wizard lists
candidate transactions, validates that no asset units are created from nothing,
and records the implied on-chain fee when sent > received.

**Untracked wallet (no CSV available)**
Marks the transaction as external (replaces the former `external` command).
After selecting this path the wizard asks for an optional wallet name and then:

- **[N] No fee to record** — implies `fee = 0`. Use when the exchange covered
  the fee or it was already embedded in the spread.
- **[F] Enter fee amount** — prompts for an exact fee in asset units. The value
  must be positive and strictly less than the transaction amount. The fee is
  stored as `implied_fee_crypto` and becomes a `fee_disposal` event at `boil`
  time (a micro-disposition taxable at the asset's CAD value on transfer date).

```
stir> transfer 41
```

You can pass multiple IDs to process a queue in one go. The wizard runs
sequentially for each ID, printing a progress header between runs:

```
stir> transfer 7 12 15 16

  — Transfer 1 of 4 (id: 7) —
  Transfer wizard — transaction 7:
  ...

  — Transfer 2 of 4 (id: 12) —
  Transfer wizard — transaction 12:
  ...
```

If validation fails for one ID (wrong type, already handled, etc.) the error is
printed and the wizard moves on to the next ID without stopping the queue.

`transfer` only works in the interactive REPL. For non-interactive scripting use
`sirop stir --link <id1> <id2>` instead.

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

### External transfers — untracked wallet path in `transfer`

Use the `transfer` wizard to handle withdrawals to cold storage, a hardware
wallet you haven't imported, or deposits from prior-year holdings or gifts.
When prompted, choose **"[untracked wallet — no CSV available]"** and enter an
optional wallet name (e.g. `cold-storage`).

**External-out** (withdrawal to untracked wallet): the transaction is excluded
from taxable events. **No ACB is deducted.** The assets remain in your cost
pool — you account for them when you sell from that wallet in a future batch.

**External-in** (deposit from untracked wallet): excluded from taxable events,
but the CAD value at receipt establishes ACB for those units.

For both directions, the wizard then asks whether to record a network fee:

```
stir> transfer 41

  Transfer wizard — transaction 41:
  ...
  [untracked wallet — no CSV available]

  Wallet name (optional, e.g. cold-storage):  cold-storage

  Network fee for this transfer?
    [N] No fee to record
    [F] Enter fee amount manually
  Choice: F
  Fee amount (BTC): 0.00003000
  Fee recorded — will become a fee micro-disposal at boil time.
```

There are no `--external-out` / `--external-in` CLI flags. External marking
is REPL-only via `transfer`.

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
sirop stir                  # interactive: fix mismatches, mark external wallets
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
| Fee ≤ 0 | `error: fee must be greater than zero.` |
| Fee ≥ tx amount | `error: fee cannot equal or exceed the transaction amount.` |

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
