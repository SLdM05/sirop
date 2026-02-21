# sirop — Language and Naming Guide

## The Theme

sirop is a Quebec maple syrup tax tool. The production process of maple syrup —
tapping trees, collecting sap, boiling it down, grading the output — maps
naturally onto the stages of importing, processing, and reporting crypto
transactions.

Use this metaphor as a **creative direction, not a rigid constraint**. When
naming a new command, status message, error, or UI element, ask: *does the
sugarbush give us a better word than the generic one?* If yes, use it. If the
metaphor doesn't fit naturally, use plain language. Never force it.

The personality lives in the **chrome** — CLI verbs, status messages, progress
text, log output, and TUI decorations. It is completely absent from **tax
output** — report data, form fields, amounts, line numbers, and anything
destined for Revenu Québec or the CRA is plain, precise, and formal.

---

## The Two Registers

### In-app language (personality on)

Everything the user sees while the tool is running: CLI commands, progress
bars, status indicators, log messages, warnings, success confirmations, TUI
headers and footers.

Use the maple theme where it fits. Keep it dry — a single well-placed word
lands better than a paragraph of forced whimsy. The tone is a Quebec sugar
shack operator who takes the work seriously but knows the product is good.

### Tax output (personality off)

Any data that flows into Schedule 3, Schedule G, TP-21.4.39-V, audit logs,
or any file intended for external use.

Zero personality. Standard accounting and tax vocabulary only. If a line on
a form says "Proceeds of disposition," the tool says "Proceeds of disposition."

---

## CLI Verb Vocabulary

These are the canonical top-level commands. Use them consistently across
the CLI, command palette, and any documentation.

| Verb | Replaces | What it does |
|------|----------|--------------|
| `tap` | import | Reads raw transaction files from exchanges and wallets |
| `boil` | calculate / process | Runs the full calculation pipeline |
| `pour` | export / generate | Produces tax report outputs |
| `grade` | summarize / status | Shows a summary of gains, losses, and ACB state |
| `verify` | verify | Node verification (kept as-is — clear enough) |

Sub-commands and flags don't need to follow the theme — use clear, descriptive
names there. `sirop tap --source shakepay --file transactions.csv` is fine.
The theme lives at the top level.

Claude Code may introduce additional top-level verbs as new workflows emerge.
New verbs should follow the same logic: find the sugarbush equivalent if one
exists naturally, otherwise use plain language.

---

## Status and Progress Message Vocabulary

These are **directional examples**, not an exhaustive list. Claude Code should
use these as anchors and extend the vocabulary in the same spirit.

### Pipeline stages

| Stage | Suggested phrasing |
|-------|--------------------|
| Starting import | `Tapping [source]...` |
| Fetching exchange rates | `Checking sap levels...` / `Fetching BoC rates...` (either works) |
| Running node verification | `Verifying on-chain...` or `Cross-checking the block...` |
| Matching transfers | `Tracing the flow...` |
| Running ACB calculation | `Refining...` |
| Detecting superficial losses | `Checking for crystallization...` |
| Generating reports | `Pouring [Schedule 3 / Schedule G / TP-21.4.39-V]...` |

### Outcome messages

| Outcome | Suggested phrasing |
|---------|--------------------|
| Clean run, no issues | `Golden batch.` |
| Completed with warnings | `Good batch — [N] items need your attention.` |
| Superficial loss detected | `Crystallization detected on [date] — loss denied, ACB adjusted.` |
| Transfer matched successfully | `Flow confirmed — [txid short] is a transfer, not a disposition.` |
| Node verification passed | `Block confirmed.` |
| Node unavailable, falling back | `Node unreachable — running without on-chain verification.` |
| Unmatched withdrawal | `Loose tap — withdrawal on [date] has no matching deposit.` |

### Error messages

Errors should be clear and actionable first, flavoured second. Don't let the
metaphor obscure what went wrong.

```
# Good — clear problem, light flavour
Sticky import: Shakepay CSV is missing the 'Blockchain Transaction ID' column.

# Bad — metaphor obscures the actual error
The sap won't flow. Check your CSV.
```

---

## TUI Chrome

The TUI header, footer, and status indicators can carry personality. The
content panels (transaction tables, ACB state, gain/loss numbers) should be
neutral and functional.

**Header examples**

```
sirop — Quebec 2025    │ BTC: $XX,XXX CAD  │ ◉ Node  │ 142 txns  │ ▲ $4,209 net
```

**Footer examples** — keybinding hints are plain; optional flavour in the app name only.

```
[tap] Import  [boil] Calculate  [pour] Export  [q] Quit  [?] Help
```

**Empty state** (no data loaded yet)

```
No transactions tapped yet.
Run `sirop tap` or press [t] to import your first file.
```

---

## Things to Avoid

- **Puns that need explanation.** If you have to think twice, it's not landing.
- **Cutesy error messages.** Errors cause stress. Be clear first.
- **Flavour in numbers.** Never. `$4,209.23` is `$4,209.23`.
- **Mixing registers in the same sentence.** Don't write
  `Tapping Shakepay CSV — Proceeds of disposition: $4,209.23`. Keep the
  flavour in the status line, the formal language in the data.
- **Overloading "tap."** In the Bitcoin world, taproot and tap already have
  technical meanings. Don't use `tap` in contexts where it could be confused
  with a protocol-level concept.

---

## The Name

The tool is `sirop`. Lowercase, always. The package name, CLI binary, and
module name are all `sirop`. In prose and documentation it may be written
as *sirop* (italicised) but never capitalized as Sirop.

---

## How the Two-Register Rule Is Enforced in Code

The `emit()` function in `src/sirop/utils/messages.py` is the implementation
of the two-register rule. Category determines register:

| Category | Register | Example |
|----------|----------|---------|
| `output` | In-app chrome | `"Tapped 42 transaction(s) from export.csv [Shakepay]..."` |
| `fluff` | In-app chrome (personality) | `"Checking sap levels..."` |
| `error` | In-app chrome (always plain) | `"error [E009]: Cannot identify CSV format."` |
| `warning` | In-app chrome (always plain) | `"warning [W001]: ..."` |

Tax output (Schedule 3, TP-21.4.39-V, audit logs) bypasses `emit()` entirely
and is written directly to files by the reports module using standard accounting
vocabulary only.

**Adding a new themed message:** set `category: fluff` in `config/messages.yaml`.
**Adding a new result message:** set `category: output`. Never add personality
to `error` or `warning` — they must be clear and actionable above all else.

---

## Extending This Guide

This document sets the direction. Claude Code is expected to exercise judgment
and invent new language as new features are built. When in doubt, the test is:

1. Does this sound like it belongs in a Quebec sugar shack? ✓
2. Is the actual meaning immediately clear? ✓
3. Is this in a tax output file? → strip it out entirely.
