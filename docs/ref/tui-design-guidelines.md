# TUI Design Guidelines

## Design Philosophy

This tool lives in the terminal. It should feel like it belongs there — fast,
keyboard-driven, information-dense, and satisfying to use. Two TUIs set the
reference standard for what we're building:

**Claude Code** — Clean single-pane flow. Status bar with key metrics at a
glance. Content streams in progressively. Minimal chrome, maximum signal.
The interface disappears and you focus on the work.

**btop** — Dense multi-panel dashboard. Every panel earns its space with
real-time, glanceable data. Box-drawing characters create clean visual
separation. Color is functional, not decorative — it encodes meaning
(green = good, red = attention, dim = secondary).

The principle connecting both: **show what matters, hide what doesn't,
and never make the user wait for the interface.**

---

## Framework: Textual

Use [Textual](https://textual.textualize.io/) (by Textualize) as the TUI
framework. It's the right choice for this project because:

- Pure Python — no JS/Node dependency, stays in our stack
- CSS-based styling with hot-reload in dev mode
- Built-in widget library (DataTable, Tree, Input, Header, Footer, Tabs)
- Async-native — can stream node verification and API calls without blocking
- First-class type hints and mypy compatibility
- Reactive attributes for live-updating data displays
- Built-in theme support (dark/light, custom themes)
- Can also serve as a web app via `textual serve` if we ever want that

### Dependencies

```toml
[project]
dependencies = [
    "textual>=3.0",
]

[project.optional-dependencies]
dev = [
    "textual-dev",    # Dev console, CSS hot-reload
]
```

### Project structure for TUI code

```
src/
├── tui/
│   ├── __init__.py
│   ├── app.py              # Main CryptoTaxApp(App) class
│   ├── screens/            # One screen per major workflow
│   │   ├── __init__.py
│   │   ├── dashboard.py    # Landing screen — summary + status
│   │   ├── import_screen.py    # File selection and import progress
│   │   ├── review.py       # Transaction review and editing
│   │   ├── report.py       # Final tax report view
│   │   └── settings.py     # Config and connection status
│   ├── widgets/            # Reusable custom widgets
│   │   ├── __init__.py
│   │   ├── acb_table.py    # ACB history table with running totals
│   │   ├── gain_loss.py    # Gain/loss display with color coding
│   │   ├── status_panel.py # Connection status indicators
│   │   └── sparkline.py    # Inline sparkline for BTC price context
│   ├── styles/             # Textual CSS files
│   │   ├── base.tcss       # Core layout and typography
│   │   ├── theme_dark.tcss # Dark theme (default)
│   │   └── theme_light.tcss
│   └── commands.py         # Command palette actions
```

---

## Design Principles

### 1. Progressive disclosure

The interface starts simple and reveals complexity only when needed. A first-time
user running the tool to process a straightforward Shakepay CSV should see
exactly what they need — not every edge case the tool can handle.

**Phase 1 (MVP):** Single-screen flow — import → calculate → report. One screen
at a time, footer with keybindings, header with status. Think Claude Code's
linear conversation flow.

**Phase 2:** Multi-panel dashboard — btop-style layout with simultaneous
visibility of transactions, ACB state, and running totals. Add this only after
the core pipeline is stable.

**Phase 3:** Interactive editing — click into transactions to adjust, re-run
calculations live, compare scenarios (e.g., what-if on superficial loss).

Never build Phase 2 features into Phase 1 scaffolding. The screen system in
Textual makes it easy to swap layouts without rewriting logic.

### 2. Keyboard-first, mouse-friendly

Every action must be reachable by keyboard. Mouse support is a convenience,
not a requirement.

Keybinding conventions (consistent across all screens):

| Key | Action |
|-----|--------|
| `q` / `ctrl+c` | Quit |
| `?` / `F1` | Help overlay |
| `tab` / `shift+tab` | Navigate between panels/widgets |
| `j` / `k` or `↑` / `↓` | Navigate within lists/tables |
| `enter` | Select / confirm / drill into detail |
| `esc` | Back / close overlay / cancel |
| `/` | Search / filter current view |
| `r` | Refresh / re-run calculation |
| `e` | Export current view |
| `ctrl+p` | Command palette |

The **command palette** (`ctrl+p`) is the power-user escape hatch. Every action
in the app should be accessible from the command palette, even if it also has
a dedicated keybinding. This follows the Claude Code pattern where the command
palette is the universal entry point.

The **footer** always shows context-sensitive keybinding hints for the current
screen. Only show the 4–6 most relevant bindings — not every possible action.

### 3. Information density done right

**btop's lesson:** Dense is good when every element earns its pixel. Dense is
bad when it's just clutter.

Rules for information density:

- Every visible number must be actionable or contextual. If a number doesn't
  help the user make a decision or verify correctness, hide it behind a
  drill-down.
- Use **alignment** to make columns scannable. Right-align all currency values.
  Left-align descriptions. Fixed-width font is already our friend in the
  terminal — use it.
- Use **color sparingly and semantically** (see Color section below). If
  everything is colorful, nothing stands out.
- Use **dim/muted text** for secondary information (timestamps, txids, metadata)
  and **bright/bold text** for primary information (amounts, gains/losses).
- Group related information with **box-drawing borders** (Textual's Container
  and Static widgets handle this via CSS borders). Avoid nested borders —
  one level of boxing is usually enough.

### 4. Feedback is immediate

The user should never wonder "is it working?"

- **Import progress:** Show a progress bar with file name, rows processed, and
  estimated time. Stream results as they arrive — don't wait for the full
  file to parse before showing anything.
- **Node verification:** Show each txid being verified in real-time, with a
  checkmark/X as each completes. If the node is unreachable, show the error
  within 2 seconds and offer to continue without verification.
- **Calculation:** For small datasets, this is instant. For larger ones,
  show a progress indicator and stream dispositions as they're computed.
- **Errors:** Red text with a clear one-line message. Full traceback available
  via `?` or drill-down, never dumped on the main screen.

---

## Color System

Use a **semantic color palette** — colors encode meaning, not decoration.
Define colors in the TCSS theme file so they can be swapped for light mode
or accessibility needs.

### Semantic color assignments

| Semantic role | Dark theme color | Usage |
|---------------|-----------------|-------|
| `gain` | Green (`#50fa7b`) | Positive capital gains, successful operations |
| `loss` | Red (`#ff5555`) | Capital losses, errors, warnings |
| `neutral` | White/light gray | Default text, zero-change values |
| `muted` | Dim gray (`#6272a4`) | Timestamps, txids, secondary metadata |
| `accent` | Cyan (`#8be9fd`) | Interactive elements, links, selected items |
| `highlight` | Yellow (`#f1fa8c`) | Flagged items (superficial loss, discrepancy) |
| `header` | Bold white on dark | Panel titles, section headers |
| `border` | Medium gray (`#44475a`) | Box borders, separators |

### Color rules

- **Never use color as the only differentiator.** Always pair with a symbol
  or text label: `▲ +$1,234.56` (green) vs `▼ -$567.89` (red).
- **Gains are always green, losses are always red.** No exceptions, no
  conditional theming. This is universal financial convention.
- **Dim everything that isn't the primary focus.** If the user is looking at
  a disposition table, the header and footer should be visually receded.
- **Borders should be subtle.** They separate, not decorate. Use `border:
  solid $border` in TCSS, not bright or double borders.

### Textual CSS example

```css
/* styles/base.tcss */

Screen {
    background: $surface;
}

#main-content {
    height: 1fr;
    padding: 0 1;
}

.gain {
    color: $success;
}

.loss {
    color: $error;
}

.muted {
    color: $text-muted;
}

DataTable > .datatable--cursor {
    background: $accent 20%;
}

Footer {
    background: $surface-darken-1;
}
```

---

## Layout Patterns

### Phase 1: Linear flow (MVP)

Single screen visible at a time. Simple and unambiguous.

```
┌──────────────────────────────────────────────────────────────┐
│  Crypto Tax Tool — Quebec 2025          BTC: $XX,XXX CAD    │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  [Main content area — changes per screen]                    │
│                                                              │
│  Import screen:    File picker → progress bar → summary      │
│  Review screen:    Scrollable transaction table               │
│  Report screen:    Schedule 3 / Schedule G / TP-21.4.39-V    │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  [q] Quit  [?] Help  [tab] Navigate  [enter] Select         │
└──────────────────────────────────────────────────────────────┘
```

### Phase 2: Dashboard (btop-inspired)

Multi-panel simultaneous view. Only build after Phase 1 is stable.

```
┌──────────────────────────────────────────────────────────────┐
│  Crypto Tax Tool — Quebec 2025          Node: ● Connected    │
├────────────────────────────┬─────────────────────────────────┤
│  Transactions              │  Capital Gains Summary          │
│  ─────────────────────     │  ─────────────────────          │
│  2025-03-15 BUY  0.1 BTC  │  Total Gains:    ▲ $4,521.33   │
│  2025-04-02 BUY  0.05 BTC │  Total Losses:   ▼ $312.10     │
│  2025-06-18 SELL 0.08 BTC │  Net:            ▲ $4,209.23   │
│  2025-09-01 BUY  0.2 BTC  │  Taxable (50%):    $2,104.62   │
│  ...                       │                                 │
│                            ├─────────────────────────────────┤
│                            │  ACB Status (BTC)               │
│                            │  ─────────────────────          │
│                            │  Holdings:  0.27 BTC            │
│                            │  Total ACB: $18,432.10          │
│                            │  ACB/unit:  $68,267.04          │
├────────────────────────────┴─────────────────────────────────┤
│  [q] Quit  [/] Filter  [r] Recalculate  [e] Export  [?] Help│
└──────────────────────────────────────────────────────────────┘
```

### Phase 3: Interactive detail view

Drill into any transaction for full context.

```
┌──────────────────────────────────────────────────────────────┐
│  Transaction Detail                              [esc] Back  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Type:       SELL                                            │
│  Date:       2025-06-18 14:32:07 UTC                         │
│  Source:     Shakepay                                        │
│  Amount:     0.08 BTC                                        │
│  Proceeds:   $7,234.56 CAD                                   │
│  ACB used:   $5,461.36 CAD ($68,267.04/BTC)                  │
│  Fees:       $12.40 CAD                                      │
│  Gain:       ▲ $1,760.80 CAD                                 │
│                                                              │
│  ── Node Verification ──────────────────────────────         │
│  Status:     ✓ Verified                                      │
│  Block:      #845,231 (confirmed 2025-06-18 14:38:12 UTC)   │
│  On-chain fee: 0.00004200 BTC ($2.87 CAD)                    │
│  Timestamp Δ: +365s (within tolerance)                       │
│                                                              │
│  ── Superficial Loss Check ─────────────────────             │
│  Status:     Not applicable (gain, not loss)                  │
│                                                              │
├──────────────────────────────────────────────────────────────┤
│  [esc] Back  [e] Edit  [n] Next  [p] Previous               │
└──────────────────────────────────────────────────────────────┘
```

---

## Widget Specifications

### Status bar (header)

Always visible. One line. Provides global context.

```
Crypto Tax Tool — Quebec 2025    │ BTC: $XX,XXX │ Node: ● │ 142 txns │ $0.00 gains
```

Contents (left to right):
- App name and tax year
- Current BTC/CAD rate (if available, fetched once on startup)
- Node connection status: `●` green = connected, `●` red = disconnected, `●` yellow = checking
- Transaction count (after import)
- Running net gains (after calculation)

### Data tables

The primary display widget. Use Textual's `DataTable` with these conventions:

- **Right-align** all numeric/currency columns
- **Fixed column widths** for currency columns to maintain alignment
- **Alternating row shading** (subtle, via TCSS `datatable--even-row`)
- **Sort indicators** in column headers: `▲` ascending, `▼` descending
- **Truncate** long values (txids, addresses) with `...` — full value shown on hover or drill-down
- **Color-code** the gain/loss column (green/red)

### Progress indicators

For multi-step operations (import → normalize → verify → calculate → report):

```
Importing Shakepay CSV...
████████████████░░░░░░░░  156/234 rows  [67%]  ETA: 2s

Verifying transactions...
████████░░░░░░░░░░░░░░░░   23/87 txids  [26%]  ✓ 22  ✗ 0  ⊘ 1
```

Use Textual's `ProgressBar` widget. Show counts, not just percentages.
The `✓ ✗ ⊘` counters give immediate confidence in data quality.

### Command palette

Activated with `ctrl+p`. Use Textual's built-in `CommandPalette` provider.
Register all actions as commands:

```
> import shakepay       Import Shakepay CSV...
> import ndax           Import NDAX CSV...
> import koinly         Import Koinly export...
> calculate             Run ACB calculation
> verify                Run node verification
> export schedule3      Export Schedule 3 data
> export scheduleg      Export Quebec Schedule G
> export tp21           Export TP-21.4.39-V data
> settings              Open settings
> theme dark            Switch to dark theme
> theme light           Switch to light theme
```

---

## Separation from Business Logic

**The TUI is a presentation layer only.** It must never contain:

- Tax calculation logic
- CSV parsing logic
- API calls (BoC rates, node verification)
- File I/O for data files

The TUI calls into the engine and importers through a clean interface.
All long-running operations are async and report progress via callbacks
or reactive attributes.

```python
# Good — TUI calls engine, displays result
async def on_button_pressed(self, event: Button.Pressed) -> None:
    if event.button.id == "calculate":
        results = await self.engine.calculate_gains(self.transactions)
        self.query_one(GainsTable).update(results)

# Bad — TUI contains calculation logic
async def on_button_pressed(self, event: Button.Pressed) -> None:
    if event.button.id == "calculate":
        for tx in self.transactions:
            acb = self.total_acb / self.total_units  # NO
```

The engine should expose a **progress callback protocol** that any UI
(TUI, CLI, tests) can implement:

```python
from typing import Protocol

class ProgressCallback(Protocol):
    def on_step(self, current: int, total: int, message: str) -> None: ...
    def on_warning(self, message: str) -> None: ...
    def on_error(self, message: str, exception: Exception | None = None) -> None: ...
    def on_complete(self, summary: str) -> None: ...
```

---

## CLI Fallback

The TUI is the primary interface, but a non-interactive CLI mode must also
exist for scripting and CI:

```bash
# Full TUI (default)
crypto-tax

# Non-interactive — import, calculate, export
crypto-tax --no-tui --import data/shakepay.csv --export output/

# Pipe-friendly — output to stdout as JSON
crypto-tax --no-tui --import data/ --format json > results.json
```

The `--no-tui` path uses `rich` (already a Textual dependency) for formatted
console output and progress bars. Same engine, same config, different
presentation layer. This is a natural consequence of keeping the TUI
separated from business logic.

---

## Accessibility

- All interactive elements must be reachable by keyboard
- Color is never the sole indicator (always paired with symbols or text)
- Support both dark and light themes
- Respect terminal width — layouts must degrade gracefully at 80 columns
  (full dashboard may require 120+, but Phase 1 linear flow works at 80)
- No animations that can't be disabled. Textual supports
  `TEXTUAL_ANIMATIONS=none` environment variable
- Screen reader compatibility isn't a Textual strength, but meaningful
  widget titles and labels help where possible

---

## Conventions Summary

- **Textual** is the framework. No curses, no blessed, no Ink.
- **TCSS** for all styling. No inline styles in Python code.
- **Screens** for major workflow steps. **Widgets** for reusable components.
- **Keyboard-first.** Every action has a keybinding.
- **Command palette** is the universal fallback for discoverability.
- **Color is semantic.** Green = gain, red = loss, dim = secondary, yellow = flagged.
- **Progressive disclosure.** Build Phase 1 (linear) first. Dashboard later.
- **TUI never contains business logic.** It's a view over the engine.
- **CLI fallback** for non-interactive use. Same engine, `rich` output.
- **80-column minimum.** Degrade gracefully on small terminals.