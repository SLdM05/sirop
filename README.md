# sirop

Canadian bitcoin tax calculator. Computes capital gains, ACB, and
superficial losses from exchange and wallet exports. Produces Schedule 3, Schedule G,
and the Quebec TP-21.4.39-V form.

---

## Why sirop exists

Most tax tools require you to hand over your wallet addresses or xpubs
so they can pull your transaction history from the blockchain. That means a
third-party service sees your full financial picture — every address you've
ever used, your balances, your counterparties.

sirop takes the opposite approach: **your keys never leave your machine.**

You `tap` CSV files from your exchanges and wallets yourself, feed them to
sirop locally, and the tool `boils` all the math on your computer. No account
required, no data uploaded, no API keys, no address scanning. When you're done,
`pour` produces the tax forms you need to file — nothing more leaves your hands.

Privacy is not a feature that gets toggled on. It is the default, and nothing
in sirop's design trades it away for convenience.

---

## Disclaimer

sirop performs calculations based on publicly available CRA and Revenu Québec guidelines
as understood by its author, who is a hobby software developer, not a tax professional or
accountant. Do not file a tax return based solely on this tool's output without independent
review by a qualified professional. The author accepts no liability for errors, omissions,
penalties, or interest charges arising from its use.

*That said — the math is open source. Check it.*

---

## Currently supported import formats

| Source | Format | Notes |
|--------|--------|-------|
| Shakepay | CSV export | BTC and CAD accounts; fees embedded in spread |
| NDAX | AlphaPoint Ledgers CSV | Grouped by TX\_ID; explicit fee rows |
| Sparrow Wallet | CSV export | BTC or satoshi amounts; unit auto-detected |

---

## Installation

Requires Python 3.12+ and [Poetry](https://python-poetry.org/).

```bash
poetry install
cp .env.example .env   # set DATA_DIR and optional node config
```

---

## Quickstart

```bash
# 1. Create a batch for your tax year
sirop create my2025tax --year 2025

# 2. Import your exchange and wallet exports
#    Pass a directory to tap all CSVs at once (with confirmation prompt):
sirop tap ~/Downloads/exports/
#    Or import files individually:
sirop tap ~/Downloads/ndax_2025_ledger.csv
sirop tap ~/Downloads/shakepay_2025_btc.csv
sirop tap ~/Downloads/sparrow_2025.csv

# 3. Run the tax calculation pipeline
sirop boil

# 4. Sometimes you will need to adjust your transfer pairs (wallet-to-wallet moves)
sirop stir

# 5. Output your tax forms (PLANNED)
sirop pour
```

After `boil`, the `.sirop` batch file contains fully computed dispositions and
superficial-loss-adjusted gains, ready for `pour` (report generation).

---

## Documentation

- [Batch management — create, list, switch](docs/usage/sirop-create.md)
- [tap — import exchange and wallet transactions](docs/usage/sirop-tap.md)
- [stir — review and confirm transfer pairs](docs/usage/sirop-stir.md)
- [boil — run the tax calculation pipeline](docs/usage/sirop-boil.md)

---

## Planned

- **`pour` command** — export Schedule 3, Schedule G, and TP-21.4.39-V to PDF and CSV
- **`grade` command** — batch status and pipeline overview
- **Textual TUI** — interactive transaction browser, ACB state viewer, and batch switcher
- **Bitcoin node verification** — confirm on-chain transaction details via Bitcoin Core RPC
  or Mempool REST API before ACB calculation
- **Additional import formats** — Koinly capital gains CSV (as a validation source),
  more Canadian exchanges

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, branch model, commit format, and the
privacy rule (no real transaction data anywhere in the repo).

---

## License

MIT — see [LICENSE](LICENSE).
