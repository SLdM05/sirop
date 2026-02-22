# sirop

Canadian crypto tax calculator. Computes capital gains, ACB, and
superficial losses from exchange and wallet exports. Produces Schedule 3, Schedule G,
and the Quebec TP-21.4.39-V form.

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
sirop tap ~/Downloads/ndax_2025_ledger.csv
sirop tap ~/Downloads/shakepay_2025_btc.csv
sirop tap ~/Downloads/sparrow_2025.csv

# 3. Review transfer pairs (wallet-to-wallet moves)
sirop stir

# 4. Run the tax calculation pipeline
sirop boil
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
