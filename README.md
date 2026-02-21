# sirop

Quebec maple syrup-themed crypto tax tool for Canadian tax filers.

Calculates capital gains, ACB, and superficial losses from exchange and wallet
transaction exports. Produces Schedule 3, Schedule G, and the Quebec
TP-21.4.39-V form.

Import formats for major Canadian exchanges and wallets are planned.

---

## Quickstart

Requires Python 3.12+ and [Poetry](https://python-poetry.org/).

```bash
git clone https://github.com/SLdM05/sirop.git
cd sirop
poetry install
cp .env.example .env   # fill in your values
```

## Quickstart workflow

```bash
# 1. Create a batch for your tax year
sirop create my2025tax --year 2025

# 2. Import your exchange exports
sirop tap ~/Downloads/ndax_2025_ledger.csv

# 3. Run the tax calculation pipeline
sirop boil
```

After `boil`, the `.sirop` file contains fully computed dispositions and
superficial-loss-adjusted gains, ready for `pour` (report generation).

## Documentation

See [`docs/usage/`](docs/usage/) for CLI usage guides:

- [Batch management — create, list, switch](docs/usage/sirop-create.md)
- [tap — import exchange transactions](docs/usage/sirop-tap.md)
- [boil — run the tax calculation pipeline](docs/usage/sirop-boil.md)

See [`docs/ref/`](docs/ref/) for full reference material:

- [Tax rules and ACB formulas](docs/ref/crypto-tax-reference-quebec-2025.md)
- [Database schema](docs/ref/database-schema.md)
- [Data pipeline](docs/ref/data-pipeline.mermaid)
- [TUI design and keyboard bindings](docs/ref/tui-design-guidelines.md)
- [Bitcoin node verification](docs/ref/bitcoin-node-validation-module.md)
- [Language guide and CLI verbs](docs/ref/sirop-language-guide.md)

## License

MIT — see [LICENSE](LICENSE).
