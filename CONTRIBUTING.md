# Contributing to sirop

## Before you start

**Privacy rule — strictly enforced:** This repo may be public. Never commit real transaction
data of any kind: no BTC amounts, no CAD values, no Bitcoin addresses, no txids, no export
files from any exchange or wallet. Test fixtures use obviously fake data only
(e.g., `txid: "aaa...111"`, address: `bc1qfakeaddressfortesting`).

This is a Canada/Quebec crypto tax tool. Familiarity with the ACB method and Canada's capital gains
rules is helpful for engine and report work. See
[docs/ref/crypto-tax-reference-quebec-2025.md](docs/ref/crypto-tax-reference-quebec-2025.md)
for the authoritative reference.

## Setup

Requires Python 3.12+ and [Poetry](https://python-poetry.org/).

```bash
git clone https://github.com/SLdM05/sirop.git
cd sirop
poetry install
cp .env.example .env   # fill in DATA_DIR and optional node config
poetry run pytest -m "not slow and not integration" -v   # should all pass
```

## Branch model

```
main          ← releases only (tagged v0.1.0, v0.2.0, …)
  sirop-0.x   ← RC branch; PR → main with merge commit
    dev        ← integration branch; target for all feature PRs
      feat/*   ← your feature branch
```

Fork the repo, branch from `dev`, and open your PR targeting `dev`. Direct PRs to `main`
will be closed. The maintainer squash-merges approved PRs into `dev`, so each feature lands
as a single commit.

## Making changes

- **Money values:** `decimal.Decimal` always. `float` in a financial calculation is a bug
  and the PR will be rejected.
- **New importers:** YAML-driven — zero hardcoded column names, date formats, or transaction
  type strings in Python source. See
  [docs/ref/transaction-import-formats.md](docs/ref/transaction-import-formats.md) and
  [config/importers/shakepay.yaml](config/importers/shakepay.yaml) for the pattern.
- **Tests required:** All new engine logic needs known-answer unit tests. All new importers
  need tests against a synthetic fixture CSV.
- **Typed:** Every function needs full annotations. `datetime` must be timezone-aware.
  No bare `Any`.

## Commit format

sirop uses [Conventional Commits](https://www.conventionalcommits.org/). Subject line ≤ 72
characters.

Allowed types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`, `style`, `perf`

```
feat(importers): add Coinbase Advanced CSV importer
fix(engine): correct superficial loss window boundary calculation
docs: update TP-21.4.39-V form field reference
test(importers): add NDAX fee-row edge case fixture
```

No emoji in commit messages.

## Running quality checks

Run all four before opening a PR. The pre-commit hook runs ruff automatically on commit,
but CI runs all four.

```bash
poetry run ruff check --fix .
poetry run ruff format .
poetry run mypy .
poetry run pytest -m "not slow and not integration" -v
```

## Pull request process

1. Target branch is `dev` (never `main` directly).
2. Fill out the PR template fully.
3. Link the issue your PR closes: `Closes #<n>`.
4. The maintainer will squash-merge your branch to a single commit on `dev`. Your branch
   history is preserved on your fork.

## What will be rejected

- Real transaction data anywhere (amounts, dates, addresses, txids)
- `float` for any monetary value
- Hardcoded column names or date formats in importer Python code
- Missing tests for new engine logic or importers
- PRs that mix unrelated changes
- Commits to `main` or direct merges bypassing `dev`

## Adding a new exchange importer

1. Read [docs/ref/transaction-import-formats.md](docs/ref/transaction-import-formats.md) — the
   authoritative schema spec for all supported formats.
2. Add a YAML config file to `config/importers/` modelled on
   [config/importers/shakepay.yaml](config/importers/shakepay.yaml). Zero column names in Python.
3. Implement the importer class in `src/sirop/importers/` extending `BaseImporter`.
4. Add a synthetic fixture CSV to `tests/fixtures/` with obviously fake data.
5. Write tests covering happy path, missing columns, and unit/format edge cases.
6. Wire the importer into the format detector registry.
7. Open a PR describing the exchange format and what the fixture covers.

## Reporting a privacy issue

If you discover a commit that contains real user data (amounts, addresses, txids), open a
**private security advisory** via the GitHub Security tab — do not open a public issue.

## License

MIT. By submitting a pull request you agree your contribution will be licensed under MIT.
