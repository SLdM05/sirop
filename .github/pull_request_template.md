## What does this PR do?

<!-- One paragraph. What problem does it solve or what capability does it add? -->

## Type of change

- [ ] `feat` — new feature
- [ ] `fix` — bug fix
- [ ] `docs` — documentation only
- [ ] `test` — tests only
- [ ] `refactor` — no behaviour change
- [ ] `chore` — tooling, dependencies, config

## Checklist

- [ ] Target branch is `dev` (not `main`)
- [ ] All four quality checks pass locally:
  - `poetry run ruff check --fix .`
  - `poetry run ruff format .`
  - `poetry run mypy .`
  - `poetry run pytest -m "not slow and not integration" -v`
- [ ] No real transaction data, addresses, txids, or amounts in any file
- [ ] New engine logic has known-answer unit tests
- [ ] Commit messages follow Conventional Commits format (≤ 72 char subject)
- [ ] Related issue linked: Closes #

## For importer PRs only

- [ ] YAML config file added to `config/importers/` (zero column names in Python)
- [ ] Synthetic fixture CSV added to `tests/fixtures/` with obviously fake data
- [ ] `docs/ref/transaction-import-formats.md` consulted

## Notes for reviewer

<!-- Optional: anything needing special attention, known limitations, or follow-up work -->
