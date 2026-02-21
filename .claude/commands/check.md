Run the full sirop quality suite in order. Stop and report clearly if any step fails — do not proceed to the next step.

1. **Lint and auto-fix** — `poetry run ruff check --fix .`
2. **Format** — `poetry run ruff format .`
3. **Type check** — `poetry run mypy .`
4. **Tests** — `poetry run pytest -m "not slow and not integration" -v`

After all steps pass, print a one-line summary: `All checks passed.`

If any step fails, print the failure output and stop. Do not run subsequent steps. Do not proceed with a commit.
