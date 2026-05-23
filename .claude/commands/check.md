Run the full sirop quality suite in order. Stop and report clearly if any step fails — do not proceed to the next step.

## Session cache check (do this first)

Before running anything:

1. Run `git diff HEAD` and `git status` to see if there are uncommitted changes.
2. Look back in the current conversation for a passing run of each of the four steps — whether invoked via `/check` or run individually:
   - `ruff check` passed with no errors
   - `ruff format` passed with no changes
   - `mypy` passed with no errors
   - `pytest` passed with no failures
3. For each step where a passing run exists **and** none of the files it covers have changed since (working tree is clean or only unrelated files changed), skip that step and note it as `(cached)`.
4. Only re-run steps that either failed last time, were never run this session, or cover files that have since changed.

If all four steps are cached and passing, print `All checks passed. (cached — no changes since last run)` and stop.

## Full suite

1. **Lint and auto-fix** — `uv run ruff check --fix .`
2. **Format** — `uv run ruff format .`
3. **Type check** — `uv run mypy .`
4. **Tests** — `uv run pytest -m "not slow and not integration" -v`

After all steps pass, print a one-line summary: `All checks passed.`

If any step fails, print the failure output and stop. Do not run subsequent steps. Do not proceed with a commit.
