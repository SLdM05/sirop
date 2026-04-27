Run the full sirop quality suite in order. Stop and report clearly if any step fails — do not proceed to the next step.

1. **Lint and auto-fix** — `poetry run ruff check --fix .`
2. **Format** — `poetry run ruff format .`
3. **Type check** — `poetry run mypy .`
4. **Tests** — `poetry run pytest -m "not slow and not integration" -v`
5. **Docs drift** — `poetry run python .claude/scripts/docs_audit.py audit --max-commits 3`

After all steps pass, print a one-line summary: `All checks passed.`

If any step fails, print the failure output and stop. Do not run subsequent steps. Do not proceed with a commit.

### Step 5 failure handling

If step 5 reports `suspect` docs, the tracked code has changed since the doc was last verified. Do not paper over with `bump` blindly. For each suspect doc:

1. Run `/docs-audit show <doc> --show-diff` to see exactly what changed.
2. Re-read the doc and the affected code paths.
3. Either edit the doc to match reality, **or** confirm the doc is still accurate.
4. Only then run `/docs-audit bump <doc>` to set `verified-at` to HEAD.

`missing-sha` (verified-at sha not in git history) and `error` (parse error in frontmatter) also fail the check. `unverified` (no frontmatter) does not — those docs have opted out and exit 0.
