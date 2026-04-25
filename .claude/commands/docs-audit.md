Audit `docs/` for staleness against the code each doc tracks. Use after touching `src/`, before opening a PR, or when starting work in an unfamiliar area to confirm the reference docs are still accurate.

## When to use

- A reference doc looked authoritative but a `git log` on the related code shows recent changes — confirm whether the doc is still correct.
- You just merged a feature that touched code referenced by one of the reference docs and want to know which docs need a re-read.
- Before quoting `docs/ref/<file>.md` as ground truth in a PR description, technical decision, or another doc.
- During a release / PR-prep pass: enumerate suspect docs and clear the list.

If the question is *"do the docs match the code right now?"* — use this skill. If the question is *"what does the code currently do?"* — read the code directly.

## How it works

Every code-adjacent doc in `docs/` carries YAML frontmatter:

```yaml
---
verified-at: <short-sha>
tracks:
  - src/sirop/db/schema.py
  - config/messages.yaml
notes: optional free text
---
```

The script (`.claude/scripts/docs_audit.py`) does, for each doc:

1. Parse the frontmatter.
2. Run `git log <verified-at>..HEAD -- <tracks>`.
3. If any commits come back, mark the doc **suspect**. If none, **fresh**. If the sha is missing from history (rebased away), **missing-sha**. If frontmatter is absent or incomplete, **unverified**.

"Suspect" is not the same as "wrong" — the script can't tell whether a code change actually invalidated a doc claim. It only narrows the search space. A human (or Claude reading commits) still has to confirm.

## Quick start

```bash
poetry run python .claude/scripts/docs_audit.py --help
poetry run python .claude/scripts/docs_audit.py audit
poetry run python .claude/scripts/docs_audit.py show docs/ref/database-schema.md
```

`audit` exits non-zero (1) when there is anything suspect, missing-sha, or with parse errors — so it can run in CI as a soft check.

## Subcommands

| Subcommand | Purpose |
|---|---|
| `audit` | Scan every doc, group output by status. Use `--max-commits N` to widen/narrow per-doc commit list (default 5). Use `--since <sha>` to override every doc's `verified-at` (useful for "what changed since release v0.3.3"). |
| `show <doc>` | One doc's frontmatter, status, and the commits that make it suspect. `--show-diff` adds the git diff for the first track of the first suspect commit (often enough to see whether the doc is affected). |
| `list` | Inventory: every doc, its `verified-at`, `tracks`, and current status. Useful when adding frontmatter to a new doc — confirms the format parsed correctly. |
| `bump <doc> [--to <sha>] [--dry-run]` | Rewrite the doc's `verified-at` to current HEAD (or `--to <sha>`). Use **only after** confirming the doc actually still matches the code. The line-level rewrite preserves comments, key order, and other YAML formatting. |

## Workflow: clear the suspect list

When `audit` reports suspect docs, work through them one at a time:

1. `... show <doc>` to see the commits and (with `--show-diff`) the diff.
2. Read the doc's relevant section.
3. Decide:
   - **Doc still matches code.** Run `... bump <doc>` to set `verified-at` to HEAD.
   - **Doc has drift.** Edit the doc to match. Then `... bump <doc>` once it does.
   - **Drift is real but you're not the right person to fix it now.** Leave it suspect. Optionally add a `notes:` line in the frontmatter explaining what's known to be stale.
4. Re-run `audit` to confirm the list shrunk.

## Rules

- **Verify before bumping.** `bump` is the only write operation in this skill. It should only run after a human has actually re-read the doc against the code. Mechanical bumping (without verification) defeats the whole point of the metadata.
- **Don't bump from a stale checkout.** Run `git pull` on the integration branch first. Bumping to a HEAD that is behind sets a verified-at that the next person's audit will then mark fresh incorrectly.
- **Treat `missing-sha` as suspect.** When the recorded sha was rewritten away (rebase, force-push), there's no commit to diff against, so the doc has to be re-verified from scratch and bumped.
- **Don't bulk-bump.** There is no `bump --all` flag on purpose. Each doc has different tracks; verification is per-doc.
- The audit is a *narrowing* tool, not an oracle. A "fresh" doc can still have prose drift the audit can't detect (e.g., a code path the `tracks:` list forgot). Re-read full docs periodically anyway.

## Adding frontmatter to a new doc

1. Add a YAML frontmatter block at the top:
   ```yaml
   ---
   verified-at: <current HEAD short-sha>
   tracks:
     - <code path 1>
     - <code path 2>
   ---
   ```
2. `tracks` may list files or directories (relative to repo root). Directories cover the whole subtree. Be precise — over-broad tracks generate false-positive suspect noise.
3. Run `... show <doc>` to confirm the frontmatter parses and `verified-at` resolves.
4. The crypto-tax reference doc is intentionally uninstrumented (external tax law, not code). Other docs that describe non-code subjects can opt out the same way.

## Related

- `/sirop-query` — for inspecting `.sirop` SQLite files. Different surface (DB rows, not source files), same "give Claude one tool instead of N ad-hoc invocations" idea.
- The schema cheatsheet in `/sirop-query` mirrors `docs/ref/database-schema.md`. If `database-schema.md` goes suspect because of a schema change, the `/sirop-query` SKILL.md likely needs an update too.
