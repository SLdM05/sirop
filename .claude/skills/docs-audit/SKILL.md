---
name: docs-audit
description: Detect and resolve drift between `docs/ref/*.md` and the code each doc anchors to. Use when editing or referencing a `docs/ref/` doc, when changing code under a path that one of those docs anchors (`src/sirop/db/`, `src/sirop/utils/messages.py`, `src/sirop/importers/`, `config/importers/`, `src/sirop/cli/`, etc.), when the user asks "is doc X still accurate" or "run the docs audit", or before bumping/committing a doc edit. Also use to migrate a coarse `tracks`-only doc to precise line-range `anchors`.
---

Audit `docs/` for staleness against the code each doc anchors. The skill is
both user-triggerable (`/docs-audit`) and auto-invoked when the description
above matches.

## When to use

Auto-invoke on these triggers:

- About to edit a `docs/ref/*.md` file → run `audit` for that doc first to see
  whether anchors already flag drift; surface findings before editing.
- Just edited code under a path anchored by some doc (most commonly
  `src/sirop/utils/messages.py`, `src/sirop/db/`, `src/sirop/importers/`) →
  run `audit` to see whether the edit broke an anchor hash; if so, the doc
  may need updating in the same PR.
- User asks "is doc X still accurate" / "check for doc drift" / `/docs-audit`.
- Before running `/commit` on a change that touched both code and a doc with
  anchors — confirm anchors match before letting the commit go.

Do NOT auto-invoke on:

- Pure refactors touching files no doc anchors.
- Doc-only edits that don't change anchor line ranges (the bump on commit
  handles those).
- Repeated triggers within the same session if the audit was just run and
  the working tree hasn't changed.

If the question is *"do the docs match the code right now?"* — use this skill.
If the question is *"what does the code currently do?"* — read the code directly.

## How it works

Every code-adjacent doc in `docs/` carries YAML frontmatter. The audit has
two layers of drift detection:

### Layer 1 — `tracks` (coarse, fallback)

A list of file or directory paths. The doc is suspect if any commit since
`verified-at` touched one of those paths.

```yaml
---
verified-at: <short-sha>
tracks:
  - src/sirop/db/schema.py
  - config/messages.yaml
notes: optional free text
---
```

`tracks` is high-recall and low-precision. In a squash-merge repo every
feature PR touches many files, so most docs go suspect after every merge.
Use `tracks` only when there is no obvious narrower anchor.

### Layer 2 — `anchors` (precise, preferred)

A list of `{path, lines, hash}` entries. Each anchor pins a specific line
range; its hash is recomputed on every audit and compared to the stored
value. Edits *outside* the cited range don't trip false positives.

```yaml
---
verified-at: <short-sha>
tracks:                                # optional, coarse fallback
  - src/sirop/utils/messages.py
anchors:                               # precise drift detection
  - path: src/sirop/utils/messages.py
    lines: 42-78                       # the emit() implementation
    hash: a1b2c3d4e5f6                 # set by `bump`; do not hand-edit
  - path: config/messages.yaml
    lines: 1-200
    hash: deadbeef1234
---
```

When `anchors` is present, it is authoritative: the doc is suspect only
when one or more anchor hashes mismatch. The `tracks` list is still surfaced
as advisory commit context to help a reviewer locate the relevant change,
but it does not by itself flip status.

### Status rules

For each doc, the script returns:

- **fresh** — all anchors match (or `tracks`-only doc with no new commits).
- **suspect** — an anchor hash mismatched, or a `tracks`-only doc has new commits since `verified-at`.
- **unverified** — frontmatter missing, or no `tracks` and no `anchors`.
- **missing-sha** — `verified-at` is no longer in history (rebased / squashed away).
- **error** — frontmatter parsed but malformed (e.g. `tracks` not a list).

"Suspect" is not the same as "wrong" — even with anchors, the cited code
may have changed in a way that doesn't invalidate the prose. A human (or
Claude in the active session) still confirms.

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

1. `... show <doc>` to see the commits, anchor mismatches, and (with `--show-diff`) the diff.
2. Read the doc's relevant section.
3. Decide:
   - **Doc still matches code.** Run `... bump <doc>` to set `verified-at` to HEAD and recompute every anchor hash.
   - **Doc has drift.** Edit the doc to match. Then `... bump <doc>` once it does.
   - **Drift is real but you're not the right person to fix it now.** Leave it suspect. Optionally add a `notes:` line in the frontmatter explaining what's known to be stale.
4. Re-run `audit` to confirm the list shrunk.

### Verdict workflow for Claude

If `audit` runs during a Claude Code session and returns suspects, Claude
should reply inline (not shell out to another `claude -p`) with one
verdict per suspect doc, after reading the necessary context:

1. For each suspect, read the doc body and run `git show <sha> -- <tracks>` for the listed commits (or `... show <doc> --show-diff` if quicker).
2. Reply with one of:
   - `ACCURATE: <brief reason>` — doc still matches; the user can `bump`.
   - `BROKEN: <quoted claim>` — specific prose contradicts current code.
   - `UNCERTAIN: <what to check>` — need more context.
3. Do **not** auto-`bump`. Human-gated bump is intentional — mechanical bumping defeats the mechanism.

## Rules

- **Verify before bumping.** `bump` is the only write operation in this skill. It should only run after a human has actually re-read the doc against the code. Mechanical bumping (without verification) defeats the whole point of the metadata.
- **Don't bump from a stale checkout.** Run `git pull` on the integration branch first. Bumping to a HEAD that is behind sets a verified-at that the next person's audit will then mark fresh incorrectly.
- **Treat `missing-sha` as suspect.** When the recorded sha was rewritten away (rebase, force-push), there's no commit to diff against, so the doc has to be re-verified from scratch and bumped.
- **Don't bulk-bump.** There is no `bump --all` flag on purpose. Each doc has different tracks; verification is per-doc.
- The audit is a *narrowing* tool, not an oracle. A "fresh" doc can still have prose drift the audit can't detect (e.g., a code path the `tracks:` list forgot). Re-read full docs periodically anyway.

## Adding frontmatter to a new doc

1. Add a YAML frontmatter block at the top. Prefer `anchors` over `tracks`
   whenever the doc cites specific lines of code:
   ```yaml
   ---
   verified-at: <current HEAD short-sha>
   tracks:
     - <code path 1>                 # optional, coarse fallback
   anchors:
     - path: src/sirop/some/file.py
       lines: 42-78
       # hash: omitted; `bump` will fill it in on first run
   ---
   ```
2. `tracks` may list files or directories (relative to repo root). Be
   precise — over-broad tracks generate false-positive suspect noise.
3. `anchors` must point to exact line ranges. The `bump` command computes
   the hash; do not hand-edit the `hash:` value. If you leave it off, the
   doc shows up as suspect (`no-stored-hash`) until the first `bump`.
4. Run `... show <doc>` to confirm the frontmatter parses and that
   `verified-at` resolves.
5. The crypto-tax reference doc is intentionally uninstrumented (external
   tax law, not code). Other docs that describe non-code subjects can opt
   out the same way.

## Pre-push hook (CI surface)

`.pre-commit-config.yaml` runs `docs_audit.py audit` on `git push` (not on
every commit). It surfaces suspect docs as a warning but does not block
the push — authors fix in a follow-up PR if the bump isn't part of the
same change. Use `--no-verify` to skip if a separate bump PR is intended.

## Related

- `/sirop-query` — for inspecting `.sirop` SQLite files. Different surface (DB rows, not source files), same "give Claude one tool instead of N ad-hoc invocations" idea.
- The schema cheatsheet in `/sirop-query` mirrors `docs/ref/database-schema.md`. If `database-schema.md` goes suspect because of a schema change, the `/sirop-query` SKILL.md likely needs an update too.
