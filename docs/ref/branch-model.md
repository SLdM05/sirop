---
verified-at: 9f411cb
tracks:
  - .github/
notes: Branch policy is enforced by GitHub branch protection on main and dev; this doc captures the local workflow.
---

# Branch model

```
main   ← releases only (tagged v0.1.0, v0.2.0, …)
 └── dev   ← integration branch; squash merges in from feat/*
      └── feat/<name>
            └── claude/<name>-<id>   ← Claude session sub-branches
```

## Starting a session

Always branch from the relevant `feat/<name>` branch (or create one from `dev`), then
create a `claude/<name>-<id>` sub-branch for the session:

```bash
git checkout dev && git pull sirop dev
git checkout -b feat/<name>                 # if the feature branch doesn't exist yet
git checkout -b claude/<name>-<random-id>   # session branch
```

All commits go on the `claude/` branch. When the session is done, open a PR
targeting the parent `feat/<name>` branch. Never open a `claude/` branch
directly from `dev`.

## Squash merge policy

Linear history is enforced. All merges are squash merges. The release tag is
the boundary — it replaces the role a merge commit would otherwise play.

| Merge direction        | GitHub method    | Result on target      |
|------------------------|------------------|-----------------------|
| `claude/*` → `feat/*`  | Squash and merge | 1 commit per session  |
| `feat/*` → `dev`       | Squash and merge | 1 commit per feature  |
| `dev` → `main`         | Squash and merge | 1 commit per release  |

After merging `dev → main`, tag immediately:
`git tag v0.x.0 main && git push sirop v0.x.0`.

The tag is the authoritative release record. Feature history is preserved on
`dev` and in each PR's commit list on GitHub.

## Branch naming

- Feature branches: `feat/<short-noun>` — e.g. `feat/koinly-importer`, `feat/pour-command`
- Session branches: `claude/<feature>-<id>` — e.g. `claude/koinly-importer-x7k2q`

## What NOT to do

- Never commit directly to `main` or `dev` — both are branch-protected
- Never rebase a branch after it has been pushed and shared
- Never open a `claude/` branch directly from `dev`
- Never use the old `feature-` prefix — always use `feat/`
