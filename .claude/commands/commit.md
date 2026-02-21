Prepare and execute a git commit for the current working state of the sirop project.

## Steps

1. **Run /check first.** If any check fails, stop. Do not commit broken code.

2. **Gather context** — run these in parallel:
   - `git status`
   - `git diff HEAD`
   - `git log --oneline -5`

3. **Stage files** — add specific files by name. Never use `git add -A` or `git add .`
   (those can accidentally include `.env`, data files, or large binaries). Stage only
   the files that are part of this change.

4. **Draft the commit message** following these rules:
   - Subject line: conventional prefix (`feat`, `fix`, `docs`, `test`, `refactor`, `style`, `chore`),
     colon, space, imperative verb, ≤ 72 characters total.
   - Body (optional): wrap at 100 characters. Explain *why*, not *what*.
   - Trailer (required): `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`

5. **Commit** using a HEREDOC to preserve formatting:
   ```bash
   git commit -m "$(cat <<'EOF'
   subject line here

   Optional body here.

   Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
   EOF
   )"
   ```

6. Run `git status` to confirm the commit succeeded.

## Rules

- Never use `--no-verify` or `--amend` unless explicitly asked.
- Never commit `.env`, `data/`, `output/`, or any `*.csv` that might contain real transaction data.
- If a pre-commit hook fails, fix the issue and create a **new** commit — do not amend.
- Do not push unless the user explicitly asks.
