#!/usr/bin/env bash
# PreToolUse hook for Bash: block `git commit` on protected branches (main, dev).
# The hook receives the tool-call JSON on stdin. Exit 2 blocks the call and
# surfaces stderr back to Claude.

set -u
cd "$CLAUDE_PROJECT_DIR" || exit 0

payload="$(cat)"
cmd="$(printf '%s' "$payload" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("tool_input",{}).get("command",""))' 2>/dev/null)"

# Only intercept commits. Non-commit Bash calls pass straight through.
case "$cmd" in
  *"git commit"*) : ;;
  *) exit 0 ;;
esac

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)"
case "$branch" in
  main|dev)
    cat >&2 <<EOF
Blocked: refusing to commit on protected branch '$branch'.
sirop branch policy requires commits on a 'claude/<feature>-<id>' branch.
See docs/ref/branch-model.md.
EOF
    exit 2
    ;;
esac

exit 0
