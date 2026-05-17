"""
Doc staleness auditor for sirop.

Two layers of drift detection:

1. **tracks** (coarse) — list of file/dir paths. A doc is suspect if any
   commit since `verified-at` touched those paths. Cheap, but a squash-merge
   commit that touches many files makes almost every doc suspect.

2. **anchors** (precise) — list of `{path, lines, hash}` entries. Each
   anchor pins a specific line range; its hash is recomputed at audit time
   and compared to the stored value. Edits *outside* the cited range don't
   trip false positives. Anchors override `tracks` when both are present.

Frontmatter format the script expects:

    ---
    verified-at: <short-sha>
    tracks:                              # optional, coarse fallback
      - src/sirop/some/path
      - config/some.yaml
    anchors:                             # optional, precise drift detection
      - path: src/sirop/utils/messages.py
        lines: 42-78
        hash: a1b2c3d4e5f6
    notes: optional free-text
    ---

Subcommands:
    audit              scan all docs, report suspect ones with their commits
    show <doc>         details for one doc: commits and files touched
    list               inventory: every doc, its verified-at, tracks, status
    bump <doc>         rewrite verified-at to HEAD (or --to <sha>); also
                       recomputes every anchor hash

When suspects appear in a Claude Code session, Claude reads each suspect doc
plus `git show <commit>` for the listed commits and replies with a verdict
inline. There is no separate `review` subcommand — shelling to a fresh
`claude -p` would discard the active session's context and duplicate work.

Run `poetry run python .claude/scripts/docs_audit.py --help` for usage.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from rich.console import Console
from rich.table import Table

DOC_DIRS = ("docs",)
DOC_GLOBS = ("*.md", "*.mermaid")

console = Console()
err_console = Console(stderr=True, style="bold red")


# ── data model ───────────────────────────────────────────────────────────────


@dataclass
class Anchor:
    path: str
    line_start: int
    line_end: int
    stored_hash: str | None  # None if anchor was added but not yet hashed


@dataclass
class DocMeta:
    path: Path
    verified_at: str | None
    tracks: list[str]
    anchors: list[Anchor] = field(default_factory=list)
    notes: str = ""
    parse_error: str | None = None


@dataclass
class AnchorMismatch:
    anchor: Anchor
    reason: str  # "hash-mismatch" | "missing-file" | "out-of-range" | "no-stored-hash"


@dataclass
class AuditResult:
    doc: DocMeta
    status: str  # "fresh" | "suspect" | "unverified" | "error" | "missing-sha"
    commits: list[str]  # commit lines (short-sha + subject)
    missing_tracks: list[str]
    anchor_mismatches: list[AnchorMismatch] = field(default_factory=list)


# ── frontmatter parsing ──────────────────────────────────────────────────────


_FM_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def parse_frontmatter(path: Path) -> DocMeta:
    text = path.read_text(encoding="utf-8")
    match = _FM_RE.match(text)
    if not match:
        return DocMeta(
            path=path, verified_at=None, tracks=[], notes="", parse_error="no frontmatter"
        )

    try:
        data = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError as exc:
        return DocMeta(
            path=path, verified_at=None, tracks=[], notes="", parse_error=f"yaml error: {exc}"
        )

    if not isinstance(data, dict):
        return DocMeta(
            path=path,
            verified_at=None,
            tracks=[],
            notes="",
            parse_error="frontmatter is not a mapping",
        )

    verified_at = data.get("verified-at")
    if verified_at is not None and not isinstance(verified_at, str):
        verified_at = str(verified_at)

    raw_tracks = data.get("tracks", [])
    if not isinstance(raw_tracks, list):
        return DocMeta(
            path=path,
            verified_at=verified_at,
            tracks=[],
            parse_error="tracks must be a list",
        )
    tracks = [str(t) for t in raw_tracks]

    anchors, anchor_err = _parse_anchors(data.get("anchors", []))
    if anchor_err:
        return DocMeta(
            path=path,
            verified_at=verified_at,
            tracks=tracks,
            parse_error=anchor_err,
        )

    notes_val = data.get("notes", "")
    notes = str(notes_val) if notes_val is not None else ""

    return DocMeta(
        path=path,
        verified_at=verified_at,
        tracks=tracks,
        anchors=anchors,
        notes=notes,
    )


_LINE_RANGE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")


def _parse_anchors(raw: object) -> tuple[list[Anchor], str | None]:  # noqa: PLR0911
    if raw is None or raw == []:
        return [], None
    if not isinstance(raw, list):
        return [], "anchors must be a list"
    out: list[Anchor] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            return [], f"anchors[{i}] must be a mapping"
        apath = entry.get("path")
        lines = entry.get("lines")
        stored = entry.get("hash")
        if not isinstance(apath, str) or not apath:
            return [], f"anchors[{i}].path must be a non-empty string"
        if not isinstance(lines, str):
            return [], f"anchors[{i}].lines must be a string like '42-78'"
        m = _LINE_RANGE_RE.match(lines)
        if not m:
            return [], f"anchors[{i}].lines must match 'START-END' (got {lines!r})"
        start, end = int(m.group(1)), int(m.group(2))
        if start < 1 or end < start:
            return [], f"anchors[{i}].lines must be 1-based with end >= start"
        if stored is not None and not isinstance(stored, str):
            stored = str(stored)
        out.append(Anchor(path=apath, line_start=start, line_end=end, stored_hash=stored))
    return out, None


def hash_anchor(repo_root: Path, anchor: Anchor) -> tuple[str | None, str | None]:
    """Return (current_hash, error_reason)."""
    file_path = repo_root / anchor.path
    if not file_path.exists():
        return None, "missing-file"
    try:
        lines = file_path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return None, "missing-file"
    if anchor.line_start > len(lines):
        return None, "out-of-range"
    end = min(anchor.line_end, len(lines))
    block = "\n".join(lines[anchor.line_start - 1 : end])
    digest = hashlib.sha1(block.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
    return digest, None


def iter_docs(repo_root: Path) -> list[Path]:
    paths: list[Path] = []
    for doc_dir in DOC_DIRS:
        base = repo_root / doc_dir
        if not base.exists():
            continue
        for pattern in DOC_GLOBS:
            paths.extend(sorted(base.rglob(pattern)))
    return paths


# ── git helpers ──────────────────────────────────────────────────────────────


def run_git(args: list[str], repo_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["git", *args],  # noqa: S607
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )


def sha_exists(sha: str, repo_root: Path) -> bool:
    result = run_git(["rev-parse", "--verify", f"{sha}^{{commit}}"], repo_root)
    return result.returncode == 0


def head_sha(repo_root: Path, *, short: bool = True) -> str:
    fmt = ["--short"] if short else []
    result = run_git(["rev-parse", *fmt, "HEAD"], repo_root)
    if result.returncode != 0:
        sys.exit(f"error: git rev-parse HEAD failed: {result.stderr.strip()}")
    return result.stdout.strip()


def commits_since(
    since_sha: str, tracks: list[str], repo_root: Path
) -> tuple[list[str], list[str]]:
    """Return (commit_lines, missing_track_paths)."""
    missing = [t for t in tracks if not (repo_root / t).exists()]
    existing = [t for t in tracks if (repo_root / t).exists()]
    if not existing:
        return [], missing

    args = ["log", "--oneline", f"{since_sha}..HEAD", "--", *existing]
    result = run_git(args, repo_root)
    if result.returncode != 0:
        return [f"(git log failed: {result.stderr.strip()})"], missing
    commits = [line for line in result.stdout.splitlines() if line.strip()]
    return commits, missing


# ── audit ────────────────────────────────────────────────────────────────────


def audit_one(doc: DocMeta, repo_root: Path) -> AuditResult:  # noqa: PLR0911
    if doc.parse_error == "no frontmatter":
        # Deliberate opt-out (e.g. external-tax-law reference). Not an error.
        return AuditResult(doc=doc, status="unverified", commits=[], missing_tracks=[])
    if doc.parse_error:
        return AuditResult(doc=doc, status="error", commits=[], missing_tracks=[])
    if not doc.verified_at:
        return AuditResult(doc=doc, status="unverified", commits=[], missing_tracks=[])
    if not doc.tracks and not doc.anchors:
        return AuditResult(doc=doc, status="unverified", commits=[], missing_tracks=[])
    if not sha_exists(doc.verified_at, repo_root):
        return AuditResult(doc=doc, status="missing-sha", commits=[], missing_tracks=[])

    # Anchors, when present, are authoritative. tracks degrades to a backup
    # heuristic for docs that haven't migrated yet.
    if doc.anchors:
        mismatches: list[AnchorMismatch] = []
        for anchor in doc.anchors:
            current, err = hash_anchor(repo_root, anchor)
            if err:
                mismatches.append(AnchorMismatch(anchor=anchor, reason=err))
            elif anchor.stored_hash is None:
                mismatches.append(AnchorMismatch(anchor=anchor, reason="no-stored-hash"))
            elif current != anchor.stored_hash:
                mismatches.append(AnchorMismatch(anchor=anchor, reason="hash-mismatch"))
        if mismatches:
            # Surface the underlying commits to help triage, but they're advisory.
            commits, missing = commits_since(doc.verified_at, doc.tracks, repo_root)
            return AuditResult(
                doc=doc,
                status="suspect",
                commits=commits,
                missing_tracks=missing,
                anchor_mismatches=mismatches,
            )
        return AuditResult(doc=doc, status="fresh", commits=[], missing_tracks=[])

    commits, missing = commits_since(doc.verified_at, doc.tracks, repo_root)
    status = "suspect" if commits else "fresh"
    return AuditResult(doc=doc, status=status, commits=commits, missing_tracks=missing)


_STATUS_STYLE = {
    "fresh": "green",
    "suspect": "yellow",
    "unverified": "dim",
    "missing-sha": "red",
    "error": "red",
}


def _print_suspect_section(results: list[AuditResult], repo_root: Path, max_commits: int) -> None:
    if not results:
        return
    console.rule("[bold yellow]suspect (tracked code changed since verified-at)[/bold yellow]")
    for r in results:
        rel = r.doc.path.relative_to(repo_root)
        console.print(f"[yellow]●[/yellow] {rel}  [dim](verified-at {r.doc.verified_at})[/dim]")
        for m in r.anchor_mismatches:
            label = {
                "hash-mismatch": "anchor drift",
                "missing-file": "anchor file missing",
                "out-of-range": "anchor range past EOF",
                "no-stored-hash": "anchor never bumped",
            }.get(m.reason, m.reason)
            console.print(
                f"    [yellow]→[/yellow] {label}: "
                f"{m.anchor.path}:{m.anchor.line_start}-{m.anchor.line_end}"
            )
        for commit in r.commits[:max_commits]:
            console.print(f"    {commit}")
        if len(r.commits) > max_commits:
            console.print(f"    [dim]… and {len(r.commits) - max_commits} more commits[/dim]")
        if r.missing_tracks:
            console.print(f"    [red]missing tracks:[/red] {', '.join(r.missing_tracks)}")


def _print_missing_sha_section(results: list[AuditResult], repo_root: Path) -> None:
    if not results:
        return
    console.rule("[bold red]verified-at sha not found in git history[/bold red]")
    for r in results:
        rel = r.doc.path.relative_to(repo_root)
        console.print(f"[red]●[/red] {rel}  [dim](verified-at {r.doc.verified_at})[/dim]")


def _print_unverified_section(results: list[AuditResult], repo_root: Path) -> None:
    if not results:
        return
    console.rule("[bold]unverified (no frontmatter or no tracks)[/bold]")
    for r in results:
        rel = r.doc.path.relative_to(repo_root)
        reason = r.doc.parse_error or ("no tracks" if not r.doc.tracks else "no verified-at")
        console.print(f"[dim]●[/dim] {rel}  [dim]({reason})[/dim]")


def _print_error_section(results: list[AuditResult], repo_root: Path) -> None:
    if not results:
        return
    console.rule("[bold red]parse errors[/bold red]")
    for r in results:
        rel = r.doc.path.relative_to(repo_root)
        console.print(f"[red]●[/red] {rel}  {r.doc.parse_error}")


def cmd_audit(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    since_override = args.since

    results: list[AuditResult] = []
    for path in iter_docs(repo_root):
        meta = parse_frontmatter(path)
        if since_override:
            meta = DocMeta(
                path=meta.path,
                verified_at=since_override,
                tracks=meta.tracks,
                notes=meta.notes,
                parse_error=meta.parse_error,
            )
        results.append(audit_one(meta, repo_root))

    by_status = {
        "suspect": [r for r in results if r.status == "suspect"],
        "unverified": [r for r in results if r.status == "unverified"],
        "missing-sha": [r for r in results if r.status == "missing-sha"],
        "error": [r for r in results if r.status == "error"],
    }

    head = head_sha(repo_root)
    console.print(f"[dim]repo: {repo_root}  |  HEAD: {head}  |  docs scanned: {len(results)}[/dim]")
    _print_suspect_section(by_status["suspect"], repo_root, args.max_commits)
    _print_missing_sha_section(by_status["missing-sha"], repo_root)
    _print_unverified_section(by_status["unverified"], repo_root)
    _print_error_section(by_status["error"], repo_root)

    console.rule()
    fresh_n = sum(1 for r in results if r.status == "fresh")
    console.print(
        f"[green]fresh:[/green] {fresh_n}   "
        f"[yellow]suspect:[/yellow] {len(by_status['suspect'])}   "
        f"[dim]unverified:[/dim] {len(by_status['unverified'])}   "
        f"[red]errors:[/red] {len(by_status['missing-sha']) + len(by_status['error'])}"
    )
    return 1 if by_status["suspect"] or by_status["missing-sha"] or by_status["error"] else 0


# ── show ─────────────────────────────────────────────────────────────────────


def cmd_show(args: argparse.Namespace) -> int:  # noqa: PLR0912
    repo_root = Path(args.repo).resolve()
    path = Path(args.doc)
    if not path.is_absolute():
        path = (repo_root / path).resolve()
    if not path.exists():
        sys.exit(f"error: doc not found: {path}")

    meta = parse_frontmatter(path)
    rel = path.relative_to(repo_root) if path.is_relative_to(repo_root) else path
    console.rule(f"[bold]{rel}[/bold]")
    if meta.parse_error:
        console.print(f"[red]parse error:[/red] {meta.parse_error}")
        return 1

    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column(style="dim", no_wrap=True)
    table.add_column()
    table.add_row("verified-at", meta.verified_at or "[red]<missing>[/red]")
    table.add_row("tracks", "\n".join(meta.tracks) if meta.tracks else "[dim]—[/dim]")
    if meta.anchors:
        anchor_rows = "\n".join(
            f"{a.path}:{a.line_start}-{a.line_end}  ({a.stored_hash or 'no-hash'})"
            for a in meta.anchors
        )
        table.add_row("anchors", anchor_rows)
    if meta.notes:
        table.add_row("notes", meta.notes)
    console.print(table)

    result = audit_one(meta, repo_root)
    console.rule(f"status: [{_STATUS_STYLE.get(result.status, 'white')}]{result.status}")
    if result.status == "suspect":
        for m in result.anchor_mismatches:
            loc = f"{m.anchor.path}:{m.anchor.line_start}-{m.anchor.line_end}"
            console.print(f"  [yellow]anchor[/yellow] {loc} → {m.reason}")
        console.print(f"commits since {meta.verified_at} touching tracked files:")
        for commit in result.commits:
            console.print(f"  {commit}")
        if result.missing_tracks:
            console.print(f"[red]missing tracks on disk:[/red] {', '.join(result.missing_tracks)}")
        if args.show_diff:
            sha = result.commits[0].split()[0]
            console.rule(f"files changed in {sha} (intersected with tracks)")
            files = run_git(
                ["show", "--name-only", "--pretty=format:", sha, "--", *meta.tracks],
                repo_root,
            )
            console.print(files.stdout.strip() or "[dim](none — commit touched other tracks)[/dim]")
            console.rule(f"diff in {sha} for tracked paths")
            diff = run_git(["show", "--pretty=format:", sha, "--", *meta.tracks], repo_root)
            console.print(diff.stdout or "[dim](no diff for tracked paths)[/dim]")
    elif result.status == "fresh":
        console.print(f"no tracked-file changes since {meta.verified_at}")
    elif result.status == "missing-sha":
        console.print(
            f"[red]{meta.verified_at} is not a valid commit in this repo.[/red] "
            "Run `bump` to reset."
        )
    elif result.status == "unverified":
        console.print(
            "[dim]frontmatter incomplete — add verified-at + tracks to enable audits.[/dim]"
        )
    return 0 if result.status in ("fresh", "unverified") else 1


# ── list ─────────────────────────────────────────────────────────────────────


def cmd_list(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    table = Table(header_style="bold cyan", show_lines=False)
    table.add_column("doc")
    table.add_column("verified-at", no_wrap=True)
    table.add_column("tracks")
    table.add_column("status", no_wrap=True)
    for path in iter_docs(repo_root):
        meta = parse_frontmatter(path)
        result = audit_one(meta, repo_root)
        style = _STATUS_STYLE.get(result.status, "white")
        rel = path.relative_to(repo_root)
        tracks_display = "\n".join(meta.tracks) if meta.tracks else "[dim]—[/dim]"
        table.add_row(
            str(rel),
            meta.verified_at or "[dim]—[/dim]",
            tracks_display,
            f"[{style}]{result.status}[/{style}]",
        )
    console.print(table)
    return 0


# ── bump ─────────────────────────────────────────────────────────────────────


_VERIFIED_AT_LINE_RE = re.compile(r"^(verified-at\s*:\s*)(\S+)\s*$", re.MULTILINE)


_ANCHOR_HASH_LINE_RE = re.compile(
    r"^(?P<indent>[ \t]+)hash\s*:\s*\S+\s*$",
    re.MULTILINE,
)


def _rehash_anchors_in_fm(fm: str, doc: DocMeta, repo_root: Path) -> tuple[str, list[str]]:
    """Rewrite each anchor's `hash:` line in the frontmatter, in order.

    Returns (new_fm, errors). Anchors are matched positionally — the Nth
    `hash:` line in the anchors block is overwritten with the Nth anchor's
    freshly computed hash. Anchors without an existing `hash:` line need
    one inserted; we handle that by injecting after the `lines:` line of
    the same anchor when no hash: line is present.
    """
    if not doc.anchors:
        return fm, []

    errors: list[str] = []
    new_hashes: list[str] = []
    for anchor in doc.anchors:
        h, err = hash_anchor(repo_root, anchor)
        if err or h is None:
            errors.append(f"anchor {anchor.path}:{anchor.line_start}-{anchor.line_end}: {err}")
            new_hashes.append(anchor.stored_hash or "MISSING")
        else:
            new_hashes.append(h)

    # Walk hash: lines positionally; for anchors without a hash: line yet,
    # we insert one after the matching lines: line.
    hash_iter = iter(new_hashes)
    anchor_iter = iter(doc.anchors)

    def replace(match: re.Match[str]) -> str:
        indent = match.group("indent")
        try:
            new_hash = next(hash_iter)
            next(anchor_iter, None)
        except StopIteration:
            return match.group(0)
        return f"{indent}hash: {new_hash}"

    new_fm, replaced = _ANCHOR_HASH_LINE_RE.subn(replace, fm)

    # Any anchors past the last existing hash: line need their hash inserted.
    remaining_hashes = list(hash_iter)
    if remaining_hashes:
        # Insert each missing hash after the corresponding `lines:` line.
        # We match `- path: <p>` blocks in order and insert after the lines: line
        # in each block that has no hash: line yet.
        lines_seq = new_fm.split("\n")
        out: list[str] = []
        anchor_idx = replaced  # number already handled
        i = 0
        while i < len(lines_seq):
            out.append(lines_seq[i])
            stripped = lines_seq[i].lstrip()
            if (
                stripped.startswith("lines:")
                and anchor_idx < len(doc.anchors)
                and anchor_idx >= replaced
            ):
                next_i = i + 1
                next_stripped = lines_seq[next_i].lstrip() if next_i < len(lines_seq) else ""
                if not next_stripped.startswith("hash:"):
                    indent = lines_seq[i][: len(lines_seq[i]) - len(stripped)]
                    out.append(f"{indent}hash: {remaining_hashes[anchor_idx - replaced]}")
                anchor_idx += 1
            i += 1
        new_fm = "\n".join(out)

    return new_fm, errors


def cmd_bump(args: argparse.Namespace) -> int:
    repo_root = Path(args.repo).resolve()
    path = Path(args.doc)
    if not path.is_absolute():
        path = (repo_root / path).resolve()
    if not path.exists():
        sys.exit(f"error: doc not found: {path}")

    target_sha = args.to or head_sha(repo_root)
    if not sha_exists(target_sha, repo_root):
        sys.exit(f"error: target sha {target_sha!r} is not a valid commit")

    text = path.read_text(encoding="utf-8")
    match = _FM_RE.match(text)
    if not match:
        sys.exit(f"error: {path} has no frontmatter to bump")

    fm = match.group(1)
    if not _VERIFIED_AT_LINE_RE.search(fm):
        sys.exit(f"error: {path} frontmatter has no verified-at line to replace")

    new_fm = _VERIFIED_AT_LINE_RE.sub(rf"\g<1>{target_sha}", fm, count=1)

    doc = parse_frontmatter(path)
    new_fm, anchor_errors = _rehash_anchors_in_fm(new_fm, doc, repo_root)
    if anchor_errors:
        for e in anchor_errors:
            err_console.print(f"warn: {e}")

    new_text = f"---\n{new_fm}\n---\n" + text[match.end() :]
    if args.dry_run:
        console.print(f"[dim]would bump[/dim] {path} [dim]to[/dim] {target_sha}")
        if doc.anchors:
            console.print(f"[dim]would rehash[/dim] {len(doc.anchors)} anchor(s)")
        return 0

    path.write_text(new_text, encoding="utf-8")
    rel = path.relative_to(repo_root)
    summary = f"[green]bumped[/green] {rel} [dim]→[/dim] {target_sha}"
    if doc.anchors:
        summary += (
            f" [dim](+ {len(doc.anchors)} anchor hash{'es' if len(doc.anchors) != 1 else ''})[/dim]"
        )
    console.print(summary)
    return 0


# ── argparse wiring ──────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="docs_audit",
        description="Staleness auditor for sirop docs (YAML frontmatter + git log).",
    )
    parser.add_argument("--repo", default=".", help="repo root (default: cwd)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("audit", help="scan all docs, report suspect ones")
    p.add_argument(
        "--since",
        help="override verified-at for every doc (useful for 'what changed since release X')",
    )
    p.add_argument("--max-commits", type=int, default=5, help="max commits to show per suspect doc")
    p.set_defaults(func=cmd_audit)

    p = sub.add_parser("show", help="details for a single doc (commits, optional diff)")
    p.add_argument("doc", help="path to the doc, relative to repo root or absolute")
    p.add_argument("--show-diff", action="store_true", help="show diff for first suspect commit")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("list", help="inventory of every doc with its frontmatter status")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("bump", help="rewrite verified-at to HEAD (or --to <sha>)")
    p.add_argument("doc", help="path to the doc")
    p.add_argument("--to", help="explicit target sha (default: current HEAD short-sha)")
    p.add_argument("--dry-run", action="store_true", help="print what would change, don't write")
    p.set_defaults(func=cmd_bump)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rc: int = args.func(args)
    return rc


if __name__ == "__main__":
    sys.exit(main())
