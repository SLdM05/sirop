"""
Doc staleness auditor for sirop.

Reads YAML frontmatter from every `docs/**/*.md` and `docs/**/*.mermaid`,
then asks git whether any of the tracked code paths have changed since
the doc's `verified-at` commit. Docs whose tracked files have new commits
are "suspect" — a human still has to decide whether the doc's claims were
actually affected.

Frontmatter format the script expects:

    ---
    verified-at: <short-sha>
    tracks:
      - src/sirop/some/path
      - config/some.yaml
    notes: optional free-text
    ---

Subcommands:
    audit         scan all docs, report suspect ones with their commits
    show <doc>    details for one doc: commits and files touched
    list          inventory: every doc, its verified-at, tracks, status
    bump <doc>    rewrite verified-at to HEAD (or --to <sha>)

Run `poetry run python .claude/scripts/docs_audit.py --help` for usage.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
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
class DocMeta:
    path: Path
    verified_at: str | None
    tracks: list[str]
    notes: str
    parse_error: str | None = None


@dataclass
class AuditResult:
    doc: DocMeta
    status: str  # "fresh" | "suspect" | "unverified" | "error" | "missing-sha"
    commits: list[str]  # commit lines (short-sha + subject)
    missing_tracks: list[str]


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
            notes="",
            parse_error="tracks must be a list",
        )
    tracks = [str(t) for t in raw_tracks]

    notes_val = data.get("notes", "")
    notes = str(notes_val) if notes_val is not None else ""

    return DocMeta(path=path, verified_at=verified_at, tracks=tracks, notes=notes)


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


def audit_one(doc: DocMeta, repo_root: Path) -> AuditResult:
    if doc.parse_error == "no frontmatter":
        # Deliberate opt-out (e.g. external-tax-law reference). Not an error.
        return AuditResult(doc=doc, status="unverified", commits=[], missing_tracks=[])
    if doc.parse_error:
        return AuditResult(doc=doc, status="error", commits=[], missing_tracks=[])
    if not doc.verified_at:
        return AuditResult(doc=doc, status="unverified", commits=[], missing_tracks=[])
    if not doc.tracks:
        return AuditResult(doc=doc, status="unverified", commits=[], missing_tracks=[])
    if not sha_exists(doc.verified_at, repo_root):
        return AuditResult(doc=doc, status="missing-sha", commits=[], missing_tracks=[])

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


def cmd_show(args: argparse.Namespace) -> int:
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
    table.add_row("tracks", "\n".join(meta.tracks) if meta.tracks else "[red]<none>[/red]")
    if meta.notes:
        table.add_row("notes", meta.notes)
    console.print(table)

    result = audit_one(meta, repo_root)
    console.rule(f"status: [{_STATUS_STYLE.get(result.status, 'white')}]{result.status}")
    if result.status == "suspect":
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
    new_text = f"---\n{new_fm}\n---\n" + text[match.end() :]
    if args.dry_run:
        console.print(f"[dim]would bump[/dim] {path} [dim]to[/dim] {target_sha}")
        return 0

    path.write_text(new_text, encoding="utf-8")
    console.print(f"[green]bumped[/green] {path.relative_to(repo_root)} [dim]→[/dim] {target_sha}")
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
