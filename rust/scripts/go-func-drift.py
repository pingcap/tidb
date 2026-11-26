#!/usr/bin/env python3
"""Function-level Go master drift: Rust ports of Go functions that changed
after the branch's merge-base, by commits no Rust commit has ported.

    python3 -I rust/scripts/go-func-drift.py <go-master-tree> [--base SHA] [--tsv FILE]

Provenance comes from two records the port already keeps:
  * which Go function a Rust item ports: the `Go `Name`` / `Go `Type.Method``
    citations in its comments (a Rust file "ports" every Go name it cites);
  * which Go master commit a Rust change ported: the Go SHA its commit
    message cites (`git log` over rust/), as every drift port does.
A Go commit is reported when it changed a function some Rust file ports and
no Rust commit message cites it; the register's Held list
(rust/docs/parity/go-master-drift.md) marks the ones proven present.

The Go side reads each commit's hunk headers (`git show`, Go function
context), so a change inside a function names it; a hunk outside any
function names none and is not reported here (audit.py's token check
covers those).
"""

import collections
import re
import subprocess
import sys
from pathlib import Path

RUST = Path(__file__).resolve().parents[1]
REPO = RUST.parent
REGISTER = RUST / "docs/parity/go-master-drift.md"
DEFAULT_BASE = "b94006d3abf"

FUNC_HEADER = re.compile(r"^@@[^@]*@@\s*func\s+(?:\(\s*\w*\s*\*?\s*(\w+)(?:\[[^\]]*\])?\s*\)\s*)?(\w+)")
FUNC_LINE = re.compile(r"^[+-]?func\s+(?:\(\s*\w*\s*\*?\s*(\w+)(?:\[[^\]]*\])?\s*\)\s*)?(\w+)\s*[\[(]")
CITE = re.compile(r"Go(?: master)? `(?:[a-z]\w*\.)?([A-Z]?\w+(?:\.\w+)?)`")


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout


def go_changes(go: Path, base: str) -> list[tuple[str, str, str, set[str]]]:
    """(sha, date, subject, changed function names) per non-merge commit."""
    log = git("log", "--format=%h\t%cd\t%s", "--date=short", "--no-merges", f"{base}..HEAD",
              "--", "pkg/*.go", ":!pkg/**/*_test.go", ":!pkg/parser/*", cwd=go)
    out = []
    for line in log.strip().split("\n"):
        if not line:
            continue
        sha, date, subject = line.split("\t", 2)
        diff = git("show", "-U0", "--format=", sha, "--", "pkg/*.go", ":!pkg/**/*_test.go",
                   ":!pkg/parser/*", ":!**/*.pb.go", ":!**/mock*", cwd=go)
        names: set[str] = set()
        for diff_line in diff.split("\n"):
            m = FUNC_HEADER.match(diff_line) if diff_line.startswith("@@") else FUNC_LINE.match(diff_line)
            if m:
                recv, name = m.group(1), m.group(2)
                names.add(f"{recv}.{name}" if recv else name)
        if names:
            out.append((sha, date, subject, names))
    return out


def rust_citations() -> dict[str, set[str]]:
    """Go name -> the Rust files (relative to rust/) citing it."""
    cites: dict[str, set[str]] = collections.defaultdict(set)
    for path in (RUST / "crates").rglob("*.rs"):
        if "target" in path.parts:
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        rel = str(path.relative_to(RUST))
        # Tests cite the Go tests and functions they exercise; only code ports.
        if "/tests/" in rel or "/testdata/" in rel or re.search(r"(^|/)(tests?_[^/]*|[^/]*_tests?)\.rs$", rel):
            continue
        for line in text.split("\n"):
            stripped = line.lstrip()
            if not stripped.startswith("//"):
                continue
            for name in CITE.findall(stripped):
                cites[name].add(rel)
    return cites


def ported_shas() -> set[str]:
    """Go SHAs (11+ hex) cited by commit messages touching rust/."""
    log = git("log", "--format=%B", "--", "rust", cwd=REPO)
    return {m[:11] for m in re.findall(r"\b[0-9a-f]{11,40}\b", log)}


def held_shas() -> set[str]:
    if not REGISTER.exists():
        return set()
    text = REGISTER.read_text()
    m = re.search(r"## Held\n(.*)", text, re.S)
    return set(re.findall(r"\b[0-9a-f]{11}\b", m.group(1))) if m else set()


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0].startswith("--"):
        raise SystemExit(__doc__)
    go = Path(args[0])
    base = args[args.index("--base") + 1] if "--base" in args else DEFAULT_BASE
    tsv = Path(args[args.index("--tsv") + 1]) if "--tsv" in args else None
    cites = rust_citations()
    done = ported_shas() | held_shas()
    rows = []
    for sha, date, subject, names in go_changes(go, base):
        if sha in done:
            continue
        hits = {}
        for name in sorted(names):
            files = cites.get(name)
            # A bare function name must be specific enough to mean one Go
            # function; a Type.Method citation always is.
            if files and ("." in name or len(name) >= 10):
                hits[name] = sorted(files)
        if hits:
            rows.append((sha, date, subject, hits))
    print(f"Go commits since {base} changing a ported Go function, uncited by any Rust commit: {len(rows)}")
    for sha, date, subject, hits in rows:
        print(f"{sha} {date} {subject[:90]}")
        for name, files in hits.items():
            shown = ", ".join(files[:3]) + (f" (+{len(files) - 3})" if len(files) > 3 else "")
            print(f"    {name}: {shown}")
    if tsv:
        with tsv.open("w") as f:
            f.write("sha\tdate\tsubject\tgo_function\trust_files\n")
            for sha, date, subject, hits in rows:
                for name, files in hits.items():
                    f.write(f"{sha}\t{date}\t{subject}\t{name}\t{' '.join(files)}\n")


if __name__ == "__main__":
    main()
