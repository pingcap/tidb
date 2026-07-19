#!/usr/bin/env python3
"""Create agent slice worktrees inside the writable repository boundary."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKTREE_ROOT = Path("rust/.worktrees")
SLICE_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def run(command: list[str], cwd: Path) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def branch_exists(root: Path, branch: str) -> bool:
    return (
        subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
            cwd=root,
            check=False,
        ).returncode
        == 0
    )


def create_worktree(root: Path, slice_name: str, start_point: str) -> Path:
    if not SLICE_RE.fullmatch(slice_name):
        raise ValueError(f"invalid slice name: {slice_name!r}")
    manifest = root / "rust/workstreams/slices" / f"{slice_name}.toml"
    if not manifest.is_file():
        raise ValueError(f"unknown slice manifest: {manifest}")

    destination = root / WORKTREE_ROOT / slice_name
    if destination.exists():
        raise ValueError(f"slice worktree already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)

    branch = f"codex/{slice_name}"
    if branch_exists(root, branch):
        command = ["git", "worktree", "add", str(destination), branch]
    else:
        command = [
            "git",
            "worktree",
            "add",
            "-b",
            branch,
            str(destination),
            start_point,
        ]
    run(command, root)

    probe = destination / ".codex-write-probe"
    try:
        probe.write_text("writable\n", encoding="utf-8")
    finally:
        probe.unlink(missing_ok=True)
    if not os.access(destination, os.W_OK):
        raise ValueError(f"slice worktree is not writable: {destination}")
    return destination


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--slice", required=True)
    result.add_argument("--start-point", default="HEAD")
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        destination = create_worktree(
            REPOSITORY_ROOT, arguments.slice, arguments.start_point
        )
    except (OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
