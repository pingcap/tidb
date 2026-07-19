#!/usr/bin/env python3
"""Unit tests for the writable slice-worktree helper."""

from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("slice-worktree.py")
SPEC = importlib.util.spec_from_file_location("slice_worktree", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
SLICE_WORKTREE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SLICE_WORKTREE)


class SliceWorktreeTest(unittest.TestCase):
    def test_create_keeps_worktree_below_repository_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.email", "codex@example.invalid"],
                cwd=root,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Codex Test"],
                cwd=root,
                check=True,
            )
            manifest = root / "rust/workstreams/slices/catalog-runtime.toml"
            manifest.parent.mkdir(parents=True)
            manifest.write_text('slice = "catalog-runtime"\n', encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "fixture"], cwd=root, check=True
            )

            destination = SLICE_WORKTREE.create_worktree(
                root, "catalog-runtime", "HEAD"
            )

            self.assertEqual(
                destination, root / "rust/.worktrees/catalog-runtime"
            )
            self.assertTrue((destination / ".git").is_file())
            self.assertFalse((destination / ".codex-write-probe").exists())

    def test_unknown_or_unsafe_slice_is_rejected_before_git(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "invalid slice name"):
                SLICE_WORKTREE.create_worktree(root, "../outside", "HEAD")
            with self.assertRaisesRegex(ValueError, "unknown slice manifest"):
                SLICE_WORKTREE.create_worktree(root, "missing-slice", "HEAD")


if __name__ == "__main__":
    unittest.main()
