#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
"""Local regressions for the manual RestoreRegion tools; no cluster required."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import capture
from record import Recorder


class ManualToolsTest(unittest.TestCase):
    def test_manual_fixture_is_not_discovered_as_an_integration_case(self):
        tools = Path(capture.__file__).resolve().parent
        discovered = {p.parent.name for p in tools.parent.glob("*/run.sh")}
        self.assertNotIn("br_restore_region", discovered)
        result = subprocess.run(
            ["bash", str(tools / "run-manual.sh"), "--help"],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("prepare", result.stdout)
        self.assertIn("restore", result.stdout)

    def test_capture_accepts_explicit_component_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "evidence"
            with patch.object(capture, "capture") as collect:
                capture.main([
                    str(root), "--repository", "br=/repos/tidb",
                    "--repository", "cse=/repos/cse worktree",
                    "--binary", "restore-br=/build/new-br", "--snapshots",
                ])
            collect.assert_called_once_with(
                root, {"br": "/repos/tidb", "cse": "/repos/cse worktree"},
                {"restore-br": "/build/new-br"}, True,
            )

    def test_recorder_omits_secret_environment_without_changing_child_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = "test-only-secret-never-publish"
            with patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY": secret}):
                _, code = Recorder(directory).run("child", [
                    sys.executable, "-c",
                    "import os; assert os.environ.get('AWS_SECRET_ACCESS_KEY')",
                ])
            self.assertEqual(code, 0)
            metadata = json.loads((Path(directory) / "child/command.json").read_text())
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", metadata["env"])
            for path in Path(directory).rglob("*"):
                if path.is_file():
                    self.assertNotIn(secret, path.read_text())


if __name__ == "__main__":
    unittest.main()
