from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "semantic-package-gate.py"
SPEC = importlib.util.spec_from_file_location("semantic_package_gate", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def run_git(root: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=root, check=True, stdout=subprocess.PIPE)


class SpecificationTests(unittest.TestCase):
    def write_spec(self, root: Path, body: str) -> Path:
        path = root / "gate.toml"
        path.write_text(body, encoding="utf-8")
        return path

    def base(self, claim: str, accepted: str = "") -> str:
        return f'''schema = "semantic-package-gate-v3"
claim = "{claim}"
go_package = "pkg/sample"
source_commit = "HEAD"
{accepted}rust_files = ["rust.rs"]
tests = [{{ name = "semantic", cwd = "work", argv = ["{sys.executable}", "-c", "pass"] }}]
'''

    def test_seed_requires_an_explicit_file_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rust.rs").write_text("", encoding="utf-8")
            spec = self.write_spec(root, self.base("package-seed"))
            with self.assertRaisesRegex(GATE.GateError, "requires accepted_go_files"):
                GATE.load_spec(root, spec.relative_to(root))

    def test_whole_package_cannot_hide_behind_a_file_list(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rust.rs").write_text("", encoding="utf-8")
            body = self.base(
                "whole-go-package", 'accepted_go_files = ["pkg/sample/sample.go"]\n'
            )
            spec = self.write_spec(root, body)
            with self.assertRaisesRegex(GATE.GateError, "inventories the package"):
                GATE.load_spec(root, spec.relative_to(root))

    def test_cargo_tests_require_twelve_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rust.rs").write_text("", encoding="utf-8")
            body = self.base(
                "package-seed", 'accepted_go_files = ["pkg/sample/sample.go"]\n'
            ).replace(
                f'["{sys.executable}", "-c", "pass"]',
                '["cargo", "test"]',
            )
            spec = self.write_spec(root, body)
            with self.assertRaisesRegex(GATE.GateError, "must use -j12"):
                GATE.load_spec(root, spec.relative_to(root))


class ArtifactTests(unittest.TestCase):
    def repository(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        root = Path(directory.name)
        (root / "pkg/sample").mkdir(parents=True)
        (root / "pkg/sample/sample.go").write_text("package sample\n", encoding="utf-8")
        run_git(root, "init", "-q")
        run_git(root, "add", ".")
        run_git(
            root,
            "-c",
            "user.name=gate-test",
            "-c",
            "user.email=gate@example.com",
            "commit",
            "-qm",
            "source",
        )
        return directory, root

    def test_seed_pins_current_bytes_to_the_accepted_commit(self) -> None:
        directory, root = self.repository()
        with directory:
            spec = {
                "claim": "package-seed",
                "go_package": "pkg/sample",
                "source_commit": "HEAD",
                "accepted_go_files": ["pkg/sample/sample.go"],
            }
            self.assertEqual(GATE.accepted_artifacts(root, spec), ["pkg/sample/sample.go"])
            (root / "pkg/sample/sample.go").write_text("package changed\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "accepted artifact changed"):
                GATE.accepted_artifacts(root, spec)

    def test_whole_package_rejects_an_added_tracked_artifact(self) -> None:
        directory, root = self.repository()
        with directory:
            spec = {
                "claim": "whole-go-package",
                "go_package": "pkg/sample",
                "source_commit": "HEAD",
            }
            (root / "pkg/sample/new.go").write_text("package sample\n", encoding="utf-8")
            run_git(root, "add", "pkg/sample/new.go")
            with self.assertRaisesRegex(GATE.GateError, "inventory drift"):
                GATE.accepted_artifacts(root, spec)


class VerificationTests(unittest.TestCase):
    def test_identical_commands_across_specs_run_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pkg/sample").mkdir(parents=True)
            (root / "pkg/sample/sample.go").write_text("package sample\n", encoding="utf-8")
            (root / "rust.rs").write_text("", encoding="utf-8")
            (root / "counter.txt").write_text("", encoding="utf-8")
            run_git(root, "init", "-q")
            run_git(root, "add", ".")
            run_git(
                root,
                "-c",
                "user.name=gate-test",
                "-c",
                "user.email=gate@example.com",
                "commit",
                "-qm",
                "source",
            )
            command = [
                sys.executable,
                "-c",
                "from pathlib import Path; p=Path('counter.txt'); p.write_text(p.read_text()+'x')",
            ]
            spec = {
                "claim": "package-seed",
                "go_package": "pkg/sample",
                "source_commit": "HEAD",
                "accepted_go_files": ["pkg/sample/sample.go"],
                "rust_files": ["rust.rs"],
                "tests": [{"name": "semantic", "cwd": ".", "argv": command}],
            }
            GATE.verify_specs(root, [(Path("a.toml"), spec), (Path("b.toml"), spec)], False)
            self.assertEqual((root / "counter.txt").read_text(encoding="utf-8"), "x")


if __name__ == "__main__":
    unittest.main()
