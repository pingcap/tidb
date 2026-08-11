from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock


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

    def base(self, extra: str = "") -> str:
        return f'''go_package = "pkg/sample"
source_commit = "HEAD"
evidence_files = ["rust.rs"]
commands = [["test", "-p", "sample"]]
{extra}'''

    def test_minimal_spec_loads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rust.rs").write_text("", encoding="utf-8")
            spec = self.write_spec(root, self.base())
            self.assertEqual(GATE.load_spec(root, spec.relative_to(root))["go_package"], "pkg/sample")

    def test_old_or_unknown_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "rust.rs").write_text("", encoding="utf-8")
            spec = self.write_spec(root, self.base('claim = "package-seed"\n'))
            with self.assertRaisesRegex(GATE.GateError, "unknown specification fields"):
                GATE.load_spec(root, spec.relative_to(root))

    def test_commands_are_normalized_to_twelve_job_offline_cargo(self) -> None:
        self.assertEqual(
            GATE.cargo_argv(["test", "-p", "sample"]),
            ["cargo", "test", "--offline", "--locked", "-j12", "-p", "sample"],
        )

    def test_only_test_and_check_commands_are_accepted(self) -> None:
        with self.assertRaisesRegex(GATE.GateError, "must start with check or test"):
            GATE.command_list([["run", "-p", "sample"]])

    def test_command_arguments_may_repeat(self) -> None:
        self.assertEqual(
            GATE.command_list([["test", "same", "same"]]),
            [["test", "same", "same"]],
        )


class ArtifactTests(unittest.TestCase):
    def repository(self) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        directory = tempfile.TemporaryDirectory()
        root = Path(directory.name)
        (root / "pkg/sample/nested").mkdir(parents=True)
        (root / "pkg/sample/sample.go").write_text("package sample\n", encoding="utf-8")
        (root / "pkg/sample/nested/child.go").write_text("package nested\n", encoding="utf-8")
        (root / "evidence.rs").write_text("", encoding="utf-8")
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

    def spec(self) -> dict[str, object]:
        return {
            "go_package": "pkg/sample",
            "source_commit": "HEAD",
            "evidence_files": ["evidence.rs"],
            "commands": [["test", "-p", "sample"]],
        }

    def test_package_pin_checks_direct_artifacts_but_not_nested_packages(self) -> None:
        directory, root = self.repository()
        with directory:
            self.assertEqual(GATE.accepted_artifacts(root, self.spec()), ["pkg/sample/sample.go"])
            (root / "pkg/sample/nested/child.go").write_text("package changed\n", encoding="utf-8")
            self.assertEqual(GATE.accepted_artifacts(root, self.spec()), ["pkg/sample/sample.go"])

    def test_package_pin_rejects_byte_and_inventory_drift(self) -> None:
        directory, root = self.repository()
        with directory:
            (root / "pkg/sample/sample.go").write_text("package changed\n", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "accepted artifact changed"):
                GATE.accepted_artifacts(root, self.spec())
            run_git(root, "restore", "pkg/sample/sample.go")
            (root / "pkg/sample/new.go").write_text("package sample\n", encoding="utf-8")
            run_git(root, "add", "pkg/sample/new.go")
            with self.assertRaisesRegex(GATE.GateError, "inventory drift"):
                GATE.accepted_artifacts(root, self.spec())

    def test_evidence_files_must_be_tracked(self) -> None:
        directory, root = self.repository()
        with directory:
            (root / "untracked.rs").write_text("", encoding="utf-8")
            with self.assertRaisesRegex(GATE.GateError, "must be tracked exactly"):
                GATE.require_evidence_files(root, ["untracked.rs"])


class VerificationTests(unittest.TestCase):
    def test_identical_commands_across_specs_run_once(self) -> None:
        spec = {
            "go_package": "pkg/sample",
            "source_commit": "HEAD",
            "evidence_files": ["evidence.rs"],
            "commands": [["test", "-p", "sample"]],
        }
        completed = subprocess.CompletedProcess([], 0, "")
        with mock.patch.object(GATE, "accepted_artifacts"), mock.patch.object(
            GATE, "require_evidence_files"
        ), mock.patch.object(GATE, "run", return_value=completed) as runner:
            GATE.verify_specs(
                Path("."), [(Path("a.toml"), spec), (Path("b.toml"), spec)], False
            )
        runner.assert_called_once_with(
            Path("."),
            ["cargo", "test", "--offline", "--locked", "-j12", "-p", "sample"],
            "rust",
        )


if __name__ == "__main__":
    unittest.main()
