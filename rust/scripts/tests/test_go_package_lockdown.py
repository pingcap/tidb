from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).parents[1] / "go-package-lockdown.py"
SPEC = importlib.util.spec_from_file_location("go_package_lockdown", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
LOCKDOWN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LOCKDOWN)


def obligation(identifier: str, owner: str, anchor: str | None = None) -> dict[str, str]:
    return {
        "id": identifier,
        "category": "function",
        "source": "pkg/sample/sample.go",
        "anchor": anchor or owner,
        "node_sha256": identifier.removeprefix("O").ljust(64, "0"),
        "owner": owner,
    }


def rule(identifier: str, owners: list[str]) -> dict[str, object]:
    return {
        "id": identifier,
        "status": "DECLINED",
        "rationale": "not observable at the Rust package boundary",
        "owners": owners,
    }


class RuleResolutionTests(unittest.TestCase):
    def test_unmatched_obligations_remain_visible(self) -> None:
        spec = {"rules": [rule("R_ONE", ["One"])]}
        resolved, unmatched = LOCKDOWN.resolve_rules(
            spec, [obligation("O1", "One"), obligation("O2", "Two")]
        )
        self.assertEqual([row["id"] for row in resolved["R_ONE"]], ["O1"])
        self.assertEqual([row["id"] for row in unmatched], ["O2"])

    def test_nested_package_exclusion_is_path_scoped(self) -> None:
        excluded = ["pkg/sample/nested"]

        def included(path: str) -> bool:
            return not any(
                path == prefix or path.startswith(f"{prefix}/") for prefix in excluded
            )

        self.assertTrue(included("pkg/sample/sample.go"))
        self.assertFalse(included("pkg/sample/nested/child.go"))
        self.assertTrue(included("pkg/sample/nested_name.go"))

    def test_overlapping_rules_fail_closed(self) -> None:
        spec = {"rules": [rule("R_ALL", ["*"]), rule("R_ONE", ["One"])]}
        with self.assertRaisesRegex(LOCKDOWN.LockdownError, "matches multiple rules"):
            LOCKDOWN.resolve_rules(spec, [obligation("O1", "One")])

    def test_missing_exact_obligation_fails_closed(self) -> None:
        exact = rule("R_EXACT", ["One"])
        exact["obligations"] = ["O-missing"]
        spec = {"rules": [exact]}
        with self.assertRaisesRegex(LOCKDOWN.LockdownError, "references missing obligations"):
            LOCKDOWN.resolve_rules(spec, [obligation("O1", "One")])

    def test_exclusions_remove_incidental_runtime_paths(self) -> None:
        selected = rule("R_VISIBLE", ["Worker"])
        selected["exclude_anchors"] = ["*/zero_iterations"]
        spec = {"rules": [selected]}
        resolved, unmatched = LOCKDOWN.resolve_rules(
            spec,
            [
                obligation("O1", "Worker", "Worker/loop:1/enters"),
                obligation("O2", "Worker", "Worker/loop:1/zero_iterations"),
            ],
        )
        self.assertEqual([row["id"] for row in resolved["R_VISIBLE"]], ["O1"])
        self.assertEqual([row["id"] for row in unmatched], ["O2"])


class MutationEvidenceTests(unittest.TestCase):
    def mutation_rule(self, root: Path, always_pass: bool = False) -> dict[str, object]:
        source = root / "source.txt"
        source.write_text("good\n", encoding="utf-8")
        expression = "True" if always_pass else "'good' in open('source.txt').read()"
        return {
            "id": "R_MUTATION",
            "status": "PORTED",
            "test": {
                "cwd": ".",
                "argv": [sys.executable, "-c", f"import sys; sys.exit(0 if {expression} else 1)"],
            },
            "mutation": {"path": "source.txt", "old": "good", "new": "bad", "count": 1},
        }

    def test_killed_mutation_restores_source_byte_for_byte(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rule = self.mutation_rule(root)
            before = (root / "source.txt").read_bytes()
            evidence = LOCKDOWN.verify_rule(root, rule, "fingerprint")
            self.assertEqual(evidence["result"], "KILLED")
            self.assertNotEqual(evidence["mutation"]["mutant"]["returncode"], 0)
            self.assertEqual((root / "source.txt").read_bytes(), before)

    def test_surviving_mutation_fails_and_still_restores_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rule = self.mutation_rule(root, always_pass=True)
            before = (root / "source.txt").read_bytes()
            with self.assertRaisesRegex(LOCKDOWN.LockdownError, "mutation survived"):
                LOCKDOWN.verify_rule(root, rule, "fingerprint")
            self.assertEqual((root / "source.txt").read_bytes(), before)

    def test_unrelated_failure_does_not_kill_the_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rule = self.mutation_rule(root)
            rule["mutation"]["failure_contains"] = "semantic-boundary"
            before = (root / "source.txt").read_bytes()
            with self.assertRaisesRegex(LOCKDOWN.LockdownError, "outside its named semantic test"):
                LOCKDOWN.verify_rule(root, rule, "fingerprint")
            self.assertEqual((root / "source.txt").read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
