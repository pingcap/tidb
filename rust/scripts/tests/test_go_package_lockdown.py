#!/usr/bin/env python3
"""Focused end-to-end tests for go-package-lockdown.py."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPOSITORY = Path(__file__).resolve().parents[3]
CHECKER = REPOSITORY / "rust/scripts/go-package-lockdown.py"
GO_TOOL = REPOSITORY / "rust/difftests/tools/go_package_lockdown_inventory/main.go"
GO_FIXTURE_TOOL = REPOSITORY / "rust/difftests/tools/go_test_fixture_inventory/main.go"
SPEC = Path("evidence/package.toml")
LEDGER_HEADER = [
    "obligation_id", "category", "source_path", "ast_anchor", "node_sha256", "owner",
    "status", "symbol_id", "evidence", "rule_id",
]


def run(root: Path, command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=root,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_tsv(path: Path) -> tuple[str, list[str], list[dict[str, str]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    schema = lines[0]
    reader = csv.DictReader(lines[1:], delimiter="\t")
    return schema, list(reader.fieldnames or []), list(reader)


def write_tsv(path: Path, schema: str, header: list[str], rows: list[dict[str, str]]) -> None:
    lines = [schema, "\t".join(header)]
    lines.extend("\t".join(row[column] for column in header) for row in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class GoPackageLockdownTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._class_temp = tempfile.TemporaryDirectory(prefix="go-package-lockdown-test-")
        cls.baseline = Path(cls._class_temp.name) / "baseline"
        cls._create_baseline(cls.baseline)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._class_temp.cleanup()

    def setUp(self) -> None:
        self._test_temp = tempfile.TemporaryDirectory(prefix="go-package-lockdown-case-")
        self.root = Path(self._test_temp.name) / "repo"
        shutil.copytree(self.baseline, self.root, symlinks=True)

    def tearDown(self) -> None:
        self._test_temp.cleanup()

    @classmethod
    def _checker(cls, root: Path, command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return run(
            root,
            [sys.executable, str(CHECKER), "--root", str(root), command, "--spec", SPEC.as_posix()],
            check=check,
        )

    @classmethod
    def _create_baseline(cls, root: Path) -> None:
        (root / "pkg/sample/testdata").mkdir(parents=True)
        (root / "rust/difftests/tools/go_package_lockdown_inventory").mkdir(parents=True)
        (root / "rust/difftests/tools/go_test_fixture_inventory").mkdir(parents=True)
        (root / "rust/crates/tidb-sample/tests").mkdir(parents=True)
        (root / "evidence").mkdir(parents=True)
        shutil.copy2(GO_TOOL, root / "rust/difftests/tools/go_package_lockdown_inventory/main.go")
        shutil.copy2(GO_FIXTURE_TOOL, root / "rust/difftests/tools/go_test_fixture_inventory/main.go")
        (root / "go.mod").write_text("module example.com/lockdown\n\ngo 1.26\n", encoding="utf-8")
        (root / "pkg/sample/sample.go").write_text(
            "package sample\n\n"
            "const Limit = 3\n\n"
            "func Clamp(value int) int {\n"
            "\tif value > Limit {\n"
            "\t\treturn Limit\n"
            "\t}\n"
            "\treturn value\n"
            "}\n",
            encoding="utf-8",
        )
        (root / "pkg/sample/sample_test.go").write_text(
            "package sample\n\n"
            "import (\n\t\"os\"\n\t\"testing\"\n)\n\n"
            "func TestClamp(t *testing.T) {\n"
            "\tif _, err := os.ReadFile(\"testdata/cases.txt\"); err != nil { t.Fatal(err) }\n"
            "\tfor _, row := range []struct{ input, expected int }{{2, 2}, {3, 3}, {4, 3}} {\n"
            "\t\tif actual := Clamp(row.input); actual != row.expected {\n"
            "\t\t\tt.Fatalf(\"Clamp(%d)=%d\", row.input, actual)\n"
            "\t\t}\n"
            "\t}\n"
            "}\n",
            encoding="utf-8",
        )
        (root / "pkg/sample/BUILD.bazel").write_text(
            "go_library(name = \"sample\", srcs = [\"sample.go\"])\n",
            encoding="utf-8",
        )
        (root / "pkg/sample/testdata/cases.txt").write_text("2=2\n3=3\n4=3\n", encoding="utf-8")
        anchor = root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "pub fn sample_symbol(value: i32) -> i32 { value }\n\n"
            "#[test]\nfn test_sample_boundary() { assert_eq!(sample_symbol(3), 3); }\n",
            encoding="utf-8",
        )
        run(root, ["git", "init", "-q"])
        run(root, ["git", "config", "user.name", "Lockdown Test"])
        run(root, ["git", "config", "user.email", "lockdown@example.com"])
        run(root, ["git", "add", "."])
        run(root, ["git", "commit", "-qm", "synthetic source"])
        source_commit = run(root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        (root / SPEC).write_text(
            "schema = \"go-package-lockdown-spec-v1\"\n"
            "claim = \"whole-go-package\"\n"
            "go_package = \"pkg/sample\"\n"
            f"source_commit = \"{source_commit}\"\n"
            "primary_rust_crate = \"tidb-sample\"\n"
            "mapped_rust_crates = [\"tidb-sample\"]\n"
            "extra_artifacts = []\n"
            "owned_rust_files = [\"rust/crates/tidb-sample/tests/sample_lockdown.rs\"]\n"
            "excluded_subpackages = []\n\n"
            "[unresolved_fixture_evidence]\n\n"
            "[artifact_roles]\n",
            encoding="utf-8",
        )
        cls._checker(root, "generate")
        cls._classify(root, source_commit, anchor)
        cls._checker(root, "write-receipt")
        cls._checker(root, "check")

    @classmethod
    def _classify(cls, root: Path, source_commit: str, anchor: Path) -> None:
        all_rows: list[tuple[Path, str, list[str], list[dict[str, str]]]] = []
        for ledger in sorted((root / "evidence/ledgers").glob("*.tsv")):
            schema, header, rows = read_tsv(ledger)
            all_rows.append((ledger, schema, header, rows))
        flattened = [row for _path, _schema, _header, rows in all_rows for row in rows]
        if len(flattened) < 3:
            raise AssertionError("synthetic package produced too few obligations")
        ported_id = flattened[0]["obligation_id"]
        for index, row in enumerate(flattened):
            go_quote = (
                f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                f"@sha256:{row['node_sha256']}"
            )
            if index == 0:
                row.update(
                    status="PORTED",
                    symbol_id="SAMPLE-SYMBOL",
                    evidence="boundary-test:test_sample_boundary",
                    rule_id="RULE-CLAMP",
                )
            elif index == 1:
                row.update(
                    status="DECLINED",
                    symbol_id="-",
                    evidence=f"{go_quote};measured:synthetic runtime is intentionally absent",
                    rule_id="-",
                )
            else:
                row.update(
                    status="UNREACHABLE",
                    symbol_id="-",
                    evidence=f"{go_quote};structural-proof:closed synthetic entry surface",
                    rule_id="-",
                )
        for ledger, schema, header, rows in all_rows:
            write_tsv(ledger, schema, header, rows)

        write_tsv(
            root / "evidence/symbols.tsv",
            "# symbols-v1",
            ["symbol_id", "rust_crate", "rust_symbol", "anchor_path", "anchor_name"],
            [{
                "symbol_id": "SAMPLE-SYMBOL",
                "rust_crate": "tidb-sample",
                "rust_symbol": "sample_symbol",
                "anchor_path": "rust/crates/tidb-sample/tests/sample_lockdown.rs",
                "anchor_name": "test_sample_boundary",
            }],
        )
        write_tsv(
            root / "evidence/rules.tsv",
            "# rules-v1",
            ["rule_id", "cluster_id", "description", "obligation_ids", "boundary_cases", "mutation_ids"],
            [{
                "rule_id": "RULE-CLAMP",
                "cluster_id": "CLUSTER-CLAMP",
                "description": "synthetic clamp boundary",
                "obligation_ids": ported_id,
                "boundary_cases": "below-limit;at-limit;above-limit",
                "mutation_ids": "MUT-CLAMP",
            }],
        )
        source_hash = sha256(anchor)
        write_tsv(
            root / "evidence/mutation-plan.tsv",
            "# mutation-plan-v1",
            [
                "mutation_id", "cluster_id", "rule_ids", "baseline_commit", "rust_path",
                "source_sha256", "command", "named_test",
            ],
            [{
                "mutation_id": "MUT-CLAMP",
                "cluster_id": "CLUSTER-CLAMP",
                "rule_ids": "RULE-CLAMP",
                "baseline_commit": source_commit,
                "rust_path": "rust/crates/tidb-sample/tests/sample_lockdown.rs",
                "source_sha256": source_hash,
                "command": "cargo test -p tidb-sample test_sample_boundary -- --exact",
                "named_test": "test_sample_boundary",
            }],
        )
        write_tsv(
            root / "evidence/mutation-results.tsv",
            "# mutation-results-v1",
            [
                "attempt_id", "mutation_id", "outcome", "exit_code", "restore_status",
                "restored_source_sha256", "named_failure",
            ],
            [{
                "attempt_id": "ATTEMPT-CLAMP-1",
                "mutation_id": "MUT-CLAMP",
                "outcome": "KILLED",
                "exit_code": "101",
                "restore_status": "PASS",
                "restored_source_sha256": source_hash,
                "named_failure": "test_sample_boundary failed at above-limit",
            }],
        )

    def checker(self, command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self._checker(self.root, command, check)

    def assert_checker_fails(self, command: str, message: str) -> None:
        result = self.checker(command, check=False)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)

    def test_complete_lifecycle_is_content_addressed_and_per_file(self) -> None:
        result = self.checker("check")
        self.assertIn("checked pkg/sample", result.stdout)
        self.assertEqual(
            {path.name for path in (self.root / "evidence/ledgers").glob("*.tsv")},
            {"sample.go.tsv", "sample_test.go.tsv"},
        )
        receipt = json.loads((self.root / "evidence/receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["checker_schema"], "go-package-lockdown-checker-v1")
        self.assertEqual(receipt["fixture_access_count"], 1)
        self.assertNotIn("rust/scripts/go-package-lockdown.py", receipt["owned_file_sha256"])

    def test_all_declined_package_is_valid_falsification(self) -> None:
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            schema, header, rows = read_tsv(ledger)
            for row in rows:
                row.update(
                    status="DECLINED",
                    symbol_id="-",
                    evidence=(
                        f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                        f"@sha256:{row['node_sha256']};measured:no native runtime boundary"
                    ),
                    rule_id="-",
                )
            write_tsv(ledger, schema, header, rows)
        for relative in ["symbols.tsv", "rules.tsv", "mutation-plan.tsv", "mutation-results.tsv"]:
            path = self.root / "evidence" / relative
            schema, header, _rows = read_tsv(path)
            write_tsv(path, schema, header, [])
        self.checker("write-receipt")
        self.checker("check")
        receipt = json.loads((self.root / "evidence/receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["completion_kind"], "falsification")

    def test_check_rejects_go_artifact_drift(self) -> None:
        source = self.root / "pkg/sample/sample.go"
        source.write_text(source.read_text(encoding="utf-8") + "// drift\n", encoding="utf-8")
        self.assert_checker_fails("check", "artifact manifest drifted")

    def test_generate_rejects_untracked_package_artifact(self) -> None:
        (self.root / "pkg/sample/untracked.go").write_text("package sample\n", encoding="utf-8")
        self.assert_checker_fails("generate", "untracked package artifacts")

    def test_generate_rejects_unknown_spec_field(self) -> None:
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                "excluded_subpackages = []", "typo_field = true\nexcluded_subpackages = []"
            ),
            encoding="utf-8",
        )
        self.assert_checker_fails("generate", "unknown fields")

    def test_generate_rejects_unknown_tracked_artifact_role(self) -> None:
        (self.root / "pkg/sample/notes.xyz").write_text("owned but unknown\n", encoding="utf-8")
        run(self.root, ["git", "add", "pkg/sample/notes.xyz"])
        self.assert_checker_fails("generate", "unclassified package artifact")

    def test_check_rejects_missing_ledger_obligation(self) -> None:
        ledger = self.root / "evidence/ledgers/sample_test.go.tsv"
        schema, header, rows = read_tsv(ledger)
        self.assertTrue(rows)
        write_tsv(ledger, schema, header, rows[:-1])
        self.assert_checker_fails("check", "missing from ledgers")

    def test_check_rejects_disappeared_ported_symbol(self) -> None:
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(anchor.read_text(encoding="utf-8").replace("sample_symbol", "gone_symbol"), encoding="utf-8")
        self.assert_checker_fails("check", "disappeared from compile anchor")

    def test_check_rejects_weak_decline_and_unreachable_evidence(self) -> None:
        ledger = self.root / "evidence/ledgers/sample.go.tsv"
        schema, header, rows = read_tsv(ledger)
        original = [row.copy() for row in rows]
        declined = next(row for row in rows if row["status"] == "DECLINED")
        declined["evidence"] = "handwave"
        write_tsv(ledger, schema, header, rows)
        self.assert_checker_fails("check", "DECLINED obligation")
        unreachable = next(row for row in original if row["status"] == "UNREACHABLE")
        unreachable["evidence"] = "go-quote:wrong;structural-proof:handwave"
        write_tsv(ledger, schema, header, original)
        self.assert_checker_fails("check", "UNREACHABLE obligation")

    def test_check_rejects_rule_mutation_omission(self) -> None:
        plan = self.root / "evidence/mutation-plan.tsv"
        schema, header, _rows = read_tsv(plan)
        write_tsv(plan, schema, header, [])
        self.assert_checker_fails("check", "mutation plan contains missing")

    def test_check_records_survivor_but_requires_final_kill(self) -> None:
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, rows = read_tsv(results)
        rows[0].update(outcome="SURVIVED", exit_code="0", named_failure="-")
        write_tsv(results, schema, header, rows)
        self.assert_checker_fails("check", "does not end with a killed attempt")

    def test_check_rejects_receipt_drift(self) -> None:
        receipt_path = self.root / "evidence/receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["obligation_count"] += 1
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.assert_checker_fails("check", "package receipt drifted")

    def test_generate_rejects_unmanifested_external_fixture(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace("testdata/cases.txt", "../shared.txt"),
            encoding="utf-8",
        )
        (self.root / "pkg/shared.txt").write_text("outside package\n", encoding="utf-8")
        run(self.root, ["git", "add", "pkg/shared.txt"])
        self.assert_checker_fails("generate", "is not manifested")

    def test_generate_rejects_unclassified_dynamic_fixture_access(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "if _, err := os.ReadFile(\"testdata/cases.txt\")",
                "fixturePath := \"testdata/cases.txt\"\n\tif _, err := os.ReadFile(fixturePath)",
            ),
            encoding="utf-8",
        )
        self.assert_checker_fails("generate", "unresolved fixture evidence is not exact")

    def test_generate_preserves_verdicts_and_adds_unclassified_rows(self) -> None:
        before: dict[str, tuple[str, str, str, str]] = {}
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            _schema, _header, rows = read_tsv(ledger)
            before.update({
                row["obligation_id"]: (
                    row["status"], row["symbol_id"], row["evidence"], row["rule_id"]
                ) for row in rows
            })
        source = self.root / "pkg/sample/sample.go"
        source.write_text(
            source.read_text(encoding="utf-8") + "\nfunc Identity(value int) int { return value }\n",
            encoding="utf-8",
        )
        self.checker("generate")
        after: dict[str, tuple[str, str, str, str]] = {}
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            _schema, _header, rows = read_tsv(ledger)
            after.update({
                row["obligation_id"]: (
                    row["status"], row["symbol_id"], row["evidence"], row["rule_id"]
                ) for row in rows
            })
        for identity, verdict in before.items():
            self.assertEqual(after[identity], verdict)
        self.assertTrue(any(verdict[0] == "UNCLASSIFIED" for verdict in after.values()))

    def test_generate_refuses_to_discard_removed_obligations(self) -> None:
        ledger = self.root / "evidence/ledgers/sample.go.tsv"
        before = ledger.read_bytes()
        source = self.root / "pkg/sample/sample.go"
        source.write_text("package sample\n\nconst Limit = 3\n", encoding="utf-8")
        self.assert_checker_fails("generate", "would discard classified obligations")
        self.assertEqual(ledger.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
