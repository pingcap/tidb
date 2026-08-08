#!/usr/bin/env python3
"""Focused end-to-end tests for go-package-lockdown.py."""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


REPOSITORY = Path(__file__).resolve().parents[3]
CHECKER = REPOSITORY / "rust/scripts/go-package-lockdown.py"
GO_TOOL = REPOSITORY / "rust/difftests/tools/go_package_lockdown_inventory/main.go"
GO_FIXTURE_TOOL = REPOSITORY / "rust/difftests/tools/go_test_fixture_inventory/main.go"
GO_HELPER_CALL_TOOL = REPOSITORY / "rust/difftests/tools/go_test_helper_call_inventory/main.go"
GO_EMBED_TOOL = REPOSITORY / "rust/difftests/tools/go_package_embed_inventory/main.go"
SPEC = Path("evidence/package.toml")
LEDGER_HEADER = [
    "obligation_id", "category", "source_path", "ast_anchor", "node_sha256", "owner",
    "source_blob_sha256", "status", "symbol_id", "evidence", "rule_id",
]
MUTATION_RESULT_HEADER = [
    "sequence", "attempt_id", "mutation_id", "prior_history_sha256",
    "prior_checkpoint_sha256", "attempt_plan_path", "attempt_plan_sha256",
    "run_artifact_path", "run_artifact_sha256",
    "verification_artifact_path", "verification_artifact_sha256",
    "history_sha256",
]
PROBE_RESULT_HEADER = [
    "probe_id", "run_artifact_path", "run_artifact_sha256",
    "verification_artifact_path", "verification_artifact_sha256",
]

MODULE_SPEC = importlib.util.spec_from_file_location("go_package_lockdown", CHECKER)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError("cannot import go-package-lockdown.py")
LOCKDOWN = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(LOCKDOWN)


def run(root: Path, command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["GOCACHE"] = str(root / ".test-go-cache")
    return subprocess.run(
        command,
        cwd=root,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
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


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(LOCKDOWN.canonical_json_text(payload), encoding="utf-8")


def command_log(stdout: bytes, stderr: bytes = b"") -> bytes:
    return b"--- stdout ---\n" + stdout + b"\n--- stderr ---\n" + stderr


def runtime_observation_line(path: Path) -> bytes:
    expected = json.loads(path.read_text(encoding="utf-8"))
    runtime = {
        "schema": "go-package-lockdown-runtime-observation-v1",
        "probe_id": expected["probe_id"],
        "source_commit": expected["source_commit"],
        "conclusion": expected["conclusion"],
        "boundary_observations": [
            {"name": case["name"], "input": case["input"], "observed": case["expected"]}
            for case in expected["boundary_observations"]
        ],
    }
    return b"LOCKDOWN_OBSERVATION " + LOCKDOWN.canonical_json_bytes(runtime)


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
        self._go_cache_environment = mock.patch.dict(
            os.environ, {"GOCACHE": str(self.root / ".test-go-cache")}
        )
        self._go_cache_environment.start()

    def tearDown(self) -> None:
        self._go_cache_environment.stop()
        self._test_temp.cleanup()

    @classmethod
    def _spec_source_commit_for_path(cls, root: Path, spec_path: Path) -> str:
        match = re.search(
            r'^source_commit = "([0-9a-f]{40})"$',
            (root / spec_path).read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        if match is None:
            raise AssertionError("synthetic spec has no source_commit")
        return match.group(1)

    @classmethod
    def _spec_source_commit(cls, root: Path) -> str:
        return cls._spec_source_commit_for_path(root, SPEC)

    @classmethod
    def _checker(
        cls,
        root: Path,
        command: str,
        check: bool = True,
        accepted_source_commit: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        accepted = accepted_source_commit or cls._spec_source_commit(root)
        return run(
            root,
            [
                sys.executable, str(CHECKER), "--root", str(root), command,
                "--spec", SPEC.as_posix(), "--accepted-source-commit", accepted,
            ],
            check=check,
        )

    @classmethod
    def _create_baseline(cls, root: Path) -> None:
        (root / "pkg/sample/testdata").mkdir(parents=True)
        (root / "rust/difftests/tools/go_package_lockdown_inventory").mkdir(parents=True)
        (root / "rust/difftests/tools/go_test_fixture_inventory").mkdir(parents=True)
        (root / "rust/difftests/tools/go_test_helper_call_inventory").mkdir(parents=True)
        (root / "rust/difftests/tools/go_package_embed_inventory").mkdir(parents=True)
        (root / "rust/crates/tidb-sample/src").mkdir(parents=True)
        (root / "rust/crates/tidb-sample/tests").mkdir(parents=True)
        (root / "evidence").mkdir(parents=True)
        shutil.copy2(GO_TOOL, root / "rust/difftests/tools/go_package_lockdown_inventory/main.go")
        shutil.copy2(GO_FIXTURE_TOOL, root / "rust/difftests/tools/go_test_fixture_inventory/main.go")
        shutil.copy2(
            GO_HELPER_CALL_TOOL,
            root / "rust/difftests/tools/go_test_helper_call_inventory/main.go",
        )
        shutil.copy2(
            GO_EMBED_TOOL,
            root / "rust/difftests/tools/go_package_embed_inventory/main.go",
        )
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
        definition = root / "rust/crates/tidb-sample/src/lib.rs"
        definition.write_text(
            "pub fn sample_symbol(value: i32) -> i32 { value }\n", encoding="utf-8"
        )
        anchor = root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "#[test]\nfn test_sample_boundary() { "
            "assert_eq!(tidb_sample::sample_symbol(3), 3); }\n",
            encoding="utf-8",
        )
        (root / "rust/crates/tidb-sample/Cargo.toml").write_text(
            "[package]\nname = \"tidb-sample\"\nversion = \"0.0.0\"\n"
            "edition = \"2021\"\n\n"
            "[[test]]\nname = \"sample_lockdown\"\npath = \"tests/sample_lockdown.rs\"\n",
            encoding="utf-8",
        )
        run(root, ["git", "init", "-q"])
        run(root, ["git", "config", "user.name", "Lockdown Test"])
        run(root, ["git", "config", "user.email", "lockdown@example.com"])
        run(root, ["git", "add", "."])
        run(root, ["git", "commit", "-qm", "synthetic source"])
        source_commit = run(root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        (root / SPEC).write_text(
            "schema = \"go-package-lockdown-spec-v2\"\n"
            "claim = \"whole-go-package\"\n"
            "go_package = \"pkg/sample\"\n"
            f"source_commit = \"{source_commit}\"\n"
            "primary_rust_crate = \"tidb-sample\"\n"
            "mapped_rust_crates = [\"tidb-sample\"]\n"
            "extra_artifacts = []\n"
            "owned_rust_files = [\"rust/crates/tidb-sample/src/lib.rs\", "
            "\"rust/crates/tidb-sample/tests/sample_lockdown.rs\"]\n"
            "excluded_subpackages = []\n\n"
            "[unresolved_fixture_evidence]\n\n"
            "[artifact_roles]\n",
            encoding="utf-8",
        )
        cls._checker(root, "generate")
        cls._classify(root, source_commit, definition, anchor)
        cls._checker(root, "write-receipt")
        cls._checker(root, "check")

    @classmethod
    def _classify(
        cls, root: Path, source_commit: str, definition: Path, anchor: Path
    ) -> None:
        all_rows: list[tuple[Path, str, list[str], list[dict[str, str]]]] = []
        for ledger in sorted((root / "evidence/ledgers").glob("*.tsv")):
            schema, header, rows = read_tsv(ledger)
            all_rows.append((ledger, schema, header, rows))
        flattened = [row for _path, _schema, _header, rows in all_rows for row in rows]
        if len(flattened) < 3:
            raise AssertionError("synthetic package produced too few obligations")

        helper_path = root / "evidence/helper-contracts.tsv"
        helper_schema, helper_header, helper_rows = read_tsv(helper_path)
        fixture_row = [
            "pkg/sample/sample_test.go", "9", "os.ReadFile", '"testdata/cases.txt"',
            "pkg/sample/testdata/cases.txt",
        ]
        direct_hash = hashlib.sha256(LOCKDOWN.canonical_json_bytes(fixture_row)).hexdigest()
        for helper in helper_rows:
            if helper["callee"].split(".")[-1] == "ReadFile":
                helper.update(
                    status="DIRECT-FIXTURE",
                    evidence=f"direct-fixture-sha256:{direct_hash}",
                )
                continue
            proof = root / f"evidence/helper-proofs/{helper['helper_id']}.json"
            write_json(proof, {
                "schema": "go-package-lockdown-evidence-v1",
                "kind": "helper-no-fixture",
                "source_commit": source_commit,
                "helper_id": helper["helper_id"],
                "callee": helper["callee"],
                "call_ids": helper["call_ids"].split(";"),
                "call_set_sha256": helper["call_set_sha256"],
                "conclusion": "the helper consumes values or reports failures without file access",
                "boundary_cases": ["successful-value", "failure-value"],
                "proof_steps": [
                    "bound every call expression to the package AST inventory",
                    "inspected the exact helper semantics for fixture or file access",
                ],
            })
            helper.update(
                status="NO-FIXTURE",
                evidence=(
                    f"evidence-artifact:{proof.relative_to(root).as_posix()}"
                    f"@sha256:{sha256(proof)}"
                ),
            )
        write_tsv(helper_path, helper_schema, helper_header, helper_rows)

        ported_id = flattened[0]["obligation_id"]
        declined_ids = [flattened[1]["obligation_id"]]
        unreachable_ids = [row["obligation_id"] for row in flattened[2:]]

        probe_conclusion = "the exact Go boundary is measured but has no native runtime surface"
        observation_path = root / "evidence/observations/PROBE-DECLINED.json"
        write_json(observation_path, {
            "schema": "go-package-lockdown-observation-v1",
            "probe_id": "PROBE-DECLINED",
            "source_commit": source_commit,
            "conclusion": probe_conclusion,
            "boundary_observations": [
                {"name": "limit-minus-one", "input": "2", "expected": "2"},
                {"name": "limit-plus-one", "input": "4", "expected": "3"},
            ],
        })
        probe_plan_path = root / "evidence/probes/PROBE-DECLINED.json"
        write_json(probe_plan_path, {
            "schema": "go-package-lockdown-evidence-v1",
            "kind": "measured-probe",
            "probe_id": "PROBE-DECLINED",
            "obligation_ids": declined_ids,
            "conclusion": probe_conclusion,
            "source_commit": source_commit,
            "boundary_cases": ["limit-minus-one", "limit-plus-one"],
            "runner": "go-test",
            "test_subject": "pkg/sample",
            "test_target": "-",
            "named_test": "TestClamp",
            "expected_exit_code": 0,
            "observation_path": observation_path.relative_to(root).as_posix(),
            "observation_sha256": sha256(observation_path),
        })
        proof_path = root / "evidence/proofs/UNREACHABLE.json"
        write_json(proof_path, {
            "schema": "go-package-lockdown-evidence-v1",
            "kind": "structural-proof",
            "obligation_ids": unreachable_ids,
            "conclusion": "the remaining support nodes have no independent native entry",
            "source_commit": source_commit,
            "boundary_cases": ["direct-call", "package-test-call"],
            "entry_surface": "synthetic package exports only Clamp",
            "proof_steps": [
                "enumerated every direct exported declaration",
                "traced every synthetic test call to Clamp",
            ],
        })
        probe_ref = (
            f"evidence-artifact:{probe_plan_path.relative_to(root).as_posix()}"
            f"@sha256:{sha256(probe_plan_path)}"
        )
        proof_ref = (
            f"evidence-artifact:{proof_path.relative_to(root).as_posix()}"
            f"@sha256:{sha256(proof_path)}"
        )
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
                    evidence=f"{go_quote};{probe_ref}",
                    rule_id="-",
                )
            else:
                row.update(
                    status="UNREACHABLE",
                    symbol_id="-",
                    evidence=f"{go_quote};{proof_ref}",
                    rule_id="-",
                )
        for ledger, schema, header, rows in all_rows:
            write_tsv(ledger, schema, header, rows)

        write_tsv(
            root / "evidence/symbols.tsv",
            "# symbols-v1",
            [
                "symbol_id", "rust_crate", "rust_symbol", "definition_path", "anchor_path",
                "anchor_target", "anchor_name",
            ],
            [{
                "symbol_id": "SAMPLE-SYMBOL",
                "rust_crate": "tidb-sample",
                "rust_symbol": "tidb_sample::sample_symbol",
                "definition_path": "rust/crates/tidb-sample/src/lib.rs",
                "anchor_path": "rust/crates/tidb-sample/tests/sample_lockdown.rs",
                "anchor_target": "sample_lockdown",
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
        source_hash = sha256(definition)
        operator_path = root / "evidence/mutation-operators/MUT-CLAMP.json"
        write_json(operator_path, {
            "schema": "go-package-lockdown-mutation-operator-v1",
            "mutation_id": "MUT-CLAMP",
            "rust_path": "rust/crates/tidb-sample/src/lib.rs",
            "source_sha256": source_hash,
            "replacements": [{
                "old": "{ value }",
                "new": "{ value + 1 }",
                "expected_count": 1,
            }],
        })
        write_tsv(
            root / "evidence/mutation-plan.tsv",
            "# mutation-plan-v2",
            [
                "mutation_id", "cluster_id", "rule_ids", "baseline_commit", "rust_path",
                "source_sha256", "runner", "test_subject", "test_target", "named_test",
                "operator_path", "operator_sha256",
            ],
            [{
                "mutation_id": "MUT-CLAMP",
                "cluster_id": "CLUSTER-CLAMP",
                "rule_ids": "RULE-CLAMP",
                "baseline_commit": source_commit,
                "rust_path": "rust/crates/tidb-sample/src/lib.rs",
                "source_sha256": source_hash,
                "runner": "cargo-test",
                "test_subject": "tidb-sample",
                "test_target": "sample_lockdown",
                "named_test": "test_sample_boundary",
                "operator_path": operator_path.relative_to(root).as_posix(),
                "operator_sha256": sha256(operator_path),
            }],
        )

        mutation_stdout = b"running 1 test\ntest test_sample_boundary ... FAILED\nfinished in 0.01s\n"
        mutation_verify_stdout = mutation_stdout.replace(b"0.01s", b"0.02s")
        mutation_argv = [
            "cargo", "test", "--offline", "--locked", "-j12", "--quiet", "-p",
            "tidb-sample", "--test", "sample_lockdown", "test_sample_boundary", "--",
            "--exact", "--nocapture",
        ]
        mutation_observation = LOCKDOWN.normalized_test_observation(
            "cargo-test", "test_sample_boundary", 101, mutation_stdout, b""
        )
        mutation_pass_stdout = b"running 1 test\ntest test_sample_boundary ... ok\n"
        mutation_pass_observation = LOCKDOWN.normalized_test_observation(
            "cargo-test", "test_sample_boundary", 0, mutation_pass_stdout, b""
        )
        mutated_source_hash = hashlib.sha256(
            definition.read_bytes().replace(b"{ value }", b"{ value + 1 }")
        ).hexdigest()
        attempt_plan_path = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.plan.json"
        write_json(attempt_plan_path, {
            "schema": "go-package-lockdown-mutation-attempt-plan-v1",
            "producer": "go-package-lockdown-checker-v2",
            "attempt_id": "ATTEMPT-CLAMP-1",
            "mutation_id": "MUT-CLAMP",
            "cluster_id": "CLUSTER-CLAMP",
            "rule_ids": "RULE-CLAMP",
            "baseline_commit": source_commit,
            "rust_path": "rust/crates/tidb-sample/src/lib.rs",
            "source_sha256": source_hash,
            "runner": "cargo-test",
            "test_subject": "tidb-sample",
            "test_target": "sample_lockdown",
            "named_test": "test_sample_boundary",
            "command_argv": mutation_argv,
            "operator_path": operator_path.relative_to(root).as_posix(),
            "operator_sha256": sha256(operator_path),
            "mutated_source_sha256": mutated_source_hash,
        })
        mutation_run_log = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.run.log"
        mutation_run_log.parent.mkdir(parents=True, exist_ok=True)
        mutation_run_log.write_bytes(command_log(mutation_stdout))
        run_baseline_log = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.run.baseline.log"
        run_restored_log = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.run.restored.log"
        run_baseline_log.write_bytes(command_log(mutation_pass_stdout))
        run_restored_log.write_bytes(command_log(mutation_pass_stdout))
        mutation_run_path = mutation_run_log.with_suffix(".json")
        mutation_common = {
            "schema": "go-package-lockdown-mutation-run-v1",
            "producer": "go-package-lockdown-checker-v2",
            "attempt_id": "ATTEMPT-CLAMP-1",
            "mutation_id": "MUT-CLAMP",
            "attempt_plan_path": attempt_plan_path.relative_to(root).as_posix(),
            "attempt_plan_sha256": sha256(attempt_plan_path),
            "baseline_commit": source_commit,
            "rust_path": "rust/crates/tidb-sample/src/lib.rs",
            "original_source_sha256": source_hash,
            "mutated_source_sha256": mutated_source_hash,
            "operator_path": operator_path.relative_to(root).as_posix(),
            "operator_sha256": sha256(operator_path),
            "command_argv": mutation_argv,
            "named_test": "test_sample_boundary",
            "restored_source_sha256": source_hash,
        }
        write_json(mutation_run_path, {
            **mutation_common,
            "phase": "run",
            "prior_artifact_path": "-",
            "prior_artifact_sha256": "-",
            "baseline_exit_code": 0,
            "baseline_output_path": run_baseline_log.relative_to(root).as_posix(),
            "baseline_output_sha256": sha256(run_baseline_log),
            "baseline_normalized_observation_sha256": mutation_pass_observation,
            "exit_code": 101,
            "outcome": "KILLED",
            "normalized_observation_sha256": mutation_observation,
            "output_path": mutation_run_log.relative_to(root).as_posix(),
            "output_sha256": sha256(mutation_run_log),
            "restored_exit_code": 0,
            "restored_output_path": run_restored_log.relative_to(root).as_posix(),
            "restored_output_sha256": sha256(run_restored_log),
            "restored_normalized_observation_sha256": mutation_pass_observation,
        })
        mutation_verify_log = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.verify.log"
        mutation_verify_log.write_bytes(command_log(mutation_verify_stdout))
        verify_baseline_log = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.verify.baseline.log"
        verify_restored_log = root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.verify.restored.log"
        verify_baseline_log.write_bytes(command_log(mutation_pass_stdout))
        verify_restored_log.write_bytes(command_log(mutation_pass_stdout))
        mutation_verify_path = mutation_verify_log.with_suffix(".json")
        write_json(mutation_verify_path, {
            **mutation_common,
            "phase": "verify",
            "prior_artifact_path": mutation_run_path.relative_to(root).as_posix(),
            "prior_artifact_sha256": sha256(mutation_run_path),
            "baseline_exit_code": 0,
            "baseline_output_path": verify_baseline_log.relative_to(root).as_posix(),
            "baseline_output_sha256": sha256(verify_baseline_log),
            "baseline_normalized_observation_sha256": mutation_pass_observation,
            "exit_code": 101,
            "outcome": "KILLED",
            "normalized_observation_sha256": mutation_observation,
            "output_path": mutation_verify_log.relative_to(root).as_posix(),
            "output_sha256": sha256(mutation_verify_log),
            "restored_exit_code": 0,
            "restored_output_path": verify_restored_log.relative_to(root).as_posix(),
            "restored_output_sha256": sha256(verify_restored_log),
            "restored_normalized_observation_sha256": mutation_pass_observation,
        })
        history_genesis = hashlib.sha256(LOCKDOWN.canonical_json_bytes({
            "schema": "go-package-lockdown-mutation-history-v1",
            "go_package": "pkg/sample",
        })).hexdigest()
        mutation_result = {
            "sequence": "1",
            "attempt_id": "ATTEMPT-CLAMP-1",
            "mutation_id": "MUT-CLAMP",
            "prior_history_sha256": history_genesis,
            "prior_checkpoint_sha256": "-",
            "attempt_plan_path": attempt_plan_path.relative_to(root).as_posix(),
            "attempt_plan_sha256": sha256(attempt_plan_path),
            "run_artifact_path": mutation_run_path.relative_to(root).as_posix(),
            "run_artifact_sha256": sha256(mutation_run_path),
            "verification_artifact_path": mutation_verify_path.relative_to(root).as_posix(),
            "verification_artifact_sha256": sha256(mutation_verify_path),
            "history_sha256": "-",
        }
        mutation_result["history_sha256"] = hashlib.sha256(LOCKDOWN.canonical_json_bytes({
            "schema": "go-package-lockdown-mutation-history-v1",
            **{
                key: value for key, value in mutation_result.items()
                if key != "history_sha256"
            },
        })).hexdigest()
        write_tsv(
            root / "evidence/mutation-results.tsv",
            "# mutation-results-v3",
            MUTATION_RESULT_HEADER,
            [mutation_result],
        )

        observation_marker = runtime_observation_line(observation_path) + b"\n"
        probe_stdout = (
            b"=== RUN   TestClamp\n"
            + observation_marker
            + b"--- PASS: TestClamp (0.00s)\n"
            + b"PASS\nok  example.com/lockdown/pkg/sample  0.01s\n"
        )
        probe_verify_stdout = probe_stdout.replace(b"0.01s", b"0.02s")
        probe_argv = [
            "go", "test", "./pkg/sample", "-run", "^TestClamp$", "-count=1", "-v",
        ]
        probe_observation = LOCKDOWN.normalized_test_observation(
            "go-test", "TestClamp", 0, probe_stdout, b""
        )
        probe_run_log = root / "evidence/execution/probe/PROBE-DECLINED.run.log"
        probe_run_log.parent.mkdir(parents=True, exist_ok=True)
        probe_run_log.write_bytes(command_log(probe_stdout))
        probe_run_path = probe_run_log.with_suffix(".json")
        probe_common = {
            "schema": "go-package-lockdown-evidence-run-v1",
            "producer": "go-package-lockdown-checker-v2",
            "probe_id": "PROBE-DECLINED",
            "source_commit": source_commit,
            "evidence_plan_path": probe_plan_path.relative_to(root).as_posix(),
            "evidence_plan_sha256": sha256(probe_plan_path),
            "runner": "go-test",
            "test_subject": "pkg/sample",
            "test_target": "-",
            "named_test": "TestClamp",
            "observation_path": observation_path.relative_to(root).as_posix(),
            "observation_sha256": sha256(observation_path),
            "conclusion_sha256": hashlib.sha256(probe_conclusion.encode()).hexdigest(),
            "command_argv": probe_argv,
            "expected_exit_code": 0,
            "exit_code": 0,
            "normalized_observation_sha256": probe_observation,
        }
        write_json(probe_run_path, {
            **probe_common,
            "phase": "run",
            "prior_artifact_path": "-",
            "prior_artifact_sha256": "-",
            "output_path": probe_run_log.relative_to(root).as_posix(),
            "output_sha256": sha256(probe_run_log),
        })
        probe_verify_log = root / "evidence/execution/probe/PROBE-DECLINED.verify.log"
        probe_verify_log.write_bytes(command_log(probe_verify_stdout))
        probe_verify_path = probe_verify_log.with_suffix(".json")
        write_json(probe_verify_path, {
            **probe_common,
            "phase": "verify",
            "prior_artifact_path": probe_run_path.relative_to(root).as_posix(),
            "prior_artifact_sha256": sha256(probe_run_path),
            "output_path": probe_verify_log.relative_to(root).as_posix(),
            "output_sha256": sha256(probe_verify_log),
        })
        write_tsv(
            root / "evidence/probe-results.tsv",
            "# probe-results-v1",
            PROBE_RESULT_HEADER,
            [{
                "probe_id": "PROBE-DECLINED",
                "run_artifact_path": probe_run_path.relative_to(root).as_posix(),
                "run_artifact_sha256": sha256(probe_run_path),
                "verification_artifact_path": probe_verify_path.relative_to(root).as_posix(),
                "verification_artifact_sha256": sha256(probe_verify_path),
            }],
        )

    def checker(self, command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self._checker(self.root, command, check)

    def assert_checker_fails(self, command: str, message: str) -> None:
        result = self.checker(command, check=False)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)

    def commit_source_update(self, *paths: str) -> str:
        run(self.root, ["git", "add", *paths])
        run(self.root, ["git", "commit", "-qm", "update pinned synthetic source"])
        source_commit = run(self.root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        spec = self.root / SPEC
        spec.write_text(
            re.sub(
                r'source_commit = "[0-9a-f]{40}"',
                f'source_commit = "{source_commit}"',
                spec.read_text(encoding="utf-8"),
            ),
            encoding="utf-8",
        )
        return source_commit

    def clear_mutation_execution(self) -> None:
        execution = self.root / "evidence/execution/mutation"
        for path in execution.iterdir():
            path.unlink()
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, _rows = read_tsv(results)
        write_tsv(results, schema, header, [])

    def clear_probe_execution(self) -> None:
        execution = self.root / "evidence/execution/probe"
        for path in execution.iterdir():
            path.unlink()
        results = self.root / "evidence/probe-results.tsv"
        schema, header, _rows = read_tsv(results)
        write_tsv(results, schema, header, [])

    def rewrite_recorded_probe_observation(self, mutate: object) -> None:
        results_path = self.root / "evidence/probe-results.tsv"
        schema, header, rows = read_tsv(results_path)
        for phase in ["run", "verification"]:
            artifact_path = self.root / rows[0][f"{phase}_artifact_path"]
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
            log_path = self.root / str(payload["output_path"])
            stdout, stderr = LOCKDOWN.parse_command_output(
                log_path.read_bytes(), f"{phase} observation rewrite"
            )
            output_lines = stdout.splitlines()
            marker_index = next(
                index for index, line in enumerate(output_lines)
                if line.startswith(b"LOCKDOWN_OBSERVATION ")
            )
            runtime = json.loads(
                output_lines[marker_index].removeprefix(b"LOCKDOWN_OBSERVATION ")
            )
            mutate(runtime)
            output_lines[marker_index] = (
                b"LOCKDOWN_OBSERVATION " + LOCKDOWN.canonical_json_bytes(runtime)
            )
            new_stdout = b"\n".join(output_lines) + b"\n"
            log_path.write_bytes(command_log(new_stdout, stderr))
            payload["output_sha256"] = sha256(log_path)
            payload["normalized_observation_sha256"] = LOCKDOWN.normalized_test_observation(
                "go-test", "TestClamp", 0, new_stdout, stderr
            )
            if phase == "verification":
                payload["prior_artifact_sha256"] = sha256(
                    self.root / rows[0]["run_artifact_path"]
                )
            write_json(artifact_path, payload)
            rows[0][f"{phase}_artifact_sha256"] = sha256(artifact_path)
        write_tsv(results_path, schema, header, rows)

    def rehash_mutation_history(self, rows: list[dict[str, str]]) -> None:
        prior = hashlib.sha256(LOCKDOWN.canonical_json_bytes({
            "schema": "go-package-lockdown-mutation-history-v1",
            "go_package": "pkg/sample",
        })).hexdigest()
        for sequence, row in enumerate(rows, start=1):
            row["sequence"] = str(sequence)
            row["prior_history_sha256"] = prior
            row["history_sha256"] = hashlib.sha256(LOCKDOWN.canonical_json_bytes({
                "schema": "go-package-lockdown-mutation-history-v1",
                **{key: value for key, value in row.items() if key != "history_sha256"},
            })).hexdigest()
            prior = row["history_sha256"]

    def commit_accepted_history_baseline(self) -> str:
        run(self.root, ["git", "add", "evidence"])
        run(self.root, ["git", "commit", "-qm", "commit accepted package receipt history"])
        accepted = run(self.root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        spec = self.root / SPEC
        spec.write_text(
            re.sub(
                r'source_commit = "[0-9a-f]{40}"',
                f'source_commit = "{accepted}"',
                spec.read_text(encoding="utf-8"),
            ),
            encoding="utf-8",
        )
        return accepted

    def test_complete_lifecycle_is_content_addressed_and_per_file(self) -> None:
        result = self.checker("check")
        self.assertIn("checked pkg/sample", result.stdout)
        self.assertEqual(
            {path.name for path in (self.root / "evidence/ledgers").glob("*.tsv")},
            {"sample.go.tsv", "sample_test.go.tsv"},
        )
        receipt = json.loads((self.root / "evidence/receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["checker_schema"], "go-package-lockdown-checker-v2")
        self.assertEqual(receipt["fixture_access_count"], 1)
        self.assertEqual(receipt["completion_kind"], "classified-gaps")
        self.assertTrue(receipt["inventory_complete"])
        self.assertFalse(receipt["implementation_complete"])
        self.assertNotIn("rust/scripts/go-package-lockdown.py", receipt["owned_file_sha256"])

    def test_all_declined_package_is_valid_falsification(self) -> None:
        ledger_data = []
        obligation_ids: list[str] = []
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            schema, header, rows = read_tsv(ledger)
            obligation_ids.extend(row["obligation_id"] for row in rows)
            ledger_data.append((ledger, schema, header, rows))
        plan_path = self.root / "evidence/probes/PROBE-DECLINED.json"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        plan["obligation_ids"] = obligation_ids
        write_json(plan_path, plan)
        plan_ref = (
            f"evidence-artifact:evidence/probes/PROBE-DECLINED.json@sha256:{sha256(plan_path)}"
        )
        for ledger, schema, header, rows in ledger_data:
            for row in rows:
                row.update(
                    status="DECLINED",
                    symbol_id="-",
                    evidence=(
                        f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                        f"@sha256:{row['node_sha256']};{plan_ref}"
                    ),
                    rule_id="-",
                )
            write_tsv(ledger, schema, header, rows)
        run_path = self.root / "evidence/execution/probe/PROBE-DECLINED.run.json"
        run_payload = json.loads(run_path.read_text(encoding="utf-8"))
        run_payload["evidence_plan_sha256"] = sha256(plan_path)
        write_json(run_path, run_payload)
        verify_path = self.root / "evidence/execution/probe/PROBE-DECLINED.verify.json"
        verify_payload = json.loads(verify_path.read_text(encoding="utf-8"))
        verify_payload["evidence_plan_sha256"] = sha256(plan_path)
        verify_payload["prior_artifact_sha256"] = sha256(run_path)
        write_json(verify_path, verify_payload)
        results = self.root / "evidence/probe-results.tsv"
        schema, header, rows = read_tsv(results)
        rows[0]["run_artifact_sha256"] = sha256(run_path)
        rows[0]["verification_artifact_sha256"] = sha256(verify_path)
        write_tsv(results, schema, header, rows)
        for relative in ["symbols.tsv", "rules.tsv", "mutation-plan.tsv", "mutation-results.tsv"]:
            path = self.root / "evidence" / relative
            schema, header, _rows = read_tsv(path)
            write_tsv(path, schema, header, [])
        (self.root / "evidence/proofs/UNREACHABLE.json").unlink()
        for path in (self.root / "evidence/mutation-operators").iterdir():
            path.unlink()
        for path in (self.root / "evidence/execution/mutation").iterdir():
            path.unlink()
        self.checker("write-receipt")
        self.checker("check")
        receipt = json.loads((self.root / "evidence/receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["completion_kind"], "falsification")

    def test_check_rejects_go_artifact_drift(self) -> None:
        source = self.root / "pkg/sample/sample.go"
        source.write_text(source.read_text(encoding="utf-8") + "// drift\n", encoding="utf-8")
        self.assert_checker_fails("check", "differs from source_commit")

    def test_generate_rejects_untracked_package_artifact(self) -> None:
        (self.root / "pkg/sample/untracked.go").write_text("package sample\n", encoding="utf-8")
        self.assert_checker_fails("generate", "untracked package artifacts")

    def test_generate_twice_with_spec_inside_mapped_crate_owns_exact_proof_tree(self) -> None:
        target = self.root / "rust/crates/tidb-sample/lockdown"
        shutil.move(self.root / "evidence", target)
        scaffold = target / "semantic-clusters.md"
        scaffold.write_text("# Reviewed semantic clusters\n", encoding="utf-8")
        for path in target.rglob("*"):
            if path.is_file() and path.suffix in {".toml", ".tsv", ".json"}:
                path.write_text(
                    path.read_text(encoding="utf-8").replace(
                        "evidence/", "rust/crates/tidb-sample/lockdown/"
                    ),
                    encoding="utf-8",
                )
        spec = target / "package.toml"
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                '"rust/crates/tidb-sample/tests/sample_lockdown.rs"]',
                '"rust/crates/tidb-sample/tests/sample_lockdown.rs", '
                '"rust/crates/tidb-sample/lockdown/semantic-clusters.md"]',
            ),
            encoding="utf-8",
        )
        spec_path = Path("rust/crates/tidb-sample/lockdown/package.toml")
        accepted = self._spec_source_commit_for_path(self.root, spec_path)
        command = [
            sys.executable, str(CHECKER), "--root", str(self.root), "generate",
            "--spec", spec_path.as_posix(), "--accepted-source-commit", accepted,
        ]
        first = run(self.root, command)
        second = run(self.root, command)
        self.assertIn("generated pkg/sample", first.stdout)
        self.assertIn("generated pkg/sample", second.stdout)

    def test_generate_rejects_unknown_spec_field(self) -> None:
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                "excluded_subpackages = []", "typo_field = true\nexcluded_subpackages = []"
            ),
            encoding="utf-8",
        )
        self.assert_checker_fails("generate", "unknown fields")

    def test_cli_requires_coordinator_accepted_commit_and_rejects_candidate_self_sha(self) -> None:
        accepted = self._spec_source_commit(self.root)
        result = run(
            self.root,
            [
                sys.executable, str(CHECKER), "--root", str(self.root), "check",
                "--spec", SPEC.as_posix(),
            ],
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--accepted-source-commit", result.stderr)

        (self.root / "candidate.txt").write_text("candidate\n", encoding="utf-8")
        run(self.root, ["git", "add", "candidate.txt"])
        run(self.root, ["git", "commit", "-qm", "candidate self commit"])
        candidate = run(self.root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(accepted, candidate),
            encoding="utf-8",
        )
        result = self._checker(
            self.root, "check", check=False, accepted_source_commit=accepted
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("differs from coordinator-supplied", result.stderr)

    def test_generate_rejects_fixture_disguised_as_excluded_subpackage(self) -> None:
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                "excluded_subpackages = []",
                "excluded_subpackages = [{ path = \"pkg/sample/testdata\", "
                "proof = \"go-package-dir:pkg/sample/testdata#package:sample\" }]",
            ),
            encoding="utf-8",
        )
        self.assert_checker_fails("generate", "fixture/testdata subtree cannot be excluded")

    def test_generate_accepts_distinct_nested_package_with_same_identifier(self) -> None:
        nested = self.root / "pkg/sample/internal/nested/nested.go"
        nested.parent.mkdir(parents=True)
        nested.write_text("package sample\n\nfunc Nested() {}\n", encoding="utf-8")
        self.commit_source_update("pkg/sample/internal/nested/nested.go")
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                "excluded_subpackages = []",
                "excluded_subpackages = [{ path = \"pkg/sample/internal/nested\", "
                "proof = \"go-package-dir:pkg/sample/internal/nested#package:sample\" }]",
            ),
            encoding="utf-8",
        )
        result = self.checker("generate")
        self.assertIn("generated pkg/sample", result.stdout)

    def test_generate_fails_closed_on_embed_in_excluded_nested_package(self) -> None:
        nested = self.root / "pkg/sample/child/child.go"
        nested.parent.mkdir(parents=True)
        nested.write_text(
            "package child\n\nimport _ \"embed\"\n\n"
            "//go:embed data/value.txt\nvar value string\n",
            encoding="utf-8",
        )
        asset = self.root / "pkg/sample/child/data/value.txt"
        asset.parent.mkdir(parents=True)
        asset.write_text("value\n", encoding="utf-8")
        self.commit_source_update(
            "pkg/sample/child/child.go", "pkg/sample/child/data/value.txt"
        )
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                "excluded_subpackages = []",
                "excluded_subpackages = [{ path = \"pkg/sample/child\", "
                "proof = \"go-package-dir:pkg/sample/child#package:child\" }]",
            ),
            encoding="utf-8",
        )
        self.assert_checker_fails("generate", "outside schema v2's exact direct-file subset")

    def test_generate_accepts_direct_literal_embed_and_manifests_input(self) -> None:
        source = self.root / "pkg/sample/embed_test.go"
        source.write_text(
            "//go:build never\n\npackage sample\n\nimport _ \"embed\"\n\n"
            "//go:embed sample.go\nvar sampleSource string\n",
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/embed_test.go")
        self.checker("generate")
        _schema, _header, rows = read_tsv(self.root / "evidence/artifacts.tsv")
        production = next(row for row in rows if row["path"] == "pkg/sample/sample.go")
        directive = next(row for row in rows if row["path"] == "pkg/sample/embed_test.go")
        self.assertIn("go-embed-input", production["traits"])
        self.assertIn("go-embed", directive["traits"])

    def test_generate_rejects_unmanifested_go_generate_inputs(self) -> None:
        source = self.root / "pkg/sample/sample.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "package sample\n", "package sample\n\n//go:generate go run ../../tools/generator ../../tools/input.json\n"
            ),
            encoding="utf-8",
        )
        generator = self.root / "tools/generator/main.go"
        generator.parent.mkdir(parents=True)
        generator.write_text("package main\n\nfunc main() {}\n", encoding="utf-8")
        (self.root / "tools/input.json").write_text("{}\n", encoding="utf-8")
        self.commit_source_update(
            "pkg/sample/sample.go", "tools/generator/main.go", "tools/input.json"
        )
        self.assert_checker_fails("generate", "go:generate inputs are not manifested")

    def test_generate_fails_closed_on_shell_go_generate(self) -> None:
        source = self.root / "pkg/sample/sample.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "package sample\n", "package sample\n\n//go:generate sh -c echo-unresolvable\n"
            ),
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/sample.go")
        self.assert_checker_fails("generate", "directive is not statically resolvable")

    def test_generate_rejects_unknown_tracked_artifact_role(self) -> None:
        (self.root / "pkg/sample/notes.xyz").write_text("owned but unknown\n", encoding="utf-8")
        self.commit_source_update("pkg/sample/notes.xyz")
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

    def test_check_rejects_symbols_found_only_in_comments_and_strings(self) -> None:
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            anchor.read_text(encoding="utf-8")
            .replace("sample_symbol", "gone_symbol")
            .replace("test_sample_boundary", "gone_test")
            + '\n// sample_symbol test_sample_boundary\nconst DECOY: &str = "sample_symbol test_sample_boundary";\n',
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from compile anchor")

    def test_check_rejects_test_file_as_ported_definition(self) -> None:
        symbols = self.root / "evidence/symbols.tsv"
        schema, header, rows = read_tsv(symbols)
        rows[0]["definition_path"] = "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        write_tsv(symbols, schema, header, rows)
        self.assert_checker_fails("check", "no tracked owned production Rust definition")

    def test_check_rejects_production_usage_without_symbol_declaration(self) -> None:
        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        definition.write_text(
            "pub fn wrapper(value: i32) -> i32 { sample_symbol(value) }\n",
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from production definition")

    def test_check_rejects_same_leaf_declaration_in_wrong_module(self) -> None:
        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        definition.write_text(
            "mod decoy { pub fn sample_symbol(value: i32) -> i32 { value } }\n",
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from production definition")

    def test_check_binds_qualified_impl_target_not_same_leaf_type(self) -> None:
        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        definition.write_text(
            "mod actual { pub struct Thing; }\n"
            "impl actual::Thing { pub fn sample_symbol() {} }\n",
            encoding="utf-8",
        )
        symbols = self.root / "evidence/symbols.tsv"
        schema, header, rows = read_tsv(symbols)
        rows[0]["rust_symbol"] = "tidb_sample::decoy::Thing::sample_symbol"
        write_tsv(symbols, schema, header, rows)
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "#[test]\nfn test_sample_boundary() { "
            "tidb_sample::decoy::Thing::sample_symbol(); }\n",
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from production definition")

    def test_check_rejects_symbol_reference_outside_named_test_body(self) -> None:
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "fn helper() { let _ = tidb_sample::sample_symbol(3); }\n"
            "#[test]\nfn test_sample_boundary() {}\n",
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from compile anchor")

    def test_check_rejects_test_local_crate_symbol_and_bare_path_anchor(self) -> None:
        symbols = self.root / "evidence/symbols.tsv"
        schema, header, rows = read_tsv(symbols)
        rows[0]["rust_symbol"] = "crate::sample_symbol"
        write_tsv(symbols, schema, header, rows)
        self.assert_checker_fails("check", "test-local crate:: paths")

        rows[0]["rust_symbol"] = "tidb_sample::sample_symbol"
        write_tsv(symbols, schema, header, rows)
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "#[test]\nfn test_sample_boundary() { "
            "let _ = tidb_sample::sample_symbol; }\n",
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from compile anchor")

    def test_check_allows_assigned_executable_symbol_call(self) -> None:
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "#[test]\nfn test_sample_boundary() { "
            "let actual = tidb_sample::sample_symbol(3); assert_eq!(actual, 3); }\n",
            encoding="utf-8",
        )
        self.checker("write-receipt")
        self.checker("check")

    def test_check_rejects_mutation_of_unrelated_production_path_or_test(self) -> None:
        unrelated = self.root / "rust/crates/tidb-sample/src/unrelated.rs"
        unrelated.write_text("pub fn unrelated() -> i32 { 7 }\n", encoding="utf-8")
        run(self.root, ["git", "add", unrelated.relative_to(self.root).as_posix()])
        run(self.root, ["git", "commit", "-qm", "add unrelated candidate source"])
        baseline = run(self.root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                '"rust/crates/tidb-sample/tests/sample_lockdown.rs"]',
                '"rust/crates/tidb-sample/tests/sample_lockdown.rs", '
                '"rust/crates/tidb-sample/src/unrelated.rs"]',
            ),
            encoding="utf-8",
        )
        operator = self.root / "evidence/mutation-operators/MUT-UNRELATED.json"
        write_json(operator, {
            "schema": "go-package-lockdown-mutation-operator-v1",
            "mutation_id": "MUT-CLAMP",
            "rust_path": "rust/crates/tidb-sample/src/unrelated.rs",
            "source_sha256": sha256(unrelated),
            "replacements": [{"old": "{ 7 }", "new": "{ 8 }", "expected_count": 1}],
        })
        plan = self.root / "evidence/mutation-plan.tsv"
        plan_schema, plan_header, plans = read_tsv(plan)
        original_plan = plans[0].copy()
        plans[0].update(
            baseline_commit=baseline,
            rust_path="rust/crates/tidb-sample/src/unrelated.rs",
            source_sha256=sha256(unrelated),
            operator_path=operator.relative_to(self.root).as_posix(),
            operator_sha256=sha256(operator),
        )
        write_tsv(plan, plan_schema, plan_header, plans)
        self.assert_checker_fails("check", "not bound to the semantic rule")

        plans[0] = {
            **original_plan,
            "test_target": "unrelated_target",
            "named_test": "unrelated_test",
        }
        write_tsv(plan, plan_schema, plan_header, plans)
        self.assert_checker_fails("check", "not bound to the semantic rule")

    def test_check_rejects_conditional_test_attribute_decoy(self) -> None:
        anchor = self.root / "rust/crates/tidb-sample/tests/sample_lockdown.rs"
        anchor.write_text(
            "#[cfg_attr(feature = \"never\", test)]\n"
            "fn test_sample_boundary() { let _ = tidb_sample::sample_symbol(3); }\n",
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "disappeared from compile anchor")

    def test_check_rejects_unowned_changed_mapped_crate_file(self) -> None:
        helper = self.root / "rust/crates/tidb-sample/src/helper.rs"
        helper.write_text("pub fn omitted_helper() {}\n", encoding="utf-8")
        run(self.root, ["git", "add", helper.relative_to(self.root).as_posix()])
        run(self.root, ["git", "commit", "-qm", "unowned Rust helper"])
        self.assert_checker_fails("check", "mapped Rust change census is not exact")

    def test_fixed_runner_uses_rust_cwd_and_run_verify_restore_source(self) -> None:
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        completed = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=b"running 1 test\ntest test_sample_boundary ... ok\n", stderr=b"",
        )
        with mock.patch.object(LOCKDOWN.subprocess, "run", return_value=completed) as invoked:
            argv, _result = lockdown._run_fixed_test(
                "cargo-test", "tidb-sample", "sample_lockdown", "test_sample_boundary",
                "test runner regression",
            )
        self.assertEqual(invoked.call_args.kwargs["cwd"], (self.root / "rust").resolve())
        self.assertEqual(argv[:6], ["cargo", "test", "--offline", "--locked", "-j12", "--quiet"])

        self.clear_mutation_execution()
        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        original = definition.read_bytes()
        failure = subprocess.CompletedProcess(
            args=[], returncode=101,
            stdout=b"running 1 test\ntest test_sample_boundary ... FAILED\n", stderr=b"",
        )
        with mock.patch.object(
            lockdown, "_run_fixed_test",
            side_effect=[
                (argv, completed), (argv, failure), (argv, completed),
                (argv, completed), (argv, failure), (argv, completed),
            ],
        ):
            self.assertEqual(
                lockdown.run_evidence("mutation", "MUT-CLAMP", "ATTEMPT-FIXED"), "KILLED"
            )
            self.assertEqual(definition.read_bytes(), original)
            self.assertEqual(
                lockdown.verify_evidence("mutation", "MUT-CLAMP", "ATTEMPT-FIXED"), "KILLED"
            )
        self.assertEqual(definition.read_bytes(), original)
        _schema, _header, rows = read_tsv(self.root / "evidence/mutation-results.tsv")
        self.assertNotEqual(rows[0]["verification_artifact_path"], "-")

    def test_run_evidence_rejects_compilation_only_kill_and_restores_source(self) -> None:
        self.clear_mutation_execution()
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        original = definition.read_bytes()
        compile_failure = subprocess.CompletedProcess(
            args=[], returncode=101, stdout=b"", stderr=b"could not compile test_sample_boundary",
        )
        passing = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=b"running 1 test\ntest test_sample_boundary ... ok\n", stderr=b"",
        )
        with mock.patch.object(
            lockdown,
            "_run_fixed_test",
            side_effect=[
                (lockdown._runner_argv(
                    "cargo-test", "tidb-sample", "sample_lockdown", "test_sample_boundary",
                    "compile failure",
                ), passing),
                (lockdown._runner_argv(
                    "cargo-test", "tidb-sample", "sample_lockdown", "test_sample_boundary",
                    "compile failure",
                ), compile_failure),
                (lockdown._runner_argv(
                    "cargo-test", "tidb-sample", "sample_lockdown", "test_sample_boundary",
                    "compile failure",
                ), passing),
            ],
        ):
            with self.assertRaisesRegex(LOCKDOWN.LockdownError, "does not prove exact named test"):
                lockdown.run_evidence("mutation", "MUT-CLAMP", "ATTEMPT-COMPILE")
        self.assertEqual(definition.read_bytes(), original)
        _schema, _header, rows = read_tsv(self.root / "evidence/mutation-results.tsv")
        self.assertEqual(rows, [])

    def test_run_evidence_rejects_preexisting_baseline_failure(self) -> None:
        self.clear_mutation_execution()
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        original = definition.read_bytes()
        argv = lockdown._runner_argv(
            "cargo-test", "tidb-sample", "sample_lockdown", "test_sample_boundary",
            "preexisting failure",
        )
        failure = subprocess.CompletedProcess(
            args=[], returncode=101,
            stdout=b"running 1 test\ntest test_sample_boundary ... FAILED\n", stderr=b"",
        )
        with mock.patch.object(lockdown, "_run_fixed_test", return_value=(argv, failure)):
            with self.assertRaisesRegex(LOCKDOWN.LockdownError, "baseline test did not pass"):
                lockdown.run_evidence("mutation", "MUT-CLAMP", "ATTEMPT-PREFAIL")
        self.assertEqual(definition.read_bytes(), original)
        _schema, _header, rows = read_tsv(self.root / "evidence/mutation-results.tsv")
        self.assertEqual(rows, [])

    def test_historical_survivor_and_current_source_kill_are_both_preserved(self) -> None:
        self.clear_mutation_execution()
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        argv = lockdown._runner_argv(
            "cargo-test", "tidb-sample", "sample_lockdown", "test_sample_boundary",
            "history regression",
        )
        survivor = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=b"running 1 test\ntest test_sample_boundary ... ok\n", stderr=b"",
        )
        with mock.patch.object(lockdown, "_run_fixed_test", return_value=(argv, survivor)):
            self.assertEqual(
                lockdown.run_evidence("mutation", "MUT-CLAMP", "ATTEMPT-V1"), "SURVIVED"
            )
            self.assertEqual(
                lockdown.verify_evidence("mutation", "MUT-CLAMP", "ATTEMPT-V1"), "SURVIVED"
            )

        run(self.root, [
            "git", "add", "evidence/mutation-results.tsv", "evidence/execution/mutation",
        ])
        run(self.root, ["git", "commit", "-qm", "immutable survivor history checkpoint"])

        definition = self.root / "rust/crates/tidb-sample/src/lib.rs"
        definition.write_text(
            definition.read_text(encoding="utf-8").replace(
                "{ value }", "{ value.saturating_add(0) }"
            ),
            encoding="utf-8",
        )
        run(self.root, ["git", "add", definition.relative_to(self.root).as_posix()])
        run(self.root, ["git", "commit", "-qm", "production mutation fix"])
        baseline_v2 = run(self.root, ["git", "rev-parse", "HEAD"]).stdout.strip()
        source_hash_v2 = sha256(definition)
        operator_v2 = self.root / "evidence/mutation-operators/MUT-CLAMP-v2.json"
        write_json(operator_v2, {
            "schema": "go-package-lockdown-mutation-operator-v1",
            "mutation_id": "MUT-CLAMP",
            "rust_path": "rust/crates/tidb-sample/src/lib.rs",
            "source_sha256": source_hash_v2,
            "replacements": [{
                "old": "{ value.saturating_add(0) }",
                "new": "{ value.saturating_add(1) }",
                "expected_count": 1,
            }],
        })
        plan_path = self.root / "evidence/mutation-plan.tsv"
        schema, header, plans = read_tsv(plan_path)
        plans[0].update(
            baseline_commit=baseline_v2,
            source_sha256=source_hash_v2,
            operator_path=operator_v2.relative_to(self.root).as_posix(),
            operator_sha256=sha256(operator_v2),
        )
        write_tsv(plan_path, schema, header, plans)

        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        killed = subprocess.CompletedProcess(
            args=[], returncode=101,
            stdout=b"running 1 test\ntest test_sample_boundary ... FAILED\n", stderr=b"",
        )
        with mock.patch.object(
            lockdown, "_run_fixed_test",
            side_effect=[
                (argv, survivor), (argv, killed), (argv, survivor),
                (argv, survivor), (argv, killed), (argv, survivor),
            ],
        ):
            self.assertEqual(
                lockdown.run_evidence("mutation", "MUT-CLAMP", "ATTEMPT-V2"), "KILLED"
            )
            self.assertEqual(
                lockdown.verify_evidence("mutation", "MUT-CLAMP", "ATTEMPT-V2"), "KILLED"
            )
        self.checker("write-receipt")
        self.checker("check")
        receipt = json.loads((self.root / "evidence/receipt.json").read_text(encoding="utf-8"))
        self.assertEqual(receipt["mutation_attempt_count"], 2)
        self.assertEqual(receipt["mutation_outcome_counts"], {"KILLED": 1, "SURVIVED": 1})

    def test_check_rejects_weak_decline_and_unreachable_evidence(self) -> None:
        ledger = self.root / "evidence/ledgers/sample.go.tsv"
        schema, header, rows = read_tsv(ledger)
        original = [row.copy() for row in rows]
        declined = next(row for row in rows if row["status"] == "DECLINED")
        declined["evidence"] = "handwave"
        write_tsv(ledger, schema, header, rows)
        self.assert_checker_fails("check", "measured-probe obligation")
        unreachable = next(row for row in original if row["status"] == "UNREACHABLE")
        unreachable["evidence"] = "go-quote:wrong;structural-proof:handwave"
        write_tsv(ledger, schema, header, original)
        self.assert_checker_fails("check", "structural-proof obligation")

    def test_check_rejects_rule_mutation_omission(self) -> None:
        plan = self.root / "evidence/mutation-plan.tsv"
        schema, header, _rows = read_tsv(plan)
        write_tsv(plan, schema, header, [])
        self.assert_checker_fails("check", "mutation plan contains missing")

    def test_check_rejects_content_addressed_but_fabricated_mutation_binding(self) -> None:
        run_path = self.root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.run.json"
        payload = json.loads(run_path.read_text(encoding="utf-8"))
        payload["mutated_source_sha256"] = "0" * 64
        write_json(run_path, payload)
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, rows = read_tsv(results)
        rows[0]["run_artifact_sha256"] = sha256(run_path)
        self.rehash_mutation_history(rows)
        write_tsv(results, schema, header, rows)
        self.assert_checker_fails("check", "does not bind mutated_source_sha256")

    def test_check_rejects_duplicate_evidence_boundaries(self) -> None:
        plan_path = self.root / "evidence/probes/PROBE-DECLINED.json"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        plan["boundary_cases"] = ["same-boundary", "same-boundary"]
        write_json(plan_path, plan)
        new_hash = sha256(plan_path)
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            schema, header, rows = read_tsv(ledger)
            for row in rows:
                if row["status"] == "DECLINED":
                    row["evidence"] = re.sub(
                        r"@sha256:[0-9a-f]{64}$", f"@sha256:{new_hash}", row["evidence"]
                    )
            write_tsv(ledger, schema, header, rows)
        self.assert_checker_fails("check", "lacks two distinct boundary cases")

    def test_check_records_survivor_but_requires_final_kill(self) -> None:
        success_stdout = b"running 1 test\ntest test_sample_boundary ... ok\n"
        observation = LOCKDOWN.normalized_test_observation(
            "cargo-test", "test_sample_boundary", 0, success_stdout, b""
        )
        run_path = self.root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.run.json"
        run_log = run_path.with_suffix(".log")
        run_log.write_bytes(command_log(success_stdout))
        run_payload = json.loads(run_path.read_text(encoding="utf-8"))
        run_payload.update(
            exit_code=0,
            outcome="SURVIVED",
            normalized_observation_sha256=observation,
            output_sha256=sha256(run_log),
        )
        write_json(run_path, run_payload)
        verify_path = self.root / "evidence/execution/mutation/ATTEMPT-CLAMP-1.verify.json"
        verify_log = verify_path.with_suffix(".log")
        verify_log.write_bytes(command_log(success_stdout))
        verify_payload = json.loads(verify_path.read_text(encoding="utf-8"))
        verify_payload.update(
            exit_code=0,
            outcome="SURVIVED",
            normalized_observation_sha256=observation,
            output_sha256=sha256(verify_log),
            prior_artifact_sha256=sha256(run_path),
        )
        write_json(verify_path, verify_payload)
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, rows = read_tsv(results)
        rows[0]["run_artifact_sha256"] = sha256(run_path)
        rows[0]["verification_artifact_sha256"] = sha256(verify_path)
        self.rehash_mutation_history(rows)
        write_tsv(results, schema, header, rows)
        self.assert_checker_fails("check", "does not end with a killed attempt")

    def test_check_rejects_receipt_drift(self) -> None:
        receipt_path = self.root / "evidence/receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["obligation_count"] += 1
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.assert_checker_fails("check", "not exact canonical JSON")

    def test_check_rejects_noncanonical_reordered_and_duplicate_receipt_json(self) -> None:
        receipt_path = self.root / "evidence/receipt.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.assert_checker_fails("check", "not exact canonical JSON")

        reversed_receipt = dict(reversed(list(receipt.items())))
        receipt_path.write_text(
            json.dumps(reversed_receipt, separators=(",", ":")) + "\n", encoding="utf-8"
        )
        self.assert_checker_fails("check", "not exact canonical JSON")

        raw = LOCKDOWN.canonical_json_text(receipt)
        receipt_path.write_text(
            raw.replace("{", '{"schema":"duplicate","schema":"duplicate",', 1),
            encoding="utf-8",
        )
        self.assert_checker_fails("check", "duplicate JSON key")

    def test_content_addressed_artifacts_reject_duplicate_json_keys(self) -> None:
        operator = self.root / "evidence/mutation-operators/MUT-CLAMP.json"
        raw = operator.read_text(encoding="utf-8")
        operator.write_text(raw.replace("{", '{"schema":"duplicate",', 1), encoding="utf-8")
        plan = self.root / "evidence/mutation-plan.tsv"
        schema, header, rows = read_tsv(plan)
        rows[0]["operator_sha256"] = sha256(operator)
        write_tsv(plan, schema, header, rows)
        self.assert_checker_fails("check", "duplicate JSON key")

    def test_check_rejects_deleting_committed_mutation_history(self) -> None:
        accepted = self.commit_accepted_history_baseline()
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, _rows = read_tsv(results)
        write_tsv(results, schema, header, [])
        run(self.root, ["git", "add", "evidence"])
        run(self.root, ["git", "commit", "-qm", "candidate deletes history"])
        lockdown = LOCKDOWN.PackageLockdown(self.root, SPEC, accepted)
        with self.assertRaisesRegex(
            LOCKDOWN.LockdownError, "deleted, reordered, or rewrote"
        ):
            lockdown._validate_mutation_history([])

    def test_check_rejects_reordered_committed_mutation_history(self) -> None:
        accepted = self.commit_accepted_history_baseline()
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, rows = read_tsv(results)
        second = rows[0].copy()
        second["attempt_id"] = "ATTEMPT-CLAMP-2"
        second["prior_checkpoint_sha256"] = hashlib.sha256(
            run(self.root, ["git", "show", f"{accepted}:evidence/mutation-results.tsv"]).stdout.encode()
        ).hexdigest()
        rows.append(second)
        self.rehash_mutation_history(rows)
        rows.reverse()
        self.rehash_mutation_history(rows)
        write_tsv(results, schema, header, rows)
        run(self.root, ["git", "add", "evidence"])
        run(self.root, ["git", "commit", "-qm", "candidate reorders history"])
        lockdown = LOCKDOWN.PackageLockdown(self.root, SPEC, accepted)
        with self.assertRaisesRegex(
            LOCKDOWN.LockdownError, "deleted, reordered, or rewrote"
        ):
            lockdown._validate_mutation_history(rows)

    def test_check_rejects_rewriting_committed_mutation_history(self) -> None:
        accepted = self.commit_accepted_history_baseline()
        results = self.root / "evidence/mutation-results.tsv"
        schema, header, rows = read_tsv(results)
        rows[0]["attempt_id"] = "ATTEMPT-REWRITTEN"
        self.rehash_mutation_history(rows)
        write_tsv(results, schema, header, rows)
        run(self.root, ["git", "add", "evidence"])
        run(self.root, ["git", "commit", "-qm", "candidate rewrites history"])
        lockdown = LOCKDOWN.PackageLockdown(self.root, SPEC, accepted)
        with self.assertRaisesRegex(
            LOCKDOWN.LockdownError, "deleted, reordered, or rewrote"
        ):
            lockdown._validate_mutation_history(rows)

    def test_generate_rejects_unmanifested_external_fixture(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace("testdata/cases.txt", "../shared.txt"),
            encoding="utf-8",
        )
        (self.root / "pkg/shared.txt").write_text("outside package\n", encoding="utf-8")
        self.commit_source_update("pkg/sample/sample_test.go", "pkg/shared.txt")
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
        self.commit_source_update("pkg/sample/sample_test.go")
        self.assert_checker_fails("generate", "unresolved fixture evidence is not exact")

    def test_generate_rejects_arbitrary_measured_fixture_prose(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "if _, err := os.ReadFile(\"testdata/cases.txt\")",
                "fixturePath := \"testdata/cases.txt\"\n\tif _, err := os.ReadFile(fixturePath)",
            ),
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/sample_test.go")
        first = self.checker("generate", check=False)
        discovered = re.search(r"discovered=(\[[^\]]+\])", first.stderr)
        self.assertIsNotNone(discovered, first.stderr)
        keys = ast.literal_eval(discovered.group(1))
        self.assertEqual(len(keys), 1)
        spec = self.root / SPEC
        spec.write_text(
            spec.read_text(encoding="utf-8").replace(
                "[unresolved_fixture_evidence]\n",
                f"[unresolved_fixture_evidence]\n{json.dumps(keys[0])} = \"measured:handwave\"\n",
            ),
            encoding="utf-8",
        )
        self.assert_checker_fails("generate", "lacks a content-addressed evidence artifact")

    def test_generate_fails_closed_on_helper_mediated_fixture_calls(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8")
            + "\nfunc TestFixtureHelper(t *testing.T) {\n"
            + "\ttestDataMap.LoadTestSuiteData()\n"
            + "\ttestDataMap.GenerateOutputIfNeeded()\n"
            + "}\n",
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/sample_test.go")
        self.checker("generate")
        _schema, _header, contracts = read_tsv(self.root / "evidence/helper-contracts.tsv")
        helper_rows = [row for row in contracts if row["callee"].startswith("testDataMap.")]
        self.assertEqual(
            {row["callee"] for row in helper_rows},
            {"testDataMap.LoadTestSuiteData", "testDataMap.GenerateOutputIfNeeded"},
        )
        self.assertTrue(all(row["status"] == "UNCLASSIFIED" for row in helper_rows))
        # The source-commit-bound prior helper proofs also invalidate, so either
        # they or the new UNCLASSIFIED rows close the package rather than let a
        # helper-mediated fixture access pass silently.
        self.assert_checker_fails("check", "helper")

    def test_helper_inventory_does_not_alias_local_readfile_to_os_fixture(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8")
            + "\ntype localReader struct{}\n\n"
            + "func (localReader) ReadFile(string) {}\n\n"
            + "func localReadFileProbe() {\n"
            + "\tvar local localReader\n"
            + "\tlocal.ReadFile(\"testdata/cases.txt\")\n"
            + "}\n",
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/sample_test.go")
        self.checker("generate")
        _schema, _header, calls = read_tsv(self.root / "evidence/helper-calls.tsv")
        local = [row for row in calls if row["callee"] == "local.ReadFile"]
        direct = [row for row in calls if row["callee"] == "os.ReadFile"]
        self.assertEqual(len(local), 1)
        self.assertEqual(len(direct), 1)
        self.assertEqual(local[0]["fixture_api"], "-")
        self.assertEqual(direct[0]["fixture_api"], "os.ReadFile")

    def test_check_rejects_probe_logs_without_machine_observation(self) -> None:
        result_path = self.root / "evidence/probe-results.tsv"
        schema, header, rows = read_tsv(result_path)
        row = rows[0]
        run_path = self.root / row["run_artifact_path"]
        run_payload = json.loads(run_path.read_text(encoding="utf-8"))
        run_log = self.root / run_payload["output_path"]
        run_log.write_bytes(b"\n".join(
            line for line in run_log.read_bytes().splitlines()
            if not line.startswith(b"LOCKDOWN_OBSERVATION ")
        ) + b"\n")
        run_stdout, run_stderr = LOCKDOWN.parse_command_output(
            run_log.read_bytes(), "probe run regression"
        )
        run_payload["output_sha256"] = sha256(run_log)
        run_payload["normalized_observation_sha256"] = LOCKDOWN.normalized_test_observation(
            "go-test", "TestClamp", 0, run_stdout, run_stderr
        )
        write_json(run_path, run_payload)

        verify_path = self.root / row["verification_artifact_path"]
        verify_payload = json.loads(verify_path.read_text(encoding="utf-8"))
        verify_log = self.root / verify_payload["output_path"]
        verify_log.write_bytes(b"\n".join(
            line for line in verify_log.read_bytes().splitlines()
            if not line.startswith(b"LOCKDOWN_OBSERVATION ")
        ) + b"\n")
        verify_stdout, verify_stderr = LOCKDOWN.parse_command_output(
            verify_log.read_bytes(), "probe verify regression"
        )
        verify_payload["output_sha256"] = sha256(verify_log)
        verify_payload["normalized_observation_sha256"] = LOCKDOWN.normalized_test_observation(
            "go-test", "TestClamp", 0, verify_stdout, verify_stderr
        )
        verify_payload["prior_artifact_sha256"] = sha256(run_path)
        write_json(verify_path, verify_payload)
        row["run_artifact_sha256"] = sha256(run_path)
        row["verification_artifact_sha256"] = sha256(verify_path)
        write_tsv(result_path, schema, header, rows)
        self.assert_checker_fails("check", "did not emit its exact machine-readable observation")

    def test_check_rejects_missing_or_wrong_runtime_boundary_observations(self) -> None:
        self.rewrite_recorded_probe_observation(
            lambda runtime: runtime["boundary_observations"].pop()
        )
        self.assert_checker_fails("check", "differ from evidence")

    def test_check_rejects_wrong_runtime_observed_value(self) -> None:
        self.rewrite_recorded_probe_observation(
            lambda runtime: runtime["boundary_observations"][0].update(observed="wrong")
        )
        self.assert_checker_fails("check", "differ from evidence")

    def test_probe_rejects_named_test_hardcoded_expected_json_literal(self) -> None:
        source = self.root / "pkg/sample/sample_test.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace(
                "func TestClamp(t *testing.T) {",
                "func TestClamp(t *testing.T) {\n"
                "\tt.Log(`LOCKDOWN_OBSERVATION {\"boundary_observations\":[]}`)",
            ),
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/sample_test.go")
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        lockdown.artifact_rows()
        plan = json.loads(
            (self.root / "evidence/probes/PROBE-DECLINED.json").read_text(encoding="utf-8")
        )
        with self.assertRaisesRegex(LOCKDOWN.LockdownError, "hardcodes a machine observation"):
            lockdown._validate_observation_test_source(plan, "hardcoded observation regression")

    def test_probe_run_and_verify_execute_exact_machine_observation(self) -> None:
        self.clear_probe_execution()
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        _plan_path, plan = lockdown._probe_plans_for_execution()["PROBE-DECLINED"]
        marker = runtime_observation_line(
            self.root / str(plan["observation_path"])
        )
        run_result = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=(
                b"=== RUN   TestClamp\n" + marker
                + b"\n--- PASS: TestClamp (0.00s)\n"
                + b"PASS\nok  example.com/lockdown/pkg/sample  0.01s\n"
            ),
            stderr=b"",
        )
        verify_result = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=run_result.stdout.replace(b"0.01s", b"0.02s"), stderr=b"",
        )
        argv = lockdown._runner_argv(
            "go-test", "pkg/sample", "-", "TestClamp", "probe runner regression"
        )
        with mock.patch.object(
            lockdown, "_run_fixed_test",
            side_effect=[(argv, run_result), (argv, verify_result)],
        ):
            self.assertEqual(lockdown.run_evidence("probe", "PROBE-DECLINED", None), "OBSERVED")
            self.assertEqual(lockdown.verify_evidence("probe", "PROBE-DECLINED", None), "VERIFIED")
        _schema, _header, rows = read_tsv(self.root / "evidence/probe-results.tsv")
        self.assertEqual(len(rows), 1)
        self.assertNotEqual(rows[0]["verification_artifact_path"], "-")

    def test_probe_run_rejects_passing_test_without_observation_marker(self) -> None:
        self.clear_probe_execution()
        lockdown = LOCKDOWN.PackageLockdown(
            self.root, SPEC, self._spec_source_commit(self.root)
        )
        passing = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=(
                b"=== RUN   TestClamp\n--- PASS: TestClamp (0.00s)\n"
                b"PASS\nok  example.com/lockdown/pkg/sample  0.01s\n"
            ),
            stderr=b"",
        )
        argv = lockdown._runner_argv(
            "go-test", "pkg/sample", "-", "TestClamp", "probe marker regression"
        )
        with mock.patch.object(lockdown, "_run_fixed_test", return_value=(argv, passing)):
            with self.assertRaisesRegex(
                LOCKDOWN.LockdownError, "did not emit its exact machine-readable observation"
            ):
                lockdown.run_evidence("probe", "PROBE-DECLINED", None)
        _schema, _header, rows = read_tsv(self.root / "evidence/probe-results.tsv")
        self.assertEqual(rows, [])

    def test_generate_invalidates_verdicts_for_straight_line_body_drift(self) -> None:
        before: dict[str, tuple[str, str, str, str, str]] = {}
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            _schema, _header, rows = read_tsv(ledger)
            before.update({
                row["obligation_id"]: (
                    row["source_path"], row["status"], row["symbol_id"],
                    row["evidence"], row["rule_id"]
                ) for row in rows
            })
        source = self.root / "pkg/sample/sample.go"
        source.write_text(
            source.read_text(encoding="utf-8").replace("\treturn value\n", "\treturn value + 0\n"),
            encoding="utf-8",
        )
        self.commit_source_update("pkg/sample/sample.go")
        self.checker("generate")
        after: dict[str, tuple[str, str, str, str, str]] = {}
        for ledger in (self.root / "evidence/ledgers").glob("*.tsv"):
            _schema, _header, rows = read_tsv(ledger)
            after.update({
                row["obligation_id"]: (
                    row["source_path"], row["status"], row["symbol_id"],
                    row["evidence"], row["rule_id"]
                ) for row in rows
            })
        for identity, verdict in before.items():
            if verdict[0] == "pkg/sample/sample.go":
                self.assertEqual(after[identity][1:], ("UNCLASSIFIED", "", "", ""))
            else:
                self.assertEqual(after[identity], verdict)
        self.assertTrue(any(verdict[1] == "UNCLASSIFIED" for verdict in after.values()))

    def test_generate_refuses_to_discard_removed_obligations(self) -> None:
        ledger = self.root / "evidence/ledgers/sample.go.tsv"
        before = ledger.read_bytes()
        source = self.root / "pkg/sample/sample.go"
        source.write_text("package sample\n\nconst Limit = 3\n", encoding="utf-8")
        self.commit_source_update("pkg/sample/sample.go")
        self.assert_checker_fails("generate", "would discard classified obligations")
        self.assertEqual(ledger.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
