#!/usr/bin/env python3
"""Check the exact file-lockdown seed for pkg/executor/distsql.go."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


GO_SOURCE = Path("pkg/executor/distsql.go")
GO_DIRECT_TEST = Path("pkg/executor/distsql_test.go")
GO_REQUIRED_ROWS_TEST = Path("pkg/executor/table_readers_required_rows_test.go")
GO_BATCH_TEST = Path("pkg/executor/test/issuetest/executor_issue_test.go")
GO_EXEC_METRIC_TEST = Path("pkg/executor/internal/exec/executor_test.go")
GO_DOUBLE_READ_CLOSE_TEST = Path("pkg/executor/test/seqtest/seq_executor_test.go")
ARTIFACT_PATHS = {
    GO_SOURCE: "production-owner",
    GO_DIRECT_TEST: "direct-test-support",
    GO_REQUIRED_ROWS_TEST: "direct-IndexReaderExecutor-test-support",
    GO_BATCH_TEST: "direct-TestCalculateBatchSize-support",
    GO_EXEC_METRIC_TEST: "direct-executor-type-label-support",
    GO_DOUBLE_READ_CLOSE_TEST: "direct-LookupTableTaskChannelSize-support",
}

DATA_DIR = Path("rust/crates/tidb-executor/tests/distsql_lockdown")
ARTIFACTS = DATA_DIR / "artifacts.tsv"
INVENTORY = DATA_DIR / "inventory.tsv"
MUTATION_PLAN = DATA_DIR / "mutation-plan.tsv"
MUTATION_RESULTS = DATA_DIR / "mutation-results.tsv"
RECEIPT = DATA_DIR / "receipt.json"
RUST_GATE = Path("rust/crates/tidb-executor/tests/distsql_lockdown.rs")
SCRIPT = Path("rust/scripts/pkg-executor-distsql-lockdown.py")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")

SOURCE_SEED_COMMIT = "842867801eaddcffc25e4de15aabb391f02b1968"
CLAIM_BOUNDARY = "file-lockdown-seed-not-package-completion"
EXPECTED_SOURCE_COUNTS = {
    str(GO_SOURCE): 904,
    str(GO_DIRECT_TEST): 231,
    str(GO_REQUIRED_ROWS_TEST): 192,
    str(GO_BATCH_TEST): 8,
    str(GO_EXEC_METRIC_TEST): 116,
    str(GO_DOUBLE_READ_CLOSE_TEST): 10,
}
EXPECTED_CATEGORIES = {
    "branch": 464,
    "closure": 21,
    "const": 2,
    "declaration": 16,
    "field": 153,
    "function": 67,
    "loop": 94,
    "select_case": 11,
    "short_circuit": 74,
    "switch_case": 3,
    "var": 4,
    "test": 18,
    "test_assertion": 102,
    "test_branch": 34,
    "test_helper": 14,
    "test_helper_closure": 20,
    "test_loop": 64,
    "test_row": 285,
    "test_short_circuit": 4,
    "test_support_declaration": 3,
    "test_support_var": 2,
    "test_switch_case": 6,
}
EXPECTED_OBLIGATIONS = 1461
EXPECTED_PRODUCTION_OBLIGATIONS = 904
EXPECTED_DIRECT_TEST_OBLIGATIONS = 557
EXPECTED_MUTATION_SUITES = 8
EXPECTED_MUTATIONS = 9


def run(root: Path, command: list[str]) -> str:
    completed = subprocess.run(
        command,
        cwd=root,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def artifact_lines(root: Path) -> list[str]:
    lines = [
        "# executor-distsql-file-lockdown-artifacts-v1",
        "path\trole\tsha256",
    ]
    for path, role in ARTIFACT_PATHS.items():
        full_path = root / path
        if not full_path.is_file():
            raise RuntimeError(f"owned Go artifact is not a file: {path}")
        lines.append(f"{path}\t{role}\t{sha256(full_path)}")
    return lines


def inventory_output(root: Path, package: str, sources: list[Path]) -> list[list[str]]:
    with tempfile.TemporaryDirectory(prefix="executor-distsql-lockdown-") as temp:
        temp_root = Path(temp)
        for source in sources:
            destination = temp_root / source
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / source, destination)
        output = run(
            root,
            [
                "go",
                "run",
                f"./{GO_TOOL}",
                "--root",
                str(temp_root),
                "--package",
                package,
            ],
        )
    lines = output.rstrip("\n").splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise RuntimeError(f"Go inventory returned no header for {package}")
    rows = [line.split("\t") for line in lines[1:]]
    if any(len(row) != 6 for row in rows):
        raise RuntimeError(f"invalid Go inventory row for {package}")
    return rows


def raw_obligations(root: Path) -> list[list[str]]:
    rows = inventory_output(
        root,
        "pkg/executor",
        [GO_SOURCE, GO_DIRECT_TEST, GO_REQUIRED_ROWS_TEST],
    )
    batch_rows = inventory_output(
        root,
        "pkg/executor/test/issuetest",
        [GO_BATCH_TEST],
    )
    rows.extend(row for row in batch_rows if row[5] == "TestCalculateBatchSize")
    metric_rows = inventory_output(
        root,
        "pkg/executor/internal/exec",
        [GO_EXEC_METRIC_TEST],
    )
    rows.extend(
        row
        for row in metric_rows
        if row[5] == "TestRUV2ExecutorMetricByTypeIncludesConcreteExecutorTypes"
    )
    close_rows = inventory_output(
        root,
        "pkg/executor/test/seqtest",
        [GO_DOUBLE_READ_CLOSE_TEST],
    )
    rows.extend(row for row in close_rows if row[5] == "TestIndexDoubleReadClose")
    rows.sort(key=lambda row: (row[2], row[3], row[1]))

    ids: set[str] = set()
    anchors: set[tuple[str, str, str, str]] = set()
    for row in rows:
        identity = row[0]
        anchor = (row[2], row[1], row[3], row[4])
        if identity in ids:
            raise RuntimeError(f"duplicate obligation id: {identity}")
        if anchor in anchors:
            raise RuntimeError(f"duplicate obligation anchor: {anchor}")
        ids.add(identity)
        anchors.add(anchor)
    return rows


def reason_for(source: str, owner: str) -> str:
    if source == str(GO_SOURCE):
        return (
            f"Go owner {owner} has no exact compiled Rust distributed-executor "
            "owner at this accepted seed"
        )
    return (
        f"Go direct test/support owner {owner} has no exact Rust counterpart "
        "because its production surface is declined"
    )


def classified_inventory_lines(raw: list[list[str]]) -> list[str]:
    lines = [
        "# executor-distsql-file-lockdown-inventory-v1",
        (
            "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
            "\tstatus\trust_symbol\tevidence\treason\tmutation_policy"
        ),
    ]
    for obligation_id, category, source, anchor, node_hash, owner in raw:
        evidence = f"go-ast-quote:{source}#{anchor}@sha256:{node_hash}"
        policy = (
            "behavior-mutation-required-before-PORTED"
            if source == str(GO_SOURCE)
            else "test-parity-mutation-required-before-PORTED"
        )
        fields = [
            obligation_id,
            category,
            source,
            anchor,
            node_hash,
            owner,
            "DECLINED",
            "-",
            evidence,
            reason_for(source, owner),
            policy,
        ]
        lines.append("\t".join(fields))
    return lines


def data_rows(path: Path) -> list[dict[str, str]]:
    lines = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_source_evidence(
    root: Path,
    row: dict[str, str],
    source_field: str,
    hash_field: str,
    kind: str,
) -> None:
    sources = row[source_field].split("|")
    hashes = row[hash_field].split("|")
    if len(sources) != len(hashes):
        raise RuntimeError(f"{kind} source/hash width drift: {row}")
    for source, expected_hash in zip(sources, hashes):
        path = root / source
        if not source or not path.is_file():
            raise RuntimeError(f"{kind} source is not a file: {source}")
        if not expected_hash or sha256(path) != expected_hash:
            raise RuntimeError(f"{kind} source drifted: {source}")


def validate_inventory(root: Path, stored: list[str], raw: list[list[str]]) -> None:
    expected = classified_inventory_lines(raw)
    if stored != expected:
        raise RuntimeError("distsql AST obligations, verdicts, or evidence drifted")
    rows = [line.split("\t") for line in stored[2:]]
    if len(rows) != EXPECTED_OBLIGATIONS or any(len(row) != 11 for row in rows):
        raise RuntimeError(f"inventory width/count drift: {len(rows)}")

    source_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for row in rows:
        source_counts[row[2]] = source_counts.get(row[2], 0) + 1
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        status_counts[row[6]] = status_counts.get(row[6], 0) + 1
        if row[6] == "PORTED":
            raise RuntimeError(f"PORTED row lacks a compiled-symbol allowlist entry: {row}")
        if row[6] not in {"DECLINED", "UNREACHABLE"}:
            raise RuntimeError(f"invalid or blank verdict: {row}")
        if row[6] == "DECLINED" and row[7] != "-":
            raise RuntimeError(f"DECLINED row claims a Rust symbol: {row}")
        expected_evidence = f"go-ast-quote:{row[2]}#{row[3]}@sha256:{row[4]}"
        if row[8] != expected_evidence or not row[9]:
            raise RuntimeError(f"source quote or reason drift: {row}")
    if source_counts != EXPECTED_SOURCE_COUNTS:
        raise RuntimeError(f"source census drift: {source_counts}")
    if category_counts != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {category_counts}")
    if status_counts != {"DECLINED": EXPECTED_OBLIGATIONS}:
        raise RuntimeError(f"status census drift: {status_counts}")


def validate_mutations(root: Path) -> None:
    plan = data_rows(root / MUTATION_PLAN)
    results = data_rows(root / MUTATION_RESULTS)
    if len(plan) != EXPECTED_MUTATION_SUITES:
        raise RuntimeError(f"mutation plan suite drift: {len(plan)}")
    if len(results) != EXPECTED_MUTATIONS:
        raise RuntimeError(f"mutation result count drift: {len(results)}")
    expected_counts = {
        row["suite_id"]: int(row["mutation_count"])
        for row in plan
    }
    if len(expected_counts) != len(plan):
        raise RuntimeError("duplicate mutation suite")
    if sum(expected_counts.values()) != EXPECTED_MUTATIONS:
        raise RuntimeError(f"mutation plan total drift: {expected_counts}")
    expected_baselines = {
        row["suite_id"]: row["baseline_commit"]
        for row in plan
    }
    for row in plan:
        validate_source_evidence(
            root, row, "source_file", "source_sha256", "mutation plan"
        )

    actual_counts = {suite: 0 for suite in expected_counts}
    mutation_ids: set[str] = set()
    for row in results:
        mutation_id = row["mutation_id"]
        if mutation_id in mutation_ids:
            raise RuntimeError(f"duplicate mutation id: {mutation_id}")
        mutation_ids.add(mutation_id)
        suite = row["suite_id"]
        if suite not in actual_counts:
            raise RuntimeError(f"unplanned mutation suite: {suite}")
        actual_counts[suite] += 1
        if row["baseline_commit"] != expected_baselines[suite]:
            raise RuntimeError(f"mutation baseline drift: {row}")
        if row["status"] != "KILLED" or row["restore_status"] != "PASS":
            raise RuntimeError(f"mutation not killed/restored: {row}")
        if int(row["exit_code"]) == 0 or not row["named_test"]:
            raise RuntimeError(f"mutation lacks a named failure: {row}")
        validate_source_evidence(
            root, row, "source_file", "source_sha256", "mutation result"
        )
    if actual_counts != expected_counts:
        raise RuntimeError(f"mutation receipt drift: {actual_counts}")


def receipt_contents(root: Path) -> dict[str, object]:
    owned = [
        ARTIFACTS,
        INVENTORY,
        MUTATION_PLAN,
        MUTATION_RESULTS,
        RUST_GATE,
        SCRIPT,
    ]
    return {
        "artifact_count": len(ARTIFACT_PATHS),
        "category_counts": EXPECTED_CATEGORIES,
        "claim_boundary": CLAIM_BOUNDARY,
        "direct_test_support_obligation_count": EXPECTED_DIRECT_TEST_OBLIGATIONS,
        "go_package": "pkg/executor",
        "mutation_count": EXPECTED_MUTATIONS,
        "mutation_suites": EXPECTED_MUTATION_SUITES,
        "obligation_count": EXPECTED_OBLIGATIONS,
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
        "owning_go_source": str(GO_SOURCE),
        "ported_symbol_count": 0,
        "production_obligation_count": EXPECTED_PRODUCTION_OBLIGATIONS,
        "reachable_ported_rule_count": 0,
        "schema": "executor-distsql-file-lockdown-v1",
        "source_seed_commit": SOURCE_SEED_COMMIT,
        "status_counts": {"DECLINED": EXPECTED_OBLIGATIONS},
        "whole_go_package_complete": False,
    }


def check(root: Path, inventory_only: bool) -> None:
    actual_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if actual_artifacts != artifact_lines(root):
        raise RuntimeError("distsql source/test artifact manifest drifted")
    raw = raw_obligations(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_inventory(root, stored, raw)
    if inventory_only:
        print(
            "pkg/executor/distsql.go inventory: 6 artifacts, 1461 AST obligations, "
            "0 PORTED, 1461 DECLINED, file seed only"
        )
        return
    validate_mutations(root)
    actual_receipt = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual_receipt != receipt_contents(root):
        raise RuntimeError("distsql content-addressed receipt drifted")
    print(
        "pkg/executor/distsql.go lockdown: 6 artifacts, 1461 AST obligations, "
        "0 PORTED, 1461 DECLINED, 9 mutations killed, file seed only"
    )


def emit(root: Path, kind: str) -> None:
    if kind == "artifacts":
        print("\n".join(artifact_lines(root)))
    elif kind == "inventory":
        print("\n".join(classified_inventory_lines(raw_obligations(root))))
    elif kind == "receipt":
        validate_mutations(root)
        print(json.dumps(receipt_contents(root), indent=2, sort_keys=True))
    else:
        raise RuntimeError(f"unsupported emit kind: {kind}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
    )
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--emit", choices=["artifacts", "inventory", "receipt"])
    args = parser.parse_args()
    try:
        root = args.root.resolve()
        if args.emit:
            emit(root, args.emit)
        else:
            check(root, args.inventory_only)
    except (
        KeyError,
        OSError,
        RuntimeError,
        ValueError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"pkg/executor/distsql.go lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
