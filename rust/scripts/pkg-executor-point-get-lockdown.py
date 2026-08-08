#!/usr/bin/env python3
"""Check the exact file-lockdown seed for pkg/executor/point_get.go."""

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


GO_SOURCE = Path("pkg/executor/point_get.go")
GO_DIRECT_TEST = Path("pkg/executor/point_get_test.go")
GO_REPEATABLE_TEST = Path("pkg/executor/executor_failpoint_test.go")
GO_EXEC_METRIC_TEST = Path("pkg/executor/internal/exec/executor_test.go")
GO_INDEX_USAGE_TEST = Path("pkg/executor/internal/exec/indexusage_test.go")
GO_REPLICA_TEST = Path("tests/realtikvtest/txntest/stale_read_test.go")
ARTIFACT_PATHS = {
    GO_SOURCE: "production-owner",
    GO_DIRECT_TEST: "dedicated-test-support",
    GO_REPEATABLE_TEST: "direct-repeatable-read-failpoint-support",
    GO_EXEC_METRIC_TEST: "direct-concrete-executor-label-support",
    GO_INDEX_USAGE_TEST: "direct-point-get-index-usage-support",
    GO_REPLICA_TEST: "direct-replica-option-failpoint-support",
}

RUST_OWNER = Path("rust/crates/tidb-executor/src/point_get.rs")
RUST_INTEGRATION = Path("rust/crates/tidb-executor/src/lib.rs")
DATA_STEM = Path("rust/crates/tidb-executor/src/point_get")
ARTIFACTS = DATA_STEM.with_suffix(".artifacts.tsv")
INVENTORY = DATA_STEM.with_suffix(".inventory.tsv")
MUTATION_PLAN = DATA_STEM.with_suffix(".mutation-plan.tsv")
MUTATION_RESULTS = DATA_STEM.with_suffix(".mutation-results.tsv")
RECEIPT = DATA_STEM.with_suffix(".receipt.json")
SCRIPT = Path("rust/scripts/pkg-executor-point-get-lockdown.py")
EXECPLAN = Path("rust/docs/operations/executor-point-get-go-lockdown-execplan.md")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")

SOURCE_SEED_COMMIT = "566c460c26d5019fd32ce157531bcf431a8ce447"
CLAIM_BOUNDARY = "file-lockdown-seed-not-package-completion"
EXPECTED_SOURCE_COUNTS = {
    str(GO_SOURCE): 358,
    str(GO_DIRECT_TEST): 100,
    str(GO_REPEATABLE_TEST): 11,
    str(GO_EXEC_METRIC_TEST): 116,
    str(GO_INDEX_USAGE_TEST): 144,
    str(GO_REPLICA_TEST): 34,
}
EXPECTED_CATEGORIES = {
    "branch": 248,
    "closure": 7,
    "declaration": 2,
    "field": 24,
    "function": 27,
    "loop": 26,
    "short_circuit": 24,
    "test": 12,
    "test_assertion": 62,
    "test_branch": 18,
    "test_helper": 1,
    "test_helper_closure": 13,
    "test_loop": 26,
    "test_row": 271,
    "test_short_circuit": 2,
}
EXPECTED_OBLIGATIONS = 763
EXPECTED_PRODUCTION_OBLIGATIONS = 358
EXPECTED_DIRECT_TEST_OBLIGATIONS = 405
EXPECTED_STATUS_COUNTS = {"DECLINED": 734, "PORTED": 28, "UNREACHABLE": 1}
EXPECTED_MUTATION_SUITES = 12
EXPECTED_MUTATIONS = 22

PORTS = {
    "GetPhysID": ("point_get::physical_table_id", "R_PHYSICAL_ID"),
    "matchPartitionNames": ("point_get::partition_name_matches", "R_PARTITION_NAMES"),
    "shouldFillRowChecksum": ("point_get::row_checksum_column", "R_CHECKSUM_COLUMN"),
    "notPKPrefixCol": ("point_get::not_primary_prefix_column", "R_PREFIX_COLUMN"),
    "getColInfoByID": ("point_get::column_by_id", "R_COLUMN_BY_ID"),
}
UNREACHABLE = {
    "GetPhysID/if:2/true": (
        "point_get::physical_table_id accepts Option<usize>; a negative partition ordinal "
        "has no Rust value"
    )
}


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


def source_lines(path: Path) -> int:
    data = path.read_bytes()
    return data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)


def artifact_lines(root: Path) -> list[str]:
    lines = [
        "# executor-point-get-file-lockdown-artifacts-v1",
        "path\trole\tsha256\tbytes\tlines",
    ]
    for path, role in ARTIFACT_PATHS.items():
        full_path = root / path
        if not full_path.is_file():
            raise RuntimeError(f"owned Go artifact is not a file: {path}")
        lines.append(
            f"{path}\t{role}\t{sha256(full_path)}\t{full_path.stat().st_size}\t"
            f"{source_lines(full_path)}"
        )
    return lines


def inventory_output(root: Path, package: str, sources: list[Path]) -> list[list[str]]:
    with tempfile.TemporaryDirectory(prefix="executor-point-get-lockdown-") as temp:
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
    executor_rows = inventory_output(
        root,
        "pkg/executor",
        [GO_SOURCE, GO_DIRECT_TEST, GO_REPEATABLE_TEST],
    )
    rows = [
        row
        for row in executor_rows
        if row[2] in {str(GO_SOURCE), str(GO_DIRECT_TEST)}
        or (row[2] == str(GO_REPEATABLE_TEST) and row[5] == "TestPointGetRepeatableRead")
    ]

    internal_rows = inventory_output(
        root,
        "pkg/executor/internal/exec",
        [GO_EXEC_METRIC_TEST, GO_INDEX_USAGE_TEST],
    )
    rows.extend(
        row
        for row in internal_rows
        if (
            row[2] == str(GO_EXEC_METRIC_TEST)
            and row[5] == "TestRUV2ExecutorMetricByTypeIncludesConcreteExecutorTypes"
        )
        or (
            row[2] == str(GO_INDEX_USAGE_TEST)
            and row[5]
            in {
                "TestIndexUsageReporterWithRealData",
                "TestIndexUsageReporterWithPartitionTable",
                "TestIndexUsageReporterWithGlobalIndex",
            }
        )
    )

    replica_rows = inventory_output(
        root,
        "tests/realtikvtest/txntest",
        [GO_REPLICA_TEST],
    )
    rows.extend(
        row for row in replica_rows if row[5] == "TestStaleReadKVRequest"
    )
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


def decline_reason(source: str, owner: str) -> str:
    if source != str(GO_SOURCE):
        if source == str(GO_REPEATABLE_TEST):
            return "Rust lacks Go's two-stage unique-index snapshot repeatable-read failpoint boundary"
        if source == str(GO_EXEC_METRIC_TEST):
            return "Rust has metadata helpers, not a concrete PointGetExecutor runtime-stat label owner"
        if source == str(GO_INDEX_USAGE_TEST):
            return "Rust executor has no Go IndexUsageReporter close-time point-get sampling interface"
        if source == str(GO_REPLICA_TEST):
            return "Rust executor has no Go Snapshot ReplicaReadAdjuster option or assertPointReplicaOption hook"
        return (
            f"Go direct test/support owner {owner} requires unported point-get executor state, "
            "snapshot, locking, chunk, or cache behavior"
        )
    if owner.startswith("runtimeStatsWithSnapshot"):
        return "Rust executor crate explicitly defers Go runtime statistics and snapshot RPC stats"
    if owner in {"fillRowChecksum"}:
        return "Rust has no row-checksum pseudo-column writer over decoded chunk rows"
    if owner in {"DecodeRowValToChunk", "decodeOldRowValToChunk", "tryDecodeFromHandle"}:
        return "Rust has no Go expression.Schema plus chunk.Chunk row-decoder interface for this owner"
    return (
        f"Go owner {owner} requires sessionctx, kv.Snapshot, pessimistic lock cache, executor "
        "lifecycle, or runtime state absent from the native executor"
    )


def classify(source: str, anchor: str, owner: str) -> tuple[str, str, str, str]:
    quote = f"go-ast-quote:{source}#{anchor}"
    if source == str(GO_SOURCE) and anchor in UNREACHABLE:
        reason = UNREACHABLE[anchor]
        return "UNREACHABLE", "-", f"{quote};structural-proof:{reason}", reason
    if source == str(GO_SOURCE) and owner in PORTS:
        symbol, suite = PORTS[owner]
        reason = f"Exact native metadata rule compiled as {symbol}"
        return "PORTED", symbol, f"{quote};rust-compile-anchor:{symbol};mutation-suite:{suite}", reason
    reason = decline_reason(source, owner)
    return "DECLINED", "-", f"{quote};measured-gap:{reason}", reason


def classified_inventory_lines(raw: list[list[str]]) -> list[str]:
    lines = [
        "# executor-point-get-file-lockdown-inventory-v1",
        (
            "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
            "\tstatus\trust_symbol\tevidence\treason\tmutation_policy"
        ),
    ]
    for obligation_id, category, source, anchor, node_hash, owner in raw:
        status, symbol, evidence, reason = classify(source, anchor, owner)
        evidence = f"{evidence}@sha256:{node_hash}"
        policy = {
            "PORTED": "boundary-mutation-killed",
            "DECLINED": "promotion-mutation-required-before-PORTED",
            "UNREACHABLE": "structural-proof-gated",
        }[status]
        lines.append(
            "\t".join(
                [
                    obligation_id,
                    category,
                    source,
                    anchor,
                    node_hash,
                    owner,
                    status,
                    symbol,
                    evidence,
                    reason,
                    policy,
                ]
            )
        )
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
        if not source or Path(source).is_absolute() or ".." in Path(source).parts or not path.is_file():
            raise RuntimeError(f"{kind} source is not a repository-relative file: {source}")
        if not expected_hash or sha256(path) != expected_hash:
            raise RuntimeError(f"{kind} source drifted: {source}")


def validate_inventory(stored: list[str], raw: list[list[str]]) -> None:
    expected = classified_inventory_lines(raw)
    if stored != expected:
        raise RuntimeError("point_get AST obligations, verdicts, or evidence drifted")
    rows = [line.split("\t") for line in stored[2:]]
    if len(rows) != EXPECTED_OBLIGATIONS or any(len(row) != 11 for row in rows):
        raise RuntimeError(f"inventory width/count drift: {len(rows)}")

    source_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    symbols: set[str] = set()
    for row in rows:
        source_counts[row[2]] = source_counts.get(row[2], 0) + 1
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        status_counts[row[6]] = status_counts.get(row[6], 0) + 1
        if row[6] == "PORTED":
            symbols.add(row[7])
            if row[7] not in {port[0] for port in PORTS.values()}:
                raise RuntimeError(f"PORTED row names an unknown compiled symbol: {row}")
            if row[10] != "boundary-mutation-killed":
                raise RuntimeError(f"PORTED row lacks killed mutation policy: {row}")
        elif row[6] == "DECLINED":
            if row[7] != "-" or "measured-gap:" not in row[8]:
                raise RuntimeError(f"DECLINED row lacks measured gap: {row}")
        elif row[6] == "UNREACHABLE":
            if row[7] != "-" or "structural-proof:" not in row[8]:
                raise RuntimeError(f"UNREACHABLE row lacks structural proof: {row}")
        else:
            raise RuntimeError(f"invalid or blank verdict: {row}")
        if f"@sha256:{row[4]}" not in row[8] or not row[9]:
            raise RuntimeError(f"source quote or reason drift: {row}")
    if source_counts != EXPECTED_SOURCE_COUNTS:
        raise RuntimeError(f"source census drift: {source_counts}")
    if category_counts != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {category_counts}")
    if status_counts != EXPECTED_STATUS_COUNTS:
        raise RuntimeError(f"status census drift: {status_counts}")
    if symbols != {port[0] for port in PORTS.values()}:
        raise RuntimeError(f"compiled symbol census drift: {symbols}")


def validate_mutations(root: Path) -> None:
    plan = data_rows(root / MUTATION_PLAN)
    results = data_rows(root / MUTATION_RESULTS)
    if len(plan) != EXPECTED_MUTATION_SUITES:
        raise RuntimeError(f"mutation plan suite drift: {len(plan)}")
    if len(results) != EXPECTED_MUTATIONS:
        raise RuntimeError(f"mutation result count drift: {len(results)}")
    expected_counts = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if len(expected_counts) != len(plan) or sum(expected_counts.values()) != EXPECTED_MUTATIONS:
        raise RuntimeError(f"mutation plan census drift: {expected_counts}")
    expected_baselines = {row["suite_id"]: row["baseline_commit"] for row in plan}
    for row in plan:
        validate_source_evidence(root, row, "source_file", "source_sha256", "mutation plan")

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
        validate_source_evidence(root, row, "source_file", "source_sha256", "mutation result")
    if actual_counts != expected_counts:
        raise RuntimeError(f"mutation receipt drift: {actual_counts}")


def receipt_contents(root: Path) -> dict[str, object]:
    owned = [
        ARTIFACTS,
        INVENTORY,
        MUTATION_PLAN,
        MUTATION_RESULTS,
        RUST_OWNER,
        RUST_INTEGRATION,
        SCRIPT,
        EXECPLAN,
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
        "ported_obligation_count": EXPECTED_STATUS_COUNTS["PORTED"],
        "ported_symbol_count": len(PORTS),
        "production_obligation_count": EXPECTED_PRODUCTION_OBLIGATIONS,
        "reachable_ported_rule_count": EXPECTED_STATUS_COUNTS["PORTED"],
        "schema": "executor-point-get-file-lockdown-v1",
        "source_seed_commit": SOURCE_SEED_COMMIT,
        "status_counts": EXPECTED_STATUS_COUNTS,
        "whole_go_package_complete": False,
    }


def check(root: Path, inventory_only: bool) -> None:
    actual_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if actual_artifacts != artifact_lines(root):
        raise RuntimeError("point_get source/test artifact manifest drifted")
    raw = raw_obligations(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_inventory(stored, raw)
    if inventory_only:
        print(
            "pkg/executor/point_get.go inventory: 6 artifacts, 763 AST obligations, "
            "28 PORTED, 734 DECLINED, 1 UNREACHABLE, file seed only"
        )
        return
    validate_mutations(root)
    actual_receipt = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual_receipt != receipt_contents(root):
        raise RuntimeError("point_get content-addressed receipt drifted")
    print(
        "pkg/executor/point_get.go lockdown: 6 artifacts, 763 AST obligations, "
        "28 PORTED, 734 DECLINED, 1 UNREACHABLE, 22 mutations killed, file seed only"
    )


def write_inventory(root: Path) -> None:
    (root / ARTIFACTS).write_text("\n".join(artifact_lines(root)) + "\n", encoding="utf-8")
    (root / INVENTORY).write_text(
        "\n".join(classified_inventory_lines(raw_obligations(root))) + "\n",
        encoding="utf-8",
    )


def write_receipt(root: Path) -> None:
    validate_mutations(root)
    (root / RECEIPT).write_text(
        json.dumps(receipt_contents(root), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--write-inventory", action="store_true")
    parser.add_argument("--write-receipt", action="store_true")
    args = parser.parse_args()
    try:
        root = args.root.resolve()
        if args.write_inventory:
            write_inventory(root)
        elif args.write_receipt:
            write_receipt(root)
        else:
            check(root, args.inventory_only)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"pkg/executor/point_get.go lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
