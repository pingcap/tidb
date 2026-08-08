#!/usr/bin/env python3
"""Check the exact file-lockdown seed for pkg/util/ranger/detacher.go."""

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


GO_SOURCE = Path("pkg/util/ranger/detacher.go")
GO_TEST = Path("pkg/util/ranger/ranger_test.go")
GO_BENCH = Path("pkg/util/ranger/bench_test.go")
ARTIFACT_PATHS = {
    GO_SOURCE: "production-owner",
    GO_TEST: "direct-test-and-helper-support",
    GO_BENCH: "direct-benchmark-and-helper-support",
}

RUST_OWNER = Path("rust/crates/tidb-executor/src/ranger_detacher_lockdown.rs")
RUST_INTEGRATION = Path("rust/crates/tidb-executor/src/lib.rs")
PLANNER_OWNER = Path("rust/crates/tidb-planner/src/range_detacher.rs")
EXECUTOR_RANGE_OWNER = Path("rust/crates/tidb-executor/src/index_range.rs")
DATA_STEM = Path("rust/crates/tidb-executor/src/ranger_detacher_lockdown")
ARTIFACTS = DATA_STEM.with_suffix(".artifacts.tsv")
INVENTORY = DATA_STEM.with_suffix(".inventory.tsv")
MUTATION_PLAN = DATA_STEM.with_suffix(".mutation-plan.tsv")
MUTATION_RESULTS = DATA_STEM.with_suffix(".mutation-results.tsv")
RECEIPT = DATA_STEM.with_suffix(".receipt.json")
SCRIPT = Path("rust/scripts/pkg-util-ranger-detacher-lockdown.py")
EXECPLAN = Path("rust/docs/operations/pkg-util-ranger-detacher-go-lockdown-execplan.md")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")

SOURCE_SEED_COMMIT = "6fa49fb9112c850ffd9861651792cd043b830a8d"
CLAIM_BOUNDARY = "file-lockdown-seed-not-package-completion"
EXPECTED_SOURCE_COUNTS = {
    str(GO_SOURCE): 676,
    str(GO_TEST): 1365,
    str(GO_BENCH): 47,
}
EXPECTED_CATEGORIES = {
    "benchmark": 2,
    "branch": 394,
    "closure": 4,
    "declaration": 4,
    "field": 23,
    "function": 37,
    "loop": 72,
    "short_circuit": 128,
    "switch_case": 14,
    "test": 14,
    "test_assertion": 233,
    "test_branch": 10,
    "test_helper": 8,
    "test_helper_closure": 10,
    "test_loop": 50,
    "test_row": 1083,
    "test_short_circuit": 2,
}
EXPECTED_OBLIGATIONS = 2088
EXPECTED_PRODUCTION_OBLIGATIONS = 676
EXPECTED_DIRECT_SUPPORT_OBLIGATIONS = 1412
EXPECTED_STATUS_COUNTS = {"DECLINED": 2046, "PORTED": 42, "UNREACHABLE": 0}
EXPECTED_MUTATION_SUITES = 6
EXPECTED_MUTATIONS = 12

DIRECT_TEST_OWNERS = {
    "TestTableRange",
    "TestIndexRangeForUnsignedAndOverflow",
    "TestColumnRange",
    "TestIndexRangeForYear",
    "TestPrefixIndexRangeScan",
    "TestIndexRange",
    "TestShardIndexFuncSuites",
    "TestRangeFallbackForDetachCondAndBuildRangeForIndex",
    "TestRangeFallbackForBuildTableRange",
    "TestRangeFallbackForBuildColumnRange",
    "TestPrefixIndexRange",
    "TestMinAccessCondsForDNFCond",
    "TestBinCollationRangeForIndex",
    "getSelectionFromQuery",
    "checkDetachRangeResult",
    "checkRangeFallbackAndReset",
}
DIRECT_BENCH_OWNERS = {
    "BenchmarkDetachCondAndBuildRangeForIndex",
    "BenchmarkDetachCondAndBuildRangeForIndexLeadingLongIN",
    "buildBenchmarkSelection",
    "benchmarkConditions",
    "findBenchmarkSelection",
    "findBenchmarkDataSource",
    "makeBenchmarkIntList",
    "TestBenchDaily",
}

# One production owner may classify many branch obligations, but every row
# still names one compiled Rust symbol and one killed mutation suite.
PORTS = {
    "detachColumnCNFConditions": (
        "tidb_planner::range_detacher::detach_cnf_predicates",
        PLANNER_OWNER,
        "R_CNF",
    ),
    "detachColumnDNFConditions": (
        "tidb_planner::range_detacher::detach_dnf_predicates",
        PLANNER_OWNER,
        "R_DNF",
    ),
    "DetachCondAndBuildRangeForIndex": (
        "index_range::detach_cond_and_build_range_for_index",
        EXECUTOR_RANGE_OWNER,
        "R_INDEX",
    ),
    "DetachCondsForColumn": (
        "index_range::detach_conds_for_column",
        EXECUTOR_RANGE_OWNER,
        "R_COLUMN",
    ),
    "removeConditions": (
        "ranger_detacher_lockdown::remove_conditions",
        RUST_OWNER,
        "R_LIST",
    ),
    "AppendConditionsIfNotExist": (
        "ranger_detacher_lockdown::append_conditions_if_not_exist",
        RUST_OWNER,
        "R_LIST",
    ),
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
        "# pkg-util-ranger-detacher-file-lockdown-artifacts-v1",
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


def inventory_output(root: Path) -> list[list[str]]:
    with tempfile.TemporaryDirectory(prefix="ranger-detacher-lockdown-") as temp:
        temp_root = Path(temp)
        for source in ARTIFACT_PATHS:
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
                "pkg/util/ranger",
            ],
        )
    lines = output.rstrip("\n").splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise RuntimeError("Go inventory returned no ranger header")
    rows = [line.split("\t") for line in lines[1:]]
    if any(len(row) != 6 for row in rows):
        raise RuntimeError("invalid ranger Go inventory row")
    return rows


def raw_obligations(root: Path) -> list[list[str]]:
    rows = [
        row
        for row in inventory_output(root)
        if row[2] == str(GO_SOURCE)
        or (row[2] == str(GO_TEST) and row[5] in DIRECT_TEST_OWNERS)
        or (row[2] == str(GO_BENCH) and row[5] in DIRECT_BENCH_OWNERS)
    ]
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
        return (
            f"Go direct test/support owner {owner} requires TiDB expression BuildContext, "
            "session/planner fixtures, or range fallback accounting absent from the native seam"
        )
    if owner.startswith("type:") or owner in {
        "getPotentialEqOrInColOffset",
        "ExtractEqAndInCondition",
        "extractValueInfo",
        "isSameValue",
        "NeedAddGcColumn4ShardIndex",
        "NeedAddColumn4EqCond",
        "NeedAddColumn4InCond",
        "ExtractColumnsFromExpr",
        "IsValidShardIndex",
        "AddGcColumnCond",
        "AddGcColumn4InCond",
        "AddGcColumn4EqCond",
        "AddExpr4EqAndInCondition",
    }:
        return (
            f"Go owner {owner} requires expression.Column identity, VirtualExpr, mutable "
            "valueInfo, collation, or EvalContext interfaces not represented by this Rust seam"
        )
    return (
        f"Go owner {owner} requires full point/range memory-fallback, prefix/collation, "
        "partition recursion, or residual-expression reconstruction beyond the isolated native rule"
    )


def classify(source: str, owner: str, node_hash: str, anchor: str) -> tuple[str, str, str, str]:
    quote = f"go-ast-quote:{source}#{anchor}@sha256:{node_hash}"
    if source == str(GO_SOURCE) and owner in PORTS:
        symbol, _, suite = PORTS[owner]
        reason = f"Exact native rule compiled as {symbol}"
        return (
            "PORTED",
            symbol,
            f"{quote};rust-compile-anchor:{symbol};mutation-suite:{suite}",
            reason,
        )
    reason = decline_reason(source, owner)
    return "DECLINED", "-", f"{quote};measured-gap:{reason}", reason


def classified_inventory_lines(raw: list[list[str]]) -> list[str]:
    lines = [
        "# pkg-util-ranger-detacher-file-lockdown-inventory-v1",
        (
            "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
            "\tstatus\trust_symbol\tevidence\treason\tmutation_policy"
        ),
    ]
    for obligation_id, category, source, anchor, node_hash, owner in raw:
        status, symbol, evidence, reason = classify(source, owner, node_hash, anchor)
        policy = (
            "boundary-mutation-killed"
            if status == "PORTED"
            else "promotion-mutation-required-before-PORTED"
        )
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
    root: Path, row: dict[str, str], source_field: str, hash_field: str, kind: str
) -> None:
    sources = row[source_field].split("|")
    hashes = row[hash_field].split("|")
    if len(sources) != len(hashes):
        raise RuntimeError(f"{kind} source/hash width drift: {row}")
    for source, expected_hash in zip(sources, hashes):
        path = root / source
        if (
            not source
            or Path(source).is_absolute()
            or ".." in Path(source).parts
            or not path.is_file()
        ):
            raise RuntimeError(f"{kind} source is not a repository-relative file: {source}")
        if not expected_hash or sha256(path) != expected_hash:
            raise RuntimeError(f"{kind} source drifted: {source}")


def validate_symbols(root: Path) -> None:
    for symbol, source, _ in set(PORTS.values()):
        leaf = symbol.rsplit("::", 1)[-1]
        if leaf not in (root / source).read_text(encoding="utf-8"):
            raise RuntimeError(f"PORTED symbol disappeared from {source}: {symbol}")
        if symbol not in (root / RUST_OWNER).read_text(encoding="utf-8"):
            raise RuntimeError(f"lockdown compile anchor disappeared: {symbol}")


def validate_inventory(stored: list[str], raw: list[list[str]]) -> None:
    if stored != classified_inventory_lines(raw):
        raise RuntimeError("detacher AST obligations, verdicts, or evidence drifted")
    rows = [line.split("\t") for line in stored[2:]]
    if len(rows) != EXPECTED_OBLIGATIONS or any(len(row) != 11 for row in rows):
        raise RuntimeError(f"inventory width/count drift: {len(rows)}")
    source_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    status_counts = {"DECLINED": 0, "PORTED": 0, "UNREACHABLE": 0}
    symbols: set[str] = set()
    for row in rows:
        source_counts[row[2]] = source_counts.get(row[2], 0) + 1
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        if row[6] not in status_counts:
            raise RuntimeError(f"invalid or blank verdict: {row}")
        status_counts[row[6]] += 1
        if row[6] == "PORTED":
            symbols.add(row[7])
            if row[7] not in {port[0] for port in PORTS.values()}:
                raise RuntimeError(f"PORTED row names unknown symbol: {row}")
            if "mutation-suite:" not in row[8] or row[10] != "boundary-mutation-killed":
                raise RuntimeError(f"PORTED row lacks killed mutation evidence: {row}")
        elif row[6] == "DECLINED":
            if row[7] != "-" or "measured-gap:" not in row[8]:
                raise RuntimeError(f"DECLINED row lacks measured gap: {row}")
        else:
            raise RuntimeError(f"unexpected UNREACHABLE row: {row}")
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
            raise RuntimeError(f"mutation lacks named failure: {row}")
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
        "direct_test_support_obligation_count": EXPECTED_DIRECT_SUPPORT_OBLIGATIONS,
        "go_package": "pkg/util/ranger",
        "mutation_count": EXPECTED_MUTATIONS,
        "mutation_suites": EXPECTED_MUTATION_SUITES,
        "obligation_count": EXPECTED_OBLIGATIONS,
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
        "owning_go_source": str(GO_SOURCE),
        "ported_obligation_count": EXPECTED_STATUS_COUNTS["PORTED"],
        "ported_symbol_count": len(PORTS),
        "production_obligation_count": EXPECTED_PRODUCTION_OBLIGATIONS,
        "reachable_ported_rule_count": EXPECTED_STATUS_COUNTS["PORTED"],
        "schema": "pkg-util-ranger-detacher-file-lockdown-v1",
        "source_seed_commit": SOURCE_SEED_COMMIT,
        "status_counts": EXPECTED_STATUS_COUNTS,
        "whole_go_package_complete": False,
    }


def check(root: Path, inventory_only: bool) -> None:
    actual_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if actual_artifacts != artifact_lines(root):
        raise RuntimeError("detacher source/test artifact manifest drifted")
    raw = raw_obligations(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_inventory(stored, raw)
    validate_symbols(root)
    if inventory_only:
        print(
            "pkg/util/ranger/detacher.go inventory: 3 artifacts, 2088 AST obligations, "
            "42 PORTED, 2046 DECLINED, 0 UNREACHABLE, file seed only"
        )
        return
    validate_mutations(root)
    actual_receipt = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual_receipt != receipt_contents(root):
        raise RuntimeError("detacher content-addressed receipt drifted")
    print(
        "pkg/util/ranger/detacher.go lockdown: 3 artifacts, 2088 AST obligations, "
        "42 PORTED, 2046 DECLINED, 0 UNREACHABLE, 12 mutations killed, file seed only"
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
        print(f"pkg/util/ranger/detacher.go lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
