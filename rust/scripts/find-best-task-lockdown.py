#!/usr/bin/env python3
"""Generate and check the source-owned find_best_task.go lockdown ledger."""

from __future__ import annotations

import argparse
import collections
import hashlib
from pathlib import Path
import subprocess
import sys


GO_SOURCE = Path("pkg/planner/core/find_best_task.go")
GO_TEST = Path("pkg/planner/core/find_best_task_test.go")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
RUST_DIR = Path("rust/crates/tidb-executor/src/driver")
INVENTORY = RUST_DIR / "find_best_task.inventory.tsv"

EXPECTED_SOURCE_SHA = "4f98311980c38ca56f98e21925c45e4a412c1481f61bd33711ca31058eabf25d"
EXPECTED_SOURCE_BYTES = 136221
EXPECTED_SOURCE_LINES = 3329
EXPECTED_TEST_SHA = "be9adaa1527557fdb9a7dbac3d270139493b709a6beb15dfac8e4b44401e5612"
EXPECTED_TEST_BYTES = 6089
EXPECTED_TEST_LINES = 171
EXPECTED_PRODUCTION = {
    "branch": 918,
    "closure": 7,
    "declaration": 5,
    "field": 18,
    "function": 68,
    "loop": 124,
    "short_circuit": 510,
    "switch_case": 17,
}
EXPECTED_TEST = {
    "test": 1,
    "test_assertion": 26,
    "test_helper": 3,
    "test_helper_closure": 3,
    "test_row": 28,
}

# One source owner maps to one Rust behavior boundary only after every rule in
# its reachable domain has been audited. Unsupported Go-only domains have an
# explicit evidence id instead; there is no fallback classification.
PORTED = {
    "candidatePath.equalPredicateCount": ("skyline::compare_eq_or_in", "R_SKYLINE_EQ"),
    "candidatePath.hasOnlyEqualPredicatesInDNF": ("skyline::is_full_index_match", "R_SKYLINE_EQ"),
    "compareBool": ("skyline::compare_bool", "R_SKYLINE_CORE"),
    "compareCandidates": ("skyline::compare_candidates", "R_SKYLINE_CORE"),
    "compareEqOrIn": ("skyline::compare_eq_or_in", "R_SKYLINE_EQ"),
    "compareIndexBack": ("skyline::compare_index_back", "R_SKYLINE_CORE"),
    "comparePseudo": ("skyline::compare_pseudo", "R_SKYLINE_PSEUDO"),
    "compareRiskRatio": ("skyline::compare_risk_ratio", "R_SKYLINE_RISK"),
    "convertToBatchPointGet": ("driver::access::try_batch_point_get", "R_POINT_GET"),
    "convertToIndexScan": ("driver::access::commit_index_range_source", "R_SCAN_BUILD"),
    "convertToPointGet": ("driver::access::try_point_get", "R_POINT_GET"),
    "convertToTableScan": ("driver::from::build_from", "R_SCAN_BUILD"),
    "addPushedDownSelection4PhysicalIndexScan": ("driver::access::negotiate_scan_filter", "R_FILTER_SPLIT"),
    "addPushedDownSelection4PhysicalTableScan": ("driver::access::negotiate_scan_filter", "R_FILTER_SPLIT"),
    "findBestTask4LogicalDataSource": ("access_cost::choose_access_path", "R_ACCESS_CHOICE"),
    "getIndexCandidate": ("access_cost::enumerate_paths", "R_ACCESS_ENUM"),
    "getIndexCandidateForIndexJoin": ("driver::leaf_access::leaf_index_path", "R_LEAF_ACCESS"),
    "getTableCandidate": ("access_cost::enumerate_paths", "R_ACCESS_ENUM"),
    "isCandidatesPseudo": ("skyline::compare_candidates", "R_SKYLINE_PSEUDO"),
    "isFullIndexMatch": ("skyline::is_full_index_match", "R_SKYLINE_EQ"),
    "isPointGetConvertableSchema": ("driver::access::try_point_get", "R_POINT_GET"),
    "isPointGetPath": ("driver::access::try_point_get", "R_POINT_GET"),
    "skylinePruning": ("skyline::skyline_pruning", "R_SKYLINE_PRUNE"),
    "splitIndexFilterConditions": ("access_cost::split_index_filter_conditions", "R_FILTER_SPLIT"),
    "tryToGetDualTask": ("driver::access::install_contradiction_dual", "R_TABLE_DUAL"),
}

DECLINED = {
    "GroupRangesByCols": "D_MERGE_SORT",
    "convertToSampleTable": "D_TABLE_SAMPLE",
    "matchProperty": "D_PROPERTY_EXTENSIONS",
    "validateTableSamplePlan": "D_TABLE_SAMPLE",
}

UNREACHABLE_GROUPS = {
    "U_VOLCANO_TASK": {
        "prepareIterationDownElems", "enumeratePhysicalPlans4Task",
        "enumeratePhysicalPlans4TaskHelper", "taskTypeSatisfied",
        "iteratePhysicalPlan4GroupExpression", "iteratePhysicalPlan4BaseLogical",
        "iterateChildPlan4LogicalSequenceGE", "iterateChildPlan4LogicalSequence",
        "compareTaskCost", "getTaskPlanCost", "getGEAndSelf", "findBestTask",
        "exploreEnforcedPlan",
    },
    "U_INDEX_MERGE": {
        "matchPartialOrderProperty", "matchPropForIndexMergeAlternatives",
        "removeCoveredIndexMergeTopLevelFilters", "indexMergeTopLevelFilterCovered",
        "indexMergePartialPathCoversFilter", "expressionContainsHash",
        "isMatchPropForIndexMerge", "convergeIndexMergeCandidate",
        "getIndexMergeCandidate", "convertToIndexMergeScan",
        "canBuildSingleMVIndexOnlyIndexMerge", "checkColinSchema",
        "convertToPartialTableScan", "overwritePartialTableScanSchema",
    },
    "U_TIFLASH_MPP": {
        "buildPhysPlanPartInfo", "compareGlobalIndex", "hasV0NewCollationStringHandle",
        "addPushedDownSelectionToMppTask4PhysicalTableScan",
    },
    "U_GO_TEST_MOCK": {
        "mockLogicalPlan4Test.Init", "mockLogicalPlan4Test.getPhysicalPlan1",
        "mockLogicalPlan4Test.getPhysicalPlan2", "ExhaustPhysicalPlans4MockLogicalPlan",
        "mockPhysicalPlan4Test.Init", "mockPhysicalPlan4Test.Attach2Task",
        "mockPhysicalPlan4Test.MemoryUsage",
    },
    "U_PRUNING_DIAGNOSTIC": {"getPruningInfo"},
}

TEST_OWNERS = {"TestFindBestTaskSuite", "testCostOverflow", "testEnforcedProperty", "testHintCannotFitProperty"}

PORT_FIELD_SUFFIXES = {
    "path", "accessCondsColMap", "indexCondsColMap", "matchPropResult",
    "isFullRange", "eqOrInCount",
}
UNREACHABLE_FIELD_SUFFIXES = {
    "partialOrderMatchResult", "matchWithAdvisorySortItems",
    "partialPathMatchResults", "indexJoinCols",
}


def run(root: Path, command: list[str]) -> str:
    return subprocess.run(
        command, cwd=root, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    ).stdout


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def raw_rows(root: Path) -> list[list[str]]:
    output = run(root, [
        "go", "run", f"./{GO_TOOL}", "--root", ".", "--package", "pkg/planner/core",
    ])
    rows = []
    for line in output.splitlines()[1:]:
        row = line.split("\t")
        if row[2] in {str(GO_SOURCE), str(GO_TEST)}:
            rows.append(row)
    return rows


def unreachable_owner(owner: str) -> str | None:
    found = [proof for proof, owners in UNREACHABLE_GROUPS.items() if owner in owners]
    if len(found) > 1:
        raise RuntimeError(f"owner has multiple unreachable proofs: {owner}: {found}")
    return found[0] if found else None


def classify(row: list[str]) -> tuple[str, str, str]:
    _, category, source, anchor, _, owner = row
    if source == str(GO_TEST):
        if owner not in TEST_OWNERS:
            raise RuntimeError(f"unclassified direct-test owner: {row}")
        return "UNREACHABLE", "-", "U_GO_TEST_MOCK"

    if owner in PORTED:
        symbol, evidence = PORTED[owner]
        return "PORTED", symbol, evidence
    if owner in DECLINED:
        return "DECLINED", "-", DECLINED[owner]
    proof = unreachable_owner(owner)
    if proof:
        return "UNREACHABLE", "-", proof

    if owner == "type:candidatePath":
        if category == "declaration":
            return "PORTED", "skyline::Candidate", "R_ACCESS_ENUM"
        suffix = anchor.rsplit(":", 1)[-1]
        if suffix in PORT_FIELD_SUFFIXES:
            return "PORTED", "skyline::Candidate", "R_ACCESS_ENUM"
        if suffix in UNREACHABLE_FIELD_SUFFIXES:
            return "UNREACHABLE", "-", "U_INDEX_MERGE"
    if owner in {"type:iterFunc", "type:enumerateState"}:
        return "UNREACHABLE", "-", "U_VOLCANO_TASK"
    if owner in {"type:mockLogicalPlan4Test", "type:mockPhysicalPlan4Test"}:
        return "UNREACHABLE", "-", "U_GO_TEST_MOCK"
    raise RuntimeError(f"unclassified find_best_task obligation: {row}")


def inventory_lines(root: Path) -> list[str]:
    source = root / GO_SOURCE
    test = root / GO_TEST
    rows = raw_rows(root)
    lines = [
        "# find-best-task-go-lockdown-inventory-v1",
        f"# owning-go-source\t{GO_SOURCE}",
        f"# source-sha256\t{digest(source)}",
        f"# source-bytes\t{source.stat().st_size}",
        f"# source-lines\t{len(source.read_bytes().splitlines())}",
        f"# owning-go-test\t{GO_TEST}",
        f"# test-sha256\t{digest(test)}",
        f"# test-bytes\t{test.stat().st_size}",
        f"# test-lines\t{len(test.read_bytes().splitlines())}",
        "# production-obligations\t1667",
        "# direct-test-support-obligations\t61",
        "# explicitly-unclaimed-source\tpkg/executor/distsql.go",
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner\tstatus\trust_symbol\tevidence",
    ]
    for row in rows:
        lines.append("\t".join([*row, *classify(row)]))
    validate(root, lines)
    return lines


def validate(root: Path, lines: list[str]) -> None:
    source = root / GO_SOURCE
    test = root / GO_TEST
    actual = (
        digest(source), source.stat().st_size, len(source.read_bytes().splitlines()),
        digest(test), test.stat().st_size, len(test.read_bytes().splitlines()),
    )
    expected = (
        EXPECTED_SOURCE_SHA, EXPECTED_SOURCE_BYTES, EXPECTED_SOURCE_LINES,
        EXPECTED_TEST_SHA, EXPECTED_TEST_BYTES, EXPECTED_TEST_LINES,
    )
    if actual != expected:
        raise RuntimeError(f"source/test drift: expected {expected}, found {actual}")
    rows = [line.split("\t") for line in lines if line and not line.startswith("#")][1:]
    if len(rows) != 1728:
        raise RuntimeError(f"obligation count drift: {len(rows)}")
    ids = [row[0] for row in rows]
    anchors = [(row[2], row[3]) for row in rows]
    if len(set(ids)) != len(ids) or len(set(anchors)) != len(anchors):
        raise RuntimeError("duplicate obligation identity")
    production = collections.Counter(row[1] for row in rows if row[2] == str(GO_SOURCE))
    tests = collections.Counter(row[1] for row in rows if row[2] == str(GO_TEST))
    if dict(production) != EXPECTED_PRODUCTION:
        raise RuntimeError(f"production category drift: {dict(production)}")
    if dict(tests) != EXPECTED_TEST:
        raise RuntimeError(f"test category drift: {dict(tests)}")
    for row in rows:
        if len(row) != 9 or tuple(row[6:]) != classify(row[:6]):
            raise RuntimeError(f"classification drift: {row}")
        if row[6] not in {"PORTED", "DECLINED", "UNREACHABLE"}:
            raise RuntimeError(f"invalid verdict: {row}")
        if row[2] == "pkg/executor/distsql.go":
            raise RuntimeError("distsql.go must remain outside this unit")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("generate", "check"))
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    root = args.root.resolve()
    wanted = "\n".join(inventory_lines(root)) + "\n"
    path = root / INVENTORY
    if args.mode == "generate":
        path.write_text(wanted, encoding="utf-8")
    elif not path.exists() or path.read_text(encoding="utf-8") != wanted:
        raise RuntimeError(f"lockdown inventory drift: run {Path(__file__).name} generate")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
