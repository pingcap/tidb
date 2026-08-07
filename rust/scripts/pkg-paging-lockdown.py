#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/util/paging lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys


PACKAGE = Path("pkg/util/paging")
RUST_DIR = Path("rust/crates/tidb-util/src")
ARTIFACTS = RUST_DIR / "paging.artifacts.tsv"
INVENTORY = RUST_DIR / "paging.inventory.tsv"
MUTATION_PLAN = RUST_DIR / "paging.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "paging.mutation-results.tsv"
RECEIPT = RUST_DIR / "paging.receipt.json"
RUST_SOURCE = RUST_DIR / "paging.rs"
RUST_GATE = Path("rust/crates/tidb-util/tests/paging_lockdown.rs")
SCRIPT = Path("rust/scripts/pkg-paging-lockdown.py")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")

EXPECTED_ZERO_CLASSES = {
    "build_tags": 0,
    "platform_variants": 0,
    "code_generated": 0,
    "go_generate": 0,
    "go_embed": 0,
    "tracked_testdata": 0,
}
EXPECTED_CATEGORIES = {
    "branch": 10,
    "const": 6,
    "function": 2,
    "test": 2,
    "test_assertion": 3,
    "test_main": 1,
    "test_row": 4,
}
CONSTANTS = {
    "const:MinAllowedMaxPagingSize:0": "MIN_ALLOWED_MAX_PAGING_SIZE",
    "const:MinPagingSize:0": "MIN_PAGING_SIZE",
    "const:Threshold:0": "THRESHOLD",
    "const:maxPagingSizeShift:0": "MAX_PAGING_SIZE_SHIFT",
    "const:pagingGrowingSum:0": "PAGING_GROWING_SUM",
    "const:pagingSizeGrow:0": "PAGING_SIZE_GROW",
}
FUNCTIONS = {
    "CalculateSeekCnt": "calculate_seek_cnt",
    "GrowPagingSize": "grow_paging_size",
}
DECLINED_SUPPORT = {
    "TestMain",
    "TestMain/composite:1/element:0",
    "TestMain/composite:1/element:1",
    "TestMain/composite:1/element:2",
    "TestMain/composite:1/element:3",
}
SYMBOL_EVIDENCE = "rust-test:paging_lockdown_inventory_is_complete_and_symbols_compile"
GROW_EVIDENCE = "rust-test:grow_paging_size_preserves_source_wrapping_and_cap_order"
SEEK_EVIDENCE = "rust-test:calculate_seek_cnt_preserves_source_piecewise_boundaries"
GO_TESTS = {
    "TestCalculateSeekCnt": (
        "calculate_seek_cnt_test",
        "rust-test:calculate_seek_cnt_test",
    ),
    "TestGrowPagingSize": (
        "grow_paging_size_test",
        "rust-test:grow_paging_size_test",
    ),
}
DECLINED_EVIDENCE = "source-quote:go_testsetup_and_goleak_only"


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


def direct_package_paths(root: Path) -> tuple[list[str], list[str]]:
    tracked = run(root, ["git", "ls-files", "--", str(PACKAGE)]).splitlines()
    untracked = run(
        root,
        ["git", "ls-files", "--others", "--exclude-standard", "--", str(PACKAGE)],
    ).splitlines()
    return sorted(tracked), sorted(untracked)


def zero_classes(root: Path, paths: list[str]) -> dict[str, int]:
    go_paths = [path for path in paths if path.endswith(".go")]
    texts = {path: (root / path).read_text(encoding="utf-8") for path in go_paths}
    platforms = {
        "aix", "android", "darwin", "dragonfly", "freebsd", "illumos", "ios",
        "js", "linux", "netbsd", "openbsd", "plan9", "solaris", "wasip1",
        "windows", "386", "amd64", "arm", "arm64", "loong64", "mips",
        "mips64", "mips64le", "mipsle", "ppc64", "ppc64le", "riscv64",
        "s390x", "wasm",
    }
    return {
        "build_tags": sum("//go:build" in text or "// +build" in text for text in texts.values()),
        "platform_variants": sum(
            any(part in platforms for part in Path(path).stem.split("_")[1:])
            for path in go_paths
        ),
        "code_generated": sum("// Code generated" in text for text in texts.values()),
        "go_generate": sum("//go:generate" in text for text in texts.values()),
        "go_embed": sum("//go:embed" in text for text in texts.values()),
        "tracked_testdata": sum("/testdata/" in f"/{path}/" for path in paths),
    }


def artifact_lines(root: Path) -> list[str]:
    paths, untracked = direct_package_paths(root)
    if untracked:
        raise RuntimeError(f"untracked pkg/util/paging artifacts: {untracked}")
    expected = [
        "pkg/util/paging/BUILD.bazel",
        "pkg/util/paging/main_test.go",
        "pkg/util/paging/paging.go",
        "pkg/util/paging/paging_test.go",
    ]
    if paths != expected:
        raise RuntimeError(f"pkg/util/paging artifact drift: expected {expected}, found {paths}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    lines = ["# pkg-paging-artifacts-v1"]
    for name, count in EXPECTED_ZERO_CLASSES.items():
        lines.append(f"# zero\t{name}\t{count}")
    lines.append("path\trole\tsha256")
    roles = {
        "pkg/util/paging/BUILD.bazel": "build",
        "pkg/util/paging/main_test.go": "test-support",
        "pkg/util/paging/paging.go": "production",
        "pkg/util/paging/paging_test.go": "test",
    }
    lines.extend(f"{path}\t{roles[path]}\t{sha256(root / path)}" for path in paths)
    return lines


def raw_obligation_lines(root: Path) -> list[str]:
    output = run(
        root,
        ["go", "run", f"./{GO_TOOL}", "--root", ".", "--package", str(PACKAGE)],
    )
    lines = output.rstrip("\n").splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise RuntimeError("Go package inventory returned no header")
    return lines


def ported_mapping(
    category: str, source_path: str, anchor: str, owner: str
) -> tuple[str, str, str] | None:
    if source_path == "pkg/util/paging/paging.go":
        if category == "const" and owner in CONSTANTS and anchor == owner:
            return CONSTANTS[owner], SYMBOL_EVIDENCE, "compile-owner-gate"
        if category == "function" and owner in FUNCTIONS and anchor == owner:
            return FUNCTIONS[owner], SYMBOL_EVIDENCE, "compile-owner-gate"
        if category == "branch" and owner == "GrowPagingSize":
            return FUNCTIONS[owner], GROW_EVIDENCE, "behavior-mutation"
        if category == "branch" and owner == "CalculateSeekCnt":
            return FUNCTIONS[owner], SEEK_EVIDENCE, "behavior-mutation"
    if source_path == "pkg/util/paging/paging_test.go":
        if category == "test" and owner in GO_TESTS and anchor == owner:
            symbol, evidence = GO_TESTS[owner]
            return symbol, evidence, "test-evidence-gate"
        if category == "test_assertion" and owner == "TestGrowPagingSize":
            symbol, evidence = GO_TESTS[owner]
            return symbol, evidence, "test-evidence-gate"
    return None


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-paging-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        ported = ported_mapping(category, source_path, anchor, owner)
        if ported is not None:
            symbol, evidence, mutation_policy = ported
            status = "PORTED"
        elif (
            source_path == "pkg/util/paging/main_test.go"
            and anchor in DECLINED_SUPPORT
            and category in {"test_main", "test_row"}
            and owner == "TestMain"
        ):
            status = "DECLINED"
            symbol = "-"
            evidence = DECLINED_EVIDENCE
            mutation_policy = "classification-evidence-gate"
        else:
            raise RuntimeError(f"unclassified paging obligation: {line}")
        lines.append(
            "\t".join(
                [
                    obligation_id,
                    category,
                    source_path,
                    anchor,
                    node_hash,
                    owner,
                    status,
                    symbol,
                    evidence,
                    mutation_policy,
                ]
            )
        )
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    data = [line for line in lines if line and not line.startswith("#")]
    return [line.split("\t") for line in data][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    if len(rows) != 28:
        raise RuntimeError(f"obligation census drift: {len(rows)}")
    ids: set[str] = set()
    source_anchors: set[tuple[str, str]] = set()
    categories: dict[str, int] = {}
    statuses: dict[str, int] = {}
    declined_anchors: set[str] = set()
    for row in rows:
        if len(row) != 10:
            raise RuntimeError(f"invalid classification row: {row}")
        source_anchor = (row[2], row[3])
        if row[0] in ids or source_anchor in source_anchors:
            raise RuntimeError(f"duplicate obligation row: {row}")
        ids.add(row[0])
        source_anchors.add(source_anchor)
        expected = ported_mapping(row[1], row[2], row[3], row[5])
        if expected is not None:
            symbol, evidence, mutation_policy = expected
            wanted = ("PORTED", symbol, evidence, mutation_policy)
        elif (
            row[2] == "pkg/util/paging/main_test.go"
            and row[3] in DECLINED_SUPPORT
            and row[1] in {"test_main", "test_row"}
            and row[5] == "TestMain"
        ):
            wanted = ("DECLINED", "-", DECLINED_EVIDENCE, "classification-evidence-gate")
            declined_anchors.add(row[3])
        else:
            raise RuntimeError(f"unexpected source row: {row}")
        if tuple(row[6:10]) != wanted:
            raise RuntimeError(f"classification or evidence drift: {row}")
        categories[row[1]] = categories.get(row[1], 0) + 1
        statuses[row[6]] = statuses.get(row[6], 0) + 1
    if categories != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {categories}")
    if statuses != {"DECLINED": 5, "PORTED": 23}:
        raise RuntimeError(f"status census drift: {statuses}")
    if declined_anchors != DECLINED_SUPPORT:
        raise RuntimeError(f"declined support drift: {declined_anchors}")


def tsv_rows(path: Path) -> list[dict[str, str]]:
    lines = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_mutations(root: Path) -> None:
    plan = tsv_rows(root / MUTATION_PLAN)
    results = tsv_rows(root / MUTATION_RESULTS)
    if len(plan) != 7 or len(results) != 28:
        raise RuntimeError(f"mutation census drift: plan={len(plan)} results={len(results)}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if sum(expected.values()) != 28:
        raise RuntimeError(f"mutation plan total drift: {expected}")
    actual = {suite: 0 for suite in expected}
    ids: set[str] = set()
    baselines: set[str] = set()
    for row in results:
        if row["mutation_id"] in ids:
            raise RuntimeError(f"duplicate mutation id: {row['mutation_id']}")
        ids.add(row["mutation_id"])
        suite = row["suite_id"]
        if suite not in actual:
            raise RuntimeError(f"unplanned mutation suite: {suite}")
        actual[suite] += 1
        baselines.add(row["baseline_commit"])
        if row["status"] != "KILLED" or row["restore_status"] != "PASS":
            raise RuntimeError(f"mutation not killed/restored: {row}")
        if int(row["exit_code"]) == 0 or not row["named_test"]:
            raise RuntimeError(f"mutation lacks named failure: {row}")
        sources = row["source_file"].split("|")
        expected_hashes = row["source_sha256"].split("|")
        if len(sources) != len(expected_hashes):
            raise RuntimeError(f"mutation source/hash width drift: {row}")
        for source, expected_hash in zip(sources, expected_hashes):
            if sha256(root / source) != expected_hash:
                raise RuntimeError(f"mutation source drifted: {source}")
    if actual != expected or len(baselines) != 1:
        raise RuntimeError(
            f"mutation receipt drift: expected={expected} actual={actual} baselines={baselines}"
        )


def receipt_contents(root: Path, obligations: list[str]) -> dict[str, object]:
    validate_mutations(root)
    rows = parse_obligations(obligations)
    owned = [
        ARTIFACTS,
        INVENTORY,
        MUTATION_PLAN,
        MUTATION_RESULTS,
        RUST_SOURCE,
        RUST_GATE,
        SCRIPT,
    ]
    return {
        "schema": "pkg-paging-lockdown-v1",
        "go_package": str(PACKAGE),
        "source_seed_commit": "589ea5ec8a20687604cc20a4ebe82fb56137b8b5",
        "artifact_count": 4,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(rows),
        "category_counts": EXPECTED_CATEGORIES,
        "status_counts": {"DECLINED": 5, "PORTED": 23},
        "mutation_suites": len(tsv_rows(root / MUTATION_PLAN)),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def check(root: Path, inventory_only: bool = False) -> None:
    stored_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if artifact_lines(root) != stored_artifacts:
        raise RuntimeError("pkg/util/paging artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    if classified_obligation_lines(raw) != stored:
        raise RuntimeError("pkg/util/paging classifications or evidence drifted")
    stored_raw = ["\t".join(row[:6]) for row in parse_obligations(stored)]
    if raw[1:] != stored_raw:
        raise RuntimeError("pkg/util/paging AST obligation set drifted")
    if inventory_only:
        print("pkg/util/paging inventory: 4 artifacts, 28 AST obligations, 23 PORTED, 5 DECLINED")
        return
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/paging content-addressed receipt drifted")
    print("pkg/util/paging lockdown: 4 artifacts, 28 AST obligations, 23 PORTED, 5 DECLINED")


def write(root: Path, write_receipt: bool) -> None:
    if write_receipt:
        stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
        validate_classifications(stored)
        (root / RECEIPT).write_text(
            json.dumps(receipt_contents(root, stored), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {RECEIPT}")
        return
    artifacts = artifact_lines(root)
    obligations = classified_obligation_lines(raw_obligation_lines(root))
    validate_classifications(obligations)
    (root / ARTIFACTS).write_text("\n".join(artifacts) + "\n", encoding="utf-8")
    (root / INVENTORY).write_text("\n".join(obligations) + "\n", encoding="utf-8")
    print(f"wrote 4 artifacts and {len(obligations) - 2} obligations")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--write-receipt", action="store_true")
    parser.add_argument("--inventory-only", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.write or args.write_receipt:
            write(root, args.write_receipt)
        else:
            check(root, args.inventory_only)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"pkg/util/paging lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
