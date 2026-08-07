#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/util/slice lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys


PACKAGE = Path("pkg/util/slice")
RUST_DIR = Path("rust/crates/tidb-util/src")
ARTIFACTS = RUST_DIR / "slice.artifacts.tsv"
INVENTORY = RUST_DIR / "slice.inventory.tsv"
MUTATION_PLAN = RUST_DIR / "slice.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "slice.mutation-results.tsv"
RECEIPT = RUST_DIR / "slice.receipt.json"
RUST_SOURCE = RUST_DIR / "slice.rs"
RUST_GATE = Path("rust/crates/tidb-util/tests/slice_lockdown.rs")
SCRIPT = Path("rust/scripts/pkg-slice-lockdown.py")
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
    "branch": 2,
    "closure": 1,
    "function": 3,
    "loop": 4,
    "test": 1,
    "test_assertion": 1,
    "test_helper_closure": 2,
    "test_loop": 2,
    "test_main": 1,
    "test_row": 24,
}
FUNCTIONS = {
    "AllOf": "all_of",
    "DeepClone": "deep_clone",
    "Int64sToStrings": "int64s_to_strings",
}
BEHAVIOR_EVIDENCE = {
    "AllOf": "rust-test:all_of_preserves_source_order_short_circuit_and_empty_truth",
    "DeepClone": "rust-test:deep_clone_preserves_nil_empty_and_element_clone_ownership",
    "Int64sToStrings": "rust-test:int64s_to_strings_preserves_source_decimal_domain",
}
DECLINED_SUPPORT = {
    "TestMain",
    "TestMain/composite:1/element:0",
    "TestMain/composite:1/element:1",
    "TestMain/composite:1/element:2",
    "TestMain/composite:1/element:3",
}
DECLINED_EVIDENCE = "source-quote:go_testsetup_and_goleak_only"
SYMBOL_EVIDENCE = "rust-test:slice_lockdown_inventory_is_complete_and_symbols_compile"
GO_TEST_EVIDENCE = "rust-test:TestSlice"


def run(root: Path, command: list[str]) -> str:
    completed = subprocess.run(
        command, cwd=root, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
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
        raise RuntimeError(f"untracked pkg/util/slice artifacts: {untracked}")
    expected = [
        "pkg/util/slice/BUILD.bazel",
        "pkg/util/slice/main_test.go",
        "pkg/util/slice/slice.go",
        "pkg/util/slice/slice_test.go",
    ]
    if paths != expected:
        raise RuntimeError(f"pkg/util/slice artifact drift: expected {expected}, found {paths}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    roles = {
        "pkg/util/slice/BUILD.bazel": "build",
        "pkg/util/slice/main_test.go": "test-support",
        "pkg/util/slice/slice.go": "production",
        "pkg/util/slice/slice_test.go": "test",
    }
    lines = ["# pkg-slice-artifacts-v1"]
    for name, count in EXPECTED_ZERO_CLASSES.items():
        lines.append(f"# zero\t{name}\t{count}")
    lines.append("path\trole\tsha256")
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
    if source_path == "pkg/util/slice/slice.go" and owner in FUNCTIONS:
        if category == "function" and anchor == owner:
            return FUNCTIONS[owner], SYMBOL_EVIDENCE, "compile-owner-gate"
        if category in {"branch", "closure", "loop"}:
            evidence = BEHAVIOR_EVIDENCE[owner]
            if owner == "DeepClone" and category == "loop" and anchor.endswith("/enters"):
                evidence = "rust-test:deep_clone_invokes_clone_once_per_item_in_source_order"
            return FUNCTIONS[owner], evidence, "behavior-mutation"
    if source_path == "pkg/util/slice/slice_test.go" and owner == "TestSlice":
        if category in {
            "test", "test_assertion", "test_helper_closure", "test_loop", "test_row"
        } and anchor.startswith("TestSlice"):
            return "TestSlice", GO_TEST_EVIDENCE, "test-evidence-gate"
    return None


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-slice-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        ported = ported_mapping(category, source_path, anchor, owner)
        if ported is not None:
            symbol, evidence, policy = ported
            status = "PORTED"
        elif (
            source_path == "pkg/util/slice/main_test.go"
            and anchor in DECLINED_SUPPORT
            and category in {"test_main", "test_row"}
            and owner == "TestMain"
        ):
            status, symbol, evidence, policy = (
                "DECLINED", "-", DECLINED_EVIDENCE, "classification-evidence-gate"
            )
        else:
            raise RuntimeError(f"unclassified slice obligation: {line}")
        lines.append("\t".join([
            obligation_id, category, source_path, anchor, node_hash, owner,
            status, symbol, evidence, policy,
        ]))
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    data = [line for line in lines if line and not line.startswith("#")]
    return [line.split("\t") for line in data][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    if len(rows) != 41:
        raise RuntimeError(f"obligation census drift: {len(rows)}")
    ids: set[str] = set()
    source_anchors: set[tuple[str, str]] = set()
    categories: dict[str, int] = {}
    statuses: dict[str, int] = {}
    declined: set[str] = set()
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
            wanted = ("PORTED", *expected)
        elif (
            row[2] == "pkg/util/slice/main_test.go"
            and row[3] in DECLINED_SUPPORT
            and row[1] in {"test_main", "test_row"}
            and row[5] == "TestMain"
        ):
            wanted = ("DECLINED", "-", DECLINED_EVIDENCE, "classification-evidence-gate")
            declined.add(row[3])
        else:
            raise RuntimeError(f"unexpected source row: {row}")
        if tuple(row[6:10]) != wanted:
            raise RuntimeError(f"classification or evidence drift: {row}")
        categories[row[1]] = categories.get(row[1], 0) + 1
        statuses[row[6]] = statuses.get(row[6], 0) + 1
    if categories != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {categories}")
    if statuses != {"DECLINED": 5, "PORTED": 36}:
        raise RuntimeError(f"status census drift: {statuses}")
    if declined != DECLINED_SUPPORT:
        raise RuntimeError(f"declined support drift: {declined}")


def tsv_rows(path: Path) -> list[dict[str, str]]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines()
             if line and not line.startswith("#")]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_mutations(root: Path) -> None:
    plan = tsv_rows(root / MUTATION_PLAN)
    results = tsv_rows(root / MUTATION_RESULTS)
    if len(plan) != 7 or len(results) != 23:
        raise RuntimeError(f"mutation census drift: plan={len(plan)} results={len(results)}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if sum(expected.values()) != 23:
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
        hashes = row["source_sha256"].split("|")
        if len(sources) != len(hashes):
            raise RuntimeError(f"mutation source/hash width drift: {row}")
        for source, expected_hash in zip(sources, hashes):
            if sha256(root / source) != expected_hash:
                raise RuntimeError(f"mutation source drifted: {source}")
    if actual != expected or len(baselines) != 1:
        raise RuntimeError(f"mutation receipt drift: {actual} {baselines}")


def receipt_contents(root: Path, obligations: list[str]) -> dict[str, object]:
    validate_mutations(root)
    owned = [ARTIFACTS, INVENTORY, MUTATION_PLAN, MUTATION_RESULTS,
             RUST_SOURCE, RUST_GATE, SCRIPT]
    return {
        "schema": "pkg-slice-lockdown-v1",
        "go_package": str(PACKAGE),
        "source_seed_commit": "3bc65f2096f2b888f4119f04e45034c8f16f53ab",
        "artifact_count": 4,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(parse_obligations(obligations)),
        "category_counts": EXPECTED_CATEGORIES,
        "status_counts": {"DECLINED": 5, "PORTED": 36},
        "mutation_suites": len(tsv_rows(root / MUTATION_PLAN)),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def check(root: Path, inventory_only: bool = False) -> None:
    stored_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if artifact_lines(root) != stored_artifacts:
        raise RuntimeError("pkg/util/slice artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    if classified_obligation_lines(raw) != stored:
        raise RuntimeError("pkg/util/slice classifications or evidence drifted")
    if raw[1:] != ["\t".join(row[:6]) for row in parse_obligations(stored)]:
        raise RuntimeError("pkg/util/slice AST obligation set drifted")
    if inventory_only:
        print("pkg/util/slice inventory: 4 artifacts, 41 AST obligations, 36 PORTED, 5 DECLINED")
        return
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/slice content-addressed receipt drifted")
    print("pkg/util/slice lockdown: 4 artifacts, 41 AST obligations, 36 PORTED, 5 DECLINED")


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
    try:
        root = args.root.resolve()
        if args.write or args.write_receipt:
            write(root, args.write_receipt)
        else:
            check(root, args.inventory_only)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"pkg/util/slice lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
