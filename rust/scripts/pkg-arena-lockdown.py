#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/util/arena lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


PACKAGE = Path("pkg/util/arena")
RUST_DIR = Path("rust/crates/tidb-util/src")
ARTIFACTS = RUST_DIR / "arena.artifacts.tsv"
INVENTORY = RUST_DIR / "arena.inventory.tsv"
MUTATION_PLAN = RUST_DIR / "arena.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "arena.mutation-results.tsv"
RECEIPT = RUST_DIR / "arena.receipt.json"
RUST_SOURCE = RUST_DIR / "arena.rs"
RUST_GATE = Path("rust/crates/tidb-util/tests/arena_lockdown.rs")
SCRIPT = Path("rust/scripts/pkg-arena-lockdown.py")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
ALLOWED_STATUSES = {"PORTED", "DECLINED", "UNREACHABLE"}
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
    "declaration": 3,
    "field": 5,
    "function": 7,
    "test": 2,
    "test_assertion": 18,
    "test_main": 1,
    "test_row": 4,
    "test_support_const": 4,
    "var": 2,
}

SOURCE_SYMBOLS = {
    "NewAllocator": "SimpleAllocator::new",
    "SimpleAllocator.Alloc": "SimpleAllocator::alloc",
    "SimpleAllocator.AllocWithLen": "SimpleAllocator::alloc_with_len",
    "SimpleAllocator.Reset": "SimpleAllocator::reset",
    "stdAllocator.Alloc": "StdAllocator::alloc",
    "stdAllocator.AllocWithLen": "StdAllocator::alloc_with_len",
    "stdAllocator.Reset": "StdAllocator::reset",
    "type:Allocator": "Allocator",
    "type:SimpleAllocator": "SimpleAllocator",
    "type:stdAllocator": "StdAllocator",
    "var:StdAllocator:0": "StdAllocator",
    "var:_:0": "StdAllocator",
}

EVIDENCE_TESTS = {
    "safe_rust_owned_buffers_are_zeroed_after_reset",
    "simple_arena_allocator",
    "source_simple_length_over_capacity_panics_after_allocating_capacity",
    "source_std_length_over_capacity_panics",
    "source_strict_exact_fit_and_reset_contracts",
    "std_allocator",
}

DECLINED_KEYS = {
    ("pkg/util/arena/arena.go", "SimpleAllocator.Alloc"),
    ("pkg/util/arena/arena.go", "SimpleAllocator.Alloc/if:1/true"),
    ("pkg/util/arena/arena.go", "SimpleAllocator.AllocWithLen"),
    ("pkg/util/arena/main_test.go", "TestMain"),
    ("pkg/util/arena/main_test.go", "TestMain/composite:1/element:0"),
    ("pkg/util/arena/main_test.go", "TestMain/composite:1/element:1"),
    ("pkg/util/arena/main_test.go", "TestMain/composite:1/element:2"),
    ("pkg/util/arena/main_test.go", "TestMain/composite:1/element:3"),
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
    platform_variants = 0
    for path in go_paths:
        if any(part in platforms for part in Path(path).stem.split("_")[1:]):
            platform_variants += 1
    return {
        "build_tags": sum("//go:build" in text or "// +build" in text for text in texts.values()),
        "platform_variants": platform_variants,
        "code_generated": sum("// Code generated" in text for text in texts.values()),
        "go_generate": sum("//go:generate" in text for text in texts.values()),
        "go_embed": sum("//go:embed" in text for text in texts.values()),
        "tracked_testdata": sum("/testdata/" in f"/{path}/" for path in paths),
    }


def artifact_lines(root: Path) -> list[str]:
    paths, untracked = direct_package_paths(root)
    if untracked:
        raise RuntimeError(f"untracked pkg/util/arena artifacts: {untracked}")
    if len(paths) != 4:
        raise RuntimeError(f"expected 4 pkg/util/arena artifacts, found {len(paths)}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    lines = ["# pkg-arena-artifacts-v1"]
    for name, count in EXPECTED_ZERO_CLASSES.items():
        lines.append(f"# zero\t{name}\t{count}")
    lines.append("path\trole\tsha256")
    for path in paths:
        if path.endswith("_test.go"):
            role = "test"
        elif path.endswith(".go"):
            role = "production"
        elif path.endswith("BUILD.bazel"):
            role = "build"
        else:
            raise RuntimeError(f"unclassified artifact: {path}")
        lines.append(f"{path}\t{role}\t{sha256(root / path)}")
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


def production_evidence(owner: str, anchor: str) -> str:
    if owner == "NewAllocator":
        return "source_strict_exact_fit_and_reset_contracts"
    if owner == "SimpleAllocator.Alloc":
        return "source_strict_exact_fit_and_reset_contracts"
    if owner == "SimpleAllocator.AllocWithLen":
        return "source_simple_length_over_capacity_panics_after_allocating_capacity"
    if owner == "SimpleAllocator.Reset":
        return "source_strict_exact_fit_and_reset_contracts"
    if owner == "stdAllocator.AllocWithLen":
        return "source_std_length_over_capacity_panics"
    if owner.startswith("stdAllocator"):
        return "std_allocator"
    if owner in {"type:Allocator", "type:SimpleAllocator", "var:_:0"}:
        return "simple_arena_allocator"
    if owner in {"type:stdAllocator", "var:StdAllocator:0"}:
        return "std_allocator"
    raise RuntimeError(f"missing production evidence for {owner} at {anchor}")


def test_evidence(owner: str, anchor: str) -> str:
    if owner == "TestSimpleArenaAllocator":
        return "simple_arena_allocator"
    if owner == "TestStdAllocator":
        return "std_allocator"
    if anchor in {"const:arenaCap:0", "const:allocCapOut:0"}:
        return "simple_arena_allocator"
    if anchor in {"const:allocCapSmall:0", "const:allocCapMedium:0"}:
        return "simple_arena_allocator"
    raise RuntimeError(f"missing test evidence for {owner} at {anchor}")


def decline_evidence(source_path: str) -> str:
    if source_path == "pkg/util/arena/main_test.go":
        return "go-harness:testsetup+goleak-not-rust-runtime"
    return "go-probe:reset-reuse_bytes=[7_8];rust-owned-vec"


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-arena-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        if (source_path, anchor) in DECLINED_KEYS:
            fields = [
                obligation_id, category, source_path, anchor, node_hash, owner,
                "DECLINED", "-", decline_evidence(source_path), "source-measured-decline",
            ]
        elif source_path == "pkg/util/arena/arena.go":
            try:
                symbol = SOURCE_SYMBOLS[owner]
            except KeyError as error:
                raise RuntimeError(f"missing Rust owner for {owner}") from error
            policy = "probe:P001-simple-fit-offset" if category == "branch" else "compile-owner-gate"
            fields = [
                obligation_id, category, source_path, anchor, node_hash, owner,
                "PORTED", symbol, "rust-test:" + production_evidence(owner, anchor), policy,
            ]
        elif source_path == "pkg/util/arena/arena_test.go":
            fields = [
                obligation_id, category, source_path, anchor, node_hash, owner,
                "PORTED", "SimpleAllocator" if owner != "TestStdAllocator" else "StdAllocator",
                "rust-test:" + test_evidence(owner, anchor), "source-row-hash",
            ]
        else:
            raise RuntimeError(f"unclassified arena obligation: {line}")
        if any("\t" in field or "\n" in field for field in fields):
            raise RuntimeError(f"invalid TSV field in {fields}")
        lines.append("\t".join(fields))
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    return [line.split("\t") for line in lines if line and not line.startswith("#")][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    if len(rows) != 48:
        raise RuntimeError(f"obligation census drift: {len(rows)}")
    ids: set[str] = set()
    category_counts: dict[str, int] = {}
    status_counts = {status: 0 for status in ALLOWED_STATUSES}
    actual_declines: set[tuple[str, str]] = set()
    allowed_symbols = set(SOURCE_SYMBOLS.values()) | {"SimpleAllocator", "StdAllocator"}
    for row in rows:
        if len(row) != 10:
            raise RuntimeError(f"invalid obligation row: {row}")
        if row[0] in ids:
            raise RuntimeError(f"duplicate obligation id: {row[0]}")
        ids.add(row[0])
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        if row[6] not in ALLOWED_STATUSES or not row[8] or not row[9]:
            raise RuntimeError(f"invalid classification: {row}")
        status_counts[row[6]] += 1
        if row[6] == "PORTED":
            if row[7] not in allowed_symbols:
                raise RuntimeError(f"ungated PORTED symbol: {row}")
            evidence = row[8].removeprefix("rust-test:")
            if evidence not in EVIDENCE_TESTS:
                raise RuntimeError(f"unknown Rust evidence test: {row}")
        else:
            if row[7] != "-":
                raise RuntimeError(f"non-PORTED row claims a symbol: {row}")
            actual_declines.add((row[2], row[3]))
    if category_counts != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {category_counts}")
    expected_statuses = {"PORTED": 40, "DECLINED": 8, "UNREACHABLE": 0}
    if status_counts != expected_statuses:
        raise RuntimeError(f"status census drift: {status_counts}")
    if actual_declines != DECLINED_KEYS:
        raise RuntimeError(f"decline set drift: {actual_declines}")


def tsv_rows(path: Path) -> list[dict[str, str]]:
    lines = [
        line for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_mutations(root: Path) -> None:
    plan = tsv_rows(root / MUTATION_PLAN)
    results = tsv_rows(root / MUTATION_RESULTS)
    if len(plan) != 6 or len(results) != 15:
        raise RuntimeError(f"mutation census drift: plan={len(plan)} results={len(results)}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if sum(expected.values()) != 15:
        raise RuntimeError(f"mutation plan total drift: {expected}")
    actual = {suite: 0 for suite in expected}
    ids: set[str] = set()
    baselines: set[str] = set()
    for row in results:
        mutation_id = row["mutation_id"]
        if mutation_id in ids:
            raise RuntimeError(f"duplicate mutation id: {mutation_id}")
        ids.add(mutation_id)
        suite = row["suite_id"]
        if suite not in actual:
            raise RuntimeError(f"unplanned mutation suite: {suite}")
        actual[suite] += 1
        baselines.add(row["baseline_commit"])
        if row["status"] != "KILLED" or row["restore_status"] != "PASS":
            raise RuntimeError(f"mutation not killed and restored: {row}")
        if int(row["exit_code"]) == 0 or not row["named_test"]:
            raise RuntimeError(f"mutation lacks a named failing command: {row}")
        if sha256(root / row["source_file"]) != row["source_sha256"]:
            raise RuntimeError(f"mutation source drifted: {row['source_file']}")
    if actual != expected:
        raise RuntimeError(f"mutation suite counts drift: expected={expected} actual={actual}")
    if len(baselines) != 1 or "" in baselines:
        raise RuntimeError(f"mutation baseline drift: {baselines}")


def receipt_contents(root: Path, obligations: list[str]) -> dict[str, object]:
    validate_mutations(root)
    rows = parse_obligations(obligations)
    category_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for row in rows:
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        status_counts[row[6]] = status_counts.get(row[6], 0) + 1
    owned = [
        ARTIFACTS, INVENTORY, MUTATION_PLAN, MUTATION_RESULTS, RUST_SOURCE,
        RUST_GATE, SCRIPT,
    ]
    return {
        "schema": "pkg-arena-lockdown-v1",
        "go_package": str(PACKAGE),
        "source_seed_commit": "fb8490526c5e30408fd0a444057b5b2e7072ad61",
        "artifact_count": 4,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "mutation_suites": len(tsv_rows(root / MUTATION_PLAN)),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def check(root: Path, inventory_only: bool = False) -> None:
    expected_artifacts = artifact_lines(root)
    stored_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if expected_artifacts != stored_artifacts:
        raise RuntimeError("pkg/util/arena artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    expected_inventory = classified_obligation_lines(raw)
    if expected_inventory != stored:
        raise RuntimeError("pkg/util/arena classifications or evidence drifted")
    stored_raw = ["\t".join(row[:6]) for row in parse_obligations(stored)]
    if raw[1:] != stored_raw:
        raise RuntimeError("pkg/util/arena AST obligation set drifted")
    if inventory_only:
        print(
            f"pkg/util/arena inventory: 4 artifacts, {len(stored_raw)} AST obligations, "
            "40 PORTED, 8 DECLINED"
        )
        return
    if not (root / RECEIPT).is_file():
        raise RuntimeError("pkg/util/arena content-addressed receipt is missing")
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/arena content-addressed receipt drifted")
    print(
        f"pkg/util/arena lockdown: 4 artifacts, {len(stored_raw)} AST obligations, "
        "40 PORTED, 8 DECLINED"
    )


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
        print(f"pkg/util/arena lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
