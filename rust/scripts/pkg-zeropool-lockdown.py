#!/usr/bin/env python3
"""Generate and check the complete pkg/util/zeropool lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys


PACKAGE = Path("pkg/util/zeropool")
RUST_ROOT = Path("rust/crates/tidb-util")
RUST_DIR = RUST_ROOT / "src/zeropool"
ARTIFACTS = RUST_DIR / "zeropool.artifacts.tsv"
INVENTORY = RUST_DIR / "zeropool.inventory.tsv"
SEMANTICS = RUST_DIR / "zeropool.semantic-divergences.tsv"
MUTATION_PLAN = RUST_DIR / "zeropool.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "zeropool.mutation-results.tsv"
RECEIPT = RUST_DIR / "zeropool.receipt.json"
RUST_SOURCE = RUST_DIR / "mod.rs"
RUST_BENCH = RUST_ROOT / "../tidb-util/benches/zeropool.rs"
RUST_GATE = RUST_ROOT / "tests/zeropool_lockdown.rs"
SCRIPT = Path("rust/scripts/pkg-zeropool-lockdown.py")
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
    "benchmark": 4,
    "branch": 4,
    "closure": 1,
    "declaration": 1,
    "field": 2,
    "function": 3,
    "test": 1,
    "test_assertion": 6,
    "test_branch": 2,
    "test_helper_closure": 14,
    "test_loop": 14,
    "test_row": 3,
}
FUNCTIONS = {"New": "Pool::new", "Pool[T].Get": "Pool::get", "Pool[T].Put": "Pool::put"}
BENCHMARKS = {
    "BenchmarkZeropoolPool",
    "BenchmarkSyncPoolValue",
    "BenchmarkSyncPoolNewPointer",
    "BenchmarkSyncPoolPointer",
}
GO_TEST_EVIDENCE = "rust-test:TestPool"
BENCH_EVIDENCE = {
    name: f"rust-bench:zeropool:{name}" for name in BENCHMARKS
}
SYMBOL_EVIDENCE = "rust-test:zeropool_lockdown_inventory_is_complete_and_symbols_compile"
PRODUCTION_DECLINES = {
    ("field", "type:Pool/field:1:pointers"),
    ("branch", "Pool[T].Put/if:1/false"),
    ("branch", "Pool[T].Put/if:1/true"),
}
SEMANTIC_LINES = [
    "# pkg-zeropool-semantic-divergences-v1",
    "id\tsource_contract\tstatus\trust_boundary\tevidence\tmutation_policy",
    "S01\tPool.pointers stores reusable empty *T containers\tDECLINED\tRust stores typed T values directly and has no secondary pointer pool\tsource-quote:pool.go:Pool/field:1:pointers\tclassification-evidence-gate",
    "S02\tPool[T any] zero value returns a zero T for every possible T\tDECLINED\tRust requires T: Default for the zero-value path\tsource-quote:pool.go:var-zero-T\tclassification-evidence-gate",
    "S03\tNew accepts a nil item function and empty Get panics\tDECLINED\tRust closure values are non-null and cannot represent this Go panic boundary\tgo-oracle:nil_factory_panic:runtime.errorString:runtime error: invalid memory address or nil pointer dereference\tclassification-evidence-gate",
    "S04\tNew accepts item functions without Send Sync or static lifetime constraints\tDECLINED\tRust requires Fn() -> T + Send + Sync + 'static for a shared pool\tsource-quote:pool.go:New-item-func-type\tclassification-evidence-gate",
    "S05\tGet's pooled any value can fail a private *T assertion\tUNREACHABLE\tRust's Vec<T> storage makes the mismatched private assertion unrepresentable\tsource-analysis:only-Put-writes-items-as-*T\tclassification-evidence-gate",
    "S06\tGet clears the extracted *T and returns that pointer to pointers\tDECLINED\tRust moves T out and drops no reusable pointer container\tsource-quote:pool.go:Get-clear-and-pointers-Put\tclassification-evidence-gate",
    "S07\tPut reuses a non-nil *T from the secondary pointers pool\tDECLINED\tRust Vec<T> capacity is the only reuse mechanism\tsource-quote:pool.go:Put-pointers-Get\tclassification-evidence-gate",
    "S08\tPut allocates new(T) when the secondary pointers pool is empty\tDECLINED\tRust Vec<T> growth replaces pointer allocation without a pointer object\tsource-quote:pool.go:Put-new-T\tclassification-evidence-gate",
    "S09\tsync.Pool may evict a stored value across garbage collection\tDECLINED\tRust retains entries until Get or Pool drop; it has no GC eviction boundary\tgo-oracle:two_gc_get:-1-from-factory\tclassification-evidence-gate",
]


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
        "mips64", "mips64le", "mipsle", "ppc64", "ppc64le", "riscv64", "s390x",
        "wasm",
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
        raise RuntimeError(f"untracked pkg/util/zeropool artifacts: {untracked}")
    expected = [
        "pkg/util/zeropool/BUILD.bazel",
        "pkg/util/zeropool/pool.go",
        "pkg/util/zeropool/pool_test.go",
    ]
    if paths != expected:
        raise RuntimeError(f"pkg/util/zeropool artifact drift: expected {expected}, found {paths}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    roles = {
        expected[0]: "build",
        expected[1]: "production",
        expected[2]: "test-benchmark",
    }
    lines = ["# pkg-zeropool-artifacts-v1"]
    lines.extend(f"# zero\t{name}\t{count}" for name, count in EXPECTED_ZERO_CLASSES.items())
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


def mapping(category: str, source: str, anchor: str, owner: str) -> tuple[str, str, str] | None:
    if source == "pkg/util/zeropool/pool.go":
        if category == "declaration" and anchor == "type:Pool":
            return "Pool", SYMBOL_EVIDENCE, "compile-owner-gate"
        if category == "field" and anchor == "type:Pool/field:0:items":
            return "Pool::items", SYMBOL_EVIDENCE, "compile-owner-gate"
        if category == "function" and anchor == owner and owner in FUNCTIONS:
            return FUNCTIONS[owner], SYMBOL_EVIDENCE, "compile-owner-gate"
        if category == "closure" and owner == "New":
            return "Pool::new", "rust-test:source_factory_and_zero_value_boundaries_are_exact", "behavior-mutation"
        if category == "branch" and owner == "Pool[T].Get":
            return "Pool::get", "rust-test:source_factory_and_zero_value_boundaries_are_exact", "behavior-mutation"
    if source == "pkg/util/zeropool/pool_test.go":
        if owner == "TestPool" and category in {
            "test", "test_assertion", "test_branch", "test_helper_closure", "test_loop"
        } and anchor.startswith("TestPool"):
            return "TestPool", GO_TEST_EVIDENCE, "test-evidence-gate"
        if owner in BENCHMARKS and category in {
            "benchmark", "test_helper_closure", "test_loop", "test_row"
        } and anchor.startswith(owner):
            return owner, BENCH_EVIDENCE[owner], "benchmark-evidence-gate"
    return None


def declined_mapping(category: str, source: str, anchor: str) -> tuple[str, str, str] | None:
    if source == "pkg/util/zeropool/pool.go" and (category, anchor) in PRODUCTION_DECLINES:
        return "-", f"semantic:zeropool:{anchor}", "classification-evidence-gate"
    return None


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-zeropool-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source, anchor, node_hash, owner = line.split("\t")
        ported = mapping(category, source, anchor, owner)
        declined = declined_mapping(category, source, anchor)
        if ported is not None:
            status, result = "PORTED", ported
        elif declined is not None:
            status, result = "DECLINED", declined
        else:
            raise RuntimeError(f"unclassified zeropool obligation: {line}")
        symbol, evidence, policy = result
        lines.append("\t".join([
            obligation_id, category, source, anchor, node_hash, owner,
            status, symbol, evidence, policy,
        ]))
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    data = [line for line in lines if line and not line.startswith("#")]
    return [line.split("\t") for line in data][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    if len(rows) != 55:
        raise RuntimeError(f"obligation census drift: {len(rows)}")
    ids: set[str] = set()
    anchors: set[tuple[str, str]] = set()
    categories: dict[str, int] = {}
    statuses: dict[str, int] = {}
    for row in rows:
        if len(row) != 10:
            raise RuntimeError(f"invalid classification row: {row}")
        if row[0] in ids or (row[2], row[3]) in anchors:
            raise RuntimeError(f"duplicate obligation row: {row}")
        ids.add(row[0])
        anchors.add((row[2], row[3]))
        expected = mapping(row[1], row[2], row[3], row[5])
        declined = declined_mapping(row[1], row[2], row[3])
        wanted = ("PORTED", *expected) if expected is not None else (
            ("DECLINED", *declined) if declined is not None else None
        )
        if wanted is None or tuple(row[6:10]) != wanted:
            raise RuntimeError(f"classification or evidence drift: {row}")
        categories[row[1]] = categories.get(row[1], 0) + 1
        statuses[row[6]] = statuses.get(row[6], 0) + 1
    if categories != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {categories}")
    if statuses != {"DECLINED": 3, "PORTED": 52}:
        raise RuntimeError(f"status census drift: {statuses}")


def validate_semantics(root: Path) -> None:
    stored = (root / SEMANTICS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if stored != SEMANTIC_LINES:
        raise RuntimeError("zeropool semantic divergence evidence drifted")
    rows = [line.split("\t") for line in stored[2:]]
    if len(rows) != 9 or any(len(row) != 6 for row in rows):
        raise RuntimeError("semantic census drift")
    if {row[2] for row in rows} != {"DECLINED", "UNREACHABLE"}:
        raise RuntimeError("semantic status drift")


def tsv_rows(path: Path) -> list[dict[str, str]]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines()
             if line and not line.startswith("#")]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_source_evidence(
    root: Path,
    row: dict[str, str],
    source_field: str,
    hash_field: str,
    evidence_kind: str,
) -> None:
    sources = row[source_field].split("|")
    hashes = row[hash_field].split("|")
    if len(sources) != len(hashes):
        raise RuntimeError(f"{evidence_kind} source/hash width drift: {row}")
    for source, expected_hash in zip(sources, hashes):
        path = root / source
        if not source or not path.is_file():
            raise RuntimeError(f"{evidence_kind} source is not a file: {source}")
        if not expected_hash or sha256(path) != expected_hash:
            raise RuntimeError(f"{evidence_kind} source drifted: {source}")


def validate_mutations(root: Path) -> None:
    plan = tsv_rows(root / MUTATION_PLAN)
    if len(plan) != 8:
        raise RuntimeError(f"mutation plan census drift: {len(plan)}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if len(expected) != len(plan):
        raise RuntimeError(f"duplicate mutation suite: {expected}")
    if sum(expected.values()) != 33:
        raise RuntimeError(f"mutation plan total drift: {expected}")
    expected_baselines = {row["suite_id"]: row["baseline_commit"] for row in plan}
    for row in plan:
        validate_source_evidence(
            root, row, "source_file", "source_sha256", "mutation plan"
        )
    results = tsv_rows(root / MUTATION_RESULTS)
    if len(results) != 33:
        raise RuntimeError(f"mutation result census drift: {len(results)}")
    actual = {suite: 0 for suite in expected}
    ids: set[str] = set()
    for row in results:
        if row["mutation_id"] in ids:
            raise RuntimeError(f"duplicate mutation id: {row['mutation_id']}")
        ids.add(row["mutation_id"])
        suite = row["suite_id"]
        if suite not in actual:
            raise RuntimeError(f"unplanned mutation suite: {suite}")
        actual[suite] += 1
        if row["baseline_commit"] != expected_baselines[suite]:
            raise RuntimeError(f"mutation baseline drift: {row}")
        if row["status"] != "KILLED" or row["restore_status"] != "PASS":
            raise RuntimeError(f"mutation not killed/restored: {row}")
        if int(row["exit_code"]) == 0 or not row["named_test"]:
            raise RuntimeError(f"mutation lacks named failure: {row}")
        validate_source_evidence(
            root, row, "source_file", "source_sha256", "mutation result"
        )
    if actual != expected:
        raise RuntimeError(f"mutation receipt drift: {actual}")


def receipt_contents(root: Path, obligations: list[str]) -> dict[str, object]:
    validate_semantics(root)
    validate_mutations(root)
    owned = [ARTIFACTS, INVENTORY, SEMANTICS, MUTATION_PLAN, MUTATION_RESULTS,
             RUST_SOURCE, RUST_BENCH, RUST_GATE, SCRIPT]
    return {
        "schema": "pkg-zeropool-lockdown-v2",
        "go_package": str(PACKAGE),
        "source_seed_commit": "e67e11b83b50a0a13ff59c80e6f524558585f48c",
        "artifact_count": 3,
        "semantic_divergence_count": 9,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(parse_obligations(obligations)),
        "category_counts": EXPECTED_CATEGORIES,
        "status_counts": {"DECLINED": 3, "PORTED": 52},
        "semantic_status_counts": {"DECLINED": 8, "UNREACHABLE": 1},
        "mutation_suites": len(tsv_rows(root / MUTATION_PLAN)),
        "mutation_count": sum(
            int(row["mutation_count"]) for row in tsv_rows(root / MUTATION_PLAN)
        ),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def check(root: Path, inventory_only: bool = False) -> None:
    if artifact_lines(root) != (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines():
        raise RuntimeError("pkg/util/zeropool artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    validate_semantics(root)
    if classified_obligation_lines(raw) != stored:
        raise RuntimeError("zeropool classifications or evidence drifted")
    if raw[1:] != ["\t".join(row[:6]) for row in parse_obligations(stored)]:
        raise RuntimeError("zeropool AST obligation set drifted")
    if inventory_only:
        print("pkg/util/zeropool inventory: 3 artifacts, 55 AST obligations, 52 PORTED, 3 DECLINED, 9 semantic rows")
        return
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/zeropool content-addressed receipt drifted")
    print("pkg/util/zeropool lockdown: 3 artifacts, 55 AST obligations, 52 PORTED, 3 DECLINED, 9 semantic rows")


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
    (root / SEMANTICS).write_text("\n".join(SEMANTIC_LINES) + "\n", encoding="utf-8")
    print(f"wrote 3 artifacts and {len(obligations) - 2} obligations")


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
        print(f"pkg/util/zeropool lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
