#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/util/queue lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


PACKAGE = Path("pkg/util/queue")
RUST_DIR = Path("rust/crates/tidb-util/src")
ARTIFACTS = RUST_DIR / "queue.artifacts.tsv"
INVENTORY = RUST_DIR / "queue.inventory.tsv"
MUTATION_PLAN = RUST_DIR / "queue.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "queue.mutation-results.tsv"
RECEIPT = RUST_DIR / "queue.receipt.json"
RUST_SOURCE = RUST_DIR / "queue.rs"
RUST_GATE = Path("rust/crates/tidb-util/tests/queue_lockdown.rs")
SCRIPT = Path("rust/scripts/pkg-queue-lockdown.py")
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
    "branch": 8,
    "declaration": 1,
    "field": 4,
    "function": 8,
    "loop": 2,
    "test": 1,
    "test_assertion": 21,
    "test_helper_closure": 5,
}

SOURCE_SYMBOLS = {
    "NewQueue": "Queue::new",
    "Queue[T].Cap": "Queue::cap",
    "Queue[T].Clear": "Queue::clear",
    "Queue[T].ClearAndExpandIfNeed": "Queue::clear_and_expand_if_need",
    "Queue[T].IsEmpty": "Queue::is_empty",
    "Queue[T].Len": "Queue::len",
    "Queue[T].Pop": "Queue::pop",
    "Queue[T].Push": "Queue::push",
    "type:Queue": "Queue",
}

EVIDENCE_TESTS = {
    "basic_operations",
    "circular_buffer_behavior",
    "clear_operation",
    "panic_on_empty_pop",
    "source_clear_and_expand_contracts",
    "source_clear_retains_slots_until_overwrite_or_expand",
    "source_default_and_zero_capacity_constructor_are_distinct",
    "source_wrapped_growth_preserves_fifo_order",
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
        raise RuntimeError(f"untracked pkg/util/queue artifacts: {untracked}")
    if len(paths) != 3:
        raise RuntimeError(f"expected 3 pkg/util/queue artifacts, found {len(paths)}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    lines = ["# pkg-queue-artifacts-v1"]
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
    if owner == "NewQueue":
        return "source_default_and_zero_capacity_constructor_are_distinct"
    if owner == "Queue[T].Push":
        if "/if:1/" in anchor:
            return "source_default_and_zero_capacity_constructor_are_distinct"
        return "source_wrapped_growth_preserves_fifo_order"
    if owner == "Queue[T].Pop":
        if anchor.endswith("/true"):
            return "panic_on_empty_pop"
        return "source_wrapped_growth_preserves_fifo_order"
    if owner == "Queue[T].Clear":
        return "source_clear_retains_slots_until_overwrite_or_expand"
    if owner == "Queue[T].ClearAndExpandIfNeed":
        return "source_clear_and_expand_contracts"
    return "basic_operations"


def production_probe(owner: str, anchor: str) -> str:
    if owner == "Queue[T].Push":
        if "/if:1/" in anchor:
            return "P001-construction-state"
        return "P002-fifo-growth"
    if owner == "Queue[T].Pop":
        return "P003-pop"
    if owner in {"Queue[T].Clear", "Queue[T].ClearAndExpandIfNeed"}:
        return "P004-clear-expand"
    return "P001-construction-state"


def test_evidence(category: str, anchor: str) -> str:
    if category == "test":
        return "basic_operations"
    if category == "test_helper_closure":
        index = int(anchor.rsplit(":", 1)[1])
        return {
            1: "basic_operations",
            2: "clear_operation",
            3: "panic_on_empty_pop",
            4: "panic_on_empty_pop",
            5: "circular_buffer_behavior",
        }[index]
    match = re.search(r"/assertion:(\d+):", anchor)
    if match is None:
        raise RuntimeError(f"unknown TestQueue evidence anchor: {anchor}")
    index = int(match.group(1))
    if index <= 12:
        return "basic_operations"
    if index <= 15:
        return "clear_operation"
    if index == 16:
        return "panic_on_empty_pop"
    return "circular_buffer_behavior"


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-queue-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        if source_path == "pkg/util/queue/queue.go":
            try:
                symbol = SOURCE_SYMBOLS[owner]
            except KeyError as error:
                raise RuntimeError(f"missing Rust owner for {owner}") from error
            evidence = "rust-test:" + production_evidence(owner, anchor)
            if category in {"branch", "closure", "loop", "short_circuit"}:
                policy = "probe:" + production_probe(owner, anchor)
            else:
                policy = "compile-owner-gate"
        elif source_path == "pkg/util/queue/queue_test.go" and owner == "TestQueue":
            symbol = "Queue"
            evidence = "rust-test:" + test_evidence(category, anchor)
            policy = "source-row-hash"
        else:
            raise RuntimeError(f"unclassified queue obligation: {line}")
        fields = [
            obligation_id, category, source_path, anchor, node_hash, owner,
            "PORTED", symbol, evidence, policy,
        ]
        if any("\t" in field or "\n" in field for field in fields):
            raise RuntimeError(f"invalid TSV field in {fields}")
        lines.append("\t".join(fields))
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    return [line.split("\t") for line in lines if line and not line.startswith("#")][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    if len(rows) != 50:
        raise RuntimeError(f"obligation census drift: {len(rows)}")
    ids: set[str] = set()
    allowed_symbols = set(SOURCE_SYMBOLS.values()) | {"Queue"}
    category_counts: dict[str, int] = {}
    status_counts = {status: 0 for status in ALLOWED_STATUSES}
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
        if row[6] == "PORTED" and row[7] not in allowed_symbols:
            raise RuntimeError(f"ungated PORTED symbol: {row}")
        if row[6] != "PORTED" and row[7] != "-":
            raise RuntimeError(f"non-PORTED row claims a symbol: {row}")
        if row[6] == "PORTED":
            evidence = row[8].removeprefix("rust-test:")
            if evidence not in EVIDENCE_TESTS:
                raise RuntimeError(f"unknown Rust evidence test: {row}")
    if category_counts != EXPECTED_CATEGORIES:
        raise RuntimeError(f"category census drift: {category_counts}")
    expected_statuses = {"PORTED": 50, "DECLINED": 0, "UNREACHABLE": 0}
    if status_counts != expected_statuses:
        raise RuntimeError(f"status census drift: {status_counts}")


def tsv_rows(path: Path) -> list[dict[str, str]]:
    lines = [
        line for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_mutations(root: Path) -> None:
    plan = tsv_rows(root / MUTATION_PLAN)
    results = tsv_rows(root / MUTATION_RESULTS)
    if len(plan) != 6 or len(results) != 21:
        raise RuntimeError(f"mutation census drift: plan={len(plan)} results={len(results)}")
    suite_ids = [row["suite_id"] for row in plan]
    if len(set(suite_ids)) != len(suite_ids):
        raise RuntimeError(f"duplicate mutation suite id: {suite_ids}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if sum(expected.values()) != 21:
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
    plan_rows = tsv_rows(root / MUTATION_PLAN)
    return {
        "schema": "pkg-queue-lockdown-v1",
        "go_package": str(PACKAGE),
        "source_seed_commit": "b931f16351cbd7b76bceff8477b7afc7e1ba6632",
        "artifact_count": 3,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "mutation_suites": len(plan_rows),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def check(root: Path, inventory_only: bool = False) -> None:
    expected_artifacts = artifact_lines(root)
    stored_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if expected_artifacts != stored_artifacts:
        raise RuntimeError("pkg/util/queue artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    expected_inventory = classified_obligation_lines(raw)
    if expected_inventory != stored:
        raise RuntimeError("pkg/util/queue classifications or evidence drifted")
    stored_raw = ["\t".join(row[:6]) for row in parse_obligations(stored)]
    if raw[1:] != stored_raw:
        raise RuntimeError("pkg/util/queue AST obligation set drifted")
    if inventory_only:
        print(
            f"pkg/util/queue inventory: 3 artifacts, {len(stored_raw)} AST obligations, "
            "classifications exact"
        )
        return
    if not (root / RECEIPT).is_file():
        raise RuntimeError("pkg/util/queue content-addressed receipt is missing")
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/queue content-addressed receipt drifted")
    print(f"pkg/util/queue lockdown: 3 artifacts, {len(stored_raw)} AST obligations, classifications exact")


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
    print(f"wrote 3 artifacts and {len(obligations) - 2} obligations")


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
        print(f"pkg/util/queue lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
