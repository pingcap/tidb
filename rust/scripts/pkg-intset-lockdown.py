#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/util/intset lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys


PACKAGE = Path("pkg/util/intset")
RUST_DIR = Path("rust/crates/tidb-util/src")
ARTIFACTS = RUST_DIR / "intset.artifacts.tsv"
INVENTORY = RUST_DIR / "intset.inventory.tsv"
MUTATION_PLAN = RUST_DIR / "intset.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "intset.mutation-results.tsv"
RECEIPT = RUST_DIR / "intset.receipt.json"
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

SOURCE_SYMBOLS = {
    "FastIntSet.AddRange": "FastIntSet::add_range",
    "FastIntSet.Clear": "FastIntSet::clear",
    "FastIntSet.Copy": "FastIntSet::copy",
    "FastIntSet.CopyFrom": "FastIntSet::copy_from",
    "FastIntSet.Difference": "FastIntSet::difference",
    "FastIntSet.DifferenceWith": "FastIntSet::difference_with",
    "FastIntSet.Equals": "FastIntSet::equals",
    "FastIntSet.ForEach": "FastIntSet::for_each",
    "FastIntSet.GetSmallUInt64": "FastIntSet::get_small_uint64",
    "FastIntSet.Has": "FastIntSet::has",
    "FastIntSet.Insert": "FastIntSet::insert",
    "FastIntSet.Intersection": "FastIntSet::intersection",
    "FastIntSet.IntersectionWith": "FastIntSet::intersection_with",
    "FastIntSet.Intersects": "FastIntSet::intersects",
    "FastIntSet.IsEmpty": "FastIntSet::is_empty",
    "FastIntSet.Len": "FastIntSet::len",
    "FastIntSet.Next": "FastIntSet::next",
    "FastIntSet.Only1Zero": "FastIntSet::only1_zero",
    "FastIntSet.Remove": "FastIntSet::remove",
    "FastIntSet.Shift": "FastIntSet::shift",
    "FastIntSet.SortedArray": "FastIntSet::sorted_array",
    "FastIntSet.String": "FastIntSet::Display",
    "FastIntSet.SubsetOf": "FastIntSet::subset_of",
    "FastIntSet.Union": "FastIntSet::union",
    "FastIntSet.UnionWith": "FastIntSet::union_with",
    "FastIntSet.largeToSmall": "FastIntSet::large_to_small",
    "FastIntSet.toLarge": "FastIntSet::to_large",
    "NewFastIntSet": "FastIntSet::new",
    "const:smallCutOff:0": "SMALL_CUT_OFF",
    "type:FastIntSet": "FastIntSet",
}

SOURCE_EVIDENCE = {
    "FastIntSet.Shift": "source_shift_wraps_like_go_int",
    "FastIntSet.AddRange": "source_range_and_error_contracts",
    "FastIntSet.String": "source_string_pair_and_sentinel_contracts",
}

SET_ALGEBRA_OWNERS = {
    "FastIntSet.Difference",
    "FastIntSet.DifferenceWith",
    "FastIntSet.Intersection",
    "FastIntSet.IntersectionWith",
    "FastIntSet.Intersects",
    "FastIntSet.SubsetOf",
    "FastIntSet.Union",
    "FastIntSet.UnionWith",
}

TEST_EVIDENCE = {
    "TestFastIntSetBasic": "basic",
    "TestFastIntSet": "randomized",
    "TestFastIntSetTwoSetOps": "two_set_ops",
    "TestFastIntSetAddRange": "add_range",
    "TestGetSmallUInt64": "get_small_uint64",
    "TestFastIntSetString": "string_format",
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
        raise RuntimeError(f"untracked pkg/util/intset artifacts: {untracked}")
    if len(paths) != 4:
        raise RuntimeError(f"expected 4 pkg/util/intset artifacts, found {len(paths)}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    lines = ["# pkg-intset-artifacts-v1"]
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


def production_evidence(owner: str) -> str:
    if owner in SET_ALGEBRA_OWNERS:
        return "source_mixed_representation_set_algebra"
    if owner in SOURCE_EVIDENCE:
        return SOURCE_EVIDENCE[owner]
    if owner in {"FastIntSet.Next", "FastIntSet.ForEach"}:
        return "source_max_int_sentinel_contract"
    if owner == "FastIntSet.CopyFrom":
        return "source_copy_from_preserves_go_representation"
    if owner == "FastIntSet.GetSmallUInt64":
        return "source_range_and_error_contracts"
    return "source_bitmap_transition_contracts"


def probe_for(owner: str) -> str:
    if owner in SET_ALGEBRA_OWNERS:
        return "P002-set-algebra"
    if owner == "FastIntSet.Shift":
        return "P003-shift-overflow"
    if owner == "FastIntSet.AddRange":
        return "P004-range-errors"
    if owner == "FastIntSet.String":
        return "P005-string-format"
    return "P001-representation"


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-intset-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        if source_path == "pkg/util/intset/fast_int_set.go":
            try:
                symbol = SOURCE_SYMBOLS[owner]
            except KeyError as error:
                raise RuntimeError(f"missing Rust owner for {owner}") from error
            status = "PORTED"
            evidence = "rust-test:" + production_evidence(owner)
            if category in {"branch", "closure", "loop", "short_circuit"}:
                policy = "probe:" + probe_for(owner)
            else:
                policy = "compile-owner-gate"
        elif owner in TEST_EVIDENCE:
            status = "PORTED"
            symbol = "FastIntSet"
            evidence = "rust-test:" + TEST_EVIDENCE[owner]
            policy = "source-row-hash"
        elif source_path.endswith("fast_int_set_bench_test.go"):
            status = "DECLINED"
            symbol = "-"
            evidence = "go-benchmark-runtime-only:not-a-production-semantic-contract"
            policy = "declined"
        else:
            status = "DECLINED"
            symbol = "-"
            if owner in {"getTestName", "NewTestRand", "var:lastTestName:0", "var:rng:0"}:
                evidence = "go-runtime-test-harness-only:stack-name-and-wall-clock-random-seeding"
            else:
                evidence = "go-test-reference-model-only:rust-tests-use-standard-map-oracles"
            policy = "declined"
        fields = [
            obligation_id, category, source_path, anchor, node_hash, owner, status,
            symbol, evidence, policy,
        ]
        if any("\t" in field or "\n" in field for field in fields):
            raise RuntimeError(f"invalid TSV field in {fields}")
        lines.append("\t".join(fields))
    return lines


def parse_obligations(lines: list[str]) -> list[list[str]]:
    return [line.split("\t") for line in lines if line and not line.startswith("#")][1:]


def validate_classifications(lines: list[str]) -> None:
    rows = parse_obligations(lines)
    ids: set[str] = set()
    allowed_symbols = set(SOURCE_SYMBOLS.values()) | {"FastIntSet"}
    for row in rows:
        if len(row) != 10:
            raise RuntimeError(f"invalid obligation row: {row}")
        if row[0] in ids:
            raise RuntimeError(f"duplicate obligation id: {row[0]}")
        ids.add(row[0])
        if row[6] not in ALLOWED_STATUSES or not row[8] or not row[9]:
            raise RuntimeError(f"invalid classification: {row}")
        if row[6] == "PORTED" and row[7] not in allowed_symbols:
            raise RuntimeError(f"ungated PORTED symbol: {row}")
        if row[6] != "PORTED" and row[7] != "-":
            raise RuntimeError(f"non-PORTED row claims a symbol: {row}")
    status_counts = {status: sum(row[6] == status for row in rows) for status in ALLOWED_STATUSES}
    if status_counts != {"PORTED": 446, "DECLINED": 88, "UNREACHABLE": 0}:
        raise RuntimeError(f"status census drift: {status_counts}")


def receipt_contents(root: Path, obligations: list[str]) -> dict[str, object]:
    validate_mutations(root)
    rows = parse_obligations(obligations)
    category_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for row in rows:
        category_counts[row[1]] = category_counts.get(row[1], 0) + 1
        status_counts[row[6]] = status_counts.get(row[6], 0) + 1
    owned = [
        ARTIFACTS, INVENTORY, MUTATION_PLAN, MUTATION_RESULTS,
        RUST_DIR / "intset.rs", Path("rust/crates/tidb-util/tests/intset_lockdown.rs"),
        Path("rust/scripts/pkg-intset-lockdown.py"),
    ]
    plan_rows = [
        line for line in (root / MUTATION_PLAN).read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ][1:]
    return {
        "schema": "pkg-intset-lockdown-v1",
        "go_package": str(PACKAGE),
        "source_seed_commit": "56d06365eae71a692e538986d84003565f880103",
        "artifact_count": 4,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(rows),
        "category_counts": dict(sorted(category_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "mutation_suites": len(plan_rows),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def tsv_rows(path: Path) -> list[dict[str, str]]:
    lines = [
        line for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    return list(csv.DictReader(lines, delimiter="\t"))


def validate_mutations(root: Path) -> None:
    plan = tsv_rows(root / MUTATION_PLAN)
    results = tsv_rows(root / MUTATION_RESULTS)
    if len(plan) != 7 or len(results) != 18:
        raise RuntimeError(f"mutation census drift: plan={len(plan)} results={len(results)}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    actual = {suite: 0 for suite in expected}
    ids: set[str] = set()
    for row in results:
        mutation_id = row["mutation_id"]
        if mutation_id in ids:
            raise RuntimeError(f"duplicate mutation id: {mutation_id}")
        ids.add(mutation_id)
        if row["suite_id"] not in actual:
            raise RuntimeError(f"unplanned mutation suite: {row['suite_id']}")
        actual[row["suite_id"]] += 1
        if row["status"] != "KILLED" or row["restore_status"] != "PASS":
            raise RuntimeError(f"mutation not killed and restored: {row}")
        if int(row["exit_code"]) == 0 or not row["named_test"]:
            raise RuntimeError(f"mutation lacks a named failing command: {row}")
        if sha256(root / row["source_file"]) != row["source_sha256"]:
            raise RuntimeError(f"mutation source drifted: {row['source_file']}")
    if actual != expected:
        raise RuntimeError(f"mutation suite counts drift: expected={expected} actual={actual}")


def check(root: Path) -> None:
    expected_artifacts = artifact_lines(root)
    stored_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if expected_artifacts != stored_artifacts:
        raise RuntimeError("pkg/util/intset artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    expected_inventory = classified_obligation_lines(raw)
    if expected_inventory != stored:
        raise RuntimeError("pkg/util/intset classifications or evidence drifted")
    stored_raw = ["\t".join(row[:6]) for row in parse_obligations(stored)]
    if raw[1:] != stored_raw:
        raise RuntimeError("pkg/util/intset AST obligation set drifted")
    if not (root / RECEIPT).is_file():
        raise RuntimeError("pkg/util/intset content-addressed receipt is missing")
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/intset content-addressed receipt drifted")
    print(f"pkg/util/intset lockdown: 4 artifacts, {len(stored_raw)} AST obligations, classifications exact")


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
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.write or args.write_receipt:
            write(root, args.write_receipt)
        else:
            check(root)
    except (KeyError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"pkg/util/intset lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
