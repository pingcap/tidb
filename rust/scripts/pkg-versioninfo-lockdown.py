#!/usr/bin/env python3
"""Generate and check the content-addressed pkg/util/versioninfo lockdown."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys


PACKAGE = Path("pkg/util/versioninfo")
RUST_DIR = Path("rust/crates/tidb-util/src")
ARTIFACTS = RUST_DIR / "versioninfo.artifacts.tsv"
INVENTORY = RUST_DIR / "versioninfo.inventory.tsv"
MUTATION_PLAN = RUST_DIR / "versioninfo.mutation-plan.tsv"
MUTATION_RESULTS = RUST_DIR / "versioninfo.mutation-results.tsv"
RECEIPT = RUST_DIR / "versioninfo.receipt.json"
RUST_SOURCE = RUST_DIR / "versioninfo.rs"
RUST_GATE = Path("rust/crates/tidb-util/tests/versioninfo_lockdown.rs")
SCRIPT = Path("rust/scripts/pkg-versioninfo-lockdown.py")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")

EXPECTED_ZERO_CLASSES = {
    "build_tags": 0,
    "platform_variants": 0,
    "code_generated": 0,
    "go_generate": 0,
    "go_embed": 0,
    "tracked_testdata": 0,
}
PORTED = {"const:CommunityEdition:0": "COMMUNITY_EDITION"}
DECLINED = {
    "var:TiDBBuildTS:0",
    "var:TiDBEdition:0",
    "var:TiDBEnterpriseExtensionGitHash:0",
    "var:TiDBGitBranch:0",
    "var:TiDBGitHash:0",
}
PORTED_EVIDENCE = "rust-test:source_storage_class_and_override_boundaries_are_exact"
DECLINED_EVIDENCE = "go-probe:linktime_and_runtime_mutability"


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
        raise RuntimeError(f"untracked pkg/util/versioninfo artifacts: {untracked}")
    if len(paths) != 2:
        raise RuntimeError(f"expected 2 pkg/util/versioninfo artifacts, found {len(paths)}")
    zeros = zero_classes(root, paths)
    if zeros != EXPECTED_ZERO_CLASSES:
        raise RuntimeError(f"zero-class drift: expected {EXPECTED_ZERO_CLASSES}, found {zeros}")
    lines = ["# pkg-versioninfo-artifacts-v1"]
    for name, count in EXPECTED_ZERO_CLASSES.items():
        lines.append(f"# zero\t{name}\t{count}")
    lines.append("path\trole\tsha256")
    for path in paths:
        role = "build" if path.endswith("BUILD.bazel") else "production"
        if role == "production" and not path.endswith(".go"):
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


def classified_obligation_lines(raw: list[str]) -> list[str]:
    lines = ["# pkg-versioninfo-obligations-v1"]
    lines.append(
        "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
        "\tstatus\trust_symbol\tevidence\tmutation_policy"
    )
    for line in raw[1:]:
        obligation_id, category, source_path, anchor, node_hash, owner = line.split("\t")
        if source_path != "pkg/util/versioninfo/versioninfo.go" or owner != anchor:
            raise RuntimeError(f"unclassified versioninfo obligation: {line}")
        if owner in PORTED and category == "const":
            status = "PORTED"
            symbol = PORTED[owner]
            evidence = PORTED_EVIDENCE
            mutation_policy = "compile-owner-gate"
        elif owner in DECLINED and category == "var":
            status = "DECLINED"
            symbol = "-"
            evidence = DECLINED_EVIDENCE
            mutation_policy = "classification-evidence-gate"
        else:
            raise RuntimeError(f"unclassified versioninfo obligation: {line}")
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
    if len(rows) != 6:
        raise RuntimeError(f"obligation census drift: {len(rows)}")
    ids: set[str] = set()
    anchors: set[str] = set()
    categories: dict[str, int] = {}
    statuses: dict[str, int] = {}
    for row in rows:
        if len(row) != 10:
            raise RuntimeError(f"invalid classification row: {row}")
        if row[0] in ids or row[3] in anchors:
            raise RuntimeError(f"duplicate obligation row: {row}")
        ids.add(row[0])
        anchors.add(row[3])
        if row[2] != "pkg/util/versioninfo/versioninfo.go" or row[5] != row[3]:
            raise RuntimeError(f"unexpected source row: {row}")
        if row[3] in PORTED:
            expected = ("const", "PORTED", PORTED[row[3]], PORTED_EVIDENCE, "compile-owner-gate")
        elif row[3] in DECLINED:
            expected = ("var", "DECLINED", "-", DECLINED_EVIDENCE, "classification-evidence-gate")
        else:
            raise RuntimeError(f"unexpected source owner: {row}")
        if (row[1], row[6], row[7], row[8], row[9]) != expected:
            raise RuntimeError(f"classification or evidence drift: {row}")
        categories[row[1]] = categories.get(row[1], 0) + 1
        statuses[row[6]] = statuses.get(row[6], 0) + 1
    if anchors != set(PORTED) | DECLINED:
        raise RuntimeError(f"source owner drift: {anchors}")
    if categories != {"const": 1, "var": 5} or statuses != {"PORTED": 1, "DECLINED": 5}:
        raise RuntimeError(f"classification census drift: {categories} / {statuses}")


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
    if len(plan) != 5 or len(results) != 9:
        raise RuntimeError(f"mutation census drift: plan={len(plan)} results={len(results)}")
    expected = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if sum(expected.values()) != 9:
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
        if sha256(root / row["source_file"]) != row["source_sha256"]:
            raise RuntimeError(f"mutation source drifted: {row['source_file']}")
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
        "schema": "pkg-versioninfo-lockdown-v1",
        "go_package": str(PACKAGE),
        "source_seed_commit": "6d99186063e6cda053d510ec59493e5956ffa04c",
        "artifact_count": 2,
        "zero_artifact_classes": EXPECTED_ZERO_CLASSES,
        "obligation_count": len(rows),
        "category_counts": {"const": 1, "var": 5},
        "status_counts": {"DECLINED": 5, "PORTED": 1},
        "mutation_suites": len(tsv_rows(root / MUTATION_PLAN)),
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
    }


def check(root: Path, inventory_only: bool = False) -> None:
    if artifact_lines(root) != (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines():
        raise RuntimeError("pkg/util/versioninfo artifact manifest drifted")
    raw = raw_obligation_lines(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_classifications(stored)
    if classified_obligation_lines(raw) != stored:
        raise RuntimeError("pkg/util/versioninfo classifications or evidence drifted")
    stored_raw = ["\t".join(row[:6]) for row in parse_obligations(stored)]
    if raw[1:] != stored_raw:
        raise RuntimeError("pkg/util/versioninfo AST obligation set drifted")
    if inventory_only:
        print("pkg/util/versioninfo inventory: 2 artifacts, 6 AST obligations, 1 PORTED, 5 DECLINED")
        return
    actual = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual != receipt_contents(root, stored):
        raise RuntimeError("pkg/util/versioninfo content-addressed receipt drifted")
    print("pkg/util/versioninfo lockdown: 2 artifacts, 6 AST obligations, 1 PORTED, 5 DECLINED")


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
    print(f"wrote 2 artifacts and {len(obligations) - 2} obligations")


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
        print(f"pkg/util/versioninfo lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
