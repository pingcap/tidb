#!/usr/bin/env python3
"""Check the exact file-lockdown seed for pkg/session/upgrade_def.go."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


GO_SOURCE = Path("pkg/session/upgrade_def.go")
GO_BOOTSTRAP_TEST = Path("pkg/session/bootstrap_test.go")
GO_UPGRADE_TEST = Path("pkg/session/upgrade_test.go")
GO_BACKFILL_TEST = Path("pkg/session/upgrade_backfill_test.go")
GO_SESSION_TEST = Path("pkg/session/session_test.go")
GO_EXTERNAL_UPGRADE_TEST = Path("pkg/session/test/bootstraptest/bootstrap_upgrade_test.go")
GO_EXTERNAL_BOOT_TEST = Path("pkg/session/test/bootstraptest/boot_test.go")
GO_EXTERNAL_BOOT2_TEST = Path("pkg/session/test/bootstraptest2/boot_test.go")
ARTIFACT_PATHS = {
    GO_SOURCE: "production-owner",
    GO_BOOTSTRAP_TEST: "ranked-direct-upgrade-support",
    GO_UPGRADE_TEST: "direct-registry-support",
    GO_BACKFILL_TEST: "direct-latest-upgrade-support",
    GO_SESSION_TEST: "selected-current-version-support",
    GO_EXTERNAL_UPGRADE_TEST: "external-upgrade-support",
    GO_EXTERNAL_BOOT_TEST: "external-upgrade-support",
    GO_EXTERNAL_BOOT2_TEST: "external-upgrade-support",
}

RUST_OWNER = Path("rust/crates/tidb-exec/src/upgrade_versions.rs")
RUST_TEST = Path("rust/crates/tidb-exec/tests/upgrade_versions_source.rs")
DATA_STEM = Path("rust/crates/tidb-exec/src/upgrade_versions")
ARTIFACTS = DATA_STEM.with_suffix(".artifacts.tsv")
INVENTORY = DATA_STEM.with_suffix(".inventory.tsv")
MUTATION_PLAN = DATA_STEM.with_suffix(".mutation-plan.tsv")
MUTATION_RESULTS = DATA_STEM.with_suffix(".mutation-results.tsv")
RECEIPT = DATA_STEM.with_suffix(".receipt.json")
SCRIPT = Path("rust/scripts/pkg-session-upgrade-def-lockdown.py")
EXECPLAN = Path("rust/docs/operations/pkg-session-upgrade-def-go-lockdown-execplan.md")
GO_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")

SOURCE_SEED_COMMIT = "c8dbb60fb5756fe782cae5442cb84fa33007b192"
CLAIM_BOUNDARY = "file-lockdown-seed-not-package-completion"
EXPECTED_MUTATION_SUITES = 8
EXPECTED_MUTATIONS = 12
SESSION_TEST_OWNERS = {
    "TestGetStartMode",
    "TestBootstrapSessionImplUserKSVersionGuard",
}
PORTED_TEST_ANCHORS = {
    "TestUpgradeToVerFunctionsCheck/assertion:1:Greater": "is_valid_upgrade_registry",
    "TestUpgradeToVerFunctionsCheck/assertion:4:Regexp": "registered_upgrade_function_name",
    "TestUpgradeToVerFunctionsCheck/assertion:5:Equal": "CURRENT_BOOTSTRAP_VERSION",
    "TestUpgradeToVerFunctionsCheck/loop:1/enters": "is_valid_upgrade_registry",
    "TestUpgradeToVerFunctionsCheck/loop:1/zero_iterations": "is_valid_upgrade_registry",
}
RUST_SYMBOLS = {
    "CURRENT_BOOTSTRAP_VERSION",
    "DECLARED_BOOTSTRAP_VERSIONS",
    "REGISTERED_UPGRADE_VERSIONS",
    "upgrade_versions",
    "upgrade_function_name",
    "registered_upgrade_function_name",
    "is_valid_upgrade_registry",
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
        "# pkg-session-upgrade-def-file-lockdown-artifacts-v1",
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
    with tempfile.TemporaryDirectory(prefix="session-upgrade-def-lockdown-") as temp:
        temp_root = Path(temp)
        for source in sources:
            destination = temp_root / source
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / source, destination)
        output = run(
            root,
            ["go", "run", f"./{GO_TOOL}", "--root", str(temp_root), "--package", package],
        )
    lines = output.rstrip("\n").splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise RuntimeError(f"Go inventory returned no header for {package}")
    rows = [line.split("\t") for line in lines[1:]]
    if any(len(row) != 6 for row in rows):
        raise RuntimeError(f"invalid Go inventory row for {package}")
    return rows


def raw_obligations(root: Path) -> list[list[str]]:
    primary = inventory_output(
        root,
        "pkg/session",
        [GO_SOURCE, GO_BOOTSTRAP_TEST, GO_UPGRADE_TEST, GO_BACKFILL_TEST, GO_SESSION_TEST],
    )
    rows = [
        row
        for row in primary
        if row[2]
        in {
            str(GO_SOURCE),
            str(GO_BOOTSTRAP_TEST),
            str(GO_UPGRADE_TEST),
            str(GO_BACKFILL_TEST),
        }
        or (row[2] == str(GO_SESSION_TEST) and row[5] in SESSION_TEST_OWNERS)
    ]
    rows.extend(
        inventory_output(
            root,
            "pkg/session/test/bootstraptest",
            [GO_EXTERNAL_UPGRADE_TEST, GO_EXTERNAL_BOOT_TEST],
        )
    )
    rows.extend(
        inventory_output(
            root,
            "pkg/session/test/bootstraptest2",
            [GO_EXTERNAL_BOOT2_TEST],
        )
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


def classify(source: str, category: str, anchor: str, owner: str) -> tuple[str, str, str]:
    if source == str(GO_SOURCE) and category == "const" and re.fullmatch(
        r"const:version\d+:0", anchor
    ):
        return (
            "PORTED",
            "DECLARED_BOOTSTRAP_VERSIONS",
            "Exact top-level version constant is present in the native declared-version table",
        )
    if source == str(GO_UPGRADE_TEST) and anchor in PORTED_TEST_ANCHORS:
        symbol = PORTED_TEST_ANCHORS[anchor]
        return (
            "PORTED",
            symbol,
            f"Exact representable registry-test rule is pinned by {symbol}",
        )
    if source == str(GO_SOURCE):
        if owner == "upgradeToVerFunctions":
            reason = (
                "Go owns a mutable registry of session callbacks; Rust preserves its version projection "
                "but has no executable bootstrap-upgrade callback engine"
            )
        elif owner == "currentBootstrapVersion":
            reason = (
                "Go's test-mutable package variable is not the immutable Rust current-version constant"
            )
        elif owner.startswith("upgradeToVer") or owner in {
            "doReentrantDDL",
            "writeSystemTZ",
            "writeNewCollationParameter",
            "writeDefaultExprPushDownBlacklist",
            "writeStmtSummaryVars",
            "insertBuiltinBindInfoRow",
            "updateBindInfo",
            "writeMemoryQuotaQuery",
            "importConfigOption",
            "writeDDLTableVersion",
            "writeClusterID",
        }:
            reason = (
                f"Go owner {owner} executes or transforms session SQL, metadata, DDL, bindings, chunks, "
                "or retry state absent from tidb-exec's registry-only bootstrap boundary"
            )
        else:
            reason = (
                f"Go production owner {owner} includes callback/session state not represented by the "
                "native registry metadata boundary"
            )
    elif source == str(GO_UPGRADE_TEST):
        reason = (
            f"Go registry support owner {owner} requires callback pointers or runtime reflection absent "
            "from Rust's version projection"
        )
    elif source == str(GO_SESSION_TEST):
        reason = (
            f"Go support owner {owner} exercises start-mode or keyspace bootstrap runtime, not the "
            "registry metadata boundary"
        )
    else:
        reason = (
            f"Go direct upgrade support owner {owner} requires mock storage, BootstrapSession, DDL, "
            "system-table SQL, bindings, or cluster state absent from the native registry module"
        )
    return "DECLINED", "-", reason


def classified_inventory_lines(raw: list[list[str]]) -> list[str]:
    lines = [
        "# pkg-session-upgrade-def-file-lockdown-inventory-v1",
        (
            "obligation_id\tcategory\tsource_path\tast_anchor\tnode_sha256\towner"
            "\tstatus\trust_symbol\tevidence\treason\tmutation_policy"
        ),
    ]
    for obligation_id, category, source, anchor, node_hash, owner in raw:
        status, symbol, reason = classify(source, category, anchor, owner)
        evidence_kind = "rust-compile-anchor" if status == "PORTED" else "measured-gap"
        evidence = (
            f"go-ast-quote:{source}#{anchor}@sha256:{node_hash};{evidence_kind}:{reason}"
        )
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


def inventory_counts(lines: list[str]) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    categories: dict[str, int] = {}
    sources: dict[str, int] = {}
    statuses: dict[str, int] = {}
    for row in (line.split("\t") for line in lines[2:]):
        categories[row[1]] = categories.get(row[1], 0) + 1
        sources[row[2]] = sources.get(row[2], 0) + 1
        statuses[row[6]] = statuses.get(row[6], 0) + 1
    return categories, sources, statuses


def validate_inventory(stored: list[str], raw: list[list[str]]) -> None:
    expected = classified_inventory_lines(raw)
    if stored != expected:
        raise RuntimeError("upgrade_def AST obligations, verdicts, or evidence drifted")
    ids: set[str] = set()
    anchors: set[tuple[str, str, str, str]] = set()
    symbols: set[str] = set()
    for row in (line.split("\t") for line in stored[2:]):
        if len(row) != 11 or not row[6] or not row[9]:
            raise RuntimeError(f"invalid or unclassified inventory row: {row}")
        if row[0] in ids or (row[2], row[1], row[3], row[4]) in anchors:
            raise RuntimeError(f"duplicate inventory row: {row}")
        ids.add(row[0])
        anchors.add((row[2], row[1], row[3], row[4]))
        if f"@sha256:{row[4]}" not in row[8]:
            raise RuntimeError(f"inventory row lost its AST quote: {row}")
        if row[6] == "PORTED":
            if row[7] not in RUST_SYMBOLS or "rust-compile-anchor:" not in row[8]:
                raise RuntimeError(f"PORTED row lost its Rust symbol: {row}")
            symbols.add(row[7])
        elif row[6] == "DECLINED":
            if row[7] != "-" or "measured-gap:" not in row[8]:
                raise RuntimeError(f"DECLINED row lost its measured boundary: {row}")
        else:
            raise RuntimeError(f"unsupported inventory verdict: {row}")
    if symbols != {
        "DECLARED_BOOTSTRAP_VERSIONS",
        "is_valid_upgrade_registry",
        "registered_upgrade_function_name",
        "CURRENT_BOOTSTRAP_VERSION",
    }:
        raise RuntimeError(f"PORTED symbol census drift: {symbols}")


def validate_symbols(root: Path) -> None:
    source = (root / RUST_OWNER).read_text(encoding="utf-8")
    test = (root / RUST_TEST).read_text(encoding="utf-8")
    definitions = {
        "CURRENT_BOOTSTRAP_VERSION": r"pub const CURRENT_BOOTSTRAP_VERSION\s*:",
        "DECLARED_BOOTSTRAP_VERSIONS": r"pub const DECLARED_BOOTSTRAP_VERSIONS\s*:",
        "REGISTERED_UPGRADE_VERSIONS": r"pub const REGISTERED_UPGRADE_VERSIONS\s*:",
        "upgrade_versions": r"pub fn upgrade_versions\s*\(",
        "upgrade_function_name": r"pub fn upgrade_function_name\s*\(",
        "registered_upgrade_function_name": r"pub fn registered_upgrade_function_name\s*\(",
        "is_valid_upgrade_registry": r"pub fn is_valid_upgrade_registry\s*\(",
    }
    for symbol, pattern in definitions.items():
        if re.search(pattern, source) is None or symbol not in test:
            raise RuntimeError(f"compiled owner symbol disappeared from source-backed gate: {symbol}")


def validate_source_evidence(
    root: Path, row: dict[str, str], source_field: str, hash_field: str, kind: str
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


def validate_mutations(root: Path) -> None:
    plan = data_rows(root / MUTATION_PLAN)
    results = data_rows(root / MUTATION_RESULTS)
    if len(plan) != EXPECTED_MUTATION_SUITES or len(results) != EXPECTED_MUTATIONS:
        raise RuntimeError(f"mutation census drift: suites={len(plan)} results={len(results)}")
    expected_counts = {row["suite_id"]: int(row["mutation_count"]) for row in plan}
    if len(expected_counts) != len(plan) or sum(expected_counts.values()) != EXPECTED_MUTATIONS:
        raise RuntimeError(f"mutation plan count drift: {expected_counts}")
    baselines = {row["suite_id"]: row["baseline_commit"] for row in plan}
    for row in plan:
        validate_source_evidence(root, row, "source_file", "source_sha256", "mutation plan")
    actual = {suite: 0 for suite in expected_counts}
    mutation_ids: set[str] = set()
    for row in results:
        if row["mutation_id"] in mutation_ids:
            raise RuntimeError(f"duplicate mutation result: {row['mutation_id']}")
        mutation_ids.add(row["mutation_id"])
        suite = row["suite_id"]
        if suite not in actual or row["baseline_commit"] != baselines[suite]:
            raise RuntimeError(f"unplanned mutation result: {row}")
        actual[suite] += 1
        if row["status"] != "KILLED" or row["restore_status"] != "PASS":
            raise RuntimeError(f"mutation survived or was not restored: {row}")
        if int(row["exit_code"]) == 0 or not row["named_test"]:
            raise RuntimeError(f"mutation lacks named failing evidence: {row}")
        validate_source_evidence(root, row, "source_file", "source_sha256", "mutation result")
    if actual != expected_counts:
        raise RuntimeError(f"mutation result counts drifted: {actual}")


def receipt_contents(root: Path, inventory: list[str]) -> dict[str, object]:
    categories, sources, statuses = inventory_counts(inventory)
    production = sources[str(GO_SOURCE)]
    owned = [ARTIFACTS, INVENTORY, MUTATION_PLAN, MUTATION_RESULTS, RUST_OWNER, RUST_TEST, SCRIPT, EXECPLAN]
    return {
        "artifact_count": len(ARTIFACT_PATHS),
        "category_counts": categories,
        "claim_boundary": CLAIM_BOUNDARY,
        "direct_test_support_obligation_count": len(inventory) - 2 - production,
        "go_package": "pkg/session",
        "mutation_count": EXPECTED_MUTATIONS,
        "mutation_suites": EXPECTED_MUTATION_SUITES,
        "obligation_count": len(inventory) - 2,
        "owned_file_sha256": {str(path): sha256(root / path) for path in owned},
        "owning_go_source": str(GO_SOURCE),
        "ported_obligation_count": statuses.get("PORTED", 0),
        "ported_symbol_count": 4,
        "production_obligation_count": production,
        "reachable_ported_rule_count": statuses.get("PORTED", 0),
        "schema": "pkg-session-upgrade-def-file-lockdown-v1",
        "source_seed_commit": SOURCE_SEED_COMMIT,
        "source_obligation_counts": sources,
        "status_counts": statuses,
        "whole_go_package_complete": False,
    }


def check(root: Path, inventory_only: bool) -> None:
    actual_artifacts = (root / ARTIFACTS).read_text(encoding="utf-8").rstrip("\n").splitlines()
    if actual_artifacts != artifact_lines(root):
        raise RuntimeError("upgrade_def source/test artifact manifest drifted")
    raw = raw_obligations(root)
    stored = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_inventory(stored, raw)
    validate_symbols(root)
    _, _, statuses = inventory_counts(stored)
    if inventory_only:
        print(
            f"pkg/session/upgrade_def.go inventory: {len(raw)} AST obligations, "
            f"{statuses.get('PORTED', 0)} PORTED, {statuses.get('DECLINED', 0)} DECLINED, "
            "file seed only"
        )
        return
    validate_mutations(root)
    actual_receipt = json.loads((root / RECEIPT).read_text(encoding="utf-8"))
    if actual_receipt != receipt_contents(root, stored):
        raise RuntimeError("upgrade_def content-addressed receipt drifted")
    print(
        f"pkg/session/upgrade_def.go lockdown: {len(raw)} AST obligations, "
        f"{statuses.get('PORTED', 0)} PORTED, {statuses.get('DECLINED', 0)} DECLINED, "
        f"{EXPECTED_MUTATIONS} mutations killed, file seed only"
    )


def write_inventory(root: Path) -> None:
    (root / ARTIFACTS).write_text("\n".join(artifact_lines(root)) + "\n", encoding="utf-8")
    (root / INVENTORY).write_text(
        "\n".join(classified_inventory_lines(raw_obligations(root))) + "\n",
        encoding="utf-8",
    )


def write_receipt(root: Path) -> None:
    inventory = (root / INVENTORY).read_text(encoding="utf-8").rstrip("\n").splitlines()
    validate_inventory(inventory, raw_obligations(root))
    validate_symbols(root)
    validate_mutations(root)
    (root / RECEIPT).write_text(
        json.dumps(receipt_contents(root, inventory), indent=2, sort_keys=True) + "\n",
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
        print(f"pkg/session/upgrade_def.go lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
