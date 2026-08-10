#!/usr/bin/env python3
"""Verify one Go-package rewrite with compact semantic rules.

The specification is maintained by humans. The receipt is derived. Nothing in
between is checked in.
"""

from __future__ import annotations

import argparse
import copy
import fnmatch
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tomllib
from typing import Any


SPEC_SCHEMA = "semantic-go-package-lockdown-v1"
RECEIPT_SCHEMA = "semantic-go-package-lockdown-receipt-v1"
FINAL_STATUSES = {"PORTED", "DECLINED", "UNREACHABLE"}
SELECTOR_KEYS = ("obligations", "sources", "owners", "anchors", "categories")


class LockdownError(RuntimeError):
    """A fail-closed specification or verification error."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def run(root: Path, argv: list[str], cwd: str = ".") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=root / cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def checked_output(root: Path, argv: list[str]) -> str:
    result = run(root, argv)
    if result.returncode != 0:
        raise LockdownError(f"command failed ({result.returncode}): {' '.join(argv)}\n{result.stdout}")
    return result.stdout


def repo_path(value: str, label: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path == Path("."):
        raise LockdownError(f"{label} must be a repository-relative path: {value!r}")
    return path


def string_list(value: Any, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise LockdownError(f"{label} must be a list of non-empty strings")
    return value


def load_spec(root: Path, spec_path: Path) -> dict[str, Any]:
    absolute = root / spec_path
    if not absolute.is_file():
        raise LockdownError(f"missing specification: {spec_path}")
    with absolute.open("rb") as source:
        spec = tomllib.load(source)
    if spec.get("schema") != SPEC_SCHEMA:
        raise LockdownError(f"unsupported schema: {spec.get('schema')!r}")
    for key in ("go_package", "source_commit", "claim", "receipt"):
        if not isinstance(spec.get(key), str) or not spec[key]:
            raise LockdownError(f"missing specification field: {key}")
    repo_path(spec["go_package"], "go_package")
    repo_path(spec["receipt"], "receipt")
    for value in string_list(spec.get("excluded_go_paths"), "excluded_go_paths"):
        repo_path(value, "excluded_go_paths")
    if spec["claim"] not in {"package-seed", "whole-go-package"}:
        raise LockdownError("claim must be package-seed or whole-go-package")
    rules = spec.get("rules")
    if not isinstance(rules, list) or not rules:
        raise LockdownError("the specification must contain semantic rules")
    ids: set[str] = set()
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise LockdownError(f"rules[{index}] must be a table")
        rule_id = rule.get("id")
        if not isinstance(rule_id, str) or not rule_id:
            raise LockdownError(f"rules[{index}] has no id")
        if rule_id in ids:
            raise LockdownError(f"duplicate rule id: {rule_id}")
        ids.add(rule_id)
        if rule.get("status") not in FINAL_STATUSES:
            raise LockdownError(f"{rule_id} has invalid status: {rule.get('status')!r}")
        if not isinstance(rule.get("rationale"), str) or not rule["rationale"].strip():
            raise LockdownError(f"{rule_id} has no rationale")
        if not any(rule.get(key) for key in SELECTOR_KEYS):
            raise LockdownError(f"{rule_id} has no selector")
        for key in SELECTOR_KEYS:
            string_list(rule.get(key), f"{rule_id}.{key}")
        for key in ("exclude_sources", "exclude_owners", "exclude_anchors", "exclude_categories"):
            string_list(rule.get(key), f"{rule_id}.{key}")
        if rule["status"] == "PORTED":
            rust_files = string_list(rule.get("rust_files"), f"{rule_id}.rust_files")
            if not rust_files:
                raise LockdownError(f"{rule_id} has no Rust ownership files")
            for value in rust_files:
                repo_path(value, f"{rule_id}.rust_files")
            test = rule.get("test")
            if not isinstance(test, dict):
                raise LockdownError(f"{rule_id} has no test command")
            argv = string_list(test.get("argv"), f"{rule_id}.test.argv")
            if not argv:
                raise LockdownError(f"{rule_id} has an empty test command")
            repo_path(test.get("cwd", "."), f"{rule_id}.test.cwd")
            if argv[0] == "cargo" and "-j12" not in argv:
                raise LockdownError(f"{rule_id} cargo command must use -j12")
        mutation = rule.get("mutation")
        if mutation is not None:
            if rule["status"] != "PORTED" or not isinstance(mutation, dict):
                raise LockdownError(f"{rule_id} has an invalid mutation")
            repo_path(mutation.get("path", ""), f"{rule_id}.mutation.path")
            if not isinstance(mutation.get("old"), str) or not mutation["old"]:
                raise LockdownError(f"{rule_id} mutation has no old text")
            if not isinstance(mutation.get("new"), str):
                raise LockdownError(f"{rule_id} mutation has no new text")
            if mutation.get("count", 1) != 1:
                raise LockdownError(f"{rule_id} mutations must replace exactly one invariant")
            if mutation.get("failure_contains") is not None and not isinstance(
                mutation["failure_contains"], str
            ):
                raise LockdownError(f"{rule_id} mutation failure_contains must be text")
    return spec


def accepted_artifacts(root: Path, spec: dict[str, Any]) -> dict[str, str]:
    package = spec["go_package"]
    commit = spec["source_commit"]
    excluded = string_list(spec.get("excluded_go_paths"), "excluded_go_paths")

    def included(path: str) -> bool:
        return not any(path == prefix or path.startswith(f"{prefix}/") for prefix in excluded)

    pinned = [
        path
        for path in checked_output(
            root, ["git", "ls-tree", "-r", "--name-only", commit, "--", package]
        ).splitlines()
        if included(path)
    ]
    current = [
        path
        for path in checked_output(root, ["git", "ls-files", "--", package]).splitlines()
        if included(path)
    ]
    untracked = checked_output(
        root, ["git", "ls-files", "--others", "--exclude-standard", "--", package]
    ).splitlines()
    if untracked:
        raise LockdownError(f"untracked accepted-package artifacts: {untracked}")
    if pinned != current:
        missing = sorted(set(pinned) - set(current))
        added = sorted(set(current) - set(pinned))
        raise LockdownError(f"accepted-package artifact drift; missing={missing}, added={added}")
    result: dict[str, str] = {}
    for value in pinned:
        path = repo_path(value, "accepted artifact")
        accepted = subprocess.run(
            ["git", "show", f"{commit}:{value}"], cwd=root, stdout=subprocess.PIPE, check=False
        )
        if accepted.returncode != 0:
            raise LockdownError(f"cannot read accepted artifact: {value}")
        current_bytes = (root / path).read_bytes()
        if current_bytes != accepted.stdout:
            raise LockdownError(f"accepted artifact changed from {commit}: {value}")
        result[value] = sha256_bytes(current_bytes)
    return result


def path_hashes(root: Path, values: list[str], label: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in sorted(values):
        path = repo_path(value, label)
        absolute = root / path
        if not absolute.is_file():
            raise LockdownError(f"missing {label}: {value}")
        result[value] = sha256_file(absolute)
    return result


def owned_files(root: Path, spec: dict[str, Any], spec_path: Path) -> dict[str, str]:
    roots = [repo_path(value, "owned_rust_roots") for value in string_list(
        spec.get("owned_rust_roots"), "owned_rust_roots"
    )]
    explicit = set(string_list(spec.get("owned_rust_files"), "owned_rust_files"))
    receipt = spec["receipt"]
    ignored = {spec_path.as_posix(), receipt}
    tracked: set[str] = set(explicit)
    for path in roots:
        tracked.update(
            checked_output(root, ["git", "ls-files", "--", path.as_posix()]).splitlines()
        )
        untracked = checked_output(
            root,
            ["git", "ls-files", "--others", "--exclude-standard", "--", path.as_posix()],
        ).splitlines()
        unexpected = sorted(set(untracked) - ignored)
        if unexpected:
            raise LockdownError(f"untracked owned Rust files: {unexpected}")
    tracked.difference_update(ignored)
    return path_hashes(root, sorted(tracked), "owned Rust file")


def raw_obligations(root: Path, spec: dict[str, Any]) -> list[dict[str, str]]:
    output = checked_output(
        root,
        [
            "go", "run", "./rust/difftests/tools/go_package_lockdown_inventory",
            "--root", ".", "--package", spec["go_package"],
        ],
    )
    lines = output.rstrip("\n").splitlines()
    if not lines or not lines[0].startswith("# obligation_id"):
        raise LockdownError("Go inventory returned no valid header")
    obligations: list[dict[str, str]] = []
    seen: set[str] = set()
    for line in lines[1:]:
        row = line.split("\t")
        if len(row) != 6:
            raise LockdownError(f"invalid Go inventory row: {row}")
        if row[0] in seen:
            raise LockdownError(f"duplicate obligation: {row[0]}")
        seen.add(row[0])
        obligations.append(dict(zip(
            ("id", "category", "source", "anchor", "node_sha256", "owner"), row, strict=True
        )))
    obligations.sort(key=lambda row: (row["source"], row["anchor"], row["category"], row["id"]))
    return obligations


def patterns_match(value: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


def rule_matches(rule: dict[str, Any], obligation: dict[str, str]) -> bool:
    mapping = {
        "obligations": "id",
        "sources": "source",
        "owners": "owner",
        "anchors": "anchor",
        "categories": "category",
    }
    for selector, field in mapping.items():
        patterns = rule.get(selector, [])
        if patterns and not patterns_match(obligation[field], patterns):
            return False
    excludes = {
        "exclude_sources": "source",
        "exclude_owners": "owner",
        "exclude_anchors": "anchor",
        "exclude_categories": "category",
    }
    return not any(
        patterns_match(obligation[field], rule.get(selector, []))
        for selector, field in excludes.items()
        if rule.get(selector)
    )


def resolve_rules(
    spec: dict[str, Any], obligations: list[dict[str, str]]
) -> tuple[dict[str, list[dict[str, str]]], list[dict[str, str]]]:
    resolved = {rule["id"]: [] for rule in spec["rules"]}
    unmatched: list[dict[str, str]] = []
    known_ids = {row["id"] for row in obligations}
    for rule in spec["rules"]:
        missing = sorted(set(rule.get("obligations", [])) - known_ids)
        if missing:
            raise LockdownError(f"{rule['id']} references missing obligations: {missing}")
    for obligation in obligations:
        matches = [rule for rule in spec["rules"] if rule_matches(rule, obligation)]
        if len(matches) > 1:
            raise LockdownError(
                f"obligation {obligation['id']} matches multiple rules: "
                + ", ".join(rule["id"] for rule in matches)
            )
        if not matches:
            unmatched.append(obligation)
        else:
            resolved[matches[0]["id"]].append(obligation)
    empty = [rule_id for rule_id, rows in resolved.items() if not rows]
    if empty:
        raise LockdownError(f"semantic rules matched no obligations: {empty}")
    return resolved, unmatched


def rule_fingerprint(
    root: Path, rule: dict[str, Any], obligations: list[dict[str, str]]
) -> tuple[str, dict[str, str]]:
    rust_hashes = path_hashes(root, string_list(rule.get("rust_files"), "rust_files"), "rule Rust file")
    payload = {
        "rule": rule,
        "obligations": [row["id"] for row in obligations],
        "rust_sha256": rust_hashes,
    }
    return sha256_bytes(canonical_bytes(payload)), rust_hashes


def build_state(root: Path, spec_path: Path, spec: dict[str, Any]) -> dict[str, Any]:
    artifacts = accepted_artifacts(root, spec)
    extras = path_hashes(root, string_list(spec.get("extra_artifacts"), "extra_artifacts"), "extra artifact")
    owned = owned_files(root, spec, spec_path)
    obligations = raw_obligations(root, spec)
    resolved, unmatched = resolve_rules(spec, obligations)
    counts = {status: 0 for status in sorted(FINAL_STATUSES)}
    rule_rows: list[dict[str, Any]] = []
    for rule in sorted(spec["rules"], key=lambda item: item["id"]):
        rows = resolved[rule["id"]]
        counts[rule["status"]] += len(rows)
        fingerprint, rust_hashes = rule_fingerprint(root, rule, rows)
        rule_rows.append(
            {
                "id": rule["id"],
                "status": rule["status"],
                "rationale": rule["rationale"],
                "boundary_cases": rule.get("boundary_cases", []),
                "obligation_count": len(rows),
                "obligation_sha256": sha256_bytes(
                    canonical_bytes([row["id"] for row in rows])
                ),
                "fingerprint": fingerprint,
                "rust_sha256": rust_hashes,
            }
        )
    counts["UNCLASSIFIED"] = len(unmatched)
    if spec["claim"] == "whole-go-package" and unmatched:
        sample = [f"{row['source']}#{row['anchor']}" for row in unmatched[:8]]
        raise LockdownError(
            f"whole-go-package claim has {len(unmatched)} unclassified obligations: {sample}"
        )
    inventory_hash = sha256_bytes(canonical_bytes(obligations))
    return {
        "schema": RECEIPT_SCHEMA,
        "checker_sha256": sha256_file(Path(__file__).resolve()),
        "claim": spec["claim"],
        "go_package": spec["go_package"],
        "source_commit": spec["source_commit"],
        "spec_path": spec_path.as_posix(),
        "spec_sha256": sha256_file(root / spec_path),
        "accepted_artifact_sha256": artifacts,
        "extra_artifact_sha256": extras,
        "owned_rust_sha256": owned,
        "inventory_sha256": inventory_hash,
        "counts": counts,
        "unclassified_sha256": sha256_bytes(canonical_bytes([
            row["id"] for row in unmatched
        ])),
        "unclassified_sample": [
            {key: row[key] for key in ("id", "category", "source", "anchor", "owner")}
            for row in unmatched[:12]
        ],
        "rules": rule_rows,
        "verification": {},
    }


def load_receipt(root: Path, spec: dict[str, Any]) -> dict[str, Any] | None:
    path = root / repo_path(spec["receipt"], "receipt")
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise LockdownError(f"invalid receipt: {error}") from error
    if value.get("schema") != RECEIPT_SCHEMA:
        raise LockdownError("receipt has an unsupported schema")
    return value


def merge_valid_verification(state: dict[str, Any], prior: dict[str, Any] | None) -> None:
    if prior is None:
        return
    current = {rule["id"]: rule["fingerprint"] for rule in state["rules"]}
    for rule_id, evidence in prior.get("verification", {}).items():
        if rule_id in current and evidence.get("fingerprint") == current[rule_id]:
            state["verification"][rule_id] = evidence


def write_receipt(root: Path, spec: dict[str, Any], state: dict[str, Any]) -> None:
    path = root / repo_path(spec["receipt"], "receipt")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(state))


def result_summary(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    output = result.stdout.encode()
    return {
        "returncode": result.returncode,
        "output_sha256": sha256_bytes(output),
    }


def verify_rule(root: Path, rule: dict[str, Any], fingerprint: str) -> dict[str, Any]:
    test = rule["test"]
    argv = list(test["argv"])
    cwd = test.get("cwd", ".")
    baseline = run(root, argv, cwd)
    if baseline.returncode != 0:
        raise LockdownError(f"{rule['id']} baseline test failed\n{baseline.stdout}")
    evidence: dict[str, Any] = {
        "fingerprint": fingerprint,
        "command": {"cwd": cwd, "argv": argv},
        "baseline": result_summary(baseline),
    }
    mutation = rule.get("mutation")
    if mutation is None:
        evidence["result"] = "PASSED"
        return evidence
    source = root / repo_path(mutation["path"], "mutation path")
    before = source.read_bytes()
    before_sha = sha256_bytes(before)
    try:
        text = before.decode("utf-8")
        occurrences = text.count(mutation["old"])
        if occurrences != 1:
            raise LockdownError(
                f"{rule['id']} mutation expected one source match, found {occurrences}"
            )
        source.write_text(text.replace(mutation["old"], mutation["new"], 1), encoding="utf-8")
        mutant = run(root, argv, cwd)
        if mutant.returncode == 0:
            raise LockdownError(f"{rule['id']} mutation survived")
        expected_failure = mutation.get("failure_contains")
        if expected_failure is None and argv[0] == "cargo" and "--" in argv:
            expected_failure = argv[argv.index("--") - 1]
        if expected_failure and expected_failure not in mutant.stdout:
            raise LockdownError(
                f"{rule['id']} mutation failed outside its named semantic test\n{mutant.stdout}"
            )
    finally:
        source.write_bytes(before)
    if sha256_file(source) != before_sha:
        raise LockdownError(f"{rule['id']} mutation source was not restored byte-for-byte")
    restored = run(root, argv, cwd)
    if restored.returncode != 0:
        raise LockdownError(f"{rule['id']} restored test failed\n{restored.stdout}")
    evidence.update(
        {
            "mutation": {
                "path": mutation["path"],
                "source_sha256": before_sha,
                "mutant": result_summary(mutant),
                "restored": result_summary(restored),
            },
            "result": "KILLED",
        }
    )
    return evidence


def check_receipt(state: dict[str, Any], receipt: dict[str, Any] | None) -> None:
    if receipt is None:
        raise LockdownError("missing compact receipt; run audit --write or verify")
    expected = copy.deepcopy(state)
    expected["verification"] = receipt.get("verification", {})
    if receipt != expected:
        raise LockdownError("compact receipt is stale; run audit --write or verify")
    fingerprints = {rule["id"]: rule["fingerprint"] for rule in state["rules"]}
    for rule_id, evidence in receipt.get("verification", {}).items():
        if evidence.get("fingerprint") != fingerprints.get(rule_id):
            raise LockdownError(f"stale verification evidence: {rule_id}")
    if state["claim"] == "whole-go-package":
        missing = [
            rule["id"] for rule in state["rules"]
            if rule["status"] == "PORTED" and rule["id"] not in receipt.get("verification", {})
        ]
        if missing:
            raise LockdownError(f"whole-go-package claim has unverified PORTED rules: {missing}")


def select_rules(spec: dict[str, Any], requested: list[str]) -> list[dict[str, Any]]:
    ported = [rule for rule in spec["rules"] if rule["status"] == "PORTED"]
    if not requested:
        return ported
    selected = [rule for rule in ported if rule["id"] in requested]
    missing = sorted(set(requested) - {rule["id"] for rule in selected})
    if missing:
        raise LockdownError(f"unknown or non-PORTED rules requested: {missing}")
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("audit", "check", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--spec", type=Path, required=True)
        if name == "audit":
            command.add_argument("--write", action="store_true")
        if name == "verify":
            command.add_argument("--rule", action="append", default=[])
    args = parser.parse_args()
    root = args.root.resolve()
    spec_path = repo_path(args.spec.as_posix(), "spec")
    spec = load_spec(root, spec_path)
    state = build_state(root, spec_path, spec)
    prior = load_receipt(root, spec)
    merge_valid_verification(state, prior)
    if args.command == "audit":
        if args.write:
            write_receipt(root, spec, state)
        print(json.dumps({"claim": state["claim"], "counts": state["counts"]}, sort_keys=True))
        return 0
    if args.command == "check":
        check_receipt(state, prior)
        print(json.dumps({"claim": state["claim"], "counts": state["counts"]}, sort_keys=True))
        return 0
    fingerprints = {rule["id"]: rule["fingerprint"] for rule in state["rules"]}
    for rule in select_rules(spec, args.rule):
        state["verification"][rule["id"]] = verify_rule(root, rule, fingerprints[rule["id"]])
    write_receipt(root, spec, state)
    print(json.dumps({"claim": state["claim"], "counts": state["counts"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except LockdownError as error:
        print(f"semantic package lockdown failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
