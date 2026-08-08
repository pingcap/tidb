#!/usr/bin/env python3
"""Generate and verify one complete Go-package lockdown receipt.

The checker deliberately has no package-specific policy. Human decisions live
in package.toml and the TSV evidence files beside it; Go syntax comes from the
checked-in go_package_lockdown_inventory tool.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import tomllib
from typing import Iterable


CHECKER_SCHEMA = "go-package-lockdown-checker-v1"
SPEC_SCHEMA = "go-package-lockdown-spec-v1"
LEDGER_SCHEMA = "go-package-lockdown-ledger-v1"
ARTIFACT_SCHEMA = "go-package-lockdown-artifacts-v1"
RECEIPT_SCHEMA = "go-package-lockdown-receipt-v1"
GO_INVENTORY_TOOL = Path("rust/difftests/tools/go_package_lockdown_inventory")
GO_FIXTURE_TOOL = Path("rust/difftests/tools/go_test_fixture_inventory")

ARTIFACT_HEADER = ["path", "role", "traits", "sha256", "bytes", "lines"]
LEDGER_HEADER = [
    "obligation_id",
    "category",
    "source_path",
    "ast_anchor",
    "node_sha256",
    "owner",
    "status",
    "symbol_id",
    "evidence",
    "rule_id",
]
SYMBOL_HEADER = ["symbol_id", "rust_crate", "rust_symbol", "anchor_path", "anchor_name"]
RULE_HEADER = [
    "rule_id",
    "cluster_id",
    "description",
    "obligation_ids",
    "boundary_cases",
    "mutation_ids",
]
MUTATION_PLAN_HEADER = [
    "mutation_id",
    "cluster_id",
    "rule_ids",
    "baseline_commit",
    "rust_path",
    "source_sha256",
    "command",
    "named_test",
]
MUTATION_RESULT_HEADER = [
    "attempt_id",
    "mutation_id",
    "outcome",
    "exit_code",
    "restore_status",
    "restored_source_sha256",
    "named_failure",
]

ALLOWED_ARTIFACT_ROLES = {
    "production-go",
    "test-go",
    "generated-production-go",
    "generated-test-go",
    "build",
    "fixture",
    "generated-input",
    "generated-output",
    "support",
}
ALLOWED_VERDICTS = {"PORTED", "DECLINED", "UNREACHABLE"}
SPEC_FIELDS = {
    "schema", "claim", "go_package", "source_commit", "primary_rust_crate",
    "mapped_rust_crates", "extra_artifacts", "owned_rust_files",
    "excluded_subpackages", "artifact_roles", "unresolved_fixture_evidence",
}
PLATFORM_PARTS = {
    "aix", "android", "darwin", "dragonfly", "freebsd", "illumos", "ios",
    "js", "linux", "netbsd", "openbsd", "plan9", "solaris", "wasip1",
    "windows", "386", "amd64", "arm", "arm64", "loong64", "mips",
    "mips64", "mips64le", "mipsle", "ppc64", "ppc64le", "riscv64",
    "s390x", "wasm",
}


class LockdownError(RuntimeError):
    """A deterministic, user-actionable lockdown validation failure."""


def run(root: Path, command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip() or f"exit {error.returncode}"
        raise LockdownError(f"command failed: {' '.join(command)}: {detail}") from error
    return completed.stdout


def repository_path(value: str, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise LockdownError(f"{field} must be a non-empty repository-relative path")
    pure = PurePosixPath(value)
    if pure.is_absolute() or value != pure.as_posix() or ".." in pure.parts or value == ".":
        raise LockdownError(f"unsafe {field}: {value!r}")
    return Path(*pure.parts)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_lines(path: Path) -> int:
    data = path.read_bytes()
    return data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)


def split_list(value: str) -> list[str]:
    return [item for item in value.split(";") if item]


def validate_tsv_field(value: str, context: str) -> None:
    if "\t" in value or "\n" in value or "\r" in value:
        raise LockdownError(f"{context} contains a tab or newline")


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as output:
        output.write(text)
        temporary = Path(output.name)
    temporary.replace(path)


def tsv_text(schema: str, header: list[str], rows: Iterable[Iterable[str]]) -> str:
    lines = [f"# {schema}", "\t".join(header)]
    for row in rows:
        fields = list(row)
        if len(fields) != len(header):
            raise LockdownError(f"invalid {schema} row width: {fields}")
        for field in fields:
            validate_tsv_field(field, schema)
        lines.append("\t".join(fields))
    return "\n".join(lines) + "\n"


def read_tsv(path: Path, header: list[str]) -> list[dict[str, str]]:
    if not path.is_file():
        raise LockdownError(f"missing evidence file: {path}")
    lines = [
        line for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    if not lines:
        raise LockdownError(f"missing TSV header: {path}")
    reader = csv.DictReader(lines, delimiter="\t")
    if reader.fieldnames != header:
        raise LockdownError(f"invalid TSV header in {path}: {reader.fieldnames}, expected {header}")
    rows = list(reader)
    if any(None in row or None in row.values() for row in rows):
        raise LockdownError(f"invalid TSV row width in {path}")
    return rows


class PackageLockdown:
    def __init__(self, root: Path, spec_path: Path):
        self.root = root.resolve()
        if not self.root.is_dir():
            raise LockdownError(f"repository root is not a directory: {self.root}")
        self.spec_relative = repository_path(spec_path.as_posix(), "--spec")
        self.spec_path = self.root / self.spec_relative
        if not self.spec_path.is_file():
            raise LockdownError(f"package spec does not exist: {self.spec_relative}")
        try:
            self.spec = tomllib.loads(self.spec_path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise LockdownError(f"cannot read {self.spec_relative}: {error}") from error
        self._load_spec()

    def _required_string(self, name: str) -> str:
        value = self.spec.get(name)
        if not isinstance(value, str) or not value:
            raise LockdownError(f"package.toml field {name!r} must be a non-empty string")
        return value

    def _string_list(self, name: str) -> list[str]:
        value = self.spec.get(name)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise LockdownError(f"package.toml field {name!r} must be a list of strings")
        if len(value) != len(set(value)):
            raise LockdownError(f"package.toml field {name!r} contains duplicates")
        return value

    def _load_spec(self) -> None:
        unknown_fields = set(self.spec) - SPEC_FIELDS
        if unknown_fields:
            raise LockdownError(f"package.toml contains unknown fields: {sorted(unknown_fields)}")
        if self.spec.get("schema") != SPEC_SCHEMA:
            raise LockdownError(f"package.toml schema must be {SPEC_SCHEMA!r}")
        if self.spec.get("claim") != "whole-go-package":
            raise LockdownError("package.toml claim must be 'whole-go-package'")
        self.go_package = repository_path(self._required_string("go_package"), "go_package")
        self.source_commit = self._required_string("source_commit")
        if not re.fullmatch(r"[0-9a-f]{40}", self.source_commit):
            raise LockdownError("source_commit must be a full lowercase 40-character Git SHA")
        try:
            run(self.root, ["git", "cat-file", "-e", f"{self.source_commit}^{{commit}}"])
        except LockdownError as error:
            raise LockdownError(f"source_commit is not available: {self.source_commit}") from error

        self.primary_rust_crate = self._required_string("primary_rust_crate")
        self.mapped_rust_crates = self._string_list("mapped_rust_crates")
        if not all(re.fullmatch(r"[A-Za-z0-9_-]+", crate) for crate in self.mapped_rust_crates):
            raise LockdownError("mapped_rust_crates contains an invalid crate name")
        if self.primary_rust_crate not in self.mapped_rust_crates:
            raise LockdownError("primary_rust_crate must appear in mapped_rust_crates")

        self.extra_artifacts = [
            repository_path(value, "extra_artifacts") for value in self._string_list("extra_artifacts")
        ]
        self.owned_rust_files = [
            repository_path(value, "owned_rust_files") for value in self._string_list("owned_rust_files")
        ]
        for path in self.owned_rust_files:
            if not any(path.is_relative_to(Path("rust/crates") / crate) for crate in self.mapped_rust_crates):
                raise LockdownError(f"owned Rust file is outside mapped crates: {path}")

        raw_fixture_evidence = self.spec.get("unresolved_fixture_evidence", {})
        if not isinstance(raw_fixture_evidence, dict) or not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in raw_fixture_evidence.items()
        ):
            raise LockdownError("unresolved_fixture_evidence must be a string-to-string TOML table")
        self.unresolved_fixture_evidence: dict[str, str] = raw_fixture_evidence

        raw_roles = self.spec.get("artifact_roles", {})
        if not isinstance(raw_roles, dict):
            raise LockdownError("artifact_roles must be a TOML table")
        self.artifact_roles: dict[Path, str] = {}
        for raw_path, role in raw_roles.items():
            path = repository_path(raw_path, "artifact_roles path")
            if not isinstance(role, str) or role not in ALLOWED_ARTIFACT_ROLES:
                raise LockdownError(f"invalid artifact role for {path}: {role!r}")
            self.artifact_roles[path] = role

        raw_exclusions = self.spec.get("excluded_subpackages")
        if not isinstance(raw_exclusions, list):
            raise LockdownError("excluded_subpackages must be a list of {path, proof} tables")
        self.excluded_subpackages: dict[Path, str] = {}
        for item in raw_exclusions:
            if not isinstance(item, dict) or set(item) != {"path", "proof"}:
                raise LockdownError("each excluded_subpackages entry must contain only path and proof")
            path = repository_path(item["path"], "excluded_subpackages path")
            proof = item["proof"]
            if not isinstance(proof, str) or not proof.strip():
                raise LockdownError(f"excluded subpackage {path} has no structural proof")
            if path.parent == path or not path.is_relative_to(self.go_package) or path == self.go_package:
                raise LockdownError(f"excluded subpackage is not below {self.go_package}: {path}")
            self.excluded_subpackages[path] = proof

        self.receipt_dir = self.spec_path.parent
        self.artifacts_path = self.receipt_dir / "artifacts.tsv"
        self.ledgers_dir = self.receipt_dir / "ledgers"
        self.symbols_path = self.receipt_dir / "symbols.tsv"
        self.rules_path = self.receipt_dir / "rules.tsv"
        self.mutation_plan_path = self.receipt_dir / "mutation-plan.tsv"
        self.mutation_results_path = self.receipt_dir / "mutation-results.tsv"
        self.receipt_path = self.receipt_dir / "receipt.json"

    def _is_excluded(self, path: Path) -> bool:
        return any(path == excluded or path.is_relative_to(excluded) for excluded in self.excluded_subpackages)

    def _git_paths(self, arguments: list[str]) -> list[Path]:
        output = run(self.root, ["git", *arguments])
        return [repository_path(value, "Git path") for value in output.split("\0") if value]

    def _tracked_artifact_paths(self) -> list[Path]:
        tracked = self._git_paths(["ls-files", "-z", "--", self.go_package.as_posix()])
        package_paths = [path for path in tracked if not self._is_excluded(path)]
        for excluded in self.excluded_subpackages:
            if not any(path == excluded or path.is_relative_to(excluded) for path in tracked):
                raise LockdownError(f"excluded subpackage has no tracked artifacts: {excluded}")
        paths = set(package_paths)
        for extra in self.extra_artifacts:
            if extra in paths:
                raise LockdownError(f"extra_artifact is already package-owned: {extra}")
            if extra not in self._git_paths(["ls-files", "-z", "--", extra.as_posix()]):
                raise LockdownError(f"extra_artifact is not tracked: {extra}")
            paths.add(extra)
        return sorted(paths, key=lambda path: path.as_posix())

    def _untracked_artifact_paths(self) -> list[Path]:
        untracked = self._git_paths(
            ["ls-files", "-z", "--others", "--exclude-standard", "--", self.go_package.as_posix()]
        )
        found = {path for path in untracked if not self._is_excluded(path)}
        for extra in self.extra_artifacts:
            found.update(self._git_paths(
                ["ls-files", "-z", "--others", "--exclude-standard", "--", extra.as_posix()]
            ))
        return sorted(found, key=lambda path: path.as_posix())

    def _automatic_role(self, path: Path, data: bytes) -> str | None:
        if path in self.artifact_roles:
            return self.artifact_roles[path]
        if path.name in {"BUILD", "BUILD.bazel"} or path.suffix == ".bzl":
            return "build"
        if "testdata" in path.parts:
            return "fixture"
        if path.suffix == ".go":
            generated = b"// Code generated" in data
            test = path.name.endswith("_test.go")
            if generated and test:
                return "generated-test-go"
            if generated:
                return "generated-production-go"
            return "test-go" if test else "production-go"
        return None

    def _artifact_traits(self, path: Path, data: bytes) -> str:
        traits: list[str] = []
        if b"//go:build" in data or b"// +build" in data:
            traits.append("build-tag")
        if any(part in PLATFORM_PARTS for part in path.stem.split("_")[1:]):
            traits.append("platform-variant")
        if b"// Code generated" in data:
            traits.append("generated")
        if b"//go:generate" in data:
            traits.append("go-generate")
        if b"//go:embed" in data:
            traits.append("go-embed")
        if "testdata" in path.parts:
            traits.append("testdata")
        return ",".join(traits) if traits else "-"

    def artifact_rows(self) -> list[list[str]]:
        untracked = self._untracked_artifact_paths()
        if untracked:
            raise LockdownError(
                "untracked package artifacts must be staged or removed before generation: "
                + ", ".join(path.as_posix() for path in untracked)
            )
        paths = self._tracked_artifact_paths()
        if not paths:
            raise LockdownError(f"Go package has no tracked artifacts: {self.go_package}")
        rows: list[list[str]] = []
        go_paths: set[Path] = set()
        for path in paths:
            full_path = self.root / path
            if not full_path.is_file():
                raise LockdownError(f"package artifact is not a regular file: {path}")
            data = full_path.read_bytes()
            role = self._automatic_role(path, data)
            if role is None:
                raise LockdownError(
                    f"unclassified package artifact {path}; add an explicit artifact_roles entry"
                )
            if role not in ALLOWED_ARTIFACT_ROLES:
                raise LockdownError(f"invalid artifact role {role!r} for {path}")
            go_role = role in {
                "production-go", "test-go", "generated-production-go", "generated-test-go",
            }
            if go_role and path.suffix != ".go":
                raise LockdownError(f"Go suffix and artifact role disagree for {path}: {role}")
            if path.suffix == ".go" and not go_role and "testdata" not in path.parts:
                raise LockdownError(f"Go suffix and artifact role disagree for {path}: {role}")
            if role.endswith("-go"):
                if path.parent != self.go_package:
                    raise LockdownError(
                        f"Go artifact {path} is outside the non-recursive package directory; "
                        "declare a nested Go package exclusion"
                    )
                go_paths.add(path)
            rows.append(
                [
                    path.as_posix(), role, self._artifact_traits(path, data),
                    hashlib.sha256(data).hexdigest(), str(len(data)), str(source_lines(full_path)),
                ]
            )
        unused_overrides = set(self.artifact_roles) - set(paths)
        if unused_overrides:
            raise LockdownError(
                "artifact_roles contains paths outside the census: "
                + ", ".join(path.as_posix() for path in sorted(unused_overrides))
            )
        if not go_paths:
            raise LockdownError(f"Go package has no direct Go sources: {self.go_package}")
        return rows

    def fixture_accesses(self, artifact_rows: list[list[str]]) -> list[list[str]]:
        manifested = {Path(row[0]) for row in artifact_rows}
        output = run(
            self.root,
            ["go", "run", f"./{GO_FIXTURE_TOOL}", "--root", "."],
        )
        lines = output.rstrip("\n").splitlines()
        if not lines or not lines[0].startswith("# source_path"):
            raise LockdownError("Go fixture inventory returned no valid header")
        accesses: list[list[str]] = []
        unresolved: set[str] = set()
        for line in lines[1:]:
            row = line.split("\t")
            if len(row) != 5:
                raise LockdownError(f"invalid Go fixture inventory row: {row}")
            source = repository_path(row[0], "fixture source_path")
            if source.parent != self.go_package:
                continue
            accesses.append(row)
            if row[4]:
                resolved = repository_path(row[4], "resolved fixture path")
                if resolved not in manifested:
                    raise LockdownError(
                        f"resolved fixture {resolved} used by {source}:{row[1]} is not manifested; "
                        "add it to extra_artifacts when it is outside the package"
                    )
            else:
                key = ":".join(row[:4])
                unresolved.add(key)
        if unresolved != set(self.unresolved_fixture_evidence):
            raise LockdownError(
                f"unresolved fixture evidence is not exact: discovered={sorted(unresolved)}, "
                f"classified={sorted(self.unresolved_fixture_evidence)}"
            )
        for key, evidence in self.unresolved_fixture_evidence.items():
            if "measured:" not in evidence:
                raise LockdownError(f"unresolved fixture {key} lacks measured evidence")
        return accesses

    def raw_obligations(self, artifact_rows: list[list[str]]) -> dict[Path, list[list[str]]]:
        go_paths = {
            Path(row[0]) for row in artifact_rows
            if row[1] in {"production-go", "test-go", "generated-production-go", "generated-test-go"}
        }
        output = run(
            self.root,
            [
                "go", "run", f"./{GO_INVENTORY_TOOL}", "--root", ".",
                "--package", self.go_package.as_posix(),
            ],
        )
        lines = output.rstrip("\n").splitlines()
        if not lines or not lines[0].startswith("# obligation_id"):
            raise LockdownError("Go package inventory returned no valid header")
        grouped = {path: [] for path in go_paths}
        seen: set[str] = set()
        for line in lines[1:]:
            row = line.split("\t")
            if len(row) != 6:
                raise LockdownError(f"invalid Go inventory row: {row}")
            source = repository_path(row[2], "Go inventory source_path")
            if source not in go_paths:
                raise LockdownError(f"Go inventory emitted an unmanifested source: {source}")
            if row[0] in seen:
                raise LockdownError(f"duplicate Go obligation ID: {row[0]}")
            seen.add(row[0])
            grouped[source].append(row)
        for rows in grouped.values():
            rows.sort(key=lambda row: (row[3], row[1], row[0]))
        return grouped

    def _ledger_path(self, source_path: Path) -> Path:
        return self.ledgers_dir / f"{source_path.name}.tsv"

    def _existing_ledgers(self) -> tuple[dict[str, list[str]], set[Path]]:
        rows: dict[str, list[str]] = {}
        files: set[Path] = set()
        if not self.ledgers_dir.exists():
            return rows, files
        for path in sorted(self.ledgers_dir.glob("*.tsv")):
            files.add(path)
            for row in read_tsv(path, LEDGER_HEADER):
                values = [row[column] for column in LEDGER_HEADER]
                obligation_id = row["obligation_id"]
                if obligation_id in rows:
                    raise LockdownError(f"duplicate existing obligation ID: {obligation_id}")
                rows[obligation_id] = values
        return rows, files

    def generate(self) -> tuple[int, int]:
        artifact_rows = self.artifact_rows()
        self.fixture_accesses(artifact_rows)
        grouped = self.raw_obligations(artifact_rows)
        existing, existing_files = self._existing_ledgers()
        raw_by_id = {row[0]: row for rows in grouped.values() for row in rows}
        removed = sorted(set(existing) - set(raw_by_id))
        if removed:
            raise LockdownError(
                "generation would discard classified obligations; review and explicitly remove "
                "retired ledger rows first: " + ", ".join(removed)
            )
        expected_files = {self._ledger_path(source) for source in grouped}
        stale_empty = existing_files - expected_files
        if stale_empty:
            raise LockdownError(
                "generation would discard stale ledger files; remove them explicitly first: "
                + ", ".join(str(path.relative_to(self.root)) for path in sorted(stale_empty))
            )

        writes: list[tuple[Path, str]] = [
            (self.artifacts_path, tsv_text(ARTIFACT_SCHEMA, ARTIFACT_HEADER, artifact_rows))
        ]
        for source, raw_rows in sorted(grouped.items(), key=lambda item: item[0].as_posix()):
            classified: list[list[str]] = []
            for raw in raw_rows:
                prior = existing.get(raw[0])
                if prior is not None:
                    if prior[:6] != raw:
                        raise LockdownError(f"existing raw obligation fields drifted for {raw[0]}")
                    classified.append(raw + prior[6:])
                else:
                    classified.append(raw + ["UNCLASSIFIED", "", "", ""])
            writes.append(
                (self._ledger_path(source), tsv_text(LEDGER_SCHEMA, LEDGER_HEADER, classified))
            )
        for path, header in [
            (self.symbols_path, SYMBOL_HEADER),
            (self.rules_path, RULE_HEADER),
            (self.mutation_plan_path, MUTATION_PLAN_HEADER),
            (self.mutation_results_path, MUTATION_RESULT_HEADER),
        ]:
            if not path.exists():
                writes.append((path, tsv_text(path.stem + "-v1", header, [])))
        for path, text in writes:
            atomic_write(path, text)
        return len(artifact_rows), len(raw_by_id)

    def _stored_ledger_rows(self, grouped: dict[Path, list[list[str]]]) -> list[dict[str, str]]:
        expected_files = {self._ledger_path(source) for source in grouped}
        actual_files = set(self.ledgers_dir.glob("*.tsv")) if self.ledgers_dir.is_dir() else set()
        if actual_files != expected_files:
            missing = sorted(str(path.relative_to(self.root)) for path in expected_files - actual_files)
            extra = sorted(str(path.relative_to(self.root)) for path in actual_files - expected_files)
            raise LockdownError(f"per-file ledger set drifted: missing={missing}, extra={extra}")
        stored: list[dict[str, str]] = []
        raw_by_id = {row[0]: row for rows in grouped.values() for row in rows}
        seen: set[str] = set()
        for source in sorted(grouped, key=lambda path: path.as_posix()):
            rows = read_tsv(self._ledger_path(source), LEDGER_HEADER)
            for row in rows:
                obligation_id = row["obligation_id"]
                if obligation_id in seen:
                    raise LockdownError(f"duplicate classified obligation ID: {obligation_id}")
                seen.add(obligation_id)
                raw = raw_by_id.get(obligation_id)
                if raw is None:
                    raise LockdownError(f"ledger contains an obsolete obligation: {obligation_id}")
                if [row[column] for column in LEDGER_HEADER[:6]] != raw:
                    raise LockdownError(f"raw Go obligation fields drifted: {obligation_id}")
                if Path(row["source_path"]) != source:
                    raise LockdownError(f"obligation is in the wrong per-file ledger: {obligation_id}")
                stored.append(row)
        if seen != set(raw_by_id):
            missing = sorted(set(raw_by_id) - seen)
            raise LockdownError(f"unclassified Go obligations are missing from ledgers: {missing}")
        return stored

    def _validate_verdicts(
        self, ledger_rows: list[dict[str, str]], symbols: list[dict[str, str]]
    ) -> tuple[dict[str, dict[str, str]], set[str], set[str]]:
        symbol_by_id: dict[str, dict[str, str]] = {}
        rust_symbols: set[tuple[str, str]] = set()
        anchors: set[tuple[str, str]] = set()
        for row in symbols:
            symbol_id = row["symbol_id"]
            if not symbol_id or symbol_id in symbol_by_id:
                raise LockdownError(f"blank or duplicate symbol_id: {symbol_id!r}")
            if row["rust_crate"] not in self.mapped_rust_crates:
                raise LockdownError(f"symbol {symbol_id} uses an unowned Rust crate")
            if not row["rust_symbol"] or not row["anchor_name"]:
                raise LockdownError(f"symbol {symbol_id} has an incomplete compile anchor")
            rust_identity = (row["rust_crate"], row["rust_symbol"])
            if rust_identity in rust_symbols:
                raise LockdownError(f"duplicate registered Rust symbol: {rust_identity}")
            rust_symbols.add(rust_identity)
            anchor_path = repository_path(row["anchor_path"], f"symbol {symbol_id} anchor_path")
            expected_crate_root = Path("rust/crates") / row["rust_crate"]
            if not anchor_path.is_relative_to(expected_crate_root):
                raise LockdownError(f"symbol {symbol_id} anchor is outside its mapped Rust crate")
            anchor = self.root / anchor_path
            if not anchor.is_file():
                raise LockdownError(f"symbol {symbol_id} anchor file disappeared: {anchor_path}")
            anchor_identity = (anchor_path.as_posix(), row["anchor_name"])
            if anchor_identity in anchors:
                raise LockdownError(f"duplicate compile anchor: {anchor_identity}")
            anchors.add(anchor_identity)
            text = anchor.read_text(encoding="utf-8")
            if row["rust_symbol"] not in text or row["anchor_name"] not in text:
                raise LockdownError(f"symbol {symbol_id} disappeared from compile anchor {anchor_path}")
            symbol_by_id[symbol_id] = row

        used_symbols: set[str] = set()
        ported_ids: set[str] = set()
        for row in ledger_rows:
            identity = row["obligation_id"]
            status = row["status"]
            evidence = row["evidence"]
            if status not in ALLOWED_VERDICTS:
                raise LockdownError(f"obligation {identity} has no final verdict: {status!r}")
            if status == "PORTED":
                symbol = symbol_by_id.get(row["symbol_id"])
                if symbol is None:
                    raise LockdownError(f"PORTED obligation {identity} has no registered Rust symbol")
                if not row["rule_id"]:
                    raise LockdownError(f"PORTED obligation {identity} has no semantic rule")
                if evidence != f"boundary-test:{symbol['anchor_name']}":
                    raise LockdownError(f"PORTED obligation {identity} lacks boundary-test evidence")
                used_symbols.add(row["symbol_id"])
                ported_ids.add(identity)
            elif status == "DECLINED":
                if row["symbol_id"] != "-" or row["rule_id"] != "-":
                    raise LockdownError(f"DECLINED obligation {identity} claims a symbol or rule")
                go_quote = (
                    f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                    f"@sha256:{row['node_sha256']}"
                )
                if go_quote not in evidence or "measured:" not in evidence:
                    raise LockdownError(f"DECLINED obligation {identity} lacks Go quote or measured evidence")
            else:
                if row["symbol_id"] != "-" or row["rule_id"] != "-":
                    raise LockdownError(f"UNREACHABLE obligation {identity} claims a symbol or rule")
                go_quote = (
                    f"go-quote:{row['source_path']}#{row['ast_anchor']}"
                    f"@sha256:{row['node_sha256']}"
                )
                if go_quote not in evidence or "structural-proof:" not in evidence:
                    raise LockdownError(f"UNREACHABLE obligation {identity} lacks Go quote or structural proof")
        if used_symbols != set(symbol_by_id):
            raise LockdownError(
                f"Rust symbol registry is not exact: used={sorted(used_symbols)}, "
                f"registered={sorted(symbol_by_id)}"
            )
        return symbol_by_id, ported_ids, used_symbols

    def _validate_rules_and_mutations(
        self,
        ledger_rows: list[dict[str, str]],
        ported_ids: set[str],
        rules: list[dict[str, str]],
        plans: list[dict[str, str]],
        results: list[dict[str, str]],
    ) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]], dict[str, int]]:
        ledger_rule = {
            row["obligation_id"]: row["rule_id"] for row in ledger_rows if row["status"] == "PORTED"
        }
        rule_by_id: dict[str, dict[str, str]] = {}
        obligation_rule: dict[str, str] = {}
        rule_mutations: dict[str, set[str]] = {}
        for row in rules:
            rule_id = row["rule_id"]
            if not rule_id or rule_id in rule_by_id:
                raise LockdownError(f"blank or duplicate rule_id: {rule_id!r}")
            if not row["cluster_id"] or not row["description"]:
                raise LockdownError(f"semantic rule {rule_id} has no cluster or description")
            obligations = split_list(row["obligation_ids"])
            boundaries = split_list(row["boundary_cases"])
            mutations = split_list(row["mutation_ids"])
            if not obligations or len(boundaries) < 2 or not mutations:
                raise LockdownError(f"semantic rule {rule_id} lacks obligations, boundaries, or mutations")
            for identity in obligations:
                if identity in obligation_rule:
                    raise LockdownError(f"PORTED obligation {identity} appears in multiple semantic rules")
                obligation_rule[identity] = rule_id
            rule_mutations[rule_id] = set(mutations)
            rule_by_id[rule_id] = row
        if set(obligation_rule) != ported_ids:
            raise LockdownError(
                f"semantic rule obligation coverage is not exact: rules={sorted(obligation_rule)}, "
                f"ported={sorted(ported_ids)}"
            )
        for identity, rule_id in obligation_rule.items():
            if ledger_rule[identity] != rule_id:
                raise LockdownError(f"ledger/rules semantic rule mismatch for {identity}")

        plan_by_id: dict[str, dict[str, str]] = {}
        plan_rule_mutations = {rule_id: set() for rule_id in rule_by_id}
        for row in plans:
            mutation_id = row["mutation_id"]
            if not mutation_id or mutation_id in plan_by_id:
                raise LockdownError(f"blank or duplicate mutation_id: {mutation_id!r}")
            rule_ids = split_list(row["rule_ids"])
            if len(rule_ids) != 1:
                raise LockdownError(f"mutation {mutation_id} must alter exactly one semantic rule")
            for rule_id in rule_ids:
                rule = rule_by_id.get(rule_id)
                if rule is None:
                    raise LockdownError(f"mutation {mutation_id} names unknown rule {rule_id}")
                if rule["cluster_id"] != row["cluster_id"]:
                    raise LockdownError(f"mutation {mutation_id} cluster differs from rule {rule_id}")
                plan_rule_mutations[rule_id].add(mutation_id)
            if not re.fullmatch(r"[0-9a-f]{40}", row["baseline_commit"]):
                raise LockdownError(f"mutation {mutation_id} has no full baseline commit")
            try:
                run(self.root, ["git", "cat-file", "-e", f"{row['baseline_commit']}^{{commit}}"])
            except LockdownError as error:
                raise LockdownError(f"mutation {mutation_id} baseline commit is unavailable") from error
            rust_path = repository_path(row["rust_path"], f"mutation {mutation_id} rust_path")
            if not any(
                rust_path.is_relative_to(Path("rust/crates") / crate)
                for crate in self.mapped_rust_crates
            ):
                raise LockdownError(f"mutation {mutation_id} source is outside mapped Rust crates")
            full_path = self.root / rust_path
            if not full_path.is_file():
                raise LockdownError(f"mutation source disappeared: {rust_path}")
            current_hash = sha256(full_path)
            if row["source_sha256"] != current_hash:
                raise LockdownError(f"mutation source hash drifted: {mutation_id}")
            if not row["command"] or not row["named_test"] or row["named_test"] not in row["command"]:
                raise LockdownError(f"mutation {mutation_id} lacks an exact named-test command")
            plan_by_id[mutation_id] = row
        if set(plan_by_id) != {item for values in rule_mutations.values() for item in values}:
            raise LockdownError("mutation plan contains missing or unreferenced mutation IDs")
        for rule_id in rule_by_id:
            if plan_rule_mutations[rule_id] != rule_mutations[rule_id]:
                raise LockdownError(f"rule/mutation plan mapping drifted for {rule_id}")

        attempts_by_mutation: dict[str, list[dict[str, str]]] = {
            mutation_id: [] for mutation_id in plan_by_id
        }
        attempt_ids: set[str] = set()
        outcome_counts: dict[str, int] = {}
        for row in results:
            attempt_id = row["attempt_id"]
            if not attempt_id or attempt_id in attempt_ids:
                raise LockdownError(f"blank or duplicate mutation attempt_id: {attempt_id!r}")
            attempt_ids.add(attempt_id)
            plan = plan_by_id.get(row["mutation_id"])
            if plan is None:
                raise LockdownError(f"mutation result names unknown mutation {row['mutation_id']}")
            if row["outcome"] not in {"KILLED", "SURVIVED"}:
                raise LockdownError(f"invalid mutation outcome in {attempt_id}")
            try:
                exit_code = int(row["exit_code"])
            except ValueError as error:
                raise LockdownError(f"mutation {attempt_id} has a non-integer exit code") from error
            if (row["outcome"] == "KILLED") != (exit_code != 0):
                raise LockdownError(f"mutation {attempt_id} outcome disagrees with exit code")
            if row["restore_status"] != "PASS":
                raise LockdownError(f"mutation {attempt_id} was not restored")
            if row["restored_source_sha256"] != plan["source_sha256"]:
                raise LockdownError(f"mutation {attempt_id} restored the wrong source bytes")
            if row["outcome"] == "KILLED" and plan["named_test"] not in row["named_failure"]:
                raise LockdownError(f"mutation {attempt_id} lacks its named failure")
            attempts_by_mutation[row["mutation_id"]].append(row)
            outcome_counts[row["outcome"]] = outcome_counts.get(row["outcome"], 0) + 1
        for mutation_id, attempts in attempts_by_mutation.items():
            if not attempts:
                raise LockdownError(f"mutation {mutation_id} has no recorded attempt")
            if attempts[-1]["outcome"] != "KILLED":
                raise LockdownError(f"mutation {mutation_id} does not end with a killed attempt")
        return rule_by_id, plan_by_id, outcome_counts

    def _owned_hashes(
        self, grouped: dict[Path, list[list[str]]], symbols: list[dict[str, str]]
    ) -> dict[str, str]:
        paths = {
            self.spec_relative,
            self.artifacts_path.relative_to(self.root),
            self.symbols_path.relative_to(self.root),
            self.rules_path.relative_to(self.root),
            self.mutation_plan_path.relative_to(self.root),
            self.mutation_results_path.relative_to(self.root),
            *[self._ledger_path(source).relative_to(self.root) for source in grouped],
            *self.owned_rust_files,
            *[repository_path(row["anchor_path"], "symbol anchor_path") for row in symbols],
        }
        hashes: dict[str, str] = {}
        for path in sorted(paths, key=lambda item: item.as_posix()):
            full_path = self.root / path
            if not full_path.is_file():
                raise LockdownError(f"receipt-owned file disappeared: {path}")
            hashes[path.as_posix()] = sha256(full_path)
        return hashes

    def _validate(self) -> tuple[dict[str, object], int, int]:
        artifact_rows = self.artifact_rows()
        fixture_accesses = self.fixture_accesses(artifact_rows)
        expected_artifacts = tsv_text(ARTIFACT_SCHEMA, ARTIFACT_HEADER, artifact_rows)
        if not self.artifacts_path.is_file() or self.artifacts_path.read_text(encoding="utf-8") != expected_artifacts:
            raise LockdownError("package artifact manifest drifted; run generate only after reviewing the change")
        grouped = self.raw_obligations(artifact_rows)
        ledger_rows = self._stored_ledger_rows(grouped)
        symbols = read_tsv(self.symbols_path, SYMBOL_HEADER)
        rules = read_tsv(self.rules_path, RULE_HEADER)
        plans = read_tsv(self.mutation_plan_path, MUTATION_PLAN_HEADER)
        results = read_tsv(self.mutation_results_path, MUTATION_RESULT_HEADER)
        _symbol_by_id, ported_ids, _used_symbols = self._validate_verdicts(ledger_rows, symbols)
        rule_by_id, plan_by_id, outcome_counts = self._validate_rules_and_mutations(
            ledger_rows, ported_ids, rules, plans, results
        )

        role_counts: dict[str, int] = {role: 0 for role in ALLOWED_ARTIFACT_ROLES}
        trait_counts: dict[str, int] = {
            trait: 0 for trait in [
                "build-tag", "platform-variant", "generated", "go-generate", "go-embed",
                "testdata",
            ]
        }
        for row in artifact_rows:
            role_counts[row[1]] = role_counts.get(row[1], 0) + 1
            for trait in split_list(row[2].replace(",", ";")) if row[2] != "-" else []:
                trait_counts[trait] = trait_counts.get(trait, 0) + 1
        category_counts: dict[str, int] = {}
        status_counts: dict[str, int] = {}
        for row in ledger_rows:
            category_counts[row["category"]] = category_counts.get(row["category"], 0) + 1
            status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
        receipt: dict[str, object] = {
            "schema": RECEIPT_SCHEMA,
            "checker_schema": CHECKER_SCHEMA,
            "claim": "whole-go-package",
            "completion_kind": "lockdown" if ported_ids else "falsification",
            "go_package": self.go_package.as_posix(),
            "source_commit": self.source_commit,
            "primary_rust_crate": self.primary_rust_crate,
            "mapped_rust_crates": self.mapped_rust_crates,
            "excluded_subpackages": {
                path.as_posix(): proof for path, proof in sorted(self.excluded_subpackages.items())
            },
            "artifact_count": len(artifact_rows),
            "artifact_role_counts": dict(sorted(role_counts.items())),
            "artifact_trait_counts": dict(sorted(trait_counts.items())),
            "fixture_access_count": len(fixture_accesses),
            "unresolved_fixture_count": len(self.unresolved_fixture_evidence),
            "go_file_count": len(grouped),
            "obligation_count": len(ledger_rows),
            "category_counts": dict(sorted(category_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
            "symbol_count": len(symbols),
            "semantic_rule_count": len(rule_by_id),
            "mutation_count": len(plan_by_id),
            "mutation_attempt_count": len(results),
            "mutation_outcome_counts": dict(sorted(outcome_counts.items())),
            "owned_file_sha256": self._owned_hashes(grouped, symbols),
        }
        return receipt, len(artifact_rows), len(ledger_rows)

    def write_receipt(self) -> tuple[int, int]:
        receipt, artifacts, obligations = self._validate()
        atomic_write(self.receipt_path, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        return artifacts, obligations

    def check(self) -> tuple[int, int]:
        receipt, artifacts, obligations = self._validate()
        if not self.receipt_path.is_file():
            raise LockdownError("content-addressed receipt is missing; run write-receipt")
        try:
            stored = json.loads(self.receipt_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise LockdownError(f"cannot read receipt {self.receipt_path}: {error}") from error
        if stored != receipt:
            raise LockdownError("content-addressed package receipt drifted; run write-receipt after review")
        return artifacts, obligations


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ["generate", "check", "write-receipt"]:
        child = subparsers.add_parser(command)
        child.add_argument("--spec", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    try:
        lockdown = PackageLockdown(args.root, args.spec)
        if args.command == "generate":
            artifacts, obligations = lockdown.generate()
            action = "generated"
        elif args.command == "write-receipt":
            artifacts, obligations = lockdown.write_receipt()
            action = "wrote receipt for"
        else:
            artifacts, obligations = lockdown.check()
            action = "checked"
        print(
            f"{action} {lockdown.go_package}: {artifacts} artifacts, "
            f"{obligations} AST obligations"
        )
    except (LockdownError, OSError) as error:
        print(f"go-package-lockdown failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
