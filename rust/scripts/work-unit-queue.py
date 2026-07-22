#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fast, checked-ledger work-family queue and local atomic claim manager."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from typing import Iterable, Iterator


RUST_ROOT = Path(
    os.environ.get("TIDB_REWRITE_RUST_ROOT", Path(__file__).resolve().parents[1])
)
SOURCE_LEDGER = Path("difftests/corpus/coverage/go_source_inventory.tsv")
TEST_LEDGER = Path("difftests/corpus/coverage/go_test_inventory.tsv")
SUPPORT_LEDGER = Path("difftests/corpus/coverage/go_package_support_inventory.tsv")
PACKAGE_EVIDENCE_DIR = Path("workstreams/package-evidence")
PACKAGE_RECEIPTS_DIR = Path("workstreams/package-receipts")
MODULE_SOURCE_LEDGER = Path("difftests/corpus/coverage/external_go_source_inventory.tsv")
MODULE_TEST_LEDGER = Path("difftests/corpus/coverage/external_go_test_inventory.tsv")
CLAIMS_DIR = Path("workstreams/claims")
TRANSFERS_DIR = Path("difftests/corpus/coverage/evidence/transfers")
SLICES_DIR = Path("workstreams/slices")
LEGACY_SCHEMA1_SLICES = SLICES_DIR / "legacy-schema1-slices.tsv"
CAMPAIGNS_DIR = Path("workstreams/campaigns")
INTEGRATED_CAMPAIGN_MEMBERS = CAMPAIGNS_DIR / "integrated-members.tsv"
INTEGRATION_RECEIPT = Path("workstreams/integration-receipt.json")
INTEGRATION_ATTEMPT = Path("workstreams/integration-attempt.json")
OWNER_RE = re.compile(r"^[a-z0-9][a-z0-9.-]*$")
GO_TOKEN_RE = re.compile(
    r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|`[^`]*`|[A-Za-z_]\w*|[()]',
    re.DOTALL,
)
OPEN_STATUSES = {"UNTRIAGED", "PARTIAL", "BLOCKED"}
SLICE_STATUSES = {"ready", "active", "partial", "covered", "blocked", "retired"}
PACKAGE_SLICE_STATUSES = {"inventory", "ready", "active", "blocked", "covered"}
WORKSPACE_INTEGRATION_PATHS = {"rust/Cargo.toml", "rust/Cargo.lock"}
EVIDENCE_MINIMUM_STATUSES = {"PARTIAL", "COVERED"}
EVIDENCE_STATUS_RANK = {"UNTRIAGED": 0, "PARTIAL": 1, "COVERED": 2}
CAMPAIGN_STATUSES = {"planned", "active", "frozen", "integrated"}
CAMPAIGN_MIN_SOURCE_COUNT = 9
CAMPAIGN_MIN_TEST_COUNT = 50
SUPPORT_DISPOSITIONS = {
    "runtime-transcreated",
    "test-transcreated",
    "build-metadata-reviewed",
    "generated-input-reviewed",
}


def read_tsv(path: Path, fields: int) -> list[list[str]]:
    rows: list[list[str]] = []
    with path.open(encoding="utf-8") as source:
        for number, raw in enumerate(source, 1):
            line = raw.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            row = line.split("\t")
            if len(row) != fields:
                raise ValueError(f"{path}:{number}: expected {fields} fields, got {len(row)}")
            rows.append(row)
    return rows


def validate_evidence_artifact(
    root: Path, ledger: Path, anchor: str, artifact: str
) -> None:
    """Requires one existing artifact path for every declared evidence row."""
    if artifact == "-":
        return
    if "," in artifact:
        raise ValueError(
            f"{root / ledger}: {anchor} evidence artifact must be one path, "
            f"not a comma-separated list: {artifact}"
        )
    if not (root.parent / artifact).is_file():
        raise ValueError(
            f"{root / ledger}: {anchor} evidence artifact is missing: {artifact}"
        )


def load_source_rows(root: Path) -> list[dict[str, object]]:
    rows = []
    for path, lines, target, generated, status, owner, artifact, note in read_tsv(
        root / SOURCE_LEDGER, 8
    ):
        validate_evidence_artifact(root, SOURCE_LEDGER, path, artifact)
        rows.append(
            {
                "path": path,
                "lines": int(lines),
                "target": target,
                "generated": generated == "true",
                "status": status,
                "owner": owner,
                "artifact": artifact,
                "note": note,
            }
        )
    return rows


def load_test_rows(root: Path) -> list[dict[str, object]]:
    rows = []
    for kind, path, line, name, ring, status, owner, artifact, note in read_tsv(
        root / TEST_LEDGER, 9
    ):
        validate_evidence_artifact(
            root, TEST_LEDGER, f"{path}:{line}:{name}", artifact
        )
        rows.append(
            {
                "kind": kind,
                "path": path,
                "line": int(line),
                "name": name,
                "ring": ring,
                "status": status,
                "owner": owner,
                "artifact": artifact,
                "note": note,
            }
        )
    return rows


def test_key(row: dict[str, object]) -> str:
    return f"{row['path']}:{row['line']}:{row['name']}"


def load_module_source_anchors(root: Path) -> set[str]:
    return set(load_module_source_rows(root))


def load_module_source_rows(root: Path) -> dict[str, dict[str, str]]:
    path = root / MODULE_SOURCE_LEDGER
    if not path.exists():
        return {}
    rows = {}
    for universe, source, _lines, sha, status, owner, artifact, note in read_tsv(path, 8):
        anchor = f"{universe}::{source}"
        if anchor in rows:
            raise ValueError(f"{path}: duplicate qualified module source anchor {anchor}")
        validate_evidence_artifact(root, MODULE_SOURCE_LEDGER, anchor, artifact)
        rows[anchor] = {
            "status": status,
            "owner": owner,
            "artifact": artifact,
            "note": note,
            "upstream_sha256": sha,
        }
    return rows


def load_module_test_anchors(root: Path) -> set[str]:
    return set(load_module_test_rows(root))


def load_module_test_rows(root: Path) -> dict[str, dict[str, str]]:
    path = root / MODULE_TEST_LEDGER
    if not path.exists():
        return {}
    rows = {}
    for universe, _kind, source, line, name, _ring, sha, status, owner, artifact, note in read_tsv(path, 11):
        anchor = f"{universe}::{source}:{int(line)}:{name}"
        if anchor in rows:
            raise ValueError(f"{path}: duplicate qualified module test anchor {anchor}")
        validate_evidence_artifact(root, MODULE_TEST_LEDGER, anchor, artifact)
        rows[anchor] = {
            "status": status,
            "owner": owner,
            "artifact": artifact,
            "note": note,
            "upstream_sha256": sha,
        }
    return rows


def require_promoted_module_evidence(root: Path, claim: dict[str, object], status: str) -> None:
    source_rows = load_module_source_rows(root)
    test_rows = load_module_test_rows(root)
    required_status = "COVERED" if status == "covered" else "PARTIAL"
    for kind, anchors, rows in (
        ("module source", claim["_module_sources"], source_rows),
        ("module test", claim["_module_tests"], test_rows),
    ):
        for anchor in anchors:
            row = rows[anchor]
            if row["owner"] != claim["owner"] or EVIDENCE_STATUS_RANK.get(row["status"], -1) < EVIDENCE_STATUS_RANK[required_status]:
                raise ValueError(
                    f"integrated release requires promoted {kind} {anchor} owned by "
                    f"{claim['owner']} at {required_status} or better; found "
                    f"{row['owner']}@{row['status']}"
                )


def _normalized_package(value: str, *, qualified: bool) -> tuple[str | None, str]:
    """Validates one non-recursive Go package selector."""
    universe = None
    package = value
    if qualified:
        universe, separator, package = value.partition("::")
        if not separator or not universe or not OWNER_RE.fullmatch(universe):
            raise ValueError(
                f"invalid module package {value!r}; use MODULE::path/to/package"
            )
    candidate = Path(package)
    if (
        not package
        or package.startswith("/")
        or package.endswith("/")
        or candidate.as_posix() != package
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or "testdata" in candidate.parts
    ):
        label = "module package" if qualified else "Go package"
        raise ValueError(f"invalid {label} {value!r}")
    return universe, package


def _normalized_repo_path(value: str, *, label: str) -> str:
    """Returns one canonical repository-relative POSIX path."""
    candidate = Path(value)
    if (
        not value
        or value.startswith("/")
        or value.endswith("/")
        or candidate.is_absolute()
        or candidate.as_posix() != value
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or any(character in value for character in ("\0", "\n", "\r", "\t"))
    ):
        raise ValueError(f"invalid {label} {value!r}; use a canonical repo-relative path")
    return value


def _paths_overlap(first: str, second: str) -> bool:
    """Whether either canonical path owns the other path's subtree."""
    first_parts = Path(first).parts
    second_parts = Path(second).parts
    shortest = min(len(first_parts), len(second_parts))
    return first_parts[:shortest] == second_parts[:shortest]


def _first_path_overlap(
    paths: list[tuple[str, str]],
) -> tuple[str, str, str, str] | None:
    """Returns the first deterministic ancestor/descendant ownership collision."""
    ordered = sorted(paths)
    for index, (first_path, first_owner) in enumerate(ordered):
        for second_path, second_owner in ordered[index + 1 :]:
            if _paths_overlap(first_path, second_path):
                return first_path, first_owner, second_path, second_owner
    return None


def load_workspace_crates(root: Path) -> dict[str, str]:
    """Maps Cargo package names to canonical repository-relative crate roots."""
    workspace_path = root / "Cargo.toml"
    if not workspace_path.is_file():
        raise ValueError(f"{workspace_path}: Rust workspace manifest is missing")
    with workspace_path.open("rb") as source:
        workspace = tomllib.load(source)
    members = workspace.get("workspace", {}).get("members")
    if not isinstance(members, list) or not all(
        isinstance(member, str) and member for member in members
    ):
        raise ValueError(f"{workspace_path}: workspace.members must be a string array")
    repo_prefix = root.relative_to(root.parent).as_posix()
    crates: dict[str, str] = {}
    for member in members:
        normalized = _normalized_repo_path(member, label="workspace member")
        matches = sorted(root.glob(normalized)) if any(char in member for char in "*?[") else [root / normalized]
        if not matches:
            raise ValueError(f"{workspace_path}: workspace member {member!r} matches no crate")
        for directory in matches:
            cargo_path = directory / "Cargo.toml"
            if not cargo_path.is_file():
                raise ValueError(f"{cargo_path}: workspace crate manifest is missing")
            with cargo_path.open("rb") as cargo_source:
                cargo = tomllib.load(cargo_source)
            name = cargo.get("package", {}).get("name")
            if not isinstance(name, str) or not OWNER_RE.fullmatch(name):
                raise ValueError(f"{cargo_path}: package.name must be a valid crate target")
            if name in crates:
                raise ValueError(f"{workspace_path}: duplicate workspace package {name}")
            relative_member = directory.relative_to(root).as_posix()
            crates[name] = f"{repo_prefix}/{relative_member}"
    return crates


def _nearest_package(path: str, packages: set[str]) -> str | None:
    """Assigns a test/support path to its nearest ancestor Go package."""
    parent = Path(path).parent
    while parent.as_posix() not in {"", "."}:
        candidate = parent.as_posix()
        if candidate in packages:
            return candidate
        parent = parent.parent
    return None


def _group_package_anchors(
    paths: Iterable[str], packages: set[str]
) -> dict[str, list[str]]:
    """Groups inventory paths once instead of rescanning it for every package."""
    grouped = {package: [] for package in packages}
    for path in paths:
        package = _nearest_package(path, packages)
        if package is not None:
            grouped[package].append(path)
    for anchors in grouped.values():
        anchors.sort()
    return grouped


def _group_go_inventory(
    source_by_path: dict[str, dict[str, object]],
    test_by_anchor: dict[str, dict[str, object]],
) -> tuple[set[str], dict[str, list[str]], dict[str, list[str]]]:
    """Returns one canonical package assignment for source and test anchors."""
    packages = _go_package_directories(source_by_path, test_by_anchor)
    sources = _group_package_anchors(source_by_path, packages)
    tests = {package: [] for package in packages}
    for anchor, row in test_by_anchor.items():
        package = _nearest_package(str(row["path"]), packages)
        if package is not None:
            tests[package].append(anchor)
    for anchors in tests.values():
        anchors.sort()
    return packages, sources, tests


def _go_module_path(repo_root: Path) -> str:
    go_mod = repo_root / "go.mod"
    if not go_mod.is_file():
        raise ValueError(f"{go_mod}: missing Go module manifest")
    for line_number, raw_line in enumerate(
        go_mod.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.split("//", 1)[0].strip()
        if not line.startswith("module"):
            continue
        fields = line.split()
        if len(fields) != 2 or fields[0] != "module":
            raise ValueError(f"{go_mod}:{line_number}: invalid module directive")
        return fields[1]
    raise ValueError(f"{go_mod}: missing module directive")


def _unquote_go_import(path: Path, token: str) -> str:
    if token.startswith("`"):
        return token[1:-1].replace("\r", "")
    try:
        value = json.loads(token)
    except json.JSONDecodeError as error:
        raise ValueError(f"{path}: unsupported Go import literal {token!r}") from error
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: invalid empty Go import path")
    return value


def go_file_imports(path: Path) -> set[str]:
    """Extracts import paths from one Go file without selecting build tags."""
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ValueError(f"cannot read Go source {path}: {error}") from error
    tokens = [
        match.group(0)
        for match in GO_TOKEN_RE.finditer(source)
        if not match.group(0).startswith(("//", "/*"))
    ]
    imports: set[str] = set()
    index = 0
    while index < len(tokens):
        if tokens[index] != "import":
            index += 1
            continue
        index += 1
        if index >= len(tokens):
            raise ValueError(f"{path}: incomplete Go import declaration")
        if tokens[index].startswith(('"', "`")):
            imports.add(_unquote_go_import(path, tokens[index]))
            index += 1
            continue
        if tokens[index] != "(":
            # A single import may carry a local alias (`name`, `_`, or `.`).
            # The lexer intentionally drops punctuation other than parens, so
            # a dot alias appears as the following string literal.
            if re.fullmatch(r"[A-Za-z_]\w*", tokens[index]):
                index += 1
            if index < len(tokens) and tokens[index].startswith(('"', "`")):
                imports.add(_unquote_go_import(path, tokens[index]))
                index += 1
                continue
        if index >= len(tokens) or tokens[index] != "(":
            raise ValueError(f"{path}: invalid Go import declaration")
        index += 1
        while index < len(tokens) and tokens[index] != ")":
            if tokens[index].startswith(('"', "`")):
                imports.add(_unquote_go_import(path, tokens[index]))
            index += 1
        if index >= len(tokens):
            raise ValueError(f"{path}: unterminated Go import block")
        index += 1
    return imports


def internal_go_dependencies(
    root: Path, sources: list[str]
) -> set[str]:
    """Returns direct TiDB package imports across every tracked source variant."""
    module_prefix = _go_module_path(root.parent) + "/"
    dependencies: set[str] = set()
    for source in sources:
        for imported in go_file_imports(root.parent / source):
            if not imported.startswith(module_prefix):
                continue
            package = imported.removeprefix(module_prefix)
            dependencies.add(package)
    return dependencies


def _go_package_directories(
    source_rows: dict[str, dict[str, object]],
    test_rows: dict[str, dict[str, object]],
) -> set[str]:
    """Returns production and test-only Go package directories."""
    packages = {
        Path(path).parent.as_posix()
        for path in source_rows
        if "testdata" not in Path(path).parts
    }
    packages.update(
        Path(str(row["path"])).parent.as_posix()
        for row in test_rows.values()
        if str(row["path"]).endswith(".go")
        and str(row["kind"]).startswith("go_test")
        and "testdata" not in Path(str(row["path"])).parts
    )
    return packages


def _module_package_directories(
    universe: str,
    source_rows: dict[str, dict[str, str]],
    test_rows: dict[str, dict[str, str]],
) -> set[str]:
    prefix = f"{universe}::"
    packages = {
        Path(anchor.removeprefix(prefix)).parent.as_posix()
        for anchor in source_rows
        if anchor.startswith(prefix)
        and "testdata" not in Path(anchor.removeprefix(prefix)).parts
    }
    packages.update(
        Path(anchor.removeprefix(prefix).rsplit(":", 2)[0]).parent.as_posix()
        for anchor in test_rows
        if anchor.startswith(prefix)
        and "testdata"
        not in Path(anchor.removeprefix(prefix).rsplit(":", 2)[0]).parts
    )
    return packages


def load_support_rows(root: Path) -> dict[str, dict[str, str]]:
    """Loads content-addressed package support anchors."""
    rows: dict[str, dict[str, str]] = {}
    tracked = _tracked_entries(root.parent)
    for package, support_path, sha256 in read_tsv(root / SUPPORT_LEDGER, 3):
        _, normalized = _normalized_package(package, qualified=False)
        support_path = _normalized_repo_path(support_path, label="support path")
        candidate = Path(support_path)
        if (
            not candidate.is_relative_to(Path(normalized))
        ):
            raise ValueError(
                f"{root / SUPPORT_LEDGER}: invalid support path {support_path!r} "
                f"for package {package!r}"
            )
        if not re.fullmatch(r"[0-9a-f]{64}", sha256):
            raise ValueError(
                f"{root / SUPPORT_LEDGER}: invalid sha256 for {support_path}"
            )
        anchor = f"{support_path}@{sha256}"
        if support_path in {row["path"] for row in rows.values()}:
            raise ValueError(
                f"{root / SUPPORT_LEDGER}: duplicate support path {support_path}"
            )
        actual_sha256 = tracked_path_digest(root.parent, support_path, tracked)
        if actual_sha256 != sha256:
            raise ValueError(
                f"{root / SUPPORT_LEDGER}: stale sha256 for {support_path}; "
                f"expected {actual_sha256}, found {sha256}"
            )
        rows[anchor] = {
            "package": normalized,
            "path": support_path,
            "sha256": sha256,
        }
    return rows


def load_package_support_evidence(
    root: Path, owner: str, expected_anchors: list[str]
) -> list[dict[str, str]]:
    """Loads one exact, owner-named disposition for each package support file."""
    path = root / PACKAGE_EVIDENCE_DIR / f"{owner}-support.tsv"
    if not expected_anchors:
        if path.exists() and read_tsv(path, 5):
            raise ValueError(f"{path}: package has no support inventory")
        return []
    if not path.is_file():
        raise ValueError(f"{path}: missing package support evidence")
    support_rows = load_support_rows(root)
    expected = {anchor: support_rows[anchor] for anchor in expected_anchors}
    for anchor, row in expected.items():
        support_path = root.parent / row["path"]
        if not support_path.is_file():
            raise ValueError(f"{path}: package support is missing: {row['path']}")
        current = hashlib.sha256(support_path.read_bytes()).hexdigest()
        if current != row["sha256"]:
            raise ValueError(
                f"{path}: package support digest changed for {row['path']}; "
                f"expected {row['sha256']}, found {current}"
            )
    found: dict[str, dict[str, str]] = {}
    for support_path, sha256, disposition, evidence_artifact, note in read_tsv(path, 5):
        anchor = f"{support_path}@{sha256}"
        if anchor in found:
            raise ValueError(f"{path}: duplicate support evidence {anchor}")
        if anchor not in expected:
            raise ValueError(f"{path}: stale or foreign support evidence {anchor}")
        if disposition not in SUPPORT_DISPOSITIONS:
            raise ValueError(
                f"{path}: invalid support disposition {disposition!r} for {support_path}"
            )
        if not note or note == "-":
            raise ValueError(f"{path}: support evidence {support_path} requires a note")
        artifact_path = Path(evidence_artifact)
        if (
            evidence_artifact == "-"
            or artifact_path.is_absolute()
            or artifact_path.as_posix() != evidence_artifact
            or any(part in {"", ".", ".."} for part in artifact_path.parts)
            or not (root.parent / artifact_path).is_file()
        ):
            raise ValueError(
                f"{path}: support evidence artifact is missing: {evidence_artifact}"
            )
        found[anchor] = {
            "anchor": anchor,
            "path": support_path,
            "sha256": sha256,
            "disposition": disposition,
            "evidence_artifact": evidence_artifact,
            "evidence_sha256": file_digest(root.parent / artifact_path),
            "note": note,
        }
    missing = sorted(set(expected) - set(found))
    if missing:
        raise ValueError(f"{path}: missing support evidence {missing[0]}")
    return [found[anchor] for anchor in sorted(found)]


def expand_go_packages(
    packages: list[str],
    source_rows: dict[str, dict[str, object]],
    test_rows: dict[str, dict[str, object]],
    support_rows: dict[str, dict[str, str]],
) -> tuple[list[str], list[str], list[str]]:
    """Expands checked TiDB inventories without recursing into subpackages."""
    known_packages = _go_package_directories(source_rows, test_rows)
    sources: set[str] = set()
    tests: set[str] = set()
    supports: set[str] = set()
    for raw_package in packages:
        _, package = _normalized_package(raw_package, qualified=False)
        package_sources = {
            path
            for path in source_rows
            if _nearest_package(path, known_packages) == package
        }
        package_tests = {
            anchor
            for anchor, row in test_rows.items()
            if _nearest_package(str(row["path"]), known_packages) == package
        }
        if not package_sources and not package_tests:
            raise ValueError(f"Go package {package!r} has no checked inventory")
        sources.update(package_sources)
        tests.update(package_tests)
        supports.update(
            anchor
            for anchor, row in support_rows.items()
            if row["package"] == package
        )
    return sorted(sources), sorted(tests), sorted(supports)


def expand_module_packages(
    packages: list[str],
    source_rows: dict[str, dict[str, str]],
    test_rows: dict[str, dict[str, str]],
) -> tuple[list[str], list[str]]:
    """Expands module-qualified packages from the pinned external inventories."""
    sources: set[str] = set()
    tests: set[str] = set()
    for raw_package in packages:
        universe, package = _normalized_package(raw_package, qualified=True)
        assert universe is not None
        prefix = f"{universe}::"
        known_packages = _module_package_directories(
            universe, source_rows, test_rows
        )
        package_sources = {
            anchor
            for anchor in source_rows
            if anchor.startswith(prefix)
            and _nearest_package(anchor.removeprefix(prefix), known_packages)
            == package
        }
        package_tests = {
            anchor
            for anchor, row in test_rows.items()
            if anchor.startswith(prefix)
            and _nearest_package(
                anchor.removeprefix(prefix).rsplit(":", 2)[0], known_packages
            )
            == package
        }
        if not package_sources and not package_tests:
            raise ValueError(f"module package {raw_package!r} has no checked inventory")
        sources.update(package_sources)
        tests.update(package_tests)
    return sorted(sources), sorted(tests)


def load_legacy_schema1_slices(root: Path) -> set[str]:
    """Returns the frozen pre-package manifest registry."""
    path = root / LEGACY_SCHEMA1_SLICES
    if not path.is_file():
        return set()
    names: set[str] = set()
    for (name,) in read_tsv(path, 1):
        if not OWNER_RE.fullmatch(name):
            raise ValueError(f"{path}: invalid legacy slice {name!r}")
        if name in names:
            raise ValueError(f"{path}: duplicate legacy slice {name}")
        names.add(name)
    return names


def load_claims(root: Path) -> list[dict[str, object]]:
    directory = root / CLAIMS_DIR
    if not directory.exists():
        return []
    claims = []
    for path in sorted(directory.glob("*.claim.json")):
        try:
            claim = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid claim {path}: {error}") from error
        claim["_path"] = path
        claims.append(claim)
    return claims


def claim_sources(claim: dict[str, object]) -> list[str]:
    """Returns the normalized source set for legacy and vertical-slice claims."""
    if claim.get("schema") == 1:
        sources = claim.get("sources")
        if isinstance(sources, list) and all(isinstance(item, str) for item in sources):
            return sorted(set(sources))
        source = claim.get("source")
        return [source] if isinstance(source, str) else []
    sources = claim.get("sources")
    if not isinstance(sources, list) or not all(isinstance(item, str) for item in sources):
        return []
    return sorted(set(sources))


def load_slices(root: Path) -> dict[str, dict[str, object]]:
    """Loads and validates checked dependency-ready vertical-slice records."""
    directory = root / SLICES_DIR
    if not directory.exists():
        return {}
    source_rows = {str(row["path"]): row for row in load_source_rows(root)}
    test_rows = {test_key(row): row for row in load_test_rows(root)}
    module_source_rows = load_module_source_rows(root)
    module_test_rows = load_module_test_rows(root)
    support_rows = load_support_rows(root)
    workspace_crates = load_workspace_crates(root)
    known_sources = set(source_rows)
    known_tests = set(test_rows)
    legacy_path = root / LEGACY_SCHEMA1_SLICES
    legacy_slices = load_legacy_schema1_slices(root)
    if not legacy_path.is_file():
        raise ValueError(
            f"{legacy_path}: missing frozen schema-1 slice registry"
        )
    records: dict[str, dict[str, object]] = {}
    required_strings = ("slice", "status", "consumer", "test_target")
    common_lists = ("depends_on", "rust_paths")
    for path in sorted(directory.glob("*.toml")):
        with path.open("rb") as source:
            record = tomllib.load(source)
        schema = record.get("schema")
        if schema not in {"1", "2"}:
            raise ValueError(f"{path}: expected slice schema 1 or 2")
        common_fields = {
            "schema",
            "slice",
            "status",
            "consumer",
            "test_target",
            "depends_on",
            "evidence_prerequisites",
            "support_anchors",
            "blocked_by",
            "rust_paths",
        }
        schema_fields = (
            {"target", "ring", "go_sources", "go_tests", "module_sources", "module_tests"}
            if schema == "1"
            else {
                "targets",
                "rings",
                "go_packages",
                "module_packages",
                "integration_paths",
            }
        )
        unknown_fields = sorted(set(record) - common_fields - schema_fields)
        if unknown_fields:
            raise ValueError(f"{path}: unknown schema-{schema} field {unknown_fields[0]}")
        for field in required_strings:
            if not isinstance(record.get(field), str) or not record[field]:
                raise ValueError(f"{path}: {field} must be a non-empty string")
        for field in common_lists:
            if not isinstance(record.get(field), list) or not all(
                isinstance(item, str) and item for item in record[field]
            ):
                raise ValueError(f"{path}: {field} must be a string array")
        record["rust_paths"] = sorted(
            {
                _normalized_repo_path(item, label="Rust path")
                for item in record["rust_paths"]
            }
        )
        internal_overlap = _first_path_overlap(
            [(rust_path, str(record.get("slice", path.stem))) for rust_path in record["rust_paths"]]
        )
        if internal_overlap is not None:
            first, _first_owner, second, _second_owner = internal_overlap
            raise ValueError(
                f"{path}: Rust paths overlap by ancestry: {first} and {second}"
            )
        name = str(record["slice"])
        if not OWNER_RE.fullmatch(name) or path.name != f"{name}.toml":
            raise ValueError(f"{path}: invalid slice name or filename")
        if name in records:
            raise ValueError(f"{path}: duplicate slice {name}")
        allowed_statuses = SLICE_STATUSES if schema == "1" else PACKAGE_SLICE_STATUSES
        if record["status"] not in allowed_statuses:
            raise ValueError(f"{path}: invalid status {record['status']!r}")
        if schema == "1":
            if name not in legacy_slices:
                raise ValueError(
                    f"{path}: schema-1 slice is not in frozen legacy registry "
                    f"{legacy_path}"
                )
            for field in ("go_sources", "go_tests"):
                value = record.get(field)
                if not isinstance(value, list) or not all(
                    isinstance(item, str) and item for item in value
                ):
                    raise ValueError(f"{path}: {field} must be a string array")
            for field in ("module_sources", "module_tests"):
                value = record.get(field, [])
                if not isinstance(value, list) or not all(
                    isinstance(item, str) and item for item in value
                ):
                    raise ValueError(f"{path}: {field} must be a string array")
                record[field] = sorted(set(value))
            sources = sorted(set(record["go_sources"]))
            tests = sorted({parse_test_anchor(item) for item in record["go_tests"]})
            module_sources = list(record["module_sources"])
            module_tests = sorted(
                {parse_module_test_anchor(item) for item in record["module_tests"]}
            )
            record["go_packages"] = []
            record["module_packages"] = []
            record["integration_paths"] = []
            if not isinstance(record.get("target"), str) or not record["target"]:
                raise ValueError(f"{path}: target must be a non-empty string")
            record["targets"] = [record["target"]]
            record["rust_targets"] = [record["target"]]
            record["source_targets"] = [record["target"]]
            if not isinstance(record.get("ring"), str) or not record["ring"]:
                raise ValueError(f"{path}: ring must be a non-empty string")
            record["rings"] = [record["ring"]]
        else:
            forbidden = sorted(
                field
                for field in ("go_sources", "go_tests", "module_sources", "module_tests")
                if field in record
            )
            if forbidden:
                raise ValueError(
                    f"{path}: schema-2 package slice cannot declare flattened "
                    f"anchors: {forbidden[0]}"
                )
            for field in (
                "targets",
                "rings",
            ):
                value = record.get(field, [])
                if not isinstance(value, list) or not all(
                    isinstance(item, str) and item for item in value
                ):
                    raise ValueError(f"{path}: {field} must be a string array")
                record[field] = sorted(set(value))
            for field, qualified in (
                ("go_packages", False),
                ("module_packages", True),
            ):
                value = record.get(field, [])
                if not isinstance(value, list) or not all(
                    isinstance(item, str) and item for item in value
                ):
                    raise ValueError(f"{path}: {field} must be a string array")
                normalized_packages = [
                    raw
                    if qualified
                    else _normalized_package(raw, qualified=False)[1]
                    for raw in value
                ]
                if qualified:
                    normalized_packages = [
                        f"{universe}::{package}"
                        for raw in value
                        for universe, package in [
                            _normalized_package(raw, qualified=True)
                        ]
                    ]
                record[field] = sorted(set(normalized_packages))
            if not record["targets"] or not all(
                OWNER_RE.fullmatch(item) for item in record["targets"]
            ):
                raise ValueError(
                    f"{path}: targets must contain valid Rust crate targets"
                )
            if not record["go_packages"] and not record["module_packages"]:
                raise ValueError(
                    f"{path}: schema-2 slice must own at least one Go or module package"
                )
            if record["module_packages"]:
                raise ValueError(
                    f"{path}: external package support inventory is unavailable; "
                    "schema-2 module_packages fail closed"
                )
            missing_targets = sorted(set(record["targets"]) - set(workspace_crates))
            if missing_targets:
                raise ValueError(
                    f"{path}: schema-2 target is not a Rust workspace crate: "
                    f"{missing_targets[0]}"
                )
            integration_paths = record.get("integration_paths", [])
            if not isinstance(integration_paths, list) or not all(
                isinstance(item, str) and item for item in integration_paths
            ):
                raise ValueError(f"{path}: integration_paths must be a string array")
            record["integration_paths"] = sorted(
                {
                    _normalized_repo_path(item, label="integration path")
                    for item in integration_paths
                }
            )
            for integration_path in record["integration_paths"]:
                candidate = Path(integration_path)
                if not candidate.is_relative_to(Path("rust")):
                    raise ValueError(
                        f"{path}: invalid schema-2 integration path "
                        f"{integration_path!r}"
                    )
                if not any(
                    candidate.is_relative_to(Path(workspace_crates[target]))
                    for target in record["targets"]
                ) and integration_path not in WORKSPACE_INTEGRATION_PATHS:
                    raise ValueError(
                        f"{path}: integration path {integration_path} is outside "
                        "the declared target workspace crates and shared workspace manifests"
                    )
                if not (root.parent / integration_path).is_file():
                    raise ValueError(
                        f"{path}: integration path is missing: {integration_path}"
                    )
            uncovered_targets = [
                target
                for target in record["targets"]
                if not any(
                    Path(rust_path).is_relative_to(Path(workspace_crates[target]))
                    for rust_path in record["rust_paths"]
                )
            ]
            if uncovered_targets:
                target = uncovered_targets[0]
                raise ValueError(
                    f"{path}: target {target} has no Rust path inside workspace crate "
                    f"{workspace_crates[target]}"
                )
            record["_target_crates"] = {
                target: workspace_crates[target] for target in record["targets"]
            }
            sources, tests, supports = expand_go_packages(
                list(record["go_packages"]), source_rows, test_rows, support_rows
            )
            module_sources, module_tests = expand_module_packages(
                list(record["module_packages"]), module_source_rows, module_test_rows
            )
            checked_targets = sorted(
                {str(source_rows[source]["target"]) for source in sources}
            )
            record["source_targets"] = checked_targets
            checked_rings = sorted({str(test_rows[test]["ring"]) for test in tests})
            expected_rings = checked_rings or ["unassigned"]
            if record["rings"] != expected_rings:
                raise ValueError(
                    f"{path}: schema-2 rings must exactly match the checked package "
                    f"obligation rings; expected {expected_rings}, found "
                    f"{record['rings']}"
                )
            record["rust_targets"] = list(record["targets"])
            record["target"] = ",".join(record["targets"])
            record["ring"] = ",".join(record["rings"])
            for rust_path in record["rust_paths"]:
                candidate = Path(rust_path)
                if (
                    candidate.is_absolute()
                    or candidate.as_posix() != rust_path
                    or any(part in {"", ".", ".."} for part in candidate.parts)
                    or not candidate.is_relative_to(Path("rust"))
                ):
                    raise ValueError(
                        f"{path}: invalid schema-2 Rust write path {rust_path!r}"
                    )
        if schema == "1":
            supports = []
        if not sources and not tests and not module_sources and not module_tests:
            raise ValueError(
                f"{path}: vertical slice must own checked Go or module inventory"
            )
        unknown_sources = sorted(set(sources) - known_sources)
        if unknown_sources:
            raise ValueError(f"{path}: unknown source anchor {unknown_sources[0]}")
        unknown_tests = sorted(set(tests) - known_tests)
        if unknown_tests:
            raise ValueError(f"{path}: unknown test anchor {unknown_tests[0]}")
        unknown_module_sources = sorted(set(module_sources) - set(module_source_rows))
        if unknown_module_sources:
            raise ValueError(
                f"{path}: unknown qualified module source anchor {unknown_module_sources[0]}"
            )
        unknown_module_tests = sorted(set(module_tests) - set(module_test_rows))
        if unknown_module_tests:
            raise ValueError(
                f"{path}: unknown qualified module test anchor {unknown_module_tests[0]}"
            )
        record["go_sources"] = sources
        record["go_tests"] = tests
        record["go_supports"] = supports
        record["module_sources"] = module_sources
        record["module_tests"] = module_tests
        prerequisites = record.get("evidence_prerequisites", [])
        if not isinstance(prerequisites, list):
            raise ValueError(f"{path}: evidence_prerequisites must be an array of tables")
        normalized_prerequisites = []
        for index, prerequisite in enumerate(prerequisites):
            location = f"{path}: evidence_prerequisites[{index}]"
            if not isinstance(prerequisite, dict):
                raise ValueError(f"{location} must be a table")
            expected_fields = {
                "capability",
                "evidence_owner",
                "kind",
                "anchor",
                "minimum_status",
            }
            unknown_fields = sorted(set(prerequisite) - expected_fields)
            if unknown_fields:
                raise ValueError(f"{location}: unknown field {unknown_fields[0]}")
            for field in expected_fields:
                if not isinstance(prerequisite.get(field), str) or not prerequisite[field]:
                    raise ValueError(f"{location}: {field} must be a non-empty string")
            capability = str(prerequisite["capability"])
            evidence_owner = str(prerequisite["evidence_owner"])
            kind = str(prerequisite["kind"])
            anchor = str(prerequisite["anchor"])
            minimum_status = str(prerequisite["minimum_status"])
            if not OWNER_RE.fullmatch(capability):
                raise ValueError(f"{location}: invalid capability {capability!r}")
            if not OWNER_RE.fullmatch(evidence_owner):
                raise ValueError(f"{location}: invalid evidence_owner {evidence_owner!r}")
            if kind == "source":
                row = source_rows.get(anchor)
            elif kind == "test":
                anchor = parse_test_anchor(anchor)
                row = test_rows.get(anchor)
            else:
                raise ValueError(f"{location}: kind must be 'source' or 'test'")
            if row is None:
                raise ValueError(f"{location}: unknown {kind} anchor {anchor}")
            if minimum_status not in EVIDENCE_MINIMUM_STATUSES:
                raise ValueError(
                    f"{location}: minimum_status must be PARTIAL or COVERED"
                )
            normalized_prerequisites.append(
                {
                    "capability": capability,
                    "evidence_owner": evidence_owner,
                    "kind": kind,
                    "anchor": anchor,
                    "minimum_status": minimum_status,
                    "current_status": str(row["status"]),
                    "current_owner": str(row["owner"]),
                }
            )
        record["evidence_prerequisites"] = normalized_prerequisites
        if schema == "2" and normalized_prerequisites:
            raise ValueError(
                f"{path}: schema-2 package slices cannot use legacy evidence_prerequisites"
            )
        record["_path"] = path
        records[name] = record

    if legacy_path.is_file():
        stale_legacy = sorted(legacy_slices - set(records))
        if stale_legacy:
            raise ValueError(
                f"{legacy_path}: frozen legacy slice has no manifest: {stale_legacy[0]}"
            )
        nonlegacy_schema = sorted(
            name for name in legacy_slices if records[name]["schema"] != "1"
        )
        if nonlegacy_schema:
            raise ValueError(
                f"{legacy_path}: schema-2 slice cannot be in legacy registry: "
                f"{nonlegacy_schema[0]}"
            )

    package_owners: dict[tuple[str, str], str] = {}
    schema2_rust_paths: list[tuple[str, str]] = []
    schema2_integration_paths: list[tuple[str, str]] = []
    for name, record in sorted(records.items()):
        if record["schema"] != "2":
            continue
        for kind, packages in (
            ("Go package", record["go_packages"]),
            ("module package", record["module_packages"]),
        ):
            for package in packages:
                key = (kind, package)
                previous = package_owners.get(key)
                if previous is not None:
                    raise ValueError(
                        f"{record['_path']}: {kind} {package} already has canonical "
                        f"schema-2 manifest {previous}"
                    )
                package_owners[key] = name
        schema2_rust_paths.extend(
            (rust_path, name) for rust_path in record["rust_paths"]
        )
        schema2_integration_paths.extend(
            (integration_path, name)
            for integration_path in record["integration_paths"]
        )
    for name, record in sorted(records.items()):
        if record["schema"] != "2" or record["status"] not in {
            "ready",
            "active",
            "covered",
        }:
            continue
        owned_packages = set(record["go_packages"])
        dependency_sources = set(record["go_sources"])
        dependency_sources.update(
            _test_source_path(anchor)
            for anchor in record["go_tests"]
            if _test_source_path(anchor).endswith(".go")
        )
        dependency_sources.update(
            support_rows[anchor]["path"]
            for anchor in record["go_supports"]
            if support_rows[anchor]["path"].endswith(".go")
        )
        dependencies = internal_go_dependencies(root, sorted(dependency_sources))
        for package in sorted(dependencies - owned_packages):
            dependency_owner = package_owners.get(("Go package", package))
            if dependency_owner is None:
                raise ValueError(
                    f"{record['_path']}: direct internal Go import {package} has no "
                    "canonical schema-2 package manifest"
                )
            if dependency_owner not in record["depends_on"]:
                raise ValueError(
                    f"{record['_path']}: direct internal Go import {package} requires "
                    f"depends_on = {dependency_owner}"
                )
    manifest_overlap = _first_path_overlap(schema2_rust_paths)
    if manifest_overlap is not None:
        first, first_owner, second, second_owner = manifest_overlap
        raise ValueError(
            f"schema-2 Rust ownership overlaps manifests {first_owner} ({first}) "
            f"and {second_owner} ({second})"
        )
    stable_seam_overlap = next(
        (
            (rust_path, rust_owner, integration_path, integration_owner)
            for rust_path, rust_owner in sorted(schema2_rust_paths)
            for integration_path, integration_owner in sorted(
                schema2_integration_paths
            )
            if _paths_overlap(rust_path, integration_path)
        ),
        None,
    )
    if stable_seam_overlap is not None:
        rust_path, rust_owner, integration_path, integration_owner = (
            stable_seam_overlap
        )
        raise ValueError(
            "schema-2 integration path overlaps stable Rust ownership: "
            f"{integration_owner} ({integration_path}) and "
            f"{rust_owner} ({rust_path})"
        )

    for name, record in records.items():
        for dependency in record["depends_on"]:
            if dependency == name:
                raise ValueError(f"{record['_path']}: slice cannot depend on itself")
            if dependency not in records:
                raise ValueError(f"{record['_path']}: unknown dependency {dependency}")
            if record["schema"] == "2" and records[dependency]["schema"] != "2":
                raise ValueError(
                    f"{record['_path']}: schema-2 package slice dependency "
                    f"{dependency} must also use schema 2"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise ValueError(f"{records[name]['_path']}: dependency cycle at {name}")
        if name in visited:
            return
        visiting.add(name)
        for dependency in records[name]["depends_on"]:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)

    for name in records:
        visit(name)
    for name, record in records.items():
        if record["schema"] == "2" and record["status"] == "covered":
            validate_package_receipt(root, name, record)
    return records


def load_campaigns(
    root: Path, slices: dict[str, dict[str, object]]
) -> dict[str, dict[str, object]]:
    """Loads checked package frontiers and proves their write sets."""
    directory = root / CAMPAIGNS_DIR
    if not directory.exists():
        return {}
    frozen_members: dict[str, set[str]] = {}
    frozen_path = root / INTEGRATED_CAMPAIGN_MEMBERS
    if frozen_path.exists():
        for line_number, raw_line in enumerate(
            frozen_path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not raw_line or raw_line.startswith("#"):
                continue
            fields = raw_line.split("\t")
            if len(fields) != 2 or not all(OWNER_RE.fullmatch(field) for field in fields):
                raise ValueError(
                    f"{frozen_path}:{line_number}: expected campaign<TAB>slice"
                )
            campaign, member = fields
            if member in frozen_members.setdefault(campaign, set()):
                raise ValueError(
                    f"{frozen_path}:{line_number}: duplicate frozen member {member}"
                )
            frozen_members[campaign].add(member)
    records: dict[str, dict[str, object]] = {}
    for path in sorted(directory.glob("*.toml")):
        with path.open("rb") as source:
            record = tomllib.load(source)
        campaign_schema = record.get("schema")
        if campaign_schema not in {"1", "2"}:
            raise ValueError(f"{path}: expected campaign schema 1 or 2")
        name = record.get("campaign")
        status = record.get("status")
        members = record.get("slices")
        if (
            not isinstance(name, str)
            or not OWNER_RE.fullmatch(name)
            or path.name != f"{name}.toml"
        ):
            raise ValueError(f"{path}: invalid campaign name or filename")
        if status not in CAMPAIGN_STATUSES:
            raise ValueError(f"{path}: invalid campaign status {status!r}")
        if not isinstance(members, list) or not all(
            isinstance(member, str) and member for member in members
        ):
            raise ValueError(f"{path}: slices must be a non-empty string array")
        if not members or len(set(members)) != len(members):
            raise ValueError(
                f"{path}: campaign slices must contain one or more unique members"
            )
        unknown = sorted(set(members) - set(slices))
        if unknown:
            raise ValueError(f"{path}: unknown campaign slice {unknown[0]}")
        member_schemas = {str(slices[member]["schema"]) for member in members}
        if member_schemas != {str(campaign_schema)}:
            raise ValueError(
                f"{path}: campaign schema must match every member slice schema"
            )
        if campaign_schema == "1" and status not in {"frozen", "integrated"}:
            raise ValueError(
                f"{path}: schema-1 campaigns may only be frozen or historical integrated"
            )
        if campaign_schema == "2" and status == "frozen":
            raise ValueError(
                f"{path}: schema-2 campaigns cannot use the legacy frozen state"
            )
        frozen = frozen_members.get(name)
        if status == "integrated":
            if frozen is None:
                raise ValueError(f"{path}: integrated campaign has no frozen member archive")
            if set(members) != frozen:
                raise ValueError(
                    f"{path}: integrated campaign membership differs from "
                    f"{frozen_path}"
                )
        elif frozen is not None:
            raise ValueError(
                f"{path}: only integrated campaigns may have frozen member archives"
            )
        if name in records:
            raise ValueError(f"{path}: duplicate campaign {name}")

        seen_sources: dict[str, str] = {}
        seen_tests: dict[str, str] = {}
        seen_supports: dict[str, str] = {}
        seen_module_sources: dict[str, str] = {}
        seen_module_tests: dict[str, str] = {}
        seen_rust_paths: dict[str, str] = {}
        campaign_rust_paths: list[tuple[str, str]] = []
        for member in members:
            slice_record = slices[member]
            for label, values, seen in (
                ("source", slice_record["go_sources"], seen_sources),
                ("test", slice_record["go_tests"], seen_tests),
                ("support", slice_record["go_supports"], seen_supports),
                ("module source", slice_record["module_sources"], seen_module_sources),
                ("module test", slice_record["module_tests"], seen_module_tests),
                ("Rust path", slice_record["rust_paths"], seen_rust_paths),
            ):
                for value in values:
                    if value in seen:
                        raise ValueError(
                            f"{path}: campaign {label} {value} overlaps slices "
                            f"{seen[value]} and {member}"
                        )
                    seen[value] = member
                    if label == "Rust path":
                        campaign_rust_paths.append((value, member))
        rust_overlap = _first_path_overlap(campaign_rust_paths)
        if rust_overlap is not None:
            first, first_owner, second, second_owner = rust_overlap
            raise ValueError(
                f"{path}: campaign Rust paths overlap slices {first_owner} ({first}) "
                f"and {second_owner} ({second})"
            )
        if status in {"planned", "active"}:
            unregistered_targets: dict[str, set[str]] = {}
            for member in members:
                slice_record = slices[member]
                for target in slice_record["rust_targets"]:
                    crate_root = slice_record.get("_target_crates", {}).get(target)
                    if crate_root is None:
                        continue
                    cargo_path = root.parent / str(crate_root) / "Cargo.toml"
                    if not cargo_path.is_file():
                        continue
                    with cargo_path.open("rb") as cargo_source:
                        cargo = tomllib.load(cargo_source)
                    if cargo.get("package", {}).get("autotests", True) is not False:
                        continue
                    registered = {
                        str(test["name"])
                        for test in cargo.get("test", [])
                        if isinstance(test, dict) and isinstance(test.get("name"), str)
                    }
                    test_target = str(slice_record["test_target"])
                    if test_target not in registered:
                        unregistered_targets.setdefault(str(target), set()).add(test_target)
            for target, test_targets in sorted(unregistered_targets.items()):
                cargo_rust_path = f"rust/crates/{target}/Cargo.toml"
                if cargo_rust_path not in seen_rust_paths:
                    raise ValueError(
                        f"{path}: campaign adds unregistered {target} test targets "
                        f"{sorted(test_targets)} while autotests is disabled; exactly one "
                        f"member must own {cargo_rust_path}"
                    )
        # The batch floor is an admission rule, not a mutable property of a
        # historical receipt. Later ownership consolidation can move exact
        # anchors out of an integrated member without changing what its gate
        # validated; rewriting campaign membership to satisfy today's count
        # would instead falsify that history.
        if status in {"planned", "active"}:
            combined_source_count = len(seen_sources) + len(seen_module_sources)
            combined_test_count = len(seen_tests) + len(seen_module_tests)
            # The batch floor is advisory. It was a disjunction in the design
            # (nine production files OR fifty obligations) and an AND in this
            # code, and the stricter reading rewarded padding a member with
            # anchors it could not discharge purely to clear the count — that is
            # how one bounded signed-BIGINT row writer came to claim the whole
            # tablecodec/rowcodec test inventory. A batch being small is not a
            # correctness problem, so it warns instead of blocking every gate.
            if (
                combined_source_count < CAMPAIGN_MIN_SOURCE_COUNT
                and combined_test_count < CAMPAIGN_MIN_TEST_COUNT
            ):
                print(
                    f"note: {path}: small batch — {combined_source_count} production "
                    f"sources and {combined_test_count} original obligations",
                    file=sys.stderr,
                )
        record["_path"] = path
        record["source_count"] = len(seen_sources) + len(seen_module_sources)
        record["test_count"] = len(seen_tests) + len(seen_module_tests)
        records[name] = record
    stale_frozen = sorted(set(frozen_members) - set(records))
    if stale_frozen:
        raise ValueError(
            f"{frozen_path}: frozen members reference unknown campaign {stale_frozen[0]}"
        )
    return records


def unmet_slice_prerequisites(
    record: dict[str, object], records: dict[str, dict[str, object]]
) -> list[str]:
    unmet = [
        f"slice:{dependency}>=covered (current {records[dependency]['status']})"
        for dependency in record["depends_on"]
        if records[dependency]["status"] != "covered"
    ]
    for prerequisite in record["evidence_prerequisites"]:
        current = prerequisite["current_status"]
        current_owner = prerequisite["current_owner"]
        expected_owner = prerequisite["evidence_owner"]
        minimum = prerequisite["minimum_status"]
        if (
            current_owner != expected_owner
            or EVIDENCE_STATUS_RANK.get(current, -1) < EVIDENCE_STATUS_RANK[minimum]
        ):
            unmet.append(
                f"{prerequisite['capability']}:{prerequisite['kind']}:"
                f"{prerequisite['anchor']}@{expected_owner}>={minimum} "
                f"(current {current_owner}@{current})"
            )
    return unmet


def slice_is_ready(record: dict[str, object], records: dict[str, dict[str, object]]) -> bool:
    return record["status"] == "ready" and not unmet_slice_prerequisites(record, records)


def validate_transfers(root: Path) -> None:
    directory = root / TRANSFERS_DIR
    if not directory.exists():
        return
    source_rows = {str(row["path"]): row for row in load_source_rows(root)}
    test_rows = {test_key(row): row for row in load_test_rows(root)}
    module_source_rows = load_module_source_rows(root)
    module_test_rows = load_module_test_rows(root)
    transfers: list[dict[str, object]] = []
    chains: dict[tuple[str, str], list[dict[str, object]]] = {}
    all_retired_artifacts: set[str] = set()
    for path in sorted(directory.glob("*.tsv")):
        for row in read_tsv(path, 9):
            (
                old_owner,
                new_owner,
                source_path,
                test_path,
                test_line,
                test_name,
                retired_artifact_field,
                new_artifact_field,
                _note,
            ) = row
            if not OWNER_RE.fullmatch(old_owner) or not OWNER_RE.fullmatch(new_owner):
                raise ValueError(f"{path}: invalid transfer owner")
            if old_owner == new_owner:
                raise ValueError(f"{path}: transfer owners must differ")
            test_fields = (test_path, test_line, test_name)
            has_source = source_path != "-"
            has_test = test_fields != ("-", "-", "-")
            if not has_source and not has_test:
                raise ValueError(f"{path}: transfer must contain a source or test anchor")
            if has_test and "-" in test_fields:
                raise ValueError(
                    f"{path}: test transfer must provide path, line, and name or use '-\t-\t-'"
                )
            if has_test and not test_line.isdigit():
                raise ValueError(f"{path}: invalid test line {test_line!r}")
            test_anchor = "-" if not has_test else f"{test_path}:{int(test_line)}:{test_name}"
            record = {
                "path": path,
                "old_owner": old_owner,
                "new_owner": new_owner,
                "retired_artifacts": []
                if retired_artifact_field == "-"
                else retired_artifact_field.split(","),
                "new_artifacts": []
                if new_artifact_field == "-"
                else new_artifact_field.split(","),
            }
            transfers.append(record)
            retired_artifacts_set = set(record["retired_artifacts"])
            all_retired_artifacts.update(retired_artifacts_set)
            if has_source:
                chains.setdefault(("source", source_path), []).append(record)
            if has_test:
                chains.setdefault(("test", test_anchor), []).append(record)

    for (kind, anchor), records in chains.items():
        by_old_owner: dict[str, dict[str, object]] = {}
        incoming: dict[str, dict[str, object]] = {}
        for record in records:
            old_owner = str(record["old_owner"])
            new_owner = str(record["new_owner"])
            if old_owner in by_old_owner:
                raise ValueError(
                    f"{record['path']}: branched {kind} ownership transfer for "
                    f"{anchor} from {old_owner}"
                )
            if new_owner in incoming:
                raise ValueError(
                    f"{record['path']}: merged {kind} ownership transfer for "
                    f"{anchor} into {new_owner}"
                )
            by_old_owner[old_owner] = record
            incoming[new_owner] = record
        starts = [owner for owner in by_old_owner if owner not in incoming]
        if len(starts) != 1:
            raise ValueError(
                f"{records[0]['path']}: {kind} ownership transfers for {anchor} "
                "must form one acyclic chain"
            )
        owner = starts[0]
        visited = 0
        while owner in by_old_owner:
            record = by_old_owner[owner]
            owner = str(record["new_owner"])
            visited += 1
        if visited != len(records):
            raise ValueError(
                f"{records[0]['path']}: {kind} ownership transfers for {anchor} "
                "must form one connected chain"
            )
        is_module_anchor = "::" in anchor
        if kind == "source":
            ledger_row = (
                module_source_rows.get(anchor)
                if is_module_anchor
                else source_rows.get(anchor)
            )
        else:
            ledger_row = (
                module_test_rows.get(anchor)
                if is_module_anchor
                else test_rows.get(anchor)
            )
        display_kind = f"module {kind}" if is_module_anchor else kind
        if ledger_row is None:
            if is_module_anchor:
                raise ValueError(
                    f"{records[-1]['path']}: transferred {display_kind} {anchor} "
                    f"is not present in the external {kind} ledger"
                )
        if ledger_row is None or ledger_row["owner"] != owner:
            raise ValueError(
                f"{records[-1]['path']}: transferred {display_kind} {anchor} is not owned by "
                f"terminal owner {owner}"
            )

    checked_records: set[int] = set()
    for record in transfers:
        identity = id(record)
        if identity in checked_records:
            continue
        checked_records.add(identity)
        path = record["path"]
        for artifact in record["retired_artifacts"]:
            if (root.parent / artifact).exists():
                raise ValueError(f"{path}: retired artifact still exists: {artifact}")
        for artifact in record["new_artifacts"]:
            if artifact not in all_retired_artifacts and not (root.parent / artifact).is_file():
                raise ValueError(f"{path}: replacement artifact is missing: {artifact}")


def validate_claims(
    root: Path, slices: dict[str, dict[str, object]] | None = None
) -> list[dict[str, object]]:
    validate_transfers(root)
    if slices is None:
        slices = load_slices(root)
    load_campaigns(root, slices)
    sources = {str(row["path"]) for row in load_source_rows(root)}
    tests = {test_key(row) for row in load_test_rows(root)}
    supports = load_support_rows(root)
    seen_sources: dict[str, str] = {}
    seen_tests: dict[str, str] = {}
    seen_supports: dict[str, str] = {}
    seen_module_sources: dict[str, str] = {}
    seen_module_tests: dict[str, str] = {}
    seen_rust_paths: dict[str, str] = {}
    active_rust_paths: list[tuple[str, str]] = []
    claims = load_claims(root)
    for claim in claims:
        path = Path(claim["_path"])
        owner = claim.get("owner")
        schema = claim.get("schema")
        claimed_sources = claim_sources(claim)
        claimed_tests = claim.get("tests")
        claimed_supports = claim.get("supports", [])
        claimed_rust_paths = claim.get("rust_paths", [])
        claimed_integration_paths = claim.get("integration_paths")
        claimed_module_sources = claim.get("module_sources", [])
        claimed_module_tests = claim.get("module_tests", [])
        if schema not in {1, 2} or not isinstance(owner, str):
            raise ValueError(f"{path}: expected schema 1 or 2 and string owner")
        if not OWNER_RE.fullmatch(owner) or path.name != f"{owner}.claim.json":
            raise ValueError(f"{path}: invalid owner or filename")
        for source in claimed_sources:
            if source not in sources:
                raise ValueError(f"{path}: stale source anchor {source!r}")
        if not isinstance(claimed_tests, list) or not all(
            isinstance(item, str) for item in claimed_tests
        ):
            raise ValueError(f"{path}: tests must be a string array")
        if not isinstance(claimed_supports, list) or not all(
            isinstance(item, str) for item in claimed_supports
        ):
            raise ValueError(f"{path}: supports must be a string array")
        claimed_supports = sorted(set(claimed_supports))
        if not isinstance(claimed_rust_paths, list) or not all(
            isinstance(item, str) for item in claimed_rust_paths
        ):
            raise ValueError(f"{path}: rust_paths must be a string array")
        claimed_rust_paths = sorted(set(claimed_rust_paths))
        if schema == 2 and (
            not isinstance(claimed_integration_paths, list)
            or not all(isinstance(item, str) for item in claimed_integration_paths)
        ):
            raise ValueError(f"{path}: integration_paths must be a string array")
        claimed_integration_paths = (
            sorted(set(claimed_integration_paths)) if schema == 2 else []
        )
        if not isinstance(claimed_module_sources, list) or not all(
            isinstance(item, str) for item in claimed_module_sources
        ):
            raise ValueError(f"{path}: module_sources must be a string array")
        if not isinstance(claimed_module_tests, list) or not all(
            isinstance(item, str) for item in claimed_module_tests
        ):
            raise ValueError(f"{path}: module_tests must be a string array")
        claimed_module_sources = sorted(set(claimed_module_sources))
        claimed_module_tests = sorted({parse_module_test_anchor(item) for item in claimed_module_tests})
        unknown_module_sources = sorted(set(claimed_module_sources) - load_module_source_anchors(root))
        if unknown_module_sources:
            raise ValueError(f"{path}: stale qualified module source anchor {unknown_module_sources[0]!r}")
        unknown_module_tests = sorted(set(claimed_module_tests) - load_module_test_anchors(root))
        if unknown_module_tests:
            raise ValueError(f"{path}: stale qualified module test anchor {unknown_module_tests[0]!r}")
        if (
            not claimed_sources
            and not claimed_tests
            and not claimed_supports
            and not claimed_module_sources
            and not claimed_module_tests
        ):
            raise ValueError(f"{path}: claim must own checked Go or module inventory")
        slice_record = slices.get(owner)
        if schema == 2 and (
            slice_record is None or slice_record["schema"] != "2"
        ):
            raise ValueError(
                f"{path}: schema-2 claim requires a matching schema-2 package slice"
            )
        if schema == 2:
            assert slice_record is not None
            expected_sources = list(slice_record["go_sources"])
            expected_tests = list(slice_record["go_tests"])
            expected_supports = list(slice_record["go_supports"])
            expected_rust_paths = list(slice_record["rust_paths"])
            expected_integration_paths = list(slice_record["integration_paths"])
            expected_module_sources = list(slice_record["module_sources"])
            expected_module_tests = list(slice_record["module_tests"])
            if (
                claimed_sources != expected_sources
                or sorted(claimed_tests) != expected_tests
                or claimed_supports != expected_supports
                or claimed_rust_paths != expected_rust_paths
                or claimed_integration_paths != expected_integration_paths
                or claimed_module_sources != expected_module_sources
                or claimed_module_tests != expected_module_tests
            ):
                differences = []
                for label, values in (
                    ("missing sources", sorted(set(expected_sources) - set(claimed_sources))),
                    ("extra sources", sorted(set(claimed_sources) - set(expected_sources))),
                    ("missing tests", sorted(set(expected_tests) - set(claimed_tests))),
                    ("extra tests", sorted(set(claimed_tests) - set(expected_tests))),
                    ("missing supports", sorted(set(expected_supports) - set(claimed_supports))),
                    ("extra supports", sorted(set(claimed_supports) - set(expected_supports))),
                    ("missing Rust paths", sorted(set(expected_rust_paths) - set(claimed_rust_paths))),
                    ("extra Rust paths", sorted(set(claimed_rust_paths) - set(expected_rust_paths))),
                    ("missing integration paths", sorted(set(expected_integration_paths) - set(claimed_integration_paths))),
                    ("extra integration paths", sorted(set(claimed_integration_paths) - set(expected_integration_paths))),
                    ("missing module sources", sorted(set(expected_module_sources) - set(claimed_module_sources))),
                    ("extra module sources", sorted(set(claimed_module_sources) - set(expected_module_sources))),
                    ("missing module tests", sorted(set(expected_module_tests) - set(claimed_module_tests))),
                    ("extra module tests", sorted(set(claimed_module_tests) - set(expected_module_tests))),
                ):
                    if values:
                        differences.append(f"{label}: {','.join(values)}")
                raise ValueError(
                    f"{path}: schema-2 claim for slice {owner} must exactly match "
                    f"its go_sources and go_tests; {'; '.join(differences)}"
                )
            upstream_sha256 = claim.get("upstream_sha256")
            if not isinstance(upstream_sha256, dict) or not all(
                isinstance(key, str)
                and isinstance(value, str)
                and re.fullmatch(r"[0-9a-f]{64}", value)
                for key, value in upstream_sha256.items()
            ):
                raise ValueError(
                    f"{path}: schema-2 claim requires a path-to-sha256 upstream snapshot"
                )
            expected_upstream_sha256 = upstream_snapshot(
                root, claimed_sources, list(claimed_tests), claimed_supports
            )
            if upstream_sha256 != expected_upstream_sha256:
                changed = sorted(
                    key
                    for key in set(upstream_sha256) | set(expected_upstream_sha256)
                    if upstream_sha256.get(key) != expected_upstream_sha256.get(key)
                )
                raise ValueError(
                    f"{path}: upstream package snapshot is stale at {changed[0]}"
                )
            claim["_upstream_sha256"] = expected_upstream_sha256
            base_commit = claim.get("base_commit")
            if not isinstance(base_commit, str) or not re.fullmatch(
                r"[0-9a-f]{40,64}", base_commit
            ):
                raise ValueError(
                    f"{path}: schema-2 claim requires an exact Git base_commit"
                )
            claim["_base_commit"] = base_commit
            claim["_rust_paths"] = claimed_rust_paths
            claim["_integration_paths"] = claimed_integration_paths
            for rust_path in claim["_rust_paths"]:
                if rust_path in seen_rust_paths:
                    raise ValueError(
                        f"Rust path {rust_path} overlaps slice claims "
                        f"{seen_rust_paths[rust_path]} and {owner}"
                    )
                seen_rust_paths[rust_path] = owner
                active_rust_paths.append((rust_path, owner))
        else:
            claim["_rust_paths"] = []
            claim["_integration_paths"] = []
            claim["_upstream_sha256"] = {}
            claim["_base_commit"] = ""
        claim["_sources"] = claimed_sources
        claim["_supports"] = claimed_supports
        claim["_module_sources"] = claimed_module_sources
        claim["_module_tests"] = claimed_module_tests
        for source in claimed_sources:
            if source in seen_sources:
                raise ValueError(
                    f"source {source} overlaps claims {seen_sources[source]} and {owner}"
                )
            seen_sources[source] = owner
        for item in claimed_tests:
            if item not in tests:
                raise ValueError(f"{path}: stale test anchor {item!r}")
            if item in seen_tests:
                raise ValueError(
                    f"test {item} overlaps claims {seen_tests[item]} and {owner}"
                )
            seen_tests[item] = owner
        for item in claimed_supports:
            if item not in supports:
                raise ValueError(f"{path}: stale support anchor {item!r}")
            if item in seen_supports:
                raise ValueError(
                    f"support {item} overlaps claims {seen_supports[item]} and {owner}"
                )
            seen_supports[item] = owner
        for item in claimed_module_sources:
            if item in seen_module_sources:
                raise ValueError(
                    f"module source {item} overlaps claims {seen_module_sources[item]} and {owner}"
                )
            seen_module_sources[item] = owner
        for item in claimed_module_tests:
            if item in seen_module_tests:
                raise ValueError(
                    f"module test {item} overlaps claims {seen_module_tests[item]} and {owner}"
                )
            seen_module_tests[item] = owner
    rust_overlap = _first_path_overlap(active_rust_paths)
    if rust_overlap is not None:
        first, first_owner, second, second_owner = rust_overlap
        raise ValueError(
            f"Rust path ownership overlaps claims {first_owner} ({first}) and "
            f"{second_owner} ({second})"
        )
    validate_claimed_rust_changes(root, claims)
    return claims


@contextlib.contextmanager
def claim_lock(root: Path) -> Iterator[None]:
    directory = root / CLAIMS_DIR
    directory.mkdir(parents=True, exist_ok=True)
    lock = directory / ".lock"
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as error:
        os.close(descriptor)
        raise ValueError(f"claim transaction already active: {lock}") from error
    try:
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"{os.getpid()}\n".encode())
        yield
    finally:
        os.ftruncate(descriptor, 0)
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_bytes(path: Path, contents: bytes) -> None:
    """Replaces one transaction file without exposing partial contents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(contents)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _attested_ledger_rows(
    root: Path, anchors: list[str], rows: dict[str, dict[str, object]]
) -> list[dict[str, str]]:
    attested = []
    for anchor in anchors:
        row = rows[anchor]
        artifact = str(row["artifact"])
        artifact_path = root.parent / artifact
        if artifact == "-" or not artifact_path.is_file():
            raise ValueError(f"package evidence artifact is missing: {artifact}")
        if "upstream_sha256" in row:
            upstream_sha256 = str(row["upstream_sha256"])
        else:
            source_path = anchor.rsplit(":", 2)[0] if ":" in anchor else anchor
            upstream = root.parent / source_path
            if not upstream.is_file():
                raise ValueError(f"upstream package source is missing: {source_path}")
            upstream_sha256 = file_digest(upstream)
        attested.append(
            {
                "anchor": anchor,
                "upstream_sha256": upstream_sha256,
                "owner": str(row["owner"]),
                "status": str(row["status"]),
                "evidence_artifact": artifact,
                "evidence_sha256": file_digest(artifact_path),
            }
        )
    return attested


def package_completion_snapshot(
    root: Path, owner: str, record: dict[str, object]
) -> dict[str, object]:
    """Returns the complete content-addressed package result for its receipt."""
    source_rows = {str(row["path"]): row for row in load_source_rows(root)}
    test_rows = {test_key(row): row for row in load_test_rows(root)}
    module_source_rows: dict[str, dict[str, object]] = dict(
        load_module_source_rows(root)
    )
    module_test_rows: dict[str, dict[str, object]] = dict(load_module_test_rows(root))
    rust_paths = []
    for relative in record["rust_paths"]:
        path = root.parent / str(relative)
        if not path.is_file():
            raise ValueError(f"package {owner} Rust path is missing: {relative}")
        rust_paths.append(
            {"path": str(relative), "sha256": file_digest(path)}
        )
    manifest_path = Path(record["_path"])
    snapshot = {
        "manifest": {
            "path": manifest_path.relative_to(root.parent).as_posix(),
            "sha256": file_digest(manifest_path),
        },
        "go_packages": list(record["go_packages"]),
        "module_packages": list(record["module_packages"]),
        "sources": _attested_ledger_rows(
            root, list(record["go_sources"]), source_rows
        ),
        "tests": _attested_ledger_rows(root, list(record["go_tests"]), test_rows),
        "module_sources": _attested_ledger_rows(
            root, list(record["module_sources"]), module_source_rows
        ),
        "module_tests": _attested_ledger_rows(
            root, list(record["module_tests"]), module_test_rows
        ),
        "supports": load_package_support_evidence(
            root, owner, list(record["go_supports"])
        ),
        "rust_targets": list(record["rust_targets"]),
        "rust_paths": rust_paths,
        # Shared seams are frozen by path and by the gate attestation, but are
        # intentionally not content-addressed by a leaf package receipt. A
        # later steward edit to the same seam must not stale completed leaves.
        "integration_paths": list(record["integration_paths"]),
    }
    inventory = {key: value for key, value in snapshot.items() if key != "manifest"}
    snapshot["inventory_sha256"] = hashlib.sha256(
        json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return snapshot


def package_receipt_payload(
    root: Path,
    owner: str,
    close_id: str,
    record: dict[str, object],
    gate_receipt: dict[str, object],
    *,
    close_kind: str = "campaign",
    gate_command: list[str] | None = None,
) -> dict[str, object]:
    claim = gate_receipt.get("claims", {}).get(owner)
    if not isinstance(claim, dict) or not isinstance(claim.get("claim_sha256"), str):
        raise ValueError(f"gate receipt is missing exact package claim {owner}")
    if close_kind not in {"package", "campaign"}:
        raise ValueError(f"invalid package receipt close kind {close_kind}")
    if close_kind == "package" and close_id != owner:
        raise ValueError("direct package receipt id must equal its owner")
    gate_command = gate_command or ["scripts/rewrite-gate.sh", "integrate"]
    if gate_command[:2] not in (
        ["scripts/rewrite-gate.sh", "integrate"],
        ["scripts/rewrite-gate.sh", "package"],
    ):
        raise ValueError("invalid package receipt gate command")
    if gate_command[:2] == ["scripts/rewrite-gate.sh", "package"]:
        if gate_command != package_gate_command(record):
            raise ValueError("package receipt gate does not match touched Rust crates")
    return {
        "schema": 2,
        "owner": owner,
        "close": {"kind": close_kind, "id": close_id},
        "package": package_completion_snapshot(root, owner, record),
        "gate": {
            "command": gate_command,
            "result": "passed",
            "claim_sha256": claim["claim_sha256"],
            "workspace_sha256": gate_receipt.get("workspace_sha256"),
            "slice_manifests_sha256": gate_receipt.get("slice_manifests_sha256"),
        },
    }


def validate_package_receipt(
    root: Path, owner: str, record: dict[str, object]
) -> dict[str, object]:
    """Validates the immutable durable receipt required by covered packages."""
    path = root / PACKAGE_RECEIPTS_DIR / f"{owner}.json"
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid package receipt {path}: {error}") from error
    schema = receipt.get("schema")
    identity_valid = receipt.get("owner") == owner
    if schema == 1:
        campaign = receipt.get("campaign")
        identity_valid = (
            identity_valid
            and isinstance(campaign, str)
            and OWNER_RE.fullmatch(campaign) is not None
        )
    elif schema == 2:
        close = receipt.get("close")
        identity_valid = identity_valid and isinstance(close, dict)
        if isinstance(close, dict):
            close_kind = close.get("kind")
            close_id = close.get("id")
            identity_valid = (
                identity_valid
                and close_kind in {"package", "campaign"}
                and isinstance(close_id, str)
                and OWNER_RE.fullmatch(close_id) is not None
                and (close_kind != "package" or close_id == owner)
            )
    else:
        identity_valid = False
    if not identity_valid:
        raise ValueError(f"{path}: invalid package receipt identity")
    expected = package_completion_snapshot(root, owner, record)
    if receipt.get("package") != expected:
        raise ValueError(f"{path}: package inventory or artifact digest is stale")
    gate = receipt.get("gate")
    gate_command = gate.get("command") if isinstance(gate, dict) else None
    valid_gate_command = gate_command == ["scripts/rewrite-gate.sh", "integrate"]
    if (
        isinstance(gate_command, list)
        and len(gate_command) >= 3
        and gate_command[:2] == ["scripts/rewrite-gate.sh", "package"]
    ):
        valid_gate_command = gate_command == package_gate_command(record)
    if (
        not isinstance(gate, dict)
        or not valid_gate_command
        or gate.get("result") != "passed"
        or not all(
            isinstance(gate.get(field), str) and gate[field]
            for field in (
                "claim_sha256",
                "workspace_sha256",
                "slice_manifests_sha256",
            )
        )
    ):
        raise ValueError(f"{path}: incomplete package gate attestation")
    return receipt


def package_gate_targets(record: dict[str, object]) -> list[str]:
    """Returns Cargo packages containing the exact Rust files in a package."""
    targets = {
        Path(str(path)).parts[2]
        for path in record["rust_paths"]
        if len(Path(str(path)).parts) >= 4
        and Path(str(path)).parts[:2] == ("rust", "crates")
    }
    # Schema-2 test fixtures and generator-only packages predate crate-path
    # derivation. Their declared targets remain the canonical fallback.
    if not targets:
        targets = {str(target) for target in record["rust_targets"]}
    if not targets or any(
        re.fullmatch(r"[A-Za-z0-9_-]+", target) is None for target in targets
    ):
        raise ValueError("package has no valid Cargo target to validate")
    return sorted(targets)


def package_gate_command(record: dict[str, object]) -> list[str]:
    """Builds the deterministic touched-crate and touched-test gate command."""
    packages = package_gate_targets(record)
    tests = sorted(
        {
            f"{parts[2]}:{Path(parts[4]).stem}"
            for path in record["rust_paths"]
            for parts in [Path(str(path)).parts]
            if len(parts) == 5
            and parts[:2] == ("rust", "crates")
            and parts[3] == "tests"
            and parts[4].endswith(".rs")
        }
    )
    command = ["scripts/rewrite-gate.sh", "package", *packages]
    if tests:
        command.extend(["--", *tests])
    return command


def _tracked_entries(repo_root: Path) -> dict[str, tuple[str, str]]:
    """Reads the stage-zero Git index as path -> (mode, object id)."""
    result = subprocess.run(
        ["git", "ls-files", "--stage", "-z"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        message = result.stderr.decode(errors="replace").strip()
        raise ValueError(f"cannot inspect tracked upstream artifacts: {message}")
    entries: dict[str, tuple[str, str]] = {}
    for raw_record in result.stdout.split(b"\0"):
        if not raw_record:
            continue
        header, separator, raw_path = raw_record.partition(b"\t")
        fields = header.split()
        if not separator or len(fields) != 3:
            raise ValueError("git ls-files returned an invalid stage record")
        mode, object_id, stage = (field.decode("ascii") for field in fields)
        if stage != "0":
            raise ValueError(
                "cannot snapshot upstream artifacts with unresolved Git index stages"
            )
        path = raw_path.decode("utf-8", errors="surrogateescape")
        entries[path] = (mode, object_id)
    return entries


def git_head(repo_root: Path) -> str:
    """Returns the exact commit from which a package claim starts."""
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD^{commit}"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    commit = result.stdout.strip()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40,64}", commit):
        message = result.stderr.strip() or "repository has no valid HEAD commit"
        raise ValueError(f"cannot freeze package claim base commit: {message}")
    return commit


def _changed_paths_since(repo_root: Path, base_commit: str) -> list[str]:
    """Returns canonical committed paths whose tree differs from the claim base."""
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", base_commit, "HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if ancestor.returncode != 0:
        raise ValueError(
            f"package claim base commit {base_commit} is not an ancestor of HEAD"
        )
    result = subprocess.run(
        ["git", "diff", "--name-only", "-z", f"{base_commit}..HEAD", "--"],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        message = result.stderr.decode(errors="replace").strip()
        raise ValueError(
            f"cannot inspect committed changes since {base_commit}: {message}"
        )
    return sorted(
        _normalized_repo_path(
            raw.decode("utf-8", errors="surrogateescape"),
            label="committed path",
        )
        for raw in result.stdout.split(b"\0")
        if raw
    )


def _requires_package_rust_ownership(relative: str) -> bool:
    path = Path(relative)
    return path.is_relative_to(Path("rust/crates")) or (
        path.is_relative_to(Path("rust"))
        and (path.name == "Cargo.toml" or relative == "rust/Cargo.lock")
    )


def validate_claimed_rust_changes(
    root: Path, claims: list[dict[str, object]]
) -> None:
    """Rejects committed Rust implementation changes outside active claims."""
    package_claims = [claim for claim in claims if claim["schema"] == 2]
    if not package_claims:
        return
    bases = {str(claim["_base_commit"]) for claim in package_claims}
    if len(bases) != 1:
        owners = ", ".join(
            f"{claim['owner']}={claim['_base_commit']}"
            for claim in sorted(package_claims, key=lambda item: str(item["owner"]))
        )
        raise ValueError(
            "active schema-2 package claims must share one base_commit before "
            f"integration: {owners}"
        )
    base_commit = next(iter(bases))
    owned_paths = sorted(
        {
            path
            for claim in package_claims
            for path in [*claim["_rust_paths"], *claim["_integration_paths"]]
        }
    )
    for changed_path in _changed_paths_since(root.parent, base_commit):
        if not _requires_package_rust_ownership(changed_path):
            continue
        if not any(
            Path(changed_path).is_relative_to(Path(owned_path))
            for owned_path in owned_paths
        ):
            raise ValueError(
                "committed Rust change is outside active package claims: "
                f"{changed_path} (base_commit {base_commit})"
            )


def uncommitted_rust_paths(repo_root: Path) -> list[str]:
    """Returns staged, unstaged, or untracked Rust implementation paths."""
    pathspecs = ("rust/crates", "rust/Cargo.toml", "rust/Cargo.lock")
    commands = (
        ["git", "diff", "--name-only", "-z", "--", *pathspecs],
        ["git", "diff", "--cached", "--name-only", "-z", "--", *pathspecs],
        [
            "git",
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            *pathspecs,
        ],
    )
    paths: set[str] = set()
    for command in commands:
        result = subprocess.run(
            command,
            cwd=repo_root,
            check=False,
            capture_output=True,
        )
        if result.returncode != 0:
            message = result.stderr.decode(errors="replace").strip()
            raise ValueError(f"cannot inspect uncommitted Rust changes: {message}")
        paths.update(
            _normalized_repo_path(
                raw.decode("utf-8", errors="surrogateescape"),
                label="uncommitted Rust path",
            )
            for raw in result.stdout.split(b"\0")
            if raw
        )
    return sorted(paths)


def tracked_path_digest(
    repo_root: Path,
    relative_path: str,
    tracked: dict[str, tuple[str, str]] | None = None,
) -> str:
    """Hashes the actual bytes or symlink target of one tracked artifact."""
    relative_path = _normalized_repo_path(relative_path, label="upstream path")
    entries = tracked if tracked is not None else _tracked_entries(repo_root)
    entry = entries.get(relative_path)
    if entry is None:
        raise ValueError(f"upstream artifact is not tracked by Git: {relative_path}")
    mode, object_id = entry
    path = repo_root / relative_path
    if mode == "160000":
        if not path.exists():
            raise ValueError(f"tracked Git submodule is missing: {relative_path}")
        return hashlib.sha256(f"gitlink\0{object_id}".encode()).hexdigest()
    if mode == "120000":
        if not path.is_symlink():
            raise ValueError(f"tracked symlink is missing or replaced: {relative_path}")
        target = os.readlink(path)
        return hashlib.sha256(os.fsencode(target)).hexdigest()
    if not mode.startswith("100"):
        raise ValueError(
            f"unsupported tracked artifact mode {mode} for {relative_path}"
        )
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"tracked file is missing or replaced: {relative_path}")
    return file_digest(path)


def _test_source_path(anchor: str) -> str:
    source_and_line, separator, _name = anchor.rpartition(":")
    source_path, line_separator, line = source_and_line.rpartition(":")
    if not separator or not line_separator or not line.isdigit() or not source_path:
        raise ValueError(f"invalid test anchor {anchor!r}; use PATH:LINE:NAME")
    return source_path


def upstream_snapshot(
    root: Path,
    sources: list[str],
    tests: list[str],
    supports: list[str],
) -> dict[str, str]:
    """Content-addresses every unique upstream file owned by a package claim."""
    support_rows = load_support_rows(root)
    paths = set(sources)
    paths.update(_test_source_path(anchor) for anchor in tests)
    for anchor in supports:
        row = support_rows.get(anchor)
        if row is None:
            raise ValueError(f"stale support anchor {anchor!r}")
        paths.add(row["path"])
    tracked = _tracked_entries(root.parent)
    return {
        path: tracked_path_digest(root.parent, path, tracked)
        for path in sorted(paths)
    }


def _workspace_files(
    root: Path,
    *,
    excluded_prefixes: tuple[str, ...],
    excluded_paths: set[str],
) -> list[tuple[str, Path]]:
    """Lists digest inputs without descending into excluded build trees.

    ``Path.rglob`` still traverses every entry below a directory whose files
    are later excluded. A Rust ``target/`` can contain hundreds of thousands of
    files, so pruning directories during the walk is both equivalent and much
    cheaper.
    """
    excluded_directories = {prefix.rstrip("/") for prefix in excluded_prefixes}
    files: list[tuple[str, Path]] = []
    for directory, directory_names, file_names in os.walk(root):
        directory_path = Path(directory)
        relative_directory = directory_path.relative_to(root)
        relative_prefix = (
            "" if relative_directory == Path(".") else relative_directory.as_posix()
        )
        directory_names[:] = sorted(
            name
            for name in directory_names
            if name != "__pycache__"
            and (
                f"{relative_prefix}/{name}" if relative_prefix else name
            )
            not in excluded_directories
        )
        for name in file_names:
            path = directory_path / name
            if not path.is_file():
                continue
            relative = (
                f"{relative_prefix}/{name}" if relative_prefix else name
            )
            if relative in excluded_paths or relative.endswith((".pyc", ".DS_Store")):
                continue
            files.append((relative, path))
    # Preserve the historical ``sorted(root.rglob("*"))`` component ordering
    # so existing gate digests remain stable across this traversal optimization.
    return sorted(files, key=lambda item: item[1])


def release_workspace_digest(root: Path) -> str:
    """Hashes inputs that must remain immutable after a successful gate.

    Checked evidence, generated coverage ledgers, slice/claim state, and the
    two steward handoff documents may be promoted after the gate. Everything
    else under the Rust root is frozen from gate begin through final release.
    """
    mutable_prefixes = (
        "workstreams/claims/",
        "workstreams/package-receipts/",
        "difftests/corpus/coverage/evidence/",
        "target/",
    )
    mutable_paths = {
        INTEGRATION_ATTEMPT.as_posix(),
        INTEGRATION_RECEIPT.as_posix(),
        INTEGRATED_CAMPAIGN_MEMBERS.as_posix(),
        "difftests/corpus/coverage/go_source_inventory.tsv",
        "difftests/corpus/coverage/go_test_inventory.tsv",
        "difftests/corpus/coverage/external_go_source_inventory.tsv",
        "difftests/corpus/coverage/external_go_test_inventory.tsv",
        "HANDOFF.md",
        "PARALLEL.md",
    }
    digest = hashlib.sha256()
    for relative, path in _workspace_files(
        root,
        excluded_prefixes=mutable_prefixes,
        excluded_paths=mutable_paths,
    ):
        if (
            relative.startswith(("workstreams/slices/", "workstreams/campaigns/"))
            and relative.endswith(".toml")
        ):
            continue
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(file_digest(path).encode())
        digest.update(b"\0")
    return digest.hexdigest()


def gate_workspace_digest(root: Path) -> str:
    """Hashes every checked gate input, excluding only runtime/build noise."""
    runtime_prefixes = (
        "target/",
        "workstreams/claims/",
    )
    runtime_paths = {
        INTEGRATION_ATTEMPT.as_posix(),
        INTEGRATION_RECEIPT.as_posix(),
    }
    digest = hashlib.sha256()
    for relative, path in _workspace_files(
        root,
        excluded_prefixes=runtime_prefixes,
        excluded_paths=runtime_paths,
    ):
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(file_digest(path).encode())
        digest.update(b"\0")
    return digest.hexdigest()


def integration_slice_manifest_digest(root: Path) -> str:
    """Hashes slice/campaign contracts while allowing post-gate status promotion."""
    digest = hashlib.sha256()
    for directory_name in (SLICES_DIR, CAMPAIGNS_DIR):
        directory = root / directory_name
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.toml")):
            with path.open("rb") as source:
                record = tomllib.load(source)
            record.pop("status", None)
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(b"\0")
            digest.update(json.dumps(record, sort_keys=True, separators=(",", ":")).encode())
            digest.update(b"\0")
    return digest.hexdigest()


def claim_digests(claims: list[dict[str, object]]) -> dict[str, object]:
    return {
        str(claim["owner"]): {
            "claim_sha256": file_digest(Path(claim["_path"])),
            "upstream_sha256": claim["_upstream_sha256"],
        }
        for claim in claims
    }


def gate_snapshot(root: Path, claims: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema": 1,
        "gate_workspace_sha256": gate_workspace_digest(root),
        "claims": claim_digests(claims),
    }


def release_snapshot(
    root: Path, claims: list[dict[str, object]]
) -> dict[str, object]:
    return {
        "schema": 1,
        "workspace_sha256": release_workspace_digest(root),
        "slice_manifests_sha256": integration_slice_manifest_digest(root),
        "claims": claim_digests(claims),
    }


def load_integration_state(root: Path, relative: Path) -> dict[str, object]:
    path = root / relative
    if not path.exists():
        return {"schema": 1, "claims": {}}
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid integration state {path}: {error}") from error
    if receipt.get("schema") != 1 or not isinstance(receipt.get("claims"), dict):
        raise ValueError(f"{path}: expected schema 1 and object claims")
    return receipt


def begin_integration(root: Path) -> None:
    """Freezes the active claim set and complete workspace before the gate."""
    with claim_lock(root):
        attempt = root / INTEGRATION_ATTEMPT
        if attempt.exists():
            raise ValueError(
                "integration gate already has an active begin snapshot; abort it first"
            )
        claims = validate_claims(root)
        if any(claim["schema"] != 2 for claim in claims):
            raise ValueError(
                "integration gate accepts only schema-2 package claims"
            )
        dirty_rust_paths = uncommitted_rust_paths(root.parent)
        if dirty_rust_paths:
            raise ValueError(
                "integration gate requires a clean Rust code tree; uncommitted path: "
                f"{dirty_rust_paths[0]}"
            )
        snapshot = gate_snapshot(root, claims)
        # A new integration window invalidates every older release receipt;
        # no claim may leave while the workspace is under test.
        (root / INTEGRATION_RECEIPT).unlink(missing_ok=True)
        atomic_write_json(attempt, snapshot)
        print(f"integration_begin\t{len(claims)}")


def finish_integration(root: Path) -> None:
    """Issues a release receipt only when the gate's inputs stayed frozen."""
    with claim_lock(root):
        attempt_path = root / INTEGRATION_ATTEMPT
        if not attempt_path.exists():
            raise ValueError("integration gate has no active begin snapshot")
        before = load_integration_state(root, INTEGRATION_ATTEMPT)
        claims = validate_claims(root)
        if any(claim["schema"] != 2 for claim in claims):
            raise ValueError(
                "integration gate accepts only schema-2 package claims"
            )
        after = gate_snapshot(root, claims)
        if before != after:
            raise ValueError(
                "integration inputs changed while the shared gate was running; rerun it"
            )
        attempt_path.unlink()
        atomic_write_json(root / INTEGRATION_RECEIPT, release_snapshot(root, claims))
        print(f"integration_receipt\t{len(claims)}")


def abort_integration(root: Path) -> None:
    with claim_lock(root):
        (root / INTEGRATION_ATTEMPT).unlink(missing_ok=True)


def consume_integration_receipt(root: Path, claim: dict[str, object]) -> None:
    owner = str(claim["owner"])
    destination = root / INTEGRATION_RECEIPT
    receipt = load_integration_state(root, INTEGRATION_RECEIPT)
    if (
        not isinstance(receipt.get("workspace_sha256"), str)
        or not isinstance(receipt.get("slice_manifests_sha256"), str)
    ):
        raise ValueError(f"{destination}: incomplete integration receipt")
    recorded = receipt["claims"].get(owner)
    if recorded is None:
        raise ValueError(
            f"integrated release for {owner} requires a successful shared "
            "integration gate receipt"
        )
    current = {
        "claim_sha256": file_digest(Path(claim["_path"])),
        "upstream_sha256": claim["_upstream_sha256"],
    }
    if (
        recorded != current
        or receipt["workspace_sha256"] != release_workspace_digest(root)
        or receipt["slice_manifests_sha256"]
        != integration_slice_manifest_digest(root)
    ):
        raise ValueError(
            f"integration gate receipt for {owner} is stale; rerun the shared gate"
        )
    del receipt["claims"][owner]
    if receipt["claims"]:
        atomic_write_json(destination, receipt)
    else:
        destination.unlink(missing_ok=True)


def discard_integration_receipt(root: Path, owner: str) -> None:
    destination = root / INTEGRATION_RECEIPT
    receipt = load_integration_state(root, INTEGRATION_RECEIPT)
    if owner not in receipt["claims"]:
        return
    del receipt["claims"][owner]
    if receipt["claims"]:
        atomic_write_json(destination, receipt)
    else:
        destination.unlink(missing_ok=True)


def parse_test_anchor(value: str) -> str:
    path, separator, tail = value.rpartition(":")
    if not separator:
        raise ValueError(f"invalid test anchor {value!r}; use PATH:LINE:NAME")
    source_path, separator, line = path.rpartition(":")
    if not separator or not line.isdigit() or not source_path or not tail:
        raise ValueError(f"invalid test anchor {value!r}; use PATH:LINE:NAME")
    return f"{source_path}:{int(line)}:{tail}"


def parse_module_test_anchor(value: str) -> str:
    module, separator, anchor = value.partition("::")
    if not separator or not module or not anchor:
        raise ValueError(
            f"invalid qualified module test anchor {value!r}; use MODULE::PATH:LINE:NAME"
        )
    return f"{module}::{parse_test_anchor(anchor)}"


def claim_package(
    root: Path,
    owner: str,
    sources: list[str],
    tests: list[str],
    module_sources: list[str] | None = None,
    module_tests: list[str] | None = None,
    supports: list[str] | None = None,
) -> None:
    if not OWNER_RE.fullmatch(owner):
        raise ValueError("owner must use lowercase letters, digits, '.', or '-'")
    normalized_sources = sorted(set(sources))
    normalized_tests = sorted({parse_test_anchor(item) for item in tests})
    normalized_module_sources = sorted(set(module_sources or []))
    normalized_module_tests = sorted(
        {parse_module_test_anchor(item) for item in (module_tests or [])}
    )
    normalized_supports = sorted(set(supports or []))
    if (
        not normalized_sources
        and not normalized_tests
        and not normalized_supports
        and not normalized_module_sources
        and not normalized_module_tests
    ):
        raise ValueError("claim requires checked Go or module inventory")
    with claim_lock(root):
        claims = validate_claims(root)
        new_slice = load_slices(root).get(owner)
        package_slice = (
            new_slice if new_slice is not None and new_slice["schema"] == "2" else None
        )
        if package_slice is None:
            raise ValueError(
                f"schema-2 package claim for {owner} requires a matching package manifest"
            )
        if package_slice is not None and (
            normalized_sources != list(package_slice["go_sources"])
            or normalized_tests != list(package_slice["go_tests"])
            or normalized_supports != list(package_slice["go_supports"])
            or normalized_module_sources != list(package_slice["module_sources"])
            or normalized_module_tests != list(package_slice["module_tests"])
        ):
            raise ValueError(
                f"schema-2 package claim for {owner} must exactly match its "
                "expanded package inventory; use claim-slice"
            )
        new_rust_paths = set(package_slice["rust_paths"])
        integration_paths = list(package_slice["integration_paths"])
        for active in claims:
            if active["owner"] == owner:
                raise ValueError(f"owner {owner} already has an active claim")
            overlap_sources = sorted(set(active["_sources"]) & set(normalized_sources))
            if overlap_sources:
                raise ValueError(
                    f"source {overlap_sources[0]} is already claimed by {active['owner']}"
                )
            overlap = sorted(set(active["tests"]) & set(normalized_tests))
            if overlap:
                raise ValueError(f"test {overlap[0]} is already claimed by {active['owner']}")
            support_overlap = sorted(
                set(active["_supports"]) & set(normalized_supports)
            )
            if support_overlap:
                raise ValueError(
                    f"support {support_overlap[0]} is already claimed by {active['owner']}"
                )
            module_source_overlap = sorted(
                set(active["_module_sources"]) & set(normalized_module_sources)
            )
            if module_source_overlap:
                raise ValueError(
                    f"module source {module_source_overlap[0]} is already claimed by {active['owner']}"
                )
            module_test_overlap = sorted(
                set(active["_module_tests"]) & set(normalized_module_tests)
            )
            if module_test_overlap:
                raise ValueError(
                    f"module test {module_test_overlap[0]} is already claimed by {active['owner']}"
                )
            rust_overlap = _first_path_overlap(
                [(path, str(active["owner"])) for path in active["_rust_paths"]]
                + [(path, owner) for path in new_rust_paths]
            )
            if rust_overlap is not None:
                first, first_owner, second, second_owner = rust_overlap
                raise ValueError(
                    f"Rust path ownership overlaps claims {first_owner} ({first}) and "
                    f"{second_owner} ({second})"
                )
        sources = {str(row["path"]) for row in load_source_rows(root)}
        known_tests = {test_key(row) for row in load_test_rows(root)}
        unknown_sources = sorted(set(normalized_sources) - sources)
        if unknown_sources:
            raise ValueError(f"unknown source anchor {unknown_sources[0]}")
        unknown = sorted(set(normalized_tests) - known_tests)
        if unknown:
            raise ValueError(f"unknown test anchor {unknown[0]}")
        unknown_module_sources = sorted(
            set(normalized_module_sources) - load_module_source_anchors(root)
        )
        if unknown_module_sources:
            raise ValueError(f"unknown qualified module source anchor {unknown_module_sources[0]}")
        unknown_module_tests = sorted(
            set(normalized_module_tests) - load_module_test_anchors(root)
        )
        if unknown_module_tests:
            raise ValueError(f"unknown qualified module test anchor {unknown_module_tests[0]}")
        destination = root / CLAIMS_DIR / f"{owner}.claim.json"
        active_package_claim = next(
            (claim for claim in claims if claim["schema"] == 2), None
        )
        base_commit = (
            str(active_package_claim["_base_commit"])
            if active_package_claim is not None
            else git_head(root.parent)
        )
        payload = {
            "schema": 2,
            "owner": owner,
            "base_commit": base_commit,
            "sources": normalized_sources,
            "tests": normalized_tests,
            "supports": normalized_supports,
            "rust_paths": sorted(new_rust_paths) if package_slice is not None else [],
            "integration_paths": integration_paths,
            "module_sources": normalized_module_sources,
            "module_tests": normalized_module_tests,
            "upstream_sha256": upstream_snapshot(
                root, normalized_sources, normalized_tests, normalized_supports
            ),
        }
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
    print(destination.relative_to(root))


def claim(
    _root: Path,
    _owner: str,
    _sources: list[str],
    _tests: list[str],
) -> None:
    """Rejects the retired partial-anchor dispatch path."""
    raise ValueError(
        "raw claim dispatch is disabled; claim one complete schema-2 package "
        "with claim-slice"
    )


def release(root: Path, owner: str, integrated: bool, abandon: bool) -> None:
    if not OWNER_RE.fullmatch(owner):
        raise ValueError("invalid owner")
    with claim_lock(root):
        path = root / CLAIMS_DIR / f"{owner}.claim.json"
        if not path.exists():
            raise ValueError(f"owner {owner} has no active claim")
        if integrated:
            claims = validate_claims(root)
            current = next((item for item in claims if item["owner"] == owner), None)
            if current is None:
                raise ValueError(f"owner {owner} has no active claim")
            if current["schema"] != 2:
                raise ValueError(
                    "integrated release accepts only schema-2 package claims"
                )
            record = load_slices(root).get(owner)
            if record is None:
                raise ValueError(
                    f"integrated release requires a checked slice named {owner}"
                )
            releasable_statuses = (
                {"covered"} if record["schema"] == "2" else {"partial", "covered"}
            )
            if record["status"] not in releasable_statuses:
                raise ValueError(
                    f"integrated slice {owner} must be "
                    f"{'covered' if record['schema'] == '2' else 'partial or covered'} "
                    "before release; "
                    f"found {record['status']}"
                )
            require_promoted_module_evidence(root, current, str(record["status"]))
            consume_integration_receipt(root, current)
        elif abandon:
            discard_integration_receipt(root, owner)
        else:
            raise ValueError("release requires exactly one of --integrated or --abandon")
        path.unlink()


def amend(root: Path, owner: str, sources: list[str], tests: list[str]) -> None:
    del root, owner, sources, tests
    raise ValueError(
        "claim amendment is disabled; package snapshots are immutable, so abandon "
        "and reclaim the complete schema-2 package"
    )


def _manifest_with_status(contents: bytes, old: str, new: str, path: Path) -> bytes:
    """Changes the single package-manifest status while preserving all other bytes."""
    try:
        text = contents.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{path}: package manifest is not UTF-8") from error
    pattern = re.compile(
        rf'(?m)^([ \t]*status[ \t]*=[ \t]*)(["\']){re.escape(old)}\2([ \t]*(?:#.*)?)$'
    )
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        raise ValueError(
            f"{path}: expected exactly one status = {old!r} assignment"
        )
    match = matches[0]
    replacement = (
        f"{match.group(1)}{match.group(2)}{new}{match.group(2)}{match.group(3)}"
    )
    return (text[: match.start()] + replacement + text[match.end() :]).encode("utf-8")


def _transitively_depends_on(
    records: dict[str, dict[str, object]], candidate: str, dependency: str
) -> bool:
    pending = list(records[candidate]["depends_on"])
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current == dependency:
            return True
        if current in visited:
            continue
        visited.add(current)
        pending.extend(records[current]["depends_on"])
    return False


def reopen_package(root: Path, owner: str) -> None:
    """Reopens one exact receipted package for a new complete repair campaign."""
    if not OWNER_RE.fullmatch(owner):
        raise ValueError("invalid owner")
    with claim_lock(root):
        for gate_path in (INTEGRATION_ATTEMPT, INTEGRATION_RECEIPT):
            if (root / gate_path).exists():
                raise ValueError(
                    f"cannot reopen a package while integration state exists: {gate_path}"
                )
        active_claims = sorted((root / CLAIMS_DIR).glob("*.claim.json"))
        if active_claims:
            raise ValueError(
                "cannot reopen a package while any claim is active: "
                f"{active_claims[0].relative_to(root)}"
            )

        manifest_path = root / SLICES_DIR / f"{owner}.toml"
        receipt_path = root / PACKAGE_RECEIPTS_DIR / f"{owner}.json"
        try:
            manifest_before = manifest_path.read_bytes()
            receipt_before = receipt_path.read_bytes()
        except OSError as error:
            raise ValueError(f"cannot snapshot package reopen inputs: {error}") from error

        # This validates every covered package and its exact immutable receipt
        # against the bytes frozen above before changing either durable file.
        records = load_slices(root)
        record = records.get(owner)
        if record is None:
            raise ValueError(f"unknown package slice {owner}")
        if record["schema"] != "2":
            raise ValueError("reopen-package accepts only schema-2 package slices")
        if record["status"] != "covered":
            raise ValueError(
                f"package slice {owner} must be covered before reopen; "
                f"found {record['status']}"
            )
        validate_package_receipt(root, owner, record)

        unmet = unmet_slice_prerequisites(record, records)
        if unmet:
            raise ValueError(
                f"package slice {owner} cannot return to a claimable state; "
                f"unmet prerequisites: {'; '.join(unmet)}"
            )
        covered_dependents = sorted(
            name
            for name, candidate in records.items()
            if name != owner
            and candidate["status"] == "covered"
            and _transitively_depends_on(records, name, owner)
        )
        if covered_dependents:
            raise ValueError(
                f"package slice {owner} has covered downstream dependent "
                f"{covered_dependents[0]}; reopen downstream packages first"
            )

        if Path(record["_path"]) != manifest_path:
            raise ValueError(f"package slice {owner} resolved to an unexpected manifest")
        manifest_after = _manifest_with_status(
            manifest_before, "covered", "ready", manifest_path
        )
        if (
            manifest_path.read_bytes() != manifest_before
            or receipt_path.read_bytes() != receipt_before
        ):
            raise ValueError("package manifest or receipt changed during reopen")

        try:
            # Ready-with-a-receipt is validation-safe during the short interval
            # between the two file operations. The claim lock prevents a new
            # claim from observing the transaction before the receipt is gone.
            atomic_write_bytes(manifest_path, manifest_after)
            receipt_path.unlink()
            reopened = load_slices(root)
            load_campaigns(root, reopened)
            if not slice_is_ready(reopened[owner], reopened):
                raise ValueError(
                    f"package slice {owner} did not become legally claimable"
                )
        except Exception as error:
            rollback_errors: list[str] = []
            for path, contents in (
                (manifest_path, manifest_before),
                (receipt_path, receipt_before),
            ):
                try:
                    atomic_write_bytes(path, contents)
                except OSError as rollback_error:
                    rollback_errors.append(f"{path}: {rollback_error}")
            if rollback_errors:
                raise ValueError(
                    f"package reopen failed and rollback failed: {'; '.join(rollback_errors)}"
                ) from error
            raise ValueError(f"package reopen failed and was rolled back: {error}") from error
    print(f"package_reopened\t{owner}")


def queue(root: Path, target: str, ring: str, limit: int | None) -> None:
    slices = load_slices(root)
    claims = validate_claims(root, slices)
    covered_packages = _covered_go_packages(slices)
    claimed_sources = {source for item in claims for source in item["_sources"]}
    claimed_tests = {test for item in claims for test in item["tests"]}
    claimed_supports = {support for item in claims for support in item["_supports"]}
    source_rows = load_source_rows(root)
    test_rows = load_test_rows(root)
    support_rows = load_support_rows(root)
    source_by_path = {str(row["path"]): row for row in source_rows}
    test_by_anchor = {test_key(row): row for row in test_rows}
    known_packages, sources_by_package, tests_by_package = _group_go_inventory(
        source_by_path, test_by_anchor
    )
    support_by_package = {
        package: sorted(
            anchor
            for anchor, row in support_rows.items()
            if row["package"] == package
        )
        for package in known_packages
    }
    packages = sorted(known_packages)
    candidates = []
    for package in packages:
        if package in covered_packages:
            continue
        package_sources = sources_by_package[package]
        package_tests = tests_by_package[package]
        package_supports = support_by_package[package]
        package_source_rows = [source_by_path[path] for path in package_sources]
        package_targets = sorted(
            {str(source["target"]) for source in package_source_rows}
        )
        # Test-only packages have no checked target mapping, so they stay
        # visible as unassigned in every target-filtered queue. Hiding them
        # would make their original obligations impossible to dispatch.
        if package_targets and target not in package_targets:
            continue
        if (
            set(package_sources) & claimed_sources
            or set(package_tests) & claimed_tests
            or set(package_supports) & claimed_supports
        ):
            continue
        open_sources = [
            source
            for source in package_source_rows
            if source["status"] in OPEN_STATUSES
        ]
        open_ring_tests = [
            row
            for row in test_rows
            if test_key(row) in package_tests
            and row["ring"] == ring
            and row["status"] in OPEN_STATUSES
        ]
        open_tests = [
            row
            for row in test_rows
            if test_key(row) in package_tests and row["status"] in OPEN_STATUSES
        ]
        if not open_sources and not open_tests and not package_supports:
            continue
        priority = (
            sum(2 if source["status"] == "UNTRIAGED" else 1 for source in open_sources)
            * 1_000_000
            + len(open_ring_tests) * 10_000
            + min(sum(int(source["lines"]) for source in open_sources), 9_999)
        )
        family_id = hashlib.sha256(
            f"{target}\0{ring}\0{package}".encode()
        ).hexdigest()[:16]
        candidates.append(
            (
                priority,
                package,
                family_id,
                package_sources,
                package_tests,
                package_supports,
                len(open_sources),
                len(open_tests),
                len(open_ring_tests),
                package_targets,
            )
        )
    candidates.sort(key=lambda item: (-item[0], item[1]))
    print(
        "family_id\ttargets\tpriority_ring\tgo_package\tproduction_source_count\t"
        "original_obligation_count\tsupport_artifact_count\topen_source_count\topen_obligation_count\t"
        "open_priority_ring_obligation_count\tpriority"
    )
    for (
        priority,
        package,
        family_id,
        package_sources,
        package_tests,
        package_supports,
        open_source_count,
        open_test_count,
        open_ring_test_count,
        package_targets,
    ) in candidates[:limit]:
        print(
            f"{family_id}\t{','.join(package_targets) if package_targets else '-'}\t"
            f"{ring}\t{package}\t{len(package_sources)}\t{len(package_tests)}\t"
            f"{len(package_supports)}\t{open_source_count}\t{open_test_count}\t"
            f"{open_ring_test_count}\t{priority}"
        )


def _aggregate_status(statuses: set[str]) -> str:
    if "BLOCKED" in statuses:
        return "BLOCKED"
    if "UNTRIAGED" in statuses:
        return "UNTRIAGED"
    if "PARTIAL" in statuses:
        return "PARTIAL"
    return "COVERED"


def _covered_go_packages(
    slices: dict[str, dict[str, object]],
) -> set[str]:
    """Returns Go packages whose checked schema-2 receipt is already valid."""
    return {
        str(package)
        for record in slices.values()
        if record["schema"] == "2" and record["status"] == "covered"
        for package in record["go_packages"]
    }


def _suggested_package_owner(package: str) -> str:
    parts = package.split("/")
    if parts[0] == "pkg":
        parts = parts[1:]
    stem = "-".join(parts)
    stem = re.sub(r"[^a-z0-9.-]+", "-", stem.lower()).strip("-.")
    if not stem:
        raise ValueError(f"cannot derive package owner from {package!r}")
    return f"{stem}-package"


def _package_dependency_paths(
    package_sources: list[str],
    package_tests: list[str],
    package_supports: list[str],
    support_rows: dict[str, dict[str, str]],
) -> list[str]:
    paths = set(package_sources)
    paths.update(
        path
        for anchor in package_tests
        for path in [_test_source_path(anchor)]
        if path.endswith(".go")
    )
    paths.update(
        support_rows[anchor]["path"]
        for anchor in package_supports
        if support_rows[anchor]["path"].endswith(".go")
    )
    return sorted(paths)


def package_frontier(
    root: Path,
    target: str | None,
    ring: str | None,
    limit: int | None,
    include_blocked: bool,
) -> None:
    """Prints dependency-checked whole packages that can start next."""
    slices = load_slices(root)
    claims = validate_claims(root, slices)
    source_rows = load_source_rows(root)
    test_rows = load_test_rows(root)
    support_rows = load_support_rows(root)
    source_by_path = {str(row["path"]): row for row in source_rows}
    test_by_anchor = {test_key(row): row for row in test_rows}
    known_packages, sources_by_package, tests_by_package = _group_go_inventory(
        source_by_path, test_by_anchor
    )
    supports_by_package = {
        package: sorted(
            anchor
            for anchor, row in support_rows.items()
            if row["package"] == package
        )
        for package in known_packages
    }
    package_owners = {
        str(package): name
        for name, record in slices.items()
        if record["schema"] == "2"
        for package in record["go_packages"]
    }
    claimed_packages = {
        str(package)
        for claim in claims
        if claim["schema"] == 2
        for package in slices[str(claim["owner"])]["go_packages"]
    }
    candidates = []
    for package in sorted(known_packages):
        if package in package_owners or package in claimed_packages:
            continue
        package_sources = sources_by_package[package]
        package_tests = tests_by_package[package]
        package_supports = supports_by_package[package]
        targets = sorted({str(source_by_path[path]["target"]) for path in package_sources})
        rings = sorted({str(test_by_anchor[anchor]["ring"]) for anchor in package_tests})
        if not rings:
            rings = ["unassigned"]
        if target is not None and target not in targets:
            continue
        if ring is not None and ring not in rings:
            continue
        dependencies = internal_go_dependencies(
            root,
            _package_dependency_paths(
                package_sources, package_tests, package_supports, support_rows
            ),
        ) - {package}
        depends_on = []
        blockers = []
        for dependency in sorted(dependencies):
            owner = package_owners.get(dependency)
            if owner is None:
                blockers.append(f"{dependency}:missing-manifest")
            elif slices[owner]["status"] != "covered":
                blockers.append(f"{dependency}:{owner}@{slices[owner]['status']}")
            else:
                depends_on.append(owner)
        readiness = "BLOCKED" if blockers else "READY"
        if blockers and not include_blocked:
            continue
        production_lines = sum(
            int(source_by_path[path]["lines"]) for path in package_sources
        )
        score = production_lines + 100 * len(package_tests)
        candidates.append(
            (
                readiness != "READY",
                -score,
                package,
                _suggested_package_owner(package),
                targets,
                rings,
                package_sources,
                package_tests,
                package_supports,
                production_lines,
                depends_on,
                blockers,
            )
        )
    candidates.sort()
    print(
        "readiness\tpackage\tsuggested_owner\ttargets\trings\t"
        "production_source_count\tproduction_lines\toriginal_obligation_count\t"
        "support_count\tdepends_on\tblockers"
    )
    for (
        blocked,
        _negative_score,
        package,
        owner,
        targets,
        rings,
        package_sources,
        package_tests,
        package_supports,
        production_lines,
        depends_on,
        blockers,
    ) in candidates[:limit]:
        print(
            f"{'BLOCKED' if blocked else 'READY'}\t{package}\t{owner}\t"
            f"{','.join(targets) if targets else '-'}\t{','.join(rings)}\t"
            f"{len(package_sources)}\t{production_lines}\t{len(package_tests)}\t"
            f"{len(package_supports)}\t"
            f"{','.join(depends_on) if depends_on else '-'}\t"
            f"{','.join(blockers) if blockers else '-'}"
        )


def start_package(
    root: Path,
    package: str,
    owner: str | None,
    targets: list[str],
    rust_paths: list[str],
    integration_paths: list[str],
    consumer: str | None,
    test_target: str | None,
) -> None:
    """Creates and claims one dependency-ready whole-package manifest."""
    _, package = _normalized_package(package, qualified=False)
    owner = owner or _suggested_package_owner(package)
    if not OWNER_RE.fullmatch(owner):
        raise ValueError("owner must use lowercase letters, digits, '.', or '-'")
    slices = load_slices(root)
    if validate_claims(root, slices):
        raise ValueError(
            "start-package is a single-worker transaction and requires no active claim"
        )
    for name, record in slices.items():
        if package in record["go_packages"]:
            raise ValueError(
                f"Go package {package} already has canonical schema-2 manifest {name}"
            )
    source_rows = {str(row["path"]): row for row in load_source_rows(root)}
    test_rows = {test_key(row): row for row in load_test_rows(root)}
    support_rows = load_support_rows(root)
    package_sources, package_tests, package_supports = expand_go_packages(
        [package], source_rows, test_rows, support_rows
    )
    checked_targets = sorted({str(source_rows[path]["target"]) for path in package_sources})
    targets = sorted(set(targets or checked_targets))
    if not targets:
        raise ValueError("test-only packages require at least one explicit --target")
    rings = sorted({str(test_rows[anchor]["ring"]) for anchor in package_tests})
    if not rings:
        rings = ["unassigned"]
    package_owners = {
        str(go_package): name
        for name, record in slices.items()
        if record["schema"] == "2"
        for go_package in record["go_packages"]
    }
    dependencies = internal_go_dependencies(
        root,
        _package_dependency_paths(
            package_sources, package_tests, package_supports, support_rows
        ),
    ) - {package}
    depends_on = []
    for dependency in sorted(dependencies):
        dependency_owner = package_owners.get(dependency)
        if dependency_owner is None:
            raise ValueError(
                f"cannot start {package}: direct internal Go import {dependency} "
                "has no canonical schema-2 package manifest"
            )
        dependency_status = slices[dependency_owner]["status"]
        if dependency_status != "covered":
            raise ValueError(
                f"cannot start {package}: dependency {dependency_owner} is "
                f"{dependency_status}, not covered"
            )
        depends_on.append(dependency_owner)
    rust_paths = sorted(
        {_normalized_repo_path(path, label="Rust path") for path in rust_paths}
    )
    if not rust_paths:
        raise ValueError("start-package requires at least one --rust-path")
    integration_paths = sorted(
        {
            _normalized_repo_path(path, label="integration path")
            for path in integration_paths
        }
    )
    consumer = consumer or (
        f"Provides the complete Rust-native {package} contract; downstream "
        "package completion is not credited here."
    )
    test_target = test_target or f"{owner.replace('-', '_')}_source"
    manifest = root / SLICES_DIR / f"{owner}.toml"
    if manifest.exists():
        raise ValueError(f"package manifest already exists: {manifest}")
    contents = (
        'schema = "2"\n'
        f"slice = {json.dumps(owner)}\n"
        'status = "ready"\n'
        f"targets = {json.dumps(targets)}\n"
        f"rings = {json.dumps(rings)}\n"
        f"consumer = {json.dumps(consumer)}\n"
        f"test_target = {json.dumps(test_target)}\n"
        f"go_packages = {json.dumps([package])}\n"
        "module_packages = []\n"
        f"depends_on = {json.dumps(depends_on)}\n"
        f"rust_paths = {json.dumps(rust_paths, indent=2)}\n"
        f"integration_paths = {json.dumps(integration_paths, indent=2)}\n"
    )
    atomic_write_bytes(manifest, contents.encode())
    try:
        load_slices(root)
        claim_slice(root, owner, owner)
    except (OSError, ValueError):
        manifest.unlink(missing_ok=True)
        raise
    print(
        f"package_start\t{owner}\t{package}\tsources={len(package_sources)}\t"
        f"tests={len(package_tests)}\tsupports={len(package_supports)}"
    )


def package_inventory(root: Path, target: str | None, module: str | None) -> None:
    """Prints exact package-sized dispatch units from checked inventories."""
    slices = load_slices(root)
    claims = validate_claims(root, slices)
    source_rows = load_source_rows(root)
    test_rows = load_test_rows(root)
    support_rows = load_support_rows(root)
    print(
        "package\ttarget\tproduction_source_count\tproduction_lines\t"
        "original_obligation_count\taggregate_status\tactive_owner"
    )
    if module is None:
        covered_packages = _covered_go_packages(slices)
        source_by_path = {str(row["path"]): row for row in source_rows}
        test_by_anchor = {test_key(row): row for row in test_rows}
        known_packages, sources_by_package, tests_by_package = _group_go_inventory(
            source_by_path, test_by_anchor
        )
        declared_targets: dict[str, set[str]] = {}
        for record in slices.values():
            if record["schema"] != "2":
                continue
            for package in record["go_packages"]:
                declared_targets.setdefault(str(package), set()).update(
                    str(item) for item in record["targets"]
                )
        support_by_package = {
            package: sorted(
                anchor
                for anchor, row in support_rows.items()
                if row["package"] == package
            )
            for package in known_packages
        }
        for package in sorted(known_packages):
            package_sources = [
                source_by_path[path] for path in sources_by_package[package]
            ]
            source_targets = sorted(
                {str(row["target"]) for row in package_sources}
            )
            package_targets = sorted(declared_targets.get(package, set()))
            visible_targets = package_targets or source_targets
            if target is not None and target not in visible_targets:
                continue
            package_tests = [
                test_by_anchor[anchor] for anchor in tests_by_package[package]
            ]
            package_supports = support_by_package[package]
            source_anchors = {str(row["path"]) for row in package_sources}
            test_anchors = {test_key(row) for row in package_tests}
            owners = sorted(
                str(claim["owner"])
                for claim in claims
                if source_anchors & set(claim["_sources"])
                or test_anchors & set(claim["tests"])
                or set(package_supports) & set(claim["_supports"])
            )
            statuses = {
                str(row["status"]) for row in [*package_sources, *package_tests]
            }
            if package_supports and package not in covered_packages:
                statuses.add("UNTRIAGED")
            print(
                f"{package}\t{','.join(visible_targets) if visible_targets else '-'}\t"
                f"{len(package_sources)}\t"
                f"{sum(int(row['lines']) for row in package_sources)}\t"
                f"{len(package_tests) + len(package_supports)}\t"
                f"{_aggregate_status(statuses)}\t"
                f"{','.join(owners) if owners else '-'}"
            )
        return

    if not OWNER_RE.fullmatch(module):
        raise ValueError("--module must name one checked external universe")
    prefix = f"{module}::"
    module_sources = load_module_source_rows(root)
    module_tests = load_module_test_rows(root)
    source_inventory = {
        f"{universe}::{path}": {"lines": int(lines), "status": status}
        for universe, path, lines, _sha, status, _owner, _artifact, _note in read_tsv(
            root / MODULE_SOURCE_LEDGER, 8
        )
    }
    known_packages = _module_package_directories(module, module_sources, module_tests)
    if not known_packages:
        raise ValueError(f"unknown external module {module}")
    for package in sorted(known_packages):
        package_sources = sorted(
            anchor
            for anchor in module_sources
            if anchor.startswith(prefix)
            and _nearest_package(anchor.removeprefix(prefix), known_packages)
            == package
        )
        package_tests = sorted(
            anchor
            for anchor in module_tests
            if anchor.startswith(prefix)
            and _nearest_package(
                anchor.removeprefix(prefix).rsplit(":", 2)[0], known_packages
            )
            == package
        )
        owners = sorted(
            str(claim["owner"])
            for claim in claims
            if set(package_sources) & set(claim["_module_sources"])
            or set(package_tests) & set(claim["_module_tests"])
        )
        statuses = {
            module_sources[anchor]["status"] for anchor in package_sources
        } | {module_tests[anchor]["status"] for anchor in package_tests}
        print(
            f"{module}::{package}\texternal:{module}\t{len(package_sources)}\t"
            f"{sum(source_inventory[anchor]['lines'] for anchor in package_sources)}\t"
            f"{len(package_tests)}\t{_aggregate_status(statuses)}\t"
            f"{','.join(owners) if owners else '-'}"
        )


def ready_slices(root: Path, target: str | None, ring: str | None) -> None:
    records = load_slices(root)
    claims = validate_claims(root, records)
    claimed_sources = {source for item in claims for source in item["_sources"]}
    claimed_module_sources = {
        source for item in claims for source in item["_module_sources"]
    }
    claimed_tests = {test for item in claims for test in item["tests"]}
    claimed_supports = {support for item in claims for support in item["_supports"]}
    claimed_module_tests = {
        test for item in claims for test in item["_module_tests"]
    }
    claimed_rust_paths = {path for item in claims for path in item["_rust_paths"]}
    print(
        "slice\tstatus\ttargets\tring\tconsumer\tsource_count\ttest_count\t"
        "support_count\t"
        "test_target\tdepends_on"
    )
    for name, record in sorted(records.items()):
        if record["schema"] != "2":
            continue
        if not slice_is_ready(record, records):
            continue
        if target is not None and target not in record["rust_targets"]:
            continue
        if ring is not None and ring not in record["rings"]:
            continue
        if set(record["go_sources"]) & claimed_sources:
            continue
        if set(record["module_sources"]) & claimed_module_sources:
            continue
        if set(record["go_tests"]) & claimed_tests:
            continue
        if set(record["go_supports"]) & claimed_supports:
            continue
        if set(record["module_tests"]) & claimed_module_tests:
            continue
        if set(record["rust_paths"]) & claimed_rust_paths:
            continue
        print(
            f"{name}\t{record['status']}\t{record['target']}\t{record['ring']}\t"
            f"{record['consumer']}\t{len(record['go_sources']) + len(record['module_sources'])}\t"
            f"{len(record['go_tests']) + len(record['module_tests'])}\t"
            f"{len(record['go_supports'])}\t{record['test_target']}\t"
            f"{','.join(record['depends_on']) if record['depends_on'] else '-'}"
        )


def claim_slice(root: Path, owner: str, slice_name: str) -> None:
    records = load_slices(root)
    record = records.get(slice_name)
    if record is None:
        raise ValueError(f"unknown vertical slice {slice_name}")
    if record["schema"] != "2":
        raise ValueError(
            "claim-slice dispatch accepts only schema-2 package slices"
        )
    if owner != slice_name:
        raise ValueError(
            "schema-2 package slice claim owner must equal the checked slice name"
        )
    if not slice_is_ready(record, records):
        unmet = unmet_slice_prerequisites(record, records)
        detail = f"; unmet prerequisites: {'; '.join(unmet)}" if unmet else ""
        raise ValueError(f"vertical slice {slice_name} is not ready{detail}")
    claim_package(
        root,
        owner,
        list(record["go_sources"]),
        list(record["go_tests"]),
        list(record["module_sources"]),
        list(record["module_tests"]),
        list(record["go_supports"]),
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subcommands = result.add_subparsers(dest="command", required=True)
    queue_command = subcommands.add_parser("queue")
    queue_command.add_argument("--target", required=True)
    queue_command.add_argument("--ring", required=True)
    queue_command.add_argument("--limit", type=int)
    packages_command = subcommands.add_parser("packages")
    package_filter = packages_command.add_mutually_exclusive_group()
    package_filter.add_argument("--target")
    package_filter.add_argument("--module")
    frontier_command = subcommands.add_parser(
        "frontier",
        help="rank dependency-checked whole packages that can start next",
    )
    frontier_command.add_argument("--target")
    frontier_command.add_argument("--ring")
    frontier_command.add_argument("--limit", type=int)
    frontier_command.add_argument(
        "--include-blocked",
        action="store_true",
        help="also show packages whose direct internal imports are not covered",
    )
    start_command = subcommands.add_parser(
        "start-package",
        help="scaffold, validate, and claim one dependency-ready whole package",
    )
    start_command.add_argument("--package", required=True)
    start_command.add_argument("--owner")
    start_command.add_argument("--target", action="append", default=[])
    start_command.add_argument("--rust-path", action="append", required=True)
    start_command.add_argument("--integration-path", action="append", default=[])
    start_command.add_argument("--consumer")
    start_command.add_argument("--test-target")
    ready_command = subcommands.add_parser("ready")
    ready_command.add_argument("--target")
    ready_command.add_argument("--ring")
    claim_slice_command = subcommands.add_parser("claim-slice")
    claim_slice_command.add_argument("--owner", required=True)
    claim_slice_command.add_argument("--slice", required=True)
    claim_command = subcommands.add_parser("claim")
    claim_command.add_argument("--owner", required=True)
    claim_command.add_argument("--source", action="append", required=True)
    claim_command.add_argument("--test", action="append", default=[])
    release_command = subcommands.add_parser("release")
    release_command.add_argument("--owner", required=True)
    release_mode = release_command.add_mutually_exclusive_group(required=True)
    release_mode.add_argument(
        "--integrated",
        action="store_true",
        help="release after a receipted shared gate; requires partial or covered",
    )
    release_mode.add_argument(
        "--abandon",
        action="store_true",
        help="explicitly abandon or recover a stale claim without asserting integration",
    )
    amend_command = subcommands.add_parser("amend")
    amend_command.add_argument("--owner", required=True)
    amend_command.add_argument("--source", action="append", default=[])
    amend_command.add_argument("--test", action="append", default=[])
    reopen_command = subcommands.add_parser(
        "reopen-package",
        help="atomically reopen one exact receipted schema-2 package for repair",
    )
    reopen_command.add_argument("--owner", required=True)
    close_command = subcommands.add_parser(
        "close-package",
        help="validate touched Rust crates and atomically close one whole package",
    )
    close_command.add_argument("--owner", required=True)
    subcommands.add_parser("gate-begin", help=argparse.SUPPRESS)
    subcommands.add_parser("gate-finish", help=argparse.SUPPRESS)
    subcommands.add_parser("gate-abort", help=argparse.SUPPRESS)
    subcommands.add_parser("check")
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.command == "queue":
            if arguments.limit is not None and arguments.limit < 1:
                raise ValueError("--limit must be positive")
            queue(RUST_ROOT, arguments.target, arguments.ring, arguments.limit)
        elif arguments.command == "packages":
            package_inventory(RUST_ROOT, arguments.target, arguments.module)
        elif arguments.command == "frontier":
            if arguments.limit is not None and arguments.limit < 1:
                raise ValueError("--limit must be positive")
            package_frontier(
                RUST_ROOT,
                arguments.target,
                arguments.ring,
                arguments.limit,
                arguments.include_blocked,
            )
        elif arguments.command == "start-package":
            start_package(
                RUST_ROOT,
                arguments.package,
                arguments.owner,
                arguments.target,
                arguments.rust_path,
                arguments.integration_path,
                arguments.consumer,
                arguments.test_target,
            )
        elif arguments.command == "ready":
            ready_slices(RUST_ROOT, arguments.target, arguments.ring)
        elif arguments.command == "claim-slice":
            claim_slice(RUST_ROOT, arguments.owner, arguments.slice)
        elif arguments.command == "claim":
            claim(RUST_ROOT, arguments.owner, arguments.source, arguments.test)
        elif arguments.command == "release":
            release(
                RUST_ROOT,
                arguments.owner,
                arguments.integrated,
                arguments.abandon,
            )
        elif arguments.command == "amend":
            amend(RUST_ROOT, arguments.owner, arguments.source, arguments.test)
        elif arguments.command == "reopen-package":
            reopen_package(RUST_ROOT, arguments.owner)
        elif arguments.command == "close-package":
            subprocess.run(
                [
                    sys.executable,
                    "scripts/campaign_close.py",
                    "--package",
                    arguments.owner,
                    "--gate",
                ],
                cwd=RUST_ROOT,
                check=True,
            )
        elif arguments.command == "gate-begin":
            begin_integration(RUST_ROOT)
        elif arguments.command == "gate-finish":
            finish_integration(RUST_ROOT)
        elif arguments.command == "gate-abort":
            abort_integration(RUST_ROOT)
        else:
            # Validate consolidated external ledgers even when the fixture or
            # repository currently has no module-owning slice. Qualified keys
            # are an input invariant, not something callers may overwrite.
            load_module_source_rows(RUST_ROOT)
            load_module_test_rows(RUST_ROOT)
            slices = load_slices(RUST_ROOT)
            campaigns = load_campaigns(RUST_ROOT, slices)
            claims = validate_claims(RUST_ROOT, slices)
            claimed_sources = {source for item in claims for source in item["_sources"]}
            claimed_tests = {test for item in claims for test in item["tests"]}
            claimed_supports = {
                support for item in claims for support in item["_supports"]
            }
            claimed_module_sources = {
                source for item in claims for source in item["_module_sources"]
            }
            claimed_module_tests = {
                test for item in claims for test in item["_module_tests"]
            }
            claimed_rust_paths = {
                path for item in claims for path in item["_rust_paths"]
            }
            ready = sum(
                1
                for record in slices.values()
                if record["schema"] == "2"
                if slice_is_ready(record, slices)
                and not (set(record["go_sources"]) & claimed_sources)
                and not (set(record["go_tests"]) & claimed_tests)
                and not (set(record["go_supports"]) & claimed_supports)
                and not (set(record["module_sources"]) & claimed_module_sources)
                and not (set(record["module_tests"]) & claimed_module_tests)
                and not (set(record["rust_paths"]) & claimed_rust_paths)
            )
            print(f"active_claims\t{len(claims)}")
            print(f"ready_slices\t{ready}")
            print(f"campaigns\t{len(campaigns)}")
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
