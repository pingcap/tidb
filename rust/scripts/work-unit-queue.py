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
import sys
import tomllib
from typing import Iterator


RUST_ROOT = Path(
    os.environ.get("TIDB_REWRITE_RUST_ROOT", Path(__file__).resolve().parents[1])
)
SOURCE_LEDGER = Path("difftests/corpus/coverage/go_source_inventory.tsv")
TEST_LEDGER = Path("difftests/corpus/coverage/go_test_inventory.tsv")
MODULE_SOURCE_LEDGER = Path("difftests/corpus/coverage/external_go_source_inventory.tsv")
MODULE_TEST_LEDGER = Path("difftests/corpus/coverage/external_go_test_inventory.tsv")
CLAIMS_DIR = Path("workstreams/claims")
TRANSFERS_DIR = Path("difftests/corpus/coverage/evidence/transfers")
SLICES_DIR = Path("workstreams/slices")
CAMPAIGNS_DIR = Path("workstreams/campaigns")
INTEGRATED_CAMPAIGN_MEMBERS = CAMPAIGNS_DIR / "integrated-members.tsv"
INTEGRATION_RECEIPT = Path("workstreams/integration-receipt.json")
INTEGRATION_ATTEMPT = Path("workstreams/integration-attempt.json")
OWNER_RE = re.compile(r"^[a-z0-9][a-z0-9.-]*$")
OPEN_STATUSES = {"UNTRIAGED", "PARTIAL", "BLOCKED"}
SLICE_STATUSES = {"ready", "active", "partial", "covered", "blocked", "retired"}
EVIDENCE_MINIMUM_STATUSES = {"PARTIAL", "COVERED"}
EVIDENCE_STATUS_RANK = {"UNTRIAGED": 0, "PARTIAL": 1, "COVERED": 2}
CAMPAIGN_STATUSES = {"planned", "active", "integrated"}
CAMPAIGN_MIN_SOURCE_COUNT = 9
CAMPAIGN_MIN_TEST_COUNT = 50


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
    for universe, source, _lines, _sha, status, owner, artifact, note in read_tsv(path, 8):
        anchor = f"{universe}::{source}"
        if anchor in rows:
            raise ValueError(f"{path}: duplicate qualified module source anchor {anchor}")
        validate_evidence_artifact(root, MODULE_SOURCE_LEDGER, anchor, artifact)
        rows[anchor] = {"status": status, "owner": owner, "artifact": artifact, "note": note}
    return rows


def load_module_test_anchors(root: Path) -> set[str]:
    return set(load_module_test_rows(root))


def load_module_test_rows(root: Path) -> dict[str, dict[str, str]]:
    path = root / MODULE_TEST_LEDGER
    if not path.exists():
        return {}
    rows = {}
    for universe, _kind, source, line, name, _ring, _sha, status, owner, artifact, note in read_tsv(path, 11):
        anchor = f"{universe}::{source}:{int(line)}:{name}"
        if anchor in rows:
            raise ValueError(f"{path}: duplicate qualified module test anchor {anchor}")
        validate_evidence_artifact(root, MODULE_TEST_LEDGER, anchor, artifact)
        rows[anchor] = {"status": status, "owner": owner, "artifact": artifact, "note": note}
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


def source_test_stem(path: str) -> tuple[str, str]:
    item = Path(path)
    stem = item.name.removesuffix("_test.go").removesuffix(".go")
    return item.parent.as_posix(), stem


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
    known_sources = set(source_rows)
    known_tests = set(test_rows)
    records: dict[str, dict[str, object]] = {}
    required_strings = ("slice", "status", "target", "ring", "consumer", "test_target")
    required_lists = ("go_sources", "go_tests", "depends_on", "rust_paths")
    for path in sorted(directory.glob("*.toml")):
        with path.open("rb") as source:
            record = tomllib.load(source)
        if record.get("schema") != "1":
            raise ValueError(f"{path}: expected slice schema 1")
        for field in required_strings:
            if not isinstance(record.get(field), str) or not record[field]:
                raise ValueError(f"{path}: {field} must be a non-empty string")
        for field in required_lists:
            if not isinstance(record.get(field), list) or not all(
                isinstance(item, str) and item for item in record[field]
            ):
                raise ValueError(f"{path}: {field} must be a string array")
        name = str(record["slice"])
        if not OWNER_RE.fullmatch(name) or path.name != f"{name}.toml":
            raise ValueError(f"{path}: invalid slice name or filename")
        if name in records:
            raise ValueError(f"{path}: duplicate slice {name}")
        if record["status"] not in SLICE_STATUSES:
            raise ValueError(f"{path}: invalid status {record['status']!r}")
        for field in ("module_sources", "module_tests"):
            value = record.get(field, [])
            if not isinstance(value, list) or not all(
                isinstance(item, str) and item for item in value
            ):
                raise ValueError(f"{path}: {field} must be a string array")
            record[field] = sorted(set(value))
        sources = sorted(set(record["go_sources"]))
        if not sources and not record["module_sources"]:
            raise ValueError(
                f"{path}: vertical slice must own at least one Go or module source"
            )
        unknown_sources = sorted(set(sources) - known_sources)
        if unknown_sources:
            raise ValueError(f"{path}: unknown source anchor {unknown_sources[0]}")
        tests = sorted({parse_test_anchor(item) for item in record["go_tests"]})
        unknown_tests = sorted(set(tests) - known_tests)
        if unknown_tests:
            raise ValueError(f"{path}: unknown test anchor {unknown_tests[0]}")
        unknown_module_sources = sorted(
            set(record["module_sources"]) - load_module_source_anchors(root)
        )
        if unknown_module_sources:
            raise ValueError(
                f"{path}: unknown qualified module source anchor {unknown_module_sources[0]}"
            )
        normalized_module_tests = sorted(
            {parse_module_test_anchor(item) for item in record["module_tests"]}
        )
        unknown_module_tests = sorted(
            set(normalized_module_tests) - load_module_test_anchors(root)
        )
        if unknown_module_tests:
            raise ValueError(
                f"{path}: unknown qualified module test anchor {unknown_module_tests[0]}"
            )
        record["go_sources"] = sources
        record["go_tests"] = tests
        record["module_tests"] = normalized_module_tests
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
        record["_path"] = path
        records[name] = record

    for name, record in records.items():
        for dependency in record["depends_on"]:
            if dependency == name:
                raise ValueError(f"{record['_path']}: slice cannot depend on itself")
            if dependency not in records:
                raise ValueError(f"{record['_path']}: unknown dependency {dependency}")

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
    return records


def load_campaigns(
    root: Path, slices: dict[str, dict[str, object]]
) -> dict[str, dict[str, object]]:
    """Loads checked multi-slice batches and proves their parallel write sets."""
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
        if record.get("schema") != "1":
            raise ValueError(f"{path}: expected campaign schema 1")
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
        if len(members) < 2 or len(set(members)) != len(members):
            raise ValueError(f"{path}: campaign slices must contain two or more unique members")
        unknown = sorted(set(members) - set(slices))
        if unknown:
            raise ValueError(f"{path}: unknown campaign slice {unknown[0]}")
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
        seen_module_sources: dict[str, str] = {}
        seen_module_tests: dict[str, str] = {}
        seen_rust_paths: dict[str, str] = {}
        for member in members:
            slice_record = slices[member]
            for label, values, seen in (
                ("source", slice_record["go_sources"], seen_sources),
                ("test", slice_record["go_tests"], seen_tests),
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
        # The batch floor is an admission rule, not a mutable property of a
        # historical receipt. Later ownership consolidation can move exact
        # anchors out of an integrated member without changing what its gate
        # validated; rewriting campaign membership to satisfy today's count
        # would instead falsify that history.
        if status != "integrated":
            combined_source_count = len(seen_sources) + len(seen_module_sources)
            combined_test_count = len(seen_tests) + len(seen_module_tests)
            if combined_source_count < CAMPAIGN_MIN_SOURCE_COUNT:
                raise ValueError(
                    f"{path}: campaign has {combined_source_count} production sources; "
                    f"minimum is {CAMPAIGN_MIN_SOURCE_COUNT}"
                )
            if combined_test_count < CAMPAIGN_MIN_TEST_COUNT:
                raise ValueError(
                    f"{path}: campaign has {combined_test_count} original obligations; "
                    f"minimum is {CAMPAIGN_MIN_TEST_COUNT}"
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


def validate_claims(root: Path) -> list[dict[str, object]]:
    validate_transfers(root)
    slices = load_slices(root)
    load_campaigns(root, slices)
    sources = {str(row["path"]) for row in load_source_rows(root)}
    tests = {test_key(row) for row in load_test_rows(root)}
    seen_sources: dict[str, str] = {}
    seen_tests: dict[str, str] = {}
    seen_module_sources: dict[str, str] = {}
    seen_module_tests: dict[str, str] = {}
    seen_rust_paths: dict[str, str] = {}
    claims = load_claims(root)
    for claim in claims:
        path = Path(claim["_path"])
        owner = claim.get("owner")
        schema = claim.get("schema")
        claimed_sources = claim_sources(claim)
        claimed_tests = claim.get("tests")
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
        if not claimed_sources and not claimed_module_sources:
            raise ValueError(f"{path}: claim must own at least one Go or module source")
        slice_record = slices.get(owner) if schema == 2 else None
        if slice_record is not None:
            expected_sources = list(slice_record["go_sources"])
            expected_tests = list(slice_record["go_tests"])
            expected_module_sources = list(slice_record["module_sources"])
            expected_module_tests = list(slice_record["module_tests"])
            if (
                claimed_sources != expected_sources
                or sorted(claimed_tests) != expected_tests
                or claimed_module_sources != expected_module_sources
                or claimed_module_tests != expected_module_tests
            ):
                differences = []
                for label, values in (
                    ("missing sources", sorted(set(expected_sources) - set(claimed_sources))),
                    ("extra sources", sorted(set(claimed_sources) - set(expected_sources))),
                    ("missing tests", sorted(set(expected_tests) - set(claimed_tests))),
                    ("extra tests", sorted(set(claimed_tests) - set(expected_tests))),
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
            claim["_rust_paths"] = list(slice_record["rust_paths"])
            for rust_path in claim["_rust_paths"]:
                if rust_path in seen_rust_paths:
                    raise ValueError(
                        f"Rust path {rust_path} overlaps slice claims "
                        f"{seen_rust_paths[rust_path]} and {owner}"
                    )
                seen_rust_paths[rust_path] = owner
        else:
            claim["_rust_paths"] = []
        claim["_sources"] = claimed_sources
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


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def release_workspace_digest(root: Path) -> str:
    """Hashes inputs that must remain immutable after a successful gate.

    Checked evidence, generated coverage ledgers, slice/claim state, and the
    two steward handoff documents may be promoted after the gate. Everything
    else under the Rust root is frozen from gate begin through final release.
    """
    mutable_prefixes = (
        "workstreams/claims/",
        "difftests/corpus/coverage/evidence/",
        "target/",
    )
    mutable_paths = {
        INTEGRATION_ATTEMPT.as_posix(),
        INTEGRATION_RECEIPT.as_posix(),
        "difftests/corpus/coverage/go_source_inventory.tsv",
        "difftests/corpus/coverage/go_test_inventory.tsv",
        "difftests/corpus/coverage/external_go_source_inventory.tsv",
        "difftests/corpus/coverage/external_go_test_inventory.tsv",
        "HANDOFF.md",
        "PARALLEL.md",
    }
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if (
            relative in mutable_paths
            or (
                relative.startswith("workstreams/slices/")
                and relative.endswith(".toml")
            )
            or any(relative.startswith(prefix) for prefix in mutable_prefixes)
            or "/__pycache__/" in f"/{relative}"
            or relative.endswith((".pyc", ".DS_Store"))
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
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if (
            relative in runtime_paths
            or any(relative.startswith(prefix) for prefix in runtime_prefixes)
            or "/__pycache__/" in f"/{relative}"
            or relative.endswith((".pyc", ".DS_Store"))
        ):
            continue
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


def claim(
    root: Path,
    owner: str,
    sources: list[str],
    tests: list[str],
    module_sources: list[str] | None = None,
    module_tests: list[str] | None = None,
) -> None:
    if not OWNER_RE.fullmatch(owner):
        raise ValueError("owner must use lowercase letters, digits, '.', or '-'")
    normalized_sources = sorted(set(sources))
    normalized_tests = sorted({parse_test_anchor(item) for item in tests})
    normalized_module_sources = sorted(set(module_sources or []))
    normalized_module_tests = sorted(
        {parse_module_test_anchor(item) for item in (module_tests or [])}
    )
    if not normalized_sources and not normalized_module_sources:
        raise ValueError("claim requires at least one Go or module source")
    with claim_lock(root):
        claims = validate_claims(root)
        new_slice = load_slices(root).get(owner)
        new_rust_paths = set(new_slice["rust_paths"]) if new_slice is not None else set()
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
            rust_overlap = sorted(set(active["_rust_paths"]) & new_rust_paths)
            if rust_overlap:
                raise ValueError(
                    f"Rust path {rust_overlap[0]} is already claimed by {active['owner']}"
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
        payload = {
            "schema": 2,
            "owner": owner,
            "sources": normalized_sources,
            "tests": normalized_tests,
            "module_sources": normalized_module_sources,
            "module_tests": normalized_module_tests,
        }
        descriptor = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2, sort_keys=True)
            output.write("\n")
    print(destination.relative_to(root))


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
            record = load_slices(root).get(owner)
            if record is None:
                raise ValueError(
                    f"integrated release requires a checked slice named {owner}"
                )
            if record["status"] not in {"partial", "covered"}:
                raise ValueError(
                    f"integrated slice {owner} must be partial or covered before release; "
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
    if not OWNER_RE.fullmatch(owner):
        raise ValueError("invalid owner")
    source_additions = set(sources)
    test_additions = {parse_test_anchor(item) for item in tests}
    if not source_additions and not test_additions:
        raise ValueError("amend requires at least one --source or --test anchor")
    with claim_lock(root):
        claims = validate_claims(root)
        current = next((item for item in claims if item["owner"] == owner), None)
        if current is None:
            raise ValueError(f"owner {owner} has no active claim")
        known_sources = {str(row["path"]) for row in load_source_rows(root)}
        unknown_sources = sorted(source_additions - known_sources)
        if unknown_sources:
            raise ValueError(f"unknown source anchor {unknown_sources[0]}")
        known_tests = {test_key(row) for row in load_test_rows(root)}
        unknown = sorted(test_additions - known_tests)
        if unknown:
            raise ValueError(f"unknown test anchor {unknown[0]}")
        for active in claims:
            if active["owner"] == owner:
                continue
            source_overlap = sorted(set(active["_sources"]) & source_additions)
            if source_overlap:
                raise ValueError(
                    f"source {source_overlap[0]} is already claimed by {active['owner']}"
                )
            test_overlap = sorted(set(active["tests"]) & test_additions)
            if test_overlap:
                raise ValueError(
                    f"test {test_overlap[0]} is already claimed by {active['owner']}"
                )
        payload = {
            "schema": 2,
            "owner": owner,
            "sources": sorted(set(current["_sources"]) | source_additions),
            "tests": sorted(set(current["tests"]) | test_additions),
            "module_sources": list(current["_module_sources"]),
            "module_tests": list(current["_module_tests"]),
        }
        destination = root / CLAIMS_DIR / f"{owner}.claim.json"
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                json.dump(payload, output, indent=2, sort_keys=True)
                output.write("\n")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)


def queue(root: Path, target: str, ring: str, limit: int | None) -> None:
    claims = validate_claims(root)
    claimed_sources = {source for item in claims for source in item["_sources"]}
    claimed_tests = {test for item in claims for test in item["tests"]}
    test_rows = [
        row
        for row in load_test_rows(root)
        if row["ring"] == ring
        and row["status"] in OPEN_STATUSES
        and test_key(row) not in claimed_tests
    ]
    candidates = []
    for source in load_source_rows(root):
        if (
            source["target"] != target
            or source["status"] not in OPEN_STATUSES
            or source["path"] in claimed_sources
        ):
            continue
        directory, stem = source_test_stem(str(source["path"]))
        matches = [
            row
            for row in test_rows
            if source_test_stem(str(row["path"])) == (directory, stem)
        ]
        keys = sorted(test_key(row) for row in matches)
        priority = (
            (2 if source["status"] == "UNTRIAGED" else 1) * 1_000_000
            + len(keys) * 10_000
            + min(int(source["lines"]), 9_999)
        )
        family_id = hashlib.sha256(
            f"{target}\0{ring}\0{source['path']}".encode()
        ).hexdigest()[:16]
        candidates.append((priority, str(source["path"]), family_id, source, keys))
    candidates.sort(key=lambda item: (-item[0], item[1]))
    print(
        "family_id\ttarget\tring\tsource_path\tsource_status\tsource_lines\t"
        "match_kind\tcandidate_test_count\tcandidate_tests\tpriority"
    )
    for priority, _, family_id, source, keys in candidates[:limit]:
        print(
            f"{family_id}\t{target}\t{ring}\t{source['path']}\t{source['status']}\t"
            f"{source['lines']}\tsame-directory-stem-candidate\t{len(keys)}\t"
            f"{','.join(keys) if keys else '-'}\t{priority}"
        )


def ready_slices(root: Path, target: str | None, ring: str | None) -> None:
    records = load_slices(root)
    claims = validate_claims(root)
    claimed_sources = {source for item in claims for source in item["_sources"]}
    claimed_module_sources = {
        source for item in claims for source in item["_module_sources"]
    }
    claimed_rust_paths = {path for item in claims for path in item["_rust_paths"]}
    print(
        "slice\tstatus\ttarget\tring\tconsumer\tsource_count\ttest_count\t"
        "test_target\tdepends_on"
    )
    for name, record in sorted(records.items()):
        if not slice_is_ready(record, records):
            continue
        if target is not None and record["target"] != target:
            continue
        if ring is not None and record["ring"] != ring:
            continue
        if set(record["go_sources"]) & claimed_sources:
            continue
        if set(record["module_sources"]) & claimed_module_sources:
            continue
        if set(record["rust_paths"]) & claimed_rust_paths:
            continue
        print(
            f"{name}\t{record['status']}\t{record['target']}\t{record['ring']}\t"
            f"{record['consumer']}\t{len(record['go_sources']) + len(record['module_sources'])}\t"
            f"{len(record['go_tests']) + len(record['module_tests'])}\t{record['test_target']}\t"
            f"{','.join(record['depends_on']) if record['depends_on'] else '-'}"
        )


def claim_slice(root: Path, owner: str, slice_name: str) -> None:
    records = load_slices(root)
    record = records.get(slice_name)
    if record is None:
        raise ValueError(f"unknown vertical slice {slice_name}")
    if not slice_is_ready(record, records):
        unmet = unmet_slice_prerequisites(record, records)
        detail = f"; unmet prerequisites: {'; '.join(unmet)}" if unmet else ""
        raise ValueError(f"vertical slice {slice_name} is not ready{detail}")
    claim(
        root,
        owner,
        list(record["go_sources"]),
        list(record["go_tests"]),
        list(record["module_sources"]),
        list(record["module_tests"]),
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subcommands = result.add_subparsers(dest="command", required=True)
    queue_command = subcommands.add_parser("queue")
    queue_command.add_argument("--target", required=True)
    queue_command.add_argument("--ring", required=True)
    queue_command.add_argument("--limit", type=int)
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
            claims = validate_claims(RUST_ROOT)
            claimed_sources = {source for item in claims for source in item["_sources"]}
            claimed_rust_paths = {
                path for item in claims for path in item["_rust_paths"]
            }
            ready = sum(
                1
                for record in slices.values()
                if slice_is_ready(record, slices)
                and not (set(record["go_sources"]) & claimed_sources)
                and not (
                    set(record["module_sources"])
                    & {source for item in claims for source in item["_module_sources"]}
                )
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
