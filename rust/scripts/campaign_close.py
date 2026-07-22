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

"""Atomically close a package-complete schema-2 rewrite campaign.

The default mode is a read-only preflight. ``--gate`` runs the full shared
integration gate while every package claim remains active. Only the exact gate
receipt can authorize one transaction that marks all package manifests covered,
integrates the campaign, archives its membership, and releases its claims.

Legacy slice promotion and ungated campaign close are deliberately unsupported.
Historical integrated schema-1 campaigns remain readable through the queue.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
from typing import Callable, Iterable


RUST_ROOT = Path(
    os.environ.get("TIDB_REWRITE_RUST_ROOT", Path(__file__).resolve().parents[1])
)
SCRIPT_DIR = Path(__file__).resolve().parent


def _load_queue_module():
    spec = importlib.util.spec_from_file_location(
        "tidb_rewrite_work_unit_queue", SCRIPT_DIR / "work-unit-queue.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load work-unit-queue.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


queue = _load_queue_module()

SOURCE_EVIDENCE = Path("difftests/corpus/coverage/evidence/source")
TEST_EVIDENCE = Path("difftests/corpus/coverage/evidence/tests")
SOURCE_LEDGER = Path("difftests/corpus/coverage/go_source_inventory.tsv")
TEST_LEDGER = Path("difftests/corpus/coverage/go_test_inventory.tsv")
GENERATED_PATHS = (Path("STATUS.md"), SOURCE_LEDGER, TEST_LEDGER)


class ClosePlan:
    """A fully validated package campaign status transaction."""

    def __init__(
        self,
        campaign: str,
        members: tuple[str, ...],
        active_claim_owners: tuple[str, ...],
        claim_digests: tuple[tuple[str, str], ...],
        package_records: dict[str, dict[str, object]],
        writes: dict[Path, str],
    ) -> None:
        self.campaign = campaign
        self.members = members
        self.active_members = members
        self.active_claim_owners = active_claim_owners
        self.claim_digests = claim_digests
        self.package_records = package_records
        self.writes = writes
        self.deletes: set[Path] = set()
        self.transfer_count = 0

    @property
    def touched_paths(self) -> set[Path]:
        return set(self.writes)


def _repository_path(root: Path, artifact: str) -> Path:
    path = Path(artifact)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == "rust":
        return root.parent / path
    return root / path


def _replace_status(path: Path, current: str, replacement: str) -> str:
    text = path.read_text(encoding="utf-8")
    old = f'status = "{current}"'
    if text.count(old) != 1:
        raise ValueError(f"{path}: expected exactly one {old!r}")
    return text.replace(old, f'status = "{replacement}"', 1)


def _archived_members_text(path: Path, campaign: str, members: Iterable[str]) -> str:
    current = path.read_text(encoding="utf-8") if path.exists() else ""
    existing = [
        line.split("\t")
        for line in current.splitlines()
        if line and not line.startswith("#")
    ]
    if any(row[0] == campaign for row in existing if len(row) == 2):
        raise ValueError(f"{path}: campaign {campaign} is already archived")
    suffix = "" if not current or current.endswith("\n") else "\n"
    return current + suffix + "".join(
        f"{campaign}\t{member}\n" for member in members
    )


def _evidence_records(root: Path, kind: str) -> dict[str, list[dict[str, str]]]:
    directory = root / (SOURCE_EVIDENCE if kind == "source" else TEST_EVIDENCE)
    records: dict[str, list[dict[str, str]]] = {}
    for path in sorted(directory.glob("*.tsv")):
        relative_artifact = path.relative_to(root.parent).as_posix()
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            expected = 5 if kind == "source" else 8
            if len(fields) != expected:
                raise ValueError(
                    f"{path}:{number}: {kind} evidence row has {len(fields)} "
                    f"fields, expected {expected}"
                )
            if kind == "source":
                anchor, status, owner, implementation = fields[:4]
            else:
                anchor = f"{fields[1]}:{int(fields[2])}:{fields[3]}"
                status, owner, implementation = fields[4:7]
            records.setdefault(anchor, []).append(
                {
                    "status": status,
                    "owner": owner,
                    "fragment": relative_artifact,
                    "implementation": implementation,
                }
            )
    return records


def _require_covered_evidence(
    root: Path,
    member: str,
    label: str,
    anchors: Iterable[str],
    ledger: dict[str, dict[str, object]],
    fragments: dict[str, list[dict[str, str]]] | None,
) -> None:
    for anchor_value in anchors:
        anchor = str(anchor_value)
        row = ledger[anchor]
        if row["owner"] != member or row["status"] != "COVERED":
            raise ValueError(
                f"package {member} requires {label} {anchor} owned by {member} "
                f"at COVERED; found {row['owner']}@{row['status']}"
            )
        if fragments is None:
            continue
        found = fragments.get(anchor, [])
        if len(found) != 1:
            raise ValueError(
                f"package {member} requires exactly one checked {label} evidence "
                f"row for {anchor}; found {len(found)}"
            )
        evidence = found[0]
        if evidence["owner"] != member or evidence["status"] != "COVERED":
            raise ValueError(
                f"package {member} requires {label} evidence {anchor} owned by "
                f"{member} at COVERED; found "
                f"{evidence['owner']}@{evidence['status']}"
            )
        if row["artifact"] != evidence["fragment"]:
            raise ValueError(
                f"package {member} {label} {anchor} ledger artifact "
                f"{row['artifact']} does not name its exact evidence fragment "
                f"{evidence['fragment']}"
            )
        implementation = str(evidence["implementation"])
        if implementation == "-" or not _repository_path(root, implementation).is_file():
            raise ValueError(
                f"package {member} {label} {anchor} evidence artifact is missing: "
                f"{implementation}"
            )


def _require_current_supports(
    root: Path, member: str, anchors: Iterable[str]
) -> None:
    anchors = list(anchors)
    support_rows = queue.load_support_rows(root)
    for anchor_value in anchors:
        anchor = str(anchor_value)
        row = support_rows[anchor]
        path = root.parent / str(row["path"])
        if not path.is_file():
            raise ValueError(f"package {member} support is missing: {row['path']}")
        current = hashlib.sha256(path.read_bytes()).hexdigest()
        if current != row["sha256"]:
            raise ValueError(
                f"package {member} support digest changed for {row['path']}; "
                f"expected {row['sha256']}, found {current}"
            )
    queue.load_package_support_evidence(root, member, anchors)


def _expected_claim(record: dict[str, object]) -> dict[str, list[str]]:
    return {
        "sources": list(record["go_sources"]),
        "tests": list(record["go_tests"]),
        "supports": list(record["go_supports"]),
        "rust_paths": list(record["rust_paths"]),
        "integration_paths": list(record["integration_paths"]),
        "module_sources": list(record["module_sources"]),
        "module_tests": list(record["module_tests"]),
    }


def _validate_member(
    root: Path,
    member: str,
    record: dict[str, object],
    claim: dict[str, object],
    source_rows: dict[str, dict[str, object]],
    test_rows: dict[str, dict[str, object]],
    source_evidence: dict[str, list[dict[str, str]]],
    test_evidence: dict[str, list[dict[str, str]]],
) -> None:
    if record["schema"] != "2":
        raise ValueError(f"campaign member {member} is not a schema-2 package")
    if record["status"] not in {"ready", "active"}:
        raise ValueError(
            f"campaign package {member} must be ready or active before close; "
            f"found {record['status']}"
        )
    if claim.get("schema") != 2 or claim.get("owner") != member:
        raise ValueError(f"campaign package {member} requires its active schema-2 claim")
    expected = _expected_claim(record)
    for field, values in expected.items():
        actual = claim.get(field)
        if not isinstance(actual, list) or sorted(set(actual)) != values:
            raise ValueError(
                f"campaign package {member} claim differs from its frozen {field}"
            )
    _require_covered_evidence(
        root, member, "source", record["go_sources"], source_rows, source_evidence
    )
    _require_covered_evidence(
        root, member, "test", record["go_tests"], test_rows, test_evidence
    )
    _require_covered_evidence(
        root,
        member,
        "module source",
        record["module_sources"],
        queue.load_module_source_rows(root),
        None,
    )
    _require_covered_evidence(
        root,
        member,
        "module test",
        record["module_tests"],
        queue.load_module_test_rows(root),
        None,
    )
    _require_current_supports(root, member, record["go_supports"])
    for rust_path in record["rust_paths"]:
        if not _repository_path(root, str(rust_path)).is_file():
            raise ValueError(f"package {member} Rust path is missing: {rust_path}")
    for integration_path in record["integration_paths"]:
        if not _repository_path(root, str(integration_path)).is_file():
            raise ValueError(
                f"package {member} integration path is missing: {integration_path}"
            )


def _exact_campaign_claims(
    root: Path, members: Iterable[str]
) -> dict[str, dict[str, object]]:
    """Return the exact active schema-2 claim set for campaign members."""
    members = tuple(members)
    claims = queue.validate_claims(root)
    claim_by_owner = {str(claim["owner"]): claim for claim in claims}
    missing = [member for member in members if member not in claim_by_owner]
    if missing:
        raise ValueError(
            f"campaign package {missing[0]} has no active claim; every member must be active"
        )
    legacy_claims = sorted(
        str(claim["owner"]) for claim in claims if claim.get("schema") != 2
    )
    if legacy_claims:
        raise ValueError(
            "package campaign close requires exact active schema-2 claims; "
            f"found legacy claim {legacy_claims[0]}"
        )
    extra = sorted(set(claim_by_owner) - set(members))
    if extra:
        raise ValueError(
            "package campaign members must exactly match active schema-2 claims; "
            f"active claim {extra[0]} is outside the campaign"
        )
    return claim_by_owner


def build_close_plan(root: Path, campaign_name: str) -> ClosePlan:
    """Preflight complete package evidence and return the status mutation."""
    if not queue.OWNER_RE.fullmatch(campaign_name):
        raise ValueError("invalid campaign name")
    slices = queue.load_slices(root)
    campaigns = queue.load_campaigns(root, slices)
    campaign = campaigns.get(campaign_name)
    if campaign is None:
        raise ValueError(f"unknown campaign {campaign_name}")
    if campaign["schema"] != "2":
        raise ValueError(
            f"campaign {campaign_name} is legacy schema 1 and cannot be closed"
        )
    if campaign["status"] not in {"planned", "active"}:
        raise ValueError(
            f"campaign {campaign_name} must be planned or active before close; "
            f"found {campaign['status']}"
        )

    members = tuple(str(member) for member in campaign["slices"])
    claim_by_owner = _exact_campaign_claims(root, members)

    source_rows = {str(row["path"]): row for row in queue.load_source_rows(root)}
    test_rows = {queue.test_key(row): row for row in queue.load_test_rows(root)}
    source_evidence = _evidence_records(root, "source")
    test_evidence = _evidence_records(root, "test")
    writes: dict[Path, str] = {}
    package_records: dict[str, dict[str, object]] = {}
    for member in members:
        record = slices[member]
        receipt_path = root / queue.PACKAGE_RECEIPTS_DIR / f"{member}.json"
        if receipt_path.exists():
            raise ValueError(
                f"package receipt already exists and cannot be overwritten: {receipt_path}"
            )
        _validate_member(
            root,
            member,
            record,
            claim_by_owner[member],
            source_rows,
            test_rows,
            source_evidence,
            test_evidence,
        )
        manifest_path = Path(record["_path"])
        writes[manifest_path] = _replace_status(
            manifest_path, str(record["status"]), "covered"
        )
        package_records[member] = record

    campaign_path = Path(campaign["_path"])
    writes[campaign_path] = _replace_status(
        campaign_path, str(campaign["status"]), "integrated"
    )
    archive_path = root / queue.INTEGRATED_CAMPAIGN_MEMBERS
    writes[archive_path] = _archived_members_text(
        archive_path, campaign_name, members
    )
    active_owners = tuple(sorted(claim_by_owner))
    digests = tuple(
        (owner, queue.file_digest(Path(claim_by_owner[owner]["_path"])))
        for owner in active_owners
    )
    return ClosePlan(
        campaign_name, members, active_owners, digests, package_records, writes
    )


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _snapshot(paths: Iterable[Path]) -> dict[Path, bytes | None]:
    return {path: path.read_bytes() if path.exists() else None for path in paths}


def _restore(snapshot: dict[Path, bytes | None]) -> None:
    for path, content in snapshot.items():
        if content is None:
            path.unlink(missing_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.rollback")
            temporary.write_bytes(content)
            os.replace(temporary, path)


CommandRunner = Callable[[list[str], Path], None]


def _run(command: list[str], root: Path) -> None:
    subprocess.run(command, cwd=root, check=True)


def _ledger_rows(content: bytes | None, kind: str) -> dict[str, tuple[str, ...]]:
    """Parse generated ledger rows by their stable source/test anchor."""
    if content is None:
        return {}
    rows: dict[str, tuple[str, ...]] = {}
    for number, line in enumerate(content.decode("utf-8").splitlines(), 1):
        if not line or line.startswith("#"):
            continue
        fields = tuple(line.split("\t"))
        minimum = 6 if kind == "source" else 7
        if len(fields) < minimum:
            raise ValueError(f"prepared {kind} ledger row {number} is malformed")
        key = (
            fields[0]
            if kind == "source"
            else f"{fields[1]}:{int(fields[2])}:{fields[3]}"
        )
        if key in rows:
            raise ValueError(f"prepared {kind} ledger duplicates anchor {key}")
        rows[key] = fields
    return rows


def _validate_prepared_ledger_scope(
    before: bytes | None,
    after: bytes | None,
    *,
    kind: str,
    claim_owner_by_anchor: dict[str, str],
) -> int:
    """Reject generated changes outside exact active package claims."""
    old_rows = _ledger_rows(before, kind)
    new_rows = _ledger_rows(after, kind)
    owner_index = 5 if kind == "source" else 6
    changed = 0
    for anchor in sorted(set(old_rows) | set(new_rows)):
        old = old_rows.get(anchor)
        new = new_rows.get(anchor)
        if old == new:
            continue
        changed += 1
        final = new if new is not None else old
        assert final is not None
        owner = final[owner_index]
        if claim_owner_by_anchor.get(anchor) != owner:
            raise ValueError(
                f"package close preparation changed {kind} ledger anchor "
                f"{anchor} outside active campaign claims: owner={owner}"
            )
    return changed


def prepare_close_plan(
    root: Path,
    campaign_name: str,
    *,
    command_runner: CommandRunner = _run,
) -> ClosePlan:
    """Refresh derived package rows, reject unrelated churn, then preflight."""
    slices = queue.load_slices(root)
    campaign = queue.load_campaigns(root, slices).get(campaign_name)
    if campaign is None:
        raise ValueError(f"unknown campaign {campaign_name}")
    members = tuple(str(member) for member in campaign["slices"])
    claims = _exact_campaign_claims(root, members)
    source_claim_owners = {
        str(anchor): member
        for member in members
        for anchor in claims[member]["_sources"]
    }
    test_claim_owners = {
        str(anchor): member
        for member in members
        for anchor in claims[member]["tests"]
    }
    paths = {root / path for path in GENERATED_PATHS}
    before = _snapshot(paths)
    try:
        command_runner(["scripts/rewrite-gate.sh", "prepare"], root)
        source_changes = _validate_prepared_ledger_scope(
            before[root / SOURCE_LEDGER],
            (root / SOURCE_LEDGER).read_bytes(),
            kind="source",
            claim_owner_by_anchor=source_claim_owners,
        )
        test_changes = _validate_prepared_ledger_scope(
            before[root / TEST_LEDGER],
            (root / TEST_LEDGER).read_bytes(),
            kind="test",
            claim_owner_by_anchor=test_claim_owners,
        )
        plan = build_close_plan(root, campaign_name)
    except BaseException:
        _restore(before)
        raise
    print(
        f"campaign_prepare\t{campaign_name}\t"
        f"source_rows={source_changes}\ttest_rows={test_changes}"
    )
    return plan


def _same_plan(left: ClosePlan, right: ClosePlan) -> bool:
    return (
        left.members == right.members
        and left.active_claim_owners == right.active_claim_owners
        and left.claim_digests == right.claim_digests
        and left.package_records.keys() == right.package_records.keys()
        and left.writes == right.writes
    )


def _validate_receipt(root: Path, plan: ClosePlan) -> None:
    receipt = queue.load_integration_state(root, queue.INTEGRATION_RECEIPT)
    if tuple(sorted(receipt["claims"])) != plan.active_claim_owners:
        raise ValueError(
            "gate receipt claims differ from the exact active schema-2 claim set"
        )
    expected_digests = dict(plan.claim_digests)
    for owner, entry in receipt["claims"].items():
        if not isinstance(entry, dict) or entry.get("claim_sha256") != expected_digests[owner]:
            raise ValueError(f"gate receipt claim digest differs for {owner}")
    if receipt.get("workspace_sha256") != queue.release_workspace_digest(root):
        raise ValueError("gate receipt workspace digest differs from the exact workspace")
    if receipt.get("slice_manifests_sha256") != queue.integration_slice_manifest_digest(root):
        raise ValueError("gate receipt package manifest digest differs from the workspace")


def apply_close_plan(
    root: Path,
    plan: ClosePlan,
    *,
    command_runner: CommandRunner = _run,
    validate: bool = True,
) -> None:
    """Gate, integrate, and release every package as one rollback transaction."""
    paths = plan.touched_paths | {root / path for path in GENERATED_PATHS}
    paths |= {
        root / queue.CLAIMS_DIR / f"{owner}.claim.json"
        for owner in plan.active_claim_owners
    }
    paths |= {
        root / queue.PACKAGE_RECEIPTS_DIR / f"{member}.json"
        for member in plan.members
    }
    paths |= {
        root / queue.INTEGRATION_ATTEMPT,
        root / queue.INTEGRATION_RECEIPT,
    }
    before = _snapshot(paths)
    try:
        command_runner(["scripts/rewrite-gate.sh", "integrate"], root)
        with queue.claim_lock(root):
            current = build_close_plan(root, plan.campaign)
            if not _same_plan(current, plan):
                raise ValueError("package campaign inputs changed after preflight")
            _validate_receipt(root, plan)
            claims = {
                str(claim["owner"]): claim for claim in queue.validate_claims(root)
            }
            for path, content in plan.writes.items():
                _atomic_write(path, content)
            gate_receipt = queue.load_integration_state(
                root, queue.INTEGRATION_RECEIPT
            )
            for member in plan.members:
                receipt_path = (
                    root / queue.PACKAGE_RECEIPTS_DIR / f"{member}.json"
                )
                if receipt_path.exists():
                    raise ValueError(
                        "package receipt appeared after preflight and cannot be "
                        f"overwritten: {receipt_path}"
                    )
                queue.atomic_write_json(
                    receipt_path,
                    queue.package_receipt_payload(
                        root,
                        member,
                        plan.campaign,
                        plan.package_records[member],
                        gate_receipt,
                    ),
                )
            if validate:
                queue.validate_claims(root)
            for member in plan.members:
                claim = claims[member]
                queue.consume_integration_receipt(root, claim)
                Path(claim["_path"]).unlink()
            if validate:
                queue.validate_claims(root)
        command_runner(
            [sys.executable, "scripts/status-dashboard.py", "--write"], root
        )
    except BaseException:
        _restore(before)
        raise


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--campaign", required=True)
    result.add_argument(
        "--gate",
        action="store_true",
        help="run the full shared gate and atomically close the package campaign",
    )
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.gate:
            plan = prepare_close_plan(RUST_ROOT, arguments.campaign)
            apply_close_plan(RUST_ROOT, plan)
            action = "gated-and-integrated"
        else:
            plan = build_close_plan(RUST_ROOT, arguments.campaign)
            action = "dry-run"
        print(
            f"campaign_close\t{action}\t{plan.campaign}\t"
            f"members={len(plan.members)}\t"
            f"active_claims={len(plan.active_claim_owners)}\t"
            f"writes={len(plan.writes)}"
        )
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
