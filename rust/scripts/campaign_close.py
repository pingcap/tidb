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

"""Promote campaign members and close campaigns without manual bookkeeping.

The default mode is a read-only preflight. ``--promote-member`` selects one
completed member whose exact evidence and slice status should advance to
PARTIAL while its claim remains active; add ``--apply`` to commit that
transaction. Without ``--promote-member``, ``--apply`` closes the campaign's
bookkeeping. Covered members that were receipt-released earlier may be
inactive. ``--gate`` additionally runs one shared integration gate over the
exact current active claim set, then receipt-releases only this campaign's
active members while preserving unrelated claims and their receipt entries.
"""

from __future__ import annotations

import argparse
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
TEST_DOMAIN_MANIFEST = Path(
    "difftests/corpus/coverage/go_test_domain_manifest.tsv"
)
GENERATED_PATHS = (
    Path("difftests/corpus/coverage/go_source_inventory.tsv"),
    Path("difftests/corpus/coverage/go_test_declaration_inventory.tsv"),
    Path("difftests/corpus/coverage/go_test_fixture_access_inventory.tsv"),
    Path("difftests/corpus/coverage/go_test_inventory.tsv"),
    Path("STATUS.md"),
)


class ClosePlan:
    """Fully validated, in-memory campaign bookkeeping mutation."""

    def __init__(
        self,
        campaign: str,
        members: tuple[str, ...],
        active_members: tuple[str, ...] | None,
        writes: dict[Path, str],
        deletes: set[Path],
        transfer_count: int,
    ) -> None:
        self.campaign = campaign
        self.members = members
        self.active_members = members if active_members is None else active_members
        self.writes = writes
        self.deletes = deletes
        self.transfer_count = transfer_count

    @property
    def touched_paths(self) -> set[Path]:
        return set(self.writes) | self.deletes


class MemberPromotionPlan(ClosePlan):
    """One campaign member's checked evidence/status promotion."""

    def __init__(
        self,
        campaign: str,
        member: str,
        writes: dict[Path, str],
        deletes: set[Path],
        transfer_count: int,
    ) -> None:
        super().__init__(campaign, (member,), None, writes, deletes, transfer_count)
        self.member = member


def _repository_path(root: Path, artifact: str) -> Path:
    path = Path(artifact)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == "rust":
        return root.parent / path
    return root / path


def _split_artifacts(field: str) -> tuple[str, ...]:
    # '-' means that the transfer has no artifact to retire. It is not a path.
    return () if field == "-" else tuple(field.split(","))


def _transfer_anchor(row: list[str], kind: str) -> str | None:
    if kind == "source":
        return None if row[2] == "-" else row[2]
    if (row[3], row[4], row[5]) == ("-", "-", "-"):
        return None
    return f"{row[3]}:{int(row[4])}:{row[5]}"


def _fragment_kind(root: Path, path: Path) -> str | None:
    try:
        relative = path.relative_to(root)
    except ValueError:
        return None
    if relative.is_relative_to(SOURCE_EVIDENCE):
        return "source"
    if relative.is_relative_to(TEST_EVIDENCE):
        return "test"
    return None


def _row_anchor(fields: list[str], kind: str) -> str:
    if kind == "source":
        if len(fields) != 5:
            raise ValueError(f"source evidence row has {len(fields)} fields, expected 5")
        return fields[0]
    if len(fields) != 8:
        raise ValueError(f"test evidence row has {len(fields)} fields, expected 8")
    return f"{fields[1]}:{int(fields[2])}:{fields[3]}"


def _remove_fragment_rows(
    path: Path, kind: str, anchors: set[str]
) -> tuple[str | None, set[str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    retained: list[str] = []
    removed: set[str] = set()
    data_rows = 0
    for number, line in enumerate(lines, start=1):
        if not line or line.startswith("#"):
            retained.append(line)
            continue
        fields = line.split("\t")
        try:
            anchor = _row_anchor(fields, kind)
        except (ValueError, IndexError) as error:
            raise ValueError(f"{path}:{number}: {error}") from error
        if anchor in anchors:
            if anchor in removed:
                raise ValueError(f"{path}:{number}: duplicate retired evidence {anchor}")
            removed.add(anchor)
            continue
        retained.append(line)
        data_rows += 1
    if data_rows == 0:
        return None, removed
    return "\n".join(retained) + "\n", removed


def _render_transfer_retirements(
    path: Path, retained_artifacts: list[tuple[str, ...]]
) -> str:
    """Render transfer rows with only artifacts absent after the transaction."""
    rendered: list[str] = []
    data_index = 0
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line or line.startswith("#"):
            rendered.append(line)
            continue
        fields = line.split("\t")
        if len(fields) != 9:
            raise ValueError(f"{path}:{number}: expected 9 fields, got {len(fields)}")
        if data_index >= len(retained_artifacts):
            raise ValueError(f"{path}:{number}: transfer row count changed during preflight")
        artifacts = retained_artifacts[data_index]
        fields[6] = ",".join(artifacts) if artifacts else "-"
        rendered.append("\t".join(fields))
        data_index += 1
    if data_index != len(retained_artifacts):
        raise ValueError(f"{path}: transfer row count changed during preflight")
    return "\n".join(rendered) + "\n"


def _evidence_records(
    root: Path, planned_writes: dict[Path, str], planned_deletes: set[Path], kind: str
) -> dict[str, list[tuple[str, str]]]:
    directory = root / (SOURCE_EVIDENCE if kind == "source" else TEST_EVIDENCE)
    records: dict[str, list[tuple[str, str]]] = {}
    for path in sorted(directory.glob("*.tsv")):
        if path in planned_deletes:
            continue
        text = planned_writes.get(path, path.read_text(encoding="utf-8"))
        for number, line in enumerate(text.splitlines(), start=1):
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            try:
                anchor = _row_anchor(fields, kind)
            except (ValueError, IndexError) as error:
                raise ValueError(f"{path}:{number}: {error}") from error
            status_index = 1 if kind == "source" else 4
            owner_index = 2 if kind == "source" else 5
            records.setdefault(anchor, []).append(
                (fields[status_index], fields[owner_index])
            )
    return records


def _evidence_owners(
    root: Path, planned_writes: dict[Path, str], planned_deletes: set[Path], kind: str
) -> dict[str, list[str]]:
    return {
        anchor: [owner for _status, owner in records]
        for anchor, records in _evidence_records(
            root, planned_writes, planned_deletes, kind
        ).items()
    }


def _integrated_campaign_text(path: Path, current_status: str) -> str:
    text = path.read_text(encoding="utf-8")
    old = f'status = "{current_status}"'
    if text.count(old) != 1:
        raise ValueError(f"{path}: expected exactly one {old!r}")
    return text.replace(old, 'status = "integrated"', 1)


def _partial_slice_text(path: Path, current_status: str) -> str:
    text = path.read_text(encoding="utf-8")
    old = f'status = "{current_status}"'
    if text.count(old) != 1:
        raise ValueError(f"{path}: expected exactly one {old!r}")
    return text.replace(old, 'status = "partial"', 1)


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
    return current + suffix + "".join(f"{campaign}\t{member}\n" for member in members)


def _validate_transfer_topology(
    root: Path, affected: dict[tuple[str, str], str]
) -> None:
    """Prove affected ownership histories remain single connected chains."""
    chains: dict[tuple[str, str], list[tuple[str, str, Path]]] = {
        key: [] for key in affected
    }
    for path in sorted((root / queue.TRANSFERS_DIR).glob("*.tsv")):
        for row in queue.read_tsv(path, 9):
            for kind in ("source", "test"):
                anchor = _transfer_anchor(row, kind)
                key = (kind, anchor) if anchor is not None else None
                if key in chains:
                    chains[key].append((row[0], row[1], path))
    for (kind, anchor), records in chains.items():
        outgoing: dict[str, tuple[str, Path]] = {}
        incoming: dict[str, tuple[str, Path]] = {}
        for old_owner, new_owner, path in records:
            if old_owner in outgoing:
                raise ValueError(
                    f"{path}: branched {kind} ownership transfer for {anchor} "
                    f"from {old_owner}"
                )
            if new_owner in incoming:
                raise ValueError(
                    f"{path}: merged {kind} ownership transfer for {anchor} "
                    f"into {new_owner}"
                )
            outgoing[old_owner] = (new_owner, path)
            incoming[new_owner] = (old_owner, path)
        starts = [owner for owner in outgoing if owner not in incoming]
        if len(starts) != 1:
            raise ValueError(
                f"ownership transfers for {kind} {anchor} must form one acyclic chain"
            )
        owner = starts[0]
        visited = 0
        while owner in outgoing:
            owner = outgoing[owner][0]
            visited += 1
        if visited != len(records):
            raise ValueError(
                f"ownership transfers for {kind} {anchor} must form one connected chain"
            )
        if owner != affected[(kind, anchor)]:
            raise ValueError(
                f"ownership transfers for {kind} {anchor} terminate at {owner}, "
                f"not campaign owner {affected[(kind, anchor)]}"
            )


def _build_evidence_transition(
    root: Path,
    transfer_path: Path,
    transfer_rows: list[list[str]],
    slices: dict[str, dict[str, object]],
    eligible_members: set[str],
    terminal_members: set[str],
) -> tuple[dict[Path, str], set[Path], list[tuple[str, str, str]]]:
    """Build one checked ownership mutation for the eligible terminal owners.

    A campaign transfer file may contain rows for members already promoted and
    for the member being promoted now. Rows for a future member are rejected:
    publishing that transfer early would make the global transfer ledger claim
    an ownership change which the generated inventories do not yet contain.
    """
    removals: dict[Path, set[str]] = {}
    terminal_anchors: list[tuple[str, str, str]] = []
    # A successor artifact can be retired by a later campaign while this
    # historical campaign is still open. Accept that absence only when the
    # artifact is declared retired somewhere in the global append-only
    # transfer history; transfer-topology validation below still proves the
    # anchor itself reaches the current generated owner.
    all_retired_artifacts = {
        artifact
        for path in sorted((root / queue.TRANSFERS_DIR).glob("*.tsv"))
        for row in queue.read_tsv(path, 9)
        for artifact in _split_artifacts(row[6])
    }
    for row in transfer_rows:
        old_owner, new_owner = row[0], row[1]
        if not queue.OWNER_RE.fullmatch(old_owner) or not queue.OWNER_RE.fullmatch(new_owner):
            raise ValueError(f"{transfer_path}: invalid transfer owner")
        if old_owner == new_owner:
            raise ValueError(f"{transfer_path}: transfer owners must differ")
        if new_owner not in eligible_members:
            raise ValueError(
                f"{transfer_path}: transfer terminal {new_owner} has not been "
                "selected or previously promoted"
            )
        source_anchor = _transfer_anchor(row, "source")
        test_anchor = _transfer_anchor(row, "test")
        if source_anchor is None and test_anchor is None:
            raise ValueError(f"{transfer_path}: transfer must contain an anchor")
        if (
            source_anchor is not None
            and source_anchor not in slices[new_owner]["go_sources"]
        ):
            raise ValueError(
                f"{transfer_path}: source {source_anchor} is not frozen in {new_owner}"
            )
        if (
            test_anchor is not None
            and test_anchor not in slices[new_owner]["go_tests"]
        ):
            raise ValueError(
                f"{transfer_path}: test {test_anchor} is not frozen in {new_owner}"
            )
        for kind, anchor in (("source", source_anchor), ("test", test_anchor)):
            if anchor is not None and new_owner in terminal_members:
                terminal_anchors.append((kind, anchor, new_owner))
        for artifact in _split_artifacts(row[6]):
            path = _repository_path(root, artifact)
            kind = _fragment_kind(root, path)
            if kind is None:
                if path.exists():
                    raise ValueError(
                        f"{transfer_path}: non-evidence retired artifact still exists: {artifact}"
                    )
                continue
            anchor = source_anchor if kind == "source" else test_anchor
            if anchor is None:
                raise ValueError(
                    f"{transfer_path}: {artifact} has no matching {kind} anchor"
                )
            # A prior member promotion may already have deleted this exact
            # fragment. Its continued absence is the transfer assertion; the
            # terminal evidence and topology checks below still prove the move.
            if path.is_file():
                removals.setdefault(path, set()).add(anchor)
        for artifact in _split_artifacts(row[7]):
            path = _repository_path(root, artifact)
            if not path.is_file() and artifact not in all_retired_artifacts:
                raise ValueError(f"{transfer_path}: replacement artifact is missing: {artifact}")

    if terminal_anchors:
        _validate_transfer_topology(
            root, {(kind, anchor): owner for kind, anchor, owner in terminal_anchors}
        )

    writes: dict[Path, str] = {}
    deletes: set[Path] = set()
    for path, anchors in removals.items():
        kind = _fragment_kind(root, path)
        assert kind is not None
        rendered, removed = _remove_fragment_rows(path, kind, anchors)
        missing = sorted(anchors - removed)
        if missing:
            raise ValueError(f"{path}: retired evidence row is missing: {missing[0]}")
        if rendered is None:
            deletes.add(path)
        else:
            writes[path] = rendered

    retained_retirements: list[tuple[str, ...]] = []
    for row in transfer_rows:
        retained = []
        for artifact in _split_artifacts(row[6]):
            path = _repository_path(root, artifact)
            if path in deletes or not path.exists():
                retained.append(artifact)
        retained_retirements.append(tuple(retained))
    if transfer_rows:
        writes[transfer_path] = _render_transfer_retirements(
            transfer_path, retained_retirements
        )

    for kind in ("source", "test"):
        owners = _evidence_owners(root, writes, deletes, kind)
        for anchor, found in sorted(owners.items()):
            if len(found) > 1 and any(owner in eligible_members for owner in found):
                raise ValueError(
                    f"campaign post-close {kind} evidence {anchor} has duplicate "
                    f"owners {found}"
                )
        for anchor_kind, anchor, new_owner in terminal_anchors:
            if anchor_kind != kind:
                continue
            found = owners.get(anchor, [])
            if found != [new_owner]:
                raise ValueError(
                    f"campaign transfer terminal {kind} evidence {anchor} must have "
                    f"exact owner {new_owner}; found {found}"
                )
    return writes, deletes, terminal_anchors


def build_close_plan(root: Path, campaign_name: str) -> ClosePlan:
    """Preflight every input and return an in-memory mutation plan."""
    if not queue.OWNER_RE.fullmatch(campaign_name):
        raise ValueError("invalid campaign name")
    slices = queue.load_slices(root)
    campaigns = queue.load_campaigns(root, slices)
    campaign = campaigns.get(campaign_name)
    if campaign is None:
        raise ValueError(f"unknown campaign {campaign_name}")
    if campaign["status"] not in {"planned", "active"}:
        raise ValueError(
            f"campaign {campaign_name} must be planned or active before close; "
            f"found {campaign['status']}"
        )
    members = tuple(str(member) for member in campaign["slices"])
    claims = {str(claim["owner"]): claim for claim in queue.load_claims(root)}
    active_members = tuple(member for member in members if member in claims)
    for member in members:
        record = slices[member]
        if record["status"] not in {"partial", "covered"}:
            raise ValueError(
                f"campaign member {member} must be partial or covered before close; "
                f"found {record['status']}"
            )
        claim = claims.get(member)
        if claim is None:
            if record["status"] != "covered":
                raise ValueError(
                    f"inactive campaign member {member} must already be covered; "
                    f"found {record['status']}"
                )
            continue
        if claim.get("schema") != 2:
            raise ValueError(f"campaign member {member} requires a schema-2 slice claim")
        expected = (
            list(record["go_sources"]),
            list(record["go_tests"]),
            list(record["module_sources"]),
            list(record["module_tests"]),
        )
        actual = (
            sorted(set(claim.get("sources", []))),
            sorted(set(claim.get("tests", []))),
            sorted(set(claim.get("module_sources", []))),
            sorted(set(claim.get("module_tests", []))),
        )
        if actual != expected:
            raise ValueError(f"campaign member {member} claim differs from its frozen slice")

    transfer_path = root / queue.TRANSFERS_DIR / f"{campaign_name}.tsv"
    if not transfer_path.is_file():
        raise ValueError(f"campaign transfer file is missing: {transfer_path}")
    transfer_rows = queue.read_tsv(transfer_path, 9)
    if not transfer_rows:
        raise ValueError(f"campaign transfer file is empty: {transfer_path}")

    writes, deletes, _terminal_anchors = _build_evidence_transition(
        root, transfer_path, transfer_rows, slices, set(members), set(members)
    )

    campaign_path = Path(campaign["_path"])
    archive_path = root / queue.INTEGRATED_CAMPAIGN_MEMBERS
    writes[campaign_path] = _integrated_campaign_text(
        campaign_path, str(campaign["status"])
    )
    writes[archive_path] = _archived_members_text(
        archive_path, campaign_name, members
    )
    return ClosePlan(
        campaign_name,
        members,
        active_members,
        writes,
        deletes,
        len(transfer_rows),
    )


def _validate_exact_member_evidence(
    root: Path,
    member: str,
    record: dict[str, object],
    writes: dict[Path, str],
    deletes: set[Path],
) -> None:
    for kind, anchors in (
        ("source", record["go_sources"]),
        ("test", record["go_tests"]),
    ):
        evidence = _evidence_records(root, writes, deletes, kind)
        for anchor in anchors:
            found = evidence.get(str(anchor), [])
            if len(found) != 1:
                raise ValueError(
                    f"campaign member {member} requires exact {kind} evidence "
                    f"{anchor}; found {found}"
                )
            status, owner = found[0]
            if (
                owner != member
                or queue.EVIDENCE_STATUS_RANK.get(status, -1)
                < queue.EVIDENCE_STATUS_RANK["PARTIAL"]
            ):
                raise ValueError(
                    f"campaign member {member} requires {kind} evidence {anchor} "
                    f"owned by {member} at PARTIAL or better; found {owner}@{status}"
                )

    for kind, anchors, evidence in (
        (
            "module source",
            record["module_sources"],
            queue.load_module_source_rows(root),
        ),
        (
            "module test",
            record["module_tests"],
            queue.load_module_test_rows(root),
        ),
    ):
        for anchor in anchors:
            found = evidence[str(anchor)]
            if (
                found["owner"] != member
                or queue.EVIDENCE_STATUS_RANK.get(found["status"], -1)
                < queue.EVIDENCE_STATUS_RANK["PARTIAL"]
            ):
                raise ValueError(
                    f"campaign member {member} requires {kind} evidence {anchor} "
                    f"owned by {member} at PARTIAL or better; found "
                    f"{found['owner']}@{found['status']}"
                )


def _member_has_promoted_ledger_evidence(
    root: Path, member: str, record: dict[str, object]
) -> bool:
    source_rows = {str(row["path"]): row for row in queue.load_source_rows(root)}
    test_rows = {
        queue.test_key(row): row for row in queue.load_test_rows(root)
    }
    module_source_rows = queue.load_module_source_rows(root)
    module_test_rows = queue.load_module_test_rows(root)
    for anchors, evidence in (
        (record["go_sources"], source_rows),
        (record["go_tests"], test_rows),
        (record["module_sources"], module_source_rows),
        (record["module_tests"], module_test_rows),
    ):
        for anchor in anchors:
            found = evidence[str(anchor)]
            if (
                found["owner"] != member
                or queue.EVIDENCE_STATUS_RANK.get(found["status"], -1)
                < queue.EVIDENCE_STATUS_RANK["PARTIAL"]
            ):
                return False
    return True


def _promoted_test_domain_manifest_text(
    root: Path, member: str, test_anchors: Iterable[str]
) -> str | None:
    """Add missing exact rows only for Go test files already split.

    Existing rows are immutable partition history. New rows are appended in
    deterministic anchor order and use the promoted member as their domain.
    """
    path = root / TEST_DOMAIN_MANIFEST
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8")
    existing: set[tuple[str, int, str]] = set()
    split_paths: set[str] = set()
    for number, line in enumerate(text.splitlines(), start=1):
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != 4:
            raise ValueError(
                f"{path}:{number}: expected 4 fields, got {len(fields)}"
            )
        try:
            anchor = (fields[0], int(fields[1]), fields[2])
        except ValueError as error:
            raise ValueError(
                f"{path}:{number}: invalid test line {fields[1]!r}"
            ) from error
        if anchor in existing:
            raise ValueError(
                f"{path}:{number}: duplicate test-domain anchor "
                f"{anchor[0]}:{anchor[1]}:{anchor[2]}"
            )
        existing.add(anchor)
        split_paths.add(anchor[0])

    top_level_test_anchors = {
        queue.test_key(row)
        for row in queue.load_test_rows(root)
        if row["kind"] == "go_test"
    }
    promoted = []
    for value in test_anchors:
        if str(value) not in top_level_test_anchors:
            continue
        test_path, line, name = str(value).rsplit(":", 2)
        anchor = (test_path, int(line), name)
        if test_path in split_paths and anchor not in existing:
            promoted.append(anchor)
    if not promoted:
        return None
    promoted.sort(key=lambda anchor: (anchor[0], anchor[1], anchor[2]))
    suffix = "" if not text or text.endswith("\n") else "\n"
    return text + suffix + "".join(
        f"{test_path}\t{line}\t{name}\t{member}\n"
        for test_path, line, name in promoted
    )


def build_member_promotion_plan(
    root: Path, campaign_name: str, member: str
) -> MemberPromotionPlan:
    """Preflight one member's PARTIAL promotion without releasing its claim."""
    if not queue.OWNER_RE.fullmatch(campaign_name):
        raise ValueError("invalid campaign name")
    if not queue.OWNER_RE.fullmatch(member):
        raise ValueError("invalid campaign member")
    slices = queue.load_slices(root)
    campaigns = queue.load_campaigns(root, slices)
    campaign = campaigns.get(campaign_name)
    if campaign is None:
        raise ValueError(f"unknown campaign {campaign_name}")
    if campaign["status"] not in {"planned", "active"}:
        raise ValueError(
            f"campaign {campaign_name} must be planned or active before member promotion; "
            f"found {campaign['status']}"
        )
    campaign_members = tuple(str(item) for item in campaign["slices"])
    if member not in campaign_members:
        raise ValueError(f"{member} is not a member of campaign {campaign_name}")
    record = slices[member]
    if record["status"] not in {"ready", "active"}:
        raise ValueError(
            f"campaign member {member} must be ready or active before promotion; "
            f"found {record['status']}"
        )

    claims = {str(claim["owner"]): claim for claim in queue.load_claims(root)}
    claim = claims.get(member)
    if claim is None:
        raise ValueError(f"campaign member {member} has no active claim")
    if claim.get("schema") != 2:
        raise ValueError(f"campaign member {member} requires a schema-2 slice claim")
    expected = (
        list(record["go_sources"]),
        list(record["go_tests"]),
        list(record["module_sources"]),
        list(record["module_tests"]),
    )
    actual = (
        sorted(set(claim.get("sources", []))),
        sorted(set(claim.get("tests", []))),
        sorted(set(claim.get("module_sources", []))),
        sorted(set(claim.get("module_tests", []))),
    )
    if actual != expected:
        raise ValueError(f"campaign member {member} claim differs from its frozen slice")

    # A previously promoted member may have been superseded by a later
    # campaign before this historical campaign closes. Its checked PARTIAL
    # status is still sufficient to make the older transfer terminal
    # available: `validate_transfers` below proves the full ownership chain
    # reaches the current generated ledger owner. Requiring the predecessor
    # to remain the terminal ledger owner would strand every unfinished
    # campaign after a valid cross-campaign successor transfer.
    eligible_members = {
        item
        for item in campaign_members
        if item == member or slices[item]["status"] in {"partial", "covered"}
    }
    transfer_path = root / queue.TRANSFERS_DIR / f"{campaign_name}.tsv"
    transfer_rows = queue.read_tsv(transfer_path, 9) if transfer_path.is_file() else []
    writes, deletes, _terminal_anchors = _build_evidence_transition(
        root, transfer_path, transfer_rows, slices, eligible_members, {member}
    )
    _validate_exact_member_evidence(root, member, record, writes, deletes)
    domain_manifest = _promoted_test_domain_manifest_text(
        root, member, record["go_tests"]
    )
    if domain_manifest is not None:
        writes[root / TEST_DOMAIN_MANIFEST] = domain_manifest
    slice_path = Path(record["_path"])
    writes[slice_path] = _partial_slice_text(slice_path, str(record["status"]))
    return MemberPromotionPlan(
        campaign_name, member, writes, deletes, len(transfer_rows)
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


def _apply_bookkeeping_plan(
    root: Path,
    plan: ClosePlan,
    rebuild: Callable[[], ClosePlan],
    command_runner: CommandRunner,
    validate: bool,
) -> None:
    """Apply and regenerate one plan while holding the shared claim lock."""
    with queue.claim_lock(root):
        current = rebuild()
        if (
            current.members != plan.members
            or current.active_members != plan.active_members
            or current.writes != plan.writes
            or current.deletes != plan.deletes
            or current.transfer_count != plan.transfer_count
        ):
            raise ValueError("campaign bookkeeping inputs changed after preflight")
        for path, content in plan.writes.items():
            _atomic_write(path, content)
        for path in plan.deletes:
            path.unlink()
        command_runner(
            [
                "cargo", "run", "--offline", "--locked", "-j12", "-p", "difftest",
                "--bin", "go_source_ledger", "--", "--write",
            ],
            root,
        )
        command_runner(
            [
                "cargo", "run", "--offline", "--locked", "-j12", "-p", "difftest",
                "--bin", "go_test_ledger", "--", "--write-evidence",
            ],
            root,
        )
        command_runner(
            [sys.executable, "scripts/status-dashboard.py", "--write"], root
        )
        if validate:
            queue.validate_claims(root)


def apply_close_plan(
    root: Path,
    plan: ClosePlan,
    *,
    run_gate: bool = False,
    command_runner: CommandRunner = _run,
    validate: bool = True,
) -> None:
    """Apply a preflighted plan and roll back every known mutation on failure."""
    campaign_path = root / queue.CAMPAIGNS_DIR / f"{plan.campaign}.toml"
    paths = plan.touched_paths | {root / path for path in GENERATED_PATHS}
    paths |= {
        root / queue.CLAIMS_DIR / f"{member}.claim.json"
        for member in plan.active_members
    }
    paths |= {
        root / queue.INTEGRATION_ATTEMPT,
        root / queue.INTEGRATION_RECEIPT,
        campaign_path,
    }
    before = _snapshot(paths)
    try:
        # Share the queue's lock for the complete bookkeeping transaction so a
        # claim cannot change between preflight and generated-ledger validation.
        _apply_bookkeeping_plan(
            root,
            plan,
            lambda: build_close_plan(root, plan.campaign),
            command_runner,
            validate,
        )
        if run_gate:
            command_runner(["scripts/rewrite-gate.sh", "integrate"], root)
            receipt = queue.load_integration_state(root, queue.INTEGRATION_RECEIPT)
            active_claims = {
                str(claim["owner"]) for claim in queue.load_claims(root)
            }
            if set(receipt["claims"]) != active_claims:
                raise ValueError(
                    "shared gate receipt claims differ from the exact active claim set"
                )
            if not set(plan.active_members).issubset(active_claims):
                raise ValueError(
                    "shared gate receipt is missing an active campaign member"
                )
            for member in plan.active_members:
                queue.release(root, member, integrated=True, abandon=False)
            # Receipt consumption changes the dashboard's active-claim count.
            # Regenerate only after the final member release so the shared
            # receipt remains immutable while it is consumed member-by-member.
            command_runner(
                [sys.executable, "scripts/status-dashboard.py", "--write"], root
            )
    except BaseException:
        _restore(before)
        raise


def apply_member_promotion_plan(
    root: Path,
    plan: MemberPromotionPlan,
    *,
    command_runner: CommandRunner = _run,
    validate: bool = True,
) -> None:
    """Promote one member atomically while preserving every active claim."""
    paths = plan.touched_paths | {root / path for path in GENERATED_PATHS}
    claim_path = root / queue.CLAIMS_DIR / f"{plan.member}.claim.json"
    paths.add(claim_path)
    before = _snapshot(paths)
    try:
        _apply_bookkeeping_plan(
            root,
            plan,
            lambda: build_member_promotion_plan(
                root, plan.campaign, plan.member
            ),
            command_runner,
            validate,
        )
    except BaseException:
        _restore(before)
        raise


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--campaign", required=True)
    result.add_argument(
        "--promote-member",
        help="preflight one member's exact PARTIAL evidence/status promotion",
    )
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument(
        "--gate",
        action="store_true",
        help="apply bookkeeping, run one shared gate, then receipt-release all members",
    )
    return result


def main() -> int:
    arguments = parser().parse_args()
    try:
        if arguments.promote_member is not None:
            if arguments.gate:
                raise ValueError(
                    "--promote-member cannot run the shared gate; close the whole "
                    "campaign with --gate"
                )
            plan = build_member_promotion_plan(
                RUST_ROOT, arguments.campaign, arguments.promote_member
            )
            if arguments.apply:
                apply_member_promotion_plan(RUST_ROOT, plan)
                action = "member-promoted"
            else:
                action = "member-promotion-dry-run"
        else:
            plan = build_close_plan(RUST_ROOT, arguments.campaign)
            if arguments.apply or arguments.gate:
                apply_close_plan(RUST_ROOT, plan, run_gate=arguments.gate)
                action = "gated-and-released" if arguments.gate else "applied"
            else:
                action = "dry-run"
        print(
            f"campaign_close\t{action}\t{plan.campaign}\t"
            f"members={len(plan.members)}\tactive_members={len(plan.active_members)}\t"
            f"transfers={plan.transfer_count}\t"
            f"writes={len(plan.writes)}\tdeletes={len(plan.deletes)}"
        )
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
