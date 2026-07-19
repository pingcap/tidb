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

"""Close one rewrite campaign without hand-editing ownership bookkeeping.

The default mode is a read-only preflight. ``--apply`` promotes evidence,
regenerates the two Go inventories and STATUS.md, and validates the resulting
queue. ``--gate`` additionally runs the one shared integration gate and then
releases every campaign member using that gate's receipt.
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
        writes: dict[Path, str],
        deletes: set[Path],
        transfer_count: int,
    ) -> None:
        self.campaign = campaign
        self.members = members
        self.writes = writes
        self.deletes = deletes
        self.transfer_count = transfer_count

    @property
    def touched_paths(self) -> set[Path]:
        return set(self.writes) | self.deletes


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


def _evidence_owners(
    root: Path, planned_writes: dict[Path, str], planned_deletes: set[Path], kind: str
) -> dict[str, list[str]]:
    directory = root / (SOURCE_EVIDENCE if kind == "source" else TEST_EVIDENCE)
    owners: dict[str, list[str]] = {}
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
            owner_index = 2 if kind == "source" else 5
            owners.setdefault(anchor, []).append(fields[owner_index])
    return owners


def _integrated_campaign_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    old = 'status = "active"'
    if text.count(old) != 1:
        raise ValueError(f"{path}: expected exactly one {old!r}")
    return text.replace(old, 'status = "integrated"', 1)


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


def build_close_plan(root: Path, campaign_name: str) -> ClosePlan:
    """Preflight every input and return an in-memory mutation plan."""
    if not queue.OWNER_RE.fullmatch(campaign_name):
        raise ValueError("invalid campaign name")
    slices = queue.load_slices(root)
    campaigns = queue.load_campaigns(root, slices)
    campaign = campaigns.get(campaign_name)
    if campaign is None:
        raise ValueError(f"unknown campaign {campaign_name}")
    if campaign["status"] != "active":
        raise ValueError(
            f"campaign {campaign_name} must be active before close; found {campaign['status']}"
        )
    members = tuple(str(member) for member in campaign["slices"])
    claims = {str(claim["owner"]): claim for claim in queue.load_claims(root)}
    if set(claims) != set(members):
        missing = sorted(set(members) - set(claims))
        extra = sorted(set(claims) - set(members))
        raise ValueError(
            f"campaign {campaign_name} active claims must exactly match members; "
            f"missing={missing}, extra={extra}"
        )
    for member in members:
        record = slices[member]
        if record["status"] not in {"partial", "covered"}:
            raise ValueError(
                f"campaign member {member} must be partial or covered before close; "
                f"found {record['status']}"
            )
        claim = claims[member]
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

    removals: dict[Path, set[str]] = {}
    terminal_anchors: list[tuple[str, str, str]] = []
    all_retired_artifacts = {
        artifact for row in transfer_rows for artifact in _split_artifacts(row[6])
    }
    for row in transfer_rows:
        old_owner, new_owner = row[0], row[1]
        if not queue.OWNER_RE.fullmatch(old_owner) or not queue.OWNER_RE.fullmatch(new_owner):
            raise ValueError(f"{transfer_path}: invalid transfer owner")
        if old_owner == new_owner:
            raise ValueError(f"{transfer_path}: transfer owners must differ")
        if new_owner not in members:
            raise ValueError(
                f"{transfer_path}: terminal owner {new_owner} is not a campaign member"
            )
        source_anchor = _transfer_anchor(row, "source")
        test_anchor = _transfer_anchor(row, "test")
        if source_anchor is None and test_anchor is None:
            raise ValueError(f"{transfer_path}: transfer must contain an anchor")
        if source_anchor is not None and source_anchor not in slices[new_owner]["go_sources"]:
            raise ValueError(
                f"{transfer_path}: source {source_anchor} is not frozen in {new_owner}"
            )
        if test_anchor is not None and test_anchor not in slices[new_owner]["go_tests"]:
            raise ValueError(
                f"{transfer_path}: test {test_anchor} is not frozen in {new_owner}"
            )
        for kind, anchor in (("source", source_anchor), ("test", test_anchor)):
            if anchor is not None:
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
            if not path.is_file():
                raise ValueError(f"{transfer_path}: retired evidence fragment is missing: {artifact}")
            removals.setdefault(path, set()).add(anchor)
        for artifact in _split_artifacts(row[7]):
            path = _repository_path(root, artifact)
            if not path.is_file() and artifact not in all_retired_artifacts:
                raise ValueError(f"{transfer_path}: replacement artifact is missing: {artifact}")

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

    for kind in ("source", "test"):
        owners = _evidence_owners(root, writes, deletes, kind)
        for anchor_kind, anchor, new_owner in terminal_anchors:
            if anchor_kind != kind:
                continue
            found = owners.get(anchor, [])
            if found != [new_owner]:
                raise ValueError(
                    f"campaign transfer terminal {kind} evidence {anchor} must have "
                    f"exact owner {new_owner}; found {found}"
                )

    campaign_path = Path(campaign["_path"])
    archive_path = root / queue.INTEGRATED_CAMPAIGN_MEMBERS
    writes[campaign_path] = _integrated_campaign_text(campaign_path)
    writes[archive_path] = _archived_members_text(
        archive_path, campaign_name, members
    )
    return ClosePlan(campaign_name, members, writes, deletes, len(transfer_rows))


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
        root / queue.CLAIMS_DIR / f"{member}.claim.json" for member in plan.members
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
        with queue.claim_lock(root):
            current = build_close_plan(root, plan.campaign)
            if (
                current.members != plan.members
                or current.writes != plan.writes
                or current.deletes != plan.deletes
            ):
                raise ValueError("campaign close inputs changed after preflight")
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
                    "--bin", "go_test_ledger", "--", "--write",
                ],
                root,
            )
            command_runner(
                [sys.executable, "scripts/status-dashboard.py", "--write"], root
            )
            if validate:
                queue.validate_claims(root)
        if run_gate:
            command_runner(["scripts/rewrite-gate.sh", "integrate"], root)
            receipt = queue.load_integration_state(root, queue.INTEGRATION_RECEIPT)
            if set(receipt["claims"]) != set(plan.members):
                raise ValueError(
                    "shared gate receipt claims differ from exact campaign membership"
                )
            for member in plan.members:
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


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--campaign", required=True)
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
        plan = build_close_plan(RUST_ROOT, arguments.campaign)
        if arguments.apply or arguments.gate:
            apply_close_plan(RUST_ROOT, plan, run_gate=arguments.gate)
            action = "gated-and-released" if arguments.gate else "applied"
        else:
            action = "dry-run"
        print(
            f"campaign_close\t{action}\t{plan.campaign}\t"
            f"members={len(plan.members)}\ttransfers={plan.transfer_count}\t"
            f"writes={len(plan.writes)}\tdeletes={len(plan.deletes)}"
        )
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
