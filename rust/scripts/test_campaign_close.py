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

"""Focused regression tests for transactional campaign close bookkeeping."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("campaign_close.py")
SPEC = importlib.util.spec_from_file_location("campaign_close", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
campaign_close = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign_close)


class CampaignCloseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.repository = Path(self.temporary.name)
        self.root = self.repository / "rust"
        coverage = self.root / "difftests/corpus/coverage"
        source_evidence = coverage / "evidence/source"
        test_evidence = coverage / "evidence/tests"
        transfers = coverage / "evidence/transfers"
        source_evidence.mkdir(parents=True)
        test_evidence.mkdir(parents=True)
        transfers.mkdir(parents=True)
        (coverage / "external_go_source_inventory.tsv").write_text(
            "# external source\n", encoding="utf-8"
        )
        (coverage / "external_go_test_inventory.tsv").write_text(
            "# external test\n", encoding="utf-8"
        )

        self.old_source = source_evidence / "old-owner.tsv"
        self.old_test = test_evidence / "old-owner.tsv"
        self.new_source = source_evidence / "member-a.tsv"
        self.new_test = test_evidence / "member-a.tsv"
        self.old_source.write_text(
            "# predecessor source evidence\n"
            "pkg/planner/source0.go\tPARTIAL\told-owner\trust/old.rs\tmove me\n"
            "pkg/planner/source1.go\tPARTIAL\told-owner\trust/old.rs\tkeep me\n",
            encoding="utf-8",
        )
        self.old_test.write_text(
            "# predecessor test evidence\n"
            "go_test\tpkg/planner/source0_test.go\t10\tTest0\tPARTIAL\told-owner\trust/old.rs\tmove me\n",
            encoding="utf-8",
        )
        self.new_source.write_text(
            "# terminal source evidence\n"
            "pkg/planner/source0.go\tPARTIAL\tmember-a\trust/impl.rs\tported\n",
            encoding="utf-8",
        )
        self.new_test.write_text(
            "# terminal test evidence\n"
            "go_test\tpkg/planner/source0_test.go\t10\tTest0\tPARTIAL\tmember-a\trust/impl.rs\tported\n"
            "go_test\tpkg/planner/source1_test.go\t11\tTest1\tPARTIAL\tmember-a\trust/impl.rs\tported\n",
            encoding="utf-8",
        )
        (self.root / "old.rs").write_text("old", encoding="utf-8")
        (self.root / "impl.rs").write_text("new", encoding="utf-8")

        sources = []
        for index in range(9):
            if index in (0, 1):
                owner = "old-owner"
                artifact = "rust/difftests/corpus/coverage/evidence/source/old-owner.tsv"
                status = "PARTIAL"
                note = "old"
            else:
                owner = artifact = note = "-"
                status = "UNTRIAGED"
            sources.append(
                f"pkg/planner/source{index}.go\t100\ttidb-planner\tfalse\t"
                f"{status}\t{owner}\t{artifact}\t{note}\n"
            )
        (coverage / "go_source_inventory.tsv").write_text(
            "# source inventory\n" + "".join(sources), encoding="utf-8"
        )

        tests = []
        for index in range(50):
            if index == 0:
                status, owner, artifact, note = (
                    "PARTIAL",
                    "old-owner",
                    "rust/difftests/corpus/coverage/evidence/tests/old-owner.tsv",
                    "old",
                )
            else:
                status = "UNTRIAGED"
                owner = artifact = note = "-"
            tests.append(
                f"go_test\tpkg/planner/source{index}_test.go\t{10 + index}\t"
                f"Test{index}\tplan\t{status}\t{owner}\t{artifact}\t{note}\n"
            )
        (coverage / "go_test_inventory.tsv").write_text(
            "# test inventory\n" + "".join(tests), encoding="utf-8"
        )

        slices = self.root / "workstreams/slices"
        campaigns = self.root / "workstreams/campaigns"
        claims = self.root / "workstreams/claims"
        slices.mkdir(parents=True)
        campaigns.mkdir(parents=True)
        claims.mkdir(parents=True)
        self.campaign_path = campaigns / "campaign-x.toml"
        self.archive_path = campaigns / "integrated-members.tsv"

        self.member_anchors: dict[str, tuple[list[str], list[str]]] = {}
        for member, source_range, test_range in (
            ("member-a", range(5), range(25)),
            ("member-b", range(5, 9), range(25, 50)),
        ):
            member_sources = [f"pkg/planner/source{index}.go" for index in source_range]
            member_tests = [
                f"pkg/planner/source{index}_test.go:{10 + index}:Test{index}"
                for index in test_range
            ]
            self.member_anchors[member] = (member_sources, member_tests)
            source_text = ", ".join(json.dumps(item) for item in member_sources)
            test_text = ", ".join(json.dumps(item) for item in member_tests)
            (slices / f"{member}.toml").write_text(
                f'schema = "1"\nslice = "{member}"\nstatus = "partial"\n'
                'target = "tidb-planner"\nring = "plan"\n'
                f'consumer = "{member}"\ntest_target = "{member}"\n'
                f"go_sources = [{source_text}]\ngo_tests = [{test_text}]\n"
                f'depends_on = []\nrust_paths = ["rust/{member}.rs"]\n',
                encoding="utf-8",
            )
            (claims / f"{member}.claim.json").write_text(
                json.dumps(
                    {
                        "schema": 2,
                        "owner": member,
                        "sources": member_sources,
                        "tests": member_tests,
                        "module_sources": [],
                        "module_tests": [],
                    }
                ),
                encoding="utf-8",
            )
        self.campaign_path.write_text(
            'schema = "1"\ncampaign = "campaign-x"\nstatus = "planned"\n'
            'slices = ["member-a", "member-b"]\n',
            encoding="utf-8",
        )
        (transfers / "campaign-x.tsv").write_text(
            "# old_owner new_owner source_path test_path test_line test_name retired_artifacts new_artifacts note\n"
            "old-owner\tmember-a\tpkg/planner/source0.go\tpkg/planner/source0_test.go\t10\tTest0\t"
            "rust/difftests/corpus/coverage/evidence/source/old-owner.tsv,"
            "rust/difftests/corpus/coverage/evidence/tests/old-owner.tsv\t"
            "rust/difftests/corpus/coverage/evidence/source/member-a.tsv,"
            "rust/difftests/corpus/coverage/evidence/tests/member-a.tsv,rust/impl.rs\tmove exact rows\n"
            "old-owner\tmember-a\t-\tpkg/planner/source1_test.go\t11\tTest1\t-\t"
            "rust/difftests/corpus/coverage/evidence/tests/member-a.tsv,rust/impl.rs\t"
            "no predecessor artifact existed\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def prepare_complete_member_a_evidence(self) -> None:
        for member in ("member-a", "member-b"):
            path = self.root / f"workstreams/slices/{member}.toml"
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    'status = "partial"', 'status = "ready"', 1
                ),
                encoding="utf-8",
            )
        self.new_source.write_text(
            self.new_source.read_text(encoding="utf-8")
            + "".join(
                f"pkg/planner/source{index}.go\tPARTIAL\tmember-a\t"
                "rust/impl.rs\tported\n"
                for index in range(1, 5)
            ),
            encoding="utf-8",
        )
        self.new_test.write_text(
            self.new_test.read_text(encoding="utf-8")
            + "".join(
                f"go_test\tpkg/planner/source{index}_test.go\t{10 + index}\t"
                f"Test{index}\tPARTIAL\tmember-a\trust/impl.rs\tported\n"
                for index in range(2, 25)
            ),
            encoding="utf-8",
        )
        transfer_path = (
            self.root
            / "difftests/corpus/coverage/evidence/transfers/campaign-x.tsv"
        )
        transfer_path.write_text(
            transfer_path.read_text(encoding="utf-8")
            + "old-owner\tmember-a\tpkg/planner/source1.go\t-\t-\t-\t"
            "rust/difftests/corpus/coverage/evidence/source/old-owner.tsv\t"
            "rust/difftests/corpus/coverage/evidence/source/member-a.tsv,"
            "rust/impl.rs\tmove remaining source row\n",
            encoding="utf-8",
        )

    def prepare_split_member_a_test_file(self) -> tuple[Path, list[str]]:
        self.prepare_complete_member_a_evidence()
        replacements = []
        for index in (2, 3):
            old_path = f"pkg/planner/source{index}_test.go"
            new_path = "pkg/planner/source1_test.go"
            line = 10 + index
            name = f"Test{index}"
            replacements.append(
                (old_path, new_path, line, name)
            )
        for path in (
            self.root / "difftests/corpus/coverage/go_test_inventory.tsv",
            self.new_test,
            self.root / "workstreams/slices/member-a.toml",
        ):
            text = path.read_text(encoding="utf-8")
            for old_path, new_path, _line, _name in replacements:
                text = text.replace(old_path, new_path)
            path.write_text(text, encoding="utf-8")

        claim_path = self.root / "workstreams/claims/member-a.claim.json"
        claim = json.loads(claim_path.read_text(encoding="utf-8"))
        for old_path, new_path, line, name in replacements:
            old_anchor = f"{old_path}:{line}:{name}"
            new_anchor = f"{new_path}:{line}:{name}"
            index = claim["tests"].index(old_anchor)
            claim["tests"][index] = new_anchor
            member_index = self.member_anchors["member-a"][1].index(old_anchor)
            self.member_anchors["member-a"][1][member_index] = new_anchor
        claim_path.write_text(json.dumps(claim), encoding="utf-8")

        manifest = (
            self.root
            / "difftests/corpus/coverage/go_test_domain_manifest.tsv"
        )
        manifest.write_text(
            "# source_path\tsource_line\ttest_name\ttest_domain\n"
            "pkg/planner/source1_test.go\t11\tTest1\tstable-existing-domain\n",
            encoding="utf-8",
        )
        additions = [
            f"pkg/planner/source1_test.go:{line}:{name}"
            for _old_path, _new_path, line, name in replacements
        ]
        return manifest, additions

    def regenerate_member_a(self, command: list[str], root: Path) -> None:
        if "go_source_ledger" in command:
            path = root / "difftests/corpus/coverage/go_source_inventory.tsv"
            rendered = []
            for line in path.read_text(encoding="utf-8").splitlines():
                fields = line.split("\t")
                if fields[0] in self.member_anchors["member-a"][0]:
                    fields[4:] = [
                        "PARTIAL",
                        "member-a",
                        "rust/difftests/corpus/coverage/evidence/source/member-a.tsv",
                        "ported",
                    ]
                rendered.append("\t".join(fields))
            path.write_text("\n".join(rendered) + "\n", encoding="utf-8")
        elif "go_test_ledger" in command:
            path = root / "difftests/corpus/coverage/go_test_inventory.tsv"
            anchors = set(self.member_anchors["member-a"][1])
            rendered = []
            for line in path.read_text(encoding="utf-8").splitlines():
                fields = line.split("\t")
                anchor = (
                    f"{fields[1]}:{int(fields[2])}:{fields[3]}"
                    if len(fields) == 9 and not line.startswith("#")
                    else None
                )
                if anchor in anchors:
                    fields[5:] = [
                        "PARTIAL",
                        "member-a",
                        "rust/difftests/corpus/coverage/evidence/tests/member-a.tsv",
                        "ported",
                    ]
                rendered.append("\t".join(fields))
            path.write_text("\n".join(rendered) + "\n", encoding="utf-8")
        else:
            (root / "STATUS.md").write_text("generated\n", encoding="utf-8")

    def test_preflight_preserves_partial_fragment_and_treats_dash_as_no_path(self) -> None:
        before_source = self.old_source.read_text(encoding="utf-8")
        before_test = self.old_test.read_text(encoding="utf-8")

        plan = campaign_close.build_close_plan(self.root, "campaign-x")

        self.assertIn(self.old_source, plan.writes)
        self.assertNotIn("source0.go", plan.writes[self.old_source])
        self.assertIn("source1.go", plan.writes[self.old_source])
        self.assertIn(self.old_test, plan.deletes)
        self.assertNotIn(self.root / "-", plan.touched_paths)
        transfer_path = (
            self.root
            / "difftests/corpus/coverage/evidence/transfers/campaign-x.tsv"
        )
        transfer_rows = [
            line.split("\t")
            for line in plan.writes[transfer_path].splitlines()
            if line and not line.startswith("#")
        ]
        self.assertEqual(
            transfer_rows[0][6],
            "rust/difftests/corpus/coverage/evidence/tests/old-owner.tsv",
        )
        self.assertEqual(transfer_rows[1][6], "-")
        self.assertEqual(plan.transfer_count, 2)
        # Dry-run/preflight computes the complete mutation but changes nothing.
        self.assertEqual(self.old_source.read_text(encoding="utf-8"), before_source)
        self.assertEqual(self.old_test.read_text(encoding="utf-8"), before_test)
        self.assertIn('status = "planned"', self.campaign_path.read_text())
        self.assertFalse(self.archive_path.exists())

    def test_preflight_accepts_covered_inactive_member_and_unrelated_claim(self) -> None:
        member_b = self.root / "workstreams/slices/member-b.toml"
        member_b.write_text(
            member_b.read_text(encoding="utf-8").replace(
                'status = "partial"', 'status = "covered"', 1
            ),
            encoding="utf-8",
        )
        (self.root / "workstreams/claims/member-b.claim.json").unlink()

        source_inventory = (
            self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        )
        source_inventory.write_text(
            source_inventory.read_text(encoding="utf-8")
            + "pkg/planner/source9.go\t100\ttidb-planner\tfalse\t"
            "UNTRIAGED\t-\t-\t-\n",
            encoding="utf-8",
        )
        (self.root / "unrelated.rs").write_text("unrelated", encoding="utf-8")
        (self.root / "workstreams/slices/unrelated.toml").write_text(
            'schema = "1"\nslice = "unrelated"\nstatus = "ready"\n'
            'target = "tidb-planner"\nring = "plan"\nconsumer = "unrelated"\n'
            'test_target = "unrelated"\ngo_sources = ["pkg/planner/source9.go"]\n'
            'go_tests = []\ndepends_on = []\nrust_paths = ["rust/unrelated.rs"]\n',
            encoding="utf-8",
        )
        (self.root / "workstreams/claims/unrelated.claim.json").write_text(
            json.dumps(
                {
                    "schema": 2,
                    "owner": "unrelated",
                    "sources": ["pkg/planner/source9.go"],
                    "tests": [],
                    "module_sources": [],
                    "module_tests": [],
                }
            ),
            encoding="utf-8",
        )

        plan = campaign_close.build_close_plan(self.root, "campaign-x")

        self.assertEqual(plan.members, ("member-a", "member-b"))
        self.assertEqual(plan.active_members, ("member-a",))

        releases: list[str] = []
        with (
            mock.patch.object(
                campaign_close.queue,
                "load_integration_state",
                return_value={
                    "schema": 1,
                    "claims": {"member-a": {}, "unrelated": {}},
                },
            ),
            mock.patch.object(
                campaign_close.queue,
                "release",
                side_effect=lambda _root, member, integrated, abandon: releases.append(member),
            ),
        ):
            campaign_close.apply_close_plan(
                self.root,
                plan,
                run_gate=True,
                command_runner=lambda _command, _root: None,
                validate=False,
            )
        self.assertEqual(releases, ["member-a"])

    def test_preflight_rejects_inactive_partial_member(self) -> None:
        (self.root / "workstreams/claims/member-b.claim.json").unlink()

        with self.assertRaisesRegex(
            ValueError, "inactive campaign member member-b must already be covered"
        ):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_preflight_rejects_omitted_transfer_with_duplicate_evidence_owner(self) -> None:
        self.new_source.write_text(
            self.new_source.read_text(encoding="utf-8")
            + "pkg/planner/source1.go\tPARTIAL\tmember-a\trust/impl.rs\tported without transfer\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(
            ValueError,
            "post-close source evidence pkg/planner/source1.go has duplicate owners",
        ):
            campaign_close.build_close_plan(self.root, "campaign-x")

    def test_apply_keeps_partial_fragment_and_passes_real_queue_validation(self) -> None:
        source_inventory = (
            self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        )
        test_inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"

        def regenerate(command: list[str], root: Path) -> None:
            if "go_source_ledger" in command:
                source_inventory.write_text(
                    source_inventory.read_text(encoding="utf-8").replace(
                        "PARTIAL\told-owner\t"
                        "rust/difftests/corpus/coverage/evidence/source/old-owner.tsv\told",
                        "PARTIAL\tmember-a\t"
                        "rust/difftests/corpus/coverage/evidence/source/member-a.tsv\tported",
                        1,
                    ),
                    encoding="utf-8",
                )
            elif "go_test_ledger" in command:
                text = test_inventory.read_text(encoding="utf-8")
                text = text.replace(
                    "PARTIAL\told-owner\t"
                    "rust/difftests/corpus/coverage/evidence/tests/old-owner.tsv\told",
                    "PARTIAL\tmember-a\t"
                    "rust/difftests/corpus/coverage/evidence/tests/member-a.tsv\tported",
                    1,
                )
                text = text.replace(
                    "Test1\tplan\tUNTRIAGED\t-\t-\t-",
                    "Test1\tplan\tPARTIAL\tmember-a\t"
                    "rust/difftests/corpus/coverage/evidence/tests/member-a.tsv\tported",
                    1,
                )
                test_inventory.write_text(text, encoding="utf-8")
            else:
                (root / "STATUS.md").write_text("generated\n", encoding="utf-8")

        # Reproduce the Campaign 23 failure after inventory generation: the
        # terminal owner is correct, but the surviving partial fragment is
        # still falsely declared retired by the original transfer row.
        regenerate(["go_source_ledger"], self.root)
        regenerate(["go_test_ledger"], self.root)
        with self.assertRaisesRegex(ValueError, "retired artifact still exists"):
            campaign_close.queue.validate_transfers(self.root)

        plan = campaign_close.build_close_plan(self.root, "campaign-x")

        campaign_close.apply_close_plan(
            self.root, plan, command_runner=regenerate, validate=True
        )

        self.assertTrue(self.old_source.exists())
        self.assertIn("source1.go", self.old_source.read_text(encoding="utf-8"))
        self.assertFalse(self.old_test.exists())
        transfer_path = (
            self.root
            / "difftests/corpus/coverage/evidence/transfers/campaign-x.tsv"
        )
        rows = campaign_close.queue.read_tsv(transfer_path, 9)
        self.assertNotIn(
            "rust/difftests/corpus/coverage/evidence/source/old-owner.tsv",
            rows[0][6],
        )
        self.assertEqual(
            rows[0][6],
            "rust/difftests/corpus/coverage/evidence/tests/old-owner.tsv",
        )
        self.assertEqual(rows[1][6], "-")
        # Exercise the production validator again explicitly: a surviving
        # predecessor fragment must no longer be treated as a retired artifact.
        campaign_close.queue.validate_claims(self.root)

    def test_apply_rolls_back_all_bookkeeping_when_a_generator_fails(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        before = {
            path: path.read_bytes() if path.exists() else None
            for path in plan.touched_paths
        }

        def fail(_command: list[str], _root: Path) -> None:
            raise RuntimeError("generator failed")

        with self.assertRaisesRegex(RuntimeError, "generator failed"):
            campaign_close.apply_close_plan(
                self.root, plan, command_runner=fail, validate=False
            )

        for path, content in before.items():
            self.assertEqual(path.read_bytes() if path.exists() else None, content)

    def test_member_promotion_retains_claim_and_does_not_close_campaign(self) -> None:
        self.prepare_complete_member_a_evidence()
        claim_path = self.root / "workstreams/claims/member-a.claim.json"
        before_claim = claim_path.read_bytes()
        plan = campaign_close.build_member_promotion_plan(
            self.root, "campaign-x", "member-a"
        )

        slice_path = Path(
            campaign_close.queue.load_slices(self.root)["member-a"]["_path"]
        )
        self.assertIn('status = "partial"', plan.writes[slice_path])
        self.assertIn(
            'status = "ready"',
            (self.root / "workstreams/slices/member-a.toml").read_text(
                encoding="utf-8"
            ),
        )

        commands: list[list[str]] = []

        def regenerate(command: list[str], root: Path) -> None:
            commands.append(command)
            self.regenerate_member_a(command, root)

        campaign_close.apply_member_promotion_plan(
            self.root, plan, command_runner=regenerate, validate=True
        )

        self.assertEqual(claim_path.read_bytes(), before_claim)
        self.assertIn(
            'status = "partial"',
            (self.root / "workstreams/slices/member-a.toml").read_text(
                encoding="utf-8"
            ),
        )
        self.assertIn(
            'status = "ready"',
            (self.root / "workstreams/slices/member-b.toml").read_text(
                encoding="utf-8"
            ),
        )
        self.assertIn('status = "planned"', self.campaign_path.read_text())
        self.assertNotIn(["scripts/rewrite-gate.sh", "integrate"], commands)
        self.assertIn(
            [
                "cargo", "run", "--offline", "--locked", "-j12", "-p", "difftest",
                "--bin", "go_test_ledger", "--", "--write-evidence",
            ],
            commands,
        )
        with self.assertRaisesRegex(
            ValueError, "campaign member member-b must be partial or covered"
        ):
            campaign_close.build_close_plan(self.root, "campaign-x")
        member_b_path = self.root / "workstreams/slices/member-b.toml"
        member_b_path.write_text(
            member_b_path.read_text(encoding="utf-8").replace(
                'status = "ready"', 'status = "partial"', 1
            ),
            encoding="utf-8",
        )
        # Reprocessing the already-applied transfer history remains a valid
        # close preflight; deleted predecessor fragments stay declared retired.
        close_plan = campaign_close.build_close_plan(self.root, "campaign-x")
        transfer_path = (
            self.root
            / "difftests/corpus/coverage/evidence/transfers/campaign-x.tsv"
        )
        transfer_rows = campaign_close.queue.read_tsv(transfer_path, 9)
        planned_rows = [
            line.split("\t")
            for line in close_plan.writes[transfer_path].splitlines()
            if line and not line.startswith("#")
        ]
        self.assertEqual(planned_rows[0][6], transfer_rows[0][6])

    def test_member_promotion_rejects_incomplete_exact_evidence(self) -> None:
        member_a = self.root / "workstreams/slices/member-a.toml"
        member_a.write_text(
            member_a.read_text(encoding="utf-8").replace(
                'status = "partial"', 'status = "ready"', 1
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            ValueError, "requires source evidence pkg/planner/source1.go owned"
        ):
            campaign_close.build_member_promotion_plan(
                self.root, "campaign-x", "member-a"
            )

    def test_member_promotion_adds_only_missing_rows_in_split_test_file(self) -> None:
        manifest, additions = self.prepare_split_member_a_test_file()

        def regenerate_with_domain_gate(command: list[str], root: Path) -> None:
            if "go_test_ledger" in command:
                manifest_text = manifest.read_text(encoding="utf-8")
                for anchor in additions:
                    test_path, line, name = anchor.rsplit(":", 2)
                    expected = f"{test_path}\t{line}\t{name}\tmember-a"
                    if expected not in manifest_text:
                        raise RuntimeError(f"missing exact test-domain row {anchor}")
            self.regenerate_member_a(command, root)

        with self.assertRaisesRegex(RuntimeError, "missing exact test-domain row"):
            regenerate_with_domain_gate(["go_test_ledger"], self.root)

        plan = campaign_close.build_member_promotion_plan(
            self.root, "campaign-x", "member-a"
        )
        planned = plan.writes[manifest].splitlines()
        self.assertIn(
            "pkg/planner/source1_test.go\t11\tTest1\tstable-existing-domain",
            planned,
        )
        self.assertEqual(
            planned[-2:],
            [
                "pkg/planner/source1_test.go\t12\tTest2\tmember-a",
                "pkg/planner/source1_test.go\t13\tTest3\tmember-a",
            ],
        )
        self.assertFalse(any("source0_test.go" in line for line in planned))

        campaign_close.apply_member_promotion_plan(
            self.root,
            plan,
            command_runner=regenerate_with_domain_gate,
            validate=True,
        )

        self.assertEqual(manifest.read_text(encoding="utf-8"), plan.writes[manifest])

    def test_member_promotion_skips_non_top_level_test_domain_anchors(self) -> None:
        manifest, _additions = self.prepare_split_member_a_test_file()
        inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        inventory.write_text(
            inventory.read_text(encoding="utf-8")
            + "go_test_file\tpkg/planner/source1_test.go\t0\tsource1_test.go\tplan\t"
            "UNTRIAGED\t-\t-\t-\n",
            encoding="utf-8",
        )

        rendered = campaign_close._promoted_test_domain_manifest_text(
            self.root,
            "member-a",
            ["pkg/planner/source1_test.go:0:source1_test.go"],
        )

        self.assertIsNone(rendered)
        self.assertEqual(
            manifest.read_text(encoding="utf-8"),
            "# source_path\tsource_line\ttest_name\ttest_domain\n"
            "pkg/planner/source1_test.go\t11\tTest1\tstable-existing-domain\n",
        )

    def test_member_promotion_allows_direct_evidence_without_transfer_file(self) -> None:
        self.prepare_complete_member_a_evidence()
        self.old_source.unlink()
        self.old_test.unlink()
        transfer_path = (
            self.root
            / "difftests/corpus/coverage/evidence/transfers/campaign-x.tsv"
        )
        transfer_path.unlink()
        source_inventory = (
            self.root / "difftests/corpus/coverage/go_source_inventory.tsv"
        )
        source_inventory.write_text(
            source_inventory.read_text(encoding="utf-8").replace(
                "PARTIAL\told-owner\t"
                "rust/difftests/corpus/coverage/evidence/source/old-owner.tsv\told",
                "UNTRIAGED\t-\t-\t-",
            ),
            encoding="utf-8",
        )
        test_inventory = self.root / "difftests/corpus/coverage/go_test_inventory.tsv"
        test_inventory.write_text(
            test_inventory.read_text(encoding="utf-8").replace(
                "PARTIAL\told-owner\t"
                "rust/difftests/corpus/coverage/evidence/tests/old-owner.tsv\told",
                "UNTRIAGED\t-\t-\t-",
            ),
            encoding="utf-8",
        )

        plan = campaign_close.build_member_promotion_plan(
            self.root, "campaign-x", "member-a"
        )

        self.assertEqual(plan.transfer_count, 0)
        self.assertNotIn(transfer_path, plan.touched_paths)

    def test_member_promotion_rejects_future_member_transfer(self) -> None:
        self.prepare_complete_member_a_evidence()
        transfer_path = (
            self.root
            / "difftests/corpus/coverage/evidence/transfers/campaign-x.tsv"
        )
        transfer_path.write_text(
            transfer_path.read_text(encoding="utf-8")
            + "old-owner\tmember-b\tpkg/planner/source5.go\t-\t-\t-\t-\t"
            "rust/impl.rs\tfuture transfer\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "member-b has not been selected"):
            campaign_close.build_member_promotion_plan(
                self.root, "campaign-x", "member-a"
            )

    def test_member_promotion_rolls_back_status_and_transfers_on_failure(self) -> None:
        manifest, _additions = self.prepare_split_member_a_test_file()
        before_manifest = manifest.read_bytes()
        plan = campaign_close.build_member_promotion_plan(
            self.root, "campaign-x", "member-a"
        )
        self.assertIn(manifest, plan.touched_paths)
        before = {
            path: path.read_bytes() if path.exists() else None
            for path in plan.touched_paths
        }

        def fail(_command: list[str], _root: Path) -> None:
            raise RuntimeError("generator failed")

        with self.assertRaisesRegex(RuntimeError, "generator failed"):
            campaign_close.apply_member_promotion_plan(
                self.root,
                plan,
                command_runner=fail,
                validate=False,
            )

        for path, content in before.items():
            self.assertEqual(path.read_bytes() if path.exists() else None, content)
        self.assertEqual(manifest.read_bytes(), before_manifest)
        self.assertTrue(
            (self.root / "workstreams/claims/member-a.claim.json").is_file()
        )

    def test_gate_releases_exact_members_only_after_exact_receipt(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        commands: list[list[str]] = []
        releases: list[str] = []

        def succeed(command: list[str], _root: Path) -> None:
            commands.append(command)

        with (
            mock.patch.object(
                campaign_close.queue,
                "load_integration_state",
                return_value={"schema": 1, "claims": {member: {} for member in plan.members}},
            ),
            mock.patch.object(
                campaign_close.queue,
                "release",
                side_effect=lambda _root, member, integrated, abandon: releases.append(member),
            ),
        ):
            campaign_close.apply_close_plan(
                self.root,
                plan,
                run_gate=True,
                command_runner=succeed,
                validate=False,
            )

        self.assertIn(["scripts/rewrite-gate.sh", "integrate"], commands)
        self.assertEqual(
            commands[-1],
            [campaign_close.sys.executable, "scripts/status-dashboard.py", "--write"],
        )
        self.assertEqual(releases, list(plan.members))

    def test_gate_receipt_mismatch_releases_nothing_and_rolls_back(self) -> None:
        plan = campaign_close.build_close_plan(self.root, "campaign-x")
        before_campaign = self.campaign_path.read_bytes()
        releases: list[str] = []

        with (
            mock.patch.object(
                campaign_close.queue,
                "load_integration_state",
                return_value={"schema": 1, "claims": {"member-a": {}}},
            ),
            mock.patch.object(
                campaign_close.queue,
                "release",
                side_effect=lambda _root, member, integrated, abandon: releases.append(member),
            ),
        ):
            with self.assertRaisesRegex(
                ValueError, "receipt claims differ from the exact active claim set"
            ):
                campaign_close.apply_close_plan(
                    self.root,
                    plan,
                    run_gate=True,
                    command_runner=lambda _command, _root: None,
                    validate=False,
                )

        self.assertEqual(releases, [])
        self.assertEqual(self.campaign_path.read_bytes(), before_campaign)
        self.assertFalse(self.archive_path.exists())


if __name__ == "__main__":
    unittest.main()
